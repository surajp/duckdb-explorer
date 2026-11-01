# /// script
# requires-python = ">=3.10"
# dependencies = ["duckdb", "pytz"]
# ///
"""DuckDB explorer: browse tables/columns and group-by counts with charts.

Usage: uv run app.py path/to/db.duckdb [--port 8765]
"""

import argparse
import datetime
import decimal
import json
import threading
import uuid
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import duckdb

import sfchecks

INDEX_HTML = Path(__file__).with_name("index.html")
MAX_STACK_SERIES = 7  # + "(other)" = 8 categorical slots
# Date/time bucketing for group-by: grain -> (label, SQL template). Labels sort chronologically as text.
GRAINS = {
    "year": ("year", "year({c})"),
    "quarter": ("quarter", "printf('%d-Q%d', year({c}), quarter({c}))"),
    "month": ("month", "strftime({c}, '%Y-%m')"),
    "week": ("week", "printf('%d-W%02d', isoyear({c}), week({c}))"),
    "day": ("day", "cast({c} as date)"),
    "hour": ("hour", "strftime({c}, '%Y-%m-%d %H:00')"),
    "month_of_year": ("month of year", "strftime({c}, '%m %b')"),
    "week_of_year": ("week of year", "printf('W%02d', week({c}))"),
    "day_of_week": ("day of week", "printf('%d %s', isodow({c}), strftime({c}, '%a'))"),
    "hour_of_day": ("hour of day", "printf('%02d:00', hour({c}))"),
}


def qi(name: str) -> str:
    """Quote an identifier for DuckDB."""
    return '"' + name.replace('"', '""') + '"'


def to_json(v):
    if v is None or isinstance(v, (bool, int, float, str)):
        return v
    if isinstance(v, decimal.Decimal):
        return float(v)
    if isinstance(v, (datetime.date, datetime.datetime, datetime.time, uuid.UUID)):
        return str(v)
    if isinstance(v, bytes):
        return v.hex()
    return str(v)


class Explorer:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.con = duckdb.connect(db_path, read_only=True)
        self.local = threading.local()
        self.sf = sfchecks.SfAnalyzer(self)

    def cur(self):
        if not hasattr(self.local, "cur"):
            self.local.cur = self.con.cursor()
        return self.local.cur

    def tables(self):
        rows = self.cur().execute(
            """
            select t.table_schema, t.table_name, t.table_type,
                   (select count(*) from information_schema.columns c
                     where c.table_schema = t.table_schema and c.table_name = t.table_name) as ncols,
                   d.estimated_size
            from information_schema.tables t
            left join duckdb_tables() d
              on d.schema_name = t.table_schema and d.table_name = t.table_name
            where t.table_schema not in ('information_schema', 'pg_catalog')
            order by t.table_schema, t.table_name
            """
        ).fetchall()
        return [
            {"schema": s, "name": n, "type": ty, "columns": c, "rows": r}
            for s, n, ty, c, r in rows
        ]

    def columns(self, schema: str, table: str):
        rows = self.cur().execute(
            """
            select column_name, data_type from information_schema.columns
            where table_schema = ? and table_name = ? order by ordinal_position
            """,
            [schema, table],
        ).fetchall()
        return [{"name": n, "type": t} for n, t in rows]

    def source(self, schema: str, table: str, where: str = "") -> str:
        """FROM-clause source for a table, optionally restricted by a user WHERE clause."""
        src = f"{qi(schema)}.{qi(table)}"
        return f"(select * from {src} where ({where}))" if where else src

    def check_where(self, schema: str, table: str, where: str) -> str:
        """Validate a user WHERE clause against the table; returns it stripped."""
        where = where.strip()
        if not where:
            return ""
        self._validate(schema, table, [])
        sql = f"select 1 from {self.source(schema, table, where)} limit 0"
        if len(self.con.extract_statements(sql)) != 1:
            raise ValueError("WHERE clause must be a single expression")
        self.cur().execute(sql)
        return where

    def count(self, schema: str, table: str, where: str = "") -> int:
        return self.cur().execute(f"select count(*) from {self.source(schema, table, where)}").fetchone()[0]

    def _validate(self, schema, table, cols):
        known = {c["name"] for c in self.columns(schema, table)}
        if not known:
            raise ValueError(f"Unknown table {schema}.{table}")
        bad = [c for c in cols if c not in known]
        if bad:
            raise ValueError(f"Unknown columns: {bad}")

    def group_by(self, schema: str, table: str, cols: list[str], limit: int, where: str = "",
                 grains: dict[str, str] | None = None):
        """Top `limit` groups by count, bucketing columns that have a grain by date part. Rows are
        ordered by count descending at every level: by the first column's total, then within it by
        the next column's subtotal, and so on down to the group's own count. When the first column
        is bucketed by date, the first level is in timeline order instead."""
        grains = grains or {}
        self._validate(schema, table, cols)
        bad = {c: g for c, g in grains.items() if c not in cols or g not in GRAINS}
        if bad:
            raise ValueError(f"Unknown date grains: {bad}")
        exprs = [GRAINS[grains[c]][1].format(c=qi(c)) if c in grains else qi(c) for c in cols]
        labels = [f"{c} ({GRAINS[grains[c]][0]})" if c in grains else c for c in cols]
        chrono = cols[0] in grains
        src = self.source(schema, table, where)
        sel = ", ".join(f"{e} as {qi(f'c{i}')}" for i, e in enumerate(exprs))
        cur = self.cur()

        total = cur.execute(f"select count(*) from {src}").fetchone()[0]
        distinct = cur.execute(f"select count(*) from (select distinct {sel} from {src})").fetchone()[0]
        keys = [qi(f"c{i}") for i in range(len(cols))]
        # Subtotal of each ancestor level (first column, first two columns, ...) over all groups.
        subtotals = [f"sum(cnt) over (partition by {', '.join(keys[:k])}) as {qi(f't{k}')}"
                     for k in range(1, len(keys))]
        order = [f"{qi(f't{k}')} desc, {keys[k - 1]} nulls last" for k in range(1, len(keys))]
        if chrono:  # Date buckets label-sort chronologically; deeper levels stay by count.
            order = [f"{keys[0]} nulls last"] + order[1:]
        top = cur.execute(
            f"""
            with g as (select {sel}, count(*) as cnt from {src} group by all),
            topg as (select *{''.join(', ' + t for t in subtotals)} from g order by cnt desc limit ?)
            select {', '.join(keys)}, cnt from topg
            order by {', '.join(order + ['cnt desc', f'{keys[-1]} nulls last'])}
            """,
            [limit],
        ).fetchall()
        result = {
            "columns": labels,
            "total": total,
            "distinct": distinct,
            "rows": [[to_json(v) for v in r] for r in top],
            "stack": None,
        }
        if len(cols) > 1:
            result["stack"] = self._stack(cur, src, exprs, labels, limit, chrono)
        return result

    def _stack(self, cur, src, exprs, labels, limit, chrono=False):
        """Top `limit` values of the first expression, broken down by the remaining ones."""
        first, rest = exprs[0], exprs[1:]
        rest_label = " || ' | ' || ".join(f"coalesce(cast({c} as varchar), '(null)')" for c in rest)
        rows = cur.execute(
            f"""
            with base as (
              select cast({first} as varchar) as x, {rest_label} as s from {src}
            ),
            topx as (
              select x, count(*) as n from base group by x order by n desc limit ?
            ),
            inx as (
              select b.x, b.s from base b join topx t on b.x is not distinct from t.x
            ),
            tops as (
              select s from inx group by s order by count(*) desc limit ?
            )
            select i.x, case when i.s in (select s from tops) then i.s else '(other)' end as s,
                   count(*) as n
            from inx i group by all
            """,
            [limit, MAX_STACK_SERIES],
        ).fetchall()
        x_order = [r[0] for r in cur.execute(
            f"select cast({first} as varchar) x, count(*) n from {src} group by x order by n desc limit ?",
            [limit],
        ).fetchall()]
        if chrono:
            x_order.sort(key=lambda x: (x is None, x))
        totals: dict[str, int] = {}
        cells: dict[tuple, int] = {}
        for x, s, n in rows:
            totals[s] = totals.get(s, 0) + n
            cells[(x, s)] = n
        series = sorted(totals, key=lambda s: (s == "(other)", -totals[s]))
        return {
            "x_label": labels[0],
            "series_label": " | ".join(labels[1:]),
            "x": x_order,
            "series": [
                {"name": s, "data": [cells.get((x, s), 0) for x in x_order]} for s in series
            ],
        }


def make_handler(explorer: Explorer):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format, *args):
            pass

        def send(self, code, body: bytes, ctype: str):
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def send_json(self, obj, code=200):
            self.send(code, json.dumps(obj).encode(), "application/json")

        def do_GET(self):
            url = urlparse(self.path)
            q = {k: v[0] for k, v in parse_qs(url.query).items()}
            try:
                where = explorer.check_where(q["schema"], q["table"], q.get("where", "")) if "schema" in q else ""
                if url.path == "/":
                    self.send(200, INDEX_HTML.read_bytes(), "text/html; charset=utf-8")
                elif url.path == "/api/info":
                    self.send_json({"db": explorer.db_path})
                elif url.path == "/api/tables":
                    self.send_json(explorer.tables())
                elif url.path == "/api/columns":
                    self.send_json(explorer.columns(q["schema"], q["table"]))
                elif url.path == "/api/groupby":
                    cols = json.loads(q["cols"])
                    limit = max(1, min(int(q.get("limit", 10)), 100))
                    grains = json.loads(q.get("grains", "{}"))
                    self.send_json(explorer.group_by(q["schema"], q["table"], cols, limit, where, grains))
                elif url.path == "/api/profile":
                    self.send_json(explorer.sf.scoped(where).profile(q["schema"], q["table"]))
                elif url.path == "/api/sfchecks":
                    self.send_json(explorer.sf.scoped(where).sfchecks(q["schema"], q["table"]))
                elif url.path == "/api/where":
                    total = explorer.count(q["schema"], q["table"])
                    self.send_json({"where": where, "total": total,
                                    "matched": explorer.count(q["schema"], q["table"], where) if where else total})
                else:
                    self.send_json({"error": "not found"}, 404)
            except Exception as e:  # surface errors to the UI
                self.send_json({"error": str(e)}, 400)

    return Handler


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("db", help="path to a DuckDB database file")
    ap.add_argument("--port", type=int, default=8765)
    ap.add_argument("--no-browser", action="store_true")
    args = ap.parse_args()

    explorer = Explorer(args.db)
    server = ThreadingHTTPServer(("127.0.0.1", args.port), make_handler(explorer))
    url = f"http://127.0.0.1:{args.port}/"
    print(f"Serving {args.db} at {url}  (Ctrl+C to stop)")
    if not args.no_browser:
        webbrowser.open(url)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
