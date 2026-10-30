"""Data-quality profiling and Salesforce-specific checks for the DuckDB explorer."""

import copy
import re
import threading

BATCH = 40
SEV = {"error": 0, "warn": 1, "ok": 2}
MAX_ROWS = 50

KEY_PREFIXES = {
    "001": "Account", "003": "Contact", "005": "User", "006": "Opportunity", "00Q": "Lead",
    "500": "Case", "00G": "Group/Queue", "012": "RecordType", "02s": "EmailMessage",
    "00T": "Task", "00U": "Event", "701": "Campaign", "01t": "Product2", "801": "Order",
    "0Q0": "Quote", "00D": "Organization", "00e": "Profile", "00E": "UserRole",
    "0PS": "PermissionSet", "068": "ContentVersion", "069": "ContentDocument", "00O": "Report",
}

COUNTRY_SYN = {
    "unitedstates": "us", "usa": "us", "us": "us", "america": "us", "unitedstatesofamerica": "us",
    "unitedkingdom": "gb", "uk": "gb", "gb": "gb", "greatbritain": "gb", "england": "gb",
    "canada": "ca", "ca": "ca", "mexico": "mx", "mx": "mx", "australia": "au", "au": "au",
}

US_STATES = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR", "california": "CA",
    "colorado": "CO", "connecticut": "CT", "delaware": "DE", "florida": "FL", "georgia": "GA",
    "hawaii": "HI", "idaho": "ID", "illinois": "IL", "indiana": "IN", "iowa": "IA",
    "kansas": "KS", "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD",
    "massachusetts": "MA", "michigan": "MI", "minnesota": "MN", "mississippi": "MS",
    "missouri": "MO", "montana": "MT", "nebraska": "NE", "nevada": "NV", "newhampshire": "NH",
    "newjersey": "NJ", "newmexico": "NM", "newyork": "NY", "northcarolina": "NC",
    "northdakota": "ND", "ohio": "OH", "oklahoma": "OK", "oregon": "OR", "pennsylvania": "PA",
    "rhodeisland": "RI", "southcarolina": "SC", "southdakota": "SD", "tennessee": "TN",
    "texas": "TX", "utah": "UT", "vermont": "VT", "virginia": "VA", "washington": "WA",
    "westvirginia": "WV", "wisconsin": "WI", "wyoming": "WY", "districtofcolumbia": "DC",
}
STATE_SYN = {k: v.lower() for k, v in US_STATES.items()}

PLACEHOLDERS = [
    "test", "testing", "asdf", "n/a", "na", "none", "null", "unknown", "tbd", "xxx", "x", "-",
    ".", "?", "abc", "123", "sample", "dummy", "delete", "do not use",
]
PLACEHOLDER_COLS = {"name", "firstname", "lastname", "company", "email", "phone", "website", "city", "title"}
LEGAL = "inc|llc|ltd|corp|co|company|corporation|limited"
LOOKS = {"looks_boolean": "boolean", "looks_timestamp": "timestamp", "looks_date": "date", "looks_number": "number"}


def qi(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def short(v, n=60):
    s = str(v)
    return s if len(s) <= n else s[:n]


def is_str(c):
    return c["type"] == "VARCHAR"


def col_kind(name):
    if "." in name:
        return "relationship"
    return "custom" if re.search(r"__[a-z]+$", name, re.I) else "standard"


def is_id_col(c):
    n = c["name"]
    return is_str(c) and "." not in n and n.endswith("Id")


def dup_kind(c):
    """Duplicate/format kind of a VARCHAR column by name, or None."""
    if not is_str(c):
        return None
    n = c["name"].lower()
    if "date" in n:
        return None
    if n in ("name",):
        return "name"
    if n in ("email",) or n.endswith("email") or n.endswith("email__c"):
        return "email"
    if n in ("phone", "mobilephone"):
        return "phone"
    if n in ("website", "domain"):
        return "website"
    return None


def batches(seq, n=BATCH):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def sev_row(col, sev, cells, examples=None, weight=0):
    return {"column": col, "severity": sev, "cells": cells, "examples": (examples or [])[:5], "_w": weight}


def norm_key(s):
    return re.sub(r"[^a-z0-9]", "", s.strip().lower())


class SfAnalyzer:
    def __init__(self, explorer):
        self.ex = explorer
        self.lock = threading.Lock()
        self.key_locks = {}
        self.cache = {}
        self.where = ""

    def scoped(self, where):
        """A view of this analyzer whose queries only see rows matching `where` (shares the cache)."""
        s = copy.copy(self)
        s.where = where or ""
        return s

    def cached(self, kind, key, fn):
        """Compute fn() once per (kind, key, where); concurrent callers wait."""
        if kind != "idtables":
            key = (key, self.where)
        with self.lock:
            lk = self.key_locks.setdefault((kind, key), threading.Lock())
        with lk:
            if (kind, key) not in self.cache:
                self.cache[(kind, key)] = fn()
            return self.cache[(kind, key)]

    def src(self, schema, table, filtered=True):
        return self.ex.source(schema, table, self.where if filtered else "")

    # ---------------------------------------------------------------- profile

    def profile(self, schema, table):
        return self.cached("profile", (schema, table), lambda: self._profile(schema, table))

    def _profile(self, schema, table):
        cols = self.ex.columns(schema, table)
        if not cols:
            raise ValueError(f"Unknown table {schema}.{table}")
        cur, src = self.ex.cur(), self.src(schema, table)
        rows = cur.execute(f"select count(*) from {src}").fetchone()[0]
        stats = {}
        for batch in batches(cols):
            stats.update(self._profile_batch(cur, src, batch))
        out = [self._profile_col(c, stats.get(c["name"], {}), rows) for c in cols]
        return {"table": f"{schema}.{table}", "rows": rows, "columns": out}

    def _profile_exprs(self, c, with_range=True):
        q, t = qi(c["name"]), c["type"]
        e = {"nulls": f"count(*) - count({q})", "distinct": f"approx_count_distinct({q})"}
        if is_str(c):
            e["empties"] = f"count(*) filter (where trim({q}) = '')"
            e["untrimmed"] = f"count(*) filter (where {q} <> trim({q}) and trim({q}) <> '')"
            e["bool"] = f"count(*) filter (where lower(trim({q})) in ('true', 'false'))"
            e["ts"] = f"count(try_cast(nullif(trim({q}), '') as timestamp)) filter (where {q} like '%T%' or {q} like '%:%')"
            e["date"] = f"count(try_cast(nullif(trim({q}), '') as date))"
            e["num"] = f"count(try_cast(nullif(trim({q}), '') as double))"
        simple = t != "BOOLEAN" and not re.search(r"\[|STRUCT|MAP|UNION|BLOB", t)
        if with_range and simple:
            expr = f"nullif(trim({q}), '')" if is_str(c) else q
            e["min"], e["max"] = f"cast(min({expr}) as varchar)", f"cast(max({expr}) as varchar)"
        return e

    def _profile_batch(self, cur, src, batch):
        for with_range in (True, False):
            try:
                return self._run_profile(cur, src, batch, with_range)
            except Exception:
                continue
        out = {}
        for c in batch:
            try:
                out.update(self._run_profile(cur, src, [c], False))
            except Exception:
                out[c["name"]] = {}
        return out

    def _run_profile(self, cur, src, batch, with_range):
        parts, layout = [], []
        for c in batch:
            for k, expr in self._profile_exprs(c, with_range).items():
                parts.append(expr)
                layout.append((c["name"], k))
        row = cur.execute(f"select {', '.join(parts)} from {src}").fetchone()
        out = {}
        for (name, k), v in zip(layout, row):
            out.setdefault(name, {})[k] = v
        return out

    def _profile_col(self, c, s, rows):
        nulls, empties = s.get("nulls", 0) or 0, s.get("empties", 0) or 0
        filled = rows - nulls - empties
        pct = round(filled / rows * 100, 2) if rows else 0
        distinct = s.get("distinct", 0) or 0
        untrimmed = s.get("untrimmed", 0) or 0
        flags = self._flags(c, s, filled, pct, distinct, untrimmed)
        return {
            "name": c["name"], "type": c["type"], "kind": col_kind(c["name"]),
            "nulls": nulls, "empties": empties, "untrimmed": untrimmed, "filled_pct": pct,
            "distinct": distinct,
            "min": short(s["min"]) if s.get("min") is not None else None,
            "max": short(s["max"]) if s.get("max") is not None else None,
            "flags": flags,
        }

    def _flags(self, c, s, filled, pct, distinct, untrimmed):
        flags = []
        if filled <= 0:
            flags.append("all_null")
        elif pct < 1:
            flags.append("sparse")
        if distinct == 1:
            flags.append("constant")
        if filled > 0 and distinct >= 0.98 * filled:
            flags.append("unique")
        if untrimmed > 0:
            flags.append("whitespace")
        drift = self._drift_flag(c, s, filled)
        return flags + ([drift] if drift else [])

    def _drift_flag(self, c, s, filled):
        if not is_str(c) or filled < 1:
            return None
        for flag, key in (("looks_boolean", "bool"), ("looks_timestamp", "ts"), ("looks_date", "date"), ("looks_number", "num")):
            if (s.get(key) or 0) >= 0.95 * filled:
                if flag == "looks_number" and re.search(r"(phone|fax|zip|postal|id$)", c["name"], re.I):
                    continue
                return flag
        return None

    # ----------------------------------------------------------------- checks

    def sfchecks(self, schema, table):
        return self.cached("checks", (schema, table), lambda: self._sfchecks(schema, table))

    def _sfchecks(self, schema, table):
        cols = self.ex.columns(schema, table)
        if not cols:
            raise ValueError(f"Unknown table {schema}.{table}")
        t = (schema, table, cols)
        specs = [
            ("ids", "Salesforce Id format", "Id columns should hold 15- or 18-character alphanumeric Salesforce Ids with a consistent key prefix.", self.check_ids),
            ("refs", "Lookup integrity", "Lookup values should exist in the Id column of the object they point to.", self.check_refs),
            ("duplicates", "Duplicate records", "Names, emails, phones and websites that repeat exactly or after normalization.", self.check_duplicates),
            ("picklists", "Inconsistent values", "Text values that differ only by case, punctuation or spelling of the same thing.", self.check_picklists),
            ("formats", "Format & placeholder values", "Malformed emails, phones and websites, and placeholder junk such as 'test' or 'n/a'.", self.check_formats),
            ("drift", "Wrong column types", "Text columns whose values are really booleans, dates or numbers.", self.check_drift),
        ]
        return {"checks": [self._run_check(i, ti, d, lambda f=f: f(*t)) for i, ti, d, f in specs]}

    def _run_check(self, cid, title, desc, fn):
        base = {"id": cid, "title": title, "description": desc}
        try:
            summary, headers, rows = fn()
        except Exception as e:
            return {**base, "status": "error", "summary": f"Check failed: {e}", "headers": [], "rows": []}
        rows = sorted(rows, key=lambda r: (SEV[r["severity"]], -(r.get("_w") or 0)))[:MAX_ROWS]
        for r in rows:
            r.pop("_w", None)
        status = min((r["severity"] for r in rows), key=lambda s: SEV[s], default="ok")
        return {**base, "status": status, "summary": summary, "headers": headers, "rows": rows}

    # -- 1. ids

    def id_stats(self, schema, table, cols):
        return self.cached("ids", (schema, table), lambda: self._id_stats(schema, table, cols))

    def _id_stats(self, schema, table, cols):
        names = [c["name"] for c in cols if is_id_col(c)]
        cur, src = self.ex.cur(), self.src(schema, table)
        stats = {n: {"values": 0, "n15": 0, "n18": 0, "invalid": 0, "prefixes": []} for n in names}
        for batch in batches(names):
            parts = []
            for n in batch:
                q = qi(n)
                parts += [
                    f"count(*) filter (where {q} <> '')",
                    f"count(*) filter (where length({q}) = 15)",
                    f"count(*) filter (where length({q}) = 18)",
                    f"count(*) filter (where {q} <> '' and not regexp_matches({q}, '^[a-zA-Z0-9]{{15}}([a-zA-Z0-9]{{3}})?$'))",
                ]
            row = cur.execute(f"select {', '.join(parts)} from {src}").fetchone()
            for i, n in enumerate(batch):
                stats[n].update(zip(("values", "n15", "n18", "invalid"), row[i * 4:i * 4 + 4]))
        self._fill_prefixes(cur, src, names, stats)
        for n in names:
            stats[n]["examples"] = self._invalid_examples(cur, src, n) if stats[n]["invalid"] else []
        return stats

    def _fill_prefixes(self, cur, src, names, stats):
        for batch in batches(names, 20):
            sql = " union all ".join(
                f"select {i} as i, left({qi(n)}, 3) as p, count(*) as n from {src} where {qi(n)} <> '' group by 2"
                for i, n in enumerate(batch)
            )
            for i, p, n in cur.execute(sql).fetchall():
                stats[batch[i]]["prefixes"].append((p, n))
        for n in names:
            stats[n]["prefixes"].sort(key=lambda x: -x[1])

    def _invalid_examples(self, cur, src, n):
        q = qi(n)
        rows = cur.execute(
            f"select distinct {q} from {src} where {q} <> '' and not regexp_matches({q}, '^[a-zA-Z0-9]{{15}}([a-zA-Z0-9]{{3}})?$') limit 5"
        ).fetchall()
        return [short(r[0], 40) for r in rows]

    def prefix_label(self, p):
        return f"{p} {KEY_PREFIXES[p]}" if p in KEY_PREFIXES else p

    def check_ids(self, schema, table, cols):
        stats = self.id_stats(schema, table, cols)
        rows = []
        for n, s in stats.items():
            rows.append(self._id_row(n, s))
        mixed = sum(1 for n, s in stats.items() if s["n15"] and s["n18"])
        invalid = sum(1 for s in stats.values() if s["invalid"])
        multi = "Id" in stats and len(stats["Id"]["prefixes"]) > 1
        if not stats:
            summary = "No Id columns found"
        else:
            bits = []
            if invalid:
                bits.append(f"{invalid} contain invalid Ids")
            if mixed:
                bits.append(f"{mixed} mix 15- and 18-character Ids")
            if multi:
                bits.append("Id has multiple key prefixes")
            summary = f"{len(stats)} Id columns checked" + (": " + "; ".join(bits) if bits else ", all well-formed")
        headers = ["Column", "Values", "15-char", "18-char", "Invalid", "Prefixes"]
        return summary, headers, rows

    def _id_row(self, n, s):
        total = s["values"] or 1
        pref = " · ".join(f"{self.prefix_label(p)} {c / total * 100:.1f}%" for p, c in s["prefixes"][:3])
        if s["invalid"]:
            sev = "error"
        elif (s["n15"] and s["n18"]) or (n == "Id" and len(s["prefixes"]) > 1):
            sev = "warn"
        else:
            sev = "ok"
        cells = [n, s["values"], s["n15"], s["n18"], s["invalid"], pref]
        return sev_row(n, sev, cells, s["examples"], s["values"])

    # -- 2. refs

    def id_tables(self):
        """Base tables having a VARCHAR Id column, with their dominant key prefix."""
        def build():
            cur = self.ex.cur()
            rows = cur.execute(
                """select c.table_schema, c.table_name from information_schema.columns c
                   join information_schema.tables t using (table_schema, table_name)
                   where t.table_type = 'BASE TABLE' and c.column_name = 'Id' and c.data_type = 'VARCHAR'
                     and c.table_schema not in ('information_schema', 'pg_catalog')"""
            ).fetchall()
            out = []
            for s, t in rows:
                p = cur.execute(
                    f"select left(\"Id\", 3) p from {self.src(s, t, False)} where \"Id\" <> '' group by 1 order by count(*) desc limit 1"
                ).fetchone()
                out.append((s, t, p[0] if p else None))
            return out
        return self.cached("idtables", "all", build)

    def check_refs(self, schema, table, cols):
        stats = self.id_stats(schema, table, cols)
        lookups = {n: s for n, s in stats.items() if n != "Id" and s["values"] and s["prefixes"]}
        results, rows = {}, []
        by_target = {}
        for n, s in lookups.items():
            p = s["prefixes"][0][0]
            targets = [(ts, tt) for ts, tt, tp in self.id_tables() if tp == p]
            if not targets:
                label = f"{KEY_PREFIXES.get(p, p)} (not loaded)"
                rows.append(sev_row(n, "ok", [n, label, s["prefixes"][0][1], None, None], [], 0))
            for tg in targets:
                by_target.setdefault(tg, []).append(n)
        for tg, names in by_target.items():
            for batch in batches(names, 20):
                results.update(self._orphan_batch(schema, table, tg, batch, lookups))
        for (n, tg), (vals, orphans, ex) in results.items():
            pct = orphans / vals * 100 if vals else 0
            sev = "error" if pct > 5 else "warn" if orphans else "ok"
            tname = tg[1] if tg[0] == "main" else f"{tg[0]}.{tg[1]}"
            rows.append(sev_row(n, sev, [n, tname, vals, orphans, round(pct, 2)], ex, orphans))
        bad = len({r["column"] for r in rows if r["severity"] != "ok"})
        summary = f"{bad} of {len(lookups)} lookup columns have orphan values" if lookups else "No lookup columns found"
        return summary, ["Column", "Target", "Values", "Orphans", "Orphan %"], rows

    def _orphan_batch(self, schema, table, tg, batch, lookups):
        cur, src = self.ex.cur(), self.src(schema, table)
        tsrc = self.src(*tg, filtered=False)
        parts, pref = [], {}
        for n in batch:
            p = lookups[n]["prefixes"][0][0]
            pref[n] = p
            q = qi(n)
            cond = f"left({q}, 3) = '{p}'"
            parts += [f"count(*) filter (where {cond})",
                      f"count(*) filter (where {cond} and left({q}, 15) not in (select k from ids))"]
        row = cur.execute(
            f"with ids as materialized (select distinct left(\"Id\", 15) as k from {tsrc} where \"Id\" is not null) "
            f"select {', '.join(parts)} from {src}"
        ).fetchone()
        out = {}
        for i, n in enumerate(batch):
            vals, orphans = row[i * 2], row[i * 2 + 1]
            ex = []
            if orphans:
                q = qi(n)
                ex = [short(r[0], 40) for r in cur.execute(
                    f"select distinct {q} from {src} where left({q}, 3) = '{pref[n]}' and left({q}, 15) not in "
                    f"(select left(\"Id\", 15) from {tsrc} where \"Id\" is not null) limit 5").fetchall()]
            out[(n, tg)] = (vals, orphans, ex)
        return out

    # -- 3. duplicates

    def norm_sql(self, kind, v):
        if kind == "email":
            return f"lower(trim({v}))"
        if kind == "phone":
            d = f"regexp_replace({v}, '[^0-9]', '', 'g')"
            return f"case when length({d}) = 11 and starts_with({d}, '1') then substr({d}, 2) when length({d}) >= 7 then {d} end"
        if kind == "website":
            s = f"regexp_replace(regexp_replace(lower(trim({v})), '^https?://', ''), '^www\\.', '')"
            return f"nullif(regexp_replace({s}, '[/?#].*$', ''), '')"
        s = f"trim(regexp_replace(lower({v}), '[^a-z0-9]+', ' ', 'g'))"
        for _ in range(2):
            s = f"regexp_replace({s}, '\\s+({LEGAL})$', '')"
        return f"nullif(trim(regexp_replace({s}, '^the\\s+', '')), '')"

    def exact_sql(self, kind, v):
        return f"nullif({v}, '')" if kind == "phone" else f"lower(trim({v}))"

    def dup_stats(self, cur, src, expr, top=False):
        sql = (
            f"with g as materialized (select k, n from (select {expr} as k, count(*) as n from {src} group by 1) "
            f"where k is not null and k <> '' and n > 1) "
            f"select count(*), coalesce(sum(n), 0)"
            + (", (select list(k || ' (' || n || ')' order by n desc) from (select k, n from g order by n desc limit 5))" if top else "")
            + " from g"
        )
        return cur.execute(sql).fetchone()

    def check_duplicates(self, schema, table, cols):
        cur, src = self.ex.cur(), self.src(schema, table)
        rows, cands = [], [(c, dup_kind(c)) for c in cols if dup_kind(c)]
        for c, kind in cands:
            v = qi(c["name"])
            try:
                eg, er = self.dup_stats(cur, src, self.exact_sql(kind, v))[:2]
                ng, nr, ex = self.dup_stats(cur, src, self.norm_sql(kind, v), True)
            except Exception as e:
                rows.append(sev_row(c["name"], "ok", [c["name"], None, None, None, None], [f"failed: {e}"]))
                continue
            sev = "warn" if ng else "ok"
            rows.append(sev_row(c["name"], sev, [c["name"], eg, er, ng, nr], ex or [], nr))
        warn = sum(1 for r in rows if r["severity"] == "warn")
        summary = f"{warn} of {len(cands)} key columns contain duplicate values" if cands else "No name/email/phone/website columns found"
        return summary, ["Column", "Exact dup groups", "Rows in exact dups", "Normalized dup groups", "Rows in normalized dups"], rows

    # -- 4. picklists

    def check_picklists(self, schema, table, cols):
        prof = {p["name"]: p for p in self.profile(schema, table)["columns"]}
        cands = [c["name"] for c in cols if is_str(c) and not is_id_col(c) and not dup_kind(c)
                 and "." not in c["name"] and 2 <= prof[c["name"]]["distinct"] <= 2000
                 and not re.search(r"(phone|fax|email|website)", c["name"], re.I)
                 and not re.search(r"id(__c)?$", c["name"], re.I)]
        cur, src = self.ex.cur(), self.src(schema, table)
        rows = []
        for batch in batches(cands, 25):
            rows += self._picklist_batch(cur, src, batch)
        summary = f"{len(rows)} of {len(cands)} text columns have values that differ only by formatting" if cands else "No picklist-like columns found"
        warn = [r for r in rows]
        return summary, ["Column", "Clusters", "Affected rows"], warn

    def _picklist_batch(self, cur, src, batch):
        sql = " union all ".join(
            f"select {i} as i, {qi(n)} as v, count(*) as c from {src} where {qi(n)} <> '' group by 2"
            for i, n in enumerate(batch)
        )
        groups = {}
        for i, v, c in cur.execute(sql).fetchall():
            groups.setdefault(batch[i], []).append((v, c))
        return [r for n in batch if (r := self._picklist_row(n, groups.get(n, [])))]

    def picklist_key(self, name, raw):
        k = norm_key(raw)
        low = name.lower()
        if "country" in low:
            return COUNTRY_SYN.get(k, k)
        if "state" in low and "status" not in low:
            return STATE_SYN.get(k, k)
        return k

    def _picklist_row(self, name, values):
        clusters = {}
        for raw, c in values:
            clusters.setdefault(self.picklist_key(name, raw), []).append((raw, c))
        multi = [v for v in clusters.values() if len(v) > 1]
        if not multi:
            return None
        multi.sort(key=lambda v: -sum(c for _, c in v))
        affected = sum(c for v in multi for _, c in v)
        ex = [
            " | ".join(short(r, 30) for r, _ in sorted(v, key=lambda x: -x[1])[:4]) + f" ({sum(c for _, c in v):,})"
            for v in multi[:5]
        ]
        return sev_row(name, "warn", [name, len(multi), affected], ex, affected)

    # -- 5. formats

    def format_rules(self, cols):
        rules = []
        for c in cols:
            if not is_str(c):
                continue
            n, v, kind = c["name"], qi(c["name"]), dup_kind(c)
            if kind == "email":
                rules.append((n, "email", f"not regexp_matches(trim({v}), '^[^@\\s]+@[^@\\s]+\\.[A-Za-z]{{2,}}$')"))
            if re.search(r"(phone|fax)", n, re.I):
                d = f"regexp_replace({v}, '[^0-9]', '', 'g')"
                rules.append((n, "phone", f"(length({d}) < 10 or length({d}) > 15 or length(replace({d}, left({d}, 1), '')) = 0)"))
            if n.lower() == "website":
                rules.append((n, "website", f"not regexp_matches(lower(trim({v})), '^(https?://)?([a-z0-9-]+\\.)+[a-z]{{2,}}(/.*)?$')"))
            if n.lower() in PLACEHOLDER_COLS:
                rules.append((n, "placeholder", self._placeholder_sql(v, kind == "email")))
        return rules

    def _placeholder_sql(self, v, is_email):
        lst = ", ".join("'" + p + "'" for p in PLACEHOLDERS)
        cond = f"lower(trim({v})) in ({lst})"
        if is_email:
            cond += f" or lower(trim({v})) like 'test@%' or lower(trim({v})) like '%@example.com' or lower(trim({v})) like '%@test.com'"
        return f"({cond})"

    def check_formats(self, schema, table, cols):
        cur, src = self.ex.cur(), self.src(schema, table)
        rules = self.format_rules(cols)
        counts = {}
        for batch in batches(rules, 30):
            parts = []
            for n, _, bad in batch:
                v = qi(n)
                parts += [f"count(*) filter (where trim({v}) <> '')", f"count(*) filter (where trim({v}) <> '' and {bad})"]
            row = cur.execute(f"select {', '.join(parts)} from {src}").fetchone()
            for i, (n, ck, _) in enumerate(batch):
                counts[(n, ck)] = (row[i * 2], row[i * 2 + 1])
        rows = []
        for n, ck, bad in rules:
            vals, nbad = counts[(n, ck)]
            pct = nbad / vals * 100 if vals else 0
            sev = "error" if pct > 5 else "warn" if nbad else "ok"
            ex = self._bad_examples(cur, src, n, bad) if nbad else []
            rows.append(sev_row(n, sev, [n, ck, nbad, round(pct, 2)], ex, nbad))
        warn = sum(1 for r in rows if r["severity"] != "ok")
        summary = f"{warn} of {len(rows)} format checks found bad values" if rows else "No email, phone, website or placeholder-prone columns found"
        return summary, ["Column", "Check", "Bad values", "Bad %"], rows

    def _bad_examples(self, cur, src, n, bad):
        v = qi(n)
        rs = cur.execute(f"select distinct {v} from {src} where trim({v}) <> '' and {bad} limit 5").fetchall()
        return [short(r[0], 60) for r in rs]

    # -- 6. drift

    def check_drift(self, schema, table, cols):
        prof = self.profile(schema, table)["columns"]
        rows = []
        for p in prof:
            f = next((x for x in p["flags"] if x in LOOKS), None)
            if f:
                rows.append(sev_row(p["name"], "warn", [p["name"], p["type"], LOOKS[f], p["kind"]], [], 1 - p["filled_pct"] / 100))
        kinds = sorted({r["cells"][2] for r in rows})
        summary = (f"{len(rows)} text columns hold {', '.join(kinds)} values" if rows else "No text columns look mistyped")
        return summary, ["Column", "Current type", "Looks like", "Kind"], rows
