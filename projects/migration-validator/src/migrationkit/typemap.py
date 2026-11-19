"""Source-system type -> Delta Lake type mapping, with the semantic traps that break migrations.

Most migration bugs aren't syntax errors. The code runs, and the numbers are slightly wrong.
Each mapping carries a risk level and a note explaining what changes.

risk = "none"      safe, lossless
     = "precision" representable, but precision/range/format can change
     = "semantic"  meaning changes (e.g. Oracle DATE has a time part; SQL Server TINYINT is unsigned)
"""

from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class TypeMapping:
    source: str
    delta: str
    risk: str = "none"
    note: str = ""


_PARAMS = re.compile(r"^\s*([a-z0-9_ ]+?)\s*(?:\(\s*([^)]*)\s*\))?\s*((?:with|without)?\s*(?:local\s+)?(?:time\s+zone)?)?\s*$", re.I)


def _parse(t: str) -> tuple[str, list[str], str]:
    m = _PARAMS.match(t.strip().lower())
    if not m:
        return t.strip().lower(), [], ""
    base = re.sub(r"\s+", " ", m.group(1)).strip()
    params = [p.strip() for p in (m.group(2) or "").split(",") if p.strip()]
    return base, params, (m.group(3) or "").strip()


def _decimal(p: str | None, s: str | None, src: str) -> TypeMapping:
    if p in (None, "*"):
        return TypeMapping(src, "DECIMAL(38,10)", "precision",
                           "unconstrained precision: profile max integer digits/scale before fixing DECIMAL(38,10)")
    pi, si = int(p), int(s or 0)
    if si < 0:
        return TypeMapping(src, f"DECIMAL({min(38, pi - si)},0)", "precision", "negative scale (rounds to 10^n) expanded")
    if pi > 38:
        return TypeMapping(src, f"DECIMAL(38,{min(si, 37)})", "precision", "precision above 38 digits truncated")
    return TypeMapping(src, f"DECIMAL({pi},{si})")


def _ts(n: str | None, src: str, tz: bool = False) -> TypeMapping:
    note, risk = "", "none"
    if n and int(n) > 6:
        risk, note = "precision", f"fractional seconds({n}) truncated to microseconds"
    if tz:
        risk = "semantic"
        note = (note + "; " if note else "") + "converted to UTC instant: original offset is lost (store it separately if needed)"
    return TypeMapping(src, "TIMESTAMP", risk, note)


def _sqlserver(base: str, p: list[str], tz: str, src: str) -> TypeMapping:
    simple = {"bit": "BOOLEAN", "smallint": "SMALLINT", "int": "INT", "bigint": "BIGINT", "real": "FLOAT",
              "date": "DATE", "uniqueidentifier": "STRING", "xml": "STRING", "text": "STRING", "ntext": "STRING",
              "image": "BINARY", "varbinary": "BINARY", "binary": "BINARY", "smalldatetime": "TIMESTAMP"}
    if base == "tinyint":
        return TypeMapping(src, "SMALLINT", "semantic", "SQL Server TINYINT is UNSIGNED 0..255; Spark TINYINT is -128..127")
    if base in ("decimal", "numeric"):
        return _decimal(p[0] if p else "18", p[1] if len(p) > 1 else "0", src)
    if base == "money":
        return TypeMapping(src, "DECIMAL(19,4)")
    if base == "smallmoney":
        return TypeMapping(src, "DECIMAL(10,4)")
    if base == "float":
        return TypeMapping(src, "FLOAT" if p and int(p[0]) <= 24 else "DOUBLE")
    if base == "datetime":
        return TypeMapping(src, "TIMESTAMP", "precision", "DATETIME is rounded to .000/.003/.007s; compare at millisecond tolerance")
    if base == "datetime2":
        return _ts(p[0] if p else "7", src)
    if base == "datetimeoffset":
        return _ts(p[0] if p else "7", src, tz=True)
    if base == "time":
        return TypeMapping(src, "STRING", "semantic", "no TIME type in Delta: stored as 'HH:mm:ss.ffffff' string")
    if base in ("char", "nchar"):
        return TypeMapping(src, "STRING", "precision", "CHAR is blank-padded: trailing spaces change equality/joins; rtrim on load")
    if base in ("varchar", "nvarchar"):
        return TypeMapping(src, "STRING", "none", "collation was likely case-insensitive; Delta comparisons are case-sensitive")
    if base in ("timestamp", "rowversion"):
        return TypeMapping(src, "BINARY", "semantic", "ROWVERSION is a change counter, NOT a time; don't use as event time")
    if base == "sql_variant":
        return TypeMapping(src, "STRING", "semantic", "mixed-type column: cast explicitly per business meaning")
    if base in simple:
        return TypeMapping(src, simple[base])
    raise KeyError(base)


def _oracle(base: str, p: list[str], tz: str, src: str) -> TypeMapping:
    if base in ("number", "numeric", "decimal"):
        if not p:
            return _decimal(None, None, src)
        prec, scale = p[0], (p[1] if len(p) > 1 else "0")
        if prec != "*" and int(scale) == 0 and int(prec) <= 18:
            return TypeMapping(src, "INT" if int(prec) <= 9 else "BIGINT")
        return _decimal(prec, scale, src)
    if base in ("integer", "int", "smallint"):
        return TypeMapping(src, "DECIMAL(38,0)", "none", "Oracle INTEGER is NUMBER(38)")
    if base == "date":
        return TypeMapping(src, "TIMESTAMP", "semantic", "Oracle DATE includes a TIME part; mapping to DATE silently drops it")
    if base == "timestamp":
        return _ts(p[0] if p else "6", src, tz="time zone" in tz)
    if base in ("varchar2", "nvarchar2", "varchar", "clob", "nclob", "long"):
        return TypeMapping(src, "STRING", "semantic", "Oracle stores '' as NULL: target must not distinguish '' from NULL")
    if base in ("char", "nchar"):
        return TypeMapping(src, "STRING", "precision", "blank-padded CHAR: rtrim on load; '' is NULL in Oracle")
    if base in ("blob", "raw", "long raw"):
        return TypeMapping(src, "BINARY")
    if base in ("binary_double", "float", "double precision"):
        return TypeMapping(src, "DOUBLE")
    if base == "binary_float":
        return TypeMapping(src, "FLOAT")
    if base.startswith("interval"):
        return TypeMapping(src, "STRING", "semantic", "INTERVAL: store as ISO-8601 duration or seconds BIGINT")
    raise KeyError(base)


def _teradata(base: str, p: list[str], tz: str, src: str) -> TypeMapping:
    simple = {"byteint": "TINYINT", "smallint": "SMALLINT", "integer": "INT", "int": "INT", "bigint": "BIGINT",
              "float": "DOUBLE", "real": "DOUBLE", "double precision": "DOUBLE", "date": "DATE", "clob": "STRING",
              "blob": "BINARY", "byte": "BINARY", "varbyte": "BINARY", "varchar": "STRING"}
    if base in ("decimal", "numeric", "number"):
        return _decimal(p[0] if p else "5", p[1] if len(p) > 1 else "0", src)
    if base == "timestamp":
        return _ts(p[0] if p else "6", src, tz="time zone" in tz)
    if base == "char":
        return TypeMapping(src, "STRING", "precision", "blank-padded CHAR; Teradata comparisons are often NOT CASESPECIFIC")
    if base.startswith("period"):
        return TypeMapping(src, "STRUCT<begin: TIMESTAMP, end: TIMESTAMP>", "semantic", "PERIOD split into begin/end")
    if base in simple:
        return TypeMapping(src, simple[base])
    raise KeyError(base)


def _hive(base: str, p: list[str], tz: str, src: str) -> TypeMapping:
    if base in ("varchar", "char"):
        return TypeMapping(src, "STRING", "precision" if base == "char" else "none",
                           "Hive CHAR is padded" if base == "char" else "")
    if base == "decimal":
        return _decimal(p[0] if p else "10", p[1] if len(p) > 1 else "0", src)
    if base == "timestamp":
        return TypeMapping(src, "TIMESTAMP", "semantic",
                           "Hive TIMESTAMP is zone-less; check Impala/Hive int96 UTC conversion settings")
    ident = {"tinyint", "smallint", "int", "bigint", "float", "double", "boolean", "string", "binary", "date"}
    if base in ident:
        return TypeMapping(src, base.upper())
    raise KeyError(base)


_DIALECTS = {"sqlserver": _sqlserver, "oracle": _oracle, "teradata": _teradata, "hive": _hive}


def map_type(dialect: str, type_str: str) -> TypeMapping:
    fn = _DIALECTS.get(dialect.lower())
    if fn is None:
        raise ValueError(f"unsupported dialect '{dialect}', choose from {sorted(_DIALECTS)}")
    base, params, tz = _parse(type_str)
    try:
        return fn(base, params, tz, type_str.strip())
    except KeyError:
        return TypeMapping(type_str.strip(), "STRING", "semantic", f"unknown {dialect} type: review manually")


# ---------------------------------------------------------------- DDL conversion
_CREATE = re.compile(r"create\s+table\s+([\w.\[\]\"`]+)\s*\((.*)\)\s*[^)]*$", re.I | re.S)
_CONSTRAINT = re.compile(r"^(constraint|primary\s+key|foreign\s+key|unique|check|index|key)\b", re.I)


def _split_top(body: str) -> list[str]:
    parts, depth, cur = [], 0, []
    for ch in body:
        depth += ch == "("
        depth -= ch == ")"
        if ch == "," and depth == 0:
            parts.append("".join(cur))
            cur = []
        else:
            cur.append(ch)
    parts.append("".join(cur))
    return [p.strip() for p in parts if p.strip()]


_COL_TAIL = re.compile(
    r"\s(not\s+null|null|default|identity|constraint|primary\s+key|generated|collate|check|references|"
    r"unique|character\s+set|format|compress|sparse)\b", re.I)


def _type_end(tokens: str) -> tuple[str, bool]:
    """Split '<type ...> [NOT NULL] [DEFAULT ...] [IDENTITY(...)]' into (type, not_null)."""
    m = _COL_TAIL.search(tokens)
    type_str = tokens[: m.start()] if m else tokens
    low = tokens.lower()
    return type_str.strip(), bool(re.search(r"\snot\s+null\b|\sprimary\s+key\b", low))


def convert_ddl(dialect: str, ddl: str, target_table: str | None = None) -> tuple[str, list[tuple[str, TypeMapping]]]:
    statements = [s for s in re.split(r";\s*(?:\n|$)|\n\s*go\s*\n", ddl, flags=re.I) if s.strip()]
    outputs, risks = [], []
    for stmt in statements:
        m = _CREATE.search(stmt.strip())
        if not m:
            continue
        name = re.sub(r"[\[\]\"`]", "", m.group(1))
        cols = []
        for item in _split_top(m.group(2)):
            if _CONSTRAINT.match(item):
                continue
            col_m = re.match(r"([\[\"`]?[\w$#]+[\]\"`]?)\s+(.+)", item, re.S)
            if not col_m:
                continue
            col = re.sub(r"[\[\]\"`]", "", col_m.group(1)).lower()
            type_str, not_null = _type_end(" " + col_m.group(2).strip())
            tm = map_type(dialect, type_str)
            line = f"  `{col}` {tm.delta}{' NOT NULL' if not_null else ''}"
            if tm.risk != "none":
                risks.append((col, tm))
                line += f" COMMENT 'migrated from {tm.source} [{tm.risk}]'"
            cols.append(line)
        tbl = target_table or name.lower()
        outputs.append(f"CREATE TABLE IF NOT EXISTS {tbl} (\n" + ",\n".join(cols) + "\n) USING DELTA\n"
                       f"TBLPROPERTIES ('migration.source' = '{dialect}:{name}');")
    return "\n\n".join(outputs), risks
