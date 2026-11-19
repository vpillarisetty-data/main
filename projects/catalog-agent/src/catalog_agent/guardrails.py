"""Deterministic SQL guardrails for LLM-generated queries.

Prompting the model with "only write SELECT statements" does not guarantee anything.
Every generated statement passes through this validator **before** it reaches a warehouse:

* exactly one statement, and it must be ``SELECT`` / ``WITH``; no DDL, DML or admin commands anywhere
* no file/network table functions (``read_files``, ``http_request`` ...)
* every table must be fully qualified and on the allow-list (certified Gold tables retrieved for this question)
* PII-tagged columns, and ``*`` over tables containing PII, are rejected unless the caller is entitled
* a ``LIMIT`` is enforced (added or lowered)

It uses a real tokenizer (string literals, quoted identifiers, comments), so ``'DROP'`` inside a
string literal is fine, while ``SELECT 1 /* x */ ; DROP TABLE t`` is caught.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass

from catalog_agent.catalog import Catalog

_TOKEN_RX = re.compile(
    r"""
    (?P<ws>\s+)
  | (?P<lcomment>--[^\n]*)
  | (?P<bcomment>/\*.*?\*/)
  | (?P<string>'(?:[^']|'')*')
  | (?P<qident>`[^`]+`|"[^"]+")
  | (?P<word>[A-Za-z_][A-Za-z0-9_$]*)
  | (?P<number>\d+(?:\.\d+)?)
  | (?P<punct><=>|<>|!=|<=|>=|\|\||::|[(),;.*=<>+\-/%])
    """,
    re.X | re.S,
)

FORBIDDEN = frozenset("""
insert update delete merge drop alter create replace truncate grant revoke deny copy call exec execute
optimize vacuum refresh msck analyze use load put remove unload undrop restore clone attach detach pragma
""".split())
FORBIDDEN_FUNCS = frozenset({"read_files", "read_kafka", "read_kinesis", "read_pubsub", "http_request",
                             "secret", "java_method", "reflect", "ai_query", "sleep", "pg_sleep"})
_CLAUSE_END = frozenset({"where", "group", "order", "having", "limit", "union", "intersect", "except",
                         "on", "using", "window", "qualify", "join", "inner", "left", "right", "full",
                         "cross", "natural", "lateral", "pivot", "unpivot", "tablesample"})


class GuardrailError(ValueError):
    """Raised with a message written so the LLM can fix its own query."""


@dataclass(frozen=True)
class Tok:
    kind: str
    text: str

    @property
    def low(self) -> str:
        return self.text.lower()


def tokenize(sql: str) -> list[Tok]:
    out, pos = [], 0
    while pos < len(sql):
        m = _TOKEN_RX.match(sql, pos)
        if not m:
            raise GuardrailError(f"unrecognised character {sql[pos]!r} at position {pos}")
        kind = m.lastgroup
        if kind not in ("ws", "lcomment", "bcomment"):
            out.append(Tok(kind, m.group()))
        pos = m.end()
    return out


def _ident(t: Tok) -> str | None:
    if t.kind == "word":
        return t.low
    if t.kind == "qident":
        return t.text[1:-1].lower()
    return None


@dataclass(frozen=True)
class GuardedSQL:
    sql: str
    tables: tuple[str, ...]


class SqlGuard:
    def __init__(self, catalog: Catalog, max_rows: int = 1000):
        self.catalog = catalog
        self.max_rows = max_rows

    # ---- parsing helpers --------------------------------------------------
    @staticmethod
    def _qualified_name(toks: list[Tok], i: int) -> tuple[str | None, int]:
        parts = []
        while i < len(toks):
            name = _ident(toks[i])
            if name is None:
                break
            parts.append(name)
            i += 1
            if i < len(toks) and toks[i].text == ".":
                i += 1
                continue
            break
        return (".".join(parts) if parts else None), i

    def _tables_and_ctes(self, toks: list[Tok]) -> tuple[set[str], set[str]]:
        ctes = {
            _ident(toks[i]) for i in range(len(toks) - 2)
            if _ident(toks[i]) and toks[i + 1].low == "as" and toks[i + 2].text == "("
        }
        tables: set[str] = set()
        i = 0
        while i < len(toks):
            if toks[i].low in ("from", "join"):
                i += 1
                while i < len(toks):
                    if toks[i].text == "(":
                        break  # subquery: its own FROM is visited by the outer loop
                    name, i = self._qualified_name(toks, i)
                    if name is None:
                        break
                    if i < len(toks) and toks[i].text == "(":  # table-valued function
                        raise GuardrailError(f"table-valued function '{name}(...)' is not allowed")
                    tables.add(name)
                    if i < len(toks) and toks[i].low == "as":
                        i += 1
                    if i < len(toks) and _ident(toks[i]) and toks[i].low not in _CLAUSE_END and toks[i].text != ",":
                        i += 1  # alias
                    if i < len(toks) and toks[i].text == ",":
                        i += 1
                        continue
                    break
                continue
            i += 1
        return tables - {c for c in ctes if c}, {c for c in ctes if c}

    # ---- main entry -------------------------------------------------------
    def check(self, sql: str, entitlements: Iterable[str] = (), allowed_tables: Iterable[str] | None = None
              ) -> GuardedSQL:
        toks = tokenize(sql.strip())
        while toks and toks[-1].text == ";":
            toks.pop()
        if not toks:
            raise GuardrailError("empty query")
        if any(t.text == ";" for t in toks):
            raise GuardrailError("only a single SQL statement is allowed")
        if toks[0].low not in ("select", "with"):
            raise GuardrailError("only read-only SELECT queries are allowed")
        for idx, t in enumerate(toks):
            if t.kind == "word" and t.low in FORBIDDEN:
                raise GuardrailError(f"keyword '{t.text.upper()}' is not allowed in read-only queries")
            if t.kind == "word" and t.low in FORBIDDEN_FUNCS and idx + 1 < len(toks) and toks[idx + 1].text == "(":
                raise GuardrailError(f"function '{t.low}' is not allowed")

        tables, _ = self._tables_and_ctes(toks)
        allowed = {a.lower() for a in (allowed_tables if allowed_tables is not None
                                        else (d.name for d in self.catalog.certified()))}
        for name in sorted(tables):
            if name.count(".") != 2:
                raise GuardrailError(f"table '{name}' must be fully qualified as catalog.schema.table")
            if name not in allowed:
                raise GuardrailError(
                    f"table '{name}' is not an approved dataset; use only: {', '.join(sorted(allowed))}"
                )

        ents = {e.lower() for e in entitlements}
        if "pii_reader" not in ents:
            pii: set[str] = set()
            for name in tables:
                doc = self.catalog.get(name)
                if doc:
                    pii |= doc.pii_columns
            if pii:
                for i, t in enumerate(toks):
                    if t.text == "*" and (i == 0 or toks[i - 1].low in ("select", ",", ".", "distinct")):
                        if i + 1 < len(toks) and toks[i + 1].text == ")":
                            continue  # count(*)
                        raise GuardrailError("SELECT * is not allowed on tables with PII; list the columns you need")
                    name = _ident(t)
                    if name in pii:
                        raise GuardrailError(f"column '{name}' is PII and you are not entitled to it")

        return GuardedSQL(self._enforce_limit(toks), tuple(sorted(tables)))

    def _enforce_limit(self, toks: list[Tok]) -> str:
        depth, limit_idx = 0, None
        for i, t in enumerate(toks):
            depth += t.text == "("
            depth -= t.text == ")"
            if depth == 0 and t.low == "limit":
                limit_idx = i
        text = [t.text for t in toks]
        if limit_idx is not None and limit_idx + 1 < len(toks) and toks[limit_idx + 1].kind == "number":
            if int(float(toks[limit_idx + 1].text)) > self.max_rows:
                text[limit_idx + 1] = str(self.max_rows)
        else:
            text += ["LIMIT", str(self.max_rows)]
        out = ""
        for piece in text:
            glue = piece in (",", ")", ".") or out.endswith(("(", ".")) or (piece == "(" and out[-1:].isalnum())
            if out and not glue:
                out += " "
            out += piece
        return out
