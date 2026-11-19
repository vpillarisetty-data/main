"""Scalable source-vs-target reconciliation using hash buckets (Python backend).

Algorithm (same idea as data-diff tools, two streaming passes, memory O(buckets + diffs)):

1. **Summary pass.** Stream both sides once. For each row compute ``bucket = H(key) mod N`` and
   ``row_hash = H(canonical row)``, and keep per bucket ``(count, sum(row_hash))`` plus control totals
   (SUM of numeric columns), the reconciliation evidence finance and SOX auditors ask for.
2. **Drill-down pass.** Only buckets whose fingerprints differ are re-read, and their rows are
   compared by key into *missing*, *extra*, *changed* and *duplicate keys*, with per-column mismatch
   counts, samples and a diagnosis of each column's likely cause.

When 99.99% of the data matches, the second pass touches a tiny fraction of the rows. The Spark
backend (``migrationkit.spark``) runs the same algorithm distributed.
"""

from __future__ import annotations

import hashlib
from collections import Counter, defaultdict
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from decimal import Decimal

from migrationkit.normalize import NULL, SEP, Normalization, canonical, diagnose

RowSource = Callable[[], Iterable[Mapping[str, object]]]


@dataclass
class ColumnDiff:
    column: str
    mismatches: int = 0
    samples: list[tuple[str, str, str]] = field(default_factory=list)  # (key, source, target)
    diagnosis: str = ""


@dataclass
class ReconReport:
    table: str
    source_rows: int
    target_rows: int
    buckets: int
    mismatched_buckets: int
    missing_in_target: int = 0
    extra_in_target: int = 0
    changed: int = 0
    duplicate_keys_source: int = 0
    duplicate_keys_target: int = 0
    column_diffs: dict[str, ColumnDiff] = field(default_factory=dict)
    control_totals: dict[str, tuple[str, str]] = field(default_factory=dict)
    samples: dict[str, list[str]] = field(default_factory=dict)
    normalization: list[str] = field(default_factory=list)

    @property
    def matched(self) -> int:
        return self.source_rows - self.missing_in_target - self.changed - self.duplicate_keys_source

    @property
    def control_totals_ok(self) -> bool:
        return all(s == t for s, t in self.control_totals.values())

    @property
    def passed(self) -> bool:
        return (self.missing_in_target == self.extra_in_target == self.changed == 0
                and self.duplicate_keys_source == self.duplicate_keys_target == 0 and self.control_totals_ok
                and self.source_rows == self.target_rows)

    def to_markdown(self) -> str:
        icon = "✅ PASS" if self.passed else "❌ FAIL"
        pct = (self.matched / self.source_rows * 100) if self.source_rows else 100.0
        lines = [
            f"### `{self.table}`: {icon}",
            "",
            "| metric | value |", "|---|---|",
            f"| rows source / target | {self.source_rows:,} / {self.target_rows:,} |",
            f"| matched | {self.matched:,} ({pct:.4f}%) |",
            f"| missing in target | {self.missing_in_target:,} |",
            f"| extra in target | {self.extra_in_target:,} |",
            f"| changed | {self.changed:,} |",
            f"| duplicate keys (src / tgt) | {self.duplicate_keys_source:,} / {self.duplicate_keys_target:,} |",
            f"| buckets drilled into | {self.mismatched_buckets} of {self.buckets} |",
        ]
        if self.control_totals:
            lines += ["", "**Control totals**", "", "| column | source | target | |", "|---|---|---|---|"]
            for c, (s, t) in self.control_totals.items():
                lines.append(f"| {c} | {s} | {t} | {'✅' if s == t else '❌'} |")
        if self.column_diffs:
            lines += ["", "**Column differences**", "", "| column | rows | likely cause | example (source → target) |",
                      "|---|---|---|---|"]
            for d in sorted(self.column_diffs.values(), key=lambda d: -d.mismatches):
                ex = f"`{d.samples[0][1]}` → `{d.samples[0][2]}`" if d.samples else ""
                lines.append(f"| {d.column} | {d.mismatches:,} | {d.diagnosis} | {ex} |")
        for k, v in self.samples.items():
            if v:
                lines.append(f"\n_{k} sample keys_: {', '.join(v)}")
        lines.append(f"\n_normalization: {'; '.join(self.normalization)}_")
        return "\n".join(lines)


def _h64(s: str) -> int:
    return int.from_bytes(hashlib.blake2b(s.encode(), digest_size=8).digest(), "big")


class _Side:
    def __init__(self, rows: RowSource, keys: Sequence[str], columns: Sequence[str], numeric: Sequence[str],
                 norm: Normalization, n_buckets: int):
        self.rows, self.keys, self.columns, self.numeric = rows, keys, columns, numeric
        self.norm, self.n = norm, n_buckets

    def _canon(self, row: Mapping[str, object]) -> tuple[str, list[str]]:
        key = SEP.join(canonical(row.get(k), self.norm) for k in self.keys)
        return key, [canonical(row.get(c), self.norm) for c in self.columns]

    def summary(self) -> tuple[dict[int, list[int]], dict[str, Decimal], int]:
        buckets: dict[int, list[int]] = defaultdict(lambda: [0, 0])
        totals = {c: Decimal(0) for c in self.numeric}
        count = 0
        for row in self.rows():
            key, vals = self._canon(row)
            b = buckets[_h64(key) % self.n]
            b[0] += 1
            b[1] = (b[1] + _h64(SEP.join(vals))) % (1 << 64)
            for c in self.numeric:
                v = row.get(c)
                if v is not None:
                    totals[c] += Decimal(canonical(v, self.norm))
            count += 1
        return dict(buckets), totals, count

    def rows_in(self, wanted: set[int]) -> tuple[dict[str, list[str]], int]:
        out: dict[str, list[str]] = {}
        dups = 0
        for row in self.rows():
            key, vals = self._canon(row)
            if _h64(key) % self.n in wanted:
                if key in out:
                    dups += 1
                out[key] = vals
        return out, dups


def reconcile_rows(
    table: str,
    source: RowSource,
    target: RowSource,
    keys: Sequence[str],
    columns: Sequence[str],
    *,
    numeric_columns: Sequence[str] = (),
    norm: Normalization | None = None,
    n_buckets: int = 256,
    sample_size: int = 5,
) -> ReconReport:
    """Compare two row streams. ``source``/``target`` are zero-arg callables returning fresh iterators."""
    norm = norm or Normalization()
    cols = [c for c in columns if c not in norm.ignore_columns and c not in keys]
    src = _Side(source, keys, cols, numeric_columns, norm, n_buckets)
    tgt = _Side(target, keys, cols, numeric_columns, norm, n_buckets)
    sb, stot, scount = src.summary()
    tb, ttot, tcount = tgt.summary()
    bad = {b for b in set(sb) | set(tb) if sb.get(b) != tb.get(b)}

    rep = ReconReport(table, scount, tcount, n_buckets, len(bad), normalization=norm.describe())
    rep.control_totals = {c: (_fmt(stot[c]), _fmt(ttot[c])) for c in numeric_columns}
    if not bad:
        return rep

    srows, rep.duplicate_keys_source = src.rows_in(bad)
    trows, rep.duplicate_keys_target = tgt.rows_in(bad)
    missing = [k for k in srows if k not in trows]
    extra = [k for k in trows if k not in srows]
    rep.missing_in_target, rep.extra_in_target = len(missing), len(extra)
    diffs: dict[str, ColumnDiff] = {}
    causes: dict[str, Counter[str]] = defaultdict(Counter)
    changed = []
    for k in srows.keys() & trows.keys():
        s, t = srows[k], trows[k]
        if s == t:
            continue
        changed.append(k)
        for i, c in enumerate(cols):
            if s[i] != t[i]:
                d = diffs.setdefault(c, ColumnDiff(c))
                d.mismatches += 1
                causes[c][diagnose(s[i], t[i])] += 1
                if len(d.samples) < sample_size:
                    d.samples.append((_show(k), s[i], t[i]))
    for c, d in diffs.items():
        d.diagnosis = causes[c].most_common(1)[0][0]
    rep.changed = len(changed)
    rep.column_diffs = diffs
    rep.samples = {"missing": [_show(k) for k in sorted(missing)[:sample_size]],
                   "extra": [_show(k) for k in sorted(extra)[:sample_size]]}
    return rep


def _fmt(d: Decimal) -> str:
    return format(d.normalize(), "f") if d == d.to_integral() else format(d, "f").rstrip("0")


def _show(key: str) -> str:
    return key.replace(SEP, "|").replace(NULL, "NULL")


def dbapi_rows(conn, sql: str, batch: int = 10_000) -> RowSource:
    """Wrap any DB-API 2.0 connection (pyodbc, oracledb, teradatasql, databricks-sql, sqlite3...)."""

    def gen():
        cur = conn.cursor()
        cur.execute(sql)
        names = [d[0].lower() for d in cur.description]
        while chunk := cur.fetchmany(batch):
            for r in chunk:
                yield dict(zip(names, r, strict=True))

    return gen
