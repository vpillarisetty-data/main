"""Canonical value representation, so "equal" means *business-equal* across two engines.

A naive source-vs-target compare flags millions of false differences after every migration:
``10.50`` vs ``10.5``, ``'AB  '`` vs ``'AB'`` (CHAR padding), ``''`` vs ``NULL`` (Oracle),
``...123456789`` vs ``...123456`` (nanos vs micros), timezone-aware vs naive timestamps.
Each rule here is explicit and opt-in, so the report says exactly what was tolerated.
The same rules are implemented for the Python and the Spark backends.
"""

from __future__ import annotations

import datetime as dt
import math
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation

NULL = "\\N"
SEP = "\x1f"
_PRECISION = {"s": 0, "ms": 3, "us": 6}


@dataclass(frozen=True)
class Normalization:
    rtrim_strings: bool = True            # CHAR(n) blank padding
    empty_string_as_null: bool = False    # Oracle semantics
    case_insensitive: bool = False        # SQL Server / Teradata default collations
    float_decimals: int = 6               # floating point noise tolerance
    timestamp_precision: str = "us"       # "s" | "ms" | "us"
    ignore_columns: frozenset[str] = field(default_factory=frozenset)

    def __post_init__(self) -> None:
        if self.timestamp_precision not in _PRECISION:
            raise ValueError("timestamp_precision must be one of s, ms, us")

    def describe(self) -> list[str]:
        out = []
        if self.rtrim_strings:
            out.append("trailing spaces ignored")
        if self.empty_string_as_null:
            out.append("'' treated as NULL")
        if self.case_insensitive:
            out.append("string case ignored")
        out.append(f"floats compared to {self.float_decimals} decimals")
        out.append(f"timestamps compared at {self.timestamp_precision} precision, in UTC")
        if self.ignore_columns:
            out.append(f"ignored columns: {sorted(self.ignore_columns)}")
        return out


def _num(d: Decimal) -> str:
    if d.is_nan():
        return "NaN"
    s = format(d, "f")
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return "0" if s in ("-0", "") else s


def canonical(v: object, n: Normalization) -> str:
    if v is None:
        return NULL
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, int):
        return str(v)
    if isinstance(v, float):
        if math.isnan(v):
            return "NaN"
        if math.isinf(v):
            return "Infinity" if v > 0 else "-Infinity"
        return _num(Decimal(repr(round(v, n.float_decimals))).quantize(Decimal(1).scaleb(-n.float_decimals)))
    if isinstance(v, Decimal):
        return _num(v)
    if isinstance(v, dt.datetime):
        if v.tzinfo is not None:
            v = v.astimezone(dt.timezone.utc).replace(tzinfo=None)
        digits = _PRECISION[n.timestamp_precision]
        micro = v.microsecond - v.microsecond % (10 ** (6 - digits))
        return f"{v:%Y-%m-%d %H:%M:%S}.{micro:06d}"
    if isinstance(v, dt.date):
        return v.isoformat()
    if isinstance(v, (bytes, bytearray, memoryview)):
        return bytes(v).hex()
    s = str(v)
    if n.rtrim_strings:
        s = s.rstrip(" ")
    if n.empty_string_as_null and s == "":
        return NULL
    return s.lower() if n.case_insensitive else s


def parse_canonical_ts(s: str) -> dt.datetime | None:
    try:
        return dt.datetime.strptime(s, "%Y-%m-%d %H:%M:%S.%f")
    except ValueError:
        return None


def diagnose(source: str, target: str) -> str:
    """Explain a column difference in migration terms."""
    if source == NULL and target == "" or source == "" and target == NULL:
        return "empty string vs NULL (Oracle-style semantics): set empty_string_as_null"
    if source.rstrip(" ") == target.rstrip(" "):
        return "trailing whitespace (CHAR padding): set rtrim_strings"
    if source.lower() == target.lower():
        return "case difference (case-insensitive source collation): set case_insensitive"
    a, b = parse_canonical_ts(source), parse_canonical_ts(target)
    if a and b:
        delta = abs((a - b).total_seconds())
        if delta < 1 and a.replace(microsecond=0) == b.replace(microsecond=0):
            return "sub-second precision loss: set timestamp_precision='ms' or 's' if acceptable"
        if delta >= 3600 and delta % 900 == 0:
            return f"shifted by {delta / 3600:g}h: timezone conversion bug (local time loaded as UTC?)"
    try:
        x, y = Decimal(source), Decimal(target)
        if x != 0 and abs(x - y) / abs(x) < Decimal("1e-6"):
            return "floating-point rounding: lower float_decimals or migrate as DECIMAL"
        if x == y.scaleb(2) or y == x.scaleb(2) or x == y.scaleb(-2) or y == x.scaleb(-2):
            return "off by 100x: units mismatch (cents vs dollars?)"
        if x.copy_abs() == y.copy_abs():
            return "sign flipped: debit/credit convention"
    except (InvalidOperation, ValueError):
        pass
    if source == NULL or target == NULL:
        return "value lost (NULL on one side): check join/lookup in the migrated pipeline"
    if len(target) < len(source) and source.startswith(target):
        return "truncated: target column too short or substring in transform"
    return "value differs"
