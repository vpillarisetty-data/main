"""Validate a sample of records against a contract (producer-side pre-publish check)."""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterable, Mapping
from decimal import Decimal, InvalidOperation
from typing import Any

from contractkit.model import Contract, parse_decimal

_INT_RANGE = {"int": 2**31, "bigint": 2**63}


def _type_ok(value: Any, t: str) -> bool:
    if t == "string":
        return isinstance(value, str)
    if t == "boolean":
        return isinstance(value, bool)
    if t in _INT_RANGE:
        return isinstance(value, int) and not isinstance(value, bool) and -_INT_RANGE[t] <= value < _INT_RANGE[t]
    if t in ("float", "double"):
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if t == "date":
        return isinstance(value, dt.date) or (isinstance(value, str) and _parses(dt.date.fromisoformat, value))
    if t == "timestamp":
        return isinstance(value, dt.datetime) or (
            isinstance(value, str) and _parses(dt.datetime.fromisoformat, value)
        )
    if dec := parse_decimal(t):
        try:
            d = Decimal(str(value))
        except (InvalidOperation, ValueError):
            return False
        sign, digits, exp = d.as_tuple()
        scale = max(0, -exp)
        return scale <= dec[1] and len(digits) - scale <= dec[0] - dec[1]
    return True


def _parses(fn, v: str) -> bool:
    try:
        fn(v)
        return True
    except ValueError:
        return False


def validate_records(contract: Contract, records: Iterable[Mapping[str, Any]], max_errors: int = 50) -> list[str]:
    errors: list[str] = []
    seen: dict[str, set] = {f.name: set() for f in contract.fields if f.unique}
    known = contract.field_map()
    for i, rec in enumerate(records):
        for extra in rec.keys() - known.keys():
            errors.append(f"row {i}: unexpected field '{extra}'")
        for f in contract.fields:
            v = rec.get(f.name)
            if v is None:
                if not f.nullable:
                    errors.append(f"row {i}: '{f.name}' is NULL but contract says NOT NULL")
                continue
            if not _type_ok(v, f.type):
                errors.append(f"row {i}: '{f.name}'={v!r} is not {f.type}")
            if f.enum and str(v) not in f.enum:
                errors.append(f"row {i}: '{f.name}'={v!r} not in {list(f.enum)}")
            if f.unique:
                if v in seen[f.name]:
                    errors.append(f"row {i}: duplicate value for unique '{f.name}'")
                seen[f.name].add(v)
        if len(errors) >= max_errors:
            break
    return errors[:max_errors]
