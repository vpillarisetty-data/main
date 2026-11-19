"""Classify contract changes from the consumer's point of view.

A change is BREAKING if a consumer written against the old contract could fail
or silently compute wrong results against data produced under the new one.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from contractkit.model import Contract, parse_decimal

_WIDENING = {("int", "bigint"), ("float", "double"), ("int", "double"), ("date", "timestamp")}


class Severity(str, Enum):
    BREAKING = "breaking"
    NON_BREAKING = "non-breaking"


@dataclass(frozen=True)
class Change:
    severity: Severity
    path: str
    message: str

    def __str__(self) -> str:
        icon = "✖" if self.severity is Severity.BREAKING else "•"
        return f"{icon} [{self.severity.value}] {self.path}: {self.message}"


def _type_change(old: str, new: str) -> Severity:
    if (old, new) in _WIDENING:
        return Severity.NON_BREAKING
    od, nd = parse_decimal(old), parse_decimal(new)
    if od and nd and nd[1] == od[1] and nd[0] >= od[0]:
        return Severity.NON_BREAKING  # same scale, wider precision
    return Severity.BREAKING


def diff_contracts(old: Contract, new: Contract) -> list[Change]:
    B, N = Severity.BREAKING, Severity.NON_BREAKING
    changes: list[Change] = []
    if old.dataset != new.dataset:
        changes.append(Change(B, "dataset", f"moved {old.dataset} -> {new.dataset}"))
    of, nf = old.field_map(), new.field_map()

    for name in of.keys() - nf.keys():
        changes.append(Change(B, f"schema.{name}", "field removed (or renamed)"))
    for name in nf.keys() - of.keys():
        changes.append(Change(N, f"schema.{name}", f"field added ({nf[name].type})"))

    for name in of.keys() & nf.keys():
        o, n = of[name], nf[name]
        p = f"schema.{name}"
        if o.type != n.type:
            changes.append(Change(_type_change(o.type, n.type), p, f"type {o.type} -> {n.type}"))
        if not o.nullable and n.nullable:
            changes.append(Change(B, p, "became nullable: consumers may receive NULLs"))
        if o.nullable and not n.nullable:
            changes.append(Change(N, p, "became NOT NULL"))
        if o.unique and not n.unique:
            changes.append(Change(B, p, "uniqueness guarantee dropped: joins may fan out"))
        if o.enum and set(n.enum) - set(o.enum):
            added = sorted(set(n.enum) - set(o.enum))
            changes.append(Change(B, p, f"new enum values {added}: CASE/IN logic downstream may miss them"))
        if o.enum and set(o.enum) - set(n.enum):
            changes.append(Change(N, p, f"enum values retired {sorted(set(o.enum) - set(n.enum))}"))
        if o.enum and not n.enum:
            changes.append(Change(B, p, "enum constraint removed"))
        if o.pii and not n.pii:
            changes.append(Change(B, p, "PII classification removed: masking policies will stop applying"))
        if not o.pii and n.pii:
            changes.append(Change(N, p, "now classified as PII"))

    for attr, label in (("freshness_minutes", "freshness"), ("completeness_pct", "completeness")):
        ov, nv = getattr(old.sla, attr), getattr(new.sla, attr)
        if ov is None or ov == nv:
            continue
        looser = nv is None or (nv > ov if attr == "freshness_minutes" else nv < ov)
        changes.append(Change(B if looser else N, f"sla.{label}", f"{ov} -> {nv}"))
    return sorted(changes, key=lambda c: (c.severity is not Severity.BREAKING, c.path))


def semver_violation(old: Contract, new: Contract, changes: list[Change]) -> str | None:
    """Return an error message if the version bump does not match the changes."""
    if new.version <= old.version and changes:
        return f"contract changed but version not bumped ({old.version_str} -> {new.version_str})"
    breaking = any(c.severity is Severity.BREAKING for c in changes)
    if breaking and new.version[0] <= old.version[0]:
        return (
            f"breaking changes require a MAJOR version bump "
            f"({old.version_str} -> {new.version_str}); publish as {old.version[0] + 1}.0.0 "
            f"and keep the old version served until consumers migrate"
        )
    return None
