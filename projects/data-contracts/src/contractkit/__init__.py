"""Data contracts as code: schema + semantics + SLA, with breaking-change detection."""

from contractkit.diff import Change, Severity, diff_contracts, semver_violation
from contractkit.model import Contract, Field, load_contract, parse_contract
from contractkit.validate import validate_records

__all__ = [
    "Change", "Contract", "Field", "Severity", "diff_contracts", "load_contract",
    "parse_contract", "semver_violation", "validate_records",
]
