"""Contract model: a versioned promise a data producer makes to its consumers."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

_SEMVER = re.compile(r"^(\d+)\.(\d+)\.(\d+)$")
_DECIMAL = re.compile(r"^decimal\((\d+),\s*(\d+)\)$")
PRIMITIVES = {"string", "boolean", "int", "bigint", "float", "double", "date", "timestamp", "binary"}


class ContractError(ValueError):
    pass


def normalize_type(t: str) -> str:
    t = t.strip().lower().replace(" ", "")
    aliases = {"integer": "int", "long": "bigint", "str": "string", "bool": "boolean", "real": "float"}
    t = aliases.get(t, t)
    if t in PRIMITIVES or _DECIMAL.match(t):
        return t
    raise ContractError(f"unsupported type '{t}'")


def parse_decimal(t: str) -> tuple[int, int] | None:
    m = _DECIMAL.match(t)
    return (int(m.group(1)), int(m.group(2))) if m else None


@dataclass(frozen=True)
class Field:
    name: str
    type: str
    nullable: bool = True
    description: str = ""
    pii: bool = False
    unique: bool = False
    enum: tuple[str, ...] = ()


@dataclass(frozen=True)
class SLA:
    freshness_minutes: int | None = None
    completeness_pct: float | None = None


@dataclass(frozen=True)
class Contract:
    id: str
    version: tuple[int, int, int]
    owner: str
    dataset: str
    fields: tuple[Field, ...]
    sla: SLA = field(default_factory=SLA)
    description: str = ""

    @property
    def version_str(self) -> str:
        return ".".join(map(str, self.version))

    def field_map(self) -> dict[str, Field]:
        return {f.name: f for f in self.fields}

    # ---- code generation -------------------------------------------------
    def to_ddl(self) -> str:
        cols = []
        for f in self.fields:
            col = f"  `{f.name}` {f.type.upper()}{'' if f.nullable else ' NOT NULL'}"
            if f.description:
                col += " COMMENT '" + f.description.replace("'", "\\'") + "'"
            cols.append(col)
        props = f"'contract.id' = '{self.id}', 'contract.version' = '{self.version_str}', 'owner' = '{self.owner}'"
        return (
            f"CREATE TABLE IF NOT EXISTS {self.dataset} (\n" + ",\n".join(cols) +
            f"\n) USING DELTA\nTBLPROPERTIES ({props})"
        )

    def to_expectations(self) -> list[dict[str, str]]:
        """Expectations in the format consumed by the medallion framework."""
        out = []
        for f in self.fields:
            if not f.nullable:
                out.append({"name": f"{f.name}_not_null", "expr": f"{f.name} IS NOT NULL", "action": "fail"})
            if f.enum:
                vals = ", ".join("'" + v.replace("'", "''") + "'" for v in f.enum)
                out.append({"name": f"{f.name}_in_enum", "expr": f"{f.name} IN ({vals})", "action": "quarantine"})
        return out


def parse_contract(raw: dict[str, Any]) -> Contract:
    for k in ("id", "version", "owner", "dataset", "schema"):
        if not raw.get(k):
            raise ContractError(f"contract missing '{k}'")
    m = _SEMVER.match(str(raw["version"]))
    if not m:
        raise ContractError(f"version '{raw['version']}' is not semver MAJOR.MINOR.PATCH")
    seen: set[str] = set()
    fields = []
    for f in raw["schema"]:
        if f["name"] in seen:
            raise ContractError(f"duplicate field '{f['name']}'")
        seen.add(f["name"])
        fields.append(Field(
            name=f["name"], type=normalize_type(f["type"]), nullable=bool(f.get("nullable", True)),
            description=f.get("description", ""), pii=bool(f.get("pii", False)),
            unique=bool(f.get("unique", False)), enum=tuple(str(v) for v in f.get("enum", []) or []),
        ))
    sla_raw = raw.get("sla") or {}
    return Contract(
        id=raw["id"], version=tuple(int(x) for x in m.groups()), owner=raw["owner"],
        dataset=raw["dataset"], fields=tuple(fields), description=raw.get("description", ""),
        sla=SLA(sla_raw.get("freshness_minutes"), sla_raw.get("completeness_pct")),
    )


def load_contract(path: str | Path) -> Contract:
    with open(path, encoding="utf-8") as fh:
        return parse_contract(yaml.safe_load(fh))
