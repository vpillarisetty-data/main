"""Typed pipeline specification loaded from YAML.

One YAML file describes a whole Bronze -> Silver -> Gold flow, so adding a new
source is a config change rather than a new notebook.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from medallion.expectations import Expectation, ExpectationSuite

_VALID_FORMATS = {"json", "csv", "parquet", "avro", "kafka", "delta"}


class SpecError(ValueError):
    """Raised when a pipeline spec is invalid."""


@dataclass(frozen=True)
class SourceSpec:
    format: str
    location: str
    options: dict[str, str] = field(default_factory=dict)
    schema_hints: str | None = None


@dataclass(frozen=True)
class SilverSpec:
    table: str
    keys: tuple[str, ...]
    sequence_by: str
    delete_condition: str | None = None
    expectations: ExpectationSuite = field(default_factory=ExpectationSuite)
    quarantine_table: str | None = None
    select: tuple[str, ...] = ()


@dataclass(frozen=True)
class GoldSpec:
    table: str
    scd_type: int
    keys: tuple[str, ...]
    sequence_by: str
    track_columns: tuple[str, ...] = ()
    cluster_by: tuple[str, ...] = ()


@dataclass(frozen=True)
class PipelineSpec:
    name: str
    owner: str
    source: SourceSpec
    bronze_table: str
    silver: SilverSpec
    gold: GoldSpec | None = None
    checkpoint_root: str = "/Volumes/main/ops/checkpoints"
    tags: dict[str, str] = field(default_factory=dict)

    def checkpoint(self, layer: str) -> str:
        return f"{self.checkpoint_root.rstrip('/')}/{self.name}/{layer}"


def _require(d: dict[str, Any], key: str, ctx: str) -> Any:
    if key not in d or d[key] in (None, "", []):
        raise SpecError(f"{ctx}: missing required field '{key}'")
    return d[key]


def _tuple(v: Any) -> tuple[str, ...]:
    if v is None:
        return ()
    if isinstance(v, str):
        return (v,)
    return tuple(v)


def parse_spec(raw: dict[str, Any]) -> PipelineSpec:
    name = _require(raw, "name", "pipeline")
    src = _require(raw, "source", name)
    fmt = _require(src, "format", f"{name}.source").lower()
    if fmt not in _VALID_FORMATS:
        raise SpecError(f"{name}.source: unsupported format '{fmt}', expected one of {sorted(_VALID_FORMATS)}")
    source = SourceSpec(
        format=fmt,
        location=_require(src, "location", f"{name}.source"),
        options={k: str(v) for k, v in (src.get("options") or {}).items()},
        schema_hints=src.get("schema_hints"),
    )

    s = _require(raw, "silver", name)
    expectations = ExpectationSuite(
        tuple(Expectation.from_dict(e) for e in (s.get("expectations") or []))
    )
    if expectations.needs_quarantine and not s.get("quarantine_table"):
        raise SpecError(f"{name}.silver: expectations use action=quarantine but no quarantine_table is set")
    silver = SilverSpec(
        table=_require(s, "table", f"{name}.silver"),
        keys=_tuple(_require(s, "keys", f"{name}.silver")),
        sequence_by=_require(s, "sequence_by", f"{name}.silver"),
        delete_condition=s.get("delete_condition"),
        expectations=expectations,
        quarantine_table=s.get("quarantine_table"),
        select=_tuple(s.get("select")),
    )

    gold = None
    if g := raw.get("gold"):
        scd = int(g.get("scd_type", 1))
        if scd not in (1, 2):
            raise SpecError(f"{name}.gold: scd_type must be 1 or 2")
        gold = GoldSpec(
            table=_require(g, "table", f"{name}.gold"),
            scd_type=scd,
            keys=_tuple(g.get("keys") or silver.keys),
            sequence_by=g.get("sequence_by", silver.sequence_by),
            track_columns=_tuple(g.get("track_columns")),
            cluster_by=_tuple(g.get("cluster_by")),
        )
        if scd == 2 and not gold.track_columns:
            raise SpecError(f"{name}.gold: scd_type 2 requires track_columns")

    return PipelineSpec(
        name=name,
        owner=_require(raw, "owner", name),
        source=source,
        bronze_table=_require(raw, "bronze_table", name),
        silver=silver,
        gold=gold,
        checkpoint_root=raw.get("checkpoint_root", PipelineSpec.checkpoint_root),
        tags={k: str(v) for k, v in (raw.get("tags") or {}).items()},
    )


def load_spec(path: str | Path) -> PipelineSpec:
    with open(path, encoding="utf-8") as fh:
        return parse_spec(yaml.safe_load(fh))
