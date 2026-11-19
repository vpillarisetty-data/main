"""Minimal Delta transaction-log replay (protocol: add / remove / metaData / commitInfo).

Reads JSON commits, plus the latest checkpoint when pyarrow is available, and rebuilds the
set of active files with their stats. No Spark and no cluster needed, so it is cheap
enough to run against every table in a catalog nightly.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import unquote


@dataclass
class FileEntry:
    path: str
    size: int
    partition: tuple[tuple[str, str | None], ...]
    modification_time: int
    num_records: int | None = None
    min_values: dict[str, Any] = field(default_factory=dict)
    max_values: dict[str, Any] = field(default_factory=dict)
    has_dv: bool = False


@dataclass
class Tombstone:
    path: str
    size: int
    deletion_timestamp: int


@dataclass
class DeltaSnapshot:
    path: str
    version: int
    files: dict[str, FileEntry]
    tombstones: dict[str, Tombstone]
    partition_columns: list[str]
    configuration: dict[str, str]
    clustering_columns: list[str]
    commits: list[dict[str, Any]]  # commitInfo, newest last
    schema_columns: list[str]


def _parse_stats(raw: str | None) -> tuple[int | None, dict, dict]:
    if not raw:
        return None, {}, {}
    s = json.loads(raw)
    return s.get("numRecords"), s.get("minValues", {}) or {}, s.get("maxValues", {}) or {}


def _apply(snap: DeltaSnapshot, action: dict[str, Any]) -> None:
    if add := action.get("add"):
        n, mn, mx = _parse_stats(add.get("stats"))
        p = unquote(add["path"])
        snap.files[p] = FileEntry(
            path=p, size=int(add.get("size", 0)),
            partition=tuple(sorted((add.get("partitionValues") or {}).items())),
            modification_time=int(add.get("modificationTime", 0)),
            num_records=n, min_values=mn, max_values=mx,
            has_dv=bool(add.get("deletionVector")),
        )
        snap.tombstones.pop(p, None)
    elif rm := action.get("remove"):
        p = unquote(rm["path"])
        prev = snap.files.pop(p, None)
        size = int(rm.get("size") or (prev.size if prev else 0))
        snap.tombstones[p] = Tombstone(p, size, int(rm.get("deletionTimestamp", 0)))
    elif md := action.get("metaData"):
        snap.partition_columns = list(md.get("partitionColumns", []))
        snap.configuration = dict(md.get("configuration", {}) or {})
        try:
            snap.schema_columns = [f["name"] for f in json.loads(md.get("schemaString", "{}")).get("fields", [])]
        except (ValueError, TypeError):
            snap.schema_columns = []
    elif dm := action.get("domainMetadata"):
        if dm.get("domain") == "delta.clustering" and not dm.get("removed"):
            cols = json.loads(dm.get("configuration", "{}")).get("clusteringColumns", [])
            snap.clustering_columns = [".".join(c) if isinstance(c, list) else c for c in cols]
    elif ci := action.get("commitInfo"):
        snap.commits.append(ci)


def _load_checkpoint(snap: DeltaSnapshot, log_dir: str, version: int, parts: list[str]) -> bool:
    try:
        import pyarrow.parquet as pq
    except ImportError:
        return False
    for part in parts:
        for row in pq.read_table(os.path.join(log_dir, part)).to_pylist():
            for k in ("add", "remove", "metaData", "domainMetadata"):
                if row.get(k):
                    act = dict(row[k])
                    if k == "add" and isinstance(act.get("partitionValues"), list):
                        act["partitionValues"] = dict(act["partitionValues"])
                    if k == "metaData" and isinstance(act.get("configuration"), list):
                        act["configuration"] = dict(act["configuration"])
                    _apply(snap, {k: act})
    return True


def load_snapshot(table_path: str) -> DeltaSnapshot:
    log_dir = os.path.join(table_path, "_delta_log")
    if not os.path.isdir(log_dir):
        raise FileNotFoundError(f"{table_path} is not a Delta table (no _delta_log)")
    names = os.listdir(log_dir)
    commits = sorted(int(n[:20]) for n in names if n.endswith(".json") and n[:20].isdigit())
    snap = DeltaSnapshot(table_path, -1, {}, {}, [], {}, [], [], [])

    start = 0
    ckpt_versions = sorted({int(n[:20]) for n in names if ".checkpoint" in n and n.endswith(".parquet")})
    for cv in reversed(ckpt_versions):
        parts = sorted(n for n in names if n.startswith(f"{cv:020d}.checkpoint") and n.endswith(".parquet"))
        if _load_checkpoint(snap, log_dir, cv, parts):
            start, snap.version = cv + 1, cv
            break
    if start == 0 and commits and commits[0] != 0:
        raise RuntimeError("log starts after version 0 and checkpoint could not be read (pip install pyarrow)")

    for v in (c for c in commits if c >= start):
        with open(os.path.join(log_dir, f"{v:020d}.json"), encoding="utf-8") as fh:
            for line in fh:
                if line.strip():
                    _apply(snap, json.loads(line))
        snap.version = v
    return snap
