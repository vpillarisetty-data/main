"""Rule engine turning a :class:`DeltaSnapshot` into prioritized, actionable findings."""

from __future__ import annotations

import math
import statistics
import time
from bisect import bisect_left, bisect_right
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from typing import Any

from deltadoctor.log import DeltaSnapshot, FileEntry

MB, GB = 1024**2, 1024**3
_PENALTY = {"critical": 25, "warning": 10, "info": 2}


@dataclass
class Finding:
    id: str
    severity: str  # critical | warning | info
    title: str
    detail: str
    fix: str = ""
    est_monthly_savings_usd: float | None = None
    metrics: dict[str, Any] = field(default_factory=dict)


@dataclass
class Config:
    target_file_bytes: int = 128 * MB
    small_file_bytes: int = 32 * MB
    storage_usd_per_gb_month: float = 0.023
    scan_usd_per_tb: float = 5.0
    monthly_full_scans: int = 300
    now_ms: int | None = None


@dataclass
class Report:
    table: str
    version: int
    health_score: int
    summary: dict[str, Any]
    findings: list[Finding]

    def to_dict(self) -> dict[str, Any]:
        return {**asdict(self), "findings": [asdict(f) for f in self.findings]}


def _human(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024 or unit == "TB":
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


def _parse_interval_ms(v: str | None, default_days: float) -> int:
    """Parse Delta interval strings such as ``interval 7 days`` / ``interval 168 hours``."""
    if not v:
        return int(default_days * 86_400_000)
    parts = v.lower().replace("interval", "").split()
    try:
        n, unit = float(parts[0]), parts[1].rstrip("s")
    except (IndexError, ValueError):
        return int(default_days * 86_400_000)
    mult = {"second": 1e3, "minute": 6e4, "hour": 3.6e6, "day": 8.64e7, "week": 6.048e8}.get(unit, 8.64e7)
    return int(n * mult)


def clustering_overlap(files: list[FileEntry], column: str) -> float | None:
    """Average fraction of files a point lookup on ``column`` must read (1/n = perfect, 1.0 = useless).

    For each file, count the files whose [min, max] range overlaps it, in O(n log n) with bisect.
    """
    ranges = [(f.min_values.get(column), f.max_values.get(column)) for f in files]
    ranges = [(a, b) for a, b in ranges if a is not None and b is not None]
    if len(ranges) < 2:
        return None
    try:
        mins = sorted(a for a, _ in ranges)
        maxs = sorted(b for _, b in ranges)
        n = len(ranges)
        total = 0
        for lo, hi in ranges:
            starts_after = n - bisect_right(mins, hi)
            ends_before = bisect_left(maxs, lo)
            total += n - starts_after - ends_before
    except TypeError:  # mixed types in stats
        return None
    return total / (n * n)


def diagnose(snap: DeltaSnapshot, cfg: Config | None = None) -> Report:
    cfg = cfg or Config()
    now = cfg.now_ms or int(time.time() * 1000)
    files = list(snap.files.values())
    total = sum(f.size for f in files)
    n = len(files)
    target = int(snap.configuration.get("delta.targetFileSize", cfg.target_file_bytes))
    findings: list[Finding] = []

    # ---- 1. small files -------------------------------------------------
    small = [f for f in files if f.size < cfg.small_file_bytes]
    ideal = max(1, math.ceil(total / target))
    if n > 2 * ideal and len(small) / max(n, 1) > 0.3 and n >= 50:
        ratio = n / ideal
        sev = "critical" if ratio > 20 else "warning"
        waste = cfg.monthly_full_scans * (n - ideal) * 0.0004  # ~ cloud GET + task-launch overhead
        findings.append(Finding(
            "small_files", sev, f"Small-file problem: {n:,} files where ~{ideal:,} would do",
            f"{len(small):,} files ({len(small) / n:.0%}) are < {_human(cfg.small_file_bytes)}; "
            f"median file is {_human(statistics.median(f.size for f in files))}. Every query pays a "
            f"listing + open + footer-read cost per file, which usually dominates scan time at this ratio ({ratio:.0f}x).",
            "OPTIMIZE <table>;\nALTER TABLE <table> SET TBLPROPERTIES (\n"
            "  'delta.autoOptimize.optimizeWrite' = 'true',\n  'delta.autoOptimize.autoCompact' = 'true');",
            round(waste, 2), {"files": n, "ideal_files": ideal, "small_files": len(small)},
        ))

    # ---- 2. partitioning -------------------------------------------------
    if snap.partition_columns:
        by_part: dict[tuple, int] = defaultdict(int)
        for f in files:
            by_part[f.partition] += f.size
        sizes = sorted(by_part.values())
        med = statistics.median(sizes) if sizes else 0
        if len(sizes) > 100 and med < 1 * GB:
            findings.append(Finding(
                "over_partitioned", "warning",
                f"Over-partitioned: {len(sizes):,} partitions, median {_human(med)}",
                f"Partitioning by {snap.partition_columns} creates many tiny partitions. Databricks guidance is "
                f"to partition only when each partition holds about 1 GB or more. Liquid clustering gives the same "
                f"pruning without the small-file and skew problems, and its keys can be changed later.",
                f"-- rebuild once, then let clustering maintain layout incrementally\n"
                f"CREATE OR REPLACE TABLE <table> CLUSTER BY ({', '.join(snap.partition_columns)}) AS SELECT * FROM <table>;",
                metrics={"partitions": len(sizes), "median_partition_bytes": med},
            ))
        if len(sizes) >= 5 and med > 0 and sizes[-1] / med > 10:
            findings.append(Finding(
                "partition_skew", "warning",
                f"Partition skew: largest partition is {sizes[-1] / med:.0f}x the median",
                f"Largest partition {_human(sizes[-1])} vs median {_human(med)}. Tasks reading the hot partition "
                f"become stragglers, and MERGE/UPDATE on it rewrites far more data than necessary.",
                "Consider liquid clustering, or salting the hot key for heavy aggregations.",
                metrics={"max_partition_bytes": sizes[-1], "median_partition_bytes": med},
            ))

    # ---- 3. data skipping effectiveness --------------------------------
    stats_cov = sum(1 for f in files if f.num_records is not None) / n if n else 1.0
    if n >= 20 and stats_cov < 0.9:
        findings.append(Finding(
            "missing_stats", "warning", f"Only {stats_cov:.0%} of files have column statistics",
            "Files without min/max stats can never be skipped, so every query reads them.",
            "ANALYZE TABLE <table> COMPUTE DELTA STATISTICS;",
            metrics={"stats_coverage": stats_cov},
        ))
    candidates = snap.clustering_columns or [c for c in snap.schema_columns if c not in snap.partition_columns][:3]
    for col in candidates:
        ov = clustering_overlap(files, col)
        if ov is None or n < 20:
            continue
        if ov > 0.5:
            label = "clustering column" if col in snap.clustering_columns else "column"
            scan_cost = cfg.monthly_full_scans * (total / 1024**4) * cfg.scan_usd_per_tb * (ov - 1 / n)
            findings.append(Finding(
                f"weak_skipping:{col}", "warning" if col in snap.clustering_columns else "info",
                f"Data skipping on {label} `{col}` is ineffective ({ov:.0%} of files per lookup)",
                f"File min/max ranges for `{col}` overlap heavily, so a point filter on `{col}` still reads "
                f"~{ov:.0%} of the table. Clustered data should be close to {1 / n:.1%}.",
                f"ALTER TABLE <table> CLUSTER BY ({col});\nOPTIMIZE <table>;",
                round(scan_cost, 2), {"overlap_fraction": round(ov, 4)},
            ))

    # ---- 4. vacuum ---------------------------------------------------------
    retention = _parse_interval_ms(snap.configuration.get("delta.deletedFileRetentionDuration"), 7)
    reclaim = [t for t in snap.tombstones.values() if now - t.deletion_timestamp > retention]
    reclaim_bytes = sum(t.size for t in reclaim)
    if reclaim_bytes > max(1 * GB, 0.2 * total):
        findings.append(Finding(
            "vacuum_overdue", "warning", f"{_human(reclaim_bytes)} of removed files can be vacuumed",
            f"{len(reclaim):,} files were removed more than the retention period ago but are still in storage "
            f"({reclaim_bytes / max(total, 1):.0%} of the live table size).",
            "VACUUM <table>;  -- schedule weekly; consider predictive optimization on Unity Catalog",
            round(reclaim_bytes / GB * cfg.storage_usd_per_gb_month, 2), {"reclaimable_bytes": reclaim_bytes},
        ))

    # ---- 5. write patterns -------------------------------------------------
    day_ago = now - 86_400_000
    recent = [c for c in snap.commits if c.get("timestamp", 0) >= day_ago]
    writes = [c for c in recent if c.get("operation") in ("WRITE", "STREAMING UPDATE", "MERGE")]
    if len(writes) > 500:
        findings.append(Finding(
            "commit_storm", "warning", f"{len(writes):,} write commits in the last 24h",
            "Very frequent small commits inflate the transaction log and create small files. They usually come from "
            "a stream with a short trigger interval and no optimized writes.",
            "Increase the trigger interval (e.g. 1 minute) and enable delta.autoOptimize.optimizeWrite.",
            metrics={"commits_24h": len(writes)},
        ))
    dml = sum(1 for c in snap.commits[-200:] if c.get("operation") in ("MERGE", "DELETE", "UPDATE"))
    dv_enabled = snap.configuration.get("delta.enableDeletionVectors", "false").lower() == "true"
    if dml >= 20 and not dv_enabled:
        findings.append(Finding(
            "enable_deletion_vectors", "info", f"{dml} MERGE/UPDATE/DELETE commits without deletion vectors",
            "Without deletion vectors, every row-level change rewrites entire files. Deletion vectors mark rows "
            "as deleted instead, which typically makes MERGE several times faster on large tables.",
            "ALTER TABLE <table> SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true');",
            metrics={"dml_commits": dml},
        ))
    dv_files = sum(1 for f in files if f.has_dv)
    if n and dv_files / n > 0.3:
        findings.append(Finding(
            "dv_accumulation", "info", f"{dv_files / n:.0%} of files carry deletion vectors",
            "Readers must apply deletion vectors on every scan. Purge them periodically.",
            "REORG TABLE <table> APPLY (PURGE);",
            metrics={"dv_files": dv_files},
        ))

    order = {"critical": 0, "warning": 1, "info": 2}
    findings.sort(key=lambda f: (order[f.severity], -(f.est_monthly_savings_usd or 0)))
    score = max(0, 100 - sum(_PENALTY[f.severity] for f in findings))
    summary = {
        "files": n, "total_bytes": total, "total_human": _human(total),
        "avg_file_human": _human(total / n) if n else "0 B",
        "partition_columns": snap.partition_columns, "clustering_columns": snap.clustering_columns,
        "tombstone_bytes": sum(t.size for t in snap.tombstones.values()),
    }
    return Report(snap.path, snap.version, score, summary, findings)
