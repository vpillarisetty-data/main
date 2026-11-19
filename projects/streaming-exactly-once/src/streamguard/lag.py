"""Consumer-lag computation from Structured Streaming progress events.

Spark reports, per micro-batch, the ``endOffset`` it processed and (for Kafka)
``latestOffset`` on the broker. Lag per partition is the gap between them. Summing
lag and estimating time-to-drain from the processing rate tells on-call whether the job
is falling behind its SLA, which a "job is RUNNING" health check cannot.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class PartitionLag:
    topic: str
    partition: int
    end_offset: int
    latest_offset: int

    @property
    def lag(self) -> int:
        return max(0, self.latest_offset - self.end_offset)


def _as_dict(v: Any) -> dict[str, Any]:
    if v is None:
        return {}
    return json.loads(v) if isinstance(v, str) else v


def compute_lag(end_offset: Any, latest_offset: Any) -> list[PartitionLag]:
    """Offsets are Spark's JSON form: ``{"topic": {"0": 123, "1": 456}}``."""
    end, latest = _as_dict(end_offset), _as_dict(latest_offset)
    out: list[PartitionLag] = []
    for topic, parts in latest.items():
        for p, latest_off in parts.items():
            done = end.get(topic, {}).get(p, 0)
            out.append(PartitionLag(topic, int(p), int(done), int(latest_off)))
    return sorted(out, key=lambda x: (x.topic, x.partition))


@dataclass(frozen=True)
class LagReport:
    total_lag: int
    worst_partition: PartitionLag | None
    rows_per_second: float
    seconds_to_drain: float | None
    breached: bool


def evaluate(progress: dict[str, Any], sla_seconds: float) -> LagReport:
    """Evaluate a ``StreamingQueryProgress.json`` dict against a latency SLA."""
    lags: list[PartitionLag] = []
    for src in progress.get("sources", []):
        lags.extend(compute_lag(src.get("endOffset"), src.get("latestOffset")))
    total = sum(x.lag for x in lags)
    rate = float(progress.get("processedRowsPerSecond") or 0.0)
    drain = (total / rate) if rate > 0 else (None if total else 0.0)
    breached = total > 0 and (drain is None or drain > sla_seconds)
    worst = max(lags, key=lambda x: x.lag, default=None)
    return LagReport(total, worst, rate, drain, breached)


class LagListener:  # pragma: no cover - thin adapter over the JVM listener API
    """``spark.streams.addListener(LagListener(sla_seconds=300, alert=print))``"""

    def __new__(cls, sla_seconds: float, alert):
        from pyspark.sql.streaming import StreamingQueryListener

        class _Impl(StreamingQueryListener):
            def onQueryStarted(self, event):
                pass

            def onQueryIdle(self, event):
                pass

            def onQueryTerminated(self, event):
                if event.exception:
                    alert(f"stream {event.id} terminated: {event.exception}")

            def onQueryProgress(self, event):
                report = evaluate(json.loads(event.progress.json), sla_seconds)
                if report.breached:
                    alert(
                        f"[{event.progress.name}] lag={report.total_lag} "
                        f"drain={report.seconds_to_drain}s worst={report.worst_partition}"
                    )

        return _Impl()
