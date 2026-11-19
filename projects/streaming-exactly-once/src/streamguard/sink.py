"""Idempotent multi-sink ``foreachBatch`` writer.

Spark guarantees exactly-once only for built-in sinks. With ``foreachBatch``
(needed for MERGE, multiple tables, or a dead-letter queue) a batch that fails
after writing table A but before committing its checkpoint is **re-executed** and
table A gets the rows twice.

Delta Lake handles this with ``txnAppId`` + ``txnVersion``: a write carrying an
(appId, version) pair the table has already committed is skipped. This sink tags
every write with ``(query_name, batch_id)``, so replays are no-ops on every
sink, including the DLQ.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame

from streamguard.wire_format import spark_body_expr, spark_schema_id_expr

log = logging.getLogger("streamguard")


@dataclass
class ExactlyOnceSink:
    app_id: str
    target_table: str
    dlq_table: str
    parse_sql: str
    """SQL expression turning ``body`` (binary) into a struct, e.g. ``from_json(cast(body as string), '...')``."""
    dedup_keys: tuple[str, ...] = ()
    required_fields: tuple[str, ...] = ()

    def split(self, batch: DataFrame) -> tuple[DataFrame, DataFrame]:
        """Split raw Kafka rows into ``(good, dead_letters)`` with reasons, all in SQL."""
        from pyspark.sql import functions as F

        decoded = batch.selectExpr(
            "topic", "partition", "offset", "timestamp AS kafka_ts", "key", "value",
            f"{spark_schema_id_expr('value')} AS schema_id",
            f"{spark_body_expr('value')} AS body",
        ).selectExpr("*", f"{self.parse_sql} AS rec")

        reason = F.when(F.col("value").isNull(), F.lit("tombstone")) \
            .when(F.col("schema_id").isNull(), F.lit("bad_wire_format")) \
            .when(F.col("rec").isNull(), F.lit("unparseable"))
        for f in self.required_fields:
            reason = reason.when(F.col(f"rec.{f}").isNull(), F.lit(f"missing_{f}"))
        decoded = decoded.withColumn("_error", reason)

        good = decoded.where("_error IS NULL").select("rec.*", "topic", "partition", "offset", "kafka_ts")
        if self.dedup_keys:
            good = good.dropDuplicates(list(self.dedup_keys))
        dead = decoded.where("_error IS NOT NULL").select(
            "topic", "partition", "offset", "kafka_ts", "key", "value", "_error",
            F.current_timestamp().alias("_dlq_ts"),
        )
        return good, dead

    def _write(self, df: DataFrame, table: str, batch_id: int) -> None:
        (
            df.write.format("delta").mode("append")
            .option("txnAppId", f"{self.app_id}:{table}")
            .option("txnVersion", batch_id)
            .option("mergeSchema", "true")
            .saveAsTable(table)
        )

    def __call__(self, batch: DataFrame, batch_id: int) -> None:
        good, dead = self.split(batch)
        good = good.persist()
        try:
            self._write(good, self.target_table, batch_id)
            self._write(dead, self.dlq_table, batch_id)
            log.info("batch %s committed to %s (+DLQ)", batch_id, self.target_table)
        finally:
            good.unpersist()


def start(spark, sink: ExactlyOnceSink, bootstrap: str, topic: str, checkpoint: str,
          trigger: dict | None = None):  # pragma: no cover - needs a broker
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "true")  # never silently skip offsets in financial data
        .option("maxOffsetsPerTrigger", 500_000)
        .load()
        .writeStream.queryName(sink.app_id)
        .foreachBatch(sink)
        .option("checkpointLocation", checkpoint)
        .trigger(**(trigger or {"processingTime": "15 seconds"}))
        .start()
    )
