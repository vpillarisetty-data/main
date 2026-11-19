"""Spark execution of a :class:`PipelineSpec` on Databricks / Delta Lake.

Bronze  - Auto Loader (or Kafka) stream, append-only, with lineage columns.
Silver  - expectations -> quarantine / drop / fail, then sequence-guarded MERGE.
Gold    - SCD1 or SCD2 MERGE from Silver change data feed.

All three layers run as ``availableNow`` streams, so the same code serves both
"batch every 15 min" jobs and continuous streaming by changing the trigger.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from medallion.expectations import Action
from medallion.merge_builder import MergeBuilder
from medallion.spec import PipelineSpec

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame, SparkSession

log = logging.getLogger("medallion")


class DataQualityError(RuntimeError):
    pass


class MedallionRunner:
    def __init__(self, spark: SparkSession, spec: PipelineSpec, continuous: bool = False):
        self.spark = spark
        self.spec = spec
        self.trigger = {"processingTime": "30 seconds"} if continuous else {"availableNow": True}

    # ------------------------------------------------------------------ bronze
    def read_source(self) -> DataFrame:
        src = self.spec.source
        if src.format == "kafka":
            return (
                self.spark.readStream.format("kafka")
                .option("subscribe", src.location)
                .options(**src.options)
                .load()
            )
        if src.format == "delta":
            return self.spark.readStream.options(**src.options).table(src.location)
        reader = (
            self.spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", src.format)
            .option("cloudFiles.schemaLocation", self.spec.checkpoint("bronze_schema"))
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("rescuedDataColumn", "_rescued_data")
            .options(**src.options)
        )
        if src.schema_hints:
            reader = reader.option("cloudFiles.schemaHints", src.schema_hints)
        return reader.load(src.location)

    def run_bronze(self) -> None:
        from pyspark.sql import functions as F

        df = self.read_source().withColumns(
            {
                "_ingest_ts": F.current_timestamp(),
                "_source_file": F.col("_metadata.file_path")
                if self.spec.source.format not in ("kafka", "delta")
                else F.lit(self.spec.source.location),
            }
        )
        (
            df.writeStream.option("checkpointLocation", self.spec.checkpoint("bronze"))
            .option("mergeSchema", "true")
            .trigger(**self.trigger)
            .toTable(self.spec.bronze_table)
            .awaitTermination()
        )

    # ------------------------------------------------------------------ silver
    def _silver_batch(self, batch: DataFrame, batch_id: int) -> None:
        silver = self.spec.silver
        suite = silver.expectations
        spark = batch.sparkSession
        if silver.select:
            batch = batch.selectExpr(*silver.select)
        scored = batch.selectExpr("*", suite.reasons_column_sql()).cache()
        try:
            if suite.expectations:
                metrics = scored.selectExpr("count(*) AS rows", *suite.metrics_sql()).first().asDict()
                log.info("dq metrics batch=%s %s", batch_id, metrics)
                if suite.by_action(Action.FAIL) and scored.where(suite.fail_predicate()).limit(1).count():
                    raise DataQualityError(f"{self.spec.name}: hard expectation failed in batch {batch_id}")
            if silver.quarantine_table:
                (
                    scored.where(suite.quarantine_predicate())
                    .selectExpr("*", f"{batch_id} AS _batch_id", "current_timestamp() AS _quarantined_at")
                    .write.format("delta").mode("append").option("mergeSchema", "true")
                    .saveAsTable(silver.quarantine_table)
                )
            clean = scored.where(suite.keep_predicate()).drop("_dq_violations")
            builder = MergeBuilder(silver.table, silver.keys, silver.sequence_by, clean.columns)
            if not spark.catalog.tableExists(silver.table):
                empty = clean.limit(0)
                if silver.delete_condition:  # tombstone bookkeeping columns
                    empty = empty.selectExpr("*", "CAST(NULL AS boolean) AS __deleted",
                                             "CAST(NULL AS timestamp) AS __updated_at")
                empty.write.format("delta").saveAsTable(silver.table)
                if silver.delete_condition:
                    spark.sql(builder.current_view_sql(f"{silver.table}_current"))
            view = f"_silver_src_{self.spec.name}"
            clean.createOrReplaceTempView(view)
            spark.sql(builder.scd1(view, silver.delete_condition))
        finally:
            scored.unpersist()

    def run_silver(self) -> None:
        (
            self.spark.readStream.table(self.spec.bronze_table)
            .writeStream.foreachBatch(self._silver_batch)
            .option("checkpointLocation", self.spec.checkpoint("silver"))
            .trigger(**self.trigger)
            .start()
            .awaitTermination()
        )

    # ------------------------------------------------------------------ gold
    def _gold_batch(self, batch: DataFrame, batch_id: int) -> None:
        gold = self.spec.gold
        assert gold is not None
        spark = batch.sparkSession
        if "__deleted" in batch.columns:
            batch = batch.where("NOT __deleted")
        cols = [c for c in batch.columns if not c.startswith("_")]
        batch = batch.select(*cols)
        view = f"_gold_src_{self.spec.name}"
        batch.createOrReplaceTempView(view)
        builder = MergeBuilder(gold.table, gold.keys, gold.sequence_by, cols)
        if not spark.catalog.tableExists(gold.table):
            empty = batch.limit(0)
            if gold.scd_type == 2:
                empty = empty.selectExpr(
                    "*", f"CAST(NULL AS {dict(batch.dtypes)[gold.sequence_by]}) AS __start_at",
                    f"CAST(NULL AS {dict(batch.dtypes)[gold.sequence_by]}) AS __end_at",
                    "CAST(NULL AS boolean) AS __is_current",
                )
            if gold.cluster_by:
                # liquid clustering must be declared at creation (ALTER ... CLUSTER BY only works on
                # tables that already have clustering enabled)
                empty.createOrReplaceTempView(f"_gold_empty_{self.spec.name}")
                spark.sql(
                    f"CREATE TABLE {gold.table} USING delta CLUSTER BY ({', '.join(gold.cluster_by)}) "
                    f"AS SELECT * FROM _gold_empty_{self.spec.name}"
                )
            else:
                empty.write.format("delta").saveAsTable(gold.table)
        sql = builder.scd2(view, gold.track_columns) if gold.scd_type == 2 else builder.scd1(view)
        spark.sql(sql)

    def run_gold(self) -> None:
        if not self.spec.gold:
            return
        (
            self.spark.readStream.option("skipChangeCommits", "true")
            .table(self.spec.silver.table)
            .writeStream.foreachBatch(self._gold_batch)
            .option("checkpointLocation", self.spec.checkpoint("gold"))
            .trigger(**self.trigger)
            .start()
            .awaitTermination()
        )

    def run(self) -> None:
        self.run_bronze()
        self.run_silver()
        self.run_gold()


def main(argv: list[str] | None = None) -> None:  # pragma: no cover - Databricks entrypoint
    import argparse

    from medallion.spec import load_spec

    ap = argparse.ArgumentParser(description="Run a Medallion pipeline spec")
    ap.add_argument("spec")
    ap.add_argument("--layer", choices=["bronze", "silver", "gold", "all"], default="all")
    ap.add_argument("--continuous", action="store_true")
    ap.add_argument("--validate-only", action="store_true", help="parse and validate the spec, then exit")
    args = ap.parse_args(argv)
    spec = load_spec(args.spec)
    if args.validate_only:
        print(f"spec '{spec.name}' is valid: {spec.bronze_table} -> {spec.silver.table}"
              f"{' -> ' + spec.gold.table if spec.gold else ''}")
        return
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        raise SystemExit("pyspark is required to run pipelines: pip install 'de-medallion[spark]'") from None
    logging.basicConfig(level=logging.INFO)
    runner = MedallionRunner(SparkSession.builder.getOrCreate(), spec, args.continuous)
    {"bronze": runner.run_bronze, "silver": runner.run_silver, "gold": runner.run_gold, "all": runner.run}[
        args.layer
    ]()


if __name__ == "__main__":  # pragma: no cover
    main()
