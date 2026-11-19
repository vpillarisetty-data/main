"""Run the exactly-once stream locally against docker-compose Kafka.

    docker compose -f local/docker-compose.yml up -d
    python local/run_local.py
"""

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from streamguard.lag import LagListener
from streamguard.sink import ExactlyOnceSink, start

builder = (
    SparkSession.builder.master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(
    builder, extra_packages=["org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1"]
).getOrCreate()
spark.streams.addListener(LagListener(sla_seconds=120, alert=print))

sink = ExactlyOnceSink(
    app_id="card-txn",
    target_table="card_txn_events",
    dlq_table="card_txn_dlq",
    parse_sql="from_json(cast(body AS string), 'txn_id STRING, account_id STRING, amount DECIMAL(18,2), event_ts TIMESTAMP')",
    dedup_keys=("txn_id",),
    required_fields=("txn_id", "account_id"),
)
start(spark, sink, "localhost:9092", "card-txn", "/tmp/chk/card-txn").awaitTermination()
