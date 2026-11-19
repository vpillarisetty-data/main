"""Simulates Spark retrying a foreachBatch after a crash, against real Delta tables."""

import json
import struct

from streamguard.sink import ExactlyOnceSink


def _msg(rec):
    return b"\x00" + struct.pack(">I", 7) + json.dumps(rec).encode() if rec is not None else None


def _kafka_batch(spark, values):
    rows = [("txn", 0, i, None, b"k", v) for i, v in enumerate(values)]
    return spark.createDataFrame(
        rows, "topic STRING, partition INT, offset LONG, timestamp TIMESTAMP, key BINARY, value BINARY"
    )


def test_replayed_batch_is_written_once_and_poison_goes_to_dlq(spark):
    for t in ("txn_events", "txn_dlq"):
        spark.sql(f"DROP TABLE IF EXISTS {t}")
    sink = ExactlyOnceSink(
        app_id="txn-stream",
        target_table="txn_events",
        dlq_table="txn_dlq",
        parse_sql="from_json(cast(body AS string), 'txn_id STRING, amount DOUBLE')",
        dedup_keys=("txn_id",),
        required_fields=("txn_id",),
    )
    batch = _kafka_batch(spark, [
        _msg({"txn_id": "a", "amount": 10.0}),
        _msg({"txn_id": "a", "amount": 10.0}),       # producer retry duplicate
        _msg({"txn_id": "b", "amount": 5.5}),
        b"\x01garbage",                               # wrong magic byte
        None,                                         # tombstone
        _msg({"amount": 1.0}),                        # missing key field
    ])

    sink(batch, batch_id=0)
    sink(batch, batch_id=0)  # crash-and-retry of the SAME batch -> must be a no-op
    sink(batch, batch_id=0)

    good = sorted(r.txn_id for r in spark.table("txn_events").collect())
    errors = sorted(r._error for r in spark.table("txn_dlq").collect())
    assert good == ["a", "b"]
    assert errors == ["bad_wire_format", "missing_txn_id", "tombstone"]

    sink(_kafka_batch(spark, [_msg({"txn_id": "c", "amount": 1.0})]), batch_id=1)
    assert spark.table("txn_events").count() == 3
