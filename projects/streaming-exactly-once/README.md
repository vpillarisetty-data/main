# streamguard: exactly-once Kafka → Delta streaming

## The problem

"We use Structured Streaming with checkpoints, so we're exactly-once." That's only true for
built-in sinks. As soon as you use `foreachBatch` (for a MERGE, a dead-letter queue, or several tables)
a failure **between your write and the checkpoint commit** makes Spark re-run the batch, and every
table you already wrote now has duplicates. In card transactions, that means a customer is charged twice
in the analytics layer.

The other common failures:

* **Poison messages** (bad Schema Registry header, unparseable JSON/Avro, tombstones) either crash the
  stream, so the whole pipeline stops on one bad record, or get silently dropped, so data is lost with no trace.
* **"Job is RUNNING" is not health.** A stream can run while falling hours behind.

## What's here

| Module | What it does |
|---|---|
| `sink.ExactlyOnceSink` | `foreachBatch` writer that tags **every** sink write with Delta `txnAppId`/`txnVersion` = `(app, batch_id)`. Replays become no-ops on the main table *and* the DLQ. |
| `sink.split` | Decodes the Confluent wire format in SQL (no Python UDF), parses the payload, and routes failures to a DLQ with a reason (`tombstone`, `bad_wire_format`, `unparseable`, `missing_<field>`) plus topic/partition/offset for replay. |
| `wire_format` | Pure-Python and Spark-SQL helpers for the `0x00 + schema_id` header that `from_avro` doesn't strip. |
| `lag` | Per-partition consumer lag from `StreamingQueryProgress`, time-to-drain estimate, SLA breach detection, and a `StreamingQueryListener` that alerts. |

`failOnDataLoss=true` is set on purpose: for financial data, a silently skipped offset range is worse than a stopped job.

## Proof

`tests/test_streamguard_spark.py` runs the same micro-batch through the sink **three times** (simulated
crash-and-retry) against real Delta tables and checks that each record lands exactly once, producer-retry duplicates
are removed, and each poison message is in the DLQ with the correct reason.

## Run locally

```bash
docker compose -f local/docker-compose.yml up -d
python local/run_local.py
```

> Cross-batch duplicates (the same event in two different batches) are handled downstream by the
> sequence-guarded MERGE in [`medallion-framework`](../medallion-framework), or with
> `dropDuplicatesWithinWatermark` when the stream has an event-time column.
