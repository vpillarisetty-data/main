# Medallion Framework: config-driven Bronze → Silver → Gold on Delta Lake

> One YAML file per source. No new notebook per feed.

## The problem

Most lakehouse teams copy-paste a notebook for every new feed. A year later there are 80 of them,
each handling late data, duplicates, and deletes slightly differently, and some of them wrongly.
The bugs that cause the most production incidents are:

| Failure | What goes wrong with a naive `MERGE` | How this framework handles it |
|---|---|---|
| **Out-of-order events** | An older event arriving late overwrites the newer state | Every update/delete is guarded by `s.seq > t.seq` |
| **Duplicate keys in one micro-batch** | `MERGE` fails: *multiple source rows matched* | Source is reduced to latest-per-key with `row_number()` before merging |
| **Reversals / deletes** | A hard `DELETE` forgets the key, so a late or replayed older insert **resurrects** the row | `delete_condition` becomes a sequence-guarded **tombstone** (`__deleted`), consumers read `<table>_current`, tombstones are purged after the lateness window (`delete_mode="hard"` is available, and a test shows why it isn't the default) |
| **Replays after failure** | Reprocessing creates duplicates | MERGE + sequence guard is idempotent (tested) |
| **Bad records** | Either the job crashes or garbage leaks into Silver | Per-rule `warn` / `drop` / `quarantine` / `fail` actions, compiled to SQL |
| **SCD2 history** | Two statements (close + insert) leave a window of inconsistency | Single atomic MERGE using the staged-updates pattern |

## How it works

```
cloud storage / Kafka ──► BRONZE (Auto Loader, append-only, _ingest_ts, _source_file, _rescued_data)
                              │  availableNow stream
                              ▼
                          SILVER  expectations ─► quarantine table (with _dq_violations reasons)
                              │   dedup latest-per-key ─► sequence-guarded MERGE (SCD1 + deletes)
                              ▼
                          GOLD    SCD1 / SCD2 MERGE, liquid clustering
```

* `spec.py`: typed, validated YAML spec. Mistakes fail at load time, not at 3 AM.
* `expectations.py`: data-quality rules compiled to **NULL-safe** Spark SQL (a `NULL` result counts as a
  violation), so the checks run inside Catalyst with no Python UDFs.
* `merge_builder.py`: generates SCD1/SCD2 `MERGE` SQL. Pure Python, so you can unit-test and review the SQL.
* `runner.py`: Spark execution with `foreachBatch`; the same code runs as a batch (`availableNow`) or continuous stream.
* `deploy/`: Databricks Asset Bundle job (bronze → silver → gold tasks) plus an equivalent Airflow DAG.

## Example spec

See [`conf/pipelines/card_transactions.yaml`](conf/pipelines/card_transactions.yaml): a card-transaction feed with
reversals, five quality rules across all four actions, and an SCD2 Gold dimension with liquid clustering.

```bash
pip install "./projects/medallion-framework[spark]"
medallion-run conf/pipelines/card_transactions.yaml --validate-only   # CI-friendly spec check
medallion-run conf/pipelines/card_transactions.yaml --layer silver
```

## Tests

* `test_medallion_unit.py`: spec validation, expectation compilation, SQL shape.
* `test_medallion_spark.py`: runs the generated SQL against **real Delta tables**: out-of-order updates,
  reversals, delete-before-insert, resurrection (soft vs hard), tombstone purge, SCD2 history.
* `test_medallion_runner_spark.py`: drives the runner's `foreachBatch` logic: fail/quarantine/drop/warn routing,
  quarantine reasons, a replayed batch after a reversal, and Gold SCD2.

Writing these tests surfaced four real bugs, all fixed: Databricks-only `SELECT * EXCEPT` syntax, an ambiguous delete
column inside MERGE, tables created as Parquet instead of Delta on OSS Spark, and deleted rows that came back on replay.
