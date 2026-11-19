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
| **Reversals / deletes** | Deletes are applied out of order, or "resurrect" rows | `delete_condition` becomes a sequence-guarded `DELETE`; unseen deletes never insert |
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
python -m medallion.runner conf/pipelines/card_transactions.yaml --layer silver
```

## Tests

* `test_medallion_unit.py`: spec validation, expectation compilation, SQL shape.
* `test_medallion_spark.py`: runs the generated SQL against a **real Delta table** and checks
  out-of-order updates, reversals, idempotent replay, phantom deletes, and SCD2 history.
