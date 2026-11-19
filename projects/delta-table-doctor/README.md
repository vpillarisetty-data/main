# Delta Table Doctor: cost and performance diagnostics from the transaction log

## The problem

Lakehouse bills grow quietly. The usual causes are the same few table-layout problems, and nobody
notices them until a query that used to take 20 seconds takes 20 minutes:

* **small files** from streaming appends and over-eager writers
* **over-partitioning** (`PARTITIONED BY (date, hour, country)` on a 50 GB table)
* **data skipping that doesn't skip**: Z-ORDER or clustering was declared but never maintained, so every file's
  min/max covers the full range
* **storage paying for files that were deleted months ago** because `VACUUM` was never scheduled
* MERGE-heavy tables still rewriting whole files because **deletion vectors** aren't on

`DESCRIBE DETAIL` gives you a file count. It doesn't tell you *what's wrong, how much it costs, or the fix*.

## What it does

It replays the `_delta_log` directly (JSON commits + checkpoint), with **no Spark and no cluster**, so it's cheap
enough to sweep every table in a catalog nightly. It rebuilds the live file set with per-file stats and runs these rules:

| Rule | Signal | Fix it suggests |
|---|---|---|
| `small_files` | files ≫ `ceil(size / targetFileSize)` and >30% under 32 MB | `OPTIMIZE` + optimizeWrite/autoCompact |
| `over_partitioned` | >100 partitions, median < 1 GB | migrate to **liquid clustering** |
| `partition_skew` | max partition > 10× median | clustering / salting |
| `weak_skipping:<col>` | **clustering depth**: the fraction of files a point lookup must read, from min/max overlap (O(n log n)) | `CLUSTER BY` + `OPTIMIZE` |
| `missing_stats` | <90% of files have stats | `ANALYZE ... COMPUTE DELTA STATISTICS` |
| `vacuum_overdue` | tombstones past `deletedFileRetentionDuration` | `VACUUM`, with $/month estimate |
| `commit_storm` | >500 writes/24h | longer trigger interval, optimized writes |
| `enable_deletion_vectors` / `dv_accumulation` | DML-heavy table without DVs, or too many DVs | enable DVs / `REORG ... PURGE` |

Each finding includes an explanation, the SQL to fix it, and a rough monthly cost impact. The table gets a
0–100 health score you can gate on in CI (`--fail-under 70`) or chart over time.

```bash
python -m deltadoctor.cli /lake/fin/silver/card_txn --json
```
```
Delta Table Doctor: /lake/fin/silver/card_txn @ v18422
Health score: 55/100   files=48,212  size=1.9 TB  avg file=41.3 MB  partitions=['event_date']  clustering=-

🔴 Small-file problem: 48,212 files where ~15,573 would do   (~$3,918.48/month)
   31,920 files (66%) are < 32.0 MB; median file is 9.2 MB. ...
   fix:
     OPTIMIZE <table>;
     ALTER TABLE <table> SET TBLPROPERTIES (...
```

## Tests

Synthetic logs cover every rule. One test writes a real Delta table with Spark (30 small appends) and checks that the
Doctor parses the real log and flags small files.
