# migrationkit: prove a legacy → lakehouse migration is correct before cutover

## The problem

On-prem → cloud migrations (SQL Server, Oracle, Teradata, Hive → Databricks/Delta) usually go over time
at the end, during validation, not during code conversion. Teams compare `COUNT(*)`, it matches, they cut over,
and then finance finds that revenue is off by $520K. The usual causes:

* **Type semantics that don't survive**: Oracle `DATE` has a time part, SQL Server `TINYINT` is unsigned, `DATETIMEOFFSET`
  loses its offset, Oracle treats `''` as `NULL`, `CHAR(n)` is blank-padded
* **Silent data bugs** in the new pipelines: dropped rows from a bad join, local time loaded as UTC, cents vs dollars,
  duplicates because the target has no primary key
* **False alarms**: naive row-by-row compares flag *every* row because of `10.50` vs `10.5` or microsecond precision, so
  people stop reading the report

## What's here

| Module | What it does |
|---|---|
| `typemap` | SQL Server / Oracle / Teradata / Hive → Delta type mapping with a **risk level** (`none` / `precision` / `semantic`) and a note on what changes. `convert-ddl` translates whole DDL files and prints a risk report. |
| `normalize` | Explicit, opt-in rules for "business-equal": CHAR padding, `''`≡`NULL`, case-insensitive collations, float tolerance, timestamp precision, UTC. The report lists which rules were applied, which is what auditors sign off on. |
| `reconcile` | **Hash-bucket reconciliation** in two streaming passes: bucket fingerprints `(count, Σhash)` first, then drill down only into mismatched buckets → missing / extra / changed / duplicate keys, per-column mismatch counts with samples, **control totals** (SOX/finance evidence), and a **diagnosis** for each difference. |
| `spark` | The same algorithm distributed on Spark (source via JDBC or Hive, target Delta). Only N bucket fingerprints and mismatched rows reach the driver. A test checks it matches the Python backend exactly. |

The Python backend works with **any DB-API driver** (`pyodbc`, `oracledb`, `teradatasql`, `databricks-sql-connector`)
through `dbapi_rows(conn, sql)`, with memory bounded by `O(buckets + differing rows)`.

### Diagnosis: the report tells you *why*, not just *where*

| Pattern detected | Diagnosis |
|---|---|
| `'AB  '` vs `'AB'` | trailing whitespace (CHAR padding) |
| `NULL` vs `''` | Oracle empty-string semantics |
| same up to the second | sub-second precision loss |
| shifted by whole hours | timezone bug (local time loaded as UTC) |
| ×100 / ÷100 | units mismatch (cents vs dollars) |
| same magnitude, opposite sign | debit/credit convention |
| prefix of the source | truncation |
| relative error < 1e-6 | floating-point rounding |

## The demo: a cutover rehearsal

```bash
pip install ./projects/migration-validator
migrationkit demo
```
20,000 card transactions "from SQL Server" vs the migrated Delta table, with four planted bugs.

**Run 1, strict**: 19,997 "changed" rows, almost all noise. The report already explains why
(`trailing whitespace (CHAR padding)`, `sub-second precision loss`).

**Run 2, with the two signed-off normalizations**: only the real defects remain, each with its cause:

| metric | value |
|---|---|
| matched | 19,970 (99.85%) |
| missing in target | 3 → `T0001337, T0008888, T0015001` |
| duplicate keys (target) | 1 |
| buckets drilled into | **31 of 512** |

| column | rows | likely cause |
|---|---|---|
| event_ts | 25 | shifted by 4h: timezone conversion bug (local time loaded as UTC?) |
| amount | 2 | off by 100x: units mismatch (cents vs dollars?) |

Control totals: `amount 50,009,807.95 vs 50,530,025.48 ❌`. Cutover verdict: **NO-GO**, with the exact fixes listed.

## Other commands

```bash
migrationkit convert-ddl --from oracle examples/oracle_schema.sql --target-table fin.silver.customer_account
migrationkit reconcile --source-db legacy.db --target-db lake.db --table card_txn --keys txn_id \
             --numeric amount --ts-precision ms        # exit code 1 on FAIL: use it as a CI / cutover gate
```
```python
from migrationkit.spark import reconcile_spark
src = spark.read.jdbc(url, "dbo.card_txn", properties=props)
rep = reconcile_spark("card_txn", src, spark.table("fin.silver.card_txn"), keys=["txn_id"],
                      numeric_columns=["amount"], norm=Normalization(timestamp_precision="ms"))
print(rep.to_markdown())
```
