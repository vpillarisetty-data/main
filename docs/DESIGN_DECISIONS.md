# Design decisions, trade-offs, scaling, and known limitations

Senior engineering is mostly about trade-offs. For each component: what was chosen, what was rejected, how it scales,
and what it doesn't do yet.

## medallion-framework
| Decision | Alternative | Why |
|---|---|---|
| Generate SQL `MERGE` as strings | DataFrame `DeltaTable.merge` API | Reviewable in PRs, unit-testable without Spark, runs inside Catalyst, portable to Databricks SQL |
| Soft-delete tombstones by default | Hard `DELETE` | A hard delete forgets the key, so late or replayed inserts resurrect it (a test proves this). Cost: tombstone rows + a purge job + a `_current` view |
| Dedup latest-per-key with `row_number()` before MERGE | Fail on duplicates | Duplicates within a micro-batch are normal (producer retries); MERGE would fail with *multiple source rows matched* |
| DQ as SQL predicates with 4 actions | Python UDF checks / external DQ tool | No serialization cost; same rules can feed DLT expectations or contracts |
| SCD2 via the staged-updates single MERGE | Two statements (close, insert) | Atomic; no window where a key has zero or two current versions |

**Scaling:** MERGE cost is driven by files touched. Use liquid clustering on keys and deletion vectors (`delta-table-doctor` flags both).
**Limitations:** schema evolution in Silver relies on Delta `mergeSchema`; no built-in tombstone purge scheduling (SQL provided).

## streaming-exactly-once
| Decision | Alternative | Why |
|---|---|---|
| Delta `txnAppId` + `txnVersion` per sink | Idempotent MERGE everywhere | Works for append sinks and the DLQ at ~zero cost; MERGE is reserved for state |
| Decode the wire format in SQL | Python deserializer UDF | Stays on the JVM; 10–100× cheaper per row |
| `failOnDataLoss=true` | `false` | Financial data: a stopped job is better than silently skipped offsets |
**Limitations:** dedup is within a batch; cross-batch dedup belongs in Silver (sequence-guarded MERGE) or `dropDuplicatesWithinWatermark`. Avro/Protobuf body decoding is left to `from_avro`/`from_protobuf` with the registry schema.

## data-contracts
| Decision | Alternative | Why |
|---|---|---|
| Rules from the **consumer's** perspective | Schema-registry compatibility modes | Registries check types only; most real breaks are semantic (enum values, nullability, PII tags, SLAs) |
| New enum value = breaking | Non-breaking (common default) | Downstream `CASE`/`IN` logic silently mis-buckets new values |
**Limitations:** nested types are not modeled yet; no consumer registry (who depends on what). Lineage from Unity Catalog would add that.

## delta-table-doctor
| Decision | Alternative | Why |
|---|---|---|
| Replay `_delta_log` directly | `DESCRIBE DETAIL` / Spark | Runs without a cluster, so it's cheap enough for every table nightly; sees per-file stats and tombstones |
| Clustering depth via interval overlap (O(n log n)) | Sampling queries | Exact, from metadata only |
**Scaling:** linear in log size; checkpoints read via pyarrow. Cloud storage needs an fsspec path (local paths today).
**Limitations:** cost figures are estimates from configurable unit prices; no v2 checkpoint/sidecar support yet.

## pii-guardian
| Decision | Alternative | Why |
|---|---|---|
| Regex + structural validation | Pure regex / ML NER | Validation removes the false positives that kill adoption; deterministic, auditable |
| Free-text leak detection only on textual columns | Apply everywhere | Random 16-digit IDs pass Luhn ~10% of the time; the test suite covers this |
| Deterministic SHA-256 tokens as default mask | Redact to `***` | Analysts can still join and count distinct |
**Limitations:** names/addresses rely on column-name hints; an NER model could feed the human-review queue. Not a substitute for a DLP program.

## catalog-agent
| Decision | Alternative | Why |
|---|---|---|
| BM25 retrieval | Vector DB | Few thousand tables: competitive, zero infra, explainable scores; the interface allows swapping in embeddings |
| Tokenizer-based guard | LLM self-check / regex | Deterministic; handles literals, comments, CTEs, subqueries, comma joins |
| Feed errors back for bounded self-correction | Single shot | Fixes most typos and policy violations without a human; bounded attempts cap cost |
**Limitations:** the guard is not a full SQL parser (it fails closed on syntax it doesn't understand); warehouse-side permissions (Unity Catalog grants, row filters) remain the final control: defense in depth.

## migrationkit
| Decision | Alternative | Why |
|---|---|---|
| Hash-bucket two-pass diff | Full row-by-row join | Only mismatched buckets are drilled into: 31 of 512 in the demo |
| Canonicalize in one engine | Push hashing into each source DB | Cross-engine hash and format differences cause false diffs; pushdown is a future optimization per dialect |
| Explicit normalization rules listed in the report | Fuzzy matching | Auditors sign off on stated tolerances, not heuristics |
**Scaling:** Python backend is streaming (memory ~ buckets + differing rows; reads each side twice). Spark backend distributes both passes; only bucket fingerprints and mismatched rows reach the driver.
**Limitations:** composite-type normalization is JSON-based; no automatic column mapping for renamed columns (pass explicit lists).

## Cross-cutting
* **Testing against the real engine and real domains** found 10 bugs that unit tests alone would have missed:
  1. Databricks-only `SELECT * EXCEPT` in generated MERGE (fails on OSS Spark)
  2. ambiguous delete column inside MERGE
  3. tables created as Parquet instead of Delta on OSS Spark
  4. reversed rows resurrected by late replays (fixed with tombstones)
  5. a PAN validator rejecting valid test cards
  6. liquid clustering via `ALTER TABLE` failing on tables not created clustered (bundled banking config)
  7. Doctor losing commit history after a checkpoint (write-pattern rules never fired on real tables)
  8. telecom `cell_id` classified as a phone number
  9. eval CLI running banking questions against any catalog
  10. Hive `PARTITIONED BY` breaking DDL conversion
* **Dependencies:** the core needs only `pyyaml`; Spark is optional and imported lazily, so the tools run in CI, on laptops
  and in notebooks.
