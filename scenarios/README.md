# Industry scenarios: every accelerator, working together, on realistic data

Three end-to-end scenarios from the industries these patterns come from. Each one runs **all seven tools on the
same data**, in the order a real platform would, against real Spark + Delta Lake.

```bash
make install && make install-spark
python -m pytest scenarios -v
```

## The matrix

| | 📡 **Telecom** (`telecom/`) | 🏨 **Hospitality** (`hospitality/`) | 💳 **Banking** (`banking/`) |
|---|---|---|---|
| **Data** | Call-detail records from network switches: 40 cells, 300 subscribers | PMS reservations: 120 hotels, 600 loyalty guests | Card authorizations: 800 accounts, 200 merchants |
| **streaming-exactly-once** | CDR Kafka feed; executor-loss retry; producer duplicates; poison and tombstones → DLQ | PMS event stream | Authorization stream |
| **medallion-framework** | **Re-rated CDRs** win; late replays of old versions ignored; cancelled CDRs hidden; non-E.164 numbers quarantined | Stay extensions win over **PMS outage replays**; cancellation is a status, **GDPR erasure** is a delete; bad stays quarantined | **The bundled production YAML**: reversals stay reversed despite late re-sends; future-dated auths dropped; `usd`→`USD` standardised; 22 chargeback adjustments |
| **Gold SCD2** | Subscriber serving-cell history | Loyalty tier history (**liquid clustered**) | Account dimension (**liquid clustered**) |
| **data-contracts** | `network_usage_daily` validated; `5G-SA` enum + nullable region **blocked** | **Contract generates pipeline rules** (`status_in_enum` quarantines bad PMS status); RevPAR KPIs validated; money-as-DOUBLE **blocked** | Rewards data product validated against the published `customer_rewards` contract |
| **pii-guardian** | MSISDN, IMSI, **IMEI (Luhn)** found; `cell_id` correctly *not* PII; masked view | Email, phone, **card on file (PCI)**, passport, loyalty number, **cards typed into special requests**; Synapse-style masked view | PAN, SSN, cards in agent notes; UC policies + masked view |
| **delta-table-doctor** | Streaming small files flagged → `OPTIMIZE` → fixed; hour × type partitioning flagged | 120 property partitions flagged → **liquid clustering** → fixed | MERGE-heavy table without deletion vectors flagged → enabled → fixed |
| **catalog-agent** | "Most dropped calls by region?" PII attempt blocked → correct answer; eval suite 100% | "Highest RevPAR hotel?" guest PII blocked → correct answer; eval suite 100% | "Which segment spends most on travel?" on real pipeline outputs |
| **migrationkit** | **Hadoop/Hive → Delta**: lost HDFS hour partition + ms-vs-s bug → found and **diagnosed (1000x)** | **SQL Server → lakehouse** via DB-API: dropped cancellations + EST-as-UTC → diagnosed; `CHAR(6)` padding tolerated | **Oracle → Delta**: FAIL (`''` vs NULL, `DATE` lost time) → diagnosed → fixed with the `convert-ddl` mapping → **PASS** |

Every expected result is computed independently in the test (for example the agent's answer is recomputed with Spark), so a
passing scenario means the tools agree with each other **and** with the ground truth.

## Bugs these scenarios found in the accelerators (all fixed)

1. `cell_id` was classified as a **phone number** (the `cell` name hint). Every telecom network table would have been masked.
2. Gold tables with `cluster_by` failed: Delta only allows `ALTER TABLE … CLUSTER BY` on tables **created** clustered,
   which broke the bundled banking config.
3. The Doctor lost commit **history after a checkpoint**, so write-pattern rules (deletion vectors, commit storms) never fired on
   real tables older than 10 commits.
4. `catalog-agent-evals my_catalog.yaml` silently ran **banking** questions against any catalog. It now requires a domain suite.
5. Hive DDL with `PARTITIONED BY (...)` confused the DDL converter. It now carries partition columns over and suggests `CLUSTER BY`.
