# Data Engineering Accelerators

**Seven tested, installable building blocks for the problems that page data engineers in 2026:**
late and duplicate events, silent upstream breaks, migrations that don't reconcile, runaway lakehouse bills,
PII leaks, and GenAI-on-data that can't pass a security review.

[![ci](../../actions/workflows/ci.yml/badge.svg)](../../actions/workflows/ci.yml)
&nbsp; `132 tests` · `Spark 3.5 + Delta 3.2 integration tests` · `GenAI red-team evals` · `7 pip-installable packages` · `ruff clean`

Every project ships its own package and command-line tool. Every claim in the READMEs is backed by a test, and the
Spark tests run against **real Delta tables**.

---

## The projects

| # | Project | The problem | Who uses it | Highlights |
|---|---|---|---|---|
| 1 | [**medallion-framework**](projects/medallion-framework) | Copy-pasted pipelines that each handle late data, dupes and reversals differently | Data engineering teams | YAML Bronze→Silver→Gold · **sequence-guarded MERGE** · tombstones stop **resurrected reversals** · SCD2 in one atomic MERGE · quarantine with reasons · Asset Bundle + Airflow |
| 2 | [**streaming-exactly-once**](projects/streaming-exactly-once) | Retries **double-count** transactions; one bad message stops the stream | Real-time / fraud teams | Idempotent multi-sink writes (Delta `txnAppId/txnVersion`) · Schema Registry decoding in SQL · DLQ with reasons · lag SLA alerts |
| 3 | [**data-contracts**](projects/data-contracts) | "Harmless" upstream changes silently break consumers | Data product owners, governance | Consumer-centric breaking-change rules · semver enforcement · **PR gate with change table** · DDL + DQ codegen |
| 4 | [**delta-table-doctor**](projects/delta-table-doctor) | Lakehouse bills creep up from bad table layout | Platform / FinOps | Reads `_delta_log` with **no cluster** · clustering-depth metric · 9 rules with fix SQL and $/month impact |
| 5 | [**pii-guardian**](projects/pii-guardian) | PII hides in odd columns and **free-text notes** | Security, compliance (PCI/SOX/GDPR) | Luhn/SSA/mod-97 validated detection · **Unity Catalog tags + masks as code** |
| 6 | [**catalog-agent**](projects/catalog-agent) | Text-to-SQL can't pass security review | Analysts, PMs, data teams | Certified-only retrieval · PII never enters the prompt · deterministic SQL guard · self-correction · **red-team evals in CI** |
| 7 | [**migration-validator**](projects/migration-validator) | Migrations "match on COUNT(*)" and then finance finds the numbers are off | Migration leads, finance, auditors | Type mapping with **semantic-risk report** (SQL Server/Oracle/Teradata/Hive) · **hash-bucket reconciliation** (Python + Spark) · control totals · **root-cause diagnosis** |

## 10-minute tour

```bash
make install                 # installs all 7 packages (editable) + pytest/ruff
make test                    # unit tests; Spark tests auto-skip
make install-spark && make test   # + real Spark/Delta integration tests (Python ≤ 3.12)

migrationkit demo            # cutover rehearsal: finds 4 planted bugs among 20,000 rows and explains each
catalog-agent-demo           # LLM tries to read PII → blocked → self-corrects → safe SQL runs
catalog-agent-evals          # retrieval recall + 17-attack red-team suite
contractkit diff projects/data-contracts/contracts/customer_rewards.v1.yaml \
                 projects/data-contracts/contracts/customer_rewards.v2-proposed.yaml
piiguard projects/pii-guardian/examples/customer_sample.jsonl --table fin.silver.customers
migrationkit convert-ddl --from oracle projects/migration-validator/examples/oracle_schema.sql
```

Use one tool on its own in your own platform:
```bash
pip install "git+https://github.com/vpillarisetty-data/main.git#subdirectory=projects/migration-validator"
```

## Read next

* 📈 [**Business value**](docs/BUSINESS_VALUE.md): who uses each tool, adoption plan, and the KPIs that prove it worked
* 🤖 [**AI and data engineering**](docs/AI_AND_DATA_ENGINEERING.md): how AI changes the job, and where this repo uses AI and where it deliberately doesn't
* 🧭 [**Design decisions**](docs/DESIGN_DECISIONS.md): trade-offs, alternatives rejected, scaling, known limitations

## How they fit together

```
 legacy DB ──migration-validator──┐            ┌──────── data-contracts (PR gate, DQ rules) ────────┐
                                   ▼            ▼                                                     │
 Kafka ─► streaming-exactly-once ─► BRONZE ─► medallion-framework ─► SILVER ─► GOLD ─► catalog-agent ─► business users / AI
 files ───────────────────────────────┘     (MERGE, tombstones, SCD2, quarantine)  │   (governed text-to-SQL)
                                                                                    │
                       delta-table-doctor ◄── nightly layout & cost sweep of every table
                       pii-guardian       ◄── sample ─► tags + column masks in Unity Catalog
```

## Engineering principles

* **Prove it against the real engine.** Integration tests found five real bugs during development: Databricks-only
  `SELECT * EXCEPT`, an ambiguous column in MERGE, Parquet-by-default tables on OSS Spark, **reversed transactions
  reappearing after a replay**, and a card validator rejecting valid cards. All are fixed and covered by tests.
* **Make the dangerous thing impossible, not discouraged.** Sequence guards, tombstones, transaction ids, deterministic
  SQL guards, semver gates, cutover gates with exit codes.
* **Generate SQL, keep it in Catalyst.** DQ rules, MERGE statements and masks compile to Spark SQL, which is fast,
  reviewable in a PR, and unit-testable as text.
* **Deterministic core, AI at the edges.** Models handle language; rules handle permissions, correctness and money.
* **Minimal dependencies.** The core needs only `pyyaml`; Spark is optional and imported lazily.

## Repository layout and branches

```
projects/<name>/pyproject.toml   installable package + CLI
projects/<name>/src/<package>    library code
projects/<name>/tests            unit + Spark/Delta integration tests
docs/                            business value, AI impact, design decisions
.github/workflows/ci.yml         unit (3.10–3.13) · Spark/Delta integration · clean-install smoke test · GenAI evals
```
Each capability was built on its own `feature/*` branch and merged with `--no-ff`, so `git log --graph` shows one
reviewable unit per capability.

---

Built by **Venkat Pillarisetty**, Senior Data Engineer · Databricks Certified Data Engineer Professional ·
[LinkedIn](https://www.linkedin.com/in/vpillarisetty). Patterns drawn from production Lakehouse work in financial services,
hospitality and telecom, including three on-prem → cloud migrations. All data in this repository is synthetic.
MIT licensed.
