# Data Engineering Accelerators

**Six production-grade, tested, reusable building blocks for the problems that actually page data engineers in 2026:**
late and duplicate events, silent upstream breaks, runaway lakehouse bills, PII leaks, and GenAI-on-data that can't
pass a security review.

Every project is small enough to read in one sitting and complete enough to drop into a real Databricks / Delta Lake
platform. Every claim in the READMEs is backed by a test, and the Spark tests run against **real Delta tables**.

[![ci](../../actions/workflows/ci.yml/badge.svg)](../../actions/workflows/ci.yml)
&nbsp; `90 tests` · `Spark 3.5 + Delta 3.2 integration` · `GenAI red-team evals` · `ruff clean`

---

## The projects

| # | Project | Problem it solves | Highlights |
|---|---|---|---|
| 1 | [**medallion-framework**](projects/medallion-framework) | Copy-pasted notebooks that each handle late data, dupes and deletes differently, and some wrongly | YAML-driven Bronze→Silver→Gold · **sequence-guarded MERGE** (late events can't overwrite newer ones) · reversals/deletes · SCD2 in one atomic MERGE · quarantine with reasons · Asset Bundle + Airflow |
| 2 | [**streaming-exactly-once**](projects/streaming-exactly-once) | `foreachBatch` retries silently **double-write** financial events, and one poison message stops the stream | Idempotent multi-sink writes via Delta `txnAppId/txnVersion` · Schema Registry wire-format decoding in SQL · DLQ with reasons + offsets · consumer-lag SLA alerts |
| 3 | [**data-contracts**](projects/data-contracts) | "Harmless" upstream changes (new enum value, nullable column, dropped PII tag) break consumers silently | Consumer-centric breaking-change rules · semver enforcement · **CI gate posting a change table on the PR** · DDL + DQ-rule codegen |
| 4 | [**delta-table-doctor**](projects/delta-table-doctor) | Lakehouse costs creep up from small files, over-partitioning, skipping that doesn't skip, un-vacuumed storage | Reads `_delta_log` directly (**no cluster**) · clustering-depth metric · 9 rules with fix SQL and $/month impact · health score for CI |
| 5 | [**pii-guardian**](projects/pii-guardian) | PII hides in odd columns and **free-text notes**, and regex scanners cry wolf | Luhn/issuer, SSA and mod-97 validated detectors · free-text leak detection · **Unity Catalog tags + masks as code** (PCI last-4, deterministic tokens) |
| 6 | [**catalog-agent**](projects/catalog-agent) | Text-to-SQL prototypes can't pass security review | Retrieval over certified tables only · PII never enters the prompt · tokenizer-based SQL guard · self-correction loop · **red-team eval suite in CI** |

## How they fit together

```
                       ┌──────────────────────── data-contracts ────────────────────────┐
                       │ CI gate on schema/semantic/SLA changes ─► DQ expectations       │
                       ▼                                                                  │
Kafka ─► streaming-exactly-once ─► BRONZE ─► medallion-framework ─► SILVER ─► GOLD ─► catalog-agent ─► business users
 files ─────────────────────────────┘          (MERGE, SCD2, quarantine)        │      (governed text-to-SQL)
                                                                                 │
                    delta-table-doctor ◄── nightly sweep of every table (cost & layout)
                    pii-guardian       ◄── sample ─► tags + column masks in Unity Catalog
```

## Quick start

```bash
pip install -e ".[dev]"            # pyyaml, pytest, ruff
make test                          # unit tests; Spark tests auto-skip
pip install -e ".[spark]"          # pyspark + delta-spark (Python ≤3.12 for Spark 3.5)
make test                          # now also runs the Delta integration tests

# try the tools
PYTHONPATH=projects/data-contracts/src python -m contractkit.cli diff \
  projects/data-contracts/contracts/customer_rewards.v1.yaml \
  projects/data-contracts/contracts/customer_rewards.v2-proposed.yaml
PYTHONPATH=projects/catalog-agent/src python -m catalog_agent.demo
PYTHONPATH=projects/pii-guardian/src python -m piiguard.cli \
  projects/pii-guardian/examples/customer_sample.jsonl --table fin.silver.customers
```

## Engineering principles

* **Generate SQL, keep it in Catalyst.** DQ rules, MERGE statements and masks are compiled to Spark SQL rather than Python
  UDFs, so they're fast, reviewable, and unit-testable as strings.
* **Make the dangerous thing impossible, not discouraged.** Sequence guards, idempotent transaction ids, deterministic
  SQL guards, semver gates.
* **Prove it against the real engine.** The integration tests caught three real bugs during development (Databricks-only
  `SELECT * EXCEPT`, an ambiguous column in MERGE, and a PAN validator that rejected valid test cards). That's the
  reason they exist.
* **Minimal dependencies.** Core logic needs only `pyyaml`; Spark is optional and imported lazily.

## Repository layout and branches

```
projects/<name>/src/<package>   library code
projects/<name>/tests           unit + Spark integration tests
projects/<name>/README.md       problem → design → usage
.github/workflows/ci.yml        unit (3.10–3.13) · Spark/Delta integration · GenAI evals
```

Each project was developed on its own `feature/*` branch and merged with `--no-ff`, so `git log --graph` shows one
reviewable unit per capability.

---

Built by **Venkat Pillarisetty**, Senior Data Engineer (Databricks Certified Data Engineer Professional) ·
[LinkedIn](https://www.linkedin.com/in/vpillarisetty). These patterns come from production work on Lakehouse platforms in
financial services, hospitality and telecom. All data in this repository is synthetic.
