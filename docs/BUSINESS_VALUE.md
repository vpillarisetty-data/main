# Business value: who uses each accelerator, for what, and how to measure it

Each tool here targets a cost that shows up on a business owner's desk: an incorrect number in a finance report, a
failed audit, a cloud bill, a delayed migration, or an AI project that never ships. This page answers four questions
for each tool: **who uses it, the scenario, how to adopt it, and which KPI proves it worked.**

> Figures in "what it's worth" are **illustrative ranges** to show how to build the business case, not measured results
> from this repository. Measure your own baseline with the KPIs listed.

---

## 1. medallion-framework: one way to build every pipeline

| | |
|---|---|
| **Users** | Data engineering teams that own 20+ ingestion pipelines; platform teams setting standards |
| **Scenario** | A bank onboards a new card-processor feed. Today that's a new notebook (1–2 weeks, reviewed by hand). With the framework it's one YAML file reviewed in a PR, inheriting late-data, reversal, quarantine and SCD2 handling that's already tested. |
| **Risk removed** | Reversed transactions reappearing after a replay (tested); late events overwriting newer balances; bad records crashing jobs or silently landing in Gold |
| **Adopt** | Day 1: port one existing feed to YAML and run it side by side. Week 1: run `migrationkit` reconciliation between old and new outputs. Month 1: new feeds must use the framework (PR template). |
| **KPIs** | Lead time to onboard a feed · % pipelines on the framework · quarantine rate per source (supplier quality scorecard) · incidents caused by late or duplicate data |
| **What it's worth** | Onboarding from ~10 days to ~1–2 days per feed; one fewer finance restatement a year often pays for the whole effort |

## 2. streaming-exactly-once: trustworthy real-time numbers

| | |
|---|---|
| **Users** | Teams building fraud signals, real-time spend dashboards, offer triggering, IoT/telemetry |
| **Scenario** | A cluster node dies mid-batch. Without idempotent writes the batch replays and **every transaction in it is counted twice**, so fraud models and spend dashboards spike. With `ExactlyOnceSink` the replay is a no-op on every table, including the dead-letter queue. |
| **Risk removed** | Double counting on retry · whole-stream outage from one malformed message · silent lag ("the job is green but data is 3 hours old") |
| **Adopt** | Swap the `foreachBatch` function in an existing stream; add the lag listener with your SLA; give the DLQ table an owner and a replay runbook. |
| **KPIs** | Duplicate rate after failover (chaos test: kill a worker) · DLQ volume by reason and producer · p95 end-to-end latency vs SLA · pages caused by poison messages |

## 3. data-contracts: stop upstream changes from breaking downstream teams

| | |
|---|---|
| **Users** | Data product owners (producers), analytics and ML teams (consumers), data governance |
| **Scenario** | The rewards team adds tier `centurion`. Without a contract, three dashboards bucket it as `NULL` and an ML feature drifts, and someone notices weeks later. With `contractkit` the producer's PR is **blocked** until it's published as v2.0.0, and consumers see the change table in the PR. |
| **Risk removed** | Silent semantic breaks · PII tags dropped (masking stops) · SLAs loosened without notice |
| **Adopt** | Start with the 10 most-consumed Gold tables; add the GitHub Action; require a contract for anything marked "certified". |
| **KPIs** | Incidents caused by upstream change (target: near zero) · time to detect a breaking change (from weeks to "at PR time") · % certified tables with contracts |

## 4. delta-table-doctor: cut the lakehouse bill without touching business logic

| | |
|---|---|
| **Users** | Platform/FinOps teams, data engineering leads |
| **Scenario** | A nightly job sweeps every table in Unity Catalog, posts the 20 worst health scores with the exact `OPTIMIZE` / `CLUSTER BY` / `VACUUM` SQL and the estimated $/month, and the owning team fixes them in minutes. |
| **Risk removed** | Slow dashboards from small files · paying to store deleted data · Z-ORDER/clustering that silently stopped working |
| **Adopt** | Run on the top 50 tables by size; trend the health score; gate new tables with `--fail-under 70` in CI. |
| **KPIs** | Storage $ reclaimed · median query time on fixed tables · files per table · health-score trend |
| **What it's worth** | Layout fixes commonly yield double-digit % compute savings on affected workloads; the resume work this mirrors cut compute costs by 25–35% |

## 5. pii-guardian: pass the audit and reduce breach exposure

| | |
|---|---|
| **Users** | Governance, security, compliance (PCI-DSS, SOX, GDPR/CCPA), platform teams |
| **Scenario** | Before a PCI audit, scan every Silver/Gold table. The tool finds card numbers in a call-center `agent_notes` column (which nobody had tagged), generates the Unity Catalog masks, and produces an evidence trail of what is masked, for whom, and why. |
| **Risk removed** | PII in unexpected columns and free text · inconsistent manual masking · false-positive fatigue (Luhn/SSA/mod-97 validation) |
| **Adopt** | Run in report-only mode (`--json`) across the catalog; review the low-confidence queue; apply the generated SQL via CI; re-scan new tables automatically. |
| **KPIs** | % tables scanned · PII columns found vs masked · time to remediate a new finding · audit findings |

## 6. catalog-agent: self-service analytics that security can approve

| | |
|---|---|
| **Users** | Business analysts, product managers, executives; the data team that answers their ad-hoc questions |
| **Scenario** | A PM asks "which segment spends most on travel?". The agent uses only certified Gold tables, never sees PII columns, and every query passes a deterministic guard. If no certified table fits, it says so instead of guessing. |
| **Risk removed** | Data exfiltration by prompt injection · hallucinated tables and numbers · uncontrolled warehouse cost (LIMIT enforcement) |
| **Adopt** | Generate the catalog YAML from Unity Catalog comments and tags; start with one domain; run `catalog-agent-evals` in CI; grow the red-team suite from real user questions. |
| **KPIs** | Ad-hoc request backlog · answer acceptance rate · guard rejections (by reason) · retrieval recall on a growing question set |

## 7. migrationkit: migrate on schedule, cut over with evidence

| | |
|---|---|
| **Users** | Migration program leads, data engineers, finance controllers, auditors |
| **Scenario** | Week before cutover: reconcile all 400 tables nightly. The report gives GO/NO-GO per table, control totals for finance, and a *diagnosis* for every difference ("shifted by 4h: timezone bug"), so engineers fix causes instead of chasing rows. |
| **Risk removed** | Cutting over with wrong numbers · weeks lost to false-positive diffs · type-semantic traps (Oracle `DATE`, unsigned `TINYINT`) found too late |
| **Adopt** | Run `convert-ddl` on the source schema in week 1 to surface risks early. Schedule `reconcile_spark` per table, keep the reports as cutover evidence, and require all-PASS for go-live. |
| **KPIs** | Tables reconciled / total · open differences by cause · days from "code complete" to cutover · post-cutover defects |
| **What it's worth** | Validation is often 30–50% of a migration's timeline; the resume's three migrations are the source of these patterns |

---

## How they work together in a real program

```
Migration (migrationkit) ──► standard pipelines (medallion, streamguard) ──► protected, contracted data products
      (contractkit, piiguard) ──► efficient storage (deltadoctor) ──► safe self-service and AI (catalog-agent)
```
A company moving off a legacy warehouse can adopt them **in that order**. Each step pays off on its own, and each one makes
the next safer.
