# How AI is changing data engineering, and how this repository responds

AI changes data engineering in two directions at once. **AI is now a producer of data-engineering work** (it writes
pipelines, converts SQL, and drafts models), and **AI is now a consumer of data** (agents, RAG, text-to-SQL, features for
models). Each direction moves where data engineers add value.

## 1. When AI writes the code, the value moves to proving correctness

Code assistants can produce a plausible PySpark job or convert a thousand Oracle procedures overnight. The
engineering bottleneck becomes **knowing whether the output is right**: whether late events are handled, whether
the migrated numbers tie out, whether this change breaks a consumer.

| Shift | What engineers own now | Here |
|---|---|---|
| Pipelines are cheap to write | **Invariants**: sequence guards, idempotency, exactly-once, all *tested against the real engine* | `medallion-framework`, `streaming-exactly-once` |
| SQL dialect conversion is automated | **Verification** of the converted system, independent of who or what converted it | `migrationkit` (reconciliation + diagnosis) |
| Schema changes ship faster | **Contracts** that machines enforce at PR time | `data-contracts` |

> AI-assisted migrations make reconciliation more important, not less: an LLM can translate a stored procedure and
> silently change a timezone or rounding rule. `migrationkit` diagnoses exactly that class of error.

## 2. When AI consumes the data, governance becomes a runtime concern

A dashboard that shows wrong numbers gets questioned by a human. An agent that reads wrong or sensitive data acts on it
automatically, at scale. Requirements that used to be "nice to have" become **preconditions for any AI use case**:

* **Certified, documented data**: an agent is only as good as table and column descriptions. Contracts and the catalog
  YAML double as the agent's knowledge.
* **PII must never enter a prompt**: `pii-guardian` tags it, and `catalog-agent` removes tagged columns from the model's
  context and blocks them in generated SQL.
* **Deterministic controls around probabilistic models**: prompting "only write SELECT" isn't a control. A tokenizer-based
  guard with an allow-list is. The model can be wrong; the system can't let that error reach data.
* **Evals are the new unit tests**: `catalog-agent-evals` runs retrieval recall and a red-team suite in CI, so a prompt or
  catalog change can't silently regress safety.

## 3. AI workloads amplify cost and layout problems

Agents and RAG pipelines run many more, less predictable queries than human analysts. Small files, broken clustering
and unbounded scans that used to cost a little now cost a lot. `delta-table-doctor` (layout health with $ impact)
and LIMIT enforcement in the agent guard address this directly.

## 4. Where AI is used in this repository, and where it deliberately isn't

| Concern | Approach | Why |
|---|---|---|
| Understanding a question, writing SQL | LLM (pluggable, any OpenAI-compatible endpoint) | Language is what models are good at |
| Choosing which tables the model may see | BM25 over certified docs | Deterministic, explainable, debuggable |
| Deciding whether SQL may run | Deterministic guard | Security decisions must be reproducible and auditable |
| Detecting PII | Validated detectors (Luhn, SSA, mod-97) + name hints | Precise, explainable, cheap enough to scan every table; an LLM classifier can be added for the low-confidence queue |
| Diagnosing migration differences | Rules over canonical values | Each explanation is checkable by a human |

A good fit for AI is ambiguity at the edges (language, triage, drafting) wrapped in a deterministic core
(contracts, guards, reconciliation, tests).

## 5. Skills that matter more for data engineers

1. **Specifying correctness**: invariants, contracts, reconciliation rules, idempotency.
2. **Governance as code**: tags, masks, lineage, access policies generated and tested, not clicked.
3. **Evaluation**: building test sets and red-team suites for systems that include models.
4. **Platform economics**: understanding storage layout and query cost.
5. **Domain semantics**: knowing that a reversal isn't a delete, or that Oracle `DATE` has a time. Models get these
   wrong in ways that look right.

These are the skills this repository is built to demonstrate.
