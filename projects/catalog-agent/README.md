# Catalog Agent: governed text-to-SQL over certified Gold tables

## The problem

Every company wants "ask the data a question in English". Most GenAI-on-data prototypes fail the
security review for the same reasons:

* the model can generate **any** SQL, and "please only write SELECT" in a prompt is not a control
* it sees every table, including raw/uncertified ones, so answers are unreliable
* it happily selects `email` and `full_name`
* it **hallucinates tables and columns** instead of saying it doesn't know
* there's no regression testing, so a prompt tweak silently breaks things

## Design

```
question ─► BM25 retrieval over CERTIFIED table docs ──(no match)──► "no certified dataset" (no guessing)
                     │ top-k tables, PII columns removed from context
                     ▼
               LLM writes SQL ◄──────────── feedback: guard rejection / warehouse error
                     │                                       ▲  (bounded self-correction)
                     ▼                                       │
          deterministic SqlGuard ──────── reject ────────────┘
          • single SELECT/WITH  • no DDL/DML/admin keywords  • no read_files/http_request
          • tables must be fully qualified AND in the retrieved allow-list
          • PII columns / SELECT * over PII blocked unless entitled  • LIMIT enforced
                     │
                     ▼
               executor (Databricks SQL / sqlite demo) ─► rows + SQL + tables used (citations)
```

The guard is a real tokenizer, not regex-on-the-whole-string. `'please DROP me'` in a string literal passes, while
`SELECT 1 /* x */ ; DROP TABLE t` is blocked. A `LIMIT` inside a subquery doesn't count as the outer limit.

The LLM is pluggable: `OpenAICompatibleLLM` works with OpenAI, Azure OpenAI, Databricks Model Serving, vLLM and Ollama
through `/chat/completions` with no SDK. `ScriptedLLM` makes tests and demos deterministic.

## Evals in CI

`python -m catalog_agent.evals catalog/gold_catalog.yaml`
```
retrieval_recall@3       100%
attack_block_rate        100%   (17 red-team queries: stacked statements, comment tricks, DML in CTEs,
safe_query_pass_rate     100%    PII via UNION, comma-joins to unknown tables, file/network functions …)
```
The command exits non-zero on any leak or false block, so prompt, catalog, or rule changes can't regress silently.

## Demo (offline)

```bash
python -m catalog_agent.demo
```
```
· retrieved: fin.gold.card_spend_daily(4.8146), fin.gold.customer_segments(3.4382)
· attempt 1: Your query was rejected by policy: column 'full_name' is PII and you are not entitled to it. Rewrite it.
· attempt 2: ok
SQL: SELECT c.segment, d.mcc_group, sum(d.spend_usd) AS spend_usd FROM ... ORDER BY spend_usd DESC LIMIT 1000
('hnw', 'travel', 7100.0)
```
