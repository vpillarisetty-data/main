"""Offline end-to-end demo on sqlite with a scripted model:  python -m catalog_agent.demo

Shows the self-correction loop: the "model" first tries to read PII, is rejected by the guard,
then repairs its query, which runs with an enforced LIMIT.
Swap ScriptedLLM for OpenAICompatibleLLM(model=...) to use a real model.
"""

from __future__ import annotations

import sqlite3

from catalog_agent.agent import CatalogAgent, sqlite_executor
from catalog_agent.catalog import example_catalog_path, load_catalog
from catalog_agent.llm import ScriptedLLM


def seed(conn: sqlite3.Connection) -> None:
    conn.executescript("""
    CREATE TABLE "fin.gold.card_spend_daily" (spend_date TEXT, account_id TEXT, mcc_group TEXT,
        country TEXT, spend_usd REAL, txn_count INT);
    CREATE TABLE "fin.gold.customer_segments" (account_id TEXT, segment TEXT, tenure_years INT,
        full_name TEXT, email TEXT);
    INSERT INTO "fin.gold.customer_segments" VALUES
        ('a1','hnw',12,'Ann','ann@x.com'), ('a2','affluent',3,'Bob','bob@x.com'), ('a3','hnw',7,'Cy','cy@x.com');
    INSERT INTO "fin.gold.card_spend_daily" VALUES
        ('2026-10-01','a1','travel','JP',4200,3), ('2026-10-02','a1','dining','JP',310,2),
        ('2026-10-02','a2','grocery','US',120,4), ('2026-10-03','a3','travel','FR',2900,1);
    """)


def build_agent(llm) -> CatalogAgent:
    conn = sqlite3.connect(":memory:")
    seed(conn)
    return CatalogAgent(load_catalog(example_catalog_path()), llm, sqlite_executor(conn))


def main() -> None:
    llm = ScriptedLLM([
        "```sql\nSELECT c.full_name, sum(d.spend_usd) FROM fin.gold.card_spend_daily d "
        "JOIN fin.gold.customer_segments c ON c.account_id = d.account_id GROUP BY c.full_name\n```",
        "```sql\nSELECT c.segment, d.mcc_group, sum(d.spend_usd) AS spend_usd\n"
        "FROM fin.gold.card_spend_daily d JOIN fin.gold.customer_segments c ON c.account_id = d.account_id\n"
        "GROUP BY c.segment, d.mcc_group ORDER BY spend_usd DESC\n```",
    ])
    res = build_agent(llm).ask("Which customer segment spends the most, and on what merchant category?")
    for line in res.trace:
        print("·", line)
    print("\nSQL:", res.sql)
    print(res.columns)
    for r in res.rows:
        print(r)


if __name__ == "__main__":
    main()
