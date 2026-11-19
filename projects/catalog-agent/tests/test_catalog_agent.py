import pytest

from catalog_agent import BM25Retriever, GuardrailError, SqlGuard, load_catalog
from catalog_agent.catalog import example_catalog_path
from catalog_agent.demo import build_agent
from catalog_agent.evals import ATTACKS, SAFE, run
from catalog_agent.llm import ScriptedLLM

CAT = load_catalog(example_catalog_path())


def test_eval_suite_is_perfect():
    assert run(CAT) == {"retrieval_recall@3": 1.0, "attack_block_rate": 1.0, "safe_query_pass_rate": 1.0}


@pytest.mark.parametrize("sql,label", ATTACKS)
def test_attacks_blocked(sql, label):
    with pytest.raises(GuardrailError):
        SqlGuard(CAT).check(sql)


@pytest.mark.parametrize("sql", SAFE)
def test_safe_queries_pass(sql):
    SqlGuard(CAT).check(sql)


def test_uncertified_tables_not_retrievable():
    names = {d.name for d in BM25Retriever(CAT).docs}
    assert "fin.silver.card_txn" not in names


def test_limit_enforced_and_lowered():
    g = SqlGuard(CAT, max_rows=100)
    assert g.check("SELECT country FROM fin.gold.card_spend_daily").sql.endswith("LIMIT 100")
    assert g.check("SELECT country FROM fin.gold.card_spend_daily LIMIT 5000").sql.endswith("LIMIT 100")
    assert g.check("SELECT country FROM fin.gold.card_spend_daily LIMIT 7").sql.endswith("LIMIT 7")
    # a LIMIT inside a subquery does not count as the outer limit
    out = g.check("SELECT * FROM (SELECT country FROM fin.gold.card_spend_daily LIMIT 3) t").sql
    assert out.endswith("LIMIT 100")


def test_pii_allowed_for_entitled_users():
    out = SqlGuard(CAT).check("SELECT email FROM fin.gold.customer_segments", entitlements=["pii_reader"])
    assert out.tables == ("fin.gold.customer_segments",)


def test_allowed_tables_restrict_to_retrieved_context():
    with pytest.raises(GuardrailError, match="not an approved dataset"):
        SqlGuard(CAT).check("SELECT points_balance FROM fin.gold.rewards_points_balance",
                            allowed_tables=["fin.gold.card_spend_daily"])


def test_pii_hidden_from_prompt():
    llm = ScriptedLLM(["```sql\nSELECT segment, count(*) AS n FROM fin.gold.customer_segments GROUP BY segment\n```"])
    res = build_agent(llm).ask("How many cardmembers per segment?")
    system_prompt = llm.calls[0][0]
    assert "segment" in system_prompt and "email" not in system_prompt and "full_name" not in system_prompt
    assert res.ok and sorted(res.rows) == [("affluent", 1), ("hnw", 2)]


def test_self_correction_from_guard_and_db_errors():
    llm = ScriptedLLM([
        "```sql\nSELECT email FROM fin.gold.customer_segments\n```",            # guard rejects
        "```sql\nSELECT segmnt FROM fin.gold.customer_segments\n```",           # db rejects (typo)
        "```sql\nSELECT segment FROM fin.gold.customer_segments WHERE tenure_years > 10\n```",
    ])
    res = build_agent(llm).ask("Which cardmember segments have tenure over 10 years?")
    assert res.ok and res.attempts == 3 and res.rows == [("hnw",)]
    feedback = [m["content"] for m in llm.calls[2][1] if m["role"] == "user"]
    assert "PII" in feedback[1] and "segmnt" in feedback[2]


def test_refuses_when_no_dataset_matches():
    llm = ScriptedLLM([])
    res = build_agent(llm).ask("What is the weather in Tampa?")
    assert not res.ok and "No certified dataset" in res.error and llm.calls == []


def test_gives_up_after_max_attempts():
    llm = ScriptedLLM(["```sql\nDROP TABLE fin.gold.card_spend_daily\n```"] * 3)
    res = build_agent(llm).ask("total spend by country")
    assert not res.ok and res.attempts == 3


def test_cannot_answer_passthrough():
    res = build_agent(ScriptedLLM(["CANNOT_ANSWER: no merchant names in these tables"])).ask(
        "top merchants by spend")
    assert res.error.startswith("CANNOT_ANSWER")
