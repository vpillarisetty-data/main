"""Banking (card issuer, Amex-style) end-to-end on the bundled production config
``projects/medallion-framework/conf/pipelines/card_transactions.yaml``: authorizations -> Kafka -> Delta ->
rewards and spend data products -> PCI controls -> governed self-service, plus an Oracle -> Delta migration
that goes FAIL -> diagnosed -> fixed -> PASS."""

from __future__ import annotations

import datetime as dt
import json
import sqlite3
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

from catalog_agent import CatalogAgent, load_catalog
from catalog_agent.agent import sqlite_executor
from catalog_agent.catalog import example_catalog_path
from catalog_agent.llm import ScriptedLLM
from contractkit import load_contract, validate_records
from deltadoctor import diagnose, load_snapshot
from medallion import load_spec
from medallion.runner import MedallionRunner
from migrationkit import Normalization, convert_ddl, reconcile_rows
from piiguard import classify_table, generate_masked_view, generate_policies
from scenarios.banking.data import TXN_SCHEMA, accounts, merchants, txn_feed
from scenarios.common import confluent, drop, kafka_batch, localize, sql_reply, to_sqlite
from streamguard.sink import ExactlyOnceSink

ROOT = Path(__file__).parents[2]
CONF = ROOT / "projects/medallion-framework/conf/pipelines/card_transactions.yaml"
REWARDS_V1 = ROOT / "projects/data-contracts/contracts/customer_rewards.v1.yaml"
PII_SAMPLE = ROOT / "projects/pii-guardian/examples/customer_sample.jsonl"
ORACLE_DDL = ROOT / "projects/migration-validator/examples/oracle_schema.sql"


def _location(spark, table):
    return spark.sql(f"DESCRIBE DETAIL {table}").first()["location"].replace("file:", "")


@pytest.fixture(scope="module")
def bank(spark):
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spec = localize(load_spec(CONF))
    drop(spark, spec.bronze_table, spec.silver.table, spec.silver.quarantine_table, spec.gold.table,
         "fin_bronze_dlq", "fin_dim_merchant", "fin_dim_account")
    spark.createDataFrame(merchants()).write.format("delta").saveAsTable("fin_dim_merchant")
    spark.createDataFrame(accounts()).write.format("delta").saveAsTable("fin_dim_account")
    feed = txn_feed()
    sink = ExactlyOnceSink("auth-stream", spec.bronze_table, "fin_bronze_dlq",
                           f"from_json(cast(body AS string), '{TXN_SCHEMA}')",
                           dedup_keys=("txn_id", "seq"), required_fields=("txn_id",))
    runner = MedallionRunner(spark, spec)
    offset = 0
    for batch_id, events in enumerate(feed.batches):
        raw = kafka_batch(spark, "card-auth", [confluent(e) for e in events], offset)
        offset += len(events)
        sink(raw, batch_id)
        runner._silver_batch(sink.split(raw)[0], batch_id)
    runner._gold_batch(spark.table(spec.silver.table), 0)
    silver = spark.table(f"{spec.silver.table}_current")
    spend = spark.sql(f"""
        SELECT to_date(t.event_ts) AS spend_date, t.account_id, m.mcc_group, m.country,
               CAST(sum(t.amount) AS DECIMAL(18,2)) AS spend_usd, count(*) AS txn_count
        FROM {spec.silver.table}_current t JOIN fin_dim_merchant m ON m.merchant_id = t.merchant_id
        GROUP BY 1, 2, 3, 4""")
    return SimpleNamespace(spec=spec, feed=feed, runner=runner, silver=silver, spend=spend)


def test_production_config_runs_end_to_end(bank):
    assert bank.spec.name == "card_transactions"           # the bundled YAML, unmodified except table names
    cur = {r.txn_id for r in bank.silver.collect()}
    f = bank.feed
    assert cur == f.valid - f.reversed                      # reversals stay reversed despite late re-sends
    assert not f.dropped & cur                              # future-dated authorizations dropped


def test_quarantine_reasons(spark, bank):
    q = {r.txn_id: r._dq_violations for r in spark.table(bank.spec.silver.quarantine_table).collect()}
    assert set(q) == set(bank.feed.quarantined)
    assert all(rule in q[t] for t, rule in bank.feed.quarantined.items())


def test_lowercase_currency_is_standardised_not_rejected(bank):
    assert {r.currency for r in bank.silver.select("currency").distinct().collect()} == {"USD"}


def test_gold_dimension_is_clustered_and_current(spark, bank):
    gold = spark.table(bank.spec.gold.table)
    assert gold.where("__is_current").count() == bank.silver.select("account_id").distinct().count()
    assert list(spark.sql(f"DESCRIBE DETAIL {bank.spec.gold.table}").first()["clusteringColumns"]) == ["account_id"]


def test_rewards_data_product_meets_published_contract(spark, bank):
    rewards = spark.sql(f"""
        SELECT a.account_id, a.tier, CAST(floor(sum(t.amount)) AS INT) AS points,
               CAST(floor(sum(t.amount)) * 0.01 AS DECIMAL(12,2)) AS points_value, DATE'2026-11-30' AS as_of_date
        FROM {bank.spec.silver.table}_current t JOIN fin_dim_account a ON a.account_id = t.account_id
        GROUP BY a.account_id, a.tier""")
    rows = [r.asDict() for r in rewards.collect()]
    assert len(rows) == bank.silver.select("account_id").distinct().count()
    assert validate_records(load_contract(REWARDS_V1), rows) == []


def test_chargeback_adjustments_and_deletion_vector_advice(spark, bank):
    from pyspark.sql import functions as F

    schema = TXN_SCHEMA
    for i, adj in enumerate(bank.feed.adjustments):     # 22 small MERGE commits, like intraday adjustments
        df = spark.createDataFrame([tuple(adj[c] for c in [f.split()[0] for f in schema.split(", ")])], schema)
        bank.runner._silver_batch(df.withColumn("currency", F.upper("currency")), 100 + i)
    adj = bank.feed.adjustments[0]
    assert bank.silver.where(f"txn_id = '{adj['txn_id']}'").first().amount == adj["amount"]
    loc = _location(spark, bank.spec.silver.table)
    assert "enable_deletion_vectors" in {f.id for f in diagnose(load_snapshot(loc)).findings}
    spark.sql(f"ALTER TABLE {bank.spec.silver.table} SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")
    assert "enable_deletion_vectors" not in {f.id for f in diagnose(load_snapshot(loc)).findings}


def test_pci_controls_on_customer_data(spark):
    rows = [json.loads(x) for x in PII_SAMPLE.read_text().splitlines() if x]
    spark.createDataFrame(rows).write.format("delta").mode("overwrite").saveAsTable("fin_silver_customers")
    cls = classify_table(rows)
    uc = generate_policies("fin.silver.customers", cls)
    assert "mask_credit_card_restricted" in uc and "mask_ssn_restricted" in uc
    spark.sql(generate_masked_view("fin_silver_customers", "fin_customers_masked", list(rows[0]), cls))
    masked = spark.table("fin_customers_masked").collect()
    raw_cards = {r["card_number"] for r in rows}
    assert not raw_cards & {r.card_number for r in masked}
    assert all(r.tax_id.startswith("***-**-") for r in masked)
    assert all("[PAN]" in r.agent_notes for r in masked if "card" in r.agent_notes and "read" in r.agent_notes)
    assert {r.order_id for r in masked} == {r["order_id"] for r in rows}   # non-PII left untouched


def test_agent_on_real_pipeline_outputs(spark, bank):
    conn = sqlite3.connect(":memory:")
    to_sqlite(conn, "fin.gold.card_spend_daily", bank.spend)
    to_sqlite(conn, "fin.gold.customer_segments", spark.table("fin_dim_account")
              .select("account_id", "segment", "tenure_years", "full_name", "email"))
    llm = ScriptedLLM([
        sql_reply("SELECT c.full_name, sum(d.spend_usd) FROM fin.gold.card_spend_daily d JOIN "
                  "fin.gold.customer_segments c ON c.account_id = d.account_id GROUP BY c.full_name"),
        sql_reply("SELECT c.segment, round(sum(d.spend_usd), 2) AS travel_spend FROM fin.gold.card_spend_daily d "
                  "JOIN fin.gold.customer_segments c ON c.account_id = d.account_id "
                  "WHERE d.mcc_group = 'travel' GROUP BY c.segment ORDER BY travel_spend DESC"),
    ])
    res = CatalogAgent(load_catalog(example_catalog_path()), llm, sqlite_executor(conn)).ask(
        "Which customer segment spends the most on travel?")
    assert res.ok and res.attempts == 2
    exp = spark.sql(f"""SELECT a.segment, sum(t.amount) AS s FROM {bank.spec.silver.table}_current t
        JOIN fin_dim_merchant m ON m.merchant_id = t.merchant_id JOIN fin_dim_account a ON a.account_id = t.account_id
        WHERE m.mcc_group = 'travel' GROUP BY a.segment ORDER BY s DESC""").first()
    assert res.rows[0][0] == exp["segment"] and abs(res.rows[0][1] - float(exp["s"])) < 0.01


def test_oracle_migration_fail_diagnose_fix_pass(spark):
    ddl, risks = convert_ddl("oracle", ORACLE_DDL.read_text(), "fin.silver.customer_account")
    assert "`opened_on` TIMESTAMP NOT NULL" in ddl                      # the advice: DATE -> TIMESTAMP
    base = dt.datetime(2015, 1, 1, 9, 30)
    src = [{"account_id": i, "customer_name": None if i % 7 == 0 else f"Cardmember {i}",
            "credit_limit": Decimal(5000 + i * 10), "opened_on": base + dt.timedelta(days=i, hours=i % 9),
            "status_code": "A"} for i in range(1, 501)]
    cols = ["customer_name", "credit_limit", "opened_on", "status_code"]

    def load(rows, opened_type):
        schema = f"account_id BIGINT, customer_name STRING, credit_limit DECIMAL(12,2), opened_on {opened_type}, " \
                 "status_code STRING"
        spark.createDataFrame([tuple(r[c] for c in ["account_id", *cols]) for r in rows], schema) \
            .write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
            .saveAsTable("fin_silver_customer_account")
        t = spark.table("fin_silver_customer_account")
        return lambda: (r.asDict() for r in t.toLocalIterator())

    # attempt 1: naive mapping (DATE -> DATE) and NULL names written as '' by the ETL tool
    naive = [{**r, "customer_name": r["customer_name"] or "", "opened_on": r["opened_on"].date()} for r in src]
    rep = reconcile_rows("CARDS.CUSTOMER_ACCOUNT", lambda: src, load(naive, "DATE"), ["account_id"], cols,
                         numeric_columns=["credit_limit"])
    assert not rep.passed and set(rep.column_diffs) == {"customer_name", "opened_on"}
    assert "empty string vs NULL" in rep.column_diffs["customer_name"].diagnosis
    assert "time part dropped" in rep.column_diffs["opened_on"].diagnosis

    # attempt 2: accept Oracle '' == NULL semantics -> only the real type bug remains
    rep = reconcile_rows("CARDS.CUSTOMER_ACCOUNT", lambda: src, load(naive, "DATE"), ["account_id"], cols,
                         numeric_columns=["credit_limit"], norm=Normalization(empty_string_as_null=True))
    assert set(rep.column_diffs) == {"opened_on"}

    # attempt 3: apply convert_ddl's TIMESTAMP mapping -> PASS, with matching control totals
    fixed = [{**r, "customer_name": r["customer_name"] or ""} for r in src]
    rep = reconcile_rows("CARDS.CUSTOMER_ACCOUNT", lambda: src, load(fixed, "TIMESTAMP"), ["account_id"], cols,
                         numeric_columns=["credit_limit"], norm=Normalization(empty_string_as_null=True))
    assert rep.passed, rep.to_markdown()
    assert rep.control_totals["credit_limit"][0] == rep.control_totals["credit_limit"][1]
