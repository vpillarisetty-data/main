"""Exercises the runner's foreachBatch logic (Silver DQ + MERGE, Gold SCD2) on real Delta tables."""

import pytest

from medallion.runner import DataQualityError, MedallionRunner
from medallion.spec import parse_spec

SCHEMA = "txn_id STRING, account_id STRING, amount DOUBLE, currency STRING, mcc STRING, seq INT, op STRING"


def _spec(**silver_over):
    silver = {
        "table": "r_silver", "keys": ["txn_id"], "sequence_by": "seq", "delete_condition": "op = 'D'",
        "quarantine_table": "r_quarantine",
        "expectations": [
            {"name": "txn_id_present", "expr": "txn_id IS NOT NULL", "action": "fail"},
            {"name": "valid_currency", "expr": "currency RLIKE '^[A-Z]{3}$'", "action": "quarantine"},
            {"name": "positive_amount", "expr": "amount > 0", "action": "drop"},
            {"name": "has_mcc", "expr": "mcc IS NOT NULL", "action": "warn"},
        ],
        **silver_over,
    }
    return parse_spec({
        "name": "r", "owner": "t", "bronze_table": "r_bronze",
        "source": {"format": "json", "location": "/x"}, "silver": silver,
        "gold": {"table": "r_gold", "scd_type": 2, "keys": ["account_id"], "sequence_by": "seq",
                 "track_columns": ["mcc"], "cluster_by": ["account_id"]},
    })


@pytest.fixture()
def clean(spark):
    spark.sql("DROP VIEW IF EXISTS r_silver_current")
    for t in ("r_silver", "r_quarantine", "r_gold"):
        spark.sql(f"DROP TABLE IF EXISTS {t}")
    return spark


def test_silver_batch_routes_quality_and_merges(clean):
    spark = clean
    runner = MedallionRunner(spark, _spec())
    batch = spark.createDataFrame([
        ("t1", "a1", 10.0, "USD", "5411", 1, "U"),
        ("t2", "a1", 20.0, "usd", "5812", 1, "U"),    # lowercase currency -> quarantine
        ("t3", "a2", -5.0, "EUR", "5411", 1, "U"),    # negative -> dropped
        ("t4", "a2", 7.0, "EUR", None, 1, "U"),       # missing mcc -> warn only, kept
    ], SCHEMA)
    runner._silver_batch(batch, 0)
    assert sorted(r.txn_id for r in spark.table("r_silver_current").collect()) == ["t1", "t4"]
    q = spark.table("r_quarantine").collect()
    assert [(r.txn_id, r._dq_violations) for r in q] == [("t2", ["valid_currency"])]

    # reversal of t1 arrives; replay of batch 0 is harmless
    runner._silver_batch(spark.createDataFrame([("t1", "a1", 10.0, "USD", "5411", 2, "D")], SCHEMA), 1)
    runner._silver_batch(batch, 0)
    assert sorted(r.txn_id for r in spark.table("r_silver_current").collect()) == ["t4"]  # no resurrection


def test_silver_batch_fails_hard_on_contract_violation(clean):
    runner = MedallionRunner(clean, _spec())
    bad = clean.createDataFrame([(None, "a1", 1.0, "USD", "1", 1, "U")], SCHEMA)
    with pytest.raises(DataQualityError):
        runner._silver_batch(bad, 0)


def test_gold_batch_builds_scd2(clean):
    spark = clean
    runner = MedallionRunner(spark, _spec())
    runner._gold_batch(spark.createDataFrame([("t1", "a1", 1.0, "USD", "5411", 1, "U")], SCHEMA), 0)
    runner._gold_batch(spark.createDataFrame([("t2", "a1", 1.0, "USD", "5812", 2, "U")], SCHEMA), 1)
    rows = sorted((r["mcc"], r["__is_current"]) for r in spark.table("r_gold").collect())
    assert rows == [("5411", False), ("5812", True)]
    # liquid clustering declared at creation (ALTER ... CLUSTER BY alone fails on unclustered tables)
    assert list(spark.sql("DESCRIBE DETAIL r_gold").first()["clusteringColumns"]) == ["account_id"]
