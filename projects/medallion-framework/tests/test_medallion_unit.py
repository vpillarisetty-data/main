from pathlib import Path

import pytest

from medallion import Action, Expectation, ExpectationSuite, MergeBuilder, load_spec
from medallion.spec import SpecError, parse_spec

CONF = Path(__file__).parents[1] / "conf" / "pipelines" / "card_transactions.yaml"


def test_example_spec_loads():
    spec = load_spec(CONF)
    assert spec.silver.keys == ("txn_id",)
    assert spec.gold and spec.gold.scd_type == 2
    assert spec.checkpoint("silver") == "/Volumes/fin/ops/checkpoints/card_transactions/silver"
    assert len(spec.silver.expectations.by_action(Action.QUARANTINE)) == 2


def _minimal(**over):
    raw = {
        "name": "p", "owner": "o", "bronze_table": "b",
        "source": {"format": "json", "location": "/x"},
        "silver": {"table": "s", "keys": ["id"], "sequence_by": "seq"},
    }
    raw.update(over)
    return raw


def test_quarantine_requires_table():
    raw = _minimal(silver={"table": "s", "keys": ["id"], "sequence_by": "seq",
                           "expectations": [{"name": "x", "expr": "id > 0", "action": "quarantine"}]})
    with pytest.raises(SpecError, match="quarantine_table"):
        parse_spec(raw)


def test_unknown_format_rejected():
    with pytest.raises(SpecError, match="unsupported format"):
        parse_spec(_minimal(source={"format": "xml", "location": "/x"}))


def test_scd2_requires_track_columns():
    with pytest.raises(SpecError, match="track_columns"):
        parse_spec(_minimal(gold={"table": "g", "scd_type": 2}))


def test_expectation_null_is_violation():
    e = Expectation("pos", "amount > 0", Action.DROP)
    assert e.violation_sql() == "NOT coalesce((amount > 0), false)"


def test_suite_predicates():
    suite = ExpectationSuite((
        Expectation("a", "x > 0", Action.WARN),
        Expectation("b", "y IS NOT NULL", Action.QUARANTINE),
    ))
    assert "x > 0" not in suite.keep_predicate()  # warn never filters
    assert "y IS NOT NULL" in suite.quarantine_predicate()
    assert suite.fail_predicate() == "false"
    assert "filter(array(" in suite.reasons_column_sql()


def test_scd1_soft_delete_sql():
    sql = MergeBuilder("t1", ["id"], "seq", ["id", "v", "seq", "op"]).scd1("src", "op = 'D'")
    assert "coalesce((op = 'D'), false) AS `__is_delete`" in sql
    assert "t.`__deleted` = s.`__is_delete`" in sql          # tombstone, not DELETE
    assert "THEN DELETE" not in sql
    assert "WHEN MATCHED AND s.`seq` > t.`seq` THEN UPDATE" in sql
    assert "row_number() OVER (PARTITION BY `id` ORDER BY `seq` DESC)" in sql


def test_scd1_hard_delete_sql():
    sql = MergeBuilder("t1", ["id"], "seq", ["id", "v", "seq", "op"]).scd1("src", "op = 'D'", delete_mode="hard")
    assert "WHEN MATCHED AND s.`__is_delete` AND s.`seq` > t.`seq` THEN DELETE" in sql
    assert "WHEN NOT MATCHED AND NOT s.`__is_delete` THEN INSERT" in sql
    with pytest.raises(ValueError):
        MergeBuilder("t1", ["id"], "seq", ["id", "seq"]).scd1("src", delete_mode="nope")


def test_merge_builder_validates_columns():
    with pytest.raises(ValueError):
        MergeBuilder("t", ["missing"], "seq", ["id", "seq"])
