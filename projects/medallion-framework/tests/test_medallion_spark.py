"""End-to-end proof against real Delta tables: out-of-order events, reversals, resurrection, SCD2."""

import pytest

from medallion import MergeBuilder

COLS = ["id", "v", "seq", "op"]
SRC_SCHEMA = "id INT, v STRING, seq INT, op STRING"


def _apply(spark, rows, mode, target="silver_t"):
    spark.createDataFrame(rows, SRC_SCHEMA).createOrReplaceTempView("src")
    spark.sql(MergeBuilder(target, ["id"], "seq", COLS).scd1("src", "op = 'D'", delete_mode=mode))


def _visible(spark, mode, table="silver_t"):
    where = "WHERE NOT __deleted" if mode == "soft" else ""
    return {r.id: (r.v, r.seq) for r in spark.sql(f"SELECT * FROM {table} {where}").collect()}


def _create(spark, mode):
    spark.sql("DROP TABLE IF EXISTS silver_t")
    extra = ", __deleted BOOLEAN, __updated_at TIMESTAMP" if mode == "soft" else ""
    spark.sql(f"CREATE TABLE silver_t (id INT, v STRING, seq INT, op STRING{extra}) USING delta")


@pytest.mark.parametrize("mode", ["soft", "hard"])
def test_scd1_out_of_order_and_reversals(spark, mode):
    _create(spark, mode)
    # duplicates of the same key inside one batch -> latest wins, no MERGE "multiple matches" error
    _apply(spark, [(1, "a1", 1, "U"), (1, "a2", 2, "U"), (2, "b1", 1, "U"), (3, "c1", 5, "U")], mode)
    assert _visible(spark, mode) == {1: ("a2", 2), 2: ("b1", 1), 3: ("c1", 5)}

    # late older event for id=1 must NOT overwrite; reversal deletes id=2; stale delete for id=3 ignored
    _apply(spark, [(1, "stale", 1, "U"), (2, None, 9, "D"), (3, "c-old-delete", 4, "D")], mode)
    assert _visible(spark, mode) == {1: ("a2", 2), 3: ("c1", 5)}

    # an unseen key's delete never becomes a visible row
    _apply(spark, [(42, None, 1, "D")], mode)
    assert 42 not in _visible(spark, mode)


def test_soft_delete_prevents_resurrection(spark):
    _create(spark, "soft")
    _apply(spark, [(1, "a", 1, "U")], "soft")
    _apply(spark, [(1, None, 2, "D")], "soft")        # reversal
    _apply(spark, [(1, "a", 1, "U")], "soft")         # replay / late duplicate of the original insert
    assert _visible(spark, "soft") == {}

    # delete overtakes its own insert (out-of-order across partitions)
    _apply(spark, [(7, None, 3, "D")], "soft")
    _apply(spark, [(7, "late insert", 2, "U")], "soft")
    assert 7 not in _visible(spark, "soft")


def test_hard_delete_is_vulnerable_to_resurrection(spark):
    """Documents WHY soft mode is the default."""
    _create(spark, "hard")
    _apply(spark, [(1, "a", 1, "U")], "hard")
    _apply(spark, [(1, None, 2, "D")], "hard")
    _apply(spark, [(1, "a", 1, "U")], "hard")
    assert _visible(spark, "hard") == {1: ("a", 1)}  # resurrected


def test_tombstone_purge_and_view(spark):
    _create(spark, "soft")
    b = MergeBuilder("silver_t", ["id"], "seq", COLS)
    _apply(spark, [(1, "a", 1, "U"), (2, None, 1, "D")], "soft")
    spark.sql(b.current_view_sql("silver_t_current"))
    assert [r.id for r in spark.table("silver_t_current").collect()] == [1]
    spark.sql(b.purge_tombstones_sql(0).replace("INTERVAL 0 DAYS", "INTERVAL -1 DAYS"))
    assert spark.table("silver_t").count() == 1


def test_scd2_history(spark):
    spark.sql("DROP TABLE IF EXISTS dim_t")
    spark.sql(
        "CREATE TABLE dim_t (id INT, v STRING, seq INT, op STRING, "
        "__start_at INT, __end_at INT, __is_current BOOLEAN) USING delta"
    )
    b = MergeBuilder("dim_t", ["id"], "seq", COLS)

    def apply(rows):
        spark.createDataFrame(rows, SRC_SCHEMA).createOrReplaceTempView("src2")
        spark.sql(b.scd2("src2", ["v"]))

    apply([(1, "gold", 1, "U"), (2, "silver", 1, "U")])
    apply([(1, "platinum", 2, "U"), (2, "silver", 3, "U")])  # id=2 unchanged -> no new version
    apply([(1, "gold", 1, "U")])  # late replay of old version -> ignored

    rows = sorted((r["id"], r["v"], r["__start_at"], r["__end_at"], r["__is_current"])
                  for r in spark.table("dim_t").collect())
    assert rows == [
        (1, "gold", 1, 2, False),
        (1, "platinum", 2, None, True),
        (2, "silver", 1, None, True),
    ]
