"""End-to-end proof against a real Delta table: out-of-order events, reversals, SCD2."""

from medallion import MergeBuilder

COLS = ["id", "v", "seq", "op"]


def _apply(spark, rows, target="silver_t"):
    spark.createDataFrame(rows, "id INT, v STRING, seq INT, op STRING").createOrReplaceTempView("src")
    spark.sql(MergeBuilder(target, ["id"], "seq", COLS).scd1("src", "op = 'D'"))


def _state(spark, table="silver_t"):
    return {r.id: (r.v, r.seq) for r in spark.table(table).collect()}


def test_scd1_out_of_order_and_reversals(spark):
    spark.sql("DROP TABLE IF EXISTS silver_t")
    spark.sql("CREATE TABLE silver_t (id INT, v STRING, seq INT, op STRING) USING delta")

    # batch 1: duplicates of the same key inside one batch -> latest wins, no MERGE error
    _apply(spark, [(1, "a1", 1, "U"), (1, "a2", 2, "U"), (2, "b1", 1, "U"), (3, "c1", 5, "U")])
    assert _state(spark) == {1: ("a2", 2), 2: ("b1", 1), 3: ("c1", 5)}

    # batch 2: late-arriving older event for id=1 must NOT overwrite; reversal deletes id=2
    _apply(spark, [(1, "stale", 1, "U"), (2, None, 9, "D"), (3, "c-old-delete", 4, "D")])
    assert _state(spark) == {1: ("a2", 2), 3: ("c1", 5)}  # stale delete for id=3 ignored

    # batch 3: replaying an identical batch is a no-op (idempotent)
    before = _state(spark)
    _apply(spark, [(1, "stale", 1, "U"), (2, None, 9, "D")])
    assert _state(spark) == before

    # a delete for a key we never saw must not create a row
    _apply(spark, [(42, None, 1, "D")])
    assert 42 not in _state(spark)


def test_scd2_history(spark):
    spark.sql("DROP TABLE IF EXISTS dim_t")
    spark.sql(
        "CREATE TABLE dim_t (id INT, v STRING, seq INT, op STRING, "
        "__start_at INT, __end_at INT, __is_current BOOLEAN) USING delta"
    )
    b = MergeBuilder("dim_t", ["id"], "seq", COLS)

    def apply(rows):
        spark.createDataFrame(rows, "id INT, v STRING, seq INT, op STRING").createOrReplaceTempView("src2")
        spark.sql(b.scd2("src2", ["v"]))

    apply([(1, "gold", 1, "U"), (2, "silver", 1, "U")])
    apply([(1, "platinum", 2, "U"), (2, "silver", 3, "U")])  # id=2 unchanged -> no new version
    apply([(1, "gold", 1, "U")])  # late replay of old version -> ignored

    rows = sorted((r.id, r.v, r.__start_at, r.__end_at, r.__is_current) for r in spark.table("dim_t").collect())
    assert rows == [
        (1, "gold", 1, 2, False),
        (1, "platinum", 2, None, True),
        (2, "silver", 1, None, True),
    ]
