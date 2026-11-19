import datetime as dt
import sqlite3
from decimal import Decimal
from pathlib import Path

import pytest

from migrationkit import Normalization, convert_ddl, map_type, reconcile_rows
from migrationkit.cli import main
from migrationkit.demo import build
from migrationkit.normalize import NULL, canonical, diagnose
from migrationkit.reconcile import dbapi_rows

EX = Path(__file__).parents[1] / "examples"


# ------------------------------------------------------------------ type mapping
@pytest.mark.parametrize("dialect,src,delta,risk", [
    ("sqlserver", "TINYINT", "SMALLINT", "semantic"),
    ("sqlserver", "money", "DECIMAL(19,4)", "none"),
    ("sqlserver", "DATETIME2(7)", "TIMESTAMP", "precision"),
    ("sqlserver", "datetimeoffset(3)", "TIMESTAMP", "semantic"),
    ("sqlserver", "float(24)", "FLOAT", "none"),
    ("sqlserver", "rowversion", "BINARY", "semantic"),
    ("oracle", "NUMBER(9)", "INT", "none"),
    ("oracle", "NUMBER(18,0)", "BIGINT", "none"),
    ("oracle", "NUMBER(20,4)", "DECIMAL(20,4)", "none"),
    ("oracle", "NUMBER", "DECIMAL(38,10)", "precision"),
    ("oracle", "NUMBER(5,-2)", "DECIMAL(7,0)", "precision"),
    ("oracle", "DATE", "TIMESTAMP", "semantic"),
    ("oracle", "VARCHAR2(10 CHAR)", "STRING", "semantic"),
    ("teradata", "BYTEINT", "TINYINT", "none"),
    ("teradata", "PERIOD(DATE)", "STRUCT<begin: TIMESTAMP, end: TIMESTAMP>", "semantic"),
    ("hive", "varchar(20)", "STRING", "none"),
    ("sqlserver", "geography", "STRING", "semantic"),  # unknown -> flagged, not crashed
])
def test_map_type(dialect, src, delta, risk):
    tm = map_type(dialect, src)
    assert (tm.delta, tm.risk) == (delta, risk)


def test_convert_ddl_oracle_example():
    sql, risks = convert_ddl("oracle", (EX / "oracle_schema.sql").read_text(), "fin.silver.customer_account")
    assert "`account_id` BIGINT NOT NULL" in sql
    assert "`opened_on` TIMESTAMP NOT NULL" in sql
    assert "PK_ACCOUNT" not in sql
    assert {c for c, _ in risks} >= {"opened_on", "risk_score", "last_txn_at"}


def test_convert_ddl_sqlserver_brackets_and_go():
    ddl = "CREATE TABLE [dbo].[T] ([Id] UNIQUEIDENTIFIER NOT NULL, [Amt] DECIMAL(18, 2) DEFAULT 0)\nGO\n" \
          "CREATE TABLE [dbo].[U] ([X] INT IDENTITY(1,1) PRIMARY KEY)"
    sql, _ = convert_ddl("sqlserver", ddl)
    assert "dbo.t (" in sql and "`id` STRING NOT NULL" in sql and "`amt` DECIMAL(18,2)" in sql
    assert "dbo.u (" in sql and "`x` INT NOT NULL" in sql


# ------------------------------------------------------------------ normalization
def test_canonical_rules():
    n = Normalization()
    assert canonical(Decimal("10.50"), n) == canonical(10.5, n) == "10.5"
    assert canonical(Decimal("100"), n) == "100" and canonical(0.0, n) == "0"
    assert canonical("AB  ", n) == "AB"
    assert canonical("", Normalization(empty_string_as_null=True)) == NULL
    aware = dt.datetime(2026, 1, 1, 5, 0, tzinfo=dt.timezone(dt.timedelta(hours=-5)))
    assert canonical(aware, n) == "2026-01-01 10:00:00.000000"
    ts = dt.datetime(2026, 1, 1, 0, 0, 0, 123456)
    assert canonical(ts, Normalization(timestamp_precision="ms")) == "2026-01-01 00:00:00.123000"
    assert canonical(b"\x01\xff", n) == "01ff"


@pytest.mark.parametrize("s,t,expect", [
    ("2026-01-01 00:00:00.123456", "2026-01-01 00:00:00.123000", "precision"),
    ("2026-01-01 00:00:00.000000", "2026-01-01 05:00:00.000000", "timezone"),
    ("12.34", "1234", "100x"),
    ("12.34", "-12.34", "sign"),
    (NULL, "", "empty string"),
    ("ABC", "abc", "case"),
    ("Jonathan", "Jonat", "truncated"),
    ("x", NULL, "NULL on one side"),
])
def test_diagnose(s, t, expect):
    assert expect in diagnose(s, t)


# ------------------------------------------------------------------ reconciliation
def test_identical_passes_and_skips_drilldown():
    rows = [{"id": i, "v": f"x{i}", "amt": Decimal(i)} for i in range(1000)]
    rep = reconcile_rows("t", lambda: rows, lambda: list(reversed(rows)), ["id"], ["v", "amt"],
                         numeric_columns=["amt"])
    assert rep.passed and rep.mismatched_buckets == 0 and rep.control_totals["amt"] == ("499500", "499500")


def test_demo_finds_exactly_the_planted_bugs():
    src, tgt = build()
    cols = ["account_id", "amount", "merchant_code", "event_ts", "status"]
    rep = reconcile_rows("demo", lambda: src, lambda: tgt, ["txn_id"], cols, numeric_columns=["amount"],
                         norm=Normalization(timestamp_precision="ms"), n_buckets=512)
    assert not rep.passed
    assert rep.missing_in_target == 3 and rep.samples["missing"] == ["T0001337", "T0008888", "T0015001"]
    assert rep.duplicate_keys_target == 1 and rep.extra_in_target == 0
    assert rep.changed == 27
    assert rep.column_diffs["event_ts"].mismatches == 25 and "timezone" in rep.column_diffs["event_ts"].diagnosis
    assert rep.column_diffs["amount"].mismatches == 2 and "100x" in rep.column_diffs["amount"].diagnosis
    assert rep.mismatched_buckets < 40  # drill-down touched < 8% of buckets
    assert "❌ FAIL" in rep.to_markdown()


def test_extra_rows_detected():
    src = [{"id": 1, "v": "a"}]
    tgt = [{"id": 1, "v": "a"}, {"id": 2, "v": "b"}]
    rep = reconcile_rows("t", lambda: src, lambda: tgt, ["id"], ["v"])
    assert rep.extra_in_target == 1 and rep.samples["extra"] == ["2"]


def _sqlite(path, rows):
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE card_txn (txn_id TEXT, amount NUMERIC, status TEXT)")
    conn.executemany("INSERT INTO card_txn VALUES (?, ?, ?)", rows)
    conn.commit()
    return conn


def test_dbapi_and_cli(tmp_path, capsys):
    rows = [(f"T{i}", i * 1.5, "OK  ") for i in range(300)]
    s = _sqlite(tmp_path / "src.db", rows)
    _sqlite(tmp_path / "tgt.db", [(t, a, st.strip()) for t, a, st in rows[:-1]])
    src_rows = list(dbapi_rows(s, "SELECT * FROM card_txn")())
    assert src_rows[0] == {"txn_id": "T0", "amount": 0, "status": "OK  "}
    code = main(["reconcile", "--source-db", str(tmp_path / "src.db"), "--target-db", str(tmp_path / "tgt.db"),
                 "--table", "card_txn", "--keys", "txn_id", "--numeric", "amount"])
    out = capsys.readouterr().out
    assert code == 1 and "| missing in target | 1 |" in out and "T299" in out


def test_cli_convert_ddl(capsys):
    assert main(["convert-ddl", "--from", "oracle", str(EX / "oracle_schema.sql")]) == 0
    assert "RISK REPORT" in capsys.readouterr().out


# ------------------------------------------------------------------ Spark backend parity
def test_spark_backend_matches_python_backend(spark):
    from pyspark.sql import functions as F

    from migrationkit.spark import reconcile_spark

    spark.conf.set("spark.sql.session.timeZone", "UTC")
    src, tgt = build()
    schema = "txn_id STRING, account_id STRING, amount DECIMAL(18,2), merchant_code STRING, event_ts TIMESTAMP, status STRING"
    order = ["txn_id", "account_id", "amount", "merchant_code", "event_ts", "status"]
    sdf = spark.createDataFrame([tuple(r[c] for c in order) for r in src], schema)
    # target stored with a different numeric type (DOUBLE) to prove type-aware canonicalization
    tdf = spark.createDataFrame([tuple(r[c] for c in order) for r in tgt], schema) \
        .withColumn("amount", F.col("amount").cast("double"))
    norm = Normalization(timestamp_precision="ms")
    rep = reconcile_spark("demo", sdf, tdf, ["txn_id"], order[1:], numeric_columns=["amount"], norm=norm,
                          n_buckets=64)
    py = reconcile_rows("demo", lambda: src, lambda: tgt, ["txn_id"], order[1:], numeric_columns=["amount"],
                        norm=norm, n_buckets=64)
    for attr in ("source_rows", "target_rows", "missing_in_target", "extra_in_target", "changed",
                 "duplicate_keys_target"):
        assert getattr(rep, attr) == getattr(py, attr), attr
    assert rep.control_totals == py.control_totals
    assert {c: d.mismatches for c, d in rep.column_diffs.items()} == {"event_ts": 25, "amount": 2}
    assert "timezone" in rep.column_diffs["event_ts"].diagnosis
    assert rep.samples["missing"] == py.samples["missing"]
