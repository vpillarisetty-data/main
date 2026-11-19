"""Telecom (Telstra-style) end-to-end: CDRs from switches -> Kafka -> Delta -> KPIs -> governed self-service,
plus the Hadoop -> Delta migration check. Every accelerator in the repository is exercised on the same data."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest

from catalog_agent import CatalogAgent, load_catalog
from catalog_agent.agent import sqlite_executor
from catalog_agent.evals import load_suite
from catalog_agent.evals import run as run_evals
from catalog_agent.llm import ScriptedLLM
from contractkit import Severity, diff_contracts, load_contract, semver_violation, validate_records
from deltadoctor import diagnose, load_snapshot
from medallion import load_spec
from medallion.runner import MedallionRunner
from migrationkit import Normalization, convert_ddl
from piiguard import classify_table, generate_masked_view, generate_policies
from scenarios.common import confluent, drop, kafka_batch, localize, sql_reply, to_sqlite
from scenarios.telecom.data import CDR_SCHEMA, cdr_feed, cells, subscribers
from streamguard.sink import ExactlyOnceSink

HERE = Path(__file__).parent
KPI_SQL = """
SELECT to_date(c.start_ts) AS usage_date, c.cell_id, d.region, d.technology,
       count_if(c.call_type = 'VOICE') AS voice_calls,
       CAST(round(sum(CASE WHEN c.call_type = 'VOICE' THEN c.duration_s ELSE 0 END) / 60.0, 2) AS DOUBLE) AS voice_minutes,
       CAST(round(sum(c.data_mb), 3) AS DOUBLE) AS data_mb,
       count_if(c.release_cause = 'DROPPED') AS dropped_calls,
       CAST(round(count_if(c.release_cause = 'DROPPED') / greatest(count_if(c.call_type = 'VOICE'), 1), 4) AS DOUBLE)
         AS drop_rate
FROM {silver} c JOIN telco_dim_cell d ON d.cell_id = c.cell_id
GROUP BY 1, 2, 3, 4
"""


@pytest.fixture(scope="module")
def telco(spark):
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spec = localize(load_spec(HERE / "cdr_pipeline.yaml"))
    drop(spark, spec.bronze_table, spec.silver.table, spec.silver.quarantine_table, spec.gold.table,
         "telco_bronze_cdr_dlq", "telco_dim_cell", "telco_gold_network_usage_daily")
    spark.createDataFrame(cells()).write.format("delta").saveAsTable("telco_dim_cell")

    feed = cdr_feed()
    sink = ExactlyOnceSink(
        app_id="cdr-stream", target_table=spec.bronze_table, dlq_table="telco_bronze_cdr_dlq",
        parse_sql=f"from_json(cast(body AS string), '{CDR_SCHEMA}')",
        dedup_keys=("cdr_id", "event_seq"), required_fields=("cdr_id",),
    )
    runner = MedallionRunner(spark, spec)
    offset = 0
    for batch_id, payloads in enumerate(feed.batches[:3]):
        raw = kafka_batch(spark, "cdr-events", [p if isinstance(p, bytes) or p is None else confluent(p)
                                                for p in payloads], offset)
        offset += len(payloads)
        sink(raw, batch_id)
        if batch_id == 0:
            sink(raw, batch_id)                      # executor lost mid-batch -> Spark retries batch 0
        good, _ = sink.split(raw)
        runner._silver_batch(good, batch_id)         # Silver consumes each Bronze micro-batch

    runner._gold_batch(spark.table(spec.silver.table), 0)
    raw = kafka_batch(spark, "cdr-events", [confluent(p) for p in feed.batches[3]], offset)
    sink(raw, 3)
    runner._silver_batch(sink.split(raw)[0], 3)
    runner._gold_batch(spark.table(spec.silver.table).where("cdr_id LIKE 'MOVE%'"), 1)

    kpi = spark.sql(KPI_SQL.format(silver=f"{spec.silver.table}_current"))
    kpi.write.format("delta").saveAsTable("telco_gold_network_usage_daily")
    return SimpleNamespace(spec=spec, feed=feed, kpi=spark.table("telco_gold_network_usage_daily"))


# ---------------------------------------------------------------- 1. streaming ingestion
def test_stream_lands_each_cdr_exactly_once_and_poison_goes_to_dlq(spark, telco):
    bronze = spark.table(telco.spec.bronze_table)
    # exactly-once: the retried batch 0 did not write any Kafka offset twice
    assert bronze.groupBy("partition", "offset").count().where("count > 1").count() == 0
    # producer retries (same record, new offset) were collapsed inside the batch
    b0 = bronze.where(f"offset < {len(telco.feed.batches[0])}")
    assert b0.count() == b0.select("cdr_id", "event_seq").distinct().count()
    # late replays are real messages: Bronze keeps them (raw history); Silver decides they are stale
    b = telco.feed.batches
    b2_start = len(b[0]) + len(b[1])
    assert bronze.where(f"offset >= {b2_start} AND offset < {b2_start + len(b[2])}").count() == len(b[2])
    dlq = {r["_error"]: r["count"] for r in spark.table("telco_bronze_cdr_dlq").groupBy("_error").count().collect()}
    assert dlq == telco.feed.poison


# ---------------------------------------------------------------- 2. medallion silver
def test_silver_applies_rerating_ignores_late_replays_and_cancels(spark, telco):
    cur = {r.cdr_id: r for r in spark.table(f"{telco.spec.silver.table}_current").collect()}
    feed = telco.feed
    for cid, corrected in feed.rerated.items():
        assert cur[cid].duration_s == corrected, cid           # newer correction kept, late original ignored
    assert not feed.cancelled & cur.keys()                     # cancelled CDRs hidden ...
    assert not feed.negative & cur.keys()                      # corrupt counters dropped
    assert not feed.bad_msisdn & cur.keys()                    # non-E.164 numbers quarantined
    expected = (feed.valid_originals - feed.cancelled) | {k for k in cur if k.startswith("MOVE")}
    assert set(cur) == expected
    q = spark.table(telco.spec.silver.quarantine_table).collect()
    assert {r.cdr_id for r in q} == feed.bad_msisdn
    assert all("msisdn_e164" in r._dq_violations for r in q)


# ---------------------------------------------------------------- 3. medallion gold (SCD2)
def test_gold_tracks_serving_cell_history(spark, telco):
    gold = spark.table(telco.spec.gold.table)
    current = {r["msisdn"]: r["cell_id"] for r in gold.where("__is_current").collect()}
    assert gold.where("__is_current").count() == gold.select("msisdn").distinct().count()  # one current row each
    for msisdn, cell in telco.feed.moved.items():
        assert current[msisdn] == cell
        versions = gold.where(f"msisdn = '{msisdn}'").count()
        assert versions >= 1
    assert gold.where("NOT __is_current").count() >= 1


# ---------------------------------------------------------------- 4. data contracts
def test_kpi_table_honours_its_contract_and_breaking_change_is_caught(telco):
    v1 = load_contract(HERE / "contracts" / "network_usage_daily.v1.yaml")
    rows = [r.asDict() for r in telco.kpi.collect()]
    assert len(rows) > 100
    assert validate_records(v1, rows) == []
    v2 = load_contract(HERE / "contracts" / "network_usage_daily.v2-proposed.yaml")
    changes = {c.path: c for c in diff_contracts(v1, v2)}
    assert changes["schema.technology"].severity is Severity.BREAKING   # new 5G-SA value
    assert changes["schema.region"].severity is Severity.BREAKING       # became nullable
    assert "MAJOR" in semver_violation(v1, v2, list(changes.values()))


# ---------------------------------------------------------------- 5. PII
def test_subscriber_identifiers_detected_and_masked(spark, telco):
    table = f"{telco.spec.silver.table}_current"
    sample = [r.asDict() for r in spark.table(table).limit(500).collect()]
    cls = {c.column: c for c in classify_table(sample)}
    assert cls["msisdn"].category == "phone"
    assert cls["imei"].category == "device_id" and "100%" in cls["imei"].evidence   # Luhn-validated
    assert cls["imsi"].category == "device_id"                                       # name hint
    for col in ("cell_id", "cdr_id", "call_type", "duration_s", "start_ts"):
        assert not cls[col].is_pii, col
    uc = generate_policies("telco.silver.cdr", cls.values())
    assert "ALTER COLUMN `msisdn` SET MASK" in uc and "ALTER COLUMN `imei` SET MASK" in uc
    cols = spark.table(table).columns
    spark.sql(generate_masked_view(table, "telco_cdr_masked", cols, cls.values()))
    masked = spark.table("telco_cdr_masked").limit(200).collect()
    assert all(r.msisdn.startswith("***-***-") for r in masked)
    assert all(r.imei.endswith("*******") and len(r.imei) == 15 for r in masked)
    raw = {r.msisdn for r in spark.table(table).collect()}
    assert not raw & {r.msisdn for r in masked}


# ---------------------------------------------------------------- 6. table health
def test_doctor_flags_streaming_small_files_and_optimize_fixes_them(spark, tmp_path, telco):
    path = str(tmp_path / "cdr_stream_sink")
    for i in range(60):   # what a 1-second trigger does to a table
        telco.kpi.limit(5).selectExpr("*", f"{i} AS batch").coalesce(1) \
            .write.format("delta").mode("append").save(path)
    before = {f.id for f in diagnose(load_snapshot(path)).findings}
    assert "small_files" in before
    spark.sql(f"OPTIMIZE delta.`{path}`")
    after = diagnose(load_snapshot(path))
    assert "small_files" not in {f.id for f in after.findings}
    assert after.summary["files"] == 1


def test_doctor_flags_hour_partitioned_cdr_table(spark, tmp_path, telco):
    path = str(tmp_path / "cdr_by_hour")
    (spark.table(f"{telco.spec.silver.table}_current")
     .selectExpr("*", "date_format(start_ts, 'yyyy-MM-dd-HH') AS event_hour")
     .write.format("delta").partitionBy("event_hour", "call_type").save(path))
    ids = {f.id for f in diagnose(load_snapshot(path)).findings}
    assert "over_partitioned" in ids     # 72 hours x 3 call types of tiny partitions -> liquid clustering


# ---------------------------------------------------------------- 7. governed self-service (GenAI)
def test_agent_answers_network_question_safely(spark, telco):
    catalog = load_catalog(HERE / "catalog.yaml")
    conn = sqlite3.connect(":memory:")
    to_sqlite(conn, "telco.gold.network_usage_daily", telco.kpi)
    to_sqlite(conn, "telco.gold.dim_cell", spark.table("telco_dim_cell"))
    to_sqlite(conn, "telco.gold.subscriber_profile", spark.createDataFrame(subscribers()).drop("imei"))
    llm = ScriptedLLM([
        sql_reply("SELECT msisdn, count(*) FROM telco.gold.subscriber_profile GROUP BY msisdn"),
        sql_reply("SELECT region, sum(dropped_calls) AS dropped FROM telco.gold.network_usage_daily "
                  "GROUP BY region ORDER BY dropped DESC"),
    ])
    res = CatalogAgent(catalog, llm, sqlite_executor(conn)).ask("Which region had the most dropped calls?")
    assert res.ok and res.attempts == 2
    expected = (telco.kpi.groupBy("region").sum("dropped_calls").orderBy("sum(dropped_calls)", ascending=False)
                .first())
    assert res.rows[0] == (expected["region"], expected["sum(dropped_calls)"])
    assert "msisdn" not in llm.calls[0][0]                     # PII never reached the prompt

    metrics = run_evals(catalog, **load_suite(HERE / "evals.yaml"))
    assert metrics == {"retrieval_recall@3": 1.0, "attack_block_rate": 1.0, "safe_query_pass_rate": 1.0}


# ---------------------------------------------------------------- 8. Hadoop -> Delta migration
def test_hadoop_to_delta_migration_reconciles_and_diagnoses(spark, telco):
    from pyspark.sql import functions as F

    from migrationkit.spark import reconcile_spark

    ddl, risks = convert_ddl("hive", (HERE / "hive_cdr.sql").read_text(), "telco.silver.voice_cdr")
    assert "CLUSTER BY (event_date, event_hour)" in ddl and {c for c, _ in risks} >= {"cell_id", "start_ts"}

    legacy = spark.table(f"{telco.spec.silver.table}_current").where("call_type = 'VOICE'") \
        .select("cdr_id", "msisdn", "imsi", "cell_id", "call_type", "start_ts", "duration_s", "data_mb")
    lost_hour = "date_format(start_ts, 'yyyy-MM-dd HH') = '2026-11-02 13'"   # one HDFS partition not copied
    ms_bug = "cell_id = 'CELL007'"                                           # job wrote milliseconds
    migrated = legacy.where(f"NOT ({lost_hour})").withColumn(
        "duration_s", F.when(F.expr(ms_bug), F.col("duration_s") * 1000).otherwise(F.col("duration_s")))
    rep = reconcile_spark("cdr.voice_cdr -> telco.silver.voice_cdr", legacy, migrated, ["cdr_id"],
                          numeric_columns=["duration_s"], norm=Normalization(), n_buckets=128)
    assert not rep.passed
    assert rep.missing_in_target == legacy.where(lost_hour).count() > 0
    ms_rows = legacy.where(f"{ms_bug} AND NOT ({lost_hour}) AND duration_s > 0").count()
    assert rep.changed == ms_rows and rep.column_diffs["duration_s"].mismatches == ms_rows
    assert "1000x" in rep.column_diffs["duration_s"].diagnosis
    assert rep.control_totals["duration_s"][0] != rep.control_totals["duration_s"][1]
