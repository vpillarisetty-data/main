"""Hospitality (Remington-style) end-to-end: 120 hotels' PMS events -> Kafka -> Delta -> RevPAR KPIs ->
governed self-service, plus the on-prem SQL Server -> lakehouse migration check.
The silver data contract *generates* part of the pipeline's quality rules, so the two cannot drift apart."""

from __future__ import annotations

import datetime as dt
import sqlite3
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from catalog_agent import CatalogAgent, load_catalog
from catalog_agent.agent import sqlite_executor
from catalog_agent.evals import load_suite
from catalog_agent.evals import run as run_evals
from catalog_agent.llm import ScriptedLLM
from contractkit import Severity, diff_contracts, load_contract, semver_violation, validate_records
from deltadoctor import diagnose, load_snapshot
from medallion.runner import MedallionRunner
from medallion.spec import parse_spec
from migrationkit import Normalization, convert_ddl, reconcile_rows
from migrationkit.reconcile import dbapi_rows
from piiguard import classify_table, generate_masked_view, generate_policies
from scenarios.common import confluent, drop, kafka_batch, localize, sql_reply, to_sqlite
from scenarios.hospitality.data import RES_SCHEMA, guests, properties, reservation_feed
from streamguard.sink import ExactlyOnceSink

HERE = Path(__file__).parent
KPI_SQL = """
WITH nights AS (
  SELECT r.property_code, explode(sequence(r.check_in, date_sub(r.check_out, 1))) AS stay_date,
         r.rooms, r.room_rate
  FROM {silver} r WHERE r.status <> 'CANCELLED'
)
SELECT n.stay_date, n.property_code, p.city, sum(n.rooms) AS rooms_sold, p.rooms_available,
       CAST(sum(n.rooms * n.room_rate) AS DECIMAL(14,2)) AS room_revenue,
       CAST(round(sum(n.rooms) / p.rooms_available, 4) AS DOUBLE) AS occupancy,
       CAST(sum(n.rooms * n.room_rate) / sum(n.rooms) AS DECIMAL(10,2)) AS adr,
       CAST(sum(n.rooms * n.room_rate) / p.rooms_available AS DECIMAL(10,2)) AS revpar
FROM nights n JOIN hosp_dim_property p ON p.property_code = n.property_code
GROUP BY n.stay_date, n.property_code, p.city, p.rooms_available
"""


def build_spec():
    """Pipeline YAML + rules generated from the silver data contract."""
    raw = yaml.safe_load((HERE / "reservations_pipeline.yaml").read_text())
    contract = load_contract(HERE / "contracts" / "reservation.v1.yaml")
    raw["silver"]["expectations"] += contract.to_expectations()
    return localize(parse_spec(raw)), contract


@pytest.fixture(scope="module")
def hotel(spark):
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spec, contract = build_spec()
    drop(spark, spec.bronze_table, spec.silver.table, spec.silver.quarantine_table, spec.gold.table,
         "hosp_bronze_dlq", "hosp_dim_property", "hosp_gold_property_daily_kpis")
    spark.createDataFrame(properties()).write.format("delta").saveAsTable("hosp_dim_property")
    feed = reservation_feed()
    sink = ExactlyOnceSink("pms-stream", spec.bronze_table, "hosp_bronze_dlq",
                           f"from_json(cast(body AS string), '{RES_SCHEMA}')",
                           dedup_keys=("reservation_id", "modified_seq"), required_fields=("reservation_id",))
    runner = MedallionRunner(spark, spec)
    offset = 0
    for batch_id, events in enumerate(feed.batches):
        raw = kafka_batch(spark, "pms-reservations", [confluent(e) for e in events], offset)
        offset += len(events)
        sink(raw, batch_id)
        runner._silver_batch(sink.split(raw)[0], batch_id)
        if batch_id == 2:
            runner._gold_batch(spark.table(spec.silver.table).where("reservation_id LIKE 'R%'"), 0)
    runner._gold_batch(spark.table(spec.silver.table).where("reservation_id LIKE 'U%'"), 1)
    spark.sql(KPI_SQL.format(silver=f"{spec.silver.table}_current")).write.format("delta") \
        .saveAsTable("hosp_gold_property_daily_kpis")
    return SimpleNamespace(spec=spec, contract=contract, feed=feed, kpi=spark.table("hosp_gold_property_daily_kpis"),
                           silver=spark.table(f"{spec.silver.table}_current"))


# ---------------------------------------------------------------- pipeline correctness
def test_contract_rules_are_part_of_the_pipeline(hotel):
    names = {e.name for e in hotel.spec.silver.expectations.expectations}
    assert {"status_in_enum", "channel_in_enum", "reservation_id_not_null"} <= names


def test_modifications_replays_cancellations_and_erasure(hotel):
    cur = {r.reservation_id: r for r in hotel.silver.collect()}
    f = hotel.feed
    for rid, latest in f.modified.items():   # extension kept although the old version was replayed later
        assert (cur[rid].check_out, cur[rid].room_rate) == (latest["check_out"], latest["room_rate"]), rid
    assert all(cur[r].status == "CANCELLED" for r in f.cancelled)   # cancellation is a status, not a delete
    assert not f.erased & cur.keys()                                 # GDPR erasure removes the row
    upgrades = {e["reservation_id"] for e in f.batches[3]}
    assert set(cur) == (f.valid - f.erased) | upgrades


def test_quarantine_explains_every_rejected_reservation(spark, hotel):
    q = {r.reservation_id: r._dq_violations for r in spark.table(hotel.spec.silver.quarantine_table).collect()}
    assert set(q) == set(hotel.feed.quarantined)
    for rid, rule in hotel.feed.quarantined.items():
        assert rule in q[rid], (rid, q[rid])


def test_silver_rows_satisfy_the_reservation_contract(hotel):
    cols = [f.name for f in hotel.contract.fields]
    rows = [{c: r[c] for c in cols} for r in hotel.silver.collect()]
    assert validate_records(hotel.contract, rows, max_errors=5) == []


def test_gold_loyalty_tier_history(spark, hotel):
    gold = spark.table(hotel.spec.gold.table)
    current = {r["guest_id"]: r["loyalty_tier"] for r in gold.where("__is_current").collect()}
    for guest_id, tier in hotel.feed.upgraded.items():
        assert current[guest_id] == tier
    assert gold.where("NOT __is_current").count() >= len(hotel.feed.upgraded) - 1
    detail = spark.sql(f"DESCRIBE DETAIL {hotel.spec.gold.table}").first()
    assert list(detail["clusteringColumns"]) == ["guest_id"]   # liquid clustering applied by the runner


# ---------------------------------------------------------------- KPIs + contract
def test_revpar_kpis_are_correct_and_contracted(hotel):
    rows = [r.asDict() for r in hotel.kpi.collect()]
    assert validate_records(load_contract(HERE / "contracts" / "property_daily_kpis.v1.yaml"), rows) == []
    assert all(0 < r["occupancy"] <= 1 for r in rows)
    for r in rows[:50]:   # RevPAR = ADR x occupancy (within rounding)
        assert abs(r["revpar"] - r["adr"] * Decimal(r["rooms_sold"]) / r["rooms_available"]) <= Decimal("0.02")
    v1 = load_contract(HERE / "contracts" / "property_daily_kpis.v1.yaml")
    v2 = load_contract(HERE / "contracts" / "property_daily_kpis.v2-proposed.yaml")
    ch = {c.path: c for c in diff_contracts(v1, v2)}
    assert ch["schema.adr"].severity is Severity.BREAKING            # money as DOUBLE
    assert ch["schema.room_revenue"].severity is Severity.NON_BREAKING
    assert ch["sla.freshness"].severity is Severity.BREAKING
    assert semver_violation(v1, v2, list(ch.values()))


# ---------------------------------------------------------------- PII / PCI
def test_guest_pii_and_cards_in_free_text_are_masked(spark, hotel):
    table = f"{hotel.spec.silver.table}_current"
    cls = {c.column: c for c in classify_table([r.asDict() for r in hotel.silver.limit(800).collect()])}
    assert cls["card_on_file"].category == "credit_card" and cls["card_on_file"].sensitivity == "restricted"
    assert cls["guest_email"].category == "email" and cls["guest_phone"].category == "phone"
    assert cls["special_requests"].embedded and cls["special_requests"].category == "credit_card"
    assert cls["loyalty_number"].category == "loyalty_id" and cls["passport_no"].category == "government_id"
    for col in ("reservation_id", "property_code", "guest_id", "channel", "status"):
        assert not cls[col].is_pii, col
    assert "pci_cardholder_data_readers" in generate_policies("hosp.silver.reservations", cls.values())
    # Azure Synapse / OSS has no UC column masks -> masked view with identical rules
    spark.sql(generate_masked_view(table, "hosp_reservations_masked", hotel.silver.columns, cls.values()))
    masked = spark.table("hosp_reservations_masked").collect()
    assert all(r.card_on_file.startswith("XXXX-XXXX-XXXX-") for r in masked)
    leaked = [r.special_requests for r in masked if r.special_requests and "card" in r.special_requests]
    assert leaked and all("[PAN]" in s and not any(ch.isdigit() for ch in s) for s in leaked)


# ---------------------------------------------------------------- table layout
def test_property_partitioning_flagged_and_liquid_clustering_fixes_it(spark, tmp_path, hotel):
    part = str(tmp_path / "kpi_by_property")
    hotel.kpi.write.format("delta").partitionBy("property_code").save(part)
    assert "over_partitioned" in {f.id for f in diagnose(load_snapshot(part)).findings}
    spark.sql("DROP TABLE IF EXISTS hosp_kpi_clustered")
    spark.sql(f"CREATE TABLE hosp_kpi_clustered USING delta CLUSTER BY (property_code) "
              f"AS SELECT * FROM delta.`{part}`")
    location = spark.sql("DESCRIBE DETAIL hosp_kpi_clustered").first()["location"].replace("file:", "")
    snap = load_snapshot(location)
    assert snap.clustering_columns == ["property_code"] and not snap.partition_columns
    assert "over_partitioned" not in {f.id for f in diagnose(snap).findings}


# ---------------------------------------------------------------- governed self-service
def test_agent_finds_top_revpar_hotel_without_touching_guest_pii(spark, hotel):
    conn = sqlite3.connect(":memory:")
    to_sqlite(conn, "hosp.gold.property_daily_kpis", hotel.kpi)
    to_sqlite(conn, "hosp.gold.dim_property", spark.table("hosp_dim_property"))
    to_sqlite(conn, "hosp.gold.guest_profile",
              spark.createDataFrame(guests()).select("guest_id", "loyalty_tier", "guest_email", "guest_phone",
                                                     "card_on_file"))
    llm = ScriptedLLM([
        sql_reply("SELECT g.guest_email, k.revpar FROM hosp.gold.property_daily_kpis k, hosp.gold.guest_profile g"),
        sql_reply("SELECT k.property_code, p.property_name, round(sum(k.room_revenue) / sum(k.rooms_available), 2) "
                  "AS revpar FROM hosp.gold.property_daily_kpis k JOIN hosp.gold.dim_property p "
                  "ON p.property_code = k.property_code GROUP BY k.property_code, p.property_name "
                  "ORDER BY revpar DESC"),
    ])
    agent = CatalogAgent(load_catalog(HERE / "catalog.yaml"), llm, sqlite_executor(conn))
    res = agent.ask("Which hotel property had the highest RevPAR (revenue per available room)?")
    assert res.ok and res.attempts == 2
    from pyspark.sql import functions as F
    top = (hotel.kpi.groupBy("property_code")
           .agg((F.sum("room_revenue") / F.sum("rooms_available")).alias("revpar"))
           .orderBy(F.desc("revpar")).first())
    assert res.rows[0][0] == top["property_code"]
    assert abs(res.rows[0][2] - float(top["revpar"])) < 0.01
    assert "guest_email" not in llm.calls[0][0]
    metrics = run_evals(load_catalog(HERE / "catalog.yaml"), **load_suite(HERE / "evals.yaml"))
    assert metrics == {"retrieval_recall@3": 1.0, "attack_block_rate": 1.0, "safe_query_pass_rate": 1.0}


# ---------------------------------------------------------------- SQL Server -> lakehouse migration
def _legacy_sqlserver(path, rows):
    sqlite3.register_converter("DATETIME", lambda b: dt.datetime.fromisoformat(b.decode()))
    sqlite3.register_converter("DATE", lambda b: dt.date.fromisoformat(b.decode()))
    conn = sqlite3.connect(path, detect_types=sqlite3.PARSE_DECLTYPES)
    conn.execute("CREATE TABLE reservation (reservation_id TEXT, property_code CHAR(6), check_in DATE, "
                 "check_out DATE, rooms TINYINT, room_rate MONEY, status TEXT, created_at DATETIME)")
    conn.executemany("INSERT INTO reservation VALUES (?,?,?,?,?,?,?,?)", rows)
    conn.commit()
    return conn


def test_sqlserver_to_lakehouse_migration(spark, tmp_path, hotel):
    ddl, risks = convert_ddl("sqlserver", (HERE / "sqlserver_reservations.sql").read_text(),
                             "hosp.silver.reservation")
    assert "`rooms` SMALLINT NOT NULL" in ddl and "`roomrate` DECIMAL(19,4) NOT NULL" in ddl
    assert {c for c, _ in risks} >= {"rooms", "propertycode", "createdat"}

    base = dt.datetime(2026, 10, 1)
    src = []
    for r in hotel.silver.orderBy("reservation_id").collect():
        created = base + dt.timedelta(minutes=r.modified_seq, milliseconds=r.modified_seq % 997)
        src.append((r.reservation_id, r.property_code.ljust(6), r.check_in.isoformat(), r.check_out.isoformat(),
                    r.rooms, float(r.room_rate), r.status, created.isoformat(sep=" ")))
    legacy = _legacy_sqlserver(str(tmp_path / "legacy.db"), src)

    migrated = []
    for rid, prop, ci, co, rooms, rate, status, created in src:
        if status == "CANCELLED":
            continue                                   # BUG: migration job filtered out cancellations
        ts = dt.datetime.fromisoformat(created)
        if prop.strip() == "P007":
            ts += dt.timedelta(hours=5)                # BUG: one property's EST timestamps loaded as UTC
        migrated.append((rid, prop.strip(), dt.date.fromisoformat(ci), dt.date.fromisoformat(co), rooms,
                         Decimal(str(rate)).quantize(Decimal("0.0001")), status, ts))
    spark.createDataFrame(migrated, "reservation_id STRING, property_code STRING, check_in DATE, check_out DATE, "
                          "rooms SMALLINT, room_rate DECIMAL(19,4), status STRING, created_at TIMESTAMP") \
        .write.format("delta").mode("overwrite").saveAsTable("hosp_silver_reservation_migrated")
    target = spark.table("hosp_silver_reservation_migrated")

    rep = reconcile_rows(
        "dbo.Reservation -> hosp.silver.reservation",
        dbapi_rows(legacy, "SELECT * FROM reservation"),
        lambda: (r.asDict() for r in target.toLocalIterator()),
        keys=["reservation_id"],
        columns=["property_code", "check_in", "check_out", "rooms", "room_rate", "status", "created_at"],
        numeric_columns=["room_rate"], norm=Normalization(rtrim_strings=True, timestamp_precision="ms"),
    )
    cancelled = sum(1 for s in src if s[6] == "CANCELLED")
    p007 = sum(1 for s in src if s[1].strip() == "P007" and s[6] != "CANCELLED")
    assert not rep.passed
    assert rep.missing_in_target == cancelled > 0
    assert rep.changed == p007 > 0 and set(rep.column_diffs) == {"created_at"}
    assert "timezone" in rep.column_diffs["created_at"].diagnosis
    assert "property_code" not in rep.column_diffs      # CHAR(6) padding tolerated by the signed-off rule
