import json
import random

import pytest

from deltadoctor import diagnose, load_snapshot
from deltadoctor.cli import main, render
from deltadoctor.diagnose import Config, clustering_overlap
from deltadoctor.log import FileEntry

MB = 1024**2
NOW = 1_800_000_000_000
DAY = 86_400_000


def write_log(root, commits, partition_cols=(), config=None, columns=("id", "amount")):
    log = root / "_delta_log"
    log.mkdir(parents=True)
    schema = {"type": "struct", "fields": [{"name": c, "type": "long", "nullable": True, "metadata": {}} for c in columns]}
    meta = {"metaData": {"id": "x", "format": {"provider": "parquet"}, "schemaString": json.dumps(schema),
                         "partitionColumns": list(partition_cols), "configuration": config or {}}}
    all_commits = [[meta, {"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}}], *commits]
    for v, actions in enumerate(all_commits):
        (log / f"{v:020d}.json").write_text("\n".join(json.dumps(a) for a in actions) + "\n")
    return str(root)


def add(path, size, lo=0, hi=10, part=None, stats=True):
    a = {"path": path, "size": size, "partitionValues": part or {}, "modificationTime": NOW, "dataChange": True}
    if stats:
        a["stats"] = json.dumps({"numRecords": 100, "minValues": {"id": lo}, "maxValues": {"id": hi}})
    return {"add": a}


def test_healthy_table_scores_100(tmp_path):
    adds = [add(f"f{i}.parquet", 200 * MB, i * 100, i * 100 + 99) for i in range(30)]
    report = diagnose(load_snapshot(write_log(tmp_path, [adds])), Config(now_ms=NOW))
    assert report.findings == [] and report.health_score == 100


def test_small_files_and_vacuum(tmp_path):
    adds = [add(f"f{i}.parquet", 1 * MB, i, i) for i in range(600)]
    removes = [{"remove": {"path": f"old{i}.parquet", "size": 50 * MB, "deletionTimestamp": NOW - 30 * DAY}}
               for i in range(40)]
    adds_old = [add(f"old{i}.parquet", 50 * MB) for i in range(40)]
    snap = load_snapshot(write_log(tmp_path, [adds_old, adds, removes]))
    assert len(snap.files) == 600 and len(snap.tombstones) == 40
    r = diagnose(snap, Config(now_ms=NOW))
    ids = {f.id: f for f in r.findings}
    assert ids["small_files"].severity == "critical"
    assert ids["vacuum_overdue"].metrics["reclaimable_bytes"] == 40 * 50 * MB
    assert r.health_score < 100
    assert "OPTIMIZE" in render(r)


def test_recent_tombstones_not_reclaimable(tmp_path):
    adds_old = [add(f"old{i}.parquet", 50 * MB) for i in range(40)]
    removes = [{"remove": {"path": f"old{i}.parquet", "size": 50 * MB, "deletionTimestamp": NOW - DAY}} for i in range(40)]
    r = diagnose(load_snapshot(write_log(tmp_path, [adds_old, removes])), Config(now_ms=NOW))
    assert "vacuum_overdue" not in {f.id for f in r.findings}


def test_clustering_overlap_math():
    sorted_files = [FileEntry(f"{i}", 1, (), 0, 1, {"id": i * 10}, {"id": i * 10 + 9}) for i in range(100)]
    assert clustering_overlap(sorted_files, "id") == pytest.approx(0.01)
    random.seed(1)
    rand = [FileEntry(f"{i}", 1, (), 0, 1, {"id": random.randint(0, 50)}, {"id": random.randint(950, 1000)})
            for i in range(100)]
    assert clustering_overlap(rand, "id") == 1.0
    assert clustering_overlap(sorted_files, "missing") is None


def test_weak_skipping_on_clustering_column(tmp_path):
    adds = [add(f"f{i}.parquet", 200 * MB, 0, 1_000_000) for i in range(40)]
    dm = {"domainMetadata": {"domain": "delta.clustering", "removed": False,
                             "configuration": json.dumps({"clusteringColumns": [["id"]]})}}
    snap = load_snapshot(write_log(tmp_path, [[dm, *adds]]))
    assert snap.clustering_columns == ["id"]
    f = {x.id: x for x in diagnose(snap, Config(now_ms=NOW)).findings}["weak_skipping:id"]
    assert f.severity == "warning" and f.metrics["overlap_fraction"] == 1.0


def test_over_partitioned_and_skew(tmp_path):
    adds = [add(f"d={i}/f.parquet", 5 * MB, part={"d": str(i)}) for i in range(150)]
    adds.append(add("d=hot/f.parquet", 900 * MB, part={"d": "hot"}))
    r = diagnose(load_snapshot(write_log(tmp_path, [adds], partition_cols=["d"])), Config(now_ms=NOW))
    ids = {f.id for f in r.findings}
    assert {"over_partitioned", "partition_skew"} <= ids


def test_missing_stats(tmp_path):
    adds = [add(f"f{i}.parquet", 200 * MB, stats=(i < 5)) for i in range(30)]
    ids = {f.id for f in diagnose(load_snapshot(write_log(tmp_path, [adds])), Config(now_ms=NOW)).findings}
    assert "missing_stats" in ids


def test_not_a_delta_table(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_snapshot(str(tmp_path))


def test_cli_json(tmp_path, capsys):
    path = write_log(tmp_path, [[add("a.parquet", 10)]])
    assert main([path, "--json"]) == 0
    assert json.loads(capsys.readouterr().out)[0]["health_score"] == 100


def test_real_delta_table_from_spark(spark, tmp_path):
    """Many tiny streaming-style appends -> doctor must flag small files on a real Delta log."""
    path = str(tmp_path / "events")
    spark.range(0).selectExpr("id", "id * 2 AS amount").write.format("delta") \
        .option("delta.checkpointInterval", "1000").save(path)
    spark.sql(f"ALTER TABLE delta.`{path}` SET TBLPROPERTIES ('delta.checkpointInterval' = '1000')")
    for i in range(30):
        spark.range(i * 10, i * 10 + 10, numPartitions=2).selectExpr("id", "id * 2 AS amount") \
            .write.format("delta").mode("append").save(path)
    snap = load_snapshot(path)
    assert len(snap.files) == 60 and snap.files[next(iter(snap.files))].num_records is not None
    ids = {f.id for f in diagnose(snap).findings}
    assert "small_files" in ids
