"""Shared fixtures. Spark tests are skipped automatically when pyspark/delta are absent."""

import pytest


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    pytest.importorskip("pyspark")
    delta = pytest.importorskip("delta")
    from pyspark.sql import SparkSession

    wh = tmp_path_factory.mktemp("warehouse")
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("de-accelerators-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", str(wh))
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
    )
    session = delta.configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()
