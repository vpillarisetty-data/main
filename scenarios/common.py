"""Shared plumbing for the industry scenarios: the glue a platform team writes once."""

from __future__ import annotations

import dataclasses
import json
import random
import sqlite3
import struct
from collections.abc import Iterable, Mapping
from decimal import Decimal

from medallion.spec import PipelineSpec


def local_name(table: str) -> str:
    """``telco.silver.cdr`` -> ``telco_silver_cdr`` (local Spark has no Unity Catalog)."""
    return table.replace(".", "_")


def localize(spec: PipelineSpec) -> PipelineSpec:
    silver = dataclasses.replace(
        spec.silver, table=local_name(spec.silver.table),
        quarantine_table=local_name(spec.silver.quarantine_table) if spec.silver.quarantine_table else None,
    )
    gold = dataclasses.replace(spec.gold, table=local_name(spec.gold.table)) if spec.gold else None
    return dataclasses.replace(spec, bronze_table=local_name(spec.bronze_table), silver=silver, gold=gold)


def drop(spark, *tables: str) -> None:
    for t in tables:
        spark.sql(f"DROP VIEW IF EXISTS {t}_current")
        spark.sql(f"DROP TABLE IF EXISTS {t}")


def _json_default(v):
    if isinstance(v, Decimal):
        return float(v)
    return v.isoformat() if hasattr(v, "isoformat") else str(v)


def confluent(record: Mapping | None, schema_id: int = 1) -> bytes | None:
    if record is None:
        return None
    return b"\x00" + struct.pack(">I", schema_id) + json.dumps(record, default=_json_default).encode()


def kafka_batch(spark, topic: str, payloads: Iterable[bytes | None], start_offset: int = 0):
    rows = [(topic, 0, start_offset + i, None, b"k", v) for i, v in enumerate(payloads)]
    return spark.createDataFrame(
        rows, "topic STRING, partition INT, offset LONG, timestamp TIMESTAMP, key BINARY, value BINARY")


def luhn_complete(prefix: str, length: int, rnd: random.Random) -> str:
    digits = [int(c) for c in prefix]
    while len(digits) < length - 1:
        digits.append(rnd.randint(0, 9))
    total = 0
    for i, d in enumerate(reversed(digits)):
        if i % 2 == 0:
            d = d * 2 - 9 if d * 2 > 9 else d * 2
        total += d
    return "".join(map(str, digits + [(10 - total % 10) % 10]))


def to_sqlite(conn: sqlite3.Connection, name: str, df) -> None:
    """Materialize a Spark DataFrame as a sqlite table named exactly ``name`` (agent demo executor)."""
    types = {"string": "TEXT", "date": "TEXT", "timestamp": "TEXT", "boolean": "INTEGER"}
    cols = ", ".join(f'"{f}" {types.get(t.split("(")[0], "REAL" if t.startswith(("decimal", "double", "float")) else "INTEGER")}'
                     for f, t in df.dtypes)
    conn.execute(f'CREATE TABLE "{name}" ({cols})')
    rows = [tuple(float(v) if isinstance(v, Decimal) else (v.isoformat() if hasattr(v, "isoformat") else v)
                  for v in r) for r in df.collect()]
    conn.executemany(f'INSERT INTO "{name}" VALUES ({", ".join("?" * len(df.columns))})', rows)


def sql_reply(sql: str) -> str:
    return f"```sql\n{sql.strip()}\n```"
