"""Confluent Schema Registry wire format.

Every Avro/Protobuf/JSON-Schema message produced with a Schema Registry serializer
starts with ``0x00`` followed by a 4-byte big-endian schema id. Spark's ``from_avro``
does not strip this header, which is the most common reason Kafka-to-Delta
jobs fail with "malformed records". These helpers strip and validate it, and the
Spark expressions do the same on the JVM side without a Python UDF.
"""

from __future__ import annotations

import struct

MAGIC_BYTE = 0


class WireFormatError(ValueError):
    pass


def split_confluent_payload(payload: bytes | None) -> tuple[int, bytes]:
    """Return ``(schema_id, body)`` or raise :class:`WireFormatError`."""
    if payload is None:
        raise WireFormatError("null payload (tombstone)")
    if len(payload) < 5:
        raise WireFormatError(f"payload too short ({len(payload)} bytes)")
    if payload[0] != MAGIC_BYTE:
        raise WireFormatError(f"unknown magic byte {payload[0]!r}")
    (schema_id,) = struct.unpack(">I", payload[1:5])
    return schema_id, payload[5:]


def spark_schema_id_expr(col: str = "value") -> str:
    """Spark SQL expression that extracts the schema id (NULL when header invalid)."""
    return (
        f"CASE WHEN length({col}) >= 5 AND substring({col}, 1, 1) = X'00' "
        f"THEN conv(hex(substring({col}, 2, 4)), 16, 10) END"
    )


def spark_body_expr(col: str = "value") -> str:
    return f"substring({col}, 6, length({col}) - 5)"
