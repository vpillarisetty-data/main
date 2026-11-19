import struct

import pytest

from streamguard import WireFormatError, compute_lag, split_confluent_payload
from streamguard.lag import evaluate


def test_wire_format_roundtrip():
    payload = b"\x00" + struct.pack(">I", 1234) + b"avro-bytes"
    assert split_confluent_payload(payload) == (1234, b"avro-bytes")


@pytest.mark.parametrize("bad", [None, b"\x00\x01", b"\x01\x00\x00\x00\x01xx"])
def test_wire_format_rejects_bad(bad):
    with pytest.raises(WireFormatError):
        split_confluent_payload(bad)


def test_compute_lag_handles_new_partitions():
    lags = compute_lag('{"txn": {"0": 100}}', {"txn": {"0": 150, "1": 20}})
    assert [(x.partition, x.lag) for x in lags] == [(0, 50), (1, 20)]


def test_evaluate_sla_breach():
    progress = {
        "processedRowsPerSecond": 10.0,
        "sources": [{"endOffset": {"t": {"0": 0}}, "latestOffset": {"t": {"0": 10_000}}}],
    }
    r = evaluate(progress, sla_seconds=300)
    assert r.total_lag == 10_000 and r.seconds_to_drain == 1000 and r.breached
    assert not evaluate(progress, sla_seconds=5000).breached


def test_evaluate_stalled_stream_is_breach():
    progress = {"processedRowsPerSecond": 0, "sources": [{"endOffset": {"t": {"0": 1}}, "latestOffset": {"t": {"0": 9}}}]}
    assert evaluate(progress, 60).breached
