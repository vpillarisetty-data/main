"""Synthetic telecom data: cells, subscribers and call-detail records with realistic defects."""

from __future__ import annotations

import datetime as dt
import random
from dataclasses import dataclass, field

from scenarios.common import luhn_complete

REGIONS = ["NSW", "VIC", "QLD", "WA"]
CDR_SCHEMA = ("cdr_id STRING, msisdn STRING, imsi STRING, imei STRING, cell_id STRING, call_type STRING, "
              "start_ts TIMESTAMP, duration_s INT, data_mb DOUBLE, release_cause STRING, event_seq BIGINT, op STRING")


def cells() -> list[dict]:
    rnd = random.Random(1)
    return [{"cell_id": f"CELL{i:03d}", "site_name": f"Site {i:03d}", "region": REGIONS[i % 4],
             "technology": rnd.choice(["4G", "5G"])} for i in range(40)]


def subscribers(n: int = 300) -> list[dict]:
    rnd = random.Random(2)
    return [{
        "msisdn": f"+614{rnd.randint(10_000_000, 99_999_999)}",
        "imsi": f"50501{rnd.randint(10**9, 10**10 - 1)}",
        "imei": luhn_complete("35" + str(rnd.randint(100000, 999999)), 15, rnd),
        "plan": rnd.choice(["prepaid", "postpaid-s", "postpaid-l"]),
        "home_region": rnd.choice(REGIONS),
        "tenure_months": rnd.randint(1, 120),
    } for _ in range(n)]


@dataclass
class CdrFeed:
    batches: list[list[dict | None | bytes]] = field(default_factory=list)  # raw payloads per micro-batch
    rerated: dict[str, int] = field(default_factory=dict)       # cdr_id -> corrected duration
    cancelled: set[str] = field(default_factory=set)
    bad_msisdn: set[str] = field(default_factory=set)
    negative: set[str] = field(default_factory=set)
    valid_originals: set[str] = field(default_factory=set)
    poison: dict[str, int] = field(default_factory=dict)        # DLQ reason -> count
    moved: dict[str, str] = field(default_factory=dict)         # msisdn -> new cell (gold SCD2)


def cdr_feed(n: int = 2000) -> CdrFeed:
    rnd = random.Random(3)
    subs, cell_ids = subscribers(), [c["cell_id"] for c in cells()]
    feed, seq = CdrFeed(), 0
    start = dt.datetime(2026, 11, 1)
    originals, b0, b1, b2 = {}, [], [], []
    for i in range(n):
        s = subs[i % len(subs)]
        seq += 1
        call_type = rnd.choices(["VOICE", "SMS", "DATA"], [5, 2, 3])[0]
        rec = {
            "cdr_id": f"CDR{i:06d}", "msisdn": s["msisdn"], "imsi": s["imsi"], "imei": s["imei"],
            "cell_id": rnd.choice(cell_ids), "call_type": call_type,
            "start_ts": start + dt.timedelta(seconds=rnd.randint(0, 3 * 86_400 - 1)),
            "duration_s": rnd.randint(5, 1200) if call_type == "VOICE" else 0,
            "data_mb": round(rnd.uniform(0.5, 800), 3) if call_type == "DATA" else 0.0,
            "release_cause": "DROPPED" if call_type == "VOICE" and rnd.random() < 0.04 else "NORMAL",
            "event_seq": seq, "op": "U",
        }
        if i % 97 == 5:
            rec["msisdn"] = "0412" + rec["msisdn"][-6:]          # local format from a legacy switch
            feed.bad_msisdn.add(rec["cdr_id"])
        elif i % 211 == 9:
            rec["duration_s"] = -rec["duration_s"] - 1            # corrupt counter
            feed.negative.add(rec["cdr_id"])
        else:
            feed.valid_originals.add(rec["cdr_id"])
        originals[rec["cdr_id"]] = rec
        b0.append(rec)
        if i % 50 == 0:
            b0.append(dict(rec))                                    # producer retry: exact duplicate
    for cid, rec in originals.items():
        idx = int(cid[3:])
        if cid not in feed.valid_originals:
            continue
        if idx % 40 == 0 and rec["call_type"] == "VOICE":
            seq += 1
            fixed = {**rec, "duration_s": rec["duration_s"] + 30, "event_seq": seq}   # re-rated
            feed.rerated[cid] = fixed["duration_s"]
            b1.append(fixed)
            b2.append(dict(rec))                                    # late replay of the ORIGINAL
        elif idx % 100 == 7:
            seq += 1
            b1.append({**rec, "op": "D", "event_seq": seq})         # cancelled CDR
            feed.cancelled.add(cid)
    poison = [b"\x01not-confluent"] * 3 + [None] * 2
    feed.poison = {"bad_wire_format": 3, "tombstone": 2, "missing_cdr_id": 2}
    b0 += poison + [{**originals["CDR000001"], "cdr_id": None}] * 2
    # subscribers who moved to a new serving cell (drives the SCD2 gold history)
    b3 = []
    for s in subs[:10]:
        seq += 1
        new_cell = "CELL039" if s is not subs[0] else "CELL038"
        b3.append({**originals["CDR000002"], "cdr_id": f"MOVE{seq}", "msisdn": s["msisdn"], "cell_id": new_cell,
                   "event_seq": seq, "call_type": "SMS", "duration_s": 0, "data_mb": 0.0,
                   "release_cause": "NORMAL"})
        feed.moved[s["msisdn"]] = new_cell
    feed.batches = [b0, b1, b2, b3]
    return feed
