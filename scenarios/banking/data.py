"""Synthetic card-issuer data: accounts, merchants and authorization events with realistic defects."""

from __future__ import annotations

import datetime as dt
import random
from dataclasses import dataclass, field
from decimal import Decimal

TXN_SCHEMA = ("txn_id STRING, account_id STRING, amount DECIMAL(18,2), currency STRING, merchant_id STRING, "
              "mcc STRING, event_ts TIMESTAMP, seq BIGINT, op STRING")
MCC_GROUP = {"5411": "grocery", "5812": "dining", "4511": "travel", "7011": "travel", "5999": "retail"}


def accounts(n: int = 800) -> list[dict]:
    rnd = random.Random(20)
    return [{"account_id": f"A{i:05d}", "segment": rnd.choice(["mass", "affluent", "hnw"]),
             "tenure_years": rnd.randint(0, 30), "full_name": f"Cardmember {i}", "email": f"cm{i}@example.com",
             "tier": rnd.choice(["green", "gold", "platinum"])} for i in range(n)]


def merchants(n: int = 200) -> list[dict]:
    rnd = random.Random(21)
    out = []
    for i in range(n):
        mcc = rnd.choice(list(MCC_GROUP))
        out.append({"merchant_id": f"M{i:04d}", "mcc": mcc, "mcc_group": MCC_GROUP[mcc],
                    "country": rnd.choice(["US", "US", "US", "JP", "GB", "MX"])})
    return out


@dataclass
class TxnFeed:
    batches: list[list[dict]] = field(default_factory=list)
    reversed: set[str] = field(default_factory=set)
    quarantined: dict[str, str] = field(default_factory=dict)
    dropped: set[str] = field(default_factory=set)
    valid: set[str] = field(default_factory=set)
    adjustments: list[dict] = field(default_factory=list)


def txn_feed(n: int = 4000) -> TxnFeed:
    rnd = random.Random(22)
    accts, merch = accounts(), merchants()
    feed, seq, base = TxnFeed(), 0, dt.datetime(2026, 9, 1)   # in the past: the pipeline drops future-dated auths
    b0, b1, b2, originals = [], [], [], {}
    for i in range(n):
        m = rnd.choice(merch)
        seq += 1
        rec = {"txn_id": f"T{i:07d}", "account_id": accts[i % len(accts)]["account_id"],
               "amount": Decimal(rnd.randint(500, 250_000)) / 100, "currency": rnd.choice(["USD", "usd"]),
               "merchant_id": m["merchant_id"], "mcc": m["mcc"],
               "event_ts": base + dt.timedelta(seconds=rnd.randint(0, 29 * 86_400)), "seq": seq, "op": "U"}
        tid = rec["txn_id"]
        if i % 180 == 5:
            rec["currency"] = "U$"
            feed.quarantined[tid] = "valid_currency"
        elif i % 500 == 9:
            rec["amount"] = Decimal("2500000.00")
            feed.quarantined[tid] = "amount_in_range"
        elif i % 450 == 3:
            rec["event_ts"] = dt.datetime(2031, 1, 1)
            feed.dropped.add(tid)
        else:
            feed.valid.add(tid)
        if i % 90 == 1:
            rec["merchant_id"] = None             # warn-only rule: kept
        originals[tid] = rec
        b0.append(rec)
    for tid in sorted(feed.valid):
        if int(tid[1:]) % 50 == 7:
            seq += 1
            b1.append({**originals[tid], "op": "D", "seq": seq})   # reversal / void
            feed.reversed.add(tid)
            b2.append(dict(originals[tid]))                       # network re-sends the original late
    for tid in sorted(feed.valid - feed.reversed)[:22]:
        seq += 1
        feed.adjustments.append({**originals[tid], "amount": originals[tid]["amount"] - Decimal("1.00"), "seq": seq})
    feed.batches = [b0, b1, b2]
    return feed
