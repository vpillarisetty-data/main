"""Synthetic hotel data: 120 properties, loyalty guests and reservation events with realistic defects."""

from __future__ import annotations

import datetime as dt
import random
from dataclasses import dataclass, field
from decimal import Decimal

from scenarios.common import luhn_complete

CITIES = ["Tampa", "Dallas", "Austin", "Atlanta", "Denver", "Chicago", "Orlando", "Nashville"]
TIERS = ["MEMBER", "SILVER", "GOLD", "PLATINUM"]
RES_SCHEMA = ("reservation_id STRING, guest_id STRING, property_code STRING, check_in DATE, check_out DATE, "
              "rooms INT, room_rate DECIMAL(10,2), status STRING, channel STRING, loyalty_tier STRING, "
              "loyalty_number STRING, guest_email STRING, guest_phone STRING, card_on_file STRING, "
              "passport_no STRING, special_requests STRING, modified_seq BIGINT, op STRING")


def properties() -> list[dict]:
    rnd = random.Random(10)
    return [{"property_code": f"P{i:03d}", "property_name": f"Hotel {i:03d}", "city": CITIES[i % len(CITIES)],
             "brand": rnd.choice(["Select", "Premium", "Resort"]), "rooms_available": rnd.randint(120, 320)}
            for i in range(1, 121)]


def guests(n: int = 600) -> list[dict]:
    rnd = random.Random(11)
    out = []
    for i in range(n):
        out.append({
            "guest_id": f"G{i:05d}",
            "loyalty_tier": rnd.choice(TIERS),
            "loyalty_number": f"RW{rnd.randint(10**8, 10**9 - 1)}",
            "guest_email": f"guest{i}@example.com",
            "guest_phone": f"813-{rnd.randint(200, 999)}-{rnd.randint(1000, 9999)}",
            "card_on_file": luhn_complete("4", 16, rnd),
            "passport_no": f"X{rnd.randint(10**6, 10**7 - 1)}" if rnd.random() < 0.3 else None,
        })
    return out


@dataclass
class ReservationFeed:
    batches: list[list[dict]] = field(default_factory=list)
    modified: dict[str, dict] = field(default_factory=dict)   # reservation_id -> latest version
    cancelled: set[str] = field(default_factory=set)
    erased: set[str] = field(default_factory=set)
    quarantined: dict[str, str] = field(default_factory=dict)  # reservation_id -> rule
    upgraded: dict[str, str] = field(default_factory=dict)     # guest_id -> new tier
    valid: set[str] = field(default_factory=set)


def reservation_feed(n: int = 3000) -> ReservationFeed:
    rnd = random.Random(12)
    props, gs = properties(), guests()
    feed, seq = ReservationFeed(), 0
    base = dt.date(2026, 11, 1)
    b0, b1, b2, b3, originals = [], [], [], [], {}
    for i in range(n):
        g, p = gs[i % len(gs)], props[i % len(props)]
        seq += 1
        check_in = base + dt.timedelta(days=rnd.randint(0, 27))
        rec = {
            "reservation_id": f"R{i:06d}", "guest_id": g["guest_id"], "property_code": p["property_code"],
            "check_in": check_in, "check_out": check_in + dt.timedelta(days=rnd.randint(1, 4)),
            "rooms": rnd.choice([1, 1, 1, 2, 3]), "room_rate": Decimal(rnd.randint(8900, 42900)) / 100,
            "status": "BOOKED", "channel": rnd.choice(["DIRECT", "OTA", "GDS"]),
            "loyalty_tier": g["loyalty_tier"], "loyalty_number": g["loyalty_number"],
            "guest_email": g["guest_email"], "guest_phone": g["guest_phone"], "card_on_file": g["card_on_file"],
            "passport_no": g["passport_no"], "special_requests": rnd.choice(["late check-in", "high floor", "", "crib"]),
            "modified_seq": seq, "op": "U",
        }
        if i % 40 == 13:   # front desk typed the card into a free-text field
            rec["special_requests"] = f"guarantee with card {luhn_complete('4', 16, rnd)} per guest call"
        rid = rec["reservation_id"]
        if i % 150 == 4:
            rec["check_out"] = rec["check_in"]
            feed.quarantined[rid] = "stay_has_nights"
        elif i % 333 == 8:
            rec["room_rate"] = Decimal("-1.00")
            feed.quarantined[rid] = "rate_non_negative"
        elif i % 277 == 6:
            rec["status"] = "PENDING_X"                       # caught by the CONTRACT-derived rule
            feed.quarantined[rid] = "status_in_enum"
        else:
            feed.valid.add(rid)
        originals[rid] = rec
        b0.append(rec)
    for rid in sorted(feed.valid):
        i = int(rid[1:])
        rec = originals[rid]
        if i % 30 == 0:                                       # guest extends the stay at a new rate
            seq += 1
            new = {**rec, "check_out": rec["check_out"] + dt.timedelta(days=1),
                   "room_rate": rec["room_rate"] + Decimal("20.00"), "modified_seq": seq}
            feed.modified[rid] = new
            b1.append(new)
            b2.append(dict(rec))                              # PMS outage replay of the old version
        elif i % 25 == 3:
            seq += 1
            b1.append({**rec, "status": "CANCELLED", "modified_seq": seq})
            feed.cancelled.add(rid)
        elif i % 400 == 11:
            seq += 1
            b1.append({**rec, "op": "D", "modified_seq": seq})  # GDPR erasure
            feed.erased.add(rid)
    for g in gs[:15]:                                          # loyalty upgrades drive gold SCD2
        seq += 1
        new_tier = "PLATINUM" if g["loyalty_tier"] != "PLATINUM" else "GOLD"
        rid = f"U{seq:06d}"
        b3.append({**originals["R000001"], "reservation_id": rid, "guest_id": g["guest_id"],
                   "loyalty_tier": new_tier, "modified_seq": seq, "loyalty_number": g["loyalty_number"]})
        feed.upgraded[g["guest_id"]] = new_tier
    feed.batches = [b0, b1, b2, b3]
    return feed
