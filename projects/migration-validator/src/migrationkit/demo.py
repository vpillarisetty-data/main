"""A realistic legacy-to-lakehouse cutover rehearsal:  ``migrationkit demo``

Source: 20,000 card transactions "extracted from SQL Server" (CHAR padding, DATETIME2(7), DECIMAL).
Target: the migrated Delta table, with the defects real migrations actually have, some
harmless representation differences and some real bugs.

Run 1 (strict) flags everything. Run 2 applies the normalizations the business signs off on, and
what's left are the real defects, each with a diagnosed cause.
"""

from __future__ import annotations

import datetime as dt
import random
from decimal import Decimal

from migrationkit.normalize import Normalization
from migrationkit.reconcile import reconcile_rows

N = 20_000


def build() -> tuple[list[dict], list[dict]]:
    rnd = random.Random(42)
    base = dt.datetime(2026, 10, 1)
    src = []
    for i in range(N):
        src.append({
            "txn_id": f"T{i:07d}",
            "account_id": f"A{rnd.randint(1, 3000):05d}",
            "amount": Decimal(rnd.randint(100, 500_000)) / 100,
            "merchant_code": rnd.choice(["5411", "5812", "4511", "5999"]).ljust(6),  # CHAR(6) padding
            "event_ts": base + dt.timedelta(seconds=rnd.randint(0, 86_400 * 30), microseconds=rnd.randint(0, 999_999)),
            "status": rnd.choice(["APPROVED", "DECLINED", "REVERSED"]),
        })
    tgt = []
    for i, r in enumerate(src):
        t = dict(r)
        t["merchant_code"] = r["merchant_code"].rstrip()                       # harmless: trimmed on load
        t["event_ts"] = r["event_ts"].replace(microsecond=r["event_ts"].microsecond // 1000 * 1000)  # ms precision
        if i in (1_337, 8_888, 15_001):
            continue                                                            # BUG: rows dropped by a bad join
        if 5_000 <= i < 5_025:
            t["event_ts"] = t["event_ts"] + dt.timedelta(hours=4)              # BUG: EDT local loaded as UTC
        if i in (12_345, 17_000):
            t["amount"] = r["amount"] * 100                                     # BUG: cents vs dollars
        tgt.append(t)
    tgt.append(dict(tgt[100]))                                                  # BUG: duplicated row (no PK on target)
    return src, tgt


def main() -> None:
    src, tgt = build()
    cols = ["account_id", "amount", "merchant_code", "event_ts", "status"]
    common = dict(keys=["txn_id"], columns=cols, numeric_columns=["amount"], n_buckets=512)

    strict = reconcile_rows("legacy.dbo.card_txn -> fin.silver.card_txn", lambda: src, lambda: tgt,
                            norm=Normalization(rtrim_strings=False), **common)
    print("## Run 1: strict comparison\n")
    print(strict.to_markdown())

    tuned = reconcile_rows("legacy.dbo.card_txn -> fin.silver.card_txn", lambda: src, lambda: tgt,
                           norm=Normalization(rtrim_strings=True, timestamp_precision="ms"), **common)
    print("\n\n## Run 2: with signed-off normalizations (CHAR padding, ms precision)\n")
    print(tuned.to_markdown())


if __name__ == "__main__":
    main()
