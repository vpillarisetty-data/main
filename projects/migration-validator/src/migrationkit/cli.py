"""``migrationkit`` command line.

  migrationkit convert-ddl --from oracle schema.sql [--target-table cat.schema.t]
  migrationkit reconcile --source-db legacy.db --target-db lake.db --table card_txn --keys txn_id \\
               [--numeric amount] [--ts-precision ms] [--empty-as-null] [--case-insensitive] [--no-rtrim]
  migrationkit demo

``reconcile`` here takes SQLite files for a zero-setup trial. In production call
``reconcile_rows`` with ``dbapi_rows(conn, sql)`` on any DB-API driver, or ``reconcile_spark``.
"""

from __future__ import annotations

import argparse
import sqlite3
import sys

from migrationkit.normalize import Normalization
from migrationkit.reconcile import dbapi_rows, reconcile_rows
from migrationkit.typemap import convert_ddl


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="migrationkit")
    sub = ap.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("convert-ddl", help="translate source DDL to Delta DDL with a risk report")
    c.add_argument("ddl_file")
    c.add_argument("--from", dest="dialect", required=True, choices=["sqlserver", "oracle", "teradata", "hive"])
    c.add_argument("--target-table")
    r = sub.add_parser("reconcile", help="hash-bucket reconciliation between two SQLite databases")
    r.add_argument("--source-db", required=True)
    r.add_argument("--target-db", required=True)
    r.add_argument("--table", required=True)
    r.add_argument("--target-table")
    r.add_argument("--keys", required=True, help="comma-separated")
    r.add_argument("--numeric", default="", help="comma-separated control-total columns")
    r.add_argument("--ts-precision", default="us", choices=["s", "ms", "us"])
    r.add_argument("--empty-as-null", action="store_true")
    r.add_argument("--case-insensitive", action="store_true")
    r.add_argument("--no-rtrim", action="store_true")
    r.add_argument("--buckets", type=int, default=256)
    sub.add_parser("demo", help="run the cutover rehearsal demo")
    a = ap.parse_args(argv)

    if a.cmd == "demo":
        from migrationkit.demo import main as demo

        demo()
        return 0
    if a.cmd == "convert-ddl":
        with open(a.ddl_file, encoding="utf-8") as fh:
            sql, risks = convert_ddl(a.dialect, fh.read(), a.target_table)
        print(sql)
        if risks:
            print("\n-- RISK REPORT")
            for col, tm in risks:
                print(f"-- [{tm.risk:9s}] {col}: {tm.source} -> {tm.delta}: {tm.note}")
        return 0

    src, tgt = sqlite3.connect(a.source_db), sqlite3.connect(a.target_db)
    keys = [k.strip() for k in a.keys.split(",")]
    cur = src.execute(f"SELECT * FROM {a.table} LIMIT 0")  # noqa: S608 - CLI operator input
    cols = [d[0].lower() for d in cur.description]
    norm = Normalization(rtrim_strings=not a.no_rtrim, empty_string_as_null=a.empty_as_null,
                         case_insensitive=a.case_insensitive, timestamp_precision=a.ts_precision)
    rep = reconcile_rows(
        f"{a.table} -> {a.target_table or a.table}",
        dbapi_rows(src, f"SELECT * FROM {a.table}"),
        dbapi_rows(tgt, f"SELECT * FROM {a.target_table or a.table}"),
        keys, cols, numeric_columns=[c for c in a.numeric.split(",") if c], norm=norm, n_buckets=a.buckets,
    )
    print(rep.to_markdown())
    return 0 if rep.passed else 1


if __name__ == "__main__":
    sys.exit(main())
