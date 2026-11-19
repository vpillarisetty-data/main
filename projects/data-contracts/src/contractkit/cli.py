"""``python -m contractkit.cli <command>``

  diff OLD NEW         classify changes; exit 1 on a semver violation
  ddl CONTRACT         print Delta CREATE TABLE
  expectations CONTRACT  print medallion expectations YAML
  validate CONTRACT DATA.jsonl
"""

from __future__ import annotations

import argparse
import json
import sys

import yaml

from contractkit.diff import Severity, diff_contracts, semver_violation
from contractkit.model import load_contract
from contractkit.validate import validate_records


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="contractkit")
    sub = ap.add_subparsers(dest="cmd", required=True)
    d = sub.add_parser("diff")
    d.add_argument("old")
    d.add_argument("new")
    d.add_argument("--format", choices=["text", "markdown"], default="text")
    sub.add_parser("ddl").add_argument("contract")
    sub.add_parser("expectations").add_argument("contract")
    v = sub.add_parser("validate")
    v.add_argument("contract")
    v.add_argument("data")
    a = ap.parse_args(argv)

    if a.cmd == "diff":
        old, new = load_contract(a.old), load_contract(a.new)
        changes = diff_contracts(old, new)
        err = semver_violation(old, new, changes)
        if a.format == "markdown":
            print(f"### Data contract `{new.id}` {old.version_str} → {new.version_str}\n")
            print("| Severity | Path | Change |\n|---|---|---|")
            for c in changes:
                sev = "🔴 breaking" if c.severity is Severity.BREAKING else "🟢 non-breaking"
                print(f"| {sev} | `{c.path}` | {c.message} |")
            if err:
                print(f"\n> ❌ **{err}**")
        else:
            for c in changes:
                print(c)
            print(f"ERROR: {err}" if err else "OK: version bump is consistent")
        return 1 if err else 0
    if a.cmd == "ddl":
        print(load_contract(a.contract).to_ddl())
        return 0
    if a.cmd == "expectations":
        print(yaml.safe_dump({"expectations": load_contract(a.contract).to_expectations()}, sort_keys=False))
        return 0
    with open(a.data, encoding="utf-8") as fh:
        errs = validate_records(load_contract(a.contract), (json.loads(line) for line in fh if line.strip()))
    for e in errs:
        print(e)
    return 1 if errs else 0


if __name__ == "__main__":
    sys.exit(main())
