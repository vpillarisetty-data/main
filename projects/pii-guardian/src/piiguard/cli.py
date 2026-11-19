"""``python -m piiguard.cli sample.jsonl --table cat.schema.table [--json]``"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict

from piiguard.classify import classify_table
from piiguard.policy import PolicyConfig, generate_policies


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="piiguard")
    ap.add_argument("sample", help="JSON-lines sample of the table")
    ap.add_argument("--table", required=True)
    ap.add_argument("--json", action="store_true", help="print the classification report instead of SQL")
    ap.add_argument("--min-confidence", type=float, default=0.7)
    a = ap.parse_args(argv)
    with open(a.sample, encoding="utf-8") as fh:
        rows = [json.loads(line) for line in fh if line.strip()]
    result = classify_table(rows)
    if a.json:
        print(json.dumps([asdict(c) for c in result], indent=2))
    else:
        print(generate_policies(a.table, result, PolicyConfig(min_confidence=a.min_confidence)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
