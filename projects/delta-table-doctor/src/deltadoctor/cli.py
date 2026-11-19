"""``python -m deltadoctor.cli <table_path> [<table_path> ...] [--json]``"""

from __future__ import annotations

import argparse
import json
import sys

from deltadoctor.diagnose import Config, Report, diagnose
from deltadoctor.log import load_snapshot

_ICON = {"critical": "🔴", "warning": "🟠", "info": "🔵"}


def render(r: Report) -> str:
    s = r.summary
    lines = [
        f"Delta Table Doctor: {r.table} @ v{r.version}",
        f"Health score: {r.health_score}/100   files={s['files']:,}  size={s['total_human']}  "
        f"avg file={s['avg_file_human']}  partitions={s['partition_columns'] or '-'}  "
        f"clustering={s['clustering_columns'] or '-'}",
        "",
    ]
    if not r.findings:
        lines.append("✅ No issues found.")
    for f in r.findings:
        save = f"   (~${f.est_monthly_savings_usd:,.2f}/month)" if f.est_monthly_savings_usd else ""
        lines += [f"{_ICON[f.severity]} {f.title}{save}", f"   {f.detail}"]
        if f.fix:
            lines += ["   fix:"] + [f"     {x}" for x in f.fix.splitlines()]
        lines.append("")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="deltadoctor")
    ap.add_argument("tables", nargs="+")
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--fail-under", type=int, default=0, help="exit 1 if any score is below this")
    a = ap.parse_args(argv)
    reports = [diagnose(load_snapshot(t), Config()) for t in a.tables]
    if a.json:
        print(json.dumps([r.to_dict() for r in reports], indent=2, default=str))
    else:
        print("\n".join(render(r) for r in reports))
    return 1 if any(r.health_score < a.fail_under for r in reports) else 0


if __name__ == "__main__":
    sys.exit(main())
