from pathlib import Path

import pytest

from contractkit import Severity, diff_contracts, load_contract, parse_contract, semver_violation, validate_records
from contractkit.cli import main
from contractkit.model import ContractError

C = Path(__file__).parents[1] / "contracts"
V1, V2 = C / "customer_rewards.v1.yaml", C / "customer_rewards.v2-proposed.yaml"


def _by_path(changes):
    return {c.path: c for c in changes}


def test_detects_the_silent_breaks():
    old, new = load_contract(V1), load_contract(V2)
    ch = _by_path(diff_contracts(old, new))
    assert ch["schema.account_id"].severity is Severity.BREAKING  # PII tag removed
    assert ch["schema.tier"].severity is Severity.BREAKING        # new enum value
    assert ch["schema.as_of_date"].severity is Severity.BREAKING  # became nullable
    assert ch["sla.freshness"].severity is Severity.BREAKING      # 60 -> 240 minutes
    assert ch["schema.points"].severity is Severity.NON_BREAKING  # int -> bigint widening
    assert ch["schema.points_value"].severity is Severity.NON_BREAKING  # decimal widened
    assert ch["schema.expiring_points"].severity is Severity.NON_BREAKING
    assert "MAJOR" in semver_violation(old, new, list(ch.values()))


def test_cli_exit_codes(capsys):
    assert main(["diff", str(V1), str(V2), "--format", "markdown"]) == 1
    assert "🔴 breaking" in capsys.readouterr().out
    assert main(["diff", str(V1), str(V1)]) == 0


def test_type_narrowing_is_breaking():
    base = {"id": "x", "owner": "o", "dataset": "d", "version": "1.0.0"}
    old = parse_contract({**base, "schema": [{"name": "a", "type": "decimal(10,2)"}]})
    new = parse_contract({**base, "version": "1.1.0", "schema": [{"name": "a", "type": "decimal(10,4)"}]})
    assert diff_contracts(old, new)[0].severity is Severity.BREAKING


def test_unbumped_change_rejected():
    base = {"id": "x", "owner": "o", "dataset": "d", "version": "1.0.0"}
    old = parse_contract({**base, "schema": [{"name": "a", "type": "int"}]})
    new = parse_contract({**base, "schema": [{"name": "a", "type": "int"}, {"name": "b", "type": "int"}]})
    assert "not bumped" in semver_violation(old, new, diff_contracts(old, new))


@pytest.mark.parametrize("bad", [
    {"id": "x", "owner": "o", "dataset": "d", "version": "v1", "schema": [{"name": "a", "type": "int"}]},
    {"id": "x", "owner": "o", "dataset": "d", "version": "1.0.0", "schema": [{"name": "a", "type": "uuid"}]},
    {"id": "x", "owner": "o", "dataset": "d", "version": "1.0.0",
     "schema": [{"name": "a", "type": "int"}, {"name": "a", "type": "int"}]},
])
def test_invalid_contracts(bad):
    with pytest.raises(ContractError):
        parse_contract(bad)


def test_validate_records():
    c = load_contract(V1)
    good = {"account_id": "a1", "tier": "gold", "points": 10, "points_value": "1.25", "as_of_date": "2026-11-01"}
    errs = validate_records(c, [
        good,
        {**good, "account_id": "a1"},                       # duplicate unique
        {**good, "account_id": "a2", "tier": "bronze"},     # enum
        {**good, "account_id": "a3", "points": None},       # not null
        {**good, "account_id": "a4", "points_value": "1.234"},  # scale overflow
        {**good, "account_id": "a5", "foo": 1},             # unexpected
    ])
    assert len(errs) == 5
    assert validate_records(c, [good]) == []


def test_codegen():
    c = load_contract(V1)
    ddl = c.to_ddl()
    assert "`account_id` STRING NOT NULL" in ddl and "'contract.version' = '1.4.0'" in ddl
    exps = {e["name"]: e for e in c.to_expectations()}
    assert exps["tier_in_enum"]["expr"] == "tier IN ('green', 'gold', 'platinum')"
