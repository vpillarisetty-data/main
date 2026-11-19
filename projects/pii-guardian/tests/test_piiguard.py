import json
import random
from pathlib import Path

import pytest

from piiguard import classify_column, classify_table, generate_policies
from piiguard.detectors import find_iban, find_pan, find_ssn, luhn_ok
from piiguard.policy import MASK_EXPR

SAMPLE = Path(__file__).parents[1] / "examples" / "customer_sample.jsonl"


@pytest.mark.parametrize("pan,ok", [
    ("4111 1111 1111 1111", True),        # Visa test PAN
    ("5555-5555-5555-4444", True),        # Mastercard
    ("378282246310005", True),            # Amex (15 digits)
    ("4111111111111112", False),          # Luhn fails
    ("1234567812345670", False),          # Luhn ok but no issuer
    ("0000000000000000", False),          # degenerate
    ("378282246310005000", False),        # Amex prefix, wrong length
])
def test_pan(pan, ok):
    assert find_pan(f"card {pan} on file") is ok


def test_luhn():
    assert luhn_ok("79927398713") and not luhn_ok("79927398710")


@pytest.mark.parametrize("ssn,ok", [
    ("123-45-6789", False),  # well-known fake
    ("078-05-1120", True),
    ("000-12-3456", False), ("666-12-3456", False), ("912-12-3456", False),
    ("123-00-4567", False), ("123-45-0000", False),
])
def test_ssn(ssn, ok):
    assert find_ssn(ssn) is ok


def test_iban():
    assert find_iban("GB82WEST12345698765432")
    assert not find_iban("GB82WEST12345698765433")


def test_random_ids_are_not_cards():
    random.seed(0)
    ids = [str(random.randint(10**15, 10**16 - 1)) for _ in range(500)]
    assert not classify_column("order_id", ids).is_pii


def test_name_hint_without_values():
    c = classify_column("customer_dob", ["1980-01-02", "1975-12-31"])
    assert c.category == "date_of_birth" and c.confidence >= 0.75


def test_embedded_pii_in_notes():
    notes = ["all good"] * 95 + ["card 4111 1111 1111 1111 read out"] * 5
    c = classify_column("agent_notes", notes)
    assert c.embedded and c.category == "credit_card" and c.sensitivity == "restricted"


def test_end_to_end_sample():
    rows = [json.loads(x) for x in SAMPLE.read_text().splitlines() if x]
    result = {c.column: c for c in classify_table(rows)}
    assert result["card_number"].category == "credit_card"
    assert result["tax_id"].category == "ssn"
    assert result["contact"].category == "email"
    assert result["mobile"].category == "phone"
    assert result["agent_notes"].embedded
    assert not result["order_id"].is_pii
    assert not result["segment"].is_pii
    assert not result["customer_ref"].is_pii

    sql = generate_policies("fin.silver.customers", result.values())
    assert "ALTER COLUMN `card_number` SET MASK governance.masks.mask_credit_card_restricted" in sql
    assert "is_account_group_member('pci_cardholder_data_readers')" in sql
    assert "mask_free_text_restricted" in sql and "'pii_embedded' = 'true'" in sql
    assert sql.count("CREATE OR REPLACE FUNCTION governance.masks.mask_credit_card_restricted") == 1
    assert "order_id" not in sql


def test_mask_expressions_execute_in_spark(spark):
    """Every mask body must be valid Spark SQL and actually hide the value."""
    cases = {
        "credit_card": ("4111-1111-1111-1111", "XXXX-XXXX-XXXX-1111"),
        "ssn": ("078-05-1120", "***-**-1120"),
        "email": ("jane.doe@example.com", "****@example.com"),
        "phone": ("+1 813-555-0199", "***-***-0199"),
        "date_of_birth": ("1980-04-23", "1980-**-**"),
        "free_text": ("pay 4111 1111 1111 1111 mail a@b.io ssn 078-05-1120", "pay [PAN] mail [EMAIL] ssn [SSN]"),
    }
    for key, (raw, expected) in cases.items():
        got = spark.sql(f"SELECT {MASK_EXPR[key]} AS m FROM (SELECT '{raw}' AS v)").first().m
        assert got == expected, key
