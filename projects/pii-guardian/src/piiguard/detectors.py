"""Value-level PII detectors.

Regex alone produces a lot of false positives (every 16-digit id looks like a card number, every
9-digit id looks like an SSN), so each detector here adds structural validation:
Luhn checksum + issuer prefix for PANs, SSA allocation rules for SSNs, ISO 7064 mod-97 for IBANs.
"""

from __future__ import annotations

import ipaddress
import re
from collections.abc import Callable
from dataclasses import dataclass

_EMAIL = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
_PHONE = re.compile(r"(?:\+?1[\s.-]?)?\(?\b[2-9]\d{2}\)?[\s.-]?\d{3}[\s.-]?\d{4}\b|\+[1-9]\d{7,14}\b")
_SSN = re.compile(r"\b(\d{3})-?(\d{2})-?(\d{4})\b")
_PAN = re.compile(r"\b(?:\d[ -]?){12,18}\d\b")
_IBAN = re.compile(r"\b[A-Z]{2}\d{2}[A-Z0-9]{11,30}\b")
_IPV4 = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b")


def luhn_ok(digits: str) -> bool:
    total, alt = 0, False
    for ch in reversed(digits):
        d = int(ch)
        if alt:
            d = d * 2 - 9 if d > 4 else d * 2
        total += d
        alt = not alt
    return total % 10 == 0


def _pan_issuer_ok(d: str) -> bool:
    if d[0] == "4":
        return len(d) in (13, 16, 19)  # Visa
    two, four = int(d[:2]), int(d[:4])
    if 51 <= two <= 55 or 2221 <= four <= 2720:
        return len(d) == 16  # Mastercard
    if two in (34, 37):
        return len(d) == 15  # Amex
    if d.startswith("6011") or d.startswith("65") or 644 <= int(d[:3]) <= 649:
        return len(d) in (16, 19)  # Discover
    return 3528 <= four <= 3589 and 16 <= len(d) <= 19  # JCB


def find_pan(text: str) -> bool:
    for m in _PAN.finditer(text):
        d = re.sub(r"[ -]", "", m.group())
        if 13 <= len(d) <= 19 and len(set(d)) > 1 and _pan_issuer_ok(d) and luhn_ok(d):
            return True
    return False


_IMEI = re.compile(r"(?<!\d)\d{15}(?!\d)")


def find_imei(text: str) -> bool:
    """15-digit device id with a Luhn check digit (3GPP TS 23.003) that is not a card number."""
    return any(luhn_ok(m) and len(set(m)) > 1 and not _pan_issuer_ok(m) for m in _IMEI.findall(text))


def find_ssn(text: str) -> bool:
    for m in _SSN.finditer(text):
        area, group, serial = m.groups()
        if area in ("000", "666") or area[0] == "9" or group == "00" or serial == "0000":
            continue
        if len(set(area + group + serial)) == 1 or area + group + serial == "123456789":
            continue
        return True
    return False


def find_iban(text: str) -> bool:
    for m in _IBAN.finditer(text):
        s = m.group()
        rearranged = s[4:] + s[:4]
        num = "".join(str(int(c, 36)) for c in rearranged)
        if int(num) % 97 == 1:
            return True
    return False


def find_ipv4(text: str) -> bool:
    for m in _IPV4.finditer(text):
        try:
            ip = ipaddress.IPv4Address(m.group())
        except ValueError:
            continue
        if not (ip.is_loopback or ip.is_unspecified):
            return True
    return False


@dataclass(frozen=True)
class Detector:
    category: str
    sensitivity: str  # restricted | confidential
    match: Callable[[str], bool]


DETECTORS: tuple[Detector, ...] = (
    Detector("credit_card", "restricted", find_pan),
    Detector("ssn", "restricted", find_ssn),
    Detector("iban", "restricted", find_iban),
    Detector("device_id", "confidential", find_imei),
    Detector("email", "confidential", lambda s: bool(_EMAIL.search(s))),
    Detector("phone", "confidential", lambda s: bool(_PHONE.search(s))),
    Detector("ip_address", "confidential", find_ipv4),
)

# Column-name hints for PII that has no reliable value pattern (names, DOB, addresses).
NAME_HINTS: tuple[tuple[re.Pattern[str], str, str], ...] = tuple(
    (re.compile(p, re.I), cat, sens)
    for p, cat, sens in (
        (r"(^|_)(ssn|social_sec|tax_id|tin)($|_)", "ssn", "restricted"),
        (r"(card_?num|^pan$|_pan$|cc_?num|primary_account)", "credit_card", "restricted"),
        (r"(cvv|cvc|card_?verif)", "card_security_code", "restricted"),
        (r"(e_?mail)", "email", "confidential"),
        (r"(phone|mobile|msisdn|cell_?(phone|num|number|no)\b)", "phone", "confidential"),  # not cell_id (towers)
        (r"(^|_)(dob|birth_?date|date_of_birth)($|_)", "date_of_birth", "confidential"),
        (r"(first_?name|last_?name|full_?name|surname|given_name)", "person_name", "confidential"),
        (r"(street|address_line|addr_?1|postal_address|home_address)", "address", "confidential"),
        (r"(passport|driver_?lic|national_id)", "government_id", "restricted"),
        (r"(ip_?addr|client_ip|remote_addr)", "ip_address", "confidential"),
        (r"(^|_)(msisdn|a_number|b_number|calling_number|called_number)($|_)", "phone", "confidential"),
        (r"(^|_)(imsi|imei|iccid|device_id)($|_)", "device_id", "confidential"),
        (r"(loyalty_?(number|id|no)|member_?number)", "loyalty_id", "confidential"),
    )
)
