# PII Guardian: find PII in your lakehouse and generate Unity Catalog masking policies as code

## The problem

PCI-DSS, SOX, GDPR, and CCPA audits all ask the same question: *where is the sensitive data, and who can see it?*
In practice:

* PII hides in columns with innocent names (`contact`, `ref2`, `agent_notes`)
* **free-text columns leak card numbers and emails** that agents typed in, and nobody tags those
* regex-only scanners flag every 16-digit order id as a credit card, so the team stops trusting the scanner
* masking is applied by hand, inconsistently, and the next table someone builds has none

## What it does

```
sample rows ─► detectors (regex + structural validation) ─► classifier (value evidence + name hints)
            ─► confidence-scored report ─► Unity Catalog SQL: tags + masking functions + SET MASK
                                        └► low-confidence columns go to a human-review list instead
```

**Precise detectors.** Each regex match is validated before it counts:

| Category | Validation |
|---|---|
| Card PAN | Luhn checksum **and** issuer prefix/length (Visa, MC incl. 2-series, Amex 15, Discover, JCB) |
| SSN | SSA rules: no `000`/`666`/`9xx` area, `00` group, `0000` serial, well-known fakes |
| IBAN | ISO 7064 mod-97 |
| Email / phone / IPv4 | RFC-style patterns, loopback excluded |
| DOB, names, address, CVV, passport | column-name hints |

**Free-text leak detection.** In a genuinely textual column, even 2% of rows containing a *validated* PAN is flagged as
`pii_embedded` and gets a redaction mask (`[PAN]`, `[SSN]`, `[EMAIL]`) instead of a whole-value mask.

**Policy as code.** It generates one masking function per category and sensitivity, reused across tables, and binds them with
`ALTER COLUMN ... SET MASK`. Restricted data (PAN, SSN) is gated on a separate PCI group. Masks keep data
useful where that's safe: PAN last-4 (as PCI-DSS permits), email domain, DOB year, and deterministic SHA-256
tokens for everything else so joins still work.

```bash
python -m piiguard.cli examples/customer_sample.jsonl --table fin.silver.customers > policies.sql
```
```sql
-- card_number: credit_card [restricted] confidence=1.0, 100% of 200 sampled values match credit_card
ALTER TABLE fin.silver.customers ALTER COLUMN `card_number` SET TAGS ('pii' = 'credit_card', 'sensitivity' = 'restricted');
ALTER TABLE fin.silver.customers ALTER COLUMN `card_number` SET MASK governance.masks.mask_credit_card_restricted;
-- agent_notes: credit_card [restricted] confidence=0.8, free text contains credit_card in 5% of rows
ALTER TABLE fin.silver.customers ALTER COLUMN `agent_notes` SET TAGS (..., 'pii_embedded' = 'true');
```
In the example sample, `order_id` (random 16-digit numbers) is correctly **not** flagged.

## Tests

21 tests: detector edge cases (test PANs, fake SSNs, bad IBAN checksums), false-positive resistance on 500 random
16-digit ids, embedded-leak detection, end-to-end SQL generation, and **every mask expression executed in Spark SQL**
to prove it is valid and actually hides the value.
