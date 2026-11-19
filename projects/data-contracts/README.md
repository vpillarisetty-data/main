# contractkit: data contracts with breaking-change detection in CI

## The problem

Most data incidents aren't pipeline crashes. They're **upstream changes that look harmless**:

* a new enum value (`centurion` tier) that every downstream `CASE WHEN` silently maps to `NULL`
* a column that "can be null now, rarely" and breaks a join or a `NOT NULL` dashboard metric
* someone removes a `pii: true` tag, the masking policy stops applying, and nobody notices until audit
* the freshness SLA quietly goes from 1h to 4h

Schema-registry compatibility checks only look at **types**. Data contracts need to cover **semantics, governance,
and SLAs** too, and the check needs to run in the PR before the change merges.

## What's here

```
contracts/*.yaml ──► contractkit diff (CI gate) ──► PR comment table, merge blocked on a semver mismatch
                 ──► contractkit ddl          ──► Delta CREATE TABLE with contract version in TBLPROPERTIES
                 ──► contractkit expectations ──► DQ rules consumed by ../medallion-framework
                 ──► contractkit validate     ──► producer-side check on a sample before publishing
```

The rules are written from the **consumer's** point of view:

| Change | Severity | Why |
|---|---|---|
| field removed / renamed | 🔴 | queries fail |
| `int → bigint`, `float → double`, `decimal(p,s) → decimal(p+n,s)` | 🟢 | safe widening |
| any narrowing or scale change | 🔴 | truncation / overflow |
| `NOT NULL → nullable` | 🔴 | consumers receive NULLs they never handled |
| new enum value | 🔴 | `CASE` / `IN` logic misses it |
| `unique` dropped | 🔴 | joins fan out and double-count |
| `pii` tag removed | 🔴 | masking and row filters stop applying |
| SLA loosened | 🔴 | downstream schedules break |
| field added, SLA tightened, enum value retired | 🟢 | |

Breaking changes **require a MAJOR version bump**, and any change requires some bump.

## Try it

```bash
python -m contractkit.cli diff contracts/customer_rewards.v1.yaml contracts/customer_rewards.v2-proposed.yaml
```
```
✖ [breaking] schema.account_id: PII classification removed: masking policies will stop applying
✖ [breaking] schema.as_of_date: became nullable: consumers may receive NULLs
✖ [breaking] schema.tier: new enum values ['centurion']: CASE/IN logic downstream may miss them
✖ [breaking] sla.freshness: 60 -> 240
• [non-breaking] schema.expiring_points: field added (int)
• [non-breaking] schema.points: type int -> bigint
• [non-breaking] schema.points_value: type decimal(12,2) -> decimal(14,2)
ERROR: breaking changes require a MAJOR version bump (1.4.0 -> 1.5.0); publish as 2.0.0 ...
```

To use the CI gate, copy [`ci/contract-check.yml`](ci/contract-check.yml) into `.github/workflows/`.
