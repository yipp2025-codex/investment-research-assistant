# v1.1 Dual-Source Research Contract

The v1.1 contract adds bounded resilience to the read-only research pipeline
while preserving TWSE authority and the existing TWSE-only research behavior.

## Authority and roles

- TWSE is the canonical market-data source for selected research observations.
  Canonical TWSE values remain authoritative.
- E.SUN is a supported secondary provider with two explicitly different roles:
  validation and qualified supplemental coverage.
- In the validation role, E.SUN observations are compared or recorded as
  independent evidence. They do not select a winning source and do not rewrite
  canonical TWSE-derived research data.
- In the qualified supplemental role, E.SUN may cover a bounded TWSE gap only
  after the provider identity, normalized observation, provenance, security,
  and eligibility checks pass. Supplemental observations are marked as such
  and never become canonical.
- E.SUN cannot replace an existing canonical TWSE observation, and a TWSE
  failure by itself does not authorize substitution.

## Normal and recovery paths

1. A complete TWSE history follows the normal canonical path and produces a
   TWSE-derived canonical research dataset.
2. A complete TWSE result may be accompanied by E.SUN validation. The
   validation path does not change the canonical dataset or choose a provider
   winner.
3. If TWSE has a gap in a category eligible for supplemental recovery, the
   explicitly gated recovery path may create a bounded provisional dataset
   containing the available canonical TWSE observations and clearly marked
   supplemental observations from E.SUN.
4. If any required safety or eligibility check fails, the recovery path fails
   closed. It does not silently fall back, return E.SUN as canonical, or
   replace an existing TWSE value.

Supplemental recovery is therefore conditional, not automatic. The current
public build documents E.SUN as the supported secondary adapter; this document
does not imply support for another provider or guarantee account access,
permissions, or production activation.

## Dataset lifecycle

- A canonical dataset contains TWSE-derived canonical research observations.
- A provisional dataset is a bounded research result created only by the
  qualified supplemental path. Its mixed provenance and provisional status
  remain explicit for downstream consumers.
- When later TWSE observations resolve a provisional gap, reconciliation creates
  a new immutable child dataset. The provisional parent and its report identity
  are not rewritten in place.
- Reconciliation preserves the canonical role of TWSE. It does not turn an
  E.SUN observation into a canonical value merely because the datasets are
  being compared.

## Failure and safety behavior

Provider identity mismatch, malformed or incomplete data, unsupported
credentials or permissions, normalization failure, and non-eligible provider
errors fail closed. Only explicitly documented recoverable provider-failure
cases may enter the bounded supplemental path, and they remain subject to the
same identity, provenance, security, and eligibility checks.

The project remains read-only and research-only. It does not place orders,
submit brokerage instructions, automate trading, or provide investment advice.

All examples and offline regression fixtures are local and deterministic. The
public repository does not include production databases, market-data dumps,
credentials, scheduler state, or runtime reports.
