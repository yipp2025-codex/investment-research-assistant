# Changelog

## Unreleased

- Clarified the public dual-source contract: TWSE remains canonical; E.SUN
  may validate or provide bounded supplemental coverage only after explicit
  eligibility checks, with no implicit fallback or canonical overwrite.

## 1.1.0 — 2026-08-13

- Added resilient dual-source research datasets with TWSE canonical authority
  and qualified E.SUN supplemental observations when the documented recovery
  conditions are satisfied.
- Added immutable per-observation provenance, provisional datasets, and
  reconciled child datasets.
- Added legal-short listing-history coverage derived from authoritative listing
  evidence.
- Added bounded redirect, credential, response-size, deadline, retry, identity,
  and artifact-safety protections with deterministic regression coverage.
- Kept the project research-only, read-only, and free of order execution or
  automated trading.

## 1.0.0

- Initial MIT-licensed public release of the deterministic TWSE research
  pipeline, offline fixtures, SQLite persistence, replay, reports, and risk
  analysis.
