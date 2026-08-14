# Changelog

## Unreleased

- Clarified that dual-source resilience means TWSE canonical authority plus a
  qualified, pluggable secondary market-data provider; E.SUN remains one
  supported adapter rather than a permanent architectural requirement.

## 1.1.0 — 2026-08-13

- Added resilient dual-source research datasets with TWSE canonical authority
  and qualified E.SUN supplemental observations.
- Added immutable per-observation provenance, provisional datasets, and
  reconciled child datasets.
- Added legal-short listing-history coverage derived from authoritative listing
  evidence.
- Added additive schema v12 support through migration `0012` without rewriting
  legacy v1 data or replay identities.
- Added bounded redirect, credential, response-size, deadline, retry, identity,
  and artifact-safety protections with deterministic regression coverage.
- Kept the project research-only, read-only, and free of order execution or
  automated trading.

## 1.0.0

- Initial MIT-licensed public release of the deterministic TWSE research
  pipeline, offline fixtures, SQLite persistence, replay, reports, and risk
  analysis.
