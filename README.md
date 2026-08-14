# Investment Research Assistant

A deterministic, read-only Python pipeline for official TWSE market data,
reproducible research records, and descriptive risk analysis.

This project is for research only. It does not place orders, submit brokerage
instructions, automate trading, provide buy/sell guarantees, or constitute
investment advice.

## v1.1 highlights

- Dual-source resilience while preserving TWSE as the canonical authority.
- Pluggable secondary market-data providers may participate in validation and
  qualified supplemental coverage under strict identity, provenance,
  eligibility, security, and fail-closed contracts.
- E.SUN is one currently supported secondary-provider adapter; its observations
  are qualified supplemental or validation evidence only. It is not part of
  the canonical authority contract.
- Per-observation provider role, source-run, artifact, and provenance hashes.
- Immutable provisional datasets for temporary TWSE gaps and immutable
  reconciled child datasets when TWSE observations arrive later.
- Legal-short listing-history coverage derived from authoritative listing
  evidence rather than a hard-coded observation count.
- Additive SQLite schema v12 support through migration `0012`; legacy v1
  semantics and strict replay remain separate and compatible.
- Deterministic local fixtures and bounded provider protections for redirects,
  credentials, response size, deadlines, and retries.

## Data and safety boundaries

- TWSE market data is read only from the official endpoints documented under
  `docs/`.
- TWSE plus a qualified secondary provider is the dual-source resilience
  pattern; a secondary provider cannot replace TWSE canonical observations.
- E.SUN is the currently supported adapter implementation for that secondary
  role, not a permanent architectural requirement.
- `MockMarketDataProvider` produces synthetic test data and is never presented
  as real market data.
- Credentials are loaded only from process configuration or an ignored local
  `.env`; credentials, databases, reports, backups, and runtime state do not
  belong in Git.
- Invalid payloads, identity mismatches, normalization failures, and permanent
  provider errors fail closed.

## Quick start

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"
.\.venv\Scripts\python.exe -m pytest
.\.venv\Scripts\python.exe -m app --help
```

The offline suite uses deterministic local fixtures. Network live-smoke
commands are explicit and are not required for the normal test run.

## Documentation

- [`docs/dual-source-resilience.md`](docs/dual-source-resilience.md) — public
  v1.1 contract overview.
- [`docs/twse-openapi-contract.md`](docs/twse-openapi-contract.md) — official
  TWSE endpoint boundary.
- [`docs/esun-marketdata-contract.md`](docs/esun-marketdata-contract.md) —
  E.SUN adapter-specific source boundary.

## License

This project is available under the [MIT License](LICENSE).
