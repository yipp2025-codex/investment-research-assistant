# Investment Research Assistant

A deterministic, read-only Python pipeline for official TWSE market data,
reproducible research records, and descriptive risk analysis.

This project is for research only. It does not place orders, submit brokerage
instructions, automate trading, provide buy/sell guarantees, or constitute
investment advice.

## v1.1 highlights

- TWSE remains the canonical authority for market-data observations used by
  research output.
- The current build supports E.SUN as a secondary provider for validation and
  bounded supplemental coverage under explicit identity, provenance,
  eligibility, security, and fail-closed checks.
- E.SUN never becomes canonical, and its data cannot replace or overwrite an
  existing canonical TWSE observation.
- There is no implicit or automatic provider fallback. Supplemental recovery
  is available only when the documented eligibility conditions are satisfied;
  otherwise the pipeline fails closed.
- Provisional datasets make qualified supplemental coverage explicit, and a
  later reconciliation creates a new immutable child without rewriting its
  provisional parent.
- Legal-short listing-history coverage is derived from authoritative listing
  evidence rather than a hard-coded observation count.
- Deterministic local fixtures and bounded provider protections cover
  redirects, credentials, response size, deadlines, and retries.

## Data and safety boundaries

- TWSE market data is read only from the official endpoints documented under
  `docs/`.
- A complete TWSE path produces canonical research data. E.SUN may validate
  that data, but validation does not select a winning provider or rewrite
  canonical values.
- A qualified supplemental path may cover an eligible TWSE gap in a clearly
  marked provisional research dataset. It is not an unrestricted substitute
  for TWSE and cannot make E.SUN canonical.
- E.SUN is the currently supported secondary adapter for this contract; its
  presence does not make it a permanent architectural requirement.
- `MockMarketDataProvider` produces synthetic test data and is never presented
  as real market data.
- Credentials are loaded only from process configuration or an ignored local
  `.env`; credentials, databases, reports, backups, and runtime state do not
  belong in Git.
- Invalid payloads, identity mismatches, normalization failures, and
  ineligible or permanent provider errors fail closed.

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
  v1.1 dual-source contract and dataset lifecycle.
- [`docs/twse-openapi-contract.md`](docs/twse-openapi-contract.md) — official
  TWSE endpoint boundary.
- [`docs/esun-marketdata-contract.md`](docs/esun-marketdata-contract.md) —
  E.SUN adapter-specific source boundary.

## License

This project is available under the [MIT License](LICENSE).
