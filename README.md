# Investment Research Assistant

A read-only Python research pipeline for collecting market data, normalizing it into SQLite, producing reproducible research records, and calculating descriptive risk metrics.

This project is for research only. It does not place orders, submit brokerage instructions, automate trading, or produce buy/sell recommendations. Outputs require human review and are not investment advice.

## Data and safety boundaries

- Canonical TWSE market data is read only from the official `openapi.twse.com.tw` or `www.twse.com.tw` endpoints documented under `docs/`.
- `MockMarketDataProvider` always produces synthetic test data. Mock results must never be represented as real market observations.
- The optional E.SUN adapter is validation-only and requires separately obtained official SDK/configuration. It is not a substitute for canonical TWSE data.
- Secrets are loaded only from process environment variables or a local ignored `.env`. Never commit credentials, certificates, databases, reports, or runtime state.
- Provider failures retry only when classified as temporary or timeout failures. Invalid payloads, normalization failures, and permanent errors fail closed.
- Successful checkpoints are idempotent and must not be overwritten. Resumption requires the exact recorded run identifier.

## Requirements

- Python 3.11 or newer
- `tzdata` (installed with the package)
- `pytest` for development tests

## Setup

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"
Copy-Item .env.example .env
```

The example configuration stores the local SQLite database under `data/`. Replace only local values in `.env`; the file is ignored by Git.

## Safe local example

This command uses synthetic data and does not contact a market-data service:

```powershell
.\.venv\Scripts\python.exe -m app --provider mock --symbol MOCK1
```

To inspect available batch, report, live-smoke, and scheduler commands, run each script with `--help`. Live smoke scripts are intentionally separate from the offline test suite because network availability must not be a test prerequisite.

## Verification

```powershell
.\.venv\Scripts\python.exe -m pytest
.\.venv\Scripts\python.exe -m app --help
```

The pytest suite uses local fixtures and synthetic inputs. Any command prefixed with `live_` can perform network requests and should be run explicitly.

## Repository layout

- `app/`: providers, pipelines, analysis, reports, SQLite storage, screener, and generic scheduler components
- `scripts/`: explicit batch, report, live-smoke, provider-probe, and scheduler entry points
- `tests/`: offline tests and minimal fixtures
- `benchmarks/`: reproducible benchmark programs; machine-specific result snapshots are not included
- `docs/`: provider contract notes for official data boundaries

## Publication status

No license has been selected. Do not assume permission to copy, modify, or redistribute this code until a license is added by the maintainers.
