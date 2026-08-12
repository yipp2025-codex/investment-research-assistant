# TWSE fixture provenance

- Source: official TWSE OpenAPI `STOCK_DAY_ALL` and `BWIBBU_ALL`.
- Retrieved: 2026-08-05 Asia/Taipei.
- Market date: ROC `1150804` / Gregorian `2026-08-04`.
- Scope: minimized public market records only; request headers and unrelated records removed.
- Included common stocks: 1101, 2317, 2330, 2454.
- Included exclusion control: 0050 appears only in the stock-day fixture and must be rejected by the Phase 3 common-stock intersection.

The full-response hashes and official schema evidence are recorded in `docs/twse-openapi-contract.md`. Normal pytest reads only these local fixtures and never requires internet access.
