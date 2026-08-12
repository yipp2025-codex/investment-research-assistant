# TWSE historical fixture provenance

- Official page: `https://www.twse.com.tw/zh/trading/historical/stock-day.html`
- Official JSON endpoint: `https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY`
- Retrieved: 2026-08-05 Asia/Taipei.
- Fixture: 2330 / 2026-04, complete 20-row official response.
- Full-response SHA-256: `A7785861CC83CB77842C908721DD8DCE229183079B2888C694AFD53C933ACA9D`.
- Fixture: 2317 / 2025-07, minimized to the official 07-29／07-30／07-31
  rows; `total` is adjusted to 3 and only the format note is retained.
- The complete 23-row source response SHA-256 is
  `7DC065673A8AC786DA4605A41C7E9BA07B2C63EE5DB8056BEA0F6B382F44D418`.
- The minimized local fixture SHA-256 is
  `005ECC0BDE1E4218BB31FCB75FB6DB5B43C16BF5C5379D9A262CFFA7483DDB47`.
- Its 07-30 row has zero volume／turnover／transactions and all OHLC values
  represented as `--`; this row is not converted to a zero-price observation.

Normal pytest uses this local fixture. Multi-month coverage and 60/120/250-window tests derive deterministic synthetic month batches separately and never require internet access.
