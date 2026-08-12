# TWSE historical monthly contract evidence

初次驗證日期：2026-08-05（Asia/Taipei）
契約增補日期：2026-08-10（Asia/Taipei）

## 官方來源

- 官方頁面：<https://www.twse.com.tw/zh/trading/historical/stock-day.html>
- 頁面宣告：`data-api="/afterTrading/STOCK_DAY"`
- 官方 JSON endpoint：`GET https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY`
- Query：`response=json&date=YYYYMMDD&stockNo=NNNN`
- 官方頁面 SHA-256：`93055D68B32E68AFF2C7AF9C0857AEB5443C9394D6C102AA67CD9A441C8D0E10`

此 endpoint 不在 TWSE OpenAPI Swagger 中，因此契約證據是「官方頁面 data-api 宣告＋官方 live monthly response」。不得把 Phase 3 `STOCK_DAY_ALL` 的 OpenAPI schema 套用到本 endpoint。

## Live response envelope

成功 response 必須包含：

- `stat`：string，已驗證成功值為 `OK`。
- `date`：string，回傳 query month 的 Gregorian `YYYYMMDD`。
- `title`：string，例如 `115年04月 2330 台積電 各日成交資訊`。
- `fields`：固定 10 欄的 string array。
- `data`：row array；每列固定 10 個 string。
- `notes`：string array。
- `total`：integer，必須等於 `data` 長度。

沒有資料時，官方 response 只保證 `stat` 與 `total=0`，本階段視為該月份不可提供資料，不建立空的 canonical batch。

固定欄位順序：

1. 日期
2. 成交股數
3. 成交金額
4. 開盤價
5. 最高價
6. 最低價
7. 收盤價
8. 漲跌價差
9. 成交筆數
10. 註記

## Verified formats

- Row date：ROC `YYY/MM/DD`，轉為 Gregorian ISO date。
- 成交股數、成交金額、成交筆數：含千分位逗號的非負整數字串。
- OHLC：一般交易列為含千分位逗號、兩位小數的價格字串。
- 已實證無價格列：四個 OHLC 皆為 `--`。此列不形成 canonical `DailyPrice`；不得把 `--` 映射成價格 0，也不得 forward-fill。成交欄可以全為 `0`，也可能只有零股成交而為非零；TWSE `STOCK_DAY` 報表含零股成交，但零股成交價依法不決定當日開盤、最高、最低、收盤價。成交股數、成交金額、成交筆數仍須通過非負整數格式驗證。部分 OHLC 為 `--` 一律 fail closed。
- 漲跌價差：可有 `+`、`-`、前置空白；官方 notes 另允許 `X` 表示不比價。本欄只驗證 string，不映射至 canonical model。
- 註記：string；Phase 4 保存 OHLCV，不將註記推導為交易訊號。

## Evidence snapshots

- 2330 / 2026-04：20 rows，SHA-256 `A7785861CC83CB77842C908721DD8DCE229183079B2888C694AFD53C933ACA9D`。
- 2330 / 2026-05：20 rows，SHA-256 `ABAF0063BEE1513FC2B9285947CE90DD6165B745F91BE80F4AE026B242663EBD`。
- 2330 / 2026-06：21 rows，SHA-256 `1F3D24C2D49921D29A723DE582EF88F578C0B26FE69FB2F0866720D58341B813`。
- 2317 / 2025-07：23 rows，SHA-256 `7DC065673A8AC786DA4605A41C7E9BA07B2C63EE5DB8056BEA0F6B382F44D418`；07-30 為零成交且 OHLC 全 `--`，07-31 漲跌價差為 `X0.00`。
- 1213 / 2026-07：22 rows；07-16（1 股／7 元／1 筆）與 07-17（2 股／14 元／1 筆）為 OHLC 全 `--` 的零股活動列。`1213` 是股票代號，不是 HTTP status、TWSE business code 或 provider error code。

## Redirect 與 pacing 邊界

- transport 不自動 follow redirect；provider 只允許一次 `307`／`308`，且 target 必須是官方 TWSE HTTPS authority、相同 endpoint path、相同 query 語意。
- 同址 redirect、帶 `Retry-After` 的 redirect、或第二次 redirect 視為 temporary failure，由 pipeline 的 bounded retry 處理。
- 官方 endpoint 在批次節奏門檻後可回傳 `HTTP 307`、`text/html`、無 `Location`／`Retry-After`；這不是可安全 follow 的正常 redirect，而是 temporary pacing／edge response。provider 必須保留原 method/query、不得猜 target，並交給 bounded retry 與批次 circuit breaker。
- 第三方 authority、path 或 query 改寫立即 fail closed。
- `429`／`5xx` 可 bounded retry；官方 `Retry-After` 大於本地 backoff 時必須遵守。pacing 只影響操作時間，不進入 canonical identity。

## Provider boundary

- `TwseMarketDataProvider` 保持 Phase 3 最新快照用途，不修改其 endpoint 或語意。
- `TwseHistoricalMarketDataProvider` 一次只讀取一個股票月份，由 sync pipeline 控制月份 checkpoint、retry 與 resume。
- 歷史 sync 只接受已由 Phase 3 登記為 `market=TWSE` 的上市普通股。
- Provider 不做 retry、SQLite、分析或摘要。
