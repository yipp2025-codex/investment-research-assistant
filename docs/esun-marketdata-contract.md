# E.SUN market-data contract evidence

驗證日期：2026-08-06（Asia/Taipei）

## 官方來源與範圍

- 事前準備：<https://www.esunsec.com.tw/trading-platforms/api-trading/docs/prerequisites/>
- 行情 API 簡介：<https://www.esunsec.com.tw/trading-platforms/api-trading/docs/market-data/intro/>
- HTTP API 開始使用：<https://www.esunsec.com.tw/trading-platforms/api-trading/docs/market-data/http-api/getting-started/>
- 方案權限：<https://www.esunsec.com.tw/trading-platforms/api-trading/docs/faq/price_and_plan/>
- 速率限制：<https://www.esunsec.com.tw/trading-platforms/api-trading/docs/market-data/rate-limit/>
- SDK 下載：<https://www.esunsec.com.tw/trading-platforms/api-trading/docs/download/download-sdk/>
- SDK 遷移指南：<https://www.esunsec.com.tw/trading-platforms/api-trading/docs/market-data/migration_guide/>

本階段只使用官方 `esun_marketdata` 證券行情功能。不得 import `esun_trade`，不得呼叫委託、刪改單、帳務或任何交易方法。官方明確說明行情 API 不區分正式／模擬環境，因此交易 API 的模擬下單流程不是行情串接的前置條件。

## 架構角色

本文件描述目前支援的 E.SUN adapter implementation，不是 canonical authority
契約本身：

- TWSE 保持 canonical authority。
- Secondary market-data provider 是可替換的 validation／qualified supplemental
  角色；E.SUN 是目前其中一個支援的 adapter。
- 任一未來 adapter 都必須通過 identity verification、normalized observation
  contract、supplemental eligibility、provenance、security boundary 與
  fail-closed 語意，才能加入相同角色。
- 本 public build 的 provider identity allowlist 仍是顯式且有限的；新增 adapter
  需要另行修改實作、契約與 regression tests，不會因文件描述而自動開放。

## 官方 SDK 與驗證契約

- Windows wheel：`esun_marketdata-2.2.0-cp37-abi3-win_amd64.whl`。
- 官方下載檔 SHA-256：`57616A2AB7BA94172B52AEAB33036FF6266F046F0D721A32C760171F8B5C820C`。
- Python distribution metadata：`2.2.0`；專案對其他版本 fail closed。
- Python 入口：`from esun_marketdata import EsunMarketdata`。
- 初始化：`EsunMarketdata(ConfigParser)` → `login()` → `sdk.rest_client.stock`。

設定檔必須有非空白的 `[Core] Entry`、`[Core] Environment`、`[Cert] Path`、`[Api] Key`、`[Api] Secret`、`[User] Account`；憑證路徑必須是可讀取的 `.p12`。帳號密碼與憑證密碼由官方 SDK 寫入 Windows Credential Manager，專案不接收、不讀出也不記錄明文。

專案只由 `ESUN_MARKETDATA_CONFIG_PATH` 或命令列參數接收「被 Git 忽略且位於 repo 外的設定檔路徑」。設定內容、帳號、Key、Secret、憑證路徑、SDK token 與密碼不得進入程式、fixture、SQLite、文件或 log。

官方 SDK 登入後回傳的 REST origin 在本次驗證為 `https://api.fugle.tw/marketdata/v1.0/stock`。玉山官方行情簡介亦說明資料服務技術來源；本 E.SUN adapter 只接受由已驗證 SDK 動態取得、且 host/path 完全符合本契約的 HTTPS URL，不把它當成可任意替換的 endpoint。SDK 只負責官方登入／token exchange；每個市場資料 GET 由 transport 加入 timeout，且只送出一次。

## Live fixture 證據

2026-08-06 以核准帳號對 2330、2317、2454 執行唯讀 live probe。最小化 fixture 位於 `tests/fixtures/esun/`，只保留 mapping／契約測試必要欄位；完整 response 只保留 canonical SHA-256，不保存秘密。各 hash、擷取範圍與去識別規則見該目錄 `README.md`。

已確認的 live 差異：

- `historical/candles` 頂層包含 `sort`，沒有文件範例可能讓人誤認的頂層 `date`；row 自帶 Gregorian `yyyy-MM-dd` 日期。
- live `sort=desc`，Provider 不依賴來源順序，驗證日期唯一後改成升冪 canonical 順序。
- 2330 quote 的 `lastUpdated=1785994200000000`；以微秒 epoch 解讀後為 `2026-08-06T13:30:00+08:00`，與 payload date 及收盤狀態一致。本轉換由 live 證據與測試固定；raw integer 同時保留。
- 2330 live quote 有 `isClose=true`，沒有 `isOpen`／`isTrial`；Provider 不要求後兩欄。
- Basic 方案呼叫 snapshot 實際回傳 `{"statusCode":403,"message":"Forbidden"}`，不得當成成功的空陣列。

## Endpoint 與 canonical mapping

### `/intraday/ticker/{symbol}`

- 用於 symbol identity 與上市普通股 gate。
- 必須是四位數代碼，且 live payload 同時符合 `type=EQUITY`、`exchange=TWSE`、`market=TSE`、`securityType=01`、`tradingCurrency=TWD`。
- `name`、`securityStatus`、`boardLot` 與 Gregorian date 做 required-field/type validation。
- `securityStatus=NORMAL` 映射 canonical `is_active=true`；未知狀態 fail closed。

### `/historical/candles/{symbol}`

- Query：`from`、`to` 使用 Gregorian `yyyy-MM-dd`，`timeframe=D`，fields 固定為 `open,high,low,close,volume,turnover,change`；單次區間最多一年。
- `data[].date` → `DailyPrice.trade_date`。
- `open/high/low/close` → canonical OHLC，必須為有限正數且關係合法。
- `volume` → `DailyPrice.volume`，官方單位為股，必須是非負 integer。
- `turnover`（元）與 `change` 只驗證，不滲透既有 domain model。
- ticker 與 candles 的 URL、最大 `fetched_at`、實際最新 `market_date` 進入 provider-neutral provenance。

`fetch_market_data()` 的正式主幹是 ticker + daily historical candles。玉山欄位不會滲透 normalization、analysis 或 summary。

### `/intraday/quote/{symbol}`

- `openPrice/highPrice/lowPrice/closePrice` 只允許「四欄都有值」或「四欄皆 null／缺省」；部分缺值 fail closed。
- `total.tradeVolume` 目前只保存為 `volume_raw`，不冒充 canonical 日成交股數，因日內欄位定義／單位需與歷史日 K 分開看待。
- `lastUpdated` 保存 raw integer 及已驗證的 UTC datetime；null 保持 null，不補成本機抓取時間。
- quote 是 live diagnostics，不混入 daily candle canonical row。

### `/historical/stats/{symbol}`

- 驗證 `date`、OHLC、`tradeVolume`、`tradeValue`、`previousClose`、`week52High`、`week52Low` 與 `change`。
- 第一個 slice 只提供 typed diagnostics／一致性檢查，不新增估值、評分或交易訊號。

### `/snapshot/quotes/{market}`

- 本階段只允許 `market=TSE`。
- Basic 方案 live outcome 為 HTTP／embedded 403，映射 `ProviderPermanentError`，不 retry、不回傳空集合。
- 因尚未取得有權限的成功 live payload，成功 response mapping 明確 `ProviderNotImplementedError` fail closed。只有升級權限、凍結最小成功 fixture 並補 mapping tests 後才能啟用。

## Rate limit 與 failure taxonomy

- 官方一般文件列出日內 600/min、snapshot 600/min、historical 60/min；方案 FAQ 顯示 Basic 日內為 60/min、沒有 snapshot 權限、historical 60/min。
- HTTP 429 與 5xx → `ProviderTemporaryError`。
- connection／read timeout → `ProviderTimeoutError`（temporary 子型別）。
- 其他 4xx、設定／SDK 版本／驗證失敗 → `ProviderPermanentError`。
- malformed JSON、required field/type/date/number/OHLC mismatch → `ProviderInvalidPayloadError`。

Provider／transport 本身沒有內建 retry。只有既有 Phase 2 pipeline 對 temporary／timeout 做 bounded retry 與 configurable backoff；permanent、invalid payload、normalization 與 SQLite failure 全部 fail closed。

## Schema v5 cross-validation

`market_data_validation_runs` 保存 provider pair、target date、狀態、attempt 與 outcome；`market_data_observations` 分別保存 TWSE／玉山的 canonical OHLCV、實際 market date、source endpoints、`fetched_at`、raw／parsed source timestamp；`market_data_discrepancies` 保存欄位、左右值、原因及可計算時的 absolute／relative difference。

- 同一 `(symbol, target_date, left_provider, right_provider)` 成功後零抓取回放。
- failed 可 retry；running 只能以完整 `run_id` 明確恢復。
- 兩筆 observation、所有 discrepancy 與 success transition 在同一 transaction。
- cross-validation 表不寫入或覆蓋 `daily_prices`，也不選擇「勝出來源」。
- 若 market date 不同，只記錄日期差，不跨日比較 OHLCV。
- source timestamp 缺值保留為 SQL NULL；一側缺值會記錄 `missing_value`，不拿 `fetched_at` 代填。
- volume 差異標為 `volume_definition_or_update_timing_unresolved`；其他行情差異也保留 unresolved，不自行判定哪一方錯誤。

## 2026-08-06 live smoke 結果

- 以 target date 2026-08-06 執行時，TWSE `_ALL` 最新市場日仍為 2026-08-05，玉山 daily candles 已為 2026-08-06；2330、2317、2454 均只記錄 `market_date_mismatch`，未跨日硬比行情。
- 再以共同市場日 2026-08-05 錨定後，三檔的 OHLCV 逐欄完全一致：
  - 2330：2385／2415／2370／2405，36,782,301 股。
  - 2317：257／260.5／256／258.5，72,633,762 股。
  - 2454：4095／4125／3980／4000，12,935,125 股。
- 三檔 quote 市場日皆為 2026-08-06，source timestamp 皆為臺北 13:30 收盤時間；historical stats 同日 OHLCV 與玉山 candles 一致。
- Snapshot 仍為 Basic 方案 403；成功 mapping 未啟用。
- 2330 完成 `E.SUN → canonical model → SQLite → Research Summary`，11 筆行情，第二次執行為 idempotent replay。
- 兩個 live DB 均為 schema v5、`integrity_check=ok`、foreign-key violations 0。

## Schema v6：歷史研究主線

`EsunHistoricalMarketDataProvider` 不新增 mapping 規則，而是把已驗證的 ticker + historical candles adapter 限制成 Phase 4 所需的「一次一個日曆月」契約，source 固定為 `esun-historical`。Retry、timeout、月份 cursor、failed recovery、explicit interrupted resume 與 60–250 coverage 全部沿用既有 `HistoricalSyncPipeline`。

每個歷史月 checkpoint 先把 normalized rows 寫入 `historical_source_observations`，再與 cursor 同一 transaction 提交：

- Key 是 `(historical_run_id, trade_date)`，不會以另一 provider 更新同一 row。
- 保存 provider、OHLCV、所有 source endpoints、`fetched_at` 與 nullable source timestamp。
- `twse-historical` 同時可投影到既有 `daily_prices`，維持 Phase 4 指標語意。
- `esun-historical` 在 pipeline constructor 層禁止啟用 canonical write，因此無法覆寫 TWSE。

完成的 TWSE／E.SUN historical runs 才能建立 `historical_validation_runs`。比較規則：

- 各自取 target date 以前最近 60–250 個 provider-specific observations。
- 相同交易日逐欄比較 open／high／low／close／volume。
- 只存在一側的日期分別記錄 `missing_in_twse`／`missing_in_esun`。
- 最新市場日分欄保存；不把不同日期的行情互相比較。
- 差異保存左右值、reason、absolute difference 與 relative difference；不選擇勝出來源。
- 研究指標固定由 TWSE 原始歷史觀察計算；E.SUN 只做 validation，不平均、不補洞、不改寫指標輸入。

## 60／250 日 live historical smoke

2026-08-06 對 2330、2317、2454 執行兩輪唯讀驗證：

- 60 日：三檔期間均為 2026-05-13～2026-08-06；60 個共同交易日、60 日 OHLCV 全部一致、無缺口，兩來源最新市場日皆為 2026-08-06。
- 250 日：三檔各有 250 個共同交易日、無日期缺口、兩來源最新市場日皆為 2026-08-06；249 日 OHLCV 全部一致。
- 唯一 live 差異集中在 2026-04-28 的 volume，三檔 OHLC 均一致：
  - 2330：TWSE 57,336,004；E.SUN 53,038,648。
  - 2317：TWSE 104,089,860；E.SUN 103,741,860。
  - 2454：TWSE 24,314,216；E.SUN 24,102,216。
- 現有官方證據不足以判定差異來自交易範圍定義或後續修訂，因此固定為 `volume_definition_or_source_revision_unresolved`，不得自行歸責。
- 每檔成功重跑均為零抓取 replay；兩個 schema v6 live DB 均 `integrity_check=ok`、foreign-key violations 0，canonical sources 只有 `twse-historical`。
