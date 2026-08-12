# TWSE OpenAPI contract evidence

驗證日期：2026-08-05（Asia/Taipei）

## 官方來源

- Swagger 2.0：<https://openapi.twse.com.tw/v1/swagger.json>
- Host：`openapi.twse.com.tw`
- Base path：`/v1`
- Swagger SHA-256：`5A43E299E366F9318AA6CACED07559D555E640D4DED7979CDCA5376FB521D454`
- 日成交 endpoint：`GET /exchangeReport/STOCK_DAY_ALL`
- 估值 endpoint：`GET /exchangeReport/BWIBBU_ALL`

本次 live snapshot：

- `STOCK_DAY_ALL`：HTTP 200、JSON array、1,377 筆，SHA-256 `BD2FAD083C2DD266691D4DCE9C7EE5060306DBFC8D4F601A1CBDBA6AB6FBB278`。
- `BWIBBU_ALL`：HTTP 200、JSON array、1,082 筆，SHA-256 `01259017CB4E0D1CD4796464D066830828961D6F5CA805E561465F0E5B5C792F`。
- 2330、2317、2454 在兩份 payload 的日期均為 `1150804`，即民國年 115 年 8 月 4 日／西元 2026-08-04。

Swagger 的 200 response schema 將頂層宣告為 `object`，但官方 endpoint 的 live response 是 record array。Provider 因此要求頂層必須是實際驗證到的 JSON array，並逐筆依 Swagger properties 驗證；這項官方文件與實際回應差異不得被隱藏。

## Swagger 欄位契約

`STOCK_DAY_ALL` 必要欄位皆宣告為 string：

- `Date`：日期。
- `Code`：證券代號。
- `Name`：證券名稱。
- `TradeVolume`：成交股數。
- `TradeValue`：成交金額。
- `OpeningPrice`、`HighestPrice`、`LowestPrice`、`ClosingPrice`：OHLC 價格。
- `Change`：漲跌價差。
- `Transaction`：成交筆數。

`BWIBBU_ALL` 必要欄位皆宣告為 string：

- `Date`：日期。
- `Code`：股票代號。
- `Name`：股票名稱。
- `PEratio`：本益比。
- `DividendYield`：殖利率，Swagger 明定單位為 `%`。
- `PBratio`：股價淨值比。

## Live format evidence and fail-closed rules

- 日期：Swagger 未提供 pattern；本次官方資料是 7 位民國年月日 `YYYMMDD`。Provider 僅接受此已驗證格式，以民國年加 1911 轉為 ISO date；其他格式視為 invalid payload。
- 整數：成交股數、成交金額、成交筆數均為不含千分位的十進位數字字串。
- 小數：OHLC、漲跌價差、PE、殖利率與 PB 均為不含千分位的十進位字串；漲跌可為負值。
- 缺值：本次 `BWIBBU_ALL` 中 `PEratio` 有 244 筆、`DividendYield` 有 234 筆是空字串；空字串代表該 metric 不建立 canonical record。其他非數字標記不在已驗證契約內，會 fail closed。
- 單位：`TradeVolume` 依 Swagger 的「成交股數」映射為股數；殖利率依 Swagger 映射為百分比；PE/PB 為 ratio。Swagger 未另帶 OHLC currency metadata；本 adapter 僅支援 TWSE 上市普通股，canonical symbol currency 固定為 `TWD`，並保留此限制。

## TWSE to canonical mapping

| TWSE | Canonical | Conversion |
|---|---|---|
| `Code` | `Symbol.symbol` / record `symbol` | trim，四位數代碼 |
| `Name` | `Symbol.name` | trim；兩 endpoint 必須一致 |
| market | `Symbol.market` | 固定 `TWSE` |
| currency | `Symbol.currency` | 固定 `TWD` |
| `Date` | `trade_date` / `metric_date` | ROC `YYYMMDD` → Gregorian date |
| `OpeningPrice` | `DailyPrice.open` | decimal string → float |
| `HighestPrice` | `DailyPrice.high` | decimal string → float |
| `LowestPrice` | `DailyPrice.low` | decimal string → float |
| `ClosingPrice` | `DailyPrice.close` | decimal string → float |
| `TradeVolume` | `DailyPrice.volume` | digit string → integer shares |
| `PEratio` | `price_earnings_ratio` | decimal → ratio；blank omitted |
| `DividendYield` | `dividend_yield_pct` | decimal → percent；blank omitted |
| `PBratio` | `price_to_book_ratio` | decimal → ratio；blank omitted |

`TradeValue`、`Change`、`Transaction` 仍依官方 string/numeric contract 驗證，但 Phase 3 canonical model 尚無對應欄位，因此不寫入 domain model。

### No-price characterization

`STOCK_DAY_ALL` 對只有零股活動、沒有 regular-lot canonical price 的證券，可能回傳非零 `TradeVolume` 但 OHLC 四欄全為 `0.00`。1538／2026-08-07 已由同月官方 `STOCK_DAY` 交叉確認為 OHLC 全 `--` 的 no-price row；因此四個 0 不是 0 元價格，不得寫入 `DailyPrice`。四欄全 0 分類為 target-date canonical price unavailable；部分為 0 或負值仍是 malformed payload，fail closed。

## Supported-security boundary

Phase 3 僅接受四位數代碼，且代碼必須同時存在於 `STOCK_DAY_ALL` 與 `BWIBBU_ALL`。這會保留 2330、2317、2454 等上市普通股，並排除只出現在成交 endpoint 的 ETF（例如 0050）及其他非本階段商品。若未來官方分類契約需要擴張，必須另立 migration／provider contract，不得用名稱猜測商品類型。
