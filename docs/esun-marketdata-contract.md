# E.SUN market-data contract

本文件描述公開可理解的 E.SUN 行情資料邊界與安全語意；不代表任何
帳號已取得特定方案權限，也不代表已啟用 production 資料流程。

## 官方來源與範圍

- 事前準備：<https://www.esunsec.com.tw/trading-platforms/api-trading/docs/prerequisites/>
- 行情 API 簡介：<https://www.esunsec.com.tw/trading-platforms/api-trading/docs/market-data/intro/>
- HTTP API 開始使用：<https://www.esunsec.com.tw/trading-platforms/api-trading/docs/market-data/http-api/getting-started/>
- 方案權限：<https://www.esunsec.com.tw/trading-platforms/api-trading/docs/faq/price_and_plan/>
- 速率限制：<https://www.esunsec.com.tw/trading-platforms/api-trading/docs/market-data/rate-limit/>
- SDK 下載：<https://www.esunsec.com.tw/trading-platforms/api-trading/docs/download/download-sdk/>
- SDK 遷移指南：<https://www.esunsec.com.tw/trading-platforms/api-trading/docs/market-data/migration_guide/>

本專案只使用官方 E.SUN 證券行情能力，不使用委託、刪改單、帳務或
其他交易功能。行情來源的可用性仍受官方方案、帳號權限、服務狀態與
本文件所述驗證條件限制。

## 公開角色契約

TWSE 始終是研究資料的 canonical authority。E.SUN 是目前支援的
secondary market-data adapter，可在下列兩種明確角色之一運作：

1. **Validation**：在 TWSE canonical path 上提供獨立比對或診斷資料。
   E.SUN 不會因差異而被選為勝出來源，也不會改寫 TWSE canonical values。
2. **Qualified supplemental**：只有在 TWSE 出現符合條件的可恢復缺口，
   且 provider identity、資料格式、日期與 symbol identity、provenance、
   security boundary 及 supplemental eligibility 全部通過後，才可為
   有限範圍的缺口提供補充觀察。這些觀察必須保持 supplemental 標記，
   不得被描述或提升為 canonical。

E.SUN 的 supplemental role 不是 unrestricted fallback。TWSE 失敗本身不
足以授權替換；沒有明確 eligibility decision 時，系統不會以 E.SUN
取代 TWSE，也不會把 E.SUN 的結果當成既有 canonical value。

## 資料處理邊界

- 只有通過官方來源、symbol identity、market date、必要欄位、型別與數值
  合法性檢查的行情，才可進入後續研究資料處理。
- E.SUN 的原始欄位不會因為存在於 payload 就自動進入研究指標或摘要；
  只有通過契約且被明確選入的觀察，才會依其 validation 或 supplemental
  角色處理。
- Validation observations 不會寫入或覆蓋 TWSE canonical research data。
- Qualified supplemental observations 只能形成明確標記的 provisional
  research dataset，且不得覆蓋已存在的 TWSE canonical observation。
- 若後續取得 TWSE 觀察，reconciliation 會建立新的 immutable child；
  原 provisional dataset 不在原地改寫。

## Fail-closed 與錯誤處理

以下情況均 fail closed，不產生靜默替代資料：

- provider identity、symbol、market 或日期不一致；
- malformed、缺欄、型別錯誤、數值不合法或 normalization failure；
- 帳號、權限、SDK、endpoint 或安全驗證失敗；
- 不屬於明確 supplemental eligibility 的 permanent、unsupported 或其他
  provider failure。

只有明確允許的可恢復 provider-failure case，才可能進入 bounded
supplemental path；即使如此，仍須重新通過 identity、provenance、security
與 eligibility checks。Provider 不會因失敗而自行變成 canonical，也不會
以空集合偽裝成功。

## 憑證與交易隔離

帳號、Key、Secret、token、憑證與密碼只可由本機的受保護設定提供，不能
進入程式碼、測試 fixture、SQLite、文件或 log。公開 repository 不包含
這些秘密，也不包含 production 資料或 runtime 報告。

本專案是 read-only、research-only 系統，不提供下單、委託、自動交易、
買賣保證或投資建議。
