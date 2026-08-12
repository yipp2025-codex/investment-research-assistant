# E.SUN market-data fixture provenance

- Captured: 2026-08-06 with the official Windows `esun_marketdata` 2.2.0 SDK.
- SDK wheel SHA-256: `57616A2AB7BA94172B52AEAB33036FF6266F046F0D721A32C760171F8B5C820C`.
- Live calls were read-only and used only 2330, 2317, and 2454.
- Config, account, Key, Secret, certificate path, SDK token, and passwords were excluded before writing fixtures.
- Ticker／quote／stats fixtures retain only fields used by contract tests.
- Historical candle fixtures retain the latest three rows from a 2026-07-23 through 2026-08-06 request. Full live response hashes:
  - 2330: `0689AC5B3CD103FA705F79945BB78363A0810FEDB0A3A204D595F91E2F4CB216`
  - 2317: `581271B093CE29387AB9B79B0B118AFCE7CF4AA244187E92B751B64245A3ACD3`
  - 2454: `054A8808957EF8120E1CA4F74C276D070EEF1A282809D1CDB3E0CAB1C175D408`
- Full live response hashes for retained single-symbol evidence:
  - ticker 2330: `44BC274DF0A94771A55548C16AD1A1046834690714447A099C3B4AB3F4579D3D`
  - quote 2330: `7219EE027F1092839F8E235E5BEA48E01E2FF5FC2C505BB608D9B6F59565D58D`
  - stats 2330: `2709430CD4333547B9FFFC6BA16FB6852384945FCE44379725FFEBA7A10ABD6A`
- Snapshot live response was `403 Forbidden` under the Basic plan. Official plan documentation states snapshots require Developer or Advanced access; this fixture must not be treated as a successful empty snapshot.
- Snapshot full canonical response SHA-256: `78FEF13654A80508761FE8ECB45E952B2190E7C96DE6B46D0FD5596CFE5096DD`.
- Quote `lastUpdated` was verified as microsecond epoch: `1785994200000000` → `2026-08-06T13:30:00+08:00`, matching the payload date and close state. Raw and parsed forms are both tested.
- `rate_limit_documented.json` is copied from the official rate-limit documentation, not produced by intentionally exhausting the live quota.
- `historical_cross_validation_20260428.json` is minimized canonical evidence from the 2026-08-06 250-day live smoke. All three symbols had identical OHLC and different volume only on 2026-04-28. The fixture preserves both values and deliberately leaves the cause unresolved.

Normal pytest reads only these minimized local fixtures and never logs in or accesses the network.
