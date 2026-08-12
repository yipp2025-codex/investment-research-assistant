"""Capture minimized, credential-safe live evidence from official E.SUN market data."""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
from configparser import ConfigParser
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any


SYMBOLS = ("2330", "2317", "2454")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Capture minimized E.SUN market-data contract evidence."
    )
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--symbols", nargs="+", default=list(SYMBOLS))
    parser.add_argument("--from-date", type=date.fromisoformat)
    parser.add_argument("--to-date", type=date.fromisoformat, default=date.today())
    return parser


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def _evidence(value: object) -> dict[str, object]:
    return {
        "sha256": hashlib.sha256(_canonical_bytes(value)).hexdigest().upper(),
        "top_level_type": type(value).__name__,
        "top_level_keys": sorted(value) if isinstance(value, dict) else None,
        "null_paths": _null_paths(value),
    }


def _null_paths(value: object, path: str = "$") -> list[str]:
    found: list[str] = []
    if value is None:
        found.append(path)
    elif isinstance(value, dict):
        for key, child in value.items():
            found.extend(_null_paths(child, f"{path}.{key}"))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            found.extend(_null_paths(child, f"{path}[{index}]"))
    return found


def _pick(value: dict[str, Any], fields: tuple[str, ...]) -> dict[str, Any]:
    return {field: value[field] for field in fields if field in value}


def _load_config(path: Path) -> tuple[ConfigParser, tuple[str, ...]]:
    config = ConfigParser(interpolation=None)
    if not config.read(path, encoding="utf-8-sig"):
        raise RuntimeError("E.SUN config could not be read")
    required = ("Core", "Cert", "Api", "User")
    if not all(config.has_section(section) for section in required):
        raise RuntimeError("E.SUN config is missing required sections")
    sensitive = tuple(
        value
        for value in (
            config.get("Api", "Key", fallback="").strip(),
            config.get("Api", "Secret", fallback="").strip(),
            config.get("User", "Account", fallback="").strip(),
            config.get("Cert", "Path", fallback="").strip(),
        )
        if value
    )
    return config, sensitive


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config_path = args.config.expanduser().resolve()
    output_path = args.output.expanduser().resolve()
    from_date = args.from_date or (args.to_date - timedelta(days=14))
    if from_date > args.to_date:
        raise SystemExit("from-date must not be after to-date")

    config, sensitive_values = _load_config(config_path)
    from esun_marketdata import EsunMarketdata

    sdk = EsunMarketdata(config)
    sdk.login()
    stock = sdk.rest_client.stock

    captured_at = datetime.now(timezone.utc).isoformat()
    tickers: list[dict[str, object]] = []
    quotes: list[dict[str, object]] = []
    candles: list[dict[str, object]] = []
    stats: list[dict[str, object]] = []
    raw_by_endpoint: dict[str, list[object]] = {
        "intraday_ticker": [],
        "intraday_quote": [],
        "historical_candles": [],
        "historical_stats": [],
    }

    for symbol in args.symbols:
        ticker_raw = stock.intraday.ticker(symbol=symbol)
        quote_raw = stock.intraday.quote(symbol=symbol)
        candles_raw = stock.historical.candles(
            **{
                "symbol": symbol,
                "from": from_date.isoformat(),
                "to": args.to_date.isoformat(),
                "timeframe": "D",
                "fields": "open,high,low,close,volume,turnover,change",
            }
        )
        stats_raw = stock.historical.stats(symbol=symbol)
        for endpoint, raw in (
            ("intraday_ticker", ticker_raw),
            ("intraday_quote", quote_raw),
            ("historical_candles", candles_raw),
            ("historical_stats", stats_raw),
        ):
            if not isinstance(raw, dict):
                raise RuntimeError(f"{endpoint} returned a non-object payload")
            raw_by_endpoint[endpoint].append(raw)

        tickers.append(
            {
                "payload": _pick(
                    ticker_raw,
                    (
                        "date",
                        "type",
                        "exchange",
                        "market",
                        "symbol",
                        "name",
                        "securityType",
                        "securityStatus",
                        "tradingCurrency",
                        "boardLot",
                    ),
                ),
                "evidence": _evidence(ticker_raw),
            }
        )
        quote_payload = _pick(
            quote_raw,
            (
                "date",
                "type",
                "exchange",
                "market",
                "symbol",
                "name",
                "openPrice",
                "highPrice",
                "lowPrice",
                "closePrice",
                "lastUpdated",
                "isTrial",
                "isOpen",
                "isClose",
            ),
        )
        if "total" in quote_raw:
            quote_payload["total"] = quote_raw["total"]
        quotes.append({"payload": quote_payload, "evidence": _evidence(quote_raw)})
        candle_payload = _pick(
            candles_raw,
            ("type", "exchange", "market", "symbol", "timeframe", "sort"),
        )
        candle_payload["data"] = [
            _pick(
                row,
                ("date", "open", "high", "low", "close", "volume", "turnover", "change"),
            )
            for row in candles_raw.get("data", [])
            if isinstance(row, dict)
        ]
        candles.append(
            {"payload": candle_payload, "evidence": _evidence(candles_raw)}
        )
        stats.append(
            {
                "payload": _pick(
                    stats_raw,
                    (
                        "date",
                        "type",
                        "exchange",
                        "market",
                        "symbol",
                        "name",
                        "openPrice",
                        "highPrice",
                        "lowPrice",
                        "closePrice",
                        "change",
                        "tradeVolume",
                        "tradeValue",
                        "previousClose",
                        "week52High",
                        "week52Low",
                    ),
                ),
                "evidence": _evidence(stats_raw),
            }
        )

    snapshot_raw = stock.snapshot.quotes(market="TSE")
    if not isinstance(snapshot_raw, dict):
        raise RuntimeError("snapshot quotes returned a non-object payload")
    embedded_status = snapshot_raw.get("statusCode", snapshot_raw.get("status"))
    if isinstance(embedded_status, int) and embedded_status >= 400:
        snapshot_rows: list[dict[str, object]] = []
        snapshot = {
            "payload": _pick(snapshot_raw, ("statusCode", "status", "message")),
            "full_row_count": 0,
            "evidence": _evidence(snapshot_raw),
        }
    else:
        requested = set(args.symbols)
        snapshot_rows = [
            _pick(
                row,
                (
                    "type",
                    "symbol",
                    "name",
                    "openPrice",
                    "highPrice",
                    "lowPrice",
                    "closePrice",
                    "tradeVolume",
                    "tradeValue",
                    "lastUpdated",
                ),
            )
            for row in snapshot_raw.get("data", [])
            if isinstance(row, dict) and row.get("symbol") in requested
        ]
        snapshot = {
            "payload": {
                **_pick(snapshot_raw, ("date", "time", "market")),
                "data": snapshot_rows,
            },
            "full_row_count": len(snapshot_raw.get("data", [])),
            "evidence": _evidence(snapshot_raw),
        }

    result = {
        "captured_at": captured_at,
        "sdk_distribution": "esun_marketdata",
        "sdk_version": importlib.metadata.version("esun_marketdata"),
        "requested_symbols": list(args.symbols),
        "historical_request": {
            "from": from_date.isoformat(),
            "to": args.to_date.isoformat(),
            "timeframe": "D",
            "fields": "open,high,low,close,volume,turnover,change",
        },
        "intraday_ticker": tickers,
        "intraday_quote": quotes,
        "snapshot_quotes": snapshot,
        "historical_candles": candles,
        "historical_stats": stats,
    }
    serialized = json.dumps(result, ensure_ascii=False, indent=2)
    if any(secret in serialized for secret in sensitive_values):
        raise RuntimeError("captured evidence unexpectedly contains credential material")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(serialized + "\n", encoding="utf-8")
    print(
        json.dumps(
            {
                "ok": True,
                "sdk_version": result["sdk_version"],
                "symbols": list(args.symbols),
                "snapshot_rows_kept": len(snapshot_rows),
                "snapshot_full_row_count": snapshot["full_row_count"],
                "output_sha256": hashlib.sha256(
                    (serialized + "\n").encode("utf-8")
                ).hexdigest().upper(),
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
