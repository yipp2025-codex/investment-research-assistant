"""Perform the official E.SUN market-data SDK's interactive first login."""

from __future__ import annotations

import argparse
import os
from configparser import ConfigParser
from pathlib import Path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Prompt for E.SUN account/certificate passwords through the official "
            "SDK and store them in its configured system keyring."
        )
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=os.environ.get("ESUN_MARKETDATA_CONFIG_PATH"),
        help="Path to the official E.SUN key config file.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.config is None:
        raise SystemExit(
            "Set ESUN_MARKETDATA_CONFIG_PATH or pass --config. No secret values "
            "belong in this project."
        )
    config_path = args.config.expanduser().resolve()
    if not config_path.is_file():
        raise SystemExit("The E.SUN config path does not reference a file.")

    config = ConfigParser(interpolation=None)
    loaded = config.read(config_path, encoding="utf-8-sig")
    if not loaded:
        raise SystemExit("The E.SUN config could not be read.")

    try:
        from esun_marketdata import EsunMarketdata

        sdk = EsunMarketdata(config)
        sdk.login()
    except (KeyboardInterrupt, EOFError):
        raise SystemExit("E.SUN interactive login was cancelled.") from None
    except Exception as error:
        raise SystemExit(
            "E.SUN market-data login failed without exposing details: "
            f"{type(error).__name__}"
        ) from None

    print(
        "E.SUN market-data login succeeded. Passwords are managed by the "
        "official SDK keyring; no password was written to this project."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
