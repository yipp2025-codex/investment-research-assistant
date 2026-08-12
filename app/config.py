"""Environment-backed application configuration."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path


_ENV_KEY = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def load_env_file(path: str | Path = ".env") -> None:
    """Load a small, predictable subset of dotenv syntax without overwriting env vars.

    The loader intentionally performs no interpolation and never prints values, which
    keeps secrets out of logs. Existing process environment variables take precedence.
    """

    env_path = Path(path)
    if not env_path.is_file():
        return

    for line_number, raw_line in enumerate(
        env_path.read_text(encoding="utf-8").splitlines(), start=1
    ):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].lstrip()
        if "=" not in line:
            raise ValueError(f"Invalid .env entry at line {line_number}")

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not _ENV_KEY.fullmatch(key):
            raise ValueError(f"Invalid environment variable name at line {line_number}")
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        os.environ.setdefault(key, value)


@dataclass(frozen=True, slots=True)
class Settings:
    """Runtime paths only; broker credentials remain inside ignored SDK config."""

    database_path: Path
    esun_marketdata_config_path: Path | None = None

    @classmethod
    def from_env(cls, env_file: str | Path = ".env") -> "Settings":
        load_env_file(env_file)
        database_path = Path(
            os.environ.get("IRA_DATABASE_PATH", "data/research.db")
        ).expanduser()
        raw_esun_path = os.environ.get("ESUN_MARKETDATA_CONFIG_PATH", "").strip()
        esun_path = Path(raw_esun_path).expanduser() if raw_esun_path else None
        return cls(
            database_path=database_path,
            esun_marketdata_config_path=esun_path,
        )
