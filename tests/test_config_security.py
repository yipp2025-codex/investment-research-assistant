from pathlib import Path

from app.config import Settings


def test_dotenv_does_not_override_process_environment(tmp_path, monkeypatch) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("IRA_DATABASE_PATH=from-file.db\n", encoding="utf-8")
    monkeypatch.setenv("IRA_DATABASE_PATH", "from-process.db")

    settings = Settings.from_env(env_file)

    assert settings.database_path == Path("from-process.db")


def test_secret_placeholders_are_blank_and_local_env_is_ignored() -> None:
    example_lines = Path(".env.example").read_text(encoding="utf-8").splitlines()
    setting_lines = [
        line
        for line in example_lines
        if line and not line.startswith("#")
    ]

    assert "ESUN_MARKETDATA_CONFIG_PATH=" in setting_lines
    assert not any(
        marker in line.upper()
        for line in setting_lines
        for marker in ("API_KEY=", "SECRET=", "TOKEN=", "PASSWORD=", "ACCOUNT=")
    )
    ignored = Path(".gitignore").read_text(encoding="utf-8").splitlines()
    assert ".env" in ignored
    assert "*.ini" in ignored
    assert "*.p12" in ignored


def test_settings_load_only_the_ignored_esun_config_path(
    tmp_path, monkeypatch
) -> None:
    env_file = tmp_path / ".env"
    config_path = tmp_path / "private" / "provider.ini"
    env_file.write_text(
        f"ESUN_MARKETDATA_CONFIG_PATH={config_path}\n", encoding="utf-8"
    )
    monkeypatch.delenv("ESUN_MARKETDATA_CONFIG_PATH", raising=False)

    settings = Settings.from_env(env_file)

    assert settings.esun_marketdata_config_path == config_path
