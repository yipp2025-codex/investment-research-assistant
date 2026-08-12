from __future__ import annotations

import ast
from datetime import date
from pathlib import Path
from xml.etree import ElementTree

from app.windows_scheduler_adapter import (
    WindowsDailyRunnerTaskConfig,
    build_daily_runner_command,
    build_daily_runner_command_line,
    build_daily_runner_task_xml,
)


def _config(tmp_path: Path) -> WindowsDailyRunnerTaskConfig:
    root = (tmp_path / "project root with spaces").resolve()
    return WindowsDailyRunnerTaskConfig(
        task_name="IRA S6B Test",
        project_root=root,
        python_path=str((root / "Python 312" / "python.exe").resolve()),
        database_path=(root / "data with spaces" / "research.db").resolve(),
        report_directory=(root / "reports with spaces").resolve(),
        lock_directory=(root / "locks with spaces").resolve(),
        start_date=date(2026, 8, 9),
        schedule_time="18:00",
        s5_factory="tests.fake_composition:build_s5",
        latest_date_factory="tests.fake_calendar:build_latest_date_provider",
    )


def test_s6b_adapter_command_is_explicit_and_quotes_paths(tmp_path: Path) -> None:
    config = _config(tmp_path)
    command = build_daily_runner_command(
        config,
        target_market_date=date(2026, 8, 7),
    )
    command_line = build_daily_runner_command_line(
        config,
        target_market_date=date(2026, 8, 7),
    )

    assert command[0] == config.python_path
    assert command[1:3] == ("-m", "app.daily_runner")
    assert "--db" in command
    assert str(config.database_path) in command
    assert "--report-dir" in command
    assert str(config.report_directory) in command
    assert "--lock-dir" in command
    assert str(config.lock_directory) in command
    assert "--market-date" in command
    assert "2026-08-07" in command
    assert "--s5-factory" in command
    assert "tests.fake_composition:build_s5" in command
    assert "--latest-date-factory" in command
    assert "tests.fake_calendar:build_latest_date_provider" in command
    assert '"' in command_line
    assert "app.daily_runner" in command_line
    assert "research.db" in command_line


def test_s6b_adapter_xml_is_deterministic_daily_wakeup_only(tmp_path: Path) -> None:
    config = _config(tmp_path)
    first = build_daily_runner_task_xml(config)
    second = build_daily_runner_task_xml(config)

    assert first == second
    root = ElementTree.fromstring(first)
    namespace = "{http://schemas.microsoft.com/windows/2004/02/mit/task}"
    assert root.find(f"{namespace}Triggers/{namespace}CalendarTrigger") is not None
    assert root.find(
        f"{namespace}Triggers/{namespace}CalendarTrigger/{namespace}StartBoundary"
    ).text == "2026-08-09T18:00:00"
    assert root.find(
        f"{namespace}Triggers/{namespace}CalendarTrigger/{namespace}ScheduleByDay/{namespace}DaysInterval"
    ).text == "1"
    assert root.find(
        f"{namespace}Settings/{namespace}MultipleInstancesPolicy"
    ).text == "IgnoreNew"
    assert root.find(f"{namespace}Actions/{namespace}Exec/{namespace}Command").text == (
        config.python_path
    )
    arguments = root.find(
        f"{namespace}Actions/{namespace}Exec/{namespace}Arguments"
    ).text
    assert "app.daily_runner" in arguments
    assert "--db" in arguments
    assert str(config.database_path) in arguments
    assert root.find(
        f"{namespace}Actions/{namespace}Exec/{namespace}WorkingDirectory"
    ).text == str(config.project_root)
    assert "Register-ScheduledTask" not in first
    assert "schtasks" not in first.lower()


def test_s6b_adapter_has_no_application_or_registration_logic() -> None:
    source_path = Path(__file__).parents[1] / "app" / "windows_scheduler_adapter.py"
    source = source_path.read_text(encoding="utf-8")
    tree = ast.parse(source)
    imports = {
        node.module
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module
    }
    assert not any(module.startswith("app.") for module in imports)
    assert "SQLite" not in source
    assert "DailyScreener" not in source
    assert "ScreenerReport" not in source
    assert "subprocess.run" not in source
