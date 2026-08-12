"""Thin Windows Task Scheduler adapter for the S6B Daily Runner.

This module only builds a command and an optional daily XML definition.  It
does not contain market-date policy, lock behavior, S5/S6A calls, retry
rules, persistence, or task registration.
"""

from __future__ import annotations

import subprocess
from dataclasses import dataclass
from datetime import date, datetime, time as dt_time
from pathlib import Path
from xml.etree import ElementTree


@dataclass(frozen=True, slots=True)
class WindowsDailyRunnerTaskConfig:
    task_name: str
    project_root: Path
    python_path: str
    database_path: Path
    report_directory: Path
    start_date: date
    schedule_time: str = "18:00"
    lock_directory: Path | None = None
    s5_factory: str | None = None
    latest_published_date: date | None = None
    latest_date_factory: str | None = None
    runner_module: str = "app.daily_runner"
    runner_arguments: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.task_name.strip():
            raise ValueError("task_name must not be empty")
        for path, name in (
            (self.project_root, "project_root"),
            (self.database_path, "database_path"),
            (self.report_directory, "report_directory"),
        ):
            if not isinstance(path, Path) or not path.is_absolute():
                raise ValueError(f"{name} must be an absolute Path")
        if self.lock_directory is not None and (
            not isinstance(self.lock_directory, Path)
            or not self.lock_directory.is_absolute()
        ):
            raise ValueError("lock_directory must be an absolute Path")
        if not self.python_path.strip():
            raise ValueError("python_path must not be empty")
        if not self.runner_module.strip():
            raise ValueError("runner_module must not be empty")
        if not isinstance(self.runner_arguments, tuple) or any(
            not isinstance(value, str) or not value.strip()
            for value in self.runner_arguments
        ):
            raise ValueError("runner_arguments must be a tuple of non-empty strings")
        if not isinstance(self.start_date, date):
            raise TypeError("start_date must be a date")
        _parse_time(self.schedule_time)
        if self.s5_factory is not None and not self.s5_factory.strip():
            raise ValueError("s5_factory must not be blank")
        if self.latest_date_factory is not None and not self.latest_date_factory.strip():
            raise ValueError("latest_date_factory must not be blank")
        if self.latest_published_date is not None and self.latest_date_factory is not None:
            raise ValueError(
                "latest_published_date and latest_date_factory are mutually exclusive"
            )

    @property
    def effective_lock_directory(self) -> Path:
        return self.lock_directory or (self.database_path.parent / ".s6b-locks")


def build_daily_runner_command(
    config: WindowsDailyRunnerTaskConfig,
    *,
    target_market_date: date | None = None,
) -> tuple[str, ...]:
    """Build the stable argv used by Windows or a manual launcher."""

    command = [
        config.python_path,
        "-m",
        config.runner_module,
        "--db",
        str(config.database_path),
        "--report-dir",
        str(config.report_directory),
        "--lock-dir",
        str(config.effective_lock_directory),
    ]
    if target_market_date is not None:
        command.extend(["--market-date", target_market_date.isoformat()])
    if config.s5_factory is not None:
        command.extend(["--s5-factory", config.s5_factory])
    if config.latest_published_date is not None:
        command.extend(
            ["--latest-published-date", config.latest_published_date.isoformat()]
        )
    if config.latest_date_factory is not None:
        command.extend(["--latest-date-factory", config.latest_date_factory])
    command.extend(config.runner_arguments)
    return tuple(command)


def build_daily_runner_command_line(
    config: WindowsDailyRunnerTaskConfig,
    *,
    target_market_date: date | None = None,
) -> str:
    """Return a Windows-safe command line using ``list2cmdline`` quoting."""

    return subprocess.list2cmdline(
        list(
            build_daily_runner_command(
                config,
                target_market_date=target_market_date,
            )
        )
    )


def build_daily_runner_task_xml(config: WindowsDailyRunnerTaskConfig) -> str:
    """Build a daily wake-up task; never registers or enables it."""

    schedule = _parse_time(config.schedule_time)
    start_boundary = datetime.combine(config.start_date, schedule).replace(
        microsecond=0
    ).isoformat()
    namespace = "http://schemas.microsoft.com/windows/2004/02/mit/task"
    ElementTree.register_namespace("", namespace)

    def q(name: str) -> str:
        return f"{{{namespace}}}{name}"

    task = ElementTree.Element(q("Task"), {"version": "1.4"})
    registration = ElementTree.SubElement(task, q("RegistrationInfo"))
    ElementTree.SubElement(registration, q("Author")).text = (
        "Investment Research Assistant"
    )
    ElementTree.SubElement(registration, q("Description")).text = (
        "Daily wake-up for the S6B application runner; market-date gating "
        "remains in the application layer."
    )

    triggers = ElementTree.SubElement(task, q("Triggers"))
    trigger = ElementTree.SubElement(triggers, q("CalendarTrigger"))
    ElementTree.SubElement(trigger, q("StartBoundary")).text = start_boundary
    ElementTree.SubElement(trigger, q("Enabled")).text = "true"
    by_day = ElementTree.SubElement(trigger, q("ScheduleByDay"))
    ElementTree.SubElement(by_day, q("DaysInterval")).text = "1"

    principals = ElementTree.SubElement(task, q("Principals"))
    principal = ElementTree.SubElement(principals, q("Principal"), {"id": "Author"})
    ElementTree.SubElement(principal, q("LogonType")).text = "InteractiveToken"
    ElementTree.SubElement(principal, q("RunLevel")).text = "LeastPrivilege"

    settings = ElementTree.SubElement(task, q("Settings"))
    ElementTree.SubElement(settings, q("MultipleInstancesPolicy")).text = "IgnoreNew"
    ElementTree.SubElement(settings, q("StartWhenAvailable")).text = "true"
    ElementTree.SubElement(settings, q("DisallowStartIfOnBatteries")).text = "false"
    ElementTree.SubElement(settings, q("StopIfGoingOnBatteries")).text = "false"
    ElementTree.SubElement(settings, q("ExecutionTimeLimit")).text = "PT2H"

    actions = ElementTree.SubElement(task, q("Actions"), {"Context": "Author"})
    action = ElementTree.SubElement(actions, q("Exec"))
    command = build_daily_runner_command(config)
    ElementTree.SubElement(action, q("Command")).text = command[0]
    ElementTree.SubElement(action, q("Arguments")).text = subprocess.list2cmdline(
        list(command[1:])
    )
    ElementTree.SubElement(action, q("WorkingDirectory")).text = str(
        config.project_root
    )
    return ElementTree.tostring(task, encoding="unicode")


def _parse_time(value: str) -> dt_time:
    try:
        hour_text, minute_text = value.strip().split(":", 1)
        hour = int(hour_text)
        minute = int(minute_text)
    except (AttributeError, ValueError) as error:
        raise ValueError("schedule_time must use HH:MM") from error
    if hour not in range(24) or minute not in range(60):
        raise ValueError("schedule_time must use HH:MM")
    return dt_time(hour, minute)


__all__ = [
    "WindowsDailyRunnerTaskConfig",
    "build_daily_runner_command",
    "build_daily_runner_command_line",
    "build_daily_runner_task_xml",
]
