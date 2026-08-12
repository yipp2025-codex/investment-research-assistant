"""Windows Task Scheduler XML generation for the Phase 6C EOD entrypoint."""

from __future__ import annotations

import subprocess
from dataclasses import dataclass, field
from datetime import date, datetime, time as dt_time
from pathlib import Path
from xml.etree import ElementTree


@dataclass(frozen=True, slots=True)
class WindowsTaskConfig:
    task_name: str = "ResearchAssistant-EOD"
    project_root: Path = field(
        default_factory=lambda: Path(__file__).resolve().parents[2]
    )
    python_path: str = "python"
    watchlist: str = "default"
    provider: str = "twse"
    schedule_time: str = "18:00"
    grace_period_minutes: int = 30
    database_path: Path | None = None

    def __post_init__(self) -> None:
        if not self.task_name.strip():
            raise ValueError("task_name must not be empty")
        if not self.project_root:
            raise ValueError("project_root must not be empty")
        if not self.python_path.strip():
            raise ValueError("python_path must not be empty")
        if not self.watchlist.strip():
            raise ValueError("watchlist must not be empty")
        if self.provider not in {"twse", "mock"}:
            raise ValueError("provider must be twse or mock")
        _parse_time(self.schedule_time)
        if self.grace_period_minutes < 0:
            raise ValueError("grace_period_minutes must be >= 0")

    @property
    def effective_database_path(self) -> Path:
        return self.database_path or (self.project_root / "data" / "research.db")


def build_task_scheduler_xml(config: WindowsTaskConfig) -> str:
    """Return a daily, non-overlapping, least-privilege EOD task definition."""

    schedule = _parse_time(config.schedule_time)
    start_boundary = datetime.combine(
        date.today(), schedule
    ).replace(microsecond=0).isoformat()
    ns = "http://schemas.microsoft.com/windows/2004/02/mit/task"
    ElementTree.register_namespace("", ns)
    q = lambda name: f"{{{ns}}}{name}"  # noqa: E731
    task = ElementTree.Element(q("Task"), {"version": "1.4"})
    registration = ElementTree.SubElement(task, q("RegistrationInfo"))
    ElementTree.SubElement(registration, q("Author")).text = "Investment Research Assistant"
    ElementTree.SubElement(registration, q("Description")).text = (
        "Read-only EOD daily research operation; invokes the existing Phase 6A CLI."
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
    ElementTree.SubElement(settings, q("DisallowStartIfOnBatteries")).text = "false"
    ElementTree.SubElement(settings, q("StopIfGoingOnBatteries")).text = "false"
    ElementTree.SubElement(settings, q("StartWhenAvailable")).text = "true"
    ElementTree.SubElement(settings, q("ExecutionTimeLimit")).text = "PT2H"
    ElementTree.SubElement(settings, q("AllowHardTerminate")).text = "true"

    actions = ElementTree.SubElement(task, q("Actions"), {"Context": "Author"})
    action = ElementTree.SubElement(actions, q("Exec"))
    ElementTree.SubElement(action, q("Command")).text = config.python_path
    arguments = [
        str(config.project_root / "scripts" / "scheduler.py"),
        "run",
        "--scheduled",
        "--project-root",
        str(config.project_root),
        "--database",
        str(config.effective_database_path),
        "--watchlist",
        config.watchlist,
        "--provider",
        config.provider,
        "--schedule-time",
        config.schedule_time,
        "--grace-minutes",
        str(config.grace_period_minutes),
    ]
    ElementTree.SubElement(action, q("Arguments")).text = subprocess.list2cmdline(
        arguments
    )
    ElementTree.SubElement(action, q("WorkingDirectory")).text = str(config.project_root)

    return ElementTree.tostring(task, encoding="unicode")


def _parse_time(value: str) -> dt_time:
    try:
        hour_text, minute_text = value.strip().split(":", 1)
        hour = int(hour_text)
        minute = int(minute_text)
    except (AttributeError, ValueError) as exc:
        raise ValueError("schedule_time must use HH:MM 24-hour format") from exc
    if hour not in range(24) or minute not in range(60):
        raise ValueError("schedule_time must use HH:MM 24-hour format")
    return dt_time(hour, minute)


__all__ = ["WindowsTaskConfig", "build_task_scheduler_xml"]
