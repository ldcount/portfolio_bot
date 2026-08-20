"""Persistent runtime settings for the Telegram bot."""

import json
import logging
import os
from typing import Any

logger = logging.getLogger(__name__)

DEFAULT_DAILY_REPORT_TIME = "20:30"
DAILY_REPORT_TIMES = ("12:30", "20:30")

_SETTINGS_FILE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "bot_settings.json",
)


def _validated_report_time(report_time: Any) -> str:
    if report_time in DAILY_REPORT_TIMES:
        return report_time
    return DEFAULT_DAILY_REPORT_TIME


def load_settings() -> dict[str, Any]:
    """Load scheduling settings and migrate the legacy interval schema."""
    defaults = {
        "daily_report_time": DEFAULT_DAILY_REPORT_TIME,
        "scheduled_reports_enabled": True,
    }
    if not os.path.exists(_SETTINGS_FILE):
        return defaults

    try:
        with open(_SETTINGS_FILE, "r", encoding="utf-8") as file:
            stored = json.load(file)
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("Could not load bot settings; using defaults: %s", exc)
        return defaults

    if not isinstance(stored, dict):
        logger.warning("Bot settings are not a JSON object; using safe defaults.")
        stored = {}

    report_time = _validated_report_time(stored.get("daily_report_time"))
    enabled = stored.get("scheduled_reports_enabled", True)
    if not isinstance(enabled, bool):
        enabled = True

    settings = {
        "daily_report_time": report_time,
        "scheduled_reports_enabled": enabled,
    }
    if "daily_report_time" not in stored or stored != settings:
        try:
            save_schedule(report_time, enabled)
        except OSError as exc:
            logger.warning(
                "Could not persist migrated bot settings; using them in memory: %s",
                exc,
            )
    return settings


def save_schedule(report_time: str, enabled: bool = True) -> None:
    """Persist the automatic-report time and enabled state atomically."""
    if report_time not in DAILY_REPORT_TIMES:
        raise ValueError(f"report_time must be one of: {', '.join(DAILY_REPORT_TIMES)}")
    if not isinstance(enabled, bool):
        raise ValueError("enabled must be a boolean")

    os.makedirs(os.path.dirname(_SETTINGS_FILE), exist_ok=True)
    temporary_file = f"{_SETTINGS_FILE}.tmp"
    payload = {
        "daily_report_time": report_time,
        "scheduled_reports_enabled": enabled,
    }
    try:
        with open(temporary_file, "w", encoding="utf-8") as file:
            json.dump(payload, file, indent=2)
        os.replace(temporary_file, _SETTINGS_FILE)
    except OSError:
        try:
            if os.path.exists(temporary_file):
                os.remove(temporary_file)
        except OSError:
            pass
        raise
