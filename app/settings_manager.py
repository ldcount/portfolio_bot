"""Persistent runtime settings for the Telegram bot."""

import json
import logging
import os
from typing import Any

logger = logging.getLogger(__name__)

_SETTINGS_FILE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "bot_settings.json",
)


def load_settings(default_interval_minutes: int) -> dict[str, Any]:
    """Load persisted scheduling settings, falling back to configured defaults."""
    defaults = {
        "poll_interval_minutes": default_interval_minutes,
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

    interval = stored.get("poll_interval_minutes", default_interval_minutes)
    enabled = stored.get("scheduled_reports_enabled", True)
    if not isinstance(interval, int) or interval < 1:
        interval = default_interval_minutes
    if not isinstance(enabled, bool):
        enabled = True

    return {
        "poll_interval_minutes": interval,
        "scheduled_reports_enabled": enabled,
    }


def save_schedule(interval_minutes: int, enabled: bool = True) -> None:
    """Persist the automatic-report interval and enabled state atomically."""
    if interval_minutes < 1:
        raise ValueError("interval_minutes must be positive")

    os.makedirs(os.path.dirname(_SETTINGS_FILE), exist_ok=True)
    temporary_file = f"{_SETTINGS_FILE}.tmp"
    payload = {
        "poll_interval_minutes": interval_minutes,
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
