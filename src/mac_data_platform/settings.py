"""경로와 config/pipelines.yaml 설정."""

from datetime import date, datetime
from functools import cache
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_FILE = PROJECT_ROOT / "config" / "pipelines.yaml"
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"  # 수집 원본 (CSV/JSON/PDF/XML/SQLite)
LOG_DIR = PROJECT_ROOT / "logs"
ERROR_LOG = LOG_DIR / "errors.log"


@cache
def config() -> dict[str, Any]:
    return yaml.safe_load(CONFIG_FILE.read_text(encoding="utf-8"))


def tz() -> ZoneInfo:
    return ZoneInfo(config().get("timezone", "Asia/Seoul"))


def today() -> date:
    return datetime.now(tz()).date()


def raw_postfix() -> str:
    return config().get("raw_postfix", "")


def retry_settings() -> dict[str, int]:
    return {"item_retries": 2, "max_retries": 2, "delay_seconds": 300, **config().get("retry", {})}


def db_settings() -> dict[str, Any]:
    return {"retention_days": 365, **(config().get("db") or {})}
