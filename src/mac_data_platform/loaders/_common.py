import csv
from datetime import date, datetime
from pathlib import Path

import psycopg

from mac_data_platform import settings


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def file_date(path: Path) -> date:
    """파일 수정일 (= 수집일, Asia/Seoul)."""
    return datetime.fromtimestamp(path.stat().st_mtime, settings.tz()).date()


def file_time(path: Path) -> datetime:
    return datetime.fromtimestamp(path.stat().st_mtime, settings.tz())


def purge_older_than(conn: psycopg.Connection, table: str, column: str, days: int) -> int:
    """보존 기간이 지난 스냅샷 삭제."""
    n = conn.execute(f"DELETE FROM {table} WHERE {column} < current_date - %s::int", (days,)).rowcount  # noqa: S608 — 코드 상수만 들어옴
    conn.commit()
    return n
