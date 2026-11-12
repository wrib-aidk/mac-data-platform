"""수집기 공통 도구: 항목별 재시도 실행, 안전한 파일 쓰기.

수집기 모듈(collectors/*)은 Dagster를 모른다. 로그는 logging.Logger 호환 객체(log)를 받아서 쓴다.
"""

import csv
import io
import logging
import random
import threading
import time
from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


class Skipped(Exception):  # noqa: N818
    """이미 받은 항목이라 건너뜀 (재시도 대상 아님)."""


class Empty(Exception):  # noqa: N818
    """서버에 데이터가 없음 (재시도 대상 아님, 실패로 세지 않음)."""


@dataclass
class ItemResult:
    total: int = 0
    ok: int = 0
    skipped: int = 0
    empty: int = 0
    failed: dict[str, str] = field(default_factory=dict)  # 항목 → 마지막 오류
    counters: dict[str, int] = field(default_factory=dict)  # 수집기별 추가 통계 (예: 갱신된 문서 수)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def count(self, name: str, n: int = 1) -> None:
        with self._lock:
            self.counters[name] = self.counters.get(name, 0) + n

    def summary(self) -> dict[str, int]:
        return {"total": self.total, "ok": self.ok, "skipped": self.skipped, "empty": self.empty, "failed": len(self.failed)}


def run_items(
    items: Iterable[Any],
    fn: Callable[[Any], object],
    *,
    log: logging.Logger,
    key: Callable[[Any], str] = str,
    workers: int = 4,
    retries: int = 2,
    backoff: float = 5.0,
    result: ItemResult | None = None,
) -> ItemResult:
    """items 각각에 fn 실행. 실패하면 지수 백오프로 retries번 더 시도하고, 그래도 실패하면 result.failed에 기록.

    한 항목이 실패해도 나머지는 계속 받는다 (원본 스크립트와 같은 동작).
    """
    items = list(items)
    result = result if result is not None else ItemResult()
    result.total = len(items)

    def _one(item: Any) -> str:
        for attempt in range(retries + 1):
            try:
                fn(item)
                return "ok"
            except Skipped:
                return "skipped"
            except Empty as e:
                log.info(f"{key(item)}: 데이터 없음 ({e})")
                return "empty"
            except Exception as e:
                if attempt == retries:
                    raise
                wait = backoff * 2**attempt * random.uniform(0.8, 1.2)  # noqa: S311
                log.warning(f"{key(item)}: {type(e).__name__}: {e} → {wait:.0f}초 후 재시도 ({attempt + 1}/{retries})")
                time.sleep(wait)
        raise AssertionError("unreachable")

    done = 0
    with ThreadPoolExecutor(max(1, workers)) as pool:
        futures = {pool.submit(_one, item): key(item) for item in items}
        for future in as_completed(futures):
            name = futures[future]
            try:
                status = future.result()
                setattr(result, status, getattr(result, status) + 1)
            except Exception as e:
                result.failed[name] = f"{type(e).__name__}: {e}"
                log.error(f"{name}: 최종 실패 {type(e).__name__}: {e}")
            done += 1
            if done % 100 == 0 or done == len(items):
                log.info(f"진행 {done}/{len(items)} ({result.summary()})")
    return result


def safe_write(path: Path, data: bytes) -> None:
    """임시 파일에 쓰고 rename (중간에 끊겨도 반쪽 파일이 남지 않게)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".part")
    tmp.write_bytes(data)
    tmp.replace(path)


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str] | None = None) -> None:
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=fieldnames or list(rows[0]), lineterminator="\n")
    w.writeheader()
    w.writerows(rows)
    safe_write(path, buf.getvalue().encode("utf-8"))


def read_csv_column(path: Path, column: str) -> list[str]:
    with path.open(encoding="utf-8", newline="") as f:
        return [row[column] for row in csv.DictReader(f)]


def merge_rows(path: Path, new_rows: list[dict[str, Any]], key: str, fieldnames: list[str]) -> int:
    """기존 CSV 에 new_rows 를 key 기준으로 합쳐 덮어씀 (같은 key 는 새 값 우선). 새로 추가·변경된 행 수를 돌려줌."""
    old = {}
    if path.exists():
        with path.open(encoding="utf-8", newline="") as f:
            old = {row[key]: row for row in csv.DictReader(f)}
    changed = sum(1 for row in new_rows if old.get(row[key]) != row)
    if changed:
        merged = {**old, **{row[key]: row for row in new_rows}}
        write_csv(path, [merged[k] for k in sorted(merged)], fieldnames)
    return changed


def last_value(path: Path, column: str) -> str | None:
    """CSV 마지막 행의 column 값 (정렬된 시계열의 마지막 날짜 확인용)."""
    if not path.exists():
        return None
    last = None
    with path.open(encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            last = row[column]
    return last
