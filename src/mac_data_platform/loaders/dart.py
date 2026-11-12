"""DART → raw.dart_corps / raw.dart_filings"""

import re
from pathlib import Path

import psycopg

from mac_data_platform import db

from ._common import read_csv

CORP_COLUMNS = ["corp_code", "corp_name", "corp_eng_name", "stock_code", "modify_date"]
_FILING = re.compile(r"^(?P<corp>\d{8})_(?P<period>\d{6})_(?P<report>[^_]+)_(?P<rcept>\d+)\.xml$")


def load_corps(conn: psycopg.Connection, path: Path, *, log, force: bool = False) -> dict:  # noqa: ANN001
    def _one(conn: psycopg.Connection, p: Path) -> int:
        rows = {r["corp_code"]: [db.text(r.get(c)) for c in CORP_COLUMNS] for r in read_csv(p)}
        return db.replace_rows(conn, "raw.dart_corps", None, CORP_COLUMNS, rows.values())

    return db.load_items(conn, "raw.dart_corps", [(db.rel(path), db.fingerprint(path), path)] if path.exists() else [], _one, log=log, force=force)


def load_filings(conn: psycopg.Connection, filings_dir: Path, *, log, force: bool = False) -> dict:  # noqa: ANN001
    """filings/<corp>_<YYYYMM>_<보고서>_<접수번호>.xml 목록 → 매번 전체 재작성 (파일 목록만이라 가벼움)."""
    rows = []
    for f in sorted(filings_dir.glob("*.xml")) if filings_dir.exists() else []:
        if m := _FILING.match(f.name):
            rows.append([m["corp"], m["period"], m["report"], m["rcept"], db.rel(f), f.stat().st_size])
    n = db.replace_rows(conn, "raw.dart_filings", None, ["corp_code", "fiscal_period", "report", "rcept_no", "path", "size"], rows)
    conn.commit()
    log.info(f"raw.dart_filings: {n}건")
    return {"items_total": n, "items_loaded": n, "rows_loaded": n}
