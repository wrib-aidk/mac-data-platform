"""KOFIA → raw.kofia_fund_nav / raw.kofia_holdings / raw.kofia_funds"""

import sqlite3
from pathlib import Path

import psycopg

from mac_data_platform import db

from ._common import read_csv

NAV_COLUMNS = ["date", "nav", "nav_change", "tax_nav", "principal"]
HOLDING_COLUMNS = [
    "std_dt", "standard_cd", "line_no", "company_cd", "item_nm", "security_type", "quantity", "acquisition_amt",
    "market_amt", "book_amt", "issue_region", "listing", "issue_dt", "coupon_rate", "raw_json", "loaded_at",
]  # fmt: skip
FUND_COLUMNS = ["standard_cd", "company_cd", "fund_nm", "fund_type", "short_cd", "start_dt", "class_cd", "is_etf", "is_private", "updated_at"]


def load_nav(conn: psycopg.Connection, funds_dir: Path, *, log, force: bool = False) -> dict:  # noqa: ANN001
    def _one(conn: psycopg.Connection, p: Path) -> int:
        code = p.parent.name
        rows = ([code, r["date"], *(db.num(r.get(c)) for c in NAV_COLUMNS[1:])] for r in read_csv(p) if r.get("date"))
        return db.replace_rows(conn, "raw.kofia_fund_nav", {"fund_code": code}, ["fund_code", *NAV_COLUMNS], rows)

    files = sorted(funds_dir.glob("*/nav.csv")) if funds_dir.exists() else []
    return db.load_items(conn, "raw.kofia_fund_nav", ((db.rel(p), db.fingerprint(p), p) for p in files), _one, log=log, force=force)


def load_holdings(conn: psycopg.Connection, sqlite_path: Path, *, log, force: bool = False) -> dict:  # noqa: ANN001
    """kofia_holdings.db 가 바뀌면 holdings·funds 를 통째로 다시 넣음 (분기당 수십만 행 수준이라 충분히 빠름)."""

    def _one(conn: psycopg.Connection, p: Path) -> int:
        src = sqlite3.connect(f"file:{p}?mode=ro", uri=True)
        try:
            n = db.replace_rows(conn, "raw.kofia_holdings", None, HOLDING_COLUMNS, src.execute(f"SELECT {', '.join(HOLDING_COLUMNS)} FROM holdings"))  # noqa: S608
            funds = ([*r[:7], bool(r[7]) if r[7] is not None else None, bool(r[8]) if r[8] is not None else None, r[9]] for r in src.execute(f"SELECT {', '.join(FUND_COLUMNS)} FROM funds"))  # noqa: S608
            db.replace_rows(conn, "raw.kofia_funds", None, FUND_COLUMNS, funds)
        finally:
            src.close()
        return n

    return db.load_items(conn, "raw.kofia_holdings", [(db.rel(sqlite_path), db.fingerprint(sqlite_path), sqlite_path)] if sqlite_path.exists() else [], _one, log=log, force=force)
