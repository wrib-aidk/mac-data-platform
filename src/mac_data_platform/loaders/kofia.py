"""KOFIA → raw.kofia_fund_nav / raw.kofia_holdings / raw.kofia_funds / raw.kofia_fund_fees"""

import sqlite3
from contextlib import closing
from datetime import datetime
from pathlib import Path

import psycopg

from mac_data_platform import db

from ._common import read_csv

NAV_COLUMNS = ["date", "nav", "nav_change", "tax_nav", "principal"]
HOLDING_COLUMNS = [
    "std_dt", "standard_cd", "line_no", "company_cd", "item_nm", "security_type", "quantity", "acquisition_amt",
    "market_amt", "book_amt", "issue_region", "listing", "issue_dt", "coupon_rate", "raw_json", "loaded_at",
]  # fmt: skip
FUND_COLUMNS = ["standard_cd", "company_cd", "fund_nm", "fund_type", "short_cd", "start_dt", "class_cd", "is_etf", "is_private", "updated_at", "is_class"]
_BOOL = {"is_etf", "is_private", "is_class"}
FEE_COLUMNS = [
    "standard_cd", "company_nm", "fund_nm", "fund_type", "start_dt", "manage_fee", "sale_fee", "trust_fee", "admin_fee",
    "total_fee", "peer_avg_fee", "other_expense", "ter", "front_load", "back_load", "trading_fee",
]  # fmt: skip
_FEE_TEXT = {"standard_cd", "company_nm", "fund_nm", "fund_type", "start_dt"}


def load_nav(conn: psycopg.Connection, funds_dir: Path, *, log, force: bool = False) -> dict:  # noqa: ANN001
    def _one(conn: psycopg.Connection, p: Path) -> int:
        code = p.parent.name
        rows = ([code, r["date"], *(db.num(r.get(c)) for c in NAV_COLUMNS[1:])] for r in read_csv(p) if r.get("date"))
        return db.replace_rows(conn, "raw.kofia_fund_nav", {"fund_code": code}, ["fund_code", *NAV_COLUMNS], rows)

    files = sorted(funds_dir.glob("*/nav.csv")) if funds_dir.exists() else []
    return db.load_items(conn, "raw.kofia_fund_nav", ((db.rel(p), db.fingerprint(p), p) for p in files), _one, log=log, force=force)


def load_fees(conn: psycopg.Connection, fees_dir: Path, *, log, force: bool = False) -> dict:  # noqa: ANN001
    """fees/<기준일>.csv → 기준일 단위로 교체."""

    def _one(conn: psycopg.Connection, p: Path) -> int:
        day = datetime.strptime(p.name[:8], "%Y%m%d").date()
        rows = ([day, *(db.text(r.get(c)) if c in _FEE_TEXT else db.num(r.get(c)) for c in FEE_COLUMNS)] for r in read_csv(p) if r.get("standard_cd"))
        return db.replace_rows(conn, "raw.kofia_fund_fees", {"standard_dt": day}, ["standard_dt", *FEE_COLUMNS], rows)

    files = sorted(fees_dir.glob("*.csv")) if fees_dir.exists() else []
    return db.load_items(conn, "raw.kofia_fund_fees", ((db.rel(p), db.fingerprint(p), p) for p in files), _one, log=log, force=force)


def load_holdings(conn: psycopg.Connection, sqlite_path: Path, *, log, force: bool = False) -> dict:  # noqa: ANN001
    """kofia_holdings.db 가 바뀌면 holdings 를 통째로 다시 넣음 (분기당 수십만 행 수준이라 충분히 빠름)."""

    def _one(conn: psycopg.Connection, p: Path) -> int:
        with closing(sqlite3.connect(f"file:{p}?mode=ro", uri=True)) as src:
            return db.replace_rows(conn, "raw.kofia_holdings", None, HOLDING_COLUMNS, src.execute(f"SELECT {', '.join(HOLDING_COLUMNS)} FROM holdings"))  # noqa: S608

    return db.load_items(conn, "raw.kofia_holdings", _items(sqlite_path), _one, log=log, force=force)


def load_funds(conn: psycopg.Connection, sqlite_path: Path, *, log, force: bool = False) -> dict:  # noqa: ANN001
    """kofia_holdings.db 가 바뀌면 funds(펀드 목록)를 통째로 다시 넣음."""

    def _one(conn: psycopg.Connection, p: Path) -> int:
        with closing(sqlite3.connect(f"file:{p}?mode=ro", uri=True)) as src:
            cols = [c for c in FUND_COLUMNS if c in {r[1] for r in src.execute("PRAGMA table_info(funds)")}]  # 예전 DB 에는 is_class 가 없음
            funds = ([bool(v) if c in _BOOL and v is not None else v for c, v in zip(cols, r, strict=True)] for r in src.execute(f"SELECT {', '.join(cols)} FROM funds"))  # noqa: S608
            return db.replace_rows(conn, "raw.kofia_funds", None, cols, funds)

    return db.load_items(conn, "raw.kofia_funds", _items(sqlite_path), _one, log=log, force=force)


def _items(sqlite_path: Path) -> list:
    return [(db.rel(sqlite_path), db.fingerprint(sqlite_path), sqlite_path)] if sqlite_path.exists() else []
