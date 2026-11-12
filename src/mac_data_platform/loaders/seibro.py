"""SEIBro → raw.seibro_funds / raw.seibro_fund_nav"""

from pathlib import Path

import psycopg

from mac_data_platform import db

from ._common import read_csv

NAV_COLUMNS = ["date", "nav", "nav_change", "nav_change_pct", "tax_nav", "principal", "net_asset", "dividend", "note"]
_NUMERIC = set(NAV_COLUMNS) - {"date", "note"}


def load_funds(conn: psycopg.Connection, path: Path, *, log, force: bool = False) -> dict:  # noqa: ANN001
    def _one(conn: psycopg.Connection, p: Path) -> int:
        rows = {r["isin"]: [r["isin"], db.text(r.get("fund_name")), db.text(r.get("fund_class_code"))] for r in read_csv(p) if r.get("isin")}
        return db.replace_rows(conn, "raw.seibro_funds", None, ["isin", "fund_name", "fund_class_code"], rows.values())

    return db.load_items(conn, "raw.seibro_funds", [(db.rel(path), db.fingerprint(path), path)] if path.exists() else [], _one, log=log, force=force)


def load_nav(conn: psycopg.Connection, funds_dir: Path, *, log, force: bool = False) -> dict:  # noqa: ANN001
    """funds/<isin>/nav.csv → 펀드(ISIN) 단위로 교체."""

    def _one(conn: psycopg.Connection, p: Path) -> int:
        isin = p.parent.name
        rows = (
            [isin, *((db.num(r.get(c)) if c in _NUMERIC else db.text(r.get(c))) for c in NAV_COLUMNS)]
            for r in read_csv(p) if r.get("date")
        )
        return db.replace_rows(conn, "raw.seibro_fund_nav", {"isin": isin}, ["isin", *NAV_COLUMNS], rows)

    files = sorted(funds_dir.glob("*/nav.csv"))
    return db.load_items(conn, "raw.seibro_fund_nav", ((db.rel(p), db.fingerprint(p), p) for p in files), _one, log=log, force=force)
