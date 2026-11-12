"""KRX → raw.krx_* / extracted.krx_* (HF 정제본 parquet)"""

from pathlib import Path

import psycopg
import pyarrow.parquet as pq

from mac_data_platform import db

from ._common import read_csv

_TEXT = {"isu_cd", "isu_nm", "mkt_nm", "sect_tp_nm", "idx_ind_nm"}


def load_stock_list(conn: psycopg.Connection, path: Path, *, log, force: bool = False) -> dict:  # noqa: ANN001
    def _one(conn: psycopg.Connection, p: Path) -> int:
        rows = {r["ISU_CD"]: [r["ISU_CD"], r] for r in read_csv(p) if r.get("ISU_CD")}
        return db.replace_rows(conn, "raw.krx_stock_list", None, ["isu_cd", "data"], rows.values())

    return db.load_items(conn, "raw.krx_stock_list", [(db.rel(path), db.fingerprint(path), path)] if path.exists() else [], _one, log=log, force=force)


def load_daily(conn: psycopg.Connection, quotes_dir: Path, kind: str, *, log, force: bool = False) -> dict:  # noqa: ANN001
    """quotes/<YYYYMMDD>_<kind>.csv → raw.krx_<kind>_daily (날짜 단위로 교체). 컬럼은 API 이름 소문자."""
    table = f"raw.krx_{kind}_daily"
    columns = [c for c, _ in db.table_columns(conn, table)]

    def _one(conn: psycopg.Connection, p: Path) -> int:
        day = p.name.split("_")[0]
        rows = []
        for r in read_csv(p):
            low = {k.lower(): v for k, v in r.items()}
            if not low.get("isu_cd"):
                continue
            rows.append([day if c == "bas_dd" else (db.text(low.get(c)) if c in _TEXT else db.num(low.get(c))) for c in columns])
        return db.replace_rows(conn, table, {"bas_dd": day}, columns, rows)

    files = sorted(quotes_dir.glob(f"*_{kind}.csv"))
    return db.load_items(conn, table, ((db.rel(p), db.fingerprint(p), p) for p in files), _one, log=log, force=force)


def load_etf_constituents(conn: psycopg.Connection, base_dir: Path, *, log, force: bool = False) -> dict:  # noqa: ANN001
    """etf_constituents/<YYYYMMDD>/<ETF ISIN>.csv → (날짜, ETF) 단위로 교체."""

    def _one(conn: psycopg.Connection, p: Path) -> int:
        day, isin = p.parent.name, p.stem
        rows = ([day, isin, i, r] for i, r in enumerate(read_csv(p), 1))
        return db.replace_rows(conn, "raw.krx_etf_constituents", {"trade_date": day, "etf_isin": isin}, ["trade_date", "etf_isin", "line_no", "data"], rows)

    files = sorted(base_dir.glob("*/*.csv")) if base_dir.exists() else []
    return db.load_items(conn, "raw.krx_etf_constituents", ((db.rel(p), db.fingerprint(p), p) for p in files), _one, log=log, force=force)


def load_parquet(conn: psycopg.Connection, path: Path, table: str, *, log, force: bool = False) -> dict:  # noqa: ANN001
    """parquet 파일이 바뀌면 테이블을 통째로 다시 넣음. 컬럼은 테이블 정의 순서로 맞춤."""
    columns = [c for c, _ in db.table_columns(conn, table)]

    def _one(conn: psycopg.Connection, p: Path) -> int:
        f = pq.ParquetFile(p)

        def rows():  # noqa: ANN202
            for batch in f.iter_batches(batch_size=50_000, columns=columns):
                cols = [batch.column(c).to_pylist() for c in columns]
                yield from zip(*cols, strict=True)

        return db.replace_rows(conn, table, None, columns, rows())

    return db.load_items(conn, table, [(db.rel(path), db.fingerprint(path), path)] if path.exists() else [], _one, log=log, force=force)
