"""PostgreSQL 공통 도구 (Dagster 를 모름). Dagster 에서는 defs/resources.py 의 PostgresResource 로 씀.

    with connect() as conn:                       # 처음 연결할 때 sql/*.sql 마이그레이션 자동 적용
        replace_rows(conn, "raw.seibro_fund_nav", {"isin": isin}, COLUMNS, rows)   # 해당 키 행을 지우고 새로 COPY
        conn.commit()

증분 적재: FileTracker 가 파일 지문(크기·수정시각)을 etl.loaded_files 에 기록 → 바뀐 파일만 다시 넣음 (load_files).
"""

import json
import os
from collections.abc import Callable, Iterable, Iterator, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from . import settings

SQL_DIR = settings.PROJECT_ROOT / "sql"
_migrated = False


def dsn() -> str:
    env = os.environ.get
    return (
        f"host={env('POSTGRES_HOST', '127.0.0.1')} port={env('POSTGRES_PORT', '5432')} "
        f"dbname={env('POSTGRES_DB', 'platform')} user={env('POSTGRES_USER', 'platform')} password={env('POSTGRES_PASSWORD', '')}"
    )


@contextmanager
def connect() -> Iterator[psycopg.Connection]:
    global _migrated  # noqa: PLW0603
    with psycopg.connect(dsn(), application_name="mac-data-platform") as conn:
        if not _migrated:
            migrate(conn)
            _migrated = True
        yield conn


def migrate(conn: psycopg.Connection) -> list[str]:
    """sql/NNN_*.sql 중 아직 적용 안 된 것을 번호 순서대로 한 번씩 실행.

    여러 asset 이 동시에 처음 연결해도 한 프로세스만 적용하도록 advisory lock (트랜잭션 끝나면 자동 해제).
    """
    conn.execute("SELECT pg_advisory_xact_lock(hashtext('mac_data_platform.migrate'))")
    conn.execute("CREATE SCHEMA IF NOT EXISTS etl")
    conn.execute("CREATE TABLE IF NOT EXISTS etl.schema_migrations (version text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())")
    done = {r[0] for r in conn.execute("SELECT version FROM etl.schema_migrations")}
    applied = []
    for f in sorted(SQL_DIR.glob("[0-9]*.sql")):
        if f.stem in done:
            continue
        conn.execute(f.read_text(encoding="utf-8"))
        conn.execute("INSERT INTO etl.schema_migrations (version) VALUES (%s)", (f.stem,))
        applied.append(f.stem)
    conn.commit()
    return applied


def _ident(table: str) -> sql.Identifier:
    return sql.Identifier(*table.split("."))


def copy_rows(conn: psycopg.Connection, table: str, columns: Sequence[str], rows: Iterable[Sequence[Any]]) -> int:
    """COPY 로 빠르게 넣기. rows 는 columns 순서의 값 목록 (None = NULL, dict/list 는 jsonb 로)."""
    n = 0
    stmt = sql.SQL("COPY {} ({}) FROM STDIN").format(_ident(table), sql.SQL(", ").join(map(sql.Identifier, columns)))
    with conn.cursor() as cur, cur.copy(stmt) as cp:
        for row in rows:
            cp.write_row([json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list)) else v for v in row])
            n += 1
    return n


def delete_rows(conn: psycopg.Connection, table: str, where: dict[str, Any] | None = None) -> int:
    """where 가 None 이면 테이블 전체 삭제."""
    if where is None:
        conn.execute(sql.SQL("TRUNCATE {}").format(_ident(table)))
        return -1
    cond = sql.SQL(" AND ").join(sql.SQL("{} = %s").format(sql.Identifier(k)) for k in where)
    return conn.execute(sql.SQL("DELETE FROM {} WHERE {}").format(_ident(table), cond), list(where.values())).rowcount


def replace_rows(conn: psycopg.Connection, table: str, where: dict[str, Any] | None, columns: Sequence[str], rows: Iterable[Sequence[Any]]) -> int:
    """where 에 해당하는 행(=파일 하나, 펀드 하나, 날짜 하나 등)을 지우고 rows 로 교체."""
    delete_rows(conn, table, where)
    return copy_rows(conn, table, columns, rows)


# ---------------------------------------------------------------------------- 값 정리
def num(v: str | None) -> str | None:
    """'1,234.5' → '1234.5', '' / '-' → NULL."""
    if v is None:
        return None
    v = v.strip().replace(",", "")
    return None if v in ("", "-") else v


def text(v: str | None) -> str | None:
    if v is None:
        return None
    v = v.replace("\x00", "").strip()
    return v or None


# ---------------------------------------------------------------------------- 증분 적재
def fingerprint(*paths: Path) -> str:
    parts = []
    for p in paths:
        st = p.stat() if p.exists() else None
        parts.append(f"{p.name}:{st.st_size}:{st.st_mtime_ns}" if st else f"{p.name}:-")
    return "|".join(parts)


def rel(path: Path) -> str:
    try:
        return str(path.relative_to(settings.DATA_DIR))
    except ValueError:
        return str(path)


class FileTracker:
    """etl.loaded_files: (테이블, 항목) → 마지막으로 넣었을 때의 지문."""

    def __init__(self, conn: psycopg.Connection, table: str) -> None:
        self.conn, self.table = conn, table
        self.known = dict(conn.execute("SELECT item, fingerprint FROM etl.loaded_files WHERE table_name = %s", (table,)).fetchall())

    def is_current(self, item: str, fp: str) -> bool:
        return self.known.get(item) == fp

    def mark(self, item: str, fp: str) -> None:
        self.conn.execute(
            "INSERT INTO etl.loaded_files (table_name, item, fingerprint) VALUES (%s, %s, %s) "
            "ON CONFLICT (table_name, item) DO UPDATE SET fingerprint = EXCLUDED.fingerprint, loaded_at = now()",
            (self.table, item, fp),
        )
        self.known[item] = fp


def load_items(
    conn: psycopg.Connection,
    table: str,
    items: Iterable[tuple[str, str, Any]],
    load_one: Callable[[psycopg.Connection, Any], int],
    *,
    log,  # noqa: ANN001
    force: bool = False,
    commit_every: int = 200,
) -> dict[str, int]:
    """items: (항목 이름, 지문, load_one 에 넘길 값). 지문이 바뀐 항목만 load_one 실행 (행 교체) 후 기록.

    commit_every 개마다 커밋하므로 중간에 끊겨도 다음 실행 때 남은 것부터 이어서 넣는다.
    """
    tracker = FileTracker(conn, table)
    items = list(items)
    todo = [(name, fp, arg) for name, fp, arg in items if force or not tracker.is_current(name, fp)]
    rows = 0
    for i, (name, fp, arg) in enumerate(todo, 1):
        rows += load_one(conn, arg)
        tracker.mark(name, fp)
        if i % commit_every == 0:
            conn.commit()
            log.info(f"{table}: {i}/{len(todo)} 적재 ({rows:,}행)")
    conn.commit()
    if todo:
        log.info(f"{table}: {len(todo)}/{len(items)}개 항목 적재 ({rows:,}행)")
    return {"items_total": len(items), "items_loaded": len(todo), "rows_loaded": rows}


# ---------------------------------------------------------------------------- 조회 (UI 메타데이터용)
def table_columns(conn: psycopg.Connection, table: str) -> list[tuple[str, str]]:
    schema, name = table.split(".")
    return conn.execute(
        "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position",
        (schema, name),
    ).fetchall()


def column_comments(conn: psycopg.Connection, table: str) -> dict[str, str]:
    """COMMENT ON COLUMN 으로 넣은 컬럼 설명 (sql/005_column_comments.sql)."""
    rows = conn.execute(
        "SELECT a.attname, col_description(a.attrelid, a.attnum) FROM pg_attribute a "
        "WHERE a.attrelid = %s::regclass AND a.attnum > 0 AND NOT a.attisdropped",
        (table,),
    ).fetchall()
    return {name: desc for name, desc in rows if desc}


def column_examples(conn: psycopg.Connection, table: str, columns: Sequence[str], sample: int = 200, max_len: int = 80) -> dict[str, str]:
    """컬럼별 예시값 1개: 앞 sample 행 중 처음으로 비어 있지 않은 값 (긴 값은 max_len 자에서 자름)."""
    with conn.cursor(row_factory=dict_row) as cur:
        rows = cur.execute(sql.SQL("SELECT * FROM {} LIMIT {}").format(_ident(table), sql.Literal(sample))).fetchall()
    out = {}
    for c in columns:
        if (v := next((r[c] for r in rows if r.get(c) not in (None, "")), None)) is not None:
            t = json.dumps(v, ensure_ascii=False, default=str) if isinstance(v, (dict, list)) else str(v)
            out[c] = t if len(t) <= max_len else t[: max_len - 1] + "…"
    return out


def table_count(conn: psycopg.Connection, table: str) -> int:
    return conn.execute(sql.SQL("SELECT count(*) FROM {}").format(_ident(table))).fetchone()[0]


def table_head(conn: psycopg.Connection, table: str, n: int = 5, order_by: str | None = None) -> list[dict[str, Any]]:
    q = sql.SQL("SELECT * FROM {}").format(_ident(table))
    if order_by:
        q += sql.SQL(" ORDER BY ") + sql.SQL(order_by)
    q += sql.SQL(" LIMIT {}").format(sql.Literal(n))
    with conn.cursor(row_factory=dict_row) as cur:
        return cur.execute(q).fetchall()
