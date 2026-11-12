"""펀드슈퍼마켓 → raw.fundsupermarket_* / extracted.fundsupermarket_document_text"""

import json
from pathlib import Path

import psycopg

from mac_data_platform import db

from ._common import file_date, file_time, purge_older_than, read_csv


def load_fund_list(conn: psycopg.Connection, path: Path, *, log, retention_days: int, force: bool = False) -> dict:  # noqa: ANN001
    """fund_list.csv → 수집일 스냅샷. 같은 날 다시 받으면 그날 스냅샷을 교체."""

    def _one(conn: psycopg.Connection, p: Path) -> int:
        snap = file_date(p)
        rows = ([snap, r["fund_code"], r.get("fund_name"), r] for r in read_csv(p))
        return db.replace_rows(conn, "raw.fundsupermarket_fund_list", {"snapshot_date": snap}, ["snapshot_date", "fund_code", "fund_name", "data"], rows)

    stats = db.load_items(conn, "raw.fundsupermarket_fund_list", [(db.rel(path), db.fingerprint(path), path)] if path.exists() else [], _one, log=log, force=force)
    stats["snapshots_purged"] = purge_older_than(conn, "raw.fundsupermarket_fund_list", "snapshot_date", retention_days)
    return stats


def load_fund_details(conn: psycopg.Connection, funds_dir: Path, *, log, retention_days: int, force: bool = False) -> dict:  # noqa: ANN001
    """funds/<code>/details.json → 수집일 스냅샷 (펀드·날짜별 1행)."""

    def _one(conn: psycopg.Connection, p: Path) -> int:
        data = json.loads(p.read_text(encoding="utf-8"))
        snap = file_date(p)
        code = p.parent.name
        return db.replace_rows(conn, "raw.fundsupermarket_fund_details", {"fund_code": code, "snapshot_date": snap}, ["fund_code", "snapshot_date", "fund_name", "data"], [[code, snap, data.get("fund_name"), data]])

    files = sorted(funds_dir.glob("*/details.json"))
    stats = db.load_items(conn, "raw.fundsupermarket_fund_details", ((db.rel(p), db.fingerprint(p), p) for p in files), _one, log=log, force=force)
    stats["snapshots_purged"] = purge_older_than(conn, "raw.fundsupermarket_fund_details", "snapshot_date", retention_days)
    return stats


DOC_COLUMNS = ["fund_code", "doc_type", "path", "size", "file_mtime", "doc_date", "text_sha"]


def load_documents(conn: psycopg.Connection, funds_dir: Path, *, log, force: bool = False) -> dict:  # noqa: ANN001
    """funds/<code>/*.pdf → 문서 목록 (펀드 단위로 교체). 문서 날짜·해시는 .docs.json 에 있을 때만."""

    def _one(conn: psycopg.Connection, fund_dir: Path) -> int:
        manifest_path = fund_dir / ".docs.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8")) if manifest_path.exists() else {}
        rows = []
        for pdf in sorted(fund_dir.glob("*.pdf")):
            m = manifest.get(pdf.name) or manifest.get(pdf.stem) or {}
            rows.append([fund_dir.name, pdf.stem, db.rel(pdf), pdf.stat().st_size, file_time(pdf), m.get("doc_date") or None, m.get("text_sha")])
        return db.replace_rows(conn, "raw.fundsupermarket_documents", {"fund_code": fund_dir.name}, DOC_COLUMNS, rows)

    dirs = sorted(p for p in funds_dir.iterdir() if p.is_dir())
    items = ((db.rel(d), db.fingerprint(*sorted(d.glob("*.pdf")), d / ".docs.json"), d) for d in dirs)
    return db.load_items(conn, "raw.fundsupermarket_documents", items, _one, log=log, force=force)


def load_document_text(conn: psycopg.Connection, text_dir: Path, *, log, force: bool = False) -> dict:  # noqa: ANN001
    """extracted/.../funds/<code>/*.md → extracted.fundsupermarket_document_text (펀드 단위로 교체)."""

    def _one(conn: psycopg.Connection, fund_dir: Path) -> int:
        rows = []
        for md in sorted(fund_dir.glob("*.md")):
            body = md.read_text(encoding="utf-8", errors="replace").replace("\x00", "")
            rows.append([fund_dir.name, md.stem, body, len(body), file_time(md)])
        return db.replace_rows(conn, "extracted.fundsupermarket_document_text", {"fund_code": fund_dir.name}, ["fund_code", "doc_type", "text", "chars", "updated_at"], rows)

    dirs = sorted(p for p in text_dir.iterdir() if p.is_dir()) if text_dir.exists() else []
    items = ((db.rel(d), db.fingerprint(*sorted(d.glob("*.md"))), d) for d in dirs)
    return db.load_items(conn, "extracted.fundsupermarket_document_text", items, _one, log=log, force=force)
