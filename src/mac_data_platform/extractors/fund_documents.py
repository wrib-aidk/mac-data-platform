"""펀드 공시 PDF → 텍스트(.md) 변환.

원본: wrib-crawler/scripts/fundsupermarket/convert_pdf_to_txt.py (pypdfium2, 레이아웃 분석 없음)
변환 방식(convert_pdf_to_text)은 원본과 한 글자도 다르지 않게 유지한다. HF 의 기존 .md 도 이 방식으로 만들어졌다.

원본과 다른 점: 원본은 .txt 가 이미 있으면 건너뛰어서 공시가 바뀌어도 텍스트가 예전 판으로 남았다.
여기서는 변환할 때의 PDF 지문(크기·수정시각)을 <out>/<fund_code>/.text.json 에 적어 두고,
PDF 가 바뀌었거나(수집 단계가 덮어씀) 기록이 없으면 다시 변환한다.
"""

import json
import os
from collections.abc import Callable
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import pypdfium2 as pdfium

from mac_data_platform.collectors.common import ItemResult, safe_write

MANIFEST = ".text.json"  # {"<문서>.pdf": {"pdf_size": ..., "pdf_mtime_ns": ...}}


def convert_pdf_to_text(pdf_file: Path) -> str:
    """원본 convert_pdf_to_txt() 와 같은 결과."""
    doc = pdfium.PdfDocument(pdf_file.read_bytes())
    try:
        return "\n\n".join(page.get_textpage().get_text_bounded().replace("\r\n", "\n").strip() for page in doc) + "\n"
    finally:
        doc.close()


def _fingerprint(pdf: Path) -> dict[str, int]:
    st = pdf.stat()
    return {"pdf_size": st.st_size, "pdf_mtime_ns": st.st_mtime_ns}


def _convert_fund(fund_dir: Path, out_dir: Path, force: bool) -> dict[str, object]:
    """펀드 1개: 바뀐 PDF 만 변환. 별도 프로세스에서 실행 (pdfium 은 스레드 안전하지 않음)."""
    manifest_path = out_dir / MANIFEST
    manifest = json.loads(manifest_path.read_text(encoding="utf-8")) if manifest_path.exists() else {}
    stats: dict[str, object] = {"converted": [], "unchanged": 0, "failed": {}}
    for pdf in sorted(fund_dir.glob("*.pdf")):
        fp = _fingerprint(pdf)
        out = out_dir / f"{pdf.stem}.md"
        if not force and out.exists() and manifest.get(pdf.name) == fp:
            stats["unchanged"] += 1
            continue
        try:
            safe_write(out, convert_pdf_to_text(pdf).encode("utf-8"))
            manifest[pdf.name] = fp
            stats["converted"].append(pdf.stem)
        except Exception as e:  # noqa: BLE001 — 깨진 PDF 하나 때문에 펀드 전체를 실패시키지 않음
            stats["failed"][pdf.name] = f"{type(e).__name__}: {e}"
    if stats["converted"]:
        safe_write(manifest_path, json.dumps(manifest, ensure_ascii=False, indent=1).encode("utf-8"))
    return stats


def extract_fund_documents(
    raw_dir: Path,
    out_dir: Path,
    *,
    log,  # noqa: ANN001
    workers: int | None = None,
    limit: int | None = None,
    force: bool = False,
    on_progress: Callable[[int, int], None] | None = None,
) -> ItemResult:
    """raw_dir/<fund_code>/*.pdf → out_dir/<fund_code>/<문서>.md"""
    funds = sorted(p for p in raw_dir.iterdir() if p.is_dir())[:limit]
    result = ItemResult(total=len(funds))
    workers = workers or os.cpu_count() or 4
    done = 0
    with ProcessPoolExecutor(workers) as pool:
        futures = {pool.submit(_convert_fund, f, out_dir / f.name, force): f.name for f in funds}
        for future in as_completed(futures):
            code = futures[future]
            try:
                stats = future.result()
            except Exception as e:  # noqa: BLE001
                result.failed[code] = f"{type(e).__name__}: {e}"
                log.error(f"{code}: 변환 실패 {type(e).__name__}: {e}")
            else:
                result.count("docs_converted", len(stats["converted"]))
                result.count("docs_unchanged", stats["unchanged"])
                if stats["failed"]:
                    result.count("docs_failed", len(stats["failed"]))
                    result.failed[code] = "; ".join(f"{k}: {v}" for k, v in stats["failed"].items())
                    log.warning(f"{code}: {result.failed[code]}")
                if stats["converted"]:
                    result.ok += 1  # 1개 이상 새로 변환
                elif not stats["failed"]:
                    result.skipped += 1  # 바뀐 PDF 없음
            done += 1
            if done % 200 == 0 or done == len(funds):
                log.info(f"진행 {done}/{len(funds)} ({result.summary()}, {result.counters})")
    return result
