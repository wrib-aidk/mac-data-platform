"""Open DART: 공시대상 회사 목록, 회사별 정기보고서(사업·반기·분기) 본문 XML. API 인증키 필요.

원본: wrib-crawler/scripts/dart/fetch_corp_list.py, fetch_corp_filings.py
"""

import io
import logging
import re
import ssl
import threading
import xml.etree.ElementTree as ET
import zipfile
from datetime import date
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException

from .common import ItemResult, run_items, safe_write, write_csv

API = "https://opendart.fss.or.kr/api"
CORP_FIELDS = ["corp_code", "corp_name", "corp_eng_name", "stock_code", "modify_date"]


class OpenDartTLSAdapter(HTTPAdapter):
    def init_poolmanager(self, *args: object, **kwargs: object) -> None:
        ctx = ssl.create_default_context()
        ctx.set_ciphers("DEFAULT@SECLEVEL=1")  # SSLError - [SSL: DH_KEY_TOO_SMALL] dh key too small
        super().init_poolmanager(*args, **kwargs, ssl_context=ctx)


_local = threading.local()


def _get(endpoint: str, api_key: str, **params: object) -> requests.Response:
    if (s := getattr(_local, "session", None)) is None:
        s = _local.session = requests.Session()
        s.mount("https://opendart.fss.or.kr", OpenDartTLSAdapter())
    try:
        r = s.get(f"{API}/{endpoint}", params={"crtfc_key": api_key, **params}, timeout=120)
        r.raise_for_status()
    except RequestException as e:  # 예외 메시지의 URL에 인증키가 들어 있어 가림
        raise RuntimeError(str(e).replace(api_key, "***")) from None
    return r


def _raise_xml_error(content: bytes) -> None:
    """오류는 zip 대신 XML로 응답됨"""
    status = re.search(rb"<status>(\d+)</status>", content)
    message = re.search(rb"<message>(.*?)</message>", content, re.DOTALL)
    raise RuntimeError(f"status {status.group(1).decode() if status else 'unknown'}: {message.group(1).decode(errors='replace') if message else repr(content[:200])}")


def fetch_corp_list(api_key: str, out: Path) -> int:
    """종목코드가 있는 회사(상장폐지 포함)만 저장."""
    buf = io.BytesIO(_get("corpCode.xml", api_key).content)
    if not zipfile.is_zipfile(buf):
        _raise_xml_error(buf.getvalue())
    corps = ET.fromstring(zipfile.ZipFile(buf).read("CORPCODE.xml"))  # noqa: S314
    rows = [
        {
            "corp_code": corp.findtext("corp_code", "").strip(),
            "corp_name": corp.findtext("corp_name", "").strip(),
            "corp_eng_name": corp.findtext("corp_eng_name", "").strip(),
            "stock_code": ticker,
            "modify_date": corp.findtext("modify_date", "").strip(),
        }
        for corp in corps
        if (ticker := corp.findtext("stock_code", "").strip())
    ]
    write_csv(out, rows, CORP_FIELDS)
    return len(rows)


def fetch_corp_filings(api_key: str, corp_list: Path, out_dir: Path, *, log: logging.Logger, start: date, end: date, postfix: str = "", workers: int = 4, limit: int | None = None, retries: int = 2) -> ItemResult:
    """회사별 결산기별 최신 정기보고서 본문을 out_dir/<corp_code>_<YYYYMM>_<보고서>_<rcept_no>.xml<postfix> 로 저장."""
    import csv

    with corp_list.open(encoding="utf-8", newline="") as f:
        companies = list(csv.DictReader(f))[:limit]

    def _fetch_filings(company: dict[str, str]) -> None:
        filings = []
        for detail_ty in ("A001", "A002", "A003"):  # 사업, 반기, 분기보고서
            page_no = 1
            while True:
                data = _get(
                    "list.json",
                    api_key,
                    corp_code=company["corp_code"],
                    bgn_de=f"{start:%Y%m%d}",
                    end_de=f"{end:%Y%m%d}",
                    last_reprt_at="N",  # 최종본이 [첨부정정]이면 본문을 받을 수 없어 전체 이력에서 직접 고름
                    pblntf_detail_ty=detail_ty,
                    page_no=page_no,
                    page_count=100,
                ).json()
                if data["status"] == "013":  # 조회 결과 없음
                    break
                if data["status"] != "000":
                    raise RuntimeError(f"status {data['status']}: {data['message']}")
                filings += data["list"]
                if page_no >= data["total_page"]:
                    break
                page_no += 1

        latest, errors = {}, []
        for filing in sorted(filings, key=lambda f: f["rcept_no"]):
            if "첨부정정" in filing["report_nm"] or "연장신고서" in filing["report_nm"]:
                continue
            if m := re.search(r"(사업|반기|분기)보고서 \((\d{4})\.(\d{2})\)", filing["report_nm"]):
                latest[(f"{m.group(1)}보고서", m.group(2) + m.group(3))] = filing
            else:
                errors.append(f"보고서명 형식 오류 {filing['report_nm']}")

        for (report, fiscal_period), filing in latest.items():
            rcept_no = filing["rcept_no"]
            outfile = out_dir / f"{company['corp_code']}_{fiscal_period}_{report}_{rcept_no}.xml{postfix}"
            if outfile.exists():
                continue
            try:
                r = _get("document.xml", api_key, rcept_no=rcept_no)
                buf = io.BytesIO(r.content)
                if not zipfile.is_zipfile(buf):
                    _raise_xml_error(r.content)
                with zipfile.ZipFile(buf) as zf:
                    raw = zf.read(f"{rcept_no}.xml")  # 본문만 저장 (첨부 제외)
                try:
                    text = raw.decode("utf-8-sig")
                except UnicodeDecodeError:
                    text = re.sub(r'^(<\?xml[^>]*?encoding=")[^"]+', r"\1utf-8", raw.decode("cp949"), count=1)
                safe_write(outfile, text.encode("utf-8"))
                for old in out_dir.glob(f"{company['corp_code']}_{fiscal_period}_{report}_*.xml{postfix}"):  # 정정 전 판은 지움 (최신본만 유지)
                    if old != outfile:
                        old.unlink()
            except Exception as e:  # 한 문서 실패로 회사 전체가 멈추지 않게
                errors.append(f"{fiscal_period} {report}: {type(e).__name__}: {e}")
        if errors:
            raise RuntimeError("; ".join(errors))

    return run_items(companies, _fetch_filings, log=log, key=lambda c: f"{c['corp_code']}({c['corp_name']})", workers=workers, retries=retries)
