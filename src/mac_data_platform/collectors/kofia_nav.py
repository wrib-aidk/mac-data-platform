"""금융투자협회 전자공시(dis.kofia.or.kr): 펀드코드별 일별 NAV. ⚠️ 외부망 PC에서는 접속 불가.

원본: wrib-crawler/scripts/kofia_dis/fetch_fund_nav.py
"""

import logging
import ssl
import threading
import xml.etree.ElementTree as ET
from datetime import date, datetime, timedelta
from pathlib import Path
from xml.sax.saxutils import escape

import requests
from requests.adapters import HTTPAdapter

from .common import ItemResult, last_value, merge_rows, read_csv_column, run_items

URL = "https://dis.kofia.or.kr/proframeWeb/XMLSERVICES/"
BODY = """<?xml version="1.0" encoding="utf-8"?>
<message>
  <proframeHeader>
    <pfmAppName>FS-COM</pfmAppName>
    <pfmSvcName>COMFundPriceModSO</pfmSvcName>
    <pfmFnName>priceModSrch</pfmFnName>
  </proframeHeader>
  <systemHeader></systemHeader>
  <COMFundUnityInfoInputDTO>
    <standardCd>{standard_cd}</standardCd>
    <companyCd></companyCd>
    <vSrchTrmFrom>{start}</vSrchTrmFrom>
    <vSrchTrmTo>{end}</vSrchTrmTo>
    <vSrchStd>1</vSrchStd>
  </COMFundUnityInfoInputDTO>
</message>"""
FIELDS = {
    "standardDt": "date",  # 기준일자
    "standardCot": "nav",  # 기준가격
    "vBefDayFltstdcot": "nav_change",  # 전일대비
    "standardassStdCot": "tax_nav",  # 과표기준가격
    "uOriginalAmt": "principal",  # 설정원본 (백만원)
}


class KofiaTLSAdapter(HTTPAdapter):
    def init_poolmanager(self, *args: object, **kwargs: object) -> None:
        ctx = ssl.create_default_context()
        ctx.set_ciphers("DEFAULT@SECLEVEL=1")  # SSLError - [SSL: DH_KEY_TOO_SMALL] dh key too small
        super().init_poolmanager(*args, **kwargs, ssl_context=ctx)


def fetch_fund_nav(fund_list: Path, out_dir: Path, *, log: logging.Logger, start: date, end: date, postfix: str = "", workers: int = 4, limit: int | None = None, retries: int = 2) -> ItemResult:
    """펀드 목록(fund_code 컬럼)의 펀드별 일별 NAV 를 out_dir/<fund_code>/nav.csv<postfix> 에 저장."""
    codes = read_csv_column(fund_list, "fund_code")[:limit]
    local = threading.local()
    result = ItemResult()

    def _session() -> requests.Session:
        if not hasattr(local, "s"):
            local.s = requests.Session()
            local.s.mount("https://dis.kofia.or.kr", KofiaTLSAdapter())
        return local.s

    def _fetch(code: str) -> None:
        out = out_dir / code / f"nav.csv{postfix}"
        fetch_start = start  # 파일이 있으면 마지막 날짜 7일 전부터만 다시 받아 합침
        if last := last_value(out, "date"):
            fetch_start = max(start, datetime.strptime(last, "%Y%m%d").date() - timedelta(days=7))
        body = BODY.format(standard_cd=escape(code), start=f"{fetch_start:%Y%m%d}", end=f"{end:%Y%m%d}")
        r = _session().post(URL, data=body.encode(), headers={"Content-Type": "application/xml"}, timeout=120)
        r.raise_for_status()
        if (dto := ET.fromstring(r.content).find(".//COMFundPriceModListDTO")) is None:  # noqa: S314
            raise RuntimeError(f"unexpected response: {r.text[:500]!r}")
        rows = sorted(({col: (item.findtext(tag) or "").strip() for tag, col in FIELDS.items()} for item in dto.iter("priceModList")), key=lambda row: row["date"])
        if changed := merge_rows(out, rows, "date", list(FIELDS.values())):
            result.count("nav_rows_changed", changed)
            result.count("funds_updated")

    return run_items(codes, _fetch, log=log, workers=workers, retries=retries, result=result)
