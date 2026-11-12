"""SEIBro 증권정보포털: 펀드 목록(ISIN), ISIN별 일별 NAV.

원본: wrib-crawler/scripts/seibro/fetch_fund_list.py, fetch_fund_nav.py
"""

import logging
import threading
import xml.etree.ElementTree as ET
from datetime import date, datetime, timedelta
from pathlib import Path
from xml.sax.saxutils import quoteattr

import requests

from .common import Empty, ItemResult, last_value, merge_rows, read_csv_column, run_items, write_csv

URL = "https://seibro.or.kr/websquare/engine/proworks/callServletService.jsp"

LIST_FIELDS = {
    "ISIN": "isin",  # 펀드 ISIN
    "FUND_SECN_NM": "fund_name",  # 펀드명
    "FUND_CACD": "fund_class_code",  # 펀드 분류코드
}
NAV_FIELDS = {
    "ANYTM_REPTG_DT": "date",  # 기준일자
    "NAV_AMT": "nav",  # 기준가격
    "STDPRC_INCDEC_AMT": "nav_change",  # 전일대비
    "DD1_PRATE": "nav_change_pct",  # 전일대비 등락률 (%)
    "TAXSTD": "tax_nav",  # 과표기준가격
    "FUND_SETUP_ORCP_AMT": "principal",  # 설정원본
    "FUND_NETASST_TOTAMT": "net_asset",  # 순자산총액
    "TOT_DIV_PAY_AMT": "dividend",  # 분배금
    "RGT_RACD": "note",  # 비고
}


def _rows(root: ET.Element, fields: dict[str, str]) -> list[dict[str, str]]:
    return [{col: el.get("value", "") if (el := res.find(tag)) is not None else "" for tag, col in fields.items()} for res in root.iter("result")]


def fetch_fund_list(out: Path) -> int:
    r = requests.post(
        URL,
        data=(
            b'<reqParam action="searchFundContentList" task="ksd.safe.bip.cmuc.User.process.SearchPTask">'
            b'<CTYPE value="1"/><RECU_WHCD value="11"/><S_TYPE value="1"/><search_string value="KR"/>'  # 모든 ISIN이 KR로 시작
            b"</reqParam>"
        ),
        headers={
            "Content-Type": 'application/xml; charset="UTF-8"',
            "Referer": "https://seibro.or.kr/websquare/control.jsp?w2xPath=/IPORTAL/user/etc/BIP_CMUC01044P.xml",  # 없으면 '서버오류3'
            "User-Agent": "Mozilla/5.0",
        },
        timeout=300,
    )
    r.raise_for_status()
    root = ET.fromstring(r.content)  # noqa: S314
    if root.tag != "vector":
        raise RuntimeError(f"unexpected response: {r.text[:500]!r}")
    rows = _rows(root, LIST_FIELDS)
    if len(rows) != int(root.get("result", -1)):
        raise RuntimeError(f"expected {root.get('result')} rows, got {len(rows)}")
    write_csv(out, rows, list(LIST_FIELDS.values()))
    return len(rows)


def fetch_fund_nav(fund_list: Path, out_dir: Path, *, log: logging.Logger, start: date, end: date, postfix: str = "", workers: int = 8, limit: int | None = None, retries: int = 2) -> ItemResult:
    """ISIN별 일별 NAV 를 out_dir/<isin>/nav.csv<postfix> 에 저장.

    파일이 있으면 마지막 날짜 7일 전부터만 다시 받아 합침 (정정된 값은 새 값으로 덮어씀).
    """
    isins = read_csv_column(fund_list, "isin")[:limit]
    local = threading.local()
    result = ItemResult()

    def _fetch(isin: str) -> None:
        out = out_dir / isin / f"nav.csv{postfix}"
        fetch_start = start
        if last := last_value(out, "date"):
            fetch_start = max(start, datetime.strptime(last, "%Y%m%d").date() - timedelta(days=7))
        if not hasattr(local, "s"):
            local.s = requests.Session()
        isin_attr = quoteattr(isin)
        body = (
            '<reqParam action="fundCompstInfoStdprcDtramtPList" task="ksd.safe.bip.cnts.Fund.process.FundCompstInfoPTask">'
            f'<ISIN value={isin_attr}/><FUND_ISIN value={isin_attr}/><IC_START value="{fetch_start:%Y%m%d}"/><IC_END value="{end:%Y%m%d}"/>'
            '<PAGE_ON_CNT value="100000"/><PAGE_NUM value="1"/>'  # 한 페이지에 전체 기간을 받음
            "</reqParam>"
        )
        r = local.s.post(
            URL,
            data=body.encode(),
            headers={
                "Content-Type": 'application/xml; charset="UTF-8"',
                "Referer": "https://seibro.or.kr/websquare/control.jsp?w2xPath=/IPORTAL/user/fund/BIP_CNTS05011V.xml&menuNo=155",
                "User-Agent": "Mozilla/5.0",
            },
            timeout=120,
        )
        r.raise_for_status()
        root = ET.fromstring(r.content)  # noqa: S314
        if root.tag != "vector":
            raise RuntimeError(f"unexpected response: {r.text[:500]!r}")
        rows = sorted(_rows(root, NAV_FIELDS), key=lambda row: row["date"])
        if len(rows) != int(root.get("result", -1)):  # 페이지가 잘렸으면 저장하지 않음
            raise RuntimeError(f"expected {root.get('result')} rows, got {len(rows)}")
        if not rows and not out.exists():
            raise Empty("기간 내 NAV 없음")
        if changed := merge_rows(out, rows, "date", list(NAV_FIELDS.values())):
            result.count("nav_rows_changed", changed)
            result.count("funds_updated")

    return run_items(isins, _fetch, log=log, workers=workers, retries=retries, result=result)
