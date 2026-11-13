"""금융투자협회 전자공시(dis.kofia.or.kr): 펀드별 보수비용비교(펀드·ETF 전체) + ETF 투자설명서 PDF.

원본: ETF_FUND_pipeline 저장소에서 화면 요청을 분석해 만든 수집 스크립트 (wrib-crawler 에는 없음)
- 보수비용: 펀드공시 > 펀드 보수 및 비용 > 펀드별 보수비용비교. 기준일 하나에 전체(약 2만 개, 30MB)가 한 번에 온다.
- 투자설명서: 펀드 다모아 상세 팝업의 '투자설명서' 파일. 표준코드로 서버 파일명을 조회한 뒤 disdown 서버에서 받는다.
"""

import csv
import json
import logging
import random
import threading
import time
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
from xml.sax.saxutils import escape

import requests

from .common import Empty, ItemResult, Skipped, run_items, safe_write, write_csv
from .kofia_nav import URL, KofiaTLSAdapter

DOWNLOAD_URL = "https://disdown.kofia.or.kr/COMFSFileDownload.jsp"
BODY = """<?xml version="1.0" encoding="utf-8"?>
<message>
  <proframeHeader>
    <pfmAppName>{app}</pfmAppName>
    <pfmSvcName>{svc}</pfmSvcName>
    <pfmFnName>{fn}</pfmFnName>
  </proframeHeader>
  <systemHeader></systemHeader>
  <{dto}>{fields}</{dto}>
</message>"""

# 펀드별 보수비용비교(serviceId SDIS01005001000, 화면 양식 DIS0012) 응답 태그 → CSV 컬럼
FEE_FIELDS = {
    "tmpV15": "standard_cd",  # 표준코드
    "tmpV1": "company_nm",  # 운용회사
    "tmpV2": "fund_nm",  # 펀드명
    "tmpV3": "fund_type",  # 펀드유형
    "tmpV4": "start_dt",  # 설정일 YYYYMMDD
    "tmpV5": "manage_fee",  # 보수율 운용 (%)
    "tmpV6": "sale_fee",  # 보수율 판매
    "tmpV7": "trust_fee",  # 보수율 수탁
    "tmpV8": "admin_fee",  # 보수율 사무관리
    "tmpV9": "total_fee",  # 보수율 합계 (A)
    "tmpV10": "peer_avg_fee",  # 유사유형 평균보수율
    "tmpV11": "other_expense",  # 기타비용 (B)
    "tmpV12": "ter",  # TER (A+B)
    "tmpV13": "front_load",  # 판매수수료 선취
    "tmpV14": "back_load",  # 판매수수료 후취
    "tmpV16": "trading_fee",  # 매매·중개수수료율 (D)
}
ETF_MARK = "상장지수"  # 펀드명에 들어 있으면 ETF

# disdown.kofia.or.kr 은 서버 인증서만 보내고 중간 인증서를 빠뜨려서 검증이 실패한다
# (CERTIFICATE_VERIFY_FAILED: unable to get local issuer certificate. 브라우저는 알아서 채움).
# dis.kofia.or.kr 이 함께 보내는 공개 중간 인증서를 신뢰 목록에 더해 검증은 그대로 유지한다. 2028-11-21 만료.
GLOBALSIGN_OV_CA_2018 = """-----BEGIN CERTIFICATE-----
MIIETjCCAzagAwIBAgINAe5fIh38YjvUMzqFVzANBgkqhkiG9w0BAQsFADBMMSAw
HgYDVQQLExdHbG9iYWxTaWduIFJvb3QgQ0EgLSBSMzETMBEGA1UEChMKR2xvYmFs
U2lnbjETMBEGA1UEAxMKR2xvYmFsU2lnbjAeFw0xODExMjEwMDAwMDBaFw0yODEx
MjEwMDAwMDBaMFAxCzAJBgNVBAYTAkJFMRkwFwYDVQQKExBHbG9iYWxTaWduIG52
LXNhMSYwJAYDVQQDEx1HbG9iYWxTaWduIFJTQSBPViBTU0wgQ0EgMjAxODCCASIw
DQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAKdaydUMGCEAI9WXD+uu3Vxoa2uP
UGATeoHLl+6OimGUSyZ59gSnKvuk2la77qCk8HuKf1UfR5NhDW5xUTolJAgvjOH3
idaSz6+zpz8w7bXfIa7+9UQX/dhj2S/TgVprX9NHsKzyqzskeU8fxy7quRU6fBhM
abO1IFkJXinDY+YuRluqlJBJDrnw9UqhCS98NE3QvADFBlV5Bs6i0BDxSEPouVq1
lVW9MdIbPYa+oewNEtssmSStR8JvA+Z6cLVwzM0nLKWMjsIYPJLJLnNvBhBWk0Cq
o8VS++XFBdZpaFwGue5RieGKDkFNm5KQConpFmvv73W+eka440eKHRwup08CAwEA
AaOCASkwggElMA4GA1UdDwEB/wQEAwIBhjASBgNVHRMBAf8ECDAGAQH/AgEAMB0G
A1UdDgQWBBT473/yzXhnqN5vjySNiPGHAwKz6zAfBgNVHSMEGDAWgBSP8Et/qC5F
JK5NUPpjmove4t0bvDA+BggrBgEFBQcBAQQyMDAwLgYIKwYBBQUHMAGGImh0dHA6
Ly9vY3NwMi5nbG9iYWxzaWduLmNvbS9yb290cjMwNgYDVR0fBC8wLTAroCmgJ4Yl
aHR0cDovL2NybC5nbG9iYWxzaWduLmNvbS9yb290LXIzLmNybDBHBgNVHSAEQDA+
MDwGBFUdIAAwNDAyBggrBgEFBQcCARYmaHR0cHM6Ly93d3cuZ2xvYmFsc2lnbi5j
b20vcmVwb3NpdG9yeS8wDQYJKoZIhvcNAQELBQADggEBAJmQyC1fQorUC2bbmANz
EdSIhlIoU4r7rd/9c446ZwTbw1MUcBQJfMPg+NccmBqixD7b6QDjynCy8SIwIVbb
0615XoFYC20UgDX1b10d65pHBf9ZjQCxQNqQmJYaumxtf4z1s4DfjGRzNpZ5eWl0
6r/4ngGPoJVpjemEuunl1Ig423g7mNA2eymw0lIYkN5SQwCuaifIFJ6GlazhgDEw
fpolu4usBCOmmQDo8dIm7A9+O4orkjgTHY+GzYZSR+Y0fFukAj6KYXwidlNalFMz
hriSqHKvoflShx8xpfywgVcvzfTO3PYkz6fiNJBonf6q8amaEsybwMbDqKWwIX7e
SPY=
-----END CERTIFICATE-----"""


class Blocked(Exception):  # noqa: N818
    """KOFIA 가 403/429 로 막음. 재시도하지 않고 멈춘다."""


class DisdownTLSAdapter(KofiaTLSAdapter):
    def init_poolmanager(self, *args: object, **kwargs: object) -> None:
        super().init_poolmanager(*args, **kwargs)
        self.poolmanager.connection_pool_kw["ssl_context"].load_verify_locations(cadata=GLOBALSIGN_OV_CA_2018)


def session() -> requests.Session:
    s = requests.Session()
    s.headers["User-Agent"] = "Mozilla/5.0"
    s.mount("https://dis.kofia.or.kr", KofiaTLSAdapter())
    s.mount("https://disdown.kofia.or.kr", DisdownTLSAdapter())
    return s


def _check(r: requests.Response) -> None:
    if r.status_code in (403, 429):
        raise Blocked(f"HTTP {r.status_code} {r.url}")
    r.raise_for_status()


def call(s: requests.Session, app: str, svc: str, fn: str, dto: str, fields: dict[str, str], timeout: int = 300) -> ET.Element:
    """proframe XML 서비스 1회 호출 → 응답 XML 루트."""
    body = BODY.format(app=app, svc=svc, fn=fn, dto=dto, fields="".join(f"<{k}>{escape(v)}</{k}>" for k, v in fields.items()))
    r = s.post(URL, data=body.encode("utf-8"), headers={"Content-Type": "application/xml; charset=UTF-8"}, timeout=timeout)
    _check(r)
    return ET.fromstring(r.content)  # noqa: S314 — KOFIA 응답


def latest_fee_date(s: requests.Session) -> str:
    """화면 '최신일 기준'과 같은 기준일(YYYYMMDD). 공시 기준일이 아닌 날짜로 조회하면 0건이 나온다."""
    root = call(s, "FS-DIS2", "DISComStdYMDSO", "select", "DISComStdYMDDTO", {"codeDesc": "J_RD"})
    if not (dt := (root.findtext(".//standardDt") or "").strip()):
        raise RuntimeError("보수비용 기준일을 받지 못했습니다")
    return dt


def fetch_fees(out_dir: Path, *, log: logging.Logger, standard_dt: str = "", postfix: str = "") -> dict:
    """기준일 하나의 펀드별 보수비용 전체를 out_dir/<기준일>.csv<postfix> 에 저장. 이미 있는 기준일은 건너뜀."""
    s = session()
    standard_dt = standard_dt or latest_fee_date(s)
    out = out_dir / f"{standard_dt}.csv{postfix}"
    if out.exists():
        log.info(f"이미 받은 기준일 {standard_dt}: {out}")
        return {"standard_dt": standard_dt, "rows": 0, "skipped": 1, "path": out}
    fields = {"tmpV30": standard_dt, "tmpV11": "", "tmpV12": "", "tmpV3": "", "tmpV5": "", "tmpV4": ""}
    root = call(s, "FS-DIS2", "DISFundFeeCmsSO", "select", "DISCondFuncDTO", fields)
    rows = [{col: (item.findtext(tag) or "").strip() for tag, col in FEE_FIELDS.items()} for item in root.iter("selectMeta")]
    expected = int(root.findtext(".//dbio_total_count_") or -1)
    if not rows or len(rows) != expected:
        raise RuntimeError(f"보수비용 응답 이상: 행 {len(rows)}개, 서버 건수 {expected}")
    rows.sort(key=lambda r: r["standard_cd"])
    write_csv(out, rows, list(FEE_FIELDS.values()))
    etf = sum(ETF_MARK in r["fund_nm"] for r in rows)
    log.info(f"보수비용 {standard_dt}: {len(rows):,}개 (ETF {etf:,}개) → {out}")
    return {"standard_dt": standard_dt, "rows": len(rows), "etf": etf, "skipped": 0, "path": out}


def etf_codes(fee_csv: Path) -> list[tuple[str, str]]:
    """보수비용 CSV 에서 ETF(펀드명에 '상장지수')의 (표준코드, 펀드명)."""
    with fee_csv.open(encoding="utf-8", newline="") as f:
        return [(r["standard_cd"], r["fund_nm"]) for r in csv.DictReader(f) if ETF_MARK in r["fund_nm"]]


def fetch_etf_prospectus(
    fee_csv: Path, out_dir: Path, *, log: logging.Logger, postfix: str = "", workers: int = 1, delay: float = 1.0, limit: int | None = None, retries: int = 2,
) -> ItemResult:  # fmt: skip
    """ETF 투자설명서를 out_dir/<표준코드>/prospectus.pdf<postfix> 로 저장. 파일 정보는 같은 폴더 prospectus.json.

    서버 파일명이 지난번과 같으면 건너뛰고(Skipped), 바뀌었으면(새 판) 덮어쓴다. 등록된 투자설명서가 없으면 Empty.
    403/429 를 받으면 나머지 항목은 요청하지 않고 건너뛰며, result.counters["blocked"] 에 표시한다.
    """
    targets = etf_codes(fee_csv)[:limit]
    local = threading.local()
    blocked = threading.Event()
    result = ItemResult()

    def _session() -> requests.Session:
        if not hasattr(local, "s"):
            local.s = session()
        return local.s

    def _pause() -> None:
        time.sleep(delay * random.uniform(0.7, 1.3))  # noqa: S311 — 요청 간격을 고르지 않게

    def _fetch(item: tuple[str, str]) -> None:
        code, name = item
        if blocked.is_set():
            raise Skipped
        try:
            _pause()
            root = call(_session(), "FS-DIS2", "DISFundDamoaPopSO", "tujaFile", "DISFundDamoaDTO", {"standardCd": code, "companyCd": ""}, timeout=60)
            info = root.find(".//DISFundDamoaDTO")
            server_path, server_file = ((info.findtext("val1") or "").strip(), (info.findtext("val2") or "").strip()) if info is not None else ("", "")
            if not server_file:
                raise Empty("협회에 등록된 투자설명서 없음")
            meta_path, pdf_path = out_dir / code / "prospectus.json", out_dir / code / f"prospectus.pdf{postfix}"
            old = json.loads(meta_path.read_text(encoding="utf-8")) if meta_path.exists() else {}
            if old.get("server_file") == server_file and pdf_path.exists():
                raise Skipped
            _pause()
            r = _session().get(DOWNLOAD_URL, params={"serverPath": server_path, "serverFileNm": server_file, "filename": "prospectus.pdf"}, timeout=180)
            _check(r)
            if not r.content.startswith(b"%PDF"):
                raise RuntimeError(f"PDF 가 아닌 응답: {r.content[:200]!r}")
            safe_write(pdf_path, r.content)
            meta = {
                "standard_cd": code, "fund_nm": name, "original_filename": (info.findtext("val3") or "").strip(),
                "server_path": server_path, "server_file": server_file, "size": len(r.content),
                "fetched_at": datetime.now().astimezone().isoformat(timespec="seconds"),
            }  # fmt: skip
            safe_write(meta_path, json.dumps(meta, ensure_ascii=False, indent=1).encode("utf-8"))
            result.count("docs_updated" if old else "docs_new")
        except Blocked:
            blocked.set()
            raise

    result = run_items(targets, _fetch, log=log, key=lambda t: t[0], workers=workers, retries=retries, result=result)
    if blocked.is_set():
        result.count("blocked")
    return result
