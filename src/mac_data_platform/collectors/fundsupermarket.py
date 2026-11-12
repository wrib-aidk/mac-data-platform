"""펀드슈퍼마켓 (fundsupermarket.wooriib.com): 펀드 목록, 펀드 상세(details.json), 관련 문서 PDF.

원본: wrib-crawler/scripts/fundsupermarket/fetch_fund_list.py, fetch_fund_details.py
파싱 함수는 원본 그대로이고, 실행부(main)만 Dagster에서 부를 수 있게 함수로 바꿨다.
"""

import hashlib
import html
import json
import logging
import re
import threading
import time
from datetime import date
from pathlib import Path

import pypdfium2 as pdfium
import requests

from .common import ItemResult, read_csv_column, run_items, safe_write, write_csv

# ---------------------------------------------------------------------------- 펀드 목록 (원본 fetch_fund_list.py)

def fetch_fund_list_html(page_no: int = 1, page_cnt: int = 20, **params: object) -> str:
    # 체크박스류는 여러 값을 쉼표로 이어 보낸다 (fetch 에 리스트로 넘기면 자동 처리).
    form = {
        "fundName2": "",  # 펀드 검색: 검색하실 펀드명 (fundName 과 같은 결과, 둘 다 넣으면 두 조건 모두 만족하는 펀드만)
        "fsType01": "B1",  # 펀드속성: B1=전체 / B2=일반펀드 / 77=연금펀드 / 83=해외주식 비과세(0건)
        "fstChk02": "BB",  # 펀드유형: BB=전체 / 20=국내주식(0건) / 2001=일반주식 / 20020=중소형주식 / 20030=배당주식 / 20040=테마주식 / 20050=K200인덱스 / 20060=기타인덱스 / 21010=일반주식혼합 / 21020=공격적자산배분 / 10=국내채권(0건) / 10010=일반채권 / 10020=초단기채권 / 10040=중기채권 / 10060=우량채권 / 10140=하이일드채권 / 22010=일반채권혼합 / 22020=보수적자산배분 / 60=해외주식(0건) / 60010=글로벌주식 / 60020=유럽주식 / 60030=북미주식 / 60040=아시아태평양주식 / 60050=아시아태평양주식(ex J) / 60070=동남아주식 / 60080=글로벌신흥국주식 / 60090=아시아신흥국주식 / 60100=유럽신흥국주식 / 60110=남미신흥국주식 / 60120=일본주식 / 60130=중국주식 / 60140=인도주식 / 60150=말레이시아주식 / 60160=베트남주식 / 60170=에너지섹터 / 60180=기초소재섹터 / 60190=일반산업섹터 / 60200=소비재섹터 / 60210=헬스케어섹터 / 60220=금융섹터 / 60230=정보기술섹터 / 60240=공공서비스섹터 / 60250=원자재섹터 / 60260=멀티섹터 / 60270=브라질주식 / 60280=러시아주식 / 60290=호주주식 / 60300=독일주식 / 60310=기타국가주식 / 60320=기타신흥국주식 / 60330=타이완주식 / 60340=프론티어마켓주식 / 61010=글로벌주식혼합 / 61020=신흥국주식혼합 / 61030=아태주식혼합(ex J) / 61040=일본주식혼합 / 61090=유럽주식혼합 / 61100=북미주식혼합 / 61110=아시아신흥국주식혼합 / 61120=유럽신흥국주식혼합 / 61130=남미주식혼합 / 61140=기타이머징주식혼합 / 61150=글로벌 공격적자산배분 / 61160=아태주식혼합 / 50=해외채권(0건) / 50010=글로벌채권 / 50020=유럽채권 / 50030=북미채권 / 50040=아시아태평양채권 / 50050=신흥국채권 / 50060=아시아채권(ex J) / 50070=유럽신흥국채권 / 50080=남미신흥국채권 / 50100=글로벌하이일드채권 / 62010=글로벌채권혼합 / 62020=신흥국채권혼합 / 62030=아태채권혼합(ex J) / 62040=일본채권혼합 / 62090=유럽채권혼합 / 62100=북미채권혼합 / 62110=아시아신흥국채권혼합 / 62120=유럽신흥국채권혼합 / 62130=남미채권혼합 / 62140=기타이머징채권혼합 / 62150=글로벌 보수적자산배분 / 62160=아태채권혼합 / 29=기타(0건) / 23010=채권알파 / 23020=시장중립 / 23030=공모주하이일드 / 24010=부동산개발 / 24020=부동산임대 / 24030=부동산대출채권 / 24040=부동산NPL / 28010=실물자산 / 28020=ELF / 28030=PI / 28040=메자닌 / 28050=ILS / 29060=단기매칭 / 29070=장기매칭 / 29080=라이프싸이클 / 29090=베어마켓 / 29900=단기미분류 / 29910=장기미분류 / 29920=영구미분류 / 29990=신규설정펀드 / 64010=글로벌리츠재간접 / 64020=일본리츠재간접 / 64030=아-태 리츠재간접 / 64040=글로벌부동산 / 65010=커머더티 / 68010=글로벌실물자산 / 68020=글로벌PI / 68030=글로벌메자닌 / 68040=글로벌ILS / 69010=글로벌헤지전략 / 69060=KP / 69080=글로벌라이프싸이클 / 69900=글로벌단기주식미분류 / 69910=글로벌단기채권미분류 / 69920=글로벌장기미분류 / 11=유동성(0건) / 11010=MMF
        "fstChk03": "",  # 운용전략: ""=전체 / 1=중국본토A / 2=배당주 / 3=가치주 / 4=성장주 / 5=공모주(혼합) / 6=공모주(채권) / 7=인덱스 / 8=롱/숏 / 9=로보어드바이저
        "fstChk04": "T",  # 운용규모: T=전체 / 1=10억원 미만 / 2=10~100억원 / 3=100~500억원 / 4=500~1,000억원 / 5=1,000~5,000억원 / 6=5,000억원 이상
        "fstChk05": "T",  # 위험등급: T=전체 / 1=매우 높은 위험 / 2=높은 위험 / 3=다소 높은 위험 / 4=보통 위험 / 5=낮은 위험 / 6=매우 낮은 위험
        "fstChk06": "T",  # 슈퍼뱃지: T=전체 / 1=성과지속 / 2=평가등급 / 3=수익률 / 4=판매액 / 5=조회 / 6=유입액 / 7=뱃지없음
        "fstChk07": "T",  # 운용사: T=전체 / 309=골든브릿지자산운용 / 207=교보자산운용 / 306=다올자산운용 / 203=대신자산운용 / 3H6=더제이자산운용 / 231=디더블유에스자산운용 / 3F0=디에스자산운용 / 303=마이다스에셋자산운용 / 3M3=머스트자산운용 / 301=미래에셋자산운용 / 3M7=밸류시스템자산운용 / 302=베어링자산운용 / 454=브레인자산운용 / 104=브이아이자산운용 / 3W5=브이아이피자산운용 / 212=브이자산운용 / 3N9=삼성액티브자산운용 / 105=삼성자산운용 / 230=슈로더자산운용 / 355=스팍스자산운용 / 209=신영자산운용 / 210=신한자산운용 / 234=아이비케이자산운용 / 321=알파자산운용 / 364=에셋플러스자산운용 / 308=에이케이파트너스자산운용 / 107=우리자산운용 / 244=웰컴자산운용 / 305=유경PSG자산운용 / 307=유리자산운용 / 214=유진자산운용 / 383=이지스자산운용 / 229=카디안자산운용 / 233=칸서스자산운용 / 365=케이씨지아이자산운용 / 310=코레이트자산운용 / 206=키움투자자산운용 / 3J1=타임폴리오자산운용 / 363=트러스톤자산운용 / 311=플러스자산운용 / 235=피델리티자산운용 / 102=하나자산운용 / 4P8=한국투자리얼에셋운용 / 236=한국투자밸류자산운용 / 101=한국투자신탁운용 / 213=한화자산운용 / 239=현대인베스트먼트자산운용 / 373=현대자산운용 / 224=흥국자산운용 / 370=AB자산운용 / 240=BNK자산운용 / 216=DB자산운용 / 226=HDC자산운용 / 223=KB자산운용 / 232=NH-Amundi자산운용 / 368=iM에셋자산운용
        "rlzRt": "",  # 기간수익률 기간선택: ""=전체 / 3=3개월 / 6=6개월 / 12=1년 / 36=3년 (""이면 type01 수익률(%) 범위 무시)
        "type01_con1": "-9999",  # 기간수익률 수익률(%) 최소값: -9999=0%이하 / 0=0% / 10=10% / 20=20% / 30=30% / 40=40% / 50=50% / 9999=50%이상
        "type01_con2": "9999",  # 기간수익률 수익률(%) 최대값: -9999=0%이하 / 0=0% / 10=10% / 20=20% / 30=30% / 40=40% / 50=50% / 9999=50%이상
        "type02_con1": "0",  # 상품정보 설정기간 최소값: 0=현재 / -1=1개월 / -3=3개월 / -12=1년 / -36=3년 / -60=5년 / -999=5년이상
        "type02_con2": "-999",  # 상품정보 설정기간 최대값: 0=현재 / -1=1개월 / -3=3개월 / -12=1년 / -36=3년 / -60=5년 / -999=5년이상
        "type03_con1": "0",  # 상품정보 총보수(%) 최소값: 0=0% / 0.5=0.5% / 1.0=1.0% / 1.5=1.5% / 2.0=2.0% / 20=2.0%이상
        "type03_con2": "20",  # 상품정보 총보수(%) 최대값: 0=0% / 0.5=0.5% / 1.0=1.0% / 1.5=1.5% / 2.0=2.0% / 20=2.0%이상
        "saleRate": "ON",  # 선취수수료: ON=전체 / Y=있음(0건) / N=없음
        "afrcvRate": "ON",  # 후취수수료: ON=전체 / Y=있음 / N=없음
        "zeroin": "T",  # 평가등급 제로인: T=전체 / 0=무등급 / 5=5등급 / 4=4등급 / 3=3등급 / 2=2등급 / 1=1등급
        "mstar": "",  # 평가등급 모닝스타: T=전체 / 0=무등급 / 5=5등급 / 4=4등급 / 3=3등급 / 2=2등급 / 1=1등급(0건)
        "kfr": "",  # 평가등급 한국펀드평가: T=전체 / 0=무등급 / 5=5등급 / 4=4등급 / 3=3등급 / 2=2등급 / 1=1등급
        "fng": "",  # 평가등급 에프앤가이드: T=전체 / 0=무등급 / 5=5등급 / 4=4등급 / 3=3등급 / 2=2등급 / 1=1등급
        "type04_con1": "0",  # 위험지표 표준편차(%순위) 최소값: 0=0% / 20=20% / 40=40% / 60=60% / 80=80% / 100=100%
        "type04_con2": "100",  # 위험지표 표준편차(%순위) 최대값: 0=0% / 20=20% / 40=40% / 60=60% / 80=80% / 100=100%
        "type05_con1": "0",  # 위험지표 샤프지수(%순위) 최소값: 0=0% / 20=20% / 40=40% / 60=60% / 80=80% / 100=100%
        "type05_con2": "100",  # 위험지표 샤프지수(%순위) 최대값: 0=0% / 20=20% / 40=40% / 60=60% / 80=80% / 100=100%
        "type06_con1": "0",  # 위험지표 베타(%순위) 최소값: 0=0% / 20=20% / 40=40% / 60=60% / 80=80% / 100=100%
        "type06_con2": "100",  # 위험지표 베타(%순위) 최대값: 0=0% / 20=20% / 40=40% / 60=60% / 80=80% / 100=100%
        "sClass": "N",  # S클래스 펀드만 검색: N=전체 / Y=S클래스만
        "eClass": "N",  # 운용사 추천펀드만 검색: N=전체 / Y=운용사 추천펀드만 (화면에서 숨김 처리)
        "ptnCode": "",  # 전체유형(대): ""=전체유형(대) / 20=국내주식형 / 21=국내주식혼합형 / 22=국내채권혼합형 / 10=국내채권형 / 11=국내MMF / 29=국내기타 / 60=해외주식형 / 61=해외주식혼합형 / 62=해외채권혼합형 / 50=해외채권형 / 69=해외기타 / 24=국내부동산개발 (하나만 선택 가능)
        "ptnCode3": "",  # 전체유형(소): ""=전체유형(소) / [국내주식] 2001=일반주식 / 20020=중소형주식 / 20030=배당주식 / 20040=테마주식 / 20050=K200인덱스 / 20060=기타인덱스 / 21010=일반주식혼합 / 21020=공격적자산배분 / [국내채권] 10010=일반채권 / 10020=초단기채권 / 10040=중기채권 / 10060=우량채권 / 10140=하이일드채권 / 22010=일반채권혼합 / 22020=보수적자산배분 / [해외주식] 60010=글로벌주식 / 60020=유럽주식 / 60030=북미주식 / 60040=아시아태평양주식 / 60050=아시아태평양주식(ex J) / 60070=동남아주식 / 60080=글로벌신흥국주식 / 60090=아시아신흥국주식 / 60100=유럽신흥국주식 / 60110=남미신흥국주식 / 60120=일본주식 / 60130=중국주식 / 60140=인도주식 / 60150=말레이시아주식 / 60160=베트남주식 / 60170=에너지섹터 / 60180=기초소재섹터 / 60190=일반산업섹터 / 60200=소비재섹터 / 60210=헬스케어섹터 / 60220=금융섹터 / 60230=정보기술섹터 / 60240=공공서비스섹터 / 60250=원자재섹터 / 60260=멀티섹터 / 60270=브라질주식 / 60280=러시아주식 / 60290=호주주식 / 60300=독일주식 / 60310=기타국가주식 / 60320=기타신흥국주식 / 60330=타이완주식 / 60340=프론티어마켓주식 / 61010=글로벌주식혼합 / 61020=신흥국주식혼합 / 61030=아태주식혼합(ex J) / 61040=일본주식혼합 / 61090=유럽주식혼합 / 61100=북미주식혼합 / 61110=아시아신흥국주식혼합 / 61120=유럽신흥국주식혼합 / 61130=남미주식혼합 / 61140=기타이머징주식혼합 / 61150=글로벌 공격적자산배분 / 61160=아태주식혼합 / [해외채권] 50010=글로벌채권 / 50020=유럽채권 / 50030=북미채권 / 50040=아시아태평양채권 / 50050=신흥국채권 / 50060=아시아채권(ex J) / 50070=유럽신흥국채권 / 50080=남미신흥국채권 / 50100=글로벌하이일드채권 / 62010=글로벌채권혼합 / 62020=신흥국채권혼합 / 62030=아태채권혼합(ex J) / 62040=일본채권혼합 / 62090=유럽채권혼합 / 62100=북미채권혼합 / 62110=아시아신흥국채권혼합 / 62120=유럽신흥국채권혼합 / 62130=남미채권혼합 / 62140=기타이머징채권혼합 / 62150=글로벌 보수적자산배분 / 62160=아태채권혼합 / [기타] 23010=채권알파 / 23020=시장중립 / 23030=공모주하이일드 / 24010=부동산개발 / 24020=부동산임대 / 24030=부동산대출채권 / 24040=부동산NPL / 28010=실물자산 / 28020=ELF / 28030=PI / 28040=메자닌 / 28050=ILS / 29060=단기매칭 / 29070=장기매칭 / 29080=라이프싸이클 / 29090=베어마켓 / 29900=단기미분류 / 29910=장기미분류 / 29920=영구미분류 / 29990=신규설정펀드 / 64010=글로벌리츠재간접 / 64020=일본리츠재간접 / 64030=아-태 리츠재간접 / 64040=글로벌부동산 / 65010=커머더티 / 68010=글로벌실물자산 / 68020=글로벌PI / 68030=글로벌메자닌 / 68040=글로벌ILS / 69010=글로벌헤지전략 / 69060=KP / 69080=글로벌라이프싸이클 / 69900=글로벌단기주식미분류 / 69910=글로벌단기채권미분류 / 69920=글로벌장기미분류 / [유동성] 11010=MMF (펀드유형 하위 항목과 같은 코드, 지정하면 ptnCode 와 무관하게 이 값으로 필터됨)
        "evalTp": "0",  # 정렬선택: 0=정렬선택(선호수익률 기준으로 보임) / 1=펀드규모순 / 2=평가등급순 / 7=총보수순(낮은 쪽부터) / 21=1일 수익률순 / 3=3개월 수익률순 / 4=6개월 수익률순 / 5=1년 수익률순 / 6=3년 수익률순 / 18=1개월 판매액순 / 19=3개월 판매액순 / 20=6개월 판매액순 / 9=1개월 유입액순 / 10=3개월 유입액순 / 11=6개월 유입액순
        "fundName": "",  # 펀드명 검색: 검색어 (fundName2 와 같은 결과)
        "gradeGbn": "1",  # 리스트조건설정 평가등급: 1=제로인 / 2=모닝스타 / 3=에프앤가이드 / 4=한국펀드평가
        "likeRlzrt": "36",  # 리스트조건설정 선호수익률: 3=3개월 / 6=6개월 / 12=1년 / 36=3년
        "openAbleYn": "Y",  # 리스트조건설정 판매중인 펀드만 노출: Y=판매중인 펀드만 (서버가 무시함)
        "millYn": "N",  # 리스트조건설정 운용규모 100억 이상 펀드만 노출: N=전체 / Y=100억 이상만
        "listType": "1",  # 1=리스트형식 보기 / 2=박스형식 보기 / 3=표형식 보기
        "pageNo": "1",  # 페이지 번호
        "pageCnt": "20",  # 페이지당 건수 (상한 없음, 전체 건수 이상이면 한 번에 전부 반환
        "listSort": "1",  # 화면 항목 미확인
        "gridSort": "",  # 화면 항목 미확인
        "upDown": "DOWN",  # 화면 항목 미확인: DOWN / UP (서버가 무시함)
        "sortTitle": "내림차순",  # 화면 항목 미확인
        "sortCol": "",  # 화면 항목 미확인
        "cmplxPdtYn": "Y",  # 화면 항목 미확인: Y=전체 / N=레버리지·인버스(리버스) 파생형 펀드 제외
    }

    unknown = params.keys() - form.keys()
    if unknown:
        raise ValueError(f"unknown params: {unknown}")

    s = requests.Session()
    s.headers["User-Agent"] = "Mozilla/5.0"
    s.get("https://fundsupermarket.wooriib.com", timeout=10)  # 세션 쿠키(JSESSIONID, WMONID) 발급

    form = {**form, "pageNo": page_no, "pageCnt": page_cnt, **params}
    form = {k: ",".join(map(str, v)) if isinstance(v, (list, tuple)) else str(v) for k, v in form.items()}
    r = s.post("https://fundsupermarket.wooriib.com/fmm/FMM1010301/FMM9010101T1.do", files={k: (None, v) for k, v in form.items()}, timeout=30)
    r.raise_for_status()
    return r.text


def parse_fund_list_html(page: str) -> list[dict[str, str | int]]:
    def _text(s: str) -> str:
        s = re.sub(r'<em class="blind">.*?</em>', "", s)  # "증가"/"감소" 스크린리더용 문구
        return html.unescape(re.sub(r"\s+", " ", re.sub(r"<[^>]+>", "", s))).strip()

    def _signed(s: str) -> str:
        t = _text(s)
        return "-" + t if "icon-updown down" in s and not t.startswith("-") else t  # icon-updown down은 숫자에 '-' 가 없어 클래스로 부호를 붙임

    def _td(block: str, th: str) -> str:
        return re.search(rf'<th scope="row">{re.escape(th)}</th>\s*(?:<!--.*?-->\s*)?<td>(.*?)</td>', block, re.DOTALL).group(1)

    rows = []
    for b in page.split("<!-- loop -->")[1:]:
        dt = b[b.find("<dt>") : b.find("</dt>")]
        price = re.findall(r"<li>(.*?)</li>", _td(b, "기준가"), re.DOTALL)
        badge = re.search(r'<span class="ico-\w+" title="([^"]*)"', dt)
        cls = re.search(r'class="cls-detail[^"]*">(.*?)</span>', dt, re.DOTALL)  # 없는 펀드도 있음
        row = {
            "fund_code": re.search(r"moveFundDtlPage\('(\w+)'\)", dt).group(1),  # 펀드코드
            "fund_name": html.unescape(re.search(r"name='fpName' title='([^']*)'", dt).group(1)),  # 펀드명
            "fund_class": _text(cls.group(1)) if cls else "",  # 클래스상세
            "badge": badge.group(1) if badge else "",  # 추천
            "fund_type": _text(_td(b, "유형")),  # 유형
            "total_fee": _text(_td(b, "총보수")),  # 총보수
            "aum": _text(_td(b, "규모")),  # 규모
            "net_flow_1m": _signed(_td(b, "유입액(1M)")),  # 유입액(1M)
            "nav": _text(price[0]),  # 기준가
            "nav_change": _signed(price[1]),  # 기준가 등락
            "nav_change_pct": _text(price[2]).strip("()"),  # 기준가 등락률
            "sales_1m": _signed(_td(b, "판매액(1M)")),  # 판매액(1M)
            "risk_grade": _text(re.search(r'<dd class="sort">.*?<span class="tit">(.*?)</span>', b, re.DOTALL).group(1)),  # 위험등급
        }
        GRADES = {
            "zeroin": "rating_zeroin",  # 제로인
            "mstar": "rating_morningstar",  # 모닝스타
            "kfr": "rating_kfr",  # 한국펀드평가
            "fng": "rating_fnguide",  # 에프앤가이드
        }
        for key, name in GRADES.items():
            row[name] = int(re.search(rf'<span class="icon-grade {key}"><span class="z(\d\d)">', b).group(1))  # 0=무등급
        for period in ("3M", "6M", "1Y", "3Y"):
            row[f"return_{period.lower()}"] = _text(re.search(rf'<span id="{period}" class="tit">{period}</span>\s*(<span.*?</span>)\s*</li>', b, re.DOTALL).group(1))
        rows.append(row)
    return rows


# ---------------------------------------------------------------------------- 펀드 상세 (원본 fetch_fund_details.py)

BASE = "https://fundsupermarket.wooriib.com"


def _text(s: str) -> str:
    s = re.sub(r"<!--.*?-->|<br\s*/?>", " ", s, flags=re.DOTALL)
    return html.unescape(re.sub(r"\s+", " ", re.sub(r"<[^>]+>", "", s))).strip()


def _find(pattern: str, s: str) -> str:
    m = re.search(pattern, s, re.DOTALL)
    return _text(m.group(1)) if m else ""

_pdfium_lock = threading.Lock()  # pdfium 은 스레드 안전하지 않아 한 번에 한 스레드만 호출


def _is_valid_pdf(data: bytes) -> bool:
    """모든 페이지가 열리는지 확인 (잘리거나 내용이 섞여 깨진 응답을 거름)"""
    with _pdfium_lock:
        try:
            doc = pdfium.PdfDocument(data)
        except pdfium.PdfiumError:
            return False
        try:
            for i in range(len(doc)):
                doc[i].close()
        except pdfium.PdfiumError:
            return False
        finally:
            doc.close()  # GC 가 다른 스레드에서 닫지 않게 lock 안에서 닫음
        return True


def fetch_fund_detail_html(s: requests.Session, form: dict[str, str], tab: int) -> str:
    """펀드상세정보 탭: 1=상품 설명서 / 2=성과는 어땠나요? / 3=평가등급은 어떤가요? / 4=어디에 투자하나요?"""
    r = s.post(f"{BASE}/fmm/FMM1030101/fmm1030101T{tab}.do", data=form, timeout=30)
    r.raise_for_status()
    return r.text


def parse_fund_detail_html(page: str) -> dict[str, str]:
    m = re.search(r'<div class="cost_explan">(.*?)<p>※', page, re.DOTALL)  # 비용은 얼마나 될까요?
    cost_html = m.group(1) if m else ""

    def _dd(dt: str) -> str:  # 비용 영역의 <dt>dt</dt> 바로 뒤 <dd>
        return _find(rf"<dt>{dt}</dt>\s*(?:<!--.*?-->\s*)*<dd[^>]*>(.*?)</dd>", cost_html)

    redemption = re.search(r"<dt>환매수수료</dt>(.*?)</dl>", cost_html, re.DOTALL)
    redemption_items = re.findall(r"<li>(.*?)</li>", redemption.group(1), re.DOTALL) if redemption else []
    # 매입/환매 x 마감시각 이전/이후 순서로 (기준가적용, 매입일|환매일)
    days = re.findall(r"<dt>(?:기준가적용|매입일|환매일)</dt>\s*<dd[^>]*>(.*?)</dd>", page)
    days += [""] * (8 - len(days))
    return {
        "strategy": _find(r'<div class="strategy">(.*?)</div>', page),  # 운용전략은 무엇인가요?
        "sales_charge": _dd("판매수수료"),  # 판매수수료
        "total_fee": _dd("총보수"),  # 총보수
        "distribution_fee": _dd("판매보수"),  # 판매보수
        "management_fee": _dd("운용보수"),  # 운용보수
        "custody_fee": _dd("수탁보수"),  # 수탁보수
        "admin_fee": _dd("사무보수"),  # 사무보수
        "redemption_fee": " / ".join(map(_text, redemption_items)) or _dd("환매수수료"),  # 환매수수료
        "cutoff_time": _find(r'<div class="base_price">.*?<dt><span>(.*?) 이전</span></dt>', page),  # 매매기준가 적용일 기준 시각
        "purchase_nav_date": days[0],  # 매입 (기준시각 이전) 기준가적용
        "purchase_date": days[1],  # 매입 (기준시각 이전) 매입일
        "purchase_nav_date_after_cutoff": days[2],  # 매입 (기준시각 이후) 기준가적용
        "purchase_date_after_cutoff": days[3],  # 매입 (기준시각 이후) 매입일
        "redemption_nav_date": days[4],  # 환매 (기준시각 이전) 기준가적용
        "redemption_date": days[5],  # 환매 (기준시각 이전) 환매일
        "redemption_nav_date_after_cutoff": days[6],  # 환매 (기준시각 이후) 기준가적용
        "redemption_date_after_cutoff": days[7],  # 환매 (기준시각 이후) 환매일
        "asset_manager": _find(r'<th scope="row">운용회사</th>\s*<td[^>]*>(.*?)<a', page),  # 운용회사
        "trustee": _find(r'<th scope="row">수탁회사</th>\s*<td>(.*?)</td>', page),  # 수탁회사
        "fund_administrator": _find(r'<th scope="row">사무관리회사</th>\s*<td>(.*?)</td>', page),  # 사무관리회사
    }


def parse_fund_performance_html(page: str) -> dict[str, object]:
    page = re.sub(r"<!--.*?-->", "", page, flags=re.DOTALL)

    # 표의 행·열 이름 → JSON 키 (기간수익률의 유형평균·%순위는 제로인 소유형 기준이라 sub_category)
    table_keys = {
        "기간수익률": {
            "수익률": "fund",
            "비교지수": "benchmark",
            "유형평균": "sub_category_avg",
            "%순위": "sub_category_rank",
            "1개월": "1m",
            "3개월": "3m",
            "6개월": "6m",
            "1년": "1y",
            "2년": "2y",
            "3년": "3y",
            "5년": "5y",
            "설정후": "since_inception",
        },
        "3개월 구간수익률": {
            "수익률(%)": "fund",
            "비교지수(%)": "benchmark",
            "유형(소)평균(%)": "sub_category_avg",
            "유형(소) %순위": "sub_category_rank",
            "유형(대) %순위": "category_rank",
        },
        "위험지표 상세보기": {
            "표준편차(%)": "std_dev",
            "BM민감도(베타)": "beta",
            "트래킹에러(TE,%)": "tracking_error",
            "샤프지수": "sharpe_ratio",
            "젠센알파(%)": "jensen_alpha",
            "정보비율(IR)": "information_ratio",
            "%순위": "rank",
            "유형평균": "category_avg",
            "유형평균(%)": "category_avg",
            "3개월": "3m",
            "6개월": "6m",
            "1년": "1y",
            "3년": "3y",
            "5년": "5y",
        },
    }

    def _val(s: str) -> float | str | None:  # 숫자면 float, "-" 는 None, 나머지("96/100", "131.12% (연52.03%)")는 문자열
        t = _text(s)
        try:
            return float(t.rstrip("%").replace(",", ""))
        except ValueError:
            return None if t in ("", "-") else t

    def _grid(caption: str, cols: list[str] | None = None) -> dict[str, dict[str, float | str | None]]:  # 행 x 열 표 → {행: {열: 값}}
        m = re.search(rf"<caption>{re.escape(caption)}</caption>(.*?)</table>", page, re.DOTALL)
        if not m:
            return {}
        keys = table_keys[caption]
        cols = cols or [keys[_text(c)] for c in re.findall(r'<th scope="col"(?! class="first")[^>]*>(.*?)</th>', m.group(1))]
        grid, main = {}, ""
        for cls, th, tds in re.findall(r'<th scope="row" class="first( import)?">(.*?)</th>(.*?)</tr>', m.group(1), re.DOTALL):
            name = keys[_text(th)]
            main = name if cls or caption != "위험지표 상세보기" else main
            key = name if name == main else f"{main}_{name}"  # 위험지표의 %순위·유형평균은 바로 위 지표명을 붙여 구분 (std_dev_rank)
            grid[key] = dict(zip(cols, map(_val, re.findall(r"<td>(.*?)</td>", tds, re.DOTALL)), strict=False))
        return grid

    quarters = re.search(r"function sectionChart\(\)\{.*?var xaxis = \$\.parseJSON\('(.*?)'\);", page, re.DOTALL)
    return {
        "inception_date": _find(r"<dt>펀드 설정일</dt>\s*<dd>(.*?)</dd>", page),  # 펀드 설정일
        "return_base_date": _find(r"기간수익률.*?<dt>기준일</dt>\s*<dd>(.*?)</dd>", page),  # 기간수익률 기준일
        "period_returns": _grid("기간수익률"),  # 기간수익률 {fund/benchmark/...: {1m: 값, ...}}
        "dca_period": _find(r"이 펀드에 (최근 \S+)간\(", page),  # 이 펀드에 최근 n년간 매달 50만원씩 투자했다면? (설정 기간에 따라 3년 또는 1년)
        "dca_invested": _find(r"<dt>총 투자금액</dt>\s*<dd>(.*?)</dd>", page),  # 총 투자금액
        "dca_value": _find(r"<dt>평가액</dt>\s*<dd>(.*?)</dd>", page),  # 평가액
        "dca_return": _find(r'<div class="return_box">.*?<dt>수익률</dt>\s*<dd>(.*?)</dd>', page),  # 수익률
        "class_net_assets": _find(r"클래스 규모는 순자산기준 (.*?)\(", page),  # 해당 클래스 규모 순자산기준
        "class_principal": _find(r"클래스 규모는 순자산기준 .*?\(설정액기준 (.*?)\)", page),  # 해당 클래스 규모 설정액기준
        "next_settlement_date": _find(r"<li>차기 결산예정일 : (.*?)</li>", page),  # 차기 결산예정일
        "quarterly_returns": _grid(
            "3개월 구간수익률",
            json.loads(quarters.group(1))["categories"] if quarters and quarters.group(1) else None,
        ),  # 3개월 구간수익률 {fund/benchmark/...: {2023.12: 값, ...}}
        "quarterly_top50_count": _find(r"상위 50% 이상의 성과를 올린 횟수는 <strong>(.*?)</strong>", page),  # 동일 유형(소) 안에서 상위 50% 이상의 성과를 올린 횟수
        "quarterly_outperform_count": _find(r"초과성과를 낸 횟수는 (.*?)입니다", page),  # 구간수익률이 비교지수 대비 초과성과를 낸 횟수
        "risk_metrics_base_date": _find(r"<h6>변동성 지표</h6>.*?<dt>기준일</dt>\s*<dd>(.*?)</dd>", page),  # 변동성 지표 기준일
        "risk_metrics": _grid("위험지표 상세보기"),  # 위험지표 상세보기 {std_dev/std_dev_rank/...: {3m: 값, ...}}
    }


def parse_fund_rating_html(page: str) -> dict[str, str | int | None]:
    page = re.sub(r"<!--.*?-->", "", page, flags=re.DOTALL)  # 에프앤가이드·한국펀드평가 표는 주석 처리돼 있음

    def _table(caption: str) -> str:
        m = re.search(rf"<caption>{caption}</caption>(.*?)</table>", page, re.DOTALL)
        return m.group(1) if m else ""

    def _grades(s: str) -> list[int]:  # "5등급중 1등급" → 1, "5등급중 무등급" → 0
        return [int(g) if g.isdigit() else 0 for g in re.findall(r'<i class="blind">5등급중 (\d+|무)등급</i>', s)]

    zeroin, mstar = _table("제로인의 평가등급"), _table("모닝스타의 평가등급")
    z, m = _grades(zeroin) + [None] * 2, _grades(mstar) + [None] * 4
    # 수익률/위험 세부항목: "10등급중 10등급" → 10 (3년 수익률, 3년 위험, 5년 수익률, 5년 위험 순)
    steps = [int(g) for g in re.findall(r'alt="10등급중 (\d+)등급"', zeroin)] + [None] * 4
    return {
        "zeroin_group": _find(r"<dt>\( 평가그룹</dt>\s*<dd>(.*?) \)</dd>", page),  # 제로인 평가그룹
        "zeroin_rating_3y": z[0],  # 제로인 성과 등급 3년 (0=무등급)
        "zeroin_return_grade_3y": steps[0],  # 제로인 3년 세부항목 수익률 (10등급중)
        "zeroin_risk_grade_3y": steps[1],  # 제로인 3년 세부항목 위험 (10등급중)
        "zeroin_rating_5y": z[1],  # 제로인 성과 등급 5년 (0=무등급)
        "zeroin_return_grade_5y": steps[2],  # 제로인 5년 세부항목 수익률 (10등급중)
        "zeroin_risk_grade_5y": steps[3],  # 제로인 5년 세부항목 위험 (10등급중)
        "morningstar_rating_overall": m[0],  # 모닝스타 종합 (0=무등급)
        "morningstar_rating_3y": m[1],  # 모닝스타 3년
        "morningstar_rating_5y": m[2],  # 모닝스타 5년
        "morningstar_rating_10y": m[3],  # 모닝스타 10년
    }


def parse_fund_portfolio_html(page: str) -> dict[str, object]:
    page = re.sub(r"<!--.*?-->", "", page, flags=re.DOTALL)

    # 채권종류별·채권신용등급별 보유비중 행 이름 → JSON 키 (AAA, AA, A, BBB, BB 는 그대로)
    row_keys = {
        "국채": "government",
        "지방채": "municipal",
        "통안증권": "monetary_stabilization",
        "금융채": "financial",
        "회사채": "corporate",
        "특수채": "special",
        "기타": "other",
        "B이하": "B_or_below",
        "무등급": "unrated",
    }

    def _num(s: str) -> float | None:  # "-" 같은 빈 값은 None
        try:
            return float(_text(s).rstrip("%").replace(",", ""))
        except ValueError:
            return None

    def _table(caption: str) -> str:
        m = re.search(rf"<caption>{re.escape(caption)}</caption>(.*?)</table>", page, re.DOTALL)
        return m.group(1) if m else ""

    def _top(*captions: str) -> list[dict]:  # 순위 / 순위 변동 / 이름 / 비중(%) 표
        rows = re.findall(r'<th scope="row"[^>]*>(\d+)</th>\s*<td>(.*?)</td>\s*<td>(.*?)</td>\s*<td>(.*?)</td>', "".join(map(_table, captions)), re.DOTALL)
        return [
            {
                "rank": int(rank),
                "rank_change": ("-" if "icon-updown down" in change else "+" if "icon-updown up" in change else "") + _text(re.sub(r'<em class="hd-text">.*?</em>', "", change)),
                "name": _text(name),
                "weight": _num(weight),
            }
            for rank, change, name, weight in rows
            if _text(name) != "-"
        ]

    def _by_date(caption: str) -> dict[str, dict[str, float | None]]:  # 구분 x 기준일 표 → {기준일: {구분: 비중}}
        t = _table(caption)
        dates = re.findall(r'<th scope="col">(\d{4}\.\d\d\.\d\d)</th>', t)
        rows = [(row_keys.get(_text(name), _text(name)), re.findall(r"<td>(.*?)</td>", tds, re.DOTALL)) for name, tds in re.findall(r'<th scope="row"[^>]*>(.*?)</th>(.*?)</tr>', t, re.DOTALL)]
        return {d: {name: _num(tds[i]) for name, tds in rows} for i, d in enumerate(dates)}

    def _style(caption: str) -> tuple[str, str, dict[str, float | None]]:  # 대표 스타일, (최근 12개월 중 n회), 칸별 보유비중
        m = re.search(rf'<p class="txt">\s*<strong>([^<]*)</strong>([^<]*)</p>\s*<div class="chart_table">\s*<table[^>]*>\s*<caption>{re.escape(caption)}</caption>', page, re.DOTALL)
        cells = re.findall(r'<span class="blind">(.*?)</span>(.*?)</td>', _table(caption))
        return (_text(m.group(1)), _text(m.group(2)).strip("()"), {k: _num(v) for k, v in cells}) if m else ("", "", {})

    def _chart(func: str) -> tuple[list, list]:  # 차트 스크립트의 data, cate
        m = re.search(rf"function {func}\(\)\{{.*?data = \$\.parseJSON\('(.*?)'\);\s*cate = \$\.parseJSON\('(.*?)'\);", page, re.DOTALL)
        return (json.loads(m.group(1)), json.loads(m.group(2))) if m and m.group(1) else ([], [])

    sector, sector_dates = _chart("createIndtpCharts")
    duration, duration_dates = _chart("createHoldingSectorCharts")
    assets = {k: (a, w) for k, a, w in re.findall(r'<th scope="row" class="first">(.*?)</th>\s*<td>(.*?)</td>\s*<td>(.*?)</td>', _table("자산구성 비중"), re.DOTALL)}
    stock_style, bond_style = _style("주식 스타일 및 보유비중(%)"), _style("채권스타일 및 보유비중(%)")
    row = {"portfolio_date": _find(r"<span>제로인\((.*?)\) 제공</span>", page)}  # 자산구성 비중 기준일 (제로인 제공)
    for name, key in (("주식", "stock"), ("채권", "bond"), ("유동성", "cash"), ("기타", "other")):  # 자산구성 비중
        amount, weight = assets.get(name, ("", ""))
        row[f"{key}_amount"] = _text(amount)  # 평가액(억원)
        row[f"{key}_weight"] = _text(weight)  # 펀드내 비중(%)
    return {
        **row,
        "top5_countries": _top("투자국가 TOP1~5"),  # 투자국가 TOP 5
        "stock_style": stock_style[0],  # 주식 스타일 및 보유비중(%) 대표 스타일
        "stock_style_freq_last_12m": stock_style[1],  # 최근 12개월 중 n회 (대표 스타일로 분류된 횟수)
        "stock_style_weights": stock_style[2],  # 주식 스타일 및 보유비중(%) {대형가치주: 비중, ...}
        "sector_weights": {d: {s["name"]: s["data"][i] for s in sector} for i, d in enumerate(sector_dates[: len(sector[0]["data"])])} if sector else {},  # 업종별 투자비중(%) {기준일: {업종: 비중}}
        "top10_stocks": _top("보유주식 TOP1~5", "보유주식 TOP6~10"),  # 보유주식 TOP10
        "bond_style": bond_style[0],  # 채권스타일 및 보유비중(%) 대표 스타일
        "bond_style_freq_last_12m": bond_style[1],  # 최근 12개월 중 n회 (대표 스타일로 분류된 횟수)
        "bond_style_weights": bond_style[2],  # 채권스타일 및 보유비중(%) {상등급단기채: 비중, ...}
        "avg_duration": dict(zip(duration_dates, duration[-1]["data"], strict=False)) if duration else {},  # 평균 듀레이션(년) {기준일: 값}
        "bond_type_weights": _by_date("채권종류별 보유비중"),  # 채권종류별 보유비중 {기준일: {government: 비중, ...}}
        "credit_rating_weights": _by_date("채권신용등급별 보유비중"),  # 채권신용등급별 보유비중 {기준일: {AAA: 비중, ...}}
        "top10_bonds": _top("보유채권 TOP1~5", "보유채권 TOP6~10"),  # 보유채권 TOP10
    }



# ---------------------------------------------------------------------------- 실행부 (원본 main() 대체)

# 관련 문서 file_gb → 저장 파일명 (원본 download_fund_documents 와 같음)
DOCUMENT_FILES = {
    "R1": "collective_investment_agreement.pdf",  # 집합투자규약
    "R2": "statutory_prospectus.pdf",  # 투자설명서
    "R3": "summary_prospectus.pdf",  # 간이투자설명서
    "R5": "asset_management_report.pdf",  # 운용보고서
    "R8": "asset_safekeeping_and_management_report.pdf",  # 자산관리보고서
}
KEY_DOCUMENT_FILE = "key_summary_explanation.pdf"  # 핵심(요약)상품설명서

# fundDetail 폼 이름 → JSON 키 (None 은 저장 안 함, 원본 main() 의 form_keys)
FORM_KEYS = {
    "kofia_fund_code": None,
    "lrg_type_code_inout": "portfolio_provider_code",
    "fp_code": "product_code",
    "fp_name": "fund_name",
    "fpClassTp": "fund_class_type",
    "classTypeGb": "class_type_code",
    "strFundBaseDt": None,
    "strMaxDate": "max_date",
    "strFundGrdDt": "rating_month",
    "strFundTermMonth6": "six_months_ago_date",
    "strRiskIndctDt": None,
    "strFundPtflDt": None,
    "fundTypeCode": "fund_type_code",
    "gradeGbn": "rating_agency_code",
}


def _iso_dates(v: object) -> object:
    """값과 dict 키의 날짜 표기를 yyyy-mm-dd / yyyy-mm 으로 (원본 main() 내부 함수)"""
    if isinstance(v, dict):
        return {_iso_dates(k): _iso_dates(x) for k, x in v.items()}
    if isinstance(v, list):
        return [_iso_dates(x) for x in v]
    if isinstance(v, str):
        return re.sub(r"^(\d{4})\.(\d{1,2})(?:\.(\d{1,2}))?(?=$| )", lambda m: "-".join([m[1], m[2].zfill(2)] + ([m[3].zfill(2)] if m[3] else [])), v)
    return v


MANIFEST = ".docs.json"  # 문서별 {doc_date: 문서 안의 기준일, text_sha: 본문 해시, version: 링크 파일명, updated_at}
_DATE = re.compile(r"(20\d{2})\s*[.\-년/]\s*(\d{1,2})\s*[.\-월/]\s*(\d{1,2})")


def _pdf_info(data: bytes) -> tuple[str, str]:
    """(본문 텍스트 해시, 첫 2쪽에 나오는 가장 늦은 날짜 YYYY-MM-DD).

    핵심설명서처럼 요청마다 새로 만들어지는 PDF 는 바이트가 매번 달라서 텍스트로 비교한다.
    날짜는 작성기준일·운용기간·심사필 기간 같은 판 구분용 (서버가 가끔 예전 판을 돌려줘서 되돌아가지 않게 막는 데 씀).
    """
    with _pdfium_lock:
        doc = pdfium.PdfDocument(data)
        try:
            pages = [page.get_textpage().get_text_bounded() for page in doc]
        finally:
            doc.close()
    def _max_date(text: str) -> str:
        return max((f"{y}-{int(m):02d}-{int(d):02d}" for y, m, d in _DATE.findall(text) if 1 <= int(m) <= 12 and 1 <= int(d) <= 31), default="")

    # 첫 2쪽(작성기준일·운용기간)에 없으면 전체(규약의 부칙 시행일 등)에서 찾음
    doc_date = _max_date(" ".join(pages[:2])) or _max_date(" ".join(pages))
    return hashlib.sha256("\n".join(pages).encode("utf-8")).hexdigest(), doc_date


def fetch_pdf(s: requests.Session, url: str, data: dict[str, str], attempts: int = 3) -> bytes | None:
    """PDF 를 받아 돌려줌. 깨진 응답이 종종 와서 attempts 번까지 다시 받음 (원본은 별도 재시도 스레드)."""
    for i in range(attempts):
        try:
            r = s.post(url, data=data, timeout=120)
            if r.ok and r.content[:4] == b"%PDF" and _is_valid_pdf(r.content):
                return r.content
        except requests.RequestException:
            pass
        time.sleep(2 * (i + 1))
    return None


class _Docs:
    """펀드 폴더의 문서 갱신기. 내용이 바뀐 문서만 덮어쓰고 manifest 에 기록."""

    def __init__(self, s: requests.Session, fund_dir: Path, postfix: str, result: ItemResult, log: logging.Logger) -> None:
        self.s, self.dir, self.postfix, self.result, self.log = s, fund_dir, postfix, result, log
        self.path = fund_dir / MANIFEST
        self.manifest = json.loads(self.path.read_text(encoding="utf-8")) if self.path.exists() else {}
        self.failed: list[str] = []

    def update(self, name: str, version: str | None, url: str, data: dict[str, str]) -> None:
        """문서를 받아서 본문이 달라졌으면 덮어씀. 단, 문서 안의 기준일이 기존보다 이르면 예전 판으로 보고 무시.

        version(링크 파일명)은 기록만 함. 같은 파일명에도 서버가 다른 판을 주는 경우가 있어서 건너뛰는 근거로 쓰지 않음.
        """
        path, entry = self.dir / (name + self.postfix), self.manifest.get(name, {})
        content = fetch_pdf(self.s, url, data)
        if content is None:
            self.failed.append(name)
            return
        sha, doc_date = _pdf_info(content)
        if "text_sha" not in entry and path.exists() and _is_valid_pdf(old := path.read_bytes()):
            entry = dict(zip(("text_sha", "doc_date"), _pdf_info(old), strict=True))  # manifest 없이 있던 파일 (HF 이관분 등)
        if sha == entry.get("text_sha"):
            self.manifest[name] = {**entry, "version": version}
            return
        if doc_date and entry.get("doc_date") and doc_date < entry["doc_date"]:
            self.result.count("docs_stale_ignored")
            self.log.warning(f"{self.dir.name}/{name}: 서버가 예전 판({doc_date})을 줌, 기존 판({entry['doc_date']}) 유지")
            return
        safe_write(path, content)
        self.result.count("docs_updated" if "text_sha" in entry else "docs_new")
        self.manifest[name] = {"doc_date": doc_date, "text_sha": sha, "version": version, "updated_at": f"{date.today():%Y-%m-%d}"}

    def save(self) -> None:
        safe_write(self.path, json.dumps(self.manifest, ensure_ascii=False, indent=1).encode("utf-8"))


def update_fund_documents(docs: _Docs, page: str) -> None:
    """관련 문서 PDF 갱신 (원본 download_fund_documents 의 링크 파싱 그대로)."""
    links = re.findall(
        r"""fileDownLoadNo == "(\d+)"\) \{\s*\$\('#pathType'\)\.val\(fileDownLoadNo\);\s*\$\('#fileName'\)\.val\("(.*?)"\);.*?\$\('#pfund_cd'\)\.val\("(.*?)"\);\s*\$\('#file_gb'\)\.val\("(\w+)"\)""",
        page,
        re.DOTALL,
    )
    for path_type, file_name, pfund_cd, file_gb in links:
        file_name = html.unescape(file_name)
        data = {"pathType": path_type, "fileName": file_name, "pfund_cd": pfund_cd, "file_gb": file_gb}
        # 규약·투자설명서는 파일명에 기준일이 있음 (R2_1176559_20160929.pdf), 운용·자산관리보고서는 "." (항상 최신본)
        version = file_name if file_name not in ("", ".") else None
        docs.update(DOCUMENT_FILES.get(file_gb, f"{file_gb}.pdf"), version, f"{BASE}/com/noticeFile/pdfFileDown.do", data)
    pfund = re.search(r'<form id="keyDocumentReport".*?name="pfund_cd" value="(\w+)"', page, re.DOTALL)
    if pfund:  # 핵심설명서: 보고서 XML 로 PDF 를 그때그때 생성 (고난도 상품은 양식이 다름)
        r = docs.s.post(f"{BASE}/fmm/FMM1030102/keyDocumentReport.do", data={"pfund_cd": pfund.group(1)}, timeout=60)
        if not r.ok:
            docs.failed.append(KEY_DOCUMENT_FILE)
        else:
            report = "FMM1030102" if 'id="cmplxIcon"' in page else "FMM1030102_1"
            docs.update(KEY_DOCUMENT_FILE, None, f"{BASE}/com/noticeFile/directDownloadReport.do", {"pdfFileName": report, "pdfFileXml": r.text})


def fetch_fund_list(out: Path) -> int:
    """전체 펀드 목록을 CSV 로 저장하고 행 수를 돌려줌."""
    rows = parse_fund_list_html(fetch_fund_list_html(page_cnt=100000))
    if not rows:
        raise RuntimeError("펀드 목록이 비어 있음 (사이트 구조 변경 확인)")
    write_csv(out, rows)
    return len(rows)


def fetch_fund_details(fund_list: Path, out_dir: Path, *, log: logging.Logger, pdf_postfix: str = "", workers: int = 8, limit: int | None = None, retries: int = 2) -> ItemResult:
    """펀드 목록의 펀드별로 out_dir/<fund_code>/ 를 최신으로 갱신.

    - details.json: 매번 새로 받아 덮어씀 (수익률·규모 등이 계속 바뀜)
    - 문서 PDF: 매번 받아서 본문이 달라졌을 때만 덮어씀. 문서 안 기준일이 더 이르면(서버가 예전 판을 줌) 무시 → manifest(.docs.json)
    - 목록에서 빠진 펀드의 폴더는 지우지 않음
    """
    codes = read_csv_column(fund_list, "fund_code")[:limit]
    local = threading.local()
    result = ItemResult()

    def _session() -> requests.Session:
        if not hasattr(local, "s"):
            local.s = requests.Session()
            local.s.headers["User-Agent"] = "Mozilla/5.0"
            local.s.get(BASE, timeout=10)  # 세션 쿠키 발급
        return local.s

    def _fetch(code: str) -> None:
        s, fund_dir = _session(), out_dir / code
        r = s.post(f"{BASE}/fmm/FMM1030101/main.do", data={"kofia_fund_code": code}, timeout=30)
        r.raise_for_status()
        page = r.text
        form_html = re.search(r'<form id="fundDetail".*?</form>', page, re.DOTALL).group(0)
        form = {k: html.unescape(v) for k, v in re.findall(r'name="(\w+)" id="\w+" value="([^"]*)"', form_html)}
        detail = {
            "fund_code": code,
            **{FORM_KEYS.get(k, k): re.sub(r"^(\d{4})(\d{2})(\d{2})?$", lambda m: "-".join(filter(None, m.groups())), v) if k.startswith("str") else v for k, v in form.items() if FORM_KEYS.get(k, k)},
            **parse_fund_detail_html(fetch_fund_detail_html(s, form, 1)),
            **parse_fund_performance_html(fetch_fund_detail_html(s, form, 2)),
            **parse_fund_rating_html(fetch_fund_detail_html(s, form, 3)),
            **parse_fund_portfolio_html(fetch_fund_detail_html(s, form, 4)),
        }
        safe_write(fund_dir / "details.json", json.dumps(_iso_dates(detail), ensure_ascii=False, indent=2).encode("utf-8"))

        docs = _Docs(s, fund_dir, pdf_postfix, result, log)
        try:
            update_fund_documents(docs, page)
        finally:
            docs.save()
        if docs.failed:
            raise RuntimeError(f"문서 다운로드 실패 {docs.failed}")

    return run_items(codes, _fetch, log=log, workers=workers, retries=retries, result=result)
