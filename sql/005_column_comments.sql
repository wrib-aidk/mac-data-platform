-- 005: 테이블·컬럼 설명 (COMMENT). Dagster asset 화면의 Columns 에 설명으로 나오고, DB 툴에서도 보입니다.
--      컬럼을 추가하면 새 sql 파일에 COMMENT ON COLUMN 도 같이 넣어 주세요. 예시값은 적재할 때 실제 데이터에서 뽑아 붙입니다.

-- ---------------------------------------------------------------- 펀드슈퍼마켓
COMMENT ON TABLE raw.fundsupermarket_fund_list IS '펀드슈퍼마켓 펀드 목록, 수집일별 스냅샷';
COMMENT ON COLUMN raw.fundsupermarket_fund_list.snapshot_date IS '수집일';
COMMENT ON COLUMN raw.fundsupermarket_fund_list.fund_code IS '펀드코드 (KOFIA 표준코드)';
COMMENT ON COLUMN raw.fundsupermarket_fund_list.fund_name IS '펀드명';
COMMENT ON COLUMN raw.fundsupermarket_fund_list.data IS '목록 CSV 한 행 그대로 (수익률·설정액 등, 화면 표시용 문자열)';

COMMENT ON TABLE raw.fundsupermarket_fund_details IS '펀드슈퍼마켓 펀드 상세 (details.json), 수집일별 스냅샷';
COMMENT ON COLUMN raw.fundsupermarket_fund_details.fund_code IS '펀드코드 (KOFIA 표준코드)';
COMMENT ON COLUMN raw.fundsupermarket_fund_details.snapshot_date IS '수집일';
COMMENT ON COLUMN raw.fundsupermarket_fund_details.fund_name IS '펀드명';
COMMENT ON COLUMN raw.fundsupermarket_fund_details.data IS 'details.json 전체 (수탁사, 투자전략, 보수 등)';

COMMENT ON TABLE raw.fundsupermarket_documents IS '펀드별 공시 PDF 목록 (PDF 는 파일로 두고 위치·크기만)';
COMMENT ON COLUMN raw.fundsupermarket_documents.fund_code IS '펀드코드 (KOFIA 표준코드)';
COMMENT ON COLUMN raw.fundsupermarket_documents.doc_type IS '문서 종류 (summary_prospectus 간이투자설명서, asset_management_report 자산운용보고서 등)';
COMMENT ON COLUMN raw.fundsupermarket_documents.path IS 'PDF 위치 (data/ 기준 상대 경로)';
COMMENT ON COLUMN raw.fundsupermarket_documents.size IS '파일 크기 (바이트)';
COMMENT ON COLUMN raw.fundsupermarket_documents.file_mtime IS '파일 수정 시각';
COMMENT ON COLUMN raw.fundsupermarket_documents.doc_date IS '문서 안의 기준일 (.docs.json 에 있을 때만)';
COMMENT ON COLUMN raw.fundsupermarket_documents.text_sha IS '본문 텍스트 해시 (판이 바뀌었는지 비교용)';

COMMENT ON TABLE extracted.fundsupermarket_document_text IS '공시 PDF 에서 뽑은 텍스트';
COMMENT ON COLUMN extracted.fundsupermarket_document_text.fund_code IS '펀드코드 (KOFIA 표준코드)';
COMMENT ON COLUMN extracted.fundsupermarket_document_text.doc_type IS '문서 종류 (raw.fundsupermarket_documents.doc_type 과 같음)';
COMMENT ON COLUMN extracted.fundsupermarket_document_text.text IS '본문 텍스트 (Markdown)';
COMMENT ON COLUMN extracted.fundsupermarket_document_text.chars IS '본문 글자 수';
COMMENT ON COLUMN extracted.fundsupermarket_document_text.updated_at IS '.md 파일 수정 시각';

-- ---------------------------------------------------------------- SEIBro
COMMENT ON TABLE raw.seibro_funds IS 'SEIBro 전체 펀드 목록 (ISIN 기준)';
COMMENT ON COLUMN raw.seibro_funds.isin IS '펀드 ISIN (KR...)';
COMMENT ON COLUMN raw.seibro_funds.fund_name IS '펀드명 (클래스 포함)';
COMMENT ON COLUMN raw.seibro_funds.fund_class_code IS 'SEIBro 펀드 분류코드';

COMMENT ON TABLE raw.seibro_fund_nav IS 'SEIBro 펀드별 일별 기준가';
COMMENT ON COLUMN raw.seibro_fund_nav.isin IS '펀드 ISIN';
COMMENT ON COLUMN raw.seibro_fund_nav.date IS '기준일';
COMMENT ON COLUMN raw.seibro_fund_nav.nav IS '기준가격 (원)';
COMMENT ON COLUMN raw.seibro_fund_nav.nav_change IS '기준가격 전일대비 (원)';
COMMENT ON COLUMN raw.seibro_fund_nav.nav_change_pct IS '기준가격 전일대비 등락률 (%)';
COMMENT ON COLUMN raw.seibro_fund_nav.tax_nav IS '과표기준가격 (원)';
COMMENT ON COLUMN raw.seibro_fund_nav.principal IS '설정원본 (SEIBro 응답값 그대로)';
COMMENT ON COLUMN raw.seibro_fund_nav.net_asset IS '순자산총액 (SEIBro 응답값 그대로)';
COMMENT ON COLUMN raw.seibro_fund_nav.dividend IS '분배금 (분배가 있던 날만)';
COMMENT ON COLUMN raw.seibro_fund_nav.note IS '비고';

-- ---------------------------------------------------------------- KOFIA
COMMENT ON TABLE raw.kofia_fund_nav IS 'KOFIA 전자공시 펀드별 일별 기준가';
COMMENT ON COLUMN raw.kofia_fund_nav.fund_code IS '펀드코드 (KOFIA 표준코드)';
COMMENT ON COLUMN raw.kofia_fund_nav.date IS '기준일';
COMMENT ON COLUMN raw.kofia_fund_nav.nav IS '기준가격 (원)';
COMMENT ON COLUMN raw.kofia_fund_nav.nav_change IS '기준가격 전일대비 (원)';
COMMENT ON COLUMN raw.kofia_fund_nav.tax_nav IS '과표기준가격 (원)';
COMMENT ON COLUMN raw.kofia_fund_nav.principal IS '설정원본 (백만원)';

COMMENT ON TABLE raw.kofia_holdings IS 'KOFIA 분기영업보고서 세부현황(유가증권내역): 펀드·ETF별 보유종목, 펀드 1개 = 여러 행';
COMMENT ON COLUMN raw.kofia_holdings.std_dt IS '기준일 (분기말 3/31·6/30·9/30·12/31)';
COMMENT ON COLUMN raw.kofia_holdings.standard_cd IS '펀드 표준코드 (raw.kofia_funds.standard_cd)';
COMMENT ON COLUMN raw.kofia_holdings.line_no IS '보고서 안의 행 번호 (1부터)';
COMMENT ON COLUMN raw.kofia_holdings.company_cd IS '운용사 코드';
COMMENT ON COLUMN raw.kofia_holdings.item_nm IS '종목명 (종목코드는 제공되지 않음)';
COMMENT ON COLUMN raw.kofia_holdings.security_type IS '유가증권 종류 (주식, 채권, 선물, 위탁증거금 등)';
COMMENT ON COLUMN raw.kofia_holdings.quantity IS '수량 (주식은 주, 채권은 액면 원)';
COMMENT ON COLUMN raw.kofia_holdings.acquisition_amt IS '취득가액 (원)';
COMMENT ON COLUMN raw.kofia_holdings.market_amt IS '시가평가액 (원)';
COMMENT ON COLUMN raw.kofia_holdings.book_amt IS '장부평가액 (원)';
COMMENT ON COLUMN raw.kofia_holdings.issue_region IS '발행지 (국내 / 해외)';
COMMENT ON COLUMN raw.kofia_holdings.listing IS '상장등록여부 (상장 / 비상장)';
COMMENT ON COLUMN raw.kofia_holdings.issue_dt IS '발행일 (채권 등, 없으면 빈 값)';
COMMENT ON COLUMN raw.kofia_holdings.coupon_rate IS '발행이율 (%)';
COMMENT ON COLUMN raw.kofia_holdings.raw_json IS 'KOFIA 응답 한 행 그대로 (한글 라벨 → 값). 양식이 바뀌어도 원본 보존';
COMMENT ON COLUMN raw.kofia_holdings.loaded_at IS '수집 시각';

COMMENT ON TABLE raw.kofia_funds IS 'KOFIA 펀드 표준코드 목록 (영업보고서를 낸 운용사의 운용 중 펀드)';
COMMENT ON COLUMN raw.kofia_funds.standard_cd IS '펀드 표준코드';
COMMENT ON COLUMN raw.kofia_funds.company_cd IS '운용사 코드';
COMMENT ON COLUMN raw.kofia_funds.fund_nm IS '펀드명';
COMMENT ON COLUMN raw.kofia_funds.fund_type IS '펀드 유형 (주식, 채권, 재간접 등)';
COMMENT ON COLUMN raw.kofia_funds.short_cd IS '단축코드';
COMMENT ON COLUMN raw.kofia_funds.start_dt IS '설정일 (YYYYMMDD)';
COMMENT ON COLUMN raw.kofia_funds.class_cd IS 'KOFIA 분류코드';
COMMENT ON COLUMN raw.kofia_funds.is_etf IS 'ETF 여부 (펀드명에 상장지수)';
COMMENT ON COLUMN raw.kofia_funds.is_private IS '사모펀드 여부 (펀드명에 사모, 보유종목 수집 대상 아님)';
COMMENT ON COLUMN raw.kofia_funds.is_class IS '클래스 펀드 여부 (A, C-Pe 등, 보유종목 수집 대상 아님. 모펀드 수익증권만 나옴)';
COMMENT ON COLUMN raw.kofia_funds.updated_at IS '목록 갱신 시각';

-- ---------------------------------------------------------------- DART
COMMENT ON TABLE raw.dart_corps IS 'Open DART 공시대상 회사 목록 (상장·상장폐지 회사)';
COMMENT ON COLUMN raw.dart_corps.corp_code IS 'DART 고유번호 (8자리)';
COMMENT ON COLUMN raw.dart_corps.corp_name IS '회사명';
COMMENT ON COLUMN raw.dart_corps.corp_eng_name IS '영문 회사명';
COMMENT ON COLUMN raw.dart_corps.stock_code IS '종목코드 (6자리)';
COMMENT ON COLUMN raw.dart_corps.modify_date IS 'DART 정보 최종 변경일';

COMMENT ON TABLE raw.dart_filings IS 'DART 정기보고서 목록 (본문 XML 은 파일로 둠, 보고서마다 최신본만)';
COMMENT ON COLUMN raw.dart_filings.corp_code IS 'DART 고유번호';
COMMENT ON COLUMN raw.dart_filings.fiscal_period IS '결산기 (YYYYMM)';
COMMENT ON COLUMN raw.dart_filings.report IS '보고서 종류 (사업보고서 / 반기보고서 / 분기보고서)';
COMMENT ON COLUMN raw.dart_filings.rcept_no IS '접수번호 (정정본이면 바뀜)';
COMMENT ON COLUMN raw.dart_filings.path IS '본문 XML 위치 (data/ 기준 상대 경로)';
COMMENT ON COLUMN raw.dart_filings.size IS '파일 크기 (바이트)';

-- ---------------------------------------------------------------- KRX
COMMENT ON TABLE raw.krx_stock_list IS 'KRX 코스피·코스닥 종목 기본정보';
COMMENT ON COLUMN raw.krx_stock_list.isu_cd IS '종목코드';
COMMENT ON COLUMN raw.krx_stock_list.data IS 'API 응답 한 행 그대로 (종목명, 상장일, 시장구분 등)';

COMMENT ON TABLE raw.krx_stock_daily IS 'KRX 주식 일별 시세 (컬럼명은 API 출력명 소문자)';
COMMENT ON COLUMN raw.krx_stock_daily.bas_dd IS '거래일';
COMMENT ON COLUMN raw.krx_stock_daily.isu_cd IS '종목코드';
COMMENT ON COLUMN raw.krx_stock_daily.isu_nm IS '종목명';
COMMENT ON COLUMN raw.krx_stock_daily.mkt_nm IS '시장 (KOSPI / KOSDAQ)';
COMMENT ON COLUMN raw.krx_stock_daily.sect_tp_nm IS '소속부 (코스닥 우량기업부 등, 코스피는 빈 값)';
COMMENT ON COLUMN raw.krx_stock_daily.tdd_clsprc IS '종가 (원)';
COMMENT ON COLUMN raw.krx_stock_daily.cmpprevdd_prc IS '전일대비 (원)';
COMMENT ON COLUMN raw.krx_stock_daily.fluc_rt IS '등락률 (%)';
COMMENT ON COLUMN raw.krx_stock_daily.tdd_opnprc IS '시가 (원)';
COMMENT ON COLUMN raw.krx_stock_daily.tdd_hgprc IS '고가 (원)';
COMMENT ON COLUMN raw.krx_stock_daily.tdd_lwprc IS '저가 (원)';
COMMENT ON COLUMN raw.krx_stock_daily.acc_trdvol IS '거래량 (주)';
COMMENT ON COLUMN raw.krx_stock_daily.acc_trdval IS '거래대금 (원)';
COMMENT ON COLUMN raw.krx_stock_daily.mktcap IS '시가총액 (원)';
COMMENT ON COLUMN raw.krx_stock_daily.list_shrs IS '상장주식수 (주)';

COMMENT ON TABLE raw.krx_etf_daily IS 'KRX ETF 일별 시세 (컬럼명은 API 출력명 소문자)';
COMMENT ON COLUMN raw.krx_etf_daily.bas_dd IS '거래일';
COMMENT ON COLUMN raw.krx_etf_daily.isu_cd IS '종목코드';
COMMENT ON COLUMN raw.krx_etf_daily.isu_nm IS '종목명';
COMMENT ON COLUMN raw.krx_etf_daily.tdd_clsprc IS '종가 (원)';
COMMENT ON COLUMN raw.krx_etf_daily.cmpprevdd_prc IS '전일대비 (원)';
COMMENT ON COLUMN raw.krx_etf_daily.fluc_rt IS '등락률 (%)';
COMMENT ON COLUMN raw.krx_etf_daily.nav IS '순자산가치 NAV (원)';
COMMENT ON COLUMN raw.krx_etf_daily.tdd_opnprc IS '시가 (원)';
COMMENT ON COLUMN raw.krx_etf_daily.tdd_hgprc IS '고가 (원)';
COMMENT ON COLUMN raw.krx_etf_daily.tdd_lwprc IS '저가 (원)';
COMMENT ON COLUMN raw.krx_etf_daily.acc_trdvol IS '거래량 (주)';
COMMENT ON COLUMN raw.krx_etf_daily.acc_trdval IS '거래대금 (원)';
COMMENT ON COLUMN raw.krx_etf_daily.mktcap IS '시가총액 (원)';
COMMENT ON COLUMN raw.krx_etf_daily.invstasst_netasst_totamt IS '순자산총액 (원)';
COMMENT ON COLUMN raw.krx_etf_daily.list_shrs IS '상장좌수';
COMMENT ON COLUMN raw.krx_etf_daily.idx_ind_nm IS '기초지수명';
COMMENT ON COLUMN raw.krx_etf_daily.obj_stkprc_idx IS '기초지수 종가';
COMMENT ON COLUMN raw.krx_etf_daily.cmpprevdd_idx IS '기초지수 전일대비';
COMMENT ON COLUMN raw.krx_etf_daily.fluc_rt_idx IS '기초지수 등락률 (%)';

COMMENT ON TABLE raw.krx_etf_constituents IS 'KRX ETF 구성종목 (PDF), 거래일·ETF별';
COMMENT ON COLUMN raw.krx_etf_constituents.trade_date IS '거래일';
COMMENT ON COLUMN raw.krx_etf_constituents.etf_isin IS 'ETF ISIN';
COMMENT ON COLUMN raw.krx_etf_constituents.line_no IS '구성종목 행 번호 (1부터)';
COMMENT ON COLUMN raw.krx_etf_constituents.data IS '웹 응답 한 행 그대로 (구성종목명, 수량, 평가금액 등)';
