-- ============================================================================
-- 001: 기본 스키마
--   raw.*        수집 원본을 그대로 옮긴 테이블 (data/raw 의 파일 → DB). 소스 형식 유지, 숫자/날짜만 타입 변환
--   extracted.*  추출 파이프라인 결과 (data/extracted 또는 다른 테이블 → 가공)
--   etl.*        적재 관리용 (마이그레이션 이력, 파일별 적재 상태)
-- 새 테이블은 이 파일을 고치지 말고 sql/002_xxx.sql 처럼 새 파일로 추가 (자동으로 한 번만 실행됨)
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS extracted;
CREATE SCHEMA IF NOT EXISTS etl;

-- 파일 단위 증분 적재: 지문(크기·수정시각)이 바뀐 파일만 다시 넣음
CREATE TABLE etl.loaded_files (
    table_name  text NOT NULL,
    item        text NOT NULL,          -- 보통 data/ 기준 상대 경로
    fingerprint text NOT NULL,
    loaded_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (table_name, item)
);

-- ---------------------------------------------------------------- 펀드슈퍼마켓
-- 펀드 목록 스냅샷 (수익률·설정액 등이 그 시점 값이라 수집일별로 보관, 보존 기간은 config 의 db.retention_days)
CREATE TABLE raw.fundsupermarket_fund_list (
    snapshot_date date NOT NULL,
    fund_code     text NOT NULL,
    fund_name     text,
    data          jsonb NOT NULL,       -- CSV 한 행 그대로 (값은 화면 표시용 문자열: "연 1.04%", "3,530.87")
    PRIMARY KEY (snapshot_date, fund_code)
);

-- 펀드 상세 스냅샷 (details.json, 수집일별 보관)
CREATE TABLE raw.fundsupermarket_fund_details (
    fund_code     text NOT NULL,
    snapshot_date date NOT NULL,
    fund_name     text,
    data          jsonb NOT NULL,
    PRIMARY KEY (fund_code, snapshot_date)
);
CREATE INDEX ON raw.fundsupermarket_fund_details (snapshot_date);

-- 공시 문서 목록 (PDF 는 파일로 두고 위치·지문만)
CREATE TABLE raw.fundsupermarket_documents (
    fund_code  text NOT NULL,
    doc_type   text NOT NULL,           -- summary_prospectus 등 (파일명에서 .pdf 뺀 것)
    path       text NOT NULL,           -- data/ 기준 상대 경로
    size       bigint NOT NULL,
    file_mtime timestamptz NOT NULL,
    doc_date   date,                    -- 문서 안의 기준일 (.docs.json 에 있을 때만)
    text_sha   text,
    PRIMARY KEY (fund_code, doc_type)
);

-- ---------------------------------------------------------------- SEIBro
CREATE TABLE raw.seibro_funds (
    isin            text PRIMARY KEY,
    fund_name       text,
    fund_class_code text
);

CREATE TABLE raw.seibro_fund_nav (
    isin           text NOT NULL,
    date           date NOT NULL,
    nav            numeric,             -- 기준가격
    nav_change     numeric,             -- 전일대비
    nav_change_pct numeric,             -- 전일대비 등락률 (%)
    tax_nav        numeric,             -- 과표기준가격
    principal      numeric,             -- 설정원본
    net_asset      numeric,             -- 순자산
    dividend       numeric,             -- 분배금
    note           text,
    PRIMARY KEY (isin, date)
);
CREATE INDEX ON raw.seibro_fund_nav (date);

-- ---------------------------------------------------------------- KOFIA
CREATE TABLE raw.kofia_fund_nav (
    fund_code  text NOT NULL,
    date       date NOT NULL,
    nav        numeric,
    nav_change numeric,
    tax_nav    numeric,
    principal  numeric,                 -- 설정원본 (백만원)
    PRIMARY KEY (fund_code, date)
);
CREATE INDEX ON raw.kofia_fund_nav (date);

-- kofia_holdings.db (SQLite) 의 holdings / funds 를 그대로
CREATE TABLE raw.kofia_holdings (
    std_dt          date NOT NULL,      -- 분기말 기준일
    standard_cd     text NOT NULL,      -- 펀드코드
    line_no         integer NOT NULL,
    company_cd      text,
    item_nm         text,
    security_type   text,
    quantity        numeric,
    acquisition_amt numeric,
    market_amt      numeric,
    book_amt        numeric,
    issue_region    text,
    listing         text,
    issue_dt        text,
    coupon_rate     numeric,
    raw_json        jsonb,
    loaded_at       text,
    PRIMARY KEY (std_dt, standard_cd, line_no)
);
CREATE INDEX ON raw.kofia_holdings (std_dt, item_nm);

CREATE TABLE raw.kofia_funds (
    standard_cd text PRIMARY KEY,
    company_cd  text,
    fund_nm     text,
    fund_type   text,
    short_cd    text,
    start_dt    text,
    class_cd    text,
    is_etf      boolean,
    is_private  boolean,
    updated_at  text
);

-- ---------------------------------------------------------------- DART
CREATE TABLE raw.dart_corps (
    corp_code     text PRIMARY KEY,
    corp_name     text,
    corp_eng_name text,
    stock_code    text,
    modify_date   date
);

-- 정기보고서 목록 (본문 XML 은 파일로 둠)
CREATE TABLE raw.dart_filings (
    corp_code     text NOT NULL,
    fiscal_period text NOT NULL,        -- 결산기 YYYYMM
    report        text NOT NULL,        -- 사업보고서 / 반기보고서 / 분기보고서
    rcept_no      text NOT NULL,        -- 접수번호 (정정본이면 바뀜)
    path          text NOT NULL,
    size          bigint NOT NULL,
    PRIMARY KEY (corp_code, fiscal_period, report)
);

-- ---------------------------------------------------------------- KRX
-- 종목기본정보: API 응답 필드가 많고 바뀔 수 있어 jsonb 로 보관
CREATE TABLE raw.krx_stock_list (
    isu_cd text PRIMARY KEY,
    data   jsonb NOT NULL
);

-- 일별 시세: 컬럼명은 API 출력명 소문자 그대로
CREATE TABLE raw.krx_stock_daily (
    bas_dd        date NOT NULL,
    isu_cd        text NOT NULL,
    isu_nm        text,
    mkt_nm        text,
    sect_tp_nm    text,
    tdd_clsprc    numeric,              -- 종가
    cmpprevdd_prc numeric,              -- 대비
    fluc_rt       numeric,              -- 등락률
    tdd_opnprc    numeric,              -- 시가
    tdd_hgprc     numeric,              -- 고가
    tdd_lwprc     numeric,              -- 저가
    acc_trdvol    numeric,              -- 거래량
    acc_trdval    numeric,              -- 거래대금
    mktcap        numeric,              -- 시가총액
    list_shrs     numeric,              -- 상장주식수
    PRIMARY KEY (bas_dd, isu_cd)
);
CREATE INDEX ON raw.krx_stock_daily (isu_cd, bas_dd);

CREATE TABLE raw.krx_etf_daily (
    bas_dd                   date NOT NULL,
    isu_cd                   text NOT NULL,
    isu_nm                   text,
    tdd_clsprc               numeric,
    cmpprevdd_prc            numeric,
    fluc_rt                  numeric,
    nav                      numeric,
    tdd_opnprc               numeric,
    tdd_hgprc                numeric,
    tdd_lwprc                numeric,
    acc_trdvol               numeric,
    acc_trdval               numeric,
    mktcap                   numeric,
    invstasst_netasst_totamt numeric,   -- 순자산총액
    list_shrs                numeric,
    idx_ind_nm               text,      -- 기초지수명
    obj_stkprc_idx           numeric,   -- 기초지수 종가
    cmpprevdd_idx            numeric,
    fluc_rt_idx              numeric,
    PRIMARY KEY (bas_dd, isu_cd)
);
CREATE INDEX ON raw.krx_etf_daily (isu_cd, bas_dd);

-- ETF 구성종목 (웹 응답 필드를 jsonb 로)
CREATE TABLE raw.krx_etf_constituents (
    trade_date date NOT NULL,
    etf_isin   text NOT NULL,
    line_no    integer NOT NULL,
    data       jsonb NOT NULL,
    PRIMARY KEY (trade_date, etf_isin, line_no)
);

-- ============================================================================ extracted
-- 공시 PDF 텍스트 (fundsupermarket/fund_documents_text 결과)
CREATE TABLE extracted.fundsupermarket_document_text (
    fund_code  text NOT NULL,
    doc_type   text NOT NULL,
    text       text NOT NULL,
    chars      integer NOT NULL,
    updated_at timestamptz NOT NULL,    -- .md 파일 수정 시각
    PRIMARY KEY (fund_code, doc_type)
);

-- KRX 정제본 (HF Won1026/krx-quotes, 만드는 코드는 아직 없음)
CREATE TABLE extracted.krx_stocks (
    ticker     text PRIMARY KEY,
    name       text,
    market     text,
    section    text,
    updated_at date
);

CREATE TABLE extracted.krx_etfs (
    ticker     text PRIMARY KEY,
    name       text,
    index_name text,
    updated_at date
);

CREATE TABLE extracted.krx_stock_quotes (
    ticker          text NOT NULL,
    date            date NOT NULL,
    price_close     bigint,
    change          bigint,
    change_pct      numeric(18, 2),
    price_open      bigint,
    price_high      bigint,
    price_low       bigint,
    volume          bigint,
    trading_value   bigint,
    market_cap      bigint,
    listed_shares   bigint,
    adj_factor      double precision,
    adj_price_open  numeric(18, 6),
    adj_price_high  numeric(18, 6),
    adj_price_low   numeric(18, 6),
    adj_price_close numeric(18, 6),
    PRIMARY KEY (ticker, date)
);
CREATE INDEX ON extracted.krx_stock_quotes (date);

CREATE TABLE extracted.krx_etf_quotes (
    ticker           text NOT NULL,
    date             date NOT NULL,
    price_close      bigint,
    change           bigint,
    change_pct       numeric(18, 2),
    nav              numeric(18, 2),
    price_open       bigint,
    price_high       bigint,
    price_low        bigint,
    volume           bigint,
    trading_value    bigint,
    market_cap       bigint,
    net_assets       bigint,
    listed_shares    bigint,
    index_close      numeric(18, 2),
    index_change     numeric(18, 2),
    index_change_pct numeric(18, 2),
    adj_factor       double precision,
    adj_price_open   numeric(18, 6),
    adj_price_high   numeric(18, 6),
    adj_price_low    numeric(18, 6),
    adj_price_close  numeric(18, 6),
    PRIMARY KEY (ticker, date)
);
CREATE INDEX ON extracted.krx_etf_quotes (date);
