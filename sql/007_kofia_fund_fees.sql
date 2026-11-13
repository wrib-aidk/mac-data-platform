-- 007: KOFIA 펀드별 보수비용비교 (collectors/kofia_fees.py → data/raw/kofia/fees/<기준일>.csv)
CREATE TABLE IF NOT EXISTS raw.kofia_fund_fees (
    standard_dt   date NOT NULL,        -- 공시 기준일 (파일명)
    standard_cd   text NOT NULL,        -- 펀드 표준코드
    company_nm    text,
    fund_nm       text,
    fund_type     text,
    start_dt      text,                 -- 설정일 YYYYMMDD
    manage_fee    numeric,
    sale_fee      numeric,
    trust_fee     numeric,
    admin_fee     numeric,
    total_fee     numeric,
    peer_avg_fee  numeric,
    other_expense numeric,
    ter           numeric,
    front_load    numeric,
    back_load     numeric,
    trading_fee   numeric,
    PRIMARY KEY (standard_dt, standard_cd)
);
CREATE INDEX IF NOT EXISTS kofia_fund_fees_standard_cd_idx ON raw.kofia_fund_fees (standard_cd);

COMMENT ON TABLE raw.kofia_fund_fees IS 'KOFIA 전자공시 펀드별 보수비용비교 (펀드·ETF 전체), 공시 기준일·표준코드별';
COMMENT ON COLUMN raw.kofia_fund_fees.standard_dt IS '공시 기준일 (보통 월말 영업일)';
COMMENT ON COLUMN raw.kofia_fund_fees.standard_cd IS '펀드 표준코드 (KRX 단축코드·ISIN 과 다름)';
COMMENT ON COLUMN raw.kofia_fund_fees.company_nm IS '운용회사';
COMMENT ON COLUMN raw.kofia_fund_fees.fund_nm IS '펀드명 (ETF 는 상장지수 포함)';
COMMENT ON COLUMN raw.kofia_fund_fees.fund_type IS '펀드유형 (주식형, 채권형, 재간접형 등)';
COMMENT ON COLUMN raw.kofia_fund_fees.start_dt IS '설정일 (YYYYMMDD)';
COMMENT ON COLUMN raw.kofia_fund_fees.manage_fee IS '운용보수율 (%)';
COMMENT ON COLUMN raw.kofia_fund_fees.sale_fee IS '판매보수율 (%)';
COMMENT ON COLUMN raw.kofia_fund_fees.trust_fee IS '수탁보수율 (%)';
COMMENT ON COLUMN raw.kofia_fund_fees.admin_fee IS '사무관리보수율 (%)';
COMMENT ON COLUMN raw.kofia_fund_fees.total_fee IS '보수율 합계 A (%)';
COMMENT ON COLUMN raw.kofia_fund_fees.peer_avg_fee IS '유사유형 평균보수율 (%)';
COMMENT ON COLUMN raw.kofia_fund_fees.other_expense IS '기타비용 B (%)';
COMMENT ON COLUMN raw.kofia_fund_fees.ter IS '총보수·비용비율 TER = A+B (%)';
COMMENT ON COLUMN raw.kofia_fund_fees.front_load IS '판매수수료 선취 (%)';
COMMENT ON COLUMN raw.kofia_fund_fees.back_load IS '판매수수료 후취 (%)';
COMMENT ON COLUMN raw.kofia_fund_fees.trading_fee IS '매매·중개수수료율 D (%)';
