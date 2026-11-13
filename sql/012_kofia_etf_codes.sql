-- 012: KRX ETF 종목코드 → KOFIA 펀드 표준코드 (extractors/kofia_holdings.py build_etf_codes). kofia_fund_links 가 ETF 를 이 표로 맞춤
CREATE TABLE extracted.kofia_etf_codes (
    isu_cd      text PRIMARY KEY,
    isu_nm      text NOT NULL,
    standard_cd text NOT NULL,
    fund_nm     text,
    method      text NOT NULL
);
CREATE INDEX kofia_etf_codes_standard_cd_idx ON extracted.kofia_etf_codes (standard_cd);

COMMENT ON TABLE extracted.kofia_etf_codes IS 'KRX ETF 종목코드 → KOFIA 펀드 표준코드. KRX ETF 전체(상장폐지 포함)를 KOFIA ETF 전체(보수비용·펀드 목록)와 맞춤';
COMMENT ON COLUMN extracted.kofia_etf_codes.isu_cd IS 'KRX 종목코드 (raw.krx_etf_daily.isu_cd)';
COMMENT ON COLUMN extracted.kofia_etf_codes.isu_nm IS 'KRX 종목명 (가장 최근 거래일 기준)';
COMMENT ON COLUMN extracted.kofia_etf_codes.standard_cd IS 'KOFIA 펀드 표준코드 (raw.kofia_fund_fees, raw.kofia_funds)';
COMMENT ON COLUMN extracted.kofia_etf_codes.fund_nm IS 'KOFIA 펀드명';
COMMENT ON COLUMN extracted.kofia_etf_codes.method IS '맞춘 방법: etf_seibro (KRX 종목코드 → ISIN → SEIBro 공식명 = KOFIA ETF명), etf_exact/etf_subseq (KRX ETF명 ↔ KOFIA ETF명)';
