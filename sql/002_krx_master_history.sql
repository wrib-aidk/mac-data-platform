-- 002: KRX 정제본 stocks / etfs 는 종목별 이름 변경 이력 (같은 ticker 가 updated_at 마다 1행)
--      예) 000040: S&T모터스(2010-01-04) → KR모터스(2014-04-18) → 다이나맥(2026-11-09)
ALTER TABLE extracted.krx_stocks DROP CONSTRAINT krx_stocks_pkey, ADD PRIMARY KEY (ticker, updated_at);
ALTER TABLE extracted.krx_etfs DROP CONSTRAINT krx_etfs_pkey, ADD PRIMARY KEY (ticker, updated_at);
COMMENT ON COLUMN extracted.krx_stocks.updated_at IS '이 이름·시장 정보가 적용되기 시작한 날짜';
COMMENT ON COLUMN extracted.krx_etfs.updated_at IS '이 이름·기초지수 정보가 적용되기 시작한 날짜';
