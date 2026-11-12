-- 003: KRX 정제본(HF krx-quotes parquet) 테이블 제거 — 정제 파이프라인이 새로 정해지면 그때 스키마를 다시 만든다.
--      원본 시세는 raw.krx_stock_daily / raw.krx_etf_daily 에 그대로 있음.
DROP TABLE IF EXISTS extracted.krx_stocks, extracted.krx_etfs, extracted.krx_stock_quotes, extracted.krx_etf_quotes;
DELETE FROM etl.loaded_files WHERE table_name IN ('extracted.krx_stocks', 'extracted.krx_etfs', 'extracted.krx_stock_quotes', 'extracted.krx_etf_quotes');
