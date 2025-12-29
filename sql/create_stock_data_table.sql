CREATE TABLE IF NOT EXISTS stock_data (
    symbol          VARCHAR(10) NOT NULL,
    open            NUMERIC,
    high            NUMERIC,
    low             NUMERIC,
    close           NUMERIC,
    volume          BIGINT,
    dividends       NUMERIC,
    stock_splits    NUMERIC,
    ingestion_time  TIMESTAMP DEFAULT NOW()
);