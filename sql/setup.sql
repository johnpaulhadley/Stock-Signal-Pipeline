-- Idempotent DDL, run before every pipeline step.
CREATE SCHEMA IF NOT EXISTS ref;
CREATE SCHEMA IF NOT EXISTS clean;
CREATE SCHEMA IF NOT EXISTS mart;
CREATE SCHEMA IF NOT EXISTS meta;

-- Only the two columns the signals need. Rows are inserted sorted by date, so DuckDB's
-- min/max zone maps skip untouched row groups on date filters (no index required).
CREATE TABLE IF NOT EXISTS clean.prices (
    date    DATE    NOT NULL,
    ticker  VARCHAR NOT NULL,
    close   DOUBLE  NOT NULL,
    volume  BIGINT
);

CREATE TABLE IF NOT EXISTS mart.daily_signals (
    date                DATE,
    ticker              VARCHAR,
    sector              VARCHAR,
    close               DOUBLE,
    ma_200              DOUBLE,
    momentum_12_1       DOUBLE,
    earnings_yield      DOUBLE,
    book_to_market      DOUBLE,
    roe                 DOUBLE,
    revenue_growth_yoy  DOUBLE,
    value_score         DOUBLE,
    quality_score       DOUBLE,
    momentum_score      DOUBLE,
    composite_score     DOUBLE,
    quintile            TINYINT,
    signal              VARCHAR
);

CREATE TABLE IF NOT EXISTS meta.load_log (
    asset           VARCHAR,
    partition_date  DATE,
    raw_rows        BIGINT,
    clean_rows      BIGINT,
    is_backfill     BOOLEAN,
    loaded_at       TIMESTAMP
);
