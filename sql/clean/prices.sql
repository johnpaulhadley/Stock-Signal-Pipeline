-- Incremental load of clean.prices for one date range (a single day, or a whole backfill).
-- Delete-then-insert makes every rerun idempotent: running a partition twice never double-counts.

DELETE FROM clean.prices WHERE date BETWEEN '{{start_date}}' AND '{{end_date}}';

INSERT INTO clean.prices
WITH raw AS (
    -- `date` is the hive folder name (date=YYYY-MM-DD). Filtering on it means DuckDB
    -- only opens the folders in range instead of scanning ten years of files.
    SELECT *
    FROM read_parquet('{{raw}}/prices/*/*.parquet', hive_partitioning = true)
    WHERE date BETWEEN '{{start_date}}' AND '{{end_date}}'
),
parsed AS (
    SELECT
        COALESCE(TRY_STRPTIME(trade_date, '%Y-%m-%d'),
                 TRY_STRPTIME(trade_date, '%m/%d/%Y'))::DATE               AS date,    -- mixed formats
        UPPER(TRIM(ticker))                                                AS ticker,  -- " aapl " -> AAPL
        TRY_CAST(REGEXP_REPLACE(close, '[$,]', '', 'g') AS DOUBLE)         AS close,   -- "$1,234.50"
        TRY_CAST(volume AS BIGINT)                                         AS volume
    FROM raw
)
SELECT date, ticker, close, volume
FROM parsed
WHERE date IS NOT NULL AND close > 0
-- Duplicates only become exact after normalising the ticker, so dedupe here, not on raw.
-- Prefer the full-precision price over the rounded "$" version.
QUALIFY ROW_NUMBER() OVER (PARTITION BY date, ticker ORDER BY close - ROUND(close, 2) = 0, close) = 1
ORDER BY date, ticker;

-- Row counts in vs out, per day: feeds the pipeline-health dashboard.
INSERT INTO meta.load_log
SELECT 'clean_prices', r.date, r.raw_rows, COALESCE(c.clean_rows, 0), {{is_backfill}}, now()::TIMESTAMP
FROM (
    SELECT date, COUNT(*) AS raw_rows
    FROM read_parquet('{{raw}}/prices/*/*.parquet', hive_partitioning = true)
    WHERE date BETWEEN '{{start_date}}' AND '{{end_date}}'
    GROUP BY date
) r
LEFT JOIN (
    SELECT date, COUNT(*) AS clean_rows
    FROM clean.prices
    WHERE date BETWEEN '{{start_date}}' AND '{{end_date}}'
    GROUP BY date
) c USING (date);
