-- PIPELINE HEALTH: one row per expected trading day with a status flag.
-- Tableau: calendar heatmap coloured by status + table of every non-OK day.
--   MISSING       trading day with no data loaded
--   LATE          scheduled load (not a backfill) finished after 06:00 the next morning
--   LOW_VOLUME    fewer tickers than 95% of the trailing 20-day average
--   HIGH_DUPES    over 5% of raw rows were dropped as duplicates or unparseable
--   CHECK_PRICES  a stock moved more than 50% in a day (often an unadjusted split)

WITH expected AS (
    SELECT session_date AS date
    FROM ref.trading_calendar
    WHERE session_date BETWEEN (SELECT MIN(partition_date) FROM meta.load_log) AND current_date - 1
),
loads AS (
    SELECT
        partition_date                                        AS date,
        arg_max(raw_rows, loaded_at)                          AS raw_rows,      -- latest load wins
        arg_max(clean_rows, loaded_at)                        AS clean_rows,
        MIN(loaded_at) FILTER (NOT is_backfill)               AS first_scheduled_load
    FROM meta.load_log
    WHERE asset = 'clean_prices'
    GROUP BY partition_date
),
price_jumps AS (
    SELECT date, COUNT(*) AS suspicious_moves
    FROM (
        SELECT date, close / LAG(close) OVER (PARTITION BY ticker ORDER BY date) - 1 AS day_return
        FROM clean.prices
    )
    WHERE ABS(day_return) > 0.5
    GROUP BY date
),
daily AS (
    SELECT
        e.date, l.raw_rows, l.clean_rows,
        l.raw_rows - l.clean_rows AS rows_dropped,
        l.first_scheduled_load,
        AVG(l.clean_rows) OVER (ORDER BY e.date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) AS trailing_avg_rows,
        COALESCE(j.suspicious_moves, 0) AS suspicious_moves
    FROM expected e
    LEFT JOIN loads l USING (date)
    LEFT JOIN price_jumps j USING (date)
)
SELECT *,
    CASE WHEN clean_rows IS NULL OR clean_rows = 0                         THEN 'MISSING'
         WHEN first_scheduled_load > date + INTERVAL 30 HOUR               THEN 'LATE'
         WHEN clean_rows < 0.95 * trailing_avg_rows                        THEN 'LOW_VOLUME'
         WHEN rows_dropped > 0.05 * raw_rows                               THEN 'HIGH_DUPES'
         WHEN suspicious_moves > 0                                         THEN 'CHECK_PRICES'
         ELSE 'OK' END AS status
FROM daily
ORDER BY date;
