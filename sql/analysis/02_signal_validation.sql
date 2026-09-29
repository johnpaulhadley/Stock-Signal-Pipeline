-- DOES THE SCORE WORK? Average next-month return for each score quintile, per month.
-- If the model has signal, quintile 5 should beat quintile 1 in most months.
-- Tableau: bar chart of avg return by quintile + line of the Q5-minus-Q1 spread over time.

WITH month_ends AS (
    -- Rebalance monthly: the last trading day of each month. Reading ~12 rows per ticker per year
    -- instead of ~252 makes this 20x cheaper than working off the daily table directly.
    SELECT MAX(date) AS date
    FROM mart.daily_signals
    GROUP BY DATE_TRUNC('month', date)
),
monthly AS (
    SELECT s.date, s.ticker, s.quintile, s.close,
        LEAD(s.close) OVER w AS next_close,
        LEAD(s.date)  OVER w AS next_date
    FROM mart.daily_signals s
    JOIN month_ends USING (date)
    WINDOW w AS (PARTITION BY s.ticker ORDER BY s.date)
)
SELECT
    date AS rebalance_date,
    quintile,
    COUNT(*)                             AS stocks,
    AVG(next_close / close - 1)          AS avg_next_month_return,
    MEDIAN(next_close / close - 1)       AS median_next_month_return
FROM monthly
WHERE quintile IS NOT NULL
  AND DATE_DIFF('day', date, next_date) <= 40   -- ticker has a price the following month
GROUP BY ALL
ORDER BY rebalance_date, quintile;
