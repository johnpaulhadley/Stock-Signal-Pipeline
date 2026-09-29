-- THE $10M QUESTION: would following the signals have beaten simply buying the S&P 500 (SPY)?
-- Tableau: equity curve (strategy vs SPY) + KPI tiles for final value and annual return.
--
-- Trading rule, checked on the last trading day of each month:
--   enter a stock when it gets a BUY signal,
--   keep it until it falls into quintile 3 or lower (not merely out of quintile 5).
-- That "hold band" cuts trading costs because stocks hovering around the cut-off aren't bought and
-- sold every month. Whether a stock is held depends on whether it was held last month, so each
-- month's position is built from the previous one: a recursive CTE, not a plain window function.

WITH RECURSIVE month_ends AS (
    SELECT MAX(date) AS date
    FROM clean.prices
    WHERE ticker = 'SPY'
    GROUP BY DATE_TRUNC('month', date)
),
monthly AS (
    SELECT s.date, s.ticker, s.quintile, s.signal, s.close,
        ROW_NUMBER() OVER w                  AS month_no,
        LEAD(s.close) OVER w / s.close - 1   AS next_month_return
    FROM mart.daily_signals s
    JOIN month_ends USING (date)
    WINDOW w AS (PARTITION BY s.ticker ORDER BY s.date)
),
positions AS (
    SELECT date, ticker, month_no, signal = 'BUY' AS held
    FROM monthly
    WHERE month_no = 1
    UNION ALL
    SELECT n.date, n.ticker, n.month_no,
        CASE WHEN p.held THEN COALESCE(n.quintile, 0) >= 4   -- already own it: keep while in top 2 quintiles
             ELSE n.signal = 'BUY' END                        -- don't own it: buy only on a fresh BUY
    FROM positions p
    JOIN monthly n ON n.ticker = p.ticker AND n.month_no = p.month_no + 1
),
strategy AS (
    -- Equal weight across everything held that month.
    SELECT p.date, COUNT(*) AS holdings, AVG(m.next_month_return) AS strategy_return
    FROM positions p
    JOIN monthly m USING (date, ticker)
    WHERE p.held AND m.next_month_return IS NOT NULL
    GROUP BY p.date
),
benchmark AS (
    SELECT p.date, LEAD(p.close) OVER (ORDER BY p.date) / p.close - 1 AS spy_return
    FROM clean.prices p
    JOIN month_ends USING (date)
    WHERE p.ticker = 'SPY'
)
SELECT
    s.date                        AS rebalance_date,
    s.holdings,
    s.strategy_return,
    b.spy_return,
    -- Compounded growth of $10M; EXP(SUM(LN(1 + r))) is a running product of (1 + r).
    10000000 * EXP(SUM(LN(1 + s.strategy_return)) OVER (ORDER BY s.date)) AS strategy_value,
    10000000 * EXP(SUM(LN(1 + b.spy_return))      OVER (ORDER BY s.date)) AS spy_value
FROM strategy s
JOIN benchmark b USING (date)
WHERE b.spy_return IS NOT NULL
ORDER BY rebalance_date;
