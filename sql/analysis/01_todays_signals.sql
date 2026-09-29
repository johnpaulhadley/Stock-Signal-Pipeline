-- TODAY'S CALLS: every BUY and SELL on the latest trading day, the factors behind each call,
-- and how long the stock has held that call (a brand-new BUY reads differently from a 6-month one).
-- Tableau: main dashboard table.

WITH latest AS (
    SELECT MAX(date) AS d FROM mart.daily_signals
),
runs AS (
    -- Gaps-and-islands: consecutive days with the same signal share the same run_id.
    SELECT date, ticker, signal,
        ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date)
      - ROW_NUMBER() OVER (PARTITION BY ticker, signal ORDER BY date) AS run_id
    FROM mart.daily_signals
    WHERE date >= (SELECT d FROM latest) - INTERVAL 2 YEAR
),
streaks AS (
    SELECT ticker, signal, run_id, MIN(date) AS signal_since, COUNT(*) AS trading_days_in_signal
    FROM runs
    GROUP BY ALL
)
SELECT
    s.date, s.ticker, u.company, s.sector, s.signal,
    st.signal_since, st.trading_days_in_signal,
    ROUND(s.composite_score, 3) AS composite_score,
    ROUND(s.value_score, 3)     AS value_score,
    ROUND(s.quality_score, 3)   AS quality_score,
    ROUND(s.momentum_score, 3)  AS momentum_score,
    s.close, s.ma_200, s.earnings_yield, s.roe, s.momentum_12_1
FROM mart.daily_signals s
JOIN latest ON s.date = latest.d
JOIN ref.sp500 u USING (ticker)
JOIN runs r ON r.ticker = s.ticker AND r.date = s.date
JOIN streaks st ON st.ticker = s.ticker AND st.signal = s.signal AND st.run_id = r.run_id
WHERE s.signal <> 'HOLD'
ORDER BY s.signal, s.composite_score DESC;
