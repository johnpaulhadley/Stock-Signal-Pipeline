-- Daily factor scores and BUY / SELL / HOLD calls for one date range.

DELETE FROM mart.daily_signals WHERE date BETWEEN '{{start_date}}' AND '{{end_date}}';

INSERT INTO mart.daily_signals
WITH px AS (
    -- The 200-day average and 12-month momentum need ~13 months of history before the range,
    -- but only the rows in range are written.
    SELECT date, ticker, close
    FROM clean.prices
    WHERE date BETWEEN DATE '{{start_date}}' - INTERVAL 400 DAY AND '{{end_date}}'
),
technicals AS (
    SELECT
        date, ticker, close,
        CASE WHEN COUNT(*) OVER (PARTITION BY ticker ORDER BY date ROWS 199 PRECEDING) = 200
             THEN AVG(close) OVER (PARTITION BY ticker ORDER BY date ROWS 199 PRECEDING) END AS ma_200,
        -- 12-1 momentum: return from 12 months ago to 1 month ago. Skipping the latest month
        -- avoids the short-term reversal effect that makes last month's winners dip.
        LAG(close, 21) OVER t / LAG(close, 252) OVER t - 1 AS momentum_12_1
    FROM px
    WINDOW t AS (PARTITION BY ticker ORDER BY date)
),
with_fundamentals AS (
    -- ASOF join: for each day, the most recent filing that was already public on that day.
    -- This is what prevents look-ahead bias in the backtest.
    SELECT
        t.date, t.ticker, u.sector, t.close, t.ma_200, t.momentum_12_1,
        f.ttm_eps / t.close              AS earnings_yield,   -- E/P, not P/E: stays meaningful when earnings < 0
        f.book_value_per_share / t.close AS book_to_market,
        f.roe,
        f.revenue_growth_yoy
    FROM technicals t
    JOIN ref.sp500 u USING (ticker)                        -- also drops the SPY benchmark row
    ASOF LEFT JOIN mart.fundamentals_ttm f
        ON f.ticker = t.ticker AND t.date >= f.available_from
    WHERE t.date BETWEEN '{{start_date}}' AND '{{end_date}}'
),
ranked AS (
    -- Percentile ranks within sector and day: banks are compared with banks, not with software.
    -- `x IS NULL` in the partition keeps missing values out of everyone else's ranking.
    SELECT *,
        CASE WHEN earnings_yield IS NOT NULL THEN PERCENT_RANK() OVER (PARTITION BY date, sector, earnings_yield IS NULL ORDER BY earnings_yield) END AS r_ey,
        CASE WHEN book_to_market IS NOT NULL THEN PERCENT_RANK() OVER (PARTITION BY date, sector, book_to_market IS NULL ORDER BY book_to_market) END AS r_btm,
        CASE WHEN roe IS NOT NULL THEN PERCENT_RANK() OVER (PARTITION BY date, sector, roe IS NULL ORDER BY roe) END AS r_roe,
        CASE WHEN revenue_growth_yoy IS NOT NULL THEN PERCENT_RANK() OVER (PARTITION BY date, sector, revenue_growth_yoy IS NULL ORDER BY revenue_growth_yoy) END AS r_growth,
        CASE WHEN momentum_12_1 IS NOT NULL THEN PERCENT_RANK() OVER (PARTITION BY date, sector, momentum_12_1 IS NULL ORDER BY momentum_12_1) END AS r_mom
    FROM with_fundamentals
),
scored AS (
    SELECT *,
        list_avg([r_ey, r_btm])    AS value_score,      -- list_avg skips NULLs
        list_avg([r_roe, r_growth]) AS quality_score,
        r_mom                       AS momentum_score
    FROM ranked
),
composite AS (
    SELECT *,
        CASE WHEN (value_score IS NOT NULL)::INT + (quality_score IS NOT NULL)::INT
                  + (momentum_score IS NOT NULL)::INT >= 2                   -- need 2 of 3 factors
             THEN list_avg([value_score, quality_score, momentum_score]) END AS composite_score
    FROM scored
),
bucketed AS (
    SELECT *,
        CASE WHEN composite_score IS NOT NULL
             THEN NTILE(5) OVER (PARTITION BY date, composite_score IS NULL ORDER BY composite_score) END AS quintile
    FROM composite
)
SELECT
    date, ticker, sector, close, ma_200, momentum_12_1, earnings_yield, book_to_market, roe, revenue_growth_yoy,
    value_score, quality_score, momentum_score, composite_score, quintile,
    CASE WHEN quintile = 5 AND close > ma_200 THEN 'BUY'    -- best-scored AND in an uptrend
         WHEN quintile = 1                   THEN 'SELL'
         ELSE 'HOLD' END AS signal
FROM bucketed
ORDER BY date, ticker;
