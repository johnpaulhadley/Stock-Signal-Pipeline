-- Point-in-time trailing-twelve-month fundamentals, one row per company per fiscal quarter.
-- Input: SEC num.txt (every tag, every company, tens of millions of rows across quarters).
-- Output: ~500 companies x ~45 quarters, 8 columns. Everything downstream reads this instead.

CREATE OR REPLACE TABLE mart.fundamentals_ttm AS
WITH filings AS (
    -- One filing per company per reporting period. A 10-K/A restatement filed months later is
    -- dropped in favour of the original: the backtest may only see what investors saw at the time.
    SELECT
        s.adsh,
        u.ticker,
        s.period,
        STRPTIME(s.period, '%Y%m%d')::DATE  AS period_end,
        STRPTIME(s.filed,  '%Y%m%d')::DATE  AS filed_date,
        s.fy::INT                           AS fy,
        s.fp
    FROM read_parquet('{{raw}}/sec/*/sub.parquet', union_by_name = true) s
    JOIN ref.sp500 u ON u.cik = s.cik::INT
    WHERE s.form IN ('10-K', '10-Q', '10-K/A', '10-Q/A')
      AND s.fp IN ('Q1', 'Q2', 'Q3', 'FY')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY u.ticker, s.period ORDER BY s.filed, s.accepted) = 1
),
facts AS (
    -- Keep 7 tags, only values for the filing's own period (drops prior-year comparatives),
    -- consolidated totals only (no subsidiaries / business segments).
    SELECT f.ticker, f.period_end, f.filed_date, f.fy, f.fp,
           n.tag, n.qtrs::INT AS qtrs, n.value::DOUBLE AS value
    FROM read_parquet('{{raw}}/sec/*/num.parquet', union_by_name = true) n
    JOIN filings f ON f.adsh = n.adsh AND n.ddate = f.period
    WHERE n.tag IN ('Revenues', 'RevenueFromContractWithCustomerExcludingAssessedTax', 'SalesRevenueNet',
                    'NetIncomeLoss', 'EarningsPerShareDiluted', 'StockholdersEquity',
                    'WeightedAverageNumberOfDilutedSharesOutstanding')
      AND n.coreg IS NULL
      AND n.segments IS NULL
      AND n.value IS NOT NULL
),
per_filing AS (
    -- Pivot tags to columns. Income-statement items: 3-month value from 10-Qs, 12-month from 10-Ks.
    SELECT
        ticker, period_end, filed_date, fy, fp,
        COALESCE(MAX(value) FILTER (tag = 'Revenues' AND qtrs = q),
                 MAX(value) FILTER (tag = 'RevenueFromContractWithCustomerExcludingAssessedTax' AND qtrs = q),
                 MAX(value) FILTER (tag = 'SalesRevenueNet' AND qtrs = q))           AS revenue,
        MAX(value) FILTER (tag = 'NetIncomeLoss' AND qtrs = q)                      AS net_income,
        MAX(value) FILTER (tag = 'EarningsPerShareDiluted' AND qtrs = q)            AS eps,
        MAX(value) FILTER (tag = 'StockholdersEquity' AND qtrs = 0)                 AS equity,
        MAX(value) FILTER (tag = 'WeightedAverageNumberOfDilutedSharesOutstanding' AND qtrs = q) AS diluted_shares
    FROM (SELECT *, CASE WHEN fp = 'FY' THEN 4 ELSE 1 END AS q FROM facts)
    GROUP BY ALL
),
quarterly AS (
    -- 10-Ks report the full year, never Q4 on its own, so Q4 = FY - (Q1 + Q2 + Q3).
    -- Only derived when all three quarters exist; otherwise NULL rather than a wrong number.
    -- (EPS derived this way is approximate because share counts move during the year.)
    SELECT
        ticker, period_end, filed_date, equity, diluted_shares,
        CASE WHEN fp <> 'FY' THEN revenue
             WHEN q_count = 3 THEN revenue - q_revenue END       AS revenue_q,
        CASE WHEN fp <> 'FY' THEN net_income
             WHEN q_count = 3 THEN net_income - q_net_income END AS net_income_q,
        CASE WHEN fp <> 'FY' THEN eps
             WHEN q_count = 3 THEN eps - q_eps END               AS eps_q
    FROM (
        SELECT *,
            SUM(CASE WHEN fp <> 'FY' THEN revenue END)    OVER fy_w AS q_revenue,
            SUM(CASE WHEN fp <> 'FY' THEN net_income END) OVER fy_w AS q_net_income,
            SUM(CASE WHEN fp <> 'FY' THEN eps END)        OVER fy_w AS q_eps,
            COUNT(CASE WHEN fp <> 'FY' THEN revenue END)  OVER fy_w AS q_count
        FROM per_filing
        WINDOW fy_w AS (PARTITION BY ticker, fy)
    )
),
ttm AS (
    SELECT
        ticker, period_end, filed_date, equity, diluted_shares,
        SUM(revenue_q)    OVER w AS ttm_revenue,
        SUM(net_income_q) OVER w AS ttm_net_income,
        SUM(eps_q)        OVER w AS ttm_eps,
        COUNT(eps_q)      OVER w AS quarters_in_window,
        MIN(period_end)   OVER w AS window_start
    FROM quarterly
    WINDOW w AS (PARTITION BY ticker ORDER BY period_end ROWS 3 PRECEDING)
),
valid AS (
    -- A valid TTM needs 4 consecutive quarters (first and last ~9 months apart, no gaps).
    SELECT *,
        LAG(ttm_revenue, 4) OVER y AS ttm_revenue_prior,
        LAG(period_end, 4)  OVER y AS period_end_prior
    FROM ttm
    WHERE quarters_in_window = 4
      AND DATE_DIFF('day', window_start, period_end) BETWEEN 250 AND 300
    WINDOW y AS (PARTITION BY ticker ORDER BY period_end)
)
SELECT
    ticker,
    period_end,
    filed_date + 1 AS available_from,   -- filings can land after the close; usable the next day
    ttm_revenue,
    CASE WHEN DATE_DIFF('day', period_end_prior, period_end) BETWEEN 350 AND 380
         THEN ttm_revenue / NULLIF(ttm_revenue_prior, 0) - 1 END AS revenue_growth_yoy,
    ttm_eps,
    equity / NULLIF(diluted_shares, 0) AS book_value_per_share,
    ttm_net_income / NULLIF(equity, 0) AS roe
FROM valid
ORDER BY ticker, period_end;
