-- WHERE ARE THE OPPORTUNITIES? Share of each sector rated BUY vs SELL, month by month.
-- Tableau: heatmap, sector (rows) x month (columns), colour = net_buy_share.

WITH month_ends AS (
    SELECT MAX(date) AS date
    FROM mart.daily_signals
    GROUP BY DATE_TRUNC('month', date)
)
SELECT
    DATE_TRUNC('month', s.date)          AS month,
    s.sector,
    COUNT(*)                             AS stocks,
    AVG((s.signal = 'BUY')::INT)         AS buy_share,
    AVG((s.signal = 'SELL')::INT)        AS sell_share,
    AVG((s.signal = 'BUY')::INT) - AVG((s.signal = 'SELL')::INT) AS net_buy_share,
    AVG(s.composite_score)               AS avg_composite_score
FROM mart.daily_signals s
JOIN month_ends USING (date)
GROUP BY ALL
ORDER BY month, sector;
