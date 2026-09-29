-- Asset check: clean.prices must have exactly one row per ticker per day.
SELECT COUNT(*) - COUNT(DISTINCT (date, ticker)) AS dupes FROM clean.prices;
