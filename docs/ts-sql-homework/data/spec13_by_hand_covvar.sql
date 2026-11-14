-- The pairs trade (spec-13), KO against PEP, written by hand for DuckDB: every calendar window one window frame,
-- the regression fitted once for both the hedge and the spread.
WITH pair AS (                                   -- the two lines, met on their dates
    SELECT k.date, k.close AS ko, p.close AS pep
    FROM prices AS k
    JOIN prices AS p ON p.date = k.date AND p.symbol = 'PEP'
    WHERE k.symbol = 'KO'
),
fit AS (                                         -- KO on PEP over (date - 6 months, date]
    SELECT date, ko, pep,
           covar_samp(ko, pep) OVER w6 / NULLIF(var_samp(pep) OVER w6, 0) AS b,
           avg(ko)  OVER w6 AS mean_ko,
           avg(pep) OVER w6 AS mean_pep,
           min(date) OVER () AS first_date
    FROM pair
    WINDOW w6 AS (ORDER BY date RANGE BETWEEN INTERVAL '6 months -1 day' PRECEDING AND CURRENT ROW)
),
spread AS (                                      -- none until six months of pairs (the ramp)
    SELECT date, ko, pep,
           CASE WHEN date >= first_date + INTERVAL 6 MONTH THEN b END                                   AS hedge,
           CASE WHEN date >= first_date + INTERVAL 6 MONTH THEN ko - (mean_ko - b * mean_pep + b * pep) END AS spread
    FROM fit
),
zscore AS (                                      -- the spread against (date - 3 months, date]
    SELECT *,
           (spread - avg(spread) OVER w3) / NULLIF(stddev_samp(spread) OVER w3, 0) AS z_raw,
           min(date) FILTER (WHERE spread IS NOT NULL) OVER () AS spread_first
    FROM spread
    WINDOW w3 AS (ORDER BY date RANGE BETWEEN INTERVAL '3 months -1 day' PRECEDING AND CURRENT ROW)
),
trade AS (
    SELECT date, hedge, spread,
           CASE WHEN date >= spread_first + INTERVAL 3 MONTH THEN z_raw END AS z,
           ko - lag(ko) OVER d AS d_ko,
           pep - lag(pep) OVER d AS d_pep
    FROM zscore
    WINDOW d AS (ORDER BY date)
),
positioned AS (
    SELECT *, CASE WHEN z > 2 THEN -1 WHEN z < -2 THEN 1 ELSE 0 END AS position
    FROM trade
),
pnl AS (
    SELECT *, lag(position) OVER d * (d_ko - lag(hedge) OVER d * d_pep) AS daily_pnl
    FROM positioned
    WINDOW d AS (ORDER BY date)
)
SELECT date, hedge, spread, z, position, daily_pnl,
       sum(CASE WHEN date >= DATE '2023-01-01' THEN daily_pnl END) OVER (ORDER BY date ROWS UNBOUNDED PRECEDING) AS pnl_since_2023
FROM pnl
QUALIFY date >= DATE '2023-01-01'
ORDER BY date
