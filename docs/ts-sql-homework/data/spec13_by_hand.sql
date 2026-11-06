-- The pairs trade (spec-13) by hand, DuckDB: one scan of prices, five window passes, no self-join.
-- A calendar window (date - n months, date] is a RANGE frame of 'n months -1 day' (measured: the same rows as
-- the library's join on every date). The ramps (none until a full window has passed) are kept, though the
-- FILTER from 2023 never sees them on this data.
WITH pair AS (                       -- KO and PEP met on their dates
    SELECT k.date, k.close AS ko, p.close AS pep
    FROM prices k JOIN prices p ON p.date = k.date AND p.symbol = 'PEP'
    WHERE k.symbol = 'KO'
),
fit AS (                             -- hedge_ratio and residual: one 6-month regression of KO on PEP
    SELECT date, ko, pep,
           CASE WHEN date >= min(date) OVER () + INTERVAL 6 MONTH
                THEN regr_slope(ko, pep) OVER w6 END                                          AS hedge,
           CASE WHEN date >= min(date) OVER () + INTERVAL 6 MONTH
                THEN ko - (regr_intercept(ko, pep) OVER w6 + regr_slope(ko, pep) OVER w6 * pep) END AS spread
    FROM pair
    WINDOW w6 AS (ORDER BY date RANGE BETWEEN INTERVAL '6 months -1 day' PRECEDING AND CURRENT ROW)
),
scored AS (                          -- zscores(spread, 3mo)
    SELECT *,
           CASE WHEN date >= min(date) FILTER (WHERE spread IS NOT NULL) OVER () + INTERVAL 3 MONTH
                THEN (spread - avg(spread) OVER w3) / NULLIF(stddev_samp(spread) OVER w3, 0) END AS z
    FROM fit
    WINDOW w3 AS (ORDER BY date RANGE BETWEEN INTERVAL '3 months -1 day' PRECEDING AND CURRENT ROW)
),
traded AS (                          -- position, and the day's P&L from yesterday's position and hedge
    SELECT date, hedge, spread, z, position,
           lag(position) OVER d * ((ko - lag(ko) OVER d) - lag(hedge) OVER d * (pep - lag(pep) OVER d)) AS daily_pnl
    FROM (SELECT *, CASE WHEN z > 2 THEN -1 WHEN z < -2 THEN 1 ELSE 0 END AS position FROM scored)
    WINDOW d AS (ORDER BY date)
)
SELECT date, hedge, spread, z, position, daily_pnl,
       sum(daily_pnl) FILTER (WHERE date >= DATE '2023-01-01') OVER (ORDER BY date) AS pnl_since_2023
FROM traded
QUALIFY date >= DATE '2023-01-01'
ORDER BY date
