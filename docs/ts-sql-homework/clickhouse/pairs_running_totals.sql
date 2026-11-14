-- The pairs trade (spec-13) for ClickHouse: no interval frames there, so each calendar window is the running
-- totals to the date less the running totals as of the window's start (an ASOF JOIN). Exact for everything built
-- from sums: count, mean, variance, covariance, the regression.
WITH
pair AS (
    SELECT k.date AS date, k.close AS ko, p.close AS pep
    FROM file('prices_body.csv', 'CSV', 'symbol String, date Date, close Float64') AS k
    INNER JOIN file('prices_body.csv', 'CSV', 'symbol String, date Date, close Float64') AS p ON p.date = k.date
    WHERE k.symbol = 'KO' AND p.symbol = 'PEP'
),
run6 AS (                    -- running totals to each date
    SELECT 1 AS g, date, ko, pep,
           count() OVER w AS n, sum(ko) OVER w AS sx, sum(pep) OVER w AS sy,
           sum(ko * pep) OVER w AS sxy, sum(pep * pep) OVER w AS syy,
           min(date) OVER () AS first_date
    FROM pair
    WINDOW w AS (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
fit AS (                     -- (date - 6 months, date]: the totals less those as of the window's start
    SELECT c.date AS date, c.ko AS ko, c.pep AS pep, c.first_date AS first_date,
           c.n - s.n AS n, c.sx - s.sx AS sx, c.sy - s.sy AS sy, c.sxy - s.sxy AS sxy, c.syy - s.syy AS syy
    FROM (SELECT *, addMonths(date, -6) AS lo FROM run6) AS c
    ASOF LEFT JOIN run6 AS s ON c.g = s.g AND c.lo >= s.date
),
spread AS (
    SELECT date, ko, pep,
           (sxy - sx * sy / n) / (syy - sy * sy / n) AS b,
           if(date >= addMonths(first_date, 6) AND syy - sy * sy / n != 0, b, NULL) AS hedge,
           if(date >= addMonths(first_date, 6) AND syy - sy * sy / n != 0, ko - (sx / n - b * sy / n + b * pep), NULL) AS spread
    FROM fit
),
run3 AS (
    SELECT 1 AS g, date, ko, pep, hedge, spread,
           count(spread) OVER w AS n, sum(ifNull(spread, 0)) OVER w AS s1, sum(ifNull(spread * spread, 0)) OVER w AS s2,
           minIf(date, spread IS NOT NULL) OVER () AS spread_first
    FROM spread
    WINDOW w AS (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
scored AS (                  -- zscores(spread, 3mo), the same way
    SELECT c.date AS date, c.ko AS ko, c.pep AS pep, c.hedge AS hedge, c.spread AS spread,
           c.n - s.n AS n, c.s1 - s.s1 AS s1, c.s2 - s.s2 AS s2, c.spread_first AS spread_first
    FROM (SELECT *, addMonths(date, -3) AS lo FROM run3) AS c
    ASOF LEFT JOIN run3 AS s ON c.g = s.g AND c.lo >= s.date
),
traded AS (
    SELECT date, ko, pep, hedge, spread,
           if(date >= addMonths(spread_first, 3) AND n > 1 AND s2 - s1 * s1 / n > 0,
              (spread - s1 / n) / sqrt((s2 - s1 * s1 / n) / (n - 1)), NULL) AS z,
           multiIf(z > 2, -1, z < -2, 1, 0) AS position
    FROM scored
),
pnl AS (
    SELECT date, hedge, spread, z, position,
           lagInFrame(toNullable(position), 1) OVER d
             * ((ko - lagInFrame(toNullable(ko), 1) OVER d) - lagInFrame(hedge, 1) OVER d * (pep - lagInFrame(toNullable(pep), 1) OVER d)) AS daily_pnl
    FROM traded
    WINDOW d AS (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT date AS DATE, hedge, spread, z, position, daily_pnl,
       sum(if(date >= toDate('2023-01-01'), daily_pnl, NULL)) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS pnl_since_2023
FROM pnl
WHERE date >= toDate('2023-01-01')
ORDER BY date
FORMAT CSVWithNames
