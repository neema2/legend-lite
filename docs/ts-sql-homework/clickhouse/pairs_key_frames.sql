-- The pairs trade (spec-13) for ClickHouse, each calendar window one numeric frame: the key is month number * 31 +
-- day of month - 1, so (date - n months, date] is RANGE BETWEEN n*31 - 1 PRECEDING (exact, month ends included).
WITH
pair AS (
    SELECT k.date AS date, k.close AS ko, p.close AS pep,
           (toYear(k.date) * 12 + toMonth(k.date)) * 31 + toDayOfMonth(k.date) - 1 AS mk
    FROM file('prices_body.csv', 'CSV', 'symbol String, date Date, close Float64') AS k
    INNER JOIN file('prices_body.csv', 'CSV', 'symbol String, date Date, close Float64') AS p ON p.date = k.date
    WHERE k.symbol = 'KO' AND p.symbol = 'PEP'
),
fit AS (                                         -- hedge_ratio and residual: one 6-month regression of KO on PEP
    SELECT date, ko, pep, mk,
           covarSampStable(ko, pep) OVER w6 / varSampStable(pep) OVER w6 AS b,
           if(date >= addMonths(min(date) OVER (), 6) AND varSampStable(pep) OVER w6 != 0, b, NULL) AS hedge,
           if(date >= addMonths(min(date) OVER (), 6) AND varSampStable(pep) OVER w6 != 0,
              ko - (avg(ko) OVER w6 - b * avg(pep) OVER w6 + b * pep), NULL) AS spread
    FROM pair
    WINDOW w6 AS (ORDER BY mk RANGE BETWEEN 185 PRECEDING AND CURRENT ROW)
),
scored AS (                                      -- zscores(spread, 3mo)
    SELECT date, ko, pep, hedge, spread,
           if(date >= addMonths(minIf(date, spread IS NOT NULL) OVER (), 3) AND stddevSampStable(spread) OVER w3 != 0,
              (spread - avg(spread) OVER w3) / stddevSampStable(spread) OVER w3, NULL) AS z
    FROM fit
    WINDOW w3 AS (ORDER BY mk RANGE BETWEEN 92 PRECEDING AND CURRENT ROW)
),
traded AS (
    SELECT date, hedge, spread, z, position,
           lagInFrame(toNullable(position), 1) OVER d
             * ((ko - lagInFrame(toNullable(ko), 1) OVER d) - lagInFrame(hedge, 1) OVER d * (pep - lagInFrame(toNullable(pep), 1) OVER d)) AS daily_pnl
    FROM (SELECT *, multiIf(z > 2, -1, z < -2, 1, 0) AS position FROM scored)
    WINDOW d AS (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT date AS DATE, hedge, spread, z, position, daily_pnl,
       sum(if(date >= toDate('2023-01-01'), daily_pnl, NULL)) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS pnl_since_2023
FROM traded
WHERE date >= toDate('2023-01-01')
ORDER BY date
FORMAT CSVWithNames
