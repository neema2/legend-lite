-- @example q01
SELECT CLOSE AS c, mean(CLOSE, 1mo) AS m, sum(CLOSE, 1mo) AS s, min(CLOSE, 1mo) AS lo, max(CLOSE, 1mo) AS hi,
       std(CLOSE, 3mo) AS sd, var(CLOSE, 3mo) AS va, range(CLOSE, 1mo) AS rg
FROM   prices ON date BY symbol

-- @example q02
SELECT zscores(close, 3mo) AS z, drawdown(close, 3mo) AS dd, max_drawdown(close, 3mo) AS md, lag(close, 1mo) AS l,
       diff(close, 1mo) AS d, diff(close, 1) AS d1, diff(close, 3) AS d3
FROM   prices ON date BY symbol

-- @example q03
SELECT mean(close, 5) AS m5, zscores(m5, 1mo) AS z, diff(m5, 2) AS d, sum(m5, '2023-01-01') AS s, lag(m5, 1mo) AS l,
       max_drawdown(m5, 3mo) AS md, mean(m5, 2mo) AS mm, diff(m5, 1mo) AS dm
FROM   prices ON date BY symbol

-- @example q04
SELECT zscores(close['KO'], 3mo) AS z, diff(close['KO'], 1) AS d, sum(close['KO'], '2023-06-01') AS s,
       mean(close['PEP'], 1mo) AS m
FROM   prices ON date BY symbol

-- @example q05
SELECT last(close) AS c, mean(c, 3mo) AS m, zscores(c, 6mo) AS z, diff(c, 1) AS d, sum(c, '2023-01-01') AS s
FROM   prices ON date BY symbol
SAMPLE BY 1w

-- @example q06
SELECT hedge_ratio(close['KO'], close['PEP'], 60) AS h, residual(close['KO'], close['PEP'], 60) AS r,
       cov(close['KO'], close['PEP'], 60) AS c, hedge_ratio(close['KO'], close['PEP'], all) AS h0,
       residual(close['KO'], close['PEP'], 3mo) AS r3, cov(close['KO'], close['PEP'], 1y) AS c1
FROM   prices ON date BY symbol

-- @example q07
SELECT hedge_ratio(close, close['SPX'], 60) AS h, residual(close, close['SPX'], 3mo) AS r, cov(close, close['SPX'], all) AS c
FROM   prices ON date BY symbol

-- @example q08
SELECT last(close['KO']) AS a, last(close['PEP']) AS b, hedge_ratio(a, b, 12) AS h, residual(a, b, 6mo) AS r
FROM   prices ON date BY symbol
SAMPLE BY 1w

-- @example q09
SELECT close['KO'] - 1.2 * close['PEP'] AS sp, hedge_ratio(close['KO'], close['PEP'], 60) AS h,
       close['KO'] / close['PEP'] AS ratio, residual(close['KO'], close['PEP'], 60) - sp AS gap
FROM   prices ON date BY symbol

-- @example q10
SELECT hedge_ratio(mean(close['KO'], 5), close['PEP'], 30) AS h,
       residual(mean(close['KO'], 5), mean(close['PEP'], 10), 20) AS r, zscores(r, 2mo) AS z
FROM   prices ON date BY symbol

-- @example q11
SELECT hedge_ratio(px.close['SPX'], fx.rate['EURUSD'], 60) AS h, residual(px.close['SPX'], fx.rate['EURUSD'], 2mo) AS r
FROM   prices px ON date BY symbol,
       fx     fx ON date BY pair

-- @example q12
SELECT hedge_ratio(close['KO'], close['PEP'], 6mo) AS hedge, residual(close['KO'], close['PEP'], 6mo) AS spread,
       zscores(spread, 3mo) AS z, CASE WHEN z > 2 THEN -1 WHEN z < -2 THEN 1 ELSE 0 END AS position,
       lag(position, 1) * (diff(close['KO'], 1) - lag(hedge, 1) * diff(close['PEP'], 1)) AS daily_pnl,
       sum(daily_pnl, '2023-01-01') AS pnl_since_2023
FROM   prices ON date BY symbol
FILTER date >= '2023-01-01'

-- @example q13
SELECT close AS c, diff(close, 1) AS d, zscores(d, 1mo) AS z, max(d, 2mo) AS mx
FROM   prices ON date
