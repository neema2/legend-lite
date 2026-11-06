-- THE #TS DOCUMENTS' EXAMPLES (docs/TS_V1_PROGRAM_2026_11_01.md A9): every query the spec
-- (docs/TIMESERIES_LANGUAGE_2026_10_29.md) and the guide for quants (docs/TIMESERIES_GUIDE_FOR_QUANTS_2026_10_30.md)
-- show, each under its "-- @example <id>" line. This file is the examples' one source: //core:ts_documents_test runs
-- each, and the documents' copies are written from it at their "<!-- example: <id> -->" markers by
-- bazel run //docs:update_ts_examples (no diff test: prose is never a test input, G17). Edit an example here.

-- @example spec-01
SELECT last(close['SPX'])                 AS wk_spx,
       last(close['QQQ'])                 AS wk_qqq,
       volatility(wk_spx, 3mo)            AS spx_vol,
       spx_vol - volatility(wk_qqq, 3mo)  AS spread
FROM   prices ON date BY symbol
SAMPLE BY 1w FILL(PREV)
FILTER date >= '2024-01-01'

-- @example spec-02
WITH quotes AS (SELECT instrument.ticker AS ticker, quoteTime, closePrice AS close FROM MarketQuote)
   SELECT avg(close, 20) AS ma20, volatility(close['AAPL'], 63) AS aapl_vol
   FROM   quotes ON quoteTime BY ticker

-- @example spec-03
WITH daily AS TS (SELECT close['SPX'] / close['QQQ'] AS r FROM prices ON date BY symbol)
   SELECT ohlc(first(daily.r), max(daily.r), min(daily.r), last(daily.r)) AS ratio
   FROM   daily ON date
   SAMPLE BY 1w

-- @example spec-04
SELECT last(close['SPX'])  AS wk_close,
         sum(volume['SPX'])  AS wk_volume,
         avg(wk_close, 4)    AS ma_4w,
         sum(wk_volume, 4)   AS volume_4w
  FROM   prices ON date BY symbol
  SAMPLE BY 1w

-- @example spec-05
WITH s AS TS (SELECT returns(close['SPX']) AS spx, returns(close['QQQ']) AS qqq
              FROM prices ON date BY symbol FILTER date > latest - 1y)
SELECT regr_slope(qqq, spx), regr_intercept(qqq, spx), regr_r2(qqq, spx), corr(qqq, spx) FROM s

-- @example spec-06
-- a moving-average crossover, the last two years shown
SELECT close['SPX']           AS spx,
       avg(close['SPX'], 50)  AS ma50,
       avg(close['SPX'], 200) AS ma200,
       ma50 - ma200           AS gap,
       ma50 > ma200           AS golden_cross
FROM   prices ON date BY symbol
FILTER date > latest - 2y

-- @example spec-07
-- AAPL's weekly bars, with the week's range and change
SELECT first(open['AAPL'])     AS wk_open,
       max(high['AAPL'])       AS wk_high,
       min(low['AAPL'])        AS wk_low,
       last(close['AAPL'])     AS wk_close,
       sum(volume['AAPL'])     AS wk_volume,
       wk_high - wk_low        AS wk_range,
       wk_close / wk_open - 1  AS wk_change
FROM   prices ON date BY symbol
SAMPLE BY 1w

-- @example spec-08
-- every instrument's 3-month volatility, the last year shown
SELECT volatility(close, 3mo) AS vol
FROM   prices ON date BY symbol
FILTER date > latest - 1y

-- @example spec-09
-- the yield curve: two spreads in basis points, a month's change, and an inversion flag
SELECT 100 * (yield['10Y'] - yield['2Y']) AS s2s10_bp,
       100 * (yield['30Y'] - yield['5Y']) AS s5s30_bp,
       diff(s2s10_bp, 1mo)                AS s2s10_1m_chg,
       s2s10_bp < 0                       AS inverted
FROM   rates ON date BY tenor
FILTER date > latest - 5y

-- @example spec-10
-- sector relative strength: each against SPX, its 3-month momentum, above its own 50-day trend?
SELECT close / close['SPX']    AS rel,
       returns(rel, 3mo)       AS rel_mom_3m,
       avg(rel, 50)            AS rel_ma50,
       rel > rel_ma50          AS outperforming
FROM   prices ON date BY symbol
FILTER symbol LIKE 'XL%' AND date > latest - 1y

-- @example spec-11
-- order flow from individual trades: each day's buying, selling, imbalance and VWAP, dark trades left out
SELECT sum(qty['AAPL', side = 'buy'])                      AS bought,
       sum(qty['AAPL', side = 'sell'])                     AS sold,
       (bought - sold) / (bought + sold)                   AS imbalance,
       sum(notional['AAPL']) / sum(qty['AAPL'])            AS day_vwap,
       avg(imbalance, 20)                                  AS imbalance_20d
FROM   (SELECT *, price * qty AS notional FROM trades WHERE venue <> 'DARK') ON trade_date BY ticker
SAMPLE BY 1d

-- @example spec-12
-- inflation: CPI year on year, and its last 3 months annualized
SELECT returns(value['CPI'], 1y)                    AS yoy,
       power(1 + returns(value['CPI'], 3mo), 4) - 1 AS three_month_annualized
FROM   macro ON date BY series

-- @example spec-13
-- a pairs trade, KO against PEP, with its P&L since 2023
SELECT hedge_ratio(close['KO'], close['PEP'], 6mo)        AS hedge,
       residual(close['KO'], close['PEP'], 6mo)           AS spread,
       zscores(spread, 3mo)                               AS z,
       CASE WHEN z >  2 THEN -1
            WHEN z < -2 THEN  1
            ELSE 0 END                                    AS position,
       lag(position, 1) * (diff(close['KO'], 1) - lag(hedge, 1) * diff(close['PEP'], 1)) AS daily_pnl,
       sum(daily_pnl, '2023-01-01')                       AS pnl_since_2023
FROM   prices ON date BY symbol
FILTER date >= '2023-01-01'

-- @example spec-14
-- risk-adjusted momentum: each month, the equities ranked by 12-1 momentum per unit of volatility; the top three
SELECT last(close)                               AS mo_close,
       lag(mo_close, 1) / lag(mo_close, 12) - 1  AS mom_12_1,
       volatility(mo_close, 12)                  AS vol_1y,
       mom_12_1 / vol_1y                         AS risk_adj,
       rank(risk_adj DESC)                       AS rnk
FROM   (prices WHERE asset_class = 'Equity') ON date BY symbol
SAMPLE BY 1mo
FILTER rnk <= 3 AND date > latest - 2y

-- @example spec-15
-- a 60/40 portfolio held in euros: weekly excess return over German 3-month rates, rolling Sharpe, worst drawdown
SELECT last(px.close['SPX'])                 AS spx,
       last(px.close['TLT'])                 AS tlt,
       last(fx.rate['EURUSD'])               AS eurusd,
       last(rf.yield['DE3M']) / 100          AS de3m,
       basket(spx, 0.6, tlt, 0.4, rebalance => 'monthly') / eurusd AS eur_6040,
       returns(eur_6040) - de3m / 52         AS excess,
       sharpe_ratio(prices(excess), 1y)      AS sharpe_1y,
       max_drawdown(eur_6040, 1y)            AS mdd_1y
FROM   prices px ON date BY symbol,
       fx     fx ON date BY pair,
       yields rf ON date BY tenor
SAMPLE BY 1w FILL(PREV)
FILTER date > latest - 3y

-- @example spec-16
-- a snapshot table: every equity's 1-year beta to SPX and its idiosyncratic volatility, as of the latest date
SELECT beta(close, close['SPX'], 1y)                                          AS beta,
       volatility(close, 3mo) * sqrt(1 - r_squared(close, close['SPX'], 3mo)) AS idio_vol
FROM   (prices WHERE asset_class = 'Equity' OR symbol = 'SPX') ON date BY symbol
FILTER symbol <> 'SPX' AND date = latest

-- @example guide-02
SELECT close['SPX']           AS spx,
       avg(close['SPX'], 50)  AS ma50,
       avg(close['SPX'], 200) AS ma200
FROM   prices ON date BY symbol
FILTER date > latest - 2y

-- @example guide-03
SELECT volatility(close['SPX'], 21)  AS vol_1m,
       volatility(close['SPX'], 63)  AS vol_3m,
       volatility(close['SPX'], 252) AS vol_1y
FROM   prices ON date BY symbol

-- @example guide-04
SELECT drawdown(close['NVDA'], all)     AS dd,
       max_drawdown(close['NVDA'], all) AS deepest
FROM   prices ON date BY symbol

-- @example guide-05
SELECT ohlc(first(open['SPX']), max(high['SPX']),
            min(low['SPX']),    last(close['SPX'])) AS spx
FROM   prices ON date BY symbol
SAMPLE BY 1w
FILTER date > latest - 1y

-- @example guide-06
SELECT rebase(close, latest - 3y) AS perf
FROM   prices ON date BY symbol
FILTER symbol IN ('SPX', 'XOM', 'JPM', 'EURUSD')
   AND date >= latest - 3y

-- @example guide-07
SELECT volatility(close, 63) AS vol_3m
FROM   (prices WHERE asset_class = 'Equity') ON date BY symbol
FILTER date > latest - 3y

-- @example guide-08
SELECT close['GLD']                         AS gold,
       bollinger_bands(close['GLD'], 20, 2) AS bands,
       avg(close['GLD'], 20)                AS ma20,
       gold > bands.upper                   AS stretched
FROM   prices ON date BY symbol
FILTER date > latest - 6mo

-- @example guide-09
SELECT basket(close['SPX'], 0.6, close['TLT'], 0.4)                         AS daily,
       basket(close['SPX'], 0.6, close['TLT'], 0.4, rebalance => 'monthly') AS monthly
FROM   prices ON date BY symbol

-- @example guide-10
SELECT px.close['SPX']                  AS spx,
       rt.yield['10Y'] - rt.yield['2Y'] AS s2s10,
       s2s10 < 0                        AS inverted
FROM   prices px ON date BY symbol,
       rates  rt ON date BY tenor

-- @example guide-11
SELECT px.close['SPX']                         AS spx,
       px.close['SPX'] / mc.value['CPI'] * 100 AS spx_real
FROM   prices px ON date BY symbol,
       macro  mc ON date BY series
FILL(PREV)
FILTER date > latest - 5y

-- @example guide-12
SELECT alpha(close['NVDA'], close['SPX'], 126)        AS alpha_6m,
       r_squared(close['NVDA'], close['SPX'], 126)    AS r2_6m,
       hedge_ratio(close['MSFT'], close['AAPL'], 126) AS hedge,
       residual(close['MSFT'], close['AAPL'], 126)    AS spread
FROM   prices ON date BY symbol

-- @example guide-13
SELECT hedge_ratio(close['MSFT'], close['AAPL'], 126) AS hedge,
       residual(close['MSFT'], close['AAPL'], 126)    AS spread,
       zscores(spread, 63)                            AS z,
       CASE WHEN z >  2 THEN -1
            WHEN z < -2 THEN  1
            ELSE 0 END                                AS position,
       lag(position, 1) * (diff(close['MSFT'], 1)
         - lag(hedge, 1) * diff(close['AAPL'], 1))    AS daily_pnl,
       sum(daily_pnl, '2023-01-01')                   AS pnl_since_2023
FROM   prices ON date BY symbol
FILTER date >= '2023-01-01'

-- @example guide-14
SELECT last(close)                              AS mo_close,
       lag(mo_close, 1) / lag(mo_close, 12) - 1 AS mom_12_1,
       volatility(mo_close, 12)                 AS vol_1y,
       mom_12_1 / vol_1y                        AS risk_adj,
       rank(risk_adj DESC)                      AS rnk
FROM   (prices WHERE asset_class = 'Equity') ON date BY symbol
SAMPLE BY 1mo
FILTER rnk <= 3 AND date > latest - 2y
