// THE TIMESERIES GALLERY (the user, 2026-10-25: "can we make this a real saved page with multiple tabs showing all of
// these similar to how we have some chart examples in the Open dialog?"): every function of the timeseries tile's
// catalog (timeseries-expr.ts TS_FUNCTIONS) drawn at least once, a sheet per family of it, titled by what it does --
// then a sheet of the ways a tile can be read (a line per symbol, a week or a month at a time, a log scale, every line
// from one start, its styles, two grids in one query) and the two grids they read. Over the same two samples as the
// markets example (markets.ts), each sheet fitting its window; test/examples.test.ts holds it to every function, each
// tile drawable, and test/timeseries-queries.test.ts runs every series of it on its samples.

import type { Band, Node } from '../layout/bands.ts';
import { writePageOf, type PageDocument, type PageSheet, type PageView } from '../page-document.ts';
import { CASH, CASH_GRID, cashCube, MACRO, MACRO_GRID, macroCube, MARKET_GRIDS, MARKS, marketCubes, PRICES, RATES, s, tile, type PageOver } from './markets.ts';

const ON_PRICES = [PRICES];
const PRICES_AND_MACRO = [PRICES, MACRO];

/** A sheet of the gallery: its name, and its tiles row by row (each row sharing the width, the rows the height). */
interface GallerySheet {
  readonly name: string;
  readonly rows: readonly (readonly PageView[])[];
}

const RETURNS: GallerySheet = {
  name: 'Returns',
  rows: [[
    tile('returns', 'Returns: the S&P 500\'s daily return, simple and log', {
      sources: ON_PRICES,
      series: [
        s('A', "returns(close['SPX'], 1, 'simple')", { name: 'Simple' }),
        s('B', "returns(close['SPX'], 1, 'log')", { name: 'Log', style: { dash: 'dashed', width: 1 } }),
      ],
      range: { last: '1y' },
    }),
    tile('diff-lag', 'Difference and lag: gold\'s close a week before, and its change over the week', {
      sources: ON_PRICES,
      series: [
        s('A', "close['GLD']", { name: 'Gold' }),
        s('B', "lag(close['GLD'], 5, 'truncate')", { name: 'A week before', style: { dash: 'dashed', width: 1 } }),
        s('C', "diff(close['GLD'], 5)", { name: 'Change over the week', pane: 1, style: { kind: 'bars' } }),
      ],
      range: { last: '6m' },
      panes: [{ height: 2 }, { height: 1 }],
    }),
  ], [
    tile('compounded', 'Prices from returns: Nvidia\'s daily returns compounded back into a level', {
      sources: ON_PRICES,
      series: [
        s('A', "rebase(close['NVDA'], base => 100)", { name: 'Close, rebased' }),
        s('B', "prices(returns(close['NVDA'], 1, 'simple'), initial => 100, type => 'simple')", { name: 'Its returns compounded from 100', style: { dash: 'dashed' } }),
        s('C', "returns(close['NVDA'], 1, 'simple')", { name: 'Daily return', pane: 1, style: { kind: 'dots' } }),
      ],
      // from the start: the first date shown is where the returns are compounded from, so the two lines are one
      panes: [{ height: 2 }, { height: 1 }],
    }),
    tile('annualize', 'Annualize: the S&P 500\'s 21-day standard deviation of returns, a day\'s and a year\'s', {
      sources: ON_PRICES,
      series: [
        s('A', "annualize(std(returns(close['SPX'], 1, 'simple'), 21))", { name: 'Annualized' }),
        s('B', "std(returns(close['SPX'], 1, 'simple'), 21)", { name: 'Daily', pane: 1, style: { kind: 'area' } }),
      ],
      panes: [{ height: 1 }, { height: 1 }],
      annotations: [...MARKS],
    }),
  ], [
    tile('excess', 'Excess returns: the S&P 500 over 5% cash accrued day by day, over the bond fund, and over dollar cash', {
      sources: [PRICES, CASH],
      series: [
        s('A', "close['SPX']", { name: 'S&P 500', style: { width: 1, dash: 'dashed' } }),
        s('B', "excess_returns(close['SPX'], 0.05)", { name: 'Over 5% cash' }),
        s('C', "excess_returns(close['SPX'], close['TLT'])", { name: 'Over the bond fund' }),
        // the risk-free data model (item 13b): the dollar's cash index made from its overnight rate
        s('D', "excess_returns(close['SPX'], cash_index(cash.rate['USD'], day_count => 'actual_360'))", { name: 'Over dollar cash' }),
      ],
    }),
    tile('compounded-to-date', 'Compounded to date: Microsoft\'s daily returns, their product less 1', {
      sources: ON_PRICES,
      series: [
        s('A', "geometrically_aggregate(returns(close['MSFT'], 1, 'simple'))", { name: 'Compounded return', style: { kind: 'area' } }),
        s('B', "returns(close['MSFT'], 1, 'simple')", { name: 'Daily return', pane: 1, style: { kind: 'dots' } }),
      ],
      panes: [{ height: 2 }, { height: 1 }],
    }),
    tile('total-return', 'Total return: the S&P 500 with a 1.5% dividend yield reinvested, beside its price', {
      sources: ON_PRICES,
      series: [
        s('A', "close['SPX']", { name: 'Price', style: { width: 1, dash: 'dashed' } }),
        s('B', "total_return(close['SPX'], 0.015)", { name: 'With a 1.5% yield' }),
      ],
    }),
  ]],
};

const LEVEL: GallerySheet = {
  name: 'Level',
  rows: [[
    tile('rebase', 'Rebase to 100: four markets from one start, the first date shown', {
      sources: ON_PRICES,
      series: [
        s('A', "rebase(close['SPX'], base => 100)", { name: 'S&P 500' }), s('B', "rebase(close['XOM'], base => 100)", { name: 'Exxon' }),
        s('C', "rebase(close['JPM'], base => 100)", { name: 'JPMorgan' }), s('D', "rebase(close['EURUSD'], base => 100)", { name: 'Euro' }),
      ],
      range: { last: '3y' },
      annotations: [{ kind: 'level', value: 100 }],
    }),
    tile('index', 'Index: the bank and the oil major, each 1 at the first date shown', {
      sources: ON_PRICES,
      series: [s('A', "index(close['JPM'], initial => 1)", { name: 'JPMorgan' }), s('B', "index(close['XOM'], initial => 1)", { name: 'Exxon' })],
      range: { last: '1y' },
      annotations: [{ kind: 'level', value: 1 }],
    }),
  ], [
    tile('change', 'Change from start: gold and long bonds this year, each in its own units', {
      sources: ON_PRICES,
      series: [
        s('A', "change(close['GLD'])", { name: 'Gold', style: { kind: 'area' } }),
        s('B', "change(close['TLT'])", { name: 'Long bonds', style: { kind: 'area' } }),
      ],
      range: { last: 'ytd' },
      annotations: [{ kind: 'level', value: 0 }],
    }),
    tile('percent-change', '% change from start: Apple, Microsoft and Nvidia over the last year', {
      sources: ON_PRICES,
      series: [
        s('A', "percent_change(close['AAPL'])", { name: 'Apple' }), s('B', "percent_change(close['MSFT'])", { name: 'Microsoft' }),
        s('C', "percent_change(close['NVDA'])", { name: 'Nvidia' }),
      ],
      range: { last: '1y' },
      annotations: [{ kind: 'level', value: 0 }],
    }),
  ], [
    tile('decompose', 'A season taken out: retail sales, their trend and their seasonally adjusted line, a month at a time', {
      sources: [MACRO],
      grain: 'month',
      series: [
        s('A', "value['RETAIL']", { name: 'Retail sales', style: { width: 1 } }),
        s('B', "trend(value['RETAIL'], method => 'multiplicative', freq => 'year')", { name: 'Trend', style: { dash: 'dashed' } }),
        s('C', "seasonally_adjusted(value['RETAIL'], method => 'multiplicative', freq => 'year')", { name: 'Seasonally adjusted' }),
      ],
    }),
  ]],
};

const ROLLING: GallerySheet = {
  name: 'Rolling statistics',
  rows: [[
    tile('mean', 'Moving average: the S&P 500 with its 50- and 200-day', {
      sources: ON_PRICES,
      series: [
        s('A', "close['SPX']", { name: 'S&P 500' }),
        s('B', "mean(close['SPX'], 50)", { name: '50-day', style: { width: 1 } }),
        s('C', "mean(close['SPX'], 200)", { name: '200-day', style: { width: 1, dash: 'dashed' } }),
      ],
      range: { last: '2y' },
    }),
    tile('min-max-range', 'Rolling minimum, maximum and range: Microsoft\'s 52-week channel', {
      sources: ON_PRICES,
      series: [
        s('A', "close['MSFT']", { name: 'Microsoft' }),
        s('B', "max(close['MSFT'], 252)", { name: '52-week high', style: { kind: 'step', width: 1 } }),
        s('C', "min(close['MSFT'], 252)", { name: '52-week low', style: { kind: 'step', width: 1 } }),
        s('D', "range(close['MSFT'], w => 252)", { name: 'Its range', pane: 1, style: { kind: 'area' } }),
      ],
      panes: [{ height: 2 }, { height: 1 }],
    }),
    tile('sum', 'Rolling sum: Apple\'s volume over a month, and its running total', {
      sources: ON_PRICES,
      series: [
        s('A', "sum(volume['AAPL'], 21)", { name: '21-day volume', style: { kind: 'bars' } }),
        s('B', "sum(volume['AAPL'], all)", { name: 'Since the start', pane: 1, style: { kind: 'area' } }),
      ],
      panes: [{ height: 1 }, { height: 1 }],
    }),
  ], [
    tile('std-var', 'Standard deviation and variance: gold\'s over the last quarter', {
      sources: ON_PRICES,
      series: [
        s('A', "std(close['GLD'], 63)", { name: '63-day standard deviation' }),
        s('B', "var(close['GLD'], 63)", { name: '63-day variance', pane: 1, style: { kind: 'area' } }),
      ],
      panes: [{ height: 1 }, { height: 1 }],
    }),
    tile('zscores', 'Z-score: Nvidia, standard deviations from its 63-day mean', {
      sources: ON_PRICES,
      series: [s('A', "zscores(close['NVDA'], w => 63)", { name: 'Nvidia', style: { kind: 'area' } })],
      range: { last: '2y' },
      annotations: [
        { kind: 'level', value: 2, label: '+2σ' }, { kind: 'level', value: 0 }, { kind: 'level', value: -2, label: '−2σ' },
      ],
    }),
    tile('winsorize', 'Winsorize: the euro\'s daily returns held within 1.5 standard deviations', {
      sources: ON_PRICES,
      series: [
        s('A', "winsorize(returns(close['EURUSD'], 1, 'simple'), limit => 1.5)", { name: 'Winsorized at 1.5σ', style: { kind: 'area', width: 1 } }),
        s('B', "returns(close['EURUSD'], 1, 'simple')", { name: 'Daily return', style: { kind: 'dots' } }),
      ],
      range: { last: '3m' },
    }),
    // across a date's instruments (item 13c, the user's ruling of 2026-11-08): the factor researcher's normalization --
    // each stock's 3-month return as a z-score among that day's stocks, no date reading another
    tile('cross-section', 'Across the stocks: each one\'s 3-month return as a z-score among the day\'s equities', {
      sources: ON_PRICES,
      series: [s('A', "zscores(returns(close[asset_class = 'Equity'], 63, 'simple'), across)", { name: 'Momentum z-score', split: 'symbol' })],
      range: { last: '1y' },
      annotations: [{ kind: 'level', value: 0 }],
    }),
  ], [
    tile('exponential', 'Exponential and Wilder\'s averages: the S&P 500, its EMA, its smoothed average, its exponential deviation', {
      sources: ON_PRICES,
      series: [
        s('A', "close['SPX']", { name: 'S&P 500' }),
        s('B', "exponential_moving_average(close['SPX'], beta => 0.9)", { name: 'EMA (beta 0.9)', style: { width: 1 } }),
        s('C', "smoothed_moving_average(close['SPX'], w => 20)", { name: 'Wilder 20', style: { width: 1, dash: 'dashed' } }),
        s('D', "exponential_std(close['SPX'], beta => 0.95)", { name: 'Exponential deviation', pane: 1, style: { kind: 'area' } }),
      ],
      range: { last: '1y' },
      panes: [{ height: 2 }, { height: 1 }],
    }),
    tile('percentiles', 'Percentiles: gold within its year -- its median, its 90th percentile, and where it ranks', {
      sources: ON_PRICES,
      series: [
        s('A', "close['GLD']", { name: 'Gold' }),
        s('B', "median(close['GLD'], w => 1y)", { name: '1-year median', style: { width: 1 } }),
        s('C', "percentile(close['GLD'], n => 90, w => 1y)", { name: '1-year 90th percentile', style: { width: 1, dash: 'dashed' } }),
        s('D', "percentiles(close['GLD'], w => 1y)", { name: 'Its rank in the year', pane: 1, style: { kind: 'area' } }),
      ],
      panes: [{ height: 2 }, { height: 1 }],
      annotations: [{ kind: 'level', value: 90, pane: 1 }, { kind: 'level', value: 10, pane: 1 }],
    }),
  ], [
    tile('product-streaks', 'Rolling product and streaks: the euro\'s 5-day compounded change, and its days in a row up and down', {
      sources: ON_PRICES,
      series: [
        s('A', "product(1 + returns(close['EURUSD'], 1, 'simple'), w => 5) - 1", { name: '5-day compounded change' }),
        s('B', "consecutive(returns(close['EURUSD'], 1, 'simple'), direction => 'positive')", { name: 'Days up in a row', pane: 1, style: { kind: 'bars' } }),
        s('C', "0 - consecutive(returns(close['EURUSD'], 1, 'simple'), direction => 'negative')", { name: 'Days down in a row', pane: 1, style: { kind: 'bars' } }),
      ],
      range: { last: '6m' },
      panes: [{ height: 1 }, { height: 1 }],
    }),
    // gs-quant's Window(w, r) (item 13): a window's ramp apart from its size -- a figure from the line's (r + 1)-th point,
    // over the points it has before the window is full; the whole line drawn, so the oracle reads every ramp against
    // gs-quant's (the review of 2026-11-08: no gallery series carried an r)
    tile('ramp', 'A window\'s ramp apart from its size: Nvidia\'s 63-day z-score from its first day, its 22-day volatility from its sixth, its 63-day Sharpe ratio from its eleventh', {
      sources: ON_PRICES,
      series: [
        s('A', "zscores(close['NVDA'], w => 63, r => 0)", { name: '63-day z-score, ramp 0', style: { kind: 'area' } }),
        s('B', "volatility(close['NVDA'], w => 22, type => 'simple', r => 5)", { name: '22-day volatility, ramp 5', pane: 1 }),
        s('C', "sharpe_ratio(close['NVDA'], w => 63, r => 10)", { name: '63-day Sharpe ratio, ramp 10', pane: 2 }),
      ],
      panes: [{ height: 1 }, { height: 1 }, { height: 1 }],
    }),
  ]],
};

const RISK: GallerySheet = {
  name: 'Risk',
  rows: [[
    tile('volatility', 'Realized volatility: the S&P 500\'s over a month, a quarter and a year', {
      sources: ON_PRICES,
      series: [
        s('A', "volatility(close['SPX'], w => 21, type => 'simple')", { name: '1 month' }),
        s('B', "volatility(close['SPX'], w => 63, type => 'simple')", { name: '3 months' }),
        s('C', "volatility(close['SPX'], w => 252, type => 'simple')", { name: '1 year' }),
      ],
      annotations: [...MARKS],
    }),
    tile('drawdown', 'Drawdown and maximum drawdown: Nvidia below its running peak, and the deepest yet', {
      sources: ON_PRICES,
      series: [
        s('A', "drawdown(close['NVDA'], w => all)", { name: 'Drawdown', style: { kind: 'area' } }),
        s('B', "max_drawdown(close['NVDA'], w => all)", { name: 'Deepest yet', style: { kind: 'step' } }),
      ],
      annotations: [...MARKS],
    }),
  ], [
    // gs-quant's: each price's excess returns over the dollar's cash (the risk-free data model, item 13b), their
    // compounded return over their volatility
    tile('sharpe', 'Sharpe ratio: a year\'s over dollar cash, stocks against gold and long bonds', {
      sources: [PRICES, CASH],
      series: [
        s('A', "sharpe_ratio(excess_returns(close['SPX'], cash.index['USD']), w => 252)", { name: 'S&P 500' }),
        s('B', "sharpe_ratio(excess_returns(close['GLD'], cash.index['USD']), w => 252)", { name: 'Gold' }),
        s('C', "sharpe_ratio(excess_returns(close['TLT'], cash.index['USD']), w => 252)", { name: 'Long bonds' }),
      ],
      annotations: [{ kind: 'level', value: 0 }],
    }),
    tile('max-drawdown-year', 'Maximum drawdown over a rolling year: the two indices', {
      sources: ON_PRICES,
      series: [
        s('A', "max_drawdown(close['SPX'], w => 252)", { name: 'S&P 500', style: { kind: 'area' } }),
        s('B', "max_drawdown(close['NDX'], w => 252)", { name: 'Nasdaq 100' }),
      ],
      annotations: [...MARKS],
    }),
    tile('exponential-volatility', 'Exponential volatility: the S&P 500\'s RiskMetrics-style, beside its realized, and the curve\'s spread\'s', {
      sources: [PRICES, RATES],
      series: [
        s('A', "exponential_volatility(px.close['SPX'], beta => 0.94)", { name: 'Exponential (0.94)' }),
        s('B', "volatility(px.close['SPX'], w => 63, type => 'simple')", { name: 'Realized, 3 months', style: { dash: 'dashed', width: 1 } }),
        s('C', "exponential_spread_volatility(rates.yield['10Y'] - rates.yield['2Y'], beta => 0.94)", { name: '10s2s spread, points a year', pane: 1 }),
      ],
      panes: [{ height: 2 }, { height: 1 }],
    }),
  ], [
    tile('vol-swap', 'Vol swap volatility: the S&P 500\'s zero-mean realized volatility over a month, beside its realized volatility', {
      sources: ON_PRICES,
      series: [
        s('A', "vol_swap_volatility(close['SPX'], w => 21)", { name: 'Vol swap, 21 days' }),
        s('B', "volatility(close['SPX'], w => 21, type => 'simple')", { name: 'Realized, 21 days', style: { dash: 'dashed', width: 1 } }),
      ],
      annotations: [...MARKS],
    }),
    tile('benchmark', 'Against a benchmark: Nvidia\'s tracking error to the S&P 500 and its information ratio, over 3 months', {
      sources: ON_PRICES,
      series: [
        s('A', "tracking_error(close['NVDA'], close['SPX'], w => 63)", { name: 'Tracking error' }),
        s('B', "information_ratio(close['NVDA'], close['SPX'], w => 63)", { name: 'Information ratio', pane: 1 }),
      ],
      panes: [{ height: 1 }, { height: 1 }],
      annotations: [{ kind: 'level', value: 0, pane: 1 }],
    }),
    tile('capture', 'Up and down capture: the Nasdaq 100 on the S&P 500\'s up days and its down days, over a year', {
      sources: ON_PRICES,
      series: [
        s('A', "up_capture(close['NDX'], close['SPX'], w => 252)", { name: 'Up capture' }),
        s('B', "down_capture(close['NDX'], close['SPX'], w => 252)", { name: 'Down capture', style: { dash: 'dashed' } }),
      ],
      annotations: [{ kind: 'level', value: 1, label: '100%' }],
    }),
  ]],
};

const RELATIONSHIPS: GallerySheet = {
  name: 'Relationships',
  rows: [[
    tile('correlation', 'Rolling correlation: stocks against long bonds and gold, over 3 months', {
      sources: ON_PRICES,
      series: [
        s('A', "correlation(close['SPX'], close['TLT'], w => 63)", { name: 'S&P 500 ~ long bonds' }),
        s('B', "correlation(close['SPX'], close['GLD'], w => 63)", { name: 'S&P 500 ~ gold' }),
      ],
      annotations: [{ kind: 'level', value: 0 }],
    }),
    tile('beta', 'Rolling beta: the megacaps to the S&P 500, over 6 months', {
      sources: ON_PRICES,
      series: [
        s('A', "beta(close['AAPL'], close['SPX'], w => 126)", { name: 'Apple' }), s('B', "beta(close['MSFT'], close['SPX'], w => 126)", { name: 'Microsoft' }),
        s('C', "beta(close['NVDA'], close['SPX'], w => 126)", { name: 'Nvidia' }),
      ],
      annotations: [{ kind: 'level', value: 1, label: 'beta 1' }],
    }),
  ], [
    tile('cov', 'Rolling covariance: Apple\'s and Microsoft\'s daily returns, over 3 months', {
      sources: ON_PRICES,
      series: [s('A', "cov(returns(close['AAPL'], 1, 'simple'), returns(close['MSFT'], 1, 'simple'), w => 63)", { name: 'Apple, Microsoft', style: { kind: 'area' } })],
      annotations: [...MARKS],
    }),
    tile('regression', 'A regression: Nvidia on the S&P 500 -- its alpha and R², the hedge ratio and the pair\'s spread, over 6 months', {
      sources: ON_PRICES,
      series: [
        s('A', "alpha(close['NVDA'], close['SPX'], w => 126)", { name: 'Alpha (ann.)' }),
        s('B', "r_squared(close['NVDA'], close['SPX'], w => 126)", { name: 'R²', pane: 1 }),
        s('C', "hedge_ratio(close['MSFT'], close['AAPL'], w => 126)", { name: 'Microsoft per Apple', pane: 2 }),
        s('D', "residual(close['MSFT'], close['AAPL'], w => 126)", { name: 'The pair\'s spread', pane: 3 }),
      ],
      panes: [{ height: 1 }, { height: 1 }, { height: 1 }, { height: 1 }],
      annotations: [{ kind: 'level', value: 0 }, { kind: 'level', value: 0, pane: 3 }],
    }),
    tile('pair', 'A pair: Apple over Microsoft, and how far it is from its 6-month mean', {
      sources: ON_PRICES,
      series: [
        s('A', "close['AAPL'] / close['MSFT']", { name: 'Apple / Microsoft' }),
        s('B', "zscores(A, w => 126)", { name: 'Its z-score', pane: 1 }),
      ],
      panes: [{ height: 2 }, { height: 1 }],
      annotations: [{ kind: 'level', value: 2, pane: 1 }, { kind: 'level', value: -2, pane: 1 }],
    }),
  ], [
    tile('lead-lag', 'Lead and lag: Nvidia\'s daily returns against the S&P 500\'s a day before, and a day after, over 3 months', {
      sources: ON_PRICES,
      series: [
        s('A', "lead_lag(close['NVDA'], close['SPX'], lag => 1, w => 63)", { name: 'The index a day before' }),
        s('B', "lead_lag(close['NVDA'], close['SPX'], lag => -1, w => 63)", { name: 'The index a day after', style: { dash: 'dashed' } }),
      ],
      annotations: [{ kind: 'level', value: 0 }],
    }),
    tile('reversion', 'Mean reversion: Apple over Microsoft, its Dickey-Fuller statistic over a year, and its half-life', {
      sources: ON_PRICES,
      series: [
        s('A', "close['AAPL'] / close['MSFT']", { name: 'Apple / Microsoft' }),
        s('B', "dickey_fuller(A, w => 252)", { name: 'Dickey-Fuller t', pane: 1 }),
        s('C', "half_life(A, w => 252)", { name: 'Half-life (days)', pane: 2, style: { kind: 'area' } }),
      ],
      panes: [{ height: 2 }, { height: 1 }, { height: 1 }],
      annotations: [{ kind: 'level', value: -2.86, pane: 1, label: '5%' }],
    }),
  ]],
};

const TECHNICALS: GallerySheet = {
  name: 'Technicals',
  rows: [[
    tile('candles-weekly', 'Candles: the S&P 500, a candle a week', {
      sources: ON_PRICES,
      grain: 'week',
      series: [s('A', "ohlc(open['SPX'], high['SPX'], low['SPX'], close['SPX'])", { name: 'S&P 500' })],
      range: { last: '1y' },
    }),
    tile('bollinger', 'Bollinger bands: gold within 2 standard deviations of its 20-day average', {
      sources: ON_PRICES,
      series: [
        s('A', "close['GLD']", { name: 'Gold' }),
        s('B', "bollinger_bands(close['GLD'], w => 20, k => 2)", { name: '20d ±2σ' }),
        s('C', "mean(close['GLD'], 20)", { name: '20-day average', style: { dash: 'dashed', width: 1 } }),
      ],
      range: { last: '6m' },
    }),
  ], [
    tile('rsi', 'Relative strength index: Nvidia\'s 14-day, overbought above 70', {
      sources: ON_PRICES,
      series: [
        s('A', "close['NVDA']", { name: 'Nvidia' }),
        s('B', "rsi(close['NVDA'], w => 14)", { name: 'RSI 14', pane: 1 }),
      ],
      range: { last: '1y' },
      panes: [{ height: 2 }, { height: 1 }],
      annotations: [{ kind: 'level', value: 70, pane: 1, label: 'overbought' }, { kind: 'level', value: 30, pane: 1, label: 'oversold' }],
    }),
    tile('candles-volume', 'Candles and volume: Microsoft a day at a time, over 3 months', {
      sources: ON_PRICES,
      series: [
        s('A', "ohlc(open['MSFT'], high['MSFT'], low['MSFT'], close['MSFT'])", { name: 'Microsoft' }),
        s('B', "volume['MSFT']", { name: 'Volume', pane: 1, style: { kind: 'bars' } }),
      ],
      range: { last: '3m' },
      panes: [{ height: 3 }, { height: 1 }],
    }),
    tile('macd', 'MACD: the S&P 500\'s 12- and 26-day exponential averages apart, and its 9-day signal', {
      sources: ON_PRICES,
      series: [
        s('A', "macd(close['SPX'], m => 12, n => 26, s => 1)", { name: 'MACD (12, 26)' }),
        s('B', "macd(close['SPX'], m => 12, n => 26, s => 9)", { name: 'Signal (9)', style: { dash: 'dashed' } }),
        s('C', "A - B", { name: 'Histogram', style: { kind: 'bars' } }),
      ],
      range: { last: '1y' },
      annotations: [{ kind: 'level', value: 0 }],
    }),
  ], [
    tile('atr-stochastic', 'Average true range and stochastic: Nvidia\'s 14-day range, and where it closed within it', {
      sources: ON_PRICES,
      series: [
        s('A', "close['NVDA']", { name: 'Nvidia' }),
        s('B', "atr(high['NVDA'], low['NVDA'], close['NVDA'], w => 14)", { name: 'ATR 14', pane: 1, style: { kind: 'area' } }),
        s('C', "stochastic(high['NVDA'], low['NVDA'], close['NVDA'], w => 14)", { name: '%K 14', pane: 2 }),
      ],
      range: { last: '1y' },
      panes: [{ height: 2 }, { height: 1 }, { height: 1 }],
      annotations: [{ kind: 'level', value: 80, pane: 2 }, { kind: 'level', value: 20, pane: 2 }],
    }),
    tile('channels', 'Keltner and Donchian channels: the S&P 500 within 2 average true ranges of its average, and its 20-day range', {
      sources: ON_PRICES,
      series: [
        s('A', "close['SPX']", { name: 'S&P 500' }),
        s('B', "keltner_channels(high['SPX'], low['SPX'], close['SPX'], w => 20, a => 10, k => 2)", { name: 'Keltner (20, 10, 2)' }),
        s('C', "donchian_channels(high['SPX'], low['SPX'], w => 20)", { name: 'Donchian 20' }),
      ],
      range: { last: '6m' },
    }),
    tile('volume-flow', 'On-balance volume and VWAP: Apple\'s volume behind its moves, and its 20-day volume-weighted price', {
      sources: ON_PRICES,
      series: [
        s('A', "close['AAPL']", { name: 'Apple' }),
        s('B', "vwap(close['AAPL'], volume['AAPL'], w => 20)", { name: 'VWAP 20', style: { dash: 'dashed', width: 1 } }),
        s('C', "obv(close['AAPL'], volume['AAPL'])", { name: 'On-balance volume', pane: 1, style: { kind: 'area' } }),
      ],
      range: { last: '1y' },
      panes: [{ height: 2 }, { height: 1 }],
    }),
  ]],
};

const MATH: GallerySheet = {
  name: 'Math',
  rows: [[
    tile('log-exp', 'Natural log and exponential: the S&P 500, its log, and e to its log', {
      sources: ON_PRICES,
      series: [
        s('A', "close['SPX']", { name: 'S&P 500' }),
        s('B', "exp(ln(A))", { name: 'e to its log', axis: 'left', style: { dash: 'dashed' } }),
        s('C', "ln(A)", { name: 'Its log', pane: 1 }),
      ],
      panes: [{ height: 1 }, { height: 1 }],
    }),
    tile('sqrt-power', 'Square root and power: gold\'s standard deviation, the root of its variance, and its square', {
      sources: ON_PRICES,
      series: [
        s('A', "std(close['GLD'], 63)", { name: 'Standard deviation' }),
        s('B', "sqrt(var(close['GLD'], 63))", { name: 'Root of the variance', style: { dash: 'dashed' } }),
        s('C', "power(A, 2)", { name: 'Its square', pane: 1, style: { kind: 'area' } }),
      ],
      panes: [{ height: 1 }, { height: 1 }],
    }),
  ], [
    tile('abs', 'Absolute value: the size of the S&P 500\'s daily moves, and their monthly average', {
      sources: ON_PRICES,
      series: [
        s('A', "abs(returns(close['SPX'], 1, 'simple'))", { name: 'Daily move', style: { kind: 'bars' } }),
        s('B', "mean(A, 21)", { name: '21-day average' }),
      ],
      range: { last: '1y' },
    }),
    tile('floor-ceil', 'Floor and cap: the euro\'s z-score, held within ±2', {
      sources: ON_PRICES,
      series: [
        s('A', "zscores(close['EURUSD'], w => 63)", { name: 'Z-score', style: { width: 1 } }),
        s('B', "ceil(floor(A, -2), 2)", { name: 'Held within ±2', style: { kind: 'area' } }),
      ],
      range: { last: '2y' },
    }),
  ], [
    tile('if-compare', 'If, not and compare: the S&P 500 or its 200-day average, whichever is lower; below it shaded', {
      sources: ON_PRICES,
      series: [
        s('A', "if(close['SPX'] < mean(close['SPX'], 200), close['SPX'], mean(close['SPX'], 200))", { name: 'The lower of the two' }),
        s('B', "compare(close['SPX'], mean(close['SPX'], 200))", { name: 'Above (1) or below (-1)', pane: 1, style: { kind: 'step' } }),
        s('C', "NOT close['SPX'] > mean(close['SPX'], 200)", { name: 'Not above its average' }),
      ],
      panes: [{ height: 2 }, { height: 1 }],
    }),
    tile('filter-smooth', 'Filter, smooth and weigh: gold without its closes above 200, its spikes smoothed, and a 60/40 basket', {
      sources: ON_PRICES,
      series: [
        s('A', "filter(close['GLD'], operator => 'greater_than', value => 200)", { name: 'Gold, at or below 200' }),
        s('B', "smooth_spikes(close['GLD'], threshold => 0.02, type => 'percentage')", { name: 'Spikes smoothed', style: { dash: 'dashed', width: 1 } }),
        s('C', "weighted_sum(rebase(close['SPX'], base => 100), 0.6, rebase(close['TLT'], base => 100), 0.4)", { name: '60/40, from 100', axis: 'right' }),
      ],
      range: { last: '2y' },
    }),
    tile('basket', 'A rebalanced basket: 60% stocks, 40% long bonds from 100, rebalanced each day and each month', {
      sources: ON_PRICES,
      series: [
        s('A', "basket(close['SPX'], 0.6, close['TLT'], 0.4, rebalance => 'daily')", { name: 'Daily' }),
        s('B', "basket(close['SPX'], 0.6, close['TLT'], 0.4, rebalance => 'monthly')", { name: 'Monthly', style: { dash: 'dashed' } }),
      ],
    }),
  ]],
};

const DATES: GallerySheet = {
  name: 'Dates',
  rows: [[
    tile('align', 'Two calendars combined: the S&P 500 in real terms -- its daily close over the month-end price index, stepped to every day', {
      sources: PRICES_AND_MACRO,
      align: 'step',
      series: [
        s('A', "px.close['SPX'] / macro.value['CPI'] * 100", { name: 'S&P 500 in real terms' }),
        s('B', "px.close['SPX']", { name: 'S&P 500', axis: 'right', style: { width: 1, dash: 'dashed' } }),
      ],
      range: { last: '5y' },
    }),
    tile('interpolate', 'Onto another\'s dates: the monthly price index on the S&P 500\'s trading days, each its last month-end', {
      sources: PRICES_AND_MACRO,
      series: [
        s('A', "interpolate(macro.value['CPI'], px.close['SPX'], method => 'step')", { name: 'Price index, stepped daily', style: { kind: 'step' } }),
        s('B', "macro.value['CPI']", { name: 'Month-end', style: { kind: 'dots' } }),
      ],
      range: { last: '2y' },
    }),
  ], [
    tile('splice', 'Splicing two series: gold to 2022 then the bond fund; gold until the bond fund\'s 2023; the fund where gold is above 190', {
      sources: ON_PRICES,
      series: [
        s('A', "append(filter(if(year(close['GLD']) < 2023, close['GLD'], 0), operator => 'equals', value => 0), close['TLT'])", { name: 'Gold, then the fund' }),
        s('B', "prepend(close['GLD'], filter(if(year(close['TLT']) >= 2023, close['TLT'], 0), operator => 'equals', value => 0))", { name: 'Gold, until the fund', style: { dash: 'dashed' } }),
        s('C', "union(filter(close['GLD'], operator => 'greater_than', value => 190), close['TLT'])", { name: 'Gold at or below 190, else the fund', style: { kind: 'dots' } }),
      ],
    }),
    tile('calendar', 'The calendar of each date: the S&P 500\'s day of the week, month, quarter, day of the month and year', {
      sources: ON_PRICES,
      series: [
        s('A', "weekday(close['SPX'])", { name: 'Day of the week (0 Monday)', style: { kind: 'step' } }),
        s('B', "month(close['SPX'])", { name: 'Month', style: { kind: 'step' } }),
        s('C', "quarter(close['SPX'])", { name: 'Quarter', style: { kind: 'step' } }),
        s('D', "day(close['SPX'])", { name: 'Day of the month', style: { kind: 'dots' } }),
        s('E', "year(close['SPX'])", { name: 'Year', pane: 1, style: { kind: 'step' } }),
      ],
      range: { last: '3m' },
      panes: [{ height: 2 }, { height: 1 }],
    }),
    tile('first-last-count', 'First, last and how many: the S&P 500 between its first and last close, and its count of closes', {
      sources: ON_PRICES,
      series: [
        s('A', "close['SPX']", { name: 'S&P 500' }),
        s('B', "first(close['SPX'], all)", { name: 'Its first close', style: { dash: 'dashed', width: 1 } }),
        s('C', "last(close['SPX'], whole)", { name: 'Its last close', style: { dash: 'dashed', width: 1 } }),
        s('D', "count(close['SPX'], all)", { name: 'Closes to date', pane: 1, style: { kind: 'area' } }),
      ],
      panes: [{ height: 2 }, { height: 1 }],
    }),
  ]],
};

const WAYS: GallerySheet = {
  name: 'Ways to view',
  rows: [[
    tile('split', 'A line per symbol: each stock\'s 3-month volatility, one series split by symbol', {
      sources: ON_PRICES,
      series: [s('A', "volatility(close[asset_class = 'Equity'], w => 63, type => 'simple')", { name: '3m vol', split: 'symbol' })],
      range: { last: '3y' },
    }),
    tile('monthly', 'A month at a time: the S&P 500\'s month-end close, and its 12-month return', {
      sources: ON_PRICES,
      grain: 'month',
      series: [
        s('A', "close['SPX']", { name: 'Month-end close', style: { kind: 'step' } }),
        s('B', "returns(close['SPX'], 12, 'simple')", { name: '12-month return', pane: 1, style: { kind: 'bars' } }),
      ],
      panes: [{ height: 2 }, { height: 1 }],
    }),
    tile('log-scale', 'A log scale: Nvidia against the S&P 500, since the start', {
      sources: ON_PRICES,
      series: [s('A', "close['NVDA']", { name: 'Nvidia' }), s('B', "close['SPX']", { name: 'S&P 500', axis: 'right' })],
      panes: [{ log: true }],
      view: { navigator: true },
    }),
  ], [
    tile('compare', 'Every line from one start: each asset class\'s % change since the first date shown', {
      sources: ON_PRICES,
      series: [
        s('A', "close['SPX']", { name: 'Stocks' }), s('B', "close['TLT']", { name: 'Bonds' }),
        s('C', "close['GLD']", { name: 'Gold' }), s('D', "close['EURUSD']", { name: 'Euro' }),
      ],
      range: { last: '1y' },
      view: { compare: 'percent', stats: true },
    }),
    tile('styles', 'Styles: an area, steps, dots, bars and dashes in three panes', {
      sources: ON_PRICES,
      series: [
        s('A', "close['JPM']", { name: 'JPMorgan', style: { kind: 'area' } }),
        s('B', "max(close['JPM'], 21)", { name: 'Month high', style: { kind: 'step', width: 1 } }),
        s('C', "mean(close['JPM'], 21)", { name: 'Month average', style: { dash: 'dotted', width: 1 } }),
        s('D', "returns(close['JPM'], 1, 'simple')", { name: 'Daily return', pane: 1, style: { kind: 'dots' } }),
        s('E', "volume['JPM']", { name: 'Volume', pane: 2, style: { kind: 'bars' } }),
      ],
      range: { last: '6m' },
      panes: [{ height: 2 }, { height: 1 }, { height: 1 }],
    }),
    tile('condition', 'Shaded where it holds: the S&P 500 while the curve is inverted (the 10-year below the 2-year)', {
      sources: [PRICES, RATES],
      series: [
        s('A', "px.close['SPX']", { name: 'S&P 500' }),
        s('B', "rates.yield['10Y'] - rates.yield['2Y'] < 0", { name: 'The curve inverted' }),
        s('C', "rates.yield['10Y'] - rates.yield['2Y']", { name: '10s2s', pane: 1 }),
      ],
      panes: [{ height: 2 }, { height: 1 }],
      annotations: [{ kind: 'level', value: 0, pane: 1 }],
    }),
    tile('two-grids', 'Two grids: the S&P 500 against the 2-year yield, their correlation read in one query', {
      sources: [PRICES, RATES],
      series: [
        s('A', "px.close['SPX']", { name: 'S&P 500' }),
        s('B', "rates.yield['2Y']", { name: '2Y yield', axis: 'right' }),
        s('C', "correlation(px.close['SPX'], rates.yield['2Y'], w => 63)", { name: '3m correlation', pane: 1 }),
      ],
      panes: [{ height: 2 }, { height: 1 }],
      annotations: [...MARKS],
      view: { crosshair: true },
    }),
  ]],
};

/** The other views (the next phases' 3, 2026-10-26): a tile's series drawn otherwise than along time -- one against
 *  another, each year against the years before, its values' distribution, a curve across a split's key -- every
 *  figure the database's. Every level's Calendar tab beside them. */
const VIEWS: GallerySheet = {
  name: 'Other views',
  rows: [[
    tile('view-scatter', 'A scatter: the Nasdaq\'s daily return against the S&P 500\'s, a year of them, and their regression', {
      sources: ON_PRICES,
      series: [s('A', "returns(close['SPX'], 1, 'simple')", { name: 'S&P 500' }), s('B', "returns(close['NDX'], 1, 'simple')", { name: 'Nasdaq 100' })],
      range: { last: '1y' },
      view: { mode: 'scatter', stats: true },
    }),
    tile('view-seasonality', 'Seasonality: each year of the S&P 500 week by week from its own start, against the four before', {
      sources: ON_PRICES,
      grain: 'week',
      series: [s('A', "close['SPX']", { name: 'S&P 500' })],
      view: { mode: 'seasonality', compare: 'rebase', years: 4 },
    }),
  ], [
    tile('view-histogram', 'A histogram: the S&P 500\'s daily returns over three years, a normal beside them, the 5% marked', {
      sources: ON_PRICES,
      series: [s('A', "returns(close['SPX'], 1, 'simple')", { name: 'Daily return' })],
      range: { last: '3y' },
      view: { mode: 'histogram', bins: 40 },
    }),
    tile('view-curve', 'A curve: the Treasury curve now, a year ago and two years ago', {
      sources: [RATES],
      series: [s('A', "yield", { name: 'Yield', split: 'tenor' })],
      view: { mode: 'curve', dates: ['latest', '1y', '2y'] },
    }),
  ]],
};

/** On the chart itself (Phase 5, 2026-10-27): a trend projected -- the database's line over a series' last quarter and
 *  its last year, each on past its last date -- and a person's own: a note pinned at the low after the bear market (the
 *  sample's close that day), a line drawn from it, the bear market's period. The measure, the Draw button and the replay are the tile's own, on every tile. */
const ON_CHART: GallerySheet = {
  name: 'On the chart',
  rows: [[
    tile('projected', 'A trend projected: the S&P 500\'s least-squares line over its last quarter and its last year, each drawn on', {
      sources: ON_PRICES,
      series: [s('A', "close['SPX']", { name: 'S&P 500' })],
      range: { last: '2y' },
      annotations: [
        { kind: 'projection', series: 'A', window: 63, ahead: '3m', label: 'Last quarter\'s trend' },
        { kind: 'projection', series: 'A', window: 252, ahead: '6m', label: 'Last year\'s trend' },
      ],
    }),
    tile('drawn', 'Drawn by hand: the low after the bear market pinned, a line from it to the end of 2024, the bear market shaded', {
      sources: ON_PRICES,
      series: [s('A', "close['SPX']", { name: 'S&P 500' })],
      range: { from: '2021-06-01', to: '2024-12-31' },
      annotations: [
        { kind: 'period', from: '2022-01-03', to: '2022-10-12', label: 'Bear market' },
        { kind: 'note', at: { date: '2023-03-16', value: 4190.96 }, text: 'The low' },
        { kind: 'line', from: { date: '2023-03-16', value: 4190.96 }, to: { date: '2024-12-31', value: 6467.59 }, label: 'From the low', color: 2 },
      ],
    }),
  ]],
};

/** Every sheet of functions, in the catalog's order of families, then the ways to view, the other views and what is
 *  put on the chart itself. */
export const TIMESERIES_GALLERY: readonly GallerySheet[] = [RETURNS, LEVEL, ROLLING, RISK, RELATIONSHIPS, TECHNICALS, MATH, DATES, WAYS, VIEWS, ON_CHART];

/** A sheet's rows as bands that fit its window: each row an equal share of the height, its tiles of the width. */
function bandsOf(rows: GallerySheet['rows']): Band[] {
  return rows.map((row): Band => {
    const node: Node = row.length === 1 ? { tile: row[0]!.id }
      : { split: 'row', parts: row.map((v) => ({ node: { tile: v.id }, size: 1 / row.length })) };
    return { height: 1 / rows.length, node };
  });
}

/** The page: a sheet per family of functions, the ways to view, and the two grids every tile reads -- over the samples,
 *  or over the cubes `over` gives. */
export async function timeseriesGalleryPage(over?: PageOver): Promise<PageDocument> {
  const sheets: PageSheet[] = TIMESERIES_GALLERY.map((g, i) => ({
    id: `sheet-${i + 1}`, name: g.name, layout: { kind: 'bands', fit: true, bands: bandsOf(g.rows) },
  }));
  sheets.push({
    id: `sheet-${sheets.length + 1}`, name: 'The data', layout: { kind: 'bands', fit: true, bands: [{
      height: 1, node: { split: 'row', parts: [{ size: 0.4, node: { tile: 'prices' } }, { size: 0.25, node: { tile: 'rates' } },
        { size: 0.15, node: { tile: 'macro' } }, { size: 0.2, node: { tile: 'cash' } }] },
    }] },
  });
  return writePageOf({
    name: over?.name ?? 'Timeseries gallery',
    cubes: over?.cubes ?? [...await marketCubes(), await macroCube(), await cashCube()],
    views: { views: [...MARKET_GRIDS, MACRO_GRID, CASH_GRID, ...TIMESERIES_GALLERY.flatMap((g) => g.rows.flat())], sheets },
  });
}
