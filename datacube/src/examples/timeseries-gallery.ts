// THE TIMESERIES GALLERY (the user, 2026-10-25: "can we make this a real saved page with multiple tabs showing all of
// these similar to how we have some chart examples in the Open dialog?"): every function of the timeseries tile's
// catalog (timeseries-expr.ts TS_FUNCTIONS) drawn at least once, a sheet per family of it, titled by what it does --
// then a sheet of the ways a tile can be read (a line per symbol, a week or a month at a time, a log scale, every line
// from one start, its styles, two grids in one query) and the two grids they read. Over the same two samples as the
// markets example (markets.ts), each sheet fitting its window; test/examples.test.ts holds it to every function, each
// tile drawable, and test/timeseries-queries.test.ts runs every series of it on its samples.

import type { Band, Node } from '../layout/bands.ts';
import { writePageOf, type PageDocument, type PageSheet, type PageView } from '../page-document.ts';
import { MARKET_GRIDS, MARKS, marketCubes, PRICES, RATES, s, tile } from './markets.ts';

const ON_PRICES = [PRICES];

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
        s('A', 'returns(close[SPX])', { name: 'Simple' }),
        s('B', 'returns(close[SPX], type=log)', { name: 'Log', style: { dash: 'dashed', width: 1 } }),
      ],
      range: { last: '1y' },
    }),
    tile('diff-lag', 'Difference and lag: gold\'s close a week before, and its change over the week', {
      sources: ON_PRICES,
      series: [
        s('A', 'close[GLD]', { name: 'Gold' }),
        s('B', 'lag(close[GLD], 5)', { name: 'A week before', style: { dash: 'dashed', width: 1 } }),
        s('C', 'diff(close[GLD], 5)', { name: 'Change over the week', pane: 1, style: { kind: 'bars' } }),
      ],
      range: { last: '6m' },
      panes: [{ height: 2 }, { height: 1 }],
    }),
  ], [
    tile('compounded', 'Prices from returns: Nvidia\'s daily returns compounded back into a level', {
      sources: ON_PRICES,
      series: [
        s('A', 'rebase(close[NVDA])', { name: 'Close, rebased' }),
        s('B', 'prices(returns(close[NVDA]), 100)', { name: 'Its returns compounded from 100', style: { dash: 'dashed' } }),
        s('C', 'returns(close[NVDA])', { name: 'Daily return', pane: 1, style: { kind: 'dots' } }),
      ],
      // from the start: the first date shown is where the returns are compounded from, so the two lines are one
      panes: [{ height: 2 }, { height: 1 }],
    }),
    tile('annualize', 'Annualize: the S&P 500\'s 21-day standard deviation of returns, a day\'s and a year\'s', {
      sources: ON_PRICES,
      series: [
        s('A', 'annualize(std(returns(close[SPX]), 21))', { name: 'Annualized' }),
        s('B', 'std(returns(close[SPX]), 21)', { name: 'Daily', pane: 1, style: { kind: 'area' } }),
      ],
      panes: [{ height: 1 }, { height: 1 }],
      annotations: [...MARKS],
    }),
  ]],
};

const LEVEL: GallerySheet = {
  name: 'Level',
  rows: [[
    tile('rebase', 'Rebase to 100: four markets from one start, the first date shown', {
      sources: ON_PRICES,
      series: [
        s('A', 'rebase(close[SPX])', { name: 'S&P 500' }), s('B', 'rebase(close[XOM])', { name: 'Exxon' }),
        s('C', 'rebase(close[JPM])', { name: 'JPMorgan' }), s('D', 'rebase(close[EURUSD])', { name: 'Euro' }),
      ],
      range: { last: '3y' },
      annotations: [{ kind: 'level', value: 100 }],
    }),
    tile('index', 'Index: the bank and the oil major, each 1 at the first date shown', {
      sources: ON_PRICES,
      series: [s('A', 'index(close[JPM])', { name: 'JPMorgan' }), s('B', 'index(close[XOM])', { name: 'Exxon' })],
      range: { last: '1y' },
      annotations: [{ kind: 'level', value: 1 }],
    }),
  ], [
    tile('change', 'Change from start: gold and long bonds this year, each in its own units', {
      sources: ON_PRICES,
      series: [
        s('A', 'change(close[GLD])', { name: 'Gold', style: { kind: 'area' } }),
        s('B', 'change(close[TLT])', { name: 'Long bonds', style: { kind: 'area' } }),
      ],
      range: { last: 'ytd' },
      annotations: [{ kind: 'level', value: 0 }],
    }),
    tile('percent-change', '% change from start: Apple, Microsoft and Nvidia over the last year', {
      sources: ON_PRICES,
      series: [
        s('A', 'percent_change(close[AAPL])', { name: 'Apple' }), s('B', 'percent_change(close[MSFT])', { name: 'Microsoft' }),
        s('C', 'percent_change(close[NVDA])', { name: 'Nvidia' }),
      ],
      range: { last: '1y' },
      annotations: [{ kind: 'level', value: 0 }],
    }),
  ]],
};

const ROLLING: GallerySheet = {
  name: 'Rolling statistics',
  rows: [[
    tile('mean', 'Moving average: the S&P 500 with its 50- and 200-day', {
      sources: ON_PRICES,
      series: [
        s('A', 'close[SPX]', { name: 'S&P 500' }),
        s('B', 'mean(close[SPX], 50)', { name: '50-day', style: { width: 1 } }),
        s('C', 'mean(close[SPX], 200)', { name: '200-day', style: { width: 1, dash: 'dashed' } }),
      ],
      range: { last: '2y' },
    }),
    tile('min-max-range', 'Rolling minimum, maximum and range: Microsoft\'s 52-week channel', {
      sources: ON_PRICES,
      series: [
        s('A', 'close[MSFT]', { name: 'Microsoft' }),
        s('B', 'max(close[MSFT], 252)', { name: '52-week high', style: { kind: 'step', width: 1 } }),
        s('C', 'min(close[MSFT], 252)', { name: '52-week low', style: { kind: 'step', width: 1 } }),
        s('D', 'range(close[MSFT], 252)', { name: 'Its range', pane: 1, style: { kind: 'area' } }),
      ],
      panes: [{ height: 2 }, { height: 1 }],
    }),
    tile('sum', 'Rolling sum: Apple\'s volume over a month, and its running total', {
      sources: ON_PRICES,
      series: [
        s('A', 'sum(volume[AAPL], 21)', { name: '21-day volume', style: { kind: 'bars' } }),
        s('B', 'sum(volume[AAPL])', { name: 'Since the start', pane: 1, style: { kind: 'area' } }),
      ],
      panes: [{ height: 1 }, { height: 1 }],
    }),
  ], [
    tile('std-var', 'Standard deviation and variance: gold\'s over the last quarter', {
      sources: ON_PRICES,
      series: [
        s('A', 'std(close[GLD], 63)', { name: '63-day standard deviation' }),
        s('B', 'var(close[GLD], 63)', { name: '63-day variance', pane: 1, style: { kind: 'area' } }),
      ],
      panes: [{ height: 1 }, { height: 1 }],
    }),
    tile('zscores', 'Z-score: Nvidia, standard deviations from its 63-day mean', {
      sources: ON_PRICES,
      series: [s('A', 'zscores(close[NVDA], 63)', { name: 'Nvidia', style: { kind: 'area' } })],
      range: { last: '2y' },
      annotations: [
        { kind: 'level', value: 2, label: '+2σ' }, { kind: 'level', value: 0 }, { kind: 'level', value: -2, label: '−2σ' },
      ],
    }),
    tile('winsorize', 'Winsorize: the euro\'s daily returns held within 1.5 standard deviations', {
      sources: ON_PRICES,
      series: [
        s('A', 'winsorize(returns(close[EURUSD]), 1.5)', { name: 'Winsorized at 1.5σ', style: { kind: 'area', width: 1 } }),
        s('B', 'returns(close[EURUSD])', { name: 'Daily return', style: { kind: 'dots' } }),
      ],
      range: { last: '3m' },
    }),
  ]],
};

const RISK: GallerySheet = {
  name: 'Risk',
  rows: [[
    tile('volatility', 'Realized volatility: the S&P 500\'s over a month, a quarter and a year', {
      sources: ON_PRICES,
      series: [
        s('A', 'volatility(close[SPX], 21)', { name: '1 month' }),
        s('B', 'volatility(close[SPX], 63)', { name: '3 months' }),
        s('C', 'volatility(close[SPX], 252)', { name: '1 year' }),
      ],
      annotations: [...MARKS],
    }),
    tile('drawdown', 'Drawdown and maximum drawdown: Nvidia below its running peak, and the deepest yet', {
      sources: ON_PRICES,
      series: [
        s('A', 'drawdown(close[NVDA])', { name: 'Drawdown', style: { kind: 'area' } }),
        s('B', 'max_drawdown(close[NVDA])', { name: 'Deepest yet', style: { kind: 'step' } }),
      ],
      annotations: [...MARKS],
    }),
  ], [
    tile('sharpe', 'Sharpe ratio: a year\'s, stocks against gold and long bonds', {
      sources: ON_PRICES,
      series: [
        s('A', 'sharpe_ratio(close[SPX], 252)', { name: 'S&P 500' }), s('B', 'sharpe_ratio(close[GLD], 252)', { name: 'Gold' }),
        s('C', 'sharpe_ratio(close[TLT], 252)', { name: 'Long bonds' }),
      ],
      annotations: [{ kind: 'level', value: 0 }],
    }),
    tile('max-drawdown-year', 'Maximum drawdown over a rolling year: the two indices', {
      sources: ON_PRICES,
      series: [
        s('A', 'max_drawdown(close[SPX], 252)', { name: 'S&P 500', style: { kind: 'area' } }),
        s('B', 'max_drawdown(close[NDX], 252)', { name: 'Nasdaq 100' }),
      ],
      annotations: [...MARKS],
    }),
  ]],
};

const RELATIONSHIPS: GallerySheet = {
  name: 'Relationships',
  rows: [[
    tile('correlation', 'Rolling correlation: stocks against long bonds and gold, over 3 months', {
      sources: ON_PRICES,
      series: [
        s('A', 'correlation(close[SPX], close[TLT], 63)', { name: 'S&P 500 ~ long bonds' }),
        s('B', 'correlation(close[SPX], close[GLD], 63)', { name: 'S&P 500 ~ gold' }),
      ],
      annotations: [{ kind: 'level', value: 0 }],
    }),
    tile('beta', 'Rolling beta: the megacaps to the S&P 500, over 6 months', {
      sources: ON_PRICES,
      series: [
        s('A', 'beta(close[AAPL], close[SPX], 126)', { name: 'Apple' }), s('B', 'beta(close[MSFT], close[SPX], 126)', { name: 'Microsoft' }),
        s('C', 'beta(close[NVDA], close[SPX], 126)', { name: 'Nvidia' }),
      ],
      annotations: [{ kind: 'level', value: 1, label: 'beta 1' }],
    }),
  ], [
    tile('cov', 'Rolling covariance: Apple\'s and Microsoft\'s daily returns, over 3 months', {
      sources: ON_PRICES,
      series: [s('A', 'cov(returns(close[AAPL]), returns(close[MSFT]), 63)', { name: 'Apple, Microsoft', style: { kind: 'area' } })],
      annotations: [...MARKS],
    }),
    tile('pair', 'A pair: Apple over Microsoft, and how far it is from its 6-month mean', {
      sources: ON_PRICES,
      series: [
        s('A', 'close[AAPL] / close[MSFT]', { name: 'Apple / Microsoft' }),
        s('B', 'zscores($A, 126)', { name: 'Its z-score', pane: 1 }),
      ],
      panes: [{ height: 2 }, { height: 1 }],
      annotations: [{ kind: 'level', value: 2, pane: 1 }, { kind: 'level', value: -2, pane: 1 }],
    }),
  ]],
};

const TECHNICALS: GallerySheet = {
  name: 'Technicals',
  rows: [[
    tile('candles-weekly', 'Candles: the S&P 500, a candle a week', {
      sources: ON_PRICES,
      grain: 'week',
      series: [s('A', 'ohlc(open[SPX], high[SPX], low[SPX], close[SPX])', { name: 'S&P 500' })],
      range: { last: '1y' },
    }),
    tile('bollinger', 'Bollinger bands: gold within 2 standard deviations of its 20-day average', {
      sources: ON_PRICES,
      series: [
        s('A', 'close[GLD]', { name: 'Gold' }),
        s('B', 'bollinger_bands(close[GLD], 20, 2)', { name: '20d ±2σ' }),
        s('C', 'mean(close[GLD], 20)', { name: '20-day average', style: { dash: 'dashed', width: 1 } }),
      ],
      range: { last: '6m' },
    }),
  ], [
    tile('rsi', 'Relative strength index: Nvidia\'s 14-day, overbought above 70', {
      sources: ON_PRICES,
      series: [
        s('A', 'close[NVDA]', { name: 'Nvidia' }),
        s('B', 'rsi(close[NVDA], 14)', { name: 'RSI 14', pane: 1 }),
      ],
      range: { last: '1y' },
      panes: [{ height: 2 }, { height: 1 }],
      annotations: [{ kind: 'level', value: 70, pane: 1, label: 'overbought' }, { kind: 'level', value: 30, pane: 1, label: 'oversold' }],
    }),
    tile('candles-volume', 'Candles and volume: Microsoft a day at a time, over 3 months', {
      sources: ON_PRICES,
      series: [
        s('A', 'ohlc(open[MSFT], high[MSFT], low[MSFT], close[MSFT])', { name: 'Microsoft' }),
        s('B', 'volume[MSFT]', { name: 'Volume', pane: 1, style: { kind: 'bars' } }),
      ],
      range: { last: '3m' },
      panes: [{ height: 3 }, { height: 1 }],
    }),
  ]],
};

const MATH: GallerySheet = {
  name: 'Math',
  rows: [[
    tile('log-exp', 'Natural log and exponential: the S&P 500, its log, and e to its log', {
      sources: ON_PRICES,
      series: [
        s('A', 'close[SPX]', { name: 'S&P 500' }),
        s('B', 'exp(log($A))', { name: 'e to its log', axis: 'left', style: { dash: 'dashed' } }),
        s('C', 'log($A)', { name: 'Its log', pane: 1 }),
      ],
      panes: [{ height: 1 }, { height: 1 }],
    }),
    tile('sqrt-power', 'Square root and power: gold\'s standard deviation, the root of its variance, and its square', {
      sources: ON_PRICES,
      series: [
        s('A', 'std(close[GLD], 63)', { name: 'Standard deviation' }),
        s('B', 'sqrt(var(close[GLD], 63))', { name: 'Root of the variance', style: { dash: 'dashed' } }),
        s('C', 'power($A, 2)', { name: 'Its square', pane: 1, style: { kind: 'area' } }),
      ],
      panes: [{ height: 1 }, { height: 1 }],
    }),
  ], [
    tile('abs', 'Absolute value: the size of the S&P 500\'s daily moves, and their monthly average', {
      sources: ON_PRICES,
      series: [
        s('A', 'abs(returns(close[SPX]))', { name: 'Daily move', style: { kind: 'bars' } }),
        s('B', 'mean($A, 21)', { name: '21-day average' }),
      ],
      range: { last: '1y' },
    }),
    tile('floor-ceil', 'Floor and cap: the euro\'s z-score, held within ±2', {
      sources: ON_PRICES,
      series: [
        s('A', 'zscores(close[EURUSD], 63)', { name: 'Z-score', style: { width: 1 } }),
        s('B', 'ceil(floor($A, -2), 2)', { name: 'Held within ±2', style: { kind: 'area' } }),
      ],
      range: { last: '2y' },
    }),
  ]],
};

const WAYS: GallerySheet = {
  name: 'Ways to view',
  rows: [[
    tile('split', 'A line per symbol: each stock\'s 3-month volatility, one series split by symbol', {
      sources: ON_PRICES,
      series: [s('A', 'volatility(close[asset_class=Equity], 63)', { name: '3m vol', split: 'symbol' })],
      range: { last: '3y' },
    }),
    tile('monthly', 'A month at a time: the S&P 500\'s month-end close, and its 12-month return', {
      sources: ON_PRICES,
      grain: 'month',
      series: [
        s('A', 'close[SPX]', { name: 'Month-end close', style: { kind: 'step' } }),
        s('B', 'returns(close[SPX], 12)', { name: '12-month return', pane: 1, style: { kind: 'bars' } }),
      ],
      panes: [{ height: 2 }, { height: 1 }],
    }),
    tile('log-scale', 'A log scale: Nvidia against the S&P 500, since the start', {
      sources: ON_PRICES,
      series: [s('A', 'close[NVDA]', { name: 'Nvidia' }), s('B', 'close[SPX]', { name: 'S&P 500', axis: 'right' })],
      panes: [{ log: true }],
      view: { navigator: true },
    }),
  ], [
    tile('compare', 'Every line from one start: each asset class\'s % change since the first date shown', {
      sources: ON_PRICES,
      series: [
        s('A', 'close[SPX]', { name: 'Stocks' }), s('B', 'close[TLT]', { name: 'Bonds' }),
        s('C', 'close[GLD]', { name: 'Gold' }), s('D', 'close[EURUSD]', { name: 'Euro' }),
      ],
      range: { last: '1y' },
      view: { compare: 'percent', stats: true },
    }),
    tile('styles', 'Styles: an area, steps, dots, bars and dashes in three panes', {
      sources: ON_PRICES,
      series: [
        s('A', 'close[JPM]', { name: 'JPMorgan', style: { kind: 'area' } }),
        s('B', 'max(close[JPM], 21)', { name: 'Month high', style: { kind: 'step', width: 1 } }),
        s('C', 'mean(close[JPM], 21)', { name: 'Month average', style: { dash: 'dotted', width: 1 } }),
        s('D', 'returns(close[JPM])', { name: 'Daily return', pane: 1, style: { kind: 'dots' } }),
        s('E', 'volume[JPM]', { name: 'Volume', pane: 2, style: { kind: 'bars' } }),
      ],
      range: { last: '6m' },
      panes: [{ height: 2 }, { height: 1 }, { height: 1 }],
    }),
    tile('two-grids', 'Two grids: the S&P 500 against the 2-year yield, their correlation read in one query', {
      sources: [PRICES, RATES],
      series: [
        s('A', 'px:close[SPX]', { name: 'S&P 500' }),
        s('B', 'rates:yield[2Y]', { name: '2Y yield', axis: 'right' }),
        s('C', 'correlation(px:close[SPX], rates:yield[2Y], 63)', { name: '3m correlation', pane: 1 }),
      ],
      panes: [{ height: 2 }, { height: 1 }],
      annotations: [...MARKS],
      view: { crosshair: true },
    }),
  ]],
};

/** Every sheet of functions, in the catalog's order of families, then the ways to view. */
export const TIMESERIES_GALLERY: readonly GallerySheet[] = [RETURNS, LEVEL, ROLLING, RISK, RELATIONSHIPS, TECHNICALS, MATH, WAYS];

/** A sheet's rows as bands that fit its window: each row an equal share of the height, its tiles of the width. */
function bandsOf(rows: GallerySheet['rows']): Band[] {
  return rows.map((row): Band => {
    const node: Node = row.length === 1 ? { tile: row[0]!.id }
      : { split: 'row', parts: row.map((v) => ({ node: { tile: v.id }, size: 1 / row.length })) };
    return { height: 1 / rows.length, node };
  });
}

/** The page: a sheet per family of functions, the ways to view, and the two grids every tile reads. */
export async function timeseriesGalleryPage(): Promise<PageDocument> {
  const sheets: PageSheet[] = TIMESERIES_GALLERY.map((g, i) => ({
    id: `sheet-${i + 1}`, name: g.name, layout: { kind: 'bands', fit: true, bands: bandsOf(g.rows) },
  }));
  sheets.push({
    id: `sheet-${sheets.length + 1}`, name: 'The data', layout: { kind: 'bands', fit: true, bands: [{
      height: 1, node: { split: 'row', parts: [{ size: 0.6, node: { tile: 'prices' } }, { size: 0.4, node: { tile: 'rates' } }] },
    }] },
  });
  return writePageOf({
    name: 'Timeseries gallery',
    cubes: await marketCubes(),
    views: { views: [...MARKET_GRIDS, ...TIMESERIES_GALLERY.flatMap((g) => g.rows.flat())], sheets },
  });
}
