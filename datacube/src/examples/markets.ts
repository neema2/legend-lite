// THE MARKETS EXAMPLE (docs/DATACUBE_TIMESERIES_DESIGN_2026_10_25.md §9): the timeseries tile over two grids of
// DataCube's own samples -- daily prices of ten instruments and a Treasury curve -- a sheet each for what a markets desk
// reads first: performance rebased, risk (drawdowns, realized volatility, correlation, beta), technicals (candles,
// Bollinger bands, moving averages, RSI, volume) and rates (the curve, the 10s2s spread, a bond fund against its yield, the two
// grids read in one query). Each series is typed as a person types it and read by DataCube's parser (timeseries-expr.ts
// parseExpr); the page is written by DataCube's own writers, as the other example pages are, and opens as a saved page
// does. test/examples.test.ts reads it back.

import { element } from '../../../pure-protocol/src/index.ts';
import { DEFAULT_CONFIGURATION, type CubeConfiguration } from '../config.ts';
import { fileSource, writeCube, type CubeDocument, type FileSource } from '../cube-document.ts';
import type { Band, Node } from '../layout/bands.ts';
import { openedState } from '../cube-state.ts';
import { writePageOf, type PageDocument, type PageSheet, type PageView } from '../page-document.ts';
import { sampleById, sampleFileName } from '../samples.ts';
import type { ColumnSpec, CubeSnapshot } from '../snapshot.ts';
import { parseExpr } from '../timeseries-expr.ts';
import { readTimeseries, type TimeseriesSpec, type TsSeries, type TsSource } from '../timeseries-spec.ts';
import { TreeState } from '../tree.ts';

/** The markets sample's columns as the compiler types them (test/examples.test.ts holds them to DuckDB's reading). */
export const MARKET_SPECS: readonly ColumnSpec[] = [
  { name: 'symbol', type: 'String' },
  { name: 'asset_class', type: 'String' },
  { name: 'date', type: 'StrictDate' },
  { name: 'open', type: 'Float', kind: 'measure' },
  { name: 'high', type: 'Float', kind: 'measure' },
  { name: 'low', type: 'Float', kind: 'measure' },
  { name: 'close', type: 'Float', kind: 'measure' },
  { name: 'volume', type: 'Integer', kind: 'measure' },
];

/** The rates sample's columns as the compiler types them. */
export const RATE_SPECS: readonly ColumnSpec[] = [
  { name: 'tenor', type: 'String' },
  { name: 'years', type: 'Float', kind: 'dimension' },
  { name: 'date', type: 'StrictDate' },
  { name: 'yield', type: 'Float', kind: 'measure' },
];

/** A sample's file as a cube records it, built here and hashed, so the page reopens over the same rows rebuilt. */
export async function sampleSource(id: string, columns: readonly ColumnSpec[]): Promise<FileSource> {
  const sample = sampleById(id);
  if (!sample) throw new Error(`no sample '${id}'`);
  const bytes = new TextEncoder().encode(sample.build(sample.defaultRows));
  return fileSource({ name: sampleFileName(sample), size: bytes.length, arrayBuffer: async () => bytes.buffer as ArrayBuffer },
    'csv', columns, { id, rows: sample.defaultRows });
}

/** A table's rows grouped by `rows`, the columns given. */
const grouped = (table: string, columns: readonly ColumnSpec[], rows: readonly string[]): CubeSnapshot => ({
  source: { query: element(table) }, columns, derived: [], rows, pivotOn: [], sorts: [], epoch: 1,
});

/** A cube written as the screen holds one it opens (cube-state.ts openedState): the corpus reopens it the same. */
function cubeOf(name: string, source: CubeDocument['source'], snapshot: CubeSnapshot, configuration: CubeConfiguration): CubeDocument {
  const state = openedState(snapshot, configuration, TreeState.fromPaths([], false));
  return writeCube({ name, source, snapshot: state.snapshot, configuration: state.configuration, tree: state.tree });
}

export const PRICES: TsSource = { alias: 'px', cube: 'prices', date: 'date', key: 'symbol' };
export const RATES: TsSource = { alias: 'rates', cube: 'rates', date: 'date', key: 'tenor' };

/** A series as a person writes it: its id, its expression typed, and the rest of its slots. */
export const s = (id: string, expr: string, rest: Omit<TsSeries, 'id' | 'expr'> = {}): TsSeries => ({ id, expr: parseExpr(expr), ...rest });

/** A timeseries tile, its spec read by the tile's one reader (a slip here is refused at build, not drawn wrong). */
export function tile(id: string, title: string, spec: TimeseriesSpec): PageView {
  const read = readTimeseries(spec);
  if (typeof read === 'string') throw new Error(`${id}: ${read}`);
  return { id, kind: 'timeseries', title, spec: read };
}

/** The 2020 crash and the first hike of 2022, marked on every tile that spans them. */
export const MARKS = [
  { kind: 'period', from: '2020-02-20', to: '2020-03-23', label: 'Covid crash' },
  { kind: 'event', date: '2022-03-16', label: 'First hike' },
] as const;

/** The two grids every timeseries example reads: the prices, grouped by asset class and symbol, and the curve by tenor. */
export const MARKET_GRIDS: readonly PageView[] = [
  { id: 'prices', kind: 'grid', cube: 'prices', title: 'Prices' },
  { id: 'rates', kind: 'grid', cube: 'rates', title: 'Treasury curve' },
];

/** The cubes of MARKET_GRIDS, each over its sample rebuilt on opening. */
export async function marketCubes(): Promise<{ readonly id: string; readonly cube: CubeDocument }[]> {
  const pricesFile = await sampleSource('markets', MARKET_SPECS);
  const ratesFile = await sampleSource('rates', RATE_SPECS);
  // a price and a yield average over a group's days (a sum of prices is no figure); a volume sums
  const average = { aggregateFn: 'average' } as const;
  const prices = cubeOf('Prices', pricesFile, grouped('markets', MARKET_SPECS, ['asset_class', 'symbol']), {
    ...DEFAULT_CONFIGURATION, reportTitle: 'Prices', columns: { open: average, high: average, low: average, close: average },
  });
  const rates = cubeOf('Rates', ratesFile, grouped('rates', RATE_SPECS, ['tenor']),
    { ...DEFAULT_CONFIGURATION, reportTitle: 'Rates', columns: { yield: average } });
  return [{ id: 'prices', cube: prices }, { id: 'rates', cube: rates }];
}

/** The page: four sheets over the prices and the curve, each a timeseries tile or two beside its grid. */
export async function marketsPage(): Promise<PageDocument> {
  const views: PageView[] = [
    ...MARKET_GRIDS,
    // performance: every line rebased to 100 at the first date shown (a zoom rebases again), a pane of realized vol
    tile('performance', 'Performance, rebased to 100', {
      sources: [PRICES],
      series: [
        s('A', 'close[SPX]', { name: 'S&P 500' }), s('B', 'close[NDX]', { name: 'Nasdaq 100' }),
        s('C', 'close[GLD]', { name: 'Gold' }), s('D', 'close[TLT]', { name: 'Long bonds' }),
        s('E', 'volatility($A, 21)', { name: 'S&P 500 21d vol', pane: 1, style: { kind: 'area' } }),
      ],
      range: { last: 'max' },
      view: { compare: 'rebase', stats: true },
      panes: [{ height: 3 }, { height: 1 }],
      annotations: [...MARKS],
    }),
    // risk: how deep, how volatile, how together
    tile('drawdowns', 'Drawdowns from the running peak', {
      sources: [PRICES],
      series: [
        s('A', 'drawdown(close[SPX])', { name: 'S&P 500', style: { kind: 'area' } }),
        s('B', 'drawdown(close[NVDA])', { name: 'Nvidia' }),
        s('C', 'drawdown(close[TLT])', { name: 'Long bonds' }),
      ],
      annotations: [...MARKS],
    }),
    tile('vol-corr', 'Realized volatility, correlation and beta', {
      sources: [PRICES],
      series: [
        s('A', 'volatility(close[SPX], 63)', { name: 'S&P 500 3m vol' }),
        s('B', 'volatility(close[NVDA], 63)', { name: 'Nvidia 3m vol' }),
        s('C', 'correlation(close[AAPL], close[MSFT], 63)', { name: 'Apple ~ Microsoft 3m corr', pane: 1 }),
        s('D', 'correlation(close[SPX], close[TLT], 63)', { name: 'Stocks ~ bonds 3m corr', pane: 1 }),
        s('E', 'beta(close[NVDA], close[SPX], 126)', { name: 'Nvidia 6m beta', pane: 2 }),
      ],
      range: { last: '3y' },
      panes: [{ height: 2 }, { height: 1.5 }, { height: 1 }],
      annotations: [{ kind: 'level', value: 0, pane: 1 }, { kind: 'level', value: 1, pane: 2, label: 'beta 1' }],
    }),
    // technicals: a price with its band and averages, its RSI and its volume
    tile('technicals', 'Apple: candles, Bollinger bands, moving averages, RSI, volume', {
      sources: [PRICES],
      series: [
        s('A', 'ohlc(open[AAPL], high[AAPL], low[AAPL], close[AAPL])', { name: 'Apple' }),
        s('B', 'bollinger_bands(close[AAPL], 20, 2)', { name: '20d ±2σ' }),
        s('C', 'mean(close[AAPL], 50)', { name: '50d average', style: { dash: 'dashed', width: 1 } }),
        s('D', 'mean(close[AAPL], 200)', { name: '200d average', style: { dash: 'dashed', width: 1 } }),
        s('E', 'rsi(close[AAPL], 14)', { name: 'RSI 14', pane: 1 }),
        s('F', 'volume[AAPL].sum', { name: 'Volume', pane: 2, style: { kind: 'bars' } }),
      ],
      range: { last: '6m' },
      panes: [{ height: 3 }, { height: 1 }, { height: 1 }],
      annotations: [{ kind: 'level', value: 70, pane: 1, label: 'overbought' }, { kind: 'level', value: 30, pane: 1, label: 'oversold' }],
    }),
    // rates: the curve's tenors, the 10s2s spread, and -- reading both grids in one query -- a bond fund against its yield
    tile('curve', 'The Treasury curve and the 10s2s spread', {
      sources: [RATES],
      series: [
        s('A', 'yield', { name: 'Yield', split: 'tenor' }),
        s('B', 'yield[10Y] - yield[2Y]', { name: '10s2s', pane: 1, style: { kind: 'area' } }),
      ],
      panes: [{ height: 2 }, { height: 1 }],
      annotations: [{ kind: 'level', value: 0, pane: 1, label: 'inverted below' }, ...MARKS],
    }),
    tile('bonds', 'Long bonds against the 10-year yield (two grids, one query)', {
      sources: [PRICES, RATES],
      series: [
        s('A', 'px:close[TLT]', { name: 'TLT' }),
        s('B', 'rates:yield[10Y]', { name: '10Y yield', axis: 'right' }),
        s('C', 'correlation(px:close[TLT], rates:yield[10Y], 63)', { name: '3m correlation', pane: 1 }),
      ],
      panes: [{ height: 2 }, { height: 1 }],
      annotations: [...MARKS],
    }),
  ];
  const band = (height: number, node: Node): Band => ({ height, node });
  const sheets: PageSheet[] = [
    { id: 'sheet-1', name: 'Performance', layout: { kind: 'bands', fit: true, bands: [
      band(0.62, { tile: 'performance' }), band(0.38, { tile: 'prices' })] } },
    { id: 'sheet-2', name: 'Risk', layout: { kind: 'bands', fit: true, bands: [
      band(0.45, { tile: 'drawdowns' }), band(0.55, { tile: 'vol-corr' })] } },
    { id: 'sheet-3', name: 'Technicals', layout: { kind: 'bands', fit: true, bands: [band(1, { tile: 'technicals' })] } },
    { id: 'sheet-4', name: 'Rates', layout: { kind: 'bands', fit: true, bands: [
      band(0.5, { split: 'row', parts: [{ size: 0.6, node: { tile: 'curve' } }, { size: 0.4, node: { tile: 'rates' } }] }),
      band(0.5, { tile: 'bonds' })] } },
  ];
  return writePageOf({
    name: 'Markets',
    cubes: await marketCubes(),
    views: { views, sheets },
  });
}
