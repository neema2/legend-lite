// THE TRADER SCREENS, AS SAVED PAGES (docs/DEEPHAVEN_TRADER_DEMOS_2026_10_24.md K4, K8-K13; the user, 2026-10-25: "all
// the trader screen demos saved as datacube pages user can open and see from datacube -- can be different tabs or
// different saved pages similar to our chart pages"): each screen a DataCube page -- its grids and charts, a sheet (a
// tab) per view of the desk -- written by DataCube's own writers (writeCube, writePageOf), as the example pages are
// (src/examples/). Every grid's source is a saved query over the one trading model's classes (the stress corpus's),
// read through stress::AllMapping on the market world's runtime; the server computes every value, ticking. The host
// (traders.ts) opens them from its menu, each grid made again over its source as it is now.

import { element } from '../../../../pure-protocol/src/index.ts';
import { CHART_VERSION, DEFAULT_OPTIONS, candleMeasures, type ChartOptions, type ChartSpec } from '../../../src/chart-spec.ts';
import { DEFAULT_CONFIGURATION, type ColumnConfiguration, type CubeConfiguration } from '../../../src/config.ts';
import { writeCube, type CubeDocument, type CubeSource, type QuerySource } from '../../../src/cube-document.ts';
import { openedState } from '../../../src/cube-state.ts';
import type { Node } from '../../../src/layout/bands.ts';
import { writePageOf, type ChartView, type PageDocument, type PageSheet, type PageView } from '../../../src/page-document.ts';
import type { ColumnSpec, CubeSnapshot, FilterNode } from '../../../src/snapshot.ts';
import { TreeState } from '../../../src/tree.ts';
import { MARKET_RUNTIME } from './model.ts';

/** The mapping every screen's source reads the corpus's classes through. */
export const MARKET_MAPPING = 'stress::AllMapping';

/** A source's columns as the compiler types them: the host's planner, asked. */
export type SourceTypes = (text: string) => Promise<readonly ColumnSpec[]>;

export interface TraderPage {
  readonly id: string;
  /** Shown in the host's menu. */
  readonly label: string;
  /** What it shows. */
  readonly about: string;
  /** The page, written: its sources typed by `types`. */
  build(types: SourceTypes): Promise<PageDocument>;
}

// ---------------------------------------------------------------- the sources: queries over the corpus's classes

const QUOTE = 'marketdata::MarketQuote.all()';
const TRADE = 'marketdata::MarketTrade.all()';

/** Every name's quote now: its top of book and its day so far, against its prior close. */
export const WATCHLIST = `${QUOTE}->project(~[ticker:x|$x.instrument.ticker, name:x|$x.instrument.name,`
  + ' kind:x|$x.instrument.assetClass, sector:x|$x.instrument.sector.name, last:x|$x.last, bid:x|$x.bid,'
  + ' ask:x|$x.ask, bidSize:x|$x.bidSize, askSize:x|$x.askSize, open:x|$x.openPrice, high:x|$x.highPrice,'
  + ' low:x|$x.lowPrice, prevClose:x|$x.closePrice, volume:x|$x.volume, at:x|$x.quoteTime])'
  + '->extend(~[chg:x|$x.last->toOne() - $x.prevClose->toOne(),'
  + ' chgPct:x|($x.last->toOne() / $x.prevClose->toOne()) - 1.0, spreadBps:x|(($x.ask - $x.bid) / $x.bid) * 10000.0])';

/** The public tape: every print, its name, time, price and size, and who crossed the spread. */
export const TAPE = `${TRADE}->project(~[seq:x|$x.sequenceNumber, ticker:x|$x.instrument.ticker,`
  + ' kind:x|$x.instrument.assetClass, at:x|$x.printTime, price:x|$x.price, size:x|$x.quantity,'
  + ' side:x|$x.aggressorSide, venue:x|$x.venue, condition:x|$x.tradeCondition])';

/** A name's day from its prints: the price again under each figure the grid takes of it (its VWAP, high, low and last),
 *  and each print's notional. */
export const DAY = `${TAPE}->extend(~[vwap:x|$x.price, high:x|$x.price, low:x|$x.price, close:x|$x.price,`
  + ' notional:x|$x.price * $x.size])';

/** The tape with each name's exponential moving average of its prices, print by print (lite's ema, K6). */
export const TAPE_EMA = `${TAPE}->extend(over(~ticker, ~seq->ascending(), rows(unbounded(), 0)),`
  + ' ~[ema:{p,w,r|$r.price}:y|$y->ema(0.1)])';

// ---------------------------------------------------------------- writing a page

/** The source as a saved page writes it down: the query it is, its text, on the market's runtime, a saved query of the
 *  demo's own project -- and its columns, never its rows: they are the server's. */
export function marketSource(name: string, content: string, columns: readonly ColumnSpec[]): QuerySource {
  return {
    _type: 'savedQuery',
    name,
    query: {
      name, groupId: 'org.finos.legend.lite.demo', artifactId: 'trader-screens', versionId: 'local',
      executionContext: { _type: 'explicitExecutionContext', mapping: MARKET_MAPPING, runtime: MARKET_RUNTIME },
      content,
    },
    columns: columns.map((c) => ({ name: c.name, type: c.type })),
  };
}

/** Whether a saved cube's source is a screen's: a saved query on the market's runtime, its text the source's. */
export function isMarketSource(source: CubeSource): source is QuerySource {
  const context = source._type === 'savedQuery' ? source.query.executionContext : undefined;
  return context?._type === 'explicitExecutionContext' && context.runtime === MARKET_RUNTIME;
}

/** A source's columns as a screen shows them: the compiler's names and types, each with what the screen sets on it
 *  (its kind, its aggregate and that aggregate's second column). A setting for a column the source lacks is refused. */
async function columnsOf(types: SourceTypes, text: string,
  how: Readonly<Record<string, Omit<ColumnSpec, 'name' | 'type'>>> = {}): Promise<ColumnSpec[]> {
  const typed = await types(text);
  const names = new Set(typed.map((c) => c.name));
  for (const name of Object.keys(how)) if (!names.has(name)) throw new Error(`the source has no column '${name}': ${text}`);
  return typed.map((c) => ({ name: c.name, type: c.type, ...how[c.name] }));
}

/** A grid of a source: no grouping unless `over` says so. */
const cubeOver = (columns: readonly ColumnSpec[], over: Partial<CubeSnapshot> = {}): CubeSnapshot => ({
  source: { query: element('market') }, columns, derived: [], rows: [], pivotOn: [], sorts: [], epoch: 1, ...over,
});

/** A cube written as the screen holds one it opens (cube-state.ts openedState); `expandTo`: every group to that level
 *  open. */
function cubeOf(name: string, source: QuerySource, snapshot: CubeSnapshot, configuration: CubeConfiguration,
  expandTo = 0): CubeDocument {
  const state = openedState(snapshot, configuration, TreeState.saved([], false, expandTo, []));
  return writeCube({ name, source, snapshot: state.snapshot, configuration: state.configuration, tree: state.tree });
}

/** A chart of the cube `cube`, its grouping its own (frozen) unless it says otherwise -- a treemap draws its grid's rows
 *  as shown, which frozen it would stop following. */
function chart(id: string, cube: string, title: string,
  spec: Partial<Omit<ChartSpec, 'options'>> & { options?: Partial<ChartOptions> }): ChartView {
  const { options, ...rest } = spec;
  return {
    id, kind: 'chart', cube, title,
    spec: { version: CHART_VERSION, mark: 'bar', y: [], frozen: true, ...rest, options: { ...DEFAULT_OPTIONS, ...options } } as ChartSpec,
  };
}

/** A sheet: its bands, each a share of the screen and what is in it. */
const sheet = (id: string, name: string, bands: readonly (readonly [height: number, node: Node])[]): PageSheet => ({
  id, name, layout: { kind: 'bands', fit: true, bands: bands.map(([height, node]) => ({ height, node })) },
});
/** Tiles side by side, each its share of the width. */
const row = (...parts: readonly (readonly [tile: string, size: number])[]): Node => ({
  split: 'row', parts: parts.map(([tile, size]) => ({ node: { tile }, size })),
});
const tile = (id: string): Node => ({ tile: id });

/** A condition: the column equal to the value. */
const equal = (column: string, value: string): FilterNode => ({ kind: 'condition', column, operator: 'equal', value });

// what a figure looks like on a trader's screen
const PRICE: ColumnConfiguration = { format: { kind: 'number', decimals: 2 } };
const SIZE: ColumnConfiguration = { format: { kind: 'number', decimals: 0 } };
const BIG: ColumnConfiguration = { format: { kind: 'number', decimals: 1, numberScale: 'auto' } };
/** A change: red when it is down. */
const CHANGE: ColumnConfiguration = { format: { kind: 'number', decimals: 2 }, appearance: { negativeForeground: '#d64545', normalForeground: '#1f9d55' } };
const CHANGE_PCT: ColumnConfiguration = { format: { kind: 'percent', decimals: 2 }, appearance: { negativeForeground: '#d64545', normalForeground: '#1f9d55' } };
const HIDDEN: ColumnConfiguration = { hidden: true };
/** A print's time on the tape: to the millisecond. */
const PRINT_TIME: ColumnConfiguration = { format: { kind: 'datetime', code: 'hh:mm:ss.000' } };

const configured = (reportTitle: string, columns: Readonly<Record<string, ColumnConfiguration>>): CubeConfiguration => ({
  ...DEFAULT_CONFIGURATION, reportTitle, columns,
});

// ---------------------------------------------------------------- K4: the ticker

/** The coin the ticker's chart sheet follows. */
export const TICKER_COIN = 'BTC-USD';

async function tickerPage(types: SourceTypes): Promise<PageDocument> {
  const quoteFigures = { aggregate: 'average' } as const;
  const watchColumns = await columnsOf(types, WATCHLIST, {
    last: quoteFigures, bid: quoteFigures, ask: quoteFigures, open: quoteFigures, high: { aggregate: 'max' },
    low: { aggregate: 'min' }, prevClose: quoteFigures, chg: quoteFigures, chgPct: quoteFigures, spreadBps: quoteFigures,
  });
  const tapeColumns = await columnsOf(types, TAPE, { seq: { kind: 'dimension' } });
  const dayColumns = await columnsOf(types, DAY, {
    seq: { aggregate: 'count' }, price: { aggregate: 'average' }, vwap: { aggregate: 'wavg', aggregateWeight: 'size' },
    high: { aggregate: 'max' }, low: { aggregate: 'min' }, close: { aggregate: 'maxBy', aggregateWeight: 'seq' },
  });
  const emaColumns = await columnsOf(types, TAPE_EMA, { seq: { kind: 'dimension' } });

  const watchSource = marketSource('Watchlist', WATCHLIST, watchColumns);
  const watch = cubeOf('Watchlist', watchSource, cubeOver(watchColumns, { sorts: [{ column: 'ticker', direction: 'asc' }] }),
    configured('Watchlist', {
      name: HIDDEN, sector: HIDDEN, at: HIDDEN, last: PRICE, bid: PRICE, ask: PRICE, open: PRICE, high: PRICE, low: PRICE,
      prevClose: PRICE, bidSize: SIZE, askSize: SIZE, volume: BIG, chg: CHANGE, chgPct: CHANGE_PCT,
      spreadBps: { format: { kind: 'number', decimals: 1 } },
    }));
  const tapeSource = marketSource('Time and sales', TAPE, tapeColumns);
  const tape = cubeOf('Time and sales', tapeSource, cubeOver(tapeColumns, { sorts: [{ column: 'seq', direction: 'desc' }] }),
    configured('Time and sales', { seq: HIDDEN, kind: HIDDEN, at: PRINT_TIME, price: PRICE, size: SIZE }));
  const daySource = marketSource('The day by name', DAY, dayColumns);
  const day = cubeOf('The day by name', daySource,
    cubeOver(dayColumns, { rows: ['ticker'], sorts: [{ column: 'notional', direction: 'desc' }] }),
    configured('The day by name: VWAP, range and turnover', {
      seq: { displayName: 'prints', format: { kind: 'number', decimals: 0 } }, price: HIDDEN, at: HIDDEN, side: HIDDEN,
      venue: HIDDEN, condition: HIDDEN, vwap: PRICE, high: PRICE, low: PRICE, close: PRICE, size: BIG, notional: BIG,
    }));
  const coinWatch = cubeOf(`${TICKER_COIN} quote`, watchSource, cubeOver(watchColumns, { filter: equal('ticker', TICKER_COIN) }),
    configured(`${TICKER_COIN} quote`, {
      kind: HIDDEN, sector: HIDDEN, at: HIDDEN, open: HIDDEN, prevClose: HIDDEN, last: PRICE, bid: PRICE, ask: PRICE,
      high: PRICE, low: PRICE, bidSize: { format: { kind: 'number', decimals: 4 } }, askSize: { format: { kind: 'number', decimals: 4 } },
      volume: BIG, chg: CHANGE, chgPct: CHANGE_PCT, spreadBps: { format: { kind: 'number', decimals: 1 } },
    }));
  const coinSource = marketSource('The tape with its EMA', TAPE_EMA, emaColumns);
  const coin = cubeOf(`${TICKER_COIN} prints`, coinSource,
    cubeOver(emaColumns, { filter: equal('ticker', TICKER_COIN), sorts: [{ column: 'seq', direction: 'desc' }] }),
    configured(`${TICKER_COIN} prints`, {
      seq: HIDDEN, kind: HIDDEN, ticker: HIDDEN, at: PRINT_TIME, price: PRICE, ema: PRICE,
      size: { format: { kind: 'number', decimals: 4 } },
    }));

  const byTime = { sort: { by: 'x', direction: 'asc' }, limit: 5000, zoom: true, legend: 'top' } as const;
  const views: PageView[] = [
    { id: 'watchlist', kind: 'grid', cube: 'watch', title: 'Watchlist' },
    { id: 'tape', kind: 'grid', cube: 'tape', title: 'Time and sales' },
    { id: 'day', kind: 'grid', cube: 'day', title: 'The day by name: VWAP, range and turnover' },
    { id: 'coin-quote', kind: 'grid', cube: 'coinWatch', title: `${TICKER_COIN}: the quote` },
    chart('coin-last', 'coinWatch', `${TICKER_COIN}: last`, { mark: 'kpi', y: [{ column: 'last', fn: 'max' }] }),
    chart('coin-change', 'coinWatch', `${TICKER_COIN}: change on the day`, { mark: 'kpi', y: [{ column: 'chgPct', fn: 'max' }] }),
    chart('coin-candles', 'coin', `${TICKER_COIN}: one-minute candles`, {
      mark: 'candlestick', x: 'at', grain: 'minute', y: candleMeasures('price', 'seq', 'size'), options: byTime,
    }),
    chart('coin-ema', 'coin', `${TICKER_COIN}: the last print each second, and its EMA`, {
      mark: 'line', x: 'at', grain: 'second',
      y: [{ column: 'price', fn: 'maxBy', weight: 'seq', title: 'last print' }, { column: 'ema', fn: 'maxBy', weight: 'seq', title: 'EMA (0.1)' }],
      options: { ...byTime, fitAxis: true },
    }),
    { id: 'coin-prints', kind: 'grid', cube: 'coin', title: `${TICKER_COIN}: prints` },
  ];
  const sheets: PageSheet[] = [
    sheet('sheet-1', 'Watchlist', [[0.6, tile('watchlist')], [0.4, row(['tape', 0.45], ['day', 0.55])]]),
    sheet('sheet-2', TICKER_COIN, [
      [0.2, row(['coin-last', 0.2], ['coin-change', 0.2], ['coin-quote', 0.6])],
      [0.5, tile('coin-candles')],
      [0.3, row(['coin-ema', 0.6], ['coin-prints', 0.4])],
    ]),
  ];
  return writePageOf({
    name: 'Ticker',
    cubes: [
      { id: 'watch', cube: watch }, { id: 'tape', cube: tape }, { id: 'day', cube: day },
      { id: 'coinWatch', cube: coinWatch }, { id: 'coin', cube: coin },
    ],
    views: { views, sheets },
  });
}

// ---------------------------------------------------------------- K8: the market map

/** Every equity's quote as a market map reads it: its sector, its value at the last price (shares out by the last), the
 *  day's change against the prior close, its turnover, and whether it is up or down on the day. */
export const MARKET_MAP = `${QUOTE}->filter(x|$x.instrument.assetClass == 'EQUITY')->project(~[sector:x|$x.instrument.sector.name,`
  + ' ticker:x|$x.instrument.ticker, name:x|$x.instrument.name, shares:x|$x.instrument.equity.sharesOutstanding,'
  + ' last:x|$x.last, prevClose:x|$x.closePrice, volume:x|$x.volume])'
  + '->extend(~[cap:x|$x.shares->toOne() * $x.last->toOne(), chgPct:x|($x.last->toOne() / $x.prevClose->toOne()) - 1.0,'
  + ' turnover:x|$x.volume->toOne() * $x.last->toOne()])'
  + '->extend(~[up:x|if($x.chgPct > 0.0, |1, |0), down:x|if($x.chgPct < 0.0, |1, |0)])';

async function marketMapPage(types: SourceTypes): Promise<PageDocument> {
  const columns = await columnsOf(types, MARKET_MAP, {
    shares: { aggregate: 'sum' }, last: { aggregate: 'average' }, prevClose: { aggregate: 'average' },
    cap: { aggregate: 'sum' }, chgPct: { aggregate: 'wavg', aggregateWeight: 'cap' }, turnover: { aggregate: 'sum' },
    up: { aggregate: 'sum' }, down: { aggregate: 'sum' },
  });
  const source = marketSource('The market map', MARKET_MAP, columns);
  // the map's grid: by sector, then name, every sector open -- what the treemap draws, block by block
  const quoteFigures: Readonly<Record<string, ColumnConfiguration>> = {
    shares: HIDDEN, last: HIDDEN, prevClose: HIDDEN, volume: HIDDEN, up: HIDDEN, down: HIDDEN, name: HIDDEN,
    cap: BIG, chgPct: CHANGE_PCT, turnover: BIG,
  };
  const map = cubeOf('The market map', source,
    cubeOver(columns, { rows: ['sector', 'ticker'], sorts: [{ column: 'cap', direction: 'desc' }] }),
    configured('Sectors and names', quoteFigures), 1);
  /** A list of every name, flat, by `column` in `direction`, its top first. */
  const list = (name: string, column: string, direction: 'asc' | 'desc'): CubeDocument => cubeOf(name, source,
    cubeOver(columns, { sorts: [{ column, direction }] }),
    configured(name, { ...quoteFigures, sector: HIDDEN, last: PRICE }));
  const views: PageView[] = [
    chart('map', 'map', 'The market: each name sized by its value, coloured by its change on the day (green up, red down; full at 2%)', {
      mark: 'treemap', frozen: false, colour: { column: 'chgPct', fn: 'value' }, options: { colourReach: 0.02 },
    }),
    { id: 'sectors', kind: 'grid', cube: 'map', title: 'Sectors and names' },
    chart('advancing', 'map', 'Advancing', { mark: 'kpi', y: [{ column: 'up', fn: 'sum' }] }),
    chart('declining', 'map', 'Declining', { mark: 'kpi', y: [{ column: 'down', fn: 'sum' }] }),
    chart('by-sector', 'map', 'Each sector\'s change on the day, weighted by value', {
      x: 'sector', y: [{ column: 'chgPct', fn: 'wavg', weight: 'cap', title: 'change (value-weighted)' }],
      options: { sort: { by: 'y', direction: 'desc' }, labels: true, legend: 'none' },
    }),
    { id: 'gainers', kind: 'grid', cube: 'gainers', title: 'Top gainers' },
    { id: 'losers', kind: 'grid', cube: 'losers', title: 'Top losers' },
    { id: 'active', kind: 'grid', cube: 'active', title: 'Most active, by turnover' },
  ];
  const sheets: PageSheet[] = [
    sheet('sheet-1', 'Map', [[1, row(['map', 0.7], ['sectors', 0.3])]]),
    sheet('sheet-2', 'Movers', [
      [0.3, row(['advancing', 0.18], ['declining', 0.18], ['by-sector', 0.64])],
      [0.7, row(['gainers', 0.34], ['losers', 0.33], ['active', 0.33])],
    ]),
  ];
  return writePageOf({
    name: 'Market map',
    cubes: [
      { id: 'map', cube: map },
      { id: 'gainers', cube: list('Top gainers', 'chgPct', 'desc') },
      { id: 'losers', cube: list('Top losers', 'chgPct', 'asc') },
      { id: 'active', cube: list('Most active', 'turnover', 'desc') },
    ],
    views: { views, sheets },
  });
}

/** The trader screens, in the menu's order. */
export const TRADER_PAGES: readonly TraderPage[] = [
  {
    id: 'ticker',
    label: 'Ticker',
    about: 'A watchlist of every name, its quote ticking against its prior close; the tape, print by print; each name\'s '
      + `day so far (its VWAP, range and turnover); and ${TICKER_COIN}'s quote, one-minute candles and EMA.`,
    build: tickerPage,
  },
  {
    id: 'market-map',
    label: 'Market map',
    about: 'Every equity as a block sized by its value and coloured by its change on the day, by sector; the day\'s '
      + 'breadth, each sector\'s value-weighted change, and the top gainers, losers and most active names.',
    build: marketMapPage,
  },
];
