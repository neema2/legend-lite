// THE MARKETS EXAMPLE OVER THE TRADING MODEL'S CLASSES (docs/DATACUBE_TIMESERIES_DESIGN_2026_10_25.md §11, Phase 6; the
// user, 2026-10-27: "start all the charts from Class models projected to Relation ... the same Stress data model ...
// richer joins to reference data and be able to plot trades"): the markets sample's days as the trading model holds
// them -- a quote a day of each instrument, the bank's trades -- each grid a saved query of the markets project
// (config.json `projects[]`, BUILD.bazel :markets_project), `Class.all()->project(...)` through stress::AllMapping, its
// rows the project's own Data, filled in the tab's database (C5). Each instrument's sector and listing reached through
// the model's associations, so a line is matched or split by them; the trades read beside the prices in one tile.
// test/stress-markets.test.ts opens it the page's way and runs every series of it.

import { derive, fn, element, lambda, projectObjects, property, variable, type ValueSpecification } from '../../../pure-protocol/src/index.ts';
import { DEFAULT_CONFIGURATION, type CubeConfiguration } from '../config.ts';
import { writeCube, type CubeDocument, type QuerySource } from '../cube-document.ts';
import { openedState } from '../cube-state.ts';
import type { Band, Node } from '../layout/bands.ts';
import { writePageOf, type PageDocument, type PageSheet, type PageView } from '../page-document.ts';
import type { ColumnSpec, CubeSnapshot } from '../snapshot.ts';
import type { TsSource } from '../timeseries-spec.ts';
import { TreeState } from '../tree.ts';
import { s, tile } from './markets.ts';

/** The project the grids' saved queries belong to (config.json `projects[]`; the catalog's entry names it too). */
export const MARKETS_PROJECT = { groupId: 'demo', artifactId: 'markets', versionId: '0.0.0' } as const;

/** Each saved query's execution context: the trading model's one mapping and runtime over its store. */
const CONTEXT = { _type: 'explicitExecutionContext', mapping: 'stress::AllMapping', runtime: 'stress::RT' } as const;

/** A saved query's projection: each column a path of properties from the object, as its Pure text spells it. */
interface Projected {
  readonly name: string;
  readonly path: readonly string[];
  readonly type: string;
  readonly kind?: ColumnSpec['kind'];
}

/** A grid's saved query: its class, the columns it projects. */
interface SavedGrid {
  readonly id: string;
  readonly title: string;
  readonly className: string;
  readonly columns: readonly Projected[];
  /** Its text as Query saved it -- what a person typed, each column a path of properties from the object
   *  (stress-markets.test.ts holds it, parsed, to the tree below). */
  readonly text: string;
  /** Its columns grouped by on opening, and each measure's aggregate (a price averages; a volume sums). */
  readonly rows: readonly string[];
  readonly configuration: CubeConfiguration['columns'];
}

/** A quote a day: the instrument's ticker, its sector and its listing's exchange (cross::Instrument_Sector,
 *  cross::Instrument_Exchange), its time, the day's prices and volume. */
export const QUOTES_GRID: SavedGrid = {
  id: 'quotes',
  title: 'Quotes',
  className: 'marketdata::MarketQuote',
  text: '|marketdata::MarketQuote.all()->project(~[ticker: x|$x.instrument.ticker, sector: x|$x.instrument.sector.name, exchange: x|$x.instrument.exchange.name, quoteTime: x|$x.quoteTime, open: x|$x.openPrice, high: x|$x.highPrice, low: x|$x.lowPrice, close: x|$x.closePrice, volume: x|$x.volume])',
  columns: [
    { name: 'ticker', path: ['instrument', 'ticker'], type: 'String' },
    { name: 'sector', path: ['instrument', 'sector', 'name'], type: 'String' },
    { name: 'exchange', path: ['instrument', 'exchange', 'name'], type: 'String' },
    { name: 'quoteTime', path: ['quoteTime'], type: 'DateTime' },
    { name: 'open', path: ['openPrice'], type: 'Float', kind: 'measure' },
    { name: 'high', path: ['highPrice'], type: 'Float', kind: 'measure' },
    { name: 'low', path: ['lowPrice'], type: 'Float', kind: 'measure' },
    { name: 'close', path: ['closePrice'], type: 'Float', kind: 'measure' },
    { name: 'volume', path: ['volume'], type: 'Float', kind: 'measure' },
  ],
  rows: ['sector', 'ticker'],
  configuration: { open: { aggregateFn: 'average' }, high: { aggregateFn: 'average' }, low: { aggregateFn: 'average' }, close: { aggregateFn: 'average' } },
};

/** The bank's trades: the instrument's ticker and sector, the day, the side (its enumeration's name), the size, price
 *  and notional. */
export const TRADES_GRID: SavedGrid = {
  id: 'trades',
  title: 'Trades',
  className: 'trading::Trade',
  text: '|trading::Trade.all()->project(~[ticker: x|$x.instrument.ticker, sector: x|$x.instrument.sector.name, tradeDate: x|$x.tradeDate, side: x|$x.side, quantity: x|$x.quantity, price: x|$x.price, notional: x|$x.notional])',
  columns: [
    { name: 'ticker', path: ['instrument', 'ticker'], type: 'String' },
    { name: 'sector', path: ['instrument', 'sector', 'name'], type: 'String' },
    { name: 'tradeDate', path: ['tradeDate'], type: 'StrictDate' },
    { name: 'side', path: ['side'], type: 'String' },
    { name: 'quantity', path: ['quantity'], type: 'Float', kind: 'measure' },
    { name: 'price', path: ['price'], type: 'Float', kind: 'measure' },
    { name: 'notional', path: ['notional'], type: 'Float', kind: 'measure' },
  ],
  rows: ['sector', 'ticker', 'side'],
  configuration: { price: { aggregateFn: 'average' } },
};

export const SAVED_GRIDS: readonly SavedGrid[] = [QUOTES_GRID, TRADES_GRID];

/** The same query as the protocol tree a cube's snapshot reads (the test holds it to the text parsed). */
export function savedQueryTree(g: SavedGrid): ValueSpecification {
  const path = (names: readonly string[]): ValueSpecification => names.reduce<ValueSpecification>((v, n) => property(v, n), variable('x'));
  return projectObjects(fn('getAll', element(g.className)), g.columns.map((c) => derive(c.name, lambda(['x'], path(c.path))))).node;
}

/** The grid's source: its saved query by what it is (query-store sharedPart), with the columns it answers. */
export function savedQuerySource(g: SavedGrid): QuerySource {
  return {
    _type: 'savedQuery',
    name: g.title,
    query: { name: g.title, ...MARKETS_PROJECT, executionContext: CONTEXT, content: g.text, defaultParameterValues: [] },
    columns: g.columns.map((c) => ({ name: c.name, type: c.type })),
  };
}

/** A grid's cube, as the screen holds one it opens (cube-state.ts openedState). */
function cubeOf(g: SavedGrid): CubeDocument {
  const columns: ColumnSpec[] = g.columns.map((c) => ({ name: c.name, type: c.type, ...(c.kind ? { kind: c.kind } : {}) }));
  const snapshot: CubeSnapshot = { source: { query: savedQueryTree(g) }, columns, derived: [], rows: [...g.rows], pivotOn: [], sorts: [], epoch: 1 };
  const state = openedState(snapshot, { ...DEFAULT_CONFIGURATION, reportTitle: g.title, columns: g.configuration }, TreeState.fromPaths([], false));
  return writeCube({ name: g.title, source: savedQuerySource(g), snapshot: state.snapshot, configuration: state.configuration, tree: state.tree });
}

export const QUOTES: TsSource = { alias: 'px', cube: 'quotes', date: 'quoteTime', key: 'ticker' };
export const TRADES: TsSource = { alias: 'tr', cube: 'trades', date: 'tradeDate', key: 'ticker' };

/** The page: three sheets -- by sector, by listing venue, the trades against the prices -- each tile beside a grid. */
export function marketsClassesPage(): PageDocument {
  const views: PageView[] = [
    { id: 'quotes', kind: 'grid', cube: 'quotes', title: 'Quotes (MarketQuote)' },
    { id: 'trades', kind: 'grid', cube: 'trades', title: 'Trades (Trade)' },
    // by sector: a line per instrument of a sector, its sector named through the instrument's association
    tile('sectors', 'Technology, line by line; energy and financials beside it, rebased to 100', {
      sources: [QUOTES],
      series: [
        s('A', "close[sector = 'Information Technology']", { name: 'Technology', split: 'ticker' }),
        s('B', "close[sector = 'Energy']", { name: 'Energy' }),
        s('C', "close[sector = 'Financials']", { name: 'Financials' }),
        s('D', "sum(volume[sector = 'Information Technology'])", { name: 'Technology volume', pane: 1, style: { kind: 'bars' } }),
      ],
      range: { last: '2y' },
      view: { compare: 'rebase' },
      panes: [{ height: 3 }, { height: 1 }],
    }),
    // by venue: the volume of the instruments each exchange lists, and the Nasdaq's share of it
    tile('venues', 'Volume by listing venue, and the Nasdaq\'s share', {
      sources: [QUOTES],
      series: [
        s('A', "mean(sum(volume), 21)", { name: 'Volume, 21d average', split: 'exchange' }),
        s('B', "mean(sum(volume[exchange = 'Nasdaq']) / sum(volume), 21)", { name: 'Nasdaq share, 21d', pane: 1 }),
      ],
      range: { last: '3y' },
      panes: [{ height: 2 }, { height: 1 }],
    }),
    // the trades: each a mark at its price on its day against the close -- a buy up, a sell down, its quantity beside it
    // (marks from the trades grid's rows); bought and sold by day; a sector's flow
    tile('trades-vs-close', 'Apple: the bank\'s trades against the close (two grids, one tile)', {
      sources: [QUOTES, TRADES],
      series: [
        s('A', "px.close['AAPL']", { name: 'Apple close' }),
        s('B', "sum(tr.quantity['AAPL', side = 'BUY'])", { name: 'Bought', pane: 1, style: { kind: 'bars' } }),
        s('C', "sum(tr.quantity['AAPL', side = 'SELL'])", { name: 'Sold', pane: 1, style: { kind: 'bars' } }),
        s('D', "sum(tr.notional)", { name: 'Notional by sector', split: 'sector', pane: 2, style: { kind: 'bars' } }),
      ],
      // by day: the quotes' times (their close) and the trades' days on one axis
      grain: 'day',
      range: { last: '1y' },
      panes: [{ height: 3 }, { height: 1 }, { height: 1 }],
      annotations: [
        { kind: 'marks', source: 'tr', where: [{ value: 'AAPL' }, { column: 'side', value: 'BUY' }], at: 'price', text: 'quantity', shape: 'up', label: 'Bought' },
        { kind: 'marks', source: 'tr', where: [{ value: 'AAPL' }, { column: 'side', value: 'SELL' }], at: 'price', text: 'quantity', shape: 'down', label: 'Sold' },
      ],
    }),
  ];
  const band = (height: number, node: Node): Band => ({ height, node });
  const sheets: PageSheet[] = [
    { id: 'sheet-1', name: 'By sector', layout: { kind: 'bands', fit: true, bands: [band(0.62, { tile: 'sectors' }), band(0.38, { tile: 'quotes' })] } },
    { id: 'sheet-2', name: 'By venue', layout: { kind: 'bands', fit: true, bands: [band(1, { tile: 'venues' })] } },
    { id: 'sheet-3', name: 'Trades', layout: { kind: 'bands', fit: true, bands: [band(0.62, { tile: 'trades-vs-close' }), band(0.38, { tile: 'trades' })] } },
  ];
  return writePageOf({
    name: 'Markets on the trading model',
    cubes: SAVED_GRIDS.map((g) => ({ id: g.id, cube: cubeOf(g) })),
    views: { views, sheets },
  });
}
