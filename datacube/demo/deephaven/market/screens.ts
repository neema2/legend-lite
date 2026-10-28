// THE TRADER SCREENS, AS SAVED PAGES (docs/DEEPHAVEN_TRADER_DEMOS_2026_10_24.md K4, K8-K13; the user, 2026-10-25: "all
// the trader screen demos saved as datacube pages user can open and see from datacube -- can be different tabs or
// different saved pages similar to our chart pages"): each screen a DataCube page -- its grids and charts, a sheet (a
// tab) per view of the desk -- written by DataCube's own writers (writeCube, writePageOf), as the example pages are
// (src/examples/). Every grid's source is a saved query over the one trading model's classes (the stress corpus's),
// read through stress::AllMapping on an environment of the desk's data space; the database computes every value,
// ticking. Since 2026-10-25 they are the trading desk's tabs (DESK, below: one page, a tab a screen), which DataCube's
// app opens first under Open…'s Examples (desk.ts); the host of their own (traders.ts), which opened each as a page,
// went 2026-10-26.

import { element } from '../../../../pure-protocol/src/index.ts';
import { CHART_VERSION, DEFAULT_OPTIONS, candleMeasures, type ChartOptions, type ChartSpec } from '../../../src/chart-spec.ts';
import { DEFAULT_CONFIGURATION, type ColumnConfiguration, type CubeConfiguration } from '../../../src/config.ts';
import { writeCube, type CubeDocument, type QuerySource } from '../../../src/cube-document.ts';
import { openedState } from '../../../src/cube-state.ts';
import type { Node } from '../../../src/layout/bands.ts';
import {
  layoutTiles, writePageOf, type BandsLayout, type ChartView, type PageDocument, type PageLayout, type PageSheet, type PageView,
} from '../../../src/page-document.ts';
import type { ColumnSpec, CubeSnapshot, FilterNode } from '../../../src/snapshot.ts';
import { TreeState } from '../../../src/tree.ts';
import { DESK_SPACE } from './model.ts';

/** A source's columns as the compiler types them: the host's planner, asked. */
export type SourceTypes = (text: string) => Promise<readonly ColumnSpec[]>;

/** The values of a query's first column, as the market has them now: what a page starts on -- a screen's first symbol,
 *  a watchlist's names -- each a default a person changes on the page. */
export type MarketValues = (text: string) => Promise<readonly string[]>;

export interface TraderPage {
  readonly id: string;
  /** Shown in the host's menu. */
  readonly label: string;
  /** What it shows. */
  readonly about: string;
  /** The page, written: its sources typed by `types`, what it starts on asked of the market by `values`. */
  build(types: SourceTypes, values: MarketValues): Promise<PageDocument>;
}

/** The first of a query's values: a page's starting name. */
async function firstOf(values: MarketValues, text: string): Promise<string> {
  const [v] = await values(text);
  if (v === undefined) throw new Error(`the market answers nothing for ${text}`);
  return v;
}

// ---------------------------------------------------------------- the sources: the desk's model's functions

// WHAT EACH SCREEN SHOWS IS THE MODEL'S (market.pure, the desk's project's own file; the user, 2026-10-27: "screens as
// model"): every source below names one of its functions, which the compiler inlines where it is called -- a screen's
// plan is its body's. What a figure MEANS (a desk's day P&L, a limit used, a markout, a block) is there, in Pure, typed
// by its declared columns; here is only which function each grid reads, and its arguments.

/** Every name's quote now: its top of book and its day so far, against its prior close. */
export const WATCHLIST = 'market::screens::watchlist()';

/** The public tape: every print, its name, time, price and size, and who crossed the spread. */
export const TAPE = 'market::screens::tape()';

/** A name's day from its prints: the price again under each figure a grid takes of it, and each print's notional. */
export const DAY = 'market::screens::day()';

/** The weight an exponential moving average gives each new print's price, as the stock's chart says it. */
const EMA_ALPHA = '0.1';

/** THE STOCK (K14): every print with its name's prior close and its moving average: one grid, filtered to the name
 *  picked, whose views are the stock's every tile. */
export const STOCK = `market::screens::stock(${EMA_ALPHA})`;

/** The market's largest name: the stock a screen first shows. */
export const LARGEST = 'market::screens::largestNames(1)';

/** THE NEWSWIRE (K15): every story, with the name it is about and its sector. */
export const NEWS = 'market::screens::news()';

/** A watchlist's names to start with: the market's five largest. The watchlist is the person's -- its pane's filter,
 *  saved with the page and changed there -- so these are a default, read when the page is written. */
export const WATCHLIST_START = 'market::screens::largestNames(5)';

// ---------------------------------------------------------------- writing a page

/** The project every screen's saved query belongs to: the one trading model, the desk's own and its environments
 *  (desk.ts). */
export const MARKET_PROJECT = { groupId: 'org.finos.legend.lite.demo', artifactId: 'trader-screens', versionId: 'local' } as const;

/** The source as a saved page writes it down: the query it is, its text, over the desk's data space (on the context the
 *  page runs on: src/run-on.ts, the data space's default until one is written), a saved query of the demo's own project
 *  -- and its columns, never its rows: they are the database's. */
export function marketSource(name: string, content: string, columns: readonly ColumnSpec[]): QuerySource {
  return {
    _type: 'savedQuery',
    name,
    query: {
      name, ...MARKET_PROJECT,
      executionContext: { _type: 'dataSpaceExecutionContext', dataSpacePath: DESK_SPACE },
      content,
    },
    columns: columns.map((c) => ({ name: c.name, type: c.type })),
  };
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
/** A screen's sheet's bands: every screen is a board (a flow sheet is no screen's). */
const boardOf = (layout: PageLayout): BandsLayout => {
  if (layout.kind !== 'bands') throw new Error('a screen\'s sheet is a board of bands, not a flow');
  return layout;
};
/** Tiles side by side, each its share of the width. */
const row = (...parts: readonly (readonly [tile: string, size: number])[]): Node => ({
  split: 'row', parts: parts.map(([tile, size]) => ({ node: { tile }, size })),
});
const tile = (id: string): Node => ({ tile: id });
/** Places side by side (`across`) or one above another (`down`), each its share; a string is a tile. */
const split = (how: 'row' | 'column') => (...parts: readonly (readonly [node: Node | string, size: number])[]): Node => ({
  split: how, parts: parts.map(([node, size]) => ({ node: typeof node === 'string' ? { tile: node } : node, size })),
});
const across = split('row');
const down = split('column');
/** Tiles in one place, the first in front. */
const stacked = (...ids: string[]): Node => ({ stack: ids });

/** A screen before it is a page: its cubes, its views and its sheets (a page of its own, or the desk's tabs). */
interface Parts {
  readonly name: string;
  readonly cubes: readonly { readonly id: string; readonly cube: CubeDocument }[];
  readonly views: readonly PageView[];
  readonly sheets: readonly PageSheet[];
}

/** A screen's page: its parts, written as a saved page writes them. */
const pageOf = <A extends unknown[]>(parts: (...args: A) => Promise<Parts>) => async (...args: A): Promise<PageDocument> => {
  const { name, cubes, views, sheets } = await parts(...args);
  return writePageOf({ name, cubes, views: { views, sheets } });
};

/**
 * A TAB OF THE DESK: a screen's views laid out on one sheet named `name` -- the views its bands place, the cubes they
 * read, a link kept to the views on the tab -- every id under `prefix`, so the desk's screens share one page, each with
 * its own ids.
 */
function tabOf(prefix: string, parts: Parts, name: string, bands: readonly (readonly [height: number, node: Node])[]): Parts {
  const laid = sheet('sheet', name, bands);
  const shown = new Set(layoutTiles(laid.layout));
  for (const id of shown) if (!parts.views.some((v) => v.id === id)) throw new Error(`${parts.name} has no view '${id}' for the desk's ${name}`);
  const at = (id: string): string => `${prefix}-${id}`;
  const node = (n: Node): Node => ('tile' in n ? { tile: at(n.tile) }
    : 'stack' in n ? { stack: n.stack.map(at), ...(n.front !== undefined ? { front: at(n.front) } : {}) }
      : { split: n.split, parts: n.parts.map((p) => ({ node: node(p.node), size: p.size })) });
  const views = parts.views.filter((v) => shown.has(v.id)).map((v): PageView => {
    if (v.kind === 'text') return { ...v, id: at(v.id) };
    if (v.kind !== 'grid' && v.kind !== 'chart') throw new Error(`${parts.name}'s ${v.id} is a ${v.kind}, which no screen has`);
    const to = v.link?.to.filter((t) => shown.has(t)).map(at) ?? [];
    const { link: _link, ...rest } = v;
    return { ...rest, id: at(v.id), cube: at(v.cube), ...(v.link && to.length > 0 ? { link: { column: v.link.column, to } } : {}) };
  });
  const read = new Set(views.flatMap((v) => (v.cube === undefined ? [] : [v.cube])));
  return {
    name,
    cubes: parts.cubes.map((c) => ({ id: at(c.id), cube: c.cube })).filter((c) => read.has(c.id)),
    views,
    sheets: [{ ...laid, id: at(laid.id), layout: { ...boardOf(laid.layout), bands: boardOf(laid.layout).bands.map((b) => ({ ...b, node: node(b.node) })) } }],
  };
}

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

/** A grid as a trading screen shows it: its title, its columns' formats, and its drag zones folded -- every row of a
 *  tile is the market's (the zones' chevron brings them back, a step of the grid's own, kept with the page). */
const configured = (reportTitle: string, columns: Readonly<Record<string, ColumnConfiguration>>): CubeConfiguration => ({
  ...DEFAULT_CONFIGURATION, reportTitle, showDragZones: false, columns,
});

/** A story's time on the wire: to the second. */
const STORY_TIME: ColumnConfiguration = { format: { kind: 'datetime', code: 'hh:mm:ss' } };
/** The newswire as a grid shows it: its time, weight and category, its name, its headline, its source; the rest hidden. */
const WIRE_COLUMNS: Readonly<Record<string, ColumnConfiguration>> = {
  id: HIDDEN, body: HIDDEN, region: HIDDEN, at: STORY_TIME, headline: { widthMode: 'fixed', width: 460 },
  tone: { format: { kind: 'number', decimals: 2 }, appearance: { negativeForeground: '#d64545', normalForeground: '#1f9d55' } },
};
/** A news feed's cards (K15): each slot a column of the newswire (NEWS). */
const NEWS_FEED = {
  headline: 'headline', time: 'at', body: 'body', source: 'source', tags: ['category'], weight: 'weight',
  tickers: 'tickers', tone: 'tone', key: 'id',
} as const;
/** A grid of the newswire, the newest first, filtered by `filter`. */
const wireOf = (name: string, title: string, source: QuerySource, columns: readonly ColumnSpec[], filter?: FilterNode): CubeDocument =>
  cubeOf(name, source, cubeOver(columns, { ...(filter ? { filter } : {}), sorts: [{ column: 'at', direction: 'desc' }, { column: 'id', direction: 'desc' }] }),
    configured(title, WIRE_COLUMNS));
/** A feed of a grid of the newswire: its latest `limit` stories as cards, each linked -- a card or a ticker on it -- to
 *  the grids `to`, by the name it is about. */
const feedOf = (id: string, cube: string, title: string, to: readonly string[], limit = 60): ChartView => ({
  ...chart(id, cube, title, { mark: 'feed', feed: NEWS_FEED, options: { limit } }),
  link: { column: 'ticker', to },
});

// ---------------------------------------------------------------- K4, K14: the ticker

async function tickerParts(types: SourceTypes, values: MarketValues): Promise<Parts> {
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
  const stockColumns = await columnsOf(types, STOCK, { seq: { kind: 'dimension' } });
  const newsColumns = await columnsOf(types, NEWS);
  // the stock it opens on: the market's largest, as the market says now
  const symbol = await firstOf(values, LARGEST);

  // THE STOCK SHEET: the watchlist down the left; picked in it, a name's tiles beside it -- its figures, its chart (a
  // line or candles), its prints and its quote -- every one a view of two grids the list links to (K14)
  const watchSource = marketSource('Watchlist', WATCHLIST, watchColumns);
  const pick = cubeOf('Pick a name', watchSource, cubeOver(watchColumns, { sorts: [{ column: 'ticker', direction: 'asc' }] }),
    configured('Watchlist: pick a name', {
      name: HIDDEN, kind: HIDDEN, sector: HIDDEN, at: HIDDEN, bid: HIDDEN, ask: HIDDEN, bidSize: HIDDEN, askSize: HIDDEN,
      open: HIDDEN, high: HIDDEN, low: HIDDEN, prevClose: HIDDEN, chg: HIDDEN, spreadBps: HIDDEN, volume: HIDDEN,
      last: PRICE, chgPct: { ...CHANGE_PCT, displayName: 'chg' },
    }));
  const quote = cubeOf('The quote', watchSource, cubeOver(watchColumns, { filter: equal('ticker', symbol) }),
    configured('The quote', {
      kind: HIDDEN, sector: HIDDEN, at: HIDDEN, last: HIDDEN, chg: HIDDEN, chgPct: HIDDEN, bid: PRICE, ask: PRICE,
      open: PRICE, high: PRICE, low: PRICE, prevClose: PRICE, bidSize: SIZE, askSize: SIZE, volume: BIG,
      spreadBps: { format: { kind: 'number', decimals: 1 } },
    }));
  const stockSource = marketSource('The stock', STOCK, stockColumns);
  const stock = cubeOf('The stock', stockSource,
    cubeOver(stockColumns, { filter: equal('ticker', symbol), sorts: [{ column: 'seq', direction: 'desc' }] }),
    configured('Its prints', {
      seq: HIDDEN, ticker: HIDDEN, prevClose: HIDDEN, ema: HIDDEN, at: PRINT_TIME, price: PRICE, size: SIZE,
    }));

  // its news (K15): the stories about it, the newest first, a feed of them in front
  const stockNews = wireOf('Its news', 'Its news', marketSource('The newswire', NEWS, newsColumns), newsColumns,
    equal('ticker', symbol));

  // THE WATCHLIST SHEET: every name's quote, the tape, each name's day
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

  const LAST = { column: 'price', fn: 'maxBy', weight: 'seq', title: 'last' } as const;
  const PRIOR = { column: 'prevClose', fn: 'max' } as const;
  const byTime = { sort: { by: 'x', direction: 'asc' }, zoom: true } as const;
  const views: PageView[] = [
    { id: 'pick', kind: 'grid', cube: 'pick', title: 'Watchlist: pick a name', link: { column: 'ticker', to: ['prints', 'quote', 'newsTable'] } },
    chart('last', 'stock', 'Last', { mark: 'kpi', y: [LAST], layers: [{ kind: 'line', of: PRIOR, label: 'prior close' }] }),
    chart('vwap', 'stock', 'VWAP', { mark: 'kpi', y: [{ column: 'price', fn: 'wavg', weight: 'size' }] }),
    chart('high', 'stock', 'Day high', { mark: 'kpi', y: [{ column: 'price', fn: 'max' }] }),
    chart('low', 'stock', 'Day low', { mark: 'kpi', y: [{ column: 'price', fn: 'min' }] }),
    chart('volume', 'stock', 'Volume', { mark: 'kpi', y: [{ column: 'size', fn: 'sum' }] }),
    chart('price', 'stock', 'Today: the last price each second against the prior close, its EMA, the volume beneath', {
      // the whole day by the minute; a span picked by the second (a day by the second would be tens of thousands of marks)
      mark: 'area', x: 'at', grain: 'minute',
      // the EMA on the price's own axis (a column of no format of its own would be given a second)
      y: [LAST, { column: 'ema', fn: 'maxBy', weight: 'seq', title: 'EMA (0.1)', mark: 'line', axis: 'left' },
        { column: 'size', fn: 'sum', title: 'volume', pane: 'below' }],
      layers: [{ kind: 'line', of: PRIOR, label: 'prior close' }, { kind: 'threshold', of: PRIOR }, { kind: 'last' }],
      // its spans for a window, no slider: a slider's shadow of the whole day is drawn again at every tick
      options: {
        ...byTime, zoom: false, limit: 40_000, legend: 'top', fitAxis: true, span: 0,
        spans: [{ seconds: 60, grain: 'second' }, { seconds: 300, grain: 'second' }, { seconds: 900, grain: 'second' },
          { seconds: 3600, grain: 'second' }, { seconds: 0, grain: 'minute' }],
      },
    }),
    chart('candles', 'stock', 'Today: one-minute candles, the volume beneath', {
      mark: 'candlestick', x: 'at', grain: 'minute', y: candleMeasures('price', 'seq', 'size'), options: { ...byTime, limit: 1000 },
    }),
    { id: 'prints', kind: 'grid', cube: 'stock', title: 'Its prints, newest first' },
    { id: 'quote', kind: 'grid', cube: 'quote', title: 'Its quote' },
    // its news: a feed in front, its grid behind
    { ...chart('news', 'stockNews', 'Its news', { mark: 'feed', feed: NEWS_FEED, options: { limit: 40 } }) },
    { id: 'newsTable', kind: 'grid', cube: 'stockNews', title: 'Its news, as a grid' },
    { id: 'watchlist', kind: 'grid', cube: 'watch', title: 'Watchlist' },
    { id: 'tape', kind: 'grid', cube: 'tape', title: 'Time and sales' },
    { id: 'day', kind: 'grid', cube: 'day', title: 'The day by name: VWAP, range and turnover' },
  ];
  const column = (...parts: readonly (readonly [node: Node, size: number])[]): Node => ({
    split: 'column', parts: parts.map(([node, size]) => ({ node, size })),
  });
  const sheets: PageSheet[] = [
    sheet('sheet-1', 'Stock', [[1, {
      split: 'row', parts: [
        { node: tile('pick'), size: 0.2 },
        { node: column(
          [row(['last', 0.24], ['vwap', 0.19], ['high', 0.19], ['low', 0.19], ['volume', 0.19]), 0.16],
          [{ split: 'row', parts: [{ node: { stack: ['price', 'candles'] }, size: 0.7 }, { node: { stack: ['news', 'newsTable'] }, size: 0.3 }] }, 0.56],
          [row(['prints', 0.45], ['quote', 0.55]), 0.28],
        ), size: 0.8 },
      ],
    }]]),
    sheet('sheet-2', 'Watchlist', [[0.6, tile('watchlist')], [0.4, row(['tape', 0.45], ['day', 0.55])]]),
  ];
  return { name: 'Ticker', cubes: [
      { id: 'pick', cube: pick }, { id: 'stock', cube: stock }, { id: 'quote', cube: quote },
      { id: 'stockNews', cube: stockNews },
      { id: 'watch', cube: watch }, { id: 'tape', cube: tape }, { id: 'day', cube: day },
    ], views, sheets };
}

// ---------------------------------------------------------------- K8: the market map

/** Every equity's quote as a market map reads it: its value now, its change on the day, its turnover, up or down. */
export const MARKET_MAP = 'market::screens::marketMap()';

async function marketMapParts(types: SourceTypes): Promise<Parts> {
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
  return { name: 'Market map', cubes: [
      { id: 'map', cube: map },
      { id: 'gainers', cube: list('Top gainers', 'chgPct', 'desc') },
      { id: 'losers', cube: list('Top losers', 'chgPct', 'asc') },
      { id: 'active', cube: list('Most active', 'turnover', 'desc') },
    ], views, sheets };
}

// ---------------------------------------------------------------- K9: real-time P&L with limits

/** Every cash position marked to its live mid: its day's P&L, its value, its gross and its P&L against its cost. */
export const POSITIONS_LIVE = 'market::screens::positionsLive()';

/** Each desk's day against its limits, a warning from four fifths of its loss limit. */
export const DESK_LIMITS = 'market::screens::deskLimits(0.8)';

const MONEY: ColumnConfiguration = { format: { kind: 'number', decimals: 0, negativeParens: true }, appearance: { negativeForeground: '#d64545' } };
const SHARE: ColumnConfiguration = { format: { kind: 'percent', decimals: 1 } };

async function pnlParts(types: SourceTypes): Promise<Parts> {
  const positions = await columnsOf(types, POSITIONS_LIVE, {
    qty: { aggregate: 'sum' }, cost: { aggregate: 'average' }, mid: { aggregate: 'average' }, close: { aggregate: 'average' },
    mv: { aggregate: 'sum' }, dayPnl: { aggregate: 'sum' }, gross: { aggregate: 'sum' }, sinceCost: { aggregate: 'sum' },
  });
  const limits = await columnsOf(types, DESK_LIMITS, {
    breach: { aggregate: 'sum' }, warning: { aggregate: 'sum' }, pnl: { aggregate: 'sum' }, gross: { aggregate: 'sum' },
  });
  const book = cubeOf('P&L', marketSource('Positions, live', POSITIONS_LIVE, positions),
    cubeOver(positions, { rows: ['desk', 'book'], sorts: [{ column: 'dayPnl', direction: 'asc' }] }),
    configured('Day P&L by desk, book and name', {
      // a desk's or a book's quantity and price mean nothing summed or averaged over its names: the names' own, when opened
      inst: HIDDEN, qinst: HIDDEN, sector: HIDDEN, cost: HIDDEN, close: HIDDEN, mid: HIDDEN, qty: HIDDEN,
      dayPnl: { ...MONEY, displayName: 'day P&L' }, mv: { ...MONEY, displayName: 'value' }, gross: MONEY,
      sinceCost: { ...MONEY, displayName: 'P&L since cost' },
    }), 1);
  const desks = cubeOf('Limits', marketSource('Desks against their limits', DESK_LIMITS, limits),
    cubeOver(limits, { sorts: [{ column: 'lossUsed', direction: 'desc' }] }),
    configured('Each desk against its limits', {
      ldesk: HIDDEN, gdesk: HIDDEN, breach: HIDDEN, warning: HIDDEN, headroom: { ...SHARE, hidden: true },
      pnl: { ...MONEY, displayName: 'day P&L' }, lossLimit: { ...MONEY, displayName: 'loss limit' },
      lossUsed: { ...SHARE, displayName: 'loss limit used', heatmap: { from: '#e8f5ec', mid: '#ffe08a', to: '#f4a3a3', range: { min: 0, max: 1 } } },
      gross: MONEY, grossLimit: { ...MONEY, displayName: 'gross limit' },
      grossUsed: { ...SHARE, displayName: 'gross limit used', heatmap: { from: '#e8f5ec', mid: '#ffe08a', to: '#f4a3a3', range: { min: 0, max: 1 } } },
    }));
  const views: PageView[] = [
    chart('firm-pnl', 'book', 'Firm day P&L', { mark: 'kpi', y: [{ column: 'dayPnl', fn: 'sum' }], options: { target: 0, better: 'higher' } }),
    chart('breaches', 'desks', 'Desks past their loss limit', { mark: 'kpi', y: [{ column: 'breach', fn: 'sum' }], options: { target: 0, better: 'lower' } }),
    chart('warnings', 'desks', 'Desks past four fifths of it', { mark: 'kpi', y: [{ column: 'warning', fn: 'sum' }], options: { target: 0, better: 'lower' } }),
    chart('by-desk', 'book', 'Day P&L by desk', {
      x: 'desk', y: [{ column: 'dayPnl', fn: 'sum', title: 'day P&L' }],
      options: { sort: { by: 'y', direction: 'desc' }, labels: true, legend: 'none' },
    }),
    chart('by-sector', 'book', 'Day P&L by sector', {
      x: 'sector', y: [{ column: 'dayPnl', fn: 'sum', title: 'day P&L' }],
      options: { sort: { by: 'y', direction: 'desc' }, legend: 'none', orientation: 'horizontal' },
    }),
    { id: 'pnl', kind: 'grid', cube: 'book', title: 'Day P&L by desk, book and name' },
    chart('headroom', 'desks', 'Loss limit left by desk (red: under a fifth left)', {
      x: 'desk', y: [{ column: 'headroom', fn: 'sum', title: 'loss limit left' }],
      layers: [{ kind: 'threshold', value: 0.2, label: 'a fifth left' }, { kind: 'line', value: 0, label: 'the limit' }],
      options: { sort: { by: 'y', direction: 'asc' }, labels: true, legend: 'none' },
    }),
    { id: 'limits', kind: 'grid', cube: 'desks', title: 'Each desk against its limits' },
  ];
  const sheets: PageSheet[] = [
    sheet('sheet-1', 'P&L', [
      [0.28, row(['firm-pnl', 0.22], ['by-desk', 0.4], ['by-sector', 0.38])],
      [0.72, tile('pnl')],
    ]),
    sheet('sheet-2', 'Limits', [
      [0.3, row(['breaches', 0.2], ['warnings', 0.2], ['headroom', 0.6])],
      [0.7, tile('limits')],
    ]),
  ];
  return { name: 'P&L and limits', cubes: [{ id: 'book', cube: book }, { id: 'desks', cube: desks }], views, sheets };
}

// ---------------------------------------------------------------- K10: the execution blotter

/** Every fill against its order's arrival price: its slippage in basis points, positive a cost. */
export const FILLS = 'market::screens::fills()';

/** Every parent order with its fills so far: how much is done, its average price and its slippage. */
export const ORDERS = 'market::screens::orders()';

const BPS: ColumnConfiguration = { format: { kind: 'number', decimals: 1 }, appearance: { negativeForeground: '#1f9d55' } };

async function blotterParts(types: SourceTypes): Promise<Parts> {
  const fills = await columnsOf(types, FILLS, {
    seq: { kind: 'dimension' }, qty: { aggregate: 'sum' }, px: { aggregate: 'wavg', aggregateWeight: 'qty' },
    arrival: { aggregate: 'average' }, commission: { aggregate: 'sum' }, notional: { aggregate: 'sum' },
    slipBps: { aggregate: 'wavg', aggregateWeight: 'notional' },
  });
  const orders = await columnsOf(types, ORDERS, {
    qty: { aggregate: 'sum' }, filled: { aggregate: 'sum' }, fills: { aggregate: 'sum' }, fillNotional: { aggregate: 'sum' },
    open: { aggregate: 'sum' }, pctFilled: { aggregate: 'average' }, slipBps: { aggregate: 'average' },
  });
  const ordersSource = marketSource('Orders', ORDERS, orders);
  const fillsSource = marketSource('Fills', FILLS, fills);
  const orderGrid = cubeOf('Orders', ordersSource, cubeOver(orders, { sorts: [{ column: 'created', direction: 'desc' }] }),
    configured('Orders', {
      eo: HIDDEN, fillNotional: HIDDEN, arrival: PRICE, open: HIDDEN, desk: HIDDEN, book: HIDDEN, tif: HIDDEN, limit: PRICE,
      created: PRINT_TIME, qty: SIZE, filled: SIZE, fills: { format: { kind: 'number', decimals: 0 } },
      pctFilled: { ...SHARE, displayName: '% filled', heatmap: { from: '#ffffff', to: '#9fd8b2', range: { min: 0, max: 1 } } },
      avgPx: { ...PRICE, displayName: 'average price' }, slipBps: { ...BPS, displayName: 'slippage (bp)' },
    }));
  const fillGrid = cubeOf('Fills', fillsSource, cubeOver(fills, { sorts: [{ column: 'seq', direction: 'desc' }] }),
    configured('Fills, newest first', {
      exec: HIDDEN, seq: HIDDEN, order: HIDDEN, arrival: HIDDEN, commission: HIDDEN, notional: MONEY, at: PRINT_TIME,
      qty: SIZE, px: PRICE, slipBps: { ...BPS, displayName: 'slippage (bp)' },
    }));
  // grouped, a fill's sequence counts its fills
  const counted = fills.map((c) => (c.name === 'seq' ? { name: c.name, type: c.type, aggregate: 'count' as const } : c));
  const venues = cubeOf('By strategy and venue', fillsSource,
    cubeOver(counted, { rows: ['strategy', 'venue'], sorts: [{ column: 'notional', direction: 'desc' }] }),
    configured('Fills by strategy and venue', {
      exec: HIDDEN, at: HIDDEN, order: HIDDEN, ticker: HIDDEN, side: HIDDEN, broker: HIDDEN, arrival: HIDDEN, px: HIDDEN,
      seq: { displayName: 'fills', format: { kind: 'number', decimals: 0 } }, qty: SIZE, notional: MONEY, commission: MONEY,
      slipBps: { ...BPS, displayName: 'slippage (bp, notional-weighted)' },
    }));
  const views: PageView[] = [
    chart('working', 'orders', 'Orders working', { mark: 'kpi', y: [{ column: 'open', fn: 'sum' }] }),
    chart('filled', 'fills', 'Notional filled', { mark: 'kpi', y: [{ column: 'notional', fn: 'sum' }] }),
    chart('slippage', 'fills', 'Slippage against arrival (bp, notional-weighted; lower is better)', {
      mark: 'kpi', y: [{ column: 'slipBps', fn: 'wavg', weight: 'notional' }], options: { target: 0, better: 'lower' },
    }),
    { id: 'orders', kind: 'grid', cube: 'orders', title: 'Orders' },
    { id: 'fills', kind: 'grid', cube: 'fills', title: 'Fills, newest first' },
    chart('by-strategy', 'fills', 'Slippage by algorithm (bp, notional-weighted)', {
      x: 'strategy', y: [{ column: 'slipBps', fn: 'wavg', weight: 'notional', title: 'slippage (bp)' }],
      layers: [{ kind: 'line', value: 0, label: 'arrival' }],
      options: { sort: { by: 'y', direction: 'asc' }, labels: true, legend: 'none' },
    }),
    chart('by-venue', 'fills', 'Notional by venue', {
      mark: 'pie', x: 'venue', y: [{ column: 'notional', fn: 'sum', title: 'notional' }], options: { sort: { by: 'y', direction: 'desc' } },
    }),
    { id: 'venues', kind: 'grid', cube: 'venues', title: 'Fills by strategy and venue' },
  ];
  const sheets: PageSheet[] = [
    sheet('sheet-1', 'Blotter', [
      [0.2, row(['working', 0.25], ['filled', 0.25], ['slippage', 0.5])],
      [0.45, tile('orders')],
      [0.35, tile('fills')],
    ]),
    sheet('sheet-2', 'Algorithms and venues', [
      [0.45, row(['by-strategy', 0.55], ['by-venue', 0.45])],
      [0.55, tile('venues')],
    ]),
  ];
  return { name: 'Execution blotter', cubes: [{ id: 'orders', cube: orderGrid }, { id: 'fills', cube: fillGrid }, { id: 'venues', cube: venues }], views, sheets };
}

// ---------------------------------------------------------------- K11: the options desk

/** The rate the options are priced at. */
const OPTION_RATE = '0.05';

/** The listed chain, live: each option priced, and its greeks, by Black and Scholes from its underlying's live mid. */
export const OPTION_CHAIN = `market::screens::optionChain(${OPTION_RATE})`;

/** The derivatives books' option positions with the chain's live price and greeks. */
export const OPTION_BOOK = `market::screens::optionBook(${OPTION_RATE})`;

/** The columns a chain's or a book's grid leaves out: its keys, its inputs. */
const OPTION_INPUTS: Readonly<Record<string, ColumnConfiguration>> = {
  uinst: HIDDEN, inst: HIDDEN, qinst: HIDDEN, option: HIDDEN, at: HIDDEN, t: HIDDEN, call: HIDDEN, vol: HIDDEN, size: HIDDEN,
  moneyness: HIDDEN, settle: HIDDEN, pinst: HIDDEN,
};

async function optionsParts(types: SourceTypes): Promise<Parts> {
  const average = { aggregate: 'average' } as const;
  const chain = await columnsOf(types, OPTION_CHAIN, {
    strike: { kind: 'dimension' }, size: { kind: 'dimension' }, iv: average, settle: average, spot: average, t: average,
    vol: average, price: average, delta: average, gamma: average, vega: average, theta: average, chg: average, moneyness: average,
  });
  const book = await columnsOf(types, OPTION_BOOK, {
    strike: { kind: 'dimension' }, size: { kind: 'dimension' }, iv: average, settle: average, spot: average, t: average,
    vol: average, price: average, delta: average, gamma: average, vega: average, theta: average, chg: average, moneyness: average,
    contracts: { aggregate: 'sum' }, units: { aggregate: 'sum' }, posDelta: { aggregate: 'sum' }, posGamma: { aggregate: 'sum' },
    posVega: { aggregate: 'sum' }, posTheta: { aggregate: 'sum' }, mv: { aggregate: 'sum' }, dayPnl: { aggregate: 'sum' },
    dollarDelta: { aggregate: 'sum' },
  });
  // the chain as a list, each option a row, by underlying, expiry and strike: one query read by a window (a pivot of calls
  // against puts by strike is a tree's level queries, each re-read as the four busiest names re-quote: the trader screens
  // measured it at 850 ms a tick on one thread)
  const chainGrid = cubeOf('The chain', marketSource('The option chain, live', OPTION_CHAIN, chain),
    cubeOver(chain, { sorts: [{ column: 'underlying', direction: 'asc' }, { column: 'expiry', direction: 'asc' },
      { column: 'strike', direction: 'asc' }, { column: 'type', direction: 'asc' }] }),
    configured('The chain', {
      ...OPTION_INPUTS, spot: PRICE, strike: PRICE, price: PRICE, delta: { format: { kind: 'number', decimals: 3 } },
      gamma: { format: { kind: 'number', decimals: 4 } }, vega: PRICE, theta: PRICE, iv: { ...SHARE, displayName: 'vol' },
      chg: { ...CHANGE, displayName: 'change' },
    }));
  const GREEK = { format: { kind: 'number', decimals: 0, negativeParens: true } } as const;
  const risk = cubeOf('Risk', marketSource('The options books, live', OPTION_BOOK, book),
    cubeOver(book, { rows: ['underlying', 'expiry'], sorts: [{ column: 'posVega', direction: 'desc' }] }),
    configured('Risk by underlying and expiry', {
      ...OPTION_INPUTS, book: HIDDEN, type: HIDDEN, strike: HIDDEN, iv: HIDDEN, spot: HIDDEN, price: HIDDEN, delta: HIDDEN,
      gamma: HIDDEN, vega: HIDDEN, theta: HIDDEN, chg: HIDDEN, contracts: HIDDEN, units: HIDDEN, expiry: {},
      posDelta: { ...GREEK, displayName: 'delta (shares)' }, dollarDelta: { ...MONEY, displayName: 'delta ($)' },
      posGamma: { ...GREEK, displayName: 'gamma (shares a $)' }, posVega: { ...MONEY, displayName: 'vega ($ a vol point)' },
      posTheta: { ...MONEY, displayName: 'theta ($ a day)' }, mv: { ...MONEY, displayName: 'value' },
      dayPnl: { ...MONEY, displayName: 'day P&L' },
    }), 1);
  const views: PageView[] = [
    { id: 'chain', kind: 'grid', cube: 'chain', title: 'The chain, live: each option priced by Black and Scholes' },
    chart('smile', 'chain', 'The smile: implied volatility by strike over spot, each expiry a line', {
      mark: 'line', x: 'moneyness', bins: { width: 0.05 }, split: 'expiry', y: [{ column: 'iv', fn: 'average', title: 'implied vol' }],
      options: { sort: { by: 'x', direction: 'asc' }, fitAxis: true },
    }),
    chart('delta', 'risk', 'Delta ($)', { mark: 'kpi', y: [{ column: 'dollarDelta', fn: 'sum' }] }),
    chart('vega', 'risk', 'Vega ($ a vol point)', { mark: 'kpi', y: [{ column: 'posVega', fn: 'sum' }] }),
    chart('theta', 'risk', 'Theta ($ a day)', { mark: 'kpi', y: [{ column: 'posTheta', fn: 'sum' }] }),
    chart('options-pnl', 'risk', 'Day P&L', { mark: 'kpi', y: [{ column: 'dayPnl', fn: 'sum' }], options: { target: 0, better: 'higher' } }),
    chart('vega-map', 'risk', 'Vega by expiry and underlying', {
      mark: 'heatmap', x: 'expiry', split: 'underlying', y: [{ column: 'posVega', fn: 'sum', title: 'vega' }],
      options: { labels: true, sort: { by: 'x', direction: 'asc' } },
    }),
    { id: 'risk', kind: 'grid', cube: 'risk', title: 'Risk by underlying and expiry' },
  ];
  const sheets: PageSheet[] = [
    sheet('sheet-1', 'Chain', [[0.65, tile('chain')], [0.35, tile('smile')]]),
    sheet('sheet-2', 'Risk', [
      [0.22, row(['delta', 0.25], ['vega', 0.25], ['theta', 0.25], ['options-pnl', 0.25])],
      [0.38, tile('vega-map')],
      [0.4, tile('risk')],
    ]),
  ];
  return { name: 'Options desk', cubes: [{ id: 'chain', cube: chainGrid }, { id: 'risk', cube: risk }], views, sheets };
}

// ---------------------------------------------------------------- K12: FX e-trading

/** The desk's FX prices: each pair's bid and ask, its spread and its change on the day in pips. */
export const FX_RATES = 'market::screens::fxRates()';

/** Each cross's quoted mid against the mid its legs imply, and the difference in pips. */
export const FX_CROSSES = 'market::screens::fxCrosses()';

/** Every client's request for a quote: whether it was hit, and the notional done. */
export const RFQS = 'market::screens::rfqs()';

/** The requests the client answered (done, missed or let expire): a hit ratio is their hits' average. */
export const RFQS_ANSWERED = 'market::screens::rfqsAnswered()';

async function fxParts(types: SourceTypes): Promise<Parts> {
  const rates = await columnsOf(types, FX_RATES, {
    bid: { aggregate: 'average' }, ask: { aggregate: 'average' }, mid: { aggregate: 'average' }, chg: { aggregate: 'average' },
    chgPct: { aggregate: 'average' }, pip: { aggregate: 'average' }, spreadPips: { aggregate: 'average' }, chgPips: { aggregate: 'average' },
  });
  const crosses = await columnsOf(types, FX_CROSSES, {
    quoted: { aggregate: 'average' }, implied: { aggregate: 'average' }, basisPips: { aggregate: 'average' },
    k: { kind: 'dimension' }, k1: { kind: 'dimension' }, k2: { kind: 'dimension' }, k3: { kind: 'dimension' },
  });
  const rfqs = await columnsOf(types, RFQS, {
    notional: { aggregate: 'sum' }, rate: { aggregate: 'average' }, spreadBps: { aggregate: 'average' },
    responseMs: { aggregate: 'average' }, hit: { aggregate: 'sum' }, done: { aggregate: 'sum' },
  });
  const answered = await columnsOf(types, RFQS_ANSWERED, {
    notional: { aggregate: 'sum' }, rate: { aggregate: 'average' }, spreadBps: { aggregate: 'average' },
    responseMs: { aggregate: 'average' }, hit: { aggregate: 'average' }, done: { aggregate: 'sum' },
  });
  const RATE: ColumnConfiguration = { format: { kind: 'number', minimumFractionDigits: 4, maximumFractionDigits: 5 } };
  const PIPS: ColumnConfiguration = { format: { kind: 'number', decimals: 1 }, appearance: { negativeForeground: '#d64545' } };
  const ladder = cubeOf('Rates', marketSource('FX rates', FX_RATES, rates),
    cubeOver(rates, { sorts: [{ column: 'pair', direction: 'asc' }] }),
    configured('The rates', {
      bid: RATE, ask: RATE, mid: RATE, chg: HIDDEN, pip: HIDDEN, at: PRINT_TIME,
      spreadPips: { ...PIPS, displayName: 'spread (pips)' }, chgPips: { ...PIPS, displayName: 'change (pips)' },
      chgPct: CHANGE_PCT,
    }));
  const crossGrid = cubeOf('Crosses', marketSource('FX crosses by their legs', FX_CROSSES, crosses), cubeOver(crosses),
    configured('Each cross against its legs', {
      k: HIDDEN, k1: HIDDEN, k2: HIDDEN, k3: HIDDEN, pip: HIDDEN, quoted: { ...RATE, displayName: 'quoted mid' },
      implied: { ...RATE, displayName: 'implied by its legs' }, eurusd: { ...RATE, displayName: 'EUR/USD' },
      gbpusd: { ...RATE, displayName: 'GBP/USD' }, usdjpy: { ...RATE, displayName: 'USD/JPY' },
      basisPips: { ...PIPS, displayName: 'quoted less implied (pips)', width: 170 },
    }));
  const rfqSource = marketSource('Requests for quote', RFQS, rfqs);
  const blotter = cubeOf('RFQs', rfqSource, cubeOver(rfqs, { sorts: [{ column: 'at', direction: 'desc' }] }),
    configured('Requests for quote, newest first', {
      rfq: HIDDEN, hitFlag: HIDDEN, hit: HIDDEN, done: HIDDEN, at: PRINT_TIME, notional: MONEY, rate: RATE,
      spreadBps: { format: { kind: 'number', decimals: 2 }, displayName: 'spread (bp)' },
      responseMs: { format: { kind: 'number', decimals: 0 }, displayName: 'response (ms)' },
    }));
  const hits = cubeOf('Hit ratios', marketSource('Requests answered', RFQS_ANSWERED, answered),
    cubeOver(answered, { rows: ['client'], sorts: [{ column: 'done', direction: 'desc' }] }),
    configured('Each client: hit ratio and notional done', {
      rfq: HIDDEN, at: HIDDEN, dealer: HIDDEN, pair: HIDDEN, side: HIDDEN, tenor: HIDDEN, status: HIDDEN, hitFlag: HIDDEN,
      rate: HIDDEN, notional: { ...MONEY, displayName: 'notional asked' }, done: { ...MONEY, displayName: 'notional done' },
      hit: { ...SHARE, displayName: 'hit ratio', heatmap: { from: '#f4a3a3', mid: '#ffe08a', to: '#9fd8b2', range: { min: 0, max: 1 } } },
      spreadBps: { format: { kind: 'number', decimals: 2 }, displayName: 'average spread (bp)' },
      responseMs: { format: { kind: 'number', decimals: 0 }, displayName: 'average response (ms)' },
    }));
  const views: PageView[] = [
    { id: 'rates', kind: 'grid', cube: 'rates', title: 'The rates' },
    chart('moves', 'rates', 'Each pair\'s change on the day', {
      x: 'pair', y: [{ column: 'chgPct', fn: 'average', title: 'change' }],
      options: { sort: { by: 'y', direction: 'desc' }, labels: true, legend: 'none' },
    }),
    { id: 'crosses', kind: 'grid', cube: 'crosses', title: 'Each cross against its legs (live joins)' },
    chart('hit-ratio', 'hits', 'Hit ratio', { mark: 'kpi', y: [{ column: 'hit', fn: 'average' }] }),
    chart('done', 'hits', 'Notional done', { mark: 'kpi', y: [{ column: 'done', fn: 'sum' }] }),
    chart('response', 'blotter', 'Average response (ms)', { mark: 'kpi', y: [{ column: 'responseMs', fn: 'average' }],
      options: { target: 250, better: 'lower' } }),
    chart('by-spread', 'hits', 'Hit ratio by the spread quoted (bp)', {
      x: 'spreadBps', bins: { width: 0.5 }, y: [{ column: 'hit', fn: 'average', title: 'hit ratio' }],
      options: { sort: { by: 'x', direction: 'asc' }, legend: 'none' },
    }),
    { id: 'blotter', kind: 'grid', cube: 'blotter', title: 'Requests for quote, newest first' },
    { id: 'hits', kind: 'grid', cube: 'hits', title: 'Each client: hit ratio and notional done' },
  ];
  const sheets: PageSheet[] = [
    sheet('sheet-1', 'Rates', [[0.5, row(['rates', 0.6], ['moves', 0.4])], [0.5, tile('crosses')]]),
    sheet('sheet-2', 'RFQs', [
      [0.25, row(['hit-ratio', 0.2], ['done', 0.2], ['response', 0.2], ['by-spread', 0.4])],
      [0.4, tile('blotter')],
      [0.35, tile('hits')],
    ]),
  ];
  return { name: 'FX e-trading', cubes: [{ id: 'rates', cube: ladder }, { id: 'crosses', cube: crossGrid }, { id: 'blotter', cube: blotter }, { id: 'hits', cube: hits }], views, sheets };
}

// ---------------------------------------------------------------- K13: TCA and markouts

/** Every fill's cost and its markouts 1, 5 and 30 seconds after it, by as-of joins to the tape (K7). */
export const MARKOUTS = 'market::screens::markouts()';

async function tcaParts(types: SourceTypes): Promise<Parts> {
  const weighted = { aggregate: 'wavg', aggregateWeight: 'notional' } as const;
  const columns = await columnsOf(types, MARKOUTS, {
    qty: { aggregate: 'sum' }, px: { aggregate: 'wavg', aggregateWeight: 'qty' }, arrival: { aggregate: 'average' },
    commission: { aggregate: 'sum' }, notional: { aggregate: 'sum' }, sign: { aggregate: 'average' },
    p1: { aggregate: 'average' }, p5: { aggregate: 'average' }, p30: { aggregate: 'average' },
    slipBps: weighted, m1: weighted, m5: weighted, m30: weighted, commissionBps: weighted,
  });
  const source = marketSource('Fills and their markouts', MARKOUTS, columns);
  const BP: ColumnConfiguration = { format: { kind: 'number', decimals: 2 }, appearance: { negativeForeground: '#d64545' } };
  const keys: Readonly<Record<string, ColumnConfiguration>> = {
    exec: HIDDEN, inst: HIDDEN, t1: HIDDEN, t5: HIDDEN, t30: HIDDEN, i1: HIDDEN, i5: HIDDEN, i30: HIDDEN, pt1: HIDDEN,
    pt5: HIDDEN, pt30: HIDDEN, sign: HIDDEN, p1: HIDDEN, p5: HIDDEN, p30: HIDDEN, commission: HIDDEN, arrival: HIDDEN,
    slipBps: { ...BP, displayName: 'slippage (bp)' }, m1: { ...BP, displayName: 'markout 1s (bp)' },
    m5: { ...BP, displayName: 'markout 5s (bp)' }, m30: { ...BP, displayName: 'markout 30s (bp)' },
    commissionBps: { ...BP, displayName: 'commission (bp)' }, notional: MONEY, qty: SIZE, px: PRICE,
  };
  const byStrategy = cubeOf('By algorithm', source,
    cubeOver(columns, { rows: ['strategy', 'venue'], sorts: [{ column: 'notional', direction: 'desc' }] }),
    configured('TCA by algorithm and venue (notional-weighted)', {
      ...keys, at: HIDDEN, ticker: HIDDEN, side: HIDDEN, broker: HIDDEN, px: HIDDEN,
    }));
  const byBroker = cubeOf('By broker', source,
    cubeOver(columns, { rows: ['broker'], sorts: [{ column: 'notional', direction: 'desc' }] }),
    configured('TCA by broker (notional-weighted)', {
      ...keys, at: HIDDEN, ticker: HIDDEN, side: HIDDEN, strategy: HIDDEN, venue: HIDDEN, px: HIDDEN,
    }));
  const fills = cubeOf('Fills', source, cubeOver(columns, { sorts: [{ column: 'at', direction: 'desc' }] }),
    configured('Each fill, its slippage and markouts', { ...keys, at: PRINT_TIME, strategy: HIDDEN, broker: HIDDEN }));
  const views: PageView[] = [
    chart('cost', 'by-strategy', 'Slippage against arrival (bp)', { mark: 'kpi', y: [{ column: 'slipBps', fn: 'wavg', weight: 'notional' }],
      options: { target: 0, better: 'lower' } }),
    chart('markout', 'by-strategy', 'Markout after 5 seconds (bp)', { mark: 'kpi', y: [{ column: 'm5', fn: 'wavg', weight: 'notional' }],
      options: { target: 0, better: 'higher' } }),
    chart('horizons', 'by-strategy', 'Markouts by algorithm: 1, 5 and 30 seconds after the fill (bp)', {
      x: 'strategy', y: [{ column: 'm1', fn: 'wavg', weight: 'notional', title: '1s' },
        { column: 'm5', fn: 'wavg', weight: 'notional', title: '5s' }, { column: 'm30', fn: 'wavg', weight: 'notional', title: '30s' }],
      layers: [{ kind: 'line', value: 0, label: 'the fill' }],
      options: { sort: { by: 'x', direction: 'asc' }, legend: 'top' },
    }),
    { id: 'by-strategy', kind: 'grid', cube: 'by-strategy', title: 'TCA by algorithm and venue (notional-weighted)' },
    chart('venue-cost', 'by-strategy', 'Slippage by venue (bp)', {
      x: 'venue', y: [{ column: 'slipBps', fn: 'wavg', weight: 'notional', title: 'slippage' }],
      options: { sort: { by: 'y', direction: 'asc' }, labels: true, legend: 'none', orientation: 'horizontal' },
    }),
    { id: 'by-broker', kind: 'grid', cube: 'by-broker', title: 'TCA by broker (notional-weighted)' },
    { id: 'fills', kind: 'grid', cube: 'fills', title: 'Each fill, its slippage and markouts' },
  ];
  const sheets: PageSheet[] = [
    sheet('sheet-1', 'TCA', [
      [0.3, row(['cost', 0.2], ['markout', 0.2], ['horizons', 0.6])],
      [0.7, row(['by-strategy', 0.62], ['venue-cost', 0.38])],
    ]),
    sheet('sheet-2', 'Fills', [[0.3, tile('by-broker')], [0.7, tile('fills')]]),
  ];
  return { name: 'TCA and markouts', cubes: [{ id: 'by-strategy', cube: byStrategy }, { id: 'by-broker', cube: byBroker }, { id: 'fills', cube: fills }], views, sheets };
}

// ---------------------------------------------------------------- K15: the news

async function newsParts(types: SourceTypes, values: MarketValues): Promise<Parts> {
  const newsColumns = await columnsOf(types, NEWS);
  const mixColumns = await columnsOf(types, NEWS, { id: { aggregate: 'count' }, tone: { aggregate: 'average' } });
  const stockColumns = await columnsOf(types, STOCK, { seq: { kind: 'dimension' } });
  // THE WATCHLIST (the user, 2026-10-27: a pane of a news monitor is the names a trader follows): the person's names,
  // its pane's filter, saved with the page and changed there; it starts on the market's five largest, the chart beside
  // the panes on the largest
  const watchlist = await values(WATCHLIST_START);
  const symbol = await firstOf(values, LARGEST);
  const wire = marketSource('The newswire', NEWS, newsColumns);
  // THE MONITOR: a news terminal's panes, each a feed of the newswire customised by its grid's filter -- the whole
  // market's, macro and sector stories, the watchlist's names', a sector's, a keyword's (its own box) -- and the name a
  // card picks, its price today
  const hasWord = (column: string): FilterNode => ({ kind: 'condition', column, operator: 'containsCaseInsensitive', value: 'buyback' });
  const keyword: FilterNode = { kind: 'or', children: [hasWord('headline'), hasWord('body')] };
  const among = (column: string, names: readonly string[]): FilterNode => ({ kind: 'condition', column, operator: 'in', value: [...names] });
  const cubes = [
    { id: 'all', cube: wireOf('All news', 'All news', wire, newsColumns) },
    { id: 'macro', cube: wireOf('Macro and sectors', 'Macro and sectors', wire, newsColumns, among('category', ['Macro', 'Industry'])) },
    { id: 'names', cube: wireOf('My watchlist', 'My watchlist', wire, newsColumns, among('ticker', watchlist)) },
    { id: 'tech', cube: wireOf('Technology', 'Technology', wire, newsColumns, equal('sector', 'Technology')) },
    { id: 'word', cube: wireOf('Keyword', 'Keyword', wire, newsColumns, keyword) },
    { id: 'stock', cube: cubeOf('The name picked', marketSource('The stock', STOCK, stockColumns),
      cubeOver(stockColumns, { filter: equal('ticker', symbol), sorts: [{ column: 'seq', direction: 'desc' }] }),
      configured('Its prints', { seq: HIDDEN, ticker: HIDDEN, prevClose: HIDDEN, ema: HIDDEN, at: PRINT_TIME, price: PRICE, size: SIZE })) },
    // THE WIRE: every story as a grid, and the day's news by category
    { id: 'wire', cube: wireOf('The wire', 'The wire: every story', wire, newsColumns) },
    { id: 'mix', cube: cubeOf('News by category', marketSource('The newswire', NEWS, mixColumns),
      cubeOver(mixColumns, { rows: ['category'], sorts: [{ column: 'id', direction: 'desc' }] }),
      configured('News by category: its stories and their tone', {
        id: { displayName: 'stories', format: { kind: 'number', decimals: 0 } }, at: HIDDEN, headline: HIDDEN, body: HIDDEN,
        source: HIDDEN, weight: HIDDEN, tickers: HIDDEN, region: HIDDEN, ticker: HIDDEN, sector: HIDDEN,
        tone: WIRE_COLUMNS['tone']!,
      })) },
  ];
  const to = ['prints'];
  const LAST = { column: 'price', fn: 'maxBy', weight: 'seq', title: 'last' } as const;
  const PRIOR = { column: 'prevClose', fn: 'max' } as const;
  const views: PageView[] = [
    feedOf('allFeed', 'all', 'All news', to),
    { id: 'allTable', kind: 'grid', cube: 'all', title: 'All news, as a grid' },
    feedOf('macroFeed', 'macro', 'Macro and sectors', to),
    { id: 'macroTable', kind: 'grid', cube: 'macro', title: 'Macro and sectors, as a grid' },
    feedOf('namesFeed', 'names', 'My watchlist', to),
    { id: 'namesTable', kind: 'grid', cube: 'names', title: 'My watchlist, as a grid' },
    feedOf('techFeed', 'tech', 'Technology', to),
    { id: 'techTable', kind: 'grid', cube: 'tech', title: 'Technology, as a grid' },
    { ...feedOf('wordFeed', 'word', 'Keyword', to), selection: [keyword] },
    { id: 'wordTable', kind: 'grid', cube: 'word', title: 'Keyword, as a grid' },
    chart('price', 'stock', 'The name picked: today against its prior close', {
      mark: 'area', x: 'at', grain: 'minute', y: [LAST],
      layers: [{ kind: 'line', of: PRIOR, label: 'prior close' }, { kind: 'threshold', of: PRIOR }, { kind: 'last' }],
      options: {
        sort: { by: 'x', direction: 'asc' }, zoom: false, limit: 40_000, fitAxis: true, span: 0,
        spans: [{ seconds: 300, grain: 'second' }, { seconds: 3600, grain: 'second' }, { seconds: 0, grain: 'minute' }],
      },
    }),
    { id: 'prints', kind: 'grid', cube: 'stock', title: 'Its prints, newest first' },
    { id: 'wire', kind: 'grid', cube: 'wire', title: 'The wire: every story, the newest first' },
    { id: 'mix', kind: 'grid', cube: 'mix', title: 'News by category: its stories and their tone' },
    chart('mixChart', 'mix', 'Stories by category', { mark: 'bar', x: 'category', y: [{ column: 'id', fn: 'count', title: 'stories' }] }),
  ];
  const column = (...parts: readonly (readonly [node: Node, size: number])[]): Node => ({
    split: 'column', parts: parts.map(([node, size]) => ({ node, size })),
  });
  const stack = (...ids: string[]): Node => ({ stack: ids });
  const sheets: PageSheet[] = [
    sheet('sheet-1', 'Monitor', [[1, {
      split: 'row', parts: [
        { node: column([stack('allFeed', 'allTable'), 0.62], [stack('macroFeed', 'macroTable'), 0.38]), size: 0.34 },
        { node: column([stack('namesFeed', 'namesTable'), 0.5], [stack('techFeed', 'techTable'), 0.5]), size: 0.33 },
        { node: column([stack('wordFeed', 'wordTable'), 0.42], [stack('price', 'prints'), 0.58]), size: 0.33 },
      ],
    }]]),
    sheet('sheet-2', 'The wire', [[0.6, tile('wire')], [0.4, row(['mix', 0.45], ['mixChart', 0.55])]]),
  ];
  return { name: 'News', cubes, views, sheets };
}

// ---------------------------------------------------------------- the order flow

/** A block, as the street counts one: 10,000 shares of a stock, or a million dollars, in one print. */
const BLOCK_SHARES = 10_000;
const BLOCK_NOTIONAL = 1_000_000;
const BLOCK_WORDS = `${BLOCK_SHARES.toLocaleString('en-US')} shares or $${BLOCK_NOTIONAL.toLocaleString('en-US')} in one print`;

/** THE ORDER FLOW: every print, who crossed the spread and the net of the two, and whether it was a block (the street's
 *  count of one, the function's arguments). */
export const FLOW = `market::screens::flow(${BLOCK_SHARES}.0, ${BLOCK_NOTIONAL}.0)`;

/** The blocks. */
export const BLOCKS = `market::screens::blocks(${BLOCK_SHARES}.0, ${BLOCK_NOTIONAL}.0)`;

async function flowParts(types: SourceTypes): Promise<Parts> {
  const sum = { aggregate: 'sum' } as const;
  const flowHow = { seq: { aggregate: 'count' }, size: sum, notional: sum, bought: sum, sold: sum, net: sum, block: sum,
    price: { aggregate: 'wavg', aggregateWeight: 'size' } } as const;
  const flow = await columnsOf(types, FLOW, flowHow);
  const blocks = await columnsOf(types, BLOCKS, { seq: { kind: 'dimension' } });
  const source = marketSource('The order flow', FLOW, flow);
  const NET: ColumnConfiguration = { ...MONEY, appearance: { negativeForeground: '#d64545', normalForeground: '#1f9d55' } };
  const byName = cubeOf('Flow by name', source, cubeOver(flow, { rows: ['ticker'], sorts: [{ column: 'notional', direction: 'desc' }] }),
    configured('Flow by name: bought, sold and the net', {
      sector: HIDDEN, kind: HIDDEN, at: HIDDEN, side: HIDDEN, venue: HIDDEN, condition: HIDDEN, price: { ...PRICE, displayName: 'VWAP' },
      seq: { displayName: 'prints', format: { kind: 'number', decimals: 0 } }, size: { ...BIG, displayName: 'volume' },
      notional: MONEY, bought: MONEY, sold: MONEY, net: NET, block: { displayName: 'blocks', format: { kind: 'number', decimals: 0 } },
    }));
  const tape = cubeOf('Time and sales', source, cubeOver(flow.map((c) => (c.name === 'seq' ? { name: c.name, type: c.type, kind: 'dimension' as const } : c)),
    { sorts: [{ column: 'seq', direction: 'desc' }] }),
  configured('Time and sales', {
    seq: HIDDEN, sector: HIDDEN, kind: HIDDEN, bought: HIDDEN, sold: HIDDEN, net: HIDDEN, block: HIDDEN,
    at: PRINT_TIME, price: PRICE, size: SIZE, notional: MONEY, condition: { displayName: 'cond' },
  }));
  const blockGrid = cubeOf('Blocks', marketSource('Block prints', BLOCKS, blocks), cubeOver(blocks, { sorts: [{ column: 'seq', direction: 'desc' }] }),
    configured(`Blocks: ${BLOCK_WORDS}`, {
      seq: HIDDEN, kind: HIDDEN, bought: HIDDEN, sold: HIDDEN, net: HIDDEN, block: HIDDEN, condition: HIDDEN,
      at: PRINT_TIME, price: PRICE, size: SIZE, notional: MONEY,
    }));
  const views: PageView[] = [
    chart('bought', 'byName', 'Bought (buyer-initiated)', { mark: 'kpi', y: [{ column: 'bought', fn: 'sum' }] }),
    chart('sold', 'byName', 'Sold (seller-initiated)', { mark: 'kpi', y: [{ column: 'sold', fn: 'sum' }] }),
    chart('net', 'byName', 'Net flow (bought less sold)', { mark: 'kpi', y: [{ column: 'net', fn: 'sum' }], options: { target: 0, better: 'higher' } }),
    chart('blocks-count', 'byName', 'Block prints', { mark: 'kpi', y: [{ column: 'block', fn: 'sum' }] }),
    chart('by-minute', 'byName', 'Bought and sold each minute', {
      mark: 'area', x: 'at', grain: 'minute',
      y: [{ column: 'bought', fn: 'sum', title: 'bought' }, { column: 'sold', fn: 'sum', title: 'sold' }],
      options: { sort: { by: 'x', direction: 'asc' }, zoom: false, limit: 40_000, legend: 'top' },
    }),
    chart('by-sector', 'byName', 'Net flow by sector', {
      x: 'sector', y: [{ column: 'net', fn: 'sum', title: 'net flow' }],
      layers: [{ kind: 'line', value: 0, label: 'balanced' }],
      options: { sort: { by: 'y', direction: 'desc' }, labels: true, legend: 'none', orientation: 'horizontal' },
    }),
    chart('by-venue', 'byName', 'Notional by venue', {
      mark: 'pie', x: 'venue', y: [{ column: 'notional', fn: 'sum', title: 'notional' }], options: { sort: { by: 'y', direction: 'desc' } },
    }),
    { id: 'flow', kind: 'grid', cube: 'byName', title: 'Flow by name: bought, sold and the net' },
    { id: 'blocks', kind: 'grid', cube: 'blocks', title: `Blocks: ${BLOCK_WORDS}` },
    { id: 'tape', kind: 'grid', cube: 'tape', title: 'Time and sales' },
  ];
  const sheets: PageSheet[] = [
    sheet('sheet-1', 'Flow', [
      [0.16, across(['bought', 0.22], ['sold', 0.22], ['net', 0.28], ['blocks-count', 0.28])],
      [0.38, across(['by-minute', 0.62], ['by-sector', 0.38])],
      [0.46, across(['flow', 0.42], [down(['blocks', 0.5], ['tape', 0.5]), 0.36], ['by-venue', 0.22])],
    ]),
  ];
  return { name: 'Order flow', cubes: [{ id: 'byName', cube: byName }, { id: 'tape', cube: tape }, { id: 'blocks', cube: blockGrid }], views, sheets };
}

// ---------------------------------------------------------------- the crypto desk

/** The coin a crypto screen opens on: the largest, always among the world's coins (market.groovy COINS). */
const FIRST_COIN = 'BTC-USD';

async function cryptoParts(types: SourceTypes): Promise<Parts> {
  const quoteFigures = { aggregate: 'average' } as const;
  const watchColumns = await columnsOf(types, WATCHLIST, {
    last: quoteFigures, bid: quoteFigures, ask: quoteFigures, open: quoteFigures, high: { aggregate: 'max' },
    low: { aggregate: 'min' }, prevClose: quoteFigures, chg: quoteFigures, chgPct: quoteFigures, spreadBps: quoteFigures,
  });
  const stockColumns = await columnsOf(types, STOCK, { seq: { kind: 'dimension' } });
  const sum = { aggregate: 'sum' } as const;
  const flowColumns = await columnsOf(types, FLOW, { seq: { aggregate: 'count' }, size: sum, notional: sum, bought: sum,
    sold: sum, net: sum, block: sum, price: { aggregate: 'wavg', aggregateWeight: 'size' } });
  const positions = await columnsOf(types, POSITIONS_LIVE, {
    qty: sum, cost: { aggregate: 'average' }, mid: { aggregate: 'average' }, close: { aggregate: 'average' },
    mv: sum, dayPnl: sum, gross: sum, sinceCost: sum,
  });
  const crypto = equal('kind', 'CRYPTO');
  const coins = cubeOf('Coins', marketSource('Watchlist', WATCHLIST, watchColumns),
    // the majors first: a coin's price orders them as a desk lists them (BTC, ETH, SOL, ...); its volume, in coins, does not
    cubeOver(watchColumns, { filter: crypto, sorts: [{ column: 'last', direction: 'desc' }] }),
    configured('Coins: pick one', {
      name: HIDDEN, kind: HIDDEN, sector: HIDDEN, at: HIDDEN, bidSize: HIDDEN, askSize: HIDDEN, open: HIDDEN, high: HIDDEN,
      low: HIDDEN, prevClose: HIDDEN, chg: HIDDEN, bid: HIDDEN, ask: HIDDEN, volume: HIDDEN, spreadBps: HIDDEN,
      last: { format: { kind: 'number', maximumFractionDigits: 4 } }, chgPct: { ...CHANGE_PCT, displayName: '24h' },
    }));
  const coin = cubeOf('The coin', marketSource('The stock', STOCK, stockColumns),
    cubeOver(stockColumns, { filter: equal('ticker', FIRST_COIN), sorts: [{ column: 'seq', direction: 'desc' }] }),
    configured('Its prints', {
      seq: HIDDEN, ticker: HIDDEN, prevClose: HIDDEN, ema: HIDDEN, at: PRINT_TIME,
      price: { format: { kind: 'number', maximumFractionDigits: 4 } }, size: { format: { kind: 'number', maximumFractionDigits: 6 } },
    }));
  const flow = cubeOf('Coin flow', marketSource('The order flow', FLOW, flowColumns),
    cubeOver(flowColumns, { filter: crypto, rows: ['ticker'], sorts: [{ column: 'notional', direction: 'desc' }] }),
    configured('Each coin: bought, sold and the net', {
      sector: HIDDEN, kind: HIDDEN, at: HIDDEN, side: HIDDEN, venue: HIDDEN, condition: HIDDEN, block: HIDDEN, size: HIDDEN,
      price: { format: { kind: 'number', maximumFractionDigits: 4 }, displayName: 'VWAP' },
      seq: { displayName: 'prints', format: { kind: 'number', decimals: 0 } }, notional: MONEY, bought: MONEY, sold: MONEY,
      net: { ...MONEY, appearance: { negativeForeground: '#d64545', normalForeground: '#1f9d55' } },
    }));
  const book = cubeOf('The crypto book', marketSource('Positions, live', POSITIONS_LIVE, positions),
    cubeOver(positions, { filter: equal('sector', 'Crypto'), sorts: [{ column: 'mv', direction: 'desc' }] }),
    configured('The crypto book, marked live', {
      desk: HIDDEN, inst: HIDDEN, qinst: HIDDEN, sector: HIDDEN, close: HIDDEN, gross: HIDDEN, cost: HIDDEN,
      qty: { format: { kind: 'number', maximumFractionDigits: 4 } }, mid: { format: { kind: 'number', maximumFractionDigits: 4 } },
      mv: { ...MONEY, displayName: 'value' }, dayPnl: { ...MONEY, displayName: 'day P&L' },
      sinceCost: { ...MONEY, displayName: 'P&L since cost' },
    }));
  const LAST = { column: 'price', fn: 'maxBy', weight: 'seq', title: 'last' } as const;
  const views: PageView[] = [
    { id: 'coins', kind: 'grid', cube: 'coins', title: 'Coins: pick one', link: { column: 'ticker', to: ['prints'] } },
    chart('last', 'coin', 'Last', { mark: 'kpi', y: [LAST] }),
    chart('vwap', 'coin', 'VWAP', { mark: 'kpi', y: [{ column: 'price', fn: 'wavg', weight: 'size' }] }),
    chart('high', 'coin', 'High', { mark: 'kpi', y: [{ column: 'price', fn: 'max' }] }),
    chart('low', 'coin', 'Low', { mark: 'kpi', y: [{ column: 'price', fn: 'min' }] }),
    chart('book-pnl', 'book', 'Crypto day P&L', { mark: 'kpi', y: [{ column: 'dayPnl', fn: 'sum' }], options: { target: 0, better: 'higher' } }),
    chart('price', 'coin', 'The coin: its last price each second, its EMA, the volume beneath', {
      // its last five minutes by the second at first: a coin's tape here is the feed's since the world started
      mark: 'area', x: 'at', grain: 'second',
      y: [LAST, { column: 'ema', fn: 'maxBy', weight: 'seq', title: 'EMA (0.1)', mark: 'line', axis: 'left' },
        { column: 'size', fn: 'sum', title: 'volume', pane: 'below' }],
      layers: [{ kind: 'last' }],
      options: {
        sort: { by: 'x', direction: 'asc' }, zoom: false, limit: 40_000, legend: 'top', fitAxis: true, span: 300,
        spans: [{ seconds: 60, grain: 'second' }, { seconds: 300, grain: 'second' }, { seconds: 3600, grain: 'second' },
          { seconds: 0, grain: 'minute' }],
      },
    }),
    chart('candles', 'coin', 'One-minute candles, the volume beneath', {
      mark: 'candlestick', x: 'at', grain: 'minute', y: candleMeasures('price', 'seq', 'size'),
      options: { sort: { by: 'x', direction: 'asc' }, zoom: true, limit: 1000 },
    }),
    chart('net', 'flow', 'Net flow by coin (bought less sold)', {
      x: 'ticker', y: [{ column: 'net', fn: 'sum', title: 'net flow' }], layers: [{ kind: 'line', value: 0, label: 'balanced' }],
      options: { sort: { by: 'y', direction: 'desc' }, labels: true, legend: 'none', orientation: 'horizontal' },
    }),
    { id: 'prints', kind: 'grid', cube: 'coin', title: 'Its prints, newest first' },
    { id: 'flow', kind: 'grid', cube: 'flow', title: 'Each coin: bought, sold and the net' },
    { id: 'book', kind: 'grid', cube: 'book', title: 'The crypto book, marked live' },
  ];
  const sheets: PageSheet[] = [
    sheet('sheet-1', 'Crypto', [[1, across(['coins', 0.2], [down(
      [across(['last', 0.2], ['vwap', 0.2], ['high', 0.2], ['low', 0.2], ['book-pnl', 0.2]), 0.15],
      [across([stacked('price', 'candles'), 0.64], ['net', 0.36]), 0.5],
      [across(['prints', 0.36], ['flow', 0.34], ['book', 0.3]), 0.35],
    ), 0.8])]]),
  ];
  return {
    name: 'Crypto desk',
    cubes: [{ id: 'coins', cube: coins }, { id: 'coin', cube: coin }, { id: 'flow', cube: flow }, { id: 'book', cube: book }],
    views, sheets,
  };
}

// ---------------------------------------------------------------- the trading desk: one page, a tab a screen

/**
 * THE TRADING DESK (the user, 2026-10-25: "a real saved page just like charts in real datacube that opens like 8-10
 * tabs all with different realistic trading screens"): one page, ten tabs, each a screen of a trading floor -- the
 * market, a stock, the news, the order flow, crypto, the execution blotter, TCA, P&L and risk, options, FX -- every grid
 * a saved query over the one trading model, run on the market's Deephaven, ticking.
 */
async function deskParts(types: SourceTypes, values: MarketValues): Promise<Parts> {
  const [map, ticker, news, flow, crypto, blotter, tca, pnl, options, fx] = await Promise.all([
    marketMapParts(types), tickerParts(types, values), newsParts(types, values), flowParts(types), cryptoParts(types),
    blotterParts(types), tcaParts(types), pnlParts(types), optionsParts(types), fxParts(types),
  ]);
  const bandsOf = (parts: Parts): readonly (readonly [number, Node])[] => boardOf(parts.sheets[0]!.layout).bands.map((b) => [b.height, b.node] as const);
  const tabs = [
    tabOf('mk', map, 'Markets', [
      [0.6, across(['map', 0.56], ['sectors', 0.18], [down([across(['advancing', 0.5], ['declining', 0.5]), 0.32], ['by-sector', 0.68]), 0.26])],
      [0.4, across(['gainers', 0.34], ['losers', 0.33], ['active', 0.33])],
    ]),
    tabOf('st', ticker, 'Stock', bandsOf(ticker)),
    tabOf('nw', news, 'News', bandsOf(news)),
    tabOf('fl', flow, 'Order flow', bandsOf(flow)),
    tabOf('cx', crypto, 'Crypto', bandsOf(crypto)),
    tabOf('bl', blotter, 'Blotter', [
      [0.18, across(['working', 0.18], ['filled', 0.18], ['slippage', 0.24], ['by-strategy', 0.4])],
      [0.46, tile('orders')],
      [0.36, across([stacked('fills', 'venues'), 0.62], ['by-venue', 0.38])],
    ]),
    tabOf('tc', tca, 'TCA', [
      [0.28, across(['cost', 0.18], ['markout', 0.18], ['horizons', 0.64])],
      [0.42, across(['by-strategy', 0.62], ['venue-cost', 0.38])],
      [0.3, across(['by-broker', 0.35], ['fills', 0.65])],
    ]),
    tabOf('pl', pnl, 'P&L and risk', [
      [0.24, across(['firm-pnl', 0.16], ['breaches', 0.14], ['warnings', 0.14], ['by-desk', 0.28], ['headroom', 0.28])],
      [0.76, across(['pnl', 0.58], [down(['limits', 0.55], ['by-sector', 0.45]), 0.42])],
    ]),
    tabOf('op', options, 'Options', [
      [0.16, across(['delta', 0.25], ['vega', 0.25], ['theta', 0.25], ['options-pnl', 0.25])],
      [0.84, across([down(['chain', 0.62], ['smile', 0.38]), 0.56], [down(['vega-map', 0.42], ['risk', 0.58]), 0.44])],
    ]),
    tabOf('fx', fx, 'FX', [
      [0.5, across([down(['rates', 0.62], ['crosses', 0.38]), 0.55],
        [down([across(['hit-ratio', 0.34], ['done', 0.33], ['response', 0.33]), 0.3], ['moves', 0.7]), 0.45])],
      [0.5, across(['blotter', 0.6], [down(['hits', 0.55], ['by-spread', 0.45]), 0.4])],
    ]),
  ];
  return {
    name: 'Trading desk',
    cubes: tabs.flatMap((t) => t.cubes),
    views: tabs.flatMap((t) => t.views),
    sheets: tabs.flatMap((t) => t.sheets),
  };
}

/** The trading desk, as the app's Open… lists it (host.ts: first under Examples). */
export const DESK: TraderPage = {
  id: 'trading-desk',
  label: 'Trading desk: ten screens, live on Deephaven',
  about: 'A trading floor on one page, a tab a screen, every figure computed by the market\'s Deephaven and ticking: '
    + 'the market map and movers; a stock from the open, its candles, quote, prints and news; a news terminal; the order '
    + 'flow -- bought against sold, blocks, venues; a crypto desk on Coinbase\'s feed; the execution blotter; TCA and '
    + 'markouts; P&L against limits; the options chain and its risk; FX rates, crosses and RFQs.',
  build: pageOf(deskParts),
};
