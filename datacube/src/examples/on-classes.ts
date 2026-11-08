// THE MARKETS PAGE AND THE GALLERY OVER CLASSES (docs/TS_DECISIONS_2026_11_07.md §4, (a'), 2026-11-07, the user: "I like
// A' that's fine - basically copies of the charts that start with classes"): the same views and sheets as markets.ts's
// and timeseries-gallery.ts's pages -- built by the same functions, so a copy never drifts from its page -- each grid a
// saved query of the markets project over its classes (marketdata::MarketQuote, curves::CurvePoint,
// markets::MacroObservation), projected under the samples' own column names, so every series reads as written and
// answers as the sample's table does (test/stress-markets.test.ts holds each copy's every series to its page's). The
// pages over the samples stay as they are: the frozen answers' fixtures (test/timeseries-frozen.ts).

import { cast, derive, element, fn, lambda, projectObjects, property, type, variable, type ValueSpecification } from '../../../pure-protocol/src/index.ts';
import { DEFAULT_CONFIGURATION, type CubeConfiguration } from '../config.ts';
import { writeCube, type CubeDocument, type QuerySource } from '../cube-document.ts';
import { openedState } from '../cube-state.ts';
import type { PageDocument } from '../page-document.ts';
import type { ColumnSpec, CubeSnapshot } from '../snapshot.ts';
import { TreeState } from '../tree.ts';
import { MARKETS_PROJECT } from './markets-classes.ts';
import { marketsPage } from './markets.ts';
import { timeseriesGalleryPage } from './timeseries-gallery.ts';

/** Every grid's execution context: the project's own mapping (the trading model's stress::AllMapping and the macro
 *  class's) and runtime (datacube/tools/markets-data/markets-model.pure). */
const CONTEXT = { _type: 'explicitExecutionContext', mapping: 'markets::Mapping', runtime: 'markets::RT' } as const;

/** A column of a class grid: its name and type as the sample's, and its value from the object. */
interface ClassColumn extends ColumnSpec {
  readonly value: (x: ValueSpecification) => ValueSpecification;
}

/** A grid over a class: its cube's id (the sample grid's), its class, its saved query's text as Query saves it, the
 *  same as its columns' values, and how it opens (its rows grouped, each measure's aggregate). */
interface ClassGrid {
  readonly id: string;
  readonly title: string;
  readonly className: string;
  readonly text: string;
  readonly columns: readonly ClassColumn[];
  readonly rows: readonly string[];
  readonly configuration: CubeConfiguration['columns'];
}

const at = (...names: string[]) => (x: ValueSpecification): ValueSpecification =>
  names.reduce<ValueSpecification>((v, n) => property(v, n), x);

/** A quote a day as the markets sample's row: the instrument's ticker and asset class, the quote's day (its time the
 *  close), the day's prices and volume. */
export const PRICES_ON_CLASSES: ClassGrid = {
  id: 'prices',
  title: 'Prices (MarketQuote)',
  className: 'marketdata::MarketQuote',
  text: '|marketdata::MarketQuote.all()->project(~[symbol: x|$x.instrument.ticker, asset_class: x|$x.instrument.assetClass, date: x|$x.quoteTime->datePart()->cast(@StrictDate), open: x|$x.openPrice, high: x|$x.highPrice, low: x|$x.lowPrice, close: x|$x.closePrice, volume: x|$x.volume])',
  columns: [
    { name: 'symbol', type: 'String', value: at('instrument', 'ticker') },
    { name: 'asset_class', type: 'String', value: at('instrument', 'assetClass') },
    { name: 'date', type: 'StrictDate', value: (x) => cast(fn('datePart', property(x, 'quoteTime')), type('StrictDate')) },
    { name: 'open', type: 'Float', kind: 'measure', value: at('openPrice') },
    { name: 'high', type: 'Float', kind: 'measure', value: at('highPrice') },
    { name: 'low', type: 'Float', kind: 'measure', value: at('lowPrice') },
    { name: 'close', type: 'Float', kind: 'measure', value: at('closePrice') },
    { name: 'volume', type: 'Float', kind: 'measure', value: at('volume') },
  ],
  rows: ['asset_class', 'symbol'],
  configuration: { open: { aggregateFn: 'average' }, high: { aggregateFn: 'average' }, low: { aggregateFn: 'average' }, close: { aggregateFn: 'average' } },
};

/** The Treasury curve as the rates sample's row: a point a tenor a day. */
export const RATES_ON_CLASSES: ClassGrid = {
  id: 'rates',
  title: 'Treasury curve (CurvePoint)',
  className: 'curves::CurvePoint',
  text: '|curves::CurvePoint.all()->project(~[tenor: x|$x.tenorLabel, years: x|$x.tenorYears(), date: x|$x.cobDate, yield: x|$x.zeroRate])',
  columns: [
    { name: 'tenor', type: 'String', value: at('tenorLabel') },
    { name: 'years', type: 'Float', kind: 'dimension', value: at('tenorYears') },
    { name: 'date', type: 'StrictDate', value: at('cobDate') },
    { name: 'yield', type: 'Float', kind: 'measure', value: at('zeroRate') },
  ],
  rows: ['tenor'],
  configuration: { yield: { aggregateFn: 'average' } },
};

/** The macro series as the macro sample's row: a value a series a month. */
export const MACRO_ON_CLASSES: ClassGrid = {
  id: 'macro',
  title: 'Macro, monthly (MacroObservation)',
  className: 'markets::MacroObservation',
  text: '|markets::MacroObservation.all()->project(~[series: x|$x.series, date: x|$x.date, value: x|$x.value])',
  columns: [
    { name: 'series', type: 'String', value: at('series') },
    { name: 'date', type: 'StrictDate', value: at('date') },
    { name: 'value', type: 'Float', kind: 'measure', value: at('value') },
  ],
  rows: ['series'],
  configuration: { value: { aggregateFn: 'average' } },
};

/** The cash as the cash sample's row: a currency's overnight rate and cash index a day (the risk-free data model). */
export const CASH_ON_CLASSES: ClassGrid = {
  id: 'cash',
  title: 'Cash, overnight (CashRate)',
  className: 'markets::CashRate',
  text: '|markets::CashRate.all()->project(~[currency: x|$x.currency, date: x|$x.date, rate: x|$x.rate, index: x|$x.index])',
  columns: [
    { name: 'currency', type: 'String', value: at('currency') },
    { name: 'date', type: 'StrictDate', value: at('date') },
    { name: 'rate', type: 'Float', kind: 'measure', value: at('rate') },
    { name: 'index', type: 'Float', kind: 'measure', value: at('index') },
  ],
  rows: ['currency'],
  configuration: { rate: { aggregateFn: 'average' }, index: { aggregateFn: 'average' } },
};

export const CLASS_GRIDS: readonly ClassGrid[] = [PRICES_ON_CLASSES, RATES_ON_CLASSES, MACRO_ON_CLASSES, CASH_ON_CLASSES];

/** The grid's query as the protocol tree a cube's snapshot reads (the test holds it to the text parsed). */
export function classQueryTree(g: ClassGrid): ValueSpecification {
  return projectObjects(fn('getAll', element(g.className)), g.columns.map((c) => derive(c.name, lambda(['x'], c.value(variable('x')))))).node;
}

/** The grid's source: its saved query of the markets project, with the columns it answers. */
export function classQuerySource(g: ClassGrid): QuerySource {
  return {
    _type: 'savedQuery',
    name: g.title,
    query: { name: g.title, ...MARKETS_PROJECT, executionContext: CONTEXT, content: g.text, defaultParameterValues: [] },
    columns: g.columns.map((c) => ({ name: c.name, type: c.type })),
  };
}

/** A grid's cube, as the screen holds one it opens (cube-state.ts openedState). */
function cubeOf(g: ClassGrid): CubeDocument {
  const columns: ColumnSpec[] = g.columns.map((c) => ({ name: c.name, type: c.type, ...(c.kind ? { kind: c.kind } : {}) }));
  const snapshot: CubeSnapshot = { source: { query: classQueryTree(g) }, columns, derived: [], rows: [...g.rows], pivotOn: [], sorts: [], epoch: 1 };
  const state = openedState(snapshot, { ...DEFAULT_CONFIGURATION, reportTitle: g.title, columns: g.configuration }, TreeState.fromPaths([], false));
  return writeCube({ name: g.title, source: classQuerySource(g), snapshot: state.snapshot, configuration: state.configuration, tree: state.tree });
}

const cubes = (ids: readonly string[]): { readonly id: string; readonly cube: CubeDocument }[] =>
  ids.map((id) => ({ id, cube: cubeOf(CLASS_GRIDS.find((g) => g.id === id)!) }));

/** The markets page over the classes. */
export async function marketsOnClassesPage(): Promise<PageDocument> {
  return marketsPage({ name: 'Markets on classes', cubes: cubes(['prices', 'rates']) });
}

/** The timeseries gallery over the classes. */
export async function galleryOnClassesPage(): Promise<PageDocument> {
  return timeseriesGalleryPage({ name: 'Timeseries gallery on classes', cubes: cubes(['prices', 'rates', 'macro', 'cash']) });
}
