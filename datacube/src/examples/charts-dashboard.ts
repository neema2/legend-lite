// The example page of batch 2 (docs/DATACUBE_CHARTS_DESIGN_2026_10_09.md §3, §3.3): dashboard-grade charts over the
// sales sample, a sheet per family, each simple to complex. Built, not hand-written: the page is written by the same
// functions a saved page is (cube-document.ts writeCube, page-document.ts writePageOf), so it moves with the spec, and
// test/examples.test.ts reads it back and finds every chart drawable; test/chart-queries.test.ts plans and runs each
// chart's query on the sample.

import { fn, lambda, property, variable, element } from '../../../pure-protocol/src/index.ts';
import { CHART_VERSION, DEFAULT_OPTIONS, type ChartOptions, type ChartSpec } from '../chart-spec.ts';
import { DEFAULT_CONFIGURATION, type CubeConfiguration } from '../config.ts';
import { fileSource, writeCube } from '../cube-document.ts';
import { openedState } from '../cube-state.ts';
import type { Band, Node } from '../layout/bands.ts';
import { writePageOf, type PageDocument, type PageSheet, type PageView } from '../page-document.ts';
import { sampleById, sampleFileName } from '../samples.ts';
import type { ColumnSpec, CubeSnapshot } from '../snapshot.ts';
import { TreeState } from '../tree.ts';

/** The sample the page is drawn from, and how many of its rows. */
export const EXAMPLE_SAMPLE = 'sales';
export const EXAMPLE_ROWS = 6000;

/** The sales sample's columns as the compiler types them (test/chart-queries.test.ts holds them to DuckDB's reading). */
export const SALES_SPECS: readonly ColumnSpec[] = [
  { name: 'order_id', type: 'Integer', kind: 'dimension' },
  { name: 'sold', type: 'StrictDate' },
  { name: 'region', type: 'String' },
  { name: 'country_code', type: 'String' },
  { name: 'country', type: 'String' },
  { name: 'city', type: 'String' },
  { name: 'lat', type: 'Float', kind: 'dimension' },
  { name: 'lon', type: 'Float', kind: 'dimension' },
  { name: 'channel', type: 'String' },
  { name: 'category', type: 'String' },
  { name: 'product', type: 'String' },
  { name: 'units', type: 'Integer', kind: 'measure' },
  { name: 'revenue', type: 'Float', kind: 'measure' },
  { name: 'cost', type: 'Float', kind: 'measure' },
  { name: 'profit', type: 'Float', kind: 'measure' },
  { name: 'discount', type: 'Float', kind: 'measure' },
  { name: 'target', type: 'Float', kind: 'measure' },
];

const x = variable('x');
/** The grid's margin: its profit over its revenue, after grouping (a group-level column). */
const MARGIN = lambda(['x'], fn('divide', fn('toOne', property(x, 'profit')), fn('toOne', property(x, 'revenue'))));

/** The cube every chart of the page is a view of: the sales, by region then country, its margin beside its sums. */
export const EXAMPLE_CUBE: CubeSnapshot = {
  source: { query: element('sales') },
  columns: SALES_SPECS,
  derived: [],
  rows: ['region', 'country'],
  pivotOn: [],
  // its money and its units summed: each measure column's Aggregation, here its default (Column Properties sets another)
  groupDerived: [{ name: 'margin', lambda: MARGIN, type: 'Float' }],
  sorts: [{ column: 'revenue', direction: 'desc' }],
  epoch: 1,
};

/** Money in dollars, a margin as a percentage: the grid's formats, which every chart writes its values in. */
const CONFIGURATION: CubeConfiguration = {
  ...DEFAULT_CONFIGURATION,
  reportTitle: 'Sales',
  columns: {
    revenue: { format: { kind: 'currency', currency: 'USD', decimals: 0 } },
    profit: { format: { kind: 'currency', currency: 'USD', decimals: 0 } },
    cost: { format: { kind: 'currency', currency: 'USD', decimals: 0 } },
    target: { format: { kind: 'currency', currency: 'USD', decimals: 0 } },
    margin: { format: { kind: 'percent', decimals: 1 } },
    discount: { format: { kind: 'percent', decimals: 0 } },
  },
};

const CUBE = 'cube';

/** A frozen chart of the cube (it keeps its own grouping as the grid changes), its options over the defaults. */
export function chart(id: string, title: string, spec: Partial<Omit<ChartSpec, 'options'>> & { options?: Partial<ChartOptions> }): PageView {
  const { options, ...rest } = spec;
  return {
    id,
    kind: 'chart',
    cube: CUBE,
    title,
    spec: {
      version: CHART_VERSION,
      mark: 'bar',
      y: [{ column: 'revenue', fn: 'sum' }],
      ...rest,
      options: { ...DEFAULT_OPTIONS, ...options },
      frozen: true,
    } as ChartSpec,
  };
}

export const REVENUE = { column: 'revenue', fn: 'sum' } as const;
export const PROFIT = { column: 'profit', fn: 'sum' } as const;
export const BY_TIME = { sort: { by: 'x', direction: 'asc' }, limit: 2000 } as const;

/** Each sheet: its name, and its tiles two to a band, simplest first. */
export interface ExampleSheet {
  readonly name: string;
  readonly views: readonly PageView[];
}

const SHEETS: readonly ExampleSheet[] = [
  {
    name: 'Bar and line',
    views: [
      chart('c-bars', 'Revenue and profit by region', { x: 'region', y: [REVENUE, PROFIT] }),
      chart('c-combo', 'Revenue (bars) and margin (line, its own axis)', {
        x: 'country', y: [REVENUE, { column: 'margin', fn: 'value', mark: 'line', axis: 'right' }],
      }),
      chart('c-area', 'Revenue by month, stacked by channel', {
        mark: 'area', x: 'sold', grain: 'month', split: 'channel', options: { ...BY_TIME, stack: 'stacked' },
      }),
      chart('c-percent', 'Each region\'s revenue by category, as shares', {
        x: 'region', split: 'category', options: { stack: 'percent' },
      }),
      chart('c-horizontal', 'Units by product, largest first', {
        x: 'product', y: [{ column: 'units', fn: 'sum' }], options: { orientation: 'horizontal', labels: true },
      }),
      chart('c-three', 'Revenue (bars), profit (line) and target (area) by quarter', {
        x: 'sold', grain: 'quarter',
        y: [REVENUE, { ...PROFIT, mark: 'line' }, { column: 'target', fn: 'sum', mark: 'area' }],
        options: BY_TIME,
      }),
    ],
  },
  {
    name: 'Reference lines',
    views: [
      chart('r-target', 'Revenue by country against a target', {
        x: 'country', layers: [{ kind: 'line', value: 2_500_000, label: 'Target' }],
      }),
      chart('r-average', 'Revenue by product, with its average', {
        x: 'product', layers: [{ kind: 'stat', stat: 'average' }],
      }),
      chart('r-threshold', 'Margin by product: red below 30%', {
        x: 'product', y: [{ column: 'margin', fn: 'value' }], layers: [{ kind: 'threshold', value: 0.3 }],
        options: { labels: true },
      }),
      chart('r-extremes', 'Revenue by month, the best and the worst pinned', {
        mark: 'line', x: 'sold', grain: 'month', layers: [{ kind: 'extremes' }, { kind: 'stat', stat: 'average' }],
        options: BY_TIME,
      }),
      chart('r-band', 'Revenue by month, the fourth quarter of 2024 shaded', {
        x: 'sold', grain: 'month',
        layers: [{ kind: 'band', axis: 'x', from: '2024-10-01', to: '2024-12-01', label: 'Q4 2024' }],
        options: BY_TIME,
      }),
    ],
  },
  {
    name: 'Time',
    views: [
      chart('t-daily', 'Revenue by day (drag the slider to zoom)', { mark: 'line', x: 'sold', options: BY_TIME }),
      chart('t-weekly', 'Revenue by week, smoothed', { mark: 'line', x: 'sold', grain: 'week', options: { ...BY_TIME, smooth: true } }),
      chart('t-running', 'Running total of revenue by month, per region', {
        mark: 'line', x: 'sold', grain: 'month', split: 'region',
        y: [{ ...REVENUE, showAs: { kind: 'runningTotal' } }], options: BY_TIME,
      }),
      chart('t-moving', 'Revenue by day and its 30-day moving average', {
        mark: 'line', x: 'sold',
        y: [REVENUE, { ...REVENUE, showAs: { kind: 'movingAverage', rows: 30 } }], options: BY_TIME,
      }),
      chart('t-change', 'Revenue, % change on the month before (red when it fell)', {
        x: 'sold', grain: 'month', y: [{ ...REVENUE, showAs: { kind: 'percentChange' } }],
        layers: [{ kind: 'threshold', value: 0 }], options: BY_TIME,
      }),
      chart('t-step', 'Units by month, as steps', {
        mark: 'line', x: 'sold', grain: 'month', y: [{ column: 'units', fn: 'sum' }], options: { ...BY_TIME, step: true },
      }),
    ],
  },
  {
    name: 'Drill and brush',
    views: [
      chart('d-drill', 'Revenue by region: double-click a bar to see its countries', { x: 'region' }),
      chart('d-brush', 'Revenue by day: drag across a range to filter the grid', {
        mark: 'line', x: 'sold', options: { ...BY_TIME, brush: true },
      }),
      chart('d-scatter', 'Revenue against profit, order by order: drag around some to filter', {
        mark: 'scatter', x: 'revenue', y: [PROFIT], split: 'category', options: { brush: true },
      }),
      chart('d-click', 'Revenue by channel: click a bar to filter the grid to it', { x: 'channel' }),
    ],
  },
  {
    name: 'Formats and labels',
    views: [
      chart('f-labels', 'Revenue by category, labelled with its margin', {
        x: 'category', label: { column: 'margin', fn: 'value' }, options: { labels: true },
      }),
      chart('f-hover', 'Revenue by channel: hover for units and the average discount', {
        x: 'channel', tooltip: [{ column: 'units', fn: 'sum' }, { column: 'discount', fn: 'average' }],
      }),
      chart('f-share', 'Each product\'s share of revenue', {
        x: 'product', y: [{ ...REVENUE, showAs: { kind: 'percentOfTotal' } }], options: { labels: true },
      }),
      chart('f-rank', 'Revenue by country, each labelled with its rank', {
        x: 'country', label: { ...REVENUE, showAs: { kind: 'rank' } }, options: { orientation: 'horizontal', labels: true },
      }),
      chart('f-log', 'Revenue by city, on a log axis', { x: 'city', options: { logAxis: true } }),
      chart('f-patterns', 'Revenue by category, with pattern fills and the toolbox', {
        x: 'region', split: 'category', options: { decal: true, toolbox: true, labelRotate: 45 },
      }),
    ],
  },
  {
    name: 'Bins and periods',
    views: [
      chart('b-discount', 'Orders by discount', { x: 'discount', bins: { width: 0.05 }, y: [{ column: 'order_id', fn: 'count' }], options: BY_TIME }),
      chart('b-revenue', 'Orders by size of order, in 12 bins', {
        x: 'revenue', bins: { count: 12 }, y: [{ column: 'order_id', fn: 'count' }], options: BY_TIME,
      }),
      chart('b-quarter', 'Revenue by quarter', { x: 'sold', grain: 'quarter', options: BY_TIME }),
      chart('b-year', 'Revenue and profit by year', { x: 'sold', grain: 'year', y: [REVENUE, PROFIT], options: BY_TIME }),
    ],
  },
];

/** Tiles two to a band, half a screen each. */
function bandsOf(tiles: readonly string[]): Band[] {
  const out: Band[] = [];
  for (let i = 0; i < tiles.length; i += 2) {
    const pair = tiles.slice(i, i + 2);
    const node: Node = pair.length === 1 ? { tile: pair[0]! }
      : { split: 'row', parts: pair.map((tile) => ({ node: { tile }, size: 1 / pair.length })) };
    out.push({ height: 0.5, node });
  }
  return out;
}

/** The page: its sheets of charts, and a last sheet of the grid they are views of. */
export async function chartsDashboardPage(): Promise<PageDocument> {
  return salesPage('Chart examples', SHEETS);
}

/**
 * A page of examples over the sales sample: `sheets` of charts, two to a band, and a last sheet of the grid they are
 * views of -- its groups open at `expanded` (a sunburst or a tree draws the grid's rows as shown).
 */
export async function salesPage(name: string, sheets: readonly ExampleSheet[], expanded: readonly (readonly string[])[] = []): Promise<PageDocument> {
  const sample = sampleById(EXAMPLE_SAMPLE);
  if (!sample) throw new Error(`no sample '${EXAMPLE_SAMPLE}'`);
  const bytes = new TextEncoder().encode(sample.build(EXAMPLE_ROWS));
  const source = await fileSource(
    { name: sampleFileName(sample), size: bytes.length, arrayBuffer: async () => bytes.buffer as ArrayBuffer },
    'csv',
    SALES_SPECS,
    { id: EXAMPLE_SAMPLE, rows: EXAMPLE_ROWS },
  );
  // written as the screen holds the cube it opens (cube-state.ts openedState): a page written unfolded was reopened and
  // saved as another document (the saved-page corpus holds the example pages to it)
  const state = openedState(EXAMPLE_CUBE, CONFIGURATION, TreeState.fromPaths(expanded.map((p) => [...p]), false));
  const cube = writeCube({ name, source, snapshot: state.snapshot, configuration: state.configuration, tree: state.tree });
  const grid: PageView = { id: 'grid', kind: 'grid', cube: CUBE, title: 'The sales' };
  const placed: PageSheet[] = sheets.map((s, i) => ({
    id: `sheet-${i + 1}`,
    name: s.name,
    layout: { kind: 'bands', fit: false, bands: bandsOf(s.views.map((v) => v.id)) },
  }));
  placed.push({ id: `sheet-${sheets.length + 1}`, name: 'The data', layout: { kind: 'bands', fit: true, bands: [{ height: 1, node: { tile: 'grid' } }] } });
  return writePageOf({
    name,
    cubes: [{ id: CUBE, cube }],
    views: { views: [grid, ...sheets.flatMap((s) => s.views)], sheets: placed },
  });
}
