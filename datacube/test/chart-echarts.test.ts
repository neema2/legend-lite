// Charts of a cube, drawn by ECharts: the query a chart asks (the cube's
// own, regrouped), the drawing it makes of the rows, and what a click on
// a mark means. The drawing is rendered for real, to SVG, in node.

import assert from 'node:assert/strict';
import { DEFAULT_CONFIGURATION, applyToSnapshot, fromSnapshot } from '../src/config.ts';
import { describe, it } from 'node:test';

import { toJson, element, fn, lambda, property, variable } from '../../pure-protocol/src/index.ts';
import {
  chartProblems,
  chartQuery,
  chartSnapshot,
  defaultChart,
  chartValuesAsColumns,
  followCube,
  KPI_TREND_LIMIT,
  kpiQueries,
  measureName,
  chartColumns,
  axisName,
  markConditions,
  TIME_LIMIT,
  AXIS_UPPER,
  type ChartMeasure,
  type ChartSpec,
} from '../src/chart-spec.ts';
import { chartCsv, chartOption, kpiDrawing, MAX_SERIES, periodText, treemapOfGrid, type GridShown, type LabelOf } from '../src/chart-option.ts';
import * as echarts from 'echarts/core';
import { SVGRenderer } from 'echarts/renderers';
import { drawnExtents, heldOption } from '../src/chart-render.ts';   // registers the charts and components the page uses
import type { EChartsOption } from 'echarts';
import type { ChartDrawing } from '../src/chart-option.ts';

echarts.use([SVGRenderer]);

/** The drawing as an SVG document, with no DOM: what the page draws on canvas. */
function chartSvg(drawing: ChartDrawing, width = 640, height = 400): string {
  const chart = echarts.init(null, null, { renderer: 'svg', ssr: true, width, height });
  try {
    chart.setOption({ ...(drawing.option as EChartsOption), animation: false });
    return chart.renderToSVGString();
  } finally {
    chart.dispose();
  }
}
import type { ResultTable } from '../../engine-client/src/result.ts';
import type { CubeSnapshot } from '../src/snapshot.ts';

const CUBE: CubeSnapshot = {
  source: { query: element('trades') },
  columns: [
    { name: 'region', type: 'String' },
    { name: 'desk', type: 'String' },
    { name: 'trade_date', type: 'StrictDate' },
    { name: 'notional', type: 'Float' },
    { name: 'qty', type: 'Integer', kind: 'measure' },
  ],
  derived: [],
  rows: ['region', 'desk'],
  pivotOn: ['trade_date'],
  sorts: [],
  filter: { kind: 'condition', column: 'desk', operator: 'equal', value: 'FX' },
  epoch: 1,
};

/** The cube with its notional's Aggregation set to average (Column Properties, folded into its columns). */
const AVERAGED: CubeSnapshot = {
  ...CUBE, columns: CUBE.columns.map((c) => (c.name === 'notional' ? { ...c, aggregate: 'average' as const } : c)),
};

const BAR: ChartSpec = {
  version: 2,
  mark: 'bar',
  x: 'region',
  y: [{ column: 'notional', fn: 'sum' }],
  options: {
    orientation: 'vertical', stack: 'none', sort: { by: 'y', direction: 'desc' },
    limit: 50, labels: false, legend: 'top',
  },
};

function table(columns: { name: string; type: string; values: (string | number | null)[] }[]): ResultTable {
  return { columns, rowCount: columns[0]?.values.length ?? 0, epoch: 1, elapsedMs: 0 };
}

/** The relation functions a query calls, outermost last. */
function chain(query: unknown): string[] {
  const out: string[] = [];
  const walk = (n: unknown): void => {
    if (!n || typeof n !== 'object') return;
    const o = n as Record<string, unknown>;
    if (o['_type'] === 'func' && typeof o['function'] === 'string') out.push(o['function']);
    for (const v of Object.values(o)) (Array.isArray(v) ? v.forEach(walk) : walk(v));
  };
  walk(JSON.parse(toJson(query as never)));
  return out.reverse().filter((f) => ['filter', 'select', 'groupBy', 'sort', 'limit', 'pivot'].includes(f));
}

describe('the chart\'s query', () => {
  it('is the cube\'s own, regrouped: its filter kept, its pivot and other columns dropped', () => {
    const s = chartSnapshot(CUBE, { ...BAR, split: 'desk' });
    assert.deepEqual(s.rows, ['region', 'desk']);
    assert.deepEqual(s.pivotOn, []);
    assert.deepEqual(s.measures, [{ name: 'sum_notional', column: 'notional', fn: 'sum' }]);
    assert.deepEqual(s.filter, CUBE.filter);
    assert.deepEqual(s.columns.map((c) => c.name).sort(), ['desk', 'notional', 'region']);
    const { query, scope } = chartQuery(CUBE, BAR);
    assert.deepEqual(chain(query), ['filter', 'select', 'groupBy', 'sort', 'limit']);
    assert.equal(scope.limit, 50);
  });

  it('orders by the value for a top N, or by the category', () => {
    assert.deepEqual(chartSnapshot(CUBE, BAR).sorts, [{ column: 'sum_notional', direction: 'desc' }, { column: 'region', direction: 'asc' }], 'ties by the category');
    const byX = { ...BAR, options: { ...BAR.options, sort: { by: 'x', direction: 'asc' } } } as ChartSpec;
    assert.deepEqual(chartSnapshot(CUBE, byX).sorts, [{ column: 'region', direction: 'asc' }]);
  });

  it('asks a scatter for rows, not groups', () => {
    const scatter: ChartSpec = { ...BAR, mark: 'scatter', x: 'notional', y: [{ column: 'qty', fn: 'sum' }] };
    const { query, snapshot } = chartQuery(CUBE, scatter);
    assert.deepEqual(snapshot.rows, []);
    assert.deepEqual(snapshot.measures, []);
    assert.deepEqual(chain(query), ['filter', 'select', 'limit']);
  });

  it('names a count as count', () => {
    assert.equal(measureName({ column: 'x', fn: 'count' }), 'count');
  });
});

describe('a group-level column, plotted as it is (version 2: fn \'value\')', () => {
  // the cube's margin: its two measure columns' ratio, after grouping (each on its Aggregation); and a running total of
  // its notional, a window
  const ratio = lambda(['x'], fn('divide', property(variable('x'), 'pnl'), property(variable('x'), 'notional')));
  const GROUPED: CubeSnapshot = {
    ...CUBE,
    columns: [...CUBE.columns, { name: 'pnl', type: 'Float' }],
    groupDerived: [
      { name: 'margin', lambda: ratio, type: 'Float' },
      { name: 'running', window: { fn: 'sum', column: 'notional', partition: [], order: [{ column: 'region', direction: 'asc' }], frame: 'running' } },
      { name: 'smallest_desk', childAggregate: { fn: 'min', of: 'notional' } },
    ],
  };
  const MARGIN: ChartSpec = { ...BAR, y: [{ column: 'margin', fn: 'value' }] };

  it('is offered beside the number columns; a child aggregate (a second query per level) is not', () => {
    assert.deepEqual(chartColumns(GROUPED).groupLevel.map((d) => d.name), ['margin', 'running']);
  });

  it('keeps its name, and brings the measures it reads under the names it reads them by -- and nothing else', () => {
    assert.equal(measureName({ column: 'margin', fn: 'value' }), 'margin');
    const s = chartSnapshot(GROUPED, MARGIN);
    assert.deepEqual(s.groupDerived?.map((d) => d.name), ['margin']);
    assert.deepEqual(s.measures?.map((m) => m.name), ['pnl', 'notional']);
    assert.deepEqual(s.columns.map((c) => c.name).sort(), ['desk', 'notional', 'pnl', 'region']);
    assert.deepEqual(s.sorts, [{ column: 'margin', direction: 'desc' }, { column: 'region', direction: 'asc' }]);
    // grouped, then extended: the ratio is of the groups' sums, never an average of the rows' ratios
    const { query } = chartQuery(GROUPED, MARGIN);
    const text = toJson(query as never);
    assert.ok(text.indexOf('"groupBy"') < text.indexOf('"margin"'), 'the column is extended after the grouping');
  });

  it('a window over the chart\'s rows comes with what it reads', () => {
    const s = chartSnapshot(GROUPED, { ...BAR, y: [{ column: 'notional', fn: 'sum' }, { column: 'running', fn: 'value', mark: 'line' }] });
    assert.deepEqual(s.groupDerived?.map((d) => d.name), ['running']);
    assert.deepEqual(s.measures?.map((m) => m.name), ['sum_notional', 'notional']);
  });

  it('the chart\'s own calculations: a row-stage one is a dimension it can group by, a group-stage one it can plot', () => {
    const spec: ChartSpec = {
      ...BAR,
      x: 'big',
      y: [{ column: 'notional', fn: 'sum' }, { column: 'share', fn: 'value' }],
      calcs: [
        { stage: 'row', column: { name: 'big', lambda: lambda(['x'], fn('greaterThan', property(variable('x'), 'notional'), { _type: 'integer', value: 100 } as never)), kind: 'dimension', type: 'Boolean' } },
        { stage: 'group', column: { name: 'share', window: { fn: 'sum', column: 'sum_notional', partition: [], order: [], frame: 'partition' } } },
      ],
    };
    assert.deepEqual(chartProblems(spec, GROUPED), []);
    assert.deepEqual(chartColumns(GROUPED, spec).dimensions.map((c) => c.name).filter((n) => n === 'big'), ['big']);
    const s = chartSnapshot(GROUPED, spec);
    assert.deepEqual(s.derived.map((d) => d.name), ['big']);
    assert.deepEqual(s.groupDerived?.map((d) => d.name), ['share']);
    assert.deepEqual(s.rows, ['big']);
  });

  it('what stops one: not a group-level column, or on a scatter (it plots rows)', () => {
    assert.deepEqual(chartProblems({ ...BAR, y: [{ column: 'nope', fn: 'value' }] }, GROUPED), ['The cube has no group-level column \'nope\'.']);
    assert.match(chartProblems({ ...MARGIN, mark: 'scatter', x: 'notional' }, GROUPED).join(' '), /A scatter plots rows/);
  });
});

describe('the first chart of a cube', () => {
  it('comes from its grouping: the first row dimension across, the second as the split', () => {
    const spec = defaultChart(CUBE)!;
    assert.equal(spec.mark, 'bar');
    assert.equal(spec.x, 'region');
    assert.equal(spec.split, 'desk');
    assert.deepEqual(spec.y, [{ column: 'notional', fn: 'sum' }]);
  });

  it('sums a fraction the cube treats as a measure before an integer one', () => {
    const uploaded: CubeSnapshot = {
      ...CUBE,
      columns: [{ name: 'trade_id', type: 'Integer' }, ...CUBE.columns],
    };
    assert.deepEqual(defaultChart(uploaded)!.y, [{ column: 'notional', fn: 'sum' }]);
    // nothing to sum at all: count the rows
    const textOnly: CubeSnapshot = { ...uploaded, columns: uploaded.columns.filter((c) => c.type === 'String') };
    assert.deepEqual(defaultChart(textOnly)!.y, [{ column: 'region', fn: 'count' }]);
  });

  it('is a line, in date order, when a date is across', () => {
    const spec = defaultChart({ ...CUBE, rows: ['trade_date'] })!;
    assert.equal(spec.mark, 'line');
    assert.deepEqual(spec.options.sort, { by: 'x', direction: 'asc' });
  });
});

describe('what stops a chart', () => {
  it('names a missing column and a heatmap without rows', () => {
    assert.deepEqual(chartProblems(BAR, CUBE), []);
    assert.match(chartProblems({ ...BAR, x: 'nope' }, CUBE).join(), /no column 'nope'/);
    assert.match(chartProblems({ ...BAR, mark: 'heatmap' }, CUBE).join(), /second column/);
  });
});

const REGIONS = table([
  { name: 'region', type: 'String', values: ['EMEA', 'AMER', 'APAC'] },
  { name: 'sum_notional', type: 'Float', values: [300, 200, 100] },
]);

describe('the drawing', () => {
  it('draws a bar per category, rounded at the data end, no wider than 24px', () => {
    const d = chartOption(BAR, REGIONS);
    const o = d.option as { xAxis: { data: string[] }; series: { type: string; data: number[]; barMaxWidth: number; itemStyle: { borderRadius: number[] } }[] };
    assert.deepEqual(o.xAxis.data, ['EMEA', 'AMER', 'APAC']);
    assert.equal(o.series.length, 1);
    assert.deepEqual(o.series[0]!.data, [300, 200, 100]);
    assert.equal(o.series[0]!.barMaxWidth, 24);
    assert.deepEqual(o.series[0]!.itemStyle.borderRadius, [4, 4, 0, 0]);
  });

  it('renders to SVG in node: one bar per category, each labelled', () => {
    const svg = chartSvg(chartOption(BAR, REGIONS));
    assert.match(svg, /^<svg/);
    for (const r of ['EMEA', 'AMER', 'APAC']) assert.ok(svg.includes(r), `no label ${r}`);
  });

  it('says what it shows, for a screen reader', () => {
    assert.equal(chartOption(BAR, REGIONS).description,
      'Bar chart of sum_notional by region: 3 rows.');
    assert.equal(chartOption({ ...BAR, split: 'desk' }, REGIONS).description,
      'Bar chart of sum_notional by region, split by desk: 3 rows.');
  });

  it('splits into one series per value, and maps a click back to both keys', () => {
    const rows = table([
      { name: 'region', type: 'String', values: ['EMEA', 'EMEA', 'AMER'] },
      { name: 'desk', type: 'String', values: ['FX', 'Rates', 'FX'] },
      { name: 'sum_notional', type: 'Float', values: [10, 20, 30] },
    ]);
    const d = chartOption({ ...BAR, split: 'desk' }, rows);
    const o = d.option as { series: { name: string; data: (number | null)[] }[]; legend: { show: boolean } };
    assert.deepEqual(o.series.map((s) => s.name), ['FX', 'Rates']);
    assert.deepEqual(o.series[0]!.data, [10, 30]);
    assert.deepEqual(o.series[1]!.data, [20, null]);
    assert.equal(o.legend.show, true);
    assert.deepEqual(d.keyAt(1, 0), { region: 'EMEA', desk: 'Rates' });
    assert.ok(chartSvg(d).length > 0);
  });

  it('keeps the NULL group as a key, not as text', () => {
    const rows = table([
      { name: 'region', type: 'String', values: [null, 'EMEA'] },
      { name: 'sum_notional', type: 'Float', values: [5, 6] },
    ]);
    assert.deepEqual(chartOption(BAR, rows).keyAt(0, 0), { region: null });
  });

  it('scales a 100% stack to each category\'s total', () => {
    const rows = table([
      { name: 'region', type: 'String', values: ['EMEA', 'EMEA'] },
      { name: 'desk', type: 'String', values: ['FX', 'Rates'] },
      { name: 'sum_notional', type: 'Float', values: [1, 3] },
    ]);
    const spec: ChartSpec = { ...BAR, split: 'desk', options: { ...BAR.options, stack: 'percent' } };
    const o = chartOption(spec, rows).option as { series: { data: number[]; stack: string }[] };
    assert.deepEqual(o.series.map((s) => s.data[0]), [25, 75]);
    assert.ok(o.series.every((s) => s.stack === 'total-0'), 'one stack per value axis');
  });

  it('shows at most eight series and says so', () => {
    const n = 11;
    const rows = table([
      { name: 'region', type: 'String', values: Array.from({ length: n }, () => 'EMEA') },
      { name: 'desk', type: 'String', values: Array.from({ length: n }, (_, i) => `d${i}`) },
      { name: 'sum_notional', type: 'Float', values: Array.from({ length: n }, (_, i) => i) },
    ]);
    const d = chartOption({ ...BAR, split: 'desk' }, rows);
    assert.equal((d.option as { series: unknown[] }).series.length, MAX_SERIES);
    assert.match(d.notes.join(), /first 8 of 11/);
  });

  it('draws a pie, a heatmap and a scatter, each to SVG', () => {
    const pie = chartOption({ ...BAR, mark: 'pie' }, REGIONS);
    assert.deepEqual(pie.keyAt(0, 2), { region: 'APAC' });
    assert.match(chartSvg(pie), /<path/);

    const cells = table([
      { name: 'region', type: 'String', values: ['EMEA', 'AMER'] },
      { name: 'desk', type: 'String', values: ['FX', 'Rates'] },
      { name: 'sum_notional', type: 'Float', values: [1, 9] },
    ]);
    const heat = chartOption({ ...BAR, mark: 'heatmap', split: 'desk' }, cells);
    assert.deepEqual(heat.keyAt(0, 1), { region: 'AMER', desk: 'Rates' });
    assert.match(chartSvg(heat), /<svg/);

    const points = table([
      { name: 'notional', type: 'Float', values: [1, 2, 3] },
      { name: 'qty', type: 'Integer', values: [10, 20, 30] },
    ]);
    const scatter = chartOption({ ...BAR, mark: 'scatter', x: 'notional', y: [{ column: 'qty', fn: 'sum' }] }, points);
    assert.equal(scatter.keyAt(0, 0), null, 'a point is not a group: nothing to filter to');
    assert.match(chartSvg(scatter), /<svg/);
  });

  // THE TREEMAP DRAWS THE GRID AS SHOWN (the old treemap's way, the user 2026-09-30)
  const grid = (expanded: boolean): GridShown => ({
    rows: table([
      { name: '__tree', type: 'String', values: expanded ? ['', 'EMEA', 'FX', 'Rates', 'AMER'] : ['', 'EMEA', 'AMER'] },
      { name: 'region', type: 'String', values: expanded ? [null, 'EMEA', 'EMEA', 'EMEA', 'AMER'] : [null, 'EMEA', 'AMER'] },
      { name: 'sum_notional', type: 'Float', values: expanded ? [60, 30, 10, 20, 30] : [60, 30, 30] },
      { name: 'hidden_qty', type: 'Integer', values: expanded ? [1, 1, 1, 1, 1] : [1, 1, 1] },
    ]),
    tree: expanded ? [
      { path: [], level: 0, isGroup: true, expanded: true, isTotal: true },
      { path: ['EMEA'], level: 1, isGroup: true, expanded: true, isTotal: false },
      { path: ['EMEA', 'FX'], level: 2, isGroup: false, expanded: false, isTotal: false },
      { path: ['EMEA', 'Rates'], level: 2, isGroup: false, expanded: false, isTotal: false },
      { path: ['AMER'], level: 1, isGroup: true, expanded: false, isTotal: false },
    ] : [
      { path: [], level: 0, isGroup: true, expanded: true, isTotal: true },
      { path: ['EMEA'], level: 1, isGroup: true, expanded: false, isTotal: false },
      { path: ['AMER'], level: 1, isGroup: true, expanded: false, isTotal: false },
    ],
    // what the grid shows: hidden_qty is not on screen
    leaves: [{ name: '__tree', type: 'String' }, { name: 'sum_notional', type: 'Float', measure: true }],
    levels: ['region', 'desk'],
  });
  type Node = { name: string; value?: number; key: Record<string, unknown>; children?: Node[] };
  const nodes = (d: ChartDrawing): Node[] => (d.option as { series: { data: Node[] }[] }).series[0]!.data;

  it('a treemap is the grid\'s visible rows: a collapsed group a block, the total the whole', () => {
    const d = treemapOfGrid(grid(false));
    assert.deepEqual(nodes(d).map((n) => [n.name, n.value]), [['EMEA', 30], ['AMER', 30]]);
    assert.match(chartSvg(d), /<svg/);
  });

  it('an expanded group is a box around its visible children; a click names the row\'s path', () => {
    const d = treemapOfGrid(grid(true));
    const [emea, amer] = nodes(d);
    assert.deepEqual([emea!.name, emea!.children?.map((c) => [c.name, c.value])], ['EMEA', [['FX', 10], ['Rates', 20]]]);
    assert.deepEqual([amer!.name, amer!.value], ['AMER', 30]);
    assert.deepEqual(d.keyAt(0, 0, emea!.children![1]), { region: 'EMEA', desk: 'Rates' });
    assert.deepEqual(d.keyAt(0, 0, amer), { region: 'AMER' });
    assert.match(chartSvg(d), /<svg/);
  });

  it('a treemap drops zero and negative rows, and says so', () => {
    const shown = grid(false);
    const rows = { ...shown.rows, columns: shown.rows.columns.map((c) => (c.name === 'sum_notional' ? { ...c, values: [60, 30, -5] } : c)) };
    const d = treemapOfGrid({ ...shown, rows });
    assert.deepEqual(nodes(d).map((n) => n.name), ['EMEA']);
    assert.match(d.notes.join(), /1 row not shown: zero or negative cannot have an area/);
  });

  it('a treemap\'s tooltip says what the grid says (its column\'s format)', () => {
    const money = (v: unknown, column: string): string => (column === 'sum_notional' && typeof v === 'number'
      ? v.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 }) : String(v));
    const d = treemapOfGrid(grid(false), undefined, money as never);
    const tip = (d.option as { tooltip: { formatter: (p: { name: string; data: { text?: string } }) => string } }).tooltip;
    assert.equal(tip.formatter({ name: 'EMEA', data: nodes(d)[0] as { text?: string } }), 'EMEA: 30.00');
  });

  it('chartOption refuses a treemap: it is drawn from the grid, never a query of its own', () => {
    assert.throws(() => chartOption({ ...BAR, mark: 'treemap' }, REGIONS), /grid's rows as shown/);
  });
});

describe('a live chart', () => {
  const live = defaultChart(CUBE)!;

  it('follows the cube\'s pivots: first row group across, the next as the split', () => {
    const regrouped = followCube(live, { ...CUBE, rows: ['desk'], pivotOn: ['region'] });
    assert.equal(regrouped.x, 'desk');
    assert.equal(regrouped.split, 'region', 'the first column label, when there is one row group');
    const one = followCube(live, { ...CUBE, rows: ['desk'], pivotOn: [] });
    assert.equal(one.x, 'desk');
    assert.equal(one.split, undefined);
  });

  it('runs a date in time order, and a category largest first', () => {
    const byDate = followCube(live, { ...CUBE, rows: ['trade_date'], pivotOn: [] });
    assert.deepEqual(byDate.options.sort, { by: 'x', direction: 'asc' });
    assert.deepEqual(followCube(byDate, { ...CUBE, rows: ['desk'], pivotOn: [] }).options.sort,
      { by: 'y', direction: 'desc' });
  });

  it('plots each value on the grid\'s Aggregation of its column (Column Properties)', () => {
    assert.deepEqual(followCube(live, AVERAGED).y, [{ column: 'notional', fn: 'average' }]);
    // a weighted average needs a weight a chart's value has not: the chart keeps its own
    const weighted: CubeSnapshot = { ...CUBE, columns: CUBE.columns.map((c) => (c.name === 'notional'
      ? { ...c, aggregate: 'wavg' as const, aggregateWeight: 'qty' } : c)) };
    assert.deepEqual(followCube(live, weighted).y, live.y);
    // a dimension the grid does not aggregate: a count of desks stays a count
    const counted: ChartSpec = { ...live, y: [{ column: 'desk', fn: 'count' }] };
    assert.deepEqual(followCube(counted, AVERAGED).y, counted.y);
  });

  it('reads a value back from the grid column that carries it (Open in Grid\'s Update)', () => {
    const two: ChartSpec = { ...live, y: [{ column: 'notional', fn: 'sum' }, { column: 'notional', fn: 'max' }] };
    // the editing grid made the second value a calculated column of its own, its Aggregation since set to min
    const grid: CubeSnapshot = { ...CUBE, derived: [{ name: 'max_notional', kind: 'measure', type: 'Float', aggregate: 'min',
      lambda: lambda(['x'], property(variable('x'), 'notional')) }] };
    const carriedBy = (m: ChartMeasure): string => (m.fn === 'max' ? 'max_notional' : m.column);
    assert.deepEqual(followCube(two, grid, carriedBy).y, [{ column: 'notional', fn: 'sum' }, { column: 'notional', fn: 'min' }]);
  });

  it('keeps a value its grid carries as it is: a saved page\'s min and max of one column, a count made a column', () => {
    // a measures list converted on open: notional's Aggregation min, a copy of notional at max, a copy of qty at count
    const copy = (name: string, column: string, aggregate: 'max' | 'count'): CubeSnapshot['derived'][number] => ({
      name, kind: 'measure', type: 'Float', aggregate, lambda: lambda(['x'], property(variable('x'), column)) });
    const grid: CubeSnapshot = { ...CUBE,
      columns: CUBE.columns.map((c) => (c.name === 'notional' ? { ...c, aggregate: 'min' as const } : c)),
      derived: [copy('top', 'notional', 'max'), copy('trades', 'qty', 'count')] };
    const saved: ChartSpec = { ...live,
      y: [{ column: 'notional', fn: 'min' }, { column: 'notional', fn: 'max' }, { column: 'qty', fn: 'count' }] };
    assert.deepEqual(followCube(saved, grid).y, saved.y, 'each value carried: none collapses onto another');
    // a value no column carries follows its column's Aggregation
    assert.deepEqual(followCube({ ...saved, y: [{ column: 'notional', fn: 'average' }] }, grid).y,
      [{ column: 'notional', fn: 'min' }]);
  });

  it('opens a chart\'s values as Column Properties, each pair once: two values of one pair share one carrier', () => {
    const y: ChartSpec['y'] = [
      { column: 'notional', fn: 'sum' },
      { column: 'notional', fn: 'sum', showAs: { kind: 'runningTotal' } },
      { column: 'notional', fn: 'max' },
    ];
    const made = chartValuesAsColumns({ snapshot: CUBE, config: DEFAULT_CONFIGURATION }, y);
    // the pair once, notional's own Aggregation; the max a calculated column named for it; no copy for the running total
    assert.deepEqual(made.draft.snapshot.derived.map((d) => d.name), [...CUBE.derived.map((d) => d.name), measureName(y[2]!)]);
    assert.deepEqual(y.map(made.carriedBy), ['notional', 'notional', measureName(y[2]!)]);
  });

  it('keeps two values of one column apart when the grid carries one of them: neither follows onto the other', () => {
    // a max added beside the sum on the screen, the grid's notional summed and no copy at max: followed onto the sum, the
    // chart would plot the sum twice -- and, as a following chart keeps what it follows, never show its max again
    const two: ChartSpec = { ...live, y: [{ column: 'notional', fn: 'sum' }, { column: 'notional', fn: 'max' }] };
    assert.deepEqual(followCube(two, CUBE).y, two.y);
    // a copy carrying the max, its Aggregation set to min and then undone: the max comes back with the undo
    const copied = (aggregate: 'max' | 'min'): CubeSnapshot => ({ ...CUBE, derived: [{ name: 'top', kind: 'measure', type: 'Float',
      aggregate, lambda: lambda(['x'], property(variable('x'), 'notional')) }] });
    const atMin = followCube(two, copied('min'));
    assert.deepEqual(atMin.y, two.y);
    assert.deepEqual(followCube(atMin, copied('max')).y, two.y);
  });

  it('stays as it is when frozen, a scatter, or when nothing is grouped', () => {
    const frozen = { ...live, frozen: true };
    assert.equal(followCube(frozen, { ...CUBE, rows: ['desk'] }), frozen);
    const scatter: ChartSpec = { ...live, mark: 'scatter' };
    assert.equal(followCube(scatter, { ...CUBE, rows: ['desk'] }), scatter);
    assert.equal(followCube(live, { ...CUBE, rows: [], pivotOn: [] }), live);
  });

  it('answers the same object when nothing changed (no redraw of the form)', () => {
    const now = followCube(live, CUBE);
    assert.equal(followCube(now, CUBE), now);
  });
});

describe('Open in Grid, then Update with the editing grid as it opened', () => {
  /** A chart's values after an untouched Update (page/cube-page.ts): opened as the editing grid's columns, then followed back. */
  const updated = (spec: ChartSpec, source: CubeSnapshot): ChartSpec['y'] => {
    const made = chartValuesAsColumns({
      snapshot: { ...source, rows: [spec.x!, ...(spec.split !== undefined ? [spec.split] : [])], pivotOn: [] },
      config: fromSnapshot(source, DEFAULT_CONFIGURATION),
    }, spec.y);
    const { frozen: _f, ...following } = spec;
    return followCube(following, applyToSnapshot(made.draft.snapshot, made.draft.config), made.carriedBy).y;
  };
  const frozen = (y: ChartSpec['y'], more: Partial<ChartSpec> = {}): ChartSpec => ({ ...BAR, ...more, y, frozen: true });

  it('keeps a value of the column across: a histogram counts the column it bins', () => {
    const histogram = frozen([{ column: 'notional', fn: 'count' }], { x: 'notional', bins: { count: 5 } });
    assert.deepEqual(updated(histogram, CUBE), histogram.y);
  });

  it('reads a value back from the copy the grid has already: a converted page\'s max of a summed column', () => {
    const source: CubeSnapshot = { ...CUBE, derived: [{ name: 'max_notional', kind: 'measure', type: 'Float', aggregate: 'max',
      lambda: lambda(['x'], property(variable('x'), 'notional')) }] };
    const chart = frozen([{ column: 'notional', fn: 'sum' }, { column: 'notional', fn: 'max' }]);
    assert.deepEqual(updated(chart, source), chart.y);
  });

  it('gives each count a column of its own: two columns\' counts are not both "count"', () => {
    const chart = frozen([{ column: 'notional', fn: 'sum' }, { column: 'notional', fn: 'count' },
      { column: 'qty', fn: 'sum' }, { column: 'qty', fn: 'count' }]);
    assert.deepEqual(updated(chart, CUBE), chart.y);
  });
});

describe('a KPI (docs/DATACUBE_CHARTS_DESIGN_2026_10_09.md, batch 1)', () => {
  const { x: _across, ...bare } = BAR;
  const kpi: ChartSpec = { ...bare, mark: 'kpi' };

  it('its value is the measure over every row of the cube\'s query, no grouping; its trend, by its column, latest first', () => {
    const { total, trend } = kpiQueries(CUBE, kpi);
    assert.equal(trend, undefined, 'no column across, no trend');
    assert.deepEqual(total.snapshot.rows, []);
    assert.deepEqual(total.snapshot.measures, [{ name: 'sum_notional', column: 'notional', fn: 'sum' }]);
    assert.deepEqual(total.snapshot.filter, CUBE.filter, 'the cube\'s filter kept');
    assert.ok(chain(total.query).includes('groupBy'), 'one row: the whole');
    const withTrend = kpiQueries(CUBE, { ...kpi, x: 'trade_date' });
    assert.deepEqual(withTrend.trend!.snapshot.rows, ['trade_date']);
    assert.deepEqual(withTrend.trend!.snapshot.sorts, [{ column: 'trade_date', direction: 'desc' }], 'the latest first');
    assert.equal(withTrend.trend!.scope.limit, KPI_TREND_LIMIT + 1, 'one past the most it draws: it says when there were more');
    assert.deepEqual(withTrend.total.snapshot.rows, [], 'its value is still the whole');
  });

  it('needs no column across; follows the cube\'s measure, never its grouping', () => {
    assert.deepEqual(chartProblems(kpi, CUBE), []);
    const followed = followCube({ ...kpi, x: 'trade_date' }, AVERAGED);
    assert.deepEqual(followed.y, [{ column: 'notional', fn: 'average' }]);
    assert.equal(followed.x, 'trade_date', 'its trend\'s column is its own');
  });

  it('says its number as the grid writes it, against its target, good on the side it should be', () => {
    const total = table([{ name: 'sum_notional', type: 'Float', values: [1500] }]);
    const write = (v: unknown) => `$${String(v)}`;
    assert.equal(kpiDrawing(kpi, total, null, undefined, write).value, '$1500');
    assert.equal(kpiDrawing(kpi, total, null).target, undefined, 'no target, nothing against it');
    const above = kpiDrawing({ ...kpi, options: { ...kpi.options, target: 1000 } }, total, null, undefined, write);
    assert.deepEqual(above.target, { text: '▲ 50% above target ($1000)', good: true });
    const lower = kpiDrawing({ ...kpi, options: { ...kpi.options, target: 1000, better: 'lower' } }, total, null);
    assert.equal(lower.target!.good, false, 'above a target that should not be passed is bad');
    const below = kpiDrawing({ ...kpi, options: { ...kpi.options, target: 1600 } }, total, null);
    assert.deepEqual(below.target, { text: '▼ 6.3% below target (1600)', good: false });
    assert.equal(kpiDrawing({ ...kpi, options: { ...kpi.options, target: 1500 } }, total, null).target!.text, 'On target (1500)');
    assert.equal(kpiDrawing({ ...kpi, options: { ...kpi.options, target: 1 } }, total, null).target!.text, '\u25B2 1,500\u00D7 the target (1)',
      'far above: how many times the target');
    assert.equal(kpiDrawing(kpi, table([{ name: 'sum_notional', type: 'Float', values: [null] }]), null).value, '—');
    assert.match(above.description, /^KPI: sum of notional is \$1500; ▲ 50% above target/);
  });

  it('draws its trend as a line and nothing else, oldest first', () => {
    const total = table([{ name: 'sum_notional', type: 'Float', values: [6] }]);
    // as asked: the latest first
    const trend = table([
      { name: 'trade_date', type: 'StrictDate', values: ['2024-01-03', '2024-01-02', '2024-01-01'] },
      { name: 'sum_notional', type: 'Float', values: [2, 3, 1] },
    ]);
    const drawn = kpiDrawing({ ...kpi, x: 'trade_date' }, total, trend);
    const option = drawn.trend!.option as { series: { type: string; data: number[] }[]; xAxis: { show: boolean; data: string[] } };
    assert.equal(option.series[0]!.type, 'line');
    assert.deepEqual(option.series[0]!.data, [1, 3, 2]);
    assert.deepEqual(option.xAxis.data, ['2024-01-01', '2024-01-02', '2024-01-03']);
    assert.equal(option.xAxis.show, false);
    assert.deepEqual(drawn.notes, []);
    assert.match(chartSvg(drawn.trend!, 200, 60), /<path/);
  });

  it('past the most its trend draws, the latest of them, and it says so', () => {
    const total = table([{ name: 'sum_notional', type: 'Float', values: [6] }]);
    const n = KPI_TREND_LIMIT + 1;
    const trend = table([
      { name: 'day', type: 'Integer', values: Array.from({ length: n }, (_, i) => n - i) },
      { name: 'sum_notional', type: 'Float', values: Array.from({ length: n }, (_, i) => n - i) },
    ]);
    const drawn = kpiDrawing({ ...kpi, x: 'day' }, total, trend);
    const data = (drawn.trend!.option as { series: { data: number[] }[] }).series[0]!.data;
    assert.equal(data.length, KPI_TREND_LIMIT);
    assert.deepEqual([data[0], data.at(-1)], [2, n], 'the oldest one left out');
    assert.deepEqual(drawn.notes, [`The trend shows the latest ${KPI_TREND_LIMIT} values of day.`]);
  });
});

describe('presets in the spec: kept as words, expanded when the query is built (§3.2)', () => {
  const BY_MONTH: ChartSpec = { ...BAR, x: 'trade_date', grain: 'month' };

  it('a grain groups by its own column, sorted and drawn from it, and carries each period\'s end; the cube\'s column stays its source', () => {
    assert.equal(axisName(BY_MONTH), 'trade_date__month');
    const s = chartSnapshot(CUBE, { ...BY_MONTH, options: { ...BAR.options, sort: { by: 'x', direction: 'asc' } } });
    assert.deepEqual(s.rows, ['trade_date__month']);
    assert.deepEqual(s.derived.map((d) => d.name), ['trade_date__month', 'trade_date__month__upper']);
    // the period's end, the database's adjust(), carried as the most of each period's rows'
    assert.deepEqual(s.measures?.find((m) => m.name === AXIS_UPPER), { name: AXIS_UPPER, column: 'trade_date__month__upper', fn: 'max' });
    assert.deepEqual(s.sorts, [{ column: 'trade_date__month', direction: 'asc' }]);
    assert.ok(s.columns.some((c) => c.name === 'trade_date'));
    // the drawing reads its categories from the grain's column, and a period's end from the database's
    const d = chartOption(BY_MONTH, table([
      { name: 'trade_date__month', type: 'StrictDate', values: ['2024-01-01', '2024-02-01'] },
      { name: 'sum_notional', type: 'Float', values: [1, 2] },
      { name: AXIS_UPPER, type: 'StrictDate', values: ['2024-02-01', '2024-03-01'] },
    ]));
    assert.deepEqual(d.keyAt(0, 1), { trade_date: '2024-02-01' });
    assert.equal(d.upperAt?.(1), '2024-03-01');
  });

  it('a click on a period filters the cube\'s column from its first day to its end, the database\'s', () => {
    assert.deepEqual(markConditions(BY_MONTH, CUBE, { trade_date: '2024-12-01' }, '2025-01-01'), [
      { kind: 'condition', column: 'trade_date', operator: 'greaterThanEqual', value: '2024-12-01' },
      { kind: 'condition', column: 'trade_date', operator: 'lessThan', value: '2025-01-01' },
    ]);
    assert.deepEqual(markConditions({ ...BY_MONTH, grain: 'quarter' }, CUBE, { trade_date: '2024-10-01', region: 'EMEA' }, '2025-01-01'), [
      { kind: 'condition', column: 'trade_date', operator: 'greaterThanEqual', value: '2024-10-01' },
      { kind: 'condition', column: 'trade_date', operator: 'lessThan', value: '2025-01-01' },
      { kind: 'condition', column: 'region', operator: 'equal', value: 'EMEA' },
    ]);
    // no end answered: refused, never guessed
    assert.throws(() => markConditions(BY_MONTH, CUBE, { trade_date: '2024-12-01' }), /was not answered/);
  });

  it('a click on a bin filters to its range: up to its end, the database\'s -- the next bin\'s edge, drawn or not', () => {
    const BINS: ChartSpec = { ...BAR, x: 'notional', bins: { width: 100 }, y: [{ column: 'qty', fn: 'count' }] };
    // sorted by count, a bin cut off by "at most": [200, 300) is not drawn, but [100, 200) still ends at 200
    const d = chartOption(BINS, table([
      { name: 'notional__bin', type: 'Float', values: [0, 100] },
      { name: 'count', type: 'Integer', values: [5, 3] },
      { name: AXIS_UPPER, type: 'Float', values: [100, 200] },
    ]));
    assert.equal(d.upperAt?.(1), 200);
    assert.deepEqual(markConditions(BINS, CUBE, { notional: 100 }, 200), [
      { kind: 'condition', column: 'notional', operator: 'greaterThanEqual', value: 100 },
      { kind: 'condition', column: 'notional', operator: 'lessThan', value: 200 },
    ]);
  });

  it('a show-as plots its own column, over the measure it reads, along the axis, restarting per series', () => {
    const spec: ChartSpec = { ...BAR, split: 'desk', y: [{ column: 'notional', fn: 'sum', showAs: { kind: 'runningTotal' } }] };
    assert.equal(measureName(spec.y[0]!), 'runningTotal_sum_notional');
    const s = chartSnapshot(CUBE, spec);
    assert.deepEqual(s.measures?.map((m) => m.name), ['sum_notional']);
    assert.deepEqual(s.groupDerived, [{ name: 'runningTotal_sum_notional', window: {
      fn: 'sum', column: 'sum_notional', partition: ['desk'], order: [{ column: 'region', direction: 'asc' }], frame: 'running',
    } }]);
  });

  it('what stops one: a grain on what is not a date, bins on what is not a number', () => {
    assert.match(chartProblems({ ...BAR, grain: 'month' }, CUBE).join(' '), /'region' is not a date/);
    assert.match(chartProblems({ ...BAR, bins: { width: 3 } }, CUBE).join(' '), /'region' is not a number/);
    assert.deepEqual(chartProblems(BY_MONTH, CUBE), []);
  });

  it('a live chart keeps its grain while its column stays across, and drops it when the column changes', () => {
    const live = BY_MONTH;
    const cube: CubeSnapshot = { ...CUBE, rows: ['trade_date'], pivotOn: [] };
    assert.equal(followCube(live, cube).grain, 'month');
    assert.equal(followCube(live, { ...CUBE, rows: ['region'], pivotOn: [] }).grain, undefined);
  });
});

describe('dashboard-grade drawing: bar and line together, the grid\'s formats, layers, labels (batch 2, steps 5-8)', () => {
  const ROWS = table([
    { name: 'region', type: 'String', values: ['EMEA', 'AMER', 'APAC'] },
    { name: 'sum_notional', type: 'Float', values: [1500000, 900000, 300000] },
    { name: 'average_qty', type: 'Float', values: [12.5, 7, 3] },
    { name: 'percentOfTotal_sum_notional', type: 'Float', values: [0.5555, 0.3333, 0.1111] },
    { name: 'average_of_sum_notional', type: 'Float', values: [900000, 900000, 900000] },
    { name: 'max_of_sum_notional', type: 'Float', values: [1500000, 1500000, 1500000] },
    { name: 'min_of_sum_notional', type: 'Float', values: [300000, 300000, 300000] },
  ]);
  // the grid's formats: notional in dollars, millions; qty plain
  const money: LabelOf = (v, column) => (v === null ? '' : column === 'notional' ? `$${(Number(v) / 1e6).toFixed(1)}M` : String(v));
  const units = (column: string) => (column === 'notional' ? 'currency|USD|' : undefined);
  type Opt = { series: Record<string, any>[]; yAxis: Record<string, any> | Record<string, any>[]; visualMap?: Record<string, any>[]; tooltip: Record<string, any> };
  const COMBO: ChartSpec = { ...BAR, y: [{ column: 'notional', fn: 'sum' }, { column: 'qty', fn: 'average', mark: 'line' }] };

  it('a bar and a line: each its own mark, on two value axes when their units differ, each in its series\' colour', () => {
    const d = chartOption(COMBO, ROWS, undefined, money, units);
    const o = d.option as unknown as Opt;
    assert.deepEqual(o.series.map((s) => s.type), ['bar', 'line']);
    assert.ok(Array.isArray(o.yAxis) && o.yAxis.length === 2, 'two value axes');
    const axes = o.yAxis as Record<string, any>[];
    assert.equal(o.series[1]!.yAxisIndex, 1);
    assert.equal(axes[0]!.axisLabel.color, '#2a78d6');
    assert.equal(axes[1]!.axisLabel.color, '#eb6834');
    assert.match(chartSvg(d), /\$1\.5M/);
  });

  it('one value axis when they share a unit, or a measure says its own axis', () => {
    const same = chartOption(COMBO, ROWS, undefined, money, () => undefined).option as unknown as Opt;
    assert.ok(!Array.isArray(same.yAxis), 'one axis');
    const told = chartOption({ ...COMBO, y: [COMBO.y[0]!, { ...COMBO.y[1]!, axis: 'right' }] }, ROWS, undefined, money, () => undefined).option as unknown as Opt;
    assert.ok(Array.isArray(told.yAxis));
  });

  it('every value as the grid writes it: the axis, the label, the hover', () => {
    const d = chartOption({ ...BAR, options: { ...BAR.options, labels: true } }, ROWS, undefined, money);
    const o = d.option as unknown as Opt;
    assert.equal((o.yAxis as Record<string, any>).axisLabel.formatter(2000000), '$2.0M');
    assert.equal(o.series[0]!.label.formatter({ dataIndex: 0, value: 1500000 }), '$1.5M');
    assert.equal(o.series[0]!.tooltip.valueFormatter(900000), '$0.9M');
    // a share as a percentage, whatever the column's format
    const share = chartOption({ ...BAR, y: [{ column: 'notional', fn: 'sum', showAs: { kind: 'percentOfTotal' } }] }, ROWS, undefined, money);
    assert.match(String((share.option as unknown as Opt).series[0]!.tooltip.valueFormatter(0.5555)), /^55\.6\s?%$/);
  });

  it('a hover lists the further measures asked for, and writes data as text, never markup', () => {
    const rows = table([
      { name: 'region', type: 'String', values: ['<img src=x onerror=alert(1)>'] },
      { name: 'sum_notional', type: 'Float', values: [1] },
      { name: 'average_qty', type: 'Float', values: [7] },
    ]);
    const d = chartOption({ ...BAR, tooltip: [{ column: 'qty', fn: 'average' }] }, rows, undefined, (v) => String(v));
    const html = (d.option as unknown as Opt).tooltip.formatter([{ seriesIndex: 0, dataIndex: 0, value: 1, marker: '' }]) as string;
    assert.ok(!html.includes('<img'), html);
    assert.match(html, /&lt;img/);
    assert.match(html, /average of qty: 7/);
  });

  it('the label slot writes another measure on each mark', () => {
    const d = chartOption({ ...BAR, label: { column: 'qty', fn: 'average' }, options: { ...BAR.options, labels: true } }, ROWS, undefined, money);
    assert.equal((d.option as unknown as Opt).series[0]!.label.formatter({ dataIndex: 1, value: 900000 }), '7');
  });

  it('reference lines: a target where it is told, the average where the database says; a band; extremes pinned', () => {
    const spec: ChartSpec = { ...BAR, layers: [
      { kind: 'line', value: 1000000, label: 'Target' },
      { kind: 'stat', stat: 'average' },
      { kind: 'band', axis: 'x', from: 'AMER', to: 'APAC', label: 'Growth' },
      { kind: 'extremes' },
    ] };
    const d = chartOption(spec, ROWS, undefined, money);
    const s = (d.option as unknown as Opt).series[0]!;
    assert.deepEqual(s.markLine.data.map((l: Record<string, unknown>) => l['yAxis']), [1000000, 900000]);
    assert.match(s.markLine.data[1].label.formatter, /Average: \$0\.9M/);
    assert.equal(s.markArea.data.length, 1);
    assert.deepEqual(s.markPoint.data.map((p: Record<string, unknown>) => p['name']), ['Maximum', 'Minimum']);
    assert.match(chartSvg(d), /Target/);
  });

  it('a threshold colours its measure\'s marks below it in the bad colour', () => {
    const d = chartOption({ ...BAR, layers: [{ kind: 'threshold', value: 1000000 }] }, ROWS);
    const vm = (d.option as unknown as Opt).visualMap!;
    assert.equal(vm.length, 1);
    assert.deepEqual(vm[0]!.pieces[0], { lt: 1000000, color: '#e34948' });
    chartSvg(d);
  });

  it('the cheaper options: smooth, step, log axis, labels inside and turned, no dimming', () => {
    const line = chartOption({ ...BAR, mark: 'line', options: { ...BAR.options, smooth: true, logAxis: true, labelRotate: 45, dim: false } }, ROWS);
    const o = line.option as unknown as Opt & { xAxis: Record<string, any> };
    assert.equal(o.series[0]!.smooth, true);
    assert.equal((o.yAxis as Record<string, any>).type, 'log');
    assert.equal(o.xAxis.axisLabel.rotate, 45);
    assert.equal(o.series[0]!.emphasis.focus, 'none');
    chartSvg(line);
  });
});

describe('the query of a dashboard-grade chart', () => {
  it('asks for the measures a hover, a size and a label read, and the statistics its reference lines draw', () => {
    const s = chartSnapshot(CUBE, {
      ...BAR, tooltip: [{ column: 'qty', fn: 'max' }], label: { column: 'qty', fn: 'sum' },
      layers: [{ kind: 'stat', stat: 'average' }, { kind: 'extremes' }],
    });
    assert.deepEqual(s.measures?.map((m) => m.name), ['sum_notional', 'max_qty', 'sum_qty']);
    assert.deepEqual(s.groupDerived?.map((d) => d.name), ['average_of_sum_notional', 'max_of_sum_notional', 'min_of_sum_notional']);
  });
});

describe('a real time axis, zoom and pan (batch 2, step 9)', () => {
  const days = (n: number) => Array.from({ length: n }, (_, i) => new Date(Date.UTC(2024, 0, 1 + i)).toISOString().slice(0, 10));
  const LINE: ChartSpec = { ...BAR, mark: 'line', x: 'trade_date' };
  type Opt = { xAxis: Record<string, any>; series: Record<string, any>[]; dataZoom?: Record<string, any>[] };

  it('a date across is a time scale: each point at its own time, in time order whatever the chart\'s order', () => {
    const rows = table([
      { name: 'trade_date', type: 'StrictDate', values: ['2024-03-01', '2024-01-01', '2024-02-01'] },
      { name: 'sum_notional', type: 'Float', values: [3, 1, 2] },
    ]);
    const d = chartOption(LINE, rows);
    const o = d.option as unknown as Opt;
    assert.equal(o.xAxis.type, 'time');
    assert.deepEqual(o.series[0]!.data, [['2024-01-01', 1], ['2024-02-01', 2], ['2024-03-01', 3]]);
    // a click still names the point's own date
    assert.deepEqual(d.keyAt(0, 2), { trade_date: '2024-03-01' });
    assert.match(chartSvg(d), /<path/);
  });

  it('a gap in time is a gap: a missing month is spaced as a month, not drawn as a neighbour', () => {
    const rows = table([
      { name: 'trade_date', type: 'StrictDate', values: ['2024-01-01', '2024-02-01', '2024-06-01'] },
      { name: 'sum_notional', type: 'Float', values: [1, 2, 3] },
    ]);
    const o = chartOption(LINE, rows).option as unknown as Opt;
    assert.equal(o.xAxis.type, 'time', 'the axis measures time, so June sits four months on');
  });

  it('a long series gets a slider and point thinning; a short one neither, unless asked', () => {
    const many = table([
      { name: 'trade_date', type: 'StrictDate', values: days(600) },
      { name: 'sum_notional', type: 'Float', values: days(600).map((_, i) => i) },
    ]);
    const long = chartOption(LINE, many).option as unknown as Opt;
    assert.deepEqual(long.dataZoom?.map((z) => z['type']), ['inside', 'slider']);
    assert.equal(long.series[0]!.sampling, 'lttb');
    const few = table([
      { name: 'trade_date', type: 'StrictDate', values: days(10) },
      { name: 'sum_notional', type: 'Float', values: days(10).map((_, i) => i) },
    ]);
    assert.equal((chartOption(LINE, few).option as unknown as Opt).dataZoom, undefined);
    assert.ok((chartOption({ ...LINE, options: { ...LINE.options, zoom: true } }, few).option as unknown as Opt).dataZoom);
    assert.equal((chartOption({ ...LINE, options: { ...LINE.options, zoom: false } }, many).option as unknown as Opt).dataZoom, undefined);
    chartSvg(chartOption(LINE, many));
  });
});

describe('a time series asks for its points', () => {
  it('a first chart of a date asks for a time series\' points, not a top 50; so does a live chart moving onto one', () => {
    const dated: CubeSnapshot = { ...CUBE, rows: ['trade_date'], pivotOn: [] };
    assert.equal(defaultChart(dated)?.options.limit, TIME_LIMIT);
    const live: ChartSpec = { ...BAR, x: 'region' };
    assert.equal(followCube(live, dated).options.limit, TIME_LIMIT);
    // told how many, it keeps what it was told
    assert.equal(followCube({ ...live, options: { ...live.options, limit: 12 } }, dated).options.limit, 12);
  });
});

describe('drill-down in a chart (batch 2, step 10)', () => {
  const GROUPED: CubeSnapshot = { ...CUBE, rows: ['region', 'desk'], pivotOn: [] };
  const BY_REGION: ChartSpec = { ...BAR, x: 'region', frozen: true };

  it('drills through the grid\'s own groupings, from the column across down', async () => {
    const { drillLevels, drillDown, drillUp, drilledView } = await import('../src/chart-spec.ts');
    assert.deepEqual(drillLevels(BY_REGION, GROUPED), ['region', 'desk']);
    assert.equal(drillLevels(BY_REGION, { ...GROUPED, rows: ['region'] }), null, 'nothing below: no drill');
    const down = drillDown(BY_REGION, GROUPED, 'EMEA')!;
    assert.deepEqual(down.drill, { levels: ['region', 'desk'], path: ['EMEA'] });
    assert.equal(drillDown(down, GROUPED, 'FX'), null, 'the bottom');
    // drawn: desk across, its query narrowed to EMEA -- the chart's own conditions, not the cube's filter
    assert.equal(drilledView(down).spec.x, 'desk');
    const s = chartSnapshot(GROUPED, down);
    assert.deepEqual(s.rows, ['desk']);
    assert.deepEqual(s.filter, { kind: 'and', children: [CUBE.filter, { kind: 'condition', column: 'region', operator: 'equal', value: 'EMEA' }] });
    assert.deepEqual(drillUp(down, 0).drill, undefined);
  });

  it('a click on a drilled mark narrows the grid to its path and its own value', async () => {
    const { drillDown } = await import('../src/chart-spec.ts');
    const down = drillDown(BY_REGION, GROUPED, 'EMEA')!;
    const d = chartOption(down, table([
      { name: 'desk', type: 'String', values: ['FX', 'Rates'] },
      { name: 'sum_notional', type: 'Float', values: [1, 2] },
    ]));
    const key = d.keyAt(0, 1)!;
    assert.deepEqual(key, { desk: 'Rates' });
    assert.deepEqual(markConditions(down, GROUPED, key), [
      { kind: 'condition', column: 'region', operator: 'equal', value: 'EMEA' },
      { kind: 'condition', column: 'desk', operator: 'equal', value: 'Rates' },
    ]);
  });

  it('a drilled live chart stays where it was taken while the grid changes', async () => {
    const { drillDown } = await import('../src/chart-spec.ts');
    const live: ChartSpec = { ...BAR, x: 'region' };
    const down = drillDown(live, GROUPED, 'EMEA')!;
    assert.equal(followCube(down, { ...GROUPED, rows: ['desk'] }), down);
  });
});

describe('brush to filter (batch 2, step 11)', () => {
  const ROWS = table([
    { name: 'region', type: 'String', values: ['EMEA', 'AMER', 'APAC'] },
    { name: 'sum_notional', type: 'Float', values: [3, 2, 1] },
  ]);

  it('a range across categories takes them, and filters the grid to those values', async () => {
    const { brushConditions } = await import('../src/chart-spec.ts');
    const spec: ChartSpec = { ...BAR, options: { ...BAR.options, brush: true } };
    const d = chartOption(spec, ROWS);
    assert.equal((d.option as { brush?: { brushType: string } }).brush?.brushType, 'lineX');
    const took = d.brushed!([{ brushType: 'lineX', coordRange: [0, 1] }])!;
    assert.deepEqual(took, { kind: 'across', values: ['EMEA', 'AMER'] });
    assert.deepEqual(brushConditions(spec, CUBE, took), [{ kind: 'condition', column: 'region', operator: 'in', value: ['EMEA', 'AMER'] }]);
    chartSvg(d);
  });

  it('a range of time: from its first period\'s start to its last\'s end, the database\'s', async () => {
    const { brushConditions } = await import('../src/chart-spec.ts');
    const { localTime } = await import('../src/chart-option.ts');
    const spec: ChartSpec = { ...BAR, mark: 'line', x: 'trade_date', grain: 'month', options: { ...BAR.options, brush: true } };
    const d = chartOption(spec, table([
      { name: 'trade_date__month', type: 'StrictDate', values: ['2024-01-01', '2024-02-01', '2024-03-01'] },
      { name: 'sum_notional', type: 'Float', values: [1, 2, 3] },
      { name: AXIS_UPPER, type: 'StrictDate', values: ['2024-02-01', '2024-03-01', '2024-04-01'] },
    ]));
    // the brush's ends in the page's local time, as ECharts places a date with no zone
    const took = d.brushed!([{ coordRange: [localTime('2024-01-15'), localTime('2024-03-02')] }])!;
    assert.deepEqual(took, { kind: 'across', values: ['2024-02-01', '2024-03-01'], upper: '2024-04-01' });
    assert.deepEqual(brushConditions(spec, CUBE, took), [
      { kind: 'condition', column: 'trade_date', operator: 'greaterThanEqual', value: '2024-02-01' },
      { kind: 'condition', column: 'trade_date', operator: 'lessThan', value: '2024-04-01' },
    ]);
  });

  it('a scatter\'s rectangle is its two columns\' ranges', async () => {
    const { brushConditions } = await import('../src/chart-spec.ts');
    const spec: ChartSpec = { ...BAR, mark: 'scatter', x: 'notional', y: [{ column: 'qty', fn: 'sum' }], options: { ...BAR.options, brush: true } };
    const d = chartOption(spec, table([
      { name: 'notional', type: 'Float', values: [1, 5, 9] },
      { name: 'qty', type: 'Integer', values: [3, 6, 9] },
    ]));
    const took = d.brushed!([{ brushType: 'rect', coordRange: [[8, 2], [4, 7]] }])!;
    assert.deepEqual(brushConditions(spec, CUBE, took), [
      { kind: 'condition', column: 'notional', operator: 'greaterThanEqual', value: 2 },
      { kind: 'condition', column: 'notional', operator: 'lessThanEqual', value: 8 },
      { kind: 'condition', column: 'qty', operator: 'greaterThanEqual', value: 4 },
      { kind: 'condition', column: 'qty', operator: 'lessThanEqual', value: 7 },
    ]);
  });

  it('no brush unless asked: a click filters as before', () => {
    const d = chartOption(BAR, ROWS);
    assert.equal(d.brushed, undefined);
    assert.equal((d.option as { brush?: unknown }).brush, undefined);
  });
});

describe('the cheaper options (batch 2, step 12)', () => {
  const ROWS = table([
    { name: 'region', type: 'String', values: ['EMEA', 'AMER, "the Americas"'] },
    { name: 'sum_notional', type: 'Float', values: [3, 2] },
  ]);
  type Opt = { aria: { decal: { show: boolean } }; toolbox?: { feature: Record<string, unknown> } };

  it('pattern fills as well as colours, one option away', () => {
    assert.equal((chartOption(BAR, ROWS).option as unknown as Opt).aria.decal.show, false);
    const d = chartOption({ ...BAR, options: { ...BAR.options, decal: true } }, ROWS);
    assert.equal((d.option as unknown as Opt).aria.decal.show, true);
    chartSvg(d);
  });

  it('the toolbox: a picture, the data, the CSV, and bars or a line in place; none unless asked', () => {
    assert.equal((chartOption(BAR, ROWS).option as unknown as Opt).toolbox, undefined);
    const d = chartOption({ ...BAR, options: { ...BAR.options, toolbox: true } }, ROWS);
    assert.deepEqual(Object.keys((d.option as unknown as Opt).toolbox!.feature), ['saveAsImage', 'dataView', 'myCsv', 'magicType']);
    const pie = chartOption({ ...BAR, mark: 'pie', options: { ...BAR.options, toolbox: true } }, ROWS);
    assert.deepEqual(Object.keys((pie.option as unknown as Opt).toolbox!.feature), ['saveAsImage', 'dataView', 'myCsv']);
    chartSvg(d);
  });

  it('the CSV is the chart\'s rows, a value with a comma or a quote quoted', () => {
    assert.equal(chartCsv(ROWS), 'region,sum_notional\r\nEMEA,3\r\n"AMER, ""the Americas""",2\r\n');
  });
});

describe('a period is named as one', () => {
  it('a month, a quarter, a year, a week by its first day', () => {
    assert.equal(periodText('2024-03-01', 'month'), 'Mar 2024');
    assert.equal(periodText('2024-10-01', 'quarter'), '2024 Q4');
    assert.equal(periodText('2025-01-01', 'year'), '2025');
    assert.equal(periodText('2024-03-04', 'week'), 'w/c 2024-03-04');
  });

  it('bars by quarter are categories named by their quarter; a line by quarter keeps the time scale', () => {
    const rows = table([
      { name: 'trade_date__quarter', type: 'StrictDate', values: ['2024-01-01', '2024-04-01'] },
      { name: 'sum_notional', type: 'Float', values: [1, 2] },
    ]);
    const bars = chartOption({ ...BAR, x: 'trade_date', grain: 'quarter' }, rows).option as { xAxis: { type: string; data?: string[] } };
    assert.equal(bars.xAxis.type, 'category');
    assert.deepEqual(bars.xAxis.data, ['2024 Q1', '2024 Q2']);
    const line = chartOption({ ...BAR, mark: 'line', x: 'trade_date', grain: 'quarter' }, rows).option as { xAxis: { type: string } };
    assert.equal(line.xAxis.type, 'time');
  });
});

describe('the audit\'s cases (batch 2): a brush by value, the last bin, refusals', () => {
  it('a brush over bars sorted by their value takes its range by VALUE, not by where the bars are drawn', async () => {
    const { brushConditions } = await import('../src/chart-spec.ts');
    const BINS: ChartSpec = { ...BAR, x: 'notional', bins: { width: 100 }, y: [{ column: 'qty', fn: 'count' }], options: { ...BAR.options, brush: true } };
    // drawn largest count first: 200, 0, 100 -- a brush over the first two takes 200 and 0, not 0..100
    const d = chartOption(BINS, table([
      { name: 'notional__bin', type: 'Float', values: [200, 0, 100] },
      { name: 'count', type: 'Integer', values: [9, 5, 1] },
      { name: AXIS_UPPER, type: 'Float', values: [300, 100, 200] },
    ]));
    const took = d.brushed!([{ coordRange: [0, 1] }])!;
    assert.deepEqual(brushConditions(BINS, CUBE, took), [
      { kind: 'condition', column: 'notional', operator: 'greaterThanEqual', value: 0 },
      { kind: 'condition', column: 'notional', operator: 'lessThan', value: 300 },
    ]);
  });

  it('a brush ending on the last bin keeps the whole of it, to its database end', async () => {
    const { brushConditions } = await import('../src/chart-spec.ts');
    const BINS: ChartSpec = { ...BAR, x: 'notional', bins: { width: 100 }, y: [{ column: 'qty', fn: 'count' }], options: { ...BAR.options, brush: true, sort: { by: 'x', direction: 'asc' } } };
    const d = chartOption(BINS, table([
      { name: 'notional__bin', type: 'Float', values: [0, 100, 200] },
      { name: 'count', type: 'Integer', values: [1, 2, 3] },
      { name: AXIS_UPPER, type: 'Float', values: [100, 200, 300] },
    ]));
    assert.deepEqual(brushConditions(BINS, CUBE, d.brushed!([{ coordRange: [1, 2] }])!), [
      { kind: 'condition', column: 'notional', operator: 'greaterThanEqual', value: 100 },
      { kind: 'condition', column: 'notional', operator: 'lessThan', value: 300 },
    ]);
  });

  it('a stacked line chart stacks; a line beside stacked bars does not', () => {
    const rows = table([
      { name: 'region', type: 'String', values: ['EMEA', 'EMEA'] },
      { name: 'desk', type: 'String', values: ['FX', 'Rates'] },
      { name: 'sum_notional', type: 'Float', values: [1, 3] },
    ]);
    const lines = chartOption({ ...BAR, mark: 'line', split: 'desk', options: { ...BAR.options, stack: 'stacked' } }, rows).option as { series: { stack?: string }[] };
    assert.ok(lines.series.every((x) => x.stack === 'total-0'));
    const combo = chartOption({ ...BAR, y: [BAR.y[0]!, { column: 'qty', fn: 'sum', mark: 'line' }], options: { ...BAR.options, stack: 'stacked' } },
      table([{ name: 'region', type: 'String', values: ['EMEA'] }, { name: 'sum_notional', type: 'Float', values: [1] }, { name: 'sum_qty', type: 'Float', values: [2] }])).option as { series: { stack?: string }[] };
    assert.equal(combo.series[1]!.stack, undefined);
  });

  it('what nothing draws is refused by name, never ignored', () => {
    // small multiples are drawn now (batch 3): a facet on a kind they are not drawn for, or with a brush, is refused
    assert.deepEqual(chartProblems({ ...BAR, facet: { column: 'desk' } }, CUBE), []);
    assert.match(chartProblems({ ...BAR, mark: 'pie', facet: { column: 'desk' } }, CUBE).join(' '), /not drawn as small multiples/);
    assert.match(chartProblems({ ...BAR, facet: { column: 'desk' }, options: { ...BAR.options, brush: true } }, CUBE).join(' '), /no brush/);
    assert.match(chartProblems({ ...BAR, layers: [{ kind: 'stat', stat: 'average', measure: 3 }] }, CUBE).join(' '), /reads value 4/);
    assert.match(chartProblems({ ...BAR, options: { ...BAR.options, stack: 'percent' }, layers: [{ kind: 'extremes' }] }, CUBE).join(' '), /100% stack/);
    assert.match(chartProblems({ ...BAR, mark: 'scatter', x: 'notional', layers: [{ kind: 'line', value: 1 }] }, CUBE).join(' '), /scatter/);
  });

  it('a group-level column that reads a grouping the chart does not group by is refused', () => {
    const s: CubeSnapshot = { ...CUBE, pivotOn: [], groupDerived: [{ name: 'deskRank', window: { fn: 'rank', partition: ['desk'], order: [{ column: 'notional', direction: 'desc' }] } }] };
    assert.match(chartProblems({ ...BAR, y: [{ column: 'deskRank', fn: 'value' }] }, s).join(' '), /reads 'desk'/);
    assert.deepEqual(chartProblems({ ...BAR, split: 'desk', y: [{ column: 'deskRank', fn: 'value' }] }, s), []);
  });

  it('the treemap\'s hover writes data as text, never markup', () => {
    const d = treemapOfGrid({
      rows: table([{ name: 'region', type: 'String', values: ['<img src=x onerror=alert(1)>'] }, { name: 'v', type: 'Float', values: [1] }]),
      tree: [{ path: [], level: 1, isGroup: false, expanded: false, isTotal: false }],
      leaves: [{ name: 'region', type: 'String' }, { name: 'v', type: 'Float' }],
      levels: [],
    });
    const f = (d.option as { tooltip: { formatter: (p: unknown) => string } }).tooltip.formatter;
    assert.ok(!f({ name: '<img src=x>', data: { text: '<b>1</b>' } }).includes('<'));
  });

  it('a newer DataCube\'s option word is refused by name', async () => {
    const { specProblem } = await import('../src/chart-spec.ts');
    assert.match(specProblem({ ...BAR, options: { ...BAR.options, stack: 'stream' } }, ['bar']) ?? '', /stack "stream"/);
  });
});

describe('a ticking chart\'s scale (docs/DEEPHAVEN_PLAN_2026_10_22.md C2)', () => {
  /** `drawing` drawn by ECharts in node, as the page draws it; the extent of each held axis as drawn. */
  const drawn = (drawing: ChartDrawing, held?: readonly (readonly [number, number] | null)[]) => {
    const chart = echarts.init(null, null, { renderer: 'svg', ssr: true, width: 640, height: 400 });
    try {
      const option = held && drawing.hold ? heldOption(drawing.option, drawing.hold, held) : drawing.option;
      chart.setOption({ ...(option as EChartsOption), animation: false });
      return drawnExtents(chart, drawing.hold!);
    } finally {
      chart.dispose();
    }
  };
  const regions = (values: number[]) => table([
    { name: 'region', type: 'String', values: ['EMEA', 'AMER', 'APAC'] },
    { name: 'sum_notional', type: 'Float', values },
  ]);

  it('says what its values reach on its value axis, from zero', () => {
    assert.deepEqual(chartOption(BAR, regions([300, 200, 100])).hold, { key: 'yAxis', extents: [[0, 300]] });
    assert.deepEqual(chartOption(BAR, regions([-40, 200, 100])).hold?.extents, [[-40, 200]]);
    const across = chartOption({ ...BAR, options: { ...BAR.options, orientation: 'horizontal' } }, regions([300, 200, 100]));
    assert.equal(across.hold?.key, 'xAxis');
  });

  it('reaches a stack\'s sums, each sign apart', () => {
    const rows = table([
      { name: 'region', type: 'String', values: ['EMEA', 'EMEA', 'AMER', 'AMER'] },
      { name: 'desk', type: 'String', values: ['FX', 'Rates', 'FX', 'Rates'] },
      { name: 'sum_notional', type: 'Float', values: [300, 200, -50, -70] },
    ]);
    const d = chartOption({ ...BAR, split: 'desk', options: { ...BAR.options, stack: 'stacked' } }, rows);
    assert.deepEqual(d.hold?.extents, [[-120, 500]]);
  });

  it('holds nothing a percent, a log axis or a rank draws', () => {
    assert.equal(chartOption({ ...BAR, options: { ...BAR.options, stack: 'percent' } }, regions([300, 200, 100])).hold, undefined);
    assert.equal(chartOption({ ...BAR, options: { ...BAR.options, logAxis: true } }, regions([300, 200, 100])).hold, undefined);
  });

  it('a tick inside the scale keeps it; one past it widens that side only', () => {
    const [view] = drawn(chartOption(BAR, regions([300, 200, 100])));
    assert.ok(view, 'the view drew a value axis');
    const [lo, hi] = view;
    assert.ok(lo <= 0 && hi >= 300, `the view's axis ${view}`);
    // its values fall: the axis as the view drew it, not shrunk to them
    assert.deepEqual(drawn(chartOption(BAR, regions([120, 80, 40])), [view]), [view]);
    // ECharts alone shrinks it: what the hold is for
    const [fresh] = drawn(chartOption(BAR, regions([120, 80, 40])));
    assert.ok(fresh![1] < hi, `unheld, ECharts draws ${fresh}`);
    // a value past the top: the top let go and widened, the bottom kept
    const [wider] = drawn(chartOption(BAR, regions([hi * 2, 200, 100])), [view]);
    assert.ok(wider![1] >= hi * 2 && wider![0] === lo, `widened to ${wider}`);
  });
});
