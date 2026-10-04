// Charts of a cube, drawn by ECharts: the query a chart asks (the cube's
// own, regrouped), the drawing it makes of the rows, and what a click on
// a mark means. The drawing is rendered for real, to SVG, in node.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { toJson, element } from '../../pure-protocol/src/index.ts';
import {
  chartProblems,
  chartQuery,
  chartSnapshot,
  defaultChart,
  followCube,
  measureName,
  type ChartSpec,
} from '../src/chart-spec.ts';
import { chartOption, MAX_SERIES, treemapOfGrid, type GridShown } from '../src/chart-option.ts';
import * as echarts from 'echarts/core';
import { SVGRenderer } from 'echarts/renderers';
import '../src/chart-render.ts';   // registers the charts and components the page uses
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
  measures: [{ name: 'total', column: 'notional', fn: 'sum' }],
  sorts: [],
  filter: { kind: 'condition', column: 'desk', operator: 'equal', value: 'FX' },
  epoch: 1,
};

const BAR: ChartSpec = {
  version: 1,
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
    assert.deepEqual(chartSnapshot(CUBE, BAR).sorts, [{ column: 'sum_notional', direction: 'desc' }]);
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
      measures: [],
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
    assert.ok(o.series.every((s) => s.stack === 'total'));
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
    leaves: [{ name: '__tree', type: 'String' }, { name: 'sum_notional', type: 'Float' }],
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

  it('plots the cube\'s own measure', () => {
    const m = followCube(live, { ...CUBE, measures: [{ name: 'q', column: 'qty', fn: 'average' }] });
    assert.deepEqual(m.y, [{ column: 'qty', fn: 'average' }]);
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
