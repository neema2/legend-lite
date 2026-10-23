// Batch 3's kinds (docs/DATACUBE_CHARTS_DESIGN_2026_10_09.md §3): what each refuses, what its query asks the database
// for (a waterfall's starts and ends, a Pareto's cumulative share, a two-ring pie's inner ring -- never added up in
// the page), and each drawn as SVG in node, as the page draws it on canvas.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { element, fn, lambda, property, variable } from '../../pure-protocol/src/index.ts';
import { AXIS_UPPER, boxMeasures, chartProblems, chartSnapshot, drillLevels, editChart, followCube, type ChartSpec } from '../src/chart-spec.ts';
import { chartOption, describe as describeChart, kpiDrawing, type ChartDrawing, type GridShown } from '../src/chart-option.ts';
import { facetOption, gridKindOption, kindOption } from '../src/chart-kinds.ts';
import { grainName, marimekkoNames, paretoName, ringTotalName, waffleNames, waterfallNames } from '../src/calc-presets.ts';
import * as echarts from 'echarts/core';
import { JSDOM } from 'jsdom';
import { paintChartForm } from '../src/ui/chart-form.ts';
import { SVGRenderer } from 'echarts/renderers';
import '../src/chart-render.ts';
import '../src/chart-render-kinds.ts';
import type { EChartsOption } from 'echarts';
import type { ResultTable } from '../../engine-client/src/result.ts';
import type { CubeSnapshot } from '../src/snapshot.ts';

echarts.use([SVGRenderer]);

function svg(drawing: ChartDrawing, width = 640, height = 400): string {
  const chart = echarts.init(null, null, { renderer: 'svg', ssr: true, width, height });
  try {
    chart.setOption({ ...(drawing.option as EChartsOption), animation: false });
    return chart.renderToSVGString();
  } finally {
    chart.dispose();
  }
}

const CUBE: CubeSnapshot = {
  source: { query: element('sales') },
  columns: [
    { name: 'region', type: 'String' },
    { name: 'channel', type: 'String' },
    { name: 'day', type: 'StrictDate' },
    { name: 'revenue', type: 'Float' },
    { name: 'cost', type: 'Float' },
    { name: 'profit', type: 'Float' },
  ],
  derived: [],
  rows: ['region', 'channel'],
  pivotOn: [],
  sorts: [],
  epoch: 1,
};

const OPTIONS: ChartSpec['options'] = {
  orientation: 'vertical', stack: 'none', sort: { by: 'y', direction: 'desc' }, limit: 50, labels: true, legend: 'top',
};
const REVENUE_SUM = { column: 'revenue', fn: 'sum' } as const;
const of = (mark: ChartSpec['mark'], more: Partial<ChartSpec> = {}): ChartSpec =>
  ({ version: 2, mark, x: 'region', y: [{ column: 'revenue', fn: 'sum' }], options: OPTIONS, ...more });

function table(columns: { name: string; type: string; values: (string | number | null)[] }[]): ResultTable {
  return { columns, rowCount: columns[0]?.values.length ?? 0, epoch: 1, elapsedMs: 0 };
}

const REGIONS = ['EMEA', 'APAC', 'AMER', 'LATAM'];
const BY_REGION = table([
  { name: 'region', type: 'String', values: REGIONS },
  { name: 'sum_revenue', type: 'Float', values: [400, 300, 200, -50] },
  { name: 'sum_cost', type: 'Float', values: [250, 180, 150, 40] },
  { name: 'sum_profit', type: 'Float', values: [150, 120, 50, -90] },
]);

describe('batch 3\'s kinds refuse what they cannot draw, by name', () => {
  const problems = (spec: ChartSpec) => chartProblems(spec, CUBE).join(' ');
  it('a flow and a stream need their split; a kind of one series has none', () => {
    assert.match(problems(of('sankey')), /where each flow goes/);
    assert.match(problems(of('chord')), /where each flow goes/);
    assert.match(problems(of('stream', { x: 'day' })), /layers/);
    assert.match(problems(of('funnel', { split: 'channel' })), /one series: it has no split/);
    assert.match(problems(of('waterfall', { split: 'channel' })), /one series/);
  });
  it('a dumbbell and a range plot two values; a radar three or more; parallel two or more', () => {
    assert.match(problems(of('dumbbell')), /two values/);
    assert.match(problems(of('range')), /two values/);
    assert.match(problems(of('radar', { y: [{ column: 'revenue', fn: 'sum' }, { column: 'cost', fn: 'sum' }] })), /three values/);
    assert.match(problems(of('parallel')), /two values or more/);
    assert.deepEqual(chartProblems(of('dumbbell', { y: [{ column: 'cost', fn: 'sum' }, { column: 'revenue', fn: 'sum' }] }), CUBE), []);
  });
  it('a Pareto runs from the largest; a gauge needs a target and nothing across; a calendar and a stream need dates', () => {
    assert.match(problems(of('pareto', { options: { ...OPTIONS, sort: { by: 'x', direction: 'asc' } } })), /order by value, largest first/);
    assert.deepEqual(chartProblems(of('pareto'), CUBE), []);
    const { x: _x, ...gauge } = of('gauge');
    assert.match(problems(gauge), /needs a target/);
    assert.deepEqual(chartProblems({ ...gauge, options: { ...OPTIONS, target: 1000 } }, CUBE), []);
    assert.match(problems({ ...of('gauge'), options: { ...OPTIONS, target: 1000 } }), /no column across/);
    assert.match(problems(of('calendar')), /not one/);
    assert.deepEqual(chartProblems(of('calendar', { x: 'day' }), CUBE), []);
    assert.match(problems(of('calendar', { x: 'day', grain: 'month' })), /its grain is the day/);
  });
  it('layers, a label slot and hover measures only where they are drawn', () => {
    assert.match(problems(of('funnel', { layers: [{ kind: 'line', value: 1 }] })), /A funnel draws no reference lines/);
    assert.match(problems(of('rose', { label: { column: 'cost', fn: 'sum' } })), /writes no other measure/);
    assert.match(problems(of('sankey', { split: 'channel', tooltip: [{ column: 'cost', fn: 'sum' }] })), /lists no further measures/);
    assert.deepEqual(chartProblems(of('lollipop', { tooltip: [{ column: 'cost', fn: 'sum' }] }), CUBE), []);
  });
  it('drills from a kind of categories; follows the cube except a chart of the grid as shown', () => {
    assert.deepEqual(drillLevels(of('lollipop'), CUBE), ['region', 'channel']);
    assert.equal(drillLevels(of('sankey', { split: 'channel' }), CUBE), null);
    const sun = of('sunburst', { x: 'channel', frozen: false });
    assert.equal(followCube(sun, CUBE), sun);
  });
});

describe('the database computes each kind\'s own figures', () => {
  it('a waterfall\'s end is a running total along the chart\'s own order, its start that end less its value', () => {
    const s = chartSnapshot(CUBE, of('waterfall'));
    const { start, end } = waterfallNames('sum_revenue');
    const running = s.groupDerived?.find((d) => d.name === end);
    assert.deepEqual(running?.window, {
      fn: 'sum', column: 'sum_revenue', partition: [], frame: 'running',
      order: [{ column: 'sum_revenue', direction: 'desc' }, { column: 'region', direction: 'asc' }],
    });
    assert.ok(s.groupDerived?.some((d) => d.name === start && d.lambda !== undefined));
    // the rows come in that very order: ties broken by the category
    assert.deepEqual(s.sorts, [{ column: 'sum_revenue', direction: 'desc' }, { column: 'region', direction: 'asc' }]);
  });
  it('a Pareto\'s share is the running total from the largest over the whole; a two-ring pie\'s inner ring a total per category', () => {
    const p = chartSnapshot(CUBE, of('pareto'));
    assert.ok(p.groupDerived?.some((d) => d.name === paretoName('sum_revenue')));
    const ring = chartSnapshot(CUBE, of('pie', { split: 'channel' })).groupDerived?.find((d) => d.name === ringTotalName('sum_revenue'));
    assert.deepEqual(ring?.window?.partition, ['region']);
  });
  it('a gauge\'s query groups by nothing: one row', () => {
    const s = chartSnapshot(CUBE, { ...of('gauge'), options: { ...OPTIONS, target: 1000 } });
    assert.deepEqual(s.rows, []);
  });
});

describe('each kind is drawn', () => {
  it('a lollipop: a stem and a dot per category, each labelled as the grid writes it', () => {
    const d = kindOption(of('lollipop'), BY_REGION);
    const out = svg(d);
    assert.ok(out.includes('EMEA') && out.includes('LATAM'));
    assert.deepEqual(d.keyAt(1, 2), { region: 'AMER' });
  });

  it('a dumbbell and a range: two values per category', () => {
    const two = [{ column: 'cost', fn: 'sum' as const }, { column: 'revenue', fn: 'sum' as const }];
    for (const mark of ['dumbbell', 'range'] as const) {
      const out = svg(kindOption(of(mark, { y: two }), BY_REGION));
      assert.ok(out.includes('APAC'), mark);
      assert.ok(/<path|<rect/.test(out), `${mark} draws its spans`);
    }
    const horizontal = svg(kindOption(of('dumbbell', { y: two, options: { ...OPTIONS, orientation: 'horizontal' } }), BY_REGION));
    assert.ok(horizontal.includes('EMEA'));
  });

  it('a waterfall: each bar from the database\'s start to its end, then the total -- the last end, read', () => {
    const { start, end } = waterfallNames('sum_revenue');
    const rows = table([
      { name: 'region', type: 'String', values: REGIONS },
      { name: 'sum_revenue', type: 'Float', values: [400, 300, 200, -50] },
      { name: start, type: 'Float', values: [0, 400, 700, 900] },
      { name: end, type: 'Float', values: [400, 700, 900, 850] },
    ]);
    const d = kindOption(of('waterfall'), rows);
    const out = svg(d);
    assert.ok(out.includes('Total'));
    const series = (d.option as { series: { data: { value: unknown[] }[] }[] }).series[0]!;
    assert.deepEqual(series.data.at(-1)!.value, [4, 0, 850], 'the total runs from zero to the last end');
    assert.equal(d.keyAt(0, 4), null, 'the total is no category');
  });

  it('a Pareto: the bars and the database\'s cumulative share on a second axis, the 80% line drawn', () => {
    const rows = table([
      { name: 'region', type: 'String', values: REGIONS },
      { name: 'sum_revenue', type: 'Float', values: [400, 300, 200, 100] },
      { name: paretoName('sum_revenue'), type: 'Float', values: [0.4, 0.7, 0.9, 1] },
    ]);
    const out = svg(kindOption(of('pareto'), rows));
    assert.ok(out.includes('80%') && out.includes('Cumulative share'));
  });

  it('a funnel and a rose: a stage, a petal per category', () => {
    assert.ok(svg(kindOption(of('funnel'), BY_REGION)).includes('APAC'));
    assert.ok(svg(kindOption(of('rose'), BY_REGION)).includes('AMER'));
  });

  it('a polar bar and radial bars', () => {
    assert.ok(svg(kindOption(of('polar'), BY_REGION)).includes('EMEA'));
    assert.ok(svg(kindOption(of('polar', { options: { ...OPTIONS, orientation: 'horizontal' } }), BY_REGION)).includes('EMEA'));
  });

  it('a radar: a shape per category, a spoke per value; parallel: a line per category through an axis per value', () => {
    const three = [{ column: 'revenue', fn: 'sum' as const }, { column: 'cost', fn: 'sum' as const }, { column: 'profit', fn: 'sum' as const }];
    assert.ok(svg(kindOption(of('radar', { y: three }), BY_REGION)).includes('sum of cost'));
    assert.ok(svg(kindOption(of('parallel', { y: three }), BY_REGION)).includes('sum of profit'));
  });

  it('a Sankey and a chord: a flow per row, from the column across to the split; no width, no flow', () => {
    const flows = table([
      { name: 'region', type: 'String', values: ['EMEA', 'EMEA', 'APAC', 'APAC'] },
      { name: 'channel', type: 'String', values: ['Online', 'Retail', 'Online', 'Retail'] },
      { name: 'sum_revenue', type: 'Float', values: [100, 60, 80, 0] },
    ]);
    const sankey = kindOption(of('sankey', { split: 'channel' }), flows);
    assert.ok(svg(sankey).includes('Online'));
    assert.match(sankey.notes.join(), /1 flow not shown/);
    assert.deepEqual(sankey.keyAt(0, 0, { key: { region: 'EMEA', channel: 'Online' } }), { region: 'EMEA', channel: 'Online' });
    assert.ok(svg(kindOption(of('chord', { split: 'channel' }), flows)).includes('Retail'));
  });

  it('a streamgraph: layers along time', () => {
    const rows = table([
      { name: 'day', type: 'StrictDate', values: ['2024-01-01', '2024-01-01', '2024-02-01', '2024-02-01'] },
      { name: 'channel', type: 'String', values: ['Online', 'Retail', 'Online', 'Retail'] },
      { name: 'sum_revenue', type: 'Float', values: [10, 20, 15, 25] },
    ]);
    assert.ok(svg(kindOption(of('stream', { x: 'day', split: 'channel' }), rows)).length > 1000);
  });

  it('a calendar: a cell per day, a calendar per year, the latest three', () => {
    const rows = table([
      { name: 'day', type: 'StrictDate', values: ['2021-03-01', '2022-03-01', '2023-03-01', '2024-03-01', '2024-03-02'] },
      { name: 'sum_revenue', type: 'Float', values: [1, 2, 3, 4, 5] },
    ]);
    const d = kindOption(of('calendar', { x: 'day' }), rows);
    assert.match(d.notes.join(), /latest 3 of 4 years/);
    const out = svg(d, 800, 600);
    assert.ok(out.includes('2024') && !out.includes('2021'));
    assert.deepEqual(d.keyAt(0, 0, { ci: 3 }), { day: '2024-03-01' });
  });

  it('a gauge: the value against its target, as a dial or a ring; past the target, said', () => {
    const one = table([{ name: 'sum_revenue', type: 'Float', values: [1200] }]);
    const { x: _x, ...spec } = of('gauge');
    const dial = kindOption({ ...spec, options: { ...OPTIONS, target: 1000 } }, one, undefined, (v) => String(v));
    assert.ok(svg(dial).includes('1200'));
    assert.match(dial.notes.join(), /past the target/);
    const ring = kindOption({ ...spec, options: { ...OPTIONS, target: 2000, ring: true } }, one, undefined, (v) => String(v));
    assert.ok(svg(ring).includes('of 2000'));
  });
});

describe('a pie in two rings, and half a ring', () => {
  it('the inner ring the database\'s totals, the outer the split\'s slices beside their category', () => {
    const rows = table([
      { name: 'region', type: 'String', values: ['EMEA', 'EMEA', 'APAC'] },
      { name: 'channel', type: 'String', values: ['Online', 'Retail', 'Online'] },
      { name: 'sum_revenue', type: 'Float', values: [100, 60, 80] },
      { name: ringTotalName('sum_revenue'), type: 'Float', values: [160, 160, 80] },
    ]);
    const d = chartOption(of('pie', { split: 'channel' }), rows);
    const series = (d.option as { series: { data: { value: number; name: string }[] }[] }).series;
    assert.deepEqual(series[0]!.data.map((x) => x.value), [160, 80]);
    assert.deepEqual(series[1]!.data.map((x) => x.name), ['EMEA · Online', 'EMEA · Retail', 'APAC · Online']);
    assert.ok(svg(d).includes('EMEA'));
    assert.ok(svg(chartOption(of('pie', { options: { ...OPTIONS, half: true } }), BY_REGION)).includes('EMEA'));
  });
});

describe('the grid as shown, as a sunburst and a tree', () => {
  const shown: GridShown = {
    rows: table([
      { name: '__tree', type: 'String', values: ['Total', 'EMEA', 'Online', 'Retail', 'APAC'] },
      // a number column of the rows, empty on a group's row: never what the rings are sized by
      { name: 'order_id', type: 'Integer', values: [null, null, null, null, null] },
      { name: 'revenue', type: 'Float', values: [240, 160, 100, 60, 80] },
    ]),
    tree: [
      { path: [], level: 0, isGroup: true, expanded: true, isTotal: true },
      { path: ['EMEA'], level: 1, isGroup: true, expanded: true, isTotal: false },
      { path: ['EMEA', 'Online'], level: 2, isGroup: true, expanded: false, isTotal: false },
      { path: ['EMEA', 'Retail'], level: 2, isGroup: true, expanded: false, isTotal: false },
      { path: ['APAC'], level: 1, isGroup: true, expanded: false, isTotal: false },
    ],
    leaves: [{ name: '__tree', type: 'String' }, { name: 'order_id', type: 'Integer' }, { name: 'revenue', type: 'Float', measure: true }],
    levels: ['region', 'channel'],
  };
  it('each group\'s ring says its own total, the grid\'s', () => {
    const d = gridKindOption('sunburst', shown);
    const data = (d.option as { series: { data: { name: string; value?: number; children?: unknown[] }[] }[] }).series[0]!.data;
    assert.deepEqual(data.map((n) => [n.name, n.value, n.children?.length ?? 0]), [['EMEA', 160, 2], ['APAC', 80, 0]]);
    assert.ok(svg(d).includes('EMEA'));
    assert.deepEqual(d.keyAt(0, 0, { key: { region: 'EMEA', channel: 'Online' } }), { region: 'EMEA', channel: 'Online' });
  });
  it('a tree: a node per group, its rows its branches', () => {
    assert.ok(svg(gridKindOption('tree', shown), 800, 500).includes('Online'));
  });
});

describe('a bubble: a scatter sized by a third column of its rows', () => {
  it('each point [x, y, size], sized between the least and the most the rows say; a point with no size, said', () => {
    const rows = table([
      { name: 'cost', type: 'Float', values: [10, 20, 30, 40] },
      { name: 'revenue', type: 'Float', values: [15, 30, 50, 60] },
      { name: 'profit', type: 'Float', values: [5, 10, null, 20] },
    ]);
    const spec = of('scatter', { x: 'cost', y: [{ column: 'revenue', fn: 'sum' }], size: { column: 'profit', fn: 'sum' } });
    assert.deepEqual(chartProblems(spec, CUBE), []);
    const d = chartOption(spec, rows);
    const option = d.option as { series: { data: number[][] }[]; visualMap: { min: number; max: number; dimension: number } };
    assert.deepEqual(option.series[0]!.data, [[10, 15, 5], [20, 30, 10], [40, 60, 20]]);
    assert.deepEqual([option.visualMap.dimension, option.visualMap.min, option.visualMap.max], [2, 5, 20]);
    assert.match(d.notes.join(), /1 point not shown: no profit to size by/);
    assert.ok(svg(d).length > 1000);
  });
});

describe('small multiples: a chart per value, on shared axes', () => {
  const rows = table([
    { name: 'region', type: 'String', values: ['EMEA', 'APAC', 'EMEA', 'APAC', 'EMEA'] },
    { name: 'channel', type: 'String', values: ['Online', 'Online', 'Retail', 'Retail', 'Partner'] },
    { name: 'sum_revenue', type: 'Float', values: [100, 80, 60, 300, 40] },
  ]);
  it('the query groups by the facet, panel by panel, each a whole of its own', () => {
    const spec = of('bar', { x: 'region', facet: { column: 'channel' }, layers: [{ kind: 'stat', stat: 'average' }], y: [{ ...REVENUE_SUM, showAs: { kind: 'percentOfTotal' } }] });
    const s = chartSnapshot(CUBE, spec);
    assert.deepEqual(s.rows, ['region', 'channel']);
    assert.deepEqual(s.sorts[0], { column: 'channel', direction: 'asc' }, 'panel by panel');
    assert.ok(s.groupDerived?.every((d) => !d.window || d.window.fn === 'rank' || d.window.partition.includes('channel')), 'each window within its panel');
  });
  it('a grid per panel, its axes renumbered, the value axes shared: each runs to the most any panel shows', () => {
    const d = facetOption(of('bar', { x: 'region', facet: { column: 'channel' } }), rows);
    const o = d.option as { grid: unknown[]; yAxis: { max?: number; gridIndex: number }[]; series: { xAxisIndex: number; yAxisIndex: number }[]; title: { text: string }[] };
    assert.equal(o.grid.length, 3);
    assert.deepEqual(o.title.map((t) => t.text), ['Online', 'Retail', 'Partner']);
    assert.deepEqual(o.yAxis.map((a) => [a.gridIndex, a.max]), [[0, 300], [1, 300], [2, 300]]);
    assert.deepEqual(o.series.map((s) => [s.xAxisIndex, s.yAxisIndex]), [[0, 0], [1, 1], [2, 2]]);
    // a click: the panel's own mark, and its facet's value
    assert.deepEqual(d.keyAt(1, 1), { region: 'APAC', channel: 'Retail' });
    assert.ok(svg(d, 900, 500).includes('Partner'));
  });
  it('a matrix of rows and columns; a stack keeps each panel\'s own scale, said', () => {
    const grid = table([
      { name: 'day', type: 'StrictDate', values: ['2024-01-01', '2024-01-01', '2024-02-01', '2024-02-01'] },
      { name: 'region', type: 'String', values: ['EMEA', 'APAC', 'EMEA', 'APAC'] },
      { name: 'channel', type: 'String', values: ['Online', 'Retail', 'Retail', 'Online'] },
      { name: 'sum_revenue', type: 'Float', values: [1, 2, 3, 4] },
    ]);
    const d = facetOption(of('line', { x: 'day', facet: { row: 'region', column: 'channel' }, options: { ...OPTIONS, sort: { by: 'x', direction: 'asc' } } }), grid);
    assert.equal((d.option as { grid: unknown[] }).grid.length, 4);
    const stacked = facetOption(of('bar', { x: 'region', facet: { column: 'channel' }, options: { ...OPTIONS, stack: 'stacked' } }), rows);
    assert.match(stacked.notes.join(), /own scale/);
    // a series keeps its colour in every panel, whatever order each panel met it in
    const split = facetOption(of('bar', { x: 'day', split: 'channel', facet: { column: 'region' } }), grid);
    const byName = new Map<string, Set<string>>();
    for (const s of (split.option as { series: { name: string; itemStyle: { color: string } }[] }).series) {
      byName.set(s.name, (byName.get(s.name) ?? new Set()).add(s.itemStyle.color));
    }
    assert.ok([...byName.values()].every((c) => c.size === 1), 'one colour per series name');
    assert.equal(new Set([...byName.values()].map((c) => [...c][0])).size, byName.size, 'and each its own');
    // and its place: each panel's series in one order, and a click on one still says its own split's value
    const merged = (split.option as { series: { name: string; xAxisIndex: number }[] }).series;
    const panels = [...new Set(merged.map((x) => x.xAxisIndex))].map((a) => merged.filter((x) => x.xAxisIndex === a).map((x) => x.name));
    const common = panels.map((p) => p.filter((n) => panels.every((q) => q.includes(n))));
    assert.ok(common.every((p) => JSON.stringify(p) === JSON.stringify(common[0])), JSON.stringify(panels));
    merged.forEach((x, si) => {
      const key = split.keyAt(si, 0);
      if (key) assert.equal(key['channel'], x.name, `series ${si}`);
    });
  });
});

describe('a scatter matrix: each pair of the values, row by row', () => {
  const three = [{ column: 'revenue', fn: 'sum' as const }, { column: 'cost', fn: 'sum' as const }, { column: 'profit', fn: 'sum' as const }];
  it('plots rows, has nothing across, and two to five values', () => {
    const { x: _x, ...spec } = of('matrix', { y: three });
    assert.deepEqual(chartProblems(spec, CUBE), []);
    assert.match(chartProblems(of('matrix', { y: three }), CUBE).join(), /no column across/);
    assert.match(chartProblems({ ...spec, y: three.slice(0, 1) }, CUBE).join(), /two to 5 values/);
    const s = chartSnapshot(CUBE, spec);
    assert.deepEqual([s.rows, s.measures], [[], []], 'rows as they are, not groups');
    assert.deepEqual(s.columns.map((c) => c.name).sort(), ['cost', 'profit', 'revenue']);
  });
  it('a scatter in each cell off the diagonal, each value\'s name on it; a colour per group', () => {
    const rows = table([
      { name: 'revenue', type: 'Float', values: [10, 20, 30] },
      { name: 'cost', type: 'Float', values: [5, 8, 20] },
      { name: 'profit', type: 'Float', values: [5, 12, 10] },
      { name: 'channel', type: 'String', values: ['Online', 'Retail', 'Online'] },
    ]);
    const { x: _x, ...spec } = of('matrix', { y: three, split: 'channel' });
    const d = kindOption(spec, rows);
    const o = d.option as { grid: unknown[]; series: { data: number[][]; name: string; xAxisIndex: number }[]; title: { text: string }[] };
    assert.equal(o.grid.length, 9);
    assert.equal(o.series.length, 6 * 2, 'six cells off the diagonal, a series per group in each');
    assert.deepEqual(o.title.map((t) => t.text), ['revenue', 'cost', 'profit'], 'each column by its name: rows are not aggregated');
    // the first row's second cell: cost across, revenue up, Online's rows
    assert.deepEqual(o.series[0]!.data, [[5, 10], [20, 30]]);
    assert.ok(svg(d, 700, 700).includes('Retail'));
  });
});


describe('the audit of batch 3: what each fix pins', () => {
  it('a live chart of one series takes no split from the cube; one of several values keeps its own', () => {
    // the cube groups by region then channel: a live bar would take channel as its split
    const waterfall = followCube(of('waterfall', { x: 'channel' }), CUBE);
    assert.equal(waterfall.split, undefined);
    assert.deepEqual(chartProblems(waterfall, CUBE), []);
    const two = [{ column: 'cost', fn: 'sum' as const }, { column: 'revenue', fn: 'sum' as const }];
    const dumbbell = followCube(of('dumbbell', { x: 'channel', y: two }), CUBE);
    assert.deepEqual(dumbbell.y, two);
    assert.deepEqual(chartProblems(dumbbell, CUBE), []);
  });
  it('a split polar bar\'s hover says its own series\' value', () => {
    const rows = table([
      { name: 'region', type: 'String', values: ['EMEA', 'EMEA'] },
      { name: 'channel', type: 'String', values: ['Online', 'Retail'] },
      { name: 'sum_revenue', type: 'Float', values: [100, 7] },
    ]);
    const d = kindOption(of('polar', { split: 'channel' }), rows, undefined, (v) => String(v));
    const f = (d.option as { tooltip: { formatter: (p: unknown) => string } }).tooltip.formatter;
    assert.match(f({ dataIndex: 0, seriesIndex: 1, seriesName: 'Retail', value: 7 }), /<b>7<\/b>/);
  });
  it('a matrix of small multiples keeps each panel in its own row\'s and column\'s cell, an empty pair empty', () => {
    const rows = table([
      { name: 'region', type: 'String', values: ['EMEA', 'APAC', 'APAC'] },
      { name: 'channel', type: 'String', values: ['Online', 'Online', 'Retail'] },
      { name: 'day', type: 'StrictDate', values: ['2024-01-01', '2024-01-01', '2024-01-01'] },
      { name: 'sum_revenue', type: 'Float', values: [1, 2, 3] },
    ]);
    // EMEA has no Retail: APAC's two panels stay in the Online and Retail columns of the second row
    const d = facetOption(of('bar', { x: 'day', facet: { row: 'region', column: 'channel' } }), rows);
    const grids = (d.option as { grid: { left: string; top: string }[] }).grid;
    assert.equal(grids.length, 3);
    assert.equal(grids[1]!.left, grids[0]!.left, 'APAC Online under EMEA Online');
    assert.notEqual(grids[2]!.left, grids[1]!.left, 'APAC Retail in the Retail column');
  });
  it('a calendar of a date-time needs the day grain; a cut waterfall\'s last bar says it is of what is shown', () => {
    const s: CubeSnapshot = { ...CUBE, columns: [...CUBE.columns, { name: 'at', type: 'DateTime' }] };
    assert.match(chartProblems(of('calendar', { x: 'at' }), s).join(), /day grain/);
    assert.deepEqual(chartProblems(of('calendar', { x: 'at', grain: 'day' }), s), []);
    const { start, end } = waterfallNames('sum_revenue');
    const rows = table([
      { name: 'region', type: 'String', values: ['EMEA', 'APAC'] },
      { name: 'sum_revenue', type: 'Float', values: [4, 3] },
      { name: start, type: 'Float', values: [0, 4] },
      { name: end, type: 'Float', values: [4, 7] },
    ]);
    const cut = kindOption(of('waterfall', { options: { ...OPTIONS, limit: 2 } }), rows);
    assert.ok(svg(cut).includes('Total shown'));
    assert.ok(svg(kindOption(of('waterfall'), rows)).includes('>Total<'));
  });
});

describe('a KPI by a period', () => {
  it('its trend reads the period\'s column and names each period (batch 2 drew no trend for a KPI with a grain)', () => {
    const total = table([{ name: 'sum_revenue', type: 'Float', values: [30] }]);
    const trend = table([
      { name: 'day__month', type: 'StrictDate', values: ['2024-02-01', '2024-01-01'] },
      { name: 'sum_revenue', type: 'Float', values: [20, 10] },
    ]);
    const d = kpiDrawing(of('kpi', { x: 'day', grain: 'month' }), total, trend);
    assert.ok(d.trend, 'a trend is drawn');
    const xs = (d.trend!.option as { xAxis: { data: string[] } }).xAxis.data;
    assert.deepEqual(xs, ['Jan 2024', 'Feb 2024'], 'oldest first, each named as a month');
  });
});


describe('batch 4: a box plot, its five figures the database\'s', () => {
  const BOX = of('boxplot', { y: boxMeasures('revenue') });
  const problems = (spec: ChartSpec) => chartProblems(spec, CUBE).join(' ');
  it('plots its one number column\'s five figures, in order; one series; no show-as', () => {
    assert.deepEqual(chartProblems(BOX, CUBE), []);
    assert.match(problems(of('boxplot')), /five figures of one column/);
    assert.match(problems(of('boxplot', { y: [...boxMeasures('revenue')].reverse() })), /five figures go in order/);
    assert.match(problems(of('boxplot', { y: [...boxMeasures('revenue').slice(0, 4), { column: 'cost', fn: 'max' }] })), /five figures of one column/);
    assert.match(problems(of('boxplot', { y: boxMeasures('channel') })), /'channel' is not a number: it has no quartiles/);
    assert.match(problems({ ...BOX, split: 'channel' }), /A box plot draws one series/);
    assert.match(problems({ ...BOX, y: BOX.y.map((m) => ({ ...m, showAs: { kind: 'rank' as const } })) }), /computes its own figures/);
  });
  it('a chart told the kind takes its first value\'s column\'s five figures; a group-level column has none', () => {
    const made = editChart(of('bar', { y: [{ column: 'cost', fn: 'sum' }] }), { mark: 'boxplot' }, CUBE);
    assert.deepEqual(made.y, boxMeasures('cost'));
    assert.deepEqual(chartProblems(made, CUBE), []);
    const group = editChart(of('bar', { y: [{ column: 'margin', fn: 'value' }] }), { mark: 'boxplot' }, CUBE);
    assert.deepEqual(group.y, [{ column: 'margin', fn: 'value' }]);
    assert.match(chartProblems(group, CUBE).join(' '), /five figures of one column/);
  });
  it('asks the database for each figure per category, ordered by its median; keeps its five as the cube regroups; drills', () => {
    const s = chartSnapshot(CUBE, BOX);
    assert.deepEqual(s.measures?.map((m) => `${m.name}:${m.fn}`), [
      'min_revenue:min', 'percentile25_revenue:percentile25', 'median_revenue:median',
      'percentile75_revenue:percentile75', 'max_revenue:max',
    ]);
    assert.deepEqual(s.sorts, [{ column: 'median_revenue', direction: 'desc' }, { column: 'region', direction: 'asc' }]);
    const followed = followCube({ ...BOX, x: 'channel' }, CUBE);
    assert.equal(followed.x, 'region');
    assert.deepEqual(followed.y, BOX.y);
    assert.deepEqual(drillLevels(BOX, CUBE), ['region', 'channel']);
  });
  it('is drawn: a box per category, whiskers to the least and the most; a category with no numbers has no box', () => {
    const rows = table([
      { name: 'region', type: 'String', values: ['EMEA', 'APAC', 'LATAM'] },
      { name: 'min_revenue', type: 'Float', values: [10, 5, null] },
      { name: 'percentile25_revenue', type: 'Float', values: [20, 15, null] },
      { name: 'median_revenue', type: 'Float', values: [30, 25, null] },
      { name: 'percentile75_revenue', type: 'Float', values: [40, 35, null] },
      { name: 'max_revenue', type: 'Float', values: [90, 45, null] },
    ]);
    const d = kindOption(BOX, rows);
    const series = (d.option as { series: { type: string; data: { value: unknown[] }[] }[] }).series;
    assert.equal(series[0]?.type, 'boxplot');
    assert.deepEqual(series[0]?.data.map((b) => b.value), [[10, 20, 30, 40, 90], [5, 15, 25, 35, 45], ['-', '-', '-', '-', '-']]);
    const out = svg(d);
    assert.ok(out.includes('EMEA') && out.includes('APAC'));
    assert.deepEqual(d.keyAt(0, 1), { region: 'APAC' });
    const tip = (d.option as { tooltip: { formatter: (p: unknown) => string } }).tooltip.formatter({ dataIndex: 0, data: { ci: 0 } });
    assert.match(tip, /least: <b>10<\/b>.*first quartile: <b>20<\/b>.*median: <b>30<\/b>.*third quartile: <b>40<\/b>.*most: <b>90<\/b>/);
    assert.match(describeChart(BOX, rows), /^Box plot of the spread of revenue by region: 3 rows\.$/);
    // side on: the categories down, the values across
    const side = kindOption({ ...BOX, options: { ...OPTIONS, orientation: 'horizontal' } }, rows);
    assert.equal((side.option as { yAxis: { type: string } }).yAxis.type, 'category');
    assert.ok(svg(side).includes('EMEA'));
  });
});

describe('the phase 5 audit: what each fix pins', () => {
  const BOX = of('boxplot', { y: boxMeasures('revenue') });
  const problems = (spec: ChartSpec) => chartProblems(spec, CUBE).join(' ');
  it('a chart\'s own calculated number column has quartiles, as the form offers it', () => {
    const calc = { stage: 'row' as const, column: {
      name: 'double', lambda: lambda(['x'], fn('times', property(variable('x'), 'revenue'), { _type: 'integer', value: 2 } as never)),
      type: 'Float',
    } };
    assert.deepEqual(chartProblems({ ...BOX, y: boxMeasures('double'), calcs: [calc] }, CUBE), []);
  });
  it('a box plot by month or by bins says each box\'s end, so its click filters to the period', () => {
    const across = grainName('sold', 'month');
    const rows = table([
      { name: across, type: 'StrictDate', values: ['2024-01-01', '2024-02-01'] },
      ...boxMeasures('revenue').map((m, k) => ({ name: `${m.fn}_revenue`, type: 'Float', values: [10 + k, 20 + k] })),
      { name: AXIS_UPPER, type: 'StrictDate', values: ['2024-02-01', '2024-03-01'] },
    ]);
    const d = kindOption({ ...BOX, x: 'sold', grain: 'month' }, rows);
    assert.equal(d.upperAt?.(1, { ci: 1 }), '2024-03-01');
    assert.equal(d.upperAt?.(0), '2024-02-01');
    // a calendar's day says its category itself: its index is within its year's series
    const days = table([
      { name: grainName('sold', 'day'), type: 'StrictDate', values: ['2023-12-31', '2024-01-01'] },
      { name: 'sum_revenue', type: 'Float', values: [1, 2] },
      { name: AXIS_UPPER, type: 'StrictDate', values: ['2024-01-01', '2024-01-02'] },
    ]);
    const cal = kindOption(of('calendar', { x: 'sold', grain: 'day' }), days);
    assert.equal(cal.upperAt?.(0, { ci: 1 }), '2024-01-02');
  });
  it('a box plot taking a kind of one value keeps its median; a kind of several keeps the five, for its rules', () => {
    assert.deepEqual(editChart(BOX, { mark: 'bar' }, CUBE).y, [{ column: 'revenue', fn: 'median' }]);
    assert.deepEqual(editChart(BOX, { mark: 'pie' }, CUBE).y, [{ column: 'revenue', fn: 'median' }]);
    assert.deepEqual(editChart(BOX, { mark: 'dumbbell' }, CUBE).y, BOX.y);
  });
  it('a pie, a lollipop, a KPI and a heatmap draw one value: a second is refused, never asked and left undrawn', () => {
    const two = [REVENUE_SUM, { column: 'cost', fn: 'sum' } as const];
    for (const mark of ['pie', 'lollipop', 'kpi', 'heatmap'] as const) {
      assert.match(problems(of(mark, { y: two, ...(mark === 'heatmap' ? { split: 'channel' } : {}) })), /plots one value/, mark);
    }
  });
  it('the form of a box plot picks its column -- its five figures with it -- and offers no aggregate and no values to show', () => {
    const doc = new JSDOM('<!doctype html><div id="form"></div>').window.document;
    const form = doc.getElementById('form')!;
    let spec: ChartSpec = BOX;
    paintChartForm({
      doc, spec: () => spec, snapshot: () => CUBE, setOption: () => undefined,
      set: (change) => { spec = editChart(spec, change, CUBE); },
    }, form);
    const labels = [...form.querySelectorAll('.dc-field-label')].map((l) => l.textContent);
    assert.ok(labels.includes('Value:') && !labels.includes('Aggregate:'), labels.join(' '));
    assert.ok(!(form.textContent ?? '').includes('Show values'));
    const value = [...form.querySelectorAll('.dc-field')].find((f) => f.querySelector('.dc-field-label')?.textContent === 'Value:')!
      .querySelector('select')!;
    value.value = 'cost';
    value.dispatchEvent(new doc.defaultView!.Event('change'));
    assert.deepEqual(spec.y, boxMeasures('cost'));
  });
  it('the five figures in another order, or carrying a mark or an axis, are refused in their own words', () => {
    const swapped = [BOX.y[1]!, BOX.y[0]!, ...BOX.y.slice(2)];
    assert.match(problems({ ...BOX, y: swapped }), /five figures go in order: min, percentile25, median, percentile75, max/);
    const placed = { ...BOX, y: BOX.y.map((m) => ({ ...m, axis: 'left' as const })) };
    assert.deepEqual(chartProblems(placed, CUBE), ['A box plot draws its five figures as one box: a value\'s own mark or axis has no place on it.']);
  });
});

describe('batch 3\'s leftovers: a Marimekko and a waffle (shares as cells, the database\'s), and a Gantt', () => {
  const problems = (spec: ChartSpec) => chartProblems(spec, CUBE).join(' ');
  const MEKKO = of('marimekko', { split: 'channel', options: { ...OPTIONS, sort: { by: 'x', direction: 'asc' } } });
  const GANTT = of('gantt', { y: [{ column: 'day', fn: 'min' }, { column: 'day', fn: 'max' }], options: { ...OPTIONS, sort: { by: 'y', direction: 'asc' } } });
  it('their rules: a Marimekko\'s split, its order across and one value; a waffle\'s one value; a Gantt\'s two dates', () => {
    assert.deepEqual(chartProblems(MEKKO, CUBE), []);
    const { split: _split, ...unsplit } = MEKKO;
    assert.match(problems(unsplit), /what each of its columns is made of/);
    assert.match(problems({ ...MEKKO, options: OPTIONS }), /order by category/);
    assert.match(problems({ ...MEKKO, y: [REVENUE_SUM, { column: 'cost', fn: 'sum' }] }), /A Marimekko plots one value/);
    assert.deepEqual(chartProblems(of('waffle'), CUBE), []);
    assert.match(problems(of('waffle', { split: 'channel' })), /A waffle draws one series/);
    assert.match(problems(of('waffle', { y: [REVENUE_SUM, { column: 'cost', fn: 'sum' }] })), /A waffle plots one value/);
    assert.match(problems(of('waffle', { y: [{ ...REVENUE_SUM, showAs: { kind: 'rank' } }] })), /computes its own figures/);
    assert.deepEqual(chartProblems(GANTT, CUBE), []);
    assert.match(problems(of('gantt')), /two values, each the min or max of a date column/);
    assert.match(problems({ ...GANTT, y: [{ column: 'day', fn: 'min' }, { column: 'revenue', fn: 'max' }] }), /each the min or max of a date column/);
    assert.match(problems({ ...GANTT, y: [{ column: 'day', fn: 'min' }, { column: 'day', fn: 'unique' }] }), /each the min or max of a date column/);
  });
  it('a chart told the kind takes its order: a Marimekko across, a Gantt from the earliest start', () => {
    assert.deepEqual(editChart(of('bar', { split: 'channel' }), { mark: 'marimekko' }, CUBE).options.sort, { by: 'x', direction: 'asc' });
    assert.deepEqual(editChart(of('bar'), { mark: 'gantt' }, CUBE).options.sort, { by: 'y', direction: 'asc' });
    // and keeps it as its column across changes, as Python's grid.chart(mark, x=...) tells both at once
    assert.deepEqual(editChart(of('bar', { split: 'channel' }), { mark: 'marimekko', x: 'channel', split: 'region' }, CUBE).options.sort,
      { by: 'x', direction: 'asc' });
    assert.deepEqual(editChart(of('bar'), { mark: 'gantt', x: 'channel' }, CUBE).options.sort, { by: 'y', direction: 'asc' });
  });
  it('the database computes each cell: a Marimekko\'s four edges, a waffle\'s range of cells along the chart\'s order', () => {
    const m = chartSnapshot(CUBE, MEKKO);
    const names = marimekkoNames('sum_revenue');
    for (const n of [names.x0, names.x1, names.y0, names.y1, names.width]) assert.ok(m.groupDerived?.some((d) => d.name === n), n);
    const running = m.groupDerived?.find((d) => d.name === `${names.x0}__running`);
    assert.deepEqual(running?.window?.order, [{ column: 'region', direction: 'asc' }, { column: 'channel', direction: 'asc' }]);
    const w = chartSnapshot(CUBE, of('waffle'));
    const { from, to } = waffleNames('sum_revenue');
    assert.ok(w.groupDerived?.some((d) => d.name === from) && w.groupDerived?.some((d) => d.name === to));
    assert.deepEqual(w.groupDerived?.find((d) => d.name === `${to}__running`)?.window?.order,
      [{ column: 'sum_revenue', direction: 'desc' }, { column: 'region', direction: 'asc' }]);
  });
  it('a Marimekko drawn: a cell per row from its edges, a series per part, each cell\'s click its column and part', () => {
    const n = marimekkoNames('sum_revenue');
    const rows = table([
      { name: 'region', type: 'String', values: ['AMER', 'AMER', 'EMEA'] },
      { name: 'channel', type: 'String', values: ['Online', 'Store', 'Online'] },
      { name: 'sum_revenue', type: 'Float', values: [300, 100, 400] },
      { name: n.x0, type: 'Float', values: [0, 0, 0.5] },
      { name: n.x1, type: 'Float', values: [0.5, 0.5, 1] },
      { name: n.y0, type: 'Float', values: [0, 0.75, 0] },
      { name: n.y1, type: 'Float', values: [0.75, 1, 1] },
    ]);
    const d = kindOption(MEKKO, rows);
    const series = (d.option as { series: { name: string; data: { value: number[]; key?: object }[] }[] }).series;
    assert.deepEqual(series.slice(0, 2).map((x) => [x.name, x.data.map((c) => c.value)]), [
      ['Online', [[0, 0.5, 0, 0.75], [0.5, 1, 0, 1]]],
      ['Store', [[0, 0.5, 0.75, 1]]],
    ]);
    assert.deepEqual(d.keyAt(1, 0, series[1]!.data[0]), { region: 'AMER', channel: 'Store' });
    const out = svg(d);
    assert.ok(out.includes('AMER') && out.includes('EMEA'), 'each column named under it');
  });
  it('a waffle drawn: each category the cells of its range; a category with none said', () => {
    const { from, to } = waffleNames('sum_revenue');
    const rows = table([
      { name: 'region', type: 'String', values: ['EMEA', 'APAC', 'LATAM'] },
      { name: 'sum_revenue', type: 'Float', values: [600, 400, 0] },
      { name: from, type: 'Integer', values: [0, 60, 100] },
      { name: to, type: 'Integer', values: [60, 100, 100] },
    ]);
    const d = kindOption(of('waffle'), rows);
    const series = (d.option as { series: { name: string; data: { value: number[]; ci: number }[] }[] }).series;
    assert.deepEqual(series.map((x) => x.data.length), [60, 40, 0]);
    assert.deepEqual(series[1]!.data[0]!.value, [0, 6], 'the 61st cell: the seventh row\'s first');
    assert.match(d.notes.join(' '), /1 category fills no cell .*: LATAM/);
    assert.deepEqual(d.keyAt(1, 3, series[1]!.data[3]), { region: 'APAC' });
    // a hundred cells drawn: the SVG's shapes beyond those of the waffle drawn with none
    const shapes = (out: string): number => out.split('<path').length;
    const bare = { ...d, option: { ...(d.option as object), series: series.map((x) => ({ ...x, data: [] })) } } as ChartDrawing;
    assert.equal(shapes(svg(d)) - shapes(svg(bare)), 100);
  });
  it('a Gantt drawn: each row a bar from its start to its end on a time axis; a row with no end said', () => {
    const rows = table([
      { name: 'region', type: 'String', values: ['EMEA', 'APAC', 'AMER'] },
      { name: 'min_day', type: 'StrictDate', values: ['2024-01-15', '2024-02-01', '2024-03-01'] },
      { name: 'max_day', type: 'StrictDate', values: ['2024-03-05', '2024-02-20', null] },
    ]);
    const d = kindOption(GANTT, rows);
    const option = d.option as { xAxis: { type: string }; series: { data: { value: number[]; ci: number }[] }[] };
    assert.equal(option.xAxis.type, 'time');
    assert.deepEqual(option.series[0]!.data.map((b) => [b.ci, b.value[1], b.value[2]]), [
      [0, new Date(2024, 0, 15).getTime(), new Date(2024, 2, 5).getTime()],
      [1, new Date(2024, 1, 1).getTime(), new Date(2024, 1, 20).getTime()],
    ]);
    assert.match(d.notes.join(' '), /1 row has no start or no end: AMER/);
    assert.deepEqual(d.keyAt(0, 1, option.series[0]!.data[1]), { region: 'APAC' });
    assert.ok(svg(d).includes('EMEA'));
  });
});

describe('the phase 7 audit: what each fix pins', () => {
  const MEKKO = of('marimekko', { split: 'channel', options: { ...OPTIONS, sort: { by: 'x', direction: 'asc' } } });
  it('a Marimekko cell knows its category: a click on a period filters to that period\'s end; its names are the axis\'s', () => {
    const n = marimekkoNames('sum_revenue');
    const across = grainName('day', 'month');
    const rows = table([
      { name: across, type: 'StrictDate', values: ['2024-01-01', '2024-02-01', '2024-02-01'] },
      { name: 'channel', type: 'String', values: ['Online', 'Online', 'Store'] },
      { name: 'sum_revenue', type: 'Float', values: [1, 1, 2] },
      { name: n.x0, type: 'Float', values: [0, 0.25, 0.25] },
      { name: n.x1, type: 'Float', values: [0.25, 1, 1] },
      { name: n.y0, type: 'Float', values: [0, 0, 0.33] },
      { name: n.y1, type: 'Float', values: [1, 0.33, 1] },
      { name: AXIS_UPPER, type: 'StrictDate', values: ['2024-02-01', '2024-03-01', '2024-03-01'] },
    ]);
    const d = kindOption({ ...MEKKO, x: 'day', grain: 'month' }, rows);
    const series = (d.option as { series: { data: { ci?: number; text?: string }[] }[]; xAxis: { axisLabel: { show: boolean } } }).series;
    // the Store cell is its series' first, in February's column
    assert.equal(d.upperAt?.(0, series[1]!.data[0]), '2024-03-01');
    assert.deepEqual(series[2]!.data.map((c) => c.text), ['Jan 2024', 'Feb 2024']);
    assert.equal((d.option as { xAxis: { axisLabel: { show: boolean } } }).xAxis.axisLabel.show, false);
  });
  it('a waffle or a Marimekko of values below zero, or a whole of nothing, says so rather than drawing a wrong whole', () => {
    const { from, to } = waffleNames('sum_revenue');
    const negative = table([
      { name: 'region', type: 'String', values: ['EMEA', 'APAC'] },
      { name: 'sum_revenue', type: 'Float', values: [50, -20] },
      { name: from, type: 'Integer', values: [0, 100] },
      { name: to, type: 'Integer', values: [100, 70] },
    ]);
    assert.deepEqual(kindOption(of('waffle'), negative).notes, ['A waffle shows parts of a whole: 1 value is below zero.']);
    const nothing = table([
      { name: 'region', type: 'String', values: ['EMEA', 'APAC'] },
      { name: 'sum_revenue', type: 'Float', values: [0, 0] },
      { name: from, type: 'Integer', values: [0, null] },
      { name: to, type: 'Integer', values: [null, null] },
    ]);
    assert.deepEqual(kindOption(of('waffle'), nothing).notes, ['The values add up to nothing: a waffle has no hundred to share.']);
    const n = marimekkoNames('sum_revenue');
    const mekko = table([
      { name: 'region', type: 'String', values: ['EMEA'] }, { name: 'channel', type: 'String', values: ['Online'] },
      { name: 'sum_revenue', type: 'Float', values: [-1] },
      ...[n.x0, n.x1, n.y0, n.y1].map((name) => ({ name, type: 'Float', values: [0] })),
    ]);
    assert.deepEqual(kindOption(MEKKO, mekko).notes, ['A Marimekko shows parts of a whole: 1 value is below zero.']);
  });
  it('a Gantt of a time of day is refused; one that ends before it starts is said', () => {
    const timed: CubeSnapshot = { ...CUBE, columns: [...CUBE.columns, { name: 'at', type: 'StrictTime' }] };
    assert.match(chartProblems(of('gantt', { y: [{ column: 'at', fn: 'min' }, { column: 'at', fn: 'max' }] }), timed).join(' '), /min or max of a date column/);
    const rows = table([
      { name: 'region', type: 'String', values: ['EMEA'] },
      { name: 'min_day', type: 'StrictDate', values: ['2024-03-01'] },
      { name: 'max_day', type: 'StrictDate', values: ['2024-01-01'] },
    ]);
    const gantt = of('gantt', { y: [{ column: 'day', fn: 'min' }, { column: 'day', fn: 'max' }] });
    assert.match(kindOption(gantt, rows).notes.join(' '), /1 row ends before it starts: EMEA/);
  });
  it('a map taking a Marimekko or a Gantt takes the kind\'s order (the options built once)', () => {
    const map: ChartSpec = of('map', { x: 'region', options: { ...OPTIONS, limit: 250 } });
    const mekko = editChart(map, { mark: 'marimekko', split: 'channel' }, CUBE);
    assert.deepEqual([mekko.options.sort, mekko.options.limit], [{ by: 'x', direction: 'asc' }, 50]);
    assert.deepEqual(editChart(map, { mark: 'gantt' }, CUBE).options.sort, { by: 'y', direction: 'asc' });
  });
  it('the form: no values to show on these kinds, no legend on a Gantt, and its start a date column\'s least', () => {
    const doc = new JSDOM('<!doctype html><div id="form"></div>').window.document;
    const form = doc.getElementById('form')!;
    let spec: ChartSpec = { ...of('bar'), mark: 'gantt', y: [{ column: 'revenue', fn: 'sum' }] };
    paintChartForm({ doc, spec: () => spec, snapshot: () => CUBE, setOption: () => undefined,
      set: (change) => { spec = editChart(spec, change, CUBE); } }, form);
    const labels = [...form.querySelectorAll('.dc-field-label')].map((l) => l.textContent);
    assert.ok(!labels.includes('Legend:') && !(form.textContent ?? '').includes('Show values'), labels.join(' '));
    const start = [...form.querySelectorAll('.dc-field')].find((f) => f.querySelector('.dc-field-label')?.textContent === 'Start:')!
      .querySelector('select')!;
    start.value = 'day';
    start.dispatchEvent(new doc.defaultView!.Event('change'));
    assert.deepEqual(spec.y[0], { column: 'day', fn: 'min' });
    for (const mark of ['marimekko', 'waffle'] as const) {
      const f = doc.createElement('div');
      paintChartForm({ doc, spec: () => of(mark, { split: 'channel' }), snapshot: () => CUBE, setOption: () => undefined, set: () => undefined }, f);
      assert.ok(!(f.textContent ?? '').includes('Show values'), mark);
    }
  });
});
