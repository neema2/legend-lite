// THE CHART FACE (B3, docs/TS_DECISIONS_2026_11_07.md §4, the builders): a tile built in code is the tile its changes make
// by the one rule (editTimeseries), its series the builder's terms, a series plotted read by name by a later one; what
// the rule refuses, the face refuses in its words.

import assert from 'node:assert/strict';
import { it } from 'node:test';

import { Chart, column, mean, Plotted, Returns, volatility } from '../src/timeseries-builder.ts';
import { editTimeseries, TsEditRefusal, type TsGridColumns } from '../src/timeseries-edit.ts';
import type { TimeseriesSpec } from '../src/timeseries-spec.ts';
import { treeOf } from '../src/timeseries-text.ts';

const PRICES: TsGridColumns = {
  cube: 'prices', title: 'Prices',
  columns: [{ name: 'symbol', type: 'String' }, { name: 'day', type: 'StrictDate' }, { name: 'close', type: 'Float' }], rows: ['symbol'],
  filter: undefined,
};
const RATES: TsGridColumns = {
  cube: 'rates', title: 'Rates',
  columns: [{ name: 'tenor', type: 'String' }, { name: 'obs', type: 'StrictDate' }, { name: 'yield', type: 'Float' }], rows: ['tenor'],
  filter: undefined,
};

it('a tile built in code is the tile its changes make by the one rule, a series read by name', () => {
  const chart = new Chart().read(PRICES).read(RATES);
  const spx = column('close', 'SPX');
  const a = chart.plot(spx, { name: 'S&P 500' });
  const b = chart.plot(volatility(spx, 22, Returns.LOGARITHMIC), { pane: 1, name: 'Volatility' });
  const c = chart.plot(a.div(mean(a, 50)), { name: 'Over its mean' });
  const d = chart.plot(column('rates.yield', '10Y'));
  chart.grain('week').range({ last: '1y' }).view({ legend: true }).pane(1, { height: 2 }).hide(b);
  assert.deepEqual([a, b, c, d].map((p) => p.id), ['A', 'B', 'C', 'D']);
  assert.ok(a instanceof Plotted);
  // every change the face made, made again by the rule, makes the same tile
  let spec: TimeseriesSpec = { sources: [], series: [] };
  for (const change of chart.changes) spec = editTimeseries(spec, change);
  assert.deepEqual(chart.spec, spec);
  assert.deepEqual(chart.spec.sources.map((s) => s.alias), ['prices', 'rates']);
  assert.deepEqual(chart.spec.series.map((s) => [s.id, s.text]), [
    ['A', "close['SPX']"],
    ['B', "volatility(close['SPX'], w => 22, type => 'log')"],
    ['C', 'A / mean(A, 50)'],
    ['D', "rates.yield['10Y']"],
  ]);
  // C reads A by name: its tree a reference, read where it is drawn
  assert.deepEqual(treeOf(chart.spec.series[2]!, chart.spec.series), { kind: 'binary', op: '/', left: { kind: 'ref', series: 'A' },
    right: { kind: 'call', fn: 'mean', args: [{ kind: 'ref', series: 'A' }], params: { w: 50 } } });
  assert.equal(chart.spec.grain, 'week');
  assert.equal(chart.spec.series[1]!.hidden, true);
});

it('a series plotted again keeps its id; what the rule refuses, the face refuses in its words', () => {
  const chart = new Chart().read(PRICES);
  const a = chart.plot(column('close', 'SPX'));
  chart.plot(mean(a, 20));
  assert.equal(chart.replot(a, column('close', 'NDX'), { name: 'Nasdaq' }).id, 'A');
  assert.equal(chart.spec.series[0]!.name, 'Nasdaq');
  assert.throws(() => chart.unplot(a), (e: unknown) => e instanceof TsEditRefusal && /series B reads series A/.test(e.message));
  assert.throws(() => chart.read(PRICES), (e: unknown) => e instanceof TsEditRefusal && /Prices is read already/.test(e.message));
  // a refused change is no change: the tile as it was
  assert.equal(chart.changes.length, 4);
  chart.unplot('B').unplot(a);
  assert.deepEqual(chart.spec.series, []);
});

it('a column shaded where it is not 0 is a condition series of its own, plotted', () => {
  const chart = new Chart().read(PRICES);
  chart.plot(column('close', 'SPX'));
  const shaded = chart.shade('close', { where: [{ value: 'VIX' }], name: 'Stress' });
  assert.equal(shaded.id, 'B');
  assert.deepEqual([chart.spec.series[1]!.text, chart.spec.series[1]!.name], ["close['VIX'] <> 0", 'Stress']);
});
