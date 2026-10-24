// The chart model's rules, case by case, DOM-free (chart-spec.ts): what the reader refuses of a saved chart and in
// which words (a malformed chart, a newer DataCube's word, a key it has no place for -- refused by name, never drawn as
// something else), a chart changed by hand (editChart: the form's, and Python's through chartFrom), a following chart
// regrouped as its grid is (followCube) and what it refuses to replace (followRefusal), what stops a chart from being
// drawn (chartProblems), Open in Grid's values as Column Properties (chartValuesAsColumns), drill-down, a click's and a
// brush's conditions, and what the chart's query asks the database. The last block holds rules the model does not keep
// yet, each found writing these: they fail until it does.
// docs/DATACUBE_CHARTS_DESIGN_2026_10_09.md §3.1-3.2, docs/DATACUBE_ONE_GRID_EDIT_PATH_2026_10_17.md §3.5, AGENTS.md
// invariant 4.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { element, fn, lambda, property, variable } from '../../pure-protocol/src/index.ts';
import {
  AXIS_UPPER,
  brushConditions,
  chartColumns,
  chartFrom,
  chartProblems,
  chartQuery,
  chartSnapshot,
  chartValuesAsColumns,
  columnsRead,
  DEFAULT_CHART_LIMIT,
  DEFAULT_OPTIONS,
  drillDown,
  drilledView,
  drillLevels,
  drillRefusal,
  drillUp,
  editChart,
  followCube,
  followRefusal,
  markConditions,
  MAX_FACETS,
  namedMark,
  readChart,
  SCATTER_LIMIT,
  specProblem,
  TIME_LIMIT,
  type ChartMeasure,
  type ChartSpec,
} from '../src/chart-spec.ts';
import { binName, statName, upperName, waterfallNames } from '../src/calc-presets.ts';
import { DEFAULT_CONFIGURATION } from '../src/config.ts';
import { AGGREGATE_FNS, type CubeSnapshot } from '../src/snapshot.ts';

// a desk's trades: two text columns the grid groups by, a day, an instant, a time of day, and two numbers
const CUBE: CubeSnapshot = {
  source: { query: element('trades') },
  columns: [
    { name: 'region', type: 'String' },
    { name: 'desk', type: 'String' },
    { name: 'trade_date', type: 'StrictDate' },
    { name: 'booked_at', type: 'DateTime' },
    { name: 'cutoff', type: 'StrictTime' },
    { name: 'notional', type: 'Float' },
    { name: 'qty', type: 'Integer', kind: 'measure' },
  ],
  derived: [],
  rows: ['region', 'desk'],
  pivotOn: [],
  sorts: [],
  epoch: 1,
};

/** The cube with its notional's Aggregation set to average (Column Properties, folded into its columns). */
const AVERAGED: CubeSnapshot = {
  ...CUBE, columns: CUBE.columns.map((c) => (c.name === 'notional' ? { ...c, aggregate: 'average' as const } : c)),
};

const OPTIONS: ChartSpec['options'] = {
  orientation: 'vertical', stack: 'none', sort: { by: 'y', direction: 'desc' }, limit: 50, labels: false, legend: 'top',
};
const SUM_NOTIONAL: ChartMeasure = { column: 'notional', fn: 'sum' };
const SUM_QTY: ChartMeasure = { column: 'qty', fn: 'sum' };
const of = (mark: ChartSpec['mark'], more: Partial<ChartSpec> = {}): ChartSpec =>
  ({ version: 2, mark, x: 'region', y: [SUM_NOTIONAL], options: OPTIONS, ...more });
const BAR = of('bar');
/** A line of months: a date across by its period, in time order, long. */
const BY_MONTH = of('line', {
  x: 'trade_date', grain: 'month', options: { ...OPTIONS, sort: { by: 'x', direction: 'asc' }, limit: TIME_LIMIT },
});

/** A chart as a page saves it: its options may leave any out (the defaults fill them). */
const SAVED = { version: 2, mark: 'bar', x: 'region', y: [{ column: 'notional', fn: 'sum' }], options: { legend: 'top' } };

/** What the reader says of SAVED with `more` laid over it; fails when it reads it. */
function refusal(more: Record<string, unknown>): string {
  const read = readChart({ ...SAVED, ...more });
  assert.equal(typeof read, 'string', `read, not refused: ${JSON.stringify(more)}`);
  return read as string;
}
/** What the reader says of SAVED with `options` laid over its options. */
const optionRefusal = (options: Record<string, unknown>): string => refusal({ options: { ...SAVED.options, ...options } });
const measureRefusal = (m: unknown): string => refusal({ y: [m] });

describe('the reader refuses a chart it cannot read, in words', () => {
  it('a thing that is no chart, a version it does not read (older or newer), a kind it does not draw', () => {
    for (const v of [null, [], 'bar chart', 7]) assert.equal(readChart(v), 'it is not a chart');
    // no older chart is read forward (the user, 2026-10-17), and a newer one is not guessed at
    assert.equal(refusal({ version: 1 }), 'its version 1 is not one this DataCube reads (2)');
    assert.equal(refusal({ version: 3 }), 'its version 3 is not one this DataCube reads (2)');
    assert.equal(refusal({ version: '2' }), 'its version "2" is not one this DataCube reads (2)');
    const { version: _v, ...unversioned } = SAVED;
    assert.equal(readChart(unversioned), 'its version null is not one this DataCube reads (2)');
    assert.equal(refusal({ mark: 'violin' }), 'its kind "violin" is not one this DataCube draws');
    const { mark: _m, ...unmarked } = SAVED;
    assert.equal(readChart(unmarked), 'its kind null is not one this DataCube draws');
    // a DataCube that draws fewer kinds refuses one it does not
    assert.equal(specProblem({ ...SAVED, mark: 'line' }, ['bar']), 'its kind "line" is not one this DataCube draws');
  });

  it('a chart that plots nothing, or has no options, or holds them as something else', () => {
    const { y: _y, ...noValues } = SAVED;
    assert.equal(readChart(noValues), 'it plots nothing (no y)');
    assert.equal(refusal({ y: 'notional' }), 'its y "notional" is not a list of measures');
    assert.equal(refusal({ y: { column: 'notional', fn: 'sum' } }), 'its y {"column":"notional","fn":"sum"} is not a list of measures');
    const { options: _o, ...noOptions } = SAVED;
    assert.equal(readChart(noOptions), 'it has no options');
    assert.equal(refusal({ options: 'wide' }), 'its options "wide" are not an object');
    assert.equal(refusal({ options: [] }), 'its options [] are not an object');
  });

  it('a key it has no place for -- a newer DataCube\'s, or one misspelt -- is refused by name, with what it does say', () => {
    assert.match(refusal({ colour: 'red' }), /^there is no "colour" in a chart \(it says version, mark, x, .*, frozen, kept\)$/);
    assert.match(optionRefusal({ animate: true }), /^there is no "animate" in its options \(it says orientation, stack, .*, ring\)$/);
  });

  it('a value that is not a column and an aggregate this DataCube knows', () => {
    assert.equal(measureRefusal({ column: 'notional' }), 'measure 1 is not a column and an aggregate');
    assert.equal(measureRefusal({ fn: 'sum' }), 'measure 1 is not a column and an aggregate');
    assert.equal(measureRefusal({ column: 'notional', fn: 'mode' }), 'measure 1 is not a column and an aggregate');
    assert.equal(measureRefusal('notional'), 'measure 1 is not a column and an aggregate');
    // the second value is named as the second
    assert.equal(refusal({ y: [SUM_NOTIONAL, { column: 'qty', fn: 'total' }] }), 'measure 2 is not a column and an aggregate');
    // a group-level column plotted as it is, and every aggregate the grid has, are read
    for (const word of ['value', ...AGGREGATE_FNS]) {
      assert.equal(typeof readChart({ ...SAVED, y: [{ column: 'notional', fn: word }] }), 'object', word);
    }
  });

  it('a value\'s mark, axis and show-as words, and keys it has no place for', () => {
    assert.equal(measureRefusal({ ...SUM_NOTIONAL, colour: 'red' }),
      'there is no "colour" in measure 1 (it says column, fn, mark, axis, showAs)');
    assert.equal(measureRefusal({ ...SUM_NOTIONAL, mark: 'pie' }), 'measure 1\'s mark "pie" is not bar, line or area');
    assert.equal(measureRefusal({ ...SUM_NOTIONAL, axis: 'top' }), 'measure 1\'s axis "top" is not left or right');
    assert.equal(measureRefusal({ ...SUM_NOTIONAL, showAs: { kind: 'cumulative' } }),
      'measure 1 is shown as something this DataCube does not know ({"kind":"cumulative"})');
    assert.equal(measureRefusal({ ...SUM_NOTIONAL, showAs: 'rank' }),
      'measure 1 is shown as something this DataCube does not know ("rank")');
    assert.equal(measureRefusal({ ...SUM_NOTIONAL, showAs: { kind: 'movingAverage', rows: 3, window: 5 } }),
      'there is no "window" in measure 1\'s showAs (it says kind, rows)');
    // each word it knows is read
    const shown = readChart({ ...SAVED, y: [{ ...SUM_NOTIONAL, mark: 'line', axis: 'right', showAs: { kind: 'movingAverage', rows: 5 } }] });
    assert.notEqual(typeof shown, 'string', String(shown));
  });

  it('a mark\'s size, its label and a hover\'s values, each judged as a value and named by its slot', () => {
    assert.equal(refusal({ size: { column: 'qty' } }), 'its size is not a column and an aggregate');
    assert.equal(refusal({ label: { ...SUM_QTY, mark: 'dot' } }), 'its label\'s mark "dot" is not bar, line or area');
    assert.equal(refusal({ tooltip: SUM_QTY }), 'its tooltip is not a list');
    assert.equal(refusal({ tooltip: [SUM_QTY, { column: 'notional' }] }), 'tooltip measure 2 is not a column and an aggregate');
  });

  it('a grain it does not know, and bins that are no width or count of a number', () => {
    assert.equal(refusal({ grain: 'hour' }), 'its grain "hour" is not one of day, week, month, quarter, year');
    for (const bins of [{}, 10, { width: 'wide' }]) {
      assert.equal(refusal({ bins }), 'its bins have neither a width nor a count', JSON.stringify(bins));
    }
    // a bin is some width of a number, and there is a whole count of them
    assert.equal(refusal({ bins: { width: 0 } }), 'its bins\' width 0 is not a positive number');
    assert.equal(refusal({ bins: { width: -5 } }), 'its bins\' width -5 is not a positive number');
    assert.equal(refusal({ bins: { count: 2.5 } }), 'its bins\' count 2.5 is not a whole number from 1');
    assert.equal(refusal({ bins: { count: 0 } }), 'its bins\' count 0 is not a whole number from 1');
    assert.equal(refusal({ bins: { width: 10, count: 'x' } }), 'its bins\' count "x" is not a whole number from 1');
    assert.equal(refusal({ bins: { width: 10, step: 1 } }), 'there is no "step" in its bins (it says width, count)');
  });

  it('an option\'s word a newer DataCube might write is refused by name, never drawn as another', () => {
    assert.equal(optionRefusal({ orientation: 'diagonal' }), 'its orientation "diagonal" is not one of vertical, horizontal');
    assert.equal(optionRefusal({ legend: 'left' }), 'its legend "left" is not one of top, bottom, right, none');
    assert.equal(optionRefusal({ labelPosition: 'center' }), 'its labelPosition "center" is not one of inside, outside');
    assert.equal(optionRefusal({ better: 'more' }), 'its better "more" is not one of higher, lower');
    assert.equal(optionRefusal({ half: 'yes' }), 'its half "yes" is not true or false');
    assert.equal(optionRefusal({ ring: 1 }), 'its ring 1 is not true or false');
  });

  it('how many it draws is a whole number from 1; a target a number; its order by x or y, up or down', () => {
    assert.equal(optionRefusal({ limit: 0 }), 'its limit 0 is not a whole number from 1');
    assert.equal(optionRefusal({ limit: 2.5 }), 'its limit 2.5 is not a whole number from 1');
    assert.equal(optionRefusal({ limit: '50' }), 'its limit "50" is not a whole number from 1');
    assert.equal(optionRefusal({ target: '100' }), 'its target "100" is not a number');
    assert.equal(optionRefusal({ sort: { by: 'z', direction: 'asc' } }),
      'its order {"by":"z","direction":"asc"} is not by x or y, asc or desc');
    assert.equal(optionRefusal({ sort: { by: 'y', direction: 'down' } }),
      'its order {"by":"y","direction":"down"} is not by x or y, asc or desc');
    assert.equal(optionRefusal({ sort: 'desc' }), 'its order "desc" is not by x or y, asc or desc');
    assert.equal(optionRefusal({ sort: { by: 'y', direction: 'desc', nulls: 'last' } }),
      'there is no "nulls" in its order (it says by, direction)');
  });

  it('small multiples name a row or a column, and nothing else', () => {
    assert.equal(refusal({ facet: {} }), 'its facet names neither a row nor a column');
    assert.equal(refusal({ facet: 'desk' }), 'its facet names neither a row nor a column');
    assert.equal(refusal({ facet: { row: 'desk', wrap: 3 } }), 'there is no "wrap" in its facet (it says row, column)');
  });

  it('a layer of a kind or statistic it does not know, a key another kind has, a line with no value, a band with no range', () => {
    assert.equal(refusal({ layers: { kind: 'line', value: 1 } }), 'its layers are not a list');
    assert.equal(refusal({ layers: [{ kind: 'arrow' }] }), 'layer 1 is of a kind this DataCube does not know ("arrow")');
    assert.equal(refusal({ layers: [5] }), 'layer 1 is of a kind this DataCube does not know (5)');
    assert.equal(refusal({ layers: [{}] }), 'layer 1 is of a kind this DataCube does not know (null)');
    assert.equal(refusal({ layers: [{ kind: 'line', value: 1 }, { kind: 'stat', stat: 'median' }] }),
      'layer 2\'s statistic "median" is not one of average, min, max');
    assert.equal(refusal({ layers: [{ kind: 'stat' }] }), 'layer 1\'s statistic null is not one of average, min, max');
    assert.equal(refusal({ layers: [{ kind: 'line', value: 1, colour: 'red' }] }),
      'there is no "colour" in layer 1 (a line) (it says kind, value, measure, label)');
    // a band reads no value: a key of another kind of layer has no place on it
    assert.equal(refusal({ layers: [{ kind: 'band', axis: 'y', from: 1, to: 2, measure: 0 }] }),
      'there is no "measure" in layer 1 (a band) (it says kind, axis, from, to, label)');
    assert.equal(refusal({ layers: [{ kind: 'line' }] }), 'layer 1 has no value');
    assert.equal(refusal({ layers: [{ kind: 'threshold', value: '5' }] }), 'layer 1 has no value');
    assert.equal(refusal({ layers: [{ kind: 'band', axis: 'y', from: 1 }] }), 'layer 1 (a band) needs an axis, a from and a to');
    assert.equal(refusal({ layers: [{ kind: 'band', axis: 'z', from: 1, to: 2 }] }), 'layer 1 (a band) needs an axis, a from and a to');
  });

  it('a calculated column of the chart\'s own has a stage, row or group, and a named column', () => {
    assert.equal(refusal({ calcs: { stage: 'row' } }), 'its calculated columns are not a list');
    for (const c of [{ stage: 'post', column: { name: 'c' } }, { stage: 'row', column: {} }, { stage: 'row' }, 'c']) {
      assert.equal(refusal({ calcs: [c] }), 'calculated column 1 has no stage (row or group) and column', JSON.stringify(c));
    }
    assert.equal(refusal({ calcs: [{ stage: 'group', column: { name: 'c' }, note: 'x' }] }),
      'there is no "note" in calculated column 1 (it says stage, column)');
  });

  it('a drill path runs down its levels, never past the last', () => {
    // a path as long as its levels would show a level below the last
    assert.equal(refusal({ drill: { levels: ['region'], path: ['EMEA'] } }), 'its drill path is not a path down its levels');
    assert.equal(refusal({ drill: 'region' }), 'its drill path is not a path down its levels');
    assert.equal(refusal({ drill: { levels: 'region', path: [] } }), 'its drill path is not a path down its levels');
    assert.equal(refusal({ drill: { levels: ['region', 'desk'], path: [], at: 0 } }),
      'there is no "at" in its drill (it says levels, path)');
  });

  it('a chart it can read comes back as saved, every option it leaves out its default', () => {
    const full = {
      version: 2, mark: 'bar', x: 'trade_date', grain: 'month', split: 'desk',
      y: [{ ...SUM_NOTIONAL, mark: 'bar', axis: 'left', showAs: { kind: 'runningTotal' } }, { ...SUM_QTY, mark: 'line', axis: 'right' }],
      label: SUM_QTY, tooltip: [{ column: 'qty', fn: 'max' }], facet: { row: 'region' },
      layers: [{ kind: 'stat', stat: 'average', measure: 1, label: 'mean' }, { kind: 'band', axis: 'x', from: '2024-01-01', to: '2024-03-01' }],
      calcs: [{ stage: 'row', column: { name: 'big', lambda: lambda(['x'], property(variable('x'), 'notional')) } }],
      drill: { levels: ['region', 'desk'], path: [] },
      frozen: true,
      options: { legend: 'none', stack: 'stacked', zoom: true, target: 10, better: 'lower' },
    };
    const read = readChart(full);
    assert.notEqual(typeof read, 'string', String(read));
    assert.deepEqual(read, { ...full, options: { ...DEFAULT_OPTIONS, ...full.options } });
    assert.deepEqual((readChart(SAVED) as ChartSpec).options, { ...DEFAULT_OPTIONS, legend: 'top' });
  });
});

describe('a change by hand (editChart): a new column across, a kind, options, and what freezes a following chart', () => {
  const BINNED = of('bar', { x: 'notional', bins: { width: 1_000_000 }, y: [SUM_QTY] });

  it('bins go with the column they were chosen for; bins told with the new column are its own', () => {
    const moved = editChart(BINNED, { x: 'qty' }, CUBE);
    assert.equal(moved.x, 'qty');
    assert.equal(moved.bins, undefined, 'the old column\'s bins went with it');
    assert.deepEqual(editChart(BINNED, { x: 'qty', bins: { count: 10 } }, CUBE).bins, { count: 10 });
    // told the same column across, nothing moved: its bins stay
    assert.deepEqual(editChart(BINNED, { x: 'notional' }, CUBE).bins, BINNED.bins);
    // bins told away, the column stays
    const plain = editChart(BINNED, { bins: null }, CUBE);
    assert.equal(plain.bins, undefined);
    assert.equal(plain.x, 'notional');
  });

  it('a date told across with its grain runs in time, long; a period of another column says it is none', () => {
    const monthly = editChart(BAR, { x: 'trade_date', grain: 'month' }, CUBE);
    assert.equal(monthly.grain, 'month');
    assert.deepEqual(monthly.options.sort, { by: 'x', direction: 'asc' });
    assert.equal(monthly.options.limit, TIME_LIMIT);
    assert.deepEqual(chartProblems(monthly, CUBE), []);
    // the grain moved to a text column is judged, never quietly dropped
    assert.deepEqual(chartProblems(editChart(BY_MONTH, { x: 'region', grain: 'month' }, CUBE), CUBE),
      ['\'region\' is not a date: it has no month.']);
  });

  it('the column across taken away takes its grain with it, and the chart asks for one -- none is made up', () => {
    const none = editChart(BY_MONTH, { x: null }, CUBE);
    assert.equal(none.x, undefined);
    assert.equal(none.grain, undefined);
    assert.equal(none.options.limit, DEFAULT_CHART_LIMIT, 'a time series\' default limit went with the date');
    assert.deepEqual(chartProblems(none, CUBE), ['Pick a column across.']);
  });

  it('a kind takes what it needs: one series drops the split, a gauge the column across too, a Pareto runs from the largest', () => {
    const split = of('bar', { split: 'desk', options: { ...OPTIONS, sort: { by: 'x', direction: 'asc' } } });
    const funnel = editChart(split, { mark: 'funnel' }, CUBE);
    assert.equal(funnel.split, undefined);
    assert.equal(funnel.x, 'region');
    const gauge = editChart(split, { mark: 'gauge' }, CUBE);
    assert.equal(gauge.x, undefined);
    assert.equal(gauge.split, undefined);
    assert.deepEqual(editChart(split, { mark: 'pareto' }, CUBE).options.sort, { by: 'y', direction: 'desc' });
    // a scatter matrix compares its values: nothing across
    assert.equal(editChart(split, { mark: 'matrix' }, CUBE).x, undefined);
    // the grid as shown keeps the grouping it does not draw, for a change back
    const treemap = editChart(split, { mark: 'treemap' }, CUBE);
    assert.equal(treemap.x, 'region');
    assert.equal(treemap.split, 'desk');
    assert.deepEqual(editChart(treemap, { mark: 'bar' }, CUBE), split, 'back to a bar, as it was');
    // the same kind told changes nothing
    assert.deepEqual(editChart(split, { mark: 'bar' }, CUBE), split);
  });

  it('every other slot stays for the new kind\'s rules to judge -- refused by name, never dropped without a word', () => {
    const dressed = of('bar', { layers: [{ kind: 'line', value: 5 }], label: SUM_QTY });
    const pie = editChart(dressed, { mark: 'pie' }, CUBE);
    assert.deepEqual(pie.layers, dressed.layers);
    assert.deepEqual(chartProblems(pie, CUBE),
      ['A pie draws no reference lines yet.', 'A pie writes no other measure on its marks.']);
    const scatter = editChart(dressed, { mark: 'scatter', x: 'qty' }, CUBE);
    assert.ok(chartProblems(scatter, CUBE).includes('A scatter writes no other measure on its marks.'));
  });

  it('options told are laid over the chart\'s; one told as null is taken away; options that are no object are kept for the reader to refuse', () => {
    const zoomed = editChart(BAR, { options: { zoom: true, legend: 'none' } }, CUBE);
    assert.deepEqual(zoomed.options, { ...OPTIONS, zoom: true, legend: 'none' });
    assert.deepEqual(editChart(zoomed, { options: { zoom: null } }, CUBE).options, { ...OPTIONS, legend: 'none' });
    const untyped = editChart(BAR, { options: 'wide' } as never, CUBE);
    assert.equal(readChart(untyped), 'its options "wide" are not an object');
  });

  it('a hand on what the grid sets freezes a following chart: its column across, split or values; not its options or layers', () => {
    assert.equal(editChart(BAR, { y: [SUM_QTY] }, CUBE).frozen, true);
    assert.equal(editChart(BAR, { split: 'desk' }, CUBE).frozen, true);
    assert.equal(editChart(BAR, { x: 'desk' }, CUBE).frozen, true);
    for (const change of [{ options: { labels: true } }, { layers: [{ kind: 'line' as const, value: 1 }] }, { mark: 'line' as const }]) {
      assert.equal(editChart(BAR, change, CUBE).frozen, undefined, JSON.stringify(change));
    }
  });

  it('a kind of one series has no split to follow; a kind of several values keeps its own; a KPI and a gauge follow their value alone', () => {
    // a funnel told a split: nothing the grid sets, so it stays live -- and the split is refused by name
    const funnel = editChart(of('funnel'), { split: 'desk' }, CUBE);
    assert.equal(funnel.frozen, undefined);
    assert.deepEqual(chartProblems(funnel, CUBE), ['A funnel draws one series: it has no split.']);
    assert.equal(editChart(of('funnel'), { x: 'desk' }, CUBE).frozen, true);
    const dumbbell = of('dumbbell', { y: [{ column: 'notional', fn: 'min' }, { column: 'notional', fn: 'max' }] });
    assert.equal(editChart(dumbbell, { y: [{ column: 'qty', fn: 'min' }, { column: 'qty', fn: 'max' }] }, CUBE).frozen, undefined);
    assert.equal(editChart(dumbbell, { x: 'desk' }, CUBE).frozen, true);
    // a KPI's column across is its trend, which the grid does not set
    assert.equal(editChart(of('kpi'), { x: 'trade_date' }, CUBE).frozen, undefined);
    assert.equal(editChart(of('kpi'), { y: [SUM_QTY] }, CUBE).frozen, true);
    const { x: _x, ...gauge } = of('gauge', { options: { ...OPTIONS, target: 10 } });
    assert.equal(editChart(gauge, { y: [SUM_QTY] }, CUBE).frozen, true);
  });

  it('frozen told is kept as told; a chart that follows nothing is never frozen by a hand', () => {
    assert.equal(editChart(BAR, { y: [SUM_QTY], frozen: null }, CUBE).frozen, undefined, 'told to follow, it follows');
    assert.equal(editChart(BAR, { frozen: true }, CUBE).frozen, true);
    // a scatter plots rows, a treemap draws the grid as shown: neither takes the grid's grouping, so neither freezes
    assert.equal(editChart(of('scatter', { x: 'qty' }), { x: 'notional', y: [SUM_QTY] }, CUBE).frozen, undefined);
    assert.equal(editChart(of('treemap'), { x: 'desk' }, CUBE).frozen, undefined);
  });
});

describe('a chart Python tells DataCube to make (chartFrom): the panel\'s rules, then the page\'s reader', () => {
  it('its own new chart changed by hand: a date told across with its grain runs in time, and the hand froze it', () => {
    const made = chartFrom({ mark: 'line', x: 'trade_date', grain: 'month' }, CUBE);
    assert.notEqual(typeof made, 'string', String(made));
    const line = made as ChartSpec;
    assert.equal(line.grain, 'month');
    assert.equal(line.split, 'desk', 'the new chart\'s split, from the grid\'s second row group');
    assert.deepEqual(line.options.sort, { by: 'x', direction: 'asc' });
    assert.equal(line.frozen, true);
    // a chart it had, changed: what was not told stays
    assert.equal((chartFrom({ options: { legend: 'none' } }, CUBE, BY_MONTH) as ChartSpec).grain, 'month');
  });

  it('what the reader refuses is said in DataCube\'s words; a cube with nothing to put across has no chart', () => {
    assert.equal(chartFrom({ layers: [{ kind: 'stat', stat: 'median' }] }, CUBE),
      'DataCube cannot read this chart: layer 1\'s statistic "median" is not one of average, min, max.');
    assert.equal(chartFrom({ mark: 'kpi' }, { ...CUBE, columns: [{ name: 'notional', type: 'Float' }], rows: [] }),
      'This cube has no column to chart.');
  });
});

describe('a following chart regrouped as its grid is (followCube)', () => {
  it('a drilled chart stays where it was taken, whatever the grid does, until it is taken back up', () => {
    const drilled = of('bar', { drill: { levels: ['region', 'desk'], path: ['EMEA'] } });
    assert.equal(followCube(drilled, { ...AVERAGED, rows: ['desk', 'region'] }), drilled);
    // back at the top it follows again: the grid's first row group across, its Aggregation of the value
    const top = followCube(drillUp(drilled, 0), { ...AVERAGED, rows: ['desk', 'region'] });
    assert.equal(top.x, 'desk');
    assert.equal(top.split, 'region');
    assert.deepEqual(top.y, [{ column: 'notional', fn: 'average' }]);
  });

  it('a gauge follows the grid\'s Aggregation of its value, and takes nothing across nor a split from the grid\'s grouping', () => {
    const { x: _x, ...gauge } = of('gauge', { options: { ...OPTIONS, target: 1_000 } });
    const followed = followCube(gauge, AVERAGED);
    assert.deepEqual(followed.y, [{ column: 'notional', fn: 'average' }]);
    assert.equal(followed.x, undefined);
    assert.equal(followed.split, undefined);
    assert.deepEqual(chartProblems(followed, AVERAGED), []);
    // nothing to follow: the very same chart (the form is not painted again for nothing)
    assert.equal(followCube(gauge, CUBE), gauge);
  });

  it('a KPI keeps its trend\'s column though the grid groups by others', () => {
    const kpi = of('kpi', { x: 'trade_date' });
    const followed = followCube(kpi, { ...AVERAGED, rows: ['desk'] });
    assert.equal(followed.x, 'trade_date');
    assert.deepEqual(followed.y, [{ column: 'notional', fn: 'average' }]);
  });

  it('a kind of several values keeps them all though the grid aggregates their column otherwise; a kind of one series takes no split', () => {
    const spokes: ChartMeasure[] = [SUM_NOTIONAL, SUM_QTY, { column: 'notional', fn: 'max' }];
    const radar = followCube(of('radar', { x: 'desk', y: spokes }), AVERAGED);
    assert.deepEqual(radar.y, spokes, 'a radar\'s spokes are its own');
    assert.equal(radar.x, 'region', 'its grouping is the grid\'s');
    assert.equal(radar.split, undefined);
    assert.deepEqual(chartProblems(radar, AVERAGED), []);
    const lollipop = followCube(of('lollipop', { x: 'desk' }), AVERAGED);
    assert.equal(lollipop.x, 'region');
    assert.equal(lollipop.split, undefined, 'the grid\'s second row group is no split of a lollipop\'s');
    assert.deepEqual(lollipop.y, [{ column: 'notional', fn: 'average' }]);
  });

  it('a chart whose value\'s column the grid no longer has keeps its value, and says the column is gone -- never re-pointed', () => {
    const without: CubeSnapshot = { ...CUBE, columns: CUBE.columns.filter((c) => c.name !== 'notional') };
    const followed = followCube(BAR, without);
    assert.deepEqual(followed.y, BAR.y);
    assert.deepEqual(chartProblems(followed, without), ['The cube has no column \'notional\'.']);
  });

  it('a value of a dimension, a group-level column plotted as it is, and a value no column carries keep the chart\'s own', () => {
    const counted = of('bar', { y: [{ column: 'desk', fn: 'count' }] });
    assert.deepEqual(followCube(counted, AVERAGED).y, counted.y);
    const margin = of('bar', { y: [{ column: 'margin', fn: 'value' }] });
    assert.deepEqual(followCube(margin, AVERAGED).y, margin.y);
    // Update's reading: a value no column of the editing grid carries is left as it is
    assert.deepEqual(followCube(BAR, AVERAGED, () => undefined).y, BAR.y);
  });

  it('a grid that only pivots gives its first column label across and its second as the split', () => {
    const pivoted = followCube(BAR, { ...CUBE, rows: [], pivotOn: ['desk', 'region'] });
    assert.equal(pivoted.x, 'desk');
    assert.equal(pivoted.split, 'region');
    const both = followCube(BAR, { ...CUBE, rows: ['region'], pivotOn: ['desk'] });
    assert.equal(both.x, 'region');
    assert.equal(both.split, 'desk', 'one row group: the first column label splits');
  });
});

describe('what a following chart was told that its grid replaces is refused, in words (followRefusal)', () => {
  const followed = of('bar', { split: 'desk', y: [{ column: 'notional', fn: 'average' }] });

  it('a column across, a split, values: each said as the grid has it and as it was told', () => {
    assert.equal(followRefusal(['x'], of('bar', { x: 'desk' }), followed),
      'A chart that follows its grid takes \'region\' across, not \'desk\': leave x out, or let the chart keep its own (frozen).');
    assert.equal(followRefusal(['split'], BAR, followed),
      'A chart that follows its grid takes \'desk\' as its split, not none: leave split out, or let the chart keep its own (frozen).');
    assert.equal(followRefusal(['split'], of('bar', { split: 'desk' }), BAR),
      'A chart that follows its grid takes none as its split, not \'desk\': leave split out, or let the chart keep its own (frozen).');
    assert.equal(followRefusal(['y'], of('bar', { split: 'desk', y: [SUM_NOTIONAL, { column: 'qty', fn: 'max' }] }), followed),
      'A chart that follows its grid takes average of \'notional\' as its values, not sum of \'notional\', max of \'qty\': '
      + 'leave y out, or let the chart keep its own (frozen).');
  });

  it('told what the grid gives -- its keys in any order -- or not told it at all, nothing is refused', () => {
    const told = of('bar', { split: 'desk', y: [{ fn: 'average', column: 'notional' } as ChartMeasure] });
    assert.equal(followRefusal(['x', 'split', 'y'], told, followed), null);
    assert.equal(followRefusal(['options'], of('bar', { x: 'desk' }), followed), null, 'the column across was not told');
  });
});

describe('what stops a chart from being drawn (chartProblems), each in words', () => {
  const problems = (spec: ChartSpec, s: CubeSnapshot = CUBE): string[] => chartProblems(spec, s);
  const { x: _x, ...ACROSS_NOTHING } = BAR;

  it('a chart of categories needs a column across; a KPI, a gauge, a scatter matrix and the grid as shown do not', () => {
    for (const mark of ['bar', 'line', 'pie', 'heatmap'] as const) {
      assert.ok(problems({ ...ACROSS_NOTHING, mark, split: 'desk' }).includes('Pick a column across.'), mark);
    }
    assert.deepEqual(problems({ ...ACROSS_NOTHING, mark: 'kpi' }), []);
    assert.deepEqual(problems({ ...ACROSS_NOTHING, mark: 'gauge', options: { ...OPTIONS, target: 1 } }), []);
    assert.deepEqual(problems({ ...ACROSS_NOTHING, mark: 'matrix', y: [SUM_NOTIONAL, SUM_QTY] }), []);
    assert.deepEqual(problems({ ...ACROSS_NOTHING, mark: 'treemap', y: [] }), [], 'the grid as shown plots nothing of its own');
  });

  it('a chart with nothing to plot asks for something', () => {
    assert.deepEqual(problems(of('bar', { y: [] })), ['Pick something to plot.']);
  });

  it('a hover\'s value, a size or a label of a column the cube has not; a weighted average, which needs a weight', () => {
    assert.deepEqual(problems(of('bar', { tooltip: [{ column: 'pnl', fn: 'sum' }] })), ['The cube has no column \'pnl\'.']);
    assert.deepEqual(problems(of('bar', { label: { column: 'pnl', fn: 'max' } })), ['The cube has no column \'pnl\'.']);
    assert.ok(problems(of('scatter', { x: 'qty', size: { column: 'pnl', fn: 'sum' } })).includes('The cube has no column \'pnl\'.'));
    // a count reads no column: it counts the group's rows
    assert.deepEqual(problems(of('bar', { y: [{ column: 'region', fn: 'count' }] })), []);
    assert.deepEqual(problems(of('bar', { y: [{ column: 'notional', fn: 'wavg' }] })),
      ['A chart plots no weighted average: \'notional\' would need its weight column.']);
    // so every aggregate of two; a distinct count of its own column plots
    assert.deepEqual(problems(of('bar', { y: [{ column: 'notional', fn: 'corr' }] })),
      ['A chart plots no correlation: \'notional\' would need its paired column.']);
    assert.deepEqual(problems(of('bar', { y: [{ column: 'notional', fn: 'distinctCount' }] })), []);
  });

  it('a split the cube has not, a split that is the column across, a drill level the cube has not', () => {
    assert.deepEqual(problems(of('bar', { split: 'book' })), ['The cube has no column \'book\'.']);
    assert.deepEqual(problems(of('bar', { split: 'region' })), ['Split by a different column than the one across.']);
    // the grid as shown judges neither
    assert.deepEqual(problems(of('treemap', { split: 'region' })), []);
    const lost = of('bar', { drill: { levels: ['region', 'book'], path: ['EMEA'] } });
    assert.deepEqual(problems(lost), ['The cube has no column \'book\'.', 'The cube has no column \'book\' to drill into.']);
    // a drilled chart is judged as it draws: the level after its path across
    assert.deepEqual(problems(of('bar', { drill: { levels: ['region', 'desk'], path: ['EMEA'] } })), []);
  });

  it('a period of what is no date (a time of day has no month), bins of what is no number, and never both', () => {
    assert.deepEqual(problems(of('line', { x: 'cutoff', grain: 'month' })), ['\'cutoff\' is not a date: it has no month.']);
    assert.deepEqual(problems(of('line', { x: 'booked_at', grain: 'week' })), [], 'an instant has its week');
    assert.deepEqual(problems(of('bar', { x: 'trade_date', bins: { width: 7 } })), ['\'trade_date\' is not a number: it has no bins.']);
    assert.deepEqual(problems(of('bar', { x: 'trade_date', grain: 'month', bins: { width: 7 } })),
      ['\'trade_date\' is not a number: it has no bins.', 'A column across has a grain or bins, not both.']);
  });

  it('the grid as shown has no grain and no bins of its own', () => {
    assert.deepEqual(problems(of('treemap', { x: 'trade_date', grain: 'month' })), ['A treemap draws the grid as shown: it has no grain.']);
    assert.deepEqual(problems(of('sunburst', { x: 'notional', bins: { count: 5 } })), ['A sunburst draws the grid as shown: it has no bins.']);
  });

  it('a kind that runs along dates refuses a column that is none -- a time of day among them', () => {
    assert.deepEqual(problems(of('calendar', { x: 'cutoff' })), ['A calendar runs along dates: \'cutoff\' is not one.']);
    assert.deepEqual(problems(of('stream', { split: 'desk' })), ['A streamgraph runs along dates: \'region\' is not one.']);
    assert.deepEqual(problems(of('stream', { x: 'trade_date', split: 'desk' })), []);
  });

  it('a kind of one value refuses a second; a kind that computes its own figures refuses a value shown as another', () => {
    assert.deepEqual(problems(of('waterfall', { y: [SUM_NOTIONAL, SUM_QTY] })), ['A waterfall plots one value.']);
    assert.deepEqual(problems(of('rose', { y: [SUM_NOTIONAL, SUM_QTY] })), ['A rose plots one value.']);
    assert.deepEqual(problems(of('pareto', { y: [{ ...SUM_NOTIONAL, showAs: { kind: 'rank' } }] })),
      ['A Pareto computes its own figures: a value shown as something else has no place on it.']);
  });

  it('small multiples: of bars, lines and areas, by another column the cube has, one slider or brush or colour scale not yet', () => {
    assert.deepEqual(problems(of('bar', { facet: { row: 'book' } })), ['The cube has no column \'book\'.']);
    assert.deepEqual(problems(of('bar', { facet: { row: 'region' } })), ['\'region\' is across or split already: a facet is another column.']);
    assert.deepEqual(problems(of('bar', { split: 'desk', facet: { column: 'desk' } })),
      ['\'desk\' is across or split already: a facet is another column.']);
    assert.deepEqual(problems(of('bar', { x: 'trade_date', facet: { row: 'desk', column: 'desk' } })),
      ['A facet\'s rows and columns are two columns.']);
    assert.deepEqual(problems(of('line', { facet: { row: 'desk' }, options: { ...OPTIONS, zoom: true } })),
      ['Small multiples take no slider: zoom one chart.']);
    assert.deepEqual(problems(of('area', { facet: { row: 'desk' }, layers: [{ kind: 'threshold', value: 3 }] })),
      ['Small multiples are not coloured by value yet.']);
  });

  it('a size, a label, a hover\'s values and a reference line only where they are drawn', () => {
    assert.deepEqual(problems(of('bar', { size: SUM_QTY })), ['Only a scatter sizes its marks (a bubble).']);
    assert.deepEqual(problems(of('heatmap', { split: 'desk', tooltip: [SUM_QTY] })), ['A heatmap\'s hover lists no further measures.']);
    assert.deepEqual(problems(of('scatter', { x: 'qty', tooltip: [SUM_QTY], size: SUM_QTY })), [], 'a scatter lists and sizes its rows');
    // a band reads no value; a line reads one the chart plots
    assert.deepEqual(problems(of('bar', { layers: [{ kind: 'band', axis: 'y', from: 1, to: 2 }] })), []);
    assert.match(problems(of('bar', { layers: [{ kind: 'extremes', measure: 1 }] })).join(' '),
      /^An? extremes layer reads value 2, which the chart does not plot\.$/);
  });

  it('a chart of rows: no value shown as a running total; a scatter matrix of two to five values and nothing across', () => {
    assert.deepEqual(problems(of('scatter', { x: 'qty', y: [{ ...SUM_NOTIONAL, showAs: { kind: 'runningTotal' } }] })),
      ['A scatter plots rows: a value shown as a running total, a share or a rank needs groups.']);
    const { x: _mx, ...matrix } = of('matrix', { y: [SUM_NOTIONAL] });
    assert.deepEqual(problems(matrix), ['A scatter matrix plots each pair of two to 5 values.']);
    const six = Array.from({ length: 6 }, (_, i): ChartMeasure => ({ column: i % 2 === 0 ? 'notional' : 'qty', fn: 'sum' }));
    assert.deepEqual(problems({ ...matrix, y: six }), ['A scatter matrix plots each pair of two to 5 values.']);
    assert.deepEqual(problems({ ...matrix, x: 'region', y: [SUM_NOTIONAL, SUM_QTY] }),
      ['A scatter matrix has no column across: it plots its values against each other.']);
  });
});

describe('Open in Grid: a chart\'s values as the grid\'s Column Properties (chartValuesAsColumns)', () => {
  it('a value a calculated column carries is named for the value, apart from every name the grid has', () => {
    // the grid has a column called max_notional already: twice the notional, no copy of it
    const twice = { name: 'max_notional', lambda: lambda(['x'], fn('times', property(variable('x'), 'notional'), property(variable('x'), 'qty'))), type: 'Float' };
    const grid: CubeSnapshot = { ...CUBE, rows: ['region'], derived: [twice] };
    const max: ChartMeasure = { column: 'notional', fn: 'max' };
    const made = chartValuesAsColumns({ snapshot: grid, config: DEFAULT_CONFIGURATION }, [SUM_NOTIONAL, max]);
    assert.equal(made.carriedBy(SUM_NOTIONAL), 'notional', 'its own column, at its own Aggregation');
    assert.equal(made.carriedBy(max), 'max_notional_2');
    assert.deepEqual(made.draft.snapshot.derived.map((d) => d.name), ['max_notional', 'max_notional_2']);
    assert.deepEqual(made.notes, ['measure max_notional_2 is now a calculated column of notional, its Aggregation max']);
    // the next name along when that is taken too
    const busier = { ...grid, derived: [twice, { ...twice, name: 'max_notional_2' }] };
    assert.equal(chartValuesAsColumns({ snapshot: busier, config: DEFAULT_CONFIGURATION }, [SUM_NOTIONAL, max]).carriedBy(max), 'max_notional_3');
  });

  it('a count is named by its column when the grid has a column called count', () => {
    const grid: CubeSnapshot = { ...CUBE, columns: [...CUBE.columns, { name: 'count', type: 'Integer', kind: 'measure' }] };
    const count: ChartMeasure = { column: 'qty', fn: 'count' };
    const made = chartValuesAsColumns({ snapshot: grid, config: DEFAULT_CONFIGURATION }, [SUM_QTY, count]);
    assert.equal(made.carriedBy(count), 'count_qty');
  });

  it('a value the grid cannot carry is left out and said; no column carries it, so Update leaves it as it is', () => {
    const counted: ChartMeasure = { column: 'region', fn: 'count' };
    const made = chartValuesAsColumns({ snapshot: CUBE, config: DEFAULT_CONFIGURATION }, [SUM_NOTIONAL, counted]);
    assert.equal(made.carriedBy(counted), undefined);
    assert.deepEqual(made.notes, ['left out measure count: \'count\' is not offered on region, a String column: '
      + 'min, max, joinStrings, unique, maxBy, minBy (Show leaf count counts a group\'s rows)']);
    const following = of('bar', { y: [SUM_NOTIONAL, counted] });
    assert.deepEqual(followCube(following, CUBE, made.carriedBy).y[1], counted);
  });
});

describe('a brush across, by value, on a column with no period or bins', () => {
  it('a date across: from the earliest day taken to the latest, whatever order they were drawn in', () => {
    const daily = of('line', { x: 'trade_date' });
    assert.deepEqual(brushConditions(daily, CUBE, { kind: 'across', values: ['2024-03-01', null, '2024-01-15', '2024-02-10'] }), [
      { kind: 'condition', column: 'trade_date', operator: 'greaterThanEqual', value: '2024-01-15' },
      { kind: 'condition', column: 'trade_date', operator: 'lessThanEqual', value: '2024-03-01' },
    ]);
  });

  it('a number across: from the least to the greatest by value, never by its text (200 is above 30)', () => {
    const byQty = of('bar', { x: 'qty' });
    assert.deepEqual(brushConditions(byQty, CUBE, { kind: 'across', values: [30, 4, 200] }), [
      { kind: 'condition', column: 'qty', operator: 'greaterThanEqual', value: 4 },
      { kind: 'condition', column: 'qty', operator: 'lessThanEqual', value: 200 },
    ]);
    // a big integer is said as its digits
    assert.deepEqual(brushConditions(byQty, CUBE, { kind: 'across', values: [12n, 3n] }), [
      { kind: 'condition', column: 'qty', operator: 'greaterThanEqual', value: '3' },
      { kind: 'condition', column: 'qty', operator: 'lessThanEqual', value: '12' },
    ]);
  });

  it('nothing but blanks taken filters nothing more than the drill path', () => {
    assert.deepEqual(brushConditions(of('line', { x: 'trade_date' }), CUBE, { kind: 'across', values: [null] }), []);
    const drilled = of('bar', { drill: { levels: ['region', 'desk'], path: ['EMEA'] } });
    assert.deepEqual(brushConditions(drilled, CUBE, { kind: 'across', values: [null] }),
      [{ kind: 'condition', column: 'region', operator: 'equal', value: 'EMEA' }]);
  });
});

describe('drill-down through the grid\'s groupings: from where, how far, and into a blank group', () => {
  it('a blank group drilled into narrows the chart to the rows where that column is empty', () => {
    const down = drillDown(BAR, CUBE, null)!;
    assert.deepEqual(down.drill, { levels: ['region', 'desk'], path: [null] });
    const blank = { kind: 'condition', column: 'region', operator: 'isEmpty' } as const;
    assert.deepEqual(drilledView(down).conditions, [blank]);
    // a click on one of its marks narrows the grid to both; undrilled, a blank category clicked is the blank rows
    assert.deepEqual(markConditions(down, CUBE, { desk: 'FX' }), [blank, { kind: 'condition', column: 'desk', operator: 'equal', value: 'FX' }]);
    assert.deepEqual(markConditions(BAR, CUBE, { region: null }), [blank]);
  });

  it('a big integer category is drilled into, and clicked, as its digits', () => {
    const books: CubeSnapshot = {
      ...CUBE, columns: [...CUBE.columns, { name: 'book_id', type: 'Integer', kind: 'dimension' }], rows: ['book_id', 'desk'],
    };
    const byBook = of('bar', { x: 'book_id' });
    assert.deepEqual(drillDown(byBook, books, 9007199254740993n)?.drill?.path, ['9007199254740993']);
    assert.deepEqual(markConditions(byBook, books, { book_id: 9007199254740993n }),
      [{ kind: 'condition', column: 'book_id', operator: 'equal', value: '9007199254740993' }]);
  });

  it('there is nowhere down from a period or a bin, a column the grid does not group by, the split, a scatter or small multiples', () => {
    const dated: CubeSnapshot = {
      ...CUBE, columns: [...CUBE.columns, { name: 'year', type: 'Integer', kind: 'dimension' }], rows: ['trade_date', 'year', 'desk'],
    };
    assert.deepEqual(drillLevels(of('bar', { x: 'trade_date' }), dated), ['trade_date', 'year', 'desk']);
    assert.equal(drillLevels(of('bar', { x: 'trade_date', grain: 'month' }), dated), null, 'a month has no level below it');
    assert.equal(drillLevels(of('bar', { x: 'year', bins: { width: 5 } }), dated), null, 'nor a bin of years');
    assert.equal(drillLevels(of('bar', { x: 'booked_at' }), CUBE), null, 'the grid does not group by it');
    assert.equal(drillLevels(of('bar', { split: 'desk' }), CUBE), null, 'the split is no level: nothing is below region');
    assert.equal(drillLevels(of('scatter', { x: 'qty' }), CUBE), null);
    assert.equal(drillLevels(of('bar', { facet: { row: 'trade_date' } }), CUBE), null);
  });

  it('why a chart does not drill, in words (drillRefusal: what Python\'s chart.drill is told), null where it does', () => {
    assert.equal(drillRefusal(BAR, CUBE), null);
    assert.equal(drillRefusal(of('bar', { x: 'trade_date', grain: 'month' }), CUBE),
      "trade_date across is in months, not a grouping of the grid's: there is nothing below one");
    assert.equal(drillRefusal(of('bar', { x: 'booked_at' }), CUBE),
      "the grid does not group by booked_at, the chart's column across: it has no level below it");
    assert.equal(drillRefusal(of('bar', { split: 'desk' }), CUBE), "nothing is below region in the grid's groupings (region, desk)");
    assert.equal(drillRefusal(of('scatter', { x: 'qty' }), CUBE), 'a scatter chart does not drill: its marks are not its categories across');
    assert.equal(drillRefusal(of('bar', { facet: { row: 'trade_date' } }), CUBE), 'a chart of small multiples does not drill');
    const { x: _x, ...across } = BAR;
    assert.equal(drillRefusal(across, CUBE), 'the chart has nothing across to drill into');
    // drilled, it drills on through its own levels
    assert.equal(drillRefusal(drillDown(BAR, CUBE, 'EMEA')!, CUBE), null);
  });

  it('taken back up part of the way, the chart shows the level after what is left of its path', () => {
    const deep = of('bar', { drill: { levels: ['region', 'desk', 'trade_date'], path: ['EMEA', 'FX'] } });
    assert.equal(drilledView(deep).spec.x, 'trade_date');
    const up = drillUp(deep, 1);
    assert.deepEqual(up.drill?.path, ['EMEA']);
    assert.equal(drilledView(up).spec.x, 'desk');
    assert.equal(drillUp(BAR, 0), BAR, 'an undrilled chart is left as it is');
  });
});

describe('a mark named by its values, as a click names it (namedMark: Python\'s chart.select, D8)', () => {
  it('its value across, and its series\' on a split chart; a drilled chart\'s at the level it shows', () => {
    assert.deepEqual(namedMark(BAR, 'EMEA'), { region: 'EMEA' });
    assert.deepEqual(namedMark(of('bar', { split: 'desk' }), 'EMEA', { value: 'FX' }), { region: 'EMEA', desk: 'FX' });
    assert.deepEqual(namedMark(drillDown(BAR, CUBE, 'EMEA')!, 'FX'), { desk: 'FX' });
    // the same key a click gives markConditions: a drilled chart's path with it
    assert.deepEqual(markConditions(drillDown(BAR, CUBE, 'EMEA')!, CUBE, namedMark(drillDown(BAR, CUBE, 'EMEA')!, 'FX') as never), [
      { kind: 'condition', column: 'region', operator: 'equal', value: 'EMEA' },
      { kind: 'condition', column: 'desk', operator: 'equal', value: 'FX' },
    ]);
  });

  it('refused, in words: a split missing or not wanted, marks no value across names, a range across', () => {
    assert.equal(namedMark(of('bar', { split: 'desk' }), 'EMEA'), "the chart is split by desk: name the mark's desk too");
    assert.equal(namedMark(BAR, 'EMEA', { value: 'FX' }), 'the chart is split by nothing: a mark of it is named by its value across alone');
    for (const mark of ['scatter', 'treemap', 'sunburst', 'stream', 'kpi', 'gauge'] as const) {
      assert.equal(namedMark(of(mark), 'EMEA'), `a ${mark} chart's marks are not named by a value across`);
    }
    assert.equal(namedMark(BY_MONTH, '2024-01-01'),
      "the chart's marks across are months of trade_date, each a range the database ends: a range is not selected by its start");
    const { x: _x, ...across } = BAR;
    assert.equal(namedMark(across, 'EMEA'), 'the chart has nothing across: no mark of it is named by a value');
  });
});

describe('a clicked period or bin is the database\'s range of the cube\'s column (markConditions)', () => {
  const BINNED = of('bar', { x: 'notional', bins: { width: 1_000_000 }, y: [SUM_QTY] });

  it('a bin from its edge to the next bin\'s; a month from its first day to the next month\'s', () => {
    assert.deepEqual(markConditions(BINNED, CUBE, { notional: 2_000_000 }, 3_000_000), [
      { kind: 'condition', column: 'notional', operator: 'greaterThanEqual', value: 2_000_000 },
      { kind: 'condition', column: 'notional', operator: 'lessThan', value: 3_000_000 },
    ]);
    assert.deepEqual(markConditions(BY_MONTH, CUBE, { trade_date: '2024-01-01' }, '2024-02-01'), [
      { kind: 'condition', column: 'trade_date', operator: 'greaterThanEqual', value: '2024-01-01' },
      { kind: 'condition', column: 'trade_date', operator: 'lessThan', value: '2024-02-01' },
    ]);
  });

  it('a period or a bin whose end the database did not answer is never guessed: it is said, by name', () => {
    assert.throws(() => markConditions(BINNED, CUBE, { notional: 2_000_000 }), /^Error: the end of notional's bin was not answered$/);
    assert.throws(() => markConditions(BY_MONTH, CUBE, { trade_date: '2024-01-01' }, null),
      /^Error: the end of trade_date's month was not answered$/);
  });
});

describe('the chart\'s query: what it groups by, what it asks the database, how many rows', () => {
  it('a chart of bins groups by the bin, and asks each bin\'s end of the database', () => {
    const s = chartSnapshot(CUBE, of('bar', { x: 'notional', bins: { width: 1_000_000 }, y: [SUM_QTY] }));
    const bin = binName('notional');
    assert.deepEqual(s.rows, [bin]);
    assert.deepEqual(s.derived.map((d) => d.name), [bin, upperName(bin)]);
    assert.deepEqual(s.measures, [{ name: 'sum_qty', column: 'qty', fn: 'sum' }, { name: AXIS_UPPER, column: upperName(bin), fn: 'max' }]);
  });

  it('reference lines ask the database for each statistic once, however many layers draw it', () => {
    const layered = of('bar', { layers: [
      { kind: 'stat', stat: 'average' }, { kind: 'stat', stat: 'average', label: 'mean' }, { kind: 'extremes' }, { kind: 'stat', stat: 'max' },
    ] });
    assert.deepEqual(chartSnapshot(CUBE, layered).groupDerived?.map((d) => d.name),
      [statName('average', 'sum_notional'), statName('max', 'sum_notional'), statName('min', 'sum_notional')]);
  });

  it('a waterfall ordered by its category runs its total along the categories', () => {
    const byRegion = of('waterfall', { options: { ...OPTIONS, sort: { by: 'x', direction: 'asc' } } });
    const end = chartSnapshot(CUBE, byRegion).groupDerived?.find((d) => d.name === waterfallNames('sum_notional').end);
    assert.deepEqual(end?.window?.order, [{ column: 'region', direction: 'asc' }]);
  });

  it('a group-level column reads the grid\'s measures at their Aggregations, a weighted average with its weight', () => {
    const margin = { name: 'margin', type: 'Float', lambda: lambda(['x'], fn('divide', property(variable('x'), 'notional'), property(variable('x'), 'qty'))) };
    const weighted: CubeSnapshot = {
      ...CUBE,
      columns: CUBE.columns.map((c) => (c.name === 'notional' ? { ...c, aggregate: 'wavg' as const, aggregateWeight: 'qty' } : c)),
      groupDerived: [margin],
    };
    const s = chartSnapshot(weighted, of('bar', { y: [{ column: 'margin', fn: 'value' }] }));
    assert.deepEqual(s.measures, [{ name: 'notional', column: 'notional', fn: 'wavg', weight: 'qty' }, { name: 'qty', column: 'qty', fn: 'sum' }]);
    assert.deepEqual(s.columns.map((c) => c.name), ['region', 'notional', 'qty']);
    assert.deepEqual(s.groupDerived, [margin]);
  });

  it('the cube\'s filter keeps its columns typed in the chart\'s query: a negated one\'s, both of a comparison of two', () => {
    const negated: CubeSnapshot = {
      ...CUBE, filter: { kind: 'not', child: { kind: 'condition', column: 'desk', operator: 'equal', value: 'FX' } },
    };
    const s = chartSnapshot(negated, BAR);
    assert.deepEqual(s.columns.map((c) => c.name).sort(), ['desk', 'notional', 'region']);
    assert.deepEqual(s.filter, negated.filter);
    const compared: CubeSnapshot = {
      ...CUBE, filter: { kind: 'condition', column: 'trade_date', operator: 'lessThan', rightColumn: 'booked_at' },
    };
    assert.deepEqual(chartSnapshot(compared, BAR).columns.map((c) => c.name).sort(), ['booked_at', 'notional', 'region', 'trade_date']);
  });

  it('a bubble reads its size and its hover\'s columns with the rows, aggregating none', () => {
    const s = chartSnapshot(CUBE, of('scatter', { x: 'qty', size: SUM_NOTIONAL, tooltip: [{ column: 'trade_date', fn: 'max' }] }));
    assert.deepEqual(s.rows, []);
    assert.deepEqual(s.measures, []);
    assert.deepEqual(s.columns.map((c) => c.name).sort(), ['notional', 'qty', 'trade_date']);
  });

  it('small multiples ask for each panel\'s rows, the most panels drawn; a chart of rows for its own most', () => {
    assert.equal(chartQuery(CUBE, of('bar', { x: 'trade_date', facet: { row: 'desk' } })).scope.limit, OPTIONS.limit * MAX_FACETS);
    assert.equal(chartQuery(CUBE, of('scatter', { x: 'qty' })).scope.limit, SCATTER_LIMIT);
    assert.equal(chartQuery(CUBE, BAR).scope.limit, OPTIONS.limit);
  });

  it('a chart\'s own row-stage column is offered as its editor declared it: untyped, a dimension; a typed measure, a measure', () => {
    const calcs = [
      { stage: 'row' as const, column: { name: 'tier', lambda: lambda(['x'], property(variable('x'), 'desk')) } },
      { stage: 'row' as const, column: { name: 'big', type: 'Float', kind: 'measure' as const, lambda: lambda(['x'], property(variable('x'), 'notional')) } },
    ];
    const offered = chartColumns(CUBE, of('bar', { calcs }));
    const tier = offered.dimensions.find((c) => c.name === 'tier');
    assert.deepEqual(tier, { name: 'tier', kind: 'dimension', derived: true });
    assert.ok(offered.measures.some((c) => c.name === 'big' && c.kind === 'measure'));
    assert.ok(!offered.dimensions.some((c) => c.name === 'big'));
    // a child aggregate reads the column it aggregates
    assert.deepEqual(columnsRead({ name: 'c', childAggregate: { of: 'notional', fn: 'sum' } } as never), ['notional']);
  });
});

// BUGS FOUND writing these (2026-10-18): each test below fails against chart-spec.ts as it stands, and passes when the
// rule it names holds. Each is reachable by a person (the chart form), by Python (chartFrom), or by a saved page.
describe('rules the chart model does not keep yet: a slot nothing draws refused, a saved word typed, a click never thrown', () => {
  it('a monthly line made a gauge or a scatter matrix by hand drops its grain with its column across; told with none, refused', () => {
    // asMark drops the column across for a gauge and a matrix; its grain went on unjudged (every grain rule needs an x)
    // -- now it goes with the column, as a change of the column across takes it (acrossChanged's rule)
    const gauge = editChart(BY_MONTH, { mark: 'gauge', options: { target: 5 } }, CUBE);
    assert.equal(gauge.grain, undefined, JSON.stringify(gauge));
    const matrix = editChart(BY_MONTH, { mark: 'matrix', y: [SUM_NOTIONAL, SUM_QTY] }, CUBE);
    assert.equal(matrix.grain, undefined, JSON.stringify(matrix));
    // told a grain or bins with nothing across (Python's grid.chart(mark='gauge', grain='month', ...)): refused by name
    const told = chartFrom({ mark: 'gauge', grain: 'month', options: { target: 5 } }, CUBE);
    assert.ok(typeof told !== 'string', String(told));
    assert.ok(chartProblems(told, CUBE).includes('No column is across: it has no grain.'), JSON.stringify(chartProblems(told, CUBE)));
    const binned = chartFrom({ mark: 'kpi', x: null, bins: { width: 5 } }, CUBE);
    assert.ok(typeof binned !== 'string', String(binned));
    assert.ok(chartProblems(binned, CUBE).includes('No column is across: it has no bins.'), JSON.stringify(chartProblems(binned, CUBE)));
  });

  it('a layer reading a value the chart does not plot is refused -- one before its first value too', () => {
    // a stat layer over "value 0" (measure -1) is read, judged drawable, and its statistic is never asked for
    const below = of('bar', { layers: [{ kind: 'stat', stat: 'average', measure: -1 }] });
    const read = readChart(below);
    assert.ok(typeof read === 'string' || chartProblems(read, CUBE).length > 0,
      `drawn without its layer: ${JSON.stringify(chartSnapshot(CUBE, below).groupDerived ?? null)}`);
  });

  it('axis breaks that are not a list of ranges are refused in words, not thrown at the drawing', () => {
    // read as they are, they reach chart-option.ts, which throws "spec.options.breaks.map is not a function"
    const read = readChart({ ...SAVED, options: { breaks: 'x' } });
    assert.ok(typeof read === 'string' || chartProblems(read, CUBE).length > 0, JSON.stringify(read));
  });

  it('a flag of the options or the chart that is not true or false is refused, as half and ring are', () => {
    // frozen "false" was read and kept the chart frozen; labels "yes" was drawn as labels shown
    const refused = (saved: Record<string, unknown>): boolean => {
      const read = readChart(saved);
      return typeof read === 'string' || chartProblems(read, CUBE).length > 0;
    };
    const read = ['labels', 'smooth', 'step', 'logAxis', 'zoom', 'brush', 'decal', 'dim', 'toolbox']
      .filter((flag) => !refused({ ...SAVED, options: { [flag]: 'yes' } })).map((flag) => `options.${flag} "yes"`);
    if (!refused({ ...SAVED, frozen: 'false' })) read.push('frozen "false"');
    assert.deepEqual(read, [], 'read and drawn as if true');
  });

  it('a drilled chart given another column across by hand draws that column: the drill went down the old one', () => {
    // the form's Across: on a drilled chart, the told column was kept but the drill's level drawn, its order moved to
    // the date's (by x, 2000 points) under it
    const drilled = of('bar', { drill: { levels: ['region', 'desk'], path: ['EMEA'] } });
    const moved = editChart(drilled, { x: 'trade_date' }, CUBE);
    assert.equal(drilledView(moved).spec.x, 'trade_date', JSON.stringify(moved));
  });

  it('a bin of a big integer column, clicked or brushed, filters from its edge to the next as their digits -- never throws', () => {
    // markConditions says a big integer as its digits, "as the context menu's value filters do", and does for an equal;
    // rangeConditions takes no digits for a number, so a bin past 2^53 threw "a range of 'trade_id' (Integer) has no
    // literal for ..."
    const ids: CubeSnapshot = { ...CUBE, columns: [...CUBE.columns, { name: 'trade_id', type: 'Integer', kind: 'dimension' }] };
    const byId = of('bar', { x: 'trade_id', bins: { width: 1000 }, y: [SUM_QTY] });
    const said = (conditions: readonly unknown[]): string[] =>
      conditions.map((c) => { const k = c as { operator: string; value: unknown }; return `${k.operator} ${String(k.value)}`; });
    assert.deepEqual(said(markConditions(byId, ids, { trade_id: 9007199254740000n }, 9007199254741000n)),
      ['greaterThanEqual 9007199254740000', 'lessThan 9007199254741000']);
    assert.deepEqual(said(brushConditions(byId, ids, { kind: 'across', values: [9007199254740000n], upper: 9007199254741000n })),
      ['greaterThanEqual 9007199254740000', 'lessThan 9007199254741000']);
  });
});
