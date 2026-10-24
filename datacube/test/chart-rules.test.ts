// DataCube's chart rules, as cases (test/fixtures/chart-rules.json): each a chart over the fixture's columns, refused or
// not. The rules are written once, here in DataCube, and each case goes the way a chart Python makes goes
// (headless/grid.ts gridChart): made as DataCube's own new chart changed by hand (chart-spec.ts chartFrom, editChart),
// read as a page reads it, regrouped as the panel keeps it (followCube), then judged (chartProblems).
// python/tests/test_page.py holds Python to the same answers, which Python gets by asking this code, headless
// (legend_lite/page.py _made) -- never by rules of its own (the review of 2d24022).

import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { describe, it } from 'node:test';

import { element } from '../../pure-protocol/src/index.ts';
import {
  chartFrom, chartGrid, chartProblems, chartQuery, chartStateOf, DEFAULT_CHART_LIMIT, defaultChart, DEFAULT_OPTIONS, editChart,
  followCube, followRefusal, NO_CHART, readChart, showGridWay, TIME_LIMIT, type ChartMark, type ChartSpec,
} from '../src/chart-spec.ts';
import { DEFAULT_CONFIGURATION } from '../src/config.ts';
import type { CubeSnapshot } from '../src/snapshot.ts';
import { runfileFromEnv } from '../../tools/js/runfiles.mts';

interface Rules {
  readonly columns: readonly { readonly name: string; readonly type: string }[];
  readonly cases: readonly { readonly case: string; readonly refused: boolean; readonly spec: ChartSpec }[];
}

const rules = JSON.parse(readFileSync(runfileFromEnv('CHART_RULES'), 'utf8')) as Rules;

const CUBE: CubeSnapshot = {
  source: { query: element('sales') },
  columns: rules.columns.map((c) => ({ name: c.name, type: c.type })),
  derived: [],
  rows: [],
  pivotOn: [],
  sorts: [],
  epoch: 1,
};

/** What a chart Python makes is told, as gridChart judges it: its problems, the reader's among them. */
function problemsOf(given: Record<string, unknown>, s: CubeSnapshot = CUBE, before?: ChartSpec): string[] {
  const made = chartFrom(given, s, before);
  if (typeof made === 'string') return [made];
  const followed = followCube(made, s);
  const replaced = followRefusal(Object.keys(given), made, followed);
  return replaced !== null ? [replaced] : chartProblems(followed, s);
}

describe('DataCube\'s chart rules, case by case (Python asks the same code)', () => {
  for (const c of rules.cases) {
    it(`${c.case}: ${c.refused ? 'refused' : 'drawn'}`, () => {
      const problems = problemsOf(c.spec as unknown as Record<string, unknown>);
      if (c.refused) assert.ok(problems.length > 0, 'refused');
      else assert.deepEqual(problems, []);
    });
  }
});

describe('a chart DataCube makes of what it is told (chartFrom: Python\'s grid.chart, Chart.plot and Chart.options)', () => {
  const made = (given: Record<string, unknown>, before?: ChartSpec, s: CubeSnapshot = CUBE): ChartSpec => {
    const spec = chartFrom(given, s, before);
    if (typeof spec === 'string') throw new Error(spec);
    return spec;
  };
  const GROUPED: CubeSnapshot = { ...CUBE, rows: ['region', 'channel'] };

  it('what is not told is its own new chart\'s: a date across runs in time order, and long; a category leads with the largest', () => {
    const byDay = made({ mark: 'line', x: 'day' });
    assert.deepEqual(byDay.options.sort, { by: 'x', direction: 'asc' });
    assert.equal(byDay.options.limit, TIME_LIMIT);
    assert.deepEqual(made({ mark: 'bar', x: 'region' }).options, DEFAULT_OPTIONS);
    // told options win, the rest are the new chart's
    assert.deepEqual(made({ mark: 'line', x: 'day', options: { limit: 10 } }).options.sort, { by: 'x', direction: 'asc' });
    assert.equal(made({ mark: 'line', x: 'day', options: { limit: 10 } }).options.limit, 10);
    // its grouping, untold, is the new chart's: the grid's first row group across, its second split
    const pie = made({ mark: 'pie' }, undefined, GROUPED);
    assert.equal(pie.x, 'region');
    assert.equal(pie.split, 'channel');
  });

  it('its measure, untold, is the new chart\'s: a fraction first, summed; a count when the cube has no number', () => {
    assert.deepEqual(made({ mark: 'bar', x: 'region' }).y, [{ column: 'revenue', fn: 'sum' }]);
    const words: CubeSnapshot = { ...CUBE, columns: [{ name: 'region', type: 'String' }] };
    assert.deepEqual(made({ mark: 'bar', x: 'region' }, undefined, words).y, [{ column: 'region', fn: 'count' }]);
    // a cube with no column to put across has no new chart, as the panel has none
    assert.equal(chartFrom({ mark: 'kpi' }, { ...CUBE, columns: [{ name: 'revenue', type: 'Float' }] }), NO_CHART);
  });

  it('a kind takes what it needs and leaves what it has no place for, as the form\'s change of kind does', () => {
    const gauge = made({ mark: 'gauge', options: { target: 5 } }, undefined, GROUPED);
    assert.equal(gauge.x, undefined, 'a gauge has nothing across');
    assert.equal(gauge.split, undefined, 'nor a split');
    assert.equal(made({ mark: 'lollipop' }, undefined, GROUPED).split, undefined, 'a kind of one series has no split');
    // a Pareto runs from the largest, on a date too: the kind's order, not the date's
    const pareto = made({ mark: 'pareto', x: 'day' });
    assert.deepEqual(pareto.options.sort, { by: 'y', direction: 'desc' });
    assert.deepEqual(problemsOf({ mark: 'pareto', x: 'day' }), []);
    // what was told stays told, and is judged: a gauge told a column across is refused, never quietly changed
    assert.match(problemsOf({ mark: 'gauge', x: 'region', options: { target: 5 } }).join(' '), /no column across/);
  });

  it('a hand on what the grid\'s grouping sets freezes it, as the panel\'s does; frozen told is kept', () => {
    assert.equal(made({ mark: 'pie' }, undefined, GROUPED).frozen, undefined,
      'told nothing of its grouping, it follows');
    assert.equal(made({ mark: 'bar', x: 'channel' }, undefined, GROUPED).frozen, true);
    assert.equal(made({ mark: 'bar', split: 'region' }, undefined, GROUPED).frozen, true);
    assert.equal(made({ mark: 'bar', x: 'channel', frozen: null }, undefined, GROUPED).frozen, undefined, 'told live');
    // a KPI follows its measure, not its trend; a scatter plots rows, and follows nothing
    assert.equal(made({ mark: 'kpi', x: 'day' }).frozen, undefined);
    assert.equal(made({ mark: 'kpi', y: [{ column: 'cost', fn: 'sum' }] }).frozen, true);
    assert.equal(made({ mark: 'scatter', x: 'revenue', y: [{ column: 'cost', fn: 'sum' }] }).frozen, undefined);
    // a dumbbell keeps its own two values as the grid regroups: told them, it still follows
    const ends = [{ column: 'cost', fn: 'sum' }, { column: 'revenue', fn: 'sum' }];
    assert.equal(made({ mark: 'dumbbell', y: ends }, undefined, GROUPED).frozen, undefined);
  });

  it('told its grouping and told to follow the grid, what the grid would replace is refused, never quietly dropped', () => {
    assert.match(problemsOf({ mark: 'bar', x: 'channel', frozen: null }, GROUPED).join(' '),
      /follows its grid takes 'region' across, not 'channel'/);
    // told what the grid gives, it follows
    assert.deepEqual(problemsOf({ mark: 'bar', x: 'region', frozen: null }, GROUPED), []);
  });

  it('an edit keeps what it is not told; a new column across takes its own order; the grain goes with the old one', () => {
    const before = made({ mark: 'line', x: 'day', grain: 'month', split: 'channel', options: { labels: true } });
    const relabelled = made({ options: { legend: 'none' } }, before);
    assert.equal(relabelled.grain, 'month');
    assert.equal(relabelled.options.labels, true, 'the options it had stay');
    assert.equal(relabelled.options.legend, 'none');
    const moved = made({ x: 'region' }, before);
    assert.equal(moved.grain, undefined, 'the grain went with its column');
    assert.deepEqual(moved.options.sort, { by: 'y', direction: 'desc' }, 'a category leads with the largest');
    assert.equal(moved.split, 'channel');
  });

  it('null takes a slot or an option away; a chart with nothing to plot is refused', () => {
    const before = made({ mark: 'line', x: 'day', split: 'channel', options: { labels: true, zoom: true } });
    assert.equal(made({ split: null }, before).split, undefined);
    assert.equal(made({ options: { zoom: null } }, before).options.zoom, undefined);
    assert.match(String(chartFrom({ y: null }, CUBE, before)), /plots nothing/);
  });

  it('a word it does not know is refused by name, as a page refuses it', () => {
    assert.match(String(chartFrom({ mark: 'bar', x: 'region', options: { stack: 'stacked100' } }, CUBE)), /stack/);
    assert.match(String(chartFrom({ mark: 'barchart', x: 'region' }, CUBE)), /kind/);
    assert.match(String(chartFrom({ mark: 'bar', options: 'wide' }, CUBE)), /options/);
  });
});

describe('a change by hand (editChart: the charts\' form, and chartFrom)', () => {
  const GROUPED: CubeSnapshot = { ...CUBE, rows: ['region', 'channel'] };
  const start = defaultChart(GROUPED)!;

  it('the column across moved to a date takes the time order, as the grid regrouping it does', () => {
    const byDay = editChart(start, { x: 'day' }, GROUPED);
    assert.deepEqual(byDay.options.sort, { by: 'x', direction: 'asc' });
    assert.equal(byDay.options.limit, TIME_LIMIT);
    assert.equal(byDay.frozen, true, 'and freezes the live chart');
    assert.deepEqual(byDay.options.sort, followCube(start, { ...GROUPED, rows: ['day'] }).options.sort);
  });

  it('a column across moved back from a date takes a category\'s limit again; one the chart was told stays', () => {
    const back = editChart(editChart(start, { x: 'day' }, GROUPED), { x: 'region' }, GROUPED);
    assert.equal(back.options.limit, DEFAULT_CHART_LIMIT);
    const told = editChart(editChart(start, { x: 'day', options: { limit: 300 } }, GROUPED), { x: 'region' }, GROUPED);
    assert.equal(told.options.limit, 300);
  });

  it('a chart of rows keeps its options as its x moves: its query asks no order and no such limit', () => {
    const scatter = editChart(start, { mark: 'scatter', x: 'revenue', y: [{ column: 'cost', fn: 'sum' }] }, GROUPED);
    assert.deepEqual(editChart(scatter, { x: 'day' }, GROUPED).options, scatter.options);
  });

  it('a live chart the grid has not moved stays the same chart (the form is not painted again for nothing)', () => {
    assert.equal(followCube(start, GROUPED), start);
  });
});

// the user, 2026-10-23: "frozen charts don't freeze filters -- seems to only freeze pivots"; one word per state
describe('a chart\'s state: dynamic, pinned (its grouping its own) or frozen (its grouping and the grid\'s filter kept)', () => {
  const EMEA = { kind: 'condition', column: 'region', operator: 'equal', value: 'EMEA' } as const;
  const APAC = { kind: 'condition', column: 'region', operator: 'equal', value: 'APAC' } as const;
  const FILTERED: CubeSnapshot = { ...CUBE, rows: ['region', 'channel'], filter: EMEA };
  const UNFILTERED: CubeSnapshot = { ...CUBE, rows: ['region', 'channel'] };
  const made = (given: Record<string, unknown>, before?: ChartSpec, s: CubeSnapshot = FILTERED): ChartSpec => {
    const spec = chartFrom(given, s, before);
    if (typeof spec === 'string') throw new Error(spec);
    return spec;
  };

  it('frozen keeps the grid\'s filter as it is then; its query reads that, whatever the grid\'s filter is after', () => {
    const frozen = made({ state: 'frozen' }, made({ mark: 'bar' }));
    assert.equal(chartStateOf(frozen), 'frozen');
    assert.deepEqual(frozen.kept, { filter: EMEA });
    assert.equal(frozen.frozen, true, 'its grouping kept too');
    // the grid filtered otherwise since, or not at all: the chart's query is EMEA's
    assert.deepEqual(chartQuery({ ...FILTERED, filter: APAC }, frozen).snapshot.filter, EMEA);
    assert.deepEqual(chartQuery(UNFILTERED, frozen).snapshot.filter, EMEA);
    // frozen on a grid with no filter: it keeps none, and a filter put on the grid after does not reach it
    const bare = made({ state: 'frozen' }, made({ mark: 'bar' }, undefined, UNFILTERED), UNFILTERED);
    assert.deepEqual(bare.kept, {});
    assert.equal(chartQuery(FILTERED, bare).snapshot.filter, undefined);
    // frozen again, it stays as it was: the filter it kept is not taken afresh
    assert.deepEqual(made({ state: 'frozen' }, frozen, { ...FILTERED, filter: APAC }).kept, { filter: EMEA });
  });

  it('pinned keeps its grouping and follows the grid\'s filter; dynamic follows both; a hand on the axes pins', () => {
    const pinned = made({ state: 'pinned' }, made({ state: 'frozen' }, made({ mark: 'bar' })));
    assert.equal(chartStateOf(pinned), 'pinned');
    assert.equal(pinned.kept, undefined, 'its kept filter let go');
    assert.deepEqual(chartQuery({ ...FILTERED, filter: APAC }, pinned).snapshot.filter, APAC);
    const dynamic = made({ state: 'dynamic' }, pinned);
    assert.equal(chartStateOf(dynamic), 'dynamic');
    assert.equal(dynamic.frozen, undefined);
    assert.equal(chartStateOf(made({ mark: 'bar', x: 'channel' })), 'pinned', 'a hand on what the grouping sets pins it');
    // a state that is none, refused in words
    assert.equal(chartFrom({ state: 'stuck' }, FILTERED, made({ mark: 'bar' })),
      'DataCube cannot read this chart: a chart\'s state is one of dynamic, pinned, frozen, not "stuck".');
  });

  it('a kept filter is read as a page reads one: a filter, on a chart that keeps its grouping, else refused by name', () => {
    const base = made({ mark: 'bar' });
    assert.notEqual(typeof readChart({ ...base, frozen: true, kept: { filter: EMEA } }), 'string');
    assert.match(String(readChart({ ...base, kept: { filter: EMEA } })), /keeps its grouping too: it is frozen/);
    assert.match(String(readChart({ ...base, frozen: true, kept: { filter: { kind: 'nope' } } })), /the filter it keeps is not one/);
    assert.match(String(readChart({ ...base, frozen: true, kept: { rows: [] } })), /there is no "rows" in what it keeps/);
    // a column its kept filter reads that the grid has no more: said, never quietly unfiltered
    const gone = made({ state: 'frozen' }, base, { ...FILTERED, filter: { ...EMEA, column: 'channel' } });
    const without = { ...FILTERED, columns: FILTERED.columns.filter((c) => c.name !== 'channel'), rows: ['region'] };
    assert.match(chartProblems(gone, without).join(' '), /The filter it keeps reads 'channel'/);
  });

  it('Show Grid on a frozen chart: a grid of its own over the filter it kept, the chart following it, keeping nothing', () => {
    const frozen = made({ state: 'frozen' }, made({ mark: 'bar' }));
    const own = chartGrid({ snapshot: { ...FILTERED, filter: APAC }, config: DEFAULT_CONFIGURATION }, frozen, []);
    assert.deepEqual(own.snapshot.filter, EMEA);
    assert.equal(chartStateOf(own.spec), 'dynamic');
  });

  // the user, 2026-10-23: Show Grid on every chart -- a scatter, a map, a treemap, a gauge too
  describe('Show Grid on every kind', () => {
    const bar = made({ mark: 'bar' });
    /** `bar` made another kind, in a state, with `extra` and without the slots `drop` names. */
    const as = (mark: ChartMark, state: 'dynamic' | 'pinned' | 'frozen', extra: Partial<ChartSpec> = {},
      drop: readonly ('x' | 'split')[] = []): ChartSpec => {
      const { frozen: _f, kept: _k, ...rest } = { ...bar, mark, ...extra };
      const base = Object.fromEntries(Object.entries(rest).filter(([k]) => !(drop as readonly string[]).includes(k))) as unknown as ChartSpec;
      return state === 'dynamic' ? base : state === 'pinned' ? { ...base, frozen: true } : { ...base, frozen: true, kept: { filter: EMEA } };
    };
    const ownOf = (spec: ChartSpec) => chartGrid({ snapshot: { ...FILTERED, filter: APAC }, config: DEFAULT_CONFIGURATION }, spec, []);

    it('the grid it reads, where that grid holds what it draws; else one of its own', () => {
      const way = (spec: ChartSpec, follows = true) => showGridWay(spec, follows);
      // a chart of groups: its grid while it follows it; pinned or frozen, its grouping its own: a grid of its own
      assert.deepEqual([way(as('bar', 'dynamic')), way(as('bar', 'pinned')), way(as('bar', 'frozen'))], ['reveal', 'own', 'own']);
      // pinned, a chart taking nothing of its grid's grouping reads just what it read: its grid
      for (const mark of ['scatter', 'map', 'bubblemap', 'gauge', 'kpi'] as const) {
        assert.equal(way(as(mark, 'pinned')), 'reveal', mark);
        assert.equal(way(as(mark, 'frozen')), 'own', `${mark} frozen: the filter it keeps its own`);
      }
      // a treemap draws its grid as shown, whatever its state
      assert.equal(way(as('treemap', 'frozen')), 'reveal');
      // its grid removed: one of its own, whatever it is
      assert.deepEqual([way(as('bar', 'dynamic'), false), way(as('treemap', 'dynamic'), false)], ['own', 'own']);
    });

    it("a scatter's own grid: its rows as they are, its plotted columns shown and no other, over the filter it kept", () => {
      const own = ownOf(as('scatter', 'frozen', { x: 'cost', y: [{ column: 'revenue', fn: 'sum' }] }, ['split']));
      assert.deepEqual(own.snapshot.rows, []);
      assert.deepEqual(own.snapshot.filter, EMEA);
      const hidden = Object.entries(own.configuration.columns).filter(([, c]) => c.hidden).map(([n]) => n).sort();
      assert.deepEqual(hidden, ['at', 'channel', 'clock', 'day', 'month_start', 'profit', 'region']);
      assert.equal(chartStateOf(own.spec), 'dynamic');
      assert.equal(own.spec.x, 'cost', 'drawn as it was');
    });

    it("a map's grouped by its places; a gauge's and a KPI's one figure the grid's total; a treemap's the grid as it drew it", () => {
      assert.deepEqual(ownOf(as('map', 'frozen', { x: 'region' })).snapshot.rows, ['region']);
      assert.deepEqual(ownOf(as('bubblemap', 'frozen', { x: 'region', at: { lat: 'cost', lon: 'profit' } })).snapshot.rows,
        ['region', 'cost', 'profit']);
      for (const one of [as('gauge', 'frozen'), as('kpi', 'frozen', {}, ['x', 'split'])]) {
        const own = ownOf(one);
        assert.deepEqual(own.snapshot.rows, [], one.mark);
        assert.equal(own.configuration.showRootAggregation, true, `${one.mark}: its figure the total row`);
      }
      const tree = ownOf(as('treemap', 'frozen'));
      assert.deepEqual(tree.snapshot.rows, FILTERED.rows);
    });
  });
});

