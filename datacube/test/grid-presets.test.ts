// THE GRID'S PRESETS (src/grid-presets.ts; docs/DATACUBE_ONE_API_PLAN_2026_10_21.md K4): the header menu's By Day/Week/…
// and Show As, and Python's grid.grain, grid.show_as and grid.bins, are one rule -- the calculated columns the chart's
// presets are (calc-presets.ts), at the preset's level, under a name the grid has not -- and what DataCube has not is
// refused in words.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { element } from '../../pure-protocol/src/index.ts';
import { binColumns, grainColumn, showAsColumns } from '../src/calc-presets.ts';
import { presetCommands, readGridPreset, type GridPreset } from '../src/grid-presets.ts';
import { CubeRefusal, type CubeSnapshot } from '../src/snapshot.ts';

const CUBE: CubeSnapshot = {
  source: { query: element('t') },
  columns: [
    { name: 'region', type: 'String' },
    { name: 'desk', type: 'String' },
    { name: 'traded', type: 'StrictDate' },
    { name: 'notional', type: 'Float' },
  ],
  derived: [],
  rows: ['region', 'desk'],
  pivotOn: [],
  sorts: [],
  epoch: 1,
};

const refused = (f: () => unknown, words: RegExp): void => {
  assert.throws(f, (e: unknown) => e instanceof CubeRefusal && words.test(e.message), String(words));
};

const MONTH: GridPreset = { kind: 'grain', column: 'traded', grain: 'month' };
/** The names of the columns a preset's commands add. */
const named = (s: CubeSnapshot, p: GridPreset): string[] => presetCommands(s, p).map((c) => (c.kind === 'calculate' ? c.column.name : c.kind));

describe("the grid's presets: one rule for the header menu and Python (grid-presets.ts)", () => {
  it('a period: the date at its grain, before the grouping, named after the column and the grain', () => {
    assert.deepEqual(presetCommands(CUBE, MONTH),
      [{ kind: 'calculate', level: 'row', column: grainColumn('traded', 'month', 'traded_month') }]);
  });

  it('a Show As: columns after the grouping, along the row groups in order, named after the measure and the kind', () => {
    assert.deepEqual(presetCommands(CUBE, { kind: 'showAs', column: 'notional', showAs: 'percentOfTotal' }),
      showAsColumns({ kind: 'percentOfTotal' }, 'notional', ['region', 'desk'], [], 'notional_percentOfTotal')
        .map((column) => ({ kind: 'calculate', level: 'group', column })));
  });

  it('bins: the bin and its upper edge, before the grouping, named after the column', () => {
    assert.deepEqual(presetCommands(CUBE, { kind: 'bins', column: 'notional', bins: { width: 10 } }),
      binColumns('notional', { width: 10 }, 'notional_bins').map((column) => ({ kind: 'calculate', level: 'row', column })));
  });

  it('a name the grid has -- the column, or a helper column of the preset -- takes _2, then _3', () => {
    const once = { ...CUBE, derived: [grainColumn('traded', 'month', 'traded_month')] };
    assert.deepEqual(named(once, MONTH), ['traded_month_2']);
    const twice = { ...once, derived: [...once.derived, grainColumn('traded', 'month', 'traded_month_2')] };
    assert.deepEqual(named(twice, MONTH), ['traded_month_3']);
    // percentOfTotal reads a helper of its own name: one the grid has moves it on too
    const helper = { ...CUBE, groupDerived: [{ name: 'notional_percentOfTotal__total', window: { fn: 'sum' as const, column: 'notional', partition: [], order: [] } }] };
    assert.deepEqual(named(helper, { kind: 'showAs', column: 'notional', showAs: 'percentOfTotal' }),
      ['notional_percentOfTotal_2__total', 'notional_percentOfTotal_2']);
  });

  it('refused in words: a period, a Show As or bins DataCube has not', () => {
    refused(() => presetCommands(CUBE, { kind: 'grain', column: 'traded', grain: 'fortnight' as 'month' }),
      /a period is day, week, month, quarter, year, not "fortnight"/);
    refused(() => presetCommands(CUBE, { kind: 'showAs', column: 'notional', showAs: 'median' as 'rank' }), /a Show As is .*, not "median"/);
    refused(() => presetCommands(CUBE, { kind: 'bins', column: 'notional', bins: {} }), /bins are a width or a count, one of them/);
    refused(() => presetCommands(CUBE, { kind: 'bins', column: 'notional', bins: { width: 10, count: 3 } }), /one of them/);
    refused(() => presetCommands(CUBE, { kind: 'bins', column: 'notional', bins: { width: -1 } }), /a bin's width is a positive number, not -1/);
    refused(() => presetCommands(CUBE, { kind: 'bins', column: 'notional', bins: { count: 2.5 } }), /a whole number from 1, not 2.5/);
  });

  it('read from JSON as the same preset; an unknown kind or field refused in words', () => {
    for (const p of [MONTH, { kind: 'showAs', column: 'notional', showAs: 'rank' }, { kind: 'bins', column: 'notional', bins: { count: 4 } }] as GridPreset[]) {
      assert.deepEqual(readGridPreset(JSON.parse(JSON.stringify(p))), p);
    }
    refused(() => readGridPreset({ kind: 'grain', column: 'traded', grain: 'month', extra: 1 }), /a 'grain' preset takes grain, not extra/);
    refused(() => readGridPreset({ kind: 'spread', column: 'traded' }), /a preset is 'grain', 'showAs' or 'bins', not "spread"/);
    refused(() => readGridPreset(['grain']), /a preset is \{kind, column, \.\.\.\}/);
  });
});
