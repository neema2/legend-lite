// Ad Hoc Analysis mode's queries and the grid their answers make.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { assembleGrid, planQueries, segmentLabel, type AdHocCube } from '../src/adhoc/query.ts';
import {
  initialGrid,
  povToAxis,
  setPov,
  withOptions,
  zoomIn,
  type AdHocGrid,
} from '../src/adhoc/state.ts';
import type { ResultTable } from '../src/result.ts';

const CUBE: AdHocCube = {
  snapshot: {
    source: { expression: 't' },
    columns: [
      { name: 'year', type: 'String' },
      { name: 'quarter', type: 'String' },
      { name: 'region', type: 'String' },
      { name: 'notional', type: 'Float' },
      { name: 'pnl', type: 'Float' },
    ],
    derived: [],
    rows: [],
    pivotOn: [],
    measures: [],
    sorts: [],
    epoch: 1,
  },
  outline: {
    dimensions: [
      { name: 'Time', generations: ['year', 'quarter'] },
      { name: 'Geography', generations: ['region'] },
    ],
    measures: ['notional', 'pnl'],
  },
  measures: [
    { name: 'notional', column: 'notional', fn: 'sum' },
    { name: 'pnl', column: 'pnl', fn: 'sum' },
  ],
};

const table = (cols: Record<string, (string | number | null)[]>): ResultTable => {
  const entries = Object.entries(cols);
  return {
    columns: entries.map(([name, values]) => ({ name, type: 'String', values })),
    rowCount: entries[0]?.[1].length ?? 0,
    epoch: 1,
    elapsedMs: 1,
  };
};

// Time zoomed: the top, 2021 and its quarters, 2022.
const zoomed = (): AdHocGrid => {
  let g = initialGrid(CUBE.outline);
  g = zoomIn(g, 'Time', [], [['2021'], ['2022']]);
  return zoomIn(g, 'Time', ['2021'], [['2021', 'Q1'], ['2021', 'Q2']]);
};

describe('the queries: one per shape', () => {
  it('a grid mixing generations asks one query per generation, each filtered to its members', () => {
    const qs = planQueries(CUBE, zoomed());
    assert.deepEqual(qs.map((q) => q.key), ['0', '1', '2']);
    const [top, years, quarters] = qs;
    assert.doesNotMatch(top!.pure, /groupBy\(~\[year/, 'the top is one row, ungrouped');
    assert.match(years!.pure, /groupBy\(~\[year\]/);
    assert.match(years!.pure, /\$x\.year == '2021'.*\$x\.year == '2022'/s);
    assert.match(quarters!.pure, /groupBy\(~\[year, quarter\]/);
    assert.match(quarters!.pure, /\$x\.quarter == 'Q1'/);
    assert.doesNotMatch(quarters!.pure, /'2022'/, 'only the members shown at that generation');
  });

  it('the POV pins every query', () => {
    const g = setPov(zoomed(), 'Geography', ['EMEA']);
    for (const q of planQueries(CUBE, g)) assert.match(q.pure, /\$x\.region == 'EMEA'/);
  });

  it('asks only for the measures shown', () => {
    const g = { ...zoomed(), columns: [{ dimension: 'Measures', members: [['pnl']] }] };
    for (const q of planQueries(CUBE, g)) {
      assert.match(q.pure, /pnl/);
      assert.doesNotMatch(q.pure, /notional/);
    }
  });
});

describe('the grid the answers make', () => {
  const answers = (g: AdHocGrid) => {
    const qs = planQueries(CUBE, g);
    const results = new Map<string, ResultTable>([
      ['0', table({ notional: [100], pnl: [10] })],
      ['1', table({ year: ['2021', '2022'], notional: [60, 40], pnl: [6, 4] })],
      // Q2 has no data: its row is missing.
      ['2', table({ year: ['2021'], quarter: ['Q1'], notional: [60], pnl: [6] })],
    ]);
    return assembleGrid(CUBE, g, results, qs);
  };

  it('places each cell by its members, indents by generation, and suppresses missing rows', () => {
    const v = answers(zoomed());
    const labels = v.table.columns[0]?.values;
    assert.deepEqual(labels, ['Time', ' 2021', '  Q1', ' 2022']);
    assert.deepEqual(v.table.columns.slice(1).map((c) => c.values),
      [[100, 60, 60, 40], [10, 6, 6, 4]]);
    assert.deepEqual(v.table.columns.slice(1).map((c) => segmentLabel(c.name)), ['notional', 'pnl']);
  });

  it('shows the missing row when suppression is off', () => {
    const v = answers(withOptions(zoomed(), { suppressMissingRows: false }));
    assert.equal(v.table.rowCount, 5);
    assert.deepEqual(v.table.columns[1]?.values, [100, 60, 60, null, 40]);
  });

  it('nests a second row dimension, and blanks repeated outer members when asked', () => {
    let g = povToAxis(zoomIn(initialGrid(CUBE.outline), 'Time', [], [['2021']]), 'Geography', 'rows');
    g = zoomIn(g, 'Geography', [], [['EMEA'], ['AMER']]);
    g = withOptions(g, { suppressMissingRows: false, suppressRepeatedMembers: true, indentation: 'none' });
    const v = assembleGrid(CUBE, g, new Map(), planQueries(CUBE, g));
    assert.deepEqual(v.table.columns[0]?.values,
      ['Time', null, null, '2021', null, null]);
    assert.deepEqual(v.table.columns[1]?.values,
      ['Geography', 'EMEA', 'AMER', 'Geography', 'EMEA', 'AMER']);
  });
});
