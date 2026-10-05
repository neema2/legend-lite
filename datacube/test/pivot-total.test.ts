// The pivot total column: upstream stores its settings and draws
// nothing, which the user ruled a bug (2026-09-25). It is a column of the
// level's own query now -- the measure over all of the group's rows, in
// the same groupBy as the cells (docs/DATACUBE_CUBE_PLAN_DESIGN_2026_09_27.md)
// -- so these pin that query, where its header sits, and its settings.
// The figures themselves are judged by rows in //datacube:pivot_rows_test.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
  DEFAULT_CONFIGURATION,
  applyToSnapshot,
  fromSnapshot,
  toColumnLayout,
  withColumn,
} from '../src/config.ts';
import { buildColumnModel } from '../src/grid/columns.ts';
import type { ResultTable } from '../../engine-client/src/result.ts';
import {
  pivotColumns,
  type PivotFacts,
} from '../src/query.ts';
import { isPivotTotalColumn, pivotTotalColumn } from '../src/snapshot.ts';
import type { CubeSnapshot } from '../src/snapshot.ts';
import { accessor } from '../../pure-protocol/src/index.ts';
import { printLevel } from './lite-compiler.ts';

const CUBE: CubeSnapshot = {
  source: { query: accessor('db', 'trades') },
  columns: [
    { name: 'region', type: 'String', kind: 'dimension' },
    { name: 'year', type: 'Integer', kind: 'dimension' },
    { name: 'notional', type: 'Float', kind: 'measure' },
    { name: 'price', type: 'Float', kind: 'measure', aggregate: 'average' },
  ],
  derived: [],
  rows: ['region'],
  pivotOn: ['year'],
  measures: [],
  sorts: [{ column: '2021__|__notional', direction: 'desc' }],
  pivotTotal: { placement: 'right' },
  epoch: 1,
};
const YEARS: PivotFacts = { tuples: [['2021'], ['2022']] };

function table(columns: { name: string; values: (string | number | null)[] }[]):
ResultTable {
  return {
    columns: columns.map((c) => ({ name: c.name, type: 'Float', values: c.values })),
    rowCount: columns[0]?.values.length ?? 0,
    epoch: 1,
    elapsedMs: 0,
  };
}

describe('the pivot total, a column of the level\'s own query', () => {
  it('is each measure over ALL of the group\'s rows, on its own aggregate', () => {
    const q = printLevel(CUBE, { level: 1, parent: [] }, YEARS);
    // No condition: every value's rows, the NULL year's included.
    assert.ok(q.includes(`'${pivotTotalColumn('notional')}':x|$x.notional:y|$y->sum()`), q);
    // An average's total is the average of the slice, from the database.
    assert.ok(q.includes(`'${pivotTotalColumn('price')}':x|$x.price:y|$y->average()`), q);
    // One query: no second groupBy, no join in JavaScript.
    assert.equal(q.split('groupBy(').length, 2);
  });

  it('takes the configured total function over the measure\'s own', () => {
    const q = printLevel(
      { ...CUBE, pivotTotal: { placement: 'right', functions: { price: 'max' } } },
      { level: 1, parent: [] }, YEARS,
    );
    assert.ok(q.includes(`'${pivotTotalColumn('price')}':x|$x.price:y|$y->max()`), q);
  });

  it('is on the grand total too', () => {
    const q = printLevel({ ...CUBE, rows: [] }, undefined, YEARS);
    assert.match(q, /groupBy\(~\[__root__\]/);
    assert.ok(q.includes(pivotTotalColumn('notional')), q);
  });

  it('is absent without a total, and listed last among the planned columns', () => {
    const { pivotTotal: _t, ...none } = CUBE;
    assert.doesNotMatch(printLevel(none, { level: 1, parent: [] }, YEARS), /__pivot_total__/);
    const planned = pivotColumns(CUBE, YEARS).map((c) => c.name);
    assert.deepEqual(planned, [
      '2021__|__notional', '2021__|__price', '2022__|__notional', '2022__|__price',
      pivotTotalColumn('notional'), pivotTotalColumn('price'),
    ]);
  });
});

describe('the total column in the grid', () => {
  const answer = table([
    { name: '2021__|__notional', values: [1] },
    { name: '2022__|__notional', values: [2] },
    { name: pivotTotalColumn('notional'), values: [3] },
  ]);

  it('reads the configured name, not its key', () => {
    const model = buildColumnModel(answer, [], ['notional'], {
      pivotTotal: { label: 'All Years', placement: 'right' },
    }, 1);
    const total = model.leaves.find((l) => isPivotTotalColumn(l.name));
    assert.deepEqual(total?.path, ['All Years', 'notional']);
  });

  it('sits on the configured edge of the pivot', () => {
    const right = buildColumnModel(answer, [], ['notional'], {
      pivotTotal: { label: 'Total', placement: 'right' },
      pivotDirections: ['asc'],
    }, 1);
    assert.ok(isPivotTotalColumn(right.leaves.at(-1)?.name ?? ''));
    const left = buildColumnModel(answer, [], ['notional'], {
      pivotTotal: { label: 'Total', placement: 'left' },
      pivotDirections: ['desc'],
    }, 1);
    assert.ok(isPivotTotalColumn(left.leaves[0]?.name ?? ''));
    assert.deepEqual(left.leaves.map((l) => l.path[0]),
      ['Total', '2022', '2021']);
  });
});

describe('the pivot total settings', () => {
  it('reach the query, and read back from it', () => {
    const config = withColumn(
      { ...DEFAULT_CONFIGURATION, pivotStatisticColumnPlacement: 'left' },
      'price', { pivotStatisticColumnFunction: 'max' },
    );
    const { pivotTotal: _t, ...bare } = CUBE;
    const shaped = applyToSnapshot(bare, config);
    assert.deepEqual(shaped.pivotTotal,
      { placement: 'left', functions: { price: 'max' } });
    const back = fromSnapshot(shaped);
    assert.equal(back.pivotStatisticColumnPlacement, 'left');
    assert.equal(back.columns['price']?.pivotStatisticColumnFunction, 'max');
  });

  it('show a total on a new cube, and none when placement is unset', () => {
    assert.equal(DEFAULT_CONFIGURATION.pivotStatisticColumnPlacement, 'right');
    const { pivotStatisticColumnPlacement: _p, ...hidden } = DEFAULT_CONFIGURATION;
    const { pivotTotal: _t, ...bare } = CUBE;
    assert.equal(applyToSnapshot(bare, hidden).pivotTotal, undefined);
    assert.equal(toColumnLayout(hidden).pivotTotal, undefined);
    assert.deepEqual(toColumnLayout(DEFAULT_CONFIGURATION).pivotTotal,
      { label: 'Total', placement: 'right' });
  });
});

describe('measures a pivot does not spread', () => {
  // `price` kept OUT of the pivot: a column of the level's query on its
  // own aggregate, beside the cells -- never spread, never summed over a
  // finer intermediate.
  const EXCLUDED: CubeSnapshot = {
    ...CUBE,
    columns: CUBE.columns.map((c) => (c.name === 'price'
      ? { ...c, excludedFromPivot: true } : c)),
  };

  it('are carried on their own aggregate, in the same groupBy', () => {
    const pure = printLevel(EXCLUDED, { level: 1, parent: [] }, YEARS);
    assert.doesNotMatch(pure, /__\|__price/);
    assert.match(pure, /[~ ,]price:x\|\$x\.price:y\|\$y->average\(\)/);
  });

  it('a configured measure excluded from the pivot is carried under its own name', () => {
    const pure = printLevel({ ...EXCLUDED,
      measures: [{ name: 'n', column: 'notional', fn: 'sum' },
        { name: 'p', column: 'price', fn: 'max' }] },
    { level: 1, parent: [] }, YEARS);
    assert.match(pure, /'2021__\|__n'/);
    assert.doesNotMatch(pure, /__\|__p'/);
    assert.match(pure, /[~ ,]p:x\|\$x\.price:y\|\$y->max\(\)/);
  });
});
