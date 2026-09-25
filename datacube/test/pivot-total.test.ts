// The pivot total column: upstream stores its settings and draws
// nothing, which the user ruled a bug (2026-09-25). These pin the three
// halves that make it real -- the query that computes it, the join that
// puts it beside the pivot, and where its header sits.

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
import type { ResultTable } from '../src/result.ts';
import type { QueryRunner } from '../src/runner.ts';
import { NULL_GROUP, pivotTotalQuery } from '../src/serialize.ts';
import type { CubeSnapshot } from '../src/snapshot.ts';
import {
  isPivotTotalColumn,
  pivotTotalColumn,
  withPivotTotals,
} from '../src/treeview.ts';

const CUBE: CubeSnapshot = {
  source: { expression: '#>{db.trades}#' },
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

function table(columns: { name: string; values: (string | number | null)[] }[]):
ResultTable {
  return {
    columns: columns.map((c) => ({ name: c.name, type: 'Float', values: c.values })),
    rowCount: columns[0]?.values.length ?? 0,
    epoch: 1,
    elapsedMs: 0,
  };
}

describe('the pivot total query', () => {
  it('is the level with the pivot key dropped, each measure on its own aggregate', () => {
    const q = pivotTotalQuery(CUBE, { level: 1, parent: [] });
    assert.ok(q);
    assert.deepEqual(q.measures, ['notional', 'price']);
    assert.doesNotMatch(q.pure, /pivot\(/);
    assert.match(q.pure, /groupBy\(~\[region\]/);
    assert.match(q.pure, /notional:x\|\$x\.notional:y\|\$y->sum\(\)/);
    // An average's total is the average of the slice, from the database.
    assert.match(q.pure, /price:x\|\$x\.price:y\|\$y->average\(\)/);
    // A sort on a pivot column names a column this query does not have.
    assert.doesNotMatch(q.pure, /2021__\|__notional/);
    // Only the keys and the measures: the pivot key is not aggregated.
    assert.doesNotMatch(q.pure, /year:x/);
  });

  it('takes the configured total function over the measure\'s own', () => {
    const q = pivotTotalQuery(
      { ...CUBE, pivotTotal: { placement: 'right', functions: { price: 'max' } } },
      { level: 1, parent: [] },
    );
    assert.match(q?.pure ?? '', /price:x\|\$x\.price:y\|\$y->max\(\)/);
  });

  it('is a single-row grand total on a flat pivot', () => {
    const q = pivotTotalQuery({ ...CUBE, rows: [] }, undefined);
    assert.match(q?.pure ?? '', /groupBy\(~\[__root\]|groupBy\(~\['?__/);
  });

  it('names the groups to fetch when the pivot level was cut short', () => {
    const q = pivotTotalQuery(CUBE, { level: 1, parent: [] }, ['EMEA', NULL_GROUP]);
    assert.match(q?.pure ?? '', /\$x\.region == 'EMEA'/);
    assert.match(q?.pure ?? '', /isEmpty\(\)/);
  });

  it('is nothing without a pivot or without a total', () => {
    assert.equal(pivotTotalQuery({ ...CUBE, pivotOn: [] }, undefined), null);
    const { pivotTotal: _t, ...none } = CUBE;
    assert.equal(pivotTotalQuery(none, undefined), null);
  });
});

describe('joining the totals', () => {
  const runner = (totals: ResultTable): QueryRunner & { asked: string[] } => {
    const asked: string[] = [];
    return {
      name: 'fake',
      asked,
      run: async (pure) => {
        asked.push(pure);
        return { rows: totals, sql: '' };
      },
    };
  };

  it('joins by GROUP KEY, not position, and leaves a missing group blank', async () => {
    const pivot = table([
      { name: 'region', values: ['EMEA', 'AMER', 'APAC'] },
      { name: '2021__|__notional', values: [1, 2, 3] },
    ]);
    // The totals come back in another order, and without APAC.
    const totals = table([
      { name: 'region', values: ['AMER', 'EMEA'] },
      { name: 'notional', values: [20, 10] },
      { name: 'price', values: [2, 1] },
    ]);
    const r = runner(totals);
    const out = await withPivotTotals(
      CUBE, { level: 1, parent: [] }, pivot,
      [['EMEA'], ['AMER'], ['APAC']], false,
      { runner: r, snapshot: CUBE },
    );
    const col = out.columns.find((c) => c.name === pivotTotalColumn('notional'));
    assert.deepEqual(col?.values, [10, 20, null]);
    assert.equal(r.asked.length, 1);
  });

  it('puts the grand total beside a level-0 row', async () => {
    const pivot = table([{ name: '2021__|__notional', values: [5] }]);
    const totals = table([
      { name: '__root', values: ['[ROOT]'] },
      { name: 'notional', values: [99] },
      { name: 'price', values: [3] },
    ]);
    const out = await withPivotTotals(
      CUBE, { level: 0, parent: [] }, pivot, [[]], false,
      { runner: runner(totals), snapshot: CUBE },
    );
    assert.deepEqual(
      out.columns.find((c) => c.name === pivotTotalColumn('notional'))?.values,
      [99],
    );
  });

  it('asks nothing when there is no total to show', async () => {
    const r = runner(table([]));
    const { pivotTotal: _t, ...none } = CUBE;
    const pivot = table([{ name: 'region', values: ['EMEA'] }]);
    const out = await withPivotTotals(
      none, { level: 1, parent: [] }, pivot, [['EMEA']], false,
      { runner: r, snapshot: none },
    );
    assert.equal(out, pivot);
    assert.equal(r.asked.length, 0);
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
