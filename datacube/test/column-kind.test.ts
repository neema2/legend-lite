import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { serialize } from '../src/serialize.ts';
import type { CubeSnapshot } from '../src/snapshot.ts';
import {
  dimensionColumns,
  kindOf,
  measureColumns,
  totalOrderSorts,
} from '../src/snapshot.ts';

const CUBE: CubeSnapshot = {
  source: { expression: 't' },
  columns: [
    { name: 'region', type: 'String' },
    { name: 'year', type: 'Integer', kind: 'dimension' },
    { name: 'notional', type: 'Float' },
    { name: 'traded', type: 'Date' },
  ],
  derived: [],
  rows: ['region'],
  pivotOn: ['year'],
  measures: [{ name: 'total', column: 'notional', fn: 'sum' }],
  sorts: [],
  epoch: 1,
};

describe('column kind', () => {
  it('defaults numeric columns to measures and the rest to dimensions', () => {
    assert.equal(kindOf({ name: 'n', type: 'Float' }), 'measure');
    assert.equal(kindOf({ name: 'n', type: 'Integer' }), 'measure');
    assert.equal(kindOf({ name: 's', type: 'String' }), 'dimension');
    assert.equal(kindOf({ name: 'd', type: 'Date' }), 'dimension');
  });

  it('lets an explicit kind win, which a year needs', () => {
    // A year is numeric and is almost always a dimension; defaulting
    // it to a measure would offer to sum it.
    assert.equal(
      kindOf({ name: 'year', type: 'Integer', kind: 'dimension' }),
      'dimension',
    );
  });

  it('partitions the columns for a UI to offer', () => {
    assert.deepEqual(
      dimensionColumns(CUBE).map((c) => c.name),
      ['region', 'year', 'traded'],
    );
    assert.deepEqual(
      measureColumns(CUBE).map((c) => c.name),
      ['notional'],
    );
  });
});

describe('excludedFromPivot', () => {
  it('keeps the column out of the pivot even when listed there', () => {
    // The exclusion must not be defeatable by the order the user
    // configured things in.
    const s: CubeSnapshot = {
      ...CUBE,
      columns: CUBE.columns.map((c) =>
        c.name === 'year' ? { ...c, excludedFromPivot: true } : c,
      ),
    };
    const out = serialize(s);
    assert.equal(out.includes('pivot('), false, 'no pivot stage remains');
    assert.match(out, /groupBy\(~\[region\]/, 'it falls back to a plain group');
    assert.equal(out.includes('year'), false, 'and year is not selected');
  });

  it('leaves other pivot dimensions alone', () => {
    const s: CubeSnapshot = {
      ...CUBE,
      columns: [
        ...CUBE.columns,
        { name: 'qtr', type: 'String', excludedFromPivot: true },
      ],
      pivotOn: ['year', 'qtr'],
    };
    const out = serialize(s);
    assert.match(out, /pivot\(~\[year\]/);
    assert.equal(out.includes('qtr'), false);
  });
});

describe('treeColumnSort', () => {
  it('orders the groups descending when asked', () => {
    // It applies at every level, including ones not yet opened, which
    // is why it is separate from the per-column sorts.
    const s: CubeSnapshot = { ...CUBE, treeColumnSort: 'desc' };
    assert.deepEqual(totalOrderSorts(s, ['region']), [
      { column: 'region', direction: 'desc' },
    ]);
    assert.match(serialize(s), /sort\(\[~region->descending\(\)\]\)/);
  });

  it('defaults to ascending', () => {
    assert.deepEqual(totalOrderSorts(CUBE, ['region']), [
      { column: 'region', direction: 'asc' },
    ]);
  });

  it('does not override an explicit sort on that column', () => {
    const s: CubeSnapshot = {
      ...CUBE,
      treeColumnSort: 'desc',
      sorts: [{ column: 'region', direction: 'asc' }],
    };
    assert.deepEqual(totalOrderSorts(s, ['region']), [
      { column: 'region', direction: 'asc' },
    ]);
  });
});
