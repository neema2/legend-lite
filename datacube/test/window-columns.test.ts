// Window columns as Pure: every function in the `over()` form the
// planner takes (the WASM differential plans these same shapes on both
// planners), and how a group-level window follows the tree's levels.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { derivedExtend, serialize, windowExtend } from '../src/serialize.ts';
import { renameColumnReferences, type CubeSnapshot, type WindowSpec } from '../src/snapshot.ts';

const base = { partition: [] as string[], order: [] as { column: string; direction: 'asc' | 'desc' }[] };

describe('window columns as Pure', () => {
  it('a running sum: partition, order, frame', () => {
    assert.equal(windowExtend('cum', { ...base, fn: 'sum', column: 'notional',
      partition: ['region', 'desk'], order: [{ column: 'year', direction: 'asc' }], frame: 'running' }),
    'extend(over(~[region, desk], [~year->ascending()], rows(unbounded(), 0)), ~[cum:{p,w,r|$r.notional}:y|$y->plus()])');
  });

  it('with no partition, the order alone -- or the general form to carry a frame', () => {
    assert.equal(windowExtend('rn', { ...base, fn: 'rowNumber', order: [{ column: 'pnl', direction: 'desc' }] }),
      'extend(over([~pnl->descending()]), ~[rn:{p,w,r|$p->rowNumber($r)}])');
    assert.equal(windowExtend('m', { ...base, fn: 'average', column: 'pnl',
      order: [{ column: 'year', direction: 'asc' }], frame: { lastRows: 3 } }),
    'extend(over([], [~year->ascending()], rows(-2, 0)), ~[m:{p,w,r|$r.pnl}:y|$y->average()])');
  });

  it('a whole-table aggregate: every row, whatever the order', () => {
    assert.equal(windowExtend('t', { ...base, fn: 'max', column: 'pnl' }),
      'extend(over([], [~pnl->ascending()], rows(unbounded(), unbounded())), ~[t:{p,w,r|$r.pnl}:y|$y->max()])');
  });

  it('ranking takes no frame; lag reads its column n rows back; last reads the whole partition', () => {
    const o = [{ column: 'year', direction: 'asc' as const }];
    assert.equal(windowExtend('r', { ...base, fn: 'rank', order: o, frame: 'running' }),
      'extend(over([~year->ascending()]), ~[r:{p,w,r|$p->rank($w, $r)}])');
    assert.equal(windowExtend('p', { ...base, fn: 'lag', column: 'pnl', partition: ['book'], order: o, offset: 2 }),
      'extend(over(~[book], [~year->ascending()]), ~[p:{p,w,r|$p->lag($r, 2).pnl}])');
    assert.equal(windowExtend('n', { ...base, fn: 'lead', column: 'pnl', order: o }),
      'extend(over([~year->ascending()]), ~[n:{p,w,r|$p->lead($r).pnl}])');
    assert.equal(windowExtend('l', { ...base, fn: 'last', column: 'pnl', partition: ['book'], order: o }),
      'extend(over(~[book], [~year->ascending()], rows(unbounded(), unbounded())), ~[l:{p,w,r|$p->last($w, $r).pnl}])');
    assert.equal(windowExtend('b', { ...base, fn: 'ntile', order: o, buckets: 10 }),
      'extend(over([~year->ascending()]), ~[b:{p,w,r|$p->ntile($r, 10)}])');
  });

  it('refuses what cannot mean anything', () => {
    assert.throws(() => windowExtend('r', { ...base, fn: 'rank' }), /needs an order/);
    assert.throws(() => windowExtend('s', { ...base, fn: 'sum', order: [{ column: 'year', direction: 'asc' }] }),
      /needs a column/);
  });
});

describe('a group-level window follows the tree', () => {
  const CUBE: CubeSnapshot = {
    source: { expression: 't' },
    columns: [
      { name: 'region', type: 'String' }, { name: 'desk', type: 'String' },
      { name: 'notional', type: 'Float' },
    ],
    derived: [],
    rows: ['region', 'desk'],
    pivotOn: [],
    measures: [{ name: 'notional', column: 'notional', fn: 'sum' }],
    sorts: [],
    epoch: 1,
    groupDerived: [{ name: 'running', expression: '', window: {
      fn: 'sum', column: 'notional', partition: ['region'], order: [], frame: 'running' } as WindowSpec }],
  };

  it('at a level, an empty order is the level\'s own; a dimension it lacks leaves the partition', () => {
    // Level 1 has region: each region is its own partition there.
    const q1 = serialize(CUBE, { level: 1, parent: [] });
    assert.match(q1, /extend\(over\(~\[region\], \[~region->ascending\(\)\], rows\(unbounded\(\), 0\)\), ~\[running:/);
    // A partition on a dimension the level lacks drops out.
    const byDesk = { ...CUBE, groupDerived: [{ ...CUBE.groupDerived![0]!, window: {
      ...CUBE.groupDerived![0]!.window!, partition: ['desk'] } }] };
    assert.match(serialize(byDesk, { level: 1, parent: [] }),
      /extend\(over\(\[\], \[~region->ascending\(\)\], rows\(unbounded\(\), 0\)\)/);
    const q2 = serialize(CUBE, { level: 2, parent: ['EMEA'] });
    assert.match(q2, /extend\(over\(~\[region\], \[~region->ascending\(\), ~desk->ascending\(\)\], rows\(unbounded\(\), 0\)\)/);
  });

  it('follows the grid\'s own sort when it has one', () => {
    const sorted = { ...CUBE, sorts: [{ column: 'notional', direction: 'desc' as const }] };
    assert.match(serialize(sorted, { level: 1, parent: [] }),
      /over\(~\[region\], \[~notional->descending\(\), ~region->ascending\(\)\], rows\(unbounded\(\), 0\)\)/);
  });

  it('the grand total orders by its one group', () => {
    assert.match(serialize(CUBE, { level: 0, parent: [] }), /over\(\[\], \[~__root__->ascending\(\)\], rows\(unbounded\(\), 0\)\)/);
  });

  it('a row-level window is written as its extend, before the filter', () => {
    const row: CubeSnapshot = { ...CUBE, groupDerived: [], derived: [{ name: 'rk', expression: '', window: {
      fn: 'rank', partition: ['region'], order: [{ column: 'notional', direction: 'desc' }] } }],
      filter: { kind: 'condition', column: 'desk', operator: 'equal', value: 'Rates' } };
    const q = serialize(row, { level: 1, parent: [] });
    assert.ok(q.indexOf('rank(') < q.indexOf('filter('), q);
    assert.equal(derivedExtend(row.derived[0]!),
      'extend(over(~[region], [~notional->descending()]), ~[rk:{p,w,r|$p->rank($w, $r)}])');
  });

  it('a rename reaches the columns a window names', () => {
    const renamed = renameColumnReferences(CUBE, 'notional', 'amount');
    assert.equal(renamed.groupDerived?.[0]?.window?.column, 'amount');
  });
});
