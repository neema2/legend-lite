// Child-group aggregates: on each group row, an aggregate of its child
// groups' figures (the smallest desk total under a region), from each
// level's own query, placed by group key.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import type { ResultTable } from '../src/result.ts';
import type { QueryRunner } from '../src/runner.ts';
import { childAggregateQuery, serialize } from '../src/serialize.ts';
import { renameColumnReferences, totalOrderSorts, type CubeSnapshot } from '../src/snapshot.ts';
import { withChildAggregates } from '../src/treeview.ts';

const CUBE: CubeSnapshot = {
  source: { expression: 't' },
  columns: [
    { name: 'region', type: 'String' }, { name: 'desk', type: 'String' },
    { name: 'notional', type: 'Float' }, { name: 'pnl', type: 'Float' },
  ],
  derived: [],
  rows: ['region', 'desk'],
  pivotOn: [],
  measures: [{ name: 'notional', column: 'notional', fn: 'sum' }],
  sorts: [],
  epoch: 1,
  groupDerived: [{ name: 'weakest', expression: '', childAggregate: { fn: 'min', of: 'notional' } }],
};

describe('the child-group query', () => {
  it('groups one level deeper, then aggregates per group', () => {
    const q = childAggregateQuery(CUBE, { level: 1, parent: [] });
    assert.equal(q?.pure,
      "t->select(~[region, desk, notional])->groupBy(~[region, desk], ~[notional:x|$x.notional:y|$y->sum()])"
      + '->sort([~region->ascending(), ~desk->ascending()])'
      + '->groupBy(~[region], ~[weakest:x|$x.notional:y|$y->min()])');
    assert.deepEqual(q?.columns, ['weakest']);
  });

  it('the grand total groups by its root; the deepest level reads the source rows', () => {
    assert.match(childAggregateQuery(CUBE, { level: 0, parent: [] })?.pure ?? '',
      /groupBy\(~\[region\], .*\)->sort\(\[~region->ascending\(\)\]\)->extend\(~\[__root__: x\|'\[ROOT\]'\]\)->groupBy\(~\[__root__\], ~\[weakest:x\|\$x\.notional:y\|\$y->min\(\)\]\)/);
    const deepest = childAggregateQuery(CUBE, { level: 2, parent: ['EMEA'] })?.pure ?? '';
    assert.match(deepest, /extend\(~\[__child_weakest: x\|\$x\.notional\]\)/);
    assert.match(deepest, /groupBy\(~\[region, desk\], ~\[notional:.*weakest:x\|\$x\.__child_weakest:y\|\$y->min\(\)\]\)/);
    assert.match(deepest, /filter\(x\|\$x\.region == 'EMEA'\)/);
  });

  it('none on a flat or pivoted cube, or without such a column', () => {
    assert.equal(childAggregateQuery({ ...CUBE, rows: [] }, { level: 0, parent: [] }), null);
    assert.equal(childAggregateQuery({ ...CUBE, pivotOn: ['desk'] }, { level: 1, parent: [] }), null);
    assert.equal(childAggregateQuery({ ...CUBE, groupDerived: [] }, { level: 1, parent: [] }), null);
  });

  it('the level\'s own query neither computes it nor sorts by it', () => {
    const sorted = { ...CUBE, sorts: [{ column: 'weakest', direction: 'desc' as const }] };
    assert.doesNotMatch(serialize(sorted, { level: 1, parent: [] }), /weakest/);
    assert.deepEqual(totalOrderSorts(sorted, ['region']).map((x) => x.column), ['region']);
  });

  it('a rename reaches the measure it reads', () => {
    assert.equal(renameColumnReferences(CUBE, 'notional', 'amount').groupDerived?.[0]?.childAggregate?.of, 'amount');
  });
});

describe('placing the figures', () => {
  const table = (cols: Record<string, (string | number | null)[]>): ResultTable => {
    const e = Object.entries(cols);
    return { columns: e.map(([name, values]) => ({ name, type: 'String', values })), rowCount: e[0]?.[1].length ?? 0, epoch: 1, elapsedMs: 0 };
  };

  it('each group row gets its own figure, by key, whatever order the answer came in', async () => {
    const sent: string[] = [];
    const runner: QueryRunner = {
      name: 'stub',
      async run(pure: string) {
        sent.push(pure);
        return { rows: table({ region: ['EMEA', 'AMER'], weakest: [300, 50] }), sql: '' };
      },
    } as unknown as QueryRunner;
    const level = table({ region: ['AMER', 'APAC', 'EMEA'], notional: [1200, 900, 1700] });
    const out = await withChildAggregates(CUBE, { level: 1, parent: [] }, level,
      [['AMER'], ['APAC'], ['EMEA']], { runner, snapshot: CUBE });
    assert.equal(sent.length, 1);
    assert.deepEqual(out.columns.find((c) => c.name === 'weakest')?.values, [50, null, 300]);
  });
});
