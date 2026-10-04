// Step 0 of every refresh: the cube's source and calculated columns typed by
// the COMPILER before any level query (docs/DATACUBE_TYPED_VALUES_DESIGN_2026_09_27.md).

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { typeColumns } from '../src/plan.ts';
import type { PlanColumn } from '../../engine-client/src/relation-type.ts';
import type { QueryRunner } from '../src/runner.ts';
import type { CubeSnapshot } from '../src/snapshot.ts';
import { liteParse, litePrint } from './lite-compiler.ts';
import { accessor } from '../../pure-protocol/src/index.ts';
import { row } from './lite-compiler.ts';

const CUBE: CubeSnapshot = {
  source: { query: accessor('db::DB', 'T') },
  columns: [
    { name: 'region', type: 'String' },
    { name: 'amount', type: 'Float' },
    { name: 'gone', type: 'Integer' },
  ],
  derived: [{ name: 'twice', lambda: row('$x.amount * 2') }],
  rows: [],
  pivotOn: [],
  measures: [],
  sorts: [],
  epoch: 1,
};

function compiler(answer: PlanColumn[], asked: string[]): QueryRunner {
  return {
    name: 'compiler',
    stream: async () => { throw new Error('this test streams nothing'); },
    run: async () => { throw new Error('step 0 runs nothing'); },
    compile: async () => undefined,
    relationType: async (query) => { asked.push(await litePrint(query)); return answer; },
    parse: liteParse,
    print: litePrint,
  };
}

describe('typeColumns: the compiler types the cube before its first query', () => {
  it('asks once, for the source with its calculated columns, and adopts the answer', async () => {
    const asked: string[] = [];
    const out = await typeColumns(CUBE, compiler([
      { name: 'region', type: 'String' },
      { name: 'amount', type: 'Decimal' },
      { name: 'gone', type: 'Integer' },
      { name: 'twice', type: 'Decimal' },
    ], asked));
    assert.deepEqual(asked, ['|#>{db::DB.T}#->extend(~[twice:x|$x.amount * 2])']);
    assert.equal(out.snapshot.derived[0]?.type, 'Decimal', 'typed before any query');
    assert.equal(out.snapshot.columns.find((c) => c.name === 'amount')?.type, 'Decimal');
    // S1c: a declared type the compiler no longer gives is a schema change, said
    assert.deepEqual(out.changes, [{ column: 'amount', was: 'Float', now: 'Decimal' }]);
  });

  it('reports a column the source no longer has', async () => {
    const out = await typeColumns(CUBE, compiler([
      { name: 'region', type: 'String' },
      { name: 'amount', type: 'Float' },
      { name: 'twice', type: 'Float' },
    ], []));
    assert.deepEqual(out.changes, [{ column: 'gone', was: 'Integer', now: null }]);
  });

  it('changes nothing when the compiler agrees', async () => {
    const out = await typeColumns({ ...CUBE, derived: [] }, compiler([
      { name: 'region', type: 'String' },
      { name: 'amount', type: 'Float' },
      { name: 'gone', type: 'Integer' },
    ], []));
    assert.deepEqual(out.changes, []);
    assert.equal(out.snapshot.columns, CUBE.columns);
  });
});
