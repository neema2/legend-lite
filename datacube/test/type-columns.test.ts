// Step 0 of every refresh: the cube's source and calculated columns typed by
// the COMPILER before any level query (docs/DATACUBE_TYPED_VALUES_DESIGN_2026_09_27.md).

import assert from 'node:assert/strict';
import { before, describe, it } from 'node:test';

import { CubeController } from '../src/cube.ts';
import { typeColumns } from '../src/plan.ts';
import type { PlanColumn } from '../../engine-client/src/relation-type.ts';
import type { QueryRunner } from '../src/runner.ts';
import type { CubeSnapshot } from '../src/snapshot.ts';
import { liteParse, litePrint } from './lite-compiler.ts';
import { accessor } from '../../pure-protocol/src/index.ts';
import { row } from './lite-compiler.ts';
import { TreeState } from '../src/tree.ts';

let CUBE: CubeSnapshot;
before(async () => {
  CUBE = {
    source: { query: accessor('db::DB', 'T') },
    columns: [
      { name: 'region', type: 'String' },
      { name: 'amount', type: 'Float' },
      { name: 'gone', type: 'Integer' },
    ],
    derived: [{ name: 'twice', lambda: await row('$x.amount * 2') }],
    rows: [],
    pivotOn: [],
    measures: [],
    sorts: [],
    epoch: 1,
  };
});

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

/** A compile-only runner: the types it answers (or its refusal), each query it is asked to compile, printed. */
function checking(types: PlanColumn[] | Error, compiled: string[]): QueryRunner {
  return {
    name: 'compiler',
    stream: async () => { throw new Error('a compile streams nothing'); },
    run: async () => { throw new Error('a compile runs nothing'); },
    compile: async (query) => { compiled.push(await litePrint(query)); },
    relationType: async () => { if (types instanceof Error) throw types; return types; },
    parse: liteParse,
    print: litePrint,
  };
}

describe("a compile types the cube first, as a refresh does: it checks the queries the run sends", () => {
  const SOURCE_TYPES: PlanColumn[] = [
    { name: 'region', type: 'String' }, { name: 'amount', type: 'Float' }, { name: 'gone', type: 'Integer' },
  ];
  /** The cube grouped by region with a new draft column, declared a measure as the column editor starts one: untyped. */
  const drafted = async (body: string): Promise<CubeSnapshot> =>
    ({ ...CUBE, rows: ['region'], derived: [{ name: 'big', lambda: await row(body), kind: 'measure' }] });

  it('a numeric draft declared a measure compiles with sum, as its run will send it', async () => {
    const compiled: string[] = [];
    const c = new CubeController(checking([...SOURCE_TYPES, { name: 'big', type: 'Float' }], compiled));
    const out = await c.compile({ snapshot: await drafted('$x.amount * 2'), tree: TreeState.empty() }, null);
    assert.equal(out?.refusal, null);
    assert.ok(compiled.some((q) => /big:x\|\$x\.big:y\|\$y->sum\(\)/.test(q)), compiled.join('\n'));
  });

  it('a boolean draft declared a measure compiles with its unique value, never sum(Boolean)', async () => {
    const compiled: string[] = [];
    const c = new CubeController(checking([...SOURCE_TYPES, { name: 'big', type: 'Boolean' }], compiled));
    const out = await c.compile({ snapshot: await drafted('$x.amount > 50'), tree: TreeState.empty() }, null);
    assert.equal(out?.refusal, null);
    assert.ok(compiled.some((q) => /big:x\|\$x\.big:y\|\$y->uniqueValueOnly\(\)/.test(q)), compiled.join('\n'));
    assert.ok(!compiled.some((q) => /big:x\|\$x\.big:y\|\$y->sum\(\)/.test(q)), compiled.join('\n'));
  });

  it("a typing refusal is the check's answer, with the typing query", async () => {
    const c = new CubeController(checking(new Error("no property 'nope'"), []));
    const out = await c.compile({ snapshot: await drafted('$x.nope'), tree: TreeState.empty() }, null);
    assert.match(out?.refusal ?? '', /no property 'nope'/);
    assert.equal(await litePrint(out!.query!), '|#>{db::DB.T}#->extend(~[big:x|$x.nope])');
  });
});
