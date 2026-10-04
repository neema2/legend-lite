import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { CubeController, type Planner } from '../src/cube.ts';
import type { Plan, PlanColumn } from '../../engine-client/src/relation-type.ts';
import { PlanThenRun, RemoteRun } from '../src/runner.ts';
import type { ResultTable } from '../../engine-client/src/result.ts';
import type { CubeSnapshot } from '../src/snapshot.ts';
import { TreeState } from '../src/tree.ts';
import type { RemoteExecutor, RemoteResult } from '../../engine-client/src/engine-remote.ts';
import { isStale } from '../src/epoch.ts';
import { TOTAL_ROWS_COLUMN } from '../src/query.ts';
import { FakeEngine } from './fake-engine.ts';
import { fakeParse, fakePrint, limitsOf } from './fake-planner.ts';
import { fromElement, toJson, type Lambda } from '../../pure-protocol/src/index.ts';
import { variable } from '../../pure-protocol/src/index.ts';

/** A cube query, as DataCube builds one. */
const Q = fromElement('demo::T').select(['region']).lambda();

const SNAPSHOT: CubeSnapshot = {
  source: { query: variable('trades') },
  columns: [
    { name: 'region', type: 'String' },
    { name: 'notional', type: 'Float' },
  ],
  derived: [],
  rows: [],
  pivotOn: [],
  measures: [],
  sorts: [],
  epoch: 1,
};

function table(epoch: number): ResultTable {
  return {
    columns: [
      { name: 'region', type: 'String', values: ['EMEA'] },
      { name: 'notional', type: 'Float', values: [1] },
    ],
    rowCount: 1,
    epoch,
    elapsedMs: 0,
  };
}

class LocalEngine extends FakeEngine {
  readonly name = 'stub-duckdb';
  readonly sql: string[] = [];
  async answer(sql: string, epoch: number): Promise<ResultTable> {
    this.sql.push(sql);
    return table(epoch);
  }
}

class StubPlanner implements Planner {
  readonly queries: Lambda[] = [];
  async plan(query: Lambda): Promise<Plan> {
    this.queries.push(query);
    return { sql: 'SELECT 1', columns: [] };
  }
  async relationType(): Promise<PlanColumn[]> {
    return [];
  }
  parse = fakeParse;
  print = fakePrint;
}

class StubExecutor implements RemoteExecutor {
  readonly queries: Lambda[] = [];
  async execute(
    query: Lambda,
    epoch: number,
  ): Promise<RemoteResult> {
    this.queries.push(query);
    return {
      rows: table(epoch),
      sql: 'select region from TRADES -- as the engine reports it',
    };
  }

  async relationType(): Promise<PlanColumn[]> {
    return [];
  }
  parse = fakeParse;
  print = fakePrint;
}

describe('the two arrangements', () => {
  it('plans then executes locally, and says which', async () => {
    const engine = new LocalEngine();
    const planner = new StubPlanner();
    const runner = new PlanThenRun(planner, engine);
    const out = await runner.run(Q, SNAPSHOT);
    assert.deepEqual(planner.queries, [Q]);
    assert.deepEqual(engine.sql, ['SELECT 1']);
    assert.equal(out.sql, 'SELECT 1');
    assert.equal(out.rows.rowCount, 1);
    assert.equal(runner.name, 'plan+stub-duckdb');
  });

  it('asks a remote engine ONCE, and reports the SQL it ran', async () => {
    const executor = new StubExecutor();
    const runner = new RemoteRun(executor);
    const out = await runner.run(Q, SNAPSHOT);
    assert.deepEqual(executor.queries, [Q]);
    // The SQL is the engine's report of what happened, not something
    // this side produced or will run.
    assert.match(out.sql, /as the engine reports it/);
    assert.equal(runner.name, 'engine');
  });
});

/** A state to run: the snapshot, no groups open. */
const at = (snapshot: CubeSnapshot) => ({ snapshot, tree: TreeState.empty() });

describe('a controller on a remote engine', () => {
  const remote = () => {
    const executor = new StubExecutor();
    return {
      executor,
      controller: new CubeController(new RemoteRun(executor)),
    };
  };

  it('runs its queries through the engine, with no local one', async () => {
    const { executor, controller } = remote();
    const views: number[] = [];
    const v = await controller.run(at(SNAPSHOT));
    views.push(isStale(v) ? -1 : v.rows.rowCount);
    assert.deepEqual(views, [1]);
    assert.equal(executor.queries.length, 1);
    assert.match(toJson(executor.queries[0]!), /"_type":"var","name":"trades"/);
    assert.equal(controller.runnerName, 'engine');
  });

  it('runs a HOST query through the same arrangement', async () => {
    // Drill-through asks for rows the cube did not plan. It used to
    // plan and execute by hand in the app, which on this plane would
    // have had no local engine to call at all.
    const { executor, controller } = remote();
    const out = await controller.runQuery(Q, SNAPSHOT);
    assert.equal(out.rows.rowCount, 1);
    assert.deepEqual(executor.queries, [Q]);
  });

  it('REFUSES to snap, and says why', async () => {
    // Freezing a cube means materialising its source into a local
    // store. A remote engine answers queries for us and cannot do
    // that on our behalf; a refusal naming the reason beats a
    // TypeError from a null engine, and beats half-working.
    const { controller } = remote();
    await controller.run(at(SNAPSHOT));
    await assert.rejects(
      () => controller.snap(SNAPSHOT, 'frozen'),
      (e: Error) => {
        assert.match(e.message, /remote engine/);
        assert.match(e.message, /local store/);
        return true;
      },
    );
    assert.equal(controller.snaps.isSnapped, false);
  });
});

describe('Row Limit on a FLAT cube', () => {
  // A flat cube fetched every row whatever General Properties > Row
  // Limit said: only tree levels were capped (2026-09-25 sweep).
  class ManyRows implements RemoteExecutor {
    readonly queries: Lambda[] = [];
    async execute(query: Lambda, epoch: number): Promise<RemoteResult> {
      this.queries.push(query);
      const n = 5;
      // the flat cube's count, asked when the cap cut it (query.ts countLambda)
      if (JSON.stringify(query).includes(TOTAL_ROWS_COLUMN)) {
        return {
          rows: { columns: [{ name: TOTAL_ROWS_COLUMN, type: 'Integer', values: [n] }], rowCount: 1, epoch, elapsedMs: 0 },
          sql: 'select count',
        };
      }
      return {
        rows: {
          columns: [
            { name: 'region', type: 'String', values: Array.from({ length: n }, (_, i) => `R${i}`) },
            { name: 'notional', type: 'Float', values: Array.from({ length: n }, (_, i) => i) },
          ],
          rowCount: n,
          epoch,
          elapsedMs: 0,
        },
        sql: 'select',
      };
    }

    async relationType(): Promise<PlanColumn[]> {
      return [];
    }
    parse = fakeParse;
    print = fakePrint;
  }

  it('asks for one more than the limit, shows the limit, and says so', async () => {
    const executor = new ManyRows();
    const controller = new CubeController(new RemoteRun(executor));
    const v = await controller.run(at({ ...SNAPSHOT, maxRows: 3 }));
    if (isStale(v)) throw new Error('stale');
    const level = executor.queries.find((q) => !JSON.stringify(q).includes(TOTAL_ROWS_COLUMN))!;
    assert.deepEqual(limitsOf(level), [4]);
    assert.equal(v.rows.rowCount, 3);
    assert.equal(v.truncated.length, 1);
    // cut: one count says how many there are in all ("the first 3 of 5")
    assert.equal(v.totalRows, 5);
    assert.equal(executor.queries.filter((q) => JSON.stringify(q).includes(TOTAL_ROWS_COLUMN)).length, 1);
  });

  it('UNSET means no limit, as upstream', async () => {
    const executor = new ManyRows();
    const controller = new CubeController(new RemoteRun(executor));
    const v = await controller.run(at({ ...SNAPSHOT }));
    if (isStale(v)) throw new Error('stale');
    assert.deepEqual(limitsOf(executor.queries.at(-1)!), []);
    assert.equal(v.rows.rowCount, 5);
    assert.equal(v.truncated.length, 0);
  });

  it('reports nothing when the rows fit', async () => {
    const controller = new CubeController(new RemoteRun(new ManyRows()));
    const v = await controller.run(at({ ...SNAPSHOT, maxRows: 10 }));
    if (isStale(v)) throw new Error('stale');
    assert.equal(v.rows.rowCount, 5);
    assert.equal(v.truncated.length, 0);
  });
});
