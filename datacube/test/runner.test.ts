import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { CubeController, type Planner } from '../src/cube.ts';
import { PlanThenRun, RemoteRun } from '../src/runner.ts';
import type { QueryEngine } from '../src/engine.ts';
import type { ResultTable } from '../src/result.ts';
import type { CubeSnapshot } from '../src/snapshot.ts';
import type { RemoteExecutor, RemoteResult } from '../src/engine-remote.ts';
import { isStale } from '../src/epoch.ts';

const SNAPSHOT: CubeSnapshot = {
  source: { expression: '$trades' },
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

class LocalEngine implements QueryEngine {
  readonly name = 'stub-duckdb';
  readonly sql: string[] = [];
  async execute(sql: string, epoch: number): Promise<ResultTable> {
    this.sql.push(sql);
    return table(epoch);
  }
  async close(): Promise<void> {}
}

class StubPlanner implements Planner {
  readonly pure: string[] = [];
  async plan(pureGrammar: string): Promise<string> {
    this.pure.push(pureGrammar);
    return 'SELECT 1';
  }
}

class StubExecutor implements RemoteExecutor {
  readonly pure: string[] = [];
  async execute(
    pureGrammar: string,
    snapshot: CubeSnapshot,
  ): Promise<RemoteResult> {
    this.pure.push(pureGrammar);
    return {
      rows: table(snapshot.epoch),
      sql: 'select region from TRADES -- as the engine reports it',
    };
  }
}

describe('the two arrangements', () => {
  it('plans then executes locally, and says which', async () => {
    const engine = new LocalEngine();
    const planner = new StubPlanner();
    const runner = new PlanThenRun(planner, engine);
    const out = await runner.run('$t->select(~[region])', SNAPSHOT);
    assert.deepEqual(planner.pure, ['$t->select(~[region])']);
    assert.deepEqual(engine.sql, ['SELECT 1']);
    assert.equal(out.sql, 'SELECT 1');
    assert.equal(out.rows.rowCount, 1);
    assert.equal(runner.name, 'plan+stub-duckdb');
  });

  it('asks a remote engine ONCE, and reports the SQL it ran', async () => {
    const executor = new StubExecutor();
    const runner = new RemoteRun(executor);
    const out = await runner.run('$t->select(~[region])', SNAPSHOT);
    assert.deepEqual(executor.pure, ['$t->select(~[region])']);
    // The SQL is the engine's report of what happened, not something
    // this side produced or will run.
    assert.match(out.sql, /as the engine reports it/);
    assert.equal(runner.name, 'engine');
  });
});

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
    await controller.update(SNAPSHOT);
    views.push(controller.view?.rows.rowCount ?? -1);
    assert.deepEqual(views, [1]);
    assert.equal(executor.pure.length, 1);
    assert.match(executor.pure[0] ?? '', /\$trades/);
    assert.equal(controller.runnerName, 'engine');
  });

  it('runs a HOST query through the same arrangement', async () => {
    // Drill-through asks for rows the cube did not plan. It used to
    // plan and execute by hand in the app, which on this plane would
    // have had no local engine to call at all.
    const { executor, controller } = remote();
    const out = await controller.runQuery('$trades->limit(5)', SNAPSHOT);
    assert.equal(out.rows.rowCount, 1);
    assert.deepEqual(executor.pure, ['$trades->limit(5)']);
  });

  it('REFUSES to snap, and says why', async () => {
    // Freezing a cube means materialising its source into a local
    // store. A remote engine answers queries for us and cannot do
    // that on our behalf; a refusal naming the reason beats a
    // TypeError from a null engine, and beats half-working.
    const { controller } = remote();
    await controller.update(SNAPSHOT);
    await assert.rejects(
      () => controller.snap('frozen'),
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
    readonly pure: string[] = [];
    async execute(pureGrammar: string, snapshot: CubeSnapshot): Promise<RemoteResult> {
      this.pure.push(pureGrammar);
      const n = 5;
      return {
        rows: {
          columns: [
            { name: 'region', type: 'String', values: Array.from({ length: n }, (_, i) => `R${i}`) },
            { name: 'notional', type: 'Float', values: Array.from({ length: n }, (_, i) => i) },
          ],
          rowCount: n,
          epoch: snapshot.epoch,
          elapsedMs: 0,
        },
        sql: 'select',
      };
    }
  }

  it('asks for one more than the limit, shows the limit, and says so', async () => {
    const executor = new ManyRows();
    const controller = new CubeController(new RemoteRun(executor));
    const v = await controller.update({ ...SNAPSHOT, maxRows: 3 });
    if (isStale(v)) throw new Error('stale');
    assert.match(executor.pure.at(-1) ?? '', /limit\(4\)/);
    assert.equal(v.rows.rowCount, 3);
    assert.equal(v.truncated.length, 1);
  });

  it('UNSET means no limit, as upstream', async () => {
    const executor = new ManyRows();
    const controller = new CubeController(new RemoteRun(executor));
    const v = await controller.update({ ...SNAPSHOT });
    if (isStale(v)) throw new Error('stale');
    assert.doesNotMatch(executor.pure.at(-1) ?? '', /limit\(/);
    assert.equal(v.rows.rowCount, 5);
    assert.equal(v.truncated.length, 0);
  });

  it('reports nothing when the rows fit', async () => {
    const controller = new CubeController(new RemoteRun(new ManyRows()));
    const v = await controller.update({ ...SNAPSHOT, maxRows: 10 });
    if (isStale(v)) throw new Error('stale');
    assert.equal(v.rows.rowCount, 5);
    assert.equal(v.truncated.length, 0);
  });
});
