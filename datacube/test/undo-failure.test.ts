// An undo that cannot be applied must not LOOK like it was.
//
// Undo mutates the cube and then re-queries. If that query fails --
// the engine is down, the planner rejects it, the network drops -- the
// old code had already moved the snapshot, the tree and the host's
// configuration, and had already taken the step off the stack. The
// result was the worst of every world: the screen still showed the old
// view, the model believed it was somewhere else, the step was spent
// so pressing undo again skipped a state, and redo pointed at
// something that had never been rendered.
//
// So an undo is all or nothing. Either it lands and the screen follows,
// or nothing moved and the step is still there to try again.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { CubeController, type Planner } from '../src/cube.ts';
import type { QueryEngine } from '../src/engine.ts';
import type { ResultTable } from '../src/result.ts';
import type { CubeSnapshot } from '../src/snapshot.ts';

const BASE: CubeSnapshot = {
  source: { expression: '#>{db.T}#' },
  columns: [
    { name: 'region', type: 'String' },
    { name: 'notional', type: 'Float' },
  ],
  derived: [],
  rows: [],
  pivotOn: [],
  measures: [{ name: 'm', column: 'notional', fn: 'sum' }],
  sorts: [],
  epoch: 1,
};

class StubPlanner implements Planner {
  async plan(): Promise<string> {
    return 'SELECT 1';
  }
}

/** Succeeds until `failFrom`, then refuses every query. */
class FlakyEngine implements QueryEngine {
  readonly name = 'flaky';
  calls = 0;
  failFrom = Number.POSITIVE_INFINITY;
  async execute(_sql: string, epoch: number): Promise<ResultTable> {
    this.calls += 1;
    if (this.calls >= this.failFrom) throw new Error('engine is down');
    return {
      columns: [
        { name: 'region', type: 'String', values: ['EMEA'] },
        { name: 'm', type: 'Float', values: [1] },
      ],
      rowCount: 1,
      epoch,
      elapsedMs: 0,
    };
  }
  async close(): Promise<void> {}
}

describe('an undo whose refresh fails', () => {
  async function setUp() {
    const engine = new FlakyEngine();
    const errors: unknown[] = [];
    const hosts: unknown[] = [];
    let host = 'config-A';
    const c = new CubeController(engine, new StubPlanner(), {
      onError: (e) => errors.push(e),
      captureHost: () => host,
      restoreHost: (h) => {
        host = h as string;
        hosts.push(h);
      },
    });
    await c.update({ ...BASE, rows: [] });
    host = 'config-B';
    await c.update({ ...BASE, rows: ['region'] });
    return { c, engine, errors, getHost: () => host };
  }

  it('leaves the cube exactly where it was', async () => {
    const { c, engine } = await setUp();
    engine.failFrom = engine.calls + 1;

    await c.undo().catch(() => {});

    assert.deepEqual(
      c.snapshot?.rows,
      ['region'],
      'the failed undo did not move the cube',
    );
  });

  it('leaves the host configuration where it was', async () => {
    const { c, engine, getHost } = await setUp();
    engine.failFrom = engine.calls + 1;

    await c.undo().catch(() => {});

    assert.equal(getHost(), 'config-B', 'the config was put back');
  });

  it('keeps the step so it can be tried again', async () => {
    const { c, engine } = await setUp();
    assert.equal(c.canUndo, true);
    const before = c.historyDepth;
    engine.failFrom = engine.calls + 1;

    await c.undo().catch(() => {});

    assert.equal(c.canUndo, true, 'the step was not spent');
    assert.deepEqual(c.historyDepth, before, 'and the stacks are unchanged');
  });

  it('does not invent a redo out of a failed undo', async () => {
    const { c, engine } = await setUp();
    engine.failFrom = engine.calls + 1;

    await c.undo().catch(() => {});

    assert.equal(
      c.canRedo,
      false,
      'redo would have pointed at a state never rendered',
    );
  });

  it('still works once the engine comes back', async () => {
    const { c, engine } = await setUp();
    engine.failFrom = engine.calls + 1;
    await c.undo().catch(() => {});

    engine.failFrom = Number.POSITIVE_INFINITY;
    await c.undo();
    assert.deepEqual(c.snapshot?.rows, [], 'the retry landed');
  });

  it('reports the failure rather than swallowing it', async () => {
    const { c, engine, errors } = await setUp();
    engine.failFrom = engine.calls + 1;

    await assert.rejects(() => c.undo());
    assert.ok(errors.length > 0, 'the host heard about it');
  });
});
