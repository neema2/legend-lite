// An embedded app owns its tab database: failure, unmount and repeated teardown must release it.
import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { openedInTab } from '../src/doors/tab-duckdb.ts';
import { connectDuckDbInTab, type TabDatabase } from '../src/duckdb-tab.ts';
import { DuckDbEngine, type ArrowishConnection } from '../src/duckdb.ts';
import { inRun } from '../src/run-frame.ts';
import { unplanned } from './unplanned.ts';

const planner = { hostOperations: async () => [], sessionSetup: async () => [] };
const setup = { extensions: [], session: [] };
const connection = (close: () => Promise<void>): ArrowishConnection => ({
  query: () => { throw new Error('this test plans no statements'); }, close,
});

describe('an embedded tab owns its database lifetime', () => {
  it('releases a database that finishes loading after its planner failed', async () => {
    const failure = new Error('planner unavailable');
    let loaded!: (db: string) => void;
    const start = new Promise<string>((resolve) => { loaded = resolve; });
    const released: string[] = [];
    const opened = openedInTab(() => start, 'https://example.test/assets', {
      ...planner, hostOperations: async () => { throw failure; },
    }, async () => { throw new Error('must not connect'); }, async (db) => { released.push(db); });
    const refused = assert.rejects(opened, (error) => error === failure);
    loaded('database');
    await refused;
    assert.deepEqual(released, ['database']);
  });

  it('releases a database whose session setup failed', async () => {
    const failure = new Error('session setup failed');
    let released = 0;
    await assert.rejects(openedInTab(async () => 'database', 'https://example.test/assets', planner,
      async () => { throw failure; }, async () => { released++; }), (error) => error === failure);
    assert.equal(released, 1);
  });

  it('closes every connection once, including one already closed by its project', async () => {
    let closes = 0;
    let terminated = 0;
    const database: TabDatabase = {
      db: { connect: async () => connection(async () => { closes++; }) } as unknown as TabDatabase['db'],
      dispose: async () => { terminated++; },
    };
    const tab = await connectDuckDbInTab(database, setup);
    const project = await tab.connect();
    await project.close();
    await Promise.all([tab.dispose(), tab.dispose()]);
    assert.equal(closes, 2);
    assert.equal(terminated, 1);
    await assert.rejects(tab.connect(), /disposed/);
    assert.throws(() => tab.db.registerFileText('late.csv', 'value\n1'), /disposed/);
  });

  it('waits for a connection being opened and closes it before terminating', async () => {
    const order: string[] = [];
    let resolve!: (value: ArrowishConnection) => void;
    let count = 0;
    const database: TabDatabase = {
      db: { connect: () => ++count === 1
        ? Promise.resolve(connection(async () => { order.push('main closed'); }))
        : new Promise<ArrowishConnection>((ready) => { resolve = ready; }),
      } as unknown as TabDatabase['db'],
      dispose: async () => { order.push('terminated'); },
    };
    const tab = await connectDuckDbInTab(database, setup);
    const project = tab.connect();
    const rejected = assert.rejects(project, /disposed/);
    const disposed = tab.dispose();
    resolve(connection(async () => { order.push('project closed'); }));
    await Promise.all([disposed, rejected]);
    assert.deepEqual(order, ['project closed', 'main closed', 'terminated']);
  });

  it('terminates the worker even when a connection cannot close', async () => {
    const failure = new Error('close failed');
    let terminated = false;
    const tab = await connectDuckDbInTab({
      db: { connect: async () => connection(async () => { throw failure; }) } as unknown as TabDatabase['db'],
      dispose: async () => { terminated = true; },
    }, setup);
    await assert.rejects(tab.dispose(), (error) => error instanceof AggregateError && error.errors.includes(failure));
    assert.ok(terminated);
  });

  it('a connection finishes an active query before close and refuses further work', async () => {
    let finish!: () => void;
    let closed = false;
    const pending = new Promise<void>((resolve) => { finish = resolve; });
    const engine = new DuckDbEngine({
      query: async () => { await pending; return { schema: { fields: [] }, numRows: 0, getChildAt: () => null }; },
      close: async () => { closed = true; },
    });
    await inRun('the lifecycle test', async () => {
      const run = engine.run(unplanned('SELECT 1'), 0);
      const closing = engine.close();
      assert.equal(closed, false);
      await assert.rejects(engine.run(unplanned('SELECT 2'), 0), /closed/);
      finish();
      await Promise.all([run, closing]);
      assert.ok(closed);
    });
  });
});
