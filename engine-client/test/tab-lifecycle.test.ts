// An embedded app owns its tab database: failure, unmount and repeated teardown must release it.
import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { openedInTab } from '../src/doors/tab-duckdb.ts';
import { connectDuckDbInTab, type TabDatabase } from '../src/duckdb-tab.ts';
import { DuckDbEngine, type ArrowishConnection, type ArrowishPrepared } from '../src/duckdb.ts';
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
    }, async () => { throw new Error('must not connect'); }, async (db) => { released.push(db); }, () => undefined);
    const refused = assert.rejects(opened, (error) => error === failure);
    loaded('database');
    await refused;
    assert.deepEqual(released, ['database']);
  });

  it('releases a database whose session setup failed', async () => {
    const failure = new Error('session setup failed');
    let released = 0;
    await assert.rejects(openedInTab(async () => 'database', 'https://example.test/assets', planner,
      async () => { throw failure; }, async () => { released++; }, () => undefined), (error) => error === failure);
    assert.equal(released, 1);
  });

  it('cancels an unfinished database load when a planning request fails', async () => {
    const failure = new Error('setup cannot be planned');
    let cancel!: (reason: unknown) => void;
    const loading = new Promise<never>((_, reject) => { cancel = reject; });
    await assert.rejects(openedInTab(() => loading, 'https://example.test/assets', {
      ...planner, sessionSetup: async () => { throw failure; },
    }, async () => { throw new Error('must not connect'); }, async () => { throw new Error('no database acquired'); },
    cancel), (error) => error === failure);
  });

  it('aborts session setup even when terminating the driver does not reject its pending request', async () => {
    const lifetime = new AbortController();
    let started!: () => void;
    const settingUp = new Promise<void>((resolve) => { started = resolve; });
    let terminated = 0;
    const opened = openedInTab(async () => 'database', 'https://example.test/assets', planner,
      () => { started(); return new Promise<never>(() => undefined); },
      async () => { terminated++; }, (reason) => lifetime.abort(reason), lifetime.signal);
    const refused = assert.rejects(opened, (error) => error instanceof DOMException && error.name === 'AbortError');
    await settingUp;
    lifetime.abort();
    await refused;
    assert.equal(terminated, 1);
  });

  it('does not connect after cancellation when a delayed planner finally answers', async () => {
    const lifetime = new AbortController();
    let planned!: (statements: []) => void;
    const pending = new Promise<[]>((resolve) => { planned = resolve; });
    let connects = 0;
    const opened = openedInTab(async () => 'database', 'https://example.test/assets', {
      ...planner, sessionSetup: () => pending,
    }, async () => { connects++; }, async () => undefined, (reason) => lifetime.abort(reason), lifetime.signal);
    const refused = assert.rejects(opened, (error) => error instanceof DOMException && error.name === 'AbortError');
    lifetime.abort();
    await refused;
    planned([]);
    await new Promise<void>((resolve) => setImmediate(resolve));
    assert.equal(connects, 0);
  });

  it('destroys the owned database once and refuses further work on every connection', async () => {
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
    assert.equal(closes, 1, 'the project closed its connection; the worker owns the remaining native resources');
    assert.equal(terminated, 1);
    await assert.rejects(tab.connect(), /disposed/);
    await assert.rejects(tab.db.registerFileText('late.csv', 'value\n1'), /disposed/);
    await assert.rejects(tab.engine.run(unplanned('SELECT 1'), 0), /disposed/);
  });

  it('rejects a connection still being opened without waiting for the destroyed driver', async () => {
    const order: string[] = [];
    let count = 0;
    const database: TabDatabase = {
      db: { connect: () => ++count === 1
        ? Promise.resolve(connection(async () => { order.push('main closed'); }))
        : new Promise<ArrowishConnection>(() => undefined),
      } as unknown as TabDatabase['db'],
      dispose: async () => { order.push('terminated'); },
    };
    const tab = await connectDuckDbInTab(database, setup);
    const project = tab.connect();
    const rejected = assert.rejects(project, /disposed/);
    const disposed = tab.dispose();
    await Promise.all([disposed, rejected]);
    assert.deepEqual(order, ['terminated']);
  });

  it('reports worker termination failures to its owner', async () => {
    const failure = new Error('worker termination failed');
    const tab = await connectDuckDbInTab({
      db: { connect: async () => connection(async () => undefined) } as unknown as TabDatabase['db'],
      dispose: async () => { throw failure; },
    }, setup);
    await assert.rejects(tab.dispose(), (error) => error === failure);
  });

  it('rejects active queries and file registrations before a held request answers', async () => {
    let started!: () => void;
    const running = new Promise<void>((resolve) => { started = resolve; });
    let terminated = false;
    const tab = await connectDuckDbInTab({
      db: {
        connect: async () => ({ query: () => { started(); return new Promise<never>(() => undefined); } }),
        registerFileBuffer: () => new Promise<never>(() => undefined),
      } as unknown as TabDatabase['db'],
      dispose: async () => { terminated = true; },
    }, setup);
    await inRun('the lifecycle test', async () => {
      const query = assert.rejects(tab.engine.run(unplanned('SELECT 1'), 0), /disposed/);
      const file = assert.rejects(tab.data.registerFileBuffer('held.csv', new Uint8Array()), /disposed/);
      await running;
      await tab.dispose();
      await Promise.all([query, file]);
      assert.ok(terminated);
    });
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

  it('does not send a prepared query that finishes preparing after disposal', async () => {
    const lifetime = new AbortController();
    let started!: () => void;
    const preparing = new Promise<void>((resolve) => { started = resolve; });
    let prepared!: (statement: ArrowishPrepared) => void;
    const pending = new Promise<ArrowishPrepared>((resolve) => { prepared = resolve; });
    let sends = 0;
    const engine = new DuckDbEngine({
      ...connection(async () => undefined),
      prepare: () => { started(); return pending; },
    }, { lifetime: lifetime.signal });
    await inRun('the lifecycle test', async () => {
      const refused = assert.rejects(engine.exclusive((held) => held.rows(unplanned('SELECT ?'), ['1'])),
        (error) => error instanceof DOMException && error.name === 'AbortError');
      await preparing;
      lifetime.abort();
      await refused;
      prepared({ query: () => { sends++; throw new Error('must not send'); } });
      await new Promise<void>((resolve) => setImmediate(resolve));
      assert.equal(sends, 0);
    });
  });
});
