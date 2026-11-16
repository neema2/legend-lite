// The page's own store: legend-lite's saved-query rules compiled to WebAssembly (//query-store:page) over the page's
// records (src/wasm-server.ts), held to the one suite the server is -- and what only it can be asked: another user's
// query over the same records, and a body that is not JSON.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { fileURLToPath } from 'node:url';

import { QueryStoreClient, QueryStoreError } from '../src/client.ts';
import { BrowserRecords, MemoryRecords, type Records } from '../src/records.ts';
import { LOCAL_API, loadQueriesModule, wasmQueryServer } from '../src/wasm-server.ts';
import { conformance } from './conformance.ts';
import { runfileDirUrl } from '../../tools/js/runfiles.mts';

const DIR = runfileDirUrl('QUERIES_PAGE');
// Node loads the runtime by file URL and the module by path
const module = await loadQueriesModule(DIR, async (runtimeUrl) => {
  const runtime = await import(runtimeUrl) as { load(src: string, options?: unknown): Promise<unknown> };
  return { load: (src: string, options?: unknown) => runtime.load(fileURLToPath(src), options) };
});
const records = new MemoryRecords();
const server = wasmQueryServer({ load: async () => module, records, user: 'anonymous' });
conformance("the page's own store, legend-lite's rules in WebAssembly", () => ({ api: LOCAL_API, fetch: server.fetch, user: 'anonymous' }));

describe('a browser store owned by an embedded app', () => {
  it('closes before a queued IndexedDB open answers and releases its late connection', async () => {
    const request = {} as IDBOpenDBRequest;
    const store = new BrowserRecords({ open: () => request } as unknown as IDBFactory);
    const refused = assert.rejects(store.call(() => { throw new Error('must not read'); }), /closed/);
    await store.close();
    await refused;
    let closes = 0;
    Object.defineProperty(request, 'result', { value: { close: () => { closes++; } } });
    request.onsuccess!.call(request, new Event('success'));
    assert.equal(closes, 1);
    await assert.rejects(store.call(() => { throw new Error('must not read'); }), /closed/);
  });

  it('releases a late connection after refusing an upgrade blocked by another tab', async () => {
    const request = {} as IDBOpenDBRequest;
    const store = new BrowserRecords({ open: () => request } as unknown as IDBFactory);
    const refused = assert.rejects(store.call(() => { throw new Error('must not read'); }), /another tab/);
    request.onblocked!.call(request, new Event('blocked') as IDBVersionChangeEvent);
    await refused;
    let closes = 0;
    Object.defineProperty(request, 'result', { value: { close: () => { closes++; } } });
    request.onsuccess!.call(request, new Event('success'));
    assert.equal(closes, 1);
    await store.close();
  });
});

describe("the page's own store: what a server here cannot be asked", () => {
  it('only the owner updates or deletes', async () => {
    const shared = new MemoryRecords();
    const mine = new QueryStoreClient(LOCAL_API, wasmQueryServer({ load: async () => module, records: shared, user: 'me' }).fetch);
    const theirs = new QueryStoreClient(LOCAL_API, wasmQueryServer({ load: async () => module, records: shared, user: 'someone' }).fetch);
    const q = await mine.create({
      id: 'owned', name: 'Owned', groupId: 'demo', artifactId: 'trading', versionId: '0.0.0', content: '|1',
    });
    await assert.rejects(theirs.update({ ...q, name: 'Taken' }), (e: unknown) => e instanceof QueryStoreError && e.status === 403
      && e.message === 'Only owner can update the query');
    await assert.rejects(theirs.delete('owned'), (e: unknown) => e instanceof QueryStoreError && e.status === 403
      && e.message === 'Only owner can delete the query');
    // and "mine only" is the caller's
    assert.deepEqual((await theirs.search({ showCurrentUserQueriesOnly: true })).map((x) => x.id), []);
    assert.deepEqual((await mine.search({ showCurrentUserQueriesOnly: true })).map((x) => x.id), ['owned']);
  });

  it('a body that is not JSON: the engine 500, naming the error, as the server answers it', async () => {
    const r = await server.fetch(`${LOCAL_API}/pure/v1/query`, { method: 'POST', body: '{not json' });
    assert.equal(r.status, 500);
    assert.match(((await r.json()) as { message: string }).message, /^\w+(Exception|Error): /);
  });

  it('records it cannot read: the engine 500, naming them, and nothing written', async () => {
    let wrote = 0;
    const odd: Records = {
      call: async (f) => {
        const said = f(new Map([['x', '5']]));
        wrote += said.changed.size;
        return said.answer;
      },
    };
    const r = await wasmQueryServer({ load: async () => module, records: odd, user: 'me' }).fetch(`${LOCAL_API}/pure/v1/query/x`);
    assert.equal(r.status, 500);
    assert.match(((await r.json()) as { message: string }).message, /the records of query 'x' are not a list of versions/);
    assert.equal(wrote, 0);
  });

  it('a query whose id names a property every object has (`__proto__`) is kept like any other', async () => {
    const c = new QueryStoreClient(LOCAL_API, wasmQueryServer({ load: async () => module, records: new MemoryRecords(), user: 'me' }).fetch);
    const made = await c.create({ id: '__proto__', name: 'Proto', groupId: 'demo', artifactId: 'trading', versionId: '0.0.0', content: '|1' });
    assert.equal((await c.get('__proto__')).name, 'Proto');
    assert.equal((await c.update({ ...made, name: 'Again' })).version, 2);
    assert.deepEqual((await c.history('__proto__')).map((v) => v.version), [1]);
  });

  it('keeps nothing between calls: another tab\'s query, written to the same records, is there at the next call', async () => {
    const shared = new MemoryRecords();
    const one = new QueryStoreClient(LOCAL_API, wasmQueryServer({ load: async () => module, records: shared, user: 'me' }).fetch);
    const two = new QueryStoreClient(LOCAL_API, wasmQueryServer({ load: async () => module, records: shared, user: 'me' }).fetch);
    await one.create({ id: 'from-one', name: 'One', groupId: 'demo', artifactId: 'trading', versionId: '0.0.0', content: '|1' });
    assert.equal((await two.get('from-one')).name, 'One');
  });
});
