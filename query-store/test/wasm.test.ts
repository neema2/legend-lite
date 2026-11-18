// The page's own store: legend-lite's saved-query rules compiled to WebAssembly (//query-store:page) over the page's
// records (src/wasm-server.ts), held to the one suite the server is -- and what only it can be asked: another user's
// query over the same records, and a body that is not JSON.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { fileURLToPath } from 'node:url';

import { QueryStoreClient, QueryStoreError } from '../src/client.ts';
import { MemoryRecords, type Records } from '../src/records.ts';
import { LOCAL_API, loadQueriesModule, wasmQueryServer } from '../src/wasm-server.ts';
import { conformance, LITE } from './conformance.ts';
import { runfileDirUrl } from '../../tools/js/runfiles.mts';

const DIR = runfileDirUrl('QUERIES_PAGE');
// Node loads the runtime by file URL and the module by path
const module = await loadQueriesModule(DIR, async (runtimeUrl) => {
  const runtime = await import(runtimeUrl) as { load(src: string, options?: unknown): Promise<unknown> };
  return { load: (src: string, options?: unknown) => runtime.load(fileURLToPath(src), options) };
});
const records = new MemoryRecords();
const server = wasmQueryServer({ load: async () => module, records, user: 'anonymous' });
// lite's dialect alone: the page answers lite's own route (H6, 2026-11-17); legend-engine's is the server's edge's
conformance("the page's own store, legend-lite's rules in WebAssembly", () => ({ api: LOCAL_API, fetch: server.fetch, user: 'anonymous' }), LITE);

describe("the page's own store: what a server here cannot be asked", () => {
  it('only the owner updates or deletes', async () => {
    const shared = new MemoryRecords();
    const mine = new QueryStoreClient(LOCAL_API, wasmQueryServer({ load: async () => module, records: shared, user: 'me' }).fetch);
    const theirs = new QueryStoreClient(LOCAL_API, wasmQueryServer({ load: async () => module, records: shared, user: 'someone' }).fetch);
    const q = await mine.create({
      id: 'owned', name: 'Owned', groupId: 'demo', artifactId: 'trading', versionId: '0.0.0', content: '|1',
    });
    await assert.rejects(theirs.update({ ...q, name: 'Taken' }), (e: unknown) => e instanceof QueryStoreError && e.status === 403
      && e.message === 'only the owner can update it');
    await assert.rejects(theirs.delete('owned'), (e: unknown) => e instanceof QueryStoreError && e.status === 403
      && e.message === 'only the owner can delete it');
    // and "mine only" is the caller's
    assert.deepEqual((await theirs.search({ showCurrentUserQueriesOnly: true })).map((x) => x.id), []);
    assert.deepEqual((await mine.search({ showCurrentUserQueriesOnly: true })).map((x) => x.id), ['owned']);
  });

  // J9's sentences (the edge audit, 2026-11-17): the store's refusals in lite's words on lite's mount
  it("a body that is not JSON: lite's refusal, kind internal, naming what was wrong, as the server answers it", async () => {
    const r = await server.fetch(`${LOCAL_API}/lite/v1/queries`, { method: 'POST', body: '{not json' });
    assert.equal(r.status, 500);
    const said = (await r.json()) as { kind: string; message: string };
    assert.equal(said.kind, 'internal');
    assert.match(said.message, /^the request could not be served: \S/);
  });

  it("lite's user operation: the page's configured user; a request holding a key, or not a POST, refused, kind request", async () => {
    const r = await server.fetch(`${LOCAL_API}/lite/v1/user`, { method: 'POST', body: '{}' });
    assert.deepEqual(await r.json(), { user: 'anonymous' });
    const keyed = await server.fetch(`${LOCAL_API}/lite/v1/user`, { method: 'POST', body: '{"who":"me"}' });
    assert.equal(keyed.status, 400);
    assert.deepEqual(await keyed.json(), { kind: 'request', message: 'a user request has no key who (it holds none)' });
    const got = await server.fetch(`${LOCAL_API}/lite/v1/user`);
    assert.equal(got.status, 400);
    assert.equal(((await got.json()) as { kind: string }).kind, 'request');
  });

  it("legend-engine's path is not the page's: refused by name, kind request (the server's edge answers it)", async () => {
    const r = await server.fetch(`${LOCAL_API}/pure/v1/query`, { method: 'POST', body: '{}' });
    assert.equal(r.status, 404);
    assert.deepEqual(await r.json(), { kind: 'request', message: 'no such legend-lite API: /api/pure/v1/query' });
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
    const r = await wasmQueryServer({ load: async () => module, records: odd, user: 'me' }).fetch(`${LOCAL_API}/lite/v1/queries/x`);
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
