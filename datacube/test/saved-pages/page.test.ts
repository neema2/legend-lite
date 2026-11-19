// The page's own store of saved pages: legend-lite's saved-page rules (core's SavedCubes) compiled to WebAssembly on
// the saved queries' module (//query-store:page) over the page's rows (src/cube-store.ts `wasmCubeServer`), held to the
// one suite the server and Python are -- and what only it can be asked: another user's page over the same rows, rows
// the rules cannot read, and two tabs over one store.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { fileURLToPath } from 'node:url';

import { fromJson } from '../../../pure-protocol/src/index.ts';
import { loadQueriesModule } from '../../../query-store/src/wasm-server.ts';
import { runfileDirUrl } from '../../../tools/js/runfiles.mts';
import {
  CubeStoreClient, CubeStoreError, LOCAL_CUBES_API, MemoryRecords, toRow, wasmCubeServer, type CubeRecords,
} from '../../src/cube-store.ts';
import { conformance } from './conformance.ts';

// Node loads the runtime by file URL and the module by path
const module = await loadQueriesModule(runfileDirUrl('QUERIES_PAGE'), async (runtimeUrl) => {
  const runtime = await import(runtimeUrl) as { load(src: string, options?: unknown): Promise<unknown> };
  return { load: (src: string, options?: unknown) => runtime.load(fileURLToPath(src), options) };
});
const load = async () => module;
const server = wasmCubeServer({ load, records: new MemoryRecords(), user: 'this browser' });
conformance("the page's own store, legend-lite's rules in WebAssembly", () => ({ api: LOCAL_CUBES_API, fetch: server.fetch, user: 'this browser' }));

const page = { id: 'p-1', name: 'Mine', content: { kind: 'datacube.page' } };

describe("the page's own saved pages: what a server here cannot be asked", () => {
  it('only the owner updates or deletes', async () => {
    const records = new MemoryRecords();
    const mine = new CubeStoreClient(LOCAL_CUBES_API, wasmCubeServer({ load, records, user: 'me' }).fetch, 'me');
    const theirs = new CubeStoreClient(LOCAL_CUBES_API, wasmCubeServer({ load, records, user: 'you' }).fetch, 'you');
    await mine.create(page);
    await assert.rejects(theirs.update('p-1', { ...page, name: 'Mine now' }),
      (e: unknown) => e instanceof CubeStoreError && e.status === 403
        && e.message === "only its owner, 'me', can update it, and the caller is 'you'");
    await assert.rejects(theirs.delete('p-1'), (e: unknown) => e instanceof CubeStoreError && e.status === 403);
    // and a search answers the caller's own first, then the chosen sort among each's (newest first)
    await new Promise((r) => setTimeout(r, 5));
    await theirs.create({ ...page, id: 'p-2', name: 'Theirs' });
    await new Promise((r) => setTimeout(r, 5));
    await mine.create({ ...page, id: 'p-3', name: 'Mine again' });
    assert.deepEqual((await mine.search({})).map((q) => q.id), ['p-1', 'p-3', 'p-2']);
    assert.deepEqual((await theirs.search({})).map((q) => q.id), ['p-2', 'p-1', 'p-3']);
    assert.deepEqual((await mine.search({ sortByOption: 'SORT_BY_CREATE' })).map((q) => q.id), ['p-3', 'p-1', 'p-2']);
    assert.deepEqual((await theirs.search({ sortByOption: 'SORT_BY_CREATE' })).map((q) => q.id), ['p-2', 'p-3', 'p-1']);
  });

  it('a row that is not a page is left out, as every host leaves it: the others open, and a page saved under its id replaces it', async () => {
    const records = new MemoryRecords();
    await records.call(() => ({
      changed: new Map([
        ['p-1', { id: 'p-1', content: 5 } as unknown as ReturnType<typeof toRow>],
        ['p-2', { id: 'elsewhere', name: 'Moved', content: '{}' } as unknown as ReturnType<typeof toRow>],
        ['p-3', toRow({ id: 'p-3', name: 'Fine', content: { kind: 'datacube.page' }, owner: 'me' })],
      ]),
      answer: undefined,
    }));
    const client = new CubeStoreClient(LOCAL_CUBES_API, wasmCubeServer({ load, records, user: 'me' }).fetch, 'me');
    assert.deepEqual((await client.search({})).map((q) => q.id), ['p-3']);
    await assert.rejects(client.get('p-1'), (e: unknown) => e instanceof CubeStoreError && e.status === 404);
    assert.equal((await client.get('p-3')).name, 'Fine');
    await client.create({ ...page, name: 'Saved over it' });
    assert.equal((await client.get('p-1')).name, 'Saved over it');
  });

  it('keeps upstream\'s fields only: a number outside the content is never kept as a page\'s', async () => {
    const records = new MemoryRecords();
    const client = new CubeStoreClient(LOCAL_CUBES_API, wasmCubeServer({ load, records, user: 'me' }).fetch, 'me');
    const made = await client.create({ ...page, description: 'd', version: 9007199254740993 } as unknown as typeof page);
    assert.deepEqual(Object.keys(made).sort(), ['content', 'createdAt', 'description', 'id', 'lastOpenAt', 'lastUpdatedAt', 'name', 'owner']);
    assert.equal('version' in (await client.get('p-1')), false);
  });

  it('a module that fails (a trap, an exception out of it): the engine 500, naming it, and nothing written', async () => {
    let wrote = 0;
    const counted: CubeRecords = {
      call: async (f) => {
        const said = f(new Map());
        wrote += said.changed.size;
        return said.answer;
      },
    };
    const broken = { exports: { ...module.exports, cubes: () => { throw new Error('illegal cast'); } } } as typeof module;
    const r = await wasmCubeServer({ load: async () => broken, records: counted, user: 'me' })
      .fetch(`${LOCAL_CUBES_API}/lite/v1/queries/dataCube`, { method: 'POST', body: JSON.stringify(page) });
    assert.equal(r.status, 500);
    assert.deepEqual(await r.json(), { kind: 'internal', message: 'Error: illegal cast' });
    assert.equal(wrote, 0);
  });

  it("keeps nothing between calls: another tab's page, written to the same rows, is there at the next call", async () => {
    const records = new MemoryRecords();
    const one = new CubeStoreClient(LOCAL_CUBES_API, wasmCubeServer({ load, records, user: 'me' }).fetch, 'me');
    const two = new CubeStoreClient(LOCAL_CUBES_API, wasmCubeServer({ load, records, user: 'me' }).fetch, 'me');
    await one.create(page);
    assert.equal((await two.get('p-1')).name, 'Mine');
    await two.delete('p-1');
    assert.deepEqual(await one.search({}), []);
  });

  it('a row as it was kept before the rules were Java (its content the exact text) opens as it was', async () => {
    const records = new MemoryRecords();
    await records.call(() => ({
      changed: new Map([['old', toRow({ id: 'old', name: 'Kept', content: { kind: 'datacube.page', dec: 1 }, owner: 'me', createdAt: 1 })]]),
      answer: undefined,
    }));
    const client = new CubeStoreClient(LOCAL_CUBES_API, wasmCubeServer({ load, records, user: 'me' }).fetch, 'me');
    const got = await client.get('old');
    assert.deepEqual([got.name, got.createdAt, got.content], ['Kept', 1, fromJson('{"kind":"datacube.page","dec":1}')]);
  });
});
