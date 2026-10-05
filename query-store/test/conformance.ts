// ONE SUITE, every store: upstream's `/api/pure/v1/query` asked over raw HTTP -- the paths, the
// JSON and its field order, the statuses, the refusals word for word -- and through the client.
// It runs against legend-lite's server (lite.test.ts) and against the page's own store
// (local.test.ts), so the two cannot answer differently without one of them failing here.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { QueryStoreClient, QueryStoreError } from '../src/client.ts';
import type { Query } from '../src/wire.ts';

export interface Target {
  /** The API root, `http://host:port/api`. */
  readonly api: string;
  readonly fetch: typeof fetch;
  /** Who the store says the caller is. */
  readonly user: string;
}

/** The `Query` fields, in the engine's order: every record answered carries exactly these. */
const FIELDS = ['id', 'name', 'description', 'groupId', 'artifactId', 'versionId', 'originalVersionId',
  'executionContext', 'content', 'lastUpdatedAt', 'createdAt', 'lastOpenAt', 'deletedAt', 'validUntil', 'version',
  'taggedValues', 'stereotypes', 'defaultParameterValues', 'owner', 'gridConfig'];

const query = (id: string, over: Partial<Query> = {}): Query => ({
  id,
  name: `Query ${id}`,
  groupId: 'demo',
  artifactId: 'trading',
  versionId: '0.0.0',
  executionContext: { _type: 'explicitExecutionContext', mapping: 'demo::trading::TradingMapping', runtime: 'demo::trading::Runtime' },
  content: '|demo::trading::Trade.all()->project(~[id: x|$x.tradeId])',
  taggedValues: [],
  stereotypes: [],
  defaultParameterValues: [],
  ...over,
});

/** How many suites this process has run: each suite's ids are its own (Bazel workplan P3-16: a count, not the clock
 *  and a random number; every target starts on a fresh store, the page's in memory, legend-lite's in a new
 *  directory, so ids need only differ within a process). */
let suites = 0;

export function conformance(name: string, target: () => Target): void {
  describe(`the query store: ${name}`, () => {
    suites += 1;
    const run = `c${suites}`;
    const id = (n: string): string => `${run}-${n}`;
    const raw = async (method: string, path: string, body?: unknown): Promise<{ status: number; json: unknown; text: string }> => {
      const t = target();
      const r = await t.fetch(`${t.api}/pure/v1/query${path}`, {
        method,
        ...(body === undefined ? {} : { headers: { 'Content-Type': 'application/json' }, body: typeof body === 'string' ? body : JSON.stringify(body) }),
      });
      const text = await r.text();
      let json: unknown = undefined;
      try {
        json = text ? JSON.parse(text) : undefined;
      } catch {
        // not JSON: compared as text
      }
      return { status: r.status, json, text };
    };
    const refused = async (method: string, path: string, body: unknown, status: number, message: string): Promise<void> => {
      const r = await raw(method, path, body);
      assert.equal(r.status, status, r.text);
      assert.equal((r.json as { message?: string }).message, message);
    };
    const client = (): QueryStoreClient => new QueryStoreClient(target().api, target().fetch);

    it('creates a query: version 1, owned by the caller, every field in the engine order', async () => {
      const before = Date.now();
      const r = await raw('POST', '', query(id('a')));
      assert.equal(r.status, 200, r.text);
      const q = r.json as Record<string, unknown>;
      assert.deepEqual(Object.keys(q), FIELDS);
      assert.equal(q['version'], 1);
      assert.equal(q['owner'], target().user);
      assert.equal(q['validUntil'], null);
      assert.equal(q['deletedAt'], null);
      assert.equal(q['createdAt'], q['lastUpdatedAt']);
      assert.ok((q['createdAt'] as number) >= before - 5000);
      assert.equal(q['content'], query(id('a')).content);
    });

    it('refuses the same id twice', async () => {
      await refused('POST', '', query(id('a')), 400, `Query with ID '${id('a')}' already existed`);
    });

    it('refuses an incomplete or invalid query, in the engine words', async () => {
      await refused('POST', '', query(id('b'), { name: '' }), 400, 'Query name is missing or empty');
      await refused('POST', '', query(id('b'), { artifactId: 'Not Valid' }), 400, 'Query project artifact ID is invalid');
      await refused('POST', '', query(id('b'), { groupId: '1bad' }), 400, 'Query project group ID is invalid');
      await refused('POST', '', query(id('b'), { content: '' }), 400, 'Query content is missing or empty');
      await refused('POST', '', query(id('b'), { executionContext: { _type: 'explicitExecutionContext', mapping: 'm::M', runtime: '' } }),
        400, 'Query runtime is missing or empty');
      await refused('POST', '', { ...query(id('b')), executionContext: { _type: 'somethingElse' } }, 400,
        "Query execution context of _type 'somethingElse' is not served by legend-lite (explicitExecutionContext, dataSpaceExecutionContext)");
    });

    it('gets it, marked opened', async () => {
      const r = await raw('GET', `/${encodeURIComponent(id('a'))}`);
      assert.equal(r.status, 200, r.text);
      const q = r.json as Record<string, unknown>;
      assert.deepEqual(Object.keys(q), FIELDS);
      assert.ok((q['lastOpenAt'] as number) >= (q['createdAt'] as number));
      await refused('GET', `/${id('missing')}`, undefined, 404, `Can't find query with ID '${id('missing')}'`);
    });

    it('searches by name, without the heavy fields, and refuses a bad search', async () => {
      await raw('POST', '', query(id('c'), { name: `Zebra ${run}` }));
      const r = await raw('POST', '/search', { searchTermSpecification: { searchTerm: `zebra ${run}` } });
      assert.equal(r.status, 200, r.text);
      const found = r.json as Record<string, unknown>[];
      assert.deepEqual(found.map((q) => q['id']), [id('c')]);
      assert.deepEqual(Object.keys(found[0]!), FIELDS);
      for (const f of ['content', 'executionContext', 'version', 'validUntil', 'taggedValues', 'gridConfig']) assert.equal(found[0]![f], null, f);
      // by id, exactly
      const byId = (await raw('POST', '/search', { searchTermSpecification: { searchTerm: id('a') } })).json as Record<string, unknown>[];
      assert.ok(byId.some((q) => q['id'] === id('a')));
      await refused('POST', '/search', { limit: 0 }, 400, 'Limit should be greater than 0');
      await refused('POST', '/search', { searchTermSpecification: {} }, 500, 'Query search spec expecting a search term');
    });

    it('updates: a new version, the old one its history', async () => {
      const r = await raw('PUT', `/${id('a')}`, query(id('a'), { name: 'Renamed' }));
      assert.equal(r.status, 200, r.text);
      const q = r.json as Record<string, unknown>;
      assert.equal(q['version'], 2);
      assert.equal(q['name'], 'Renamed');
      assert.equal(q['owner'], target().user);
      const h = (await raw('GET', `/${id('a')}/history`)).json as Record<string, unknown>[];
      assert.deepEqual(h.map((v) => [v['version'], v['name']]), [[1, `Query ${id('a')}`]]);
      assert.notEqual(h[0]!['validUntil'], null);
      await refused('PUT', `/${id('a')}`, query(id('other')), 400, 'Updating query ID is not supported');
      await refused('PUT', `/${id('missing')}`, query(id('missing')), 404, `Can't find query with ID '${id('missing')}'`);
    });

    it('patches only the fields it sets', async () => {
      const r = await raw('PUT', `/${id('a')}/patchQuery`, { name: 'Patched', content: null });
      assert.equal(r.status, 200, r.text);
      const q = r.json as Record<string, unknown>;
      assert.equal(q['version'], 3);
      assert.equal(q['name'], 'Patched');
      assert.equal(q['content'], query(id('a')).content);
    });

    it('answers one version of the history, or says it has none', async () => {
      const one = (await raw('GET', `/${id('a')}/history?version=1`)).json as Record<string, unknown>[];
      assert.deepEqual(one.map((v) => v['version']), [1]);
      await refused('GET', `/${id('a')}/history?version=9`, undefined, 404, `Can't find version '9' for query with ID '${id('a')}'`);
      await refused('GET', `/${id('missing')}/history`, undefined, 404, `Can't find query with ID '${id('missing')}'`);
    });

    it('answers a batch, and names what it cannot find', async () => {
      const r = await raw('GET', `/batch?queryIds=${id('a')}&queryIds=${id('c')}`);
      assert.equal(r.status, 200, r.text);
      assert.deepEqual((r.json as Record<string, unknown>[]).map((q) => q['id']), [id('a'), id('c')]);
      await refused('GET', `/batch?queryIds=${id('a')}&queryIds=${id('zz')}&queryIds=${id('yy')}`, undefined, 500,
        `Can't find queries for the following ID(s):\n${id('yy')}\n${id('zz')}`);
    });

    it('deletes: gone from get and search, its versions kept as history', async () => {
      const r = await raw('DELETE', `/${id('c')}`);
      assert.equal(r.status, 204, r.text);
      assert.equal(r.text, '');
      await refused('GET', `/${id('c')}`, undefined, 404, `Can't find query with ID '${id('c')}'`);
      const found = (await raw('POST', '/search', { searchTermSpecification: { searchTerm: `zebra ${run}` } })).json as unknown[];
      assert.deepEqual(found, []);
      const h = (await raw('GET', `/${id('c')}/history`)).json as Record<string, unknown>[];
      assert.equal(h.length, 1);
      assert.notEqual(h[0]!['deletedAt'], null);
      await refused('DELETE', `/${id('c')}`, undefined, 404, `Can't find query with ID '${id('c')}'`);
    });

    it('sorts by last update, newest first', async () => {
      await raw('POST', '', query(id('s1'), { name: `Sort ${run} one` }));
      // the update times are the SERVER's: no clock of this test's reaches it, so the two saves are 5 ms apart
      await new Promise((r) => setTimeout(r, 5));
      await raw('POST', '', query(id('s2'), { name: `Sort ${run} two` }));
      const r = (await raw('POST', '/search', { searchTermSpecification: { searchTerm: `sort ${run}` }, sortByOption: 'SORT_BY_UPDATE' })).json as Record<string, unknown>[];
      assert.deepEqual(r.map((q) => q['id']), [id('s2'), id('s1')]);
    });

    it('says an unknown path is not there', async () => {
      const r = await raw('GET', `/${id('a')}/nothing`);
      assert.equal(r.status, 404);
      assert.equal((r.json as { message?: string }).message, `no such legend-engine API in legend-lite: /api/pure/v1/query/${id('a')}/nothing`);
    });

    it('the client: the same answers, typed, and a refusal as a QueryStoreError', async () => {
      const c = client();
      const made = await c.create(query(id('k')));
      assert.equal(made.version, 1);
      assert.equal((await c.get(id('k'))).id, id('k'));
      assert.equal((await c.update({ ...made, name: 'Again' })).version, 2);
      assert.equal((await c.patch(id('k'), { description: 'said' })).description, 'said');
      assert.deepEqual((await c.history(id('k'))).map((v) => v.version), [1, 2]);
      assert.deepEqual((await c.batch([id('k')])).map((q) => q.id), [id('k')]);
      assert.ok((await c.search({ searchTermSpecification: { searchTerm: id('k') } })).some((q) => q.id === id('k')));
      await c.delete(id('k'));
      await assert.rejects(c.get(id('k')), (e: unknown) => e instanceof QueryStoreError && e.status === 404
        && e.message === `Can't find query with ID '${id('k')}'`);
    });
  });
}
