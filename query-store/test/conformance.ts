// ONE SUITE, every store: the saved queries asked over raw HTTP -- the paths, the JSON and its field order, the
// statuses, the refusals word for word -- and through the client. Two dialects of the one store (H6, lite's API v1,
// 2026-11-17): LITE, lite's own `/api/lite/v1/queries` in lite's words and refusal shape (`{kind, message}`), which the
// client asks; and ENGINE, upstream's `/api/pure/v1/query` in legend-engine's words, which the server's edge alone
// answers, byte-exact. It runs against legend-lite's server in both (lite.test.ts) and against the page's own store in
// lite's (wasm.test.ts), so no two of them can answer differently without one failing here.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { QueryStoreClient, QueryStoreError } from '../src/client.ts';
import type { Query } from '../src/wire.ts';

/** A dialect of the store: where it answers under the API root, and its words for each refusal of a kind. */
export interface Dialect {
  readonly name: string;
  readonly path: string;
  queryNotFound(id: string): string;
  queriesNotFound(ids: string): string;
  versionNotFound(version: string, id: string): string;
  noRoute(rest: string): string;
  // the store's other refusals (J9 of the edge audit, 2026-11-17: each a kind, lite's sentence on lite's mount,
  // legend-engine's the edge's; the TS host, 2026-11-18, once J9's re-cut reached the merge point)
  idTaken(id: string): string;
  /** A required field missing or empty: lite names it in its words, legend-engine by its own. */
  missing(field: 'name' | 'content' | 'runtime'): string;
  invalidArtifactId: string;
  invalidGroupId: string;
  unservedContext(type: string, served: string): string;
  limitNotPositive: string;
  noSearchTerm: string;
  idChanged: string;
  /** A request the store could not serve (a 500): its message, naming what was wrong. */
  malformed(message: string): string;
  /** A refusal's body beyond its message: lite's kind, by its status; legend-engine's envelope at 500. */
  refusal(status: number, message: string): unknown;
}

/** Lite's own (spec §2, §3): `/api/lite/v1/queries`, SavedQueries' words, `{kind, message}` (a saved query or version
 *  that does not exist 404, kind notFound: the user, 2026-11-17 ~23:31; any other refusal under 500 kind request). */
export const LITE: Dialect = {
  name: "lite's /api/lite/v1/queries",
  path: '/lite/v1/queries',
  queryNotFound: (id) => `no saved query has the ID '${id}'`,
  queriesNotFound: (ids) => `no saved query has the ID(s):\n${ids}`,
  versionNotFound: (version, id) => `the saved query '${id}' has no version '${version}'`,
  noRoute: (rest) => `no such legend-lite API: /api/lite/v1/queries${rest}`,
  idTaken: (id) => `the ID '${id}' is already taken`,
  missing: (field) => `the ${field === 'runtime' ? "execution context's runtime" : field} is missing or empty`,
  invalidArtifactId: 'the project artifact ID is invalid',
  invalidGroupId: 'the project group ID is invalid',
  unservedContext: (type, served) => `the execution context of _type '${type}' is not served (${served})`,
  limitNotPositive: 'the limit must be greater than 0',
  noSearchTerm: 'the search specification has no search term',
  idChanged: 'the ID cannot be changed',
  malformed: (message) => `the request could not be served: ${message}`,
  refusal: (status, message) => ({ kind: status >= 500 ? 'internal' : 'request', message }),
};

/** legend-engine's (the edge's mount, EdgeServer: StoreWords), upstream's path and words, byte-exact. */
export const ENGINE: Dialect = {
  name: "legend-engine's /api/pure/v1/query (the edge's)",
  path: '/pure/v1/query',
  queryNotFound: (id) => `Can't find query with ID '${id}'`,
  queriesNotFound: (ids) => `Can't find queries for the following ID(s):\n${ids}`,
  versionNotFound: (version, id) => `Can't find version '${version}' for query with ID '${id}'`,
  noRoute: (rest) => `no such legend-engine API in legend-lite: /api/pure/v1/query${rest}`,
  idTaken: (id) => `Query with ID '${id}' already existed`,
  missing: (field) => `Query ${field} is missing or empty`,
  invalidArtifactId: 'Query project artifact ID is invalid',
  invalidGroupId: 'Query project group ID is invalid',
  unservedContext: (type, served) => `Query execution context of _type '${type}' is not served by legend-lite (${served})`,
  limitNotPositive: 'Limit should be greater than 0',
  noSearchTerm: 'Query search spec expecting a search term',
  idChanged: 'Updating query ID is not supported',
  malformed: (message) => `IllegalArgumentException: ${message}`,
  refusal: (status, message) => (status >= 500 ? { code: -1, message, status: 'error' } : { message }),
};

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

export function conformance(name: string, target: () => Target, d: Dialect = LITE): void {
  describe(`the query store: ${name}, ${d.name}`, () => {
    suites += 1;
    const run = `c${suites}`;
    const id = (n: string): string => `${run}-${n}`;
    const raw = async (method: string, path: string, body?: unknown): Promise<{ status: number; json: unknown; text: string }> => {
      const t = target();
      const r = await t.fetch(`${t.api}${d.path}${path}`, {
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
    // `missing`: a record that does not exist, lite's kind notFound (the user, 2026-11-17 ~23:31); the kind is the
    // store's, never read from the status (an unknown path is 404 and kind request)
    const refused = async (method: string, path: string, body: unknown, status: number, message: string,
      missing = false): Promise<void> => {
      const r = await raw(method, path, body);
      assert.equal(r.status, status, r.text);
      assert.equal((r.json as { message?: string }).message, message);
      if (d === LITE) {
        assert.equal((r.json as { kind?: string }).kind, missing ? 'notFound' : status >= 500 ? 'internal' : 'request', r.text);
      }
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
      await refused('POST', '', query(id('a')), 400, d.idTaken(id('a')));
    });

    it('refuses an incomplete or invalid query, in the engine words', async () => {
      await refused('POST', '', query(id('b'), { name: '' }), 400, d.missing('name'));
      await refused('POST', '', query(id('b'), { artifactId: 'Not Valid' }), 400, d.invalidArtifactId);
      await refused('POST', '', query(id('b'), { groupId: '1bad' }), 400, d.invalidGroupId);
      await refused('POST', '', query(id('b'), { content: '' }), 400, d.missing('content'));
      await refused('POST', '', query(id('b'), { executionContext: { _type: 'explicitExecutionContext', mapping: 'm::M', runtime: '' } }),
        400, d.missing('runtime'));
      await refused('POST', '', { ...query(id('b')), executionContext: { _type: 'somethingElse' } }, 400,
        d.unservedContext('somethingElse', 'explicitExecutionContext, dataSpaceExecutionContext'));
    });

    it('gets it, marked opened', async () => {
      const r = await raw('GET', `/${encodeURIComponent(id('a'))}`);
      assert.equal(r.status, 200, r.text);
      const q = r.json as Record<string, unknown>;
      assert.deepEqual(Object.keys(q), FIELDS);
      assert.ok((q['lastOpenAt'] as number) >= (q['createdAt'] as number));
      await refused('GET', `/${id('missing')}`, undefined, 404, d.queryNotFound(id('missing')), true);
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
      await refused('POST', '/search', { limit: 0 }, 400, d.limitNotPositive);
      await refused('POST', '/search', { searchTermSpecification: {} }, 500, d.noSearchTerm);
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
      await refused('PUT', `/${id('a')}`, query(id('other')), 400, d.idChanged);
      await refused('PUT', `/${id('missing')}`, query(id('missing')), 404, d.queryNotFound(id('missing')), true);
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
      await refused('GET', `/${id('a')}/history?version=9`, undefined, 404, d.versionNotFound('9', id('a')), true);
      await refused('GET', `/${id('missing')}/history`, undefined, 404, d.queryNotFound(id('missing')), true);
    });

    it('answers a batch, and names what it cannot find', async () => {
      const r = await raw('GET', `/batch?queryIds=${id('a')}&queryIds=${id('c')}`);
      assert.equal(r.status, 200, r.text);
      assert.deepEqual((r.json as Record<string, unknown>[]).map((q) => q['id']), [id('a'), id('c')]);
      // a batch with missing IDs names them: on lite's mount kind notFound, 404 (the orchestrator's ruling,
      // 2026-11-17); upstream's 500 the edge's alone
      await refused('GET', `/batch?queryIds=${id('a')}&queryIds=${id('zz')}&queryIds=${id('yy')}`, undefined,
        d === LITE ? 404 : 500, d.queriesNotFound(`${id('yy')}\n${id('zz')}`), d === LITE);
    });

    it('deletes: gone from get and search, its versions kept as history', async () => {
      const r = await raw('DELETE', `/${id('c')}`);
      assert.equal(r.status, 204, r.text);
      assert.equal(r.text, '');
      await refused('GET', `/${id('c')}`, undefined, 404, d.queryNotFound(id('c')), true);
      const found = (await raw('POST', '/search', { searchTermSpecification: { searchTerm: `zebra ${run}` } })).json as unknown[];
      assert.deepEqual(found, []);
      const h = (await raw('GET', `/${id('c')}/history`)).json as Record<string, unknown>[];
      assert.equal(h.length, 1);
      assert.notEqual(h[0]!['deletedAt'], null);
      await refused('DELETE', `/${id('c')}`, undefined, 404, d.queryNotFound(id('c')), true);
    });

    it('sorts by last update, newest first', async () => {
      await raw('POST', '', query(id('s1'), { name: `Sort ${run} one` }));
      // the update times are the SERVER's: no clock of this test's reaches it, so the two saves are 5 ms apart
      await new Promise((r) => setTimeout(r, 5));
      await raw('POST', '', query(id('s2'), { name: `Sort ${run} two` }));
      const r = (await raw('POST', '/search', { searchTermSpecification: { searchTerm: `sort ${run}` }, sortByOption: 'SORT_BY_UPDATE' })).json as Record<string, unknown>[];
      assert.deepEqual(r.map((q) => q['id']), [id('s2'), id('s1')]);
    });

    it('answers a search in the same order on every store: by creation, then by id (not by file name)', async () => {
      // "q-2.json" sorts before "q.json" where the ids sort "q" first: every store orders by the ids
      await raw('POST', '', query(id('q'), { name: `Order ${run} q` }));
      await raw('POST', '', query(id('q-2'), { name: `Order ${run} q-2` }));
      const r = (await raw('POST', '/search', { searchTermSpecification: { searchTerm: `order ${run}` } })).json as Record<string, unknown>[];
      assert.deepEqual(r.map((q) => q['id']), [id('q'), id('q-2')]);
    });

    it('answers a field of the wrong kind with the engine 500, naming it, on every store', async () => {
      const wrong = async (method: string, path: string, body: unknown, message: string): Promise<void> => {
        const r = await raw(method, path, body);
        assert.equal(r.status, 500, r.text);
        assert.deepEqual(r.json, d.refusal(500, d.malformed(message)));
      };
      await wrong('POST', '/search', { limit: 'ten' }, 'QuerySearchSpecification.limit is not a number');
      await wrong('POST', '/search', { searchTermSpecification: { searchTerm: 'x', includeOwner: 'yes' } },
        'QuerySearchTermSpecification.includeOwner is not a boolean');
      await wrong('POST', '/search', { projectCoordinates: [5] },
        'QuerySearchSpecification.projectCoordinates holds something not QueryProjectCoordinates');
      await wrong('POST', '/search', { projectCoordinates: [{ groupId: 'demo', artifactId: null }] },
        'QueryProjectCoordinates.artifactId is missing');
      await wrong('POST', '', query(id('w'), { taggedValues: [{ tag: { profile: 'p' }, value: 'v' }] } as unknown as Partial<Query>),
        'TagPtr.value is missing');
      await wrong('PUT', `/${id('a')}/patchQuery`, { stereotypes: 'x' }, 'Query.stereotypes is not a list');
      // and nothing was written
      await refused('GET', `/${id('w')}`, undefined, 404, d.queryNotFound(id('w')), true);
    });

    it('keeps a number as it was written: 1.0, and a long beyond 2^53', async () => {
      const written = '"gridConfig":{"width":1.0,"big":9007199254740993}';
      const made = await raw('POST', '', JSON.stringify(query(id('n'))).replace(/}$/, `,${written}}`));
      assert.equal(made.status, 200, made.text);
      assert.ok(made.text.includes(written), made.text);
      const got = await raw('GET', `/${encodeURIComponent(id('n'))}`);
      assert.ok(got.text.includes(written), got.text);
    });

    it('says an unknown path is not there', async () => {
      const r = await raw('GET', `/${id('a')}/nothing`);
      assert.equal(r.status, 404);
      assert.equal((r.json as { message?: string }).message, d.noRoute(`/${id('a')}/nothing`));
      // 404, and no record missing: lite's kind request (2026-11-17, notFound is a missing record's alone)
      if (d === LITE) assert.equal((r.json as { kind?: string }).kind, 'request', r.text);
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
        && e.message === LITE.queryNotFound(id('k')));
    });
  });
}
