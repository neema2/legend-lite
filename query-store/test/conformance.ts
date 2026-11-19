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
  /** A body's field the store cannot read -- missing, or not of its kind: lite names the field by its path in the
   *  request and says the problem in its words, legend-engine names its own class and field (PR #118 fix round,
   *  2026-11-18: lite's words named legend-engine's classes). */
  unread(where: string, field: string, problem: string, path: string, liteProblem?: string): string;
  /** The status of a malformed request (a body of the wrong shape, a search with no term): lite's `request` 400 (its
   *  API v1 spec §3), legend-engine's 500. */
  readonly malformedStatus: number;
  /** The status of a path the store does not serve: lite's `request` 400, legend-engine's 404. */
  readonly noRouteStatus: number;
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
  unread: (_where, _field, problem, path, liteProblem) => `the request's ${path} ${liteProblem ?? problem}`,
  malformedStatus: 400,
  noRouteStatus: 400,
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
  unread: (where, field, problem) => `IllegalArgumentException: ${where}.${field} ${problem}`,
  malformedStatus: 500,
  noRouteStatus: 404,
  refusal: (status, message) => (status >= 500 ? { code: -1, message, status: 'error' } : { message }),
};

export interface Target {
  /** The API root, `http://host:port/api`. */
  readonly api: string;
  readonly fetch: typeof fetch;
  /** Who the store says the caller is. */
  readonly user: string;
  /** Whether the host's HTTP layer refuses a request URI that is not percent-encoded itself, before the store is asked:
   *  legend-lite's server, whose JDK HttpServer answers its own 400 page ("Bad request URI") for `%zz` on either mount
   *  (measured 2026-11-19, PH-1); the page's and Python's hosts hand the store the raw path. */
  readonly refusesUnencodedUris?: boolean;
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
      await refused('POST', '/search', { searchTermSpecification: {} }, d.malformedStatus, d.noSearchTerm);
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

    // lite's mount: a malformed request, 400 kind request, the field by its path in lite's words; legend-engine's: its
    // 500, its class named (PR #118 fix round, 2026-11-18)
    it('answers a field of the wrong kind as a malformed request, naming it, on every store', async () => {
      const wrong = async (method: string, path: string, body: unknown, message: string): Promise<void> => {
        const r = await raw(method, path, body);
        assert.equal(r.status, d.malformedStatus, r.text);
        assert.deepEqual(r.json, d.refusal(d.malformedStatus, message));
      };
      await wrong('POST', '/search', { limit: 'ten' },
        d.unread('QuerySearchSpecification', 'limit', 'is not a number', 'limit'));
      await wrong('POST', '/search', { searchTermSpecification: { searchTerm: 'x', includeOwner: 'yes' } },
        d.unread('QuerySearchTermSpecification', 'includeOwner', 'is not a boolean', 'searchTermSpecification.includeOwner'));
      await wrong('POST', '/search', { projectCoordinates: [5] },
        d.unread('QuerySearchSpecification', 'projectCoordinates', 'holds something not QueryProjectCoordinates',
          'projectCoordinates', 'holds something that is not an object'));
      await wrong('POST', '/search', { projectCoordinates: [{ groupId: 'demo', artifactId: null }] },
        d.unread('QueryProjectCoordinates', 'artifactId', 'is missing', 'projectCoordinates[].artifactId'));
      await wrong('POST', '', query(id('w'), { taggedValues: [{ tag: { profile: 'p' }, value: 'v' }] } as unknown as Partial<Query>),
        d.unread('TagPtr', 'value', 'is missing', 'taggedValues[].tag.value'));
      await wrong('PUT', `/${id('a')}/patchQuery`, { stereotypes: 'x' },
        d.unread('Query', 'stereotypes', 'is not a list', 'stereotypes'));
      // and nothing was written
      await refused('GET', `/${id('w')}`, undefined, 404, d.queryNotFound(id('w')), true);
    });

    // a body that is not JSON, or no JSON object: a malformed request on every store, said where the request is read
    // (PR #118 fix round, 2026-11-18) -- lite's 400 request in its words, legend-engine's 500 naming its exception
    it('answers a body that is not JSON, or not an object, as a malformed request on every store', async () => {
      for (const [body, lite, engine] of [
        ['{not json', /^the request could not be read: its body is not JSON: /, /^IllegalArgumentException: /],
        ['[1]', /^the request could not be read: its body is not a JSON object$/,
          /^IllegalArgumentException: Expected top-level JSON object, got Arr$/],
      ] as const) {
        const r = await raw('POST', '/search', body);
        assert.equal(r.status, d.malformedStatus, r.text);
        const said = r.json as { kind?: string; message?: string };
        assert.match(String(said.message), d === LITE ? lite : engine, r.text);
        if (d === LITE) assert.equal(said.kind, 'request', r.text);
      }
      const v = await raw('GET', `/${id('a')}/history?version=x`);
      assert.equal(v.status, d.malformedStatus, v.text);
      if (d === LITE) assert.equal((v.json as { message?: string }).message, "the request could not be read: its version 'x' is not a number");
      const sort = await raw('POST', '/search', { sortByOption: 'SORT_BY_NAME' });
      assert.equal(sort.status, d.malformedStatus, sort.text);
      assert.equal((sort.json as { message?: string }).message, d === LITE
        ? "the request could not be read: its sort option 'SORT_BY_NAME' is none of SORT_BY_CREATE, SORT_BY_VIEW and SORT_BY_UPDATE"
        : 'IllegalArgumentException: Unknown sort-by value');
    });

    // a path segment or a parameter that is not percent-encoded: a malformed request on every store (PH-1 of the PR #118
    // fix batch's audit, 2026-11-18: it was the store's failure, lite's 500 internal) -- lite's 400 request in its
    // words, legend-engine's 500 naming the decoder's exception, as before
    it('answers a path segment or a parameter that is not percent-encoded as a malformed request on every store', async () => {
      for (const [path, lite] of [
        ['/%zz', "the request could not be read: its path segment '%zz' is not percent-encoded"],
        ['/batch?queryIds=%zz', "the request could not be read: its parameter 'queryIds=%zz' is not percent-encoded"],
        ['/%zz/history', "the request could not be read: its path segment '%zz' is not percent-encoded"],
      ] as const) {
        const r = await raw('GET', path);
        if (target().refusesUnencodedUris === true) {
          // the host's HTTP layer answers first, a request error on either mount: the store is never asked
          assert.equal(r.status, 400, `${path}: ${r.text}`);
          assert.equal(r.json, undefined, `${path}: ${r.text}`);
          continue;
        }
        assert.equal(r.status, d.malformedStatus, `${path}: ${r.text}`);
        const said = r.json as { kind?: string; message?: string };
        if (d === LITE) {
          assert.deepEqual(said, { kind: 'request', message: lite }, path);
        } else {
          assert.deepEqual(Object.keys(said), ['code', 'message', 'status'], r.text);
          assert.match(String(said.message), /^IllegalArgumentException: URLDecoder: /, r.text);
        }
      }
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
      assert.equal(r.status, d.noRouteStatus);
      assert.equal((r.json as { message?: string }).message, d.noRoute(`/${id('a')}/nothing`));
      // no record missing: lite's kind request, 400 (2026-11-17, notFound is a missing record's alone; the spec's 400,
      // PR #118 fix round, 2026-11-18); legend-engine's 404
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
