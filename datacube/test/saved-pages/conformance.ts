// ONE SUITE, every store of DataCube's saved pages (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md B6): upstream's
// lite's `/api/lite/v1/queries/dataCube` (lite API v1 §2, H6 2026-11-17: until then legend-engine's
// `/api/pure/v1/query/dataCube`) asked over raw HTTP -- the statuses and the refusals word for word, the record's fields
// -- and through the one client (src/cube-store.ts `CubeStoreClient`). It runs on the page's own store (page.test.ts:
// legend-lite's rules compiled to WebAssembly), on legend-lite's server (server.test.ts) and on Python's
// (../python-engine/saved-pages.test.ts: the native library's), so no two can answer differently without one failing.
// The rules are the ones the page kept in TypeScript before B6 (cube-store.ts's RuleStore); its tests are these.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { ExactNumber, fromJson } from '../../../pure-protocol/src/index.ts';
import { CubeStoreClient, CubeStoreError, type DataCubeQuery } from '../../src/cube-store.ts';

export interface Target {
  /** The API root, `http://host:port/api`. */
  readonly api: string;
  readonly fetch: typeof fetch;
  /** Who the store says the caller is. */
  readonly user: string;
}

/** How many suites this process has run: each suite's ids are its own (every target starts on a fresh store). */
let suites = 0;

export function conformance(name: string, target: () => Target): void {
  describe(`the saved pages: ${name}`, () => {
    suites += 1;
    const run = `c${suites}`;
    const id = (n: string): string => `${run}-${n}`;
    const page = (n: string, name: string, extra: Partial<DataCubeQuery> = {}): DataCubeQuery =>
      ({ id: id(n), name, content: { kind: 'datacube.page', n: 1 }, ...extra });
    const store = (): CubeStoreClient => new CubeStoreClient(target().api, target().fetch, target().user);
    const raw = async (method: string, path: string, body?: string): Promise<{ status: number; json: unknown; text: string }> => {
      const t = target();
      const r = await t.fetch(`${t.api}/lite/v1/queries/dataCube${path}`, {
        method, ...(body === undefined ? {} : { headers: { 'Content-Type': 'application/json' }, body }),
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
    const refused = (status: number, message: RegExp | string) => (e: unknown): boolean =>
      e instanceof CubeStoreError && e.status === status
      && (typeof message === 'string' ? e.message === message : message.test(e.message));

    it("creates with the client's id, forces the owner, and refuses a taken id", async () => {
      const made = await store().create(page('a', 'Alpha', { owner: 'someone else' }));
      assert.equal(made.owner, target().user, 'the owner is the current user, whatever the request says');
      assert.ok(made.createdAt !== undefined && made.lastUpdatedAt === made.createdAt);
      await assert.rejects(store().create(page('a', 'Again')), refused(400, `the ID '${id('a')}' is already taken`));
      await assert.rejects(store().create({ ...page('x', 'x'), id: '' }), refused(400, 'the ID is missing or empty'));
      await assert.rejects(store().create(page('b', '')), refused(400, 'the name is missing or empty'));
    });

    it('stamps lastOpenAt when a page is read', async () => {
      const made = await store().create(page('o', 'Opened'));
      await new Promise((r) => setTimeout(r, 5));
      const opened = await store().get(id('o'));
      assert.ok((opened.lastOpenAt ?? 0) > (made.lastOpenAt ?? 0));
      assert.deepEqual(opened.content, fromJson('{"kind":"datacube.page","n":1}'), 'its content read exactly');
      assert.equal(typeof opened.lastOpenAt, 'number', 'its times plain numbers');
      await assert.rejects(store().get(id('nope')), refused(404, `no saved query has the ID '${id('nope')}'`));
      // lite API v1's kind notFound (the user, 2026-11-17 ~23:31): a page that does not exist, 404, in lite's words
      const missing = await raw('GET', `/${id('nope')}`);
      assert.equal(missing.status, 404, missing.text);
      assert.deepEqual(missing.json, { kind: 'notFound', message: `no saved query has the ID '${id('nope')}'` });
    });

    it('updates and deletes the owner\'s page, and never changes an id', async () => {
      const made = await store().create(page('u', 'Alpha'));
      await assert.rejects(store().update(id('u'), page('v', 'Moved')), refused(400, 'the ID cannot be changed'));
      await assert.rejects(store().update(id('none'), page('none', 'N')), refused(404, `no saved query has the ID '${id('none')}'`));
      await new Promise((r) => setTimeout(r, 5));
      const updated = await store().update(id('u'), page('u', 'Alpha 2'));
      assert.equal(updated.name, 'Alpha 2');
      assert.equal(updated.createdAt, made.createdAt, 'its creation kept');
      assert.ok((updated.lastUpdatedAt ?? 0) > (made.lastUpdatedAt ?? 0));
      await store().delete(id('u'));
      await assert.rejects(store().get(id('u')), refused(404, /no saved query has the ID/));
      await assert.rejects(store().delete(id('u')), refused(404, /no saved query has the ID/));
    });

    it("searches upstream's way: light records, the chosen sort (newest first), the limit, a term", async () => {
      for (const [n, name] of [['s1', `Trades by desk ${run}`], ['s2', `Risk ${run}`], ['s3', `Trades again ${run}`]]) {
        await store().create(page(n!, name!));
        await new Promise((r) => setTimeout(r, 5));
      }
      const term = { searchTerm: run };
      const all = await store().search({ searchTermSpecification: term, sortByOption: 'SORT_BY_CREATE' });
      assert.deepEqual(all.map((q) => q.id).filter((i) => /-s\d$/.test(i)), [id('s3'), id('s2'), id('s1')], 'newest first');
      assert.equal('content' in (all[0] as object), false, 'no content in a search');
      assert.deepEqual((await store().search({ searchTermSpecification: { searchTerm: `trades` } })).map((q) => q.id)
        .filter((i) => i.startsWith(run)).sort(), [id('s1'), id('s3')]);
      assert.deepEqual((await store().search({ searchTermSpecification: { searchTerm: id('s2') } })).map((q) => q.id), [id('s2')],
        'an id matches');
      assert.deepEqual((await store().search({ searchTermSpecification: { searchTerm: `Risk ${run}`, exactMatchName: true } }))
        .map((q) => q.id), [id('s2')], 'an exact name');
      assert.ok((await store().search({ showCurrentUserQueriesOnly: true })).every((q) => q.owner === target().user));
      assert.equal((await store().search({ limit: 1 })).length, 1);
    });

    it("keeps a content's exact numbers", async () => {
      const content = fromJson('{"kind":"datacube.page","big":123456789012345678901234,"dec":12.30}') as Record<string, unknown>;
      await store().create({ id: id('x'), name: 'Exact', content });
      const back = (await store().get(id('x'))).content;
      assert.ok(back['big'] instanceof ExactNumber && back['big'].text === '123456789012345678901234');
      assert.ok(back['dec'] instanceof ExactNumber && back['dec'].text === '12.30');
    });

    it('refuses an id a page does not make, on every host (one host keeps a page as a file of that name)', async () => {
      for (const bad of ['a.b', 'CON', 'nul', '_x']) {
        await assert.rejects(store().create({ ...page('x', 'x'), id: bad }), refused(400, /a saved page's id is letters/), bad);
      }
      const r = await raw('GET', `/${encodeURIComponent('../escape')}`);
      assert.equal(r.status, 400, r.text);
    });

    // lite's API v1 spec §3 (PR #118 fix round, 2026-11-18): a malformed request -- a body of the wrong shape, a search
    // with no term, a path the store does not serve -- is 400 `request`, in lite's words (the request's field by its
    // path, never legend-engine's class); legend-engine's 500s and 404 are the edge's mount's alone
    it('answers a body of the wrong shape, a search with no term and an unknown path as lite\'s request refusal, 400', async () => {
      const wrong = async (body: string, message: string): Promise<void> => {
        const r = await raw('POST', '/search', body);
        assert.equal(r.status, 400, r.text);
        assert.deepEqual(r.json, { kind: 'request', message });
      };
      await wrong('{"limit":"ten"}', "the request's limit is not a number");
      await wrong('{"searchTermSpecification":{"searchTerm":"x","includeOwner":"yes"}}',
        "the request's searchTermSpecification.includeOwner is not a boolean");
      const noTerm = await raw('POST', '/search', '{"searchTermSpecification":{}}');
      assert.equal(noTerm.status, 400);
      assert.deepEqual(noTerm.json, { kind: 'request', message: 'the search specification has no search term' });
      const unknown = await raw('GET', `/${id('a')}/nothing`);
      assert.equal(unknown.status, 400);
      assert.equal((unknown.json as { kind?: string }).kind, 'request');
      assert.equal((unknown.json as { message?: string }).message,
        `no such legend-lite API: /api/lite/v1/queries/dataCube/${id('a')}/nothing`);
    });
  });
}
