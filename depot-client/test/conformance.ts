// ONE SUITE, every Depot here: upstream legend-depot's read routes asked over raw HTTP -- shapes, key
// order, statuses -- and through the client, over versions published through the SDLC beside it. Runs
// against the page's Depot (wasm.test.ts) and the model home's server (server.test.ts). The rules:
// studio/docs/DEPOT_CONTRACT.md; lite's departures: README.md.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { SdlcClient } from '../../sdlc-client/src/client.ts';
import { DepotClient } from '../src/client.ts';

export interface Target {
  readonly sdlc: { readonly api: string; readonly fetch: typeof fetch };
  readonly depot: { readonly api: string; readonly fetch: typeof fetch };
}

export function conformance(name: string, target: () => Target): void {
  describe(`Depot: ${name}`, () => {
    const run = `d${Date.now().toString(36)}${Math.floor(Math.random() * 1e6).toString(36)}`;
    const g = `org.finos.lite.${run}`;
    const sdlc = (): SdlcClient => new SdlcClient(target().sdlc.api, target().sdlc.fetch);
    const depot = (): DepotClient => new DepotClient(target().depot.api, target().depot.fetch);
    const raw = async (method: string, path: string, body?: unknown): Promise<{ status: number; json: unknown; text: string }> => {
      const t = target().depot;
      const r = await t.fetch(`${t.api}${path}`, { method, ...(body === undefined ? {} : { headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) }) });
      const text = await r.text();
      let json: unknown;
      try { json = text ? JSON.parse(text) : undefined; } catch { /* text */ }
      return { status: r.status, json, text };
    };

    /** A project at `artifact`, its files and dependencies landed by a review, then versioned. */
    const publish = async (artifact: string, files: Record<string, string>, deps: [string, string][], versionType: 'MAJOR' | 'MINOR' | 'PATCH'): Promise<void> => {
      const p = `${g}:${artifact}`;
      if (!(await sdlc().projects({ search: artifact })).some((x) => x.projectId === p)) {
        await sdlc().createProject({ name: artifact, description: '', groupId: g, artifactId: artifact });
      }
      const w = `w${Math.floor(Math.random() * 1e9)}`;
      await sdlc().createWorkspace(p, w);
      if (deps.length > 0) {
        const res = await target().sdlc.fetch(`${target().sdlc.api}/projects/${encodeURIComponent(p)}/workspaces/${w}/configuration`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ message: 'deps', projectDependenciesToAdd: deps.map(([a, v]) => ({ projectId: `${g}:${a}`, versionId: v })) }),
        });
        assert.equal(res.status, 200, await res.text());
      }
      const existing = new Set((await sdlc().pure({ project: p, workspace: w })).map((f) => f.path));
      await sdlc().performPureChanges(p, w, {
        message: 'files', changes: Object.entries(files).map(([path, pureCode]) => ({ type: existing.has(path) ? 'MODIFY' as const : 'CREATE' as const, path, pureCode })),
      });
      const call = async (path: string, body: unknown): Promise<Record<string, unknown>> => {
        const res = await target().sdlc.fetch(`${target().sdlc.api}/projects/${encodeURIComponent(p)}${path}`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body),
        });
        const text = await res.text();
        assert.equal(res.status, 200, text);
        return JSON.parse(text) as Record<string, unknown>;
      };
      const review = await call('/reviews', { workspaceId: w, title: 'publish', description: '' });
      await call(`/reviews/${String(review['id'])}/commit`, { message: 'publish' });
      await call('/versions', { versionType });
    };

    it('lists and finds projects, in upstream\'s key order; a missing one is an empty 404', async () => {
      await publish('types', { 'demo::types::Currency': 'Enum demo::types::Currency\n{\n  USD, GBP\n}\n' }, [], 'MAJOR');
      const all = await raw('GET', '/project-configurations');
      const types = (all.json as Record<string, unknown>[]).find((x) => x['artifactId'] === 'types' && x['groupId'] === g)!;
      assert.deepEqual(Object.keys(types), ['groupId', 'artifactId', 'defaultBranch', 'projectId', 'latestVersion']);
      assert.deepEqual(types, { groupId: g, artifactId: 'types', defaultBranch: null, projectId: `${g}:types`, latestVersion: '1.0.0' });
      assert.deepEqual(await depot().project(g, 'types'), types);
      const missing = await raw('GET', `/project-configurations/${g}/nope`);
      assert.equal(missing.status, 404);
      assert.equal(missing.text, '');
    });

    it('lists versions (with the snapshot), reads a version\'s entities, and refuses a missing version', async () => {
      await publish('types', { 'demo::types::Currency': 'Enum demo::types::Currency\n{\n  USD, GBP, EUR\n}\n' }, [], 'MINOR');
      assert.deepEqual(await depot().versions(g, 'types'), ['1.0.0', '1.1.0', 'master-SNAPSHOT']);
      assert.deepEqual(await depot().versions(g, 'types', false), ['1.0.0', '1.1.0']);
      const entities = await depot().entities(g, 'types', '1.0.0');
      assert.deepEqual(entities.map((e) => [e.path, e.classifierPath]), [['demo::types::Currency', 'meta::pure::metamodel::type::Enumeration']]);
      assert.equal((await depot().entities(g, 'types', 'latest'))[0]!.path, 'demo::types::Currency');
      assert.equal((await depot().version(g, 'types', 'latest'))?.versionId, '1.1.0');
      assert.equal(await depot().version(g, 'types', '9.9.9'), undefined);
      const one = await raw('GET', `/projects/${g}/types/versions/1.0.0/entities/demo::types::Currency`);
      assert.equal((one.json as { path: string }).path, 'demo::types::Currency');
      assert.equal((await raw('GET', `/projects/${g}/types/versions/1.0.0/entities/demo::types::Nope`)).status, 404);
      const missing = await raw('GET', `/projects/${g}/types/versions/9.9.9`);
      assert.equal(missing.status, 404);
      const err = missing.json as Record<string, unknown>;
      assert.deepEqual(Object.keys(err), ['code', 'message', 'timestamp']);
      assert.equal(err['message'], `project version not found for ${g}-types-9.9.9`);
      assert.deepEqual(Object.keys(err['timestamp'] as object), ['epochSecond', 'nano']);
    });

    it('resolves dependencies nearest-wins, as upstream\'s Aether: at equal depth the first (by coordinates) wins', async () => {
      await publish('party', { 'demo::party::Party': 'Class demo::party::Party\n{\n  ccy: demo::types::Currency[1];\n}\n' }, [['types', '1.0.0']], 'MINOR');
      await publish('instruments', { 'demo::instruments::Bond': 'Class demo::instruments::Bond\n{\n  ccy: demo::types::Currency[1];\n}\n' }, [['types', '1.1.0']], 'MINOR');
      await publish('trading', { 'demo::trading::Trade': 'Class demo::trading::Trade\n{\n  party: demo::party::Party[1];\n  bond: demo::instruments::Bond[1];\n}\n' },
        [['party', '0.1.0'], ['instruments', '0.1.0']], 'MAJOR');
      const closure = await depot().dependencyEntities([{ groupId: g, artifactId: 'trading', versionId: '1.0.0' }]);
      const versions = Object.fromEntries(closure.map((v) => [v.artifactId, v.versionId]));
      // types is two deep under both; instruments sorts before party, so its types 1.1.0 wins
      assert.deepEqual(versions, { trading: '1.0.0', instruments: '0.1.0', party: '0.1.0', types: '1.1.0' });
      const first = closure[0]!;
      assert.deepEqual(Object.keys(first), ['groupId', 'artifactId', 'versionId', 'versionedEntity', 'entities']);
      // a version's own dependencies, stored closure
      const deps = await raw('GET', `/projects/${g}/trading/versions/1.0.0/dependencies?transitive=true`);
      assert.deepEqual(Object.fromEntries((deps.json as { artifactId: string; versionId: string }[]).map((v) => [v.artifactId, v.versionId])),
        { instruments: '0.1.0', party: '0.1.0', types: '1.1.0' });
      // and the text twin, for the in-tab compiler
      const files = await depot().dependencyFiles([{ groupId: g, artifactId: 'party', versionId: '0.1.0' }]);
      assert.deepEqual(files.map((f) => [f.artifactId, f.versionId, f.files.map((x) => x.path)]),
        [['party', '0.1.0', ['demo::party::Party']], ['types', '1.0.0', ['demo::types::Currency']]]);
    });
  });
}
