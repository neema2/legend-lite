// The page's own SDLC (local-server.ts), held to the one suite -- and what only it answers: the
// capabilities a page does not have, in upstream's 501.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { localSdlcServer, LOCAL_API } from '../src/local-server.ts';
import { MemoryRecords } from '../src/records.ts';
import { conformance } from './conformance.ts';
import { wasmGrammar } from './grammar.ts';

const server = localSdlcServer({ records: new MemoryRecords(), user: { userId: 'local', name: 'Local User' }, grammar: wasmGrammar });
conformance("the page's own SDLC", () => ({ api: LOCAL_API, fetch: server.fetch, user: 'local' }));

describe("the page's own SDLC: what a page does not have", () => {
  it('answers reviews, versions and JSON saves with upstream\'s 501', async () => {
    const p = encodeURIComponent('org.finos.lite.page:only');
    await server.fetch(`${LOCAL_API}/projects`, {
      method: 'POST', body: JSON.stringify({ name: 'Only', description: '', groupId: 'org.finos.lite.page', artifactId: 'only' }),
    });
    await server.fetch(`${LOCAL_API}/projects/${p}/workspaces/w`, { method: 'POST' });
    for (const [method, path, capability] of [
      ['GET', `/projects/${p}/reviews`, 'REVIEWS'],
      ['POST', `/projects/${p}/versions`, 'VERSIONS'],
      ['GET', `/projects/${p}/patches`, 'PATCHES'],
      ['POST', `/projects/${p}/workspaces/w/entityChanges`, 'ENTITY_CHANGES'],
    ] as const) {
      const r = await server.fetch(`${LOCAL_API}${path}`, { method, ...(method === 'POST' ? { body: '{}' } : {}) });
      assert.equal(r.status, 501, path);
      assert.deepEqual(await r.json(), { capability, backendType: 'page', message: `The backend "page" does not support ${capability}` });
    }
  });

  it('keeps one project per coordinates', async () => {
    const body = JSON.stringify({ name: 'Twice', description: '', groupId: 'org.finos.lite.page', artifactId: 'twice' });
    assert.equal((await server.fetch(`${LOCAL_API}/projects`, { method: 'POST', body })).status, 200);
    const again = await server.fetch(`${LOCAL_API}/projects`, { method: 'POST', body });
    assert.equal(again.status, 409);
    assert.equal(((await again.json()) as { message: string }).message,
      'Failed to create project: Twice: a project with coordinates org.finos.lite.page:twice already exists');
  });
});
