// The page's SDLC compiled from Java (sdlc-server's rules, //sdlc-server:page), held to the one suite --
// and what only a page asks of it: that what it saved survives the page being opened again.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { fileURLToPath } from 'node:url';

import { SdlcClient } from '../src/client.ts';
import { MemoryRecords } from '../src/records.ts';
import { WASM_API, wasmSdlcServer, type SdlcModule } from '../src/wasm-server.ts';
import { conformance } from './conformance.ts';

const DIR = new URL('../../sdlc-server/page/', import.meta.url);

async function load(): Promise<SdlcModule> {
  const runtime = await import(new URL('wasm-gc-module-runtime.js', DIR).href) as {
    load(src: string, options: unknown): Promise<SdlcModule>;
  };
  return runtime.load(fileURLToPath(new URL('classes.wasm', DIR)), {
    stackDeobfuscator: { enabled: false },
    installImports(i: Record<string, unknown>) {
      i.teavmConsole = { putcharStdout() {}, putcharStderr() {} };
    },
  });
}

const module = await load();
const user = { userId: 'local', name: 'Local User' };
const server = await wasmSdlcServer(module, new MemoryRecords(), user);
conformance("the page's SDLC, compiled from Java", () => ({ api: WASM_API, fetch: server.fetch, user: 'local' }));

describe("the page's SDLC, compiled from Java: what a page asks", () => {
  it('answers patches and JSON saves with upstream\'s 501', async () => {
    const p = encodeURIComponent('org.finos.lite.page:only');
    await server.fetch(`${WASM_API}/projects`, {
      method: 'POST', body: JSON.stringify({ name: 'Only', description: '', groupId: 'org.finos.lite.page', artifactId: 'only' }),
    });
    await server.fetch(`${WASM_API}/projects/${p}/workspaces/w`, { method: 'POST' });
    for (const [method, path, capability] of [
      ['GET', `/projects/${p}/patches`, 'PATCHES'],
      ['POST', `/projects/${p}/workspaces/w/entityChanges`, 'ENTITY_CHANGES'],
    ] as const) {
      const r = await server.fetch(`${WASM_API}${path}`, { method, ...(method === 'POST' ? { body: '{}' } : {}) });
      assert.equal(r.status, 501, path);
      assert.deepEqual(await r.json(), { capability, backendType: 'page', message: `The backend "page" does not support ${capability}` });
    }
  });

  it('keeps one project per coordinates', async () => {
    const body = JSON.stringify({ name: 'Twice', description: '', groupId: 'org.finos.lite.page', artifactId: 'twice' });
    assert.equal((await server.fetch(`${WASM_API}/projects`, { method: 'POST', body })).status, 200);
    const again = await server.fetch(`${WASM_API}/projects`, { method: 'POST', body });
    assert.equal(again.status, 409);
    assert.equal(((await again.json()) as { message: string }).message,
      'Failed to create project: Twice: a project with coordinates org.finos.lite.page:twice already exists');
  });

  it('keeps what it saved when the page opens again', async () => {
    const records = new MemoryRecords();
    const first = new SdlcClient(WASM_API, (await wasmSdlcServer(await load(), records, user)).fetch);
    await first.createProject({ name: 'Kept', description: '', groupId: 'org.finos.lite.page', artifactId: 'kept' });
    await first.createWorkspace('org.finos.lite.page:kept', 'w');
    const saved = await first.performPureChanges('org.finos.lite.page:kept', 'w', {
      message: 'one', changes: [{ type: 'CREATE', path: 'demo::A', pureCode: 'Class demo::A {}' }],
    });
    // a fresh module, loaded from the records alone
    const again = new SdlcClient(WASM_API, (await wasmSdlcServer(await load(), records, user)).fetch);
    assert.deepEqual(await again.revision({ project: 'org.finos.lite.page:kept', workspace: 'w' }), saved);
    assert.deepEqual(await again.pure({ project: 'org.finos.lite.page:kept', workspace: 'w' }), [{ path: 'demo::A', pureCode: 'Class demo::A {}' }]);
  });
});
