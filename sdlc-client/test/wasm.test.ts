// The page's SDLC compiled from Java (sdlc-server's rules, //sdlc-server:page), held to the one suite --
// and what only a page asks of it: that what it saved survives the page being opened again.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { fileURLToPath } from 'node:url';

import { SdlcClient } from '../src/client.ts';
import { MemoryRecords, type Guard } from '../src/records.ts';
import { WASM_API, wasmSdlcServer, type SdlcModule } from '../src/wasm-server.ts';
import { conformance } from './conformance.ts';
import { runfileDirUrl } from '../../tools/js/runfiles.mts';

const DIR = runfileDirUrl('SDLC_PAGE');

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

  it('a save the browser cannot store is not made: the page stays what is stored (review finding 6)', async () => {
    class FailOnce extends MemoryRecords {
      fail = false;
      override async apply(changes: readonly (readonly [string, unknown])[], guard?: Guard): Promise<boolean> {
        if (this.fail) {
          this.fail = false;
          throw new Error('QuotaExceededError');
        }
        return super.apply(changes, guard);
      }
    }
    const records = new FailOnce();
    const page = new SdlcClient(WASM_API, (await wasmSdlcServer(await load(), records, user)).fetch);
    await page.createProject({ name: 'Quota', description: '', groupId: 'org.finos.lite.page', artifactId: 'quota' });
    await page.createWorkspace('org.finos.lite.page:quota', 'w');
    const before = await page.revision({ project: 'org.finos.lite.page:quota', workspace: 'w' });
    records.fail = true;
    await assert.rejects(page.performPureChanges('org.finos.lite.page:quota', 'w', {
      message: 'lost', changes: [{ type: 'CREATE', path: 'demo::Lost', pureCode: 'Class demo::Lost {}' }],
    }), /could not store this change/);
    // memory went back to the records: the save never happened, and the next one stands on what is stored
    assert.deepEqual(await page.revision({ project: 'org.finos.lite.page:quota', workspace: 'w' }), before);
    const next = await page.performPureChanges('org.finos.lite.page:quota', 'w', {
      message: 'kept', revisionId: before.id, changes: [{ type: 'CREATE', path: 'demo::Kept', pureCode: 'Class demo::Kept {}' }],
    });
    const reopened = new SdlcClient(WASM_API, (await wasmSdlcServer(await load(), records, user)).fetch);
    assert.deepEqual(await reopened.revision({ project: 'org.finos.lite.page:quota', workspace: 'w' }), next);
    assert.deepEqual((await reopened.pure({ project: 'org.finos.lite.page:quota', workspace: 'w' })).map((f) => f.path), ['demo::Kept']);
  });

  it('a page that cannot be put back to what is stored refuses everything after: reload (re-review A)', async () => {
    class Failing extends MemoryRecords {
      fail = false;
      override async apply(changes: readonly (readonly [string, unknown])[], guard?: Guard): Promise<boolean> {
        if (this.fail) throw new Error('QuotaExceededError');
        return super.apply(changes, guard);
      }
      override async list<T>(prefix: string): Promise<T[]> {
        if (this.fail) throw new Error('UnknownError: the database is gone');
        return super.list<T>(prefix);
      }
    }
    const records = new Failing();
    const page = new SdlcClient(WASM_API, (await wasmSdlcServer(await load(), records, user)).fetch);
    await page.createProject({ name: 'Gone', description: '', groupId: 'org.finos.lite.page', artifactId: 'gone' });
    records.fail = true;
    await assert.rejects(page.createWorkspace('org.finos.lite.page:gone', 'w'), (e: Error) => {
      assert.match(e.message, /could not store this change/);
      assert.match(String((e.cause as Error).message), /the database is gone/);
      return true;
    });
    // the records work again, but this module's memory was half reset: it must not answer, nor write over them
    records.fail = false;
    await assert.rejects(page.projects(), (e: Error) => {
      assert.match(e.message, /reload this page/);
      assert.match(String((e.cause as Error).message), /the database is gone/);
      return true;
    });
    const reloaded = new SdlcClient(WASM_API, (await wasmSdlcServer(await load(), records, user)).fetch);
    assert.deepEqual((await reloaded.projects()).map((x) => x.projectId), ['org.finos.lite.page:gone']);
  });

  it('two tabs over one store: each sees the other\'s saves, and neither writes over them (re-review C)', async () => {
    /** Runs `between` once, just before the next guarded write: another tab's write landing first. */
    class Racing extends MemoryRecords {
      between: (() => Promise<unknown>) | undefined;
      override async apply(changes: readonly (readonly [string, unknown])[], guard?: Guard): Promise<boolean> {
        const other = this.between;
        this.between = undefined;
        if (other !== undefined) await other();
        return super.apply(changes, guard);
      }
    }
    const records = new Racing();
    const one = new SdlcClient(WASM_API, (await wasmSdlcServer(await load(), records, user)).fetch);
    const two = new SdlcClient(WASM_API, (await wasmSdlcServer(await load(), records, user)).fetch);
    const p = 'org.finos.lite.page:tabs';
    await one.createProject({ name: 'Tabs', description: '', groupId: 'org.finos.lite.page', artifactId: 'tabs' });
    // tab two loaded before the project was made: it catches up before it answers
    await two.createWorkspace(p, 'w');
    const base = await one.revision({ project: p, workspace: 'w' });

    // tab two saves while tab one's save is on its way to the store: one's lost the race, runs again over two's
    records.between = () => two.performPureChanges(p, 'w', { message: 'two', changes: [{ type: 'CREATE', path: 'demo::Two', pureCode: 'Class demo::Two {}' }] });
    await one.performPureChanges(p, 'w', { message: 'one', changes: [{ type: 'CREATE', path: 'demo::One', pureCode: 'Class demo::One {}' }] });
    const kept = ['demo::One', 'demo::Two'];
    assert.deepEqual((await one.pure({ project: p, workspace: 'w' })).map((f) => f.path).sort(), kept);
    assert.deepEqual((await two.pure({ project: p, workspace: 'w' })).map((f) => f.path).sort(), kept);
    const reopened = new SdlcClient(WASM_API, (await wasmSdlcServer(await load(), records, user)).fetch);
    assert.deepEqual((await reopened.pure({ project: p, workspace: 'w' })).map((f) => f.path).sort(), kept);

    // a save locked to a revision another tab moved past is refused, as a server refuses a second client's
    await assert.rejects(two.performPureChanges(p, 'w', {
      message: 'stale', revisionId: base.id, changes: [{ type: 'CREATE', path: 'demo::Stale', pureCode: 'Class demo::Stale {}' }],
    }), /Expected revision/);
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
