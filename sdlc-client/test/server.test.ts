// sdlc-server over HTTP (`//sdlc-server:server`, a git repository on disk), held to the one suite -- the
// SDLC the page runs compiled to WebAssembly is the SDLC a server answers. That the repository it writes is
// git's is //sdlc-server:git_repository_test's (JGit reads it back; no host git in a test).

import assert from 'node:assert/strict';
import { readFileSync, writeFileSync } from 'node:fs';
import { request as httpRequest } from 'node:http';
import { basename, join } from 'node:path';
import { after, before, describe, it } from 'node:test';

import { conformance } from './conformance.ts';
import { startSdlcServer, type RunningServer } from './sdlc-server.ts';

let server: RunningServer | undefined;
let api = '';
let repo = '';

before(async () => {
  server = await startSdlcServer('sdlc');
  api = `${server.base}/sdlc/api`;
  repo = server.repo;
});

after(() => server?.stop());

conformance('sdlc-server over HTTP', () => ({ api, fetch: globalThis.fetch.bind(globalThis), user: 'local' }));

/** One raw request, with headers fetch will not let a test set (Host). */
function request(path: string, method: string, headers: Record<string, string>): Promise<{ status: number; headers: Record<string, unknown> }> {
  const url = new URL(`${api}${path}`);
  return new Promise((resolve, reject) => {
    const req = httpRequest({ host: url.hostname, port: url.port, path: url.pathname + url.search, method, headers }, (res) => {
      res.resume();
      res.on('end', () => resolve({ status: res.statusCode ?? 0, headers: res.headers }));
    });
    req.on('error', reject);
    req.end();
  });
}

describe('sdlc-server: who may call it (review findings 1, 2)', () => {
  it('refuses a page from another origin, and a request for another host (DNS rebinding)', async () => {
    const port = new URL(api).port;
    const evil = await request('/projects', 'GET', { Origin: 'https://example.com' });
    assert.equal(evil.status, 403);
    assert.equal(evil.headers['access-control-allow-origin'], undefined);
    const preflight = await request('/projects', 'OPTIONS', { Origin: 'https://example.com', 'Access-Control-Request-Method': 'DELETE' });
    assert.equal(preflight.status, 403);
    const rebound = await request('/projects', 'GET', { Host: `attacker.example:${port}` });
    assert.equal(rebound.status, 403);
  });

  it('answers a page served from this machine, naming its origin (never *)', async () => {
    const r = await request('/projects', 'GET', { Origin: 'http://127.0.0.1:8200' });
    assert.equal(r.status, 200);
    assert.equal(r.headers['access-control-allow-origin'], 'http://127.0.0.1:8200');
  });

  it('keeps a file beside the repository whatever ids it is sent', async () => {
    const canary = join(repo, '..', `canary-${Date.now()}`);
    writeFileSync(canary, 'still here');
    for (const path of [
      `/projects/x:y/workspaces/${encodeURIComponent(`../../../${basename(canary)}`)}`,
      `/projects/${encodeURIComponent(`../../${basename(canary)}`)}/workspaces/w`,
      `/projects/${encodeURIComponent(`x:../../../../${basename(canary)}`)}/workspaces/w`,
    ]) {
      assert.equal((await request(path, 'DELETE', {})).status, 400, path);
    }
    assert.equal(readFileSync(canary, 'utf8'), 'still here');
  });
});
