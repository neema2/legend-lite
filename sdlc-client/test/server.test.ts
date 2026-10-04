// sdlc-server over HTTP (`//sdlc-server:server`, a git repository on disk), held to the one suite -- the
// SDLC the page runs compiled to WebAssembly is the SDLC a server answers. Then git itself checks the
// repository the server wrote: real objects, real refs.

import assert from 'node:assert/strict';
import { spawn, spawnSync, type ChildProcess } from 'node:child_process';
import { mkdtempSync, readFileSync, writeFileSync } from 'node:fs';
import { request as httpRequest } from 'node:http';
import { createServer } from 'node:net';
import { tmpdir } from 'node:os';
import { basename, join } from 'node:path';
import { after, before, describe, it } from 'node:test';

import { conformance } from './conformance.ts';

const RUNFILES = process.env['RUNFILES_DIR'] ?? process.env['TEST_SRCDIR'] ?? '';
const SERVER = join(RUNFILES, '_main', 'sdlc-server', 'server');

const freePort = (): Promise<number> => new Promise((resolve, reject) => {
  const s = createServer();
  s.once('error', reject);
  s.listen(0, '127.0.0.1', () => {
    const port = (s.address() as { port: number }).port;
    s.close(() => resolve(port));
  });
});

let server: ChildProcess | undefined;
let api = '';
let log = '';
const repo = mkdtempSync(join(process.env['TEST_TMPDIR'] ?? tmpdir(), 'sdlc-repo-'));

before(async () => {
  const port = await freePort();
  server = spawn(SERVER, ['--port', String(port), '--repo', repo, '--user', 'local:Local User'], {
    env: { ...process.env, RUNFILES_DIR: RUNFILES, JAVA_RUNFILES: RUNFILES },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  server.stdout?.on('data', (d) => { log += d; });
  server.stderr?.on('data', (d) => { log += d; });
  const until = Date.now() + 60_000;
  for (;;) {
    const up = await fetch(`http://127.0.0.1:${port}/health`).then((r) => r.ok, () => false);
    if (up) break;
    if (Date.now() > until || server.exitCode !== null) throw new Error(`sdlc-server did not start:\n${log.slice(-2000)}`);
    await new Promise((r) => setTimeout(r, 250));
  }
  api = `http://127.0.0.1:${port}/sdlc/api`;
});

after(() => {
  if (server?.pid === undefined) return;
  // On Windows the server is Bazel's launcher, whose child is the JVM: taskkill /T takes the tree
  // (query-store/test/lite.test.ts has the history).
  if (process.platform === 'win32') {
    if (server.exitCode === null && server.signalCode === null) {
      const r = spawnSync('taskkill', ['/pid', String(server.pid), '/t', '/f'], { encoding: 'utf8' });
      if (r.error) throw new Error(`taskkill did not run: ${r.error.message}`);
      if (r.status !== 0) throw new Error(`taskkill did not stop the server (status ${r.status}): ${r.stdout}${r.stderr}`.trim());
    }
  } else server.kill();
});

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

/** git itself, over the server's repository: its output, or the failure in full (one that never ran, too). */
function git(...args: string[]): string {
  const r = spawnSync(process.env['GIT'] ?? 'git', ['--git-dir', repo, ...args], { encoding: 'utf8' });
  if (r.error) throw new Error(`git ${args[0]} did not run: ${r.error.message}`);
  assert.equal(r.status, 0, `git ${args.join(' ')}:\n${r.stdout}${r.stderr}`);
  return r.stdout;
}

describe('sdlc-server: the repository it writes is git\'s', () => {
  it('passes git fsck, and git reads the history the suite saved', async () => {
    git('fsck', '--strict', '--no-dangling');
    const refs = git('for-each-ref', '--format=%(refname)');
    const workspace = refs.split('\n').find((r) => r.endsWith('/workspace/local/w1'));
    assert.ok(workspace, refs);
    assert.deepEqual(git('log', '--format=%s', workspace).trim().split('\n'), ['drop', 'email', 'add the party', 'Build project structure']);
    // git on Windows may write CRLF: the file's own text is compared, line ends aside
    assert.match(git('show', `${workspace}:demo/party/Person.pure`).replace(/\r\n/g, '\n'), /^\/\/ a person, as the demo writes one\nClass demo::party::Person/);
  });
});
