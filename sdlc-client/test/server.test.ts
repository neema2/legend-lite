// sdlc-server over HTTP (`//sdlc-server:server`, a git repository on disk), held to the one suite -- the
// SDLC the page runs compiled to WebAssembly is the SDLC a server answers. Then git itself checks the
// repository the server wrote: real objects, real refs.

import assert from 'node:assert/strict';
import { spawn, spawnSync, type ChildProcess } from 'node:child_process';
import { mkdtempSync } from 'node:fs';
import { createServer } from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
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

describe('sdlc-server: the repository it writes is git\'s', () => {
  it('passes git fsck, and git reads the history the suite saved', async () => {
    const fsck = spawnSync('git', ['--git-dir', repo, 'fsck', '--strict', '--no-dangling'], { encoding: 'utf8' });
    assert.equal(fsck.status, 0, `${fsck.stdout}${fsck.stderr}`);
    const refs = spawnSync('git', ['--git-dir', repo, 'for-each-ref', '--format=%(refname)'], { encoding: 'utf8' });
    const workspace = refs.stdout.split('\n').find((r) => r.endsWith('/workspace/local/w1'));
    assert.ok(workspace, refs.stdout);
    const history = spawnSync('git', ['--git-dir', repo, 'log', '--format=%s', workspace], { encoding: 'utf8' });
    assert.deepEqual(history.stdout.trim().split('\n'), ['drop', 'email', 'add the party', 'Build project structure']);
    const file = spawnSync('git', ['--git-dir', repo, 'show', `${workspace}:demo/party/Person.pure`], { encoding: 'utf8' });
    assert.match(file.stdout, /^\/\/ a person, as the demo writes one\nClass demo::party::Person/);
  });
});
