// The model home's Depot over HTTP (`//sdlc-server:server`, Depot at /depot/api beside the SDLC, over a
// git repository on disk) -- held to the one suite.

import { spawn, spawnSync, type ChildProcess } from 'node:child_process';
import { mkdtempSync } from 'node:fs';
import { createServer } from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { after, before } from 'node:test';

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
let base = '';
let log = '';

before(async () => {
  const port = await freePort();
  const repo = mkdtempSync(join(process.env['TEST_TMPDIR'] ?? tmpdir(), 'depot-repo-'));
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
    if (Date.now() > until || server.exitCode !== null) throw new Error(`the model home did not start:\n${log.slice(-2000)}`);
    await new Promise((r) => setTimeout(r, 250));
  }
  base = `http://127.0.0.1:${port}`;
});

after(() => {
  if (server?.pid === undefined) return;
  // Windows: the launcher's child is the JVM; taskkill /T takes the tree (query-store/test/lite.test.ts)
  if (process.platform === 'win32') {
    if (server.exitCode === null && server.signalCode === null) {
      const r = spawnSync('taskkill', ['/pid', String(server.pid), '/t', '/f'], { encoding: 'utf8' });
      if (r.error) throw new Error(`taskkill did not run: ${r.error.message}`);
      if (r.status !== 0) throw new Error(`taskkill did not stop the server (status ${r.status}): ${r.stdout}${r.stderr}`.trim());
    }
  } else server.kill();
});

const network = globalThis.fetch.bind(globalThis);
conformance('the model home over HTTP', () => ({
  sdlc: { api: `${base}/sdlc/api`, fetch: network },
  depot: { api: `${base}/depot/api`, fetch: network },
}));
