// legend-lite's server (`//core:server --query-store`), held to the one suite: the store the page
// answers is the store a server answers.

import { spawn, spawnSync, type ChildProcess } from 'node:child_process';
import { mkdtempSync } from 'node:fs';
import { createServer } from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { after, before } from 'node:test';

import { conformance } from './conformance.ts';

const RUNFILES = process.env['RUNFILES_DIR'] ?? process.env['TEST_SRCDIR'] ?? '';
const SERVER = join(RUNFILES, '_main', 'core', 'server');

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

before(async () => {
  const port = await freePort();
  const store = mkdtempSync(join(process.env['TEST_TMPDIR'] ?? tmpdir(), 'query-store-'));
  server = spawn(SERVER, [String(port), '--query-store', store], {
    env: { ...process.env, RUNFILES_DIR: RUNFILES, JAVA_RUNFILES: RUNFILES },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  server.stdout?.on('data', (d) => { log += d; });
  server.stderr?.on('data', (d) => { log += d; });
  const until = Date.now() + 60_000;
  for (;;) {
    const up = await fetch(`http://127.0.0.1:${port}/health`).then((r) => r.ok, () => false);
    if (up) break;
    if (Date.now() > until || server.exitCode !== null) throw new Error(`legend-lite did not start:\n${log.slice(-2000)}`);
    await new Promise((r) => setTimeout(r, 250));
  }
  api = `http://127.0.0.1:${port}/api`;
});

after(() => {
  if (server?.pid === undefined) return;
  // On Windows the server is Bazel's launcher (server.exe), whose child is the JVM: a kill stops the
  // launcher alone, and the orphaned JVM kept serving and held the pipes open, so the suite never
  // ended (a 60 s TIMEOUT, 2026-10-02). taskkill /T takes the tree. Elsewhere the launcher execs java.
  if (process.platform === 'win32') {
    // taskkill takes the PID of a launcher that is still running: once it has exited, Node has released
    // its handle and Windows may have given the PID to another process, whose tree /t /f would then stop.
    if (server.exitCode === null && server.signalCode === null) {
      const r = spawnSync('taskkill', ['/pid', String(server.pid), '/t', '/f'], { encoding: 'utf8' });
      // a taskkill that failed leaves the JVM serving: the TIMEOUT above, so it is not ignored
      if (r.error) throw new Error(`taskkill did not run: ${r.error.message}`);
      if (r.status !== 0) {
        throw new Error(`taskkill did not stop the server (status ${r.status}): ${r.stdout}${r.stderr}`.trim());
      }
    }
  } else server.kill();
});

conformance("legend-lite's server", () => ({ api, fetch: globalThis.fetch.bind(globalThis), user: 'anonymous' }));
