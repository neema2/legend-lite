// legend-lite's server (`//core:server --query-store`), held to the one suite: the store the page
// answers is the store a server answers.

import { spawn, spawnSync, type ChildProcess } from 'node:child_process';
import { mkdtempSync } from 'node:fs';
import { createServer } from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { after, before } from 'node:test';

import { conformance } from './conformance.ts';
import { runfileFromEnv } from '../../tools/js/runfiles.mts';

// Windows' own taskkill by its full path: a test's PATH is Bazel's, not the desk's (Bazel workplan P1-08
// removed CI's --test_env=PATH). SystemRoot is set on every Windows process.
const taskkill = (): string => {
  const root = process.env['SystemRoot'] ?? process.env['SYSTEMROOT'];
  if (!root) throw new Error('SystemRoot is not set: cannot find taskkill.exe');
  return join(root, 'System32', 'taskkill.exe');
};

const RUNFILES = process.env['RUNFILES_DIR'] ?? process.env['TEST_SRCDIR'] ?? '';
const SERVER = runfileFromEnv('LEGEND_SERVER');

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
let health = '';
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
  health = `http://127.0.0.1:${port}/health`;
});

after(async () => {
  if (server?.pid === undefined) return;
  // On Windows the server is Bazel's launcher (server.exe), whose child is the JVM: a kill stops the
  // launcher alone, and the orphaned JVM kept serving and held the pipes open, so the suite never
  // ended (a 60 s TIMEOUT, 2026-10-02). taskkill /T takes the tree. Elsewhere the launcher execs java.
  if (process.platform === 'win32') {
    // taskkill takes the PID of a launcher that is still running: once it has exited, Node has released
    // its handle and Windows may have given the PID to another process, whose tree /t /f would then stop.
    if (server.exitCode === null && server.signalCode === null) {
      const r = spawnSync(taskkill(), ['/pid', String(server.pid), '/t', '/f'], { encoding: 'utf8' });
      if (r.error) throw new Error(`taskkill did not run: ${r.error.message}`);
      // taskkill's status is not the verdict: it stops the JVM first, the launcher may then exit on its own
      // before taskkill reaches it, and taskkill reports 255 for a tree it did stop (CI, 2026-10-03). The
      // verdict is the outcome: the launcher has exited and nothing answers on the port. A server still up
      // leaves the JVM serving: the TIMEOUT above, so it is not ignored.
      const until = Date.now() + 10_000;
      while (server.exitCode === null && server.signalCode === null && Date.now() < until) {
        await new Promise((res) => setTimeout(res, 100));
      }
      const exited = server.exitCode !== null || server.signalCode !== null;
      const answers = await fetch(health).then(() => true, () => false);
      if (!exited || answers) {
        throw new Error(`taskkill did not stop the server (status ${r.status}; launcher exited: ${exited}; `
          + `port answers: ${answers}): ${r.stdout}${r.stderr}`.trim());
      }
    }
  } else server.kill();
});

conformance("legend-lite's server", () => ({ api, fetch: globalThis.fetch.bind(globalThis), user: 'anonymous' }));
