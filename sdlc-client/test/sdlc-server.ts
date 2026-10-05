// The model home's server (`//sdlc-server:server`) for a test: started over a fresh repository on a free port,
// and stopped -- the one way sdlc-client's and depot-client's server tests run it. The launcher comes by runfiles
// path (SDLC_SERVER, Bazel workplan P1-24); the stop is query-store/test/lite.test.ts's, whose history it keeps.

import { spawn, spawnSync, type ChildProcess } from 'node:child_process';
import { mkdtempSync } from 'node:fs';
import { createServer } from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { runfileFromEnv } from '../../tools/js/runfiles.mts';

// Windows' own taskkill by its full path: a test's PATH is Bazel's, not the desk's (Bazel workplan P1-08).
// SystemRoot is set on every Windows process.
const taskkill = (): string => {
  const root = process.env['SystemRoot'] ?? process.env['SYSTEMROOT'];
  if (!root) throw new Error('SystemRoot is not set: cannot find taskkill.exe');
  return join(root, 'System32', 'taskkill.exe');
};

const freePort = (): Promise<number> => new Promise((resolve, reject) => {
  const s = createServer();
  s.once('error', reject);
  s.listen(0, '127.0.0.1', () => {
    const port = (s.address() as { port: number }).port;
    s.close(() => resolve(port));
  });
});

export interface RunningServer {
  /** `http://127.0.0.1:<port>`: the SDLC at `/sdlc/api`, Depot at `/depot/api`. */
  readonly base: string;
  /** The repository it writes. */
  readonly repo: string;
  stop(): Promise<void>;
}

export async function startSdlcServer(name: string): Promise<RunningServer> {
  const runfiles = process.env['RUNFILES_DIR'] ?? process.env['TEST_SRCDIR'] ?? '';
  const port = await freePort();
  const repo = mkdtempSync(join(process.env['TEST_TMPDIR'] ?? tmpdir(), `${name}-repo-`));
  const server: ChildProcess = spawn(runfileFromEnv('SDLC_SERVER'), ['--port', String(port), '--repo', repo, '--user', 'local:Local User'], {
    env: { ...process.env, RUNFILES_DIR: runfiles, JAVA_RUNFILES: runfiles },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let log = '';
  server.stdout?.on('data', (d) => { log += d; });
  server.stderr?.on('data', (d) => { log += d; });
  const base = `http://127.0.0.1:${port}`;
  const health = `${base}/health`;
  const until = Date.now() + 60_000;
  for (;;) {
    const up = await fetch(health).then((r) => r.ok, () => false);
    if (up) break;
    if (Date.now() > until || server.exitCode !== null) throw new Error(`the model home did not start:\n${log.slice(-2000)}`);
    await new Promise((r) => setTimeout(r, 250));
  }

  const stop = async (): Promise<void> => {
    if (server.pid === undefined) return;
    // On Windows the server is Bazel's launcher (server.exe), whose child is the JVM: a kill stops the launcher
    // alone, and the orphaned JVM keeps serving and holds the pipes open. taskkill /T takes the tree. Elsewhere
    // the launcher execs java.
    if (process.platform !== 'win32') {
      server.kill();
      return;
    }
    // taskkill takes the PID of a launcher that is still running: once it has exited, Windows may have given the
    // PID to another process, whose tree /t /f would then stop.
    if (server.exitCode !== null || server.signalCode !== null) return;
    const r = spawnSync(taskkill(), ['/pid', String(server.pid), '/t', '/f'], { encoding: 'utf8' });
    if (r.error) throw new Error(`taskkill did not run: ${r.error.message}`);
    // taskkill's status is not the verdict (it reports 255 for a tree it did stop): the outcome is -- the launcher
    // has exited and nothing answers on the port.
    const deadline = Date.now() + 10_000;
    while (server.exitCode === null && server.signalCode === null && Date.now() < deadline) {
      await new Promise((res) => setTimeout(res, 100));
    }
    const exited = server.exitCode !== null || server.signalCode !== null;
    const answers = await fetch(health).then(() => true, () => false);
    if (!exited || answers) {
      throw new Error(`taskkill did not stop the server (status ${r.status}; launcher exited: ${exited}; `
        + `port answers: ${answers}): ${r.stdout}${r.stderr}`.trim());
    }
  };
  return { base, repo, stop };
}
