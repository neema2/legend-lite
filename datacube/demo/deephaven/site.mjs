// THE RISK DEMO'S SITE (docs/DEEPHAVEN_PLAN_2026_10_22.md D0, DH23, DH24), for the tests (test/deephaven/cube.mjs,
// board.mjs) and the benchmarks (bench/deephaven/) -- its hand tool went 2026-10-26, the trading desk the one demo --
// and every Deephaven hand tool's and test's server (startDeephaven): the pinned Deephaven started with the demo's
// application, and DataCube on Deephaven (cube.html) served at / on the loopback from Bazel's runfiles, beside the
// planner's module and the server's address and key (config.json).

import { spawn } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { createServer } from 'node:http';
import { tmpdir } from 'node:os';
import { runfileFromEnv, runfileNamed } from '../../../tools/js/runfiles.mts';

/** Starts the demo's Deephaven (or the fixture `fixture` names, with `args` and `env` beside the process's own):
 *  `next()` is the fixture's next JSON line (its address and key first, then a restart's word); `restart()` restarts
 *  the server; `stop()` stops it. */
export function startDeephaven({ quiet = false, fixture = 'DEEPHAVEN_FIXTURE', args = [], env: extra = {} } = {}) {
  // `bazel run` gives no TEST_TMPDIR: the server's data goes under the system's temporary directory
  const env = { ...process.env, TEST_TMPDIR: process.env.TEST_TMPDIR ?? tmpdir(), ...extra };
  const server = spawn(runfileFromEnv(fixture), args, { stdio: ['pipe', 'pipe', 'pipe'], env });
  server.stderr.on('data', (b) => {
    for (const line of String(b).split('\n')) {
      if (line && !(quiet && / INFO | DEBUG /.test(line))) process.stderr.write(`[deephaven] ${line}\n`);
    }
  });
  let said = '';
  const lines = [];
  server.stdout.on('data', (b) => {
    said += String(b);
    const all = said.split('\n');
    said = all.pop() ?? '';
    for (const l of all) if (l.startsWith('{')) lines.push(JSON.parse(l));
  });
  const next = async (what) => {
    for (let i = 0; i < 1800 && lines.length === 0; i++) {
      if (server.exitCode !== null) throw new Error(`the Deephaven fixture exited (${server.exitCode}) ${what}`);
      await new Promise((r) => setTimeout(r, 100));
    }
    if (lines.length === 0) throw new Error(`the fixture said nothing ${what}`);
    return lines.shift();
  };
  return {
    next,
    /** The fixture's process: the server runs in it or under it (a benchmark reads their CPU). */
    pid: server.pid,
    restart: () => server.stdin.write('restart\n'),
    stop: async () => {
      server.stdin.end();
      await new Promise((r) => setTimeout(r, 2000));
      if (server.exitCode === null) server.kill('SIGKILL');
    },
  };
}

/** Serves the demo's pages on the loopback, at `port` (0: any free one), with `config` as config.json. */
export async function serveDemo(config, port = 0) {
  const style = (name) => ['text/css', readFileSync(runfileNamed('CUBE_STYLES', name))];
  const files = {
    '/': ['text/html', readFileSync(runfileFromEnv('CUBE_PAGE'))],
    '/cube.js': ['text/javascript', readFileSync(runfileFromEnv('CUBE_BUNDLE'))],
    // the page of several grids and charts (V2, V3)
    '/board.html': ['text/html', readFileSync(runfileFromEnv('BOARD_PAGE'))],
    '/board.js': ['text/javascript', readFileSync(runfileFromEnv('BOARD_BUNDLE'))],
    '/app.css': style('app.css'),
    '/grid.css': style('grid.css'),
    '/theme.css': style('theme.css'),
    '/wasm-gc-module-runtime.js': ['text/javascript', readFileSync(runfileNamed('PLANNER', 'wasm-gc-module-runtime.js'))],
    '/classes.wasm': ['application/wasm', readFileSync(runfileNamed('PLANNER', 'classes.wasm'))],
    '/config.json': ['application/json', JSON.stringify({ url: config.url, psk: config.psk })],
  };
  const site = createServer((req, res) => {
    const f = files[new URL(req.url, 'http://page').pathname]; // portable: an HTTP route, never a file's path
    if (!f) {
      res.writeHead(404);
      res.end();
      return;
    }
    res.writeHead(200, { 'content-type': f[0] });
    res.end(f[1]);
  });
  const at = await new Promise((done) => site.listen(port, '127.0.0.1', () => done(site.address().port)));
  return { url: `http://127.0.0.1:${at}`, close: () => site.close() };
}
