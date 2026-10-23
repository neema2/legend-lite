// THE RISK DEMO'S SITE (docs/DEEPHAVEN_PLAN_2026_10_22.md D0, DH23), for the hand tool (serve.mjs) and the tests
// (test/deephaven/demo.mjs, cube.mjs): the pinned Deephaven started with the demo's application, and its two pages served
// on the loopback -- the plain live grid (/) and DataCube on Deephaven (/cube.html) -- each from Bazel's runfiles, beside
// the planner's module and the server's address and key (config.json).

import { spawn } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { createServer } from 'node:http';
import { runfileFromEnv, runfileNamed } from '../../../tools/js/runfiles.mts';

/** Starts the demo's Deephaven: `next()` is the fixture's next JSON line (its address and key first, then a restart's
 *  word); `restart()` restarts the server; `stop()` stops it. */
export function startDeephaven({ quiet = false } = {}) {
  // `bazel run` gives no TEST_TMPDIR: the server's data goes under the system's temporary directory
  const env = { ...process.env, TEST_TMPDIR: process.env.TEST_TMPDIR ?? process.env.TMPDIR ?? '/tmp' };
  const server = spawn(runfileFromEnv('DEEPHAVEN_FIXTURE'), [], { stdio: ['pipe', 'pipe', 'pipe'], env });
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
    '/': ['text/html', readFileSync(runfileFromEnv('DEMO_PAGE'))],
    '/risk.js': ['text/javascript', readFileSync(runfileFromEnv('DEMO_BUNDLE'))],
    '/cube.html': ['text/html', readFileSync(runfileFromEnv('CUBE_PAGE'))],
    '/cube.js': ['text/javascript', readFileSync(runfileFromEnv('CUBE_BUNDLE'))],
    '/app.css': style('app.css'),
    '/grid.css': style('grid.css'),
    '/theme.css': style('theme.css'),
    '/wasm-gc-module-runtime.js': ['text/javascript', readFileSync(runfileNamed('PLANNER', 'wasm-gc-module-runtime.js'))],
    '/classes.wasm': ['application/wasm', readFileSync(runfileNamed('PLANNER', 'classes.wasm'))],
    '/config.json': ['application/json', JSON.stringify({ url: config.url, psk: config.psk })],
  };
  const site = createServer((req, res) => {
    const f = files[new URL(req.url, 'http://page').pathname];
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
