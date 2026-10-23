// THE LIVE RISK DEMO, by hand (docs/DEEPHAVEN_PLAN_2026_10_22.md D0):
//
//   bazel run //datacube:deephaven_demo            then open the URL it prints
//
// It starts the pinned Deephaven with the demo's start-up script (its reference data and price simulator, marked
// temporary), serves the live risk page on the loopback, and prints the page's address. The page does everything else
// in the browser: it plans the risk view with legend-lite's WebAssembly planner, sends it straight to Deephaven, and
// streams the grid. Type `restart` and Enter to restart Deephaven and watch the page come back by itself; Ctrl-C stops.
// The test server's certificate is its own: the browser asks once to accept it (open the Deephaven URL it prints
// first, or start the browser with --ignore-certificate-errors).

import { spawn } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { createServer } from 'node:http';
import { createInterface } from 'node:readline';
import { runfileFromEnv, runfileNamed } from '../../../tools/js/runfiles.mts';

const fixture = runfileFromEnv('DEEPHAVEN_FIXTURE');
const files = {
  '/': ['text/html', readFileSync(runfileFromEnv('DEMO_PAGE'))],
  '/risk.js': ['text/javascript', readFileSync(runfileFromEnv('DEMO_BUNDLE'))],
  '/wasm-gc-module-runtime.js': ['text/javascript', readFileSync(runfileNamed('PLANNER', 'wasm-gc-module-runtime.js'))],
  '/classes.wasm': ['application/wasm', readFileSync(runfileNamed('PLANNER', 'classes.wasm'))],
};

// bazel run gives no TEST_TMPDIR: the server's data goes under the system's temporary directory
const env = { ...process.env, TEST_TMPDIR: process.env.TEST_TMPDIR ?? (await import('node:os')).tmpdir() };
const server = spawn(fixture, [], { stdio: ['pipe', 'pipe', 'inherit'], env });
let said = '';
const config = await new Promise((done, fail) => {
  server.stdout.on('data', (b) => {
    said += String(b);
    for (const line of said.split('\n')) {
      if (line.startsWith('{"url"')) done(JSON.parse(line));
      else if (line.startsWith('{"restarted"')) console.log('Deephaven is back: the page reconnects by itself.');
    }
    said = said.slice(said.lastIndexOf('\n') + 1);
  });
  server.on('exit', (code) => fail(new Error(`the Deephaven fixture exited (${code})`)));
});
files['/config.json'] = ['application/json', JSON.stringify({ url: config.url, psk: config.psk })];

const port = Number(process.argv.find((a) => a.startsWith('--port='))?.slice(7) ?? 0);
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
console.log(`\nDeephaven: ${config.url} (its certificate is the test server's own: accept it once)`);
console.log(`The live risk page: http://127.0.0.1:${at}/\n`);
console.log('Type restart and Enter to restart Deephaven; Ctrl-C to stop.');

const input = createInterface({ input: process.stdin });
input.on('line', (line) => {
  if (line.trim() === 'restart') {
    console.log('Restarting Deephaven...');
    server.stdin.write('restart\n');
  }
});
const stop = () => {
  server.stdin.end();
  site.close();
  setTimeout(() => process.exit(0), 3000);
};
process.on('SIGINT', stop);
process.on('SIGTERM', stop);
