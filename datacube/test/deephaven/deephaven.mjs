// DEEPHAVEN FROM THE BROWSER, run in the pinned Chromium (//datacube:deephaven_test; docs/DEEPHAVEN_PLAN_2026_10_22.md
// DH21, L3b). Node here only drives: it starts the pinned Deephaven server (//testing EmbeddedDeephaven: HTTPS on a free
// port, a key made for this run), serves the test page from an origin that is not the server's (its own HTTP server on
// 127.0.0.1, port 0: the page, its bundle and //wasm:planner's module), and reports what the page found
// (test/deephaven/page.ts: every check runs there, against the server, with nothing of legend-lite's in between).

// first: points Playwright at the Chromium Bazel fetched
import '../../../tools/browser/pinned-chromium.mjs';
import { spawn } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { createServer } from 'node:http';
import { chromium } from 'playwright';
import { runfileFromEnv, runfileNamed } from '../../../tools/js/runfiles.mts';

const fixture = runfileFromEnv('DEEPHAVEN_FIXTURE');
const bundle = readFileSync(runfileFromEnv('DEEPHAVEN_PAGE'));
const module = readFileSync(runfileNamed('PLANNER', 'classes.wasm'));
const runtime = readFileSync(runfileNamed('PLANNER', 'wasm-gc-module-runtime.js'));

const server = spawn(fixture, [], { stdio: ['pipe', 'pipe', 'pipe'], env: { ...process.env } });
let said = '';
server.stderr.on('data', (b) => {
  for (const line of String(b).split('\n')) if (line) process.stderr.write(`[deephaven] ${line}\n`);
});
const config = await new Promise((done, fail) => {
  server.stdout.on('data', (b) => {
    said += String(b);
    const line = said.split('\n').find((l) => l.startsWith('{'));
    if (line) done(JSON.parse(line));
  });
  server.on('exit', (code) => fail(new Error(`the Deephaven fixture exited (${code}): ${said}`)));
});
const stopped = new Promise((done) => server.once('exit', done));

const files = {
  '/deephaven.html': ['text/html', '<!doctype html><meta charset="utf-8"><title>Deephaven from the browser</title>'
    + '<script type="module" src="deephaven.js"></script>'],
  '/deephaven.js': ['text/javascript', bundle],
  '/wasm-gc-module-runtime.js': ['text/javascript', runtime],
  '/classes.wasm': ['application/wasm', module],
  '/config.json': ['application/json', JSON.stringify({ url: config.url, psk: config.psk })],
};
// the page's own origin, not the server's (http, another port), so every call to Deephaven is cross-origin, as
// DataCube's are. Served for real, on the loopback: a page Playwright routes counts as a PUBLIC site, and Chrome keeps a
// public site's page from a loopback or private address without the person's permission (its Local Network Access
// rule, measured on the pinned Chromium: "Permission was denied for this request to access the `loopback` address
// space"), as it does not keep one internal site's page from another's
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
const sitePort = await new Promise((done) => site.listen(0, '127.0.0.1', () => done(site.address().port)));
const origin = `http://127.0.0.1:${sitePort}`;

let failed = false;
let browser;
try {
  browser = await chromium.launch();
  // the test server's certificate is its own, made for this run (EmbeddedDeephaven)
  const page = await (await browser.newContext({ ignoreHTTPSErrors: true })).newPage();
  page.on('pageerror', (e) => { console.log(`page error: ${e.message}`); failed = true; });
  page.on('console', (m) => { if (m.type() === 'error') console.log(`page console: ${m.text()}`); });
  page.on('requestfailed', (r) => console.log(`request failed: ${r.url()} -- ${r.failure()?.errorText ?? ''}`));
  await page.goto(`${origin}/deephaven.html`);
  await page.waitForFunction(() => window.__deephaven?.done === true, undefined, { timeout: 240_000 });
  const verdict = await page.evaluate(() => window.__deephaven);
  for (const o of verdict.outcomes) {
    console.log(`${o.ok ? 'ok  ' : 'FAIL'} ${o.name}${o.ok ? '' : `\n${o.message}`}`);
    if (!o.ok) failed = true;
  }
  if (verdict.setupError) {
    console.log(`FAIL the setup: ${verdict.setupError}`);
    failed = true;
  }
  if (verdict.outcomes.length !== 23) {
    console.log(`FAIL ${verdict.outcomes.length} checks ran, not 23`);
    failed = true;
  }
} finally {
  await browser?.close();
  site.close();
  server.stdin.end();
  await Promise.race([stopped, new Promise((r) => setTimeout(r, 30_000))]);
  if (server.exitCode === null) server.kill('SIGKILL');
}
process.exit(failed ? 1 : 0);
