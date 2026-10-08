// LIVE VERSUS SNAP, run in the pinned Chromium (//datacube:live_snap_test). Node here only drives: it starts the
// Bazel-built native warehouse serving DataCube's site (`--site`, so the page and the warehouse are one origin), hands
// the browser the test page (Playwright's routing, for /live-snap.html and its bundle), and reports what the page found
// (test/live-snap/page.ts: every check runs there, on DuckDB-WASM in its worker and the compiler in its worker).

// first: points Playwright at the Chromium Bazel fetched
import '../../../tools/browser/pinned-chromium.mjs';
import { spawn } from 'node:child_process';
import { mkdtempSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { chromium } from 'playwright';
import { runfileFromEnv } from '../../../tools/js/runfiles.mts';

const binary = runfileFromEnv('WAREHOUSE_BINARY');
const library = runfileFromEnv('WAREHOUSE_DUCKDB_LIBRARY');
const site = runfileFromEnv('DATACUBE_SITE');
const bundle = readFileSync(runfileFromEnv('LIVE_SNAP_PAGE'));
// the test's own temp directory (Bazel's TEST_TMPDIR), never the host's
const data = mkdtempSync(join(process.env.TEST_TMPDIR ?? tmpdir(), 'live-snap-'));

const server = spawn(binary, ['--port', '0', '--data', data, '--site', site, '--user', 'alice:alice-pw',
  '--user', 'rita:rita-pw', '--owner', 'alice', '--duckdb-library', library], { stdio: ['ignore', 'ignore', 'pipe'] });
// the warehouse's own account, in this test's log: a server-side reason is then never invisible
let said = '';
const port = await new Promise((done, fail) => {
  server.stderr.on('data', (b) => {
    said += String(b);
    for (const line of String(b).split('\n')) if (line) process.stderr.write(`[warehouse] ${line}\n`);
    const m = /listening on 127\.0\.0\.1:(\d+)/.exec(said);
    if (m) done(Number(m[1]));
  });
  server.on('exit', (code) => fail(new Error(`the warehouse exited (${code}): ${said}`)));
});
const stopped = new Promise((done) => server.once('exit', done));
const origin = `http://127.0.0.1:${port}`;

let failed = false;
let browser;
try {
  browser = await chromium.launch();
  const page = await browser.newPage();
  page.on('pageerror', (e) => { console.log(`page error: ${e.message}`); failed = true; });
  page.on('console', (m) => { if (m.type() === 'error') console.log(`page console: ${m.text()}`); });
  // a request that never completed, said with what the browser knows of why (a page error alone names no URL)
  page.on('requestfailed', (r) => console.log(`request failed: ${r.url()} -- ${r.failure()?.errorText ?? ''}`));
  page.on('worker', (w) => console.log(`worker started: ${w.url()}`));
  // the test page and its bundle; everything else (the site, its vendor/ assets, /sql/v1) is the warehouse's
  await page.route(`${origin}/live-snap.html`, (r) => r.fulfill({
    contentType: 'text/html',
    body: '<!doctype html><meta charset="utf-8"><title>live versus snap</title><script type="module" src="live-snap.js"></script>',
  }));
  await page.route(`${origin}/live-snap.js`, (r) => r.fulfill({ contentType: 'text/javascript', body: bundle }));
  await page.goto(`${origin}/live-snap.html`);
  await page.waitForFunction(() => window.__liveSnap?.done === true, undefined, { timeout: 280_000 }).catch(async (e) => {
    console.log(`the page did not finish; it was at: ${await page.evaluate(() => window.__liveSnapStage ?? '(no stage)')}`);
    throw e;
  });
  const verdict = await page.evaluate(() => window.__liveSnap);
  for (const o of verdict.outcomes) {
    console.log(`${o.ok ? 'ok  ' : 'FAIL'} ${o.name} (${Math.round(o.ms)} ms)${o.ok ? '' : `\n${o.message}`}`);
    if (!o.ok) failed = true;
  }
  if (verdict.setupError) {
    console.log(`FAIL the setup: ${verdict.setupError}`);
    failed = true;
  }
  if (verdict.outcomes.length !== 6) {
    console.log(`FAIL ${verdict.outcomes.length} checks ran, not 6`);
    failed = true;
  }
} finally {
  await browser?.close();
  // stopped and WAITED for: the process is gone before the test reports (Bazel workplan P3-10)
  server.kill();
  await stopped;
}
process.exit(failed ? 1 : 0);
