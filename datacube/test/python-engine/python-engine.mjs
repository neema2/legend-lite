// DATACUBE AGAINST PYTHON'S ENGINE, run in the pinned Chromium (//datacube:python_engine_test). Node here only drives:
// it starts Python's engine (engine_fixture.py, beside it: the cube corpus's rows as a Live frame, DataCube's site
// served beside the API, so the page and the engine are one origin), hands the browser the test page (Playwright's
// routing, for /python-engine.html, its bundle and upstream's recorded Arrow answer) with the engine's token, and
// reports what the page found (test/python-engine/page.ts: every check runs there).

// first: points Playwright at the Chromium Bazel fetched
import '../../../tools/browser/pinned-chromium.mjs';
import { spawn } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { createInterface } from 'node:readline';
import { chromium } from 'playwright';
import { runfileFromEnv } from '../../../tools/js/runfiles.mts';

const fixture = runfileFromEnv('ENGINE_FIXTURE');
const site = runfileFromEnv('DATACUBE_SITE');
const bundle = readFileSync(runfileFromEnv('PYTHON_ENGINE_PAGE'));
const upstream = readFileSync(runfileFromEnv('UPSTREAM_ARROW_ANSWER'));

// the library by its runfiles path, as //python's tests are given it: a program a test starts gets no BUILD env
const engine = spawn(fixture, ['--site', site], {
  stdio: ['pipe', 'pipe', 'pipe'],
  env: { ...process.env, LEGEND_LITE_LIBRARY: runfileFromEnv('LEGEND_LITE_LIBRARY'), PYTHONUTF8: '1',
    PYTHONDONTWRITEBYTECODE: '1' },
});
// the engine's own account, in this test's log: a server-side reason is then never invisible
engine.stderr.on('data', (b) => { for (const line of String(b).split('\n')) if (line) process.stderr.write(`[engine] ${line}\n`); });
const stopped = new Promise((done) => engine.once('exit', done));
const served = await new Promise((done, fail) => {
  createInterface({ input: engine.stdout }).once('line', (line) => done(JSON.parse(line)));
  engine.once('exit', (code) => fail(new Error(`the engine exited (${code}) before it served`)));
});
const origin = served.url;

let failed = false;
let browser;
try {
  browser = await chromium.launch();
  const page = await browser.newPage();
  page.on('pageerror', (e) => { console.log(`page error: ${e.message}`); failed = true; });
  page.on('console', (m) => { if (m.type() === 'error') console.log(`page console: ${m.text()}`); });
  page.on('requestfailed', (r) => console.log(`request failed: ${r.url()} -- ${r.failure()?.errorText ?? ''}`));
  // what the page is handed: the token, and the frame the engine serves (the address is the page's own origin)
  const { url: _origin, ...given } = served;
  await page.addInitScript((g) => { window.__pythonEngineServed = g; }, given);
  // the test page, its bundle and upstream's recorded answer; everything else (the site, the API) is the engine's
  await page.route(`${origin}/python-engine.html`, (r) => r.fulfill({
    contentType: 'text/html',
    body: '<!doctype html><meta charset="utf-8"><title>DataCube on Python</title><script type="module" src="python-engine.js"></script>',
  }));
  await page.route(`${origin}/python-engine.js`, (r) => r.fulfill({ contentType: 'text/javascript', body: bundle }));
  await page.route(`${origin}/upstream.arrows.zst`, (r) => r.fulfill({ contentType: 'application/octet-stream', body: upstream }));
  await page.goto(`${origin}/python-engine.html`);
  await page.waitForFunction(() => window.__pythonEngine?.done === true, undefined, { timeout: 280_000 }).catch(async (e) => {
    console.log(`the page did not finish; it was at: ${await page.evaluate(() => window.__pythonEngineStage ?? '(no stage)')}`);
    throw e;
  });
  const verdict = await page.evaluate(() => window.__pythonEngine);
  for (const o of verdict.outcomes) {
    console.log(`${o.ok ? 'ok  ' : 'FAIL'} ${o.name} (${Math.round(o.ms)} ms)${o.ok ? '' : `\n${o.message}`}`);
    if (!o.ok) failed = true;
  }
  if (verdict.setupError) {
    console.log(`FAIL the setup: ${verdict.setupError}`);
    failed = true;
  }
  if (verdict.outcomes.length !== 4) {
    console.log(`FAIL ${verdict.outcomes.length} checks ran, not 4`);
    failed = true;
  }

  // THE PAGE A PERSON OPENS (demo/engine.html, from the engine's site): the link names the frame and carries the
  // token in its fragment; the cube opens over the engine and shows the frame's rows, with nothing run in the tab
  const cube = await browser.newPage();
  cube.on('pageerror', (e) => { console.log(`engine page error: ${e.message}`); failed = true; });
  cube.on('requestfailed', (r) => console.log(`engine page request failed: ${r.url()} -- ${r.failure()?.errorText ?? ''}`));
  cube.on('console', (m) => console.log(`engine page console ${m.type()}: ${m.text()}`));
  // every file and call the page asks for is answered: a refusal (a missing file, a refused call) fails the test
  cube.on('response', (r) => {
    if (r.status() >= 400) { console.log(`FAIL engine page answer ${r.status()}: ${r.url()}`); failed = true; }
  });
  const token = served.authorization.slice('Bearer '.length);
  await cube.goto(`${origin}/engine.html?table=trades#token=${encodeURIComponent(token)}`);
  const shown = await cube.waitForFunction(() => /\b10 rows\b/.test(document.body.innerText)
    && document.body.innerText.includes('APAC'), undefined, { timeout: 60_000 }).then(() => true, () => false);
  console.log(`${shown ? 'ok  ' : 'FAIL'} the engine's page opens the frame's cube and shows its rows`);
  if (!shown) {
    console.log(`  the page showed: ${(await cube.evaluate(() => document.body.innerText)).slice(0, 400)}`);
    failed = true;
  }
} finally {
  await browser?.close();
  // stopped and WAITED for: the engine serves until its input closes, and is gone before the test reports
  engine.stdin.end();
  await stopped;
}
process.exit(failed ? 1 : 0);
