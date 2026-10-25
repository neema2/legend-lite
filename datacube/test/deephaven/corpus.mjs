// THE STRESS CORPUS ON DEEPHAVEN, AGAINST THE ORACLE (//datacube:deephaven_corpus_test; docs/DEEPHAVEN_TRADER_DEMOS_
// 2026_10_24.md, task 15), run in the pinned Chromium. Node only drives: it starts the pinned Deephaven holding the
// corpus's tables (test/deephaven/corpus/corpus.groovy, from //core:stress_deephaven_cases' tables.json), serves the
// page that sends every planned test's plan to it (corpus-page.ts), collects each answer, one JSON line a test, and
// hands them to the stress suites' own judge (//core:stress_deephaven judge): each answer's rows against the test's
// expectation, as the stress suites judge DuckDB's and H2's. The count of passes is held to its floor.

// first: points Playwright at the Chromium Bazel fetched
import '../../../tools/browser/pinned-chromium.mjs';
import { spawnSync } from 'node:child_process';
import { appendFileSync, createReadStream, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { createServer } from 'node:http';
import { join } from 'node:path';
import { chromium } from 'playwright';
import { runfileFromEnv, runfileNamed } from '../../../tools/js/runfiles.mts';
import { startDeephaven } from '../../demo/deephaven/site.mjs';

/**
 * THE FLOOR: the corpus tests the Deephaven target answers as the oracle does, which a change may not lower. Moves only
 * with a dated note naming the task (AGENTS.md: guard files are load-bearing).
 *
 * 2026-10-25, task 15 (the lane's first floor): 4,191 of the 4,195 tests that plan on Deephaven pass. The four that do
 * not: CF_Confluence, O1_CounterpartyOtherwise and F32 fail on DuckDB too (the stress suites' own ledgers), and
 * TF_TapeEma's slow EMA differs from SQL's in its last digits (Deephaven's EMA decays by ticks: docs/DEEPHAVEN_TRADER_
 * DEMOS_2026_10_24.md §6). 653 have no plan on Deephaven (the judge's deephaven-without.txt, by reason).
 */
const FLOOR = 4191;

const outputs = process.env.TEST_UNDECLARED_OUTPUTS_DIR ?? process.env.TEST_TMPDIR ?? '.';
mkdirSync(outputs, { recursive: true });
const CASES = runfileNamed('CASES', 'cases.json');
const results = join(outputs, 'results.jsonl');
writeFileSync(results, '');

const deephaven = startDeephaven({ quiet: true, fixture: 'CORPUS_FIXTURE', env: { CORPUS_TABLES: runfileNamed('CASES', 'tables.json') } });
const config = await deephaven.next('starting');

const files = {
  '/corpus.html': ['text/html', '<!doctype html><meta charset="utf-8"><title>The corpus on Deephaven</title>'
    + '<script type="module" src="corpus.js"></script>'],
  '/corpus.js': ['text/javascript', readFileSync(runfileFromEnv('CORPUS_PAGE'))],
  '/wasm-gc-module-runtime.js': ['text/javascript', readFileSync(runfileNamed('PLANNER', 'wasm-gc-module-runtime.js'))],
  '/classes.wasm': ['application/wasm', readFileSync(runfileNamed('PLANNER', 'classes.wasm'))],
  '/config.json': ['application/json', JSON.stringify({ url: config.url, psk: config.psk })],
};
// the page's own origin, not the server's, served for real on the loopback (test/deephaven/deephaven.mjs says why)
const site = createServer((req, res) => {
  const path = new URL(req.url, 'http://page').pathname;
  if (path === '/cases.json') {
    res.writeHead(200, { 'content-type': 'application/json' });
    createReadStream(CASES).pipe(res);
    return;
  }
  if (path === '/results' && req.method === 'POST') {
    const parts = [];
    req.on('data', (b) => parts.push(b));
    req.on('end', () => {
      appendFileSync(results, Buffer.concat(parts));
      res.writeHead(204);
      res.end();
    });
    return;
  }
  const f = files[path];
  if (!f) {
    res.writeHead(404);
    res.end();
    return;
  }
  res.writeHead(200, { 'content-type': f[0] });
  res.end(f[1]);
});
const sitePort = await new Promise((done) => site.listen(0, '127.0.0.1', () => done(site.address().port)));

let failed = false;
let browser;
try {
  browser = await chromium.launch();
  const page = await (await browser.newContext({ ignoreHTTPSErrors: true })).newPage();
  page.on('pageerror', (e) => { console.log(`page error: ${e.message}`); failed = true; });
  page.on('console', (m) => { if (m.type() === 'error') console.log(`page console: ${m.text().slice(0, 300)}`); });
  const started = Date.now();
  await page.goto(`http://127.0.0.1:${sitePort}/corpus.html`);
  for (;;) {
    const v = await page.evaluate(() => window.__corpus);
    if (v?.done) break;
    if (Date.now() - started > 3_000_000) throw new Error('the corpus was not sent in 50 minutes');
    if (v) console.log(`     ${v.sent} sent, ${v.answered} answered, ${v.refused} refused (${((Date.now() - started) / 1000).toFixed(0)} s)`);
    await new Promise((r) => setTimeout(r, 30_000));
  }
  const sent = await page.evaluate(() => window.__corpus);
  if (sent.setupError) {
    console.log(`FAIL the setup: ${sent.setupError}`);
    failed = true;
  }
  console.log(`     ${sent.sent} plans sent in ${((Date.now() - started) / 1000).toFixed(0)} s: ${sent.answered} answered, ${sent.refused} refused by the server`);
  // THE JUDGE: the stress suites' own, over each answer
  const judged = spawnSync(runfileFromEnv('JUDGE'), ['judge', CASES, results, outputs], { encoding: 'utf8', maxBuffer: 1 << 26 });
  process.stdout.write(judged.stdout);
  process.stderr.write(judged.stderr);
  if (judged.status !== 0) {
    console.log(`FAIL the judge exited ${judged.status}`);
    failed = true;
  } else {
    const counts = JSON.parse(readFileSync(join(outputs, 'deephaven-counts.json'), 'utf8'));
    if (counts.pass < FLOOR) {
      console.log(`FAIL ${counts.pass} corpus tests pass on Deephaven, under the floor of ${FLOOR}`);
      failed = true;
    } else {
      console.log(`ok   ${counts.pass} of ${counts.tests} corpus tests answer on Deephaven as the oracle does (the floor ${FLOOR}); `
        + `${counts.fail} do not, ${counts.without} have no plan on it`);
    }
  }
} finally {
  await browser?.close();
  site.close();
  await deephaven.stop();
}
process.exit(failed ? 1 : 0);
