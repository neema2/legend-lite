// THE STRESS CORPUS ON DEEPHAVEN, AGAINST THE ORACLE (//datacube:deephaven_corpus_test; docs/DEEPHAVEN_TRADER_DEMOS_
// 2026_10_24.md, task 15), run in the pinned Chromium. Node only drives: it starts the pinned Deephaven holding the
// corpus's tables (test/deephaven/corpus/corpus.groovy, from :deephaven_corpus_cases' tables.json), serves the
// page that sends every planned test's plan to it (corpus-page.ts), collects each answer, one JSON line a test, and
// hands them to the stress suites' own judge (//core:stress_deephaven judge): each answer's rows against the test's
// expectation, as the stress suites judge DuckDB's and H2's. The count of passes is held to its floor.
//
// THEN THE TRADING DESK'S SCREENS (2026-10-27): a server of their own holding the corpus's tables with the desk's rows
// added (desk-tables.json; test/deephaven/desk-rows.csv), each screen's plan on Deephaven sent as the corpus's are, its
// expectation what DuckDB answers to the same call over the same rows (desk-cases.json) -- every one held to passing.

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

/**
 * One pass: a Deephaven started holding `tables`, every case of `cases` sent to it from the page, the answers judged
 * into `out`. Answers the judge's counts, or null where the pass failed (said).
 */
const pass = async (name, cases, tables, out) => {
  mkdirSync(out, { recursive: true });
  const results = join(out, 'results.jsonl');
  writeFileSync(results, '');
  const deephaven = startDeephaven({ quiet: true, fixture: 'CORPUS_FIXTURE', env: { CORPUS_TABLES: tables } });
  let site;
  let browser;
  try {
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
    site = createServer((req, res) => {
      const path = new URL(req.url, 'http://page').pathname;
      if (path === '/cases.json') {
        res.writeHead(200, { 'content-type': 'application/json' });
        createReadStream(cases).pipe(res);
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
    browser = await chromium.launch();
    const page = await (await browser.newContext({ ignoreHTTPSErrors: true })).newPage();
    let pageFailed = false;
    page.on('pageerror', (e) => { console.log(`page error: ${e.message}`); pageFailed = true; });
    page.on('console', (m) => { if (m.type() === 'error') console.log(`page console: ${m.text().slice(0, 300)}`); });
    const started = Date.now();
    await page.goto(`http://127.0.0.1:${sitePort}/corpus.html`);
    for (;;) {
      const v = await page.evaluate(() => window.__corpus);
      if (v?.done) break;
      if (Date.now() - started > 3_000_000) throw new Error(`${name}: not sent in 50 minutes`);
      if (v) console.log(`     ${name}: ${v.sent} sent, ${v.answered} answered, ${v.refused} refused (${((Date.now() - started) / 1000).toFixed(0)} s)`);
      await new Promise((r) => setTimeout(r, v?.sent > 100 ? 30_000 : 2_000));
    }
    const sent = await page.evaluate(() => window.__corpus);
    if (sent.setupError) {
      console.log(`FAIL ${name}, the setup: ${sent.setupError}`);
      return null;
    }
    console.log(`     ${name}: ${sent.sent} plans sent in ${((Date.now() - started) / 1000).toFixed(0)} s: ${sent.answered} answered, ${sent.refused} refused by the server`);
    // THE JUDGE: the stress suites' own, over each answer
    const judged = spawnSync(runfileFromEnv('JUDGE'), ['judge', cases, results, out], { encoding: 'utf8', maxBuffer: 1 << 26 });
    process.stdout.write(judged.stdout);
    process.stderr.write(judged.stderr);
    if (judged.status !== 0) {
      console.log(`FAIL ${name}: the judge exited ${judged.status}`);
      return null;
    }
    return pageFailed ? null : JSON.parse(readFileSync(join(out, 'deephaven-counts.json'), 'utf8'));
  } finally {
    await browser?.close();
    site?.close();
    await deephaven.stop();
  }
};

let failed = false;
const corpus = await pass('the corpus', runfileNamed('CASES', 'cases.json'), runfileNamed('CASES', 'tables.json'), outputs);
if (corpus === null) {
  failed = true;
} else if (corpus.pass < FLOOR) {
  console.log(`FAIL ${corpus.pass} corpus tests pass on Deephaven, under the floor of ${FLOOR}`);
  failed = true;
} else {
  console.log(`ok   ${corpus.pass} of ${corpus.tests} corpus tests answer on Deephaven as the oracle does (the floor ${FLOOR}); `
    + `${corpus.fail} do not, ${corpus.without} have no plan on it`);
}
const deskOut = join(outputs, 'desk');
const desk = await pass('the desk', runfileNamed('CASES', 'desk-cases.json'), runfileNamed('CASES', 'desk-tables.json'), deskOut);
if (desk === null) {
  failed = true;
} else if (desk.pass !== desk.tests) {
  console.log(`FAIL ${desk.tests - desk.pass} of the desk's ${desk.tests} screens answer otherwise on Deephaven than on DuckDB:`);
  for (const line of readFileSync(join(deskOut, 'deephaven-fail.txt'), 'utf8').split('\n').filter(Boolean)) console.log(`     ${line.slice(0, 400)}`);
  failed = true;
} else {
  console.log(`ok   every one of the desk's ${desk.tests} screens answers on Deephaven what DuckDB answers to the same call`);
}
process.exit(failed ? 1 : 0);
