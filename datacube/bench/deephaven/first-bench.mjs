// THE FIRST-ROWS BENCHMARK (README.md; `bazel test //datacube:deephaven_first_bench --test_output=streamed`, manual): a
// result the grid has not shown before, read whole by Arrow Flight's DoGet or by a subscription of Deephaven's JS
// library -- which reaches the page first. For each of the server's update cycles it starts the pinned Deephaven with
// the risk demo's application (demo/deephaven/risk.groovy: its prices ticking ten times a second), serves the page
// (first-page.ts) on the loopback, runs its trials in the pinned Chromium, and prints each way's time to its first rows
// (p50, p95) and its steps' p50s, for a grid level and a flat page, over a source kept alive and one made again. Its
// JSON is an output.

// first: points Playwright at the Chromium Bazel fetched
import '../../../tools/browser/pinned-chromium.mjs';
import { readFileSync, writeFileSync } from 'node:fs';
import { createServer } from 'node:http';
import { join } from 'node:path';
import { chromium } from 'playwright';
import { runfileFromEnv, runfileNamed } from '../../../tools/js/runfiles.mts';
import { startDeephaven } from '../../demo/deephaven/site.mjs';

const outputs = process.env.TEST_UNDECLARED_OUTPUTS_DIR ?? process.env.TEST_TMPDIR ?? '.';
const TRIALS = Number(process.env.BENCH_TRIALS ?? 20);
/** The server's update cycles, ms (`--test_env=BENCH_CYCLES=100`). */
const CYCLES = (process.env.BENCH_CYCLES ?? '100,20').split(',').map(Number);

const files = {
  '/': ['text/html', '<!doctype html><meta charset="utf-8"><title>first rows</title><script type="module" src="first.js"></script>'],
  '/first.js': ['text/javascript', readFileSync(runfileFromEnv('FIRST_PAGE'))],
  '/wasm-gc-module-runtime.js': ['text/javascript', readFileSync(runfileNamed('PLANNER', 'wasm-gc-module-runtime.js'))],
  '/classes.wasm': ['application/wasm', readFileSync(runfileNamed('PLANNER', 'classes.wasm'))],
};

const quantile = (xs, q) => {
  if (xs.length === 0) return NaN;
  const s = [...xs].sort((a, b) => a - b);
  return s[Math.min(s.length - 1, Math.floor(q * s.length))];
};
const f1 = (x) => (Number.isFinite(x) ? x.toFixed(1) : '-');

const results = [];
let failed = false;
const browser = await chromium.launch();
try {
  for (const cycleMs of CYCLES) {
    const server = startDeephaven({ quiet: true, args: [`--jvm_flag=-Dembedded.deephaven.cycleMs=${cycleMs}`] });
    const config = await server.next('on starting');
    const served = { ...files, '/config.json': ['application/json', JSON.stringify({ url: config.url, psk: config.psk })] };
    const site = createServer((req, res) => {
      const f = served[new URL(req.url, 'http://page').pathname]; // portable: an HTTP route, never a file's path
      res.writeHead(f ? 200 : 404, f ? { 'content-type': f[0] } : {});
      res.end(f ? f[1] : undefined);
    });
    const port = await new Promise((done) => site.listen(0, '127.0.0.1', () => done(site.address().port)));
    const page = await (await browser.newContext({ ignoreHTTPSErrors: true })).newPage();
    page.on('pageerror', (e) => { console.log(`page error: ${e.message}`); failed = true; });
    page.on('console', (m) => { if (m.type() === 'error') console.log(`  [page error] ${m.text().slice(-400)}`); });
    try {
      await page.goto(`http://127.0.0.1:${port}/`);
      await page.waitForFunction(() => window.bench?.ready === true, null, { timeout: 120_000 });
      const error = await page.evaluate(() => window.bench.error);
      if (error) throw new Error(`the page failed: ${error}`);
      const timed = await Promise.race([
        page.evaluate((n) => window.bench.run(n), TRIALS),
        new Promise((_, fail) => setTimeout(() => fail(new Error('no answer in time')), 600_000)),
      ]);
      console.log(`\na ${cycleMs} ms cycle, ${TRIALS} trials`);
      console.log('  shape  source  how         sent  rows  first p50  first p95  steps (p50)');
      const groups = new Map();
      for (const t of timed) {
        const key = `${t.shape}|${t.source}|${t.how}`;
        groups.set(key, [...(groups.get(key) ?? []), t]);
      }
      for (const [key, ts] of groups) {
        const [shape, source, how] = key.split('|');
        const steps = Object.keys(ts[0].steps).map((s) => `${s} ${f1(quantile(ts.map((t) => t.steps[s]), 0.5))}`);
        const row = { cycleMs, shape, source, how, sent: quantile(ts.map((t) => t.sent), 0.5),
          rows: quantile(ts.map((t) => t.rows), 0.5), p50: quantile(ts.map((t) => t.total), 0.5),
          p95: quantile(ts.map((t) => t.total), 0.95),
          steps: Object.fromEntries(Object.keys(ts[0].steps).map((s) => [s, quantile(ts.map((t) => t.steps[s]), 0.5)])) };
        results.push(row);
        console.log(`  ${shape.padEnd(6)} ${source.padEnd(7)} ${how.padEnd(10)} ${String(row.sent).padStart(5)} `
          + `${String(row.rows).padStart(5)} ${f1(row.p50).padStart(10)} ${f1(row.p95).padStart(10)}  ${steps.join(', ')}`);
      }
    } finally {
      await page.context().close();
      site.close();
      await server.stop();
    }
  }
} catch (e) {
  console.log(`FAIL ${e?.stack ?? e}`);
  failed = true;
} finally {
  await browser.close();
  writeFileSync(join(outputs, 'first-bench.json'), JSON.stringify(results, null, 1));
}
process.exit(failed ? 1 : 0);
