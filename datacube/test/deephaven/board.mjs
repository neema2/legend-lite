// A DATACUBE PAGE OF SEVERAL GRIDS AND CHARTS, TICKING (//datacube:deephaven_board_test; docs/DEEPHAVEN_PLAN_2026_10_22.md
// V2, V3, DH27), run in the pinned Chromium. Node only drives: it starts the pinned Deephaven with the demo's
// application over a big world -- 100,000 positions over 2,000 instruments, every instrument re-priced ten times a
// second -- serves the page of several grids (demo/deephaven/board.ts: DataCube's own page, three grids and a chart on
// each pivoted one), and checks the three grids read one base on the server, the flat book says its size, each
// pivot's columns are there, and at every tick each grid runs again and each chart draws again from the live rows --
// nothing sent, nothing fetched; and it says how long each took. Screenshots are its outputs.

// first: points Playwright at the Chromium Bazel fetched
import '../../../tools/browser/pinned-chromium.mjs';
import { join } from 'node:path';
import { chromium } from 'playwright';
import { serveDemo, startDeephaven } from '../../demo/deephaven/site.mjs';

const outputs = process.env.TEST_UNDECLARED_OUTPUTS_DIR ?? process.env.TEST_TMPDIR ?? '.';
const POSITIONS = 100_000;
/** A grid's or a chart's tick on screen within this many ms of its rows (p50); see `budget`. */
const TICK_BUDGET_MS = 25;

const server = startDeephaven({
  quiet: true,
  env: { DEMO_POSITIONS: String(POSITIONS), DEMO_INSTRUMENTS: '2000', DEMO_STORM: '1' },
});
const config = await server.next('on starting');
const site = await serveDemo(config);

let failed = false;
const fail = (why) => { console.log(`FAIL ${why}`); failed = true; };
const ok = (what) => console.log(`ok   ${what}`);
const quantile = (xs, q) => [...xs].sort((a, b) => a - b)[Math.min(xs.length - 1, Math.floor(q * xs.length))];
const f1 = (x) => (Number.isFinite(x) ? x.toFixed(1) : String(x));
let browser;
try {
  browser = await chromium.launch();
  const page = await (await browser.newContext({ ignoreHTTPSErrors: true, viewport: { width: 1600, height: 1000 } })).newPage();
  page.on('pageerror', (e) => fail(`page error: ${e.message}`));
  page.on('console', (m) => { if (m.type() === 'error') console.log(`[page ${m.type()}] ${m.text().slice(0, 300)}`); });
  await page.goto(`${site.url}/board.html`);
  const state = () => page.evaluate(() => JSON.parse(JSON.stringify(window.__board)));
  const until = async (test, what, ms = 240_000) => {
    const end = Date.now() + ms;
    while (Date.now() < end) {
      const s = await state();
      if (s.status === 'failed') throw new Error(`the page failed: ${s.error}`);
      if (test(s)) return s;
      await new Promise((r) => setTimeout(r, 250));
    }
    throw new Error(`not ${what} in ${ms / 1000} s: ${JSON.stringify(await state()).slice(0, 800)}`);
  };

  const started = Date.now();
  const live = await until((s) => s.status === 'live' && Object.values(s.charts).every((c) => c.values.length > 0),
    'live, every grid and chart drawn');
  ok(`the page is live in ${((Date.now() - started) / 1000).toFixed(1)} s: ${Object.keys(live.grids).length} grids, `
    + `${Object.keys(live.charts).length} charts`);
  await page.evaluate(() => { window.__board.paused = true; });
  if (live.bases.length === 1) ok(`the three grids read one base on the server (${live.bases[0]})`);
  else fail(`the grids read ${live.bases.length} bases: ${live.bases.join(', ')}`);
  const book = live.grids.Book;
  if (book?.window?.total === POSITIONS) ok(`the book: ${book.window.total.toLocaleString()} positions, read by a window of ${book.rows.length} rows`);
  else fail(`the book: ${JSON.stringify(book?.window)}`);
  for (const [name, pivot] of [['Desks', ['AMER', 'EMEA']], ['Sectors', ['USD', 'EUR', 'JPY']]]) {
    const columns = live.grids[name]?.columns ?? [];
    const missing = pivot.filter((v) => !columns.includes(`${v}__|__mv`));
    if (missing.length === 0) ok(`${name}: pivoted, a market value column for each of ${pivot.join(', ')}`);
    else fail(`${name}: no columns for ${missing.join(', ')}: ${columns.join(', ')}`);
  }
  await page.screenshot({ path: join(outputs, '1-page.png') });

  // TICKING: each grid runs again and each chart draws again at every change, from the live rows
  const t0 = await state();
  await page.evaluate(() => { window.__board.paused = false; });
  await new Promise((r) => setTimeout(r, 6000));
  await page.evaluate(() => { window.__board.paused = true; });
  await new Promise((r) => setTimeout(r, 300));
  const t1 = await state();
  const sent = t1.counts.sent - t0.counts.sent;
  const fetched = t1.counts.fetched - t0.counts.fetched;
  if (sent === 0 && fetched === 0) ok(`six seconds of ticks: nothing sent, nothing fetched; ${t1.counts.following} results followed`);
  else fail(`six seconds of ticks sent ${sent} operations and fetched ${fetched} results`);
  // THE BUDGET: measured 2026-10-23 in the pinned Chromium, three grids and two charts ticking ten times a second on
  // one page: the book 4.7 ms, the pivoted grids 10.9 to 12.6, the charts 7.3 to 7.8 (p50), each grid's own run 1.2 to
  // 2 ms of it -- the rest is the browser drawing five live tiles on the page's one thread; twice the slowest p50
  const budget = (xs, what) => {
    const p50 = quantile(xs, 0.5);
    if (xs.length > 0 && p50 <= TICK_BUDGET_MS) ok(`${what}: ${xs.length} ticks, on screen ${f1(p50)} ms after its rows (p95 ${f1(quantile(xs, 0.95))}), within its ${TICK_BUDGET_MS} ms budget`);
    else fail(`${what}: ${xs.length} ticks, on screen ${f1(p50)} ms after its rows, past its ${TICK_BUDGET_MS} ms budget`);
  };
  for (const [name, g] of Object.entries(t1.grids)) {
    const ticks = g.ticks - t0.grids[name].ticks;
    budget(g.tickToPaint.slice(-ticks), `${name} ticked (its run ${f1(quantile(g.runMs.slice(-ticks), 0.5))} ms)`);
  }
  for (const [id, c] of Object.entries(t1.charts)) {
    const draws = c.draws - t0.charts[id].draws;
    budget(c.tickToDraw.slice(-draws), `${id} drawn again`);
    if (JSON.stringify(c.values) !== JSON.stringify(t0.charts[id].values)) ok(`${id}: its values moved with the prices`);
    else fail(`${id}: its values did not move: ${JSON.stringify(c.values).slice(0, 200)}`);
  }
  await page.screenshot({ path: join(outputs, '2-ticked.png') });
} catch (e) {
  fail(e?.stack ?? String(e));
} finally {
  await browser?.close();
  site.close();
  await server.stop();
}
process.exit(failed ? 1 : 0);
