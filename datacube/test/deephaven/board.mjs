// A DATACUBE PAGE OF SEVERAL GRIDS AND CHARTS, TICKING (//datacube:deephaven_board_test; docs/DEEPHAVEN_PLAN_2026_10_22.md
// V2, V3, DH27), run in the pinned Chromium. Node only drives: it starts the pinned Deephaven with the demo's
// application over a big world -- 100,000 positions over 2,000 instruments, every instrument re-priced ten times a
// second -- serves the page of several grids (demo/deephaven/board.ts: DataCube's own page, three grids and a chart on
// each pivoted one), and checks the three grids read one base on the server, the flat book says its size, each
// pivot's columns are there, and at every tick each grid runs again and each chart draws again from the live rows --
// nothing sent, nothing fetched; and it says how long each took. Each chart's value axis held while it ticks (C2).
// Then the page whole (B2): saved and opened again, every grid and chart back and ticking; and the server restarted,
// every grid and chart back by itself and ticking. Screenshots are its outputs.

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
    // C2: the value axis as the view drew it, through every tick -- never narrowed, widened only where values left it
    const axes = [t0.charts[id].axes.at(-1), ...c.axes.slice(-draws)].filter(Boolean);
    const moved = axes.findIndex((a, i) => i > 0 && (a[0] > axes[i - 1][0] || a[1] < axes[i - 1][1]));
    const widened = axes.filter((a, i) => i > 0 && (a[0] !== axes[i - 1][0] || a[1] !== axes[i - 1][1])).length;
    if (axes.length > 1 && moved < 0) ok(`${id}: its value axis held through ${axes.length - 1} draws (${widened} widened): ${JSON.stringify(axes.at(-1))}`);
    else fail(`${id}: its value axis narrowed under a tick: ${JSON.stringify(axes.slice(Math.max(0, moved - 1), moved + 1))} (${axes.length} draws)`);
  }
  await page.screenshot({ path: join(outputs, '2-ticked.png') });

  /** Every grid ran again and every chart drew again in `ms` of ticks, nothing sent or fetched: `what` said. */
  const ticking = async (what, ms) => {
    // the page's counts are read twice a second: the ones before the ticks, read after the last of what came before
    await new Promise((r) => setTimeout(r, 600));
    const a = await state();
    await page.evaluate(() => { window.__board.paused = false; });
    await new Promise((r) => setTimeout(r, ms));
    await page.evaluate(() => { window.__board.paused = true; });
    await new Promise((r) => setTimeout(r, 300));
    const b = await state();
    const still = Object.entries(b.grids).filter(([n, g]) => g.ticks - (a.grids[n]?.ticks ?? 0) === 0).map(([n]) => n)
      .concat(Object.entries(b.charts).filter(([id, c]) => c.draws - (a.charts[id]?.draws ?? 0) === 0).map(([id]) => id));
    const sentNow = b.counts.sent - a.counts.sent;
    const fetchedNow = b.counts.fetched - a.counts.fetched;
    if (still.length === 0 && sentNow === 0 && fetchedNow === 0) {
      ok(`${what}: every grid ran again and every chart drew again (${Object.entries(b.grids).map(([n, g]) => `${n} ${g.ticks - (a.grids[n]?.ticks ?? 0)}`).join(', ')} ticks), nothing sent or fetched`);
    } else {
      fail(`${what}: ${still.length > 0 ? `${still.join(', ')} still` : ''} ${sentNow} sent, ${fetchedNow} fetched`);
    }
    return b;
  };
  /** The page's grids as `s` shows them, against the page as it first was: its columns, the book's size, one base. */
  const same = (s, what) => {
    const differ = Object.keys(live.grids).filter((n) => JSON.stringify(s.grids[n]?.columns) !== JSON.stringify(live.grids[n].columns));
    if (differ.length === 0 && s.grids.Book?.window?.total === POSITIONS && s.bases.length === 1) {
      ok(`${what}: the same grids, columns and charts (${Object.keys(s.grids).join(', ')}; ${Object.keys(s.charts).join(', ')}), one base`);
    } else {
      fail(`${what}: ${differ.length > 0 ? `${differ.join(', ')}'s columns differ` : ''} the book ${JSON.stringify(s.grids.Book?.window)}, bases ${s.bases.join(', ')}`);
    }
  };

  // B2: THE PAGE SAVED AND OPENED AGAIN -- every grid made again over the source as it is now, its charts as saved
  const saved = await page.evaluate(() => window.__board.save());
  const doc = JSON.parse(saved);
  const sources = [...new Set(doc.cubes.map((c) => c.cube.source.name))];
  const charted = doc.views.filter((v) => v.kind === 'chart').length;
  if (doc.cubes.length === 3 && charted === 2 && sources.length === 1) {
    ok(`saved: ${doc.cubes.length} grids over ${sources[0]} (its query by its text), ${charted} charts, ${saved.length.toLocaleString()} characters`);
  } else {
    fail(`saved ${doc.cubes.length} grids over ${sources.join(', ')}, ${charted} charts`);
  }
  const opening = Date.now();
  await page.evaluate((text) => window.__board.open(text), saved);
  const reopened = await until((s) => s.status === 'live' && s.opened >= 1 && Object.keys(s.grids).length === 3
    && Object.values(s.grids).every((g) => g.views >= 1) && Object.keys(s.charts).length === 2
    && Object.values(s.charts).every((c) => c.values.length > 0), 'opened again, every grid and chart drawn');
  ok(`opened again in ${((Date.now() - opening) / 1000).toFixed(1)} s`);
  same(reopened, 'opened again');
  await ticking('opened again, ticking', 3000);
  await page.screenshot({ path: join(outputs, '3-opened.png') });

  // B2: THE SERVER RESTARTED -- Deephaven keeps no session across one; the page comes back by itself, every grid run
  // again on a new session, its base made again, its charts drawn from it
  await page.evaluate(() => { window.__board.paused = false; });
  server.restart();
  const down = await until((s) => s.status === 'reconnecting', 'reconnecting after the restart', 60_000);
  ok(`the page saw the server go (${down.error.slice(0, 80)})`);
  const restarted = await server.next('on restarting');
  if (!restarted.restarted) fail(`the fixture answered ${JSON.stringify(restarted)}`);
  const restarting = Date.now();
  const back = await until((s) => s.status === 'live' && s.reconnects >= 1 && Object.values(s.charts).every((c) => c.values.length > 0),
    'live again after the restart', 240_000);
  await page.evaluate(() => { window.__board.paused = true; });
  await new Promise((r) => setTimeout(r, 1000));
  const after = await state();
  ok(`back after the restart in ${((Date.now() - restarting) / 1000).toFixed(1)} s (${back.reconnects} sign-in, the engine's count ${after.counts.reconnects})`);
  same(after, 'after the restart');
  await ticking('after the restart, ticking', 3000);
  await page.screenshot({ path: join(outputs, '4-restarted.png') });
} catch (e) {
  fail(e?.stack ?? String(e));
} finally {
  await browser?.close();
  site.close();
  await server.stop();
}
process.exit(failed ? 1 : 0);
