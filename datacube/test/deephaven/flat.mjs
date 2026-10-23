// A FLAT GRID OF A BIG BOOK, TICKING (//datacube:deephaven_flat_test; docs/DEEPHAVEN_PLAN_2026_10_22.md V1, DH27), run
// in the pinned Chromium. Node only drives: it starts the pinned Deephaven with the demo's application over a big
// world -- 100,000 positions over 2,000 instruments, every instrument re-priced ten times a second (risk.groovy's
// DEMO_* settings) -- serves the cube page (demo/deephaven/cube.ts), and turns the cube into a flat grid of every
// position, the largest market value first. A flat view is read by a WINDOW: the rows on screen and a margin, followed
// live by a viewport, so what a tick carries and what the page does with it are bounded by the screen, never by the
// book. It checks the grid says the book's size, its rows are in order, a tick re-runs it with nothing sent or fetched
// and carries the window's rows only, and a scroll half way down shows those rows; and it says how long each took.
// Screenshots are its outputs.

// first: points Playwright at the Chromium Bazel fetched
import '../../../tools/browser/pinned-chromium.mjs';
import { join } from 'node:path';
import { chromium } from 'playwright';
import { serveDemo, startDeephaven } from '../../demo/deephaven/site.mjs';

const outputs = process.env.TEST_UNDECLARED_OUTPUTS_DIR ?? process.env.TEST_TMPDIR ?? '.';
const POSITIONS = 100_000;
const TICK_BUDGET_MS = 15;

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
  const page = await (await browser.newContext({ ignoreHTTPSErrors: true, viewport: { width: 1280, height: 860 } })).newPage();
  page.on('pageerror', (e) => fail(`page error: ${e.message}`));
  page.on('console', (m) => { if (m.type() === 'error') console.log(`[page ${m.type()}] ${m.text().slice(0, 300)}`); });
  await page.goto(`${site.url}/`);
  const state = () => page.evaluate(() => ({ ...window.__cube, change: undefined, scroll: undefined }));
  const until = async (test, what, ms = 180_000) => {
    const end = Date.now() + ms;
    while (Date.now() < end) {
      const s = await state();
      if (s.status === 'failed') throw new Error(`the page failed: ${s.error}`);
      if (test(s)) return s;
      await new Promise((r) => setTimeout(r, 200));
    }
    throw new Error(`not ${what} in ${ms / 1000} s: ${JSON.stringify(await state()).slice(0, 600)}`);
  };
  /** The view's market values, in its order; and whether each is no larger than the one above it. */
  const ordered = (s) => {
    const mv = s.rows.map((r) => r[s.columns.indexOf('mv')]);
    const wrong = mv.findIndex((v, i) => i > 0 && !(v <= mv[i - 1]));
    return { mv, wrong };
  };

  await until((s) => s.status === 'live' && s.views >= 1, 'live');
  await page.evaluate(() => { window.__cube.paused = true; });

  // FLAT: every position, the largest market value first -- a gesture, which may take its time (DH27)
  const before = await state();
  let started = Date.now();
  const kind = await page.evaluate(() => window.__cube.change('flat'));
  const flatMs = Date.now() - started;
  const flat = await state();
  if (kind !== 'applied') throw new Error(`the flat view was ${kind}: ${flat.error}`);
  const block = flat.rows.length;
  if (flat.window?.total === POSITIONS && flat.window.offset === 0 && block > 0 && block < 1000) {
    ok(`flat: the grid shows ${flat.window.total.toLocaleString()} positions from a window of ${block} rows; ${flatMs} ms to the view`);
  } else {
    fail(`flat: the window ${JSON.stringify(flat.window)} with ${block} rows`);
  }
  const top = ordered(flat);
  if (top.wrong < 0) ok(`its rows in order, the largest market value first (${f1(top.mv[0])})`);
  else fail(`row ${top.wrong} (${top.mv[top.wrong]}) is larger than the one above it (${top.mv[top.wrong - 1]})`);
  const shown = await page.evaluate(() => document.querySelector('.dc-app-stats')?.textContent ?? '');
  if (shown.includes('100,000 rows')) ok(`the status bar says the book's size: ${JSON.stringify(shown.slice(0, 80))}`);
  else fail(`the status bar says ${JSON.stringify(shown)}`);
  await page.screenshot({ path: join(outputs, '1-flat.png') });
  console.log(`     (made with ${flat.counts.sent - before.counts.sent} operations sent; ${flat.counts.following} results followed)`);

  // PROFILE=1 (`--test_env=PROFILE=1`): each ticking window's CPU profile, its costliest functions said
  const profiled = async (what, work) => {
    if (process.env.PROFILE !== '1') return work();
    const cdp = await page.context().newCDPSession(page);
    await cdp.send('Performance.enable');
    const metrics = async () => Object.fromEntries((await cdp.send('Performance.getMetrics')).metrics.map((m) => [m.name, m.value]));
    const m0 = await metrics();
    await cdp.send('Profiler.enable');
    await cdp.send('Profiler.setSamplingInterval', { interval: 100 });
    await cdp.send('Profiler.start');
    const out = await work();
    const { profile } = await cdp.send('Profiler.stop');
    const m1 = await metrics();
    const d = (k) => ((m1[k] - m0[k]) * 1000).toFixed(0);
    console.log(`${what}: the browser's own work (ms): style ${d('RecalcStyleDuration')} (${m1.RecalcStyleCount - m0.RecalcStyleCount} times), `
      + `layout ${d('LayoutDuration')} (${m1.LayoutCount - m0.LayoutCount} times), script ${d('ScriptDuration')}, tasks ${d('TaskDuration')}; `
      + `${m1.Nodes} nodes, ${m1.LayoutObjects ?? '?'} layout objects`);
    const self = new Map();
    const dt = new Map();
    profile.samples.forEach((id, i) => dt.set(id, (dt.get(id) ?? 0) + (profile.timeDeltas[i] ?? 0)));
    for (const n of profile.nodes) {
      const f = n.callFrame;
      const key = `${f.functionName || '(anonymous)'} ${f.url.split('/').pop()}:${f.lineNumber + 1}`;
      self.set(key, (self.get(key) ?? 0) + (dt.get(n.id) ?? 0) / 1000);
    }
    const top = [...self].filter(([k]) => !/^\((idle|program|garbage collector)\)/.test(k)).sort((a, b) => b[1] - a[1]).slice(0, 25);
    console.log(`${what}: the costliest functions (self ms; idle ${(self.get('(idle) :0') ?? 0).toFixed(0)}, gc ${(self.get('(garbage collector) :0') ?? 0).toFixed(0)}, program ${(self.get('(program) :0') ?? 0).toFixed(0)}):`);
    for (const [k, ms] of top) console.log(`  ${ms.toFixed(1).padStart(8)}  ${k}`);
    return out;
  };

  // TICKING: the window followed live; each tick re-runs the cube from its rows, nothing sent or fetched
  const ticking = async (seconds, what) => {
    const t0 = await state();
    await page.evaluate(() => { window.__cube.paused = false; });
    await profiled(what, () => new Promise((r) => setTimeout(r, seconds * 1000)));
    await page.evaluate(() => { window.__cube.paused = true; });
    await new Promise((r) => setTimeout(r, 200));
    const t1 = await state();
    const paints = t1.tickToPaint.slice(-Math.max(1, t1.refreshes - t0.refreshes));
    const runs = t1.runMs.slice(-paints.length);
    const sent = t1.counts.sent - t0.counts.sent;
    const fetched = t1.counts.fetched - t0.counts.fetched;
    const refreshes = t1.refreshes - t0.refreshes;
    if (refreshes > 0 && sent === 0 && fetched === 0) {
      ok(`${what}: ${refreshes} ticks in ${seconds} s, each on screen ${f1(quantile(paints, 0.5))} ms after its rows `
        + `arrived (p95 ${f1(quantile(paints, 0.95))}), the cube's run ${f1(quantile(runs, 0.5))} ms of it; `
        + `${t1.rows.length} rows a tick; nothing sent or fetched`);
    } else {
      fail(`${what}: ${refreshes} ticks, ${sent} operations sent, ${fetched} results fetched`);
    }
    const parts = t1.tickParts.slice(-paints.length);
    console.log(`     (its parts, p50: waiting ${f1(quantile(parts.map((x) => x[0]), 0.5))} ms, the run `
      + `${f1(quantile(parts.map((x) => x[1]), 0.5))}, the frame after ${f1(quantile(parts.map((x) => x[2]), 0.5))})`);
    const o = ordered(t1);
    if (o.wrong < 0) ok(`${what}: still in order after the ticks`);
    else fail(`${what}: row ${o.wrong} out of order after the ticks`);
    return { paints, t1 };
  };
  // THE BUDGET: a tick on screen within 15 ms of its rows (p50). Measured 2026-10-23 in the pinned Chromium: 8.7 to 9.9
  // ms alone, 10 to 14 beside another browser test -- the storm is the worst case, every cell on screen changing at
  // every tick (the rows sorted by the market value every price moves), and past the cube's 2.5 ms run what is left
  // is the browser's own frame: laying out and painting the cells that changed
  const budget = (paints, what) => (quantile(paints, 0.5) <= TICK_BUDGET_MS
    ? ok(`${what} within its ${TICK_BUDGET_MS} ms budget`)
    : fail(`${what} took ${f1(quantile(paints, 0.5))} ms, past its ${TICK_BUDGET_MS} ms budget`));
  const atTop = await ticking(5, 'ticking at the top');
  budget(atTop.paints, 'a tick at the top');

  // A SCROLL half way down: the window moved, no table made
  const s0 = await state();
  const row = POSITIONS / 2;
  const scrollMs = await page.evaluate((r) => window.__cube.scroll(r), row);
  const s1 = await state();
  const moved = s1.window;
  if (moved !== null && moved.offset <= row && row < moved.offset + s1.rows.length && s1.counts.sent === s0.counts.sent) {
    ok(`scrolled to row ${row.toLocaleString()}: its rows shown ${f1(scrollMs)} ms after the scroll, the window moved `
      + `(rows ${moved.offset.toLocaleString()} to ${(moved.offset + s1.rows.length).toLocaleString()}), nothing made`);
  } else {
    fail(`scrolled to row ${row}: the window ${JSON.stringify(moved)} with ${s1.rows.length} rows; `
      + `${s1.counts.sent - s0.counts.sent} operations sent`);
  }
  const mid = ordered(s1);
  if (mid.wrong < 0 && mid.mv[0] <= top.mv[0]) ok(`the rows there in order, below the top's (${f1(mid.mv[0])})`);
  else fail(`the rows there: ${mid.wrong} out of order, the first ${mid.mv[0]} against the top's ${top.mv[0]}`);
  await page.screenshot({ path: join(outputs, '2-scrolled.png') });
  const there = await ticking(3, 'ticking half way down');
  budget(there.paints, 'a tick there');

  // a small scroll stays within the rows held: drawn from them, nothing asked
  const n0 = await state();
  await page.evaluate((r) => { document.querySelector('#cube .dc-scroller').scrollTop = r * 20; }, row + 10);
  await new Promise((r) => setTimeout(r, 300));
  const n1 = await state();
  if (n1.refreshes === n0.refreshes && n1.window?.offset === n0.window?.offset) ok('a scroll of ten rows is drawn from the rows held: no run, the window unmoved');
  else fail(`a scroll of ten rows moved the window ${JSON.stringify(n0.window)} -> ${JSON.stringify(n1.window)}`);
} catch (e) {
  fail(e?.stack ?? String(e));
} finally {
  await browser?.close();
  site.close();
  await server.stop();
}
process.exit(failed ? 1 : 0);
