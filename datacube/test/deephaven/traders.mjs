// THE TRADER SCREENS, CHECKED (//datacube:deephaven_traders_test; docs/DEEPHAVEN_TRADER_DEMOS_2026_10_24.md K4, K8-K13),
// run in the pinned Chromium. Node only drives: it starts the pinned Deephaven running the market world (a small one,
// its crypto feed refused so the world simulates it), serves the trader screens (demo/deephaven/market/traders.ts), and
// opens each screen from the page's menu as a person does. For each: every grid shows rows and every chart draws; at
// every tick each grid runs again and the charts on the sheet shown draw again, from the live rows -- nothing sent,
// nothing fetched; and the page saved and opened again, every grid and chart back and ticking. A screenshot of each
// sheet of each screen is its output.

// first: points Playwright at the Chromium Bazel fetched
import '../../../tools/browser/pinned-chromium.mjs';
import { join } from 'node:path';
import { chromium } from 'playwright';
import { startDeephaven } from '../../demo/deephaven/site.mjs';
import { serveTraders } from '../../demo/deephaven/market/site.mjs';

const outputs = process.env.TEST_UNDECLARED_OUTPUTS_DIR ?? process.env.TEST_TMPDIR ?? '.';
const server = startDeephaven({
  quiet: true,
  fixture: 'MARKET_FIXTURE',
  env: {
    MARKET_NAMES: '100',
    MARKET_QUOTES: '2000',
    MARKET_TRADES: '400',
    MARKET_FEED: 'coinbase',
    MARKET_FEED_URL: 'wss://127.0.0.1:9/',
  },
});
const config = await server.next('on starting');
const site = await serveTraders(config);

let failed = false;
const fail = (why) => { console.log(`FAIL ${why}`); failed = true; };
const ok = (what) => console.log(`ok   ${what}`);
const quantile = (xs, q) => [...xs].sort((a, b) => a - b)[Math.min(xs.length - 1, Math.floor(q * xs.length))];
const f1 = (x) => (Number.isFinite(x) ? x.toFixed(1) : String(x));
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/** A sheet's tiles, from its saved layout: its bands' nodes, their splits' parts, their stacks' tabs. */
const tilesOf = (node) => (node.tile !== undefined ? [node.tile] : node.stack !== undefined ? [...node.stack]
  : node.parts.flatMap((p) => tilesOf(p.node)));

let browser;
try {
  browser = await chromium.launch();
  const page = await (await browser.newContext({ ignoreHTTPSErrors: true, viewport: { width: 1600, height: 1000 } })).newPage();
  page.on('pageerror', (e) => fail(`page error: ${e.message}`));
  page.on('console', (m) => { if (m.type() === 'error') console.log(`[page ${m.type()}] ${m.text().slice(0, 300)}`); });
  await page.goto(`${site.url}/`);
  const state = () => page.evaluate(() => JSON.parse(JSON.stringify(window.__traders)));
  const until = async (test, what, ms = 240_000) => {
    const end = Date.now() + ms;
    while (Date.now() < end) {
      const s = await state();
      if (s.status === 'failed') throw new Error(`the page failed: ${s.error}`);
      if (test(s)) return s;
      await sleep(250);
    }
    throw new Error(`not ${what} in ${ms / 1000} s: ${JSON.stringify(await state()).slice(0, 1500)}`);
  };
  const status = () => page.evaluate(() => ({ text: document.getElementById('status')?.textContent ?? '',
    kind: document.getElementById('status')?.className ?? '' }));

  /** The screen on the page now, checked: every grid's rows, every chart drawn, each sheet ticking; `name` says which. */
  const checkScreen = async (name, opened) => {
    const s = await until((x) => x.status === 'live' && x.opened >= opened
      && Object.values(x.grids).every((g) => g.views >= 1)
      && Object.values(x.charts).every((c) => c.values.length > 0 && c.values.some((v) => v !== null && v !== '' && v !== '—')),
    `${name}: live, every grid and chart drawn`);
    const said = await status();
    if (said.kind === 'error') fail(`${name}: the page says ${said.text}`);
    const grids = Object.entries(s.grids);
    const empty = grids.filter(([, g]) => g.rows.length === 0).map(([n]) => n);
    if (empty.length === 0) {
      ok(`${name}: open in ${(s.openMs / 1000).toFixed(1)} s (${Object.entries(s.openSteps).map(([k, v]) => `${k} ${(v / 1000).toFixed(1)} s`).join(', ')}; `
        + `first views ${grids.map(([n, g]) => `${n.split(':')[0]} ${((g.firstViewMs ?? 0) / 1000).toFixed(1)} s`).join(', ')}), ${grids.length} grids with rows `
        + `(${grids.map(([n, g]) => `${n} ${g.window ? g.window.total : g.rows.length}`).join(', ')}), `
        + `${Object.keys(s.charts).length} charts drawn`);
    } else {
      fail(`${name}: no rows in ${empty.join(', ')}`);
    }
    const doc = JSON.parse(await page.evaluate(() => window.__traders.save()));
    // each sheet shown, ticking: its grids ran again and its charts drew again, nothing sent or fetched
    for (const [i, sheet] of doc.sheets.entries()) {
      const label = sheet.name ?? sheet.id;
      await page.evaluate((n) => window.__traders.showSheet(n), label);
      await sleep(600);
      const a = await state();
      await sleep(4000);
      const b = await state();
      const on = new Set(sheet.layout.bands.flatMap((band) => tilesOf(band.node)));
      const charts = Object.keys(b.charts).filter((id) => on.has(id));
      const still = Object.entries(b.grids).filter(([n, g]) => g.ticks - (a.grids[n]?.ticks ?? 0) === 0).map(([n]) => n)
        .concat(charts.filter((id) => b.charts[id].draws - (a.charts[id]?.draws ?? 0) === 0));
      const sent = b.counts.sent - a.counts.sent;
      const fetched = b.counts.fetched - a.counts.fetched;
      const ticks = Object.entries(b.grids).map(([n, g]) => `${n} ${g.ticks - (a.grids[n]?.ticks ?? 0)}`).join(', ');
      const p50 = quantile(Object.values(b.grids).flatMap((g) => g.tickToPaint.slice(-20)), 0.5);
      if (still.length === 0 && sent === 0 && fetched === 0) {
        ok(`${name} ▸ ${label}: ticking (${ticks}; ${charts.length} charts drawn again), on screen ${f1(p50)} ms after its rows (p50), nothing sent or fetched`);
      } else {
        const by = Object.entries(b.grids).flatMap(([n, g]) => g.fetched.slice((a.grids[n]?.fetched.length ?? 0) === 20 ? 0
          : (a.grids[n]?.fetched.length ?? 0)).map((sql) => `${n}: ${sql}`));
        fail(`${name} ▸ ${label}: ${still.length > 0 ? `${still.join(', ')} did not tick;` : ''} ${sent} sent, ${fetched} fetched (${ticks})`
          + (by.length > 0 ? `\n  read from the server:\n  ${by.join('\n  ')}` : ''));
      }
      await page.screenshot({ path: join(outputs, `${name.toLowerCase().replace(/[^a-z0-9]+/g, '-')}-${i + 1}.png`) });
    }
    return { state: await state(), doc };
  };

  const first = await checkScreen('Ticker', 1);
  console.log(`     the planner's calls: ${Object.entries(first.state.planner).map(([k, v]) => `${k} ${v.calls} in ${(v.ms / 1000).toFixed(1)} s`).join(', ')}`);
  const startup = Object.values(first.state.startup).reduce((a, b) => a + b, 0);
  ok(`the page live in ${(startup / 1000).toFixed(1)} s from a cold tab: ${Object.entries(first.state.startup).map(([k, v]) => `${k} ${(v / 1000).toFixed(1)} s`).join(', ')}`);
  const screens = first.state.screens;

  // B2: THE SCREEN SAVED AND OPENED AGAIN -- every grid made again over its source as it is now, its charts as saved
  const saved = await page.evaluate(() => window.__traders.save());
  const doc = JSON.parse(saved);
  await page.evaluate((text) => window.__traders.open(text), saved);
  const again = await checkScreen('Ticker, saved and opened again', first.state.opened + 1);
  const same = JSON.stringify(Object.keys(again.state.grids).sort()) === JSON.stringify(Object.keys(first.state.grids).sort())
    && JSON.stringify(Object.keys(again.state.charts).sort()) === JSON.stringify(Object.keys(first.state.charts).sort());
  if (same && again.doc.cubes.length === doc.cubes.length) ok(`saved and opened again: the same ${doc.cubes.length} grids and ${Object.keys(again.state.charts).length} charts`);
  else fail(`opened again: grids ${Object.keys(again.state.grids).join(', ')}; charts ${Object.keys(again.state.charts).join(', ')}`);

  // EVERY OTHER SCREEN, opened from the menu's list
  let opened = again.state.opened;
  for (const screen of screens.slice(1)) {
    await page.evaluate((id) => window.__traders.openScreen(id), screen.id);
    opened++;
    await checkScreen(screen.label, opened);
  }
  ok(`${screens.length} trader screens: ${screens.map((x) => x.label).join(', ')}`);
} catch (e) {
  fail(e?.stack ?? String(e));
} finally {
  await browser?.close();
  site.close();
  await server.stop();
}
process.exit(failed ? 1 : 0);
