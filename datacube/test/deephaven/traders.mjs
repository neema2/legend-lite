// THE TRADER SCREENS, CHECKED (//datacube:deephaven_traders_test; docs/DEEPHAVEN_TRADER_DEMOS_2026_10_24.md K4, K8-K13),
// run in the pinned Chromium. Node only drives: it starts the pinned Deephaven running the market world (a small one,
// its crypto feed refused so the world simulates it), serves the trader screens (demo/deephaven/market/traders.ts), and
// opens each screen from the page's Open… as a person does. For each: every grid shows rows and every chart draws; at
// every tick each grid runs again and the charts on the sheet shown draw again, from the live rows -- nothing sent,
// nothing fetched. The first is saved by Save As… under a name of its own, found in Open… and opened from it, every
// grid and chart back and ticking -- and found there again after the tab is reloaded. A screenshot of each sheet of
// each screen is its output.

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
  // one screen alone, by its id, when asked (a screen being built: --test_env=TRADERS_ONLY=<id>)
  const only = process.env.TRADERS_ONLY;
  await page.goto(`${site.url}/${only ? `#${only}` : ''}`);
  const state = () => page.evaluate(() => JSON.parse(JSON.stringify(window.__traders)));
  const until = async (test, what, ms = 240_000) => {
    const end = Date.now() + ms;
    while (Date.now() < end) {
      const s = await state();
      if (s.status === 'failed') throw new Error(`the page failed: ${s.error}`);
      if (test(s)) return s;
      await sleep(250);
    }
    // stalled: what the page shows, and what each grid's own status line says
    await page.screenshot({ path: join(outputs, 'stalled.png') });
    const lines = await page.evaluate(() => [...document.querySelectorAll('[data-tile]')].map((t) =>
      `${t.getAttribute('data-tile')}: ${[...t.querySelectorAll('.dc-status-note-text, .dc-error, [role="alert"]')].map((e) => e.textContent).join(' | ')}`));
    throw new Error(`not ${what} in ${ms / 1000} s: ${lines.join('; ')}\n${JSON.stringify(await state()).slice(0, 1500)}`);
  };
  const status = () => page.evaluate(() => ({ text: document.getElementById('status')?.textContent ?? '',
    kind: document.getElementById('status')?.className ?? '' }));
  /** The page's menu entry `label` clicked, as a person does: the menu opened, the entry's group hovered if it is in one. */
  const menu = async (label) => {
    await page.click('.dc-titlebar-menu');
    const item = page.locator(`.dc-menu-item:has(> .dc-menu-label:text-is("${label}"))`).first();
    if (!(await item.isVisible())) await page.locator('.dc-menu-item:has(> .dc-menu-label:text-is("File"))').first().hover();
    await item.click();
  };
  /** Open…, as a person opens it: its window, its saved pages listed. */
  const openWindow = async () => {
    await menu('Open\u2026');
    await page.locator('#cubeswin #cubelib').waitFor({ timeout: 10_000 });
  };

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
    for (const [n, g] of grids) console.log(`     ${n}: base ${JSON.stringify(g.base)}`);
    const doc = JSON.parse(await page.evaluate(() => window.__traders.save()));
    // each sheet shown, ticking: its grids ran again and its charts drew again, nothing sent or fetched
    for (const [i, sheet] of doc.sheets.entries()) {
      const label = sheet.name ?? sheet.id;
      await page.evaluate((n) => window.__traders.showSheet(n), label);
      // settled: a chart drawn as its page opened reads its result once more at the first tick, then follows it
      await sleep(1500);
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
      console.log(`     ${name} ▸ ${label}: ${b.counts.following} results followed, the planner's ${JSON.stringify(b.planner.plan)}`);
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

  if (only) {
    const one = await checkScreen(only, 1);
    for (const [n, g] of Object.entries(one.state.grids)) console.log(`     ${n}: base ${JSON.stringify(g.base)}`);
    console.log(`     the planner's calls: ${Object.entries(one.state.planner).map(([k, v]) => `${k} ${v.calls} in ${(v.ms / 1000).toFixed(1)} s (slowest ${v.slowest} ms)`).join(', ')}`);
    throw new Error(`only ${only} was checked (TRADERS_ONLY): not a run of every screen`);
  }
  const first = await checkScreen('Ticker', 1);
  console.log(`     the planner's calls: ${Object.entries(first.state.planner).map(([k, v]) => `${k} ${v.calls} in ${(v.ms / 1000).toFixed(1)} s`).join(', ')}`);
  const startup = Object.values(first.state.startup).reduce((a, b) => a + b, 0);
  ok(`the page live in ${(startup / 1000).toFixed(1)} s from a cold tab: ${Object.entries(first.state.startup).map(([k, v]) => `${k} ${(v / 1000).toFixed(1)} s`).join(', ')}`);
  const screens = first.state.screens;

  // B2: THE SCREEN SAVED BY SAVE AS…, UNDER A NAME, AND OPENED AGAIN FROM OPEN… -- the tab's own store of saved pages;
  // every grid made again over its source as it is now, its charts as saved
  const doc = JSON.parse(await page.evaluate(() => window.__traders.save()));
  const NAME = 'My ticker';
  await menu('Save As\u2026');
  await page.locator('.dc-save').waitFor({ timeout: 10_000 });
  await page.fill('.dc-save-input', NAME);
  await page.locator('.dc-save-foot .dc-primary').click();
  await page.locator('.dc-save').waitFor({ state: 'detached', timeout: 30_000 });
  const savedSaid = await status();
  if (savedSaid.text.includes(NAME) && savedSaid.kind === 'ok') ok(`saved by Save As…: the page says “${savedSaid.text}”`);
  else fail(`Save As… said ${JSON.stringify(savedSaid)}`);
  await openWindow();
  await page.locator('#cubelib .dc-lib-row', { hasText: NAME }).waitFor({ timeout: 10_000 });
  await page.screenshot({ path: join(outputs, 'open.png') });
  const listed = await page.locator('#cubeswin .dc-open-example .dc-open-example-name').allTextContents();
  if (JSON.stringify(listed) === JSON.stringify(screens.map((x) => x.label))) ok(`Open… lists the ${listed.length} trader screens: ${listed.join(', ')}`);
  else fail(`Open… lists the screens ${JSON.stringify(listed)}`);
  const row = page.locator('#cubelib .dc-lib-row', { hasText: NAME });
  await row.waitFor({ timeout: 10_000 });
  await row.locator('.dc-lib-button', { hasText: 'Open' }).click();
  const again = await checkScreen(`${NAME}, saved and opened again from Open…`, first.state.opened + 1);
  if (await page.locator('#cubeswin').count() !== 0) fail('Open… stayed open after the page opened');
  const same = JSON.stringify(Object.keys(again.state.grids).sort()) === JSON.stringify(Object.keys(first.state.grids).sort())
    && JSON.stringify(Object.keys(again.state.charts).sort()) === JSON.stringify(Object.keys(first.state.charts).sort());
  if (same && again.doc.cubes.length === doc.cubes.length && again.doc.name === NAME) ok(`saved and opened again: “${again.doc.name}”, the same ${doc.cubes.length} grids and ${Object.keys(again.state.charts).length} charts`);
  else fail(`opened again as “${again.doc.name}”: grids ${Object.keys(again.state.grids).join(', ')}; charts ${Object.keys(again.state.charts).join(', ')}`);

  /** A list of names, one row a name, never names one twice -- in any view it shows while it ticks, nor on screen (a
   *  window over a result re-sorted at every tick: the trader screens, 2026-10-25, painted a name twice at two prices,
   *  a grid drawn on a sheet not shown redrawing its first row alone at each tick). `grids`: each its name and tile. */
  const noRepeats = async (sheet, grids, column, ms) => {
    await page.evaluate((n) => window.__traders.showSheet(n), sheet);
    await sleep(300);
    const seen = {};
    const end = Date.now() + ms;
    let views = 0;
    let paints = 0;
    const twiceIn = (names) => names.filter((n, i) => n !== null && n !== '' && names.indexOf(n) !== i);
    while (Date.now() < end) {
      const s = await state();
      // the rows a person sees: inside the grid's scroller (a row drawn below it, in its buffer, waits for a scroll)
      const painted = await page.evaluate(({ tiles, column }) => tiles.map((t) => {
        const scroller = document.querySelector(`[data-tile="${t}"] .dc-scroller`);
        const box = scroller?.getBoundingClientRect();
        return [...document.querySelectorAll(`[data-tile="${t}"] .dc-row`)].filter((r) => {
          const at = r.getBoundingClientRect();
          return box !== undefined && at.top >= box.top - 1 && at.bottom <= box.bottom + 1;
        }).map((r) => r.querySelector(`[data-column="${column}"]`)?.textContent ?? null);
      }), { tiles: grids.map((g) => g.tile), column });
      for (const [i, { name }] of grids.entries()) {
        const g = s.grids[name];
        const at = g?.columns.indexOf(column) ?? -1;
        if (g && at >= 0) {
          views++;
          const twice = twiceIn(g.rows.map((r) => r[at]));
          if (twice.length > 0) (seen[name] ??= new Set()).add(`in its view: ${twice.join(', ')}`);
        }
        paints++;
        const twice = twiceIn(painted[i]);
        if (twice.length > 0) (seen[name] ??= new Set()).add(`on screen: ${twice.join(', ')}`);
      }
      await sleep(20);
    }
    const said = Object.entries(seen).map(([n, x]) => `${n}: ${[...x].slice(0, 5).join('; ')}`);
    if (said.length === 0) ok(`${grids.map((g) => g.name).join(', ')}: no name twice in ${views} views and ${paints} screens read while ticking`);
    else fail(`a name shown twice while ticking -- ${said.join(' | ')}`);
  };
  await noRepeats('Watchlist', [{ name: 'Watchlist', tile: 'watchlist' }], 'ticker', 3000);

  // EVERY OTHER SCREEN, opened from Open…'s list of them
  let opened = again.state.opened;
  for (const screen of screens.slice(1)) {
    await openWindow();
    await page.locator(`#cubeswin .dc-open-example[data-example="${screen.id}"]`).click();
    opened++;
    await checkScreen(screen.label, opened);
    if (screen.id === 'market-map') {
      await noRepeats('Movers', [{ name: 'Top gainers', tile: 'gainers' }, { name: 'Top losers', tile: 'losers' },
        { name: 'Most active, by turnover', tile: 'active' }], 'ticker', 5000);
    }
  }
  ok(`${screens.length} trader screens: ${screens.map((x) => x.label).join(', ')}`);

  // THE SAVED PAGE KEPT: the tab reloaded, it is in Open… still, and opens
  await page.reload();
  await until((x) => x.status === 'live' && x.opened >= 1, 'live again after a reload');
  await openWindow();
  const kept = page.locator('#cubelib .dc-lib-row', { hasText: NAME });
  await kept.waitFor({ timeout: 10_000 });
  const before = (await state()).opened;
  await kept.locator('.dc-lib-button', { hasText: 'Open' }).click();
  await until((x) => x.status === 'live' && x.opened > before && x.screen === 'saved'
    && Object.values(x.grids).every((g) => g.views >= 1 && g.rows.length > 0), `“${NAME}” open after a reload`);
  ok(`after a reload, “${NAME}” is in Open… still, and opens with every grid's rows`);
} catch (e) {
  fail(e?.stack ?? String(e));
} finally {
  await browser?.close();
  site.close();
  await server.stop();
}
process.exit(failed ? 1 : 0);
