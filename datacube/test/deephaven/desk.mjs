// THE TRADING DESK, CHECKED (//datacube:deephaven_desk_test; docs/DEEPHAVEN_TRADER_DEMOS_2026_10_24.md, the trading
// desk), in the pinned Chromium. Node only drives: it starts the pinned Deephaven running the market world (a small one,
// its crypto feed refused so the world simulates it) and serves DataCube's own site with its config.json naming that
// server (demo/deephaven/market/desk-site.mjs). DataCube's page opens on the trading desk (`#example=trading-desk`), as
// Open…'s Examples open it: its ten tabs, each a screen; on each, every grid shows rows and every chart draws, and the
// tab ticks (what it shows changes as the market does). Open…'s Examples list the desk first. A screenshot of each tab
// is the test's output.

// first: points Playwright at the Chromium Bazel fetched
import '../../../tools/browser/pinned-chromium.mjs';
import { join } from 'node:path';
import { chromium } from 'playwright';
import { startDeephaven } from '../../demo/deephaven/site.mjs';
import { serveDesk } from '../../demo/deephaven/market/desk-site.mjs';

const TABS = ['Markets', 'Stock', 'News', 'Order flow', 'Crypto', 'Blotter', 'TCA', 'P&L and risk', 'Options', 'FX'];

const outputs = process.env.TEST_UNDECLARED_OUTPUTS_DIR ?? process.env.TEST_TMPDIR ?? '.';
const server = startDeephaven({
  quiet: true,
  fixture: 'MARKET_FIXTURE',
  env: {
    MARKET_NAMES: '100',
    MARKET_QUOTES: '2000',
    MARKET_TRADES: '400',
    // half an hour of the session before the world starts, on a bounded tape: a stock's chart has a morning to draw
    MARKET_MORNING: '1800',
    MARKET_MORNING_PRINTS: '120000',
    MARKET_FEED: 'coinbase',
    MARKET_FEED_URL: 'wss://127.0.0.1:9/',
    MARKET_NEWS: '90',
  },
});
const config = await server.next('on starting');
const site = await serveDesk(config);

let failed = false;
const fail = (why) => { console.log(`FAIL ${why}`); failed = true; };
const ok = (what) => console.log(`ok   ${what}`);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/** The tiles of the tab shown, as the page has them now: each grid's rows and whether it is still reading, each chart
 *  drawn (a canvas, a KPI's figure, a feed's cards), and anything a tile says went wrong. */
const shownTiles = (page) => page.evaluate(() => [...document.querySelectorAll('[data-tile]')]
  .filter((t) => t.checkVisibility())
  .map((t) => {
    const grid = t.querySelector('.dc-body');
    const text = (sel) => [...t.querySelectorAll(sel)].map((e) => e.textContent?.trim() ?? '').filter((x) => x !== '');
    return {
      id: t.getAttribute('data-tile'),
      kind: grid ? 'grid' : 'chart',
      rows: t.querySelectorAll('.dc-row:not(.dc-total)').length,
      drawn: Boolean(t.querySelector('.dc-chartpanel-chart canvas, .dc-kpi-value, .dc-feed-card')),
      errors: text('.dc-error, [role="alert"], .dc-chartpanel-error'),
      // what it shows, for a tick: its first cells, its KPI's figure, its feed's first cards
      shows: [...t.querySelectorAll('.dc-cell, .dc-kpi-value, .dc-feed-card')].slice(0, 80).map((e) => e.textContent).join('|'),
    };
  }));

let browser;
try {
  browser = await chromium.launch();
  const page = await (await browser.newContext({ ignoreHTTPSErrors: true, viewport: { width: 1680, height: 1050 } })).newPage();
  page.on('pageerror', (e) => fail(`page error: ${e.message}`));
  page.on('console', (m) => { if (m.type() === 'error' || m.type() === 'warning') console.log(`[page ${m.type()}] ${m.text().slice(0, 300)}`); });
  const started = Date.now();
  await page.goto(site.desk);
  const appStatus = () => page.evaluate(() => {
    const s = document.getElementById('status');
    return { text: s?.textContent ?? '', bad: s?.classList.contains('bad') ?? false };
  });

  // THE DESK OPENED: its ten tabs, named, in order
  const tabs = await (async () => {
    const end = Date.now() + 300_000;
    for (;;) {
      const names = await page.evaluate(() => [...document.querySelectorAll('.dc-sheet-tab .dc-sheet-label')].map((e) => e.textContent?.trim()));
      if (names.length >= TABS.length) return names;
      const s = await appStatus();
      if (s.bad) throw new Error(`the page says: ${s.text}`);
      if (Date.now() > end) {
        await page.screenshot({ path: join(outputs, 'stalled.png') });
        throw new Error(`the desk did not open in 300 s (tabs: ${JSON.stringify(names)}; status: ${s.text})`);
      }
      await sleep(500);
    }
  })();
  const opened = Date.now() - started;
  if (JSON.stringify(tabs) === JSON.stringify(TABS)) ok(`the desk opened in DataCube in ${(opened / 1000).toFixed(1)} s: ${tabs.length} tabs, ${tabs.join(', ')}`);
  else fail(`the desk's tabs are ${JSON.stringify(tabs)}, not ${JSON.stringify(TABS)}`);

  // EACH TAB: shown as a person shows it, its grids' rows and its charts drawn, then ticking
  for (const [i, name] of TABS.entries()) {
    await page.locator(`.dc-sheet-tab:has(.dc-sheet-label:text-is("${name}"))`).first().click();
    const shownAt = Date.now();
    let tiles = [];
    for (;;) {
      tiles = await shownTiles(page);
      const ready = tiles.length > 0 && tiles.every((t) => (t.kind === 'grid' ? t.rows > 0 : t.drawn));
      if (ready || tiles.some((t) => t.errors.length > 0) || Date.now() - shownAt > 120_000) break;
      await sleep(250);
    }
    const errored = tiles.filter((t) => t.errors.length > 0);
    const empty = tiles.filter((t) => (t.kind === 'grid' ? t.rows === 0 : !t.drawn));
    await page.screenshot({ path: join(outputs, `desk-${String(i + 1).padStart(2, '0')}-${name.replace(/[^A-Za-z]+/g, '-')}.png`) });
    if (errored.length > 0) fail(`${name}: ${errored.map((t) => `${t.id}: ${t.errors.join(' / ').slice(0, 300)}`).join('; ')}`);
    if (empty.length > 0) fail(`${name}: ${empty.map((t) => `${t.id} (${t.kind === 'grid' ? 'no rows' : 'not drawn'})`).join(', ')} after ${((Date.now() - shownAt) / 1000).toFixed(0)} s`);
    if (errored.length === 0 && empty.length === 0) {
      const grids = tiles.filter((t) => t.kind === 'grid');
      ok(`${name}: ${grids.length} grids (${grids.map((t) => t.rows).join(', ')} rows on screen) and ${tiles.length - grids.length} charts drawn in ${((Date.now() - shownAt) / 1000).toFixed(1)} s`);
    }
    // TICKING: what the tab shows changes as the market does, with nothing touched -- each tick on screen how long after
    // its rows (the host's dh:tick measures; the other tabs' grids wait while they are not shown)
    await page.evaluate(() => performance.clearMeasures('dh:tick'));
    const before = new Map(tiles.map((t) => [t.id, t.shows]));
    const tickEnd = Date.now() + 30_000;
    let changed = [];
    while (Date.now() < tickEnd) {
      await sleep(1000);
      changed = (await shownTiles(page)).filter((t) => before.has(t.id) && before.get(t.id) !== t.shows).map((t) => t.id);
      if (changed.length > 0) break;
    }
    // a few seconds of ticks, measured
    await sleep(5000);
    const ticks = await page.evaluate(() => performance.getEntriesByName('dh:tick').map((e) => e.duration));
    const p50 = ticks.length === 0 ? NaN : [...ticks].sort((a, b) => a - b)[Math.floor(ticks.length / 2)];
    if (changed.length > 0) ok(`${name} ticks: ${changed.join(', ')} changed; ${ticks.length} ticks, on screen ${p50.toFixed(0)} ms after their rows (p50)`);
    else fail(`${name} did not tick in 30 s`);
  }
  const status = await appStatus();
  if (status.bad) fail(`the page's status says: ${status.text}`);

  // OPEN…'S EXAMPLES: the desk first, above the chart pages
  await page.click('.dc-titlebar-menu');
  const openItem = page.locator('.dc-menu-item:has(> .dc-menu-label:text-is("Open…"))').first();
  if (!(await openItem.isVisible())) await page.locator('.dc-menu-item:has(> .dc-menu-label:text-is("File"))').first().hover();
  await openItem.click();
  await page.locator('.dc-open-example').first().waitFor({ timeout: 20_000 });
  const examples = await page.evaluate(() => [...document.querySelectorAll('.dc-open-example')].map((e) => e.getAttribute('data-example')));
  await page.screenshot({ path: join(outputs, 'desk-open-examples.png') });
  if (examples[0] === 'trading-desk' && examples.includes('charts-gallery')) ok(`Open…'s Examples: ${examples.join(', ')}`);
  else fail(`Open…'s Examples are ${JSON.stringify(examples)}: the trading desk first, then the chart pages`);
  await page.keyboard.press('Escape');

  // A SAVED PAGE OF ONE'S OWN: the desk saved by Save As… under a name, DataCube opened again on its own start, the desk
  // found in Open…'s saved pages and opened from there -- its ten tabs, the first drawn and ticking, another read when shown
  const NAME = 'My trading desk';
  const menu = async (label) => {
    await page.click('.dc-titlebar-menu');
    const item = page.locator(`.dc-menu-item:has(> .dc-menu-label:text-is("${label}"))`).first();
    if (!(await item.isVisible())) await page.locator('.dc-menu-item:has(> .dc-menu-label:text-is("File"))').first().hover();
    await item.click();
  };
  await menu('Save As\u2026');
  await page.locator('.dc-save').waitFor({ timeout: 10_000 });
  await page.fill('.dc-save-input', NAME);
  await page.locator('.dc-save-foot .dc-primary').click();
  await page.locator('.dc-save').waitFor({ state: 'detached', timeout: 30_000 });
  await page.goto(`${site.url}/demo/index.html`);
  await page.locator('.dc-row').first().waitFor({ timeout: 120_000 });
  await menu('Open\u2026');
  const row = page.locator('#cubelib .dc-lib-row', { hasText: NAME });
  await row.waitFor({ timeout: 20_000 });
  await page.screenshot({ path: join(outputs, 'desk-saved-in-open.png') });
  const reopening = Date.now();
  await row.locator('.dc-lib-button', { hasText: 'Open' }).click();
  const reopened = await (async () => {
    for (;;) {
      const names = await page.evaluate(() => [...document.querySelectorAll('.dc-sheet-tab .dc-sheet-label')].map((e) => e.textContent?.trim()));
      if (names.length >= TABS.length) return names;
      if (Date.now() - reopening > 300_000) return names;
      await sleep(500);
    }
  })();
  if (JSON.stringify(reopened) !== JSON.stringify(TABS)) fail(`“${NAME}” opened with tabs ${JSON.stringify(reopened)}`);
  else {
    let first = [];
    for (;;) {
      first = await shownTiles(page);
      if ((first.length > 0 && first.every((t) => (t.kind === 'grid' ? t.rows > 0 : t.drawn))) || Date.now() - reopening > 300_000) break;
      await sleep(250);
    }
    const drawn = first.length > 0 && first.every((t) => (t.kind === 'grid' ? t.rows > 0 : t.drawn));
    const shows = new Map(first.map((t) => [t.id, t.shows]));
    let ticked = false;
    for (let i = 0; i < 30 && !ticked; i++) {
      await sleep(1000);
      ticked = (await shownTiles(page)).some((t) => shows.has(t.id) && shows.get(t.id) !== t.shows);
    }
    await page.locator('.dc-sheet-tab:has(.dc-sheet-label:text-is("Crypto"))').first().click();
    const shownAt = Date.now();
    let crypto = [];
    for (;;) {
      crypto = await shownTiles(page);
      if ((crypto.length > 0 && crypto.every((t) => (t.kind === 'grid' ? t.rows > 0 : t.drawn))) || Date.now() - shownAt > 120_000) break;
      await sleep(250);
    }
    const cryptoDrawn = crypto.length > 0 && crypto.every((t) => (t.kind === 'grid' ? t.rows > 0 : t.drawn));
    await page.screenshot({ path: join(outputs, 'desk-saved-reopened.png') });
    if (drawn && ticked && cryptoDrawn) {
      ok(`saved by Save As… as “${NAME}”, found in Open… after DataCube opened again, and opened from it in ${((Date.now() - reopening) / 1000).toFixed(1)} s: its ten tabs, the first drawn and ticking, Crypto read when shown`);
    } else {
      fail(`“${NAME}” opened again: first tab ${drawn ? 'drawn' : 'not drawn'}, ${ticked ? 'ticking' : 'not ticking'}; Crypto ${cryptoDrawn ? 'drawn' : 'not drawn'}`);
    }
  }
} catch (e) {
  fail(String(e?.stack ?? e));
} finally {
  await browser?.close();
  site.close();
  await server.stop();
}
console.log(failed ? '\nFAIL the trading desk' : '\nok   the trading desk');
process.exit(failed ? 1 : 0);
