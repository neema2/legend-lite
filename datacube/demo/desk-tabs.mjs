// THE TRADING DESK'S TABS, CHECKED -- shared by the desk on Deephaven (test/deephaven/desk.mjs) and the desk live in the
// tab (verify-live-desk.mjs): the desk opened, its ten tabs named in order; then each tab shown as a person shows it --
// every grid's rows and every chart drawn -- and ticking: what it shows changes as the market does, nothing touched,
// each tick's time on screen after its rows measured (the hosts' dh:tick). A screenshot of each tab is the test's output.

import { join } from 'node:path';

/** The desk's tabs, in order (market/screens.ts DESK). */
export const TABS = ['Markets', 'Stock', 'News', 'Order flow', 'Crypto', 'Blotter', 'TCA', 'P&L and risk', 'Options', 'FX'];

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/** The tiles of the tab shown, as the page has them now: each grid's rows and whether it is still reading, each chart
 *  drawn (a canvas, a KPI's figure, a feed's cards), and anything a tile says went wrong. */
export const shownTiles = (page) => page.evaluate(() => [...document.querySelectorAll('[data-tile]')]
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

/** The page's status line: what it says, and whether it says something went wrong. */
export const appStatus = (page) => page.evaluate(() => {
  const s = document.getElementById('status');
  return { text: s?.textContent ?? '', bad: s?.classList.contains('bad') ?? false };
});

/**
 * The desk, opened on `page` since `started`: its tabs (within `openMs`), then each tab shown, its tiles read (within
 * `readyMs`) and ticking (within `tickMs`). Says each finding by `ok` and `fail`; screenshots into `outputs`.
 */
export async function checkDeskTabs(page, { ok, fail, outputs, started, openMs = 300_000, readyMs = 120_000, tickMs = 30_000 }) {
  // THE DESK OPENED: its ten tabs, named, in order
  const tabs = await (async () => {
    const end = Date.now() + openMs;
    for (;;) {
      const names = await page.evaluate(() => [...document.querySelectorAll('.dc-sheet-tab .dc-sheet-label')].map((e) => e.textContent?.trim()));
      if (names.length >= TABS.length) return names;
      const s = await appStatus(page);
      if (s.bad) throw new Error(`the page says: ${s.text}`);
      if (Date.now() > end) {
        await page.screenshot({ path: join(outputs, 'stalled.png') });
        throw new Error(`the desk did not open in ${openMs / 1000} s (tabs: ${JSON.stringify(names)}; status: ${s.text})`);
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
      if (ready || tiles.some((t) => t.errors.length > 0) || Date.now() - shownAt > readyMs) break;
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
    const tickEnd = Date.now() + tickMs;
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
    else fail(`${name} did not tick in ${tickMs / 1000} s`);
  }
}
