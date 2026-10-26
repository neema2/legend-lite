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
  // THE SCREENS' INTERACTIONS, as a person makes them on the desk (moved here from the trader screens' own test,
  // 2026-10-26: the desk is the one demo, its screens' host retired) -- each grid's rows read from the page the harness
  // holds (datacube-app.ts `__dataPage`: hidden columns too), the market's own counts and a chart as drawn from its host
  // (market/host.ts `__market`)
  const showTab = (name) => page.locator(`.dc-sheet-tab:has(.dc-sheet-label:text-is("${name}"))`).first().click();
  /** A grid's rows as its view has them now, every column; null before its first view. */
  const rowsOf = (tile) => page.evaluate((t) => {
    const v = window.__dataPage?.grid(t)?.view;
    if (!v) return null;
    const cols = v.rows.columns;
    return { columns: cols.map((c) => c.name), rows: Array.from({ length: v.rows.rowCount }, (_, i) => cols.map((c) => c.values[i])) };
  }, tile);
  const cell = (g, column) => g?.rows[0]?.[g.columns.indexOf(column)];
  const every = (g, column, test) => g !== null && g.rows.length > 0 && g.rows.every((r) => test(r[g.columns.indexOf(column)]));
  const viewOf = (id) => page.evaluate((v) => window.__dataPage.views().views.find((x) => x.id === v), id);
  const counts = () => page.evaluate(() => window.__market.counts());
  const chartNow = (tile) => page.evaluate((t) => window.__market.chartNow(t), tile);
  /** `test`, asked until it holds (its last answer), or a failure saying `what` after `ms`. */
  const until = async (test, what, ms = 30_000) => {
    const end = Date.now() + ms;
    for (;;) {
      const got = await test();
      if (got) return got;
      if (Date.now() > end) throw new Error(`not ${what} in ${ms / 1000} s`);
      await sleep(250);
    }
  };
  /** A grid's row whose `column` shows `value`, clicked on its first cell as a person picks it. */
  const pickRow = (tile, column, value) => page.locator(`[data-tile="${tile}"] .dc-row:has([data-column="${column}"]:text-is("${value}"))`)
    .first().locator('.dc-cell').first().click();

  // K14: THE STOCK -- a name picked on the watchlist takes the stock's tiles with it; its chart's spans; its candles; its
  // news as it crosses
  try {
    await showTab('Stock');
    await sleep(500);
    const was = cell(await rowsOf('st-quote'), 'ticker');
    const pick = await rowsOf('st-pick');
    const other = pick.rows.map((r) => r[pick.columns.indexOf('ticker')]).find((n) => n !== was && !String(n).endsWith('-USD'));
    await pickRow('st-pick', 'ticker', other);
    await until(async () => cell(await rowsOf('st-quote'), 'ticker') === other
      && every(await rowsOf('st-prints'), 'ticker', (t) => t === other), `the stock's grids at ${other}`);
    await sleep(2500);
    const bid = Number(cell(await rowsOf('st-quote'), 'bid'));
    const kpi = Number(String(await page.locator('[data-tile="st-last"] .dc-kpi-value').first().textContent()).replace(/,/g, ''));
    const lineLast = Number((await chartNow('st-price'))?.last?.[1]);
    const near = (a, b) => Math.abs(a - b) / b < 0.01;
    if (near(kpi, bid) && near(lineLast, bid)) ok(`Stock: a name picked on the watchlist takes the tiles with it: ${was} to ${other} -- its quote, its prints, its last ${kpi} and its line's ${lineLast} (bid ${bid})`);
    else fail(`Stock: picked ${other}: the KPI ${kpi}, the line's last ${lineLast}, the bid ${bid}`);
    await page.screenshot({ path: join(outputs, `desk-stock-${other}.png`) });

    // THE SPANS: 5m, by the second, its window the latest five minutes, ending at the latest mark
    await page.locator('[data-tile="st-price"] .dc-chartpanel-span[data-span="300"]').click();
    await sleep(3000);
    const spanned = await chartNow('st-price');
    const spec = (await viewOf('st-price')).spec;
    const width = (spanned?.zoom?.endValue ?? 0) - (spanned?.zoom?.startValue ?? 0);
    const edge = Math.abs((spanned?.zoom?.endValue ?? 0) - new Date(String(spanned?.last?.[0]).replace(' ', 'T')).getTime());
    if (spec.grain === 'second' && spec.options.span === 300 && width === 300_000 && edge < 1500) {
      ok(`Stock: a span picked: the latest 5 minutes by the second, the window ending at the latest mark (${JSON.stringify(spanned.zoom)})`);
    } else fail(`Stock: 5m: the spec ${spec.grain} ${spec.options.span}, the window ${JSON.stringify(spanned?.zoom)}, the latest ${JSON.stringify(spanned?.last)}`);

    // THE CANDLES, behind the line: their tab shown, they draw
    await page.locator('.dc-tile-tab[data-tab="st-candles"] .dc-tile-tab-label').click();
    const drew = await page.waitForFunction(() => {
      const el = document.querySelector('[data-tile="st-candles"] .dc-chartpanel-chart canvas');
      return el !== null && el.getBoundingClientRect().width > 0;
    }, null, { timeout: 30_000 }).then(() => true, () => false);
    if (drew) ok('Stock: the candles, behind the line: their tab shown, they draw');
    else fail('Stock: the candles did not draw when their tab was shown');
    await page.locator('.dc-tile-tab[data-tab="st-price"] .dc-tile-tab-label').click();

    // K15: ITS NEWS, AS IT CROSSES -- the stories about the name picked, followed with it
    const news = await until(async () => {
      const g = await rowsOf('st-newsTable');
      return every(g, 'ticker', (t) => t === other) || (g !== null && g.rows.length === 0) ? g : null;
    }, `its news at ${other}`);
    const cards = await page.locator('[data-tile="st-news"] .dc-feed-card').evaluateAll((els) => els.map((e) =>
      [...e.querySelectorAll('.dc-feed-ticker')].map((t) => t.textContent)));
    if (cards.every((t) => t.includes(other)) && cards.length === Math.min(news.rows.length, 40)) {
      ok(`Stock: its news followed the name picked: ${cards.length} stories about ${other} as cards, its grid behind them ${news.rows.length} rows`);
    } else fail(`Stock: its news at ${other}: cards naming ${JSON.stringify(cards.slice(0, 5))}, its grid ${news.rows.length} rows`);
  } catch (e) {
    fail(`Stock: ${e?.message ?? e}`);
  }

  // K15: THE NEWS MONITOR -- the wire ticks into its panes, each keeping only its own stories; a keyword typed into a
  // pane's box, kept with the page; a ticker clicked on a card charting that name beside them
  try {
    await showTab('News');
    await sleep(800);
    const largest = (await viewOf('nw-namesFeed')).title.split(': ')[1].split(', ');
    const topOf = () => page.evaluate(() => document.querySelector('[data-tile="nw-allFeed"] .dc-feed-card')?.getAttribute('data-key') ?? null);
    const a = await counts();
    const top0 = await topOf();
    let top1 = top0;
    for (const end = Date.now() + 20_000; Date.now() < end && top1 === top0;) {
      await sleep(250);
      top1 = await topOf();
    }
    const b = await counts();
    const [sent, fetched] = [b.sent - a.sent, b.fetched - a.fetched];
    if (top0 !== null && top1 !== top0 && sent === 0 && fetched === 0) ok(`News: the newswire ticks into its pane: a new story at the top of All news (${top0} to ${top1}), nothing sent or fetched`);
    else fail(`News: All news did not tick: its top ${top0} then ${top1}, ${sent} sent, ${fetched} fetched`);
    await page.screenshot({ path: join(outputs, 'desk-news-monitor.png') });

    // each pane's rows, by what its filter keeps (`c`: a column's place in a row)
    const panes = [
      ['Macro and sectors', 'nw-macroTable', 'nw-macroFeed', (r, c) => ['Macro', 'Industry'].includes(r[c('category')])],
      ['The five largest names', 'nw-namesTable', 'nw-namesFeed', (r, c) => largest.includes(r[c('ticker')])],
      ['Technology', 'nw-techTable', 'nw-techFeed', (r, c) => r[c('sector')] === 'Technology'],
      ['Keyword', 'nw-wordTable', 'nw-wordFeed', (r, c) => /buyback/i.test(`${r[c('headline')]} ${r[c('body')] ?? ''}`)],
    ];
    const wrong = [];
    const said = [];
    for (const [name, grid, feed, keeps] of panes) {
      const g = await rowsOf(grid);
      const c = (column) => g.columns.indexOf(column);
      const cardsIn = await page.locator(`[data-tile="${feed}"] .dc-feed-card`).count();
      if (g === null || g.rows.length === 0 || !g.rows.every((r) => keeps(r, c)) || cardsIn === 0) {
        wrong.push(`${name}: ${g?.rows.length ?? 'no'} rows, ${cardsIn} cards`);
      } else said.push(`${name} ${g.rows.length}`);
    }
    if (wrong.length === 0) ok(`News: each pane its own stories: ${said.join(', ')} (the five largest: ${largest.join(', ')})`);
    else fail(`News: a pane shows another's stories -- ${wrong.join('; ')}`);

    const box = page.locator('[data-tile="nw-wordFeed"] .dc-chartpanel-search');
    await box.fill('dividend');
    await box.press('Enter');
    const typed = await until(async () => {
      const g = await rowsOf('nw-wordTable');
      return every(g, 'headline', (h) => /dividend/i.test(String(h))) ? g : null;
    }, 'the keyword pane at "dividend"');
    await sleep(1000);
    const headlines = await page.locator('[data-tile="nw-wordFeed"] .dc-feed-headline').allTextContents();
    const kept = JSON.stringify((await viewOf('nw-wordFeed')).selection ?? null);
    if (headlines.length > 0 && headlines.every((h) => /dividend/i.test(h)) && kept.includes('dividend')) {
      ok(`News: a keyword typed into a pane: "dividend", ${typed.rows.length} stories, each card with it, kept with the page`);
    } else fail(`News: the keyword "dividend": cards ${JSON.stringify(headlines.slice(0, 3))}, kept ${kept}`);

    const chip = page.locator('[data-tile="nw-namesFeed"] .dc-feed-ticker').filter({ hasNotText: largest[0] }).first();
    const sym = await chip.textContent();
    await chip.click();
    await until(async () => every(await rowsOf('nw-prints'), 'ticker', (t) => t === sym), `the chart beside the panes at ${sym}`);
    ok(`News: a ticker clicked on a card charts its name beside the panes: ${sym}`);
    await page.screenshot({ path: join(outputs, `desk-news-${sym}.png`) });
  } catch (e) {
    fail(`News: ${e?.message ?? e}`);
  }

  // NO NAME TWICE while the movers tick -- in any view they read, nor on screen (a window over a result re-sorted at
  // every tick painted a name twice at two prices, 2026-10-25)
  try {
    await showTab('Markets');
    await sleep(300);
    const movers = [['Top gainers', 'mk-gainers'], ['Top losers', 'mk-losers'], ['Most active', 'mk-active']];
    const seen = {};
    let views = 0;
    const twiceIn = (names) => names.filter((n, i) => n !== null && n !== '' && names.indexOf(n) !== i);
    for (const end = Date.now() + 5000; Date.now() < end;) {
      const painted = await page.evaluate((tiles) => tiles.map((t) => {
        const box = document.querySelector(`[data-tile="${t}"] .dc-scroller`)?.getBoundingClientRect();
        return [...document.querySelectorAll(`[data-tile="${t}"] .dc-row`)].filter((r) => {
          const at = r.getBoundingClientRect();
          return box !== undefined && at.top >= box.top - 1 && at.bottom <= box.bottom + 1;
        }).map((r) => r.querySelector('[data-column="ticker"]')?.textContent ?? null);
      }), movers.map(([, t]) => t));
      for (const [i, [name, tile]] of movers.entries()) {
        const g = await rowsOf(tile);
        if (g !== null) {
          views++;
          const twice = twiceIn(g.rows.map((r) => r[g.columns.indexOf('ticker')]));
          if (twice.length > 0) (seen[name] ??= new Set()).add(`in its view: ${twice.join(', ')}`);
        }
        const twice = twiceIn(painted[i]);
        if (twice.length > 0) (seen[name] ??= new Set()).add(`on screen: ${twice.join(', ')}`);
      }
      await sleep(20);
    }
    const said = Object.entries(seen).map(([n, x]) => `${n}: ${[...x].slice(0, 5).join('; ')}`);
    if (said.length === 0) ok(`Markets: ${movers.map(([n]) => n).join(', ')}: no name twice in ${views} views read while ticking, nor on screen`);
    else fail(`Markets: a name shown twice while ticking -- ${said.join(' | ')}`);
  } catch (e) {
    fail(`Markets: ${e?.message ?? e}`);
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
