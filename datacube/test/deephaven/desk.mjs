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
import { TABS, appStatus, checkDeskTabs, shownTiles } from '../../demo/desk-tabs.mjs';

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

let browser;
try {
  browser = await chromium.launch();
  const page = await (await browser.newContext({ ignoreHTTPSErrors: true, viewport: { width: 1680, height: 1050 } })).newPage();
  page.on('pageerror', (e) => fail(`page error: ${e.message}`));
  page.on('console', (m) => { if (m.type() === 'error' || m.type() === 'warning') console.log(`[page ${m.type()}] ${m.text().slice(0, 300)}`); });
  const started = Date.now();
  await page.goto(site.desk);

  // THE DESK OPENED, EACH TAB DRAWN AND TICKING (demo/desk-tabs.mjs, the in-tab desk's lane's checks too)
  await checkDeskTabs(page, { ok, fail, outputs, started });
  // THE SCREENS' INTERACTIONS, as a person makes them on the desk (moved here from the trader screens' own test,
  // 2026-10-26: the desk is the one demo, its screens' host retired) -- each grid's rows read from the page the harness
  // holds (datacube-app.ts `__dataPage`: hidden columns too), the market's own counts from its host (market/host.ts
  // `__market`). A chart is read by what it says and what its page keeps: the window a span draws is chart-echarts'
  // own test's (a span of time: the latest that many seconds, a window ending at the latest mark)
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
    if (Math.abs(kpi - bid) / bid < 0.01) ok(`Stock: a name picked on the watchlist takes the tiles with it: ${was} to ${other} -- its quote, its prints and its last ${kpi} (bid ${bid})`);
    else fail(`Stock: picked ${other}: the KPI ${kpi}, the bid ${bid}`);
    await page.screenshot({ path: join(outputs, `desk-stock-${other}.png`) });

    // THE SPANS: 5m, by the second -- its button pressed, the chart drawn again, kept with the page
    await page.locator('[data-tile="st-price"] .dc-chartpanel-span[data-span="300"]').click();
    await sleep(3000);
    const spec = (await viewOf('st-price')).spec;
    const pressed = await page.locator('[data-tile="st-price"] .dc-chartpanel-span[aria-pressed="true"]').getAttribute('data-span');
    const drawn = await page.locator('[data-tile="st-price"] .dc-chartpanel-chart canvas').count();
    if (spec.grain === 'second' && spec.options.span === 300 && pressed === '300' && drawn > 0) {
      ok('Stock: a span picked: the latest 5 minutes by the second, its button pressed, the chart drawn, kept with the page');
    } else fail(`Stock: 5m: the spec ${spec.grain} ${spec.options.span}, the button pressed ${pressed}, ${drawn} canvases`);

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
    // the watchlist (the user, 2026-10-27): the person's names, its pane's filter, starting on the market's five largest
    // (market::screens::largestNames(5), read when the page is written) -- the names its pane's stories are about, at
    // most five, the market's largest -- the name the pane's chart opens on -- among them
    const largestRows = await rowsOf('nw-namesTable');
    const largest = [...new Set(largestRows.rows.map((r) => r[largestRows.columns.indexOf('ticker')]))];
    const opensOn = (await rowsOf('nw-prints')).rows[0]?.[(await rowsOf('nw-prints')).columns.indexOf('ticker')];
    const watchTitle = (await viewOf('nw-namesFeed'))?.title;
    if (largest.length >= 1 && largest.length <= 5 && largest.includes(opensOn) && watchTitle === 'My watchlist') {
      ok(`News: ${watchTitle}'s stories: about ${largest.join(', ')}, the largest (${opensOn}) among them`);
    } else fail(`News: the watchlist pane (${watchTitle}) is about ${JSON.stringify(largest)}, its chart opening on ${opensOn}`);
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
      ['My watchlist', 'nw-namesTable', 'nw-namesFeed', (r, c) => largest.includes(r[c('ticker')])],
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
    if (wrong.length === 0) ok(`News: each pane its own stories: ${said.join(', ')} (the watchlist: ${largest.join(', ')})`);
    else fail(`News: a pane shows another's stories -- ${wrong.join('; ')}`);

    const box = page.locator('[data-tile="nw-wordFeed"] .dc-chartpanel-search');
    await box.fill('dividend');
    await box.press('Enter');
    const typed = await until(async () => {
      const g = await rowsOf('nw-wordTable');
      return every(g, 'headline', (h) => /dividend/i.test(String(h))) ? g : null;
    }, 'the keyword pane at "dividend"');
    // the pane's cards drawn from those rows: waited for, not a second's sleep -- its feed redraws after its grid (the
    // 2026-10-28 run read the cards a second after the grid and found the keyword before's, buybacks, still drawn)
    const cards = page.locator('[data-tile="nw-wordFeed"] .dc-feed-headline');
    const headlines = await until(async () => {
      const shown = await cards.allTextContents();
      return shown.length > 0 && shown.every((h) => /dividend/i.test(h)) ? shown : null;
    }, 'the keyword pane\'s cards at "dividend"').catch(() => cards.allTextContents());
    const kept = JSON.stringify((await viewOf('nw-wordFeed')).selection ?? null);
    if (headlines.length > 0 && headlines.every((h) => /dividend/i.test(h)) && kept.includes('dividend')) {
      ok(`News: a keyword typed into a pane: "dividend", ${typed.rows.length} stories, each card with it, kept with the page`);
    } else fail(`News: the keyword "dividend": cards ${JSON.stringify(headlines.slice(0, 3))}, kept ${kept}`);

    const chip = page.locator('[data-tile="nw-namesFeed"] .dc-feed-ticker').filter({ hasNotText: opensOn }).first();
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

  const status = await appStatus(page);
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
