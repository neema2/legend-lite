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
let chrome;
try {
  // the browser's own output, every line, to browser.log in the outputs, and its fatal lines here: a renderer that
  // crashes says why there, nowhere else (2026-10-28: the renderer crashed at Run On's switch in 3 runs, no kernel kill,
  // nothing said -- Playwright's logger never sees the browser's own output, its process does)
  const { createWriteStream } = await import('node:fs');
  chrome = await chromium.launchServer({ args: ['--enable-logging=stderr', '--log-level=1'] });
  const said = createWriteStream(join(outputs, 'browser.log'));
  for (const out of [chrome.process().stdout, chrome.process().stderr]) {
    out?.on('data', (b) => {
      said.write(b);
      for (const line of String(b).split('\n')) {
        if (/FATAL|Check failed|crash|Received signal|OOM|out of memory|V8 error/i.test(line)) console.log(`[browser] ${line.slice(0, 400)}`);
      }
    });
  }
  browser = await chromium.connect(chrome.wsEndpoint());
  const page = await (await browser.newContext({ ignoreHTTPSErrors: true, viewport: { width: 1680, height: 1050 } })).newPage();
  page.on('pageerror', (e) => fail(`page error: ${e.message}`));
  page.on('console', (m) => { if (m.type() === 'error' || m.type() === 'warning') console.log(`[page ${m.type()}] ${m.text().slice(0, 300)}`); });
  const started = Date.now();
  await page.goto(site.desk);

  // THE DESK OPENED, EACH TAB DRAWN AND TICKING (demo/desk-tabs.mjs, the in-tab desk's lane's checks too)
  await checkDeskTabs(page, { ok, fail, outputs, started });
  // THE SCREENS' INTERACTIONS, as a person makes them on the desk (moved here from the trader screens' own test,
  // 2026-10-26: the desk is the one demo, its screens' host retired) -- each grid's rows read from the page the harness
  // holds (datacube-app.ts `__dataPage`: hidden columns too), the server's own counts from DataCube's Deephaven grids
  // (src/deephaven-grids.ts `__deephaven`). A chart is read by what it says and what its page keeps: the window a span draws is chart-echarts'
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
  const counts = () => page.evaluate(() => window.__deephaven.counts());
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
    // its cards as its grid has its stories: read together until they agree -- a story crossing between the two reads
    // draws the cards after the grid (the feed redraws from the grid's rows), so one read of each may straddle it
    const cardsOf = () => page.locator('[data-tile="st-news"] .dc-feed-card').evaluateAll((els) => els.map((e) =>
      [...e.querySelectorAll('.dc-feed-ticker')].map((t) => t.textContent)));
    let seen = { cards: await cardsOf(), rows: news.rows.length };
    const agreed = await until(async () => {
      const [g, cards] = [await rowsOf('st-newsTable'), await cardsOf()];
      seen = { cards, rows: g?.rows.length ?? 0 };
      return g !== null && cards.every((t) => t.includes(other)) && cards.length === Math.min(g.rows.length, 40);
    }, `its news cards at ${other}`).then(() => true, () => false);
    if (agreed) {
      ok(`Stock: its news followed the name picked: ${seen.cards.length} stories about ${other} as cards, its grid behind them ${seen.rows} rows`);
    } else fail(`Stock: its news at ${other}: cards naming ${JSON.stringify(seen.cards.slice(0, 5))}, ${seen.cards.length} cards, its grid ${seen.rows} rows`);
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
  // the market in the tab sized as the in-tab lane sizes it (demo/verify-live-desk.mjs): Run On, below, starts it
  await page.goto(`${site.url}/demo/index.html?market-names=40&market-tape=5000&market-feed=off`);
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

  // RUN ON (docs/ONE_CONNECTION_DESK_2026_10_28.md D2, step 4): the saved desk switched to its data space's other
  // context -- the tab's DuckDB, its market started in the tab -- and back: the same page, every grid planned on the
  // context's runtime and run where its connections say, the menu's check moving with it
  const runOnEntries = async () => {
    await page.click('.dc-titlebar-menu');
    await page.locator('.dc-menu-item:has(> .dc-menu-label:text-is("Run On"))').first().hover();
    const entries = await page.evaluate(() => [...document.querySelectorAll('.dc-menu-item[aria-checked]')]
      .map((e) => [e.querySelector('.dc-menu-label')?.textContent?.trim(), e.getAttribute('aria-checked')]));
    return entries;
  };
  /** The first tab's tiles, all drawn (a grid with rows), within `ms`. */
  const firstTabDrawn = async (ms) => {
    await page.locator('.dc-sheet-tab').first().click();
    const end = Date.now() + ms;
    for (;;) {
      const tiles = await shownTiles(page);
      if (tiles.length > 0 && tiles.every((t) => (t.kind === 'grid' ? t.rows > 0 : t.drawn))) return true;
      if (Date.now() > end) return false;
      await sleep(250);
    }
  };
  let menuSeen = null;
  // MEASURED at each switch (2026-10-28: the page's renderer crashed in 3 of 7 runs during a switch, at ~1.5 GB and no
  // kernel kill, not since the browser's fatal lines were logged, above): each renderer's resident memory, in MB
  const { execSync } = await import('node:child_process');
  const renderers = () => execSync('ps -eo rss,args').toString().split('\n').filter((l) => l.includes('--type=renderer'))
    .map((l) => Math.round(Number.parseInt(l.trim(), 10) / 1024)).sort((a, b) => b - a);
  const heap = async () => {
    const cdp = await page.context().newCDPSession(page);
    const used = (await cdp.send('Runtime.getHeapUsage')).usedSize;
    await cdp.detach();
    return Math.round(used / 1048576);
  };
  console.log(`[measure] before Run On: renderers ${JSON.stringify(renderers())} MB, the page's heap ${await heap()} MB`);
  try {
    // the Deephaven context's title names its server: this test's, at its own port (environments.mjs)
    const before = await runOnEntries();
    const deephaven = before.find(([label]) => label?.startsWith('Deephaven ('))?.[0];
    menuSeen = await page.evaluate(() => [...document.querySelectorAll('.dc-menu-item')].filter((e) => e.getBoundingClientRect().width > 0)
      .map((e) => `${e.querySelector('.dc-menu-label')?.textContent?.trim()}${e.getAttribute('aria-checked') ? `[${e.getAttribute('aria-checked')}]` : ''}`));
    const onDeephaven = JSON.stringify(before) === JSON.stringify([['In this tab (DuckDB)', 'false'], [deephaven, 'true']]);
    await page.locator('.dc-menu-item:has(> .dc-menu-label:text-is("In this tab (DuckDB)"))').first().click();
    const switching = Date.now();
    // the page opened again on the tab's context: its market started in the tab (market-sim/local-host.ts `__market`),
    // then its first tab drawn from it
    const worldRowsNow = () => page.evaluate(() => {
      const c = window.__market?.counts?.();
      return c === undefined ? 0 : Object.values(c.world.rows ?? {}).reduce((a, b) => a + Number(b), 0);
    });
    const worldRows = await until(worldRowsNow, 'the market in the tab writing', 180_000).catch(() => 0);
    const inTab = worldRows > 0 && await firstTabDrawn(180_000);
    const tabAt = ((Date.now() - switching) / 1000).toFixed(1);
    console.log(`[measure] on the tab: renderers ${JSON.stringify(renderers())} MB, the page's heap ${await heap()} MB`);
    const after = await runOnEntries();
    await page.keyboard.press('Escape');
    const changed = await page.evaluate(() => document.title.startsWith('\u2022'));
    await page.screenshot({ path: join(outputs, 'desk-run-on-tab.png') });
    await page.click('.dc-titlebar-menu');
    await page.locator('.dc-menu-item:has(> .dc-menu-label:text-is("Run On"))').first().hover();
    await page.locator(`.dc-menu-item:has(> .dc-menu-label:text-is("${deephaven}"))`).first().click();
    const back = Date.now();
    // read from the server again (a result the session keeps alive is read, not sent again: deephaven.ts keepAlive)
    const sent = (await counts()).queries;
    // the page opened again on the server: its grids' queries read from it, its first tab drawn from them
    await until(async () => (await counts()).queries > sent, 'queries read from the server again', 180_000).catch(() => undefined);
    const onServer = await firstTabDrawn(180_000);
    const backAt = ((Date.now() - back) / 1000).toFixed(1);
    console.log(`[measure] back on Deephaven: renderers ${JSON.stringify(renderers())} MB, the page's heap ${await heap()} MB`);
    const sentAfter = (await counts()).queries;
    const backEntries = await runOnEntries();
    await page.keyboard.press('Escape');
    const said = await page.evaluate(() => [...document.querySelectorAll('#cubelib .dc-lib-status, .dc-status')].map((e) => e.textContent?.trim()).filter(Boolean));
    menuSeen = { backEntries, said, counts: await counts() };
    if (onDeephaven && inTab && worldRows > 0 && JSON.stringify(after) === JSON.stringify([['In this tab (DuckDB)', 'true'], [deephaven, 'false']])
      && changed && onServer && sentAfter > sent && JSON.stringify(menuSeen.backEntries) === JSON.stringify(before)) {
      ok(`Run On: the saved desk on Deephaven, switched to the tab's DuckDB (its first tab drawn in ${tabAt} s, the market in the tab writing ${worldRows.toLocaleString('en-US')} rows, the page changed until saved) and back to Deephaven (${backAt} s, ${sentAfter - sent} queries read from the server)`);
    } else {
      fail(`Run On: before ${JSON.stringify(before)}; in the tab ${inTab ? 'drawn' : 'not drawn'} (${worldRows} rows written), after ${JSON.stringify(after)}, changed ${changed}; back on Deephaven ${onServer ? 'drawn' : 'not drawn'}, ${sentAfter - sent} queries read (${JSON.stringify(menuSeen)})`);
    }
  } catch (e) {
    await page.screenshot({ path: join(outputs, 'desk-run-on-failed.png') }).catch(() => undefined);
    fail(`Run On: ${e?.message ?? e} (the menu showed ${JSON.stringify(menuSeen)})`);
  }

  // THE SERVER RESTARTED (docs/DEEPHAVEN_START_HERE.md, the desk under load, 4): Deephaven keeps no session across one,
  // and its world starts again. The page signs in again once -- every grid's end the same sign-in, and a session or
  // live connection it let go of ending as it closes is no server gone -- and its tab shown is drawn and ticks again,
  // nothing thrown (a page error fails the lane, above)
  try {
    const signedIn = (await counts()).reconnects;
    server.restart();
    const restarted = await server.next('on restarting');
    if (!restarted.restarted) fail(`the fixture answered ${JSON.stringify(restarted)}`);
    const restarting = Date.now();
    await until(async () => (await counts()).reconnects > signedIn, 'signed in again after the restart', 240_000);
    const drawn = await firstTabDrawn(180_000);
    const before = new Map((await shownTiles(page)).map((t) => [t.id, t.shows]));
    const ticked = await until(async () => (await shownTiles(page))
      .filter((t) => before.has(t.id) && before.get(t.id) !== t.shows).map((t) => t.id).join(', '),
    'the first tab ticking after the restart', 60_000).catch(() => '');
    const backAt = ((Date.now() - restarting) / 1000).toFixed(1);
    // a while longer: no sign-in after the one, each grid ticking from it
    await sleep(15_000);
    const signIns = (await counts()).reconnects - signedIn;
    await page.screenshot({ path: join(outputs, 'desk-restarted.png') });
    if (drawn && ticked !== '' && signIns === 1) {
      ok(`restarted: the page signed in again once and its first tab drawn and ticking in ${backAt} s (${ticked} changed)`);
    } else {
      fail(`restarted: ${signIns} sign-ins, the first tab ${drawn ? 'drawn' : 'not drawn'}, ${ticked === '' ? 'not ticking' : `${ticked} changed`}`);
    }
  } catch (e) {
    await page.screenshot({ path: join(outputs, 'desk-restart-failed.png') }).catch(() => undefined);
    fail(`restarted: ${e?.message ?? e}`);
  }
} catch (e) {
  fail(String(e?.stack ?? e));
} finally {
  await browser?.close();
  await chrome?.close();
  site.close();
  await server.stop();
}
console.log(failed ? '\nFAIL the trading desk' : '\nok   the trading desk');
process.exit(failed ? 1 : 0);
