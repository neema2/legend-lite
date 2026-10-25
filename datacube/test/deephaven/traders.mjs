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
    // half an hour of the session before the world starts, on a bounded tape: a stock's chart has a morning to draw
    MARKET_MORNING: process.env.TRADERS_MORNING ?? '1800',
    MARKET_MORNING_PRINTS: '120000',
    MARKET_FEED: 'coinbase',
    MARKET_FEED_URL: 'wss://127.0.0.1:9/',
    // the newswire at a busy terminal's pace (K15): a story every second or so, each pane of the news monitor its own
    MARKET_NEWS: '90',
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
      `${t.getAttribute('data-tile')}: ${[...t.querySelectorAll('.dc-status-note-text, .dc-error, [role="alert"], .dc-chartpanel-status')].map((e) => e.textContent).join(' | ')}`));
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
      // a view of the newswire changes when a story it shows crosses, not at every tick of the market: its panes are
      // checked by K15's own (a story reaching its pane), not by this window
      const wire = (n) => b.grids[n]?.source === 'The newswire';
      const still = Object.entries(b.grids).filter(([n, g]) => !wire(n) && g.ticks - (a.grids[n]?.ticks ?? 0) === 0).map(([n]) => n)
        .concat(charts.filter((id) => !wire(b.charts[id].grid) && b.charts[id].draws - (a.charts[id]?.draws ?? 0) === 0));
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

  /** K15: THE NEWS MONITOR -- the wire ticks into its panes, each keeping only its own stories; a keyword typed into a
   *  pane's box; a ticker clicked on a card charting that name beside them. */
  const newsMonitor = async () => {
    await page.evaluate(() => window.__traders.showSheet('Monitor'));
    await sleep(800);
    const doc = JSON.parse(await page.evaluate(() => window.__traders.save()));
    const largest = doc.views.find((v) => v.id === 'namesFeed').title.split(': ')[1].split(', ');
    // a story crossing reaches the market's pane at its top, read from the rows followed: nothing sent or fetched
    const topOf = () => page.evaluate(() => document.querySelector('[data-tile="allFeed"] .dc-feed-card')?.getAttribute('data-key') ?? null);
    const a = await state();
    const top0 = await topOf();
    let top1 = top0;
    for (const end = Date.now() + 20_000; Date.now() < end && top1 === top0;) {
      await sleep(250);
      top1 = await topOf();
    }
    const b = await state();
    const [sent, fetched] = [b.counts.sent - a.counts.sent, b.counts.fetched - a.counts.fetched];
    if (top0 !== null && top1 !== top0 && sent === 0 && fetched === 0) ok(`the newswire ticks into its pane: a new story at the top of All news (${top0} to ${top1}), nothing sent or fetched`);
    else fail(`All news did not tick: its top ${top0} then ${top1}, ${sent} sent, ${fetched} fetched`);
    await page.screenshot({ path: join(outputs, 'news-monitor.png') });

    // each pane its own stories: its grid's rows, and its cards
    const panes = [
      ['Macro and sectors, as a grid', 'macroFeed', (r, c) => ['Macro', 'Industry'].includes(r[c('category')])],
      ['The five largest names, as a grid', 'namesFeed', (r, c) => largest.includes(r[c('ticker')])],
      ['Technology, as a grid', 'techFeed', (r, c) => r[c('sector')] === 'Technology'],
      ['Keyword, as a grid', 'wordFeed', (r, c) => /buyback/i.test(`${r[c('headline')]} ${r[c('body')] ?? ''}`)],
    ];
    const s = await state();
    const wrong = [];
    for (const [name, feed, keeps] of panes) {
      const g = s.grids[name];
      const c = (column) => g.columns.indexOf(column);
      const bad = g.rows.filter((r) => !keeps(r, c));
      const cards = await page.locator(`[data-tile="${feed}"] .dc-feed-card`).count();
      if (g.rows.length === 0 || bad.length > 0 || cards === 0) wrong.push(`${name}: ${g.rows.length} rows, ${bad.length} not its own, ${cards} cards`);
    }
    if (wrong.length === 0) ok(`each pane its own stories: ${panes.map(([n]) => `${n.split(',')[0]} ${s.grids[n].rows.length}`).join(', ')} (the five largest: ${largest.join(', ')})`);
    else fail(`a pane shows another's stories -- ${wrong.join('; ')}`);

    // a keyword typed into a pane's box: its stories with it, its cards too, kept with the page
    const box = page.locator('[data-tile="wordFeed"] .dc-chartpanel-search');
    await box.fill('dividend');
    await box.press('Enter');
    const typed = await until((x) => {
      const g = x.grids['Keyword, as a grid'];
      return g.rows.length > 0 && g.rows.every((r) => /dividend/i.test(String(r[g.columns.indexOf('headline')])));
    }, 'the keyword pane at "dividend"', 30_000);
    await sleep(1000);
    const said = await page.locator('[data-tile="wordFeed"] .dc-feed-headline').allTextContents();
    const saved = JSON.parse(await page.evaluate(() => window.__traders.save())).views.find((v) => v.id === 'wordFeed').selection;
    if (said.length > 0 && said.every((h) => /dividend/i.test(h)) && JSON.stringify(saved).includes('dividend')) {
      ok(`a keyword typed into a pane: "dividend", ${typed.grids['Keyword, as a grid'].rows.length} stories, each card with it, kept with the page`);
    } else fail(`the keyword "dividend": cards ${JSON.stringify(said.slice(0, 3))}, saved ${JSON.stringify(saved)}`);
    await page.screenshot({ path: join(outputs, 'news-keyword.png') });

    // a ticker clicked on a card: the name charted beside the panes
    const chip = page.locator('[data-tile="namesFeed"] .dc-feed-ticker').filter({ hasNotText: largest[0] }).first();
    const sym = await chip.textContent();
    await chip.click();
    await until((x) => {
      const g = x.grids['Its prints, newest first'];
      return g.rows.length > 0 && g.rows.every((r) => r[g.columns.indexOf('ticker')] === sym);
    }, `the chart beside the panes at ${sym}`, 30_000);
    ok(`a ticker clicked on a card charts its name beside the panes: ${sym}`);
    await page.screenshot({ path: join(outputs, `news-${sym}.png`) });
  };

  if (only) {
    const one = await checkScreen(only, 1);
    if (only === 'news') await newsMonitor();
    for (const [n, g] of Object.entries(one.state.grids)) console.log(`     ${n}: base ${JSON.stringify(g.base)}`);
    console.log(`     the planner's calls: ${Object.entries(one.state.planner).map(([k, v]) => `${k} ${v.calls} in ${(v.ms / 1000).toFixed(1)} s (slowest ${v.slowest} ms)`).join(', ')}`);
    // WHERE A TICK'S TIME GOES (--test_env=TRADERS_PROFILE=1): the page's first sheet ticking for five seconds, profiled
    // by the browser, its functions by their own time; the profile itself among the test's outputs
    if (process.env.TRADERS_PROFILE) {
      const cdp = await page.context().newCDPSession(page);
      await cdp.send('Profiler.enable');
      await page.evaluate((n) => window.__traders.showSheet(n), one.doc.sheets[0].name ?? one.doc.sheets[0].id);
      await sleep(1500);
      await cdp.send('Profiler.start');
      await sleep(5000);
      const { profile } = await cdp.send('Profiler.stop');
      const { writeFileSync } = await import('node:fs');
      writeFileSync(join(outputs, 'ticks.cpuprofile'), JSON.stringify(profile));
      const total = profile.endTime - profile.startTime;
      const self = new Map();
      const dt = profile.timeDeltas;
      const byId = new Map(profile.nodes.map((n) => [n.id, n]));
      profile.samples.forEach((id, i) => {
        const n = byId.get(id);
        const f = n.callFrame;
        const key = `${f.functionName || '(anonymous)'} ${f.url.split('/').pop()}:${f.lineNumber + 1}`;
        self.set(key, (self.get(key) ?? 0) + (dt[i] ?? 0));
      });
      const top = [...self].sort((a, b) => b[1] - a[1]).slice(0, 25);
      console.log(`     profile: ${(total / 1000).toFixed(0)} ms, its functions by their own time:`);
      for (const [k, us] of top) console.log(`       ${(us / 1000).toFixed(0).padStart(6)} ms  ${k}`);
    }
    throw new Error(`only ${only} was checked (TRADERS_ONLY): not a run of every screen`);
  }
  const first = await checkScreen('Ticker', 1);
  console.log(`     the planner's calls: ${Object.entries(first.state.planner).map(([k, v]) => `${k} ${v.calls} in ${(v.ms / 1000).toFixed(1)} s`).join(', ')}`);
  const startup = Object.values(first.state.startup).reduce((a, b) => a + b, 0);
  ok(`the page live in ${(startup / 1000).toFixed(1)} s from a cold tab: ${Object.entries(first.state.startup).map(([k, v]) => `${k} ${(v / 1000).toFixed(1)} s`).join(', ')}`);
  const screens = first.state.screens;

  // K14: THE STOCK -- a name picked on the watchlist takes its every tile with it; its chart's spans; its candles
  {
    const grid = (s, name) => s.grids[name];
    const cell = (g, column) => g?.rows[0]?.[g.columns.indexOf(column)];
    await page.evaluate(() => window.__traders.showSheet('Stock'));
    await sleep(500);
    const before = await state();
    const was = cell(grid(before, 'Its quote'), 'ticker');
    // another name on the list: a person's click on its row
    const names = grid(before, 'Watchlist: pick a name').rows.map((r) => r[grid(before, 'Watchlist: pick a name').columns.indexOf('ticker')]);
    const other = names.find((n) => n !== was && !String(n).endsWith('-USD'));
    await page.locator('[data-tile="pick"] .dc-row', { hasText: other }).first().locator('.dc-cell').first().click();
    const followed = await until((s) => cell(grid(s, 'Its quote'), 'ticker') === other
      && grid(s, 'Its prints, newest first').rows.length > 0
      && grid(s, 'Its prints, newest first').rows.every((r) => r[grid(s, 'Its prints, newest first').columns.indexOf('ticker')] === other),
    `the stock's grids at ${other}`);
    // its chart and its last price: the quote's, give or take a tick
    await sleep(2500);
    const after = await state();
    const quoteLast = Number(cell(grid(after, 'Its quote'), 'bid'));
    const kpi = Number(String(after.charts['last']?.values[0] ?? '').replace(/,/g, ''));
    const line = await page.evaluate(() => window.__traders.chartNow('price'));
    const lineLast = Number(line?.last?.[1]);
    const near = (a, b) => Math.abs(a - b) / b < 0.01;
    if (near(kpi, quoteLast) && near(lineLast, quoteLast)) {
      ok(`a name picked on the watchlist takes the stock's tiles with it: ${was} to ${other} -- its quote, its prints, its last ${kpi} and its line's ${lineLast} (bid ${quoteLast})`);
    } else fail(`picked ${other}: the quote ${JSON.stringify(cell(followed.grids['Its quote'], 'ticker'))}, the KPI ${kpi}, the line's last ${lineLast}, the bid ${quoteLast}`);
    await page.screenshot({ path: join(outputs, `stock-${other}.png`) });

    // THE SPANS: 5m, by the second, its window the latest five minutes, following the latest mark
    await page.locator('[data-tile="price"] .dc-chartpanel-span[data-span="300"]').click();
    await sleep(3000);
    const spanned = await page.evaluate(() => window.__traders.chartNow('price'));
    const spec = JSON.parse(await page.evaluate(() => window.__traders.save())).views.find((v) => v.id === 'price').spec;
    const width = (spanned?.zoom?.endValue ?? 0) - (spanned?.zoom?.startValue ?? 0);
    const edge = Math.abs((spanned?.zoom?.endValue ?? 0) - new Date(String(spanned?.last?.[0]).replace(' ', 'T')).getTime());
    if (spec.grain === 'second' && spec.options.span === 300 && width === 300_000 && edge < 1500) {
      ok(`a span picked: the latest 5 minutes by the second, the window ending at the latest mark (${JSON.stringify(spanned.zoom)})`);
    } else fail(`5m: the spec ${spec.grain} ${spec.options.span}, the window ${JSON.stringify(spanned?.zoom)}, the latest ${JSON.stringify(spanned?.last)}`);
    await page.screenshot({ path: join(outputs, 'stock-5m.png') });

    // THE CANDLES, behind the line: their tab shown, they draw
    await page.locator('.dc-tile-tab[data-tab="candles"] .dc-tile-tab-label').click();
    const drew = await page.waitForFunction(() => {
      const el = document.querySelector('[data-tile="candles"] .dc-chartpanel-chart canvas');
      return el !== null && el.getBoundingClientRect().width > 0;
    }, null, { timeout: 30_000 }).then(() => true, () => false);
    if (drew) ok('the candles, behind the line: their tab shown, they draw');
    else fail('the candles did not draw when their tab was shown');
    await page.screenshot({ path: join(outputs, 'stock-candles.png') });
    await page.locator('.dc-tile-tab[data-tab="price"] .dc-tile-tab-label').click();

    // K15: ITS NEWS, AS IT CROSSES -- the stories about the name picked, followed with it
    const news = await until((s) => {
      const g = grid(s, 'Its news, as a grid');
      return g !== undefined && g.rows.every((r) => r[g.columns.indexOf('ticker')] === other);
    }, `its news at ${other}`, 30_000);
    const cards = await page.locator('[data-tile="news"] .dc-feed-card').evaluateAll((els) => els.map((e) =>
      [...e.querySelectorAll('.dc-feed-ticker')].map((t) => t.textContent)));
    const rows = grid(news, 'Its news, as a grid').rows.length;
    if (cards.every((t) => t.includes(other)) && cards.length === Math.min(rows, 40)) {
      ok(`its news followed the name picked: ${cards.length} stories about ${other} as cards, its grid behind them ${rows} rows`);
    } else fail(`its news at ${other}: cards naming ${JSON.stringify(cards.slice(0, 5))}, its grid ${rows} rows`);
    await page.screenshot({ path: join(outputs, `stock-news-${other}.png`) });
  }

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
    if (screen.id === 'news') await newsMonitor();
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
