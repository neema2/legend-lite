// THE TRADING DESK LIVE IN THE TAB, CHECKED (//datacube:verify_live_desk_test; docs/DUCKDB_LIVE_2026_10_25.md L3-L4):
// DataCube's own site as it is published -- no config, no server behind it -- opened on the trading desk
// (`#example=trading-desk`, as Open…'s Examples open it), the market world run in the tab's own DuckDB
// (market-sim/local-host.ts): its ten tabs, on each every grid's rows and every chart drawn and the tab ticking
// (desk-tabs.mjs: the Deephaven desk's own checks), the world's rows written and the live engine's emits counted. A small
// world -- 40 names, a tape of 5,000 prints, the crypto feed off, so the test asks nothing of the network -- keeps it a CI
// lane's minutes. A screenshot of each tab is the test's output.
//
//   bazel run //datacube:verify_live_desk

// first: points Playwright at the Chromium Bazel fetched (as a browser_test; a no-op under bazel run)
import '../../tools/browser/pinned-chromium.mjs';
import { dirname } from 'node:path';
import { chromium } from 'playwright';
import { checkDeskTabs, shownTiles } from './desk-tabs.mjs';
import { outPath, serve, siteRoot } from './harness.mjs';

const outputs = dirname(outPath('desk/opened.png'));
const site = await serve(siteRoot());
const DESK = `http://127.0.0.1:${site.port}/demo/index.html?market-names=40&market-tape=5000&market-feed=off#example=trading-desk`;

let failed = false;
const fail = (why) => { console.log(`FAIL ${why}`); failed = true; };
const ok = (what) => console.log(`ok   ${what}`);

let browser;
try {
  browser = await chromium.launch();
  const page = await (await browser.newContext({ viewport: { width: 1680, height: 1050 } })).newPage();
  page.on('pageerror', (e) => fail(`page error: ${e.message}`));
  page.on('console', (m) => { if (m.type() === 'error' || m.type() === 'warning') console.log(`[page ${m.type()}] ${m.text().slice(0, 300)}`); });
  // nothing leaves the loopback: the site is the page's whole world (the crypto feed is off)
  const away = [];
  page.on('request', (r) => { if (!r.url().startsWith('http://127.0.0.1:') && !r.url().startsWith('blob:') && !r.url().startsWith('data:')) away.push(r.url()); });
  const started = Date.now();
  await page.goto(DESK);

  await checkDeskTabs(page, { ok, fail, outputs, started });

  // THE MARKET IN THE TAB: its world wrote rows, its engine emitted (local-host.ts `__market`, for this harness only)
  const counts = await page.evaluate(() => window.__market?.counts());
  const rows = counts === undefined ? 0 : Object.values(counts.world.rows ?? {}).reduce((a, b) => a + Number(b), 0);
  if (rows > 0) ok(`the market in the tab: ${rows.toLocaleString('en-US')} rows written by its world, ${JSON.stringify(counts.live)} by its engine`);
  else fail(`the market in the tab wrote no rows: ${JSON.stringify(counts)}`);
  if (away.length === 0) ok('nothing asked of the network: the site and the tab answered everything');
  else fail(`the page reached past the site: ${away.slice(0, 5).join(', ')}`);

  // A TAB LEFT AND SHOWN AGAIN TICKS AGAIN (the user, 2026-10-29: "stops ticking if you change tabs and come back"):
  // Markets shown, another tab shown for 20 s, Markets shown again -- what it shows changes within 30 s, nothing touched
  const showTab = (name) => page.locator(`.dc-sheet-tab:has(.dc-sheet-label:text-is("${name}"))`).first().click();
  const ticksWithin = async (ms) => {
    const before = new Map((await shownTiles(page)).map((t) => [t.id, t.shows]));
    const end = Date.now() + ms;
    while (Date.now() < end) {
      await new Promise((r) => setTimeout(r, 1000));
      const changed = (await shownTiles(page)).filter((t) => before.has(t.id) && before.get(t.id) !== t.shows).map((t) => t.id);
      if (changed.length > 0) return changed;
    }
    return [];
  };
  for (const [away, gone] of [['Blotter', 20_000], ['Crypto', 60_000]]) {
    await showTab('Markets');
    const first = await ticksWithin(30_000);
    await showTab(away);
    await new Promise((r) => setTimeout(r, gone));
    await showTab('Markets');
    const back = await ticksWithin(30_000);
    if (first.length > 0 && back.length > 0) ok(`Markets left for ${away} ${gone / 1000} s and shown again: ticking again (${back.join(', ')})`);
    else fail(`Markets left for ${away} ${gone / 1000} s: ${first.length > 0 ? 'ticking before' : 'not ticking before'}, ${back.length > 0 ? 'ticking after' : 'NOT ticking after'} (the market: ${JSON.stringify(await page.evaluate(() => window.__market?.counts()?.live))})`);
  }

  // OPEN…'S EXAMPLES: the desk, live in this tab, first (as the Deephaven desk's test opens the window)
  await page.click('.dc-titlebar-menu');
  const openItem = page.locator('.dc-menu-item:has(> .dc-menu-label:text-is("Open…"))').first();
  if (!(await openItem.isVisible())) await page.locator('.dc-menu-item:has(> .dc-menu-label:text-is("File"))').first().hover();
  await openItem.click();
  await page.locator('.dc-open-example').first().waitFor({ timeout: 20_000 });
  const examples = await page.evaluate(() => [...document.querySelectorAll('.dc-open-example')]
    .map((e) => ({ id: e.getAttribute('data-example'), text: e.textContent ?? '' })));
  if (examples[0]?.id === 'trading-desk' && examples[0].text.includes('live in this tab')) {
    ok(`Open…'s Examples: the desk in this tab first, then ${examples.slice(1).map((e) => e.id).join(', ')}`);
  } else fail(`Open…'s Examples are ${JSON.stringify(examples.map((e) => e.id))}: the desk in this tab first`);
  await page.keyboard.press('Escape');
} catch (e) {
  fail(String(e?.stack ?? e));
} finally {
  await browser?.close();
  await site.close();
}
console.log(failed ? '\nFAIL the trading desk in the tab' : '\nok   the trading desk in the tab');
process.exit(failed ? 1 : 0);
