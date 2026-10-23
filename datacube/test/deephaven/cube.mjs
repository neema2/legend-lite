// DATACUBE ON DEEPHAVEN, run in the pinned Chromium (//datacube:deephaven_cube_test; docs/DEEPHAVEN_PLAN_2026_10_22.md
// DH23). Node only drives: it starts the pinned Deephaven with the demo's application (risk.groovy), serves the cube page
// (demo/deephaven/cube.ts: DataCube's own app, planning in the tab on a Deephaven runtime and running on the server),
// and checks what DataCube shows and what the tab sent: the tree's positions are the seeding's (worked out here from
// the same formulas); a pivot, a regroup and a filter -- the app's own changes, as its zones and filter editor make
// them -- each send only their own operations, the source's nine (its five tables and four joins) read from what the
// session holds; a pivot's columns keep their names; no table is made twice; the grid ticks, re-run with nothing sent;
// and when Deephaven restarts the page says so and comes back by itself. Screenshots of each stage are its outputs.

// first: points Playwright at the Chromium Bazel fetched
import '../../../tools/browser/pinned-chromium.mjs';
import { join } from 'node:path';
import { chromium } from 'playwright';
import { serveDemo, startDeephaven } from '../../demo/deephaven/site.mjs';

const outputs = process.env.TEST_UNDECLARED_OUTPUTS_DIR ?? process.env.TEST_TMPDIR ?? '.';

// the seeding's own positions (demo/deephaven/risk.groovy): position i is book (i / 40) % 8 and instrument i % 40; book
// b's desk is b % 4 and its region EMEA below 4, instrument n's sector n % 4 and its currency n % 3
const DESKS = ['Rates', 'FX', 'Credit', 'Equities'];
const SECTORS = ['Tech', 'Energy', 'Banks', 'Retail'];
const CCYS = ['USD', 'EUR', 'JPY'];
const positions = (key, only = () => true) => {
  const out = new Map();
  for (let i = 0; i < 400; i++) {
    const book = Math.floor(i / 40) % 8;
    const inst = i % 40;
    const p = { desk: DESKS[book % 4], region: book < 4 ? 'EMEA' : 'AMER', sector: SECTORS[inst % 4], ccy: CCYS[inst % 3] };
    if (!only(p)) continue;
    out.set(key(p), (out.get(key(p)) ?? 0) + 1);
  }
  return out;
};
/** The source's operations every query over it shares: its five tables' views and its four joins. */
const SOURCE_OPS = 9;

const server = startDeephaven({ quiet: true });
const config = await server.next('on starting');
const site = await serveDemo(config);

let failed = false;
const fail = (why) => { console.log(`FAIL ${why}`); failed = true; };
const ok = (what) => console.log(`ok   ${what}`);
const check = (wrong, what) => (wrong.length > 0 ? fail(`${what}:\n  ${wrong.join('\n  ')}`) : ok(what));
let browser;
try {
  browser = await chromium.launch();
  const page = await (await browser.newContext({ ignoreHTTPSErrors: true, viewport: { width: 1280, height: 860 } })).newPage();
  page.on('pageerror', (e) => fail(`page error: ${e.message}`));
  page.on('console', (m) => { if (m.type() === 'error') console.log(`[page] ${m.text()}`); });
  await page.goto(`${site.url}/cube.html`);
  const state = () => page.evaluate(() => ({ ...window.__cube, change: undefined }));
  const until = async (test, what, ms = 120_000) => {
    const end = Date.now() + ms;
    while (Date.now() < end) {
      const s = await state();
      if (s.status === 'failed') throw new Error(`the page failed: ${s.error}`);
      if (test(s)) return s;
      await new Promise((r) => setTimeout(r, 200));
    }
    throw new Error(`not ${what} in ${ms / 1000} s: ${JSON.stringify(await state()).slice(0, 600)}`);
  };
  /** The view's level-one rows: each one's label and the values of `columns`. */
  const level = (s, columns) => new Map(s.rows.map((r) => [r[s.columns.indexOf('__tree')],
    columns.map((c) => r[s.columns.indexOf(c)])]));
  /** Every row's positions against the seeding's, by label. */
  const against = (s, column, expected) => {
    const wrong = [];
    const rows = level(s, [column]);
    if (rows.size !== expected.size) wrong.push(`${rows.size} rows, not ${expected.size}`);
    for (const [label, n] of expected) if (rows.get(label)?.[0] !== n) wrong.push(`${label}: ${rows.get(label)?.[0]} positions, not ${n}`);
    return wrong;
  };
  /** A change as the app makes it, with ticks paused: the view it shows, and what its queries planned and sent. */
  const change = async (what) => {
    const before = await state();
    const kind = await page.evaluate((w) => window.__cube.change(w), what);
    const after = await state();
    if (kind !== 'applied') throw new Error(`the ${what} was ${kind}: ${after.error}`);
    const queries = after.counts.queries - before.counts.queries;
    const planned = after.counts.planned - before.counts.planned;
    const sent = after.counts.sent - before.counts.sent;
    return { after, queries, planned, sent };
  };
  /** Each query of a change read the source's operations from what the session held. */
  const reused = (c, what) => (c.queries > 0 && c.sent <= c.planned - SOURCE_OPS * c.queries
    ? ok(`${what}: ${c.queries} queries planned ${c.planned} operations and sent ${c.sent}, the source's read from what the session held`)
    : fail(`${what}: ${c.queries} queries planned ${c.planned} operations and sent ${c.sent}`));

  const first = await until((s) => s.status === 'live' && s.views >= 1, 'live');
  await page.evaluate(() => { window.__cube.paused = true; });
  check(against(first, 'pid', positions((p) => p.desk)), 'the tree by desk: each desk\'s positions are the seeding\'s');
  const receipt = await page.evaluate(() => document.querySelector('.dc-app-stats')?.textContent ?? '');
  if (receipt.includes('the Deephaven server at 127.0.0.1')) ok('the status bar\'s receipt names the Deephaven server');
  else fail(`the status bar says ${JSON.stringify(receipt)}`);
  await page.screenshot({ path: join(outputs, '1-tree.png') });

  // a pivot on region: two queries (its values, then the level), each over the held source
  const pivot = await change('pivot');
  reused(pivot, 'pivoted by region');
  const pivotWrong = [];
  for (const region of ['AMER', 'EMEA']) {
    const column = `${region}__|__pid`;
    if (!pivot.after.columns.includes(column)) pivotWrong.push(`no column ${column}: ${pivot.after.columns.join(', ')}`);
    else pivotWrong.push(...against(pivot.after, column, positions((p) => p.desk, (p) => p.region === region)));
  }
  const placed = pivot.after.columns.filter((c) => /^__c\d+$/.test(c));
  if (placed.length > 0) pivotWrong.push(`columns named by their place: ${placed.join(', ')}`);
  check(pivotWrong, 'the pivot\'s columns keep their names, each region\'s positions the seeding\'s');
  await page.screenshot({ path: join(outputs, '2-pivot.png') });

  const regroup = await change('regroup');
  reused(regroup, 'regrouped by region and currency');
  check(against(regroup.after, 'pid', positions((p) => p.region)), 'the tree by region: the seeding\'s positions');
  await page.screenshot({ path: join(outputs, '3-regroup.png') });

  const filter = await change('filter');
  reused(filter, 'filtered to the FX desk');
  check(against(filter.after, 'pid', positions((p) => p.region, (p) => p.desk === 'FX')), 'the FX desk by region: the seeding\'s positions');
  await page.screenshot({ path: join(outputs, '4-filter.png') });

  // ticking: a result on screen changes on the server, and the cube runs again, sending nothing
  const t0 = await state();
  await page.evaluate(() => { window.__cube.paused = false; });
  await new Promise((r) => setTimeout(r, 5000));
  const t1 = await state();
  const refreshes = t1.refreshes - t0.refreshes;
  const mv = (s) => JSON.stringify(s.rows.map((r) => r[s.columns.indexOf('mv')]));
  if (refreshes < 5) fail(`${refreshes} re-runs in 5 s (${t1.onScreen - t0.onScreen} ticks on screen)`);
  else if (t1.counts.sent !== t0.counts.sent) fail(`re-running sent ${t1.counts.sent - t0.counts.sent} operations`);
  else if (mv(t1) === mv(t0)) fail('the market values did not change');
  else ok(`it ticks: ${refreshes} re-runs in 5 s, nothing sent, the market values moving`);
  await page.screenshot({ path: join(outputs, '5-ticking.png') });
  if (t1.counts.remade === 0) ok(`no table made twice: ${t1.counts.queries} queries planned ${t1.counts.planned} operations, ${t1.counts.sent} sent`);
  else fail(`${t1.counts.remade} tables made twice`);

  // a restart: Deephaven keeps no session across one; its start-up script seeds again, and the cube comes back by itself
  server.restart();
  const down = await until((s) => s.status === 'reconnecting', 'reconnecting after the restart', 60_000);
  ok(`the page saw the server go (${down.error.slice(0, 80)})`);
  await page.screenshot({ path: join(outputs, '6-reconnecting.png') });
  const restarted = await server.next('on restarting');
  if (!restarted.restarted) fail(`the fixture answered ${JSON.stringify(restarted)}`);
  const back = await until((s) => s.status === 'live' && s.reconnects >= 1 && s.refreshes >= t1.refreshes + 3,
    'live again and ticking', 120_000);
  check(against(back, 'pid', positions((p) => p.region, (p) => p.desk === 'FX')),
    `back after the restart (${back.counts.reconnects} sign-ins), the same view, ticking`);
  await page.screenshot({ path: join(outputs, '7-back.png') });
} catch (e) {
  fail(String(e?.stack ?? e));
} finally {
  await browser?.close();
  site.close();
  await server.stop();
}
process.exit(failed ? 1 : 0);
