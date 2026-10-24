// JOIN… as a person does it (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §17.5, the browser): two CSVs opened, the first
// grid's Join… -- the key suggested, what it will do said before it is applied (the trades with no customer, the one
// customer listed twice) -- a new grid of the rows they make together; then the trades with no customer, a second join;
// both saved, reloaded and opened again, each file asked for once; the start page saved and joined; the join example opened.
// The model of both tables, the join and each count are the tab's own module's, run on its DuckDB.
//
//   bazel test //datacube:verify_join_test        bazel run //datacube:verify_join (by hand)

// first: points Playwright at the Chromium Bazel fetched (as a browser_test; a no-op under bazel run)
import '../../tools/browser/pinned-chromium.mjs';
import { writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { chromium } from 'playwright';
import { serve, siteRoot, tmpDir } from './harness.mjs';

let failed = false;
function bad(m) { console.log(`FAIL: ${m}`); failed = true; }
function check(ok, m) { if (ok) console.log(`ok: ${m}`); else bad(m); }

// the two sources, written here: their rows are known. Trades 1, 8, 15, 22, 29, 36 have no customer; customer 5 is
// listed twice, so its 7 trades (5, 10, 20, 25, 30, 35, 40) come out twice
const dir = await tmpDir('dc-join-');
const trades = join(dir, 'trades.csv');
const customers = join(dir, 'customers.csv');
await writeFile(trades, ['trade_id,customer_id,region,notional',
  ...Array.from({ length: 40 }, (_, i) => `${i + 1},${i % 7 === 0 ? '' : (i % 5) + 1},${['EMEA', 'AMER', 'APAC'][i % 3]},${(i + 1) * 100}.25`)].join('\n') + '\n');
await writeFile(customers, ['customer_id,name',
  ...Array.from({ length: 5 }, (_, i) => `${i + 1},Customer ${i + 1}`), '5,Customer 5 again'].join('\n') + '\n');
// the trades file changed since: its customer_id gone (a join on it no longer joins)
const changedDir = await tmpDir('dc-join-changed-');
const changedTrades = join(changedDir, 'trades.csv');
await writeFile(changedTrades, ['trade_id,region,notional',
  ...Array.from({ length: 40 }, (_, i) => `${i + 1},${['EMEA', 'AMER', 'APAC'][i % 3]},${(i + 1) * 100}.25`)].join('\n') + '\n');

const { port, close } = await serve(siteRoot());
const browser = await chromium.launch();
const page = await browser.newPage({ viewport: { width: 1400, height: 900 } });
const errors = [];
const alerts = [];
page.on('pageerror', (e) => errors.push(`pageerror: ${e.message}`));
/** The next "unsaved changes: open anyway?" answered yes (the start page, below), every other one no. */
let openAnyway = false;
page.on('dialog', async (d) => {
  // a page about to be left asks first (beforeunload) while it holds work -- the files picked in this tab, here: left
  if (d.type() === 'beforeunload') { await d.accept(); return; }
  if (d.type() === 'confirm' && openAnyway) { openAnyway = false; await d.accept(); return; }
  alerts.push(d.message());
  await d.dismiss();
});

const item = (label) => page.locator('.dc-menu .dc-menu-item', { has: page.locator(`:scope > .dc-menu-label:text-is(${JSON.stringify(label)})`) }).first();
/** The grids on the page: each tile's grid (their menu buttons, which these counted, went 2026-10-23). */
const GRIDS = '[data-tile] .dc-app-grid';
/** `tile`'s right-click on its first cell (or one of `column`), then `parent` ▸ `label`: Join ▸ Join…, Tile ▸ Remove from Page. */
async function gridEntry(tile, parent, label, column) {
  await tile.locator(column ? `.dc-row .dc-cell[data-column="${column}"]` : '.dc-row .dc-cell').first().click({ button: 'right' });
  await item(parent).hover();
  await page.locator(`.dc-menu-item:has(> .dc-menu-label:text-is(${JSON.stringify(parent)})) `
    + `.dc-menu-item:has(> .dc-menu-label:text-is(${JSON.stringify(label)}))`).first().click();
}
/** A file opened through the source picker, as a person does: its grid's tile is the `n`th. */
async function openFile(path, n) {
  await page.locator('.dc-picker .dc-picker-drop').waitFor({ state: 'visible', timeout: 10_000 });
  await page.setInputFiles('.dc-picker-file', path);
  await page.waitForFunction(() => !document.querySelector('.dc-picker') || document.querySelector('.dc-picker-status.dc-failed'),
    undefined, { timeout: 120_000 });
  await page.waitForFunction((k) => document.querySelectorAll('[data-tile] .dc-app-grid').length >= k, n, { timeout: 60_000 });
}
const rowsOf = async (tile) => Number((/^([\d,]+) rows/.exec((await tile.locator('.dc-status-rows').first().textContent()) ?? '')?.[1]
  ?? 'NaN').replace(/,/g, ''));
const untilRows = (tile, n) => tile.locator('.dc-status-rows').filter({ hasText: new RegExp(`^${n} rows`) }).first()
  .waitFor({ timeout: 60_000 }).catch(() => undefined);
const tileWith = (column) => page.locator('[data-tile^="grid"]').filter({ has: page.locator(`.dc-th[data-column="${column}"]`) });
const columnsOf = (tile) => tile.locator('.dc-th[data-column]').evaluateAll((ths) => ths.map((t) => t.getAttribute('data-column')));
/** The trades' own grid: its 40 rows (a join of it has its columns too). */
const tradesTile = () => tileWith('trade_id').filter({ has: page.locator('.dc-status-rows', { hasText: /^40 rows/ }) }).first();
/**
 * The trades grid's Join ▸ Join… from a customer_id cell, with the customers' grid (by its name: a join of it, beside it,
 * has the column too), the window's answer to what it will do waited for.
 */
async function joinWindow() {
  await gridEntry(tradesTile(), 'Join', 'Join…', 'customer_id');
  await page.locator('.dc-join').waitFor({ timeout: 10_000 });
  const theirs = await page.$$eval('.dc-join-their option', (os) => os.map((o) => ({ value: o.value, label: o.textContent })));
  const customersGrid = theirs.find((o) => o.label === 'customers.csv');
  if (!customersGrid) throw new Error(`no customers grid offered: ${JSON.stringify(theirs)}`);
  await page.locator('.dc-join-their').selectOption(customersGrid.value);
  await page.waitForFunction(() => document.querySelectorAll('.dc-join-will-line').length > 0
    || document.querySelector('.dc-join .dc-picker-status.dc-failed'), undefined, { timeout: 60_000 });
}
const will = () => page.$$eval('.dc-join-will-line', (ls) => ls.map((l) => l.textContent));
/** A right-click on a cell of `column` in `tile`, as a person does it. */
async function rightClickIn(tile, column) {
  await tile.locator(`.dc-row .dc-cell[data-column="${column}"]`).first().click({ button: 'right' });
}
/** The entries of a right-click submenu, by its label, once it is pointed at. */
async function submenu(label) {
  const parent = page.locator('.dc-menu .dc-menu-item', { has: page.locator(`:scope > .dc-menu-label:text-is(${JSON.stringify(label)})`) }).first();
  await parent.hover();
  return parent.locator(':scope > .dc-submenu > .dc-menu-item > .dc-menu-label').allTextContents();
}
/** A saved page opened again after a reload: each file asked for answered (`given`), until it says it opened. */
async function openSaved(name, given = { 'trades.csv': trades, 'customers.csv': customers }) {
  await page.goto(`http://127.0.0.1:${port}/demo/index.html`, { waitUntil: 'load', timeout: 120_000 });
  await page.waitForFunction(() => document.querySelectorAll('.dc-row').length > 0, undefined, { timeout: 120_000 });
  await page.click('.dc-titlebar-menu');
  await item('Open…').click();
  const row = page.locator('#cubelib .dc-lib-row', { hasText: name });
  await row.waitFor({ timeout: 10_000 });
  await row.locator('.dc-lib-button', { hasText: 'Open' }).click();
  const asked = [];
  for (;;) {
    const state = await page.waitForFunction(() => (document.querySelector('#cubelib .dc-lib-ask:not([hidden])') ? 'ask'
      : /opened|not opened/.test(document.querySelector('#cubelib .dc-lib-message')?.textContent ?? '') ? 'done' : false),
    undefined, { timeout: 60_000 }).then((h) => h.jsonValue());
    if (state === 'done') break;
    const ask = await page.locator('#cubelib .dc-lib-ask').textContent() ?? '';
    const which = /customers\.csv/.test(ask) ? 'customers.csv' : 'trades.csv';
    asked.push(which);
    await page.setInputFiles('#cubelib .dc-lib-choose', given[which]);
    await page.waitForFunction((n) => !document.querySelector('#cubelib .dc-lib-ask:not([hidden])')
      || !document.querySelector('#cubelib .dc-lib-ask')?.textContent?.includes(n), which, { timeout: 30_000 });
  }
  return { asked, said: await page.locator('#cubelib .dc-lib-message').textContent() };
}

try {
  await page.goto(`http://127.0.0.1:${port}/demo/index.html`, { waitUntil: 'load', timeout: 120_000 });
  await page.waitForFunction(() => document.querySelectorAll('.dc-row').length > 0, undefined, { timeout: 120_000 });
  // a blank page, then the two sources
  await page.click('.dc-titlebar-menu');
  await item('Create').hover();
  await item('Blank Page').click();
  await page.locator('.dc-blank').waitFor({ timeout: 10_000 });
  await page.click('.dc-blank .dc-primary');
  await openFile(trades, 1);
  await page.click('.dc-titlebar-menu');
  await item('Create').hover();
  await item('Data Source…').click();
  await openFile(customers, 2);

  // JOIN ▸ ON THE RIGHT-CLICK (the user, 2026-10-23): a quick join to the grid sharing the column clicked -- but customer 5
  // is listed twice, so all of mine would repeat its trades: the window opens on it instead, its warning said, nothing made
  await rightClickIn(tileWith('trade_id').first(), 'customer_id');
  const joinItems = await submenu('Join');
  check(joinItems[0] === 'With customers.csv on customer_id' && joinItems.includes('Join\u2026'),
    `Join ▸ offers the grid sharing the column clicked, first: ${joinItems.join(' | ')}`);
  await page.locator('.dc-menu .dc-menu-item', { has: page.locator(':scope > .dc-menu-label:text-is("With customers.csv on customer_id")') }).first().click();
  await page.locator('.dc-join').waitFor({ timeout: 60_000 }).then(async () => {
    await page.waitForFunction(() => /more than once/.test(document.querySelector('.dc-join-will-list')?.textContent ?? ''), undefined, { timeout: 60_000 })
      .catch(() => undefined);
    const key = await page.$$eval('.dc-join-rows .dc-join-row select', (ss) => ss.map((x) => x.value));
    const form = await page.locator('.dc-join-kind[aria-checked="true"]').getAttribute('data-kind');
    check(JSON.stringify(key) === JSON.stringify(['customer_id', '==', 'customer_id']) && form === 'left'
      && /appears more than once/.test((await will()).join(' ')) && await page.locator(GRIDS).count() === 2,
    `a quick join that would repeat rows of mine: the window on it, its key and form, its warning, nothing made (${key}, ${form})`);
    await page.locator('.dc-join .dc-picker-close').click();
    await page.locator('.dc-join').waitFor({ state: 'detached', timeout: 10_000 });
  }, () => bad(`the repeating quick join opened no window: ${alerts.splice(0).join('; ')}`));

  // THE JOIN: the key suggested and taken, what it will do said before it is applied
  await joinWindow();
  // the nearest row's columns asked of as of alone: never shown under another form (they were, 2026-10-23)
  check(!(await page.locator('.dc-join-match-block').isVisible()), 'no "nearest by" row under All of mine');
  const row = await page.$$eval('.dc-join-rows .dc-join-row select', (ss) => ss.map((s) => s.value));
  check(JSON.stringify(row) === JSON.stringify(['customer_id', '==', 'customer_id']), `the key suggested, taken: ${row}`);
  const said = await will();
  check(JSON.stringify(said) === JSON.stringify([
    '34 rows of my 40 rows find a match, 6 rows do not.',
    '47 rows after it: more than mine -- a row of mine repeats once for each of theirs it matches.',
    '1 key of theirs appears more than once (at most 2 times): the rows of mine with it repeat, and totals count them again.',
  ]), `what it will do, before it is applied:\n  ${said.join('\n  ')}`);
  check(/customer_id → their_customer_id/.test(await page.locator('.dc-join-column-list').textContent() ?? ''),
    'their customer_id, a name mine has, said as it comes out');
  await page.locator('.dc-join .dc-primary').click();
  await page.locator('.dc-join').waitFor({ state: 'detached', timeout: 60_000 });
  await page.waitForFunction(() => document.querySelectorAll('[data-tile] .dc-app-grid').length >= 3, undefined, { timeout: 60_000 });
  const joined = tileWith('their_customer_id').first();
  await untilRows(joined, 47);
  check(await rowsOf(joined) === 47, `the joined grid: every trade, customer 5's twice (${await rowsOf(joined)} rows)`);
  check(JSON.stringify(await columnsOf(joined)) === JSON.stringify(['trade_id', 'customer_id', 'region', 'notional', 'their_customer_id', 'name']),
    `its columns: mine, then theirs, the one mine has prefixed (${await columnsOf(joined)})`);
  // where its rows are (§18.3): two files read into this tab, so a copy held here from the start, as a file's grid is --
  // never Live, and no Snap of a copy offered
  const pill = joined.locator('.dc-titlebar-toggle').first();
  const plane = { text: await pill.textContent(), title: await pill.getAttribute('title') ?? '' };
  check(plane.text === 'Snapped' && /^trades\.csv, customers\.csv — copied into this tab at /.test(plane.title)
    && await pill.getAttribute('aria-disabled') === 'true', `the join's plane: its files' copies, held here (${JSON.stringify(plane)})`);

  // THE TRADES WITH NO CUSTOMER: keep my rows that have none, their columns not brought
  await joinWindow();
  await page.locator('.dc-join-kind[data-kind="anti"]').click();
  await page.waitForFunction(() => /each once/.test(document.querySelector('.dc-join-will-list')?.textContent ?? ''), undefined, { timeout: 30_000 });
  check(JSON.stringify(await will()) === JSON.stringify(['34 rows of my 40 rows have a match, 6 rows none.', 'It keeps 6 rows, each once.']),
    `what keeping my rows with no match will do: ${await will()}`);
  await page.locator('.dc-join .dc-primary').click();
  await page.locator('.dc-join').waitFor({ state: 'detached', timeout: 60_000 });
  await page.waitForFunction(() => document.querySelectorAll('[data-tile] .dc-app-grid').length >= 4, undefined, { timeout: 60_000 });
  // its tile wherever the page placed it: the one grid of trades' columns and six rows (the customers' grid has six too)
  const six = () => tileWith('notional').filter({ has: page.locator('.dc-status-rows', { hasText: /^6 rows/ }) });
  await six().first().waitFor({ timeout: 60_000 }).catch(() => undefined);
  check(await six().count() === 1, `the trades with no customer: a grid of 6 rows (${await six().count()})`);

  // A COMPILER'S REFUSAL, SAID IN THE WINDOW: a condition in Pure naming a column neither has
  await joinWindow();
  await page.locator('.dc-join-pure-on').check();
  await page.fill('.dc-join-pure', '{a, b | $a.nope == $b.name}');
  await page.waitForFunction(() => /nope/.test(document.querySelector('.dc-join .dc-picker-status.dc-failed')?.textContent ?? ''),
    undefined, { timeout: 30_000 }).then(() => check(true, 'a column neither has: the compiler\'s refusal, in the window'),
    async () => bad(`the refusal of a column neither has: ${await page.locator('.dc-join .dc-picker-status').textContent()}`));
  check(await page.locator('.dc-join .dc-primary').isDisabled(), 'no join while the compiler refuses it');
  await page.locator('.dc-join .dc-picker-close').click();
  await page.locator('.dc-join').waitFor({ state: 'detached', timeout: 10_000 });

  // SAVED, RELOADED AND OPENED AGAIN: each file asked for once; the joins read what their neighbours read in
  await page.click('.dc-titlebar-menu');
  await item('Save As…').click();
  await page.locator('.dc-save').waitFor({ timeout: 10_000 });
  await page.fill('.dc-save-input', 'Joins');
  await page.locator('.dc-save-foot .dc-primary').click();
  await page.locator('.dc-save').waitFor({ state: 'detached', timeout: 30_000 });
  const { asked, said: opened } = await openSaved('Joins');
  check(JSON.stringify([...asked].sort()) === JSON.stringify(['customers.csv', 'trades.csv']), `each file asked for once: ${asked}`);
  check(/opened/.test(opened ?? '') && !/not opened/.test(opened ?? ''), `opened: ${opened}`);
  await page.waitForFunction(() => document.querySelectorAll('[data-tile] .dc-app-grid').length >= 4, undefined, { timeout: 60_000 });
  const again = tileWith('their_customer_id').first();
  await untilRows(again, 47);
  check(await rowsOf(again) === 47, `reopened: the joined grid's rows (${await rowsOf(again)})`);
  await six().first().waitFor({ timeout: 60_000 }).catch(() => undefined);
  check(await six().count() === 1, `reopened: the trades with no customer, 6 rows (${await six().count()})`);

  // A FILE THAT CHANGED UNDER ITS JOINS (the audit's 1 and 3): the page opens, its plain grids with them; each join that
  // no longer joins is left out and said, the file given said not to be the one joined
  const drifted = await openSaved('Joins', { 'trades.csv': changedTrades, 'customers.csv': customers });
  check(/opened/.test(drifted.said ?? '') && !/not opened/.test(drifted.said ?? ''), `the page opened over a changed file: ${drifted.said}`);
  check((drifted.said?.match(/left out: trades\.csv/g) ?? []).length === 2, `each join left out, said: ${drifted.said}`);
  check(/is not the file/.test(drifted.said ?? ''), `the file given said not to be the one saved over: ${drifted.said}`);
  await page.waitForFunction(() => document.querySelectorAll('[data-tile] .dc-app-grid').length === 2, undefined, { timeout: 60_000 })
    .then(() => check(true, 'its two plain grids open, its joins not'), async () => bad(`grids open: ${await page.locator(GRIDS).count()}`));

  // JOINS ALONE READING THE FILES (the audit's 5): the plain grids taken off, saved, opened again -- each file asked for
  // once, both joins reading the one table each file is read into
  await openSaved('Joins');
  await page.waitForFunction(() => document.querySelectorAll('[data-tile] .dc-app-grid').length >= 4, undefined, { timeout: 60_000 });
  const removeGrid = (tile) => gridEntry(tile, 'Tile', 'Remove from Page');
  await removeGrid(tileWith('name').filter({ hasNot: page.locator('.dc-th[data-column="trade_id"]') }).first());
  await removeGrid(tileWith('notional').filter({ has: page.locator('.dc-status-rows', { hasText: /^40 rows/ }) }).first());
  await page.waitForFunction(() => document.querySelectorAll('[data-tile] .dc-app-grid').length === 2, undefined, { timeout: 30_000 });
  await page.click('.dc-titlebar-menu');
  await item('Save As\u2026').click();
  await page.locator('.dc-save').waitFor({ timeout: 10_000 });
  await page.fill('.dc-save-input', 'Joins only');
  await page.locator('.dc-save-foot .dc-primary').click();
  await page.locator('.dc-save').waitFor({ state: 'detached', timeout: 30_000 });
  const only = await openSaved('Joins only');
  check(JSON.stringify([...only.asked].sort()) === JSON.stringify(['customers.csv', 'trades.csv']), `joins alone: each file asked for once: ${only.asked}`);
  const joinedOnly = tileWith('their_customer_id').first();
  await untilRows(joinedOnly, 47);
  check(await rowsOf(joinedOnly) === 47, `joins alone: the joined grid's rows (${await rowsOf(joinedOnly)})`);
  await six().first().waitFor({ timeout: 60_000 }).catch(() => undefined);
  check(await six().count() === 1, `joins alone: the trades with no customer, 6 rows (${await six().count()})`);
  // what a grid reads is its leaves (design §18.3): a page of only joins holds the files they read, so leaving asks first
  const asks = await page.evaluate(() => {
    const leave = new Event('beforeunload', { cancelable: true });
    window.dispatchEvent(leave);
    return leave.defaultPrevented;
  });
  check(asks, 'joins alone: leaving asks first -- the files the joins read are held in this tab');

  // THE START PAGE (the user, 2026-10-23): its grid is written down as the sample file of its rows (src/sample-trades.ts),
  // so the page it starts saves and reopens, and a file beside it joins -- it said "a grid on it does not know its
  // source", and Join… "this grid reads no table of this tab's database"
  await page.goto(`http://127.0.0.1:${port}/demo/index.html`, { waitUntil: 'load', timeout: 120_000 });
  await page.waitForFunction(() => document.querySelectorAll('.dc-row').length > 0, undefined, { timeout: 120_000 });
  // the page the app starts with: no step to undo (it offered "Undo Add Grid", which left the page blank)
  await page.click('.dc-titlebar-menu');
  const undo = page.locator('.dc-menu .dc-menu-item', { has: page.locator(':scope > .dc-menu-label', { hasText: /^Undo/ }) }).first();
  check(await undo.locator(':scope > .dc-menu-label').textContent() === 'Undo' && await undo.getAttribute('aria-disabled') === 'true',
    `a fresh page has nothing to undo: ${await undo.locator(':scope > .dc-menu-label').textContent()}`);
  await page.keyboard.press('Escape');
  await page.click('.dc-titlebar-menu');
  await item('Save').click();
  await page.locator('.dc-save').waitFor({ timeout: 10_000 });
  await page.fill('.dc-save-input', 'Start');
  await page.locator('.dc-save-foot .dc-primary').click();
  await page.locator('.dc-save').waitFor({ state: 'detached', timeout: 30_000 }).then(() => check(true, 'the start page saves'),
    async () => bad(`the start page saves: ${await page.locator('.dc-save-warning').textContent()}`));
  await page.click('.dc-titlebar-menu');
  await item('Create').hover();
  await item('Data Source…').click();
  await openFile(customers, 2);
  // the start's grid is pivoted: its Join ▸ is the window alone
  await gridEntry(page.locator('[data-tile="grid-1"]'), 'Join', 'Join…');
  await page.locator('.dc-join').waitFor({ timeout: 10_000 }).then(async () => {
    const offered = await page.$$eval('.dc-join-their option', (os) => os.map((o) => o.textContent));
    check(offered.includes('customers.csv'), `the start's grid joins the file beside it: ${offered}`);
    await page.locator('.dc-join .dc-picker-close').click();
  }, () => bad(`the start's grid's Join…: ${alerts.splice(0).join('; ') || 'no window'}`));
  // reopened (the file added since left: "open anyway?" answered yes), its sample rebuilt, nothing asked for
  openAnyway = true;
  const start = await openSaved('Start');
  check(start.asked.length === 0 && /opened/.test(start.said ?? '') && !/not opened/.test(start.said ?? ''),
    `the saved start page reopens, its sample rebuilt, nothing asked: ${start.said} ${start.asked}`);
  await page.waitForFunction(() => document.querySelectorAll('[data-tile] .dc-app-grid').length === 1, undefined, { timeout: 60_000 })
    .then(() => check(true, 'the start page reopened: its one grid'), async () => bad(`grids open: ${await page.locator(GRIDS).count()}`));

  // THE JOIN EXAMPLE (the user, 2026-10-23: join prep was on the site, but nothing showed it): opened from Open…'s
  // examples, its two samples made in this tab and its two joins made again -- every order beside its country, by
  // continent, charted; the orders whose country the table lacks (Singapore's) -- nothing asked for
  await page.click('.dc-titlebar-menu');
  await item('Open…').click();
  await page.locator('#cubeswin .dc-open-example[data-example="prep-joins"]').click();
  await page.waitForFunction(() => (window.__dataPage?.views().views ?? []).some((v) => v.id === 'joined'), undefined, { timeout: 120_000 });
  /** What a tile's grid shows, its rows' text, once it shows `want`. */
  const shown = async (id, want) => {
    await page.waitForFunction(([t, w]) => [...document.querySelectorAll(`[data-tile="${t}"] .dc-row`)].some((r) => (r.textContent ?? '').includes(w)),
      [id, want], { timeout: 120_000 }).catch(() => undefined);
    return page.locator(`[data-tile="${id}"] .dc-row`).evaluateAll((rows) => rows.map((r) => r.textContent ?? '').join(' | '));
  };
  const countries = await shown('countries', 'Mexico');
  check(/Mexico/.test(countries) && /South Africa/.test(countries) && !/Singapore/.test(countries),
    `the countries sample, made in the tab: Mexico and South Africa, no Singapore (${countries.slice(0, 120)}…)`);
  await page.locator('.dc-sheet-tab[data-sheet="sheet-2"]').click();
  const joinedRows = await shown('joined', 'Europe');
  check(['Europe', 'North America', 'South America', 'Asia', 'Oceania'].every((c) => joinedRows.includes(c)) && !joinedRows.includes('Africa'),
    `every order by its country's continent -- none in Africa, where it sells nothing (${joinedRows.slice(0, 160)}…)`);
  await page.locator('[data-tile="by-continent"] .dc-chartpanel-chart canvas').waitFor({ timeout: 60_000 })
    .then(() => check(true, 'the chart of the join draws'), async () => bad(`the chart of the join: ${await page.locator('[data-tile="by-continent"]').textContent()}`));
  await page.locator('.dc-sheet-tab[data-sheet="sheet-3"]').click();
  const unmatchedRows = await shown('unmatched', 'Singapore');
  check(/Singapore/.test(unmatchedRows) && !/Germany|Japan|United States/.test(unmatchedRows),
    `the orders with no country: Singapore's alone (${unmatchedRows.slice(0, 120)})`);

  // A QUICK JOIN THAT REPEATS NOTHING: Orders' country, a column Countries has once a row -- made at once, no window,
  // what it did said on its grid; and Create ▸ on the same right-click (the user, 2026-10-23: Insert became Create)
  await page.locator('.dc-sheet-tab[data-sheet="sheet-1"]').click();
  await page.locator('[data-tile="orders"] .dc-row').first().waitFor({ timeout: 60_000 });
  await rightClickIn(page.locator('[data-tile="orders"]'), 'country');
  // the grid's own three first -- a data source beside it among them -- then the model's, where the page has a model home
  const create = await submenu('Create');
  check(JSON.stringify(create.slice(0, 3)) === JSON.stringify(['Data Source\u2026', 'Copy of This Grid', 'Visualization']),
    `Create ▸ in place of Insert: ${create.join(' | ')}`);
  const exampleJoins = await submenu('Join');
  check(exampleJoins[0] === 'With Countries on country', `Join ▸ on the column clicked: ${exampleJoins.join(' | ')}`);
  const before = await page.locator(GRIDS).count();
  await page.locator('.dc-menu .dc-menu-item', { has: page.locator(':scope > .dc-menu-label:text-is("With Countries on country")') }).first().click();
  await page.locator('.dc-status-note-text').first().waitFor({ timeout: 120_000 }).then(async () => {
    const note = await page.locator('.dc-status-note-text').first().textContent();
    check(note === '5,840 rows of my 6,000 rows find a match, 160 rows do not.' && await page.locator('.dc-join').count() === 0
      && await page.locator(GRIDS).count() === before + 1,
    `a quick join made at once, what it did said on its grid: ${note}`);
  }, () => bad(`the quick join made no grid with a note: ${alerts.splice(0).join('; ')}`));

  check(errors.length === 0 && alerts.length === 0,
    `no page errors or alerts${errors.length + alerts.length ? `: ${[...errors, ...alerts].join('; ')}` : ''}`);
} catch (e) {
  bad(`${e instanceof Error ? e.message : String(e)}${errors.length ? `\n  page: ${errors.join('; ')}` : ''}${alerts.length ? `\n  alerts: ${alerts.join('; ')}` : ''}`);
} finally {
  await browser.close();
  await close();
}

console.log(failed ? 'verify-join: FAILED' : 'verify-join: ok');
process.exit(failed ? 1 : 0);
