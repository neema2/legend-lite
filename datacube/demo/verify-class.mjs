// CREATE A CLASS… as a person does it (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §15.7, the browser): a CSV opened,
// its grid's rows made a class -- a column a key only, one left out -- written to the page's project in the page's own
// SDLC; then a second CSV's grid made a second class of the same project, whose window shows the first one's store and
// model mapping as CHANGED: what the first class wrote was saved and is read back. The files are the route's
// (legend-lite's promoteRelationToModel in this tab's module), shown in the window as they will be written.
//
//   bazel test //datacube:verify_class_test        bazel run //datacube:verify_class (by hand)

// first: points Playwright at the Chromium Bazel fetched (as a browser_test; a no-op under bazel run)
import '../../tools/browser/pinned-chromium.mjs';
import { writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { chromium } from 'playwright';
import { serve, siteRoot, tmpDir } from './harness.mjs';

let failed = false;
function bad(m) { console.log(`FAIL: ${m}`); failed = true; }
function check(ok, m) { if (ok) console.log(`ok: ${m}`); else bad(m); }

// the two sources, written here: their rows are known
const dir = await tmpDir('dc-class-');
const trades = join(dir, 'trades.csv');
const customers = join(dir, 'customers.csv');
await writeFile(trades, ['trade_id,customer_id,region,notional',
  ...Array.from({ length: 40 }, (_, i) => `${i + 1},${i % 7 === 0 ? '' : (i % 5) + 1},${['EMEA', 'AMER', 'APAC'][i % 3]},${(i + 1) * 100}.25`)].join('\n') + '\n');
await writeFile(customers, ['customer_id,name',
  ...Array.from({ length: 5 }, (_, i) => `${i + 1},"Customer, ${i + 1}"`)].join('\n') + '\n');

const { port, close } = await serve(siteRoot());
const browser = await chromium.launch();
const page = await browser.newPage({ viewport: { width: 1400, height: 900 } });
const errors = [];
page.on('pageerror', (e) => errors.push(`pageerror: ${e.message}`));
page.on('dialog', async (d) => {
  // a page about to be left asks first (beforeunload) while it holds work -- the files picked in this tab, here: left
  if (d.type() === 'beforeunload') { await d.accept(); return; }
  alerts.push(d.message());
  await d.dismiss();
});
const alerts = [];


/** A condition of this harness's own (an alert recorded), waited on -- never a fixed sleep (G-11). */
async function until(condition, timeout) {
  const end = Date.now() + timeout;
  while (!condition() && Date.now() < end) await new Promise((r) => setImmediate(r));
}
const item = (label) => page.locator('.dc-menu .dc-menu-item', { has: page.locator(`:scope > .dc-menu-label:text-is(${JSON.stringify(label)})`) }).first();
/** A file opened through the source picker, as a person does: its grid's tile is the `n`th. */
async function openFile(path, n) {
  await page.locator('.dc-picker .dc-picker-drop').waitFor({ state: 'visible', timeout: 10_000 });
  await page.setInputFiles('.dc-picker-file', path);
  await page.waitForFunction(() => !document.querySelector('.dc-picker') || document.querySelector('.dc-picker-status.dc-failed'),
    undefined, { timeout: 120_000 });
  await page.waitForFunction((k) => document.querySelectorAll('.dc-tile-menu').length >= k, n, { timeout: 60_000 });
}
/** The grid's own Create a Class…, its window's files listed. */
async function createClassWindow(n) {
  await page.locator('.dc-tile-menu').nth(n - 1).click();
  await item('Create a Class…').click();
  await page.waitForFunction(() => document.querySelectorAll('.dc-class-file').length > 0
    || document.querySelector('.dc-class .dc-picker-status.dc-failed'), undefined, { timeout: 60_000 });
}
const files = () => page.$$eval('.dc-class-file-line', (ls) => ls.map((l) => l.textContent));
const pure = (path) => page.$$eval('.dc-class-file', (ls, p) => ls.find((l) => l.querySelector('.dc-class-path')?.textContent === p)
  ?.querySelector('.dc-class-pure')?.textContent ?? '', path);

try {
  await page.goto(`http://127.0.0.1:${port}/demo/index.html`, { waitUntil: 'load', timeout: 120_000 });
  await page.waitForFunction(() => document.querySelectorAll('.dc-row').length > 0, undefined, { timeout: 120_000 });
  // a blank page, then the first source
  await page.click('.dc-titlebar-menu');
  await item('New').hover();
  await item('Blank Page').click();
  await page.locator('.dc-blank').waitFor({ timeout: 10_000 });
  await page.click('.dc-blank .dc-primary');
  await openFile(trades, 1);

  // THE FIRST CLASS: the names suggested, every file new
  await createClassWindow(1);
  const names = await page.$$eval('.dc-class-names input', (is) => is.map((i) => i.value));
  check(JSON.stringify(names) === JSON.stringify(['Trade', 'trades', 'trades']), `the class, package and project suggested (${names})`);
  check((await page.locator('.dc-class-table tbody tr').count()) === 4, 'a row per column');
  // customer_id a key only, region left out: the window asks the route again
  const rows = page.locator('.dc-class-table tbody tr');
  await rows.nth(1).locator('select').selectOption('key');
  await rows.nth(2).locator('select').selectOption('omit');
  await page.waitForFunction(() => [...document.querySelectorAll('.dc-class-pure')].some((p) => p.textContent.includes('+customerId: Integer[0..1]: customer_id'))
    && ![...document.querySelectorAll('.dc-class-pure')].some((p) => p.textContent.includes('region: region')), undefined, { timeout: 30_000 });
  const first = await files();
  // the sample named after the source's table in this tab: `trades`, or `trades_2` beside the demo's own
  check(JSON.stringify(first.slice(0, 7)) === JSON.stringify(['newtrades::store::Browser', 'newtrades::connection::Browser',
    'newtrades::runtime::Browser', 'newtrades::function::TradeFn', 'newtrades::domain::Trade',
    'newtrades::mapping::TradeMapping', 'newtrades::mapping::Model']) && first.length === 8
    && /^newtrades::data::Trades(_2)?Sample$/.test(first[7] ?? ''), `the files it writes, all new: ${first.join(', ')}`);
  const cls = await pure('trades::domain::Trade');
  check(cls.includes('tradeId: Integer[0..1];') && cls.includes('notional: Float[0..1];') && !cls.includes('customerId')
    && !cls.includes('region'), `the class: a key only and a column left out are not on it\n${cls}`);
  await page.locator('.dc-class .dc-primary').click();
  await page.waitForFunction(() => document.querySelector('.dc-class .dc-picker-status.dc-ok, .dc-class .dc-picker-status.dc-failed'),
    undefined, { timeout: 60_000 });
  const said = await page.locator('.dc-class .dc-picker-status').textContent();
  check(/Created trades::domain::Trade in the project datacube:trades \(workspace datacube\)/.test(said), `created: ${said}`);
  check(/#\/edit\/datacube%3Atrades\/datacube$/.test(await page.locator('.dc-class-studio').getAttribute('href') ?? ''),
    'the way to Studio: its editor on the workspace');
  await page.locator('.dc-class .dc-primary').click();
  await page.locator('.dc-class').waitFor({ state: 'detached', timeout: 10_000 });

  // THE SECOND CLASS, from a second grid: the page's project, read back -- its store and model mapping CHANGED
  await page.click('.dc-titlebar-menu');
  await item('New').hover();
  await item('Data Source…').click();
  await openFile(customers, 2);
  await createClassWindow(2);
  const subtitle = await page.locator('.dc-class .dc-picker-subtitle').textContent();
  check(subtitle.includes("in the page's project, datacube:trades"), `the page's project, not a new one: ${subtitle}`);
  check(await page.locator('.dc-class-package input').isDisabled(), "the page's package, kept");
  const second = await files();
  // the project's files changed, in the project's order (the SDLC's: by path), then the new ones, as made
  check(JSON.stringify(second) === JSON.stringify(['changedtrades::mapping::Model', 'changedtrades::store::Browser',
    'newtrades::function::CustomerFn', 'newtrades::domain::Customer', 'newtrades::mapping::CustomerMapping',
    'newtrades::data::CustomersSample']), `the first class's files read back, two changed: ${second.join(', ')}`);
  const store = await pure('trades::store::Browser');
  check(/Table trades\w*/.test(store) && /Table customers\w*/.test(store), `one store, a table per source\n${store}`);
  const sample = await pure('trades::data::CustomersSample');
  check(sample.includes('\'1,"Customer, 1"\\n\''), `the sample's CSV, a comma in a value quoted\n${sample}`);
  await page.locator('.dc-class .dc-primary').click();
  await page.waitForFunction(() => document.querySelector('.dc-class .dc-picker-status.dc-ok, .dc-class .dc-picker-status.dc-failed'),
    undefined, { timeout: 60_000 });
  check(/Created trades::domain::Customer/.test(await page.locator('.dc-class .dc-picker-status').textContent()), 'the second class created');
  await page.locator('.dc-class .dc-primary').click();
  await page.locator('.dc-class').waitFor({ state: 'detached', timeout: 10_000 });

  // CREATE AN ASSOCIATION…, from the page's Model menu: the suggested pair, how many read from the tab's own rows
  await page.click('.dc-titlebar-menu');
  await item('Model').hover();
  await item('Create an Association\u2026').click();
  await page.waitForFunction(() => document.querySelectorAll('.dc-association .dc-class-file').length > 0
    || document.querySelector('.dc-association .dc-picker-status.dc-failed'), undefined, { timeout: 60_000 });
  const many = await page.$$eval('.dc-association-many', (ms) => ms.map((m) => m.value));
  check(JSON.stringify(many) === JSON.stringify(['[0..1]', '[*]']),
    `each Trade has at most one Customer (Customer.customerId unique in its rows), each Customer many Trades: ${many}`);
  const related = await page.$$eval('.dc-association .dc-class-file-line', (ls) => ls.map((l) => l.textContent));
  check(JSON.stringify(related) === JSON.stringify(['changedtrades::mapping::Model', 'newtrades::domain::Customer_Trade']),
    `the association and its ModelJoin: ${related.join(', ')}`);
  await page.locator('.dc-association .dc-primary').click();
  await page.waitForFunction(() => document.querySelector('.dc-association .dc-picker-status.dc-ok, .dc-association .dc-picker-status.dc-failed'),
    undefined, { timeout: 60_000 });
  check(/Created trades::domain::Customer_Trade/.test(await page.locator('.dc-association .dc-picker-status').textContent()),
    'the association created');
  await page.locator('.dc-association .dc-primary').click();
  await page.locator('.dc-association').waitFor({ state: 'detached', timeout: 10_000 });

  // CREATE A DATA SPACE…: the page's two classes, already made; nothing more suggested
  await page.click('.dc-titlebar-menu');
  await item('Model').hover();
  await item('Create a Data Space\u2026').click();
  await page.waitForFunction(() => document.querySelectorAll('.dc-data-space .dc-class-file').length > 0
    || document.querySelector('.dc-data-space .dc-picker-status.dc-failed'), undefined, { timeout: 60_000 });
  const spaced = await page.$$eval('.dc-data-space .dc-class-file-line', (ls) => ls.map((l) => l.textContent));
  check(JSON.stringify(spaced) === JSON.stringify(['newtrades::Trades']), `the data space alone, its classes made: ${spaced.join(', ')}`);
  await page.locator('.dc-data-space .dc-primary').click();
  await page.waitForFunction(() => document.querySelector('.dc-data-space .dc-picker-status.dc-ok, .dc-data-space .dc-picker-status.dc-failed'),
    undefined, { timeout: 60_000 });
  check(/Created trades::Trades/.test(await page.locator('.dc-data-space .dc-picker-status').textContent()), 'the data space created');
  await page.locator('.dc-data-space .dc-primary').click();
  await page.locator('.dc-data-space').waitFor({ state: 'detached', timeout: 10_000 });

  // A PAGE FILE cannot carry the classes yet: refused, said, nothing written
  const downloads = [];
  page.on('download', (d) => downloads.push(d.suggestedFilename()));
  await page.click('.dc-titlebar-menu');
  await item('Export').hover();
  await item('Page File (JSON)').click();
  await until(() => alerts.length > 0, 10_000);
  check(alerts.some((a) => /a page file cannot carry them yet/.test(a)) && downloads.length === 0,
    `the page file refused while its classes are only here: ${alerts.join(' | ') || '(nothing said)'}`);
  alerts.length = 0;

  // A SHARE LINK carries the page's settings, not its classes, and says so
  await page.click('.dc-titlebar-menu');
  await item('Share…').click();
  await page.waitForFunction(() => /Its classes stay in datacube:trades/.test(document.querySelector('#sharenote')?.textContent ?? ''),
    undefined, { timeout: 10_000 }).then(() => check(true, 'the share link says the classes stay in this browser'),
    async () => bad(`the share note: ${await page.locator('#sharenote').textContent()}`));
  await page.click('#sharewin .hostwin-close');

  // SAVED, RELOADED AND OPENED AGAIN: each grid's file asked for, and read under the table name its class's store reads
  await page.click('.dc-titlebar-menu');
  await item('Save As\u2026').click();
  await page.locator('.dc-save').waitFor({ timeout: 10_000 });
  await page.fill('.dc-save-input', 'Classes');
  await page.locator('.dc-save-foot .dc-primary').click();
  await page.locator('.dc-save').waitFor({ state: 'detached', timeout: 30_000 });
  // saved with its model: the page is not changed since (a title led by a dot is a changed page's)
  const title = await page.title();
  check(title.startsWith('Classes') && !title.startsWith('\u2022'), `saved, its model with it, unchanged since: ${title}`);
  await page.goto(`http://127.0.0.1:${port}/demo/index.html`, { waitUntil: 'load', timeout: 120_000 });
  await page.waitForFunction(() => document.querySelectorAll('.dc-row').length > 0, undefined, { timeout: 120_000 });
  await page.click('.dc-titlebar-menu');
  await item('Open\u2026').click();
  const row = page.locator('#cubelib .dc-lib-row', { hasText: 'Classes' });
  await row.waitFor({ timeout: 10_000 });
  await row.locator('.dc-lib-button', { hasText: 'Open' }).click();
  for (let asked = 0; asked < 2; asked += 1) {
    await page.waitForSelector('#cubelib .dc-lib-ask:not([hidden])', { timeout: 30_000 });
    const ask = await page.locator('#cubelib .dc-lib-ask').textContent() ?? '';
    await page.setInputFiles('#cubelib .dc-lib-choose', /customers\.csv/.test(ask) ? customers : trades);
    await page.waitForFunction((n) => !document.querySelector('#cubelib .dc-lib-ask:not([hidden])')
      || !document.querySelector('#cubelib .dc-lib-ask')?.textContent?.includes(n), /customers\.csv/.test(ask) ? 'customers.csv' : 'trades.csv',
    { timeout: 30_000 });
  }
  await page.waitForFunction(() => /opened/.test(document.querySelector('#cubelib .dc-lib-message')?.textContent ?? ''),
    undefined, { timeout: 60_000 });
  const opened = await page.locator('#cubelib .dc-lib-message').textContent();
  check(!/not .* as the page's classes read it/.test(opened ?? ''), `opened, each table under its kept name: ${opened}`);
  await page.waitForFunction(() => document.querySelectorAll('.dc-tile-menu').length >= 2, undefined, { timeout: 60_000 });
  // the trades grid's class made again, as it was: nothing to write -- its store's table is the grid's own still
  await page.locator('.dc-tile-menu').nth(0).click();
  await item('Create a Class…').click();
  await page.locator('.dc-class-table tbody tr').nth(3).waitFor({ timeout: 60_000 });
  const again = page.locator('.dc-class-table tbody tr');
  await again.nth(1).locator('select').selectOption('key');
  await again.nth(2).locator('select').selectOption('omit');
  await page.waitForFunction(() => document.querySelectorAll('.dc-class-file').length === 0
    && document.querySelector('.dc-class .dc-primary:not([disabled])'), undefined, { timeout: 30_000 })
    .then(() => check(true, 'reopened: the class made again writes nothing (its grid reads the table its store names)'),
      async () => bad(`reopened, the class made again writes: ${(await files()).join(', ')}`));
  await page.locator('.dc-class .dc-picker-button.dc-quiet').click();
  await page.locator('.dc-class').waitFor({ state: 'detached', timeout: 10_000 });

  // A DASHBOARD'S TILE (§16.1): the grid made Trade gets Add a Grid over Trade -- the class read beside it at the page's
  // pin, its properties its columns, the same rows its table holds
  const columnsOf = (tile) => tile.locator('.dc-th[data-column]').evaluateAll((ths) => ths.map((t) => t.dataset.column));
  // a grid's own readout of its rows (its status bar's `40 rows × 4 cols in 3ms`), and a wait for it to say `n`
  const rowsOf = async (tile) => Number((/^([\d,]+) rows/.exec((await tile.locator('.dc-status-rows').first().textContent()) ?? '')?.[1]
    ?? 'NaN').replace(/,/g, ''));
  const untilRows = (tile, n) => tile.locator('.dc-status-rows').filter({ hasText: new RegExp(`^${n} rows`) }).first()
    .waitFor({ timeout: 30_000 }).catch(() => undefined);
  const tileWith = (column) => page.locator('[data-tile^="grid"]').filter({ has: page.locator(`.dc-th[data-column="${column}"]`) }).first();
  await page.locator('.dc-tile-menu').nth(0).click();
  await item('Add a Grid over Trade').click();
  const classTile = tileWith('tradeId');
  await classTile.locator('.dc-th[data-column="notional"]').waitFor({ timeout: 60_000 });
  await page.waitForFunction(() => document.querySelectorAll('.dc-tile-menu').length >= 3, undefined, { timeout: 60_000 });
  check(JSON.stringify(await columnsOf(classTile)) === JSON.stringify(['tradeId', 'notional']),
    `the class's grid: its properties, under their own names (${await columnsOf(classTile)})`);
  const fileTile = tileWith('trade_id');
  await untilRows(classTile, 40);
  check(await rowsOf(classTile) === 40, `it reads its table's 40 rows (${await rowsOf(classTile)})`);

  // THE PAGE'S FILTER (§16.2): set in the filter bar on the class's property, it filters the class's grid -- before its
  // columns are taken -- and never the file's grid, nor any grid's own filter
  await page.locator('.dc-page-filter-add').click();
  await page.locator('.dc-page-filter-property').selectOption('notional');
  await page.locator('.dc-page-filter-operator').selectOption('greaterThan');
  await page.fill('.dc-page-filter-value', '2000');
  await page.locator('.dc-page-filter-apply').click();
  await page.waitForFunction(() => document.querySelector('.dc-page-filter-words')?.textContent === 'Trade: notional > 2000',
    undefined, { timeout: 30_000 });
  await untilRows(classTile, 21);
  check(await rowsOf(classTile) === 21 && await rowsOf(fileTile) === 40,
    `the class's grid filtered (${await rowsOf(classTile)} of 40 rows), the file's not (${await rowsOf(fileTile)})`);
  check((await page.locator('.dc-page-filter-reach').textContent()) === '1 grid', 'the bar says what it reaches');
  check(/Page: notional > 2000/.test((await classTile.locator('.dc-status-page-filter').textContent()) ?? ''),
    'the class\'s grid says what filters it');

  // SAVED AND OPENED AGAIN: the class's grid reopened at the revision it was read at, the page's filter on it again
  await page.click('.dc-titlebar-menu');
  await item('Save').click();
  await page.locator('.dc-save').waitFor({ timeout: 10_000 });
  await page.locator('.dc-save-foot .dc-primary').click();
  await page.locator('.dc-save').waitFor({ state: 'detached', timeout: 30_000 });
  await page.goto(`http://127.0.0.1:${port}/demo/index.html`, { waitUntil: 'load', timeout: 120_000 });
  await page.waitForFunction(() => document.querySelectorAll('.dc-row').length > 0, undefined, { timeout: 120_000 });
  await page.click('.dc-titlebar-menu');
  await item('Open\u2026').click();
  const saved = page.locator('#cubelib .dc-lib-row', { hasText: 'Classes' });
  await saved.waitFor({ timeout: 10_000 });
  await saved.locator('.dc-lib-button', { hasText: 'Open' }).click();
  for (let asked = 0; asked < 2; asked += 1) {
    await page.waitForSelector('#cubelib .dc-lib-ask:not([hidden])', { timeout: 30_000 });
    const ask = await page.locator('#cubelib .dc-lib-ask').textContent() ?? '';
    await page.setInputFiles('#cubelib .dc-lib-choose', /customers\.csv/.test(ask) ? customers : trades);
    await page.waitForFunction((n) => !document.querySelector('#cubelib .dc-lib-ask:not([hidden])')
      || !document.querySelector('#cubelib .dc-lib-ask')?.textContent?.includes(n), /customers\.csv/.test(ask) ? 'customers.csv' : 'trades.csv',
    { timeout: 30_000 });
  }
  await page.waitForFunction(() => document.querySelectorAll('.dc-tile-menu').length >= 3, undefined, { timeout: 60_000 });
  await page.waitForFunction(() => document.querySelector('.dc-page-filter-words')?.textContent === 'Trade: notional > 2000',
    undefined, { timeout: 30_000 }).then(() => check(true, 'reopened: the page\'s filter is back in its bar'),
    () => bad('reopened: the page\'s filter is not in its bar'));
  await untilRows(tileWith('tradeId'), 21);
  check(await rowsOf(tileWith('tradeId')) === 21, `reopened: the class's grid filtered again (${await rowsOf(tileWith('tradeId'))} rows)`);
  check(errors.length === 0 && alerts.length === 0,
    `no page errors or other alerts${errors.length + alerts.length ? `: ${[...errors, ...alerts].join('; ')}` : ''}`);

  // THE WHOLE PAGE IN ONE GO, in a browser of its own (a page with no model yet): two files, Create a Data Space…,
  // both grids made classes in the same window, the suggested association chosen, all one revision
  const fresh = await browser.newContext({ viewport: { width: 1400, height: 900 } });
  const one = await fresh.newPage();
  one.on('pageerror', (e) => errors.push(`pageerror (one go): ${e.message}`));
  one.on('dialog', async (d) => { if (d.type() === 'beforeunload') await d.accept(); else { alerts.push(d.message()); await d.dismiss(); } });
  const pick = (label) => one.locator('.dc-menu .dc-menu-item', { has: one.locator(`:scope > .dc-menu-label:text-is(${JSON.stringify(label)})`) }).first();
  await one.goto(`http://127.0.0.1:${port}/demo/index.html`, { waitUntil: 'load', timeout: 120_000 });
  await one.waitForFunction(() => document.querySelectorAll('.dc-row').length > 0, undefined, { timeout: 120_000 });
  await one.click('.dc-titlebar-menu');
  await pick('New').hover();
  await pick('Blank Page').click();
  await one.click('.dc-blank .dc-primary');
  for (const [i, f] of [trades, customers].entries()) {
    if (i > 0) {
      await one.click('.dc-titlebar-menu');
      await pick('New').hover();
      await pick('Data Source\u2026').click();
    }
    await one.locator('.dc-picker .dc-picker-drop').waitFor({ state: 'visible', timeout: 10_000 });
    await one.setInputFiles('.dc-picker-file', f);
    await one.waitForFunction((k) => !document.querySelector('.dc-picker') && document.querySelectorAll('.dc-tile-menu').length >= k, i + 1,
      { timeout: 120_000 });
  }
  await one.click('.dc-titlebar-menu');
  await pick('Model').hover();
  await pick('Create a Data Space\u2026').click();
  await one.waitForFunction(() => document.querySelector('.dc-data-space .dc-association-sentence input[type=checkbox]')
    || document.querySelector('.dc-data-space .dc-picker-status.dc-failed'), undefined, { timeout: 60_000 });
  const toMake = await one.$$eval('.dc-data-space .dc-class-property', (is) => is.map((i) => i.value));
  check(JSON.stringify(toMake) === JSON.stringify(['Trade', 'Customer']), `both grids made classes in the window: ${toMake}`);
  await one.locator('.dc-data-space .dc-association-sentence input[type=checkbox]').first().check();
  await one.waitForFunction(() => [...document.querySelectorAll('.dc-data-space .dc-class-path')].some((p) => /_/.test(p.textContent ?? '')
    && /::domain::/.test(p.textContent ?? '')), undefined, { timeout: 30_000 });
  const all = await one.$$eval('.dc-data-space .dc-class-file-line', (ls) => ls.map((l) => l.textContent));
  check(all.every((f) => f.startsWith('new')) && all.includes('newtrades::Trades') && all.includes('newtrades::domain::Trade')
    && all.includes('newtrades::domain::Customer') && all.some((f) => /^newtrades::domain::\w+_\w+$/.test(f)),
  `the whole page in one go, every file new: ${all.join(', ')}`);
  await one.locator('.dc-data-space .dc-primary').click();
  await one.waitForFunction(() => document.querySelector('.dc-data-space .dc-picker-status.dc-ok, .dc-data-space .dc-picker-status.dc-failed'),
    undefined, { timeout: 60_000 });
  check(/Created trades::Trades in the project datacube:trades/.test(await one.locator('.dc-data-space .dc-picker-status').textContent()),
    `one go created: ${await one.locator('.dc-data-space .dc-picker-status').textContent()}`);
  await fresh.close();
  check(errors.length === 0 && alerts.length === 0,
    `no page errors or other alerts in one go${errors.length + alerts.length ? `: ${[...errors, ...alerts].join('; ')}` : ''}`);

  // A JOIN'S GRID MADE A CLASS (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §18.3), in a browser of its own: its base reads
  // both files' tables, so its class's store declares both and a sample of each is written; saved and opened again, each
  // of its leaves is read under the name the page's model keeps for it, and the class made again writes nothing
  const third = await browser.newContext({ viewport: { width: 1400, height: 900 } });
  const j = await third.newPage();
  j.on('pageerror', (e) => errors.push(`pageerror (join): ${e.message}`));
  j.on('dialog', async (d) => { if (d.type() === 'beforeunload') await d.accept(); else { alerts.push(d.message()); await d.dismiss(); } });
  const jItem = (label) => j.locator('.dc-menu .dc-menu-item', { has: j.locator(`:scope > .dc-menu-label:text-is(${JSON.stringify(label)})`) }).first();
  const joinedTile = () => j.locator('[data-tile^="grid"]').filter({ has: j.locator('.dc-th[data-column="their_customer_id"]') }).first();
  const jFiles = () => j.$$eval('.dc-class-file-line', (ls) => ls.map((l) => l.textContent));
  const jPure = (path) => j.$$eval('.dc-class-file', (ls, p) => ls.find((l) => l.querySelector('.dc-class-path')?.textContent === p)
    ?.querySelector('.dc-class-pure')?.textContent ?? '', path);
  const joinedClassWindow = async () => {
    await joinedTile().locator('.dc-tile-menu').click();
    await jItem('Create a Class…').click();
    await j.locator('.dc-class-table tbody tr').first().waitFor({ timeout: 60_000 });
  };
  await j.goto(`http://127.0.0.1:${port}/demo/index.html`, { waitUntil: 'load', timeout: 120_000 });
  await j.waitForFunction(() => document.querySelectorAll('.dc-row').length > 0, undefined, { timeout: 120_000 });
  await j.click('.dc-titlebar-menu');
  await jItem('New').hover();
  await jItem('Blank Page').click();
  await j.click('.dc-blank .dc-primary');
  for (const [i, f] of [trades, customers].entries()) {
    if (i > 0) {
      await j.click('.dc-titlebar-menu');
      await jItem('New').hover();
      await jItem('Data Source…').click();
    }
    await j.locator('.dc-picker .dc-picker-drop').waitFor({ state: 'visible', timeout: 10_000 });
    await j.setInputFiles('.dc-picker-file', f);
    await j.waitForFunction((k) => !document.querySelector('.dc-picker') && document.querySelectorAll('.dc-tile-menu').length >= k, i + 1,
      { timeout: 120_000 });
  }
  await j.locator('.dc-tile-menu').nth(0).click();
  await jItem('Join…').click();
  await j.locator('.dc-join').waitFor({ timeout: 10_000 });
  const theirs = await j.$$eval('.dc-join-their option', (os) => os.map((o) => ({ value: o.value, label: o.textContent })));
  await j.locator('.dc-join-their').selectOption(theirs.find((o) => o.label === 'customers.csv')?.value ?? '');
  await j.waitForFunction(() => document.querySelectorAll('.dc-join-will-line').length > 0, undefined, { timeout: 60_000 });
  await j.locator('.dc-join .dc-primary').click();
  await j.locator('.dc-join').waitFor({ state: 'detached', timeout: 60_000 });
  await joinedTile().waitFor({ timeout: 60_000 });
  await joinedClassWindow();
  await j.locator('.dc-class-names input').first().fill('TradeCustomer');
  await j.waitForFunction(() => [...document.querySelectorAll('.dc-class-path')].some((p) => /::domain::TradeCustomer$/.test(p.textContent ?? '')),
    undefined, { timeout: 30_000 });
  const joinedFiles = await jFiles();
  check(joinedFiles.some((f) => /^newtrades::data::Trades(_\d+)?Sample$/.test(f ?? '')) && joinedFiles.some((f) => /^newtrades::data::Customers(_\d+)?Sample$/.test(f ?? '')),
    `a join's class: a sample of each of its tables: ${joinedFiles.join(', ')}`);
  const joinedStore = await jPure('trades::store::Browser');
  check(/Table trades(_\d+)?\b/.test(joinedStore) && /Table customers(_\d+)?\b/.test(joinedStore), `its store declares both tables\n${joinedStore}`);
  await j.locator('.dc-class .dc-primary').click();
  await j.waitForFunction(() => document.querySelector('.dc-class .dc-picker-status.dc-ok, .dc-class .dc-picker-status.dc-failed'),
    undefined, { timeout: 60_000 });
  check(/Created trades::domain::TradeCustomer/.test(await j.locator('.dc-class .dc-picker-status').textContent()),
    `the join's class created: ${await j.locator('.dc-class .dc-picker-status').textContent()}`);
  await j.locator('.dc-class .dc-primary').click();
  await j.locator('.dc-class').waitFor({ state: 'detached', timeout: 10_000 });
  await j.click('.dc-titlebar-menu');
  await jItem('Save As…').click();
  await j.locator('.dc-save').waitFor({ timeout: 10_000 });
  await j.fill('.dc-save-input', 'Joined classes');
  await j.locator('.dc-save-foot .dc-primary').click();
  await j.locator('.dc-save').waitFor({ state: 'detached', timeout: 30_000 });
  await j.goto(`http://127.0.0.1:${port}/demo/index.html`, { waitUntil: 'load', timeout: 120_000 });
  await j.waitForFunction(() => document.querySelectorAll('.dc-row').length > 0, undefined, { timeout: 120_000 });
  await j.click('.dc-titlebar-menu');
  await jItem('Open…').click();
  const joinedRow = j.locator('#cubelib .dc-lib-row', { hasText: 'Joined classes' });
  await joinedRow.waitFor({ timeout: 10_000 });
  await joinedRow.locator('.dc-lib-button', { hasText: 'Open' }).click();
  for (;;) {
    const state = await j.waitForFunction(() => (document.querySelector('#cubelib .dc-lib-ask:not([hidden])') ? 'ask'
      : /opened|not opened/.test(document.querySelector('#cubelib .dc-lib-message')?.textContent ?? '') ? 'done' : false),
    undefined, { timeout: 60_000 }).then((h) => h.jsonValue());
    if (state === 'done') break;
    const which = /customers\.csv/.test(await j.locator('#cubelib .dc-lib-ask').textContent() ?? '') ? 'customers.csv' : 'trades.csv';
    await j.setInputFiles('#cubelib .dc-lib-choose', which === 'customers.csv' ? customers : trades);
    await j.waitForFunction((n) => !document.querySelector('#cubelib .dc-lib-ask:not([hidden])')
      || !document.querySelector('#cubelib .dc-lib-ask')?.textContent?.includes(n), which, { timeout: 30_000 });
  }
  const reopenedJoin = await j.locator('#cubelib .dc-lib-message').textContent();
  check(/opened/.test(reopenedJoin ?? '') && !/not opened|as the page's classes read it/.test(reopenedJoin ?? ''),
    `reopened, the join's leaves each under its kept name: ${reopenedJoin}`);
  await joinedTile().waitFor({ timeout: 60_000 });
  await joinedClassWindow();
  await j.locator('.dc-class-names input').first().fill('TradeCustomer');
  await j.waitForFunction(() => document.querySelectorAll('.dc-class-file').length === 0
    && document.querySelector('.dc-class .dc-primary:not([disabled])'), undefined, { timeout: 30_000 })
    .then(() => check(true, "reopened: the join's class made again writes nothing (its leaves read the tables its store names)"),
      async () => bad(`reopened, the join's class made again writes: ${(await jFiles()).join(', ')}`));
  await third.close();
  check(errors.length === 0 && alerts.length === 0,
    `no page errors or other alerts in the join's class${errors.length + alerts.length ? `: ${[...errors, ...alerts].join('; ')}` : ''}`);
} catch (e) {
  bad(`${e instanceof Error ? e.message : String(e)}${errors.length ? `\n  page: ${errors.join('; ')}` : ''}`);
} finally {
  await browser.close();
  await close();
}

console.log(failed ? 'verify-class: FAILED' : 'verify-class: ok');
process.exit(failed ? 1 : 0);
