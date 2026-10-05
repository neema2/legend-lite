// `bazel run //site:verify`: the one-origin promise, end to end in Chromium, with no server at all --
// a query built and saved in Legend Query (in this browser: query-store's page store, IndexedDB)
// is listed in DataCube's Saved queries on the same origin, says it comes from this browser, and
// opens as a grid with the rows it ran to in Query.
//
// Needs Playwright's Chromium (`bazel run //datacube:install_browser` once).

import { createServer } from 'node:http';
import { readFile, stat } from 'node:fs/promises';
import { extname, resolve, sep } from 'node:path';
import { createRequire } from 'node:module';
import { fileURLToPath, pathToFileURL } from 'node:url';

// Playwright as DataCube's harnesses have it (this package links no npm packages of its own)
const { chromium } = createRequire(fileURLToPath(new URL('../datacube/node_modules/', import.meta.url)))('playwright');

const ROOT = resolve(fileURLToPath(new URL('.', import.meta.url)), 'dist');
const TYPES = {
  '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.css': 'text/css', '.wasm': 'application/wasm',
  '.pure': 'text/plain', '.sql': 'text/plain', '.json': 'application/json', '.woff2': 'font/woff2',
};
// A REMOTE FILE to read by URL, which can be made to refuse (as a private bucket refuses without its keys)
let remoteRefuses = false;
const REMOTE_CSV = ['side,qty', ...Array.from({ length: 30 }, (_, i) => `${['BUY', 'SELL', 'HOLD'][i % 3]},${(i + 1) * 10}`)].join('\n') + '\n';
const server = createServer(async (req, res) => {
  const { pathname } = new URL(req.url ?? '/', 'http://x');
  if (pathname === '/remote/positions.csv') {
    if (remoteRefuses) { res.writeHead(403, { 'Content-Type': 'text/plain' }).end('forbidden'); return; }
    res.writeHead(200, { 'Content-Type': 'text/csv', 'Content-Length': Buffer.byteLength(REMOTE_CSV), 'Accept-Ranges': 'bytes' });
    res.end(req.method === 'HEAD' ? undefined : REMOTE_CSV);
    return;
  }
  const base = pathToFileURL(ROOT.endsWith(sep) ? ROOT : `${ROOT}${sep}`);
  const file = new URL(`.${pathname}`, base);
  try {
    if (!file.href.startsWith(base.href)) throw new Error('outside');
    const path = fileURLToPath(file);
    if (!(await stat(path)).isFile()) throw new Error('not a file');
    res.writeHead(200, { 'Content-Type': TYPES[extname(path)] ?? 'application/octet-stream' });
    res.end(await readFile(path));
  } catch {
    res.writeHead(404).end('not found');
  }
});
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const ORIGIN = `http://127.0.0.1:${server.address().port}`;

const browser = await chromium.launch();
const context = await browser.newContext({ viewport: { width: 1400, height: 900 }, permissions: ['clipboard-read', 'clipboard-write'] });
const errors = [];
let failed = false;
const name = `Shared sells ${Date.now().toString(36)}`;
const enc = encodeURIComponent;

try {
  // ---- DataCube first, same origin: its Saved queries open, BEFORE Query saves anything
  const cube = await context.newPage();
  cube.on('pageerror', (e) => errors.push(`datacube: ${e.message}`));
  await cube.goto(`${ORIGIN}/datacube/demo/index.html`);
  await cube.waitForSelector('.dc-row', { timeout: 120_000 });
  await cube.click('.dc-titlebar-menu');
  await cube.locator('.dc-menu .dc-menu-item', { has: cube.locator(':scope > .dc-menu-label:text-is("New")') }).hover();
  await cube.locator('.dc-menu .dc-menu-item', { has: cube.locator(':scope > .dc-menu-label:text-is("Data Source…")') }).click();
  await cube.locator('.dc-picker-tab[data-section="saved"]').click();
  await cube.locator('.dc-picker-body').waitFor({ timeout: 15_000 });

  // ---- then Legend Query, in this browser: build Trade Id, Side, Quantity where Side = SELL; save it
  const query = await context.newPage();
  query.on('pageerror', (e) => errors.push(`query: ${e.message}`));
  await query.goto(`${ORIGIN}/query/demo/index.html#/extensions/dataspace/${enc('demo:trading:0.0.0')}/${enc('demo::trading::TradingDataSpace')}?class=${enc('demo::trading::Trade')}`);
  await query.waitForSelector('.q-node', { timeout: 90_000 });
  for (const p of ['Trade Id', 'Side', 'Quantity']) await query.dblclick(`.q-node:has-text('${p}')`);
  await query.click(".q-node:has-text('Side')", { button: 'right' });
  await query.click(".q-menu button:has-text('Add as filter condition')");
  await query.selectOption('.q-cond select[aria-label=Side]', 'SELL');
  await query.click('button.q-run');
  await query.waitForFunction(() => /\d+ rows? in \d+ ms/.test(document.querySelector('.q-results-bar')?.textContent ?? ''), undefined, { timeout: 60_000 });
  const ranTo = await query.$$eval('.q-grid tbody tr', (trs) => trs.length);
  await query.click('button[title="Save (Ctrl+S)"]');
  await query.fill('.q-dialog input.q-input', name);
  await query.click('.q-dialog button.primary');
  await query.waitForFunction(() => location.hash.startsWith('#/edit/'), undefined, { timeout: 30_000 });
  console.log(`Query: saved "${name}" in this browser (it ran to ${ranTo} rows)`);

  // ---- DataCube's open list shows it, without reopening (BroadcastChannel); it opens as a grid
  await cube.bringToFront();
  const row = cube.locator('.dc-picker-row[data-query]', { hasText: name });
  await row.waitFor({ timeout: 15_000 });
  console.log('DataCube: the open picker listed it as Query saved it, without reopening');
  const said = (await cube.textContent('.dc-picker-body')) ?? '';
  if (!/Saved in this browser/.test(said)) throw new Error(`the picker does not say the list is this browser's: ${said.slice(0, 200)}`);
  await row.click();
  await cube.locator('.dc-picker').waitFor({ state: 'detached', timeout: 60_000 });
  const tile = cube.locator('[data-tile^="grid-"]').last();
  await tile.locator('.dc-row').first().waitFor({ timeout: 60_000 });
  const cols = (await tile.locator('.dc-th').allTextContents()).map((t) => t.trim());
  const sides = await tile.locator('.dc-row').evaluateAll((rows) => rows.map((r) => r.querySelectorAll('.dc-cell')[1]?.textContent?.trim()));
  if (cols.join() !== 'Trade Id,Side,Quantity') throw new Error(`DataCube shows columns ${cols.join(', ')}`);
  if (sides.length !== ranTo || sides.some((s) => s !== 'SELL')) throw new Error(`DataCube shows ${JSON.stringify(sides)}, Query ran to ${ranTo} SELL rows`);
  console.log(`DataCube: listed from this browser, opened as a grid of ${sides.length} rows, Side as SELL`);

  // ---- THE SHARE LINK: copied in Query, opened by someone else (a fresh browser: an empty store)
  await query.bringToFront();
  await query.click('.q-header-pill:has-text("Advanced")');
  await query.click(".q-menu button:has-text('Copy share link')");
  await query.waitForSelector('.q-toast:has-text("Share link copied")', { timeout: 15_000 });
  const link = await query.evaluate(() => navigator.clipboard.readText());
  if (!/#\/shared\/q1\.[A-Za-z0-9_-]+$/.test(link)) throw new Error(`the share link is ${link}`);
  const other = await browser.newContext({ viewport: { width: 1400, height: 900 } });
  try {
    // in Query: the query, unsaved, named as shared; it runs to the same rows, and Save keeps their own copy
    const theirs = await other.newPage();
    theirs.on('pageerror', (e) => errors.push(`query (shared): ${e.message}`));
    await theirs.goto(link);
    await theirs.waitForSelector('.q-chip:has-text("shared link")', { timeout: 90_000 });
    const title = await theirs.textContent('.q-builder__title');
    if (title !== name) throw new Error(`the shared query opened as "${title}"`);
    await theirs.click('button.q-run');
    await theirs.waitForFunction(() => /\d+ rows? in \d+ ms/.test(document.querySelector('.q-results-bar')?.textContent ?? ''), undefined, { timeout: 60_000 });
    const theirRows = await theirs.$$eval('.q-grid tbody tr', (trs) => trs.length);
    if (theirRows !== ranTo) throw new Error(`the shared query ran to ${theirRows} rows, not ${ranTo}`);
    await theirs.click('button[title="Save (Ctrl+S)"]');
    if ((await theirs.inputValue('.q-dialog input.q-input')) !== name) throw new Error('Save does not offer the shared name');
    await theirs.click('.q-dialog button.primary');
    await theirs.waitForFunction(() => location.hash.startsWith('#/edit/'), undefined, { timeout: 30_000 });
    console.log(`share link (${link.length} characters): opened unsaved in another browser, ran to ${theirRows} rows, saved as their own`);
    // in DataCube: the same link's query as the cube's source
    const cubeLink = `${ORIGIN}/datacube/demo/index.html#${link.slice(link.indexOf('#/shared/') + '#/shared/'.length)}`;
    const theirCube = await other.newPage();
    theirCube.on('pageerror', (e) => errors.push(`datacube (shared): ${e.message}`));
    await theirCube.goto(cubeLink);
    await theirCube.waitForFunction(() => [...document.querySelectorAll('.dc-th')].map((e) => e.textContent?.trim()).join() === 'Trade Id,Side,Quantity',
      undefined, { timeout: 120_000 });
    const cubeRows = await theirCube.locator('.dc-row').count();
    if (cubeRows !== ranTo) throw new Error(`DataCube opened the link to ${cubeRows} rows`);
    if (await theirCube.evaluate(() => location.hash) !== '') throw new Error('the link stayed in the address');
    console.log(`DataCube: the link opened as the cube's source, ${cubeRows} rows`);

    // DataCube's own Copy link, beside the saved query's row: the same query, opened by the other browser
    await cube.bringToFront();
    await cube.click('.dc-titlebar-menu');
    await cube.locator('.dc-menu .dc-menu-item', { has: cube.locator(':scope > .dc-menu-label:text-is("New")') }).hover();
    await cube.locator('.dc-menu .dc-menu-item', { has: cube.locator(':scope > .dc-menu-label:text-is("Data Source\u2026")') }).click();
    await cube.locator('.dc-picker-tab[data-section="saved"]').click();
    await cube.locator('.dc-picker-entry', { hasText: name }).locator('.dc-picker-row-link').click();
    await cube.waitForSelector('.dc-picker-status.dc-done', { timeout: 15_000 });
    const cubeOwn = await cube.evaluate(() => navigator.clipboard.readText());
    await cube.keyboard.press('Escape');
    if (!/\/datacube\/demo\/index\.html#q1\.[A-Za-z0-9_-]+$/.test(cubeOwn)) throw new Error(`DataCube's link is ${cubeOwn}`);
    const viaCube = await other.newPage();
    viaCube.on('pageerror', (e) => errors.push(`datacube (its link): ${e.message}`));
    await viaCube.goto(cubeOwn);
    await viaCube.waitForFunction(() => [...document.querySelectorAll('.dc-th')].map((e) => e.textContent?.trim()).join() === 'Trade Id,Side,Quantity',
      undefined, { timeout: 120_000 });
    const viaRows = await viaCube.locator('.dc-row').count();
    if (viaRows !== ranTo) throw new Error(`DataCube's own link opened to ${viaRows} rows`);
    console.log(`DataCube: its own Copy link (${cubeOwn.length} characters) opened in the other browser, ${viaRows} rows`);

    // A CUBE BUILT ON IT, shared and saved: grouped by Side, then ☰ ▸ Share… and ☰ ▸ Save
    const menuOf = async (p, ...path) => {
      for (const [i, label] of path.entries()) {
        const item = p.locator('.dc-menu .dc-menu-item', { has: p.locator(`:scope > .dc-menu-label:text-is(${JSON.stringify(label)})`) }).first();
        if (i < path.length - 1) await item.hover(); else await item.click();
      }
    };
    const grouped = (p) => p.waitForFunction(() => {
      const rows = [...document.querySelectorAll('.dc-row')].map((r) => r.textContent ?? '');
      return rows.length === 1 && rows[0].includes('SELL');
    }, undefined, { timeout: 60_000 });
    await viaCube.locator('.dc-row').first().locator('.dc-cell').nth(1).click({ button: 'right' });
    await viaCube.locator('.dc-menu .dc-menu-item', { has: viaCube.locator(':scope > .dc-menu-label:text-is("Pivot")') }).first().hover();
    await viaCube.locator('.dc-menu .dc-menu-item').filter({ hasText: /^Vertical Pivot on/ }).filter({ hasNot: viaCube.locator('.dc-submenu') }).first().click();
    await grouped(viaCube);
    await viaCube.click('.dc-titlebar-menu');
    await menuOf(viaCube, 'Share\u2026');
    await viaCube.waitForFunction(() => (document.getElementById('sharelink')?.value ?? '').includes('#p1.'), undefined, { timeout: 15_000 });
    const pageLink = await viaCube.inputValue('#sharelink');
    const third = await browser.newContext({ viewport: { width: 1400, height: 900 } });
    try {
      const fresh = await third.newPage();
      fresh.on('pageerror', (e) => errors.push(`datacube (shared cube): ${e.message}`));
      await fresh.goto(pageLink);
      await grouped(fresh);
      console.log(`DataCube: Share… of a cube over the saved query (${pageLink.length} characters) opened grouped by Side in a third browser`);
    } finally {
      await third.close();
    }
    await viaCube.keyboard.press('Escape');
    await viaCube.click('.dc-titlebar-menu');
    await menuOf(viaCube, 'Save');
    await viaCube.locator('.dc-save').waitFor({ timeout: 10_000 });
    await viaCube.fill('.dc-save-input', 'Sells by side');
    if (!/runs the saved query/.test((await viaCube.textContent('.dc-save')) ?? '')) throw new Error('the Save window does not say the query runs again');
    await viaCube.click('.dc-save .dc-primary');
    await viaCube.locator('.dc-save').waitFor({ state: 'detached', timeout: 15_000 });
    await viaCube.goto(`${ORIGIN}/datacube/demo/index.html`);
    await viaCube.waitForSelector('.dc-row', { timeout: 120_000 });
    await viaCube.click('.dc-titlebar-menu');
    await menuOf(viaCube, 'Open\u2026');
    await viaCube.locator('.dc-lib-row', { hasText: 'Sells by side' }).locator('button', { hasText: 'Open' }).click();
    await grouped(viaCube);
    console.log('DataCube: Save of that cube, reopened from Open… after a reload, grouped by Side');
  } finally {
    await other.close();
  }
  // ---- A CUBE OVER A REMOTE FILE: opened by URL, grouped, shared, saved; refused, it asks for keys
  const remoteCtx = await browser.newContext({ viewport: { width: 1400, height: 900 } });
  try {
    const r = await remoteCtx.newPage();
    r.on('pageerror', (e) => errors.push(`datacube (remote): ${e.message}`));
    const menuOf = async (...path) => {
      await r.click('.dc-titlebar-menu');
      for (const [i, label] of path.entries()) {
        const item = r.locator('.dc-menu .dc-menu-item', { has: r.locator(`:scope > .dc-menu-label:text-is(${JSON.stringify(label)})`) }).first();
        if (i < path.length - 1) await item.hover(); else await item.click();
      }
    };
    const threeGroups = (p) => p.waitForFunction(() => {
      const rows = [...document.querySelectorAll('.dc-row')].map((x) => x.textContent ?? '');
      return rows.length === 3 && ['BUY', 'HOLD', 'SELL'].every((s) => rows.some((t) => t.includes(s)));
    }, undefined, { timeout: 60_000 });
    await r.goto(`${ORIGIN}/datacube/demo/index.html`);
    await r.waitForSelector('.dc-row', { timeout: 120_000 });
    await menuOf('New', 'Blank Page');
    await r.click('.dc-blank .dc-primary');
    await r.locator('.dc-picker-tab[data-section="remote"]').click();
    await r.fill('.dc-picker-url', `${ORIGIN}/remote/positions.csv`);
    await r.click('.dc-picker-form button[type=submit]');
    await r.waitForSelector('.dc-picker', { state: 'detached', timeout: 60_000 });
    await r.waitForFunction(() => document.querySelectorAll('.dc-row').length === 30, undefined, { timeout: 60_000 });
    await r.locator('.dc-row').first().locator('.dc-cell').nth(0).click({ button: 'right' });
    await r.locator('.dc-menu .dc-menu-item', { has: r.locator(':scope > .dc-menu-label:text-is("Pivot")') }).first().hover();
    await r.locator('.dc-menu .dc-menu-item').filter({ hasText: /^Vertical Pivot on/ }).filter({ hasNot: r.locator('.dc-submenu') }).first().click();
    await threeGroups(r);
    await menuOf('Share…');
    await r.waitForFunction(() => (document.getElementById('sharelink')?.value ?? '').includes('#p1.'), undefined, { timeout: 15_000 });
    const remoteLink = await r.inputValue('#sharelink');
    await r.keyboard.press('Escape');
    const someoneCtx = await browser.newContext();
    try {
      const someone = await someoneCtx.newPage();
      someone.on('pageerror', (e) => errors.push(`datacube (remote, shared): ${e.message}`));
      await someone.goto(remoteLink);
      await threeGroups(someone);
    } finally {
      await someoneCtx.close();
    }
    console.log(`DataCube: a cube over a remote file (grouped by side), shared (${remoteLink.length} characters), opened grouped in another browser`);
    // saved, then the file refuses: reopening asks for its keys, the URL filled in; once it reads again, it opens
    await menuOf('Save');
    await r.locator('.dc-save').waitFor();
    await r.fill('.dc-save-input', 'Positions by side');
    await r.click('.dc-save .dc-primary');
    await r.locator('.dc-save').waitFor({ state: 'detached' });
    remoteRefuses = true;
    await r.reload();
    await r.waitForSelector('.dc-row', { timeout: 120_000 });
    await menuOf('Open…');
    await r.locator('.dc-lib-row', { hasText: 'Positions by side' }).locator('button', { hasText: 'Open' }).click();
    await r.waitForSelector('.dc-picker .dc-picker-url', { timeout: 60_000 });
    const offered = await r.inputValue('.dc-picker-url');
    const keysOpen = await r.locator('.dc-picker-more').evaluate((d) => d.open);
    const why = await r.locator('.dc-picker-subtitle').last().textContent();
    if (offered !== `${ORIGIN}/remote/positions.csv` || !keysOpen || !/did not open without its keys/.test(why ?? '')) {
      throw new Error(`a refused remote file asked: url ${offered}, keys open ${keysOpen}, "${why}"`);
    }
    remoteRefuses = false;
    await r.click('.dc-picker-form button[type=submit]');
    await r.waitForSelector('.dc-picker', { state: 'detached', timeout: 60_000 });
    await threeGroups(r);
    console.log('DataCube: its Save, refused on reopen, asked for keys with the URL filled in, then reopened grouped');
  } finally {
    await remoteCtx.close();
  }

  // Studio on the same origin: the projects it publishes are kept in this origin's store (sdlc-client's
  // legend-projects in IndexedDB), so they are there again after the page is opened afresh
  const studio = await context.newPage();
  await studio.goto(`${ORIGIN}/studio/demo/index.html`);
  await studio.getByText('Welcome to Legend Studio').waitFor({ timeout: 60_000 });
  await studio.getByTestId('load-demo').click();
  await studio.waitForFunction(() => /Demo projects published/.test(document.querySelector('[data-testid=demo-status]')?.textContent ?? ''), undefined, { timeout: 180_000 });
  await studio.reload();
  await studio.getByTestId('project-selector').click();
  const kept = await studio.getByTestId('project-selector-menu').textContent({ timeout: 60_000 });
  if (!/org\.finos\.lite\.demo:trading/.test(kept ?? '')) throw new Error(`Studio's projects did not survive a reload on this origin: ${kept}`);
  console.log('Studio: served from the same origin, its demo projects published and still there after a reload');
  await studio.close();

  // Query lists what Studio published without loading it; opening party at HEAD from the start page puts its classes
  // in the Classes section
  const start = await context.newPage();
  await start.goto(`${ORIGIN}/query/demo/index.html#/setup`);
  const partyCard = start.locator('button[data-project="org.finos.lite.demo:party"]');
  await partyCard.waitFor({ timeout: 120_000 });
  await partyCard.click();
  await start.waitForFunction(() => document.body.textContent?.includes('PartyMapping')
    && !document.querySelector('button[data-project="org.finos.lite.demo:party"]'), undefined, { timeout: 120_000 });
  console.log('Query: the start page lists the projects in Depot unloaded; party opened at HEAD shows its classes');
  await start.close();

  // Query opens what Studio published, by name (design Phase 3): the party project at its line's HEAD (Depot's
  // master-SNAPSHOT), with its dependency on types, and runs a query on it -- the rows of its own Data element (plan A2)
  const byName = await context.newPage();
  const partyGav = enc('org.finos.lite.demo:party:master-SNAPSHOT');
  await byName.goto(`${ORIGIN}/query/demo/index.html#/create/manual/${partyGav}/${enc('demo::party::PartyMapping')}/${enc('demo::party::Runtime')}?class=${enc('demo::party::Party')}`);
  await byName.waitForSelector('.q-node', { timeout: 120_000 });
  for (const p of ['Name', 'Country']) await byName.dblclick(`.q-node:has-text('${p}')`);
  await byName.click('button.q-run');
  await byName.waitForFunction(() => document.querySelector('.q-error-box')
    || /\d+ rows? in \d+ ms/.test(document.querySelector('.q-results-bar')?.textContent ?? ''), undefined, { timeout: 120_000 });
  const refused = await byName.$('.q-error-box');
  if (refused) throw new Error(`Query on Studio's party project, by name, refused the run: ${await refused.textContent()}`);
  const parties = await byName.locator('.q-results-bar').textContent();
  if (!/(^|\D)5 rows? in/.test(parties ?? '')) throw new Error(`Query on Studio's party project, by name, did not show its 5 rows: "${parties}"`);
  const names = await byName.$$eval('.q-grid tbody tr', (trs) => trs.map((tr) => tr.textContent ?? ''));
  if (!names.some((n) => n.includes('Banque Lumière'))) throw new Error(`the party rows are not the seeded ones: ${names.join(' | ')}`);
  const shown = (parties ?? '').match(/\d+ rows? in \d+ ms/)?.[0];
  console.log(`Query: Studio's party project opened by name at HEAD (master-SNAPSHOT), with its dependencies -- ${shown}`);

  // the version picker: HEAD and the releases; 1.0.0 opens by name (loaded the first time) and runs too
  const version = byName.locator('select[aria-label=Version]');
  await byName.waitForFunction(() => (document.querySelector('select[aria-label=Version]')?.options.length ?? 0) > 1, undefined, { timeout: 60_000 });
  const offered = await version.locator('option').allTextContents();
  if (offered[0] !== 'HEAD' || !offered.includes('1.0.0')) throw new Error(`the version picker offers ${JSON.stringify(offered)}`);
  // cancelled, the picker shows HEAD again; then for real
  await version.selectOption('1.0.0');
  await byName.click(".q-dialog button:has-text('Cancel')");
  await byName.waitForFunction(() => document.querySelector('select[aria-label=Version]')?.value === 'master-SNAPSHOT', undefined, { timeout: 30_000 });
  await version.selectOption('1.0.0');
  // the builder has unsaved work: Query asks before leaving it, as upstream does
  await byName.click(".q-dialog button:has-text('Leave')");
  await byName.waitForFunction(() => location.hash.includes(encodeURIComponent('org.finos.lite.demo:party:1.0.0')), undefined, { timeout: 60_000 });
  await byName.waitForSelector('.q-node', { timeout: 120_000 });
  for (const p of ['Name', 'Country']) await byName.dblclick(`.q-node:has-text('${p}')`);
  await byName.click('button.q-run');
  await byName.waitForFunction(() => document.querySelector('.q-error-box')
    || /\d+ rows? in \d+ ms/.test(document.querySelector('.q-results-bar')?.textContent ?? ''), undefined, { timeout: 120_000 });
  const refusedAt = await byName.$('.q-error-box');
  if (refusedAt) throw new Error(`Query on party 1.0.0 refused the run: ${await refusedAt.textContent()}`);
  const atRelease = (await byName.locator('.q-results-bar').textContent() ?? '').match(/\d+ rows? in \d+ ms/)?.[0];
  console.log(`Query: the version picker offers ${offered.join(', ')}; party 1.0.0 opened by name -- ${atRelease}`);

  // saved on 1.0.0, it reopens on 1.0.0 after a reload (only HEAD loads at start: 1.0.0 is loaded by name again)
  await byName.click('button[title="Save (Ctrl+S)"]');
  await byName.fill('.q-dialog input.q-input', 'Parties at 1.0.0');
  await byName.click('.q-dialog button.primary');
  await byName.waitForFunction(() => location.hash.startsWith('#/edit/'), undefined, { timeout: 30_000 });
  await byName.reload();
  await byName.waitForFunction(() => document.querySelector('select[aria-label=Version]')?.value === '1.0.0', undefined, { timeout: 120_000 });
  console.log('Query: saved on party 1.0.0, it reopens on 1.0.0 (pinned; loaded by name after a reload)');
  await byName.close();

  // DataCube opens that saved query: its project is not in DataCube's config.json, so it opens party 1.0.0 by name
  // from the same Depot, seeds the party rows, and shows them as a grid
  const cubeByName = await context.newPage();
  await cubeByName.goto(`${ORIGIN}/datacube/demo/index.html`);
  await cubeByName.waitForSelector('.dc-row', { timeout: 120_000 });
  await cubeByName.click('.dc-titlebar-menu');
  await cubeByName.locator('.dc-menu .dc-menu-item', { has: cubeByName.locator(':scope > .dc-menu-label:text-is("New")') }).hover();
  await cubeByName.locator('.dc-menu .dc-menu-item', { has: cubeByName.locator(':scope > .dc-menu-label:text-is("Data Source…")') }).click();
  await cubeByName.locator('.dc-picker-tab[data-section="saved"]').click();
  const parties1 = cubeByName.locator('.dc-picker-row[data-query]', { hasText: 'Parties at 1.0.0' });
  await parties1.waitFor({ timeout: 30_000 });
  await parties1.click();
  await cubeByName.locator('.dc-picker').waitFor({ state: 'detached', timeout: 120_000 });
  const partyTile = cubeByName.locator('[data-tile^="grid-"]').last();
  await partyTile.locator('.dc-row').first().waitFor({ timeout: 120_000 });
  const partyRows = await partyTile.locator('.dc-row').count();
  const partyText = await partyTile.textContent();
  if (partyRows !== 5 || !partyText?.includes('Banque Lumière')) throw new Error(`DataCube showed ${partyRows} party rows: ${partyText?.slice(0, 200)}`);
  console.log('DataCube: the query saved on party 1.0.0 opened by name from Depot -- 5 rows');
  await cubeByName.close();
} catch (e) {
  failed = true;
  console.log(`FAIL: ${String(e.message ?? e).split('\n')[0]}`);
} finally {
  await browser.close();
  server.close();
}
if (errors.length) {
  failed = true;
  console.log(`page errors: ${errors.join(' | ')}`);
}
console.log(failed ? '\n!!! the apps do not share their stores !!!' : '\n*** saved in Query, opened in DataCube; Studio publishes, Query and DataCube open it by name: one origin, no server ***');
process.exit(failed ? 1 : 0);
