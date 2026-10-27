// DATACUBE AGAINST PYTHON'S ENGINE, run in the pinned Chromium (//datacube:python_engine_test). Node here only drives:
// it starts Python's engine (engine_fixture.py, beside it: the cube corpus's rows as a Live frame, DataCube's site
// served beside the API, so the page and the engine are one origin), hands the browser the test page (Playwright's
// routing, for /python-engine.html, its bundle and upstream's recorded Arrow answer) with the engine's token, and
// reports what the page found (test/python-engine/page.ts: every check runs there). Then the page a person opens
// (demo/engine.html), and a notebook's cubes (demo/widget-loader.ts and widget.ts, as anywidget runs a widget), their
// messages carried between the page and Python's widgets as a notebook's channel carries them.

// first: points Playwright at the Chromium Bazel fetched
import '../../../tools/browser/pinned-chromium.mjs';
import { spawn } from 'node:child_process';
import { mkdtempSync, readdirSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import { createInterface } from 'node:readline';
import { chromium } from 'playwright';
import { runfileFromEnv } from '../../../tools/js/runfiles.mts';

const fixture = runfileFromEnv('ENGINE_FIXTURE');
const site = runfileFromEnv('DATACUBE_SITE');
const bundle = readFileSync(runfileFromEnv('PYTHON_ENGINE_PAGE'));
const upstream = readFileSync(runfileFromEnv('UPSTREAM_ARROW_ANSWER'));
// this user's legend-lite folder, the test's own (the pages Save writes go there, never into a real home)
const home = mkdtempSync(join(process.env.TEST_TMPDIR, 'legend-lite-home-'));

// A WAREHOUSE (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md C3): the Bazel-built native one, its owner alice and a reader,
// rita, seeded here as its owner (a table rita may read, one she may not); the page reads it through Python, signed in
// there as rita
const warehouse = spawn(runfileFromEnv('WAREHOUSE_BINARY'), ['--port', '0',
  '--data', mkdtempSync(join(process.env.TEST_TMPDIR, 'warehouse-')), '--user', 'alice:alice-pw', '--user', 'rita:rita-pw',
  '--owner', 'alice', '--duckdb-library', runfileFromEnv('WAREHOUSE_DUCKDB_LIBRARY')], { stdio: ['ignore', 'ignore', 'pipe'] });
let warehouseSaid = '';
const warehousePort = await new Promise((done, fail) => {
  warehouse.stderr.on('data', (b) => {
    warehouseSaid += String(b);
    for (const line of String(b).split('\n')) if (line) process.stderr.write(`[warehouse] ${line}\n`);
    const m = /listening on 127\.0\.0\.1:(\d+)/.exec(warehouseSaid);
    if (m) done(Number(m[1]));
  });
  warehouse.on('exit', (code) => fail(new Error(`the warehouse exited (${code}): ${warehouseSaid}`)));
});
const warehouseStopped = new Promise((done) => warehouse.once('exit', done));
const warehouseUrl = `http://127.0.0.1:${warehousePort}`;
const owner = (await (await fetch(`${warehouseUrl}/sql/v1/login`, { method: 'POST',
  headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ user: 'alice', password: 'alice-pw' }) })).json()).token;
for (const sql of [
  'CREATE TABLE POSITIONS (desk VARCHAR, qty INTEGER, px DECIMAL(10,2))',
  "INSERT INTO POSITIONS VALUES ('FX', 10, 1.50), ('EQ', 5, 2.25), ('FX', 3, 4.00)",
  'CREATE TABLE secret (s VARCHAR)',
  'GRANT SELECT ON TABLE POSITIONS TO rita',
]) {
  const said = await (await fetch(`${warehouseUrl}/sql/v1/statements`, { method: 'POST',
    headers: { Authorization: `Bearer ${owner}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({ sql, catalog: 'main', waitMs: 30_000 }) })).json();
  if (said.state !== 'succeeded') throw new Error(`the warehouse's setup: ${sql}: ${JSON.stringify(said)}`);
}

// the library by its runfiles path, as //python's tests are given it: a program a test starts gets no BUILD env
const engine = spawn(fixture, ['--site', site], {
  stdio: ['pipe', 'pipe', 'pipe'],
  env: { ...process.env, LEGEND_LITE_LIBRARY: runfileFromEnv('LEGEND_LITE_LIBRARY'),
    LEGEND_LITE_DATACUBE: runfileFromEnv('LEGEND_LITE_DATACUBE'),
    LEGEND_LITE_DUCKDB_EXTENSIONS: runfileFromEnv('LEGEND_LITE_DUCKDB_EXTENSIONS'), LEGEND_LITE_HOME: home, PYTHONUTF8: '1',
    LEGEND_LITE_QUERY_SITE: runfileFromEnv('QUERY_SITE'), LEGEND_LITE_STUDIO_SITE: runfileFromEnv('STUDIO_SITE'),
    PYTHONDONTWRITEBYTECODE: '1' },
});
// the engine's own account, in this test's log: a server-side reason is then never invisible
engine.stderr.on('data', (b) => { for (const line of String(b).split('\n')) if (line) process.stderr.write(`[engine] ${line}\n`); });
const stopped = new Promise((done) => engine.once('exit', done));
// the engine's lines: the first says what it serves, each after it answers a command (`done <command>`); and, as they
// come, what its notebook cubes send their page (`sent ...`, `trait ...`), handed to the widget page as a notebook's
// channel would
const waiting = [];
const queued = [];
let toWidgets = () => {};
createInterface({ input: engine.stdout }).on('line', (line) => {
  if (line.startsWith('sent ') || line.startsWith('trait ')) {
    toWidgets(line);
    return;
  }
  const next = waiting.shift();
  if (next) next(line);
  else queued.push(line);
});
const exited = new Promise((_, fail) => engine.once('exit', (code) => fail(new Error(`the engine exited (${code})`))));
const nextLine = () => Promise.race([
  queued.length > 0 ? Promise.resolve(queued.shift()) : new Promise((done) => { waiting.push(done); }),
  exited,
]);
// (show() says its link first, "DataCube: <url>", as it does for a person)
let first = await nextLine();
while (first !== undefined && !first.startsWith('{')) first = await nextLine();
const served = JSON.parse(first);
const origin = served.url;

let failed = false;
// the browser closing at the end: a message the engine sends meanwhile (an answer to a call a view taken down made, say)
// finds its page gone, which is the test's own close and no case's failure (CI runs 38175909649 and 38187695487)
let closing = false;
let browser;
try {
  browser = await chromium.launch();
  // a browser away from UTC: every check here answers alike whatever zone the person is in (the tab's DuckDB set up as
  // Python's: services/browser.ts setUpSession)
  const page = await browser.newPage({ timezoneId: 'America/New_York' });
  page.on('pageerror', (e) => { console.log(`page error: ${e.message}`); failed = true; });
  page.on('console', (m) => { if (m.type() === 'error') console.log(`page console: ${m.text()}`); });
  page.on('requestfailed', (r) => console.log(`request failed: ${r.url()} -- ${r.failure()?.errorText ?? ''}`));
  // what the page is handed: the token, and the frame the engine serves (the address is the page's own origin)
  const { url: _origin, link: _link, ...given } = served;
  await page.addInitScript((g) => { window.__pythonEngineServed = g; }, { ...given, warehouse: warehouseUrl });
  // the test page, its bundle and upstream's recorded answer; everything else (the site, the API) is the engine's
  await page.route(`${origin}/python-engine.html`, (r) => r.fulfill({
    contentType: 'text/html',
    body: '<!doctype html><meta charset="utf-8"><title>DataCube on Python</title><script type="module" src="python-engine.js"></script>',
  }));
  await page.route(`${origin}/python-engine.js`, (r) => r.fulfill({ contentType: 'text/javascript', body: bundle }));
  await page.route(`${origin}/upstream.arrows.zst`, (r) => r.fulfill({ contentType: 'application/octet-stream', body: upstream }));
  await page.goto(`${origin}/python-engine.html`);
  await page.waitForFunction(() => window.__pythonEngine?.done === true, undefined, { timeout: 280_000 }).catch(async (e) => {
    console.log(`the page did not finish; it was at: ${await page.evaluate(() => window.__pythonEngineStage ?? '(no stage)')}`);
    throw e;
  });
  const verdict = await page.evaluate(() => window.__pythonEngine);
  for (const o of verdict.outcomes) {
    console.log(`${o.ok ? 'ok  ' : 'FAIL'} ${o.name} (${Math.round(o.ms)} ms)${o.ok ? '' : `\n${o.message}`}`);
    if (!o.ok) failed = true;
  }
  if (verdict.setupError) {
    console.log(`FAIL the setup: ${verdict.setupError}`);
    failed = true;
  }
  // 2026-10-23 (D23, L12's proof): 9 -> 11, a remote file read and a Snap copied the same on Python and in the tab
  if (verdict.outcomes.length !== 11) {
    console.log(`FAIL ${verdict.outcomes.length} checks ran, not 11`);
    failed = true;
  }
  // THE FIXTURE'S FILE SERVER (R6), asked from here: a page's own fetch of another loopback port is refused by the
  // browser's local-network rule, though DuckDB-WASM's reads above were not -- a range the file has no byte of answered
  // as HTTP says, never with a malformed part
  const past = await fetch(served.remote, { headers: { Range: 'bytes=99999999-' } });
  console.log(`${past.status === 416 ? 'ok  ' : 'FAIL'} the file server answers a range past the file's end with 416 (it answered ${past.status})`);
  if (past.status !== 416) failed = true;

  // THE PAGE A PERSON OPENS (demo/engine.html, from the engine's site): the link names the frame and carries the
  // token in its fragment; the cube opens over the engine and shows the frame's rows, with nothing run in the tab
  const cube = await browser.newPage();
  cube.on('pageerror', (e) => { console.log(`engine page error: ${e.message}`); failed = true; });
  cube.on('requestfailed', (r) => console.log(`engine page request failed: ${r.url()} ${r.postData() ?? ''} -- ${r.failure()?.errorText ?? ''}`));
  cube.on('console', (m) => console.log(`engine page console ${m.type()}: ${m.text()}`));
  // every file and call the page asks for is answered: a refusal (a missing file, a refused call) fails the test -- but
  // for the SDLC's own "not there yet": the page asks for its workspace before it makes one, and the SDLC answers a
  // workspace not yet made 404, as legend-sdlc does (the in-page SDLC answers the same, inside the tab)
  cube.on('response', (r) => {
    const notYet = r.status() === 404 && r.request().method() === 'GET'
      && /^\/sdlc\/api\/projects\/[^/]+\/workspaces\/[^/]+$/.test(new URL(r.url()).pathname);
    if (r.status() >= 400 && !notYet) {
      failed = true;
      // with the engine's reason, as the page was told it
      void r.text().then((t) => t, () => '').then((t) => console.log(`FAIL engine page answer ${r.status()}: ${r.url()} ${t.slice(0, 600)}`));
    }
  });
  // the link show() returned (cube.url)
  await cube.goto(served.link);
  const shown = await cube.waitForFunction(() => /\b10 rows\b/.test(document.body.innerText)
    && document.body.innerText.includes('APAC'), undefined, { timeout: 60_000 }).then(() => true, () => false);
  console.log(`${shown ? 'ok  ' : 'FAIL'} the engine's page opens the frame's cube and shows its rows`);
  if (!shown) {
    console.log(`  the page showed: ${(await cube.evaluate(() => document.body.innerText)).slice(0, 400)}`);
    failed = true;
  }

  // THE APP ON PYTHON (docs/DATACUBE_ONE_APP_DESIGN_2026_10_11.md, step 2a): the frame is a page of one grid (R1), the
  // tab's app over Python's services; its Generated Pure & SQL window has the query the page sent and the SQL that
  // answered it, which Python's planner wrote and Python ran (R4: the page sends Pure, never SQL)
  const onApp = await cube.waitForFunction(() => document.querySelector('.dc-page') !== null
    && window.__dataPage?.grids.length === 1
    // where it plans, as Python said it (services.json: R3), in the status bar's one word
    && document.querySelector('.dc-app-status')?.textContent === 'local'
    && /#>\{/.test(document.getElementById('pure')?.textContent ?? '')
    && /\bSELECT\b/i.test(document.getElementById('sql')?.textContent ?? ''), undefined, { timeout: 30_000 }).then(() => true, () => false);
  console.log(`${onApp ? 'ok  ' : 'FAIL'} the frame is a page of one grid on DataCube's app, its Pure & SQL window the query sent and the SQL Python ran`);
  if (!onApp) {
    const seen = await cube.evaluate(() => ({ grids: window.__dataPage?.grids.length, pure: document.getElementById('pure')?.textContent?.slice(0, 200),
      sql: document.getElementById('sql')?.textContent?.slice(0, 200) }));
    console.log(`  the page had: ${JSON.stringify(seen)}`);
    failed = true;
  }

  // FOLLOWING THE FRAME (show's cube.update): Python changes it, and the open page shows the change by itself
  const after = async (command, name, test) => {
    engine.stdin.write(`${command}\n`);
    if ((await nextLine()) !== `done ${command}`) throw new Error(`the engine did not do ${command}`);
    const ok = await cube.waitForFunction(test, undefined, { timeout: 30_000 }).then(() => true, () => false);
    console.log(`${ok ? 'ok  ' : 'FAIL'} ${name}`);
    if (!ok) {
      console.log(`  the page showed: ${(await cube.evaluate(() => document.body.innerText)).slice(0, 400)}`);
      failed = true;
    }
  };
  await after('update', 'a frame updated in Python shows on the open page by itself (the same columns: the view re-run)',
    () => /\b11 rows\b/.test(document.body.innerText));
  await after('columns', 'a frame with a new column opens the cube again over its new model',
    () => document.body.innerText.includes('trader'));

  // A FILE INTO PYTHON'S DUCKDB (docs/DATACUBE_ONE_APP_DESIGN_2026_10_11.md, step 2b): the app's own picker on Python's
  // page, a CSV chosen there; Python reads it into its DuckDB with DataCube's own code (upload.ts, headless), and the
  // grid's rows are Python's over the model that code wrote -- the SQL Python ran, in the Pure & SQL window, reads the
  // file's own table
  await cube.click('.dc-titlebar-menu');
  await cube.locator('.dc-menu .dc-menu-item', { has: cube.locator(':scope > .dc-menu-label:text-is("New Page")') }).click();
  await cube.locator('.dc-blank .dc-primary').click();
  await cube.setInputFiles('.dc-picker-file', {
    name: 'books.csv', mimeType: 'text/csv', buffer: Buffer.from('title,pages\nDune,412\nEmma,474\nUlysses,730\n'),
  });
  const fileRead = await cube.waitForFunction(() => document.querySelector('.dc-picker') === null
    && /\b3 rows\b/.test(document.body.innerText) && document.body.innerText.includes('Ulysses')
    && /"?books"?/.test(document.getElementById('sql')?.textContent ?? ''), undefined, { timeout: 60_000 }).then(() => true, () => false);
  console.log(`${fileRead ? 'ok  ' : 'FAIL'} a file chosen on Python's page is read into Python's DuckDB, its rows Python's`);
  if (!fileRead) {
    console.log(`  the page showed: ${(await cube.evaluate(() => `${document.body.innerText.slice(0, 300)} | sql: ${document.getElementById('sql')?.textContent?.slice(0, 200)}`))}`);
    failed = true;
  }
  // CREATE A CLASS… ON PYTHON'S PAGE (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md B1, B3): the app's own action over Python's
  // services -- the class's sample read where the grid's rows are (PlanAndRun on Python), written by Python's native
  // library, its project made in Python's model home (the same SDLC rules, a git repository in this test's own folder)
  const menuItem = (label) => cube.locator('.dc-menu .dc-menu-item', { has: cube.locator(`:scope > .dc-menu-label:text-is(${JSON.stringify(label)})`) }).first();
  // from the last grid's right-click (its menu button went, 2026-10-23): Create ▸ Class…
  await cube.locator('[data-tile]').filter({ has: cube.locator('.dc-app-grid') }).last().locator('.dc-row .dc-cell').first()
    .click({ button: 'right' });
  await menuItem('Create').hover();
  await menuItem('Class…').click();
  const classShown = await cube.waitForFunction(() => document.querySelectorAll('.dc-class-file').length > 0
    || document.querySelector('.dc-class .dc-picker-status.dc-failed') !== null, undefined, { timeout: 60_000 }).then(() => true, () => false);
  if (classShown) {
    await cube.locator('.dc-class .dc-primary').click();
    await cube.waitForFunction(() => document.querySelector('.dc-class .dc-picker-status.dc-ok, .dc-class .dc-picker-status.dc-failed') !== null,
      undefined, { timeout: 60_000 }).catch(() => undefined);
  }
  const classSaid = classShown ? (await cube.locator('.dc-class .dc-picker-status').textContent()) ?? '' : '(no window)';
  const repository = readdirSync(join(home, 'projects', 'refs', 'heads'), { recursive: true });
  const madeClass = /^Created \S+::domain::Book in the project \S+ \(workspace \S+\), in this user's folder/.test(classSaid)
    && repository.length > 0;
  console.log(`${madeClass ? 'ok  ' : 'FAIL'} Create a Class… on Python's page makes its class in Python's model home, a git repository`);
  if (!madeClass) {
    console.log(`  the window said: ${classSaid}; the repository's branches: ${JSON.stringify(repository)}`);
    failed = true;
  }
  // AND OPENS IN STUDIO, served by the same Python (E): the window's link, its token traded for Studio's cookie on the
  // first load, onto the project's workspace with the class in it
  const studioHref = classShown && (await cube.locator('.dc-class-studio').count()) > 0
    ? await cube.locator('.dc-class-studio').getAttribute('href') : null;
  let inStudio = false;
  if (studioHref !== null && /\/studio\/demo\/index\.html\?token=[^#]+#\/edit\/[^/]+\/[^/]+$/.test(studioHref)) {
    const studio = await browser.newPage();
    studio.on('pageerror', (e) => { console.log(`studio error: ${e.message}`); failed = true; });
    await studio.goto(studioHref);
    inStudio = await studio.waitForFunction(() => /\bBook\b/.test(document.body.innerText) && !location.search.includes('token='),
      undefined, { timeout: 60_000 }).then(() => true, () => false);
    if (!inStudio) console.log(`  Studio showed: ${(await studio.evaluate(() => document.body.innerText.slice(0, 300)))}`);
    await studio.close();
  }
  console.log(`${inStudio ? 'ok  ' : 'FAIL'} the class opens in Studio, served by the same Python, from the window's link`);
  if (!inStudio) {
    console.log(`  the link: ${studioHref}`);
    failed = true;
  }

  // A PROJECT'S QUERY ON PYTHON (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md C5): Studio's demo projects loaded
  // into Python's repository (Studio served by Python), then Query served by Python opens the party project by name
  // and runs a query over it -- its rows the project's own Data rows, in the project's database inside Python's
  // DuckDB, as a tab's Query answers them (site/verify.mjs: 5 parties, Banque Lumière among them)
  const token = served.authorization.replace(/^Bearer /, '');
  const demo = await browser.newPage();
  demo.on('pageerror', (e) => { console.log(`studio error: ${e.message}`); failed = true; });
  await demo.goto(`${origin}/studio/demo/index.html?token=${token}#/`);
  await demo.getByRole('button', { name: 'Load demo projects' }).click({ timeout: 60_000 });
  const published = await demo.waitForFunction(() => /Demo projects published/.test(
    document.querySelector('[data-testid=demo-status]')?.textContent ?? ''), undefined, { timeout: 180_000 }).then(() => true, () => false);
  await demo.close();
  let partyRows = '';
  if (published) {
    const enc = encodeURIComponent;
    const q = await browser.newPage();
    q.on('pageerror', (e) => { console.log(`query error: ${e.message}`); failed = true; });
    await q.goto(`${origin}/query/demo/index.html?token=${token}#/create/manual/${enc('org.finos.lite.demo:party:master-SNAPSHOT')}`
      + `/${enc('demo::party::PartyMapping')}/${enc('demo::party::Runtime')}?class=${enc('demo::party::Party')}`);
    await q.waitForSelector('.q-node', { timeout: 120_000 });
    for (const property of ['Name', 'Country']) await q.dblclick(`.q-node:has-text('${property}')`);
    await q.click('button.q-run');
    await q.waitForFunction(() => document.querySelector('.q-error-box')
      || /\d+ rows? in \d+ ms/.test(document.querySelector('.q-results-bar')?.textContent ?? ''), undefined, { timeout: 120_000 });
    const refused = await q.$('.q-error-box');
    partyRows = refused ? `refused: ${await refused.textContent()}`
      : `${await q.locator('.q-results-bar').textContent()} | ${(await q.$$eval('.q-grid tbody tr', (trs) => trs.map((tr) => tr.textContent ?? ''))).join(' | ')}`;
    if (!refused) {
      // saved into Python's saved queries (ll.home()/queries), for Python's own page below (C5)
      await q.click('button[title="Save (Ctrl+S)"]');
      await q.fill('.q-dialog input.q-input', 'Parties');
      await q.click('.q-dialog button.primary');
      await q.waitForFunction(() => location.hash.startsWith('#/edit/'), undefined, { timeout: 60_000 }).catch(() => undefined);
    }
    await q.close();
  }
  const ranOnPython = /(^|\D)5 rows? in/.test(partyRows) && partyRows.includes('Banque Lumière');
  console.log(`${ranOnPython ? 'ok  ' : 'FAIL'} a project loaded in Studio on Python runs in Query on Python, on its own rows (5 parties)`);
  if (!ranOnPython) {
    console.log(`  published: ${published}; Query showed: ${partyRows.slice(0, 400)}`);
    failed = true;
  }

  // A SAVED QUERY OPENED ON PYTHON'S PAGE (C5): the query Query on Python saved, listed in Saved queries on a
  // DataCube page Python serves, read by Python's compiler and opened as a grid -- planned over its project's mapping
  // and run by Python in the project's own database: the 5 parties Query showed
  let openedSaved = '';
  if (ranOnPython) {
    const dc = await browser.newPage();
    dc.on('pageerror', (e) => { console.log(`page error: ${e.message}`); failed = true; });
    await dc.goto(served.link);
    await dc.locator('.dc-row').first().waitFor({ timeout: 60_000 });
    // on a page of the tab's own (New Page leaves the page Python serves: D1) -- a page Python serves holds what
    // Python holds, its frames, until Python reads more (X1-X5)
    const tiles = 0;
    await dc.click('.dc-titlebar-menu');
    await dc.locator('.dc-menu .dc-menu-item', { has: dc.locator(':scope > .dc-menu-label:text-is("New Page")') }).click();
    await dc.locator('.dc-blank .dc-primary').click();
    await dc.locator('.dc-picker').waitFor({ timeout: 5000 });
    await dc.locator('.dc-picker-tab[data-section="saved"]').click();
    const row = dc.locator('.dc-picker-row[data-query]', { hasText: 'Parties' });
    if (await row.waitFor({ timeout: 30_000 }).then(() => true, () => false)) {
      await row.click();
      const opened = await dc.locator('.dc-picker').waitFor({ state: 'detached', timeout: 60_000 }).then(() => true, async () => {
        openedSaved = `the picker said: ${(await dc.textContent('.dc-picker-status').catch(() => '')) ?? ''}`;
        return false;
      });
      if (opened) {
        const tile = dc.locator('[data-tile^="grid-"]').nth(tiles);
        await tile.locator('.dc-row').first().waitFor({ timeout: 60_000 }).catch(() => undefined);
        const cells = await tile.locator('.dc-row').evaluateAll((rs) => rs.map((r) => r.textContent ?? ''));
        // the page's one grid: its source in its status bar
        openedSaved = `${(await tile.locator('.dc-status-source').textContent().catch(() => '')) ?? ''}: ${cells.length} rows | ${cells.join(' | ')}`;
      }
    } else {
      openedSaved = `Saved queries listed: ${(await dc.$$eval('.dc-picker-row[data-query]', (els) => els.map((e) => e.textContent ?? ''))).join(', ')}`;
    }
    await dc.close();
  }
  const savedOnPython = /Parties/.test(openedSaved) && /: 5 rows/.test(openedSaved) && openedSaved.includes('Banque Lumière');
  console.log(`${savedOnPython ? 'ok  ' : 'FAIL'} a query saved in Query on Python opens from Saved queries on Python's page, its rows the project's (5 parties)`);
  if (!savedOnPython) {
    console.log(`  ${openedSaved.slice(0, 500)}`);
    failed = true;
  }
  if (classShown) {
    await cube.locator('.dc-class .dc-primary').click();
    await cube.locator('.dc-class').waitFor({ state: 'detached', timeout: 10_000 }).catch(() => undefined);
  }
  // the page changed (its model made): the next page asks first whether to leave it, as the tab's does -- answered yes
  cube.once('dialog', (d) => { console.log(`     (asked: ${d.message().slice(0, 160)})`); void d.accept(); });
  // AN EXAMPLE goes the same way: generated in the page, read into Python's DuckDB as a file is (and the file before it
  // let go of: its grid gone, its table dropped in Python -- forget.json, every answer here a success)
  await cube.click('.dc-titlebar-menu');
  await cube.locator('.dc-menu .dc-menu-item', { has: cube.locator(':scope > .dc-menu-label:text-is("New Page")') }).click();
  await cube.locator('.dc-blank .dc-primary').click();
  await cube.locator('.dc-picker-tab[data-section="examples"]').click();
  // the trades example, a CSV (an example whose rows hold nested values, a Variant, is read too; its queries wait on
  // the compatibility plan's spelling of Variant, which Python's execute plans by)
  await cube.click('.dc-picker-card[data-example="trades"]');
  await cube.fill('.dc-picker-rows', '300');
  await cube.click('.dc-picker-choice .dc-primary');
  const exampleRead = await cube.waitForFunction(() => document.querySelector('.dc-picker') === null
    && /\b300 rows\b/.test(document.body.innerText) && /\bSELECT\b/i.test(document.getElementById('sql')?.textContent ?? ''),
  undefined, { timeout: 60_000 }).then(() => true, () => false);
  console.log(`${exampleRead ? 'ok  ' : 'FAIL'} an example opened on Python's page is read into Python's DuckDB as a file is`);
  if (!exampleRead) {
    console.log(`  the page showed: ${(await cube.evaluate(() => document.body.innerText.slice(0, 300)))}`);
    failed = true;
  }
  // NESTED VALUES (a Variant column) on Python's page: the orders example, its customers and items JSON -- planned and
  // run as the tab plans and runs them (PlanAndRun), where Python's legend-engine answer refused a Variant column
  await cube.click('.dc-titlebar-menu');
  await cube.locator('.dc-menu .dc-menu-item', { has: cube.locator(':scope > .dc-menu-label:text-is("New Page")') }).click();
  await cube.locator('.dc-blank .dc-primary').click();
  await cube.locator('.dc-picker-tab[data-section="examples"]').click();
  await cube.click('.dc-picker-card[data-example="orders-json"]');
  await cube.fill('.dc-picker-rows', '40');
  await cube.click('.dc-picker-choice .dc-primary');
  const nestedRead = await cube.waitForFunction(() => document.querySelector('.dc-picker') === null
    && /\b40 rows\b/.test(document.body.innerText), undefined, { timeout: 60_000 }).then(() => true, () => false);
  console.log(`${nestedRead ? 'ok  ' : 'FAIL'} a file of nested values (Variant columns) opens on Python's page, as in the tab`);
  if (!nestedRead) {
    console.log(`  the page showed: ${(await cube.evaluate(() => document.body.innerText.slice(0, 300)))}`);
    failed = true;
  }
  // A REMOTE FILE, read by Python's DuckDB through the httpfs extension the wheel carries: the app's own Remote section,
  // a Parquet file a server on this machine serves (nothing leaves the machine)
  await cube.click('.dc-titlebar-menu');
  await cube.locator('.dc-menu .dc-menu-item', { has: cube.locator(':scope > .dc-menu-label:text-is("New Page")') }).click();
  await cube.locator('.dc-blank .dc-primary').click();
  await cube.locator('.dc-picker-tab[data-section="remote"]').click();
  await cube.fill('.dc-picker-url', served.remote);
  await cube.click('.dc-picker-form .dc-primary');
  const remoteRead = await cube.waitForFunction(() => document.querySelector('.dc-picker') === null
    && /\b3 rows\b/.test(document.body.innerText) && document.body.innerText.includes('Ulysses'),
  undefined, { timeout: 60_000 }).then(() => true, () => false);
  console.log(`${remoteRead ? 'ok  ' : 'FAIL'} a remote file opened on Python's page is read by Python's DuckDB, through the httpfs it carries`);
  if (!remoteRead) {
    console.log(`  the page showed: ${(await cube.evaluate(() => document.body.innerText.slice(0, 300)))}`);
    failed = true;
  }

  // SAVE AND OPEN ON PYTHON (step 2d): the page saved by the app's own Save As…, its record in this user's folder (the
  // store's rules the page's own, Python keeping the records), then opened again from Open… over its source as it is now
  const menuEntry = async (label) => {
    await cube.click('.dc-titlebar-menu');
    await cube.locator(`.dc-menu-item:has(> .dc-menu-label:text-is(${JSON.stringify(label)}))`).first().click();
  };
  await menuEntry('Save As\u2026');
  await cube.locator('.dc-save').waitFor({ timeout: 10_000 });
  await cube.fill('.dc-save-input', 'Books from Python');
  await cube.locator('.dc-save-foot .dc-primary').click();
  const saved = await cube.locator('.dc-save').waitFor({ state: 'detached', timeout: 30_000 }).then(() => true, () => false);
  const records = saved ? readdirSync(join(home, 'pages')).filter((f) => f.endsWith('.json')) : [];
  const record = records.length === 1 ? JSON.parse(readFileSync(join(home, 'pages', records[0]), 'utf8')) : undefined;
  const inFolder = saved && record?.name === 'Books from Python' && record.owner === 'this user' && typeof record.content === 'string';
  console.log(`${inFolder ? 'ok  ' : 'FAIL'} a page saved on Python's page is a record in this user's folder`);
  if (!inFolder) {
    console.log(`  saved: ${saved}, records: ${JSON.stringify(records)}, record: ${JSON.stringify(record)?.slice(0, 300)}`);
    failed = true;
  }
  // a blank page, then the saved one opened from Open…: its grid over its remote file again
  await cube.click('.dc-titlebar-menu');
  await cube.locator('.dc-menu .dc-menu-item', { has: cube.locator(':scope > .dc-menu-label:text-is("New Page")') }).click();
  await cube.locator('.dc-blank').waitFor({ timeout: 10_000 });
  await menuEntry('Open\u2026');
  const row = cube.locator('#cubelib .dc-lib-row', { hasText: 'Books from Python' });
  const listed = await row.waitFor({ timeout: 10_000 }).then(() => true, () => false);
  if (listed) await row.locator('.dc-lib-button', { hasText: 'Open' }).click();
  const reopened = listed && await cube.waitForFunction(() => /\b3 rows\b/.test(document.body.innerText)
    && document.body.innerText.includes('Ulysses') && document.querySelector('.dc-page-name')?.textContent?.includes('Books from Python'),
  undefined, { timeout: 60_000 }).then(() => true, () => false);
  console.log(`${reopened ? 'ok  ' : 'FAIL'} a page saved on Python opens again from Open…, over its source`);
  if (!reopened) {
    console.log(`  listed: ${listed}; the page showed: ${(await cube.evaluate(() => document.body.innerText.slice(0, 300)))}`);
    failed = true;
  }

  // AN ICEBERG TABLE (G5), read by Python's DuckDB through the iceberg extension the wheel carries, every file it names
  // served by the server on this machine: the app's own Remote section, as for a remote file
  await cube.click('.dc-titlebar-menu');
  await cube.locator('.dc-menu .dc-menu-item', { has: cube.locator(':scope > .dc-menu-label:text-is("New Page")') }).click();
  await cube.locator('.dc-blank .dc-primary').click();
  await cube.locator('.dc-picker-tab[data-section="remote"]').click();
  await cube.fill('.dc-picker-url', served.iceberg);
  await cube.click('.dc-picker-form .dc-primary');
  const icebergRead = await cube.waitForFunction(() => document.querySelector('.dc-picker') === null
    && /\b3 rows\b/.test(document.body.innerText) && document.body.innerText.includes('Cygnus'),
  undefined, { timeout: 60_000 }).then(() => true, () => false);
  console.log(`${icebergRead ? 'ok  ' : 'FAIL'} an Iceberg table opened on Python's page is read by Python's DuckDB, through the iceberg it carries`);
  if (!icebergRead) {
    console.log(`  the page showed: ${(await cube.evaluate(() => document.body.innerText.slice(0, 300)))}`);
    failed = true;
  }

  // A WAREHOUSE TABLE ON PYTHON'S PAGE (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md C3), by the app's own Database section:
  // Python signs in as rita, the table opens Live (its rows the warehouse's, its receipt saying so), the grid's plane
  // button snaps it into Python's DuckDB (the button and the receipt saying where the copy is), and goes live again
  await cube.click('.dc-titlebar-menu');
  await cube.locator('.dc-menu .dc-menu-item', { has: cube.locator(':scope > .dc-menu-label:text-is("New Page")') }).click();
  await cube.locator('.dc-blank .dc-primary').click();
  await cube.locator('.dc-picker-tab[data-section="database"]').click();
  await cube.getByLabel('Address').fill(warehouseUrl);
  await cube.getByLabel('User').fill('rita');
  await cube.getByLabel('Password').fill('rita-pw');
  await cube.click('.dc-picker-form .dc-primary');
  await cube.click('.dc-picker-row[data-object="main.POSITIONS"]', { timeout: 60_000 });
  // the grid's state (left of its title, or a lone grid's in its sheet's tab) and the receipt of the grid on screen (an
  // earlier page's are in the document too, hidden)
  const plane = () => cube.evaluate(() => {
    const shown = (e) => e !== null && e.offsetParent !== null;
    const toggle = [...document.querySelectorAll('.dc-tile-state')].find(shown);
    const receipt = [...document.querySelectorAll('.dc-status-receipt')].find(shown);
    return { text: toggle?.textContent?.trim() ?? '', title: toggle?.getAttribute('title') ?? '', receipt: receipt?.textContent ?? '',
      rows: /\b3 rows\b/.test(document.body.innerText), picker: document.querySelector('.dc-picker') !== null };
  });
  const until = async (want) => {
    const t0 = Date.now();
    for (;;) {
      const p = await plane();
      if (want(p) || Date.now() - t0 > 60_000) return p;
      await cube.waitForTimeout(200);
    }
  };
  const live = await until((p) => !p.picker && p.rows && p.text === 'Live' && /the warehouse/.test(p.receipt));
  const liveOk = !live.picker && live.rows && live.text === 'Live' && /the warehouse/.test(live.receipt) && /rita/.test(live.receipt)
    && /snap a copy of your rows into Python's database/.test(live.title);
  console.log(`${liveOk ? 'ok  ' : 'FAIL'} a warehouse table opened on Python's page reads Live on the warehouse, as rita, its Snap offered into Python's database`);
  if (!liveOk) {
    console.log(`  the grid said: ${JSON.stringify(live)}`);
    failed = true;
  }
  // its state's menu: Snap
  const moveTo = async (pick) => {
    await cube.locator('.dc-tile-state:visible').first().click();
    await cube.locator('.dc-menu .dc-menu-item', { has: cube.locator(`:scope > .dc-menu-label:text-is("${pick}")`) }).click();
  };
  await moveTo('Snap');
  const snapped = await until((p) => p.text === 'Snapped' && /Python's copy/.test(p.receipt));
  const snappedOk = snapped.text === 'Snapped' && /Python's copy/.test(snapped.receipt) && /a copy in Python's database/.test(snapped.title)
    && /3 rows/.test(snapped.title) && snapped.rows;
  console.log(`${snappedOk ? 'ok  ' : 'FAIL'} its state's Snap snaps it into Python's DuckDB, its state and the receipt saying where the copy is`);
  if (!snappedOk) {
    console.log(`  the grid said: ${JSON.stringify(snapped)}`);
    failed = true;
  }
  await moveTo('Go Live');
  const back = await until((p) => p.text === 'Live' && /the warehouse/.test(p.receipt));
  const backOk = back.text === 'Live' && /the warehouse/.test(back.receipt) && back.rows;
  console.log(`${backOk ? 'ok  ' : 'FAIL'} Go Live: it reads Live on the warehouse once more, the copy let go`);
  if (!backOk) {
    console.log(`  the grid said: ${JSON.stringify(back)}`);
    failed = true;
  }

  // A FRAME CLOSED IN PYTHON: its open tab keeps what it shows, and says it no longer follows its page -- in its status
  // line and its title (the app's live page, docs/DATACUBE_ONE_APP_DESIGN_2026_10_11.md §5; a frame shown is a page of
  // one grid: P12)
  engine.stdin.write('show-closing\n');
  const closingLine = await nextLine();
  if (!closingLine.startsWith('done show-closing ')) throw new Error(`the engine did not show the frame: ${closingLine}`);
  const closingTab = await browser.newPage();
  closingTab.on('pageerror', (e) => { console.log(`closing tab error: ${e.message}`); failed = true; });
  await closingTab.goto(JSON.parse(closingLine.slice('done show-closing '.length)));
  const closingShown = await closingTab.waitForFunction(() => /\b2 rows\b/.test(document.body.innerText), undefined,
    { timeout: 60_000 }).then(() => true, () => false);
  engine.stdin.write('close-closing\n');
  if ((await nextLine()) !== 'done close-closing') throw new Error('the engine did not close the frame');
  const closedSaid = closingShown && await closingTab.waitForFunction(
    () => /not following the page: the page was closed/.test(document.querySelector('.dc-app-status')?.textContent ?? '')
      && /not followed: the page was closed/.test(document.title) && /\b2 rows\b/.test(document.body.innerText),
    undefined, { timeout: 30_000 }).then(() => true, () => false);
  console.log(`${closedSaid ? 'ok  ' : 'FAIL'} a frame closed in Python: its tab keeps its rows and says it no longer follows its page`);
  if (!closedSaid) {
    console.log(`  the tab said: ${await closingTab.evaluate(() => `${document.title} | ${document.querySelector('.dc-app-status')?.textContent}`)}`);
    failed = true;
  }
  await closingTab.close();

  // A PAGE FROM PYTHON (docs/DATACUBE_PYTHON_PAGES_DESIGN_2026_10_09.md, step 1): a page of the engine's two frames
  // on two sheets, written by DataCube's own writers (the test page's __pythonPageDocument), served by Python as `q3`,
  // and opened as a person opens it (demo/engine.html?page=q3); then a frame updated, and the page changed, in Python
  const doc = await page.evaluate(() => window.__pythonPageDocument());
  engine.stdin.write(`page ${JSON.stringify(doc)}\n`);
  if ((await nextLine()) !== 'done page') throw new Error('the engine did not serve the page');
  const tabbed = await browser.newPage();
  tabbed.on('pageerror', (e) => { console.log(`page tab error: ${e.message}`); failed = true; });
  tabbed.on('console', (m) => { if (m.type() === 'error') console.log(`page tab console: ${m.text()}`); });
  tabbed.on('response', (r) => {
    // the one refusal that is not a failure: the page's report of a version Python replaced while it was on its way
    // (engine.py, _page_said), the page then opening the new one
    if (r.status() === 409 && r.request().method() === 'POST' && new URL(r.url()).pathname === '/page.json') return;
    if (r.status() >= 400) { console.log(`FAIL page tab answer ${r.status()}: ${r.url()}`); failed = true; }
  });
  await tabbed.goto(`${origin}/engine.html?page=q3${new URL(served.link).hash}`);
  const sheetNames = () => [...document.querySelectorAll('.dc-sheet-tab .dc-sheet-label')].map((t) => t.textContent).join('|');
  const seen = async (name, test, arg) => {
    const ok = await tabbed.waitForFunction(test, arg, { timeout: 60_000 }).then(() => true, () => false);
    console.log(`${ok ? 'ok  ' : 'FAIL'} ${name}`);
    if (!ok) {
      console.log(`  the page showed: ${(await tabbed.evaluate(() => document.body.innerText)).slice(0, 400)}`);
      failed = true;
    }
  };
  await seen('a page from Python opens in a tab: its sheets, the first one\'s grid over its frame and the chart beside it',
    `(${String(sheetNames)})() === 'trades|Desks' && document.querySelector('[data-tile="grid-1"]')?.innerText.includes('EMEA')
      && document.querySelector('[data-tile="chart-1"]') !== null`);
  await tabbed.locator('.dc-sheet-tab', { hasText: 'Desks' }).click();
  await seen('its second sheet shows the grid over the second frame',
    () => document.querySelector('[data-tile="grid-2"]')?.innerText.includes('Credit') === true);
  const python = async (command) => {
    engine.stdin.write(`${command}\n`);
    if ((await nextLine()) !== `done ${command}`) throw new Error(`the engine did not do ${command}`);
  };
  await python('desks-update');
  await seen('a frame updated in Python shows on the open page by itself',
    () => document.querySelector('[data-tile="grid-2"]')?.innerText.includes('Equity') === true);
  await python('page-again');
  await seen('the page changed in Python opens again, on the sheet it showed',
    `(${String(sheetNames)})() === 'trades|Desks (v2)' && document.querySelector('[data-tile="grid-2"]')?.offsetParent !== null`);

  // A PAGE BUILT WITH ll.Page (step 2): built and shown in Python as a person does; its link is the one show() gives.
  // Then a sheet added and the page renamed in Python (the open page follows by their commands), and a sheet renamed in
  // DataCube that is Python's page at once (docs/DATACUBE_PAGE_KEEPER_2026_10_20.md: one copy of the page, which the open
  // page writes against the version it is at)
  engine.stdin.write('py-page\n');
  const built = await nextLine();
  if (!built.startsWith('done py-page ')) throw new Error(`the engine did not build the page: ${built}`);
  await tabbed.goto(JSON.parse(built.slice('done py-page '.length)));
  await seen('a page built with ll.Page opens in a tab: its sheets, its grid over its frame and the chart beside it',
    `(${String(sheetNames)})() === 'pytrades|Desks' && document.querySelector('[data-tile="grid-1"]')?.innerText.includes('EMEA')
      && document.querySelector('[data-tile="chart-1"]') !== null`);
  // (docs/DATACUBE_LIVE_COMMANDS_2026_10_18.md) followed by the commands Python made: the page is not opened again, so
  // a mark on its grid's tile stays
  await tabbed.evaluate(() => { document.querySelector('[data-tile="grid-1"]').dataset.kept = 'yes'; });
  await python('py-page-sheet');
  await seen('a sheet added to it in Python shows on the open page', `(${String(sheetNames)})() === 'pytrades|Desks|Added in Python'`);
  await seen('followed by its command: the page not opened again (its grid\'s tile the same)',
    () => document.querySelector('[data-tile="grid-1"]')?.dataset.kept === 'yes');
  // (the phase 3 audit's 1 and 6) renamed in Python: the open page takes the name, by its command, and is not changed
  // since it was opened -- Python's change is the page as Python has it
  await python('py-page-rename');
  await seen('a page renamed in Python takes the name on the open page, not marked changed, not opened again',
    () => document.querySelector('.dc-page-name')?.textContent === 'Renamed in Python' && !document.title.startsWith('\u2022')
      && document.querySelector('[data-tile="grid-1"]')?.dataset.kept === 'yes');
  await python('py-page-name-back');
  await seen('and named back', () => document.querySelector('.dc-page-name')?.textContent === 'Built in Python');
  await tabbed.locator('.dc-sheet-tab', { hasText: 'Desks' }).dblclick();
  await tabbed.locator('.dc-sheet-tab input').fill('Renamed in DataCube');
  await tabbed.locator('.dc-sheet-tab input').press('Enter');
  const read = await (async () => {
    // the page reports its document once a change settles (half a second): asked until it has, or ten seconds
    for (let tries = 0; tries < 20; tries += 1) {
      await new Promise((done) => { setTimeout(done, 500); });
      engine.stdin.write('py-page-read\n');
      const line = await nextLine();
      if (!line.startsWith('done py-page-read ')) throw new Error(`the engine did not read the page: ${line}`);
      const names = JSON.parse(line.slice('done py-page-read '.length));
      if (names.includes('Renamed in DataCube')) return names;
    }
    return undefined;
  })();
  console.log(`${read ? 'ok  ' : 'FAIL'} a sheet renamed in DataCube is on the page in Python${read ? ` (${read.join(', ')})` : ''}`);
  if (!read) failed = true;
  // Python's change on top of the open page's write is its command, applied there: the page not opened again, the
  // sheet renamed there kept, and the page still changed since it opened (by that rename, made there)
  await python('py-page-group');
  await seen('a change in Python after the open page wrote the page is applied by its command, what was changed there kept',
    `(${String(sheetNames)})() === 'pytrades|Renamed in DataCube|Added in Python'
      && document.querySelector('[data-tile="grid-1"] .dc-zone-rows .dc-chip[data-column="desk"]') !== null
      && document.querySelector('[data-tile="grid-1"]')?.dataset.kept === 'yes' && document.title.startsWith('\u2022')`);
  // then another grid edit in Python, the same way
  await python('py-page-regroup');
  await seen('a grid grouped in Python is grouped on the open page by its grid command, the page not opened again',
    () => document.querySelector('[data-tile="grid-1"] .dc-zone-rows .dc-chip[data-column="region"]') !== null
      && document.querySelector('[data-tile="grid-1"] .dc-zone-rows .dc-chip[data-column="desk"]') === null
      && document.querySelector('[data-tile="grid-1"]')?.dataset.kept === 'yes');

  // A FRAME CHANGED IN PLACE (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md P11): the desks grid's Live frame a column more,
  // which nothing in Python looks at; the tab opened again reads the frame as it is now, and tells the page in Python
  // so by its command (rebuilt) -- before, the cube it opened was its own, and Python's page kept the old one
  await python('py-page-desks-wider');
  await tabbed.reload();
  await seen('the page opened again after its desks frame changed in place',
    `(${String(sheetNames)})() === 'pytrades|Renamed in DataCube|Added in Python'`);
  const widened = await (async () => {
    // the page writes its command once it settles (half a second): asked until it has, or ten seconds
    for (let tries = 0; tries < 20; tries += 1) {
      await new Promise((done) => { setTimeout(done, 500); });
      engine.stdin.write('py-page-desks-columns\n');
      const line = await nextLine();
      if (!line.startsWith('done py-page-desks-columns ')) throw new Error(`the engine did not read the page: ${line}`);
      const names = JSON.parse(line.slice('done py-page-desks-columns '.length));
      if (names.includes('floor')) return names;
    }
    return undefined;
  })();
  console.log(`${widened ? 'ok  ' : 'FAIL'} the cube the open page opened again over the changed frame is the page's in Python`
    + `${widened ? ` (${widened.join(', ')})` : ''}`);
  if (!widened) failed = true;
  // opened again with nothing changed since: no cube is another, so the page writes nothing (the S1 audit's 14: never a
  // cube told back and forth between the tab and Python)
  const versionOf = async () => {
    engine.stdin.write('py-page-version\n');
    const line = await nextLine();
    if (!line.startsWith('done py-page-version ')) throw new Error(`the engine did not say the version: ${line}`);
    return JSON.parse(line.slice('done py-page-version '.length));
  };
  const steady = await versionOf();
  await tabbed.reload();
  await seen('the page opened again, nothing changed since',
    `(${String(sheetNames)})() === 'pytrades|Renamed in DataCube|Added in Python'`);
  await new Promise((done) => { setTimeout(done, 2000); });
  const versionNow = await versionOf();
  console.log(`${versionNow === steady ? 'ok  ' : 'FAIL'} opened again with nothing changed, the page writes nothing (version ${steady} -> ${versionNow})`);
  if (versionNow !== steady) failed = true;

  // SAVED FROM PYTHON (D, B6): page.save() keeps the page in this user's saved pages by the rules the tab's own Save
  // keeps it by; DataCube's Open… lists it, and it opens -- its sheets as Python saved them, its grid over its frame
  engine.stdin.write('py-page-save\n');
  const savedLine = await nextLine();
  if (!savedLine.startsWith('done py-page-save ')) throw new Error(`the engine did not save the page: ${savedLine}`);
  // opened in the first tab, which shows another page (the one saved there, "Books from Python")
  await cube.click('.dc-titlebar-menu');
  await cube.locator('.dc-menu-item:has(> .dc-menu-label:text-is("Open\u2026"))').first().click();
  const pyRow = cube.locator('#cubelib .dc-lib-row', { hasText: 'Built in Python' });
  const pyListed = await pyRow.waitFor({ timeout: 10_000 }).then(() => true, () => false);
  if (pyListed) await pyRow.locator('.dc-lib-button', { hasText: 'Open' }).click();
  const pyOpened = pyListed && await cube.waitForFunction(`(${String(sheetNames)})() === 'pytrades|Renamed in DataCube|Added in Python'
    && document.querySelector('.dc-page-name')?.textContent?.includes('Built in Python')`, undefined, { timeout: 60_000 })
    .then(() => true, () => false);
  console.log(`${pyOpened ? 'ok  ' : 'FAIL'} a page saved from Python (page.save()) is in Open… and opens, its sheets as Python saved them`);
  if (!pyOpened) {
    console.log(`  listed: ${pyListed}; sheets: ${await cube.evaluate(`(${String(sheetNames)})()`)}; the page showed: ${
      (await cube.evaluate(() => document.body.innerText.slice(0, 400)))}`);
    failed = true;
  }

  // A JOIN OF ITS FRAMES (page.join: docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §18.3), made in Python: the open page opens
  // it through Python (opened.json), whose compiler and frames it reads -- it has neither of its own here -- and reads its
  // rows there, as a frame's grid's (the audit of §18, 2026-10-20: it was left out, and taking the open page in then dropped it)
  await tabbed.locator('.dc-sheet-tab', { hasText: 'pytrades' }).click();
  await python('py-page-join');
  await seen('a join made in Python (page.join) opens on the open page, its rows from both of its frames',
    () => [...document.querySelectorAll('[data-tile^="grid"]')].some((t) => t.querySelector('.dc-th[data-column="head"]') !== null
      && t.querySelector('.dc-th[data-column="d_desk"]') !== null && t.innerText.includes('Ana')));

  // SNAP AS PAGE STATE (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §18.8): a grid snapped in Python reads Python's copy on
  // the open page, by the page's snap command (not opened again), whatever its frame does; gone live in Python it reads
  // the frame again; snapped on the page, Python's page has the copy; and ll.show(df, snap=True) opens snapped
  const pill = () => document.querySelector('[data-tile="grid-1"] .dc-tile-state')?.textContent;
  const joinTile = () => [...document.querySelectorAll('[data-tile^="grid"]')].find((t) => t.querySelector('.dc-th[data-column="d_desk"]'));
  await tabbed.evaluate(() => { document.querySelector('[data-tile="grid-1"]').dataset.kept = 'yes'; });
  await python('py-page-snap');
  await seen('a grid snapped in Python (grid.snap()) reads Python\'s copy on the open page, by its command',
    `(${String(pill)})() === 'Snapped' && document.querySelector('[data-tile="grid-1"]')?.dataset.kept === 'yes'`);
  await python('py-page-more');
  await seen('its frame grows in Python: the join over it (live) reads the new row, the snapped grid its copy',
    `(${String(joinTile)})()?.innerText.includes('11 rows') === true
      && !document.querySelector('[data-tile="grid-1"]').innerText.includes('LATAM')`);
  await python('py-page-live');
  await seen('gone live in Python (grid.live()): the open page reads the frame again',
    `(${String(pill)})() === 'Live' && document.querySelector('[data-tile="grid-1"]').innerText.includes('LATAM')`);
  await tabbed.locator('[data-tile="grid-1"] .dc-tile-state').click();
  await tabbed.locator('.dc-menu .dc-menu-item', { has: tabbed.locator(':scope > .dc-menu-label:text-is("Snap")') }).click();
  await seen('snapped on the open page', `(${String(pill)})() === 'Snapped'`);
  const copies = await (async () => {
    // the page reports its document once a change settles: asked until it has, or ten seconds
    for (let tries = 0; tries < 20; tries += 1) {
      await new Promise((done) => { setTimeout(done, 500); });
      engine.stdin.write('py-page-copies\n');
      const line = await nextLine();
      if (!line.startsWith('done py-page-copies ')) throw new Error(`the engine did not read the page: ${line}`);
      const said = JSON.parse(line.slice('done py-page-copies '.length));
      if (said['grid-1']?.table?.startsWith('snap')) return said;
    }
    return undefined;
  })();
  console.log(`${copies ? 'ok  ' : 'FAIL'} a grid snapped on the open page is snapped on the page in Python`
    + `${copies ? ` (${copies['grid-1'].label}, ${copies['grid-1'].rowCount} rows)` : ''}`);
  if (!copies) failed = true;
  await python('py-page-live');
  await seen('and gone live in Python after: the open page live again', `(${String(pill)})() === 'Live'`);

  // TWO TABS ON ONE PAGE (docs/DATACUBE_LIVE_EDITING_2026_10_24.md W15: the two-editor soak, live-soak.test.ts, run in
  // browsers once): a second tab opens Python's page; its first sheet renamed in the first tab and a sheet added in the
  // second at once, Python grouping its grid again between them; then Undo in the first tab, through the page's menu.
  // Each tab writes its edits as commands, converges where its write met another's, and follows the rest: both hold
  // Python's page -- the same sheets, the hash its keeper has (page-hash.ts) -- and neither says the message that must
  // never show (live-channel.ts HASH_MISMATCH) or opened the page again.
  const pythonHash = async () => {
    engine.stdin.write('py-page-hash\n');
    const line = await nextLine();
    if (!line.startsWith('done py-page-hash ')) throw new Error(`the engine did not say the hash: ${line}`);
    return JSON.parse(line.slice('done py-page-hash '.length));
  };
  const pythonSheets = async () => {
    engine.stdin.write('py-page-read\n');
    const line = await nextLine();
    if (!line.startsWith('done py-page-read ')) throw new Error(`the engine did not read the page: ${line}`);
    return JSON.parse(line.slice('done py-page-read '.length));
  };
  const secondTab = await browser.newPage();
  secondTab.on('pageerror', (e) => { console.log(`second tab error: ${e.message}`); failed = true; });
  secondTab.on('console', (m) => { if (m.type() === 'error') console.log(`second tab console: ${m.text()}`); });
  secondTab.on('response', (r) => {
    if (r.status() === 409 && r.request().method() === 'POST' && new URL(r.url()).pathname === '/page.json') return;
    if (r.status() >= 400) { console.log(`FAIL second tab answer ${r.status()}: ${r.url()}`); failed = true; }
  });
  const bothTabs = [tabbed, secondTab];
  // the first sheet as its tab labels it (unnamed in Python: the tab shows its grid's frame)
  const firstSheet = await tabbed.evaluate(() => document.querySelector('.dc-sheet-tab .dc-sheet-label')?.textContent);
  await secondTab.goto(tabbed.url());
  const tabsShow = (fn) => Promise.all(bothTabs.map((t) => t.evaluate(fn)));
  const sheetsOf = () => tabsShow(`(${String(sheetNames)})()`);
  const hashesOf = () => tabsShow(() => window.__dataPageHash);
  const neverShown = () => tabsShow(() => /differed from its keeper|opened again as its keeper has it/.test(document.body.innerText));
  /** Both tabs and Python at one page: asked until they are, or `tries` half-seconds; what each held last. */
  const oneHeld = async (name, also = () => true, tries = 40) => {
    let last = {};
    for (let n = 0; n < tries; n += 1) {
      const [sheets, hashes, shown, hash] = await Promise.all([sheetsOf(), hashesOf(), neverShown(), pythonHash()]);
      last = { sheets, hashes, shown, hash };
      if (shown.some(Boolean)) break;
      if (hashes.every((h) => h === hash) && sheets[0] === sheets[1] && also(sheets[0], await pythonSheets())) {
        console.log(`ok   ${name} (${sheets[0]})`);
        return true;
      }
      await new Promise((done) => { setTimeout(done, 500); });
    }
    console.log(`FAIL ${name}: tabs ${JSON.stringify(last.sheets)} ${JSON.stringify(last.hashes)}, Python ${last.hash}${
      last.shown?.some(Boolean) ? ', a tab said the message that must never show' : ''}`);
    for (const t of bothTabs) console.log(`  a tab showed: ${(await t.evaluate(() => document.body.innerText)).slice(0, 300)}`);
    failed = true;
    return false;
  };
  await oneHeld('a second tab opens Python\'s page: both tabs hold it, to its keeper\'s hash');
  // the first tab renames the first sheet while the second adds a sheet: two writes at once, one made again on top of
  // the other; Python's grouping between them
  await tabbed.locator('.dc-sheet-tab', { hasText: firstSheet }).dblclick();
  await tabbed.locator('.dc-sheet-tab input').fill('Two tabs');
  const typed = tabbed.locator('.dc-sheet-tab input').press('Enter');
  await secondTab.locator('.dc-sheet-add').click();
  // the + offers a Sheet and a Flow (2026-10-26)
  await secondTab.locator('.dc-menu .dc-menu-item', { has: secondTab.locator(':scope > .dc-menu-label:text-is("Sheet")') }).click();
  await typed;
  await python('py-page-group');
  await oneHeld('a sheet renamed in one tab, a sheet added in the other and Python\'s grouping between: one page in all three',
    (sheets, inPython) => sheets.startsWith('Two tabs|') && inPython[0] === 'Two tabs' && inPython.length === sheets.split('|').length);
  // Undo in the first tab (its menu): its rename undone as the commands between, written -- the second tab and Python
  // follow by them
  await tabbed.click('.dc-page-bar .dc-titlebar-menu');
  await tabbed.locator('.dc-menu .dc-menu-item', { has: tabbed.locator(':scope > .dc-menu-label', { hasText: /^Undo / }) }).first().click();
  await oneHeld('Undo in the first tab: the rename undone on the page in all three',
    (sheets, inPython) => sheets.startsWith(`${firstSheet}|`) && inPython[0] !== 'Two tabs');
  // A GRID GESTURE AGAINST PYTHON'S STEP ON THE SAME GRID (W13: that grid's history starts again; the converge puts it
  // back at the keeper's cube and replays the tab's edit on top): the second tab sorts the first grid while Python
  // regroups it; the tab's write (500 ms after the gesture, live-channel.ts WRITE_AFTER) lands on top of Python's and
  // combines; both tabs converge on Python's page, the grid grouped as Python left it and sorted as the tab did
  const gridOf = (t) => t.evaluate(() => ({ rows: window.__dataCube.snapshot.rows, sorts: window.__dataCube.snapshot.sorts.map((x) => x.column) }));
  await secondTab.evaluate(() => window.__dataCube.edit([{ kind: 'sorts', sorts: [{ column: 'desk', direction: 'asc' }] }], 'Sort'));
  await python('py-page-regroup');
  await oneHeld("the second tab's sort and Python's regroup of the same grid at once: one page in all three", () => true);
  const gridsNow = await Promise.all(bothTabs.map(gridOf));
  const sortedRegrouped = gridsNow.every((g) => g.rows.join() === 'region' && g.sorts.join() === 'desk');
  console.log(`${sortedRegrouped ? 'ok  ' : 'FAIL'} the grid in both tabs: Python's grouping (region) and the second tab's sort (desk) ${JSON.stringify(gridsNow)}`);
  if (!sortedRegrouped) failed = true;
  // A WRITE REFUSED IN A BROWSER (W8), made deterministic (2026-10-24: two clicks "at once" did not race on Windows'
  // runner, and no write was refused): the second tab is kept from learning of moves (its version asks held), the first
  // takes the chart off and its write lands, then the second -- still showing the chart -- takes it off too. Its write,
  // at the version before, is refused (the tile is gone), and it converges on what moved the page since, the refusal
  // said: no reopen, the chart gone in all three, and no tab left saying it is catching up
  let refusals = 0;
  secondTab.on('response', (r) => { if (r.status() === 409 && r.request().method() === 'POST' && new URL(r.url()).pathname === '/page.json') refusals += 1; });
  let letAsk;
  const askHeld = new Promise((go) => { letAsk = go; });
  // (a held ask let go as the route is taken off may be handled already: either way it goes on)
  await secondTab.route('**/version.json*', async (route) => { await askHeld; await route.continue().catch(() => {}); });
  // (the chart is on the first sheet; its Remove clicked as its handler is, whatever covers it: a tile's buttons are
  // quiet until pointed at, app.css .dc-tile:hover)
  const removeChart = async (t) => {
    await t.locator('.dc-sheet-tab', { hasText: firstSheet }).click();
    await t.locator('[data-tile="chart-1"] .dc-tile-remove').dispatchEvent('click');
  };
  const before = await pythonHash();
  await removeChart(tabbed);
  for (let n = 0; n < 40 && (await pythonHash()) === before; n += 1) await new Promise((done) => { setTimeout(done, 250); });
  const stillShown = await secondTab.evaluate(() => document.querySelector('[data-tile="chart-1"]') !== null);
  await removeChart(secondTab);
  for (let n = 0; n < 40 && refusals === 0; n += 1) await new Promise((done) => { setTimeout(done, 250); });
  letAsk();
  await new Promise((done) => { setTimeout(done, 100); });
  await secondTab.unroute('**/version.json*');
  await oneHeld("the chart taken off in one tab, then in the other, which had not followed: its write refused, both at Python's page", () => true);
  const chartGone = (await tabsShow(() => document.querySelector('[data-tile="chart-1"]') === null)).every(Boolean);
  const refusedOk = stillShown && refusals >= 1 && chartGone;
  console.log(`${refusedOk ? 'ok  ' : 'FAIL'} the second tab's removal refused (${refusals} refused; the chart still shown there when clicked: ${stillShown}), the chart gone in both tabs`);
  if (!refusedOk) failed = true;
  const catchingUp = (await tabsShow(() => /catching up with its keeper/.test(document.body.innerText))).some(Boolean);
  console.log(`${catchingUp ? 'FAIL' : 'ok  '} neither tab is left saying it is catching up with its keeper`);
  if (catchingUp) failed = true;
  await secondTab.close();

  // D1 (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md P10): New Page on the page Python serves puts a page of the
  // tab's own on screen and leaves Python's page as it is -- nothing written to it, and Python's next change to it not
  // followed here
  const servedVersion = async () => {
    engine.stdin.write('py-page-version\n');
    const line = await nextLine();
    if (!line.startsWith('done py-page-version ')) throw new Error(`the engine did not say the version: ${line}`);
    return JSON.parse(line.slice('done py-page-version '.length));
  };
  const sheetsInPython = async () => {
    engine.stdin.write('py-page-read\n');
    const line = await nextLine();
    if (!line.startsWith('done py-page-read ')) throw new Error(`the engine did not read the page: ${line}`);
    return JSON.parse(line.slice('done py-page-read '.length));
  };
  const leftAt = await servedVersion();
  const leftSheets = await sheetsInPython();
  // the tab follows Python's page by asking its versions (version.json, about once a second: demo/engine.ts ASK_EVERY)
  // while it is that page: counted, so leaving it is seen as the asking stopping -- what following needs -- not inferred
  // from nothing having changed yet
  let asked = 0;
  tabbed.on('request', (r) => { if (r.url().includes('/version.json')) asked += 1; });
  for (let i = 0; i < 50 && asked === 0; i += 1) await tabbed.waitForTimeout(100);
  const asking = asked > 0;
  tabbed.on('dialog', (d) => { void d.accept(); });
  await tabbed.click('.dc-page-bar .dc-titlebar-menu');
  await tabbed.locator('.dc-menu .dc-menu-item', { has: tabbed.locator(':scope > .dc-menu-label:text-is("New Page")') }).click();
  await seen('New Page on the page Python serves: a page of the tab\'s own, said',
    () => document.querySelector('[data-tile="grid-1"]') === null && /a page of this tab's own/.test(document.body.innerText));
  const askedThen = asked;
  await python('py-page-sheet');
  // two of its asking periods: a tab still following would ask in each
  await tabbed.waitForTimeout(2_500);
  const leftNow = await servedVersion();
  const sheetsNow = await sheetsInPython();
  const leftAlone = asking && asked === askedThen && leftNow === leftAt + 1
    && JSON.stringify(sheetsNow) === JSON.stringify([...leftSheets, 'Added in Python'])
    && (await tabbed.evaluate(() => document.querySelector('[data-tile="grid-1"]') === null
      && ![...document.querySelectorAll('.dc-sheet-tab .dc-sheet-label')].some((t) => t.textContent === 'Added in Python')));
  console.log(`${leftAlone ? 'ok  ' : 'FAIL'} Python's page left as it was: moved by Python's change alone (${leftAt} -> ${leftNow}), `
    + `its versions asked no more (${askedThen} -> ${asked}, asking before: ${String(asking)}), not followed here`);
  if (!leftAlone) failed = true;
  engine.stdin.write('show-snapped\n');
  const snappedLine = await nextLine();
  if (!snappedLine.startsWith('done show-snapped ')) throw new Error(`the engine did not show the frame: ${snappedLine}`);
  const snappedTab = await browser.newPage();
  snappedTab.on('pageerror', (e) => { console.log(`snapped tab error: ${e.message}`); failed = true; });
  await snappedTab.goto(JSON.parse(snappedLine.slice('done show-snapped '.length)));
  const tabSnapped = await snappedTab.waitForFunction(() => document.querySelector('.dc-tile-state')?.textContent === 'Snapped',
    undefined, { timeout: 60_000 }).then(() => true, () => false);
  console.log(`${tabSnapped ? 'ok  ' : 'FAIL'} ll.show(df, snap=True) opens the frame snapped`);
  if (!tabSnapped) {
    console.log(`  the tab showed: ${(await snappedTab.evaluate(() => document.body.innerText)).slice(0, 400)}`);
    failed = true;
  }
  // the tab gone (navigated away): the copy stays -- Python's page of one grid reads it (P12), and a copy a page the
  // keeper keeps reads is the keeper's (Z17): the tab's release of it is answered "kept"
  const held = async () => {
    engine.stdin.write('copies-held\n');
    const line = await nextLine();
    if (!line.startsWith('done copies-held ')) throw new Error(`the engine did not say its copies: ${line}`);
    return JSON.parse(line.slice('done copies-held '.length));
  };
  const heldBefore = await held();
  await snappedTab.goto('about:blank');
  // its release, sent as it goes (keepalive), given the time a let-go copy took to go before (P12): held throughout
  let heldAfter = heldBefore;
  for (let tries = 0; tries < 6 && JSON.stringify(heldAfter) === JSON.stringify(heldBefore); tries += 1) {
    await new Promise((done) => { setTimeout(done, 500); });
    heldAfter = await held();
  }
  const stayed = heldBefore.length > 0 && JSON.stringify(heldAfter) === JSON.stringify(heldBefore);
  console.log(`${stayed ? 'ok  ' : 'FAIL'} a tab gone keeps the copy Python's page reads (${heldBefore.join(', ')} -> ${heldAfter.join(', ')})`);
  if (!stayed) failed = true;
  await snappedTab.close();

  // EVERY PAGE PYTHON WRITES IS ONE DATACUBE READS: pages built with each kind of edit Python makes, one with a frozen
  // chart kept without its grid, and DataCube's own page loaded and changed in Python, read by DataCube's reader
  engine.stdin.write('py-documents\n');
  const written = await nextLine();
  if (!written.startsWith('done py-documents ')) throw new Error(`the engine did not write the pages: ${written}`);
  const texts = JSON.parse(written.slice('done py-documents '.length));
  const refused = await page.evaluate((all) => all.map((text) => window.__readPythonPage(text)), texts);
  const readAll = texts.length === 3 && refused.every((why) => why === null);
  console.log(`${readAll ? 'ok  ' : 'FAIL'} every page Python writes DataCube's reader reads (${texts.length} pages)`);
  if (!readAll) {
    refused.forEach((why, i) => { if (why !== null) console.log(`  page ${i + 1}: ${why}`); });
    failed = true;
  }

  // A NOTEBOOK'S CUBES (legend_lite.notebook.DataCube): two widgets on one page, each its script loaded as anywidget
  // loads one (its text as a module), their messages carried to Python's widgets and back as a notebook's channel
  // carries them. DataCube's module comes over the channel once for the page; no call goes over HTTP.
  const widgets = await browser.newPage();
  widgets.on('pageerror', (e) => { console.log(`widget page error: ${e.message}\n${(e.stack ?? '').split('\n').slice(0, 6).join('\n')}`); failed = true; });
  widgets.on('console', (m) => { if (m.type() === 'error') console.log(`widget page console: ${m.text()}`); });
  const http = [];
  widgets.on('request', (r) => { if (!r.url().endsWith('/widgets.html')) http.push(r.url()); });
  const fetched = new Map();
  await widgets.exposeFunction('__widgetSend', (name, message) => {
    const { path } = JSON.parse(message);
    fetched.set(path, (fetched.get(path) ?? 0) + 1);
    engine.stdin.write(`widget ${name} ${message}\n`);
  });
  // what Python's widgets send goes to every page with a view of that widget, as a notebook's channel brings it to
  // every view of a model
  const widgetPages = [widgets];
  toWidgets = (line) => {
    const space = line.indexOf(' ');
    for (const page of widgetPages) {
      page.evaluate(([kind, json]) => {
        const m = JSON.parse(json);
        const model = window.__models?.[m.widget];
        if (model === undefined) return;
        if (kind === 'trait') model.set(m.name, m.value);
        else {
          const views = m.buffers.map((b) => new DataView(Uint8Array.from(atob(b), (c) => c.charCodeAt(0)).buffer));
          model.fire('msg:custom', m.content, views);
        }
      }, [line.slice(0, space), line.slice(space + 1)]).catch((e) => {
        if (closing) return;
        console.log(`FAIL a widget message: ${e}`);
        failed = true;
      });
    }
  };
  await widgets.route(`${origin}/widgets.html`, (r) => r.fulfill({
    contentType: 'text/html',
    body: '<!doctype html><meta charset="utf-8"><title>notebook cubes</title><div id="nb" style="width:1000px"></div>'
      + '<div id="nb2" style="width:1000px"></div><div id="nbp" style="width:1000px"></div>',
  }));
  await widgets.goto(`${origin}/widgets.html`);
  await widgets.evaluate(async (given) => {
    window.__models = {};
    window.__heardAbove = [];
    document.addEventListener('keydown', (e) => window.__heardAbove.push(e.key));
    for (const [name, { esm, state }] of Object.entries(given)) {
      // the part of anywidget's model a widget uses, as anywidget's front end gives it
      const listeners = new Map();
      const values = { ...state };
      const model = {
        get: (key) => values[key],
        on: (event, f) => { if (!listeners.has(event)) listeners.set(event, new Set()); listeners.get(event).add(f); },
        off: (event, f) => { listeners.get(event)?.delete(f); },
        send: (content) => { window.__widgetSend(name, JSON.stringify(content)); },
        fire: (event, ...args) => { for (const f of listeners.get(event) ?? []) f(...args); },
        set: (key, value) => { values[key] = value; model.fire(`change:${key}`); },
      };
      window.__models[name] = model;
      // as anywidget loads a widget's script: its text as a module (a blob URL), imported
      const url = URL.createObjectURL(new Blob([esm], { type: 'text/javascript' }));
      const widget = (await import(url)).default;
      URL.revokeObjectURL(url);
      widget.render({ model, el: document.getElementById(name) });
    }
  }, served.widgets);
  const check = async (name, test, arg) => {
    const ok = await widgets.waitForFunction(test, arg, { timeout: 60_000 }).then(() => true, () => false);
    console.log(`${ok ? 'ok  ' : 'FAIL'} ${name}`);
    if (!ok) {
      console.log(`  the page showed: ${(await widgets.evaluate(() => document.body.innerText)).slice(0, 400)}`);
      failed = true;
    }
  };
  await check('two notebook cubes show their frames\' rows, over the widget\'s channel',
    () => /\b10 rows\b/.test(document.getElementById('nb').innerText) && document.getElementById('nb').innerText.includes('APAC')
      && /\b3 rows\b/.test(document.getElementById('nb2').innerText));
  // the app's windows in a notebook: its Generated Pure & SQL window, in the widget's own element, has the SQL that
  // answered the grid -- Python's planner's, run by Python
  await check('a notebook cube\'s Pure & SQL window has the query sent and the SQL Python ran',
    () => /#>\{/.test(document.getElementById('nb').querySelector('#pure')?.textContent ?? '')
      && /\bSELECT\b/i.test(document.getElementById('nb').querySelector('#sql')?.textContent ?? ''));
  const once = fetched.get('/widget.js') === 1 && fetched.get('/widget.css') === 1;
  console.log(`${once ? 'ok  ' : 'FAIL'} DataCube's module came once for the page (${fetched.get('/widget.js')} widget.js, ${fetched.get('/widget.css')} widget.css)`);
  if (!once) failed = true;
  console.log(`${http.length === 0 ? 'ok  ' : 'FAIL'} the notebook cubes made no HTTP call${http.length ? `: ${http.join(', ')}` : ''}`);
  if (http.length) failed = true;
  // A NOTEBOOK'S PAGE (ll.Page under a cell, notebook.PageCube): its sheet and its grid, over the widget's channel; a
  // sheet added in Python shows by itself (its versions followed, no polling)
  const pageTabs = () => [...document.getElementById('nbp').querySelectorAll('.dc-sheet-tab .dc-sheet-label')]
    .map((t) => t.textContent).join('|');
  await check('a notebook\'s page shows its sheet and its grid over its frame, over the widget\'s channel',
    `(${String(pageTabs)})() === 'nbpage' && document.getElementById('nbp').innerText.includes('EMEA')`);
  // its chart draws: the chart's code fetched over the channel, once for the page, when it first drew
  await check('a notebook page\'s charts draw -- a bar and a funnel -- their code fetched over the widget\'s channel',
    () => document.getElementById('nbp').querySelectorAll('.dc-chartpanel canvas').length >= 2);
  const charts = [...fetched.entries()].filter(([p]) => /^\/widget-chunks\/chart-(render|kinds|render-kinds)-[^/]+\.js$/.test(p));
  const chartOnce = charts.length === 3 && charts.every(([, n]) => n === 1);
  console.log(`${chartOnce ? 'ok  ' : 'FAIL'} the charts' code came once each, as chunks (${JSON.stringify(charts)})`);
  if (!chartOnce) failed = true;
  engine.stdin.write('widget-page-sheet\n');
  if ((await nextLine()) !== 'done widget-page-sheet') throw new Error('the engine did not do widget-page-sheet');
  await check('a sheet added to the notebook\'s page in Python shows by itself (its versions followed)',
    `(${String(pageTabs)})() === 'nbpage|Added in Python'`);
  engine.stdin.write('widget-update\n');
  if ((await nextLine()) !== 'done widget-update') throw new Error('the engine did not do widget-update');
  await check('a notebook cube\'s frame updated in Python shows by itself (its version followed, no polling)',
    () => /\b11 rows\b/.test(document.getElementById('nb').innerText));
  const kept = await widgets.evaluate(() => {
    const el = document.getElementById('nb');
    el.querySelector('*')?.dispatchEvent(new KeyboardEvent('keydown', { key: 'ArrowDown', bubbles: true }));
    return { marked: el.dataset.lmSuppressShortcuts === 'true', heardAbove: window.__heardAbove.length };
  });
  const keys = kept.marked && kept.heardAbove === 0;
  console.log(`${keys ? 'ok  ' : 'FAIL'} a key pressed in a notebook cube stays with it (${JSON.stringify(kept)})`);
  if (!keys) failed = true;

  // A VIEW TAKEN DOWN WHILE DATACUBE LOADS (the audit of step 7, S2): on a fresh page, two views of one widget; the
  // first starts the module's load over its channel and is taken down at once (its cell run again), which cuts its
  // calls short. The second loads the module over its own channel, and shows its rows.
  const cut = await browser.newPage();
  cut.on('pageerror', (e) => { console.log(`second widget page error: ${e.message}`); failed = true; });
  const cutFetched = new Map();
  await cut.exposeFunction('__widgetSend', (name, message) => {
    const { path } = JSON.parse(message);
    cutFetched.set(path, (cutFetched.get(path) ?? 0) + 1);
    engine.stdin.write(`widget ${name} ${message}\n`);
  });
  await cut.route(`${origin}/cut.html`, (r) => r.fulfill({
    contentType: 'text/html',
    body: '<!doctype html><meta charset="utf-8"><title>a view taken down</title><div id="a" style="width:1000px"></div>'
      + '<div id="b" style="width:1000px"></div>',
  }));
  await cut.goto(`${origin}/cut.html`);
  widgetPages.push(cut);
  await cut.evaluate(async ({ esm, state }) => {
    const listeners = new Map();
    const values = { ...state };
    const model = {
      get: (key) => values[key],
      on: (event, f) => { if (!listeners.has(event)) listeners.set(event, new Set()); listeners.get(event).add(f); },
      off: (event, f) => { listeners.get(event)?.delete(f); },
      send: (content) => { window.__widgetSend('nb2', JSON.stringify(content)); },
      fire: (event, ...args) => { for (const f of listeners.get(event) ?? []) f(...args); },
      set: (key, value) => { values[key] = value; model.fire(`change:${key}`); },
    };
    window.__models = { nb2: model };
    const url = URL.createObjectURL(new Blob([esm], { type: 'text/javascript' }));
    const widget = (await import(url)).default;
    URL.revokeObjectURL(url);
    const takeDownFirst = widget.render({ model, el: document.getElementById('a') });
    widget.render({ model, el: document.getElementById('b') });
    takeDownFirst();
  }, served.widgets.nb2);
  const rerun = await cut.waitForFunction(() => /\b3 rows\b/.test(document.getElementById('b').innerText), undefined,
    { timeout: 60_000 }).then(() => true, () => false);
  const loads = cutFetched.get('/widget.js');
  const shownAfterCut = rerun && loads === 2;
  console.log(`${shownAfterCut ? 'ok  ' : 'FAIL'} a view taken down mid-load leaves the other view to load DataCube itself (${loads} widget.js asked)`);
  if (!shownAfterCut) {
    console.log(`  the second view showed: ${(await cut.evaluate(() => document.getElementById('b').innerText)).slice(0, 300)}`);
    failed = true;
  }

  // A TIMESERIES OF TWO GRIDS ON A PAGE PYTHON SERVES (the timeseries review's A7, docs/TIMESERIES_REVIEW_2026_10_25.md:
  // the host's half of a combined series ran in no test): built with ll.Page, opened in a tab -- its one series reading
  // both grids together through Python (python-host.ts together: join.json, headless joinTogether), planned and run by
  // Python where the rows are -- its line drawn, its latest value the database's (102 - 4.2), none refused
  engine.stdin.write('py-ts\n');
  const tsBuilt = await nextLine();
  if (!tsBuilt.startsWith('done py-ts ')) throw new Error(`the engine did not build the timeseries page: ${tsBuilt}`);
  await tabbed.goto(JSON.parse(tsBuilt.slice('done py-ts '.length)));
  await seen('a timeseries of two grids on a page Python serves: read together by Python, its line drawn, its latest value the database\'s',
    () => {
      const t = document.querySelector('[data-tile^="timeseries-"] .dc-ts');
      return t !== null && t.querySelector('.dc-ts-chart canvas') !== null && t.querySelector('.dc-ts-failed') === null
        && t.querySelector('.dc-ts-banner.dc-ts-bad') === null && /97\.8/.test(t.querySelector('.dc-ts-legend')?.textContent ?? '');
    });
} finally {
  closing = true;
  await browser?.close();
  // stopped and WAITED for: the engine serves until its input closes, and is gone before the test reports
  engine.stdin.end();
  await stopped;
  warehouse.kill();
  await warehouseStopped;
}
process.exit(failed ? 1 : 0);
