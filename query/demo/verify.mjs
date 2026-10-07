// `bazel run //query:verify`: the Query app end to end in a real browser (Chromium, headless), in
// each place queries run: IN THE BROWSER (the tab's planner writes the SQL, DuckDB-WASM runs it,
// saved queries in IndexedDB -- no server), ON legend-lite's SERVER (it executes and keeps saved
// queries; started here with an empty store) and ON A WAREHOUSE (the tab's planner writes the SQL,
// the warehouse's DuckDB runs it as the signed-in user; started here, empty, seeded by the app).
// The same steps, each asserting what a person would see -- rows, not "a request was made". Exit
// code 0 when every step holds in all three.
//
// A test (//query:verify_test) on the Chromium Bazel fetched; `bazel run //query:verify` runs the same by hand.
// legend-lite's server comes with its JDK.

// first: points Playwright at the Chromium Bazel fetched (as a browser_test; a no-op under bazel run)
import '../../tools/browser/pinned-chromium.mjs';
import { strict as assert } from 'node:assert';
import { spawn, spawnSync } from 'node:child_process';
import { mkdirSync, mkdtempSync } from 'node:fs';
import { createServer } from 'node:http';
import { readFile, stat } from 'node:fs/promises';
import { dirname, extname, join, resolve, sep } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { chromium } from 'playwright';
import { runfileFromEnv, runfilesRoot } from '../../tools/js/runfiles.mts';

// every input by its runfiles path from the BUILD file's env (Bazel workplan P1-24, tools/js/runfiles.mts; no `../..`
// arithmetic): the site (its index.html names the package's directory), legend-lite's server launcher (one file,
// server.exe on Windows; it brings its own JDK, so a machine's `java` or none does not matter), the warehouse's
// native image and its DuckDB
const ROOT = resolve(dirname(runfileFromEnv('SITE')), '..');
const RUNFILES = runfilesRoot();
const SERVER = runfileFromEnv('LEGEND_SERVER');

// everything this writes (the server's store, failure screenshots): under a test, the test's own temp directory;
// by hand, the checkout's .scratch/
const OUT = process.env.TEST_TMPDIR
  ? join(process.env.TEST_TMPDIR, 'verify')
  : join(process.env.BUILD_WORKSPACE_DIRECTORY ?? resolve(ROOT, '..'), '.scratch', 'verify');
mkdirSync(OUT, { recursive: true });
const store = mkdtempSync(join(OUT, 'run-'));
const SHOTS = process.env.TEST_UNDECLARED_OUTPUTS_DIR ?? store;

// ---- legend-lite's server, for the server mode
// port 0: the server picks a free port and prints it, so no two runs (or shards) collide
const engine = spawn(SERVER, ['0', '--query-store', store], {
  env: { ...process.env, RUNFILES_DIR: RUNFILES, JAVA_RUNFILES: RUNFILES },
  stdio: ['ignore', 'pipe', 'pipe'],
});
let engineLog = '';
engine.stdout.on('data', (d) => { engineLog += d; });
engine.stderr.on('data', (d) => { engineLog += d; });
const enginePort = new Promise((ok, fail) => {
  engine.stdout.on('data', () => {
    const m = /started on port (\d+)/.exec(engineLog);
    if (m) ok(Number(m[1]));
  });
  engine.on('exit', (code) => fail(new Error(`the engine exited (${code}):\n${engineLog}`)));
  engine.on('error', (e) => fail(new Error(`the engine did not start (${SERVER}): ${e.message}`)));
});
// awaited where it matters; an early failure must not be an unhandled rejection that ends the run outside `finally`
enginePort.catch(() => {});

// ---- the site; config-server.json points at that server, config-warehouse.json at that warehouse
const TYPES = {
  '.html': 'text/html', '.js': 'text/javascript', '.css': 'text/css', '.wasm': 'application/wasm',
  '.json': 'application/json', '.pure': 'text/plain', '.sql': 'text/plain',
};
const site = createServer(async (req, res) => {
  const { pathname } = new URL(req.url ?? '/', 'http://x');
  try {
    if (pathname === '/demo/config-server.json') {
      const config = JSON.parse(await readFile(join(ROOT, 'demo', 'config-server.json'), 'utf8'));
      config.execution.engine = `http://127.0.0.1:${await enginePort}/api`;
      res.writeHead(200, { 'Content-Type': 'application/json' }).end(JSON.stringify(config));
      return;
    }
    if (pathname === '/demo/config-warehouse.json') {
      const config = JSON.parse(await readFile(join(ROOT, 'demo', 'config-warehouse.json'), 'utf8'));
      config.execution.url = await warehouseUrl;
      res.writeHead(200, { 'Content-Type': 'application/json' }).end(JSON.stringify(config));
      return;
    }
    const base = pathToFileURL(ROOT + sep);
    const file = fileURLToPath(new URL(`.${pathname}`, base));
    if (!file.startsWith(ROOT)) throw new Error('outside');
    await stat(file);
    res.writeHead(200, { 'Content-Type': TYPES[extname(file)] ?? 'application/octet-stream' }).end(await readFile(file));
  } catch {
    res.writeHead(404).end();
  }
});
// port 0 here too; the warehouse below is told this origin, so it starts after the site listens
await new Promise((r) => site.listen(0, '127.0.0.1', r));
const SITE_PORT = site.address().port;

// ---- a warehouse, for the warehouse mode: the native image DataCube's live_snap_test runs, its
// data in this run's directory; alice owns it (the demo's seed creates its tables as her)
const SITE_ORIGIN = `http://127.0.0.1:${SITE_PORT}`;
const warehouse = spawn(runfileFromEnv('WAREHOUSE_BINARY'), [
  '--port', '0', '--data', join(store, 'warehouse'), '--user', 'alice:alice-pw', '--owner', 'alice',
  '--allow-origin', SITE_ORIGIN, '--duckdb-library', runfileFromEnv('WAREHOUSE_DUCKDB_LIBRARY'),
], { stdio: ['ignore', 'ignore', 'pipe'] });
let warehouseLog = '';
const warehouseUrl = new Promise((ok, fail) => {
  warehouse.stderr.on('data', (d) => {
    warehouseLog += d;
    const m = /listening on 127\.0\.0\.1:(\d+)/.exec(warehouseLog);
    if (m) ok(`http://127.0.0.1:${m[1]}`);
  });
  warehouse.on('exit', (code) => fail(new Error(`the warehouse exited (${code}):\n${warehouseLog}`)));
  warehouse.on('error', (e) => fail(new Error(`the warehouse did not start: ${e.message}`)));
});
warehouseUrl.catch(() => {});

async function waitForEngine() {
  // bounded: a server that stays up but never prints its port would otherwise wait out the test's whole timeout
  const port = await Promise.race([
    enginePort,
    new Promise((_, fail) => setTimeout(() => fail(new Error(`no "started on port" line in 60 s:\n${engineLog}`)), 60_000)),
  ]);
  for (let i = 0; i < 120; i++) {
    try {
      if ((await fetch(`http://127.0.0.1:${port}/health`)).ok) return;
    } catch { /* not up yet */ }
    await new Promise((r) => setTimeout(r, 250));
  }
  throw new Error(`the engine did not start:\n${engineLog}`);
}

const PAGE = `http://127.0.0.1:${SITE_PORT}/demo/index.html`;
const GAV = encodeURIComponent('demo:trading:0.0.0');
const enc = encodeURIComponent;
let browser;
let failures = 0;

/** Run, and wait for what it shows: rows or objects ("n rows in", "n objects in"), or an error. */
async function run(page) {
  await page.click('button.q-run');
  await page.waitForFunction(() => document.querySelector('.q-error-box')
    || /\d+ (rows?|objects?) in \d+ ms/.test(document.querySelector('.q-results-bar')?.textContent ?? ''), undefined, { timeout: 30000 });
  const error = await page.$('.q-error-box');
  if (error) throw new Error(`the run failed: ${await error.textContent()}`);
}

/** The rows the plain grid shows, each its cells' text. */
async function gridRows(page) {
  return page.$$eval('.q-grid tbody tr', (trs) => trs.map((tr) => [...tr.querySelectorAll('td')].map((td) => td.textContent)));
}

/** The rows a results DataCube shows, each its cells' text. */
async function cubeRows(page) {
  return page.$$eval('.q-cube .dc-row', (rows) => rows.map((r) => [...r.querySelectorAll('.dc-cell')].map((c) => c.textContent?.trim())));
}

/** The warehouse's sign-in form, answered as alice: the app asks on every load and stores nothing. */
async function signIn(page) {
  await page.fill('input[autocomplete=username]', 'alice', { timeout: 60000 });
  await page.fill('input[type=password]', 'alice-pw');
  await page.click('button[type=submit]');
}

/**
 * Every step, on the page `query` configures (`''`: in the browser; `?config=...`: elsewhere);
 * `signedIn`: each load of the page (a step's goto, a reload) answers the warehouse's sign-in.
 */
async function suite(title, query, signedIn = false) {
  console.log(`\n${title}:`);
  const context = await browser.newContext({ viewport: { width: 1400, height: 900 } });
  const app = (hash = '') => `${PAGE}${query}${hash}`;
  const step = async (name, fn) => {
    const page = await context.newPage();
    if (signedIn) {
      for (const load of ['goto', 'reload']) {
        const original = page[load].bind(page);
        // a hash-only goto stays in the document (Playwright answers null): no new sign-in then
        page[load] = async (...args) => { const r = await original(...args); if (r !== null) await signIn(page); return r; };
      }
    }
    const errors = [];
    page.on('pageerror', (e) => errors.push(e.message));
    try {
      await fn(page);
      assert.deepEqual(errors, [], 'no page errors');
      console.log(`  ✔ ${name}`);
    } catch (e) {
      failures++;
      console.log(`  ✖ ${name}\n    ${String(e.message).split('\n').join('\n    ')}`);
      // under a test, where Bazel keeps a test's outputs (test.outputs/, uploaded by CI); by hand, the run's directory
      await page.screenshot({ path: join(SHOTS, `${title.replace(/\W+/g, '-')}--${name.replace(/\W+/g, '-')}.png`) });
    } finally {
      await page.close();
    }
  };

  await step('/ opens the query builder: a data space chosen, the editor opens on it', async (page) => {
    await page.goto(app());
    await page.waitForSelector('.q-builder select[aria-label="Data Space"]', { timeout: 60000 });
    assert.match(await page.textContent('.q-builder'), /Specify the class, mapping, and runtime/);
    await page.selectOption('select[aria-label="Data Space"]', { label: 'Trading' });
    await page.waitForSelector('.q-node', { timeout: 60000 });
    assert.equal(await page.inputValue('select[aria-label="Entity"]').then(Boolean), true);
  });

  await step('the setup page lists the data space, classes and services', async (page) => {
    await page.goto(app('#/setup'));
    await page.waitForSelector('.q-card', { timeout: 60000 });
    assert.match(await page.textContent('.q-landing'), /Trading[\s\S]*Firm[\s\S]*Trade[\s\S]*ExecutedEquityTrades/);
  });

  await step('the data space viewer shows its curated queries and documentation', async (page) => {
    await page.goto(app(`#/dataspace/${GAV}/${enc('demo::trading::TradingDataSpace')}`));
    await page.waitForSelector('text=Quick start', { timeout: 60000 });
    assert.equal(await page.locator('.q-card').count(), 3);
    await page.waitForFunction(() => document.querySelector('.q-card pre')?.textContent?.includes('project'));
    assert.match(await page.textContent('.q-ds'), /Models documentation[\s\S]*Firm[\s\S]*Registered legal name/);
  });

  let savedHash;
  await step('build, filter, run, save and reopen a query', async (page) => {
    await page.goto(app(`#/extensions/dataspace/${GAV}/${enc('demo::trading::TradingDataSpace')}?class=${enc('demo::trading::Trade')}`));
    await page.waitForSelector('.q-node', { timeout: 60000 });
    for (const p of ['Trade Id', 'Side', 'Quantity']) await page.dblclick(`.q-node:has-text('${p}')`);
    await run(page);
    assert.equal((await gridRows(page)).length, 12);
    await page.click(".q-node:has-text('Side')", { button: 'right' });
    await page.click(".q-menu button:has-text('Add as filter condition')");
    await page.selectOption('.q-cond select[aria-label=Side]', 'SELL');
    await run(page);
    const rows = await gridRows(page);
    assert.equal(rows.length, 5);
    assert.ok(rows.every((r) => r[1] === 'SELL'));
    await page.click('button[title="Save (Ctrl+S)"]');
    await page.fill('.q-dialog input.q-input', 'Sells');
    await page.click('.q-dialog button.primary');
    await page.waitForFunction(() => location.hash.startsWith('#/edit/'));
    savedHash = await page.evaluate(() => location.hash);
    await page.reload();
    await page.waitForSelector('.q-cond', { timeout: 60000 });
    assert.equal(await page.inputValue('.q-col input'), 'Trade Id');
    assert.equal(await page.locator('.q-chip:has-text("unsaved")').count(), 0);
    await run(page);
    assert.equal((await gridRows(page)).length, 5);
  });

  await step('the query store: search finds it, history holds each version', async (page) => {
    await page.goto(app(savedHash));
    await page.waitForSelector('.q-cond', { timeout: 60000 });
    await page.fill('.q-col input', 'Id');
    await page.press('.q-col input', 'Tab');
    await page.click('button[title="Save (Ctrl+S)"]');
    await page.waitForSelector('.q-toast:has-text("version 2")');
    await page.click('.q-header-pill:has-text("Advanced")');
    await page.click(".q-menu button:has-text('History and versions')");
    await page.waitForSelector('.q-dialog :text("Compare")');
    assert.match(await page.textContent('.q-dialog pre'), /- .*'Trade Id'[\s\S]*\+ .*Id:/);
    await page.goto(app("#/setup"));
    await page.waitForSelector('.q-list-row:has-text("Sells")', { timeout: 60000 });
  });

  await step("a data space's curated query opens in the form and runs", async (page) => {
    await page.goto(app(`#/extensions/dataspace/${GAV}/${enc('demo::trading::TradingDataSpace')}/template/by_firm`));
    await page.waitForSelector('.q-col', { timeout: 60000 });
    assert.equal(await page.textContent('.q-col .q-btn.primary'), 'count');
    await run(page);
    assert.equal((await gridRows(page)).length, 5);
  });

  await step('a parameter: required before running, then bound', async (page) => {
    await page.goto(app(`#/create/manual/${GAV}/${enc('demo::trading::TradingMapping')}/${enc('demo::trading::Runtime')}?class=${enc('demo::trading::Trade')}`));
    await page.waitForSelector('.q-node', { timeout: 60000 });
    await page.dblclick(".q-node:has-text('Trade Id')");
    await page.click(".q-node:has-text('Quantity')", { button: 'right' });
    await page.click(".q-menu button:has-text('Add as filter condition')");
    await page.selectOption('.q-cond select[aria-label=Operator]', 'greaterThan');
    // parameters show on request, as upstream (Advanced > Show Parameters)
    await page.click('.q-header-pill:has-text("Advanced")');
    await page.click(".q-menu button:has-text('Show Parameters')");
    await page.click('button[title="Add a parameter"]');
    await page.fill('.q-dialog input.q-input', 'minQty');
    await page.selectOption('.q-dialog select.q-select >> nth=0', 'Integer');
    await page.click('.q-dialog button.primary');
    await page.click(".q-cond button[title='Parameters and relative values']");
    await page.click(".q-menu button:has-text('Use parameter')");
    await page.click('button.q-run');
    await page.waitForSelector('.q-toast:has-text("Set a value for $minQty")');
    await page.fill('.q-side input[aria-label=Value]', '1000000');
    await page.press('.q-side input[aria-label=Value]', 'Tab');
    await run(page);
    assert.equal((await gridRows(page)).length, 6);
  });

  await step('a constant: made in its panel, used in a filter, saved, reopened, and run as a cube', async (page) => {
    await page.goto(app(`#/create/manual/${GAV}/${enc('demo::trading::TradingMapping')}/${enc('demo::trading::Runtime')}?class=${enc('demo::trading::Trade')}`));
    await page.waitForSelector('.q-node', { timeout: 60000 });
    await page.dblclick(".q-node:has-text('Trade Id')");
    await page.click('.q-header-pill:has-text("Advanced")');
    await page.click(".q-menu button:has-text('Show Constants')");
    await page.click('button[title="Add a constant"]');
    await page.fill('.q-dialog input[aria-label="Constant name"]', 'minQty');
    await page.selectOption('.q-dialog select[aria-label="Constant type"]', 'Integer');
    await page.fill('.q-dialog input[aria-label=Value]', '1000000');
    await page.press('.q-dialog input[aria-label=Value]', 'Tab');
    await page.click('.q-dialog button.primary');
    await page.waitForSelector('.q-constant:has-text("$minQty")');
    await page.click(".q-node:has-text('Quantity')", { button: 'right' });
    await page.click(".q-menu button:has-text('Add as filter condition')");
    await page.selectOption('.q-cond select[aria-label=Operator]', 'greaterThan');
    await page.click(".q-cond button[title='Parameters and relative values']");
    await page.click(".q-menu button:has-text('Use constant $minQty')");
    await run(page);
    assert.equal((await gridRows(page)).length, 6);
    await page.click('button[title="Save (Ctrl+S)"]');
    await page.fill('.q-dialog input.q-input', 'Big trades');
    await page.click('.q-dialog button.primary');
    await page.waitForFunction(() => location.hash.startsWith('#/edit/'));
    await page.reload();
    await page.waitForSelector('.q-constant:has-text("$minQty")', { timeout: 60000 });
    assert.match(await page.textContent('.q-cond'), /\$minQty/);
    await run(page);
    assert.equal((await gridRows(page)).length, 6);
    await page.click('.q-results-bar button.q-mode:text-is("DataCube")');
    await page.waitForFunction(() => document.querySelectorAll('.q-cube .dc-row').length === 6, undefined, { timeout: 30000 });
    await page.click('.q-results-bar button.q-mode:text-is("Grid")');
  });

  await step("a derived property's arguments: defaulted when added, edited, and run", async (page) => {
    await page.goto(app(`#/create/manual/${GAV}/${enc('demo::trading::TradingMapping')}/${enc('demo::trading::Runtime')}?class=${enc('demo::trading::Trade')}`));
    await page.waitForSelector('.q-node', { timeout: 60000 });
    await page.dblclick(".q-node:has-text('Trade Id')");
    await page.dblclick(".q-node:has-text('Notional At')");
    await page.click('.q-col >> nth=1 >> button.q-args');
    await page.fill('.q-dialog input[aria-label=Value]', '2');
    await page.press('.q-dialog input[aria-label=Value]', 'Tab');
    await page.click('.q-dialog button.primary');
    await page.click(".q-node:has-text('Is At Least')", { button: 'right' });
    await page.click(".q-menu button:has-text('Add as filter condition')");
    await page.click('.q-cond button.q-args');
    await page.fill('.q-dialog input[aria-label=Value]', '1000000');
    await page.press('.q-dialog input[aria-label=Value]', 'Tab');
    await page.click('.q-dialog button.primary');
    await run(page);
    // legend-engine 4.145.0 answers the same query (notionalAt(2.0), isAtLeast(1000000)) with these
    const rows = (await gridRows(page)).map(([id, n]) => [Number(id), Number(String(n).replace(/,/g, ''))]);
    assert.deepEqual(rows.map(([id]) => id), [5, 6, 7, 8, 9, 10]);
    assert.equal(rows[0][1], 9825000);
  });

  await step('milestoning: a temporal class as of $businessDate, then every version', async (page) => {
    const manual = (cls) => app(`#/create/manual/${GAV}/${enc('demo::trading::TradingMapping')}/${enc('demo::trading::Runtime')}?class=${enc(`demo::trading::${cls}`)}`);
    const setDate = async (value) => {
      await page.fill('.q-side input[aria-label=Value]', value);
      await page.press('.q-side input[aria-label=Value]', 'Tab');
    };
    // the rows legend-engine 4.145.0 answers for the same queries
    await page.goto(manual('FirmRating'));
    await page.waitForSelector('.q-node', { timeout: 60000 });
    assert.match(await page.textContent('.q-side'), /\$businessDate/);
    assert.match(await page.textContent('.q-options'), /as of \$businessDate/);
    await page.dblclick(".q-node:has-text('Grade')");
    await setDate('2024-01-15');
    await run(page);
    assert.deepEqual((await gridRows(page)).map((r) => r[0]).sort(), ['A', 'A3', 'BBB+']);
    await page.click('.q-options button.q-editable');
    await page.check('#q-opt-allversions');
    await page.click('.q-dialog button.primary');
    await run(page);
    assert.deepEqual((await gridRows(page)).map((r) => r[0]).sort(), ['A', 'A3', 'AA', 'BBB', 'BBB+']);
  });

  await step('milestoning: a property into a temporal class writes $businessDate, which the query gains', async (page) => {
    const manual = (cls) => app(`#/create/manual/${GAV}/${enc('demo::trading::TradingMapping')}/${enc('demo::trading::Runtime')}?class=${enc(`demo::trading::${cls}`)}`);
    const setDate = async (value) => {
      await page.fill('.q-side input[aria-label=Value]', value);
      await page.press('.q-side input[aria-label=Value]', 'Tab');
    };
    await page.goto(manual('Firm'));
    await page.waitForSelector('.q-node', { timeout: 60000 });
    await page.dblclick(".q-node:has-text('Legal Name')");
    await page.fill('.q-side input[aria-label="Search properties"]', 'grade');
    await page.dblclick(".q-node:has-text('Grade')");
    assert.match(await page.textContent('.q-side'), /\$businessDate/);
    await setDate('2024-01-15');
    await run(page);
    assert.deepEqual((await gridRows(page)).sort(), [
      ['Halberd Securities', 'BBB+'], ['Kestrel Partners', 'null'], ['Meridian Capital', 'A'], ['Northgate Asset Management', 'A3']]);
  });

  await step('a percentile and a weighted average agree with the rows they summarise', async (page) => {
    await page.goto(app(`#/create/manual/${GAV}/${enc('demo::trading::TradingMapping')}/${enc('demo::trading::Runtime')}?class=${enc('demo::trading::Trade')}`));
    await page.waitForSelector('.q-node', { timeout: 60000 });
    for (const p of ['Side', 'Price', 'Quantity']) await page.dblclick(`.q-node:has-text('${p}')`);
    await run(page);
    // the expected values, worked out here from the plain rows
    const num = (s) => Number(String(s).replace(/,/g, ''));
    const bySide = new Map();
    for (const [side, price, qty] of await gridRows(page)) bySide.set(side, [...(bySide.get(side) ?? []), [num(price), num(qty)]]);
    assert.deepEqual([...bySide.keys()].sort(), ['BUY', 'SELL']);
    const median = (xs) => { const s = [...xs].sort((a, b) => a - b); const k = (s.length - 1) / 2; return s[Math.floor(k)] + (s[Math.ceil(k)] - s[Math.floor(k)]) * (k - Math.floor(k)); };
    const close = (a, b, what) => assert.ok(Math.abs(a - b) <= 1e-3 * Math.max(1, Math.abs(b)), `${what}: ${a} vs ${b}`);
    const aggregate = async (n, label) => {
      await page.click(`.q-col >> nth=${n} >> .q-col__agg`);
      await page.click(`.q-menu button:has-text('${label}')`);
    };
    await page.click('.q-col >> nth=2 >> .q-col__remove');
    await aggregate(1, 'percentile…');
    await page.fill('.q-dialog input[aria-label=Percentile]', '50');
    await page.click('.q-dialog button.primary');
    await run(page);
    let rows = await gridRows(page);
    assert.equal(rows.length, bySide.size);
    for (const [side, value] of rows) close(num(value), median(bySide.get(side).map(([p]) => p)), `the median price of ${side}`);
    await page.dblclick(".q-node:has-text('Quantity')");
    await aggregate(1, 'weighted average…');
    await page.selectOption('.q-dialog select[aria-label="Weight column"]', 'Quantity');
    await page.click('.q-dialog button.primary');
    assert.equal(await page.textContent('.q-col >> nth=2 >> .q-col__agg'), 'weight');
    await run(page);
    rows = await gridRows(page);
    assert.equal(rows.length, bySide.size);
    for (const [side, value, ...rest] of rows) {
      assert.deepEqual(rest, [], 'the weight is consumed, not shown');
      const t = bySide.get(side);
      close(num(value), t.reduce((s, [p, q]) => s + p * q, 0) / t.reduce((s, [, q]) => s + q, 0), `the volume-weighted price of ${side}`);
    }
  });

  await step('DataCube shows the rows, and groups them on the same planner', async (page) => {
    await page.goto(app(`#/extensions/dataspace/${GAV}/${enc('demo::trading::TradingDataSpace')}?class=${enc('demo::trading::Trade')}`));
    await page.waitForSelector('.q-node', { timeout: 60000 });
    for (const p of ['Trade Id', 'Side', 'Quantity']) await page.dblclick(`.q-node:has-text('${p}')`);
    await run(page);
    assert.equal((await gridRows(page)).length, 12);
    await page.click('.q-results-bar button.q-mode:text-is("DataCube")');
    await page.waitForFunction(() => document.querySelectorAll('.q-cube .dc-row').length === 12, undefined, { timeout: 30000 });
    // the grid alone: no title bar, drag zones, columns panel or status bar
    assert.equal(await page.locator('.q-cube .dc-titlebar:visible, .q-cube .dc-zone-bar:visible, .q-cube .dc-app-side:visible, .q-cube .dc-app-stats:visible').count(), 0);
    assert.match(await page.textContent('.q-results-bar'), /12 rows in \d+ ms/);
    // group by Side (an enumeration) from the grid's right-click menu: the cube's own groupBy,
    // planned on the query
    await page.click('.q-cube .dc-row >> nth=0 >> .dc-cell >> nth=1', { button: 'right' });
    await page.hover('.dc-menu > .dc-menu-item:has(> .dc-menu-label:text-is("Pivot"))');
    await page.click('.dc-submenu .dc-menu-item:has(> .dc-menu-label:text-is("Vertical Pivot on Side"))');
    await page.waitForFunction(() => document.querySelectorAll('.q-cube .dc-row').length === 2, undefined, { timeout: 30000 });
    // a group's first cell carries the tree's expander (▸) before its label
    const groups = (await cubeRows(page)).map((r) => [r[0].replace(/^[▸▾]\s*/, ''), r[r.length - 1]]);
    assert.deepEqual(groups, [['BUY', '15,003,400'], ['SELL', '37,503,100']]);
    // its controls come back from the right-click menu's last entry (DataCube's own mode)
    await page.click('.q-cube .dc-row >> nth=0 >> .dc-cell >> nth=1', { button: 'right' });
    await page.click('.dc-menu > .dc-menu-item:has(> .dc-menu-label:text-is("Show Controls"))');
    await page.waitForSelector('.q-cube .dc-titlebar:visible, .q-cube .dc-app-stats:visible', { timeout: 10000 });
    await page.click('.q-results-bar button.q-mode:text-is("Grid")');
    await page.waitForSelector('.q-grid tbody tr', { timeout: 30000 });
    assert.equal((await gridRows(page)).length, 12);
  });

  await step('Objects mode fetches JSON', async (page) => {
    await page.goto(app(`#/create/manual/${GAV}/${enc('demo::trading::TradingMapping')}/${enc('demo::trading::Runtime')}?class=${enc('demo::trading::Firm')}`));
    await page.waitForSelector('.q-node', { timeout: 60000 });
    await page.dblclick(".q-node:has-text('Legal Name')");
    await page.click('.q-panel__header button:has-text("Graph Fetch")');
    await run(page);
    const json = JSON.parse(await page.textContent('.q-json'));
    assert.equal(json.length, 4);
    assert.ok('legalName' in json[0]);
  });

  await context.close();
}

try {
  await waitForEngine();
  browser = await chromium.launch();
  console.log('Query app, end to end');
  await suite('In the browser (DuckDB-WASM, no server)', '');
  await suite("On legend-lite's server", '?config=config-server.json');
  await suite('On a warehouse (its DuckDB, signed in)', '?config=config-warehouse.json', true);
} finally {
  await browser?.close();
  site.close();
  // On Windows the server is Bazel's launcher (server.exe) whose child is the JVM: a kill stops the launcher alone
  // and the orphaned JVM keeps the pipes open (query-store/test/lite.test.ts, 2026-10-02); taskkill /T takes the
  // tree, by its full path (a test's PATH is Bazel's). The warehouse is one native process: kill() is enough.
  if (process.platform === 'win32' && engine.exitCode === null && engine.signalCode === null) {
    const root = process.env.SystemRoot ?? process.env.SYSTEMROOT;
    if (!root) throw new Error('SystemRoot is not set: cannot find taskkill.exe');
    spawnSync(join(root, 'System32', 'taskkill.exe'), ['/pid', String(engine.pid), '/t', '/f'], { encoding: 'utf8' });
  } else {
    engine.kill();
  }
  warehouse.kill();
}

if (failures > 0) {
  console.log(`\n${failures} step(s) failed; screenshots in ${SHOTS}`);
  process.exit(1);
}
console.log('\nevery step holds, in all three');
