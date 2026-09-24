// The demo, in a real browser, with NO SERVER running.
//
// verify-wasm-planner.ts proves the module answers identically to the
// JVM under Node. That is not the same claim as "the product works in
// a browser": Node resolves paths differently, fetches differently,
// and has no DOM. The point of the WASM planner is that a tab needs
// no server, and only a browser can show that.
//
// Serves datacube/ over http (a file: origin cannot instantiate WASM
// from fetch), loads demo/index.html (the default entry), and waits for
// grid to have real rows. Nothing answers on the legend-lite port; if
// the page renders, the planning happened in the tab.
//
//   bazel run //datacube:verify_wasm_browser

import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
import { extname, join, normalize } from 'node:path';
import { chromium } from 'playwright';
import { fileURLToPath } from 'node:url';

// Serve the datacube/ directory, not demo/: index.html links its
// stylesheets as ../src/*.css, so a demo-rooted server 404s them
// and the page renders unstyled. The real demo is served from here.
const ROOT = fileURLToPath(new URL('..', import.meta.url));
const TYPES = {
  '.html': 'text/html',
  '.js': 'text/javascript',
  '.mjs': 'text/javascript',
  '.wasm': 'application/wasm',
  '.pure': 'text/plain',
  '.css': 'text/css',
  '.json': 'application/json',
};

const server = createServer(async (req, res) => {
  const path = (req.url ?? '/').split('?')[0];
  const rel = normalize(path === '/' ? '/demo/index.html' : path).replace(/^(\.\.[/])+/, '');
  try {
    const body = await readFile(join(ROOT, rel));
    res.writeHead(200, {
      'Content-Type': TYPES[extname(rel)] ?? 'application/octet-stream',
      // duckdb-wasm wants these for its threaded build; harmless here.
      'Cross-Origin-Opener-Policy': 'same-origin',
      'Cross-Origin-Embedder-Policy': 'require-corp',
    });
    res.end(body);
  } catch {
    res.writeHead(404).end('not found');
  }
});

await new Promise((r) => server.listen(0, '127.0.0.1', r));
const { port } = server.address();
const url = `http://127.0.0.1:${port}/demo/index.html`;
console.log(`serving datacube/ on ${port}\nno legend-lite server is running\n`);

const browser = await chromium.launch();
const page = await browser.newPage();

const problems = [];
const missing = [];
page.on('response', (r) => {
  if (r.status() === 404) missing.push(new URL(r.url()).pathname);
});
page.on('console', (m) => {
  // A 404 is reported separately, by PATH: "Failed to load resource"
  // with no URL is unactionable, and the paths are what say whether
  // the miss matters (a vendored asset) or not (a favicon).
  if (m.type() === 'error' && !/Failed to load resource/.test(m.text())) {
    problems.push(`console: ${m.text()}`);
  }
});
page.on('pageerror', (e) => problems.push(`pageerror: ${e.message}`));

let failed = false;
try {
  await page.goto(url, { waitUntil: 'load', timeout: 60_000 });

  // The status line names the plane that actually ran, in a word:
  // `local` plans in this tab, `remote` on legend-lite over HTTP,
  // `engine` on legend-engine itself.
  await page.waitForFunction(
    () => /local|remote|engine/.test(
      document.getElementById('status')?.textContent ?? ''),
    { timeout: 60_000 },
  );
  const status = await page.textContent('#status');
  console.log(`status line: ${status}`);

  // Rows only appear if Pure was planned to SQL and DuckDB ran it.
  await page.waitForFunction(
    () => document.querySelectorAll('.dc-row').length > 0,
    { timeout: 60_000 },
  );
  const rows = await page.locator('.dc-row').count();
  const firstCells = await page.locator('.dc-row').first()
    .locator('.dc-cell').allTextContents();
  console.log(`rendered rows: ${rows}`);
  console.log(`first row: ${JSON.stringify(firstCells.slice(0, 6))}`);

  // Pin the CONTENT, not just "something rendered".
  //
  // This check used to assert rows > 0, and a deliberately tampered
  // module -- one wrapping every plan in `SELECT * FROM (...) LIMIT 1`
  // -- sailed through it while the grid showed one row instead of
  // three. "The page is not blank" is not evidence that the planner
  // produced the right SQL, which is the only thing this run exists
  // to establish.
  //
  // Three top-level regions (AMER, APAC, EMEA) at the root of the
  // tree, and the first must be AMER with five pivoted year columns
  // of formatted currency.
  if (rows !== 3) {
    console.log(`FAIL: expected 3 root rows, got ${rows}`);
    failed = true;
  }
  if (!/AMER/.test(firstCells[0] ?? '')) {
    console.log(`FAIL: first row is not AMER: ${JSON.stringify(firstCells[0])}`);
    failed = true;
  }
  const money = firstCells.slice(1, 6);
  if (money.length !== 5 || !money.every((c) => /^\$[\d,]+$/.test(c))) {
    console.log(`FAIL: expected 5 pivoted currency cells, got `
      + JSON.stringify(money));
    failed = true;
  }
  // THIS PAGE IS THE LOCAL PLANE, and must say so rather than
  // naming any of the others: the whole point of this run is that no
  // server was involved.
  if ((status ?? '').trim() !== 'local') {
    console.log(`FAIL: the status line reads "${status}", not "local"`);
    failed = true;
  }

  // CLEARING THE COLUMN PIVOT MUST NOT COST THE OTHER COLUMNS.
  //
  // This is the cube that has a HOST-CONFIGURED measure -- the
  // demo's `measures: [notional]` -- and that is the shape the bug
  // needed: the projection carried the group keys and the configured
  // measure and dropped everything else, so clearing the pivot left
  // a grid of one data column while its own columns panel still
  // listed year, qtr, pnl and qty. The feature sweep uploads a file,
  // whose cube has no configured measures, and its own projection
  // was already wide -- so the sweep could not see this. It belongs
  // here, against the cube that shows it.
  const columnsNow = () => page.evaluate(() =>
    [...document.querySelectorAll('.dc-th[data-column]')]
      .map((e) => e.dataset.column));
  const panelNow = () => page.evaluate(() =>
    [...document.querySelectorAll('.dc-tool-panel-row')]
      .map((e) => e.dataset.column));

  await page.locator('.dc-cell').first().click({ button: 'right' });
  await page.waitForSelector('.dc-menu', { timeout: 10_000 });
  await page.locator('.dc-menu-item:has(> .dc-menu-label:text-is("Pivot"))')
    .hover();
  await page.waitForTimeout(200);
  await page.locator('.dc-menu-item:has(> .dc-menu-label'
    + ':text-is("Clear All Horizontal Pivots"))').click();
  await page.waitForFunction(
    () => ![...document.querySelectorAll('.dc-th[data-column]')]
      .some((e) => (e.dataset.column ?? '').includes('__|__')),
    { timeout: 60_000 },
  ).catch(() => {});
  await page.waitForTimeout(500);

  const flat = await columnsNow();
  const panel = await panelNow();
  console.log(`after clearing the pivot: ${flat.join(', ')}`);
  // Every non-grouped column the panel lists is on screen. The row
  // dimensions are the tree's now, which is what hides them.
  const grouped = ['region', 'desk', 'book'];
  const wanted = panel.filter((c) => !grouped.includes(c));
  const lost = wanted.filter((c) => !flat.includes(c));
  if (lost.length) {
    console.log(`FAIL: clearing the pivot lost ${lost.join(', ')}`);
    failed = true;
  }
  // AND THE MEASURES CARRY FIGURES, not blanks: a column that comes
  // back empty is the same fault one step later. By NAME, because
  // the dimensions are legitimately blank here -- `uniqueValueOnly`
  // over a group holding five years has no answer to give -- and a
  // count of non-empty cells would let a measure hide behind them.
  const byName = await page.evaluate(() => {
    const names = [...document.querySelectorAll('.dc-th[data-column]')]
      .map((e) => e.dataset.column);
    const cells = [...(document.querySelector('.dc-row')
      ?.querySelectorAll('.dc-cell') ?? [])]
      .map((c) => c.textContent?.trim() ?? '');
    return Object.fromEntries(names.map((n, i) => [n, cells[i] ?? '']));
  });
  console.log(`flat grouped row: ${JSON.stringify(byName)}`);
  for (const measure of ['notional', 'pnl', 'qty']) {
    if (!(measure in byName)) continue;
    // A figure, of any sign, including a legitimate zero: AMER's pnl
    // sums to -0.00 in this generated data, which renders as ($0).
    if (!/\d/.test(byName[measure])) {
      console.log(`FAIL: ${measure} came back empty after clearing the`
        + ` pivot: ${JSON.stringify(byName[measure])}`);
      failed = true;
    }
  }
} catch (e) {
  console.log(`FAIL: ${e.message}`);
  failed = true;
} finally {
  if (missing.length) {
    console.log(`\n404s: ${[...new Set(missing)].join(', ')}`);
    // A missing favicon is not a product fault; a missing vendored
    // asset is exactly what this run exists to catch.
    const real = [...new Set(missing)].filter((m) => m !== '/favicon.ico');
    if (real.length) {
      console.log(`FAIL: ${real.length} asset(s) missing: ${real.join(', ')}`);
      failed = true;
    }
  }
  if (problems.length) {
    console.log(`\npage reported ${problems.length} error(s):`);
    for (const p of problems.slice(0, 10)) console.log(`  ${p}`);
    failed = true;
  }
  await browser.close();
  server.close();
}

console.log(failed
  ? '\n!!! the demo did NOT run on the wasm planner !!!'
  : '\n*** the demo planned and rendered in the browser, no server ***');
process.exit(failed ? 1 : 0);
