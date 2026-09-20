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
//   npm run planner:vendor && npm run build:demo && npm run verify:browser

import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
import { extname, join, normalize } from 'node:path';
import { chromium } from 'playwright';

// Serve the datacube/ directory, not demo/: index.html links its
// stylesheets as ../src/*.css, so a demo-rooted server 404s them
// and the page renders unstyled. The real demo is served from here.
const ROOT = new URL('..', import.meta.url).pathname;
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

  // The status line names the planner that actually ran.
  await page.waitForFunction(
    () => /wasm/.test(document.getElementById('status')?.textContent ?? ''),
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

  if (rows === 0) {
    console.log('FAIL: the grid rendered no rows');
    failed = true;
  }
  if (!/wasm/.test(status ?? '')) {
    console.log('FAIL: the status line does not name the wasm planner');
    failed = true;
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
