// Load the demo in a real browser and assert it actually works.
//
// A demo that has only been typechecked is a demo nobody has seen run.
// This boots DuckDB-WASM for real, waits for the grid to render, then
// exercises snap mode and the keyboard -- the three things most likely
// to be broken in ways a unit test cannot see.
//
// Run: node demo/verify.mjs   (after npm run build:demo)
import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
import { extname, join, normalize } from 'node:path';
import { fileURLToPath } from 'node:url';

import { chromium } from 'playwright';

const ROOT = fileURLToPath(new URL('..', import.meta.url));
const PORT = 8731;
const TYPES = {
  '.html': 'text/html',
  '.js': 'text/javascript',
  '.mjs': 'text/javascript',
  '.css': 'text/css',
  '.wasm': 'application/wasm',
  '.json': 'application/json',
};

const server = createServer(async (req, res) => {
  try {
    const url = new URL(req.url ?? '/', 'http://x');
    const rel = normalize(decodeURIComponent(url.pathname)).replace(/^(\.\.[/\\])+/, '');
    const file = join(ROOT, rel === '/' ? 'demo/index.html' : rel);
    const body = await readFile(file);
    res.writeHead(200, {
      'Content-Type': TYPES[extname(file)] ?? 'application/octet-stream',
      // duckdb-wasm is happier with these, and they cost nothing here.
      'Cross-Origin-Opener-Policy': 'same-origin',
      'Cross-Origin-Embedder-Policy': 'require-corp',
      'Cross-Origin-Resource-Policy': 'cross-origin',
    });
    res.end(body);
  } catch {
    res.writeHead(404).end('not found');
  }
});

await new Promise((r) => server.listen(PORT, r));

const browser = await chromium.launch();
const page = await browser.newPage();
const problems = [];
page.on('console', (m) => {
  const text = m.text();
  // The demo probes for legend-lite on :8080 and falls back when it is
  // absent. The browser logs that refused connection regardless of the
  // fetch being caught, so it is expected noise, not a defect.
  const expected = /ERR_CONNECTION_REFUSED|Failed to load resource/.test(text)
    && /8080|localhost/.test(text + (m.location()?.url ?? ''));
  if (m.type() === 'error' && !expected) problems.push(`console: ${text}`);
});
page.on('pageerror', (e) => problems.push(`pageerror: ${e.message}`));

let failed = false;
const check = (name, ok, detail = '') => {
  console.log(`${ok ? 'ok  ' : 'FAIL'}  ${name}${detail ? ` — ${detail}` : ''}`);
  if (!ok) failed = true;
};

try {
  await page.goto(`http://localhost:${PORT}/demo/index.html`, {
    waitUntil: 'domcontentloaded',
  });

  // The grid only renders once DuckDB has booted and the first query
  // has come back, so waiting for a cell waits for the whole stack.
  await page.waitForSelector('.dc-row .dc-cell', { timeout: 120_000 });

  const status = await page.textContent('#status');
  check('boots and renders', true, status?.trim());

  const rows = await page.locator('.dc-row').count();
  check('renders a window, not every row', rows > 0 && rows < 60, `${rows} rows`);

  const headers = await page.locator('[role="columnheader"]').allTextContents();
  check(
    'pivot header carries the years',
    headers.some((h) => /^20\d\d$/.test(h.trim())),
    headers.slice(0, 8).join(' | '),
  );

  const rowcount = await page.getAttribute('[role="treegrid"]', 'aria-rowcount');
  check('announces a row count', Number(rowcount) > 0, `aria-rowcount=${rowcount}`);

  // Row 0 is the grand total and deliberately carries no dimension
  // label, so the first LABELLED dimension cell belongs to row 1.
  const dimLabels = (await page.locator('.dc-cell.dc-dim').allTextContents())
    .map((t) => t.trim())
    .filter(Boolean);
  check('row dimensions are populated', dimLabels.length > 0, dimLabels.join(', '));

  const rowCount = await page.locator('.dc-row').count();
  check(
    'collapsed tree shows the total plus the top level only',
    rowCount === 4,
    `${rowCount} rows (1 total + 3 regions)`,
  );

  check(
    'the grand total row is marked as one',
    (await page.locator('.dc-row.dc-total').count()) >= 1,
  );

  // The property the whole design rests on, read off the screen.
  const money = (t) => Number(t.replace(/[^0-9.-]/g, ''));
  const col2021 = async (row) =>
    money(
      (await page
        .locator('.dc-row')
        .nth(row)
        .locator('.dc-cell:not(.dc-dim)')
        .first()
        .textContent()) ?? '0',
    );
  const grand = await col2021(0);
  const kids = (await Promise.all([col2021(1), col2021(2), col2021(3)]))
    .reduce((a, b) => a + b, 0);
  check(
    'the grand total equals the sum of its children',
    Math.abs(grand - kids) <= 1,
    `${grand} vs ${kids}`,
  );

  const values = await page.locator('.dc-row').first()
    .locator('.dc-cell:not(.dc-dim)').allTextContents();
  const filled = values.filter((v) => v.trim().length > 0);
  check(
    'measures are formatted as currency',
    filled.length > 0 && filled.every((v) => /^\(?\$/.test(v.trim())),
    values.join(' | '),
  );
  // Correlated synthetic dimensions once made four of five pivot
  // columns null, which looks like a rendering bug and is not one.
  check(
    'the pivot is dense, not accidentally sparse',
    filled.length === values.length,
    `${filled.length}/${values.length} populated`,
  );

  // Expanding a group fetches its children and inlines them.
  await page.locator('.dc-row').nth(1).locator('.dc-cell').first().click();
  await page.locator('[role="treegrid"]').focus();
  // Move focus to the first region row, then open it with ArrowRight
  // as the APG treegrid pattern specifies.
  await page.keyboard.press('ArrowDown');
  await page.keyboard.press('ArrowRight');
  await page.waitForFunction(
    () => document.querySelectorAll('.dc-row').length > 4,
    { timeout: 60_000 },
  );
  const expanded = await page.locator('.dc-row').count();
  check(
    'ArrowRight expands a group and inlines its children',
    expanded === 9,
    `${expanded} rows (1 total + 3 regions + 5 desks)`,
  );
  check(
    'the expanded group reports aria-expanded',
    (await page.locator('.dc-row[aria-expanded="true"]').count()) === 1,
  );

  await page.keyboard.press('ArrowLeft');
  await page.waitForFunction(
    () => document.querySelectorAll('.dc-row').length === 4,
    { timeout: 60_000 },
  );
  check('ArrowLeft collapses it again', true, '4 rows');

  // Keyboard: focus the grid and move.
  await page.locator('[role="treegrid"]').focus();
  await page.keyboard.press('ArrowDown');
  await page.keyboard.press('ArrowRight');
  const focused = await page.locator('.dc-cell.dc-focus').count();
  check('keyboard moves a single focus cell', focused === 1, `${focused} focused`);

  // Snap mode.
  await page.click('#snap');
  await page.waitForFunction(
    () => document.getElementById('plane')?.className.includes('snapped'),
    { timeout: 120_000 },
  );
  const badge = await page.textContent('#plane');
  check('snap freezes and labels the plane', /frozen at/.test(badge ?? ''), badge?.trim());

  await page.click('#snap');
  await page.waitForFunction(
    () => document.getElementById('plane')?.className.includes('live'),
    { timeout: 120_000 },
  );
  check('returns to live', true, (await page.textContent('#plane'))?.trim());

  // Change the measure; the view must refresh without error.
  await page.selectOption('#measure', 'average');
  await page.waitForTimeout(2500);
  const afterAvg = await page.textContent('#status');
  check('measure change refreshes', !/error/i.test(afterAvg ?? ''), afterAvg?.trim());

  await page.screenshot({ path: 'demo/screenshot.png', fullPage: true });
  check('screenshot written', true, 'demo/screenshot.png');

  check('no console or page errors', problems.length === 0, problems.join(' ; '));
} catch (e) {
  check('demo verification', false, e instanceof Error ? e.message : String(e));
} finally {
  await browser.close();
  server.close();
}

process.exit(failed ? 1 : 0);
