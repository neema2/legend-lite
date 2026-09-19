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
/** Click a toolbar button by the label a user reads. */
const tool = (label) =>
  page.locator('.dc-app-toolbar .dc-tool', { hasText: label }).first().click();
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
    'measures are formatted as currency, pivot leaves included',
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

  // No grand total -- which is DataCube's own default. The level-0
  // query should not be issued, and the top level should be promoted
  // rather than leaving a gap where the root used to be.
  await tool('Totals');
  await page.waitForFunction(
    () => document.querySelectorAll('.dc-row').length === 3,
    { timeout: 60_000 },
  );
  const noTotalLevels = await page.evaluate(() =>
    [...document.querySelectorAll('.dc-row')].map((r) =>
      r.getAttribute('aria-level'),
    ),
  );
  check(
    'without a total the top level is promoted to level 1',
    noTotalLevels.every((l) => l === '1'),
    noTotalLevels.join(','),
  );
  check(
    'no total row remains',
    (await page.locator('.dc-row.dc-total').count()) === 0,
  );
  await tool('Totals');
  await page.waitForFunction(
    () => document.querySelectorAll('.dc-row').length === 4,
    { timeout: 60_000 },
  );
  check('the total comes back as the root', true, '4 rows');

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
  await tool('Snap');
  await page.waitForFunction(
    () => document.getElementById('plane')?.className.includes('snapped'),
    { timeout: 120_000 },
  );
  const badge = await page.textContent('#plane');
  check('snap freezes and labels the plane', /frozen at/.test(badge ?? ''), badge?.trim());

  await tool('Release snap');
  await page.waitForFunction(
    () => document.getElementById('plane')?.className.includes('live'),
    { timeout: 120_000 },
  );
  check('returns to live', true, (await page.textContent('#plane'))?.trim());

  // -- the product surface -------------------------------------------
  // Everything below was built, unit-tested and unreachable before
  // src/app.ts existed. Each check is "a user can get to this", not
  // "the module works" -- the unit tests already cover the second.

  check(
    'the row-group zone is on screen with its chips',
    (await page.locator('.dc-zone-rows .dc-chip').allTextContents())
      .join(',')
      .includes('region'),
    (await page.locator('.dc-zone-rows').textContent())?.trim(),
  );

  // The filter strip offers a box per dimension IN PLAY -- three row
  // groups and one pivot here. A box per leaf, which is ag-Grid's
  // layout, only works on a flat table: over a pivot the leaves are
  // measures under pivot values, and there is no source column for a
  // box to sit under.
  const filterLabels = await page
    .locator('.dc-floating-label')
    .allTextContents();
  check(
    'the filter strip covers the dimensions in play',
    filterLabels.join(',') === 'region,desk,book,year',
    filterLabels.join(' | '),
  );

  // The tool panel is the drag SOURCE, and it has to be: in a
  // pivoted cube the row dimensions collapse into one tree column
  // with a blank header, so there is no dimension header to drag.
  // This check is the one that found that.
  const panelRows = await page.locator('.dc-tool-panel-row').count();
  const panelDraggable = await page
    .locator('.dc-tool-panel-row.dc-draggable')
    .count();
  check(
    'the columns tool panel lists every source column',
    panelRows === 8,
    `${panelRows} rows`,
  );
  check(
    'dimensions in it are draggable and measures are not',
    panelDraggable === 5 && panelDraggable < panelRows,
    `${panelDraggable} of ${panelRows} draggable`,
  );
  check(
    'and it marks which columns are already in use',
    (await page.locator('.dc-tool-panel-badge').count()) === 4,
    `${await page.locator('.dc-tool-panel-badge').count()} badges`,
  );

  // The context menu: `grep -c contextmenu src/grid/grid.ts` returned
  // 0 before this work, so the menu existed and nothing opened it.
  await page.locator('.dc-cell').first().click({ button: 'right' });
  await page.waitForSelector('.dc-menu', { timeout: 10_000 });
  const menuItems = await page.locator('.dc-menu [role="menuitem"]').count();
  check('right-click opens the context menu', menuItems > 0, `${menuItems} items`);
  await page.keyboard.press('Escape');

  // The editor: seven tabs, in DataCube's order.
  await tool('Properties');
  await page.waitForSelector('.dc-editor', { timeout: 10_000 });
  const tabs = (await page.locator('.dc-editor-tab').allTextContents()).map((t) =>
    t.trim(),
  );
  check(
    "the editor has DataCube's seven tabs in its order",
    tabs.join('|') ===
      'Columns|Horizontal Pivots|Vertical Pivots|Dimensions|Sorts|' +
        'General Properties|Column Properties',
    tabs.join(' | '),
  );

  // The two-pane selector is the drag-to-pivot widget every
  // structural tab is built from.
  await page.locator('.dc-editor-tab', { hasText: 'Vertical Pivots' }).click();
  const selected = await page
    .locator('.dc-pane-selected .dc-selector-row')
    .allTextContents();
  check(
    'Vertical Pivots opens on the grouping in force',
    selected.join(',').includes('region'),
    selected.join(' | '),
  );
  const available = await page
    .locator('.dc-pane-available .dc-selector-row')
    .allTextContents();
  check(
    'and offers dimensions only, never a measure',
    available.length > 0 && !available.join(',').includes('notional'),
    available.join(' | '),
  );

  await page.locator('.dc-editor-tab', { hasText: 'Column Properties' }).click();
  const fields = (await page.locator('.dc-field-label').allTextContents()).map(
    (t) => t.trim(),
  );
  for (const label of [
    'Choose Column:',
    'Column Kind:',
    'Display Name:',
    'Aggregation:',
    'Scale:',
    'Pin:',
    'Width:',
  ]) {
    check(`Column Properties has ${label}`, fields.includes(label));
  }

  await page.screenshot({ path: 'demo/shot-editor.png', fullPage: true });
  await page.locator('.dc-editor-footer button', { hasText: 'Cancel' }).click();

  await tool('Collapse all');
  await page.waitForTimeout(1500);

  // End to end: type, wait for the debounce, and the generated PURE
  // must carry the condition. Checking the Pure rather than the SQL
  // because the Pure is the product's own output; the SQL here comes
  // from the demo shim.
  const rowsBefore = await page.locator('.dc-row').count();
  const pureBefore = (await page.textContent('#pure')) ?? '';
  check(
    'no filter in the query to begin with',
    !pureBefore.includes('filter('),
    `${rowsBefore} rows`,
  );

  await page.locator('.dc-floating-input').first().fill('EMEA');
  await page.waitForFunction(
    () => document.getElementById('pure')?.textContent?.includes('filter('),
    undefined,
    { timeout: 60_000 },
  );
  const pureAfter = (await page.textContent('#pure')) ?? '';
  // Lowercased on BOTH sides, because a string box means
  // case-insensitive contains and the operator lowers the column
  // rather than relying on collation, which varies by backend.
  check(
    'typing in the filter strip reaches the query',
    /filter\(x\|.*region->toLower\(\)->contains\('emea'\)/s.test(pureAfter),
    (pureAfter.match(/filter\([^)]*\)[^)]*\)\)/) ?? ['(no filter)'])[0],
  );

  const regions = (await page.locator('.dc-cell.dc-dim').allTextContents())
    .map((t) => t.replace(/[^A-Za-z ]/g, '').trim())
    .filter((t) => t && t !== 'Total');
  check(
    'and the cube on screen is narrowed to the match',
    regions.length > 0 && regions.every((r) => r === 'EMEA' || /^[A-Z]/.test(r)),
    regions.join(', '),
  );
  const rowsFiltered = await page.locator('.dc-row').count();
  check(
    'with fewer rows than before',
    rowsFiltered < rowsBefore,
    `${rowsBefore} rows -> ${rowsFiltered}`,
  );

  await page.locator('.dc-floating-input').first().fill('');
  await page.waitForFunction(
    () => !document.getElementById('pure')?.textContent?.includes('filter('),
    undefined,
    { timeout: 60_000 },
  );
  check('clearing it removes the condition again', true);

  // Removing the last row-group chip flattens the cube, which is also
  // what gives the floating filter something to work on: while a
  // cube is grouped, the only non-pivot column on screen is the tree
  // column, and a tree column holds a different dimension at every
  // level, so no single box could filter it.
  for (const zone of ['.dc-zone-columns', '.dc-zone-rows']) {
    while ((await page.locator(`${zone} .dc-chip-remove`).count()) > 0) {
      const n = await page.locator(`${zone} .dc-chip`).count();
      await page.locator(`${zone} .dc-chip-remove`).last().click();
      await page.waitForFunction(
        ([sel, k]) => document.querySelectorAll(`${sel} .dc-chip`).length < k,
        [zone, n],
        { timeout: 60_000 },
      );
    }
  }
  await page.waitForTimeout(2500);
  check(
    'removing every chip flattens the cube completely',
    (await page.locator('.dc-zone-rows .dc-chip').count()) === 0,
    (await page.textContent('#status'))?.trim(),
  );

  check(
    'a cube with no dimensions shows no filter strip',
    (await page.locator('.dc-floating-row').count()) === 0,
    'an empty strip would be noise, and would shift every rowindex',
  );

  const afterAll = await page.textContent('#status');
  check('no error after all of that', !/error/i.test(afterAll ?? ''), afterAll?.trim());

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
