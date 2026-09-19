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
/** Open the grid's right-click menu, optionally over an element. */
const rightClick = async (target = '.dc-app-grid') => {
  const loc = typeof target === 'string' ? page.locator(target) : target;
  await loc.first().click({ button: 'right' });
  await page.waitForSelector('.dc-menu', { timeout: 10_000 });
};
/** The dimension cell of a group row, which carries a real value. */
const groupLabelCell = () =>
  page.locator('.dc-row').nth(1).locator('.dc-cell.dc-dim');
/**
 * Click a menu entry by the words a user reads.
 *
 * Focuses it first: a submenu entry is not VISIBLE until its parent
 * opens, and the CSS opens one on hover or on focus-within -- so
 * focusing walks the chain open exactly as a keyboard user would,
 * and Playwright can then click something that exists on screen.
 */
const pick = async (label) => {
  const found = await page.evaluate((text) => {
    const el = [...document.querySelectorAll('.dc-menu .dc-menu-label')].find(
      (e) => e.textContent === text,
    );
    const item = el?.parentElement;
    if (!item) return false;
    item.focus();
    item.click();
    return true;
  }, label);
  if (!found) throw new Error(`no menu entry "${label}"`);
};
/** Do something from the grid's right-click menu. */
const fromMenu = async (label, selector) => {
  await rightClick(selector);
  await pick(label);
};
/** Do something from the title bar's hamburger. */
const fromTitleMenu = async (label) => {
  await page.locator('.dc-titlebar-menu').click();
  await page.waitForSelector('.dc-menu', { timeout: 10_000 });
  await pick(label);
};
const check = (name, ok, detail = '') => {
  console.log(`${ok ? 'ok  ' : 'FAIL'}  ${name}${detail ? ` — ${detail}` : ''}`);
  if (!ok) failed = true;
};

try {
  // The engine is a PRECONDITION, not a nice-to-have: the page has no
  // fallback planner, so without it there is nothing to verify. Say
  // so immediately rather than timing out on a grid that will never
  // appear.
  try {
    const health = await fetch('http://localhost:8080/health', {
      signal: AbortSignal.timeout(2000),
    });
    if (!health.ok) throw new Error(`health returned ${health.status}`);
  } catch (e) {
    check(
      'legend-lite is running on :8080',
      false,
      `${e instanceof Error ? e.message : String(e)} -- start it with \`npm run engine\``,
    );
    throw new Error('engine unavailable');
  }
  check('legend-lite is running on :8080', true);

  await page.goto(`http://localhost:${PORT}/demo/index.html`, {
    waitUntil: 'domcontentloaded',
  });

  // The grid only renders once DuckDB has booted and the first query
  // has come back, so waiting for a cell waits for the whole stack.
  await page.waitForSelector('.dc-row .dc-cell', { timeout: 120_000 });

  const status = await page.textContent('#status');
  check('boots and renders', true, status?.trim());

  // There is one planner and it is the real one. If the engine is
  // absent the page refuses to render at all, so reaching this far
  // already proves the path -- but say it, and check the SQL has
  // legend-lite's shape rather than something locally invented.
  check(
    'planning through legend-lite, with no fallback available',
    await page.locator('#plannerreal').isVisible(),
  );
  const sql = (await page.textContent('#sql')) ?? '';
  check(
    "the SQL has legend-lite's shape",
    /AS t\d+/.test(sql) && /PIVOT \(/.test(sql),
    sql.split('\n')[0],
  );

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

  // -- the style contract ------------------------------------------------
  // "Pixel identical to DataCube" is unfalsifiable on its own: there
  // is no reference render here to diff against. So the goal is
  // stated as RESOLVED VALUES instead -- each derived from their
  // stylesheet, their pinned ag-grid version, or their Tailwind
  // config -- and measured from the live page. That can fail.
  const styleOf = (selector, props) =>
    page.evaluate(
      ([sel, wanted]) => {
        const el = document.querySelector(sel);
        if (!el) return null;
        const cs = getComputedStyle(el);
        const out = {};
        for (const p of wanted) out[p] = cs.getPropertyValue(p);
        out.height = Math.round(el.getBoundingClientRect().height);
        return out;
      },
      [selector, props],
    );

  const grid = await styleOf('.dc-grid', [
    'font-family',
    'font-size',
    'color',
    'border-radius',
    'border-bottom-color',
  ]);
  check(
    'the grid is 12px Roboto',
    grid?.['font-size'] === '12px' && /Roboto/.test(grid?.['font-family'] ?? ''),
    `${grid?.['font-size']} ${grid?.['font-family']}`,
  );
  check(
    'cell text is quartz #181d1f, which they do NOT override to black',
    grid?.color === 'rgb(24, 29, 31)',
    grid?.color,
  );
  check(
    'every radius is zeroed, as their three ag radius vars are',
    grid?.['border-radius'] === '0px',
    grid?.['border-radius'],
  );

  const bodyRow = await styleOf('.dc-row', []);
  check(
    'body rows are 20px (--ag-row-height)',
    bodyRow?.height === 20,
    `${bodyRow?.height}px`,
  );

  // A LEAF header cell. The tree column's header spans the whole
  // header depth, so measuring the first cell measures 2 x 24.
  const headRow = await page.evaluate(() => {
    // Not the tree column, whose header spans the whole depth --
    // a spanning cell carries aria-rowspan.
    const el = document.querySelector('.dc-th[data-column]:not([aria-rowspan])');
    return el ? Math.round(el.getBoundingClientRect().height) : null;
  });
  check(
    'header cells are 24px (--ag-header-height)',
    headRow === 24,
    `${headRow}px`,
  );

  const cell = await styleOf('.dc-cell:not(.dc-dim)', [
    'padding-left',
    'padding-right',
    'border-right-color',
  ]);
  const rowBorder = await styleOf('.dc-row', ['border-bottom-width']);
  check(
    'cell padding is 2px -- grid-size 1px, doubled by quartz',
    cell?.['padding-left'] === '2px' && cell?.['padding-right'] === '2px',
    `${cell?.['padding-left']} / ${cell?.['padding-right']}`,
  );
  // Their defaults: horizontal lines OFF, vertical ON in
  // neutral-300 -- and the frame stays neutral-200 regardless, so
  // recolouring the lines does not move the frame.
  check(
    'horizontal grid lines are off by default',
    rowBorder?.['border-bottom-width'] === '0px',
    rowBorder?.['border-bottom-width'],
  );
  check(
    'vertical grid lines are on, in neutral-300',
    cell?.['border-right-color'] === 'rgb(212, 212, 212)',
    cell?.['border-right-color'],
  );
  check(
    'and the structural frame stays neutral-200',
    grid?.['border-bottom-color'] === 'rgb(229, 229, 229)',
    grid?.['border-bottom-color'],
  );

  const th = await styleOf('.dc-th', ['background-color', 'color', 'font-weight']);
  check(
    'the header is neutral-100 on black at weight 500',
    th?.color === 'rgb(0, 0, 0)' && th?.['font-weight'] === '500',
    `${th?.['background-color']} ${th?.color} ${th?.['font-weight']}`,
  );

  // Their five-colour rotation over pivot value groups.
  const groups = await page.evaluate(() =>
    [...document.querySelectorAll(".dc-th[class*='dc-pivot-group-']")].map(
      (el) => getComputedStyle(el).backgroundImage,
    ),
  );
  check(
    'pivot groups rotate through five gradients',
    groups.length === 5 && new Set(groups).size === 5,
    `${groups.length} groups, ${new Set(groups).size} distinct`,
  );

  // A deliberate departure from DataCube, asked for directly: on a
  // dense 20px grid the banding is what lets the eye track a row
  // across a wide pivot.
  check(
    'alternate row banding is ON by default',
    (await page.locator('.dc-row.dc-alt').count()) > 0,
    `${await page.locator('.dc-row.dc-alt').count()} banded rows`,
  );

  // The chrome, on their CUSTOM Tailwind scale -- the trap that would
  // otherwise make every panel about 40% too large.
  await fromMenu('Properties...');
  await page.waitForSelector('.dc-editor', { timeout: 10_000 });
  const title = await styleOf('.dc-panel-title', ['font-size']);
  check(
    'a panel title is text-xl = 16px, not the stock 20px',
    title?.['font-size'] === '16px',
    title?.['font-size'],
  );
  const selectorRow = await page.evaluate(() => {
    const el = document.querySelector('.dc-selector-row');
    return el ? Math.round(el.getBoundingClientRect().height) : null;
  });
  check(
    'selector rows are 20px, their selector grid row height',
    selectorRow === 20,
    `${selectorRow}px`,
  );

  await page.locator('.dc-editor-tab', { hasText: 'General Properties' }).click();
  await page.waitForTimeout(150);
  const sectionTitle = await styleOf('.dc-section-title', ['font-size']);
  check(
    'a section label is text-sm = 10px, not the stock 14px',
    sectionTitle?.['font-size'] === '10px',
    sectionTitle?.['font-size'],
  );
  const tab = await styleOf('.dc-editor-tab', []);
  check('editor tabs are h-6 = 24px', tab?.height === 24, `${tab?.height}px`);
  await page.locator('.dc-editor-footer button', { hasText: 'Cancel' }).click();

  // No grand total. In DataCube this is a SETTING -- "Show root
  // aggregation" in General Properties -- not a menu action, so it
  // is driven the way a user would drive it. The level-0 query
  // should not be issued, and the top level should be promoted
  // rather than leaving a gap where the root used to be.
  const setRootAggregation = async (on) => {
    await fromMenu('Properties...');
    await page
      .locator('.dc-editor-tab', { hasText: 'General Properties' })
      .click();
    const box = page.locator('.dc-check', {
      hasText: 'Show root aggregation',
    }).locator('input');
    if ((await box.isChecked()) !== on) await box.click();
    await page.locator('.dc-editor-footer button', { hasText: 'OK' }).click();
  };
  await setRootAggregation(false);
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
  await setRootAggregation(true);
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
  await page.locator('.dc-titlebar-toggle').click();
  await page.waitForFunction(
    () => document.getElementById('plane')?.className.includes('snapped'),
    { timeout: 120_000 },
  );
  const badge = await page.textContent('#plane');
  check('snap freezes and labels the plane', /frozen at/.test(badge ?? ''), badge?.trim());

  await page.locator('.dc-titlebar-toggle').click();
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

  // The filter boxes over the grid are GONE. DataCube has none --
  // grepping legend-data-cube for `floatingFilter` returns nothing
  // -- and everything they do lives in the right-click menu.
  check(
    'there is no filter row over the grid',
    (await page.locator('.dc-floating-row').count()) === 0,
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

  // It NESTS. Their menu is eight verbs with their variants tucked
  // underneath, not a flat wall of sixteen entries.
  const tops = await page.evaluate(() =>
    [...document.querySelectorAll('.dc-menu > [role="menuitem"]')].map((el) => ({
      label: el.querySelector('.dc-menu-label')?.textContent ?? '',
      sub: el.querySelectorAll(':scope > .dc-submenu > [role="menuitem"]').length,
    })),
  );
  check(
    'the menu nests, with a submenu per verb',
    tops.filter((t) => t.sub > 0).length === 8,
    tops.map((t) => `${t.label}${t.sub ? `(${t.sub})` : ''}`).join(' '),
  );

  // A submenu opens on hover, and is clipped by nothing.
  const filterTop = page
    .locator('.dc-menu > [role="menuitem"]', { hasText: 'Filter' })
    .first();
  await filterTop.hover();
  await page.waitForTimeout(200);
  const subVisible = await filterTop
    .locator('.dc-submenu [role="menuitem"]')
    .first()
    .isVisible();
  check('a submenu opens on hover', subVisible);

  await page.keyboard.press('Escape');

  // The third level lives under "More Filters on X...", which only
  // appears when there is a VALUE under the pointer -- so this has
  // to right-click a real group label, not empty grid.
  await rightClick(groupLabelCell());
  const deeper = await page.evaluate(() =>
    [
      ...document.querySelectorAll('.dc-submenu .dc-submenu [role="menuitem"]'),
    ].map((el) => el.querySelector('.dc-menu-label')?.textContent ?? ''),
  );
  check(
    'a third level under More Filters, typed to the column',
    deeper.length === 11,
    `${deeper.length}: ${deeper.slice(0, 3).join(', ')}...`,
  );
  await page.keyboard.press('Escape');

  // The editor: seven tabs, in DataCube's order.
  await fromMenu('Properties...');
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

  await fromMenu('Collapse All');
  await page.waitForTimeout(1500);

  // End to end through the FILTER EDITOR, which is now the only
  // way in -- opened from the grid's own menu, as DataCube does.
  const pureBefore = (await page.textContent('#pure')) ?? '';
  check(
    'no filter in the query to begin with',
    !pureBefore.includes('filter('),
  );

  // The value-aware entry first: right-click a group label and the
  // menu offers a filter on THAT value, resolved to the dimension
  // at that row's level.
  await rightClick(groupLabelCell());
  const valueEntry = await page
    .locator('.dc-menu .dc-menu-label')
    .filter({ hasText: /^Add Filter: region = / })
    .first()
    .textContent();
  check(
    'the menu offers a filter on the clicked VALUE',
    /^Add Filter: region = \w+$/.test((valueEntry ?? '').trim()),
    valueEntry?.trim(),
  );
  await page.evaluate(() => {
    const el = [...document.querySelectorAll('.dc-menu .dc-menu-label')].find(
      (e) => /^Add Filter: region = /.test(e.textContent ?? ''),
    );
    el?.parentElement?.click();
  });
  await page.waitForFunction(
    () => document.getElementById('pure')?.textContent?.includes('filter('),
    { timeout: 60_000 },
  );
  check(
    'and clicking it filters the cube',
    /filter\(x\|\$x\.region == /.test(
      (await page.textContent('#pure')) ?? '',
    ),
    ((await page.textContent('#pure')) ?? '').match(/filter\(x\|[^)]*\)/)?.[0],
  );
  await fromMenu('Clear All Filters');
  await page.waitForFunction(
    () => !document.getElementById('pure')?.textContent?.includes('filter('),
    { timeout: 60_000 },
  );

  await fromMenu('Filters...');
  await page.waitForSelector('.dc-filters', { timeout: 10_000 });
  await page.locator('.dc-filter-btn', { hasText: 'Create New Filter' }).click();
  await page.locator('.dc-filter-value').first().fill('EMEA');
  await page.locator('.dc-filter-value').first().dispatchEvent('change');
  await page.waitForFunction(
    () => document.getElementById('pure')?.textContent?.includes('filter('),
    { timeout: 60_000 },
  );
  const pureAfter = (await page.textContent('#pure')) ?? '';
  check(
    'the filter editor reaches the query',
    /filter\(x\|\$x\.region == 'EMEA'\)/.test(pureAfter),
    (pureAfter.match(/filter\(x\|[^\n]{0,80}/) ?? ['(no filter)'])[0],
  );

  // Clear All Filters is a menu entry of theirs, so use it.
  await page.locator('.dc-overlay-close').click();
  await fromMenu('Clear All Filters');
  await page.waitForFunction(
    () => !document.getElementById('pure')?.textContent?.includes('filter('),
    { timeout: 60_000 },
  );
  check('and Clear All Filters takes it away again', true);

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
    'a cube with no dimensions shows no filter row',
    (await page.locator('.dc-floating-row').count()) === 0,
    'an empty strip would be noise, and would shift every rowindex',
  );

  // -- the filter tree lines up ----------------------------------------
  // The operator word sits in a fixed-width lead so it cannot push
  // the row's controls. `and` is wider than `or`, so it is the case
  // that fails first if the box ever goes back into the flow. This
  // measures real geometry rather than trusting the markup.
  await fromMenu('Filters...');
  await page.waitForSelector('.dc-filters', { timeout: 10_000 });
  await page.locator('.dc-filter-btn', { hasText: 'Create New Filter' }).click();
  await page.locator('.dc-filter-row').nth(1).locator('.dc-filter-ctl').first().click();
  await page.locator('.dc-filter-row').nth(1).locator('.dc-filter-ctl').first().click();
  await page.waitForTimeout(300);

  /** Left edge of each sibling's column dropdown, in device pixels. */
  const columnLefts = async () =>
    page.evaluate(() =>
      [...document.querySelectorAll('.dc-filter-row .dc-filter-column')].map(
        (el) => Math.round(el.getBoundingClientRect().left),
      ),
    );

  const andLefts = await columnLefts();
  check(
    'three siblings joined by "and" line up',
    andLefts.length === 3 && new Set(andLefts).size === 1,
    `lefts: ${andLefts.join(', ')}`,
  );

  await page
    .locator('.dc-filter-row.dc-filter-group')
    .first()
    .locator('.dc-filter-join')
    .selectOption('or');
  await page.waitForTimeout(300);
  const orLefts = await columnLefts();
  check(
    'and they still line up as "or"',
    orLefts.length === 3 && new Set(orLefts).size === 1,
    `lefts: ${orLefts.join(', ')}`,
  );
  check(
    'switching the operator does not move the row either',
    andLefts[0] === orLefts[0],
    `${andLefts[0]} vs ${orLefts[0]}`,
  );

  // Nested: a sub-group's own children must line up with each other
  // too, at their deeper indent.
  await page.locator('.dc-filter-row').nth(3).locator('.dc-filter-ctl').nth(2).click();
  await page.waitForTimeout(200);
  await page.locator('.dc-filter-row').nth(4).locator('.dc-filter-ctl').first().click();
  await page.waitForTimeout(300);
  const nested = await page.evaluate(() => {
    const rows = [...document.querySelectorAll('.dc-filter-row')];
    return rows
      .map((r) => ({
        level: Number(r.style.getPropertyValue('--dc-filter-level')),
        left: r.querySelector('.dc-filter-column')
          ? Math.round(
              r.querySelector('.dc-filter-column').getBoundingClientRect().left,
            )
          : null,
      }))
      .filter((x) => x.left !== null);
  });
  const byLevel = new Map();
  for (const { level, left } of nested) {
    byLevel.set(level, (byLevel.get(level) ?? new Set()).add(left));
  }
  check(
    'nested siblings line up at their own depth',
    [...byLevel.values()].every((set) => set.size === 1),
    [...byLevel.entries()]
      .map(([l, set]) => `L${l}: ${[...set].join('/')}`)
      .join('  '),
  );
  check(
    'and a deeper level is indented further than its parent',
    [...byLevel.keys()].length > 1 &&
      Math.min(...byLevel.get(2)) > Math.min(...byLevel.get(1)),
    [...byLevel.entries()]
      .map(([l, set]) => `L${l}=${Math.min(...set)}`)
      .join(' '),
  );

  await page.locator('.dc-overlay-close').click();

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
