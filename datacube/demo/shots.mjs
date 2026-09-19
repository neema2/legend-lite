// Capture the product surface, one state per file.
//
// Separate from verify.mjs on purpose: that file ASSERTS and exits
// non-zero, this one only looks. Mixing them would mean a screenshot
// run that fails the build, or assertions nobody reads because the
// pictures were what was wanted.
//
// One boot, many shots. DuckDB takes seconds to start and generate
// 200k rows, so every state is reached by driving the page rather
// than reloading it.
//
// Run: node demo/shots.mjs   (after npm run build:demo)
import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
import { extname, join, normalize } from 'node:path';
import { fileURLToPath } from 'node:url';

import { chromium } from 'playwright';

const ROOT = fileURLToPath(new URL('..', import.meta.url));
const OUT = 'demo/shots';
const PORT = 8732;
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
    const rel = normalize(decodeURIComponent(url.pathname)).replace(
      /^(\.\.[/\\])+/,
      '',
    );
    const file = join(ROOT, rel === '/' ? 'demo/index.html' : rel);
    const body = await readFile(file);
    res.writeHead(200, {
      'Content-Type': TYPES[extname(file)] ?? 'application/octet-stream',
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
const page = await browser.newPage({ viewport: { width: 1280, height: 900 } });

let n = 0;
/** Shoot an element if given one, else the page. */
const shot = async (name, selector) => {
  n += 1;
  const file = `${OUT}/${String(n).padStart(2, '0')}-${name}.png`;
  const target = selector ? page.locator(selector).first() : page;
  await target.screenshot({ path: file, ...(selector ? {} : { fullPage: true }) });
  console.log(`  ${file}`);
};

const rightClick = async (selector = '.dc-app-grid') => {
  await page.locator(selector).first().click({ button: 'right' });
  await page.waitForSelector('.dc-menu', { timeout: 10_000 });
};
/**
 * Click a menu entry by the words a user reads.
 *
 * Focuses it first: a submenu entry is not VISIBLE until its parent
 * opens, and the CSS opens one on hover or on focus-within.
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
const tool = async (label) => {
  const viaMenu = {
    'Properties': 'Properties...',
    'Filters': 'Filters...',
    'Collapse all': 'Collapse All',
  };
  await rightClick();
  await pick(viaMenu[label] ?? label);
};
const tab = (label) =>
  page.locator('.dc-editor-tab', { hasText: label }).first().click();
const field = (label) =>
  page
    .locator('.dc-field')
    .filter({ has: page.locator('.dc-field-label', { hasText: label }) })
    .first();

try {
  await page.goto(`http://localhost:${PORT}/demo/index.html`, {
    waitUntil: 'domcontentloaded',
  });
  await page.waitForSelector('.dc-row .dc-cell', { timeout: 120_000 });
  console.log('booted; capturing');

  // -- the cube itself ----------------------------------------------
  await shot('cube');
  await shot('chrome', '.dc-app');

  // Expanded, so the tree column and the subtotals are visible.
  await page.locator('[role="treegrid"]').focus();
  await page.keyboard.press('ArrowDown');
  await page.keyboard.press('ArrowRight');
  await page.waitForFunction(
    () => document.querySelectorAll('.dc-row').length > 4,
    { timeout: 60_000 },
  );
  await shot('tree-expanded', '.dc-app-middle');

  // -- the context menu ----------------------------------------------
  // Over a GROUP LABEL, so the value-aware filter entries are the
  // ones a user would actually meet.
  await page.locator('.dc-row').nth(1).locator('.dc-cell.dc-dim').click({
    button: 'right',
  });
  await page.waitForSelector('.dc-menu', { timeout: 10_000 });
  await shot('context-menu');

  // Its submenus, which is the shape of their menu: eight verbs
  // with their variants underneath, and a third level under
  // "More Filters on...".
  await page.evaluate(() => {
    const open = (text) =>
      [...document.querySelectorAll('.dc-menu .dc-menu-label')]
        .find((e) => e.textContent === text)
        ?.parentElement?.focus();
    open('Filter');
  });
  await page.waitForTimeout(150);
  await shot('context-menu-filter-submenu');
  await page.evaluate(() => {
    const el = [...document.querySelectorAll('.dc-menu .dc-menu-label')].find(
      (e) => (e.textContent ?? '').startsWith('More Filters on'),
    );
    el?.parentElement?.focus();
  });
  await page.waitForTimeout(150);
  await shot('context-menu-third-level');
  await page.keyboard.press('Escape');

  // -- selection statistics -------------------------------------------
  await page.locator('.dc-row').nth(1).locator('.dc-cell').nth(1).click();
  await page.locator('.dc-row').nth(3).locator('.dc-cell').nth(3).click({
    modifiers: ['Shift'],
  });
  await page.waitForTimeout(300);
  await shot('selection-stats', '.dc-app');

  // -- the seven editor tabs --------------------------------------------
  await tool('Properties');
  await page.waitForSelector('.dc-editor', { timeout: 10_000 });
  for (const [label, name] of [
    ['Columns', 'editor-columns'],
    ['Horizontal Pivots', 'editor-horizontal-pivots'],
    ['Vertical Pivots', 'editor-vertical-pivots'],
    ['Sorts', 'editor-sorts'],
    ['General Properties', 'editor-general-properties'],
    ['Column Properties', 'editor-column-properties'],
  ]) {
    await tab(label);
    await page.waitForTimeout(150);
    await shot(name, '.dc-app-overlay');
  }

  // Dimensions needs one to exist before it shows anything.
  await tab('Dimensions');
  await page
    .locator('.dc-dimension-controls button', { hasText: 'Add' })
    .click();
  for (const column of ['region', 'desk']) {
    await page
      .locator('.dc-pane-available .dc-selector-row', { hasText: column })
      .first()
      .dblclick();
  }
  await shot('editor-dimensions', '.dc-app-overlay');

  // -- a heatmap, set the way a user would ------------------------------
  await tab('Column Properties');
  await field('Choose Column:').locator('select').selectOption('notional');
  await field('Heatmap:').locator('input[type="checkbox"]').check();
  await shot('editor-heatmap-on', '.dc-app-overlay');
  await page
    .locator('.dc-editor-footer button', { hasText: 'OK' })
    .click();
  await page.waitForTimeout(2500);
  await shot('heatmap', '.dc-app-middle');

  // -- the title bar menu ----------------------------------------------
  await page.locator('.dc-titlebar-menu').click();
  await page.waitForSelector('.dc-menu', { timeout: 10_000 });
  await shot('title-bar-menu');
  await page.keyboard.press('Escape');

  // -- the nested filter editor ---------------------------------------------
  await tool('Filters');
  await page.waitForSelector('.dc-filters', { timeout: 10_000 });
  // Build A AND NOT (B OR C), the shape a flat list of conditions
  // cannot express -- entirely through the controller buttons, which
  // is the point of them.
  await page.locator('.dc-filter-btn', { hasText: 'Create New Filter' }).click();
  await page.locator('.dc-filter-value').first().fill('EMEA');
  await page.locator('.dc-filter-value').first().dispatchEvent('change');
  await page.waitForTimeout(250);

  // `+` on the first condition inserts a sibling just after it.
  await page.locator('.dc-filter-row').nth(1).locator('.dc-filter-ctl').first().click();
  const second = page.locator('.dc-filter-row').nth(2);
  await second.locator('.dc-filter-column').selectOption('desk');
  await second.locator('.dc-filter-value').fill('Rates');
  await second.locator('.dc-filter-value').dispatchEvent('change');
  await page.waitForTimeout(250);

  // `( )` wraps it in its own sub-group; then a sibling inside that,
  // the group set to "Any of", and the group negated.
  await page.locator('.dc-filter-row').nth(2).locator('.dc-filter-ctl').nth(2).click();
  await page.waitForTimeout(200);
  await page.locator('.dc-filter-row').nth(3).locator('.dc-filter-ctl').first().click();
  await page.waitForTimeout(200);
  const groupRow = page.locator('.dc-filter-row.dc-filter-group').nth(1);
  await groupRow.locator('.dc-filter-join').selectOption('or');
  await groupRow.locator('.dc-filter-ctl').nth(3).click();
  const last = page.locator('.dc-filter-row').last();
  await last.locator('.dc-filter-column').selectOption('desk');
  await last.locator('.dc-filter-value').fill('Credit');
  await last.locator('.dc-filter-value').dispatchEvent('change');
  await page.waitForTimeout(400);

  await shot('filter-editor-nested', '.dc-app-overlay');
  await page.locator('.dc-overlay-close').click();
  await page.waitForTimeout(2000);
  // Clear it again, so the states after this one are not filtered.
  await tool('Filters');
  await page.waitForSelector('.dc-filters');
  while ((await page.locator('.dc-filter-row').count()) > 1) {
    await page.locator('.dc-filter-row').nth(1).locator('.dc-filter-ctl').nth(1).click();
    await page.waitForTimeout(150);
  }
  await page.waitForTimeout(1500);
  await page.locator('.dc-overlay-close').click();

  // -- the drag zones, mid-drag ----------------------------------------------
  // A drop target that does not look like one is a drop target people
  // hover over, guess at, and let go of somewhere else.
  await page.evaluate(() => {
    document.querySelector('.dc-zone-columns')?.classList.add('dc-drop-target');
  });
  await shot('drag-zone-hot', '.dc-pivot-panel');
  await page.evaluate(() => {
    document
      .querySelector('.dc-zone-columns')
      ?.classList.remove('dc-drop-target');
  });

  // -- flattened, which is where the filter strip goes away ------------------
  for (const zone of ['.dc-zone-columns', '.dc-zone-rows']) {
    while ((await page.locator(`${zone} .dc-chip-remove`).count()) > 0) {
      const before = await page.locator(`${zone} .dc-chip`).count();
      await page.locator(`${zone} .dc-chip-remove`).last().click();
      await page.waitForFunction(
        ([sel, k]) => document.querySelectorAll(`${sel} .dc-chip`).length < k,
        [zone, before],
        { timeout: 60_000 },
      );
    }
  }
  await page.waitForTimeout(2000);
  await shot('flattened', '.dc-app');

  // -- rebuilt from the tool panel, which is the drag SOURCE -----------------
  for (const column of ['book', 'desk']) {
    await page
      .locator('.dc-tool-panel-row', { hasText: column })
      .first()
      .dblclick();
    await page.waitForTimeout(1500);
  }
  await shot('rebuilt-from-tool-panel', '.dc-app');

  console.log(`\n${n} screenshots in ${OUT}/`);
} catch (e) {
  console.error('FAILED:', e instanceof Error ? e.message : String(e));
  process.exitCode = 1;
} finally {
  await browser.close();
  server.close();
}
