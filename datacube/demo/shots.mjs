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

const tool = (label) =>
  page.locator('.dc-app-toolbar .dc-tool', { hasText: label }).first().click();
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
  await page.locator('.dc-cell').nth(1).click({ button: 'right' });
  await page.waitForSelector('.dc-menu', { timeout: 10_000 });
  await shot('context-menu');
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

  // -- the filter strip, end to end ---------------------------------------
  await page.locator('.dc-floating-input').first().fill('EMEA');
  await page.waitForFunction(
    () => document.getElementById('pure')?.textContent?.includes('filter('),
    { timeout: 60_000 },
  );
  await shot('filter-strip', '.dc-app');
  await page.locator('.dc-floating-input').first().fill('');
  await page.waitForFunction(
    () => !document.getElementById('pure')?.textContent?.includes('filter('),
    { timeout: 60_000 },
  );

  // -- the nested filter editor ---------------------------------------------
  await tool('Filters');
  await page.waitForSelector('.dc-filters', { timeout: 10_000 });
  // Build a small nested filter, which is the shape a flat list of
  // conditions cannot express: A AND NOT (B OR C).
  await page.locator('.dc-filter-btn', { hasText: 'Add filter' }).click();
  await page.locator('.dc-filter-value').first().fill('EMEA');
  await page.locator('.dc-filter-btn', { hasText: '+ group' }).click();
  await page.locator('.dc-filter-btn', { hasText: '+ condition' }).last().click();
  await page.waitForTimeout(300);
  await shot('filter-editor', '.dc-app-overlay');
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
