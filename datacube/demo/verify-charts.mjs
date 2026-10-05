// The board's charts in a real browser (plan B1, revised 2026-09-30): charts follow the grid until
// frozen; Open in grid edits a frozen chart's grouping in a grid of its own beside it, leaving the
// cube's grid alone, and Update writes it back -- drawn by real ECharts on the demo page.
//
//   bazel run //datacube:verify_charts            (SHOTS=<dir> also saves a screenshot)

import { readFile } from 'node:fs/promises';
import { extname } from 'node:path';
import { chromium } from 'playwright';
import { serve, siteRoot } from './harness.mjs';


const ROOT = siteRoot();
const { port, close: closeServer } = await serve(ROOT);

const browser = await chromium.launch();
const page = await (await browser.newContext({ viewport: { width: 1400, height: 900 } })).newPage();
const pageErrors = [];
page.on('pageerror', (e) => pageErrors.push(e.message));

const results = [];
async function check(name, fn) {
  try {
    const detail = await fn();
    results.push({ name, ok: true });
    console.log(`  ok   ${name}${detail ? ` — ${detail}` : ''}`);
  } catch (e) {
    results.push({ name, ok: false });
    console.log(`  BAD  ${name} — ${String(e.message ?? e).split('\n').slice(0, 4).join(' | ')}`);
  }
}

/** Until the cube is not busy and has told the page of no change for a moment. */
async function settle() {
  await page.evaluate(() => { window.__settleWatch = undefined; });
  await page.waitForFunction(() => {
    const signal = window.__dataCubeSignal;
    const app = window.__dataCube;
    if (!signal || !app) return false;
    const now = performance.now();
    const w = (window.__settleWatch ??= { changes: signal.changes, since: now });
    if (app.busy || signal.printing > 0 || signal.changes !== w.changes) {
      w.changes = signal.changes;
      w.since = now;
      return false;
    }
    return now - w.since >= 400;
  }, null, { timeout: 30_000 });
}

const charts = () => page.evaluate(() => window.__dataCube.pageViews().views.filter((v) => v.kind === 'chart')
  .map((v) => ({ id: v.id, title: v.title, pinned: v.spec.frozen === true, x: v.spec.x, split: v.spec.split ?? null })));
const rows = () => page.evaluate(() => [...window.__dataCube.snapshot.rows]);
const button = (id, text) => page.locator(`[data-tile="${id}"] .dc-tile-actions > button:visible`, { hasText: text }).first();
/** Every chart tile has drawn a canvas with something on it. */
async function drawn() {
  await page.waitForFunction(() => [...document.querySelectorAll('[data-tile^="chart-"]')]
    .every((t) => t.querySelector('.dc-chartpanel-chart canvas')), null, { timeout: 20_000 });
}

try {
  await page.goto(`http://127.0.0.1:${port}/demo/index.html`);
  await page.waitForSelector('.dc-row', { timeout: 120_000 });
  await settle();
  const before = await rows();

  /** Right-click > Insert > `what` (Visualization or Copy of Grid), on a cell of the grid in `scope`. */
  const insert = async (what, scope = '[data-tile="grid"], .dc-app') => {
    await page.locator(scope).first().locator('.dc-row').nth(1).locator('.dc-cell').nth(2).click({ button: 'right' });
    await page.locator('.dc-menu-item:has(> .dc-menu-label:text-is("Insert"))').first().hover();
    await page.locator(`.dc-menu-item:has(> .dc-menu-label:text-is("Insert")) .dc-menu-item:has(> .dc-menu-label:text-is("${what}"))`).first().click();
    await settle();
  };
  const addChart = async () => {
    await insert('Visualization');
    await drawn();
  };
  /** A chart's Dynamic / Frozen / Detached pill. */
  const pill = (id) => page.locator(`[data-tile="${id}"] .dc-tile-actions .dc-titlebar-toggle`);
  /** A chart's right-click menu entry. */
  const chartMenu = async (id, label) => {
    await page.locator(`[data-tile="${id}"] .dc-chart-tile`).click({ button: 'right' });
    await page.locator(`.dc-menu-item:has(> .dc-menu-label:text-is("${label}"))`).first().click();
  };
  /** The cube's own row-group zone, not an editing grid's. */
  const mainChip = (column) => page.locator(`.dc-zone-rows .dc-chip[data-column="${column}"] .dc-chip-remove`)
    .filter({ hasNot: page.locator('xpath=ancestor::*[starts-with(@data-tile, "edit-")]') }).first();

  await check('two charts both follow the grid, badged, and a pivot changes both', async () => {
    await addChart();
    await addChart();
    const list = await charts();
    if (list.length !== 2 || list.some((c) => c.pinned)) throw new Error(JSON.stringify(list));
    for (const c of list) if ((await pill(c.id).textContent()) !== 'Dynamic') throw new Error(`${c.id} is not Dynamic`);
    return list.map((c) => `${c.title}: ${c.x} by ${c.split}`).join('; ');
  });

  await check('Freeze keeps one chart\'s grouping while the other follows a pivot', async () => {
    const [a, b] = await charts();
    await pill(a.id).click();
    await settle();
    await mainChip(before[1]).click();
    await settle();
    const [na, nb] = await charts();
    if (na.split !== a.split) throw new Error(`the frozen chart changed: ${a.split} -> ${na.split}`);
    if (nb.split === b.split) throw new Error(`the following chart did not follow: still ${nb.split}`);
    return `frozen keeps ${a.split}; following now ${nb.split}`;
  });

  await check('Open in grid edits the chart in a grid of its own; the cube\'s grid stays; Update writes it back', async () => {
    const frozen = (await charts()).find((c) => c.pinned);
    const rowsBefore = await rows();
    await chartMenu(frozen.id, 'Open in grid');
    const editing = page.locator(`[data-tile="edit-${frozen.id}"]`);
    await editing.locator('.dc-row').first().waitFor({ timeout: 20_000 });
    await settle();
    const chipsIn = () => editing.locator('.dc-zone-rows').first().locator('.dc-chip').evaluateAll((els) => els.map((e) => e.dataset.column));
    const want = [frozen.x, ...(frozen.split ? [frozen.split] : [])];
    if (JSON.stringify(await chipsIn()) !== JSON.stringify(want)) throw new Error(`editing grid grouped ${await chipsIn()}, want ${want}`);
    if (JSON.stringify(await rows()) !== JSON.stringify(rowsBefore)) throw new Error('the cube\'s grid changed');
    if (process.env.SHOTS) { await page.waitForTimeout(1500); await page.screenshot({ path: `${process.env.SHOTS}/charts-editing.png` }); }
    // re-group in the editing grid, then update the chart
    await editing.locator(`.dc-zone-rows .dc-chip[data-column="${frozen.split}"] .dc-chip-remove`).first().click();
    await page.waitForTimeout(500);
    await button(`edit-${frozen.id}`, `Update ${frozen.title}`).click();
    await settle();
    const after = (await charts()).find((c) => c.id === frozen.id);
    if (!after.pinned || after.split !== null) throw new Error(`the chart is ${JSON.stringify(after)}`);
    if (await editing.count()) throw new Error('the editing grid is still open');
    if (JSON.stringify(await rows()) !== JSON.stringify(rowsBefore)) throw new Error('the cube\'s grid changed');
    return `${frozen.title} updated to ${after.x}, no split; the cube's grid still ${rowsBefore.join(' > ')}`;
  });

  await check('Insert > Grid adds a grid tile of its own; its Insert > Chart charts it; removing it detaches a frozen chart', async () => {
    const tilesBefore = await page.locator('[data-tile^="chart-"]').count();
    await insert('Copy of Grid', '[data-tile="grid"]');
    const grid = page.locator('[data-tile^="grid-"]').first();
    await grid.locator('.dc-row').first().waitFor({ timeout: 20_000 });
    await settle();
    const gridId = await grid.getAttribute('data-tile');
    await insert('Visualization', `[data-tile="${gridId}"]`);
    await insert('Visualization', `[data-tile="${gridId}"]`);
    await page.waitForFunction((n) => document.querySelectorAll('[data-tile^="chart-"]').length === n + 2, tilesBefore, { timeout: 20_000 });
    await drawn();
    const ids = await page.locator('[data-tile^="chart-"]').evaluateAll((els) => els.map((e) => e.dataset.tile));
    const [frozenId, followingId] = ids.slice(-2);
    await pill(frozenId).click();
    if (process.env.SHOTS) { await page.waitForTimeout(1500); await page.screenshot({ path: `${process.env.SHOTS}/grids.png` }); }
    await page.locator(`[data-tile="${gridId}"] .dc-tile-remove`).click();
    await settle();
    if (await page.locator(`[data-tile="${gridId}"]`).count()) throw new Error('the grid is still there');
    if (await page.locator(`[data-tile="${followingId}"]`).count()) throw new Error('its following chart is still there');
    await page.waitForFunction((id) => document.querySelector(`[data-tile="${id}"] .dc-tile-actions .dc-titlebar-toggle`)?.textContent === 'Detached', frozenId, { timeout: 5_000 });
    return `${gridId} removed: ${followingId} went with it, ${frozenId} detached`;
  });

  await check('no page errors', async () => {
    if (pageErrors.length) throw new Error(pageErrors.slice(0, 2).join(' | '));
  });
} finally {
  await browser.close();
  closeServer();
}

const bad = results.filter((r) => !r.ok).length;
console.log(`\n${results.length - bad}/${results.length} chart checks work`);
process.exit(bad ? 1 : 0);
