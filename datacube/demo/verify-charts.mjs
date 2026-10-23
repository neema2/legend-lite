// The board's charts in a real browser (plan B1, revised 2026-09-30): charts follow the grid until
// frozen; Show Grid shows a frozen chart in a grid of its own grouping beside it, which it follows from
// then on, leaving the cube's grid alone (D7) -- drawn by real ECharts on the demo page.
//
//   bazel run //datacube:verify_charts            (SHOTS=<dir> also saves a screenshot)

// first: points Playwright at the Chromium Bazel fetched (as a browser_test; a no-op under bazel run)
import '../../tools/browser/pinned-chromium.mjs';
import { readFile } from 'node:fs/promises';
import { extname } from 'node:path';
import { chromium } from 'playwright';
import { frames, outPath, serve, siteRoot } from './harness.mjs';


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

// the page's views (a page of its own: window.__dataPage), its first grid the one the checks open (window.__dataCube)
const charts = () => page.evaluate(() => window.__dataPage.views().views.filter((v) => v.kind === 'chart')
  .map((v) => ({ id: v.id, title: v.title, pinned: v.spec.frozen === true, x: v.spec.x, split: v.spec.split ?? null })));
const rows = () => page.evaluate(() => [...window.__dataCube.snapshot.rows]);
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
  /** The page's first grid: every grid on a page is numbered alike, the first grid-1. */
  const FIRST = '[data-tile="grid-1"]';
  const insert = async (what, scope = `${FIRST}, .dc-app`) => {
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
  /** The cube's own row-group zone: the first grid's, not another grid's. */
  const mainChip = (column) => page.locator(FIRST).locator(`.dc-zone-rows .dc-chip[data-column="${column}"] .dc-chip-remove`).first();
  /** The grid a chart reads, by the page's views. */
  const gridOf = (chart) => page.evaluate((id) => window.__dataPage.views().views.find((v) => v.id === id)?.cube, chart);
  /** The page's Undo: the last step of the page's history taken back. */
  const undo = async () => { await page.evaluate(() => window.__dataPage.undo()); await settle(); };

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

  await check('Show Grid shows a frozen chart in a grid of its own grouping that it follows; the cube\'s grid stays; undone, as it was', async () => {
    const frozen = (await charts()).find((c) => c.pinned);
    const rowsBefore = await rows();
    const was = await gridOf(frozen.id);
    await chartMenu(frozen.id, 'Show Grid');
    await settle();
    const own = await gridOf(frozen.id);
    if (!own || own === was) throw new Error(`the chart reads ${own}, as before`);
    const shown = page.locator(`[data-tile="${own}"]`);
    await shown.locator('.dc-row').first().waitFor({ timeout: 20_000 });
    await settle();
    const chipsIn = () => shown.locator('.dc-zone-rows').first().locator('.dc-chip').evaluateAll((els) => els.map((e) => e.dataset.column));
    const want = [frozen.x, ...(frozen.split ? [frozen.split] : [])];
    if (JSON.stringify(await chipsIn()) !== JSON.stringify(want)) throw new Error(`its grid grouped ${await chipsIn()}, want ${want}`);
    if (JSON.stringify(await rows()) !== JSON.stringify(rowsBefore)) throw new Error('the cube\'s grid changed');
    if ((await charts()).find((c) => c.id === frozen.id).pinned) throw new Error('the chart does not follow its grid');
    if (process.env.SHOTS) { await frames(page, 4); await page.screenshot({ path: outPath('charts-show-grid.png') }); }
    // it follows its grid: regrouped there, it regroups
    await shown.locator(`.dc-zone-rows .dc-chip[data-column="${frozen.split}"] .dc-chip-remove`).first().click();
    await shown.locator(`.dc-zone-rows .dc-chip[data-column="${frozen.split}"]`).first().waitFor({ state: 'detached' });
    await settle();
    const after = (await charts()).find((c) => c.id === frozen.id);
    if (after.split !== null) throw new Error(`the chart did not follow its grid: ${JSON.stringify(after)}`);
    // the page's Undo takes the whole step back: its grid gone, the chart frozen on the cube's grid as it was
    await undo();
    const back = (await charts()).find((c) => c.id === frozen.id);
    if (!back.pinned || back.split !== frozen.split || (await gridOf(frozen.id)) !== was) throw new Error(`undone, the chart is ${JSON.stringify(back)}`);
    if (await shown.count()) throw new Error('undone, its grid is still on the page');
    return `${frozen.title} shown in ${own} by ${want.join(' > ')}, following it; undone, frozen again; the cube's grid still ${rowsBefore.join(' > ')}`;
  });

  await check('Column Properties > Aggregation: a following chart plots its column at it, a frozen one keeps its own; Show Grid keeps the frozen one\'s', async () => {
    // a grid's aggregates are its columns' Aggregation (docs/DATACUBE_ONE_GRID_EDIT_PATH_2026_10_17.md §3.5)
    const values = () => page.evaluate(() => window.__dataPage.views().views.filter((v) => v.kind === 'chart')
      .map((v) => ({ id: v.id, title: v.title, pinned: v.spec.frozen === true, y: v.spec.y.map((m) => [m.column, m.fn]) })));
    const before = await values();
    const live = before.find((c) => !c.pinned);
    const frozen = before.find((c) => c.pinned);
    if (!live || !frozen) throw new Error(`want a following and a frozen chart: ${JSON.stringify(before)}`);
    const [column, fn] = live.y[0];
    const next = fn === 'max' ? 'min' : 'max';
    // Properties > Column Properties > the column > Aggregation, Apply: as a person sets it
    await page.locator(FIRST).locator('.dc-row').nth(1).locator('.dc-cell').nth(2).click({ button: 'right' });
    await page.locator('.dc-menu-item:has(> .dc-menu-label:text-is("Properties..."))').first().click();
    await page.locator('.dc-editor-tab', { hasText: 'Column Properties' }).first().click();
    await page.locator(`.dc-pe-col[data-column="${column}"]`).first().click();
    await page.locator('.dc-field', { hasText: 'Aggregation:' }).first().locator('select').first().selectOption(next);
    await page.locator('button', { hasText: 'Apply' }).first().click();
    await settle();
    await page.keyboard.press('Escape');
    const after = await values();
    const liveNow = after.find((c) => c.id === live.id);
    const frozenNow = after.find((c) => c.id === frozen.id);
    if (JSON.stringify(liveNow.y[0]) !== JSON.stringify([column, next])) {
      throw new Error(`the following chart plots ${JSON.stringify(liveNow.y)}, not ${next} of ${column}`);
    }
    if (JSON.stringify(frozenNow.y) !== JSON.stringify(frozen.y)) {
      throw new Error(`the frozen chart changed: ${JSON.stringify(frozen.y)} -> ${JSON.stringify(frozenNow.y)}`);
    }
    // Show Grid: the grid of its own grouping carries the frozen chart's own values, so following it, it plots them
    await chartMenu(frozen.id, 'Show Grid');
    await settle();
    const own = page.locator(`[data-tile="${await gridOf(frozen.id)}"]`);
    await own.locator('.dc-row').first().waitFor({ timeout: 20_000 });
    await settle();
    const shown = (await values()).find((c) => c.id === frozen.id);
    if (shown.pinned || JSON.stringify(shown.y) !== JSON.stringify(frozen.y)) {
      throw new Error(`Show Grid changed the chart's values: ${JSON.stringify(frozen.y)} -> ${JSON.stringify(shown.y)}`);
    }
    // undone: the page as it was, for the checks after
    await undo();
    return `following: ${next} of ${column}; frozen kept ${JSON.stringify(frozen.y)}, and through Show Grid`;
  });

  await check('Insert > Grid adds a grid tile of its own; its Insert > Chart charts it; removing it detaches a frozen chart', async () => {
    const tilesBefore = await page.locator('[data-tile^="chart-"]').count();
    await insert('Copy of Grid', FIRST);
    // the copy: a grid beside the first, every grid on the page alike
    const grid = page.locator('[data-tile^="grid-"]:not([data-tile="grid-1"])').first();
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
    if (process.env.SHOTS) { await frames(page, 4); await page.screenshot({ path: outPath('grids.png') }); }
    await page.locator(`[data-tile="${gridId}"] .dc-tile-remove`).click();
    await settle();
    if (await page.locator(`[data-tile="${gridId}"]`).count()) throw new Error('the grid is still there');
    if (await page.locator(`[data-tile="${followingId}"]`).count()) throw new Error('its following chart is still there');
    await page.waitForFunction((id) => document.querySelector(`[data-tile="${id}"] .dc-tile-actions .dc-titlebar-toggle`)?.textContent === 'Detached', frozenId, { timeout: 5_000 });
    return `${gridId} removed: ${followingId} went with it, ${frozenId} detached`;
  });

  // A KPI (docs/DATACUBE_CHARTS_DESIGN_2026_10_09.md, batch 1): a chart's kind set to KPI in its Options -- its
  // number as the grid writes it, a target to say it against, its trend's line under it
  await check('a chart made a KPI: its number, against a target it is above, its trend\'s line under it', async () => {
    await addChart();
    const id = await page.locator('[data-tile^="chart-"]').last().getAttribute('data-tile');
    await chartMenu(id, 'Options...');
    const form = page.locator(`[data-tile="${id}"] .dc-chartpanel-form`);
    const fieldNamed = (label) => form.locator(`.dc-field:has(> .dc-field-label:text-is("${label}"))`);
    await fieldNamed('Chart:').locator('select').selectOption('kpi');
    await page.locator(`[data-tile="${id}"] .dc-kpi-value`).waitFor({ timeout: 20_000 });
    const value = await page.locator(`[data-tile="${id}"] .dc-kpi-value`).textContent();
    if (!value || value === '\u2014') throw new Error(`the KPI says ${value}`);
    // a target of 1: the total is above it, which is good
    await fieldNamed('Target:').locator('input').fill('1');
    await fieldNamed('Target:').locator('input').dispatchEvent('change');
    const target = page.locator(`[data-tile="${id}"] .dc-kpi-target`);
    await target.waitFor({ timeout: 20_000 });
    if (!(await target.getAttribute('class')).includes('dc-kpi-good')) throw new Error(`the target says ${await target.textContent()}, not good`);
    // its trend by the grid's first row group: a line drawn under the number
    const across = await fieldNamed('Trend by:').locator('option').nth(1).getAttribute('value');
    await fieldNamed('Trend by:').locator('select').selectOption(across);
    await page.locator(`[data-tile="${id}"] .dc-kpi-trend canvas`).waitFor({ timeout: 20_000 });
    const saved = await page.evaluate((c) => window.__dataPage.views().views.find((v) => v.id === c)?.spec, id);
    if (saved.mark !== 'kpi' || saved.options.target !== 1 || saved.x !== across) throw new Error(`saved as ${JSON.stringify(saved)}`);
    if (process.env.SHOTS) { await frames(page, 4); await page.screenshot({ path: outPath('kpi.png') }); }
    return `${id}: ${value}, ${await target.textContent()}, its trend by ${across}`;
  });

  // THE EXAMPLE PAGES (docs/DATACUBE_CHARTS_DESIGN_2026_10_09.md §3.3): each opened from Open…'s Examples, over the
  // sales sample it makes itself; on each of its sheets every chart draws -- batch 2's, then batch 3's kinds
  for (const [example, marker, least] of [['charts-dashboard', 'c-bars', 20], ['charts-kinds', 'k-lollipop', 20]]) {
    await check(`the example page ${example} opens from Open…, and every chart on every sheet draws`, async () => {
      await page.click('.dc-titlebar-menu');
      const open = page.locator('.dc-menu-item:has(> .dc-menu-label:text-is("Open\u2026"))').first();
      if (!(await open.isVisible())) await page.locator('.dc-menu-item:has(> .dc-menu-label:text-is("File"))').first().hover();
      await open.click();
      await page.locator(`#cubeswin .dc-open-example[data-example="${example}"]`).click();
      // this page, not the one open before it: one of its own charts is among its views
      await page.waitForFunction(([mark, n]) => {
        const views = window.__dataPage?.views().views ?? [];
        return views.some((v) => v.id === mark) && views.filter((v) => v.kind === 'chart').length >= n;
      }, [marker, least], { timeout: 120_000 });
      await settle();
      const sheets = await page.locator('.dc-sheet-tab').evaluateAll((els) => els.map((e) => e.dataset.sheet));
      let drawnCharts = 0;
      for (const sheet of sheets) {
        await page.locator(`.dc-sheet-tab[data-sheet="${sheet}"]`).click();
        await settle();
        // every chart tile shown on this sheet has drawn -- a canvas -- or said why not (a problem, failed below)
        await page.waitForFunction(() => {
          const shown = [...document.querySelectorAll('[data-tile] .dc-chart-tile')].filter((t) => t.offsetParent !== null);
          return shown.every((t) => t.querySelector('.dc-chartpanel-chart canvas') || t.querySelector('.dc-chartpanel-bad'));
        }, null, { timeout: 60_000 }).catch(async (e) => {
          // what each tile still undrawn says, beside the timeout
          const said = await page.locator('[data-tile] .dc-chart-tile').evaluateAll((els) => els
            .filter((t) => t.offsetParent !== null && !t.querySelector('.dc-chartpanel-chart canvas'))
            .map((t) => `${t.closest('[data-tile]')?.dataset.tile}: ${t.querySelector('.dc-chartpanel-status')?.textContent ?? ''}`));
          throw new Error(`sheet ${sheet}: ${e.message}; undrawn: ${said.join(' | ')}`);
        });
        if (process.env.SHOTS) { await frames(page, 4); await page.screenshot({ path: outPath(`${example}-${sheet}.png`) }); }
        const bad = await page.locator('[data-tile] .dc-chartpanel-bad').evaluateAll((els) =>
          els.filter((e) => e.offsetParent !== null).map((e) => e.textContent));
        if (bad.length) throw new Error(`sheet ${sheet}: ${bad.join(' | ')}`);
        drawnCharts += await page.locator('[data-tile] .dc-chart-tile canvas').evaluateAll((els) => els.filter((e) => e.offsetParent !== null).length);
      }
      return `${sheets.length} sheets, ${drawnCharts} canvases drawn`;
    });
  }

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
