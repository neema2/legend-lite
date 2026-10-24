// THE TOUR (the user, 2026-10-17: "definitely do the screenshot tour -- save the playbook so we can run it from time to
// time"): DataCube driven the way a person drives it, one flow after another on one page, each step CHECKED and SHOT --
// a picture per step, and a gallery page beside them that shows what each check saw. docs/DATACUBE_TOUR.md is its
// playbook: how to run it, what each step shows, what to look for in the pictures.
//
// Unlike shots.mjs (pictures only) and the verify-* harnesses (checks only), a step here is both: a failed step still
// gets its picture, so the gallery shows where it went wrong, and the run exits 1.
//
//   bazel run //datacube:tour [-- --out DIR]   the gallery at DIR/index.html (default: a scratch directory, printed)
//   bazel test //datacube:tour_test            manual, in //gates:heavy: the gallery in the test's outputs
//                                              (bazel-testlogs/datacube/tour_test/test.outputs/)

// first: points Playwright at the Chromium Bazel fetched (as a browser_test; a no-op under bazel run)
import '../../tools/browser/pinned-chromium.mjs';
import { mkdirSync } from 'node:fs';
import { readFile, writeFile } from 'node:fs/promises';
import { dirname, join, resolve } from 'node:path';
import { chromium } from 'playwright';

import { frames, outPath, serve, siteRoot, tmpDir } from './harness.mjs';

const outAt = process.argv.indexOf('--out');
const OUT = outAt >= 0
  ? resolve(process.env.BUILD_WORKING_DIRECTORY ?? process.cwd(), process.argv[outAt + 1])
  : dirname(outPath('tour/index.html'));
mkdirSync(OUT, { recursive: true });

const ROOT = siteRoot();
const { port, close: closeServer } = await serve(ROOT);
const URL_BASE = `http://127.0.0.1:${port}`;
const dir = await tmpDir('dc-tour-');

const browser = await chromium.launch();
// the clipboard: Share… writes the link there, and the share step reads it back
const context = await browser.newContext({ viewport: { width: 1400, height: 900 },
  permissions: ['clipboard-read', 'clipboard-write'] });
const page = await context.newPage();
const pageErrors = [];
page.on('pageerror', (e) => pageErrors.push(e.message));
// a question the page asks (open over unsaved changes?) is answered yes, and said in the log
page.on('dialog', (d) => {
  console.log(`       (the page asked: ${d.message().slice(0, 100)} -- yes)`);
  void d.accept();
});

// -- steps: each checked, each shot ----------------------------------------------------------------------------------

const steps = [];
const slug = (title) => title.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '').slice(0, 60);

/**
 * One step of the tour: `fn` drives the page and checks what it sees, returning what it saw (or throwing why not). It
 * is given `shot(target = page, label?)`: a picture now, of a page or a tab -- mid-step, say, with a window open. A step
 * that takes none is shot when it ends, pass or fail.
 */
async function step(title, fn) {
  const n = steps.length + 1;
  const record = { n, title, ok: true, detail: '', shots: [] };
  const shot = async (target = page, label = '') => {
    const file = `${String(n).padStart(2, '0')}-${slug(title)}${label ? `-${slug(label)}` : ''}.png`;
    await frames(target, 4);
    await target.screenshot({ path: join(OUT, file) });
    record.shots.push({ file, label });
  };
  try {
    record.detail = (await fn(shot)) ?? '';
  } catch (e) {
    record.ok = false;
    record.detail = String(e.message ?? e).split('\n').slice(0, 4).join(' | ');
  }
  if (record.shots.length === 0) {
    await shot().catch((e) => { record.detail += ` (no picture: ${e.message})`; });
  }
  steps.push(record);
  console.log(`  ${record.ok ? 'ok ' : 'BAD'}  ${n}. ${title}${record.detail ? ` — ${record.detail}` : ''}`);
  return record.ok;
}

// -- the page, read and driven as a person drives it ------------------------------------------------------------------

/** Until the cube is not busy and has told the page of no change for a moment. */
async function settle(tab = page) {
  await tab.evaluate(() => { window.__settleWatch = undefined; });
  await tab.waitForFunction(() => {
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
  }, null, { timeout: 60_000 });
}

/** The page's first grid: every grid on a page is numbered alike, the first grid-1. */
const FIRST = '[data-tile="grid-1"]';
/** The first grid's query, without its epoch (a counter each request takes a new one of, not a setting). */
const snapshot = (tab = page) => tab.evaluate(() => {
  const { epoch: _epoch, ...query } = JSON.parse(JSON.stringify(window.__dataCube.snapshot));
  return query;
});
/** The page's charts, as saved: id, title, frozen, what is across, the split, the values. */
const charts = (tab = page) => tab.evaluate(() => window.__dataPage.views().views.filter((v) => v.kind === 'chart')
  .map((v) => ({ id: v.id, title: v.title, frozen: v.spec.frozen === true, x: v.spec.x ?? null, split: v.spec.split ?? null,
    y: v.spec.y.map((m) => [m.column, m.fn]) })));
const same = (a, b) => JSON.stringify(a) === JSON.stringify(b);
/** Every chart tile shown has drawn a canvas. */
async function drawn(tab = page) {
  await tab.waitForFunction(() => [...document.querySelectorAll('[data-tile^="chart-"]')]
    .filter((t) => t.offsetParent !== null)
    .every((t) => t.querySelector('.dc-chartpanel-chart canvas')), null, { timeout: 30_000 });
}
/** A menu entry by its own label (a submenu's parent holds its children's labels too: never matched by those). */
const entry = (label) => page.locator(`.dc-menu-item:has(> .dc-menu-label:text-is(${JSON.stringify(label)}))`).first();
/** Which cell of the first grid's first row is `column`'s, read off the row (a grouped row's cells are not the header's). */
async function cellOf(column) {
  const at = await page.locator(FIRST).first().locator('.dc-row').nth(1).locator('.dc-cell').evaluateAll((cells, name) =>
    cells.findIndex((c) => (c.dataset.column ?? c.closest('[data-column]')?.dataset.column) === name), column);
  if (at < 0) throw new Error(`${column} is not on screen`);
  return at;
}
/** Right-click a cell of the first grid, then the path through the menu: its parents hovered, the last clicked. */
async function cellMenu(path, { row = 1, col = 2 } = {}) {
  await page.locator(FIRST).first().locator('.dc-row').nth(row).locator('.dc-cell').nth(col).click({ button: 'right' });
  for (const [i, label] of path.entries()) {
    if (i < path.length - 1) await entry(label).hover();
    else await entry(label).click();
  }
}
/** A page-menu entry through its submenus: an entry under File or Export shows only once its parent is hovered. */
async function burger(label) {
  await page.click('.dc-titlebar-menu');
  const item = entry(label);
  if (!(await item.isVisible())) {
    const parents = await item.evaluate((el) => {
      const out = [];
      for (let p = el.parentElement?.closest('.dc-menu-item'); p; p = p.parentElement?.closest('.dc-menu-item')) {
        out.unshift(p.querySelector(':scope > .dc-menu-label')?.textContent ?? '');
      }
      return out;
    });
    for (const parent of parents) await entry(parent).hover();
  }
  await item.click();
}
/** The first grid's Edit ▸ entry (Undo, Redo), from its right-click: its menu button went (the user, 2026-10-23). */
async function gridMenu(label) {
  await cellMenu(['Edit', label]);
  await settle();
}
/** A row group (`zone` rows) or pivot (columns) of the first grid taken off by its chip's ×, never an editing grid's. */
async function takeOff(zone, column) {
  await page.locator(`.dc-zone-${zone} .dc-chip[data-column="${column}"]`)
    .filter({ hasNot: page.locator('xpath=ancestor::*[starts-with(@data-tile, "edit-")]') })
    .first().locator('.dc-chip-remove').click();
  await settle();
}
/** A chart's Dynamic / Pinned / Frozen / Detached pill, and its right-click menu. */
const pill = (id) => page.locator(`[data-tile="${id}"] .dc-tile-actions .dc-titlebar-toggle`);
/** A chart put in a state as a person does: its pill clicked, the state picked from the menu it opens. */
async function choose(id, state) {
  await pill(id).click();
  await page.locator('.dc-menu .dc-menu-item', { has: page.locator(':scope > .dc-menu-label', { hasText: new RegExp(`^${state} \u2014 `) }) })
    .first().click();
}
async function chartMenu(id, label) {
  await page.locator(`[data-tile="${id}"] .dc-chart-tile`).click({ button: 'right' });
  await entry(label).click();
}
/** A field of a form by its label, scoped. */
const field = (scope, label) => scope.locator(`.dc-field:has(> .dc-field-label:text-is(${JSON.stringify(label)}))`).first();

/** Properties > Column Properties > `column` > Aggregation: `fn`, Apply -- as a person sets it. */
async function setAggregation(column, pick, shot) {
  await cellMenu(['Properties...']);
  await page.locator('.dc-editor-tab', { hasText: 'Column Properties' }).first().click();
  await page.locator(`.dc-pe-col[data-column="${column}"]`).first().click();
  const select = page.locator('.dc-field', { hasText: 'Aggregation:' }).first().locator('select').first();
  const offered = await select.locator('option').evaluateAll((os) => os.map((o) => o.value));
  const fn = pick(offered);
  if (!fn) throw new Error(`Aggregation offers ${offered.join(', ')}`);
  await select.selectOption(fn);
  if (shot) await shot(page, 'the editor');
  await page.locator('button', { hasText: 'Apply' }).first().click();
  await settle();
  await page.keyboard.press('Escape');
  return fn;
}

/** What differs between two snapshots, by key: the first few, each said briefly. */
function differs(a, b) {
  const keys = [...new Set([...Object.keys(a), ...Object.keys(b)])].filter((k) => !same(a[k], b[k]));
  return keys.slice(0, 4).map((k) => `${k}: ${JSON.stringify(a[k])?.slice(0, 120)} vs ${JSON.stringify(b[k])?.slice(0, 120)}`)
    .join('; ') || 'nothing';
}

/**
 * A sample at `rows` rows as a person starts a page: New > Blank Page, "Add a data source", the picker's Examples. A
 * page over a picked sample can be saved and shared (the demo's own first grid knows no source, so it cannot be).
 */
async function pickSample(id, rows) {
  await burger('Blank Page');
  await page.locator('.dc-blank').waitFor({ state: 'visible' });
  await page.click('.dc-blank .dc-primary');
  await page.locator('.dc-picker').waitFor({ state: 'visible' });
  await page.locator('.dc-picker-tab[data-section="examples"]').click();
  await page.locator(`.dc-picker-card[data-example="${id}"]`).click();
  await page.fill('.dc-picker-rows', String(rows));
  await page.click('.dc-picker-choice .dc-primary');
  await page.locator(`${FIRST} .dc-row, .dc-app .dc-row`).first().waitFor({ timeout: 90_000 });
  await settle();
}

/** The library's message (Open…'s window), waited for until it says `re`. */
async function libraryMessage(re, tab = page) {
  await tab.waitForFunction((src) => new RegExp(src).test(
    document.querySelector('#cubelib .dc-lib-message')?.textContent ?? ''), re.source, { timeout: 90_000 });
  return (await tab.locator('#cubelib .dc-lib-message').textContent()).replace(/\s+/g, ' ').trim();
}
/** Open… shown, the host's other windows closed. */
async function showOpen() {
  for (const other of ['querywin', 'sharewin']) {
    if (await page.locator(`#${other}`).isVisible()) await page.click(`#${other} .hostwin-close`);
  }
  if (!(await page.locator('#cubeswin').isVisible())) await burger('Open…');
  await page.locator('#cubeswin').waitFor({ state: 'visible' });
}
async function closeOpen() {
  if (await page.locator('#cubeswin').isVisible()) await page.click('#cubeswin .hostwin-close, #cubeswin .dc-picker-close');
}

try {
  await page.goto(`${URL_BASE}/demo/index.html`);
  await page.waitForSelector('.dc-row', { timeout: 120_000 });
  await settle();

  let start;
  await step('A page over the sample trades: New > Blank Page, Add a data source, Examples', async () => {
    await pickSample('trades', 2000);
    start = await snapshot();
    const shown = await page.locator(`${FIRST} .dc-row, .dc-app .dc-row`).count();
    if (shown < 2) throw new Error(`${shown} rows on screen`);
    return `${start.columns.length} columns, ${shown} rows on screen${start.rows.length ? `, grouped by ${start.rows.join(' > ')}` : ''}`;
  });

  await step('Grouped from the tool panel: region, then desk, then book', async () => {
    for (const column of ['region', 'desk', 'book']) {
      if ((await snapshot()).rows.includes(column)) continue;
      await page.locator(`.dc-tool-panel-row[data-column="${column}"]`).first().dblclick();
      await settle();
    }
    start = await snapshot();
    if (!same(start.rows, ['region', 'desk', 'book'])) throw new Error(`grouped by ${start.rows.join(' > ') || 'nothing'}`);
    return `rows ${start.rows.join(' > ')}`;
  });

  await step('A row group taken off by its chip', async () => {
    const last = start.rows.at(-1);
    await takeOff('rows', last);
    const now = await snapshot();
    if (!same(now.rows, start.rows.slice(0, -1))) throw new Error(`grouped by ${now.rows.join(' > ')}`);
    return `${last} off: ${now.rows.join(' > ')}`;
  });

  // a grid's aggregates are its columns' Aggregation (docs/DATACUBE_ONE_GRID_EDIT_PATH_2026_10_17.md §3.5)
  const numeric = (start.columns.find((c) => c.name === 'notional')
    ?? start.columns.find((c) => ['Float', 'Integer', 'Number', 'Decimal'].includes(c.type) && c.kind !== 'dimension'))?.name;
  await step('Column Properties: a column\'s Aggregation set to average', async (shot) => {
    if (!numeric) throw new Error('the grid has no numeric column');
    const fn = await setAggregation(numeric, (offered) => offered.find((v) => v === 'average' || v === 'avg'), shot);
    const now = await snapshot();
    const column = now.columns.find((c) => c.name === numeric);
    if (column?.aggregate !== fn) throw new Error(`${numeric}'s Aggregation is ${column?.aggregate ?? 'its default'}, not ${fn}`);
    await shot(page, 'the grid');
    return `${numeric} at ${fn}`;
  });

  await step('A sort, from a cell\'s menu: Sort > Descending', async () => {
    if (!numeric) throw new Error('the grid has no numeric column');
    const before = (await snapshot()).sorts;
    await cellMenu(['Sort', 'Descending'], { col: await cellOf(numeric) });
    await settle();
    const after = (await snapshot()).sorts;
    const added = after.find((s) => !before.some((b) => same(b, s)));
    if (!added || added.direction !== 'desc') throw new Error(`sorts ${JSON.stringify(before)} -> ${JSON.stringify(after)}`);
    return `sorted by ${added.column}, largest first`;
  });

  await step('A calculated column: Extended Columns > Add New Column…', async (shot) => {
    if (!numeric) throw new Error('the grid has no numeric column');
    await cellMenu(['Extended Columns', 'Add New Column...']);
    await page.waitForSelector('.dc-coleditor', { timeout: 10_000 });
    await page.fill('.dc-coleditor .dc-calc-input-name', 'uplift');
    await page.locator('.dc-coleditor .dc-xc-kind[data-kind="formula"]').click();
    await page.locator('.dc-coleditor .dc-xc-use [data-value="measure"]').click();
    await page.fill('.dc-coleditor .dc-calc-input-expr', `x|$x.${numeric}->toOne() * 1.1`);
    await page.waitForFunction(() => {
      const c = document.querySelector('.dc-coleditor .dc-calc-check');
      return c && c.getAttribute('data-state') !== 'compiling';
    }, undefined, { timeout: 30_000 });
    const said = await page.locator('.dc-coleditor .dc-calc-check').textContent();
    await shot(page, 'the editor');
    const ok = page.locator('.dc-coleditor .dc-calc-ok');
    if (await ok.isDisabled()) throw new Error(`OK is disabled: ${said}`);
    await ok.click();
    await settle();
    await page.keyboard.press('Escape');
    const derived = (await snapshot()).derived.map((d) => d.name);
    if (!derived.includes('uplift')) throw new Error(`the grid's calculated columns are ${derived.join(', ') || 'none'}`);
    await shot(page, 'the grid');
    return `uplift = ${numeric} × 1.1 (${said?.trim()})`;
  });

  let chart;
  await step('Create > Visualization: a chart that follows the grid', async () => {
    await cellMenu(['Create', 'Visualization']);
    await settle();
    await drawn();
    const list = await charts();
    if (list.length !== 1) throw new Error(`${list.length} charts`);
    chart = list[0];
    const said = await pill(chart.id).textContent();
    if (chart.frozen || said !== 'Dynamic') throw new Error(`the chart is ${said}`);
    return `${chart.title}: ${chart.y.map(([c, f]) => `${f} of ${c}`).join(', ')} across ${chart.x}${chart.split ? ` by ${chart.split}` : ''}`;
  });

  await step('The Aggregation changed again: the following chart plots its column at it', async () => {
    if (!chart) throw new Error('no chart');
    const [column, fn] = chart.y[0];
    const next = await setAggregation(column, (offered) => offered.find((v) => v === (fn === 'max' ? 'min' : 'max')));
    await drawn();
    const now = (await charts()).find((c) => c.id === chart.id);
    if (!same(now.y[0], [column, next])) throw new Error(`the chart plots ${JSON.stringify(now.y)}, not ${next} of ${column}`);
    chart = now;
    return `${fn} of ${column} -> ${next} of ${column}`;
  });

  await step('Two values of one column: pinned, a second value added, made Dynamic again -- both kept', async (shot) => {
    if (!chart) throw new Error('no chart');
    // grouped by one column only, nothing pivoted: a split draws one value per series, so values are added unsplit
    const grid = await snapshot();
    for (const column of grid.pivotOn.toReversed()) await takeOff('columns', column);
    for (const column of grid.rows.slice(1).toReversed()) await takeOff('rows', column);
    await choose(chart.id, 'Pinned');
    await settle();
    if ((await pill(chart.id).textContent()) !== 'Pinned') throw new Error(`the pill says ${await pill(chart.id).textContent()}`);
    const [column, fn] = (await charts()).find((c) => c.id === chart.id).y[0];
    const other = fn === 'min' ? 'max' : 'min';
    await chartMenu(chart.id, 'Options...');
    const form = page.locator(`[data-tile="${chart.id}"] .dc-chartpanel-form`);
    await form.locator('.dc-chartpanel-add').click();
    const second = () => form.locator('.dc-chartpanel-measure').first();
    await field(second(), 'Value 2:').locator('select').selectOption(column);
    await field(second(), 'Aggregate:').locator('select').selectOption(other);
    await settle();
    const frozen = (await charts()).find((c) => c.id === chart.id);
    if (!same(frozen.y, [[column, fn], [column, other]])) throw new Error(`the frozen chart plots ${JSON.stringify(frozen.y)}`);
    await shot(page, 'frozen, two values');
    // Options... again puts the form away
    await chartMenu(chart.id, 'Options...');
    await form.waitFor({ state: 'hidden' });
    await choose(chart.id, 'Dynamic');
    await settle();
    await drawn();
    const now = (await charts()).find((c) => c.id === chart.id);
    if (now.frozen) throw new Error('the chart is still frozen');
    if (now.y.length !== 2 || same(now.y[0], now.y[1]) || now.y.some(([c]) => c !== column)) {
      throw new Error(`following, it plots ${JSON.stringify(now.y)}: a value folded onto the other`);
    }
    await shot(page, 'dynamic, both kept');
    chart = now;
    return `following: ${now.y.map(([c, f]) => `${f} of ${c}`).join(' and ')}`;
  });

  await step('Show Grid on a frozen chart: a grid of its own grouping it follows, the same values', async (shot) => {
    if (!chart) throw new Error('no chart');
    await choose(chart.id, 'Pinned');
    await settle();
    const before = (await charts()).find((c) => c.id === chart.id);
    const rowsBefore = (await snapshot()).rows;
    const gridOf = () => page.evaluate((id) => window.__dataPage.views().views.find((v) => v.id === id)?.cube, chart.id);
    const was = await gridOf();
    await chartMenu(chart.id, 'Show Grid');
    await settle();
    const own = await gridOf();
    if (!own || own === was) throw new Error(`the chart reads ${own}, as before`);
    const shown = page.locator(`[data-tile="${own}"]`);
    await shown.locator('.dc-row').first().waitFor({ timeout: 30_000 });
    await settle();
    await shot(page, 'its own grid');
    const after = (await charts()).find((c) => c.id === chart.id);
    if (after.frozen) throw new Error('the chart does not follow its grid');
    if (!same(after.y, before.y)) throw new Error(`Show Grid changed the values: ${JSON.stringify(before.y)} -> ${JSON.stringify(after.y)}`);
    if (!same((await snapshot()).rows, rowsBefore)) throw new Error('the cube\'s grid changed');
    // the page's Undo takes the step back: the chart frozen on the cube's grid again, its grid gone
    await page.evaluate(() => window.__dataPage.undo());
    await settle();
    if (await shown.count()) throw new Error('undone, its grid is still on the page');
    if ((await gridOf()) !== was) throw new Error('undone, the chart reads another grid');
    await shot(page, 'undone');
    return `following ${own}: still ${after.y.map(([c, f]) => `${f} of ${c}`).join(' and ')}; undone, back on ${was}`;
  });

  await step('Undo and Redo on the grid', async (shot) => {
    const before = await snapshot();
    await gridMenu('Undo');
    const undone = await snapshot();
    if (same(undone, before)) throw new Error('Undo changed nothing');
    await shot(page, 'undone');
    await gridMenu('Redo');
    const redone = await snapshot();
    if (!same(redone, before)) throw new Error(`Redo did not bring the grid back as it was: ${differs(redone, before)}`);
    await shot(page, 'redone');
    return `undone to rows ${undone.rows.join(' > ')}${undone.pivotOn.length ? `, pivot ${undone.pivotOn.join(' × ')}` : ''}; redone`;
  });

  // THE PAGE KEPT (docs/DATACUBE_SAVE_SHARE_2026_09_28.md): saved, the tab reloaded, reopened from Open…
  let saved;
  await step('Save As, reload the tab, reopen from Open…: the same grid and charts', async () => {
    saved = { rows: (await snapshot()).rows, charts: await charts() };
    await burger('Save As…');
    await page.locator('.dc-save').waitFor({ timeout: 10_000 });
    await page.fill('.dc-save-input', 'The tour');
    await page.locator('.dc-save-foot .dc-primary').click();
    await page.locator('.dc-save').waitFor({ state: 'detached', timeout: 30_000 });
    await page.goto(`${URL_BASE}/demo/index.html`);
    await page.waitForSelector('.dc-row', { timeout: 120_000 });
    await settle();
    await showOpen();
    const row = page.locator('#cubelib .dc-lib-row', { hasText: 'The tour' });
    await row.waitFor({ timeout: 10_000 });
    await row.locator('.dc-lib-button', { hasText: 'Open' }).click();
    const said = await libraryMessage(/opened/);
    await closeOpen();
    await settle();
    await drawn();
    const now = { rows: (await snapshot()).rows, charts: await charts() };
    if (!same(now, saved)) throw new Error(`reopened as ${JSON.stringify(now)}, saved ${JSON.stringify(saved)}`);
    return said;
  });

  await step('Share…: the link opens the same page in a fresh tab', async (shot) => {
    await burger('Share…');
    await page.waitForFunction(() => /^Copied/.test(document.querySelector('#sharenote')?.textContent ?? ''), null, { timeout: 10_000 });
    const url = await page.evaluate(() => navigator.clipboard.readText());
    await page.click('#sharewin .hostwin-close');
    if (!/#p1\./.test(url)) throw new Error(`not a share link: ${url.slice(0, 80)}`);
    const tab = await context.newPage();
    tab.on('pageerror', (e) => pageErrors.push(`(shared tab) ${e.message}`));
    try {
      await tab.goto(url);
      await libraryMessage(/opened/, tab);
      await settle(tab);
      await drawn(tab);
      const got = { rows: (await snapshot(tab)).rows, charts: (await charts(tab)).map(({ y, x }) => ({ x, y })) };
      const want = { rows: saved.rows, charts: saved.charts.map(({ y, x }) => ({ x, y })) };
      await shot(tab, 'the fresh tab');
      if (!same(got, want)) throw new Error(`the shared page is ${JSON.stringify(got)}, not ${JSON.stringify(want)}`);
      return `${url.length} characters; the same grid and charts in the fresh tab`;
    } finally {
      await tab.close();
    }
  });

  // no older page is read (the user, 2026-10-17: docs/DATACUBE_ONE_GRID_EDIT_PATH_2026_10_17.md §3.4)
  await step('A page file from before a grid\'s aggregates were its columns\' is refused, and said', async (shot) => {
    const want = await snapshot();
    const [download] = await Promise.all([page.waitForEvent('download'), burger('Page File (JSON)')]);
    const doc = JSON.parse(await readFile(await download.path(), 'utf8'));
    doc.cubes[0].cube.version = 1;
    const legacy = join(dir, 'older.page.json');
    await writeFile(legacy, JSON.stringify(doc));
    await showOpen();
    await page.setInputFiles('#cubelib .dc-lib-import', legacy);
    const said = await libraryMessage(/no longer read/);
    await shot(page, 'refused');
    await closeOpen();
    if (!same(await snapshot(), want)) throw new Error('the grid shown changed');
    return said.slice(0, 160);
  });

  // THE EXAMPLE PAGES (docs/DATACUBE_CHARTS_DESIGN_2026_10_09.md §3.3): each from Open…'s Examples, every sheet shot
  for (const [example, marker] of [['charts-dashboard', 'c-bars'], ['charts-kinds', 'k-lollipop']]) {
    await step(`The example page ${example}: every chart on every sheet draws`, async (shot) => {
      await showOpen();
      await page.locator(`#cubeswin .dc-open-example[data-example="${example}"]`).click();
      await page.waitForFunction((mark) => (window.__dataPage?.views().views ?? []).some((v) => v.id === mark), marker,
        { timeout: 120_000 });
      await settle();
      const sheets = await page.locator('.dc-sheet-tab').evaluateAll((els) => els.map((e) => e.dataset.sheet));
      let canvases = 0;
      for (const sheet of sheets) {
        await page.locator(`.dc-sheet-tab[data-sheet="${sheet}"]`).click();
        await settle();
        await page.waitForFunction(() => [...document.querySelectorAll('[data-tile] .dc-chart-tile')]
          .filter((t) => t.offsetParent !== null)
          .every((t) => t.querySelector('.dc-chartpanel-chart canvas') || t.querySelector('.dc-chartpanel-bad')),
        null, { timeout: 60_000 });
        await shot(page, sheet);
        const bad = await page.locator('[data-tile] .dc-chartpanel-bad').evaluateAll((els) =>
          els.filter((e) => e.offsetParent !== null).map((e) => e.textContent));
        if (bad.length) throw new Error(`sheet ${sheet}: ${bad.join(' | ')}`);
        canvases += await page.locator('[data-tile] .dc-chart-tile canvas').evaluateAll((els) =>
          els.filter((e) => e.offsetParent !== null).length);
      }
      return `${sheets.length} sheets, ${canvases} charts drawn`;
    });
  }

  await step('No page errors on the way', async () => {
    if (pageErrors.length) throw new Error(pageErrors.slice(0, 3).join(' | '));
    return 'none';
  });
} finally {
  await browser.close();
  closeServer();
}

// -- the gallery: one page beside the pictures ------------------------------------------------------------------------

const esc = (s) => String(s).replace(/[&<>"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' })[c]);
const bad = steps.filter((s) => !s.ok).length;
await writeFile(join(OUT, 'index.html'), `<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>DataCube tour</title>
<style>
  :root { --bg: #f7f7f5; --card: #fff; --ink: #1d1d1b; --muted: #66665f; --line: #deded8; --ok: #1a7f37; --bad: #c62828; }
  @media (prefers-color-scheme: dark) {
    :root { --bg: #161615; --card: #1f1f1d; --ink: #ececea; --muted: #a3a39c; --line: #34342f; --ok: #4ac26b; --bad: #ff6b6b; }
  }
  body { margin: 0; background: var(--bg); color: var(--ink); font: 15px/1.5 system-ui, sans-serif; }
  main { max-width: 1200px; margin: 0 auto; padding: 24px 16px 64px; }
  h1 { font-size: 22px; margin: 0 0 4px; }
  .sum { color: var(--muted); margin: 0 0 24px; }
  section { background: var(--card); border: 1px solid var(--line); border-radius: 8px; padding: 16px; margin: 0 0 16px; }
  h2 { font-size: 16px; margin: 0 0 4px; display: flex; gap: 8px; align-items: baseline; }
  .tag { font-size: 12px; font-weight: 600; padding: 1px 8px; border-radius: 99px; color: #fff; }
  .ok .tag { background: var(--ok); } .bad .tag { background: var(--bad); }
  .bad { border-color: var(--bad); }
  .detail { color: var(--muted); margin: 0 0 12px; overflow-wrap: anywhere; }
  figure { margin: 0 0 12px; } figure img { width: 100%; height: auto; border: 1px solid var(--line); border-radius: 4px; }
  figcaption { color: var(--muted); font-size: 13px; }
</style></head><body><main>
<h1>DataCube tour</h1>
<p class="sum">${steps.length - bad} of ${steps.length} steps passed${bad ? ` — ${bad} failed` : ''}. Run ${esc(new Date().toISOString())}.
The playbook: docs/DATACUBE_TOUR.md.</p>
${steps.map((s) => `<section class="${s.ok ? 'ok' : 'bad'}" id="step-${s.n}">
<h2><span class="tag">${s.ok ? 'ok' : 'failed'}</span>${s.n}. ${esc(s.title)}</h2>
<p class="detail">${esc(s.detail)}</p>
${s.shots.map((f) => `<figure><img src="${esc(f.file)}" alt="${esc(`${s.title}${f.label ? `: ${f.label}` : ''}`)}" loading="lazy">${f.label ? `<figcaption>${esc(f.label)}</figcaption>` : ''}</figure>`).join('\n')}
</section>`).join('\n')}
</main></body></html>
`);

console.log(`\n${steps.length - bad}/${steps.length} tour steps work`);
console.log(`the gallery: ${join(OUT, 'index.html')}`);
process.exit(bad ? 1 : 0);
