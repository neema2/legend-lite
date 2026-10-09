// A PAGE ARRANGED BY HAND, in a real browser (docs/DATACUBE_PAGES_DESIGN_2026_10_09.md §3.6): the demo's grid, two
// charts and a copy of the grid laid out as bands -- placed beside the tile they came from, a preset previewed and
// applied, tiles dragged onto a tile's edge and between bands, a drag cancelled, a divider and a band's edge dragged,
// the page fitting its window, a tile maximised, the keyboard, Undo Layout, the page locked, and the window narrowed
// and widened -- with the browser's own long-task timing: no task over 50 ms while a tile is dragged (nothing inside a
// tile re-lays out until it is let go, §3.4).
//
//   bazel run //datacube:verify_layout            (SHOTS=<dir> also saves a screenshot)

// first: points Playwright at the Chromium Bazel fetched (as a browser_test; a no-op under bazel run)
import '../../tools/browser/pinned-chromium.mjs';
import { chromium } from 'playwright';
import { frames, outPath, serve, siteRoot } from './harness.mjs';

const ROOT = siteRoot();
const { port, close: closeServer } = await serve(ROOT);

const browser = await chromium.launch();
const page = await (await browser.newContext({ viewport: { width: 1400, height: 900 } })).newPage();
const pageErrors = [];
page.on('pageerror', (e) => pageErrors.push(e.message));
// the browser's long tasks, from the start: each drag reads those inside its own window of time
await page.addInitScript(() => {
  window.__longTasks = [];
  new PerformanceObserver((list) => {
    for (const e of list.getEntries()) window.__longTasks.push({ start: e.startTime, duration: e.duration });
  }).observe({ type: 'longtask', buffered: true });
});

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

const tile = (id) => `[data-tile="${id}"]`;
/** Every tile's box on screen, by id (a hidden tile: none). */
const boxes = () => page.evaluate(() => Object.fromEntries([...document.querySelectorAll('.dc-band-tile')]
  .filter((t) => !t.hidden)
  .map((t) => {
    const r = t.getBoundingClientRect();
    return [t.dataset.tile, { x: Math.round(r.x), y: Math.round(r.y), w: Math.round(r.width), h: Math.round(r.height) }];
  })));
const board = () => page.evaluate(() => {
  const b = document.querySelector('.dc-bands');
  const r = b.getBoundingClientRect();
  return { x: r.x, y: r.y, w: b.clientWidth, h: b.clientHeight, scroll: b.scrollHeight };
});
const same = (a, b) => JSON.stringify(a) === JSON.stringify(b);
const near = (a, b, by = 3) => Math.abs(a - b) <= by;

/** The longest task (ms) between two moments of the page's clock. */
const longest = (from, to) => page.evaluate(([a, b]) => Math.max(0, ...window.__longTasks
  .filter((t) => t.start + t.duration >= a && t.start <= b).map((t) => t.duration)), [from, to]);
const now = () => page.evaluate(() => performance.now());
/** Every drag's longest task, by what was dragged. */
const dragTasks = [];

/** Press at (x, y), move to (tx, ty) in `steps`, then let go -- or, `cancel`, press Escape first. */
async function drag(x, y, tx, ty, { steps = 12, cancel = false, what = 'a tile' } = {}) {
  const from = await now();
  await page.mouse.move(x, y);
  await page.mouse.down();
  await page.mouse.move(tx, ty, { steps });
  await frames(page, 2);
  if (cancel) await page.keyboard.press('Escape');
  await page.mouse.up();
  await frames(page, 2);
  dragTasks.push({ what, ms: await longest(from, await now()) });
}

/** A tile's title bar, by the middle of its title. */
const head = async (id) => {
  const r = await page.locator(`${tile(id)} .dc-tile-title`).boundingBox();
  return { x: r.x + Math.min(30, r.width / 2), y: r.y + r.height / 2 };
};

/** The title bar's menu, then the entry a person reads. */
async function titleMenu(label) {
  await page.keyboard.press('Escape');
  await page.locator('.dc-titlebar-menu').first().click();
  await page.locator(`.dc-menu [role^="menuitem"]:has(> .dc-menu-label:text-is("${label}"))`).first().click();
}

/** Right-click > Insert > `what`, on a cell of the grid in `scope`. */
async function insert(what, scope) {
  await page.locator(scope).first().locator('.dc-row').nth(1).locator('.dc-cell').nth(2).click({ button: 'right' });
  await page.locator('.dc-menu-item:has(> .dc-menu-label:text-is("Insert"))').first().hover();
  await page.locator(`.dc-menu-item:has(> .dc-menu-label:text-is("Insert")) .dc-menu-item:has(> .dc-menu-label:text-is("${what}"))`).first().click();
  await settle();
}

try {
  await page.goto(`http://127.0.0.1:${port}/demo/index.html`);
  await page.waitForSelector('.dc-row', { timeout: 120_000 });
  await settle();

  let ids = {};
  await check('a new tile goes beside the tile it came from while each stays readable, else in a band below', async () => {
    await insert('Visualization', '.dc-app');
    await insert('Visualization', tile('grid'));
    await insert('Copy of Grid', tile('grid'));
    await page.locator('[data-tile^="grid-"] .dc-row').first().waitFor({ timeout: 20_000 });
    await settle();
    const all = await page.locator('.dc-band-tile').evaluateAll((els) => els.map((e) => e.dataset.tile));
    const charts = all.filter((id) => id.startsWith('chart-'));
    ids = { grid: 'grid', a: charts[0], b: charts[1], copy: all.find((id) => id.startsWith('grid-')) };
    const b = await boxes();
    if (!(b[ids.a].y === b.grid.y && b[ids.b].y === b.grid.y && b[ids.a].x > b.grid.x && b[ids.b].x > b[ids.a].x)) {
      throw new Error(`the charts are not beside the grid: ${JSON.stringify(b)}`);
    }
    if (!(b[ids.copy].y > b.grid.y + b.grid.h)) throw new Error(`the copy is not below: ${JSON.stringify(b)}`);
    return `${ids.a} and ${ids.b} beside the grid (three at 1400px), ${ids.copy} below`;
  });

  await check('Arrange... previews 2 x 2 while pointed at, the page as it was when the pointer leaves, and applies it on a click', async () => {
    const before = await boxes();
    const saved = await page.evaluate(() => JSON.stringify(window.__dataCube.pageViews().layout));
    await titleMenu('Arrange…');
    const option = page.locator('.dc-layout-picker [data-preset="rows:2-2"]');
    await option.hover();
    await frames(page, 2);
    const previewed = await boxes();
    if (!(previewed.grid.y === previewed[ids.a].y && previewed[ids.b].y > previewed.grid.y && previewed[ids.b].x === previewed.grid.x)) {
      throw new Error(`not previewed as 2 x 2: ${JSON.stringify(previewed)}`);
    }
    if (await page.evaluate(() => JSON.stringify(window.__dataCube.pageViews().layout)) !== saved) throw new Error('a preview changed the layout');
    await page.mouse.move(5, 5);
    await frames(page, 2);
    if (!same(await boxes(), before)) throw new Error('the page did not come back when the pointer left');
    await option.hover();
    await option.click();
    await settle();
    const after = await boxes();
    if (!same(after, previewed)) throw new Error('applied differs from previewed');
    if (await page.locator('.dc-layout-picker').count()) throw new Error('the picker stayed open');
    return 'previewed, put back, applied';
  });

  await check('a tile dragged onto another\'s right half takes that half; the zone is outlined while dragging', async () => {
    const b = await boxes();
    const from = await head(ids.b);
    const tx = b.grid.x + b.grid.w * 0.92;
    const ty = b.grid.y + b.grid.h / 2;
    await page.mouse.move(from.x, from.y);
    await page.mouse.down();
    await page.mouse.move(tx, ty, { steps: 12 });
    await frames(page, 2);
    const zone = await page.locator('.dc-bands-zone').boundingBox();
    if (!zone || !near(zone.x + zone.width, b.grid.x + b.grid.w) || !near(zone.width, b.grid.w / 2, 4)) {
      throw new Error(`the zone is not the grid's right half: ${JSON.stringify(zone)}`);
    }
    await page.mouse.up();
    await frames(page, 2);
    await settle();
    const after = await boxes();
    if (!(after[ids.b].y === after.grid.y && after[ids.b].x > after.grid.x && after[ids.b].x < after[ids.a].x)) {
      throw new Error(`not beside the grid: ${JSON.stringify(after)}`);
    }
    return `${ids.b} now between the grid and ${ids.a}`;
  });

  await check('a tile dragged onto the line between two bands is a band of its own there', async () => {
    const b = await boxes();
    const from = await head(ids.a);
    const top = b.grid.y + b.grid.h;
    await drag(from.x, from.y, b.grid.x + 200, top + 4, { what: 'a tile between bands' });
    await settle();
    const after = await boxes();
    const full = (await board()).w;
    if (!(near(after[ids.a].w, full, 2) && after[ids.a].y > after.grid.y && after[ids.a].y < after[ids.copy].y)) {
      throw new Error(`not a band of its own between: ${JSON.stringify(after)}`);
    }
    return `${ids.a} full width, between the first band and ${ids.copy}`;
  });

  await check('Escape cancels a drag: nothing moved', async () => {
    const before = await boxes();
    const from = await head(ids.copy);
    await drag(from.x, from.y, before.grid.x + 40, before.grid.y + 60, { cancel: true, what: 'a cancelled drag' });
    await settle();
    if (!same(await boxes(), before)) throw new Error('the page changed');
    if (await page.locator('.dc-tile-dragging').count()) throw new Error('a tile is still being dragged');
  });

  await check('a divider dragged resizes the two tiles beside it, live; a double click evens them out', async () => {
    const before = await boxes();
    const divider = page.locator('.dc-band-divider-row').first();
    const d = await divider.boundingBox();
    const from = await now();
    await page.mouse.move(d.x + d.width / 2, d.y + d.height / 2);
    await page.mouse.down();
    await page.mouse.move(d.x + d.width / 2 + 120, d.y + d.height / 2, { steps: 12 });
    await frames(page, 2);
    const during = await boxes();
    await page.mouse.up();
    await frames(page, 2);
    dragTasks.push({ what: 'a divider', ms: await longest(from, await now()), live: true });
    await settle();
    const after = await boxes();
    if (!near(during.grid.w, before.grid.w + 120, 3)) throw new Error(`not live: ${before.grid.w} -> ${during.grid.w}`);
    if (!near(after.grid.w, before.grid.w + 120, 3)) throw new Error(`not kept: ${before.grid.w} -> ${after.grid.w}`);
    await page.locator('.dc-band-divider-row').first().dblclick();
    await settle();
    const even = await boxes();
    if (!near(even.grid.w, even[ids.b].w, 2)) throw new Error(`not even: ${even.grid.w} and ${even[ids.b].w}`);
    return `the grid ${before.grid.w}px -> ${after.grid.w}px, then even at ${even.grid.w}px`;
  });

  await check('a band\'s edge dragged changes its height', async () => {
    await page.locator('.dc-band-edge').first().scrollIntoViewIfNeeded();
    const before = await boxes();
    const edge = await page.locator('.dc-band-edge').first().boundingBox();
    await drag(edge.x + 300, edge.y + edge.height / 2, edge.x + 300, edge.y + edge.height / 2 + 60, { what: 'a band\'s edge' });
    dragTasks.at(-1).live = true;
    await settle();
    const after = await boxes();
    if (!near(after.grid.h, before.grid.h + 60, 3)) throw new Error(`${before.grid.h} -> ${after.grid.h}`);
    return `${before.grid.h}px -> ${after.grid.h}px`;
  });

  await check('fit to window: the bands share the window and nothing scrolls; unticked, the page scrolls again', async () => {
    await titleMenu('Arrange…');
    await page.locator('.dc-layout-fit input').check();
    await page.keyboard.press('Escape');
    await settle();
    const fit = await board();
    const b = await boxes();
    const bottom = Math.max(...Object.values(b).map((t) => t.y + t.h)) - fit.y;
    if (fit.scroll > fit.h + 1 || !near(bottom, fit.h, 2)) throw new Error(`board ${JSON.stringify(fit)}, tiles to ${bottom}`);
    await titleMenu('Arrange…');
    await page.locator('.dc-layout-fit input').uncheck();
    await page.keyboard.press('Escape');
    await settle();
    const scrolls = await board();
    if (!(scrolls.scroll > scrolls.h)) throw new Error(`the page does not scroll: ${JSON.stringify(scrolls)}`);
    return `fitted ${fit.h}px; scrolling ${scrolls.scroll}px in ${scrolls.h}px`;
  });

  await check('a tile maximised fills the board, the others hidden; again, the page as it was', async () => {
    const before = await boxes();
    await page.locator(`${tile(ids.b)} .dc-tile-maximise`).click();
    await frames(page, 2);
    const max = await boxes();
    const b = await board();
    if (Object.keys(max).length !== 1 || !near(max[ids.b].w, b.w, 2) || !near(max[ids.b].h, b.h, 2)) {
      throw new Error(`maximised: ${JSON.stringify(max)} in ${JSON.stringify(b)}`);
    }
    await page.locator(`${tile(ids.b)} .dc-tile-maximise`).click();
    await settle();
    if (!same(await boxes(), before)) throw new Error('not as it was');
  });

  await check('a focused tile swaps with its neighbour on an arrow key; Undo Layout puts it back', async () => {
    const before = await boxes();
    await page.locator(tile(ids.b)).focus();
    await page.keyboard.press('ArrowLeft');
    await settle();
    const swapped = await boxes();
    if (!(swapped[ids.b].x < swapped.grid.x)) throw new Error(`not swapped: ${JSON.stringify(swapped)}`);
    await titleMenu('Undo Layout');
    await settle();
    if (!same(await boxes(), before)) throw new Error('Undo Layout did not put it back');
  });

  await check('Edit Layout unticked locks the page: no handles, and a drag moves nothing', async () => {
    await titleMenu('Edit Layout');
    const before = await boxes();
    if (await page.locator('.dc-band-divider, .dc-band-edge').count()) throw new Error('handles still shown');
    const from = await head(ids.copy);
    await drag(from.x, from.y, before.grid.x + 40, before.grid.y + 60, { what: 'a locked page' });
    if (!same(await boxes(), before)) throw new Error('a tile moved');
    await titleMenu('Edit Layout');
    if (!(await page.locator('.dc-band-divider').count())) throw new Error('no handles once unlocked');
  });

  await check('a narrow window stacks every tile, full width, with nothing to drag; widened, the layout comes back', async () => {
    const before = await boxes();
    await page.setViewportSize({ width: 560, height: 900 });
    await frames(page, 4);
    await settle();
    const narrow = await boxes();
    const b = await board();
    const lefts = new Set(Object.values(narrow).map((t) => t.x));
    if (lefts.size !== 1 || Object.values(narrow).some((t) => !near(t.w, b.w, 2))) throw new Error(`not stacked: ${JSON.stringify(narrow)}`);
    if (await page.locator('.dc-band-divider, .dc-band-edge').count()) throw new Error('handles on a narrow page');
    await page.setViewportSize({ width: 1400, height: 900 });
    await frames(page, 4);
    await settle();
    if (!same(await boxes(), before)) throw new Error(`not back: ${JSON.stringify(await boxes())}`);
    return `${Object.keys(narrow).length} tiles stacked at ${b.w}px`;
  });

  await check('no task over 50 ms while a tile is dragged (a divider or an edge, which follow live: reported)', async () => {
    const report = dragTasks.map((d) => `${d.what} ${Math.round(d.ms)} ms`).join('; ');
    const slow = dragTasks.filter((d) => !d.live && d.ms > 50);
    if (slow.length) throw new Error(`${slow.map((d) => `${d.what}: ${Math.round(d.ms)} ms`).join('; ')} (all: ${report})`);
    return report;
  });

  if (process.env.SHOTS || process.env.TEST_UNDECLARED_OUTPUTS_DIR) {
    await frames(page, 4);
    await page.screenshot({ path: outPath('layout.png') });
  }

  await check('no page errors', async () => {
    if (pageErrors.length) throw new Error(pageErrors.slice(0, 2).join(' | '));
  });
} finally {
  await browser.close();
  closeServer();
}

const bad = results.filter((r) => !r.ok).length;
console.log(`\n${results.length - bad}/${results.length} layout checks work`);
process.exit(bad ? 1 : 0);
