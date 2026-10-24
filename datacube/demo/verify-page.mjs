// A PAGE OF SEVERAL CUBES, in a real browser (plan F6), on DataCube's own page (demo/index.html, the app: since the
// one-app design's step 1d, docs/DATACUBE_ONE_APP_DESIGN_2026_10_11.md §12, which retired the fixture page.html these
// checks first ran on): the sample grid and its copy, two cubes on one DuckDB and one planner. Each must stay its own --
// a drag, a keystroke, a window, a removal reach the cube they belong to and no other.
//
//   bazel run //datacube:verify_page            (SHOTS=<dir> also saves a screenshot)

// first: points Playwright at the Chromium Bazel fetched (as a browser_test; a no-op under bazel run)
import '../../tools/browser/pinned-chromium.mjs';
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

// the page's grids are numbered alike: the sample's first, its copy second
const A = 'grid-1';
const B = 'grid-2';
const tile = (id) => `[data-tile="${id}"]`;
const rowsOf = (id) => page.evaluate((t) => window.__dataPage.grid(t)?.snapshot.rows ?? null, id);

/** Until no cube is busy and no cube has told the page of a change for a moment: an awaited condition, measured in
 *  the page (G-11), never a sleep in the harness. */
async function settle() {
  // a fresh watch per call: a state left from the last settle would read as quiet at once
  await page.evaluate(() => { window.__settleState = undefined; });
  await page.waitForFunction((quietMs) => {
    const state = JSON.stringify([
      window.__dataCubeSignal?.changes,
      window.__dataCubeSignal?.printing,
      (window.__dataPage?.grids ?? []).map((id) => window.__dataPage.grid(id)?.busy),
    ]);
    const now = performance.now();
    if (window.__settleState !== state) {
      window.__settleState = state;
      window.__settleSince = now;
      return false;
    }
    return window.__dataPage !== undefined && window.__dataCubeSignal?.printing === 0 && !state.includes('true')
      && now - window.__settleSince > quietMs;
  }, 300, { timeout: 30_000, polling: 50 }).catch(() => { throw new Error('the page did not settle'); });
}

/** Drag a column from `from`'s columns list into `to`'s Row Groups. */
async function dragToRows(from, column, to) {
  await page.locator(`${tile(from)} .dc-tool-panel-row[data-column="${column}"]`).first()
    .dragTo(page.locator(`${tile(to)} .dc-zone-rows`).first(), { timeout: 10_000 });
  await settle();
}

/** Right-click > Create > `what`, on a cell of the grid in `scope`. */
async function insert(what, scope) {
  await page.locator(scope).first().locator('.dc-row').nth(1).locator('.dc-cell').nth(2).click({ button: 'right' });
  await page.locator('.dc-menu-item:has(> .dc-menu-label:text-is("Create"))').first().hover();
  await page.locator(`.dc-menu-item:has(> .dc-menu-label:text-is("Create")) .dc-menu-item:has(> .dc-menu-label:text-is("${what}"))`).first().click();
  await settle();
}

try {
  await page.goto(`http://127.0.0.1:${port}/demo/index.html`);
  await page.waitForSelector('.dc-row', { timeout: 120_000 });
  await settle();
  // the second cube: a copy of the sample's grid, under it
  await insert('Copy of This Grid', tile(A));
  await page.locator(`${tile(B)} .dc-row`).first().waitFor({ timeout: 20_000 });
  await settle();

  await check('two cubes on one page, each with its own rows', async () => {
    const counts = await Promise.all([A, B].map((id) => page.locator(`${tile(id)} .dc-row`).count()));
    if (counts.some((n) => n === 0)) throw new Error(`rows per tile: ${counts}`);
    const marks = await page.locator('.dc-app').evaluateAll((els) => els.map((e) => e.dataset.dcCube));
    if (new Set(marks).size !== 2) throw new Error(`cube marks: ${marks}`);
    return `rows ${counts.join(' and ')}`;
  });

  await check('a column dragged from one cube does not land on the other', async () => {
    const before = await rowsOf(B);
    // qtr: a dimension neither cube groups by yet (qty, a measure, could never land in its rows); first, while A's
    // columns list still offers it
    await dragToRows(A, 'qtr', B);
    const after = await rowsOf(B);
    if (JSON.stringify(after) !== JSON.stringify(before)) throw new Error(`B took A's column: ${after}`);
    return `B still ${after.join(' > ')}`;
  });

  await check('a column dragged within a cube groups that cube, not the other', async () => {
    const before = [await rowsOf(A), await rowsOf(B)];
    await dragToRows(A, 'qtr', A);
    const after = [await rowsOf(A), await rowsOf(B)];
    if (!after[0].includes('qtr')) throw new Error(`A did not group by qtr: ${after[0]}`);
    if (JSON.stringify(after[1]) !== JSON.stringify(before[1])) throw new Error(`B changed: ${after[1]}`);
    return `A ${after[0].join(' > ')}; B ${after[1].join(' > ')}`;
  });

  await check('Ctrl-Z undoes the cube it is pressed in, and only it', async () => {
    const mod = process.platform === 'darwin' ? 'Meta' : 'Control';
    await page.locator(`${tile(B)} .dc-row`).first().click();
    await page.keyboard.press(`${mod}+z`);
    await settle();
    if (!(await rowsOf(A)).includes('qtr')) throw new Error('Ctrl-Z in B undid A');
    await page.locator(`${tile(A)} .dc-row`).first().click();
    await page.keyboard.press(`${mod}+z`);
    await settle();
    const a = await rowsOf(A);
    if (a.includes('qtr')) throw new Error(`Ctrl-Z in A did not undo A: ${a}`);
    return `A back to ${a.join(' > ')}`;
  });

  await check('a window opened from a tile floats over the page, not inside the tile', async () => {
    const mod = process.platform === 'darwin' ? 'Meta' : 'Control';
    await page.locator(`${tile(B)} .dc-row`).first().click();
    await page.keyboard.press(`${mod}+e`);
    const win = page.locator('.dc-page > .dc-app-overlay').first();
    await win.waitFor({ timeout: 10_000 });
    const [cubeOfWin, cubeOfB] = await Promise.all([
      win.evaluate((w) => w.dataset.dcCube),
      page.locator(`${tile(B)} .dc-app`).evaluate((e) => e.dataset.dcCube),
    ]);
    if (cubeOfWin !== cubeOfB) throw new Error(`the window is marked ${cubeOfWin}, B is ${cubeOfB}`);
    // not held in the tile's box: B is a band of its own under A (a tile added goes under the one it came from), the
    // window centred on the page, reaching past it
    const [w, t] = await Promise.all([win.boundingBox(), page.locator(tile(B)).boundingBox()]);
    const inside = (o, i) => o.x >= i.x && o.y >= i.y && o.x + o.width <= i.x + i.width && o.y + o.height <= i.y + i.height;
    if (!w || !t || inside(w, t)) throw new Error(`window ${JSON.stringify(w)} inside the tile ${JSON.stringify(t)}`);
    // STYLED as it is inside a cube: the cube's tokens reach it (they are defined on the cube,
    // not the page, so a window outside it once came out unstyled)
    const [font, tokenOfWin, tokenOfCube] = await Promise.all([
      win.evaluate((w) => getComputedStyle(w).fontFamily),
      win.evaluate((w) => getComputedStyle(w).getPropertyValue('--tw-neutral-400').trim()),
      page.locator(`${tile(B)} .dc-app`).evaluate((e) => getComputedStyle(e).getPropertyValue('--tw-neutral-400').trim()),
    ]);
    if (!tokenOfWin || tokenOfWin !== tokenOfCube) throw new Error(`the window has the cube's tokens? "${tokenOfWin}" vs "${tokenOfCube}"`);
    if (!/roboto/i.test(font)) throw new Error(`the window's font is ${font}`);
    // SHOTS=<dir>: the page with the window open, to look at
    if (process.env.SHOTS) await page.screenshot({ path: `${process.env.SHOTS}/page-window.png` });
    await page.keyboard.press('Escape');
    await win.locator('.dc-overlay-close, [aria-label="Close"]').first().click().catch(() => {});
    return `a ${Math.round(w.width)}x${Math.round(w.height)} window reaching past its ${Math.round(t.width)}x${Math.round(t.height)} tile`;
  });

  await check('removing a tile disposes its cube; the other keeps working', async () => {
    await page.locator(`${tile(A)} .dc-tile-remove`).click();
    await settle();
    if (await page.locator(tile(A)).count()) throw new Error('tile A is still there');
    const before = await rowsOf(B);
    // B alone now sits in the page's bar (a page of one grid): the one cube left, its columns panel (folded, as a copy
    // starts) opened first
    const cube = page.locator('.dc-app');
    if (await cube.count() !== 1) throw new Error(`${await cube.count()} cubes left`);
    await cube.locator('.dc-tool-panel-rail').click();
    await cube.locator('.dc-tool-panel-row[data-column="qtr"]').first()
      .dragTo(cube.locator('.dc-zone-rows').first(), { timeout: 10_000 });
    await settle();
    const after = await rowsOf(B);
    if (!after.includes('qtr')) throw new Error(`B did not group by qtr: ${after}`);
    return `B ${before.join(' > ')} -> ${after.join(' > ')}`;
  });

  await check('no page errors', async () => {
    if (pageErrors.length) throw new Error(pageErrors.slice(0, 2).join(' | '));
  });
} finally {
  await browser.close();
  closeServer();
}

const bad = results.filter((r) => !r.ok).length;
console.log(`\n${results.length - bad}/${results.length} page checks work`);
process.exit(bad ? 1 : 0);
