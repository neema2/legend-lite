// A PAGE OF SEVERAL CUBES, in a real browser (plan F6): demo/page.html puts two cubes on one
// DuckDB and one planner. Each must stay its own -- a drag, a keystroke, a window, a removal
// reach the cube they belong to and no other.
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

const A = 'by-region';
const B = 'by-desk';
const tile = (id) => `[data-tile="${id}"]`;
const rowsOf = (id) => page.evaluate((t) => window.__page.cubes[t]?.snapshot.rows ?? null, id);

/** Until no cube is busy and no cube has told the page of a change for a moment: an awaited condition, measured in
 *  the page (G-11), never a sleep in the harness. */
async function settle() {
  await page.waitForFunction((quietMs) => {
    const state = JSON.stringify([
      window.__page.changes,
      Object.values(window.__page.cubes).map((c) => c.busy),
    ]);
    const now = performance.now();
    if (window.__settleState !== state) {
      window.__settleState = state;
      window.__settleSince = now;
      return false;
    }
    return !state.includes('true') && now - window.__settleSince > quietMs;
  }, 300, { timeout: 30_000, polling: 50 }).catch(() => { throw new Error('the page did not settle'); });
}

/** Drag a column from `from`'s columns list into `to`'s Row Groups. */
async function dragToRows(from, column, to) {
  await page.locator(`${tile(from)} .dc-tool-panel-row[data-column="${column}"]`).first()
    .dragTo(page.locator(`${tile(to)} .dc-zone-rows`).first(), { timeout: 10_000 });
  await settle();
}

try {
  await page.goto(`http://127.0.0.1:${port}/demo/page.html`);
  await page.waitForFunction(() => window.__page?.ready === true, null, { timeout: 120_000 });
  await settle();

  await check('two cubes on one page, each with its own rows', async () => {
    const counts = await Promise.all([A, B].map((id) => page.locator(`${tile(id)} .dc-row`).count()));
    if (counts.some((n) => n === 0)) throw new Error(`rows per tile: ${counts}`);
    const marks = await page.locator('.dc-app').evaluateAll((els) => els.map((e) => e.dataset.dcCube));
    if (new Set(marks).size !== 2) throw new Error(`cube marks: ${marks}`);
    return `rows ${counts.join(' and ')}`;
  });

  await check('a column dragged within a cube groups that cube, not the other', async () => {
    const before = [await rowsOf(A), await rowsOf(B)];
    await dragToRows(A, 'book', A);
    const after = [await rowsOf(A), await rowsOf(B)];
    if (!after[0].includes('book')) throw new Error(`A did not group by book: ${after[0]}`);
    if (JSON.stringify(after[1]) !== JSON.stringify(before[1])) throw new Error(`B changed: ${after[1]}`);
    return `A ${after[0].join(' > ')}; B ${after[1].join(' > ')}`;
  });

  await check('a column dragged from one cube does not land on the other', async () => {
    const before = await rowsOf(B);
    await dragToRows(A, 'qtr', B);
    const after = await rowsOf(B);
    if (JSON.stringify(after) !== JSON.stringify(before)) throw new Error(`B took A's column: ${after}`);
    return `B still ${after.join(' > ')}`;
  });

  await check('Ctrl-Z undoes the cube it is pressed in, and only it', async () => {
    const mod = process.platform === 'darwin' ? 'Meta' : 'Control';
    await page.locator(`${tile(B)} .dc-row`).first().click();
    await page.keyboard.press(`${mod}+z`);
    await settle();
    if (!(await rowsOf(A)).includes('book')) throw new Error('Ctrl-Z in B undid A');
    await page.locator(`${tile(A)} .dc-row`).first().click();
    await page.keyboard.press(`${mod}+z`);
    await settle();
    const a = await rowsOf(A);
    if (a.includes('book')) throw new Error(`Ctrl-Z in A did not undo A: ${a}`);
    return `A back to ${a.join(' > ')}`;
  });

  await check('a window opened from a tile floats over the page, not inside the tile', async () => {
    const mod = process.platform === 'darwin' ? 'Meta' : 'Control';
    await page.locator(`${tile(B)} .dc-row`).first().click();
    await page.keyboard.press(`${mod}+e`);
    const win = page.locator('#page > .dc-app-overlay').first();
    await win.waitFor({ timeout: 10_000 });
    const [cubeOfWin, cubeOfB] = await Promise.all([
      win.evaluate((w) => w.dataset.dcCube),
      page.locator(`${tile(B)} .dc-app`).evaluate((e) => e.dataset.dcCube),
    ]);
    if (cubeOfWin !== cubeOfB) throw new Error(`the window is marked ${cubeOfWin}, B is ${cubeOfB}`);
    const [w, t] = await Promise.all([win.boundingBox(), page.locator(tile(B)).boundingBox()]);
    if (!w || !t || w.width <= t.width) throw new Error(`window ${w?.width}px wide in a ${t?.width}px tile`);
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
    return `${Math.round(w.width)}px window over a ${Math.round(t.width)}px tile`;
  });

  await check('removing a tile disposes its cube; the other keeps working', async () => {
    await page.locator(`${tile(A)} .tile-remove`).click();
    await settle();
    if (await page.locator(tile(A)).count()) throw new Error('tile A is still there');
    const before = await rowsOf(B);
    await dragToRows(B, 'region', B);
    const after = await rowsOf(B);
    if (!after.includes('region')) throw new Error(`B did not group by region: ${after}`);
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
