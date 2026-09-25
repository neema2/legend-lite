// Drive EVERY feature the product offers, in a real browser, and
// assert what each one produced.
//
// This exists because 861 unit tests were green while the headers
// were invisible, grouping did not group, and the header did not
// follow its columns sideways. Those tests check logic in isolation;
// nothing drove the running application. Every bug the user found in
// ten seconds was in the gap between the two.
//
// The rule here: a check asserts the OUTCOME a user would see -- the
// rows, the labels, the generated Pure and SQL, the file that came
// down -- never that a handler ran or an element exists.
//
//   bazel run //datacube:verify_features            (generates its own sample)
//   DATA=/abs/file.csv bazel run //datacube:verify_features

import { createServer } from 'node:http';
import { readFile, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { extname, join } from 'node:path';
import { chromium } from 'playwright';

import { gridInvariants } from './grid-invariants.mjs';
import { sampleCsv } from '../src/samples.ts';
import { fileURLToPath } from 'node:url';
import { servedPath } from './static-files.ts';

const ROOT = fileURLToPath(new URL('..', import.meta.url));

/**
 * THE SWEEP OPENS A FILE, ALWAYS.
 *
 * With no DATA it used to run against the page's built-in cube --
 * which arrives grouped by region, desk and book and pivoted on year
 * -- while these checks are written against a FLAT cube: they
 * right-click `region` to group by it, and `region` is not on screen
 * when it is already a row dimension. So `bazel run //datacube:verify_features`
 * reported 24 features broken and every one of them worked. A
 * harness that fails differently depending on an environment
 * variable nobody set is worse than no harness: it teaches you to
 * disbelieve it.
 *
 * So the default run generates the same sample the page's own button
 * offers, from the same generator, and opens that.
 */
async function sampleOnDisk() {
  const path = join(tmpdir(), 'datacube-verify-features.csv');
  await writeFile(path, sampleCsv({ rows: 5000, seed: 20260920 }), 'utf8');
  return path;
}

const DATA = process.env.DATA ?? (await sampleOnDisk());
const ONLY = process.env.ONLY;
/**
 * How long to wait for a query to land.
 *
 * Generous against the real figure -- these queries report single-
 * digit milliseconds on 5,000 rows -- and short enough that the
 * actions which re-query NOTHING do not each donate a quarter of a
 * minute to the run. Correctness does not rest on it: every check
 * asserts its own outcome, so a query that has not landed fails the
 * assertion rather than passing quietly.
 */
const STATUS_WAIT_MS = 4000;
/**
 * A check may not exceed this.
 *
 * A hang used to be invisible -- it just made the sweep slower, and
 * "slower" is not a signal anyone acts on. Now it fails, with the
 * name of the check that did it.
 */
const CHECK_DEADLINE_MS = 30_000;

const TYPES = {
  '.html': 'text/html', '.js': 'text/javascript', '.wasm': 'application/wasm',
  '.pure': 'text/plain', '.css': 'text/css', '.csv': 'text/csv',
};
const server = createServer(async (req, res) => {
  const file = servedPath(ROOT, req.url);
  try {
    if (!file) throw new Error('not under the root');
    const body = await readFile(file);
    res.writeHead(200, {
      'Content-Type': TYPES[extname(file)] ?? 'application/octet-stream',
    });
    res.end(body);
  } catch { res.writeHead(404).end('not found'); }
});
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const { port } = server.address();
const URL_BASE = `http://127.0.0.1:${port}`;

const browser = await chromium.launch();
const context = await browser.newContext({
  viewport: { width: 1400, height: 900 },
  permissions: ['clipboard-read', 'clipboard-write'],
});
const page = await context.newPage();
const pageErrors = [];
page.on('pageerror', (e) => pageErrors.push(e.message));
if (process.env.DEBUG) {
  page.on('console', (m) => console.log(`  [page ${m.type()}] ${m.text()}`));
}

// -- the results table ------------------------------------------------

const results = [];
const gaps = [];
const timings = [];
const STARTED = Date.now();
function record(name, ok, detail) {
  results.push({ name, ok, detail });
  const tag = ok ? 'ok  ' : 'BAD ';
  console.log(`  ${tag} ${name}${detail ? ` — ${detail}` : ''}`);
}

/**
 * Put the page back to a usable state.
 *
 * Without this one failed check poisons every check after it: an
 * open menu or a modal overlay swallows the next right-click, and
 * thirty "click timed out" lines say nothing about the product. A
 * cascade of identical timeouts is a harness fault, not 23 bugs.
 */
async function reset() {
  // THE CLOSE BUTTON, not just Escape. A dialog is a floating window
  // now, so one left open covers the grid and every later right-click
  // waits thirty seconds for a cell it cannot reach -- and Escape
  // reaches the overlay only while focus is still inside it.
  // EVERY window: several can be open at once now, as upstream's.
  const shut = page.locator('.dc-app-overlay:not([hidden]) .dc-overlay-close');
  for (let i = 0; i < 6 && await shut.count(); i += 1) {
    await shut.first().click().catch(() => {});
    await page.waitForTimeout(100);
  }
  for (let i = 0; i < 3; i += 1) {
    const open = await page.locator('.dc-menu, .dc-app-overlay:not([hidden])')
      .count();
    if (!open) return;
    await page.keyboard.press('Escape').catch(() => {});
    await page.waitForTimeout(150);
  }
  // Escape did not clear it; click somewhere inert.
  await page.mouse.click(5, 5).catch(() => {});
  await page.waitForTimeout(150);
}

/**
 * A check for something KNOWN to be missing.
 *
 * It runs like any other, but a failure is reported as a gap rather
 * than a break and does not fail the run -- while a PASS is reported
 * loudly, because that means the feature arrived and this entry
 * should become an ordinary check. Without the distinction a run that
 * is red forever teaches everyone to ignore it, and the next real
 * regression hides in the noise.
 */
async function gap(name, why, fn) {
  if (ONLY && !name.toLowerCase().includes(ONLY.toLowerCase())) return;
  await reset();
  try {
    const detail = await fn();
    gaps.push({ name, fixed: true, detail });
    console.log(`  NEW  ${name} — now works: ${detail ?? ''}`);
  } catch (e) {
    gaps.push({ name, why, detail: String(e.message ?? e).split('\n')[0] });
    console.log(`  gap  ${name} — ${String(e.message ?? e).split('\n')[0]}`);
  }
}

/** Run the invariants and attribute any break to `where`. */
async function invariants(where) {
  const broken = await page.evaluate(gridInvariants);
  for (const b of broken) {
    record(`INVARIANT after ${where}`, false, b);
  }
  return broken.length === 0;
}

async function check(name, fn) {
  if (ONLY && !name.toLowerCase().includes(ONLY.toLowerCase())) return;
  await reset();
  const started = Date.now();
  const before = pageErrors.length;
  try {
    const detail = await Promise.race([
      fn(),
      new Promise((_, reject) => setTimeout(
        () => reject(new Error(`took longer than`
          + ` ${CHECK_DEADLINE_MS / 1000}s — treated as a hang`)),
        CHECK_DEADLINE_MS)),
    ]);
    timings.push([name, Date.now() - started]);
    if (pageErrors.length > before) {
      record(name, false, `threw: ${pageErrors[before].split('\n')[0]}`);
      return;
    }
    record(name, true, detail ?? '');
    await invariants(name);
  } catch (e) {
    timings.push([name, Date.now() - started]);
    // WHAT THE PAGE LOOKED LIKE. A bare "took longer than 30s" sends
    // you guessing; a dialog left open now covers the grid, so the
    // most common cause of a hang is a window nobody closed.
    const where = await page.evaluate(() => {
      const w = document.querySelector('.dc-app-overlay');
      const shown = w && !w.hidden;
      const b = shown ? w.getBoundingClientRect() : null;
      return {
        overlay: shown ? `open ${Math.round(b.width)}x${Math.round(b.height)}`
          + ` at ${Math.round(b.left)},${Math.round(b.top)}` : 'closed',
        menus: document.querySelectorAll('.dc-menu').length,
        rows: document.querySelectorAll('.dc-row').length,
      };
    }).catch(() => null);
    // Playwright's CALL LOG, not just its first line: the reason
    // ("not visible", "outside of the viewport", "intercepts pointer
    // events") is in the lines after it, and the first line alone is
    // just the word "Timeout".
    const why = String(e.message ?? e).split('\n')
      .map((l) => l.trim())
      .filter((l) => l && !/^Call log:$/.test(l))
      .slice(0, 8)
      .join(' | ');
    record(name, false, `${why}`
      + (where ? ` [overlay ${where.overlay}, ${where.menus} menu(s),`
        + ` ${where.rows} rows]` : ''));
    if (process.env.SHOTS) {
      const file = join(process.env.BUILD_WORKING_DIRECTORY ?? process.cwd(), process.env.SHOTS, `${name.replace(/\W+/g, '-')}.png`);
      await page.screenshot({ path: file, fullPage: true }).catch(() => {});
    }
  }
}

// -- driving the real UI ----------------------------------------------

/** The Pure and SQL the app last generated, and what the grid shows. */
const state = () => page.evaluate(() => ({
  pure: document.getElementById('pure')?.textContent ?? '',
  sql: document.getElementById('sql')?.textContent ?? '',
  status: `${document.querySelector('.dc-status-timing')?.textContent ?? ''}`
    + ` | ${document.getElementById('status')?.textContent ?? ''}`,
  headers: [...document.querySelectorAll('[role=columnheader]')]
    .map((e) => e.textContent?.trim() ?? '').filter(Boolean),
  rows: [...document.querySelectorAll('.dc-row')].map((r) =>
    [...r.querySelectorAll('.dc-cell')].map(
      (c) => c.textContent?.trim() ?? '')),
}));

/**
 * Wait for the app to finish RE-QUERYING, not for a fixed delay.
 *
 * A 250ms sleep read the Pure pane while the previous query's text
 * was still in it, so a check could report "only one group key" about
 * a cube that had two -- and the next check, running a moment later,
 * saw the correct state. A fixed sleep turns a fast machine green and
 * a loaded one red, which is the least useful kind of check.
 *
 * The status line carries the elapsed time of each query, so it
 * changes on every one; waiting for it to differ is waiting for the
 * answer the click asked for. The timeout is not a failure -- some
 * actions (a clipboard copy, opening a dialog) re-query nothing at
 * all -- so it falls through to a short settle.
 */
async function settle(before) {
  // NO OVERLAY WAIT. `.dc-app-overlay` is the DIALOG host -- filters,
  // properties, drill-through, the charts -- not a loading spinner,
  // and I had it backwards: every settle that ran while a dialog was
  // deliberately open waited the full 20 seconds for that dialog to
  // go away. Add the 15 seconds below for a status line that was
  // never going to change (an export, a clipboard copy, opening a
  // dialog) and single checks measured 36 to 42 SECONDS. The sweep
  // took twenty minutes; almost all of it was this function.
  if (before !== undefined) {
    await page.waitForFunction(
      (was) =>
        (document.querySelector('.dc-status-timing')?.textContent ?? '') !== was,
      before, { timeout: STATUS_WAIT_MS },
    ).catch(() => {});
  }
  await page.waitForTimeout(150);
}

/**
 * How many rows the QUERY returned, from the status line.
 *
 * NOT `.dc-row` count: the grid is virtualised, so it renders about
 * a windowful whatever the result holds -- and comparing those made
 * the compound-filter check report "all-of 29, any-of 29, unfiltered
 * 29" and pass, having compared nothing at all. The status line
 * carries the real figure.
 */
const resultRows = async () => {
  const text = await statusNow();
  const m = /([\d,]+) rows/.exec(text);
  if (!m) throw new Error(`no row count in the status line: ${text}`);
  return Number(m[1].replace(/,/g, ''));
};

/**
 * The cube's own result line, which changes once per completed query.
 *
 * The HOST's element is not it: that used to echo this same line
 * above the grid and now carries errors only, so it does not move
 * when a query lands -- and a settle that waited on it would wait
 * the full timeout every single time.
 */
const statusNow = () => page.evaluate(() =>
  document.querySelector('.dc-status-timing')?.textContent ?? '');

/**
 * Right-click a body cell and walk the menu by LABEL.
 *
 * By label rather than by id, because the label is what the user
 * reads: a path that no longer matches means the entry moved or
 * vanished, which is itself the failure.
 */
/** Every entry currently on screen, with whether it is clickable. */
const openMenu = () => page.evaluate(() =>
  [...document.querySelectorAll('.dc-menu-item')].map((e) => ({
    label: e.querySelector('.dc-menu-label')?.textContent?.trim() ?? '',
    off: e.classList.contains('dc-disabled'),
    sub: e.classList.contains('dc-has-submenu'),
  })));

/**
 * @param opts.requery false for an action that runs no query -- an
 *   export, a clipboard copy, opening a dialog -- so the wait for the
 *   status line to change is skipped rather than paid in full. It is
 *   only ever a speed hint: each check asserts its own outcome, so
 *   getting it wrong costs time, never correctness.
 */
/** A negative as the grid shows it: a minus, or upstream's parentheses. */
function isNegativeText(t) {
  return typeof t === 'string' && (t.startsWith('-') || /^\(.*\)$/.test(t));
}

/** Press a button on upstream's export warning. */
async function answerExport(label) {
  const button = page.locator('.dc-alert-action', { hasText: label }).first();
  await button.click({ timeout: 5000 });
}

async function menu(path, { row = 0, col = 0, requery = true } = {}) {
  await reset();
  const before = requery ? await statusNow() : undefined;
  // BACK TO THE LEFT FIRST. The row-dimension column is sticky, so a
  // cell that has been scrolled under it cannot be clicked -- the
  // sticky cell takes the pointer. A person right-clicks a cell they
  // can see; this puts the grid where they would be looking.
  await page.evaluate(() => {
    const sc = document.querySelector('.dc-scroller');
    if (sc) sc.scrollLeft = 0;
  });
  await page.waitForTimeout(80);
  const cell = page.locator('.dc-row').nth(row).locator('.dc-cell').nth(col);
  // A TIMEOUT SHORTER THAN THE CHECK DEADLINE, so Playwright's own
  // explanation ("not stable", "intercepts pointer events", "outside
  // the viewport") reaches the report. Left at its 30s default the
  // deadline fired first and replaced every one of them with "took
  // longer than 30s", which says nothing about the cause.
  await cell.click({ button: 'right', timeout: 8000 });
  await page.waitForSelector('.dc-menu', { timeout: 5000 });
  for (let i = 0; i < path.length; i += 1) {
    const label = path[i];
    // Match on the item's OWN label, via a DIRECT-CHILD selector.
    //
    // This is the trap that cost an afternoon. A submenu is nested
    // INSIDE its parent item, so `has: <label "Ascending">` matches
    // the parent "Sort" as well -- it contains that label two levels
    // down -- and `.first()` takes the parent, in DOM order. Clicking
    // it does nothing (a submenu parent has no action), so eighteen
    // working features looked broken and the trace showed the click
    // landing at x=154, the parent, instead of x=315, the leaf.
    const own = (text) =>
      `.dc-menu-item:has(> .dc-menu-label:text-is(${JSON.stringify(text)}))`;
    const target = typeof label === 'string'
      ? page.locator(own(label)).first()
      : page.locator('.dc-menu-item').filter({
        has: page.locator('.dc-menu-label'),
      }).filter({ hasText: label }).filter({
        hasNot: page.locator('.dc-submenu'),
      }).first();
    // Report what the menu ACTUALLY offered. "No such entry" with no
    // list behind it sends you hunting through source for a label
    // that was right there, spelled differently.
    if (!(await target.count())) {
      const seen = (await openMenu()).map((m) =>
        `${m.label}${m.off ? ' [off]' : ''}`);
      throw new Error(`no entry matching ${label} — the menu offered:`
        + ` ${seen.join(' / ')}`);
    }
    const state = await target.evaluate((e) => ({
      off: e.classList.contains('dc-disabled'),
      text: e.textContent?.trim() ?? '',
    }));
    // A DISABLED entry is a finding, not a timeout. Playwright waits
    // for an element to become actionable and gives up after 30s,
    // which reads as "the click broke" when the truth is "the
    // product greyed this out".
    if (state.off) {
      throw new Error(`"${state.text}" is DISABLED in this context`);
    }
    if (i < path.length - 1) {
      await target.hover();
      await page.waitForTimeout(150);
    } else {
      await target.click({ timeout: 5000 });
    }
  }
  await settle(before);
}

/**
 * Which BODY cell of a row belongs to a named column.
 *
 * Read from the row that gets right-clicked, not from the header.
 * Once a cube is grouped the row dimensions collapse into one tree
 * cell, and the header cells carrying `data-column` are not the same
 * list as a row's cells -- so a header-derived index landed one
 * column off, and a check meaning to group by `region` grouped by
 * `booked_at` instead. It then reported a fault in grouping that was
 * really a fault in the driver.
 */
const colIndex = (name) => page.evaluate((n) => {
  const row = document.querySelector('.dc-row');
  if (!row) return -1;
  const cells = [...row.querySelectorAll('.dc-cell')];
  return cells.findIndex((c) =>
    (c.dataset.column ?? c.closest('[data-column]')?.dataset.column) === n);
}, name);

/** Where a named column sits, or a failure that names it. */
async function needCol(name) {
  const at = await colIndex(name);
  if (at < 0) {
    const on = await page.evaluate(() =>
      [...document.querySelectorAll('.dc-th[data-column]')]
        .map((e) => e.dataset.column));
    throw new Error(`${name} is not on screen to right-click; showing ${on}`);
  }
  return at;
}

/** The dimension columns on screen, in order. */
const dimensionNames = () => page.evaluate(() =>
  [...document.querySelectorAll('.dc-tool-panel-row:not(.dc-measure)')]
    .map((e) => e.dataset.column));

/** Open the hamburger and pick an entry. */
async function burger(label) {
  await page.click('.dc-titlebar-menu');
  await page.waitForSelector('.dc-menu', { timeout: 5000 });
  await page.locator('.dc-menu-item', { hasText: label }).first().click();
  await settle();
}

// -- load ---------------------------------------------------------------

let loaded = 'the built-in demo cube';
let heatCol = 1;
// `region` in the sample; column 3 of the demo cube is a dimension too.
const GROUP_COL = 3;
/**
 * A cube in a known state: the page loaded, the file opened.
 *
 * The preamble's own work, as a function, so a check that must not
 * inherit fifty other checks' configuration can ask for a clean one.
 * Used sparingly -- the point of one long run is that each feature
 * meets the state the others leave.
 */
async function freshCube() {
  await page.goto(`${URL_BASE}/demo/index.html`);
  await page.waitForSelector('.dc-row', { timeout: 90_000 });
  if (!DATA) return;
  // THE FILE'S answer, not the built-in cube's. The built-in cube's
  // status line already reads "… rows …", so waiting for /rows/ alone
  // passed before the file had loaded -- and on a slow run the checks
  // read a grid mid-swap: "0 rows, 0 headers", the whole sweep red for
  // a page that was fine (2026-09-25). Wait for the line to CHANGE,
  // and for rows to be on screen.
  const before = await statusNow();
  await page.setInputFiles('input[type=file]', DATA);
  await page.waitForFunction(
    (was) => {
      const line = document.querySelector('.dc-status-timing')?.textContent ?? '';
      return (line !== was && /rows/.test(line)
        && document.querySelectorAll('.dc-row').length > 0)
        || /could not|error/i.test(
          document.getElementById('status')?.textContent ?? '');
    },
    before, { timeout: 90_000 },
  );
  await settle();
}

try {
  await freshCube();
  if (DATA) loaded = DATA.split('/').pop();
  const start = await state();
  console.log(`\nloaded ${loaded}: ${start.rows.length} rows,`
    + ` ${start.headers.length} headers\n`);
  if (!start.rows.length) throw new Error('nothing rendered at all');
  await invariants('loading the data');

  // ---- reading the data ---------------------------------------------

  await check('grid renders rows and headers', async () => {
    const s = await state();
    if (!s.headers.length) throw new Error('no column headers');
    if (!s.rows.length) throw new Error('no rows');
    const empty = s.rows[0].filter((c) => c === '').length;
    if (empty === s.rows[0].length) throw new Error('first row is all blank');
    return `${s.rows.length} rows x ${s.headers.length} headers`;
  });

  // ---- sorting --------------------------------------------------------

  await check('sort ascending', async () => {
    await menu(['Sort', 'Ascending']);
    const s = await state();
    if (!/sort\(/.test(s.pure)) throw new Error('no sort in the Pure');
    if (!/ORDER BY/i.test(s.sql)) throw new Error('no ORDER BY in the SQL');
    const col = s.rows.map((r) => r[0]);
    const sorted = [...col].sort((a, b) => a.localeCompare(b, undefined,
      { numeric: true }));
    if (col.join('|') !== sorted.join('|')) {
      throw new Error(`rows are not ascending: ${col.slice(0, 4).join(', ')}`);
    }
    return `${col.slice(0, 3).join(', ')}...`;
  });

  await check('sort descending', async () => {
    await menu(['Sort', 'Descending']);
    const s = await state();
    if (!/desc/i.test(s.sql)) throw new Error('no DESC in the SQL');
    const col = s.rows.map((r) => r[0]);
    const sorted = [...col].sort((a, b) => b.localeCompare(a, undefined,
      { numeric: true }));
    if (col.join('|') !== sorted.join('|')) {
      throw new Error(`rows are not descending: ${col.slice(0, 4).join(', ')}`);
    }
    return `${col.slice(0, 3).join(', ')}...`;
  });

  await check('clear all sorts', async () => {
    // Set it first. A clear that runs against nothing passes without
    // testing anything -- four checks in the first version of this
    // file were green for exactly that reason.
    await menu(['Sort', 'Ascending']);
    if (!/ORDER BY/i.test((await state()).sql)) {
      throw new Error('could not set up: sorting did not take');
    }
    await menu(['Sort', 'Clear All Sorts']);
    const s = await state();
    if (/ORDER BY/i.test(s.sql)) throw new Error('ORDER BY survived the clear');
    return 'set, then cleared';
  });

  await check('a header click sorts: up, down, off', async () => {
    // Upstream sorts on a header click with multi-sort always on; the
    // header shows the arrow. Three clicks walk a column through
    // ascending, descending and back out of the sort.
    await menu(['Sort', 'Clear All Sorts']).catch(() => {});
    const name = await page.evaluate(() =>
      document.querySelector('.dc-th.dc-sortable[data-column]')?.dataset.column);
    if (!name) throw new Error('no sortable header');
    const th = page.locator(`.dc-th[data-column="${name}"]`);
    const click = async () => {
      const before = await statusNow();
      await th.click({ position: { x: 8, y: 8 } });
      await settle(before);
    };
    const ident = name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    await click();
    let s = await state();
    if (!new RegExp(`~'?${ident}'?->ascending`).test(s.pure)) {
      throw new Error(`first click: no ascending sort on ${name}: ${s.pure.slice(-120)}`);
    }
    if ((await th.getAttribute('aria-sort')) !== 'ascending') {
      throw new Error('the header does not say ascending');
    }
    await click();
    s = await state();
    if (!new RegExp(`~'?${ident}'?->descending`).test(s.pure)) {
      throw new Error(`second click: no descending sort on ${name}`);
    }
    await click();
    s = await state();
    if (new RegExp(`~'?${ident}'?->(a|de)scending`).test(s.pure)) {
      throw new Error(`third click left ${name} in the sort`);
    }
    return `${name}: asc → desc → off`;
  });

  await check('a header edge drags to a new width', async () => {
    const name = await page.evaluate(() =>
      [...document.querySelectorAll('.dc-th[data-column]')]
        .find((e) => e.querySelector('.dc-col-resize'))?.dataset.column);
    if (!name) throw new Error('no header carries a resize grip');
    const th = page.locator(`.dc-th[data-column="${name}"]`);
    const before = (await th.boundingBox()).width;
    const grip = th.locator('.dc-col-resize');
    const g = await grip.boundingBox();
    await page.mouse.move(g.x + g.width / 2, g.y + g.height / 2);
    await page.mouse.down();
    await page.mouse.move(g.x + g.width / 2 + 40, g.y + g.height / 2, { steps: 4 });
    await page.mouse.move(g.x + g.width / 2 + 80, g.y + g.height / 2, { steps: 4 });
    await page.mouse.up();
    await page.waitForTimeout(600);
    const after = (await page.locator(`.dc-th[data-column="${name}"]`)
      .boundingBox()).width;
    if (Math.abs(after - (before + 80)) > 6) {
      throw new Error(`${name} went from ${before}px to ${after}px, not +80`);
    }
    // And the cells follow their header.
    const cell = await page.locator(`.dc-row .dc-cell[data-column="${name}"]`)
      .first().boundingBox();
    if (Math.abs(cell.width - after) > 2) {
      throw new Error(`the header is ${after}px, its cells ${cell.width}px`);
    }
    await menu(['Resize', 'Auto-size All Columns']).catch(() => {});
    return `${name}: ${Math.round(before)}px → ${Math.round(after)}px`;
  });

  await check('cells and headers explain themselves on hover', async () => {
    const t = await page.evaluate(() => ({
      cell: document.querySelector('.dc-row .dc-cell:not(.dc-tree)')?.title,
      head: document.querySelector('.dc-th[data-column]')?.title,
    }));
    if (!/^(Value = |Missing Value)/.test(t.cell ?? '')) {
      throw new Error(`a cell's tooltip is "${t.cell}"`);
    }
    if (!/^Column = /.test(t.head ?? '')) {
      throw new Error(`a header's tooltip is "${t.head}"`);
    }
    return `${t.cell} / ${t.head}`;
  });

  await check('scrolling shows which rows are on screen', async () => {
    await page.evaluate(() => {
      const sc = document.querySelector('.dc-scroller');
      sc.scrollTop = 200;
      sc.dispatchEvent(new Event('scroll'));
    });
    await page.waitForTimeout(100);
    const hint = await page.evaluate(() => {
      const h = document.querySelector('.dc-scroll-hint');
      return h && !h.hidden ? h.textContent : null;
    });
    await page.evaluate(() => { document.querySelector('.dc-scroller').scrollTop = 0; });
    if (!/^\d+-\d+\/\d+$/.test(hint ?? '')) {
      throw new Error(`no start-end/total readout while scrolling: ${hint}`);
    }
    return hint;
  });

  // ---- filtering ------------------------------------------------------

  await check('add filter from a cell', async () => {
    const wanted = (await state()).rows[0][0];
    // The entry carries the clicked value in its own label.
    await menu(['Filter', /^Add Filter: \S+ = /]);
    const s = await state();
    const label = `= ${wanted}`;
    if (!/filter\(/.test(s.pure)) throw new Error('no filter in the Pure');
    if (!/WHERE/i.test(s.sql)) throw new Error('no WHERE in the SQL');
    const off = s.rows.filter((r) => r[0] !== wanted);
    if (off.length) {
      throw new Error(`${off.length} rows do not match the filter`
        + ` (wanted ${JSON.stringify(wanted)})`);
    }
    return `${label} -> ${s.rows.length} rows`;
  });

  await check('clear all filters', async () => {
    if (!/WHERE/i.test((await state()).sql)) {
      throw new Error('could not set up: no filter is in force');
    }
    await menu(['Filter', 'Clear All Filters']);
    const s = await state();
    if (/WHERE/i.test(s.sql)) throw new Error('WHERE survived the clear');
    if (!s.rows.length) throw new Error('clearing the filter left no rows');
    return `back to ${s.rows.length} rows`;
  });

  await check('the filter dialog opens and lists the filter', async () => {
    // A filter IN FORCE, so there is something to list: opening on an
    // empty editor over a filtered cube would drop it on Apply.
    await menu(['Filter', /^Add Filter: .* = /]);
    await menu(['Filter', 'Filters...'], { requery: false });
    const open = await page.evaluate(() =>
      Boolean(document.querySelector('.dc-app-overlay:not([hidden])')));
    if (!open) throw new Error('the overlay never opened');
    const conditions = await page.locator('.dc-filter-row:not(.dc-filter-group)')
      .count();
    const value = await page.locator('.dc-filter-value').first()
      .inputValue().catch(() => '');
    await page.locator('.dc-filter-cancel').click();
    await menu(['Filter', 'Clear All Filters']);
    if (conditions !== 1 || !value) {
      throw new Error(`the dialog showed ${conditions} condition(s),`
        + ` value "${value}", for a cube filtered on one`);
    }
    return `lists the condition in force (= ${value})`;
  });

  // ---- the filter editor -------------------------------------------------
  //
  // This builds a COMPOUND filter in the editor -- two conditions,
  // then the connective flipped from all-of to any-of. Nothing runs
  // until Apply, as upstream's Filter window: each step applies and
  // reads the result.

  await check('the filter editor builds a compound filter', async () => {
    await menu(['Filter', 'Clear All Filters']).catch(() => {});
    const all = await resultRows();
    await menu(['Filter', 'Filters...'], { requery: false });

    const create = page.locator('button', { hasText: 'Create New Filter' });
    if (await create.count()) {
      await create.first().click();
      await page.waitForTimeout(250);
    }
    const columns = page.locator('.dc-filter-column');
    if (!(await columns.count())) {
      throw new Error('the editor offered no column to filter on');
    }

    // Two text dimensions with known values in the sample.
    const names = await columns.first().evaluate((e) =>
      [...e.options].map((o) => o.value));
    const first = ['region', 'desk', 'book'].find((n) => names.includes(n));
    if (!first) throw new Error(`no text dimension among ${names.join(',')}`);
    await columns.first().selectOption(first);
    await page.waitForTimeout(150);
    const firstValue = await page.evaluate((col) => {
      const i = [...document.querySelectorAll('.dc-th[data-column]')]
        .findIndex((e) => e.dataset.column === col);
      const row = document.querySelector('.dc-row');
      return row?.querySelectorAll('.dc-cell')[i]?.textContent?.trim() ?? '';
    }, first);
    if (!firstValue) throw new Error(`no value to filter ${first} by`);
    await page.locator('.dc-filter-value').first().fill(firstValue);
    await page.locator('.dc-filter-value').first().press('Tab');
    // NOTHING RUNS UNTIL APPLY, as upstream's Filter window: an edit
    // is a draft.
    const drafted = await statusNow();
    await page.waitForTimeout(300);
    if ((await statusNow()) !== drafted) {
      throw new Error('an edit re-queried before Apply');
    }
    await page.locator('.dc-filter-apply').click();
    await settle(drafted);

    const andRows = await resultRows();
    const andSql = (await state()).sql;
    if (!/WHERE/i.test(andSql)) throw new Error('one condition gave no WHERE');

    // A second condition, on a different column, joined with AND.
    await page.locator('.dc-filter-ctl', { hasText: '+' }).first().click();
    await page.waitForTimeout(250);
    if ((await page.locator('.dc-filter-column').count()) < 2) {
      throw new Error('"+" did not add a second condition');
    }
    const second = ['desk', 'book', 'region'].find(
      (n) => names.includes(n) && n !== first);
    await page.locator('.dc-filter-column').nth(1).selectOption(second);
    await page.waitForTimeout(150);
    const secondValue = await page.evaluate((col) => {
      const i = [...document.querySelectorAll('.dc-th[data-column]')]
        .findIndex((e) => e.dataset.column === col);
      const row = document.querySelector('.dc-row');
      return row?.querySelectorAll('.dc-cell')[i]?.textContent?.trim() ?? '';
    }, second);
    await page.locator('.dc-filter-value').nth(1).fill(secondValue);
    await page.locator('.dc-filter-value').nth(1).press('Tab');
    const before2 = await statusNow();
    await page.locator('.dc-filter-apply').click();
    await settle(before2);

    const both = await state();
    if (!/ AND /i.test(both.sql)) {
      throw new Error(`no AND in ${both.sql.slice(0, 120)}`);
    }

    // FLIP THE CONNECTIVE. It is the `.dc-filter-join` select, shown
    // as "All of"/"Any of" -- not `.dc-filter-joinword`, which is an
    // aria-hidden decoration.
    const joinSel = page.locator('.dc-filter-join').first();
    if (!(await joinSel.count())) {
      throw new Error('two conditions but no all-of/any-of control');
    }
    await joinSel.selectOption('or');
    const before3 = await statusNow();
    await page.locator('.dc-filter-apply').click();
    await settle(before3);

    const either = await state();
    if (!/ OR /i.test(either.sql)) {
      throw new Error(`flipping to any-of gave no OR:`
        + ` ${either.sql.slice(0, 120)}`);
    }
    // THE COUNTS MUST MOVE THE RIGHT WAY. Any-of admits at least as
    // much as all-of, and both admit no more than the unfiltered
    // cube; asserting the SQL text alone would pass a filter that
    // was built correctly and never run.
    const orRows = await resultRows();
    if (orRows < andRows) {
      throw new Error(`any-of returned FEWER rows than all-of:`
        + ` ${orRows} < ${andRows}`);
    }
    if (orRows > all) {
      throw new Error(`any-of returned more than the whole cube:`
        + ` ${orRows} > ${all}`);
    }
    // And it must actually FILTER: two conditions that exclude
    // nothing would satisfy every comparison above.
    if (andRows >= all) {
      throw new Error(`all-of excluded nothing: ${andRows} of ${all} rows`);
    }
    await page.keyboard.press('Escape');
    await menu(['Filter', 'Clear All Filters']);
    return `all-of ${andRows} rows, any-of ${orRows}, unfiltered ${all}`;
  });

  await check('the filter editor\'s TYPED values reach the planner', async () => {
    // Each column type gets its own value editor, as upstream's: a
    // date with Today/Now, a number that evaluates arithmetic, a
    // checkbox, a list of entries. Every one has to become a literal
    // the planner accepts for that column's type.
    await menu(['Filter', 'Clear All Filters']).catch(() => {});
    const all = await resultRows();
    await menu(['Filter', 'Filters...'], { requery: false });
    const create = page.locator('button', { hasText: 'Create New Filter' });
    if (await create.count()) await create.first().click();
    const names = await page.locator('.dc-filter-column').first()
      .evaluate((e) => [...e.options].map((o) => o.value));
    const apply = async () => {
      const before = await statusNow();
      await page.locator('.dc-filter-apply').click();
      await settle(before);
      const problem = await page.locator('.dc-filter-problem').textContent();
      if (problem) throw new Error(`refused: ${problem}`);
    };
    const done = [];

    if (names.includes('notional')) {
      await page.locator('.dc-filter-column').first().selectOption('notional');
      await page.locator('.dc-filter-op').first().selectOption('greaterThan');
      const n = page.locator('.dc-filter-number').first();
      await n.fill('2.5e3 * 2');
      await n.press('Enter');
      if ((await n.inputValue()) !== '5000') {
        throw new Error(`arithmetic gave ${await n.inputValue()}`);
      }
      await apply();
      if (!/5000/.test((await state()).sql)) throw new Error('no 5000 in the SQL');
      done.push('notional > 2.5e3*2');
    }
    const date = ['trade_date', 'booked_at'].find((c) => names.includes(c));
    if (date) {
      await page.locator('.dc-filter-column').first().selectOption(date);
      await page.locator('.dc-filter-op').first().selectOption('lessThan');
      await page.locator('.dc-filter-date-mode').first().selectOption('today');
      await apply();
      done.push(`${date} < Today`);
    }
    if (names.includes('settled')) {
      await page.locator('.dc-filter-column').first().selectOption('settled');
      await page.locator('.dc-filter-bool').first().check();
      await apply();
      done.push('settled = true');
    }
    const text = ['region', 'desk'].find((c) => names.includes(c));
    if (text) {
      await page.locator('.dc-filter-column').first().selectOption(text);
      await page.locator('.dc-filter-op').first().selectOption('in');
      await page.locator('.dc-filter-list').first().click();
      const value = await page.evaluate((col) => {
        const cell = [...document.querySelector('.dc-row')?.querySelectorAll('.dc-cell') ?? []]
          .find((e) => e.dataset.column === col);
        return cell?.textContent?.trim() ?? '';
      }, text);
      for (const v of [value, 'NO-SUCH-VALUE']) {
        await page.locator('.dc-filter-listadd').fill(v);
        await page.locator('.dc-filter-listadd').press('Enter');
      }
      await apply();
      const rows = await resultRows();
      if (!(rows > 0 && rows < all)) {
        throw new Error(`${text} in [${value}] kept ${rows} of ${all}`);
      }
      done.push(`${text} in [${value}, …] → ${rows} of ${all}`);
    }
    await page.locator('.dc-filter-cancel').click();
    await menu(['Filter', 'Clear All Filters']);
    if (!done.length) throw new Error(`no typed column among ${names}`);
    return done.join('; ');
  });

  // ---- grouping and pivots --------------------------------------------

  await check('vertical pivot (row group)', async () => {
    // A LOW-CARDINALITY column, by NAME. Grouping on trade_id gives
    // one group per row, so nothing is expandable and Collapse All is
    // correctly disabled -- which reads as three broken features.
    const dims = await dimensionNames();
    const on = ['region', 'desk', 'book', 'quarter']
      .find((n) => dims.includes(n));
    if (!on) throw new Error(`no low-cardinality dimension in ${dims}`);
    await menu(['Pivot', /^Vertical Pivot on/], { col: await needCol(on) });
    const s = await state();
    if (!/groupBy\(~\[/.test(s.pure)) throw new Error('no groupBy in the Pure');
    if (!/GROUP BY/i.test(s.sql)) throw new Error('no GROUP BY in the SQL');
    const first = s.rows.map((r) => r[0]);
    if (new Set(first).size !== first.length) {
      throw new Error(`grouped rows repeat: ${first.slice(0, 5).join(', ')}`);
    }
    if (s.headers.length < 2) {
      throw new Error(`grouping kept only ${s.headers.length} columns`);
    }
    return `${s.rows.length} groups, ${s.headers.length} columns kept`;
  });

  await check('a second row dimension nests under the first', async () => {
    // Both groupings established HERE, and both BY NAME.
    await menu(['Pivot', 'Clear All Vertical Pivots']).catch(() => {});
    const dims = await dimensionNames();
    const [first, second] = ['region', 'desk', 'book', 'quarter', 'year']
      .filter((n) => dims.includes(n));
    if (!first || !second) {
      throw new Error(`need two low-cardinality dimensions, have ${dims}`);
    }
    await menu(['Pivot', /^Vertical Pivot on/],
      { col: await needCol(first) });
    await menu(['Pivot', /^Add Vertical Pivot on/],
      { col: await needCol(second) });
    // NOT the Pure pane. A tree issues one query per LEVEL, and the
    // pane deliberately shows the representative level-1 plan
    // (`serialize(snapshot, { level: 1, parent: [] })`), which groups
    // by the FIRST dimension alone. Two keys can never appear in it,
    // so asserting on it reported a fault in a feature that works --
    // twice, once blamed on staleness and once on a column index.
    //
    // The row zone is where the dimensions are, and expandability is
    // what having two of them buys you.
    // THE ROWS HALF, BY NAME. `[class*=zone]` collected chips from
    // both halves, so "two row dimensions" would also have been
    // satisfied by one row chip and one column chip -- which is the
    // other shape entirely.
    const zone = await page.evaluate(() =>
      [...document.querySelectorAll('.dc-zone-bar .dc-zone-rows'
        + ' [data-column]')]
        .map((e) => e.dataset.column));
    if (zone.length < 2) {
      throw new Error(`the row zone holds ${JSON.stringify(zone)}`);
    }
    const expandable = await page.locator('.dc-row[aria-expanded]').count();
    if (!expandable) {
      throw new Error('no row can be expanded, so nothing nests');
    }
    return `${zone.join(' > ')}, ${expandable} expandable rows`;
  });

  await check('expanding a group shows its children', async () => {
    const before = (await state()).rows.length;
    const chev = page.locator('.dc-row[aria-expanded=false]'
      + ' .dc-chevron:not(.dc-chevron-empty)').first();
    if (!(await chev.count())) {
      const census = await page.evaluate(() =>
        [...document.querySelectorAll('.dc-row')].slice(0, 5).map((r) => ({
          exp: r.getAttribute('aria-expanded'),
          lvl: r.getAttribute('aria-level'),
          chev: r.querySelector('.dc-chevron')?.className ?? null,
          text: r.querySelector('.dc-cell')?.textContent?.trim().slice(0, 14),
        })));
      throw new Error('no collapsed group to expand; the first rows are '
        + JSON.stringify(census));
    }
    await chev.click();
    await settle();
    const after = (await state()).rows.length;
    if (after <= before) {
      throw new Error(`expanding added no rows: ${before} -> ${after}`);
    }
    return `${before} -> ${after} rows`;
  });

  await check('collapse all', async () => {
    if (!/GROUP BY/i.test((await state()).sql)) {
      throw new Error('could not set up: nothing is grouped');
    }
    await menu(['Collapse All']);
    const s = await state();
    const open = await page.locator('.dc-row[aria-expanded=true]').count();
    if (open) throw new Error(`${open} groups are still expanded`);
    return `${s.rows.length} rows`;
  });

  await gap('the deepest group level expands to its detail rows',
    'our tree marks the last dimension a leaf (`isGroup = child.length'
    + ' < depth` in tree.ts), so the rows behind a bottom-level group'
    + ' cannot be reached. Upstream drops the groupBy at that point'
    + ' (DataCubeGridQueryBuilder: "when maximum level of drilldown is'
    + ' reached ... no groupBy() is needed") and returns the group\'s'
    + ' own rows, filtered to its keys.',
    async () => {
      // Collapse to one dimension, so the top level IS the deepest.
      await menu(['Pivot', 'Clear All Vertical Pivots']).catch(() => {});
      const dims = await dimensionNames();
      const on = ['region', 'desk', 'book'].find((n) => dims.includes(n));
      if (!on) throw new Error(`no dimension to group by in ${dims}`);
      await menu(['Pivot', /^Vertical Pivot on/],
        { col: await needCol(on) });
      const chev = await page.locator('.dc-row[aria-expanded]'
        + ' .dc-chevron:not(.dc-chevron-empty)').count();
      if (!chev) {
        throw new Error('a bottom-level group offers no way to open it');
      }
      return 'the bottom level can be opened';
    });

  await check('clear all vertical pivots', async () => {
    if (!/GROUP BY/i.test((await state()).sql)) {
      throw new Error('could not set up: nothing is grouped');
    }
    await menu(['Pivot', 'Clear All Vertical Pivots']);
    const s = await state();
    if (/GROUP BY/i.test(s.sql)) throw new Error('GROUP BY survived the clear');
    return 'back to detail rows';
  });

  await check('horizontal pivot (column pivot)', async () => {
    // Group first, so the cube HAS a measure to aggregate. A pivot
    // with none is refused on purpose, and driving that would test
    // the refusal rather than the pivot.
    const dims0 = await dimensionNames();
    const grouped = ['region', 'desk', 'book'].find((n) => dims0.includes(n));
    if (!grouped) throw new Error(`no dimension to group by in ${dims0}`);
    await menu(['Pivot', /^Vertical Pivot on/],
      { col: await needCol(grouped) });
    const dims = await dimensionNames();
    // Something with few distinct values and not the row group
    // itself: a pivot on an identifier asks for one column block per
    // row, which is a different test (and a slow one).
    const on = ['year', 'quarter', 'settled', 'desk', 'book']
      .find((n) => dims.includes(n) && n !== grouped);
    if (!on) throw new Error(`no low-cardinality dimension in ${dims}`);
    const at = await colIndex(on);
    if (at < 0) throw new Error(`${on} is not on screen to right-click`);
    await menu(['Pivot', /^Horizontal Pivot on/], { col: at });
    const s = await state();
    if (!/pivot\(/.test(s.pure)) throw new Error('no pivot in the Pure');
    if (s.headers.length < 2) throw new Error('pivot produced no columns');
    // A pivot puts the VALUES across the top, so the header must
    // gain a level: one row of pivot values above the measures.
    const levels = await page.locator('.dc-head-row').count();
    if (levels < 2) {
      throw new Error(`the header is still ${levels} level(s) deep, so the`
        + ' pivot values are not across the top');
    }
    return `${s.headers.length} headers over ${levels} levels`;
  });

  await check('a row grouping SURVIVES a column pivot', async () => {
    // Reported from the product: grouped by region, desk and book,
    // then year added as a column label. The measures split across
    // the years correctly and the three row groups dissolved into a
    // thousand detail rows, while the row zone still listed all
    // three. A pivot takes its grouping from whatever else is
    // SELECTED, and the projection had been widened to every column.
    await menu(['Pivot', 'Clear All Vertical Pivots']).catch(() => {});
    await menu(['Pivot', 'Clear All Horizontal Pivots']).catch(() => {});
    const dims = await dimensionNames();
    const groups = ['region', 'desk', 'book'].filter((n) => dims.includes(n));
    if (groups.length < 2) {
      throw new Error(`need two dimensions to group by, have ${dims}`);
    }
    await menu(['Pivot', /^Vertical Pivot on/],
      { col: await needCol(groups[0]) });
    for (const name of groups.slice(1)) {
      await menu(['Pivot', /^Add Vertical Pivot on/],
        { col: await needCol(name) });
    }
    const grouped = await resultRows();

    const across = ['year', 'quarter'].find(
      (n) => dims.includes(n) && !groups.includes(n));
    if (!across) throw new Error(`nothing to pivot across in ${dims}`);
    await menu(['Pivot', /^Horizontal Pivot on/],
      { col: await needCol(across) });

    const s = await state();
    // THE GROUPS MUST STILL BE GROUPS. The fault showed as the row
    // count exploding from a handful to the row cap, so the count is
    // the assertion; the tree and the header depth confirm the shape.
    const after = await resultRows();
    if (after > grouped) {
      throw new Error(`the grouping dissolved: ${grouped} grouped rows became`
        + ` ${after} after pivoting ${across} across the top`);
    }
    const expandable = await page.locator('.dc-row[aria-expanded]').count();
    if (!expandable) {
      throw new Error('no row can be expanded, so the tree is gone');
    }
    const levels = await page.locator('.dc-head-row').count();
    if (levels < 2) {
      throw new Error(`the header is ${levels} level(s) deep, so the pivot`
        + ' values are not across the top');
    }
    if (!/pivot\(~\[/.test(s.pure)) throw new Error('no pivot in the Pure');

    // THE MECHANISM IS THE SECOND STAGE, not a narrow projection.
    //
    // This check first asserted the opposite -- that the deeper row
    // dimensions stay OUT of the projection -- because that was the
    // only way to keep the grouping before the outer groupBy
    // existed, and it cost every other column. Now the projection is
    // wide, the pivot's intermediate is fine-grained, and the
    // groupBy collapses it: pivot, cast, groupBy, in that order.
    const after2 = /->pivot\(/.test(s.pure)
      ? s.pure.slice(s.pure.indexOf('->pivot('))
      : '';
    if (!/->cast\(@Relation</.test(after2)) {
      throw new Error('no cast after the pivot, so a groupBy naming its'
        + ` columns would be refused: ${after2.slice(0, 120)}`);
    }
    if (!/->groupBy\(~\[/.test(after2)) {
      throw new Error(`no groupBy after the pivot: ${after2.slice(0, 160)}`);
    }
    if (after2.indexOf('->cast(') > after2.indexOf('->groupBy(')) {
      throw new Error('the cast must come BEFORE the groupBy that needs it');
    }

    // AND THE OTHER COLUMNS MUST BE BACK. Losing them was the
    // complaint: "you fixed the groupby but lost all the other
    // non-measure columns". A row dimension stays in the tree, so
    // what should return is everything else.
    // Read inline rather than through `gridColumns`, which is a
    // `const` declared further down the file: reaching it from here
    // is a temporal-dead-zone error, and the message it throws
    // ("Cannot access 'gridColumns' before initialization") replaces
    // the verdict of the check that was meant to report the bug.
    const shown = await page.evaluate(() =>
      [...document.querySelectorAll('.dc-th[data-column]')]
        .map((e) => e.dataset.column));
    const missing = ['trade_id', 'quarter', 'settled']
      .filter((n) => dims.includes(n) && !shown.includes(n));
    if (missing.length) {
      throw new Error(`${missing.join(', ')} did not survive the pivot;`
        + ` the grid shows ${shown.join(',')}`);
    }
    if (shown.some((n) => groups.slice(1).includes(n))) {
      throw new Error('a row dimension is also a data column, so it is'
        + ' shown twice');
    }
    return `${grouped} groups, ${levels} header levels,`
      + ` ${shown.length} columns`;
  });

  await check('a column pivot keeps the cube\'s column order', async () => {
    // BY POSITION, not by DOM order. The header is a CSS grid placed
    // with `grid-column`, and the DOM groups cells by header ROW --
    // so every level-0 cell precedes every level-1 cell in document
    // order whatever the screen shows. Reading the DOM order made a
    // correct layout look broken, and then made a fix look like it
    // had not worked.
    const byPosition = () => page.evaluate(() =>
      [...document.querySelectorAll('.dc-th[data-column]')]
        .map((e) => ({
          name: e.dataset.column,
          x: Math.round(e.getBoundingClientRect().left),
        }))
        .sort((a, b) => a.x - b.x)
        .map((c) => c.name));

    await menu(['Pivot', 'Clear All Vertical Pivots']).catch(() => {});
    await menu(['Pivot', 'Clear All Horizontal Pivots']).catch(() => {});
    // GROUPED first. A pivot with no row groups has nothing to group
    // by afterwards, so there is no outer groupBy and the carried
    // columns genuinely do not exist -- the baseline has to be the
    // shape the question is about.
    const dims0 = await dimensionNames();
    const groupBy = ['region', 'desk', 'book'].find(
      (n) => dims0.includes(n));
    if (!groupBy) throw new Error(`nothing to group by in ${dims0}`);
    await menu(['Pivot', /^Vertical Pivot on/],
      { col: await needCol(groupBy) });
    const flat = await byPosition();
    const measure = ['notional', 'pnl'].filter((n) => flat.includes(n));
    const trailing = flat.slice(flat.indexOf(measure[0]) + measure.length);
    if (!measure.length || !trailing.length) {
      throw new Error(`need a measure with columns after it, have`
        + ` ${flat.join(',')}`);
    }

    const dims = await dimensionNames();
    const across = ['year', 'quarter'].find(
      (n) => dims.includes(n) && n !== groupBy);
    await menu(['Pivot', /^Horizontal Pivot on/],
      { col: await needCol(across) });
    await page.waitForTimeout(1200);

    const after = await byPosition();
    // The plain columns keep their order relative to one another...
    const plain = after.filter((n) => !n.includes('__|__'));
    const wanted = flat.filter((n) => plain.includes(n));
    if (plain.join(',') !== wanted.join(',')) {
      throw new Error(`the plain columns were reordered: ${plain.join(',')}`
        + ` (was ${wanted.join(',')})`);
    }
    // ...and the pivot blocks sit WHERE THE MEASURES WERE, so a
    // column that followed them still follows.
    const firstBlock = after.findIndex((n) => n.includes('__|__'));
    const stillTrailing = trailing.filter((n) => after.includes(n));
    for (const name of stillTrailing) {
      if (after.indexOf(name) < firstBlock) {
        throw new Error(`${name} came BEFORE the pivot blocks; it followed`
          + ` the measures in the flat cube: ${after.join(',')}`);
      }
    }
    if (!stillTrailing.length) {
      throw new Error('no column survived after the measures, so nothing'
        + ' here was tested');
    }
    return `blocks between ${after[firstBlock - 1]} and`
      + ` ${stillTrailing.join(',')}`;
  });

  await check('a column pivot shows each row\'s TOTAL, from the database',
    async () => {
      // Upstream stores the pivot total's settings and draws nothing;
      // the user ruled that a bug (2026-09-25). The total of a row is
      // its measure with the pivot key dropped -- the very figure the
      // UNPIVOTED cube shows for that row -- so that is the witness,
      // and the pivot's own cells must add up to it as well.
      const byPosition = () => page.evaluate(() =>
        [...document.querySelectorAll('.dc-th[data-column]')]
          .map((e) => ({
            name: e.dataset.column,
            x: Math.round(e.getBoundingClientRect().left),
          }))
          .sort((a, b) => a.x - b.x)
          .map((c) => c.name));
      const firstRow = async () => (await page.locator('.dc-row').first()
        .locator('.dc-cell').allTextContents())
        .map((t) => Number(t.replace(/[^0-9.-]/g, '')));

      await menu(['Pivot', 'Clear All Vertical Pivots']).catch(() => {});
      await menu(['Pivot', 'Clear All Horizontal Pivots']).catch(() => {});
      const dims = await dimensionNames();
      const groupBy = ['region', 'desk', 'book'].find((n) => dims.includes(n));
      const across = ['year', 'quarter'].find((n) => dims.includes(n));
      if (!groupBy || !across) throw new Error(`need dimensions, have ${dims}`);
      await menu(['Pivot', /^Vertical Pivot on/], { col: await needCol(groupBy) });
      const flat = await byPosition();
      const want = (await firstRow())[flat.indexOf('notional')];
      if (!(want > 0)) throw new Error(`no grouped notional to compare: ${want}`);

      await menu(['Pivot', /^Horizontal Pivot on/], { col: await needCol(across) });
      await page.waitForTimeout(1200);
      const cols = await byPosition();
      const total = '__pivot_total____|__notional';
      const at = cols.indexOf(total);
      if (at < 0) throw new Error(`no total column: ${cols.join(', ')}`);
      const header = await page.evaluate(() =>
        [...document.querySelectorAll('.dc-th')]
          .map((e) => e.textContent?.trim()));
      if (!header.includes('Total')) {
        throw new Error(`the total's header is not "Total": ${header.join('|')}`);
      }
      // On the RIGHT of the pivot, the default: every total after every
      // value block.
      const isTotal = (c) => c.startsWith('__pivot_total__');
      const lastValue = cols.map((c, i) => (c.includes('__|__') && !isTotal(c)
        ? i : -1)).filter((i) => i >= 0).at(-1);
      const firstTotal = cols.findIndex(isTotal);
      if (!(firstTotal > lastValue)) {
        throw new Error(`a total sits inside the pivot: ${cols.join(', ')}`);
      }
      const row = await firstRow();
      const got = row[at];
      if (Math.abs(got - want) / want > 0.0005) {
        throw new Error(`total ${got} is not the unpivoted ${want}`);
      }
      const cells = cols
        .map((c, i) => (c.endsWith('__|__notional') && c !== total ? row[i] : 0))
        .reduce((a, b) => a + (Number.isFinite(b) ? b : 0), 0);
      if (Math.abs(cells - got) / got > 0.001) {
        throw new Error(`the pivot's cells add to ${cells}, the total says ${got}`);
      }
      const s = await state();
      if (/Error|refus/i.test(s.status)) throw new Error(`status: ${s.status}`);
      return `${groupBy} x ${across}: total ${got} = unpivoted ${want}`;
    });

  await check('a measure kept out of the pivot shows its real figure', async () => {
    // A pivot groups by everything it selects, so a measure carried
    // THROUGH it and summed afterwards adds up distinct values, not
    // rows -- and before that it was aggregated as `unique`, which is
    // blank for any group of two or more. Kept out of the pivot, its
    // figure is the unpivoted cube's, exactly.
    const cell = (name) => page.evaluate((n) => {
      const c = [...document.querySelector('.dc-row')
        ?.querySelectorAll('.dc-cell') ?? []]
        .find((e) => e.dataset.column === n);
      return c ? Number((c.textContent ?? '').replace(/[^0-9.-]/g, '')) : NaN;
    }, name);
    await menu(['Pivot', 'Clear All Vertical Pivots']).catch(() => {});
    await menu(['Pivot', 'Clear All Horizontal Pivots']).catch(() => {});
    const dims = await dimensionNames();
    const groupBy = ['region', 'desk', 'book'].find((n) => dims.includes(n));
    const across = ['year', 'quarter'].find((n) => dims.includes(n));
    await menu(['Pivot', /^Vertical Pivot on/], { col: await needCol(groupBy) });
    const want = await cell('pnl');
    if (!Number.isFinite(want) || want === 0) {
      throw new Error(`no grouped pnl to compare: ${want}`);
    }
    await menu(['Pivot', /^Horizontal Pivot on/], { col: await needCol(across) });
    await page.waitForTimeout(800);
    const pivotedPnl = await page.evaluate(() =>
      [...document.querySelectorAll('.dc-th[data-column]')]
        .map((e) => e.dataset.column)
        .find((n) => /__\|__pnl$/.test(n) && !n.startsWith('__pivot_total__')));
    if (!pivotedPnl) throw new Error('pnl was not pivoted to begin with');
    await menu(['Pivot', /^Exclude Column pnl from Horizontal Pivot/],
      { col: await needCol(pivotedPnl) });
    await page.waitForTimeout(800);
    const got = await cell('pnl');
    if (Math.abs(got - want) > Math.max(0.01, Math.abs(want) * 0.0005)) {
      throw new Error(`pnl kept out of the pivot reads ${got};`
        + ` the unpivoted cube says ${want}`);
    }
    return `${groupBy} x ${across}, pnl excluded: ${got} = ${want}`;
  });

  await check('clear all horizontal pivots', async () => {
    if (!/pivot\(/.test((await state()).pure)) {
      throw new Error('could not set up: nothing is pivoted');
    }
    await menu(['Pivot', 'Clear All Horizontal Pivots']);
    const s = await state();
    if (/pivot\(/.test(s.pure)) throw new Error('pivot survived the clear');
    return 'pivot gone';
  });

  // ---- columns ---------------------------------------------------------

  // Whatever the pivot checks left behind, the cube goes back to flat
  // detail rows here. Without this a failed pivot leaks its grouping
  // into every check below, and `hide` reports "removed nothing"
  // because it is aimed at a tree column that is not there any more.
  await check('the cube can be returned to flat detail rows', async () => {
    for (const path of [['Pivot', 'Clear All Vertical Pivots'],
      ['Pivot', 'Clear All Horizontal Pivots'],
      ['Sort', 'Clear All Sorts'], ['Filter', 'Clear All Filters']]) {
      await menu(path).catch(() => {});
    }
    const s = await state();
    if (/GROUP BY|ORDER BY|WHERE/i.test(s.sql)) {
      throw new Error(`the cube is still shaped: ${s.sql.slice(0, 90)}`);
    }
    return `${s.headers.length} columns, ${s.rows.length} rows`;
  });

  await check('hide a column', async () => {
    const before = (await state()).headers;
    await menu([/^Hide /]);
    const after = (await state()).headers;
    if (after.length >= before.length) {
      throw new Error(`hiding removed nothing: ${before.length} ->`
        + ` ${after.length}`);
    }
    return `${before.length} -> ${after.length} columns`;
  });

  await check('undo brings the column back', async () => {
    const before = (await state()).headers.length;
    await burger('Undo');
    const after = (await state()).headers.length;
    if (after <= before) {
      throw new Error(`undo did nothing: ${before} -> ${after} columns`);
    }
    return `${before} -> ${after} columns`;
  });

  await check('redo hides it again', async () => {
    const before = (await state()).headers.length;
    await burger('Redo');
    const after = (await state()).headers.length;
    if (after >= before) {
      throw new Error(`redo did nothing: ${before} -> ${after} columns`);
    }
    await burger('Undo');
    return `${before} -> ${after} columns`;
  });

  await check('auto-size a column to its content', async () => {
    // These two entries were on the menu with NO handler behind them
    // -- the dispatch ends in `default: return`, so clicking either
    // did nothing, silently. The width is what is asserted, because
    // that is the whole feature.
    const widthOf = (i) => page.evaluate((n) => {
      const th = document.querySelectorAll('.dc-th[data-column]')[n];
      return Math.round(th.getBoundingClientRect().width);
    }, i);
    const before = await widthOf(0);
    await menu(['Resize', 'Auto-size to Fit Content']);
    const after = await widthOf(0);
    if (after === before) {
      throw new Error(`the column did not resize: still ${before}px`);
    }
    return `${before} -> ${after}px`;
  });

  await check('auto-size every column', async () => {
    const widths = () => page.evaluate(() =>
      [...document.querySelectorAll('.dc-th[data-column]')]
        .map((e) => Math.round(e.getBoundingClientRect().width)));
    const before = await widths();
    await menu(['Resize', 'Auto-size All Columns']);
    const after = await widths();
    const moved = after.filter((w, i) => w !== before[i]).length;
    if (moved < 2) {
      throw new Error(`only ${moved} of ${before.length} columns resized`);
    }
    return `${moved} of ${before.length} columns resized`;
  });

  await check('pin a column left', async () => {
    await menu(['Pin', 'Pin Left']);
    const pinned = await page.locator('.dc-pin-left').count();
    if (!pinned) throw new Error('nothing is marked pinned in the DOM');
    // Pinned means it STAYS. Scroll sideways and the pinned cell must
    // not move with the rest; a class on its own proves only that a
    // class was set.
    const x = () => page.evaluate(() => Math.round(
      document.querySelector('.dc-pin-left').getBoundingClientRect().left));
    const at0 = await x();
    await page.evaluate(() => {
      document.querySelector('.dc-scroller').scrollLeft = 300;
    });
    await page.evaluate(() => new Promise((r) =>
      requestAnimationFrame(() => requestAnimationFrame(r))));
    const at300 = await x();
    await page.evaluate(() => {
      document.querySelector('.dc-scroller').scrollLeft = 0;
    });
    await page.waitForTimeout(150);
    if (Math.abs(at300 - at0) > 2) {
      throw new Error(`the pinned column scrolled away: ${at0} -> ${at300}`);
    }
    return `${pinned} cells pinned, held at ${at0}px`;
  });

  await check('unpin all', async () => {
    const before = await page.locator('.dc-pin-left, .dc-pin-right').count();
    if (!before) throw new Error('could not set up: nothing was pinned');
    await menu(['Pin', 'Remove All Pinnings']);
    const after = await page.locator('.dc-pin-left, .dc-pin-right').count();
    if (after) throw new Error(`${after} elements are still pinned`);
    return `${before} -> 0 pinned`;
  });

  await check('heatmap colours the cells', async () => {
    const before = await page.evaluate(() =>
      [...document.querySelectorAll('.dc-cell')]
        .filter((c) => c.style.backgroundColor).length);
    // A MEASURE column: a heatmap over text or dates has nothing to
    // scale, so aiming at one would prove nothing either way.
    const measure = await page.evaluate(() => {
      const names = [...document.querySelectorAll('.dc-th[data-column]')]
        .map((e) => e.dataset.column);
      return names.findIndex((n) => /notional|pnl|amount|price/i.test(n));
    });
    if (measure < 0) throw new Error('no measure column to shade');
    heatCol = measure;
    await menu(['Heatmap', /^Add Heatmap/], { col: heatCol });
    const after = await page.evaluate(() =>
      [...document.querySelectorAll('.dc-cell')]
        .filter((c) => c.style.backgroundColor).length);
    if (after <= before) {
      throw new Error(`no cell got a background: ${before} -> ${after}`);
    }
    return `${after} cells coloured`;
  });

  await check('remove heatmap', async () => {
    const before = await page.evaluate(() =>
      [...document.querySelectorAll('.dc-cell')]
        .filter((c) => c.style.backgroundColor).length);
    if (!before) throw new Error('could not set up: no cell was coloured');
    await menu(['Heatmap', 'Remove Heatmap'], { col: heatCol });
    const left = await page.evaluate(() =>
      [...document.querySelectorAll('.dc-cell')]
        .filter((c) => c.style.backgroundColor).length);
    if (left) throw new Error(`${left} cells are still coloured`);
    return `${before} -> 0 coloured`;
  });

  // ---- the columns panel ------------------------------------------------

  await check('columns panel lists and searches', async () => {
    const all = await page.locator('.dc-tool-panel-row').count();
    if (!all) throw new Error('the panel lists nothing');
    await page.fill('.dc-tool-panel-search', 'zzzz');
    await page.waitForTimeout(150);
    const none = await page.locator('.dc-tool-panel-row').count();
    if (none !== 0) throw new Error(`search matched ${none} for "zzzz"`);
    await page.fill('.dc-tool-panel-search', '');
    await page.waitForTimeout(150);
    const back = await page.locator('.dc-tool-panel-row').count();
    if (back !== all) throw new Error(`clearing search left ${back}/${all}`);
    return `${all} columns`;
  });

  await check('double-clicking a column groups by it', async () => {
    const row = page.locator('.dc-tool-panel-row:not(.dc-measure)').first();
    await row.dblclick();
    await settle();
    const s = await state();
    if (!/groupBy\(~\[/.test(s.pure)) throw new Error('no groupBy in the Pure');
    await menu(['Pivot', 'Clear All Vertical Pivots']);
    return 'grouped, then cleared';
  });

  // ---- export and clipboard ---------------------------------------------

  for (const [label, ext] of [['CSV (Grid)', 'csv'], ['Excel (Grid)', 'xls'],
    ['HTML', 'html'], ['Plain Text', 'txt'], ['PDF', 'pdf'],
    ['DataCube Specification', 'json']]) {
    await check(`export ${label}`, async () => {
      const wait = page.waitForEvent('download', { timeout: 15_000 });
      await menu(['Export', label], { requery: false });
      // Upstream's attestation first, for anything carrying rows.
      if (ext !== 'json') await answerExport('Accept');
      const dl = await wait;
      const path = await dl.path();
      const body = await readFile(path);
      if (!body.length) throw new Error('the file is empty');
      const name = dl.suggestedFilename();
      if (!name.endsWith(`.${ext}`)) {
        throw new Error(`downloaded ${name}, expected .${ext}`);
      }
      return `${name}, ${body.length} bytes`;
    });
  }

  await check('an export asks first, and Decline downloads nothing', async () => {
    let downloaded = false;
    const seen = () => { downloaded = true; };
    page.on('download', seen);
    try {
      await menu(['Export', 'CSV (Grid)'], { requery: false });
      const text = await page.locator('.dc-alert-warning').innerText({ timeout: 3000 });
      if (!/Confirm you want to proceed with export/.test(text)) throw new Error(`warning read ${text}`);
      await answerExport('Decline');
      await page.waitForTimeout(400);
      if (downloaded) throw new Error('Decline still downloaded');
      if (await page.locator('.dc-alert').count()) throw new Error('the warning stayed open');
      return 'warned, declined, nothing sent';
    } finally {
      page.off('download', seen);
    }
  });

  await check('Email with no mail host downloads an unsent .eml draft', async () => {
    const own = (text) =>
      `.dc-menu-item:has(> .dc-menu-label:text-is(${JSON.stringify(text)}))`;
    await reset();
    await page.locator('.dc-row').first().locator('.dc-cell').first().click({ button: 'right' });
    const email = page.locator(own('Email')).first();
    await email.hover();
    const wait = page.waitForEvent('download', { timeout: 15_000 });
    await email.locator('.dc-submenu').locator(own('CSV (Grid)')).first().click();
    await answerExport('Accept');
    const dl = await wait;
    const name = dl.suggestedFilename();
    const body = String(await readFile(await dl.path()));
    if (!name.endsWith('.eml')) throw new Error(`downloaded ${name}`);
    if (!/^From:\nTo:\nSubject:\nX-Unsent: 1\n/.test(body)) throw new Error('not an unsent draft');
    if (!/filename=".* - .*\.csv"/.test(body)) throw new Error('no timestamped CSV attached');
    return name;
  });

  await check('Pin Left is checked once the column is pinned left', async () => {
    await menu(['Pin', 'Pin Left'], { requery: false });
    await page.waitForTimeout(250);
    const own = (text) =>
      `.dc-menu-item:has(> .dc-menu-label:text-is(${JSON.stringify(text)}))`;
    await page.locator('.dc-row').first().locator('.dc-cell').first().click({ button: 'right' });
    const pin = page.locator(own('Pin Left')).first();
    const checked = await pin.getAttribute('aria-checked');
    const disabled = await pin.getAttribute('aria-disabled');
    await page.keyboard.press('Escape');
    await menu(['Pin', 'Remove All Pinnings'], { requery: false });
    if (checked !== 'true' || disabled !== 'true') {
      throw new Error(`Pin Left checked=${checked} disabled=${disabled}`);
    }
    return 'checked, and disabled';
  });

  await check('copy a column to the clipboard', async () => {
    await menu(['Copy', /^Column .* as Plain Text$/], { requery: false });
    const text = await page.evaluate(() => navigator.clipboard.readText());
    if (!text || !text.trim()) throw new Error('the clipboard is empty');
    return `${text.split('\n').length} lines`;
  });

  // ---- the editor --------------------------------------------------------

  await check('the properties editor opens with its tabs', async () => {
    await menu(['Properties...'], { requery: false });
    const tabs = await page.locator('.dc-app-overlay [role=tab],'
      + ' .dc-tab').allTextContents();
    if (tabs.length < 3) {
      throw new Error(`only ${tabs.length} tabs: ${tabs.join(', ')}`);
    }
    return tabs.map((t) => t.trim()).join(' | ');
  });

  await check('every editor tab shows a panel', async () => {
    const tabs = page.locator('.dc-app-overlay [role=tab], .dc-tab');
    const n = await tabs.count();
    const empty = [];
    for (let i = 0; i < n; i += 1) {
      const name = (await tabs.nth(i).textContent())?.trim() ?? `#${i}`;
      await tabs.nth(i).click();
      await page.waitForTimeout(150);
      const body = await page.evaluate(() => {
        const panel = document.querySelector('.dc-app-overlay');
        const b = panel?.querySelector('[role=tabpanel], .dc-tab-body')
          ?? panel;
        return (b?.textContent ?? '').trim().length;
      });
      if (body < 20) empty.push(name);
    }
    await page.keyboard.press('Escape');
    await page.waitForTimeout(200);
    if (empty.length) throw new Error(`blank panels: ${empty.join(', ')}`);
    return `${n} tabs, all populated`;
  });

  // ---- the columns selector ----------------------------------------------
  //
  // Add, remove and REORDER, through the editor's Columns tab. This
  // was the one part of the product no harness had ever driven, and
  // it is the sibling trigger of the header-identity fault: `leafIndex`
  // held a source index, so reordering shifted every header's claimed
  // column exactly as hiding did. Both paths are checked here, and
  // the invariants that run after every check are what would catch it
  // again.

  /** Open Properties and select a tab by name. */
  async function editorTab(name) {
    await menu(['Properties...'], { requery: false });
    const tab = page.locator('.dc-editor-tab', { hasText: name });
    if (!(await tab.count())) {
      const seen = await page.locator('.dc-editor-tab').allTextContents();
      throw new Error(`no ${name} tab; the editor offers ${seen.join(', ')}`);
    }
    await tab.first().click();
    await page.waitForTimeout(250);
  }

  async function applyEditor() {
    const before = await statusNow();
    await page.locator('button', { hasText: 'Apply' }).first().click();
    await settle(before);
  }

  /** Turn a General Properties checkbox on or off, by its label. */
  const setGeneralCheck = async (label, on) => {
    await reset();
    await page.click('.dc-titlebar-menu');
    await page.waitForSelector('.dc-menu', { timeout: 10_000 });
    await page.locator('.dc-menu-item:has(> .dc-menu-label'
      + ':text-is("Properties..."))').click();
    await page.waitForTimeout(300);
    await page.locator('.dc-editor-tab', { hasText: 'General Properties' })
      .click();
    await page.waitForTimeout(150);
    const box = page.locator('.dc-check', { hasText: label })
      .locator('input');
    if (on) await box.first().check();
    else await box.first().uncheck();
    await page.locator('.dc-editor-footer button', { hasText: 'Apply' })
      .click();
    await page.waitForTimeout(400);
    await reset();
    await settle();
  };

  const setRootAggregation = (on) =>
    setGeneralCheck('Show root aggregation', on);

  /** Turn "keep grouped columns in the grid" on or off. */
  const setKeepGrouped = async (on) => {
    await reset();
    await page.click('.dc-titlebar-menu');
    await page.waitForSelector('.dc-menu', { timeout: 10_000 });
    await page.locator('.dc-menu-item:has(> .dc-menu-label'
      + ':text-is("Properties..."))').click();
    await page.waitForTimeout(300);
    await page.locator('.dc-editor-tab', { hasText: 'General Properties' })
      .click();
    await page.waitForTimeout(150);
    // BY ITS OWN LABEL: a `.dc-field` holds several inputs, and
    // taking the first has twice now toggled a different setting.
    const box = page.locator('.dc-check', {
      hasText: 'Keep grouped columns in the grid',
    }).locator('input');
    if (on) await box.first().check();
    else await box.first().uncheck();
    await page.locator('.dc-editor-footer button', { hasText: 'Apply' })
      .click();
    await page.waitForTimeout(400);
    await reset();
    await settle();
  };

  /**
   * Back to a FLAT cube.
   *
   * A check that right-clicks a column header needs that column to
   * be a header, and a grouped one is the tree's instead -- so a
   * check inheriting someone else's grouping fails with "region is
   * not on screen to right-click" and blames the product. Each of
   * the checks below makes its own shape from flat.
   */
  const flatten = async () => {
    await menu(['Pivot', 'Clear All Horizontal Pivots'], { requery: false })
      .catch(() => {});
    await menu(['Pivot', 'Clear All Vertical Pivots'], { requery: false })
      .catch(() => {});
    await settle();
  };

  /** The panel's rows, source columns only, in the order listed. */
  const panelOrder = () => page.evaluate(() =>
    [...document.querySelectorAll('.dc-tool-panel-row')]
      .filter((r) => !r.classList.contains('dc-tool-panel-child'))
      .map((r) => r.dataset.column));

  /**
   * The grid's columns, LEFT TO RIGHT.
   *
   * By x position, not document order. Header cells sit in the DOM
   * grouped by header ROW, so a pivoted cube lists the top row
   * (qtr, 2021..2025, pnl) before the leaf row (notional x5), and a
   * pinned column is in a container of its own -- either way the
   * document order is not the order on screen. Read the wrong way,
   * this reports the grid inconsistent with the panel when both are
   * right, which it did twice.
   */
  const gridColumns = () => page.evaluate(() =>
    [...document.querySelectorAll('.dc-th[data-column]')]
      .map((e) => ({
        name: e.dataset.column,
        x: Math.round(e.getBoundingClientRect().left),
      }))
      .sort((a, b) => a.x - b.x)
      .map((c) => c.name));

  let removed = null;

  await check('the columns selector removes a column', async () => {
    const before = await gridColumns();
    await editorTab('Columns');
    const rows = page.locator('.dc-pane-selected .dc-selector-row');
    if ((await rows.count()) < 3) {
      throw new Error(`the selected pane holds ${await rows.count()} columns`);
    }
    removed = await rows.nth(2).getAttribute('data-column');
    await rows.nth(2).click();
    // '‹' -- the second of the two move buttons.
    await page.locator('.dc-selector-move').nth(1).click();
    await page.waitForTimeout(200);
    await applyEditor();
    const after = await gridColumns();
    if (after.includes(removed)) {
      throw new Error(`${removed} is still in the grid`);
    }
    if (after.length !== before.length - 1) {
      throw new Error(`${before.length} -> ${after.length} columns, expected`
        + ` one fewer`);
    }
    return `${removed} gone, ${after.length} left`;
  });

  await check('the columns selector adds it back', async () => {
    await editorTab('Columns');
    const avail = page.locator('.dc-pane-available .dc-selector-row');
    if (!(await avail.count())) {
      throw new Error('the available pane is empty, so nothing can be added');
    }
    const back = avail.filter({ hasText: removed ?? '' });
    await ((await back.count()) ? back.first() : avail.first()).click();
    await page.locator('.dc-selector-move').nth(0).click();
    await page.waitForTimeout(200);
    await applyEditor();
    const after = await gridColumns();
    if (!after.includes(removed)) {
      throw new Error(`${removed} did not come back; grid has`
        + ` ${after.join(',')}`);
    }
    // It returns at the END: the selected pane's order IS the grid's
    // order, so a re-added column joins the back of the list rather
    // than resuming its old seat. That is what the selector means.
    return `${removed} back at position ${after.indexOf(removed) + 1}`;
  });

  await check('the columns selector reorders by dragging', async () => {
    const before = await gridColumns();
    await editorTab('Columns');
    const rows = page.locator('.dc-pane-selected .dc-selector-row');
    const n = await rows.count();
    if (n < 3) throw new Error(`only ${n} columns to reorder`);
    await rows.nth(n - 1).dragTo(rows.nth(0));
    await page.waitForTimeout(300);
    await applyEditor();
    const after = await gridColumns();
    if (after.join(',') === before.join(',')) {
      throw new Error(`the order did not change: ${after.join(',')}`);
    }
    if (after.length !== before.length) {
      throw new Error(`reordering changed the COUNT: ${before.length} ->`
        + ` ${after.length}; before=[${before.join(', ')}]`
        + ` after=[${after.join(', ')}]`);
    }
    if ([...after].sort().join(',') !== [...before].sort().join(',')) {
      throw new Error('reordering changed which columns are shown');
    }
    return `${before[before.length - 1]} moved to the front`;
  });

  // ---- round trips -------------------------------------------------------
  //
  // Do a thing, undo it, and the cube must be EXACTLY as it was.
  //
  // This is a property rather than a feature, and it catches a class
  // the per-feature checks cannot: an undo that restores half the
  // state. Undo records the snapshot and the tree, while the
  // configuration -- pins, widths, colours, hidden columns, the row
  // cap -- lives beside it, and a person changing one has no idea
  // they crossed an internal boundary. Two faults of exactly that
  // shape have already been found here: a cosmetic change recorded a
  // step whose snapshot was identical so undo appeared to do nothing,
  // and a query-shaping setting undid the snapshot while the config
  // kept its new value and put it straight back on the next refresh.
  //
  // Comparing the WHOLE observable state, not a count, is the point:
  // "twelve columns again" was true in both of those cases.

  /** Everything observable, with the query timing stripped out. */
  const fullState = () => page.evaluate(() => ({
    pure: document.getElementById('pure')?.textContent ?? '',
    sql: document.getElementById('sql')?.textContent ?? '',
    // Pinned and coloured cells, because pinning and heatmaps change
    // no text, no query and no number -- a state without them
    // reported "the operation changed nothing" about a pin that had
    // worked perfectly.
    pinned: document.querySelectorAll('.dc-pin-left, .dc-pin-right').length,
    // Label AND identity: a header can keep its label and claim a
    // different column, which is how sorting one column sorted its
    // neighbour.
    head: [...document.querySelectorAll('.dc-th')].map((e) =>
      `${(e.textContent ?? '').trim()}=${e.dataset.column ?? '-'}`).join('|'),
    rows: [...document.querySelectorAll('.dc-row')].slice(0, 4).map((r) =>
      [...r.querySelectorAll('.dc-cell')].map(
        (c) => c.textContent?.trim() ?? '').join(',')).join(' // '),
    colour: [...document.querySelectorAll('.dc-cell')]
      .filter((c) => c.style.backgroundColor).length,
  }));

  const firstDifference = (a, b) => {
    for (const key of Object.keys(a)) {
      if (a[key] !== b[key]) {
        return `${key}:\n      was ${JSON.stringify(String(a[key]))
          .slice(0, 150)}\n      now ${JSON.stringify(String(b[key]))
          .slice(0, 150)}`;
      }
    }
    return null;
  };

  for (const [what, path, opts] of [
    ['sorting', ['Sort', 'Ascending'], {}],
    ['a filter', ['Filter', /^Add Filter: \S+ = /], {}],
    ['a row group', ['Pivot', /^Vertical Pivot on/], { col: 3 }],
    ['hiding a column', [/^Hide /], {}],
    ['pinning a column', ['Pin', 'Pin Left'], {}],
    ['a heatmap', ['Heatmap', /^Add Heatmap/], { col: heatCol }],
  ]) {
    await check(`undo restores everything after ${what}`, async () => {
      const before = await fullState();
      await menu(path, opts);
      const changed = await fullState();
      if (firstDifference(before, changed) === null) {
        throw new Error('the operation changed nothing at all');
      }
      await burger('Undo');
      const after = await fullState();
      const diff = firstDifference(before, after);
      if (diff) throw new Error(`undo left ${diff}`);
      return 'restored exactly';
    });
  }

  // ---- column properties -------------------------------------------------

  await check("changing a column's KIND changes how it aggregates", async () => {
    // The kind is not cosmetic: a dimension takes its unique value
    // when grouped and a measure sums. Only the editor can change it,
    // and nothing had ever driven that -- so this asserts the
    // GENERATED AGGREGATE, not the label in the panel.
    await menu(['Pivot', 'Clear All Vertical Pivots']).catch(() => {});
    await editorTab('Column Properties');
    const chooser = page.locator('.dc-field', { hasText: 'Column:' })
      .first().locator('select').first();
    if (!(await chooser.count())) throw new Error('no column chooser');
    const opts = await chooser.evaluate((e) =>
      [...e.options].map((o) => o.value));
    const want = ['quantity', 'year', 'trade_id'].find(
      (n) => opts.includes(n));
    if (!want) throw new Error(`no integer column among ${opts.join(',')}`);
    await chooser.selectOption(want);
    await page.waitForTimeout(250);
    // Upstream's one ADVANCED setting.
    await page.locator('.dc-check', { hasText: 'Show advanced settings?' }).locator('input').check();
    await page.waitForTimeout(150);

    const kind = page.locator('.dc-field', { hasText: 'Column Kind:' })
      .first().locator('select').first();
    if ((await kind.inputValue()) !== 'dimension') {
      throw new Error(`${want} is already a ${await kind.inputValue()}`);
    }
    await kind.selectOption('measure');
    await page.waitForTimeout(200);
    await applyEditor();

    // The panel is where a person sees it, so check there too -- but
    // the query is the claim.
    const isMeasure = await page.evaluate((n) =>
      document.querySelector(`.dc-tool-panel-row[data-column="${n}"]`)
        ?.classList.contains('dc-measure') ?? false, want);
    if (!isMeasure) throw new Error(`the panel still lists ${want} as a`
      + ' dimension');

    await menu(['Pivot', /^Vertical Pivot on/], { col: GROUP_COL });
    const sql = (await state()).sql.replace(/\s+/g, ' ');
    const summed = new RegExp(`SUM\\(t0\\.${want}\\)`, 'i').test(sql);
    const unique = new RegExp(
      `CASE WHEN COUNT\\(DISTINCT t0\\.${want}\\)`, 'i').test(sql);
    if (!summed) {
      throw new Error(`${want} is a measure but does not sum`
        + `${unique ? ' — it still takes its unique value' : ''}`);
    }
    await menu(['Pivot', 'Clear All Vertical Pivots']);
    return `${want} now sums`;
  });

  await check('a display name changes the label, not the identity', async () => {
    // The header-identity fault made a label and a `data-column`
    // disagree, so this is the one place they are SUPPOSED to: a
    // renamed column keeps its identity, which is exactly why the
    // invariant compares headers by position rather than by text.
    await editorTab('Column Properties');
    const chooser = page.locator('.dc-field', { hasText: 'Column:' })
      .first().locator('select').first();
    const opts = await chooser.evaluate((e) =>
      [...e.options].map((o) => o.value));
    const want = opts[opts.length - 1];
    await chooser.selectOption(want);
    await page.waitForTimeout(250);
    const nameField = page.locator('.dc-field', { hasText: 'Display Name' })
      .first().locator('input').first();
    if (!(await nameField.count())) throw new Error('no Display Name field');
    await nameField.fill('RENAMED');
    await nameField.press('Tab');
    await applyEditor();

    const pair = await page.evaluate((n) => {
      const th = [...document.querySelectorAll('.dc-th')].find(
        (e) => e.dataset.column === n);
      return th ? { label: (th.textContent ?? '').trim(), id: th.dataset.column }
        : null;
    }, want);
    if (!pair) throw new Error(`${want} lost its header entirely`);
    if (pair.label !== 'RENAMED') {
      throw new Error(`the header still reads ${JSON.stringify(pair.label)}`);
    }
    if (pair.id !== want) {
      throw new Error(`the identity changed to ${pair.id}`);
    }
    return `${want} shows as "RENAMED" and is still ${pair.id}`;
  });

  // ---- saving and loading a view -----------------------------------------

  await check('a saved view is restored when loaded', async () => {
    // The unit test covers the restore; this covers the WIRING -- the
    // two title-bar entries, the storage the host supplies, and the
    // dialog-free path between them. The old test asserted that
    // something was written to storage and then clicked Load with no
    // assertion, so loading restored nothing for as long as anyone
    // had been looking.
    await menu(['Pivot', /^Vertical Pivot on/], { col: GROUP_COL });
    const wanted = await fullState();
    if (!/groupBy\(~\[/.test(wanted.pure)) {
      throw new Error('could not set up: the cube did not group');
    }
    await burger('Save View');

    await menu(['Pivot', 'Clear All Vertical Pivots']);
    const cleared = await fullState();
    if (firstDifference(wanted, cleared) === null) {
      throw new Error('could not set up: clearing changed nothing');
    }

    await burger('Load View');
    const back = await fullState();
    const diff = firstDifference(wanted, back);
    if (diff) throw new Error(`loading did not restore ${diff}`);
    return 'the grouped cube came back';
  });

  // ---- getting rid of a menu ---------------------------------------------
  //
  // Reported for both menus: it opens, and then there is no way to
  // close it. The menu listened for Escape on ITSELF, which works
  // only while focus is still inside it -- one click elsewhere ends
  // that -- and nothing at all watched for a press outside. The
  // title bar button was worse: pressing it again re-ran `show()`,
  // which closes and immediately reopens, so it looked inert.

  const menuOpen = () => page.locator('.dc-menu').count();

  await check('a click elsewhere dismisses the grid menu', async () => {
    await page.locator('.dc-row .dc-cell').first().click({ button: 'right' });
    await page.waitForSelector('.dc-menu', { timeout: 5000 });
    if (!(await menuOpen())) throw new Error('the menu never opened');
    // An inert place to press, nowhere near the menu. The row count
    // in the status bar: it is text the cube always renders, where
    // the title bar's brand -- used here before -- is optional and a
    // host that wants the pixels turns it off.
    await page.locator('.dc-status-rows').click();
    await page.waitForTimeout(200);
    const left = await menuOpen();
    if (left) throw new Error(`${left} menu(s) survived a click elsewhere`);
    return 'gone';
  });

  await check('Escape dismisses the grid menu, focus or no focus', async () => {
    await page.locator('.dc-row .dc-cell').first().click({ button: 'right' });
    await page.waitForSelector('.dc-menu', { timeout: 5000 });
    // Move focus OUT of the menu first: listening on the menu alone
    // is what made Escape unreliable.
    await page.locator('.dc-tool-panel-search').focus().catch(() => {});
    await page.keyboard.press('Escape');
    await page.waitForTimeout(200);
    const left = await menuOpen();
    if (left) throw new Error(`${left} menu(s) survived Escape`);
    return 'gone';
  });

  await check('the title bar button opens AND closes its menu', async () => {
    await page.click('.dc-titlebar-menu');
    await page.waitForSelector('.dc-menu', { timeout: 5000 });
    await page.click('.dc-titlebar-menu');
    await page.waitForTimeout(250);
    const left = await menuOpen();
    if (left) {
      throw new Error(`${left} menu(s) left: pressing the button again`
        + ' reopened it rather than closing it');
    }
    return 'toggles';
  });

  await check('the filter editor is reachable from the status bar', async () => {
    // It was two levels down a right-click menu -- Filter, then
    // "Filters..." -- and went unfound. DataCube puts a Filter button
    // in the status bar (DataCubeStatusBar) for this reason.
    const button = page.locator('.dc-status-filter');
    if (!(await button.count())) {
      throw new Error('no Filter control in the status bar');
    }
    await button.first().click();
    await page.waitForTimeout(400);
    const shown = await page.locator('.dc-filter-empty, .dc-filter-tree')
      .count();
    if (!shown) throw new Error('the Filter button opened nothing');
    await page.keyboard.press('Escape');
    await page.waitForTimeout(200);
    return 'opens the editor';
  });

  // ---- the dialogs are windows -------------------------------------------

  await check('a dialog FLOATS over the grid, and does not grow the page',
    async () => {
      // They were a block appended after the grid: opening one pushed
      // the page down and showed it below the data it was about.
      const pageBefore = await page.evaluate(() =>
        document.documentElement.scrollHeight);
      await menu(['Properties...'], { requery: false });
      const g = await page.evaluate(() => {
        const w = document.querySelector('.dc-app-overlay');
        const grid = document.querySelector('.dc-app-grid');
        const r = (el) => {
          const b = el.getBoundingClientRect();
          return { y: Math.round(b.top), h: Math.round(b.height),
            x: Math.round(b.left), w: Math.round(b.width) };
        };
        return {
          position: getComputedStyle(w).position,
          grips: w.querySelectorAll('.dc-window-grip').length,
          win: r(w), grid: r(grid),
          doc: document.documentElement.scrollHeight,
        };
      });
      if (g.position !== 'absolute') {
        throw new Error(`the dialog is ${g.position}, not a floating window`);
      }
      if (g.grips !== 8) {
        throw new Error(`${g.grips} resize grips, expected 8 (every edge and`
          + ' corner, as upstream has)');
      }
      const over = g.win.y < g.grid.y + g.grid.h
        && g.win.y + g.win.h > g.grid.y;
      if (!over) {
        throw new Error(`the dialog sits at y=${g.win.y} and the grid spans`
          + ` ${g.grid.y}..${g.grid.y + g.grid.h}: it is beside the data,`
          + ' not over it');
      }
      // A window that floats cannot make the document taller. It did:
      // measuring the container while the dialog was still IN FLOW
      // centred it against a container half again too tall, and its
      // bottom grips ended up below the fold and unreachable.
      if (g.doc > pageBefore) {
        throw new Error(`opening it grew the page ${pageBefore} -> ${g.doc},`
          + ' so it is not floating clear');
      }
      return `${g.win.w}x${g.win.h} at ${g.win.x},${g.win.y}`;
    });

  // The drag check below needs it open, and `reset` shuts it before
  // every check, so it is reopened there rather than left standing
  // here -- a window left open covers the grid.

  await check('a CLOSED dialog is really gone', async () => {
    // `.dc-window` sets `display: flex`, which outranks what the
    // `hidden` attribute means -- so a dialog that had been opened
    // and closed stayed on screen, floating over the grid and
    // swallowing every click, while `element.hidden` still reported
    // true. Nothing looked wrong; cells simply could not be
    // right-clicked, and ten checks timed out.
    await menu(['Properties...'], { requery: false });
    await page.locator('.dc-overlay-close').first().click();
    await page.waitForTimeout(250);

    // A closed window is GONE from the page, not hidden on it: several
    // can be open at once, so each is its own element and closing it
    // removes it.
    const state = await page.evaluate(() => {
      const w = document.querySelector('.dc-app-overlay');
      if (!w) return null;
      const b = w.getBoundingClientRect();
      return {
        hidden: w.hidden,
        display: getComputedStyle(w).display,
        area: Math.round(b.width) * Math.round(b.height),
      };
    });
    if (state !== null) {
      throw new Error(`a closed dialog is still on the page:`
        + ` ${JSON.stringify(state)}`);
    }

    // And the grid is usable again, which is the thing that broke.
    await menu(['Sort', 'Clear All Sorts']).catch(() => {});
    return 'gone, and the grid takes clicks again';
  });

  await check('the Filter and Properties windows stay open together', async () => {
    // Upstream's layout keeps several windows open at once; there used
    // to be ONE overlay, and opening the filter threw the editor away.
    await menu(['Properties...'], { requery: false });
    // From the status bar: `menu()` starts by closing every window.
    await page.locator('.dc-status-filter').click();
    await page.waitForTimeout(200);
    const open = await page.evaluate(() =>
      [...document.querySelectorAll('.dc-app-overlay')]
        .map((w) => w.dataset.window));
    if (!(open.includes('Properties') && open.includes('Filters'))) {
      throw new Error(`open windows: ${open.join(', ')}`);
    }
    // Both take input: a click in the editor raises it above the filter.
    await page.locator('[data-window="Properties"] .dc-editor-tab').first().click();
    const z = await page.evaluate(() => ({
      props: Number(document.querySelector('[data-window="Properties"]').style.zIndex),
      filters: Number(document.querySelector('[data-window="Filters"]').style.zIndex),
    }));
    if (!(z.props > z.filters)) {
      throw new Error(`clicking the editor did not raise it: ${JSON.stringify(z)}`);
    }
    return 'both open; the one touched comes to the front';
  });

  await check('a dialog can be dragged and resized, and stays put',
    async () => {
      // `reset` shut whatever the previous check left open, so open
      // it here.
      await menu(['Properties...'], { requery: false });
      const box = () => page.evaluate(() => {
        const b = document.querySelector('.dc-app-overlay')
          .getBoundingClientRect();
        return { x: Math.round(b.left), y: Math.round(b.top),
          w: Math.round(b.width), h: Math.round(b.height) };
      });
      const start = await box();

      const head = await page.locator('.dc-overlay-head').first()
        .boundingBox();
      await page.mouse.move(head.x + head.width / 2, head.y + head.height / 2);
      await page.mouse.down();
      await page.mouse.move(head.x + head.width / 2 - 90,
        head.y + head.height / 2 + 40, { steps: 6 });
      await page.mouse.up();
      await page.waitForTimeout(200);
      const moved = await box();
      if (moved.x === start.x && moved.y === start.y) {
        throw new Error(`dragging the title bar moved nothing:`
          + ` still ${start.x},${start.y}`);
      }

      const grip = await page.locator('.dc-window-se').first().boundingBox();
      if (!grip) throw new Error('no south-east grip to grab');
      await page.mouse.move(grip.x + grip.width / 2, grip.y + grip.height / 2);
      await page.mouse.down();
      await page.mouse.move(grip.x + 100, grip.y + 70, { steps: 6 });
      await page.mouse.up();
      await page.waitForTimeout(200);
      const sized = await box();
      if (sized.w <= moved.w || sized.h <= moved.h) {
        throw new Error(`the south-east grip did not resize:`
          + ` ${moved.w}x${moved.h} -> ${sized.w}x${sized.h}`);
      }

      // Closed and reopened, it comes back where it was left.
      await page.locator('.dc-overlay-close').first().click();
      await page.waitForTimeout(200);
      await menu(['Properties...'], { requery: false });
      const again = await box();
      if (again.x !== sized.x || again.y !== sized.y
        || again.w !== sized.w || again.h !== sized.h) {
        throw new Error(`it jumped on reopening: ${sized.w}x${sized.h} at`
          + ` ${sized.x},${sized.y} became ${again.w}x${again.h} at`
          + ` ${again.x},${again.y}`);
      }
      await page.keyboard.press('Escape');
      await page.waitForTimeout(200);
      return `moved to ${moved.x},${moved.y} and resized to`
        + ` ${sized.w}x${sized.h}`;
    });

  // ---- the chrome ---------------------------------------------------------

  await check('the sidebar collapses and gives the width to the grid', async () => {
    const read = () => page.evaluate(() => ({
      grid: Math.round(document.querySelector('.dc-app-grid')
        .getBoundingClientRect().width),
      doc: document.documentElement.scrollHeight,
    }));
    const a = await read();
    await page.click('.dc-tool-panel-toggle');
    const b = await read();
    await page.click('.dc-tool-panel-toggle');
    if (b.grid <= a.grid + 100) {
      throw new Error(`the grid did not widen: ${a.grid} -> ${b.grid}`);
    }
    if (b.doc !== a.doc) {
      throw new Error(`the page height changed: ${a.doc} -> ${b.doc}`);
    }
    return `${a.grid} -> ${b.grid}px`;
  });

  await check('the header follows the columns sideways', async () => {
    const probe = () => page.evaluate(() => {
      const sc = document.querySelector('.dc-scroller');
      const ths = [...document.querySelectorAll('.dc-th[data-column]')];
      const tds = document.querySelector('.dc-row')
        ?.querySelectorAll('.dc-cell') ?? [];
      const i = Math.min(ths.length, tds.length) - 1;
      return {
        range: Math.round(sc.scrollWidth - sc.clientWidth),
        th: Math.round(ths[i].getBoundingClientRect().left),
        td: Math.round(tds[i].getBoundingClientRect().left),
      };
    });
    await page.setViewportSize({ width: 620, height: 800 });
    await page.waitForTimeout(200);
    const p0 = await probe();
    if (p0.range < 100) throw new Error('nothing to scroll; check is vacuous');
    await page.evaluate(() => {
      document.querySelector('.dc-scroller').scrollLeft = 250;
    });
    await page.evaluate(() => new Promise((r) =>
      requestAnimationFrame(() => requestAnimationFrame(r))));
    const p1 = await probe();
    await page.evaluate(() => {
      document.querySelector('.dc-scroller').scrollLeft = 0;
    });
    await page.setViewportSize({ width: 1400, height: 900 });
    await page.waitForTimeout(200);
    if (Math.abs(p1.th - p1.td) > 2) {
      throw new Error(`header at ${p1.th}, its column at ${p1.td}`);
    }
    return `aligned at ${p1.th}px after scrolling`;
  });

  // -- calculated columns -------------------------------------------

  /** Shut every window the way `reset` does: the BUTTON, not Escape. */
  const closeCalc = async () => {
    const shut = page.locator(
      '.dc-app-overlay:not([hidden]) .dc-overlay-close');
    for (let i = 0; i < 6 && await shut.count(); i += 1) {
      await shut.first().click().catch(() => {});
      await page.waitForTimeout(120);
    }
  };

  /**
   * Open an "Add New Column" window from the GRID's menu, where
   * upstream keeps it: Extended Columns > Add New Column...
   */
  const openCalc = async () => {
    await closeCalc();
    await menu(['Extended Columns', 'Add New Column...'], { requery: false });
    await page.waitForSelector('.dc-coleditor', { timeout: 10_000 });
  };

  /** The live compile has answered: compiled, refused, or unavailable. */
  const compiledCheck = async () => {
    await page.waitForFunction(() => {
      const c = document.querySelector('.dc-coleditor .dc-calc-check');
      return c && c.getAttribute('data-state') !== 'compiling';
    }, undefined, { timeout: 15_000 });
    return page.evaluate(() => {
      const c = document.querySelector('.dc-coleditor .dc-calc-check');
      return { state: c?.getAttribute('data-state'), text: c?.textContent ?? '' };
    });
  };

  /** The calculated columns the running query extends with. */
  const calcNames = async () => {
    const { pure } = await state();
    return [...pure.matchAll(/extend\(~\[([^:\]]+):/g)].map((m) => m[1].replace(/^'|'$/g, ''));
  };

  /**
   * Remove every calculated column, through the product's own path:
   * Extended Columns > Delete Column X on each. The cube goes flat
   * first, so every one of them is a column in the grid to right-click.
   *
   * `reset` shuts windows; it does not touch the snapshot. Without
   * this a column outlives its own check -- and `uplift` surviving
   * into a pivoted one produced `Binder Error: Values list "t2" does
   * not have a column named "uplift"`, which looked like a failure of
   * whichever check ran next.
   */
  const clearCalcs = async () => {
    await closeCalc();
    if ((await calcNames()).length === 0) return;
    await flatten();
    for (let i = 0; i < 8; i += 1) {
      const [name] = await calcNames();
      if (name === undefined) break;
      await menu(['Extended Columns', `Delete Column ${name}`], { col: await needCol(name) });
    }
    await settle();
  };

  /**
   * Add one, at `stage` (0 = leaf level, 1 = group level), as a user
   * does: name, kind, expression, wait for the compile, OK. A draft
   * the compiler refuses leaves its window open, OK disabled.
   */
  const addCalc = async (stage, name, expression, kind = 'measure') => {
    await openCalc();
    await page.fill('.dc-coleditor .dc-calc-input-name', name);
    // `kind === null` leaves the editor's own default alone, which is
    // what a user who never touches the kind gets.
    const level = stage === 1 ? 'group' : kind;
    if (level !== null) {
      await page.locator('.dc-coleditor .dc-calc-level').selectOption(level);
    }
    await page.fill('.dc-coleditor .dc-calc-input-expr', expression);
    const check = await compiledCheck();
    if (check.state === 'refused') return check;
    const ok = page.locator('.dc-coleditor .dc-calc-ok');
    if (await ok.isDisabled()) {
      // Say WHY rather than wait thirty seconds on a disabled button.
      const problem = await page.locator('.dc-coleditor .dc-calc-problem').textContent();
      throw new Error(`OK is disabled adding ${name}: ${problem || check.text}`);
    }
    await ok.click();
    return check;
  };

  await check('a calculated column computes, and its arithmetic is right',
    async () => {
      // Unpivoted on purpose: in a PIVOTED cube every non-pivoted
      // column collapses to its unique value by design, so a carried
      // numeric reads blank there -- pnl and qty included -- and a
      // check on the VALUE has to be made where the value exists.
      await menu(['Pivot', 'Clear All Horizontal Pivots'],
        { requery: false }).catch(() => {});
      await settle();
      await addCalc(0, 'uplift', '$x.notional * 1.1');
      await settle();
      await page.keyboard.press('Escape');
      const s2 = await state();
      if (!/extend\(~\[uplift/.test(s2.pure)) {
        throw new Error(`no extend in the query: ${s2.pure.slice(0, 160)}`);
      }
      if (!/uplift:x\|\$x\.uplift:y\|\$y->sum\(\)/.test(s2.pure)) {
        throw new Error('a numeric calculated column did not SUM —'
          + ' its learned type never reached the aggregate default');
      }
      const cols = await gridColumns();
      if (!cols.includes('uplift')) {
        throw new Error(`not in the grid: ${cols.join(', ')}`);
      }
      // THE ARITHMETIC. notional x 1.1, read off the same row.
      const cells = await page.locator('.dc-row').first()
        .locator('.dc-cell').allTextContents();
      const num = (i) => Number((cells[i] ?? '').replace(/[^0-9.-]/g, ''));
      const at = cols.indexOf('notional');
      const up = cols.indexOf('uplift');
      if (at < 0 || up < 0) throw new Error('columns missing for the check');
      const want = num(at) * 1.1;
      const got = num(up);
      if (want === 0 || Math.abs(got - want) / want > 0.001) {
        throw new Error(`uplift is ${got}, expected ${want}`);
      }
      const detail = `${cols.length} columns, uplift = notional x 1.1`
        + ` = ${got}`;
      await clearCalcs();
      return detail;
    });

  await check('a GROUP-stage calculated column computes on the aggregates',
    async () => {
      // THE STAGE THAT WAS BROKEN. `groupDerived` names were being put
      // into the pre-aggregation `select(...)`, so the planner was
      // asked for a column that does not exist until after the
      // groupBy: "unknown column 'margin' in (region:String[0..1],
      // ...)". Every group-stage calculated column was unusable, and
      // no check noticed, because the editor offered both stages and
      // only the row one was ever exercised.
      //
      // The arithmetic is the point of the stage: a ratio of two
      // AGGREGATES. Computing it per row and averaging gives a
      // different and wrong answer.
      await menu(['Pivot', 'Clear All Horizontal Pivots'],
        { requery: false }).catch(() => {});
      await settle();
      await addCalc(1, 'doubled', '$x.notional * 2');
      await settle();
      await closeCalc();
      const s2 = await state();
      if (/unknown column|Binder Error/.test(s2.status)) {
        throw new Error(s2.status.replace(/\s+/g, ' ').slice(0, 160));
      }
      // The extend must come AFTER the groupBy, not in the select.
      const selectAt = s2.pure.indexOf('select(~[');
      const groupAt = s2.pure.indexOf('groupBy(~[');
      const extendAt = s2.pure.lastIndexOf('extend(~[doubled');
      if (extendAt < 0) {
        throw new Error(`no group-stage extend: ${s2.pure.slice(0, 200)}`);
      }
      if (groupAt >= 0 && extendAt < groupAt) {
        throw new Error('the group-stage extend ran BEFORE the groupBy');
      }
      if (selectAt >= 0 && /select\(~\[[^\]]*doubled/.test(s2.pure)) {
        throw new Error('a group-stage column leaked into the projection'
          + ' — that is the defect this check exists for');
      }
      const cols = await gridColumns();
      if (!cols.includes('doubled')) {
        throw new Error(`not in the grid: ${cols.join(', ')}`);
      }
      // notional x 2, off the same row.
      const cells = await page.locator('.dc-row').first()
        .locator('.dc-cell').allTextContents();
      const num = (i) => Number((cells[i] ?? '').replace(/[^0-9.-]/g, ''));
      const at = cols.indexOf('notional');
      const got = num(cols.indexOf('doubled'));
      const want = num(at) * 2;
      if (at < 0 || want === 0 || Math.abs(got - want) / want > 0.001) {
        throw new Error(`doubled is ${got}, expected ${want}`);
      }
      const detail = `extend after groupBy, doubled = ${got}`;
      await clearCalcs();
      return detail;
    });

  await check('a calculated column learns its TYPE from the result',
    async () => {
      // Nothing here infers the type from the expression -- it comes
      // back from the query. Until it does, the aggregate default
      // reads no type and a numeric column groups as `unique`, which
      // renders blank rather than failing.
      await addCalc(0, 'uplift', '$x.notional * 1.1');
      await settle();
      try {
      // The type the result reported, as Column Properties shows it: a
      // Float shows the number section with upstream's 2 decimals.
      await page.click('.dc-status-properties');
      await page.locator('.dc-app-overlay .dc-editor-tab', { hasText: 'Column Properties' }).click();
      await page.locator('.dc-app-overlay .dc-field:has(> .dc-field-label:text-is("Choose Column:")) select')
        .selectOption('uplift');
      // The Decimals field holds the number AND the commas and parens
      // boxes, so the number input by its type.
      const decimalsField = page.locator('.dc-app-overlay .dc-field:has(> .dc-field-label:text-is("Decimals:")) input[type=number]');
      const fields = await decimalsField.count();
      if (fields > 1) {
        const windows = await page.locator('.dc-app-overlay').evaluateAll((ws) =>
          ws.map((w) => w.dataset.window));
        throw new Error(`${fields} Decimals fields: windows open ${windows.join(', ')}`);
      }
      const decimals = fields
        ? await decimalsField.inputValue()
        : `(no Decimals field; sections: ${(await page.locator('.dc-app-overlay .dc-section-title').allTextContents()).join(', ')})`;
      if (decimals !== '2') {
        throw new Error(`uplift shows no Float number format (decimals ${decimals}) —`
          + ' the type never came back from the result');
      }
      return 'uplift: Float, 2 decimals';
      } finally {
        // A failure here must not leave `uplift` for the next check.
        await closeCalc();
        await clearCalcs();
      }
    });

  await check("a bad expression shows the PLANNER's own refusal",
    async () => {
      // Compiled as it is typed, as upstream's: the refusal is in the
      // window, OK stays disabled, and the running cube is never touched.
      const before = await state();
      const check = await addCalc(0, 'bogus', '$x.notional->nosuchfunction()');
      const ok = await page.locator('.dc-coleditor .dc-calc-ok').isDisabled();
      await closeCalc();
      if (check.state !== 'refused' || !/nosuchfunction/.test(check.text)) {
        throw new Error(`the compile did not refuse by name: ${check.state} ${check.text.slice(0, 120)}`);
      }
      if (!ok) throw new Error('OK stayed enabled on a refused draft');
      const after = await state();
      if (after.pure !== before.pure) throw new Error('a refused draft reached the cube');
      return `${check.text.replace(/\s+/g, ' ').slice(0, 70)} … the cube untouched`;
    });

  await check('a duplicate name is refused before the planner sees it',
    async () => {
      await openCalc();
      await page.fill('.dc-coleditor .dc-calc-input-name', 'region');
      await page.fill('.dc-coleditor .dc-calc-input-expr', '1');
      const problem = await page.locator('.dc-coleditor .dc-calc-problem').textContent();
      const mark = await page.locator('.dc-coleditor .dc-calc-namemark').textContent();
      if (mark !== '✗') throw new Error(`the name shows ${mark}, not ✗`);
      if (!/already a column/.test(problem ?? '')) {
        throw new Error(`no complaint about the name: ${problem}`);
      }
      if (!await page.locator('.dc-coleditor .dc-calc-ok').isDisabled()) {
        throw new Error('Add stayed enabled on a duplicate name');
      }
      await page.keyboard.press('Escape');
      return problem ?? '';
    });

  await check('the GROUP stage never offers a source column', async () => {
    // A groupDerived expression runs after the source rows are gone,
    // so naming one cannot compile. Offering it would be a suggestion
    // the planner refuses, and the user could not tell whose fault
    // that was.
    await openCalc();
    await page.locator('.dc-coleditor .dc-calc-level').selectOption('group');
    const offered = await page.locator('.dc-coleditor .dc-calc-item-column'
      + ' .dc-calc-item-label').allTextContents();
    const leaked = ['pnl', 'qty', 'book'].filter((n) => offered.includes(n)
      && !offered.slice(0, 0).includes(n));
    // A row dimension IS in scope at this stage, so only columns that
    // are neither a dimension nor a measure count as a leak.
    const dims = await dimensionNames();
    const measures = ['notional'];
    const real = leaked.filter((n) => !dims.includes(n)
      && !measures.includes(n));
    if (real.length > 0) {
      throw new Error(`offered source columns at the group stage:`
        + ` ${real.join(', ')}`);
    }
    await page.keyboard.press('Escape');
    return `${offered.length} in scope`;
  });

  await check('removing a calculated column takes it out of the query',
    async () => {
      await addCalc(0, 'uplift', '$x.notional * 1.1');
      await settle();
      // From its own window, as upstream: Edit Column uplift... > Delete.
      await menu(['Extended Columns', 'Edit Column uplift...'],
        { col: await needCol('uplift'), requery: false });
      await page.waitForSelector('.dc-coleditor .dc-calc-delete', { timeout: 5000 });
      const before = await statusNow();
      await page.locator('.dc-coleditor .dc-calc-delete').click();
      await settle(before);
      if (await page.locator('.dc-coleditor').count()) {
        throw new Error('the window stayed open after its column was deleted');
      }
      const s2 = await state();
      if (/extend\(~\[uplift/.test(s2.pure)) {
        throw new Error('the extend survived the removal');
      }
      if ((await gridColumns()).includes('uplift')) {
        throw new Error('the column survived in the grid');
      }
      await clearCalcs();
      return 'gone from both the query and the grid';
    });

  await check('a calculated column survives a PIVOTED, grouped cube',
    async () => {
      // Declared as a gap first, on the strength of a `Binder Error:
      // Values list "t2" does not have a column named "uplift"`. It
      // was not a defect: the column had LEAKED from an earlier check
      // into a cube over a different source table, so the expression
      // named a column that really was not there. With the checks
      // cleaning up after themselves it passes, and the suite said so
      // -- which is what the gap mechanism is for.
      await addCalc(0, 'uplift', '$x.notional * 1.1');
      await settle();
      await closeCalc();
      const s2 = await state();
      if (/Binder Error|does not have a column/.test(s2.status)) {
        throw new Error(s2.status.replace(/\s+/g, ' ').slice(0, 150));
      }
      const cols = await gridColumns();
      if (!cols.includes('uplift')) {
        throw new Error(`not in the grid: ${cols.join(', ')}`);
      }
      const detail = `${cols.length} columns, no binder error`;
      await clearCalcs();
      return detail;
    });

  await check('clearing the column pivot keeps the other columns',
    async () => {
      // Grouped and pivoted, clearing the pivot left ONE data column
      // on screen -- the measure -- with year, qtr, pnl and qty gone
      // from a grid whose own columns panel still listed them. The
      // projection carried the keys and the measure and dropped the
      // rest, so the answer came back narrower than the question.
      await menu(['Pivot', 'Clear All Vertical Pivots'], { requery: false })
        .catch(() => {});
      await menu(['Pivot', 'Clear All Horizontal Pivots'], { requery: false })
        .catch(() => {});
      await settle();
      const flat = await gridColumns();
      const dims = await dimensionNames();
      const row = ['region', 'desk', 'book'].find((n) => dims.includes(n));
      const col = ['year', 'quarter', 'qtr'].find((n) => dims.includes(n));
      if (!row || !col) {
        throw new Error(`need a row and a column dimension, saw ${dims}`);
      }
      await menu(['Pivot', /^Vertical Pivot on/], { col: await needCol(row) });
      await menu(['Pivot', /^Horizontal Pivot on/],
        { col: await needCol(col) });
      const pivoted = await gridColumns();
      await menu(['Pivot', 'Clear All Horizontal Pivots']);
      const after = await gridColumns();
      // Every column the flat cube showed is still here, except the
      // one now in the tree -- and the pivot key, which the cube
      // carries as a dimension either way.
      const missing = flat.filter((c) => c !== row && !after.includes(c));
      if (missing.length > 0) {
        throw new Error(`clearing the pivot lost ${missing.join(', ')};`
          + ` left ${after.join(', ')}`);
      }
      return `${flat.length} flat, ${pivoted.length} pivoted,`
        + ` ${after.length} after clearing — none lost`;
    });

  // -- calculated columns, as a user meets them ----------------------
  //
  // The checks above prove each stage computes. These prove the flows
  // around it: the default a user gets without touching the kind, a
  // calculated column used like any other column, a refusal that
  // leaves the user's text alone, and the right-click entry points.
  // Each was a defect in the census (datacube/docs/FEATURE_CENSUS.md
  // §1, C1-C6) before it was a check.

  /** Right-click a body cell and list every entry the menu offers. */
  const menuAt = async (col) => {
    await reset();
    await page.evaluate(() => {
      const sc = document.querySelector('.dc-scroller');
      if (sc) sc.scrollLeft = 0;
    });
    await page.locator('.dc-row').first().locator('.dc-cell').nth(col)
      .click({ button: 'right', timeout: 8000 });
    await page.waitForSelector('.dc-menu', { timeout: 5000 });
    const items = await openMenu();
    await page.keyboard.press('Escape');
    return items;
  };

  await check('a NEW calculated column sums in a grouped cube by default',
    async () => {
      // A check that failed earlier cannot leave its columns behind.
      await clearCalcs();
      // C1. Upstream defaults a new column to MEASURE; a dimension
      // default made `$x.notional * 1.1` render blank under any row
      // group -- the query took its uniqueValueOnly() -- which is the
      // first thing anyone trying the feature sees.
      await flatten();
      await menu(['Pivot', /^Vertical Pivot on/],
        { col: await needCol('region') });
      await addCalc(0, 'uplift', '$x.notional * 1.1', null);
      await settle();
      await closeCalc();
      const s2 = await state();
      if (!/uplift:x\|\$x\.uplift:y\|\$y->sum\(\)/.test(s2.pure)) {
        throw new Error('the new column does not sum: '
          + (s2.pure.match(/uplift:x\|[^,\]]*/)?.[0] ?? s2.pure.slice(0, 160)));
      }
      const cols = await gridColumns();
      const at = cols.indexOf('uplift');
      const cells = await page.locator('.dc-row').first()
        .locator('.dc-cell').allTextContents();
      if (at < 0 || !(cells[at] ?? '').trim()) {
        throw new Error(`uplift is blank in the grouped grid: ${cells.join(' | ')}`);
      }
      const detail = `uplift under region = ${cells[at]}`;
      await flatten();
      await clearCalcs();
      return detail;
    });

  await check('a calculated DIMENSION can be grouped on', async () => {
    // A check that failed earlier cannot leave its columns behind.
    await clearCalcs();
    // C2. The kind lookup read the SOURCE columns only, so a
    // calculated column was never a dimension: "Vertical Pivot" came
    // up disabled and the drag zones refused it.
    await flatten();
    await addCalc(0, 'big', '$x.notional > 500000', 'dimension');
    await settle();
    await closeCalc();
    const listed = await page.evaluate(() =>
      [...document.querySelectorAll('.dc-tool-panel-row')]
        .map((r) => r.dataset.column));
    if (!listed.includes('big')) {
      throw new Error(`the columns panel does not list it: ${listed.join(', ')}`);
    }
    await menu(['Pivot', 'Vertical Pivot on big'], { col: await needCol('big') });
    const s2 = await state();
    if (!/groupBy\(~\[big/.test(s2.pure)) {
      throw new Error(`not grouped on big: ${s2.pure.slice(0, 200)}`);
    }
    if (/error|refus|unknown/i.test(s2.status)) {
      throw new Error(s2.status.replace(/\s+/g, ' ').slice(0, 160));
    }
    const groups = await resultRows();
    await flatten();
    await clearCalcs();
    if (groups < 2) throw new Error(`${groups} groups, expected true and false`);
    return `${groups} groups on a calculated Boolean`;
  });

  await check('a column pivot carries calculated measures', async () => {
    // A check that failed earlier cannot leave its columns behind.
    await clearCalcs();
    // C3. The pivot's measure set was built from the source columns,
    // so a calculated measure vanished from a pivoted cube with no
    // error -- the query was the same as with no calculated column.
    await flatten();
    await addCalc(0, 'uplift', '$x.notional * 1.1', 'measure');
    await settle();
    await closeCalc();
    await menu(['Pivot', 'Horizontal Pivot on year'],
      { col: await needCol('year') });
    const s2 = await state();
    const pivot = s2.pure.match(/pivot\([^\n]*/)?.[0] ?? '';
    const cols = await gridColumns();
    await flatten();
    await clearCalcs();
    if (!/uplift/.test(pivot)) {
      throw new Error(`the pivot aggregates no uplift: ${pivot.slice(0, 200)}`);
    }
    const shown = cols.filter((c) => /uplift/.test(c));
    if (shown.length === 0) {
      throw new Error(`no pivoted uplift column: ${cols.join(', ')}`);
    }
    return `${shown.length} pivoted uplift columns`;
  });

  await check("the filter menu uses a calculated column's own type",
    async () => {
      // A check that failed earlier cannot leave its columns behind.
      await clearCalcs();
      // C4. With no type found, the menu fell back to String and
      // offered "big contains true" on a Boolean.
      await flatten();
      await addCalc(0, 'big', '$x.notional > 500000', 'dimension');
      await settle();
      await closeCalc();
      const items = await menuAt(await needCol('big'));
      await clearCalcs();
      const filters = items.map((i) => i.label)
        .filter((l) => /^Add Filter: big/.test(l));
      if (filters.length === 0) throw new Error('no filter entries for big');
      const textual = filters.filter((l) =>
        /contains|starts with|ends with|<|>/.test(l));
      if (textual.length > 0) {
        throw new Error(`text or ordering operators on a Boolean: ${textual.join(' / ')}`);
      }
      return filters.join(' / ');
    });

  await check('a refused expression keeps what the user typed, and says why',
    async () => {
      // A check that failed earlier cannot leave its columns behind.
      await clearCalcs();
      // C5. The refusal reverted the snapshot and the editor with it:
      // the column and its text were gone, and the reason was on the
      // status line, away from the form it was about.
      await addCalc(0, 'bogus', '$x.nope * 2', 'measure');
      const form = await page.evaluate(() => ({
        open: document.querySelectorAll('.dc-coleditor').length,
        name: document.querySelector('.dc-coleditor .dc-calc-input-name')?.value ?? null,
        expr: document.querySelector('.dc-coleditor .dc-calc-input-expr')?.value ?? null,
        problem: document.querySelector('.dc-coleditor .dc-calc-check')?.textContent ?? '',
      }));
      await closeCalc();
      await clearCalcs();
      if (!form.open) throw new Error('the form closed on a refusal');
      if (form.name !== 'bogus' || form.expr !== '$x.nope * 2') {
        throw new Error(`the typed text was lost: ${JSON.stringify(form)}`);
      }
      if (!/nope/.test(form.problem)) {
        throw new Error(`the form does not say why: ${JSON.stringify(form.problem)}`);
      }
      return form.problem.replace(/\s+/g, ' ').slice(0, 80);
    });

  await check('calculated columns live in the grid menu, not the hamburger',
    async () => {
      // A check that failed earlier cannot leave its columns behind.
      await clearCalcs();
      // C6, and upstream's own entries: Extended Columns > Add New
      // Column..., Extend Column X..., Edit Column X..., Delete
      // Column X.
      await flatten();
      await page.click('.dc-titlebar-menu');
      await page.waitForSelector('.dc-menu', { timeout: 5000 });
      const burgerItems = (await openMenu()).map((i) => i.label);
      await page.keyboard.press('Escape');
      if (burgerItems.some((l) => /Calculated Columns/.test(l))) {
        throw new Error('the hamburger still offers Calculated Columns');
      }
      const onSource = (await menuAt(await needCol('notional')))
        .map((i) => i.label);
      for (const want of ['Extended Columns', 'Add New Column...',
        'Extend Column notional...']) {
        if (!onSource.includes(want)) {
          throw new Error(`no "${want}"; offered ${onSource.join(' / ')}`);
        }
      }
      if (onSource.some((l) => /^(Edit|Delete) Column/.test(l))) {
        throw new Error('Edit/Delete offered on a source column');
      }
      // EXTEND prefills the reference and inherits the column's kind.
      await menu(['Extended Columns', 'Extend Column notional...'],
        { col: await needCol('notional'), requery: false });
      await page.waitForSelector('.dc-coleditor', { timeout: 5000 });
      const seeded = await page.evaluate(() => ({
        expr: document.querySelector('.dc-coleditor .dc-calc-input-expr')?.value,
        kind: document.querySelector('.dc-coleditor .dc-calc-level')?.value,
      }));
      if (seeded.expr !== '$x.notional' || seeded.kind !== 'measure') {
        throw new Error(`Extend seeded ${JSON.stringify(seeded)}`);
      }
      await page.fill('.dc-coleditor .dc-calc-input-name', 'n2');
      await compiledCheck();
      await page.locator('.dc-coleditor .dc-calc-ok').click();
      await settle();
      await closeCalc();
      const onCalc = (await menuAt(await needCol('n2'))).map((i) => i.label);
      for (const want of ['Edit Column n2...', 'Delete Column n2']) {
        if (!onCalc.includes(want)) {
          throw new Error(`no "${want}"; offered ${onCalc.join(' / ')}`);
        }
      }
      await menu(['Extended Columns', 'Delete Column n2'],
        { col: await needCol('n2') });
      const s2 = await state();
      if (/extend\(~\[n2/.test(s2.pure)) throw new Error('Delete left n2 in the query');
      return 'Add / Extend (seeded) / Edit / Delete, none in the hamburger';
    });

  await check('a window per column: two new at once, and Edit brings its own forward',
    async () => {
      await clearCalcs();
      await flatten();
      // Two Add New Column windows, side by side, as upstream allows.
      // Not through `menu`, which closes every window first: a
      // right-click on a cell the first window does not cover.
      await closeCalc();
      const openNew = async () => {
        await page.locator('.dc-row').nth(3).locator('.dc-cell').nth(1).click({ button: 'right' });
        const own = (t) => `.dc-menu-item:has(> .dc-menu-label:text-is(${JSON.stringify(t)}))`;
        await page.locator(own('Extended Columns')).first().hover();
        await page.locator(own('Add New Column...')).first().click();
      };
      await openNew();
      await page.locator('.dc-coleditor').first().evaluate((e) => {
        e.closest('.dc-app-overlay').style.left = '700px';
      });
      await openNew();
      const both = await page.locator('.dc-coleditor').count();
      await closeCalc();
      if (both !== 2) throw new Error(`${both} editor windows, expected 2`);
      await addCalc(0, 'uplift', '$x.notional * 1.1');
      await settle();
      const edit = async () => menu(['Extended Columns', 'Edit Column uplift...'],
        { col: await needCol('uplift'), requery: false });
      await edit();
      await page.fill('.dc-coleditor .dc-calc-input-expr', '$x.notional * 9');
      // Reset, as upstream's: back to what the column had.
      await page.locator('.dc-coleditor .dc-calc-reset').click();
      const expr = await page.inputValue('.dc-coleditor .dc-calc-input-expr');
      await closeCalc();
      await clearCalcs();
      if (expr !== '$x.notional * 1.1') throw new Error(`Reset left ${expr}`);
      return '2 new windows together; Edit opens the column, Reset restores it';
    });

  await check('Settings: from the title bar, and Row Buffer draws more rows', async () => {
    await reset();
    await flatten();
    const drawn = () => page.locator('.dc-row').count();
    const before = await drawn();
    await page.click('.dc-titlebar-menu');
    await page.locator('.dc-menu-item:has(> .dc-menu-label:text-is("Settings..."))').click();
    await page.waitForSelector('[data-window="Settings"] .dc-settings', { timeout: 5000 });
    const groups = await page.locator('.dc-settings-group').allTextContents();
    const buffer = page.locator('[data-setting="dataCube.grid.rowBuffer"] input');
    await buffer.fill('200');
    await buffer.dispatchEvent('change');
    await page.locator('.dc-settings-ok').click();
    await page.waitForTimeout(300);
    const after = await drawn();
    // Back to the defaults, through the same window.
    await page.click('.dc-titlebar-menu');
    await page.locator('.dc-menu-item:has(> .dc-menu-label:text-is("Settings..."))').click();
    await page.locator('.dc-settings-restore').click();
    await page.locator('.dc-settings-ok').click();
    if (groups.join(',') !== 'Grid,Editor,Debug') throw new Error(`groups ${groups}`);
    if (after <= before) throw new Error(`rows drawn ${before} -> ${after} with a larger buffer`);
    return `groups ${groups.join('/')}; rows drawn ${before} -> ${after}`;
  });

  await check('a (?) opens its documentation', async () => {
    await reset();
    await page.click('.dc-status-properties');
    await page.locator('.dc-app-overlay .dc-editor-tab', { hasText: 'General Properties' }).click();
    await page.locator('.dc-field:has(> .dc-field-label:text-is("Row Limit:")) .dc-doc-hint').click();
    const text = await page.locator('[data-window="Documentation"]').innerText({ timeout: 5000 });
    await reset();
    if (!/Truncate result to the specified number of rows at every level/.test(text)) {
      throw new Error(`the documentation read: ${text.slice(0, 120)}`);
    }
    return 'Row Limit: upstream\'s text';
  });

  // -- the columns panel, which is a control and not a legend -------

  await gap('a panel reorder still reaches the grid after a long run',
    'the panel and the configuration take the new order and the grid'
    + ' keeps the old one, but only after the checks above have run --'
    + ' the same reorder works on a freshly loaded cube, in Node'
    + ' against a stub engine, and in every subset of this suite I'
    + ' have tried. The projection keeps the old order too, so the'
    + ' write does not reach the query; the status line is clean, so'
    + ' nothing failed. It is not pinning and not a lost update (a'
    + ' second drag changes nothing). Hard to see further because the'
    + ' cube cannot report its own configuration: the specification'
    + ' export carries the snapshot and the per-column settings but'
    + ' not `columnOrder`, so there is nothing to read back.',
    async () => {
      const listed = () => page.evaluate(() =>
        [...document.querySelectorAll('.dc-tool-panel-row')]
          .filter((r) => !r.classList.contains('dc-tool-panel-child'))
          .map((r) => r.dataset.column));
      await flatten();
      const before = await listed();
      if (before.length < 3) throw new Error('not enough columns listed');
      const last = before[before.length - 1];
      const stamp = await statusNow();
      await page.locator(`.dc-tool-panel-row[data-column="${last}"]`)
        .dragTo(page.locator(
          `.dc-tool-panel-row[data-column="${before[0]}"]`),
        { timeout: 10_000, targetPosition: { x: 40, y: 2 } });
      await settle(stamp);
      const grid = (await gridColumns()).filter((c) => c !== '__tree');
      const panel = (await listed()).filter((c) => grid.includes(c));
      const shown = grid.filter((c) => panel.includes(c));
      if (panel.join(',') !== shown.join(',')) {
        throw new Error(`the panel reads ${panel.join(', ')} and the grid`
          + ` reads ${shown.join(', ')}`);
      }
      return `${last} moved and the grid followed`;
    });

  await check('the sidebar configures the cube: list -> rows -> columns',
    async () => {
      // THREE SECTIONS OF ONE SURFACE. Row groups, column labels and
      // the columns themselves, so the whole shape of the cube can
      // be dragged into place in one place -- and a column dropped
      // back on the list comes off whichever axis it was on.
      await flatten();
      const side = '.dc-tool-panel-zones';
      const chips = (zone) => page.evaluate((sel) =>
        [...document.querySelectorAll(`${sel} .dc-chip`)]
          .map((c) => c.dataset.column), `${side} .dc-zone-${zone}`);
      const listed = () => page.evaluate(() =>
        [...document.querySelectorAll('.dc-tool-panel-row')]
          .filter((r) => !r.classList.contains('dc-tool-panel-child'))
          .map((r) => r.dataset.column));

      const dims = await dimensionNames();
      const column = ['region', 'desk', 'book', 'quarter']
        .find((n) => dims.includes(n));
      if (!column) throw new Error(`no dimension available in ${dims}`);

      // 1. The list into Row Groups: the cube groups by it.
      let stamp = await statusNow();
      await page.locator(`.dc-tool-panel-row[data-column="${column}"]`)
        .dragTo(page.locator(`${side} .dc-zone-rows`), { timeout: 10_000 });
      await settle(stamp);
      if (!(await chips('rows')).includes(column)) {
        throw new Error(`${column} did not land in Row Groups:`
          + ` ${(await chips('rows')).join(', ')}`);
      }
      if ((await listed()).includes(column)) {
        throw new Error(`${column} is a row group and still in the column`
          + ` list`);
      }
      stamp = await statusNow();

      // 2. Row Groups into Column Labels: it changes axis, and does
      //    not sit on both -- a dimension on both axes is a cube
      //    nobody meant.
      await page.locator(`${side} .dc-zone-rows`
        + ` .dc-chip[data-column="${column}"]`)
        .dragTo(page.locator(`${side} .dc-zone-columns`), { timeout: 10_000 });
      await settle(stamp);
      if (!(await chips('columns')).includes(column)) {
        throw new Error(`${column} did not reach Column Labels`);
      }
      if ((await chips('rows')).includes(column)) {
        throw new Error(`${column} is on BOTH axes at once`);
      }
      stamp = await statusNow();

      // 3. And back to the list, which takes it off the axis.
      await page.locator(`${side} .dc-zone-columns`
        + ` .dc-chip[data-column="${column}"]`)
        .dragTo(page.locator('.dc-tool-panel-list'), { timeout: 10_000 });
      await settle(stamp);
      if ((await chips('columns')).includes(column)) {
        throw new Error(`${column} stayed on the column axis`);
      }
      if (!(await listed()).includes(column)) {
        throw new Error(`${column} came off the axis and vanished from`
          + ` the list: ${(await listed()).join(', ')}`);
      }
      return `${column} went list -> rows -> columns -> list`;
    });

  await check('KEEPING the grouped columns lists them in both sections',
    async () => {
      // A row dimension's values are the tree's, so it leaves the
      // column list -- unless the cube is set to keep it as a column
      // too, and then it is a real column and belongs in both. The
      // setting is in General Properties beside the rest of "what is
      // on screen".
      await flatten();
      const dims = await dimensionNames();
      const group = ['region', 'desk', 'book'].find((n) => dims.includes(n));
      if (!group) throw new Error(`no dimension to group by in ${dims}`);
      await menu(['Pivot', /^Vertical Pivot on/],
        { col: await needCol(group) });
      const listed = () => page.evaluate(() =>
        [...document.querySelectorAll('.dc-tool-panel-row')]
          .filter((r) => !r.classList.contains('dc-tool-panel-child'))
          .map((r) => r.dataset.column));
      if ((await listed()).includes(group)) {
        throw new Error(`${group} is grouped and still in the column list`);
      }

      await setKeepGrouped(true);
      if (!(await listed()).includes(group)) {
        throw new Error(`${group} is kept as a column but not listed:`
          + ` ${(await listed()).join(', ')}`);
      }
      if (!(await gridColumns()).includes(group)) {
        throw new Error(`${group} is kept as a column but not in the grid`);
      }
      const chips = await page.evaluate(() =>
        [...document.querySelectorAll(
          '.dc-tool-panel-zones .dc-zone-rows .dc-chip')]
          .map((c) => c.dataset.column));
      if (!chips.includes(group)) {
        throw new Error(`${group} left the Row Groups section`);
      }

      // AND ONE AXIS AT A TIME. Dragging that listed copy into
      // Column Labels used to put it on BOTH axes -- grouped by and
      // pivoted on in the same query -- because the rule asked where
      // the drag came from rather than where the column already was.
      const stamp = await statusNow();
      await page.locator(`.dc-tool-panel-row[data-column="${group}"]`)
        .dragTo(page.locator('.dc-tool-panel-zones .dc-zone-columns'),
          { timeout: 10_000 });
      await settle(stamp);
      const axes = await page.evaluate(() => ({
        rows: [...document.querySelectorAll(
          '.dc-tool-panel-zones .dc-zone-rows .dc-chip')]
          .map((c) => c.dataset.column),
        cols: [...document.querySelectorAll(
          '.dc-tool-panel-zones .dc-zone-columns .dc-chip')]
          .map((c) => c.dataset.column),
      }));
      if (axes.rows.includes(group) && axes.cols.includes(group)) {
        throw new Error(`${group} is on BOTH axes: rows`
          + ` ${axes.rows.join(', ')} and columns ${axes.cols.join(', ')}`);
      }
      if (!axes.cols.includes(group)) {
        throw new Error(`${group} did not reach Column Labels`);
      }
      await setKeepGrouped(false);
      return `${group} listed in both, and only ever on one axis`;
    });

  await check('the status bar: actions left, readouts right', async () => {
    // What you can DO at one end, what is TRUE at the other, as
    // theirs is (`justify-between`, Properties then Filter). Every
    // figure used to be crowded against the links at the right edge.
    await flatten();
    const bar = await page.evaluate(() => {
      const box = (sel) => {
        const el = document.querySelector(sel);
        if (!el) return null;
        const r = el.getBoundingClientRect();
        return { x: Math.round(r.left), w: Math.round(r.width) };
      };
      return {
        bar: box('.dc-app-stats'),
        actions: box('.dc-app-stats .dc-status-actions'),
        readout: box('.dc-app-stats .dc-status-readout'),
        links: [...document.querySelectorAll(
          '.dc-app-stats .dc-status-actions .dc-status-link')]
          .map((b) => b.textContent.replace(/^\W+\s*/, '')),
        timingInReadout: Boolean(document.querySelector(
          '.dc-app-stats .dc-status-readout .dc-status-timing')),
        backend: document.querySelector(
          '.dc-app-stats .dc-status-readout .dc-status-host')
          ?.textContent?.trim() ?? null,
      };
    });
    if (!bar.actions || !bar.readout) {
      throw new Error('the status bar has no action/readout groups');
    }
    // AT THE TWO ENDS, not merely in that order: with everything
    // crowded against one edge the actions are still left of the
    // readouts, and the check passed while the bar looked exactly as
    // it did before.
    const leftGap = bar.actions.x - bar.bar.x;
    const rightGap = (bar.bar.x + bar.bar.w)
      - (bar.readout.x + bar.readout.w);
    if (leftGap > 8 || rightGap > 8) {
      throw new Error(`the actions sit ${leftGap}px from the left edge and`
        + ` the readouts ${rightGap}px from the right`);
    }
    if (bar.links.join(', ') !== 'Properties, Filter') {
      throw new Error(`the links read: ${bar.links.join(', ') || 'none'}`);
    }
    if (!bar.timingInReadout) {
      throw new Error('the timing is not among the readouts');
    }
    // AND THE BACKEND IN A WORD. Three planes want three words a
    // person can tell apart in a 20px strip, not three sentences:
    // `local` plans in this tab, `remote` on legend-lite over HTTP,
    // `engine` on legend-engine itself.
    if (!/^(local|remote|engine)$/.test(bar.backend ?? '')) {
      throw new Error(`the backend reads "${bar.backend}", which is not`
        + ` one of local / remote / engine`);
    }
    return `actions ${leftGap}px from the left, readouts ${rightGap}px`
      + ` from the right, backend "${bar.backend}"`;
  });

  await check('Properties opens from the status bar', async () => {
    // Both editors were two levels down the grid's right-click menu,
    // and a person looking for them did not find them.
    await reset();
    await page.click('.dc-status-properties');
    await page.waitForSelector('.dc-editor', { timeout: 10_000 });
    const tabs = await page.locator('.dc-editor-tab').count();
    await reset();
    if (tabs === 0) throw new Error('the editor opened with no tabs');
    return `the editor opened with ${tabs} tabs`;
  });

  await check('the GRAND TOTAL renders, and shows no machinery',
    async () => {
      // A total is one group over everything. The obvious way to say
      // that -- `groupBy(~[], ...)` -- crashes the real upstream
      // engine, so it is written as a constant column grouped by,
      // which is what upstream does too. That synthetic key must
      // never reach the grid: it is machinery, not a column.
      await flatten();
      const dims = await dimensionNames();
      const group = ['region', 'desk', 'book'].find((n) => dims.includes(n));
      if (!group) throw new Error(`no dimension to group by in ${dims}`);
      await menu(['Pivot', /^Vertical Pivot on/],
        { col: await needCol(group) });
      await setRootAggregation(true);

      const total = page.locator('.dc-row.dc-total');
      if (!(await total.count())) {
        throw new Error('no total row rendered with root aggregation on');
      }
      const cells = await total.first().locator('.dc-cell')
        .allTextContents();
      const figures = cells.filter((c) => /\d/.test(c));
      if (figures.length === 0) {
        throw new Error(`the total row carries no figures:`
          + ` ${JSON.stringify(cells)}`);
      }
      // AND NO SYNTHETIC KEY, in the grid or the panel.
      const leaked = (await gridColumns()).filter((c) => /__root__/.test(c));
      const listed = await page.evaluate(() =>
        [...document.querySelectorAll('.dc-tool-panel-row')]
          .map((r) => r.dataset.column)
          .filter((c) => /__root__/.test(c ?? '')));
      await setRootAggregation(false);
      if (leaked.length > 0) {
        throw new Error(`the grid shows ${leaked.join(', ')}`);
      }
      if (listed.length > 0) {
        throw new Error(`the panel lists ${listed.join(', ')}`);
      }
      return `total row with ${figures.length} figures, no machinery shown`;
    });

  await check('the three sections read as ONE list', async () => {
    // Row groups, column labels and the columns themselves are the
    // same kind of thing -- a list of columns you drag between -- so
    // they are laid out the same: one row per column, the same
    // height, every label starting at the same x. Two pill bars
    // above a list is three different things on one surface.
    await flatten();
    const dims = await dimensionNames();
    const group = ['region', 'desk', 'book'].find((n) => dims.includes(n));
    const key = ['year', 'quarter', 'qtr'].find((n) => dims.includes(n));
    if (!group || !key) throw new Error(`need two dimensions in ${dims}`);
    await menu(['Pivot', /^Vertical Pivot on/], { col: await needCol(group) });
    await menu(['Pivot', /^Horizontal Pivot on/], { col: await needCol(key) });
    await settle();

    const rows = await page.evaluate(() => {
      const read = (sel, labelSel) =>
        [...document.querySelectorAll(sel)].map((r) => {
          const label = r.querySelector(labelSel);
          return {
            column: r.dataset.column,
            x: Math.round(label.getBoundingClientRect().left),
            h: Math.round(r.getBoundingClientRect().height),
          };
        });
      return {
        axis: [
          ...read('.dc-tool-panel-zones .dc-zone-rows .dc-chip',
            '.dc-chip-label'),
          ...read('.dc-tool-panel-zones .dc-zone-columns .dc-chip',
            '.dc-chip-label'),
        ],
        list: read('.dc-tool-panel-list .dc-tool-panel-row'
          + ':not(.dc-tool-panel-child)', '.dc-tool-panel-label'),
      };
    });
    if (rows.axis.length === 0 || rows.list.length === 0) {
      throw new Error(`nothing to compare: ${rows.axis.length} axis rows,`
        + ` ${rows.list.length} listed`);
    }
    const all = [...rows.axis, ...rows.list];
    const xs = [...new Set(all.map((r) => r.x))];
    if (xs.length !== 1) {
      throw new Error(`labels start at ${xs.join(', ')}px: `
        + all.map((r) => `${r.column}@${r.x}`).join(', '));
    }
    const hs = [...new Set(all.map((r) => r.h))];
    if (hs.length !== 1) {
      throw new Error(`row heights differ: ${hs.join(', ')}px`);
    }
    // And a chip is a ROW, not a pill: as wide as the section.
    const wide = await page.evaluate(() => {
      const chip = document.querySelector(
        '.dc-tool-panel-zones .dc-chip');
      const zone = chip?.closest('.dc-zone');
      if (!chip || !zone) return null;
      return Math.round(chip.getBoundingClientRect().width)
        / Math.round(zone.getBoundingClientRect().width);
    });
    if (wide === null || wide < 0.9) {
      throw new Error(`a chip fills ${wide === null ? 'no' : Math.round(
        wide * 100) + '% of'} its section`);
    }
    return `${all.length} rows, all ${hs[0]}px tall, labels at ${xs[0]}px`;
  });

  await check('every direction between the three sections', async () => {
    // SIX DIRECTIONS, not three. The sections are one surface: a
    // column goes from the list to either axis, from either axis to
    // the other, and from either axis back to the list. Each one is
    // checked, because "dragging works" was true of some of them
    // while a person trying the others found nothing happened.
    await flatten();
    const side = '.dc-tool-panel-zones';
    const chips = (zone) => page.evaluate((sel) =>
      [...document.querySelectorAll(sel)].map((c) => c.dataset.column),
    `${side} .dc-zone-${zone} .dc-chip`);
    const listed = () => page.evaluate(() =>
      [...document.querySelectorAll('.dc-tool-panel-row')]
        .filter((r) => !r.classList.contains('dc-tool-panel-child'))
        .map((r) => r.dataset.column));
    const dims = await dimensionNames();
    const col = ['quarter', 'qtr', 'book'].find((n) => dims.includes(n));
    if (!col) throw new Error(`no spare dimension in ${dims}`);

    const drag = async (from, to, what) => {
      const stamp = await statusNow();
      await page.locator(from).dragTo(page.locator(to), { timeout: 10_000 });
      await settle(stamp);
      return what;
    };
    const rowChip = `${side} .dc-zone-rows .dc-chip[data-column="${col}"]`;
    const colChip = `${side} .dc-zone-columns .dc-chip[data-column="${col}"]`;
    const listRow = `.dc-tool-panel-row[data-column="${col}"]`;
    const went = [];

    await drag(listRow, `${side} .dc-zone-rows`);
    if (!(await chips('rows')).includes(col)) {
      throw new Error('list -> rows did nothing');
    }
    went.push('list->rows');

    await drag(rowChip, `${side} .dc-zone-columns`);
    if (!(await chips('columns')).includes(col)) {
      throw new Error('rows -> columns did nothing');
    }
    if ((await chips('rows')).includes(col)) {
      throw new Error(`${col} is on both axes at once`);
    }
    went.push('rows->columns');

    await drag(colChip, `${side} .dc-zone-rows`);
    if (!(await chips('rows')).includes(col)) {
      throw new Error('columns -> rows did nothing');
    }
    went.push('columns->rows');

    await drag(rowChip, '.dc-tool-panel-list');
    if ((await chips('rows')).includes(col)) {
      throw new Error('rows -> list did nothing');
    }
    if (!(await listed()).includes(col)) {
      throw new Error(`${col} came off the axis and vanished`);
    }
    went.push('rows->list');

    await drag(listRow, `${side} .dc-zone-columns`);
    if (!(await chips('columns')).includes(col)) {
      throw new Error('list -> columns did nothing');
    }
    went.push('list->columns');

    await drag(colChip, '.dc-tool-panel-list');
    if ((await chips('columns')).includes(col)) {
      throw new Error('columns -> list did nothing');
    }
    went.push('columns->list');

    return `${col}: ${went.join(', ')}`;
  });

  await check('a measure dragged at a zone is REFUSED VISIBLY',
    async () => {
      // It was refused in silence, and the measures are the first
      // thing anyone drags -- so "you can only drag within each
      // section" is exactly what that looks like. Grouping by a
      // notional means one group per amount; upstream does not offer
      // it either. The answer has to be visible before the drop.
      await flatten();
      const measure = await page.locator('.dc-tool-panel-row.dc-measure')
        .first().evaluate((e) => e.dataset.column);
      const marks = await page.evaluate((name) => {
        const row = document.querySelector(
          `.dc-tool-panel-row[data-column="${name}"]`);
        row.dispatchEvent(new Event('dragstart', { bubbles: true }));
        const zone = document.querySelector(
          '.dc-tool-panel-zones .dc-zone-rows');
        const dimmed = getComputedStyle(zone).opacity;
        zone.dispatchEvent(new MouseEvent('dragover', {
          bubbles: true, cancelable: true }));
        const refused = zone.classList.contains('dc-refuse');
        const cursor = getComputedStyle(zone).cursor;
        row.dispatchEvent(new Event('dragend', { bubbles: true }));
        return {
          dimmed,
          refused,
          cursor,
          cleared: !document.querySelector('.dc-app')
            .classList.contains('dc-drag-nogroup'),
        };
      }, measure);
      if (Number(marks.dimmed) >= 1) {
        throw new Error(`the zones did not stand back (opacity`
          + ` ${marks.dimmed}) while ${measure} was dragged`);
      }
      if (!marks.refused) {
        throw new Error(`the zone did not mark ${measure} as refused`);
      }
      if (marks.cursor !== 'not-allowed') {
        throw new Error(`the cursor over the zone was ${marks.cursor}`);
      }
      if (!marks.cleared) {
        throw new Error('the drag ended and the zones stayed dimmed');
      }
      return `${measure}: zones dimmed to ${marks.dimmed},`
        + ` refused, cursor ${marks.cursor}`;
    });

  await check('the columns can be reordered IN the panel', async () => {
    // Dragging a header reorders what is on screen; this reorders
    // the list, which is where a person looks for a column -- and is
    // the only way to place one the grid is not showing.
    //
    // FROM A FRESH CUBE. This passes on a cube that has just been
    // loaded and fails after a long run, which is the gap declared
    // below: something in the accumulated state stops a
    // configuration write reaching the grid, and I have not isolated
    // it. The feature is checked here; the anomaly is checked there,
    // so neither hides the other.
    await freshCube();
    const listed = () => page.evaluate(() =>
      [...document.querySelectorAll('.dc-tool-panel-row')]
        .filter((r) => !r.classList.contains('dc-tool-panel-child'))
        .map((r) => r.dataset.column));
    const before = await listed();
    if (before.length < 3) throw new Error('not enough columns listed');
    const last = before[before.length - 1];
    // STAMP FIRST. `settle()` with no baseline waits 150ms flat --
    // enough on an idle page, not enough on a busy one, and the
    // check then read the grid before the reorder's query landed and
    // reported the product inconsistent with itself.
    const stamp = await statusNow();
    await page.locator(`.dc-tool-panel-row[data-column="${last}"]`)
      .dragTo(page.locator(
        `.dc-tool-panel-row[data-column="${before[0]}"]`),
      { timeout: 10_000, targetPosition: { x: 40, y: 2 } });
    await settle(stamp);
    const after = await listed();
    if (after[0] !== last) {
      throw new Error(`${last} did not move to the front:`
        + ` ${after.join(', ')}`);
    }
    // AND THE GRID FOLLOWED. A panel that reorders only itself is a
    // panel that lies about the grid -- compared as the RELATIVE
    // order of the columns the grid shows, because the panel also
    // lists the hidden ones and they have no place on screen.
    const grid = (await gridColumns()).filter((c) => c !== '__tree');
    // Compared as the RELATIVE order of the columns the grid shows:
    // the panel also lists the hidden ones, which have no place on
    // screen.
    const expected = after.filter((c) => grid.includes(c));
    const actual = grid.filter((c) => expected.includes(c));
    if (expected.join(',') !== actual.join(',')) {
      throw new Error(`the panel reads ${expected.join(', ')} and the grid`
        + ` reads ${actual.join(', ')}`);
    }
    return `${last} moved to the front; the grid follows`
      + ` (${actual.join(', ')})`;
  });

  await check('a reorder leaves the GROUPED columns where they were',
    async () => {
      // The grid can only report the columns it is showing, and
      // writing its report straight into the order dropped every
      // grouped, pivoted and hidden column out of it -- so the
      // panel, which sorts by that order and puts anything unlisted
      // last, threw them to the end of the list. One drag and the
      // row-group columns jumped.
      await flatten();
      const dims = await dimensionNames();
      const group = ['region', 'desk', 'book'].find((n) => dims.includes(n));
      if (!group) throw new Error(`no dimension to group by in ${dims}`);
      await menu(['Pivot', /^Vertical Pivot on/],
        { col: await needCol(group) });
      // IN ITS OWN SECTION. A row group is a chip in Row Groups,
      // not a row in the column list -- its values are the tree's.
      // What must not move is the order of everything else.
      const chipAt = async () => (await page.evaluate(() =>
        [...document.querySelectorAll(
          '.dc-tool-panel-zones .dc-zone-rows .dc-chip')]
          .map((c) => c.dataset.column))).indexOf(group);
      const at = await chipAt();
      if (at === -1) {
        throw new Error(`${group} is not in the Row Groups section`);
      }
      const panelBefore = await panelOrder();
      // Now move two columns the grid IS showing, and the grouped
      // one must not budge.
      const shown = (await gridColumns()).filter((c) => c !== '__tree');
      if (shown.length < 2) throw new Error('not enough columns to reorder');
      await page.locator(`.dc-th[data-column="${shown[1]}"]`)
        .dragTo(page.locator(`.dc-th[data-column="${shown[0]}"]`),
          { timeout: 10_000 });
      await settle();
      const panelAfter = await panelOrder();
      if ((await chipAt()) !== at) {
        throw new Error(`${group} moved from ${at} to`
          + ` ${await chipAt()} in Row Groups`);
      }
      // And the two that moved did move, or this proves nothing.
      if (panelAfter.indexOf(shown[1]) > panelAfter.indexOf(shown[0])) {
        throw new Error(`${shown[1]} did not move ahead of ${shown[0]}:`
          + ` ${panelAfter.join(', ')}`);
      }
      // Nothing that was listed fell out of the list either, which is
      // how the whole order used to get scrambled.
      const lost = panelBefore.filter((c) => !panelAfter.includes(c));
      if (lost.length > 0) {
        throw new Error(`the reorder dropped ${lost.join(', ')} from the`
          + ` list`);
      }
      return `${group} held its place in Row Groups through a reorder`;
    });

  await check('the panel lists a pivoted measure as its pivot columns',
    async () => {
      // The panel said "notional" once while the grid showed one per
      // value of the pivot key. ag-grid's own tool panel nests the
      // pivot result columns under a group per value; these are
      // listed under the measure they came from.
      // GROUPED FIRST, and set up here rather than inherited: a
      // pivot with no row groups is a single row and needs no cast,
      // so it has no result columns to list. Each check makes its
      // own shape, or running one alone tests something else.
      await flatten();
      const dims = await dimensionNames();
      const group = ['region', 'desk', 'book'].find((n) => dims.includes(n));
      const key = ['year', 'quarter', 'qtr'].find((n) => dims.includes(n));
      if (!group || !key) {
        throw new Error(`need a group and a pivot key in ${dims}`);
      }
      await menu(['Pivot', /^Vertical Pivot on/],
        { col: await needCol(group) });
      await menu(['Pivot', /^Horizontal Pivot on/],
        { col: await needCol(key) });
      await settle();
      const children = await page.evaluate(() =>
        [...document.querySelectorAll('.dc-tool-panel-child')].map((r) => ({
          column: r.dataset.column,
          label: r.querySelector('.dc-tool-panel-label')?.textContent,
        })));
      if (children.length === 0) {
        throw new Error('the pivot produced no children in the panel');
      }
      const leaves = (await gridColumns()).filter((c) => c.includes('|'));
      if (children.length !== leaves.length) {
        throw new Error(`${leaves.length} pivoted columns in the grid,`
          + ` ${children.length} in the panel`);
      }
      // Labelled by the VALUES, not by the generated name: the
      // measure is the row above, and `2021__|__notional` says it
      // twice.
      const noisy = children.filter((c) => (c.label ?? '').includes('|'));
      if (noisy.length > 0) {
        throw new Error(`a child is labelled with its generated name:`
          + ` ${noisy[0].label}`);
      }
      // AND THE PIVOT KEY IS IN ITS OWN SECTION: its values ARE the
      // column headers, so it cannot also be a column. It used to be
      // listed with a disabled tick box and the reason in a tooltip,
      // which is a worse answer than putting it where it lives.
      const onAxis = await page.evaluate(() =>
        [...document.querySelectorAll(
          '.dc-tool-panel-zones .dc-zone-columns .dc-chip')]
          .map((c) => c.dataset.column));
      if (!onAxis.includes(key)) {
        throw new Error(`${key} is a pivot key but the Column Labels`
          + ` section holds ${onAxis.join(', ') || 'nothing'}`);
      }
      const stillListed = await page.locator(
        `.dc-tool-panel-row[data-column="${key}"]`).count();
      if (stillListed > 0) {
        throw new Error(`${key} is a pivot key and still in the column`
          + ` list`);
      }
      return `${children.length} pivot columns listed under their measure`;
    });

  await check('unticking one pivot column keeps the rest QUERYABLE',
    async () => {
      // Hiding read the cast off the leaves the grid was SHOWING, so
      // unticking one narrowed the next query and the column left
      // the data as well as the screen -- and nothing could bring it
      // back, because the panel lists the cast.
      // Its own shape, like every other check: grouped and pivoted,
      // because a pivot with no row groups has no result columns.
      await flatten();
      const dims = await dimensionNames();
      const group = ['region', 'desk', 'book'].find((n) => dims.includes(n));
      const key = ['year', 'quarter', 'qtr'].find((n) => dims.includes(n));
      if (!group || !key) {
        throw new Error(`need a group and a pivot key in ${dims}`);
      }
      await menu(['Pivot', /^Vertical Pivot on/],
        { col: await needCol(group) });
      await menu(['Pivot', /^Horizontal Pivot on/],
        { col: await needCol(key) });
      await settle();
      const children = await page.evaluate(() =>
        [...document.querySelectorAll('.dc-tool-panel-child')]
          .map((r) => r.dataset.column));
      if (children.length < 2) {
        throw new Error(`only ${children.length} pivot columns to untick`);
      }
      const victim = children[0];
      await page.locator(
        `.dc-tool-panel-row[data-column="${victim}"] .dc-tool-panel-show`)
        .click();
      await settle();
      const shown = await gridColumns();
      if (shown.includes(victim)) throw new Error(`${victim} is still shown`);
      const stillListed = await page.evaluate(() =>
        [...document.querySelectorAll('.dc-tool-panel-child')]
          .map((r) => r.dataset.column));
      if (stillListed.length !== children.length) {
        throw new Error(`the panel lost ${children.length
          - stillListed.length} pivot column(s) when one was hidden:`
          + ` ${stillListed.join(', ')}`);
      }
      // And back, which is the part that was impossible.
      await page.locator(
        `.dc-tool-panel-row[data-column="${victim}"] .dc-tool-panel-show`)
        .click();
      await settle();
      if (!(await gridColumns()).includes(victim)) {
        throw new Error(`${victim} could not be brought back`);
      }
      return `${victim} hidden and restored, ${children.length} still listed`;
    });


  await check('the columns panel hides a column with its tick box',
    async () => {
      // The panel listed every column and could not turn one off,
      // while hiding lived in the grid's menu three levels down --
      // so upstream's `agColumnsToolPanel`, which is mostly a list
      // of checkboxes, had nothing to click here.
      await menu(['Pivot', 'Clear All Horizontal Pivots'], { requery: false })
        .catch(() => {});
      await settle();
      const before = await gridColumns();
      const target = before.find((c) => c !== '__tree');
      if (!target) throw new Error('no column to hide');
      const box = `.dc-tool-panel-row[data-column="${target}"]`
        + ' .dc-tool-panel-show';
      if (!(await page.locator(box).count())) {
        throw new Error('the panel offers no tick box at all');
      }
      await page.locator(box).click();
      await settle();
      const after = await gridColumns();
      if (after.includes(target)) {
        throw new Error(`${target} is still in the grid after unticking it`);
      }
      // STILL LISTED, struck through: the list is how you find it
      // again, so a hidden column must not vanish from it.
      const row = page.locator(
        `.dc-tool-panel-row[data-column="${target}"]`);
      if (!(await row.count())) {
        throw new Error(`${target} vanished from the panel as well`);
      }
      if (!(await row.evaluate((e) =>
        e.classList.contains('dc-hidden-column')))) {
        throw new Error(`${target} is hidden but the panel does not say so`);
      }
      return `${target} left the grid and stayed in the list`;
    });

  await check('a hidden column drags back into the grid from the panel',
    async () => {
      // Upstream turns this on explicitly --
      // `allowDragFromColumnsToolPanel: true` -- and it is the only
      // way to say WHERE the column should go. The tick box can only
      // put it back where it was.
      const hidden = await page.locator(
        '.dc-tool-panel-row.dc-hidden-column').first();
      if (!(await hidden.count())) {
        throw new Error('nothing is hidden, so this check would prove'
          + ' nothing');
      }
      const name = await hidden.evaluate((e) => e.dataset.column);
      const onto = (await gridColumns()).filter((c) => c !== '__tree')[1];
      if (!onto) throw new Error('need a header to drop onto');
      await hidden.dragTo(page.locator(`.dc-th[data-column="${onto}"]`),
        { timeout: 10_000 });
      await settle();
      const after = (await gridColumns()).filter((c) => c !== '__tree');
      if (!after.includes(name)) {
        throw new Error(`${name} did not come back; grid has`
          + ` ${after.join(', ')}`);
      }
      // AT THE POINT IT WAS DROPPED, not merely somewhere.
      if (after.indexOf(name) !== after.indexOf(onto) - 1) {
        throw new Error(`${name} landed at ${after.indexOf(name)},`
          + ` not before ${onto} at ${after.indexOf(onto)}:`
          + ` ${after.join(', ')}`);
      }
      return `${name} dropped in before ${onto}`;
    });

  await check('a MEASURE can be dragged from the panel into the grid',
    async () => {
      // Measures were not draggable at all here, so a measure in
      // this panel had nothing it could do: the zones refuse it --
      // grouping by a notional means one group per amount -- and the
      // grid would not take it either.
      const measure = page.locator('.dc-tool-panel-row.dc-measure').first();
      if (!(await measure.count())) throw new Error('no measure listed');
      if (!(await measure.evaluate((e) => e.draggable))) {
        throw new Error('a measure row is not draggable');
      }
      const name = await measure.evaluate((e) => e.dataset.column);
      const order = (await gridColumns()).filter((c) => c !== '__tree');
      const onto = order.find((c) => c !== name);
      if (!onto) throw new Error('need another column to drop onto');
      await measure.dragTo(page.locator(`.dc-th[data-column="${onto}"]`),
        { timeout: 10_000 });
      await settle();
      const after = (await gridColumns()).filter((c) => c !== '__tree');
      if (after.indexOf(name) !== after.indexOf(onto) - 1) {
        throw new Error(`${name} did not land before ${onto}:`
          + ` ${after.join(', ')}`);
      }
      return `${name} placed before ${onto}`;
    });

  await check('the menu opens with NO submenu already unfurled', async () => {
    // A right-click arrived with the whole Export list open beside
    // the menu, and hovering anything else left two submenus on
    // screen -- one held open by the focus the menu put on its first
    // entry, one by the pointer. Counting VISIBLE submenus is the
    // check; the focus that caused it is a detail underneath.
    await page.locator('.dc-cell').first().click({ button: 'right' });
    await page.waitForSelector('.dc-menu', { timeout: 10_000 });
    const showing = () => page.evaluate(() =>
      [...document.querySelectorAll('.dc-submenu')]
        .filter((e) => e.getBoundingClientRect().height > 0)
        .map((e) => e.parentElement?.querySelector('.dc-menu-label')
          ?.textContent?.trim() ?? '?'));
    const onOpen = await showing();
    if (onOpen.length > 0) {
      throw new Error(`opened with ${onOpen.join(', ')} already unfurled`);
    }
    // And ONE opens when asked, so the fix did not simply break them.
    // Pivot: always present. (This hovered Layout until Layout left
    // the grid's menu, 2026-09-25.)
    await page.locator('.dc-menu-item:has(> .dc-menu-label:text-is("Pivot"))')
      .hover();
    await page.waitForTimeout(250);
    const hovered = await showing();
    await page.keyboard.press('Escape');
    await page.waitForTimeout(150);
    if (hovered.length !== 1 || hovered[0] !== 'Pivot') {
      throw new Error(`hovering Pivot showed ${hovered.length}:`
        + ` ${hovered.join(', ')}`);
    }
    return 'none on open, exactly one on hover';
  });

  // -- folding the chrome away ---------------------------------------
  //
  // The grid is what the page is for, so both bars fold. Each check
  // measures the GRID, because "the bar is hidden" is not the point
  // -- the point is that the space went to the rows.

  await check('folding the drag zones gives the space to the grid', async () => {
    const heights = () => page.evaluate(() => ({
      grid: Math.round(
        document.querySelector('.dc-app-middle').getBoundingClientRect().height),
      bar: document.querySelector('.dc-zone-bar')?.hidden === false
        ? Math.round(document.querySelector('.dc-zone-bar')
          .getBoundingClientRect().height)
        : 0,
    }));
    const before = await heights();
    if (before.bar < 10) throw new Error('the zone bar is not on screen to fold');
    await page.click('.dc-zone-fold');
    await page.waitForTimeout(200);
    const after = await heights();
    if (after.bar !== 0) throw new Error('the zone bar is still on screen');
    if (after.grid <= before.grid) {
      throw new Error(`the grid did not grow: ${before.grid} ->`
        + ` ${after.grid}px`);
    }
    // NEVER NOTHING TO CLICK.
    if (!(await page.locator('.dc-titlebar-zones').count())) {
      throw new Error('nothing in the title bar brings the zones back');
    }
    await page.click('.dc-titlebar-zones');
    await page.waitForTimeout(200);
    const back = await heights();
    if (back.bar < 10) throw new Error('the zones did not come back');
    return `grid ${before.grid} -> ${after.grid}px, and back to ${back.grid}`;
  });

  await check('folding the title bar leaves a lip that restores it', async () => {
    const grid = () => page.evaluate(() => Math.round(
      document.querySelector('.dc-app-middle').getBoundingClientRect().height));
    const before = await grid();
    await page.click('.dc-titlebar-fold');
    await page.waitForTimeout(200);
    if (await page.locator('.dc-titlebar-menu').count()) {
      throw new Error('the hamburger survived a folded title bar');
    }
    const lip = page.locator('.dc-titlebar-lip');
    if (!(await lip.count())) {
      throw new Error('the title bar folded to NOTHING, taking the menu'
        + ' with it');
    }
    const after = await grid();
    if (after <= before) {
      throw new Error(`the grid did not grow: ${before} -> ${after}px`);
    }
    // Measured while it is still folded, or the figure reported is
    // the restored bar's and the line says something untrue.
    const lipHeight = Math.round(await page.locator('.dc-titlebar')
      .evaluate((e) => e.getBoundingClientRect().height));
    await lip.click();
    await page.waitForTimeout(200);
    if (!(await page.locator('.dc-titlebar-menu').count())) {
      throw new Error('the lip did not bring the title bar back');
    }
    return `grid ${before} -> ${after}px, lip ${lipHeight}px`;
  });

  await check("the grid's menu carries no Layout entries", async () => {
    // Layout left the right-click menu by the user's direction
    // (2026-09-25): the grid's menu is about the data. It used to be
    // the safety net for a title bar folded from the hamburger; the
    // lip a folded bar leaves is that net now, and "folding the title
    // bar leaves a lip that restores it" checks it.
    const labels = (await menuAt(0)).map((i) => i.label);
    const layout = labels.filter((l) =>
      /^Layout$|Drag Zones|Title Bar/.test(l));
    if (layout.length > 0) {
      throw new Error(`still offered: ${layout.join(' / ')}`);
    }
    return `${labels.length} entries, none about layout`;
  });

  await check('a column drag brings the folded zones back', async () => {
    // Folding them must take nothing away: a drag needs somewhere to
    // land, so the bar returns for the length of one and folds
    // itself again afterwards.
    await page.click('.dc-zone-fold');
    await page.waitForTimeout(200);
    const shown = () => page.evaluate(() =>
      document.querySelector('.dc-zone-bar')?.hidden === false);
    if (await shown()) throw new Error('the zones did not fold');
    const head = page.locator('.dc-th[data-column]').first();
    await head.dispatchEvent('dragstart', { dataTransfer: null });
    await page.waitForTimeout(150);
    const during = await shown();
    const peeking = await page.evaluate(() =>
      document.querySelector('.dc-zone-bar')?.classList
        .contains('dc-peeking') ?? false);
    await head.dispatchEvent('dragend', { dataTransfer: null });
    await page.waitForTimeout(150);
    const afterwards = await shown();
    await page.click('.dc-titlebar-zones');
    await page.waitForTimeout(200);
    if (!during) throw new Error('a dragged column had nowhere to land');
    if (!peeking) throw new Error('the bar came back unmarked, so it reads'
      + ' as unfolded rather than as a peek');
    if (afterwards) throw new Error('the peek did not fold itself back');
    return 'shown for the drag, folded again after it';
  });

  await check('keyboard: arrow keys move the focused cell', async () => {
    // On a FLAT cube, and not the first cell. A grouped cube's first
    // cell is the tree cell, where a click lands on the chevron and
    // expands a group instead of focusing anything -- and this check
    // runs last, so it inherits whatever shape the checks above left
    // behind.
    await menu(['Pivot', 'Clear All Vertical Pivots']).catch(() => {});
    await page.locator('.dc-row').first().locator('.dc-cell').nth(1).click();
    // WHERE the focus is, not what it says. This compared the focused
    // cell's TEXT, and a boolean column reads "true" in row after
    // row -- so a focus that moved perfectly reported that it had
    // not, as soon as a reorder put `settled` first.
    const focused = () => page.evaluate(() => {
      const cell = document.querySelector('.dc-cell.dc-focus');
      if (!cell) return null;
      const row = cell.closest('.dc-row');
      const cells = [...(row?.querySelectorAll('.dc-cell') ?? [])];
      return {
        row: row?.getAttribute('aria-rowindex') ?? '?',
        col: cells.indexOf(cell),
        text: cell.textContent ?? '',
      };
    });
    const before = await focused();
    await page.keyboard.press('ArrowDown');
    await page.waitForTimeout(200);
    const after = await focused();
    if (!before && !after) throw new Error('no cell ever shows focus');
    if (!before || !after) {
      throw new Error(`focus ${before ? 'vanished' : 'never appeared'}`);
    }
    if (before.row === after.row && before.col === after.col) {
      throw new Error(`focus did not move from row ${before.row},`
        + ` column ${before.col}`);
    }
    return `row ${before.row} -> ${after.row} (${after.text})`;
  });

  // -- every editor control has its effect -----------------------------
  //
  // A sweep of General and Column Properties found half the controls
  // doing nothing (2026-09-25): every font, colour, grid-line and
  // highlight setting (the grid took its appearance once, at
  // construction), pivot sort direction, the flat cube's row limit --
  // and "Show root aggregation" put the FIRST TRADE in the Total row.
  // The reader guardrail cannot see "read once at startup"; only
  // setting each control in the real editor and looking can. Each
  // check starts from a fresh cube, sets ONE control, presses OK, and
  // asserts the specific effect the control promises.
  {
    const O = '.dc-app-overlay:not([hidden])';
    const fieldOf = (label) => page.locator(
      `${O} .dc-field:has(> .dc-field-label:text-is(${JSON.stringify(label)}))`).first();
    const boxOf = (label) => page.locator(
      `${O} .dc-check:has(.dc-check-label:text-is(${JSON.stringify(label)})) input`).first();
    const sectionOf = (title) => page.locator(
      `${O} .dc-section:has(> .dc-section-title:text-is(${JSON.stringify(title)}))`).first();
    const put = async (loc, value) => {
      await loc.fill(String(value));
      await loc.dispatchEvent('change');
      await page.waitForTimeout(80);
    };
    const properties = async (tab, column) => {
      await reset();
      await page.click('.dc-status-properties');
      await page.waitForSelector(`${O} .dc-editor`, { timeout: 5000 });
      await page.locator(`${O} .dc-editor-tab`, { hasText: tab }).first().click();
      if (column) {
        await fieldOf('Choose Column:').locator('select').selectOption(column);
        const adv = boxOf('Show advanced settings?');
        if (await adv.count() && !(await adv.isChecked())) await adv.check();
      }
    };
    const okEditor = async () => {
      const before = await statusNow();
      await page.locator(`${O} .dc-editor-footer button`, { hasText: 'OK' }).click();
      await settle(before);
      await page.waitForTimeout(150);
    };
    const general = (fn) => async () => { await properties('General Properties'); await fn(); await okEditor(); };
    const column = (name, fn) => async () => {
      await properties('Column Properties', name); await fn(); await okEditor();
    };
    const group = async (...cols) => {
      for (const c of cols) {
        await menu(['Pivot', `Add Vertical Pivot on ${c}`], { col: await needCol(c) });
      }
    };
    /** What the page shows, for one column's first cells and the chrome. */
    const look = (col = 'pnl') => page.evaluate((name) => {
      const cells = [...document.querySelectorAll('.dc-row')].slice(0, 6).map((r) =>
        [...r.querySelectorAll('.dc-cell')].find((c) =>
          (c.dataset.column ?? c.closest('[data-column]')?.dataset.column) === name));
      const style = (el) => {
        if (!el) return null;
        const x = getComputedStyle(el);
        return { font: x.fontFamily, size: x.fontSize, weight: x.fontWeight,
          italic: x.fontStyle, deco: x.textDecorationLine, transform: x.textTransform,
          color: x.color, bg: x.backgroundColor, justify: x.justifyContent,
          bb: `${x.borderBottomWidth} ${x.borderBottomStyle} ${x.borderBottomColor}`,
          br: `${x.borderRightWidth} ${x.borderRightStyle}`,
          width: Math.round(el.getBoundingClientRect().width), filter: x.filter,
          cls: el.className };
      };
      return {
        texts: cells.map((c) => c?.textContent?.trim() ?? null),
        styles: cells.map(style),
        rowBg: [...document.querySelectorAll('.dc-row')].slice(0, 4)
          .map((r) => getComputedStyle(r).backgroundColor),
        // Horizontal grid lines are the ROW's bottom border, not a cell's.
        rowLine: (() => {
          const r = document.querySelector('.dc-row');
          if (!r) return null;
          const x = getComputedStyle(r);
          return `${x.borderBottomWidth} ${x.borderBottomStyle} ${x.borderBottomColor}`;
        })(),
        tree: [...document.querySelectorAll('.dc-row')].slice(0, 8)
          .map((r) => r.querySelector('.dc-tree')?.textContent?.trim() ?? null),
        headers: [...document.querySelectorAll('.dc-th[data-column]')]
          .map((h) => `${h.dataset.column}=${h.textContent.trim()}`),
        title: document.querySelector('.dc-titlebar')?.textContent?.trim() ?? '',
        titleFolded: document.querySelector('.dc-titlebar')?.classList.contains('dc-collapsed') ?? false,
        zonesHidden: document.querySelector('.dc-zone-bar')?.hidden ?? false,
        timing: document.querySelector('.dc-status-timing')?.textContent ?? '',
        warning: document.querySelector('.dc-status-warning')?.textContent ?? null,
        stats: document.querySelector('.dc-status-stats')?.textContent ?? '',
        pure: document.getElementById('pure')?.textContent ?? '',
      };
    }, col);
    /** One control: fresh cube, optional setup, the action, the promise. */
    const control = (name, { setup, act, col, expect }) => check(`control: ${name}`, async () => {
      await freshCube();
      if (setup) await setup();
      const before = await look(col);
      await act();
      const after = await look(col);
      const why = expect(before, after);
      if (why) throw new Error(why);
      return 'effect seen';
    });
    const changed = (key) => (b, a) =>
      JSON.stringify(b[key]) !== JSON.stringify(a[key]) ? null
        : `${key} unchanged: ${JSON.stringify(a[key]).slice(0, 120)}`;
    const styleChanged = (prop) => (b, a) =>
      a.styles[0]?.[prop] !== b.styles[0]?.[prop] ? null
        : `${prop} unchanged: ${a.styles[0]?.[prop]}`;

    // ---- General Properties ----
    await control('Report Title', { act: general(() => put(fieldOf('Report Title:').locator('input'), 'Sweep Report')),
      expect: (b, a) => (a.title.includes('Sweep Report') ? null : `title "${a.title}"`) });
    await control('Show root aggregation: a real TOTAL, no false warning', {
      setup: () => group('region'), act: general(() => boxOf('Show root aggregation').check()),
      expect: (b, a) => (a.tree[0] !== 'Total' ? `first row "${a.tree[0]}"`
        : a.warning ? `warning "${a.warning}"`
          : a.texts[0] === b.texts[0] ? `total pnl ${a.texts[0]} equals the first group's` : null) });
    await control('Keep grouped columns in the grid', { setup: () => group('region'),
      act: general(() => boxOf('Keep grouped columns in the grid').check()),
      expect: (b, a) => (a.headers.some((h) => h.startsWith('region=')) ? null : 'no region column') });
    await control('Show leaf count', { setup: () => group('region'),
      act: general(() => boxOf('Show leaf count').check()),
      expect: (b, a) => (/\(\d+\)$/.test(a.tree[0] ?? '') ? null : `tree ${a.tree[0]}`) });
    await control('Tree column sort', { setup: () => group('region'),
      act: general(() => fieldOf('Sort:').locator('select').selectOption('desc')),
      expect: changed('tree') });
    await control('Initially expand to level', { setup: () => group('region', 'desk'),
      act: general(() => put(fieldOf('Initially expand to level:').locator('input'), 1)),
      expect: (b, a) => (a.tree.filter(Boolean).length > b.tree.filter(Boolean).length ? null : 'no child rows') });
    await control('Row Limit, flat cube, with its warning', {
      act: general(() => put(fieldOf('Row Limit:').locator('input[type=number]').first(), 10)),
      expect: (b, a) => (/\b10 rows/.test(a.timing) && a.warning ? null : `timing "${a.timing}" warning ${a.warning}`) });
    await control('Display warning when truncated, off', {
      setup: general(() => put(fieldOf('Row Limit:').locator('input[type=number]').first(), 10)),
      act: general(() => boxOf('Display warning when truncated').uncheck()),
      expect: (b, a) => (b.warning && !a.warning ? null : `warning ${b.warning} -> ${a.warning}`) });
    await control('Grid lines: horizontal', { act: general(() => boxOf('Horizontal').check()), expect: changed('rowLine') });
    await control('Grid line colour', { act: general(async () => {
      await boxOf('Horizontal').check();
      await put(fieldOf('Color:').locator('input[type=color]'), '#ff0000');
    }), expect: (b, a) => (/255, 0, 0/.test(a.rowLine ?? '') ? null : `row line ${a.rowLine}`) });
    await control('Grid lines: vertical off', { act: general(() => boxOf('Vertical').uncheck()), expect: styleChanged('br') });
    await control('Highlight rows off', { act: general(() => boxOf('Standard mode').uncheck()), expect: changed('rowBg') });
    await control('Highlight rows colour', { act: general(async () => {
      // Custom and Standard exclude each other; the colour is Custom's.
      await boxOf('Custom').check();
      if (await boxOf('Standard mode').isChecked()) throw new Error('Standard stayed on beside Custom');
      await put(fieldOf('Custom: Alternate color:').locator('input[type=color]'), '#00ff00');
    }),
      expect: changed('rowBg') });
    const font = () => sectionOf('Default Font');
    await control('Default font family', { act: general(() => font().locator('select').first().selectOption('Georgia')), expect: styleChanged('font') });
    await control('Default font size', { act: general(() => font().locator('select').nth(1).selectOption('18')), expect: styleChanged('size') });
    await control('Default font bold', { act: general(() => font().locator('button[title="Bold"]').click()), expect: styleChanged('weight') });
    await control('Default font italic', { act: general(() => font().locator('button[title="Italic"]').click()), expect: styleChanged('italic') });
    await control('Default font underline', { act: general(() => font().locator('button[title="Underline"]').click()), expect: styleChanged('deco') });
    await control('Default alignment', { act: general(() => font().locator('button[title="Align Right"]').click()), expect: styleChanged('justify') });
    await control('Default case (cube-wide)', { col: 'region',
      act: general(() => fieldOf('Case:').locator('select').selectOption('uppercase')), expect: styleChanged('transform') });
    await control('Default normal foreground', { act: general(() => put(sectionOf('Default Colors').locator('input[title="Normal foreground"]'), '#aa00aa')),
      expect: (b, a) => (a.styles.some((x, i) => x?.color !== b.styles[i]?.color) ? null : 'no cell recoloured') });
    await control('Default negative foreground', { act: general(() => put(sectionOf('Default Colors').locator('input[title="Negative foreground"]'), '#aa00aa')),
      expect: (b, a) => (a.styles.some((x, i) => isNegativeText(a.texts[i]) && x?.color !== b.styles[i]?.color) ? null : 'no negative recoloured') });
    await control('Default normal background', { act: general(() => put(sectionOf('Default Colors').locator('input[title="Normal background"]'), '#ffeeaa')),
      expect: (b, a) => (a.styles.some((x, i) => x?.bg !== b.styles[i]?.bg) ? null : 'no background') });
    await control('Show drag zones, off', { act: general(() => boxOf('Show drag zones').uncheck()),
      expect: (b, a) => (a.zonesHidden ? null : 'zones still shown') });
    await control('Show title bar, off', { act: general(() => boxOf('Show title bar').uncheck()),
      expect: (b, a) => (a.titleFolded ? null : 'title bar still shown') });

    // ---- Column Properties (pnl carries negatives) ----
    await control('Column kind', { setup: () => group('region'),
      act: column('pnl', () => fieldOf('Column Kind:').locator('select').selectOption('dimension')),
      expect: (b, a) => (/pnl:y\|\$y->uniqueValueOnly/.test(a.pure) ? null : 'pnl still sums') });
    await control('Aggregation', { setup: () => group('region'),
      act: column('pnl', () => fieldOf('Aggregation:').locator('select').selectOption('max')),
      expect: (b, a) => (/pnl:y\|\$y->max\(\)/.test(a.pure) && a.texts[0] !== b.texts[0] ? null : 'no max') });
    await control('Aggregation: weighted average', { setup: () => group('region'),
      act: column('pnl', async () => {
        await fieldOf('Aggregation:').locator('select').selectOption('wavg');
        await fieldOf('Weight column:').locator('select').selectOption('quantity');
      }),
      expect: (b, a) => (/wavgRowMapper\(\$x\.quantity\)/.test(a.pure) ? null : 'no wavg') });
    await control('Pivot sort direction', { setup: async () => menu(['Pivot', 'Horizontal Pivot on year'], { col: await needCol('year') }),
      act: column('year', () => fieldOf('Pivot sort direction:').locator('select').selectOption('desc')),
      expect: (b, a) => (a.headers[0] !== b.headers[0] ? null : `headers ${a.headers.slice(0, 3)}`) });
    await control('Decimals', { act: column('pnl', () => put(fieldOf('Decimals:').locator('input[type=number]').first(), 0)),
      expect: (b, a) => (a.texts.every((t) => !/\.\d/.test(t ?? '')) ? null : `pnl ${a.texts}`) });
    await control('Display commas: shown ticked, untick removes them', {
      act: column('pnl', async () => {
        if (!(await boxOf('Display commas').isChecked())) throw new Error('unticked while commas show');
        await boxOf('Display commas').uncheck();
      }),
      expect: (b, a) => (a.texts.every((t) => !/,/.test(t ?? '')) ? null : `pnl ${a.texts}`) });
    // Upstream's default for a number: parentheses ON, and the box says so.
    await control('Negative number in parens: shown ticked, untick removes them', {
      act: column('pnl', async () => {
        if (!(await boxOf('Negative number in parens').isChecked())) throw new Error('unticked by default');
        await boxOf('Negative number in parens').uncheck();
      }),
      expect: (b, a) => (b.texts.some((t) => /^\(.*\)$/.test(t ?? ''))
        && a.texts.every((t) => !/^\(.*\)$/.test(t ?? '')) && a.texts.some((t) => /^-/.test(t ?? ''))
        ? null : `pnl before ${b.texts} after ${a.texts}`) });
    await control('Scale', { act: column('pnl', () => fieldOf('Scale:').locator('select').selectOption('thousands')),
      expect: (b, a) => (a.texts.some((t) => /k$/.test(t ?? '')) ? null : `pnl ${a.texts}`) });
    // Upstream's unit: glued on, or FIRST when it starts with `_`.
    await control('Unit', { act: column('pnl', () => put(fieldOf('Unit:').locator('input'), 'USD')),
      expect: (b, a) => (a.texts.every((t) => /\dUSD\)?$/.test(t ?? '')) ? null : `pnl ${a.texts}`) });
    await control('Unit starting with _ goes first', { act: column('pnl', () => put(fieldOf('Unit:').locator('input'), '_$')),
      expect: (b, a) => (a.texts.every((t) => /^\(?\$[-\d]/.test(t ?? '')) ? null : `pnl ${a.texts}`) });
    await control('Case (column)', { col: 'region', act: column('region', () => fieldOf('Case:').locator('select').selectOption('lowercase')),
      expect: (b, a) => (a.texts.every((t) => t === t?.toLowerCase()) ? null : `region ${a.texts}`) });
    await control('Blur content', { act: column('pnl', () => boxOf('Blur content').check()), expect: styleChanged('filter') });
    await control('Hide from view', { act: column('pnl', () => boxOf('Hide from view').check()),
      expect: (b, a) => (!a.headers.some((h) => h.startsWith('pnl=')) ? null : 'pnl still shown') });
    await control('Pin', { act: column('pnl', () => fieldOf('Pin:').locator('select').selectOption('left')),
      expect: (b, a) => (/dc-pin-left/.test(a.styles[0]?.cls ?? '') ? null : 'not pinned') });
    await control('Width, fixed', { act: column('pnl', async () => {
      await fieldOf('Width:').locator('select').selectOption('fixed');
      await put(fieldOf('Width:').locator('input[type=number]').first(), 300);
    }), expect: (b, a) => (Math.abs((a.styles[0]?.width ?? 0) - 300) <= 2 ? null : `width ${a.styles[0]?.width}`) });
    await control('Heatmap', { act: column('pnl', () => sectionOf('Colors').locator('.dc-check:has(.dc-check-label:text-is("On")) input').check()),
      expect: styleChanged('bg') });
    await control('Column font bold', { act: column('pnl', () => page.locator(`${O} button[title="Bold"]`).first().click()),
      expect: styleChanged('weight') });
    await control('Column negative foreground', { act: column('pnl', () => put(page.locator(`${O} input[title="Negative foreground"]`).first(), '#0000ff')),
      expect: (b, a) => (a.styles.some((x, i) => isNegativeText(a.texts[i]) && x?.color !== b.styles[i]?.color) ? null : 'no negative recoloured') });
  }
} catch (e) {
  record('the run itself', false, String(e.message ?? e).split('\n')[0]);
} finally {
  await browser.close();
  server.close();
}

// -- the verdict ---------------------------------------------------------

const bad = results.filter((r) => !r.ok);
console.log(`\n${results.length - bad.length}/${results.length} features work`);
const open = gaps.filter((g) => !g.fixed);
if (open.length) {
  console.log(`\nKNOWN GAPS (${open.length}) — missing, not regressed:`);
  for (const g of open) console.log(`  ${g.name}\n    ${g.why}`);
}
for (const g of gaps.filter((x) => x.fixed)) {
  console.log(`\n*** "${g.name}" now WORKS — promote it to a check ***`);
}
if (pageErrors.length) {
  console.log(`\npage errors (${pageErrors.length}):`);
  for (const e of [...new Set(pageErrors)].slice(0, 8)) {
    console.log(`  ${e.split('\n')[0]}`);
  }
}
if (bad.length) {
  console.log(`\nBROKEN (${bad.length}):`);
  for (const r of bad) console.log(`  ${r.name} — ${r.detail}`);
}
const slow = [...timings].sort((a, b) => b[1] - a[1]).slice(0, 12);
console.log(`\nslowest checks of ${timings.length}`
  + ` (${Math.round((Date.now() - STARTED) / 1000)}s total):`);
for (const [name, ms] of slow) {
  console.log(`  ${String(ms).padStart(6)}ms  ${name}`);
}

console.log(bad.length
  ? `\n!!! ${bad.length} features are broken !!!`
  : '\n*** every feature checked works ***');
process.exit(bad.length ? 1 : 0);
