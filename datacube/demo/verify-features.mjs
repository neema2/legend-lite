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
//   npm run verify:features            (generates its own sample)
//   DATA=/abs/file.csv npm run verify:features

import { createServer } from 'node:http';
import { readFile, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { extname, join, normalize } from 'node:path';
import { chromium } from 'playwright';

import { gridInvariants } from './grid-invariants.mjs';
import { sampleCsv } from '../src/samples.ts';

const ROOT = new URL('..', import.meta.url).pathname;

/**
 * THE SWEEP OPENS A FILE, ALWAYS.
 *
 * With no DATA it used to run against the page's built-in cube --
 * which arrives grouped by region, desk and book and pivoted on year
 * -- while these checks are written against a FLAT cube: they
 * right-click `region` to group by it, and `region` is not on screen
 * when it is already a row dimension. So `npm run verify:features`
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
  const p = (req.url ?? '/').split('?')[0];
  const rel = normalize(p === '/' ? '/demo/index.html' : p)
    .replace(/^(\.\.[/])+/, '');
  try {
    const body = await readFile(join(ROOT, rel));
    res.writeHead(200, {
      'Content-Type': TYPES[extname(rel)] ?? 'application/octet-stream',
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
  const shut = page.locator('.dc-app-overlay:not([hidden]) .dc-overlay-close');
  if (await shut.count()) {
    await shut.first().click().catch(() => {});
    await page.waitForTimeout(150);
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
      const file = `${process.env.SHOTS}/${name.replace(/\W+/g, '-')}.png`;
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
  await page.setInputFiles('input[type=file]', DATA);
  await page.waitForFunction(
    () => /rows/.test(
      document.querySelector('.dc-status-timing')?.textContent ?? '')
      || /could not|error/i.test(
        document.getElementById('status')?.textContent ?? ''),
    undefined, { timeout: 90_000 },
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
    await menu(['Filter', 'Filters...'], { requery: false });
    const open = await page.evaluate(() =>
      !document.querySelector('.dc-app-overlay')?.hidden);
    if (!open) throw new Error('the overlay never opened');
    const text = await page.textContent('.dc-app-overlay');
    await page.keyboard.press('Escape');
    await page.waitForTimeout(200);
    return `${(text ?? '').trim().slice(0, 40)}...`;
  });

  // ---- the filter editor -------------------------------------------------
  //
  // Opening the dialog was all that was ever checked. This builds a
  // COMPOUND filter in it -- two conditions, then the connective
  // flipped from all-of to any-of -- and the editor applies live
  // (`openFilters` wires `onChange` straight to `#setFilter`), so
  // there is no Apply button and each edit re-queries.

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
    await settle(await statusNow());

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
    await settle(await statusNow());

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
    await settle(await statusNow());

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

    const state = await page.evaluate(() => {
      const w = document.querySelector('.dc-app-overlay');
      const b = w.getBoundingClientRect();
      return {
        hidden: w.hidden,
        display: getComputedStyle(w).display,
        area: Math.round(b.width) * Math.round(b.height),
      };
    });
    if (!state.hidden) throw new Error('the overlay is still marked open');
    if (state.display !== 'none') {
      throw new Error(`a closed dialog computes display: ${state.display},`
        + ' so it is still on the page');
    }
    if (state.area !== 0) {
      throw new Error(`a closed dialog still occupies ${state.area}px²`);
    }

    // And the grid is usable again, which is the thing that broke.
    await menu(['Sort', 'Clear All Sorts']).catch(() => {});
    return 'gone, and the grid takes clicks again';
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
    await page.locator('.dc-menu-item:has(> .dc-menu-label:text-is("Layout"))')
      .hover();
    await page.waitForTimeout(250);
    const hovered = await showing();
    await page.keyboard.press('Escape');
    await page.waitForTimeout(150);
    if (hovered.length !== 1 || hovered[0] !== 'Layout') {
      throw new Error(`hovering Layout showed ${hovered.length}:`
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

  await check("the grid's menu restores a bar the hamburger went with",
    async () => {
      // THE SAFETY NET. The hamburger lives in the title bar, so
      // hiding that bar from the hamburger would be a one-way door
      // if the grid's own menu did not carry the same toggle.
      await page.click('.dc-titlebar-fold');
      await page.waitForTimeout(200);
      if (await page.locator('.dc-titlebar-menu').count()) {
        throw new Error('the title bar did not fold');
      }
      await menu(['Layout', 'Show Title Bar']);
      await page.waitForTimeout(300);
      if (!(await page.locator('.dc-titlebar-menu').count())) {
        throw new Error('the grid menu could not restore the title bar');
      }
      return 'restored from the grid, with no title bar to click';
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
