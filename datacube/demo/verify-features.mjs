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
//   npm run verify:features            (the built-in sample data)
//   DATA=/abs/file.csv npm run verify:features

import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
import { extname, join, normalize } from 'node:path';
import { chromium } from 'playwright';

import { gridInvariants } from './grid-invariants.mjs';

const ROOT = new URL('..', import.meta.url).pathname;
const DATA = process.env.DATA;
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
    record(name, false, String(e.message ?? e).split('\n')[0]);
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
  status: document.getElementById('status')?.textContent ?? '',
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
      (was) => (document.getElementById('status')?.textContent ?? '') !== was,
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

/** The status line, which changes once per completed query. */
const statusNow = () => page.evaluate(() =>
  document.getElementById('status')?.textContent ?? '');

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
  const cell = page.locator('.dc-row').nth(row).locator('.dc-cell').nth(col);
  await cell.click({ button: 'right' });
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
try {
  await page.goto(`${URL_BASE}/demo/index.html`);
  await page.waitForSelector('.dc-row', { timeout: 90_000 });

  if (DATA) {
    await page.setInputFiles('input[type=file]', DATA);
    await page.waitForFunction(
      () => /rows/.test(document.getElementById('status')?.textContent ?? ''),
      undefined, { timeout: 90_000 },
    );
    await settle();
    loaded = DATA.split('/').pop();
  }
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
    const zone = await page.evaluate(() =>
      [...document.querySelectorAll('[class*=zone] [data-column]')]
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

  const gridColumns = () => page.evaluate(() =>
    [...document.querySelectorAll('.dc-th[data-column]')]
      .map((e) => e.dataset.column));

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
        + ` ${after.length}`);
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
    // The titlebar is a safe place to press: inert, and nowhere near
    // the menu.
    await page.locator('.dc-titlebar-title').click();
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

  await check('keyboard: arrow keys move the focused cell', async () => {
    // On a FLAT cube, and not the first cell. A grouped cube's first
    // cell is the tree cell, where a click lands on the chevron and
    // expands a group instead of focusing anything -- and this check
    // runs last, so it inherits whatever shape the checks above left
    // behind.
    await menu(['Pivot', 'Clear All Vertical Pivots']).catch(() => {});
    await page.locator('.dc-row').first().locator('.dc-cell').nth(1).click();
    const focused = () => page.evaluate(() =>
      document.querySelector('.dc-cell.dc-focus')?.textContent ?? '');
    const before = await focused();
    await page.keyboard.press('ArrowDown');
    await page.waitForTimeout(200);
    const after = await focused();
    if (!before && !after) throw new Error('no cell ever shows focus');
    if (before === after) throw new Error(`focus did not move from ${before}`);
    return `${before} -> ${after}`;
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
