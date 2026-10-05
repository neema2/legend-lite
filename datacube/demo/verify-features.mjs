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
//   WAREHOUSE=http://127.0.0.1:8772 [WAREHOUSE_SNAP=1] PORT=8022 bazel run //datacube:verify_features
//                                                  (the sample, LIVE on a warehouse: warehouse-source.mjs)

// first: points Playwright at the Chromium Bazel fetched (as a browser_test; a no-op under bazel run)
import '../../tools/browser/pinned-chromium.mjs';
import { readFile, readdir, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { extname, join } from 'node:path';
import { chromium } from 'playwright';

import { TEMPORAL_TYPES, gridInvariants } from './grid-invariants.mjs';
import {
  closeTyped, compareTyped, isNegative, orderBreak, readColumn, readView, sameTyped, stamp, sumTyped,
} from './typed-view.mjs';
import { isNumeric } from '../../engine-client/src/types.ts';
import { sampleCsv } from '../src/samples.ts';
import { WAREHOUSE, openWarehouseTable } from './warehouse-source.mjs';
import { frames, serve, siteRoot } from './harness.mjs';
import { runfilesFromEnv } from '../../tools/js/runfiles.mts';

const ROOT = siteRoot();

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
 * Settled: nothing in flight and nothing changed for this long (`settle`). An action that
 * changes nothing costs this, not the 4 seconds the status-line wait fell through after.
 * Correctness does not rest on it: every check asserts its own outcome.
 */
const QUIET_MS = 150;
/** A page that never settles is still read, as before, after this long. */
const SETTLE_TIMEOUT_MS = 20_000;
/**
 * A check may not exceed this.
 *
 * A hang used to be invisible -- it just made the sweep slower, and
 * "slower" is not a signal anyone acts on. Now it fails, with the
 * name of the check that did it.
 */
const CHECK_DEADLINE_MS = 30_000;

// THE QUERY STORE, as legend-lite answers it: the fixture records ARE its answers
// (fixtures/saved-queries/README.md), so this needs no server of its own -- CI runs it. The records are the ones the
// BUILD file names (SAVED_QUERIES), never found beside this file.
const storedQueries = async () => Promise.all(runfilesFromEnv('SAVED_QUERIES').filter((f) => f.endsWith('.json'))
  .map(async (f) => JSON.parse(await readFile(f, 'utf8'))));
const queryStore = async (req, res) => {
  const json = (status, body) => {
    res.writeHead(status, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
    res.end(JSON.stringify(body));
  };
  const all = await storedQueries();
  if (req.method === 'POST' && req.url === '/api/pure/v1/query/search') {
    let raw = '';
    for await (const chunk of req) raw += chunk;
    const term = (JSON.parse(raw || '{}').searchTermSpecification?.searchTerm ?? '').toLowerCase();
    return json(200, all.filter((q) => q.name.toLowerCase().includes(term)));
  }
  const id = decodeURIComponent(req.url.slice('/api/pure/v1/query/'.length));
  const found = all.find((q) => q.id === id);
  return found ? json(200, found) : json(404, { message: `no query ${id}` });
};

// PORT: a fixed one, for a warehouse that must allow this page's origin (warehouse-source.mjs)
const { port, close: closeServer } = await serve(ROOT, {
  port: Number(process.env.PORT ?? 0),
  route: async (req, res) => {
    if (!req.url.startsWith('/api/pure/v1/query')) return false;
    await queryStore(req, res);
    return true;
  },
});
const URL_BASE = `http://127.0.0.1:${port}`;

const browser = await chromium.launch();
const context = await browser.newContext({
  viewport: { width: 1400, height: 900 },
  permissions: ['clipboard-read', 'clipboard-write'],
});
const page = await context.newPage();
// PLANNER=remote|engine: the same harness on another planner (the one page's ?planner=).
// NO_WASM=1: the in-tab planner's files are not there at all -- every request for them is refused,
// and asking for one at all is a failure (a drop-in replacement on a server planner must never need
// them).
const PLANNER = process.env.PLANNER ?? '';
const wasmAsked = [];
if (process.env.NO_WASM) {
  await context.route(/\/(classes\.wasm|wasm-gc-module-runtime\.js|planner-worker\.js)(\?|$)/, (route) => {
    wasmAsked.push(route.request().url());
    return route.abort();
  });
}
// CPU_THROTTLE=4: the page's CPU slowed that many times (Chrome's own emulation), to see here
// what a slower machine -- CI's runner -- sees: a wait that guesses passes fast and fails slow.
if (process.env.CPU_THROTTLE) {
  const cdp = await context.newCDPSession(page);
  await cdp.send('Emulation.setCPUThrottlingRate', { rate: Number(process.env.CPU_THROTTLE) });
}
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
    await settle();
  }
  for (let i = 0; i < 3; i += 1) {
    const open = await page.locator('.dc-menu, .dc-app-overlay:not([hidden])')
      .count();
    if (!open) return;
    await page.keyboard.press('Escape').catch(() => {});
    await settle();
  }
  // Escape did not clear it; click somewhere inert.
  await page.mouse.click(5, 5).catch(() => {});
  await settle();
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
  const broken = await page.evaluate(gridInvariants, TEMPORAL_TYPES);
  for (const b of broken) {
    record(`INVARIANT after ${where}`, false, b);
  }
  return broken.length === 0;
}

// SHARDED BY SECTION under `bazel test` (Bazel workplan P4-04): shard i of n runs every n-th section, each from a
// fresh cube, so one section can fail without hiding another; the checks before the first section run in every shard.
const SHARDS = Number(process.env.TEST_TOTAL_SHARDS ?? 1);
const SHARD = Number(process.env.TEST_SHARD_INDEX ?? 0);
if (process.env.TEST_SHARD_STATUS_FILE) await writeFile(process.env.TEST_SHARD_STATUS_FILE, '');
let sectionIndex = -1;
let sectionOn = true;
async function section(name) {
  sectionIndex += 1;
  sectionOn = sectionIndex % SHARDS === SHARD;
  if (sectionOn && SHARDS > 1) {
    console.log(`\n-- section ${sectionIndex}: ${name}`);
    await freshCube();
  }
}

async function check(name, fn) {
  if (!sectionOn) return;
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
  //
  // NOW BY THE PAGE'S OWN SIGNAL (demo/boot.ts `__dataCubeSignal`, 2026-09-29), not the status
  // line: settled is nothing in flight (the cube's `busy`, the Pure pane's print) and nothing
  // changed for QUIET_MS. Waiting for the status line to change fell through after 4s for every
  // change that runs no query -- a colour, a width, a pin -- which was most of this harness's
  // 8 minutes; and the 150ms sleep with no `before` read the page before a slower machine had
  // re-queried (CI's Linux runner: double-click grouping, the undo checks). Still no dialog wait.
  // `before` stays for the callers; the signal needs no text to compare.
  void before;
  await page.evaluate(() => { window.__settleWatch = undefined; });
  await page.waitForFunction((quiet) => {
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
    return now - w.since >= quiet;
  }, QUIET_MS, { timeout: SETTLE_TIMEOUT_MS }).catch(() => {});
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
  await settle();
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
      await settle();
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
/**
 * Pick a hamburger entry by its own label, through its submenus: an entry under View or Insert is
 * shown only once its parent is hovered (and a parent's text holds its children's labels too).
 */
async function pickEntry(label) {
  const item = page.locator(`.dc-menu-item:has(> .dc-menu-label:text-is(${JSON.stringify(label)}))`).first();
  if (!(await item.isVisible())) {
    const parents = await item.evaluate((el) => {
      const out = [];
      for (let p = el.parentElement?.closest('.dc-menu-item'); p; p = p.parentElement?.closest('.dc-menu-item')) {
        out.unshift(p.querySelector(':scope > .dc-menu-label')?.textContent ?? '');
      }
      return out;
    });
    for (const parent of parents) {
      await page.locator(`.dc-menu-item:has(> .dc-menu-label:text-is(${JSON.stringify(parent)}))`).first().hover();
    }
  }
  await item.click();
}
async function burger(label) {
  await page.click('.dc-titlebar-menu');
  await page.waitForSelector('.dc-menu', { timeout: 5000 });
  await pickEntry(label);
  await settle();
}

// -- load ---------------------------------------------------------------

let loaded = 'the built-in demo cube';
let heatCol = 1;
/**
 * The column a check groups by: the first TEXT column on screen, by the compiler's type
 * (`region` in the sample). Set once the file is open; never a position taken on trust.
 */
let GROUP_COL = -1;

/**
 * THE SAMPLE'S TYPES, as the compiler must give them (the generated sample: DuckDB sniffs the
 * CSV, the model is written from its catalog, the compiler types the relation). Asserted
 * before any check, so every choice below by NAME rests on a type that was checked, and a
 * sniffer or compiler change fails here, by name, instead of as fifty puzzling checks.
 */
const SAMPLE_TYPES = {
  trade_id: 'Integer', trade_date: 'StrictDate', booked_at: 'DateTime', region: 'String',
  desk: 'String', book: 'String', year: 'Integer', quarter: 'String', notional: 'Float',
  pnl: 'Float', quantity: 'Integer', settled: 'Boolean',
};

/**
 * The pivot the view is showing, as the cube planned it: the columns it makes (cells, then
 * totals) with the measure each aggregates. Read from the view, not from the Pure text -- a
 * pivot is two plain queries (values, then conditional aggregates), with no `pivot(` in it.
 */
const pivotOf = () => page.evaluate(() => {
  const p = window.__dataCube.view?.pivot;
  return p ? p.columns.map((c) => ({
    name: c.name, measure: c.measure.name, total: c.tuple === null, tuple: c.tuple,
  })) : [];
});

/** The on-screen header names, in grid order. */
const headerNames = () => page.evaluate(() =>
  [...document.querySelectorAll('.dc-th[data-column]')].map((e) => e.dataset.column));
/**
 * A cube in a known state: the page loaded, the file opened.
 *
 * The preamble's own work, as a function, so a check that must not
 * inherit fifty other checks' configuration can ask for a clean one.
 * Used sparingly -- the point of one long run is that each feature
 * meets the state the others leave.
 */
let pageLoaded = false;
/** A file opened in place of the cube, as a person does: Data… opens the source picker (src/ui/source-picker.ts). */
async function openThroughPicker(file) {
  // a check that hid the title bar took the menu with it: a person turns it back on first
  if (!(await page.locator('.dc-titlebar-menu').isVisible())) {
    await page.evaluate(() => window.__dataCube.change((s) => ({ ...s, configuration: { ...s.configuration, showTitleBar: true } })));
    await page.locator('.dc-titlebar-menu').waitFor({ timeout: 10_000 });
  }
  // in place of the page: New ▸ Blank Page, then "Add a data source"
  await page.click('.dc-titlebar-menu');
  await page.locator('.dc-menu .dc-menu-item', { has: page.locator(':scope > .dc-menu-label:text-is("New")') }).hover();
  await page.locator('.dc-menu .dc-menu-item', { has: page.locator(':scope > .dc-menu-label:text-is("Blank Page")') }).click();
  await page.locator('.dc-blank').waitFor({ timeout: 10_000 });
  await page.click('.dc-blank .dc-primary');
  await page.locator('.dc-picker').waitFor({ timeout: 10_000 });
  await page.locator('.dc-picker-tab[data-section="files"]').click();
  await page.setInputFiles('.dc-picker-file', file);
}
/** The harness's source: its file, or the same sample as a warehouse's table, live there. */
const openSource = () => (WAREHOUSE ? openWarehouseTable(page) : openThroughPicker(DATA));
async function freshCube() {
  // AFTER THE FIRST, A FRESH CUBE, NOT A FRESH PAGE: the file opened again, as a person would,
  // gives a new cube with the default configuration -- what a check that asks for a clean one
  // needs -- without restarting DuckDB-WASM and the planner and regenerating the demo's 200,000
  // rows. The reload cost ~2s, and the 44 editor-control checks each asked for one.
  if (pageLoaded && DATA) {
    await reset();
    await page.evaluate(() => { window.__freshFrom = window.__dataCube; });
    // opening over a cube with unsaved changes asks first; Playwright's default would decline
    const accept = (d) => { if (/unsaved changes/.test(d.message())) void d.accept(); };
    page.on('dialog', accept);
    try {
      await openSource();
      // a NEW cube on the page: the status line may read the same as before, so not that
      await page.waitForFunction(
        () => (window.__dataCube !== window.__freshFrom
          && document.querySelectorAll('.dc-row').length > 0)
          || /could not|error/i.test(document.getElementById('status')?.textContent ?? ''),
        undefined, { timeout: 90_000 },
      );
    } finally {
      page.off('dialog', accept);
    }
    await settle();
    return;
  }
  await page.goto(`${URL_BASE}/demo/index.html?queryStore=${encodeURIComponent(`${URL_BASE}/api`)}${PLANNER ? `&planner=${PLANNER}` : ''}`);
  await page.waitForSelector('.dc-row', { timeout: 90_000 });
  pageLoaded = true;
  if (!DATA) return;
  // THE FILE'S answer, not the built-in cube's. The built-in cube's
  // status line already reads "… rows …", so waiting for /rows/ alone
  // passed before the file had loaded -- and on a slow run the checks
  // read a grid mid-swap: "0 rows, 0 headers", the whole sweep red for
  // a page that was fine (2026-09-25). Wait for the line to CHANGE,
  // and for rows to be on screen.
  const before = await statusNow();
  await openSource();
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
  if (DATA) loaded = WAREHOUSE ? `${WAREHOUSE.object}, ${WAREHOUSE.snap ? 'snapped from' : 'live on'} ${WAREHOUSE.url}` : DATA.split('/').pop();
  const start = await state();
  console.log(`\nloaded ${loaded}: ${start.rows.length} rows,`
    + ` ${start.headers.length} headers\n`);
  if (!start.rows.length) throw new Error('nothing rendered at all');
  await invariants('loading the data');

  await check('the source is typed by the compiler, as the sample declares', async () => {
    const columns = await page.evaluate(() =>
      window.__dataCube.snapshot.columns.map((c) => ({ name: c.name, type: c.type ?? null })));
    const shown = columns.map((c) => `${c.name}:${c.type}`).join(', ');
    const untyped = columns.filter((c) => c.type === null);
    if (untyped.length) throw new Error(`untyped: ${untyped.map((c) => c.name).join(', ')}`);
    if (!process.env.DATA) {
      const wrong = Object.entries(SAMPLE_TYPES)
        .filter(([n, t]) => columns.find((c) => c.name === n)?.type !== t)
        .map(([n, t]) => `${n} is not ${t}`);
      if (wrong.length || columns.length !== Object.keys(SAMPLE_TYPES).length) {
        throw new Error(`${wrong.join('; ') || 'the column set differs'} (${shown})`);
      }
    }
    const view = await readView(page);
    GROUP_COL = (await headerNames()).findIndex((n) =>
      view.find((c) => c.name === n)?.type === 'String');
    if (GROUP_COL < 0) throw new Error(`no text column on screen to group by (${shown})`);
    return shown;
  });

  // ---- reading the data ---------------------------------------------
  await section('reading the data');

  await check('grid renders rows and headers', async () => {
    const s = await state();
    if (!s.headers.length) throw new Error('no column headers');
    if (!s.rows.length) throw new Error('no rows');
    const empty = s.rows[0].filter((c) => c === '').length;
    if (empty === s.rows[0].length) throw new Error('first row is all blank');
    return `${s.rows.length} rows x ${s.headers.length} headers`;
  });

  // ---- sorting --------------------------------------------------------
  await section('sorting');

  await check('sort ascending', async () => {
    await menu(['Sort', 'Ascending']);
    const s = await state();
    if (!/sort\(/.test(s.pure)) throw new Error('no sort in the Pure');
    if (!/ORDER BY/i.test(s.sql)) throw new Error('no ORDER BY in the SQL');
    // the VALUES in the database's order, by the column's compiler type -- not the rendered
    // text re-sorted, which misjudges "1,234", "(5.00)" and "Mar 01, 2024"
    const c = await readColumn(page, (await headerNames())[0]);
    const at = orderBreak(c.values, c.type, 'asc');
    if (at >= 0) {
      throw new Error(`${c.name} (${c.type}) is not ascending at row ${at}:`
        + ` ${c.values.slice(Math.max(0, at - 1), at + 1).map(String).join(', ')}`);
    }
    return `${c.name} (${c.type}): ${c.values.slice(0, 3).map(String).join(', ')}...`;
  });

  await check('sort descending', async () => {
    await menu(['Sort', 'Descending']);
    const s = await state();
    if (!/desc/i.test(s.sql)) throw new Error('no DESC in the SQL');
    const c = await readColumn(page, (await headerNames())[0]);
    const at = orderBreak(c.values, c.type, 'desc');
    if (at >= 0) {
      throw new Error(`${c.name} (${c.type}) is not descending at row ${at}:`
        + ` ${c.values.slice(Math.max(0, at - 1), at + 1).map(String).join(', ')}`);
    }
    return `${c.name} (${c.type}): ${c.values.slice(0, 3).map(String).join(', ')}...`;
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

  await check('an Alt+click on a header sorts: up, down, off', async () => {
    // Upstream turns on ag-grid's column selection, which makes a plain
    // header click SELECT and sorting "require holding down the Alt
    // key". Multi-sort is always on; the header shows the arrow. Three
    // Alt+clicks walk a column through ascending, descending and out.
    await menu(['Sort', 'Clear All Sorts']).catch(() => {});
    const name = await page.evaluate(() =>
      document.querySelector('.dc-th.dc-sortable[data-column]')?.dataset.column);
    if (!name) throw new Error('no sortable header');
    const th = page.locator(`.dc-th[data-column="${name}"]`);
    const click = async () => {
      const before = await statusNow();
      await th.click({ position: { x: 8, y: 8 }, modifiers: ['Alt'] });
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

  await check('a header click selects its column; Shift+click extends; no sort', async () => {
    await reset();
    const heads = await page.evaluate(() =>
      [...document.querySelectorAll('.dc-th.dc-sortable[data-column]')].map((e) => e.dataset.column));
    const [a, b] = heads;
    if (!a || !b) throw new Error(`need two headers, have ${heads}`);
    const before = (await state()).pure;
    await page.locator(`.dc-th[data-column="${a}"]`).click({ position: { x: 8, y: 8 } });
    await page.locator(`.dc-th[data-column="${b}"]`).click({ position: { x: 8, y: 8 }, modifiers: ['Shift'] });
    await settle();
    const marked = await page.evaluate(() =>
      [...document.querySelectorAll('.dc-th.dc-th-selected')].map((e) => e.dataset.column));
    const selectedCells = await page.locator('.dc-cell.dc-selected').count();
    const rendered = await page.locator('.dc-row').count();
    const after = (await state()).pure;
    await page.keyboard.press('Escape');
    if (after !== before) throw new Error('a plain header click changed the query (sorted?)');
    if (marked.join() !== [a, b].join()) throw new Error(`headers marked ${marked}`);
    if (selectedCells < rendered * 2) {
      throw new Error(`${selectedCells} cells selected over ${rendered} rendered rows in two columns`);
    }
    return `${a}..${b} selected: ${selectedCells} cells, headers highlighted`;
  });

  await check('columns fit their content after each fetch', async () => {
    // Upstream's autoSizeAllColumns after every fetch: a column of
    // short values comes out narrow, not at the 300px default.
    await reset();
    const widths = await page.evaluate(() =>
      [...document.querySelectorAll('.dc-th[data-column]')].map((e) =>
        [e.dataset.column, Math.round(e.getBoundingClientRect().width)]));
    const narrow = widths.filter(([, w]) => w < 200);
    if (narrow.length === 0) throw new Error(`every column is wide: ${JSON.stringify(widths)}`);
    return `${narrow.length} of ${widths.length} fitted under 200px (e.g. ${narrow[0].join(' ')}px)`;
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
    await settle();
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
    await settle();
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
  await section('filtering');

  await check('add filter from a cell', async () => {
    // the clicked cell's VALUE: equal text is not equal values (a Float rounded to two
    // places, a timestamp shown to the minute)
    const name = (await headerNames())[0];
    const wanted = (await readColumn(page, name)).values[0];
    // The entry carries the clicked value in its own label.
    await menu(['Filter', /^Add Filter: \S+ = /]);
    const s = await state();
    if (!/filter\(/.test(s.pure)) throw new Error('no filter in the Pure');
    if (!/WHERE/i.test(s.sql)) throw new Error('no WHERE in the SQL');
    const c = await readColumn(page, name);
    const off = c.values.filter((v) => !sameTyped(v, wanted, c.type));
    if (off.length || c.values.length === 0) {
      throw new Error(`${off.length} of ${c.values.length} rows do not match the filter`
        + ` (wanted ${stamp(wanted)}, got ${stamp(off.slice(0, 3))})`);
    }
    return `${name} = ${String(wanted)} (${c.type}) -> ${c.values.length} rows`;
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
  await section('the filter editor');
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
      await settle();
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
    await settle();
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
    await settle();
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
    await settle();
    if ((await page.locator('.dc-filter-column').count()) < 2) {
      throw new Error('"+" did not add a second condition');
    }
    const second = ['desk', 'book', 'region'].find(
      (n) => names.includes(n) && n !== first);
    await page.locator('.dc-filter-column').nth(1).selectOption(second);
    await settle();
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
  await section('grouping and pivots');

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
    // (`levelLambda(snapshot, { level: 1, parent: [] })`), which groups
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

  await check('the deepest group opens onto its rows, as many as its count says',
    async () => {
      // Upstream drops the groupBy at the last level and returns the
      // group's own rows, filtered to its keys. An OPENED group's "(n)"
      // -- the count, on by default, of the rows directly beneath it --
      // is the promise those rows keep.
      await menu(['Pivot', 'Clear All Vertical Pivots']).catch(() => {});
      const dims = await dimensionNames();
      const on = ['desk', 'book', 'region'].find((n) => dims.includes(n));
      if (!on) throw new Error(`no dimension to group by in ${dims}`);
      await menu(['Pivot', /^Vertical Pivot on/], { col: await needCol(on) });
      const total = () => page.evaluate(() =>
        Number(document.querySelector('[aria-rowcount]')?.getAttribute('aria-rowcount')));
      const group = page.locator('.dc-row[aria-expanded=false]').first();
      const closed = (await group.locator('.dc-cell').first().innerText()).trim();
      if (/\(\d+\+?\)$/.test(closed)) throw new Error(`a CLOSED group shows a count: "${closed}"`);
      const before = await total();
      const settled = await statusNow();
      await group.locator('.dc-chevron').click();
      await settle(settled);
      const opened = (await total()) - before;
      const label = (await page.locator('.dc-row[aria-expanded=true]').first()
        .locator('.dc-cell').first().innerText()).trim();
      const n = Number(/\((\d+)\+?\)$/.exec(label)?.[1]);
      if (!Number.isFinite(n)) throw new Error(`no count on the opened "${label}"`);
      const firstDetail = await page.locator('.dc-row[aria-expanded=true] + .dc-row')
        .first().locator('.dc-cell').first().innerText();
      // Shut again, and LEAVE THE CUBE GROUPED: the next check clears
      // the grouping and needs one to clear.
      const shut = await statusNow();
      await page.locator('.dc-row[aria-expanded=true] .dc-chevron').first().click();
      await settle(shut);
      // The count is what came: every row under the cap, else the cap's
      // worth with a "+".
      const want = n;
      if (opened !== want) {
        throw new Error(`"${label}" opened onto ${opened} rows, its count says ${n}`);
      }
      if (firstDetail.trim() !== '') {
        throw new Error(`a detail row names "${firstDetail}" in the tree column`);
      }
      return `${label} opened onto ${opened} detail rows`;
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
    if ((await pivotOf()).length === 0) throw new Error('the view makes no pivot columns');
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
    // THE MECHANISM: a pivot is two plain queries since Leg A -- its values, then one
    // conditional aggregate per value under the row groupBy -- so the groups survive by
    // construction. What is checked is the view's own pivot: columns it made, per measure.
    if ((await pivotOf()).filter((c) => !c.total).length === 0) {
      throw new Error('the view makes no pivot columns');
    }
    if (!/->groupBy\(~\[/.test(s.pure)) throw new Error(`no row groupBy: ${s.pure.slice(0, 160)}`);

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
    // (the key pivoted ACROSS is spread over the top, so it is not a column)
    const missing = ['trade_id', 'quarter', 'settled']
      .filter((n) => dims.includes(n) && n !== across && !shown.includes(n));
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
    await settle();

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
      // the first row's VALUES, by column: a figure parsed out of "(1,234.00)" loses its sign
      const firstRow = async () => new Map((await readView(page))
        .map((c) => [c.name, { value: c.values[0] ?? null, type: c.type }]));

      await menu(['Pivot', 'Clear All Vertical Pivots']).catch(() => {});
      await menu(['Pivot', 'Clear All Horizontal Pivots']).catch(() => {});
      const dims = await dimensionNames();
      const groupBy = ['region', 'desk', 'book'].find((n) => dims.includes(n));
      const across = ['year', 'quarter'].find((n) => dims.includes(n));
      if (!groupBy || !across) throw new Error(`need dimensions, have ${dims}`);
      await menu(['Pivot', /^Vertical Pivot on/], { col: await needCol(groupBy) });
      const want = (await firstRow()).get('notional');
      if (!want || want.value === null) throw new Error('no grouped notional to compare');

      await menu(['Pivot', /^Horizontal Pivot on/], { col: await needCol(across) });
      await settle();
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
      const got = row.get(total);
      if (!got || !closeTyped(got.value, want.value, got.type)) {
        throw new Error(`total ${String(got?.value)} is not the unpivoted ${String(want.value)}`);
      }
      const cells = sumTyped([...row].filter(([c]) => c.endsWith('__|__notional') && c !== total)
        .map(([, v]) => v.value), got.type);
      if (!closeTyped(cells, got.value, got.type)) {
        throw new Error(`the pivot's cells add to ${String(cells)}, the total says ${String(got.value)}`);
      }
      const s = await state();
      if (/Error|refus/i.test(s.status)) throw new Error(`status: ${s.status}`);
      return `${groupBy} x ${across}: total ${String(got.value)} = unpivoted ${String(want.value)} (${got.type})`;
    });

  await check('a measure kept out of the pivot shows its real figure', async () => {
    // A pivot groups by everything it selects, so a measure carried
    // THROUGH it and summed afterwards adds up distinct values, not
    // rows -- and before that it was aggregated as `unique`, which is
    // blank for any group of two or more. Kept out of the pivot, its
    // figure is the unpivoted cube's, exactly.
    // the first row's VALUE: pnl carries negatives, which a digit strip of "(1,234.00)"
    // turned positive on both sides of the comparison
    const cell = async (name) => {
      const c = await readColumn(page, name);
      return { value: c.values[0] ?? null, type: c.type };
    };
    await menu(['Pivot', 'Clear All Vertical Pivots']).catch(() => {});
    await menu(['Pivot', 'Clear All Horizontal Pivots']).catch(() => {});
    const dims = await dimensionNames();
    const groupBy = ['region', 'desk', 'book'].find((n) => dims.includes(n));
    const across = ['year', 'quarter'].find((n) => dims.includes(n));
    await menu(['Pivot', /^Vertical Pivot on/], { col: await needCol(groupBy) });
    const want = await cell('pnl');
    if (want.value === null) throw new Error('no grouped pnl to compare');
    await menu(['Pivot', /^Horizontal Pivot on/], { col: await needCol(across) });
    await settle();
    const pivotedPnl = await page.evaluate(() =>
      [...document.querySelectorAll('.dc-th[data-column]')]
        .map((e) => e.dataset.column)
        .find((n) => /__\|__pnl$/.test(n) && !n.startsWith('__pivot_total__')));
    if (!pivotedPnl) throw new Error('pnl was not pivoted to begin with');
    await menu(['Pivot', /^Exclude Column pnl from Horizontal Pivot/],
      { col: await needCol(pivotedPnl) });
    await settle();
    const got = await cell('pnl');
    if (!closeTyped(got.value, want.value, got.type)) {
      throw new Error(`pnl kept out of the pivot reads ${String(got.value)};`
        + ` the unpivoted cube says ${String(want.value)}`);
    }
    return `${groupBy} x ${across}, pnl excluded: ${String(got.value)} = ${String(want.value)} (${got.type})`;
  });

  await check('clear all horizontal pivots', async () => {
    if ((await pivotOf()).length === 0) {
      throw new Error('could not set up: nothing is pivoted');
    }
    await menu(['Pivot', 'Clear All Horizontal Pivots']);
    if ((await pivotOf()).length > 0) throw new Error('pivot survived the clear');
    return 'pivot gone';
  });

  // ---- columns ---------------------------------------------------------
  await section('columns');

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
    await settle();
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
    // A NUMERIC column, by the compiler's type: a heatmap over text or
    // dates has nothing to scale, so aiming at one would prove nothing.
    const view = await readView(page);
    const measure = (await headerNames()).findIndex((n) =>
      isNumeric(view.find((c) => c.name === n)?.type));
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
  await section('the columns panel');

  await check('columns panel lists and searches', async () => {
    const all = await page.locator('.dc-tool-panel-row').count();
    if (!all) throw new Error('the panel lists nothing');
    await page.fill('.dc-tool-panel-search', 'zzzz');
    await settle();
    const none = await page.locator('.dc-tool-panel-row').count();
    if (none !== 0) throw new Error(`search matched ${none} for "zzzz"`);
    await page.fill('.dc-tool-panel-search', '');
    await settle();
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
  await section('export and clipboard');

  // WHAT EACH FILE IS, not only that one arrived (2026-09-29: the old check passed a .xls that
  // was not a workbook, a PDF a strict reader refused, and every format carrying columns the
  // grid did not show). Each file is parsed and its headers compared with the grid's own.
  /** The leaf headers the grid shows, in order, as text (sort marks left out). */
  const gridLabels = () => page.evaluate(() =>
    [...document.querySelectorAll('.dc-th[data-column]')].map((th) => {
      const c = th.cloneNode(true);
      c.querySelectorAll('.dc-sort-mark, .dc-col-resize').forEach((e) => e.remove());
      return (c.textContent ?? '').trim();
    }));
  /** Well-formed XML, by the browser's own parser. */
  const wellFormed = (text) => page.evaluate((t) =>
    new DOMParser().parseFromString(t, 'application/xml').getElementsByTagName('parsererror').length === 0, text);
  const csvHeader = (text) => text.replace(/^\ufeff/, '').split('\r\n')[0].split(',')
    .map((h) => h.replace(/^"|"$/g, '').replace(/""/g, '"'));
  const pdfConsistent = (bytes) => {
    // one byte, one character: a PDF's offsets are bytes (latin1 maps each to itself)
    const text = Buffer.from(bytes).toString('latin1');
    const xrefAt = Number(/startxref\n(\d+)\n%%EOF/.exec(text)?.[1]);
    if (text.slice(xrefAt, xrefAt + 4) !== 'xref') return 'startxref does not point at the xref table';
    const offsets = [...text.slice(xrefAt).matchAll(/^(\d{10}) 00000 n $/gm)].map((m) => Number(m[1]));
    const bad = offsets.findIndex((off, i) => !text.startsWith(`${i + 1} 0 obj`, off));
    if (bad >= 0) return `xref entry ${bad + 1} misses its object`;
    for (const m of text.matchAll(/<< \/Length (\d+) >>\nstream\n/g)) {
      const at = (m.index ?? 0) + m[0].length + Number(m[1]);
      if (text.slice(at, at + 10) !== '\nendstream') return 'a stream /Length is not its byte count';
    }
    return null;
  };
  const { unzipSync, strFromU8 } = await import('fflate');

  for (const [label, ext] of [['CSV (Grid)', 'csv'], ['Excel (Grid)', 'xlsx'],
    ['HTML', 'html'], ['Plain Text', 'txt'], ['PDF', 'pdf'],
    ['Cube File (JSON)', 'json']]) {
    await check(`export ${label}`, async () => {
      const labels = await gridLabels();
      const wait = page.waitForEvent('download', { timeout: 15_000 });
      await menu(['Export', label], { requery: false });
      // Upstream's attestation first, for anything carrying rows.
      if (ext !== 'json') await answerExport('Accept');
      const dl = await wait;
      const body = await readFile(await dl.path());
      if (!body.length) throw new Error('the file is empty');
      const name = dl.suggestedFilename();
      if (!name.endsWith(`.${ext}`)) throw new Error(`downloaded ${name}, expected .${ext}`);
      const text = body.toString('utf8');
      const same = (got, what) => {
        if (JSON.stringify(got) !== JSON.stringify(labels)) {
          throw new Error(`${what} ${JSON.stringify(got)} are not the grid's ${JSON.stringify(labels)}`);
        }
      };
      switch (ext) {
        case 'json':
          // the cube file is the cube's definition, never its rows
          if (!/"kind":"datacube\.(cube|page)"/.test(text)) throw new Error('the cube file is not a saved cube');
          break;
        case 'csv':
          same(csvHeader(text), 'the CSV headers');
          break;
        case 'xlsx': {
          const zip = unzipSync(new Uint8Array(body));
          const sheet = zip['xl/worksheets/sheet1.xml'];
          if (!sheet || !zip['[Content_Types].xml']) throw new Error('not an OOXML workbook');
          for (const [part, bytes] of Object.entries(zip)) {
            if (part.endsWith('.xml') && !(await wellFormed(strFromU8(bytes)))) throw new Error(`${part} is not well-formed`);
          }
          const row1 = /<row r="1">(.*?)<\/row>/s.exec(strFromU8(sheet))?.[1] ?? '';
          same([...row1.matchAll(/<t xml:space="preserve">([^<]*)<\/t>/g)].map((m) => m[1]), 'the workbook headers');
          break;
        }
        case 'html':
          for (const l of labels) if (!text.includes(`>${l}</th>`)) throw new Error(`the page has no header "${l}"`);
          break;
        case 'txt': {
          const header = text.split('\n').find((line) => labels.every((l) => line.includes(l)));
          if (!header) throw new Error('no line carries the grid\'s headers');
          break;
        }
        case 'pdf': {
          const why = pdfConsistent(new Uint8Array(body));
          if (why) throw new Error(why);
          break;
        }
      }
      return `${name}, ${body.length} bytes, headers as the grid's`;
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
      await settle();
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
    // RFC 2045/5322 (2026-09-29): the MIME version, CRLF throughout, a subject; an unsent draft
    if (!/^MIME-Version: 1\.0\r\n/.test(body)) throw new Error('no MIME-Version first');
    if (/[^\r]\n/.test(body)) throw new Error('a bare LF: every line must end CRLF');
    if (!/\r\nSubject: \S/.test(body) || !/\r\nX-Unsent: 1\r\n/.test(body)) throw new Error('not an unsent draft with a subject');
    if (!/filename=".* - .*\.csv"/.test(body)) throw new Error('no timestamped CSV attached');
    // the attachment decodes to the CSV the grid would export
    const b64 = /filename="[^"]*\.csv"[^\r]*\r\n\r\n([A-Za-z0-9+/=\r\n]+?)\r\n--/.exec(body)?.[1] ?? '';
    const csv = Buffer.from(b64.replace(/\r\n/g, ''), 'base64').toString('utf8');
    const labels = await gridLabels();
    if (JSON.stringify(csvHeader(csv)) !== JSON.stringify(labels)) {
      throw new Error(`the attached CSV's headers ${JSON.stringify(csvHeader(csv))} are not the grid's`);
    }
    return `${name}: a draft, its CSV decoded, headers as the grid's`;
  });

  await check('Pin Left is checked once the column is pinned left', async () => {
    await menu(['Pin', 'Pin Left'], { requery: false });
    await settle();
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
    // the copy is asynchronous (the clipboard API is a promise): read until it lands
    let text = '';
    for (const until = Date.now() + 5_000; Date.now() < until && !text.trim(); await frames(page)) {
      text = await page.evaluate(() => navigator.clipboard.readText());
    }
    if (!text || !text.trim()) throw new Error('the clipboard is empty');
    return `${text.split('\n').length} lines`;
  });

  // ---- the editor --------------------------------------------------------
  await section('the editor');

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
      await settle();
      const body = await page.evaluate(() => {
        const panel = document.querySelector('.dc-app-overlay');
        const b = panel?.querySelector('[role=tabpanel], .dc-tab-body')
          ?? panel;
        return (b?.textContent ?? '').trim().length;
      });
      if (body < 20) empty.push(name);
    }
    await page.keyboard.press('Escape');
    await settle();
    if (empty.length) throw new Error(`blank panels: ${empty.join(', ')}`);
    return `${n} tabs, all populated`;
  });

  // ---- the columns selector ----------------------------------------------
  await section('the columns selector');
  //
  // Add, remove and REORDER, through the editor's Columns tab. This
  // was the one part of the product no harness had ever driven, and
  // it is the sibling trigger of the header-identity fault: `leafIndex`
  // held a source index, so reordering shifted every header's claimed
  // column exactly as hiding did. Both paths are checked here, and
  // the invariants that run after every check are what would catch it
  // again.

  /** Open Properties and select a tab by name. */
  /** Column Properties' columns, in the list beside the form (src/ui/panel-column.ts). */
  const columnChoices = () => page.locator('.dc-app-overlay .dc-pe-col')
    .evaluateAll((els) => els.map((e) => e.dataset.column));
  /** Choose a column there, as a person does. */
  const chooseColumn = async (name) => {
    await page.locator(`.dc-app-overlay .dc-pe-col[data-column="${name}"]`).click();
  };

  async function editorTab(name) {
    await menu(['Properties...'], { requery: false });
    const tab = page.locator('.dc-editor-tab', { hasText: name });
    if (!(await tab.count())) {
      const seen = await page.locator('.dc-editor-tab').allTextContents();
      throw new Error(`no ${name} tab; the editor offers ${seen.join(', ')}`);
    }
    await tab.first().click();
    await settle();
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
    // under View, now
    await pickEntry('Properties...');
    await settle();
    await page.locator('.dc-editor-tab', { hasText: 'General Properties' })
      .click();
    await settle();
    const box = page.locator('.dc-check', { hasText: label })
      .locator('input');
    if (on) await box.first().check();
    else await box.first().uncheck();
    await page.locator('.dc-editor-footer button', { hasText: 'Apply' })
      .click();
    await settle();
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
    // under View, now
    await pickEntry('Properties...');
    await settle();
    await page.locator('.dc-editor-tab', { hasText: 'General Properties' })
      .click();
    await settle();
    // BY ITS OWN LABEL: a `.dc-field` holds several inputs, and
    // taking the first has twice now toggled a different setting.
    const box = page.locator('.dc-check', {
      hasText: 'Keep grouped columns in the grid',
    }).locator('input');
    if (on) await box.first().check();
    else await box.first().uncheck();
    await page.locator('.dc-editor-footer button', { hasText: 'Apply' })
      .click();
    await settle();
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
    await settle();
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
    await settle();
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
    await settle();
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
  await section('round trips');
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
    // WHICH cells, in WHAT colour: a count could not tell a heatmap from a colour another
    // check left on every cell of the same rows.
    colour: [...document.querySelectorAll('.dc-row')].slice(0, 8).map((r) =>
      [...r.querySelectorAll('.dc-cell')].map((c) => c.style.backgroundColor || '-').join(','))
      .join(' // '),
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
    // a NUMERIC column as the grid shows it NOW: the index the heatmap check found is stale
    // once a later check has reordered the columns
    ['a heatmap', ['Heatmap', /^Add Heatmap/], async () => {
      const view = await readView(page);
      return { col: (await headerNames()).findIndex((n) => isNumeric(view.find((c) => c.name === n)?.type)) };
    }],
  ]) {
    await check(`undo restores everything after ${what}`, async () => {
      const at = typeof opts === 'function' ? await opts() : opts;
      if (at.col !== undefined && at.col < 0) throw new Error('no numeric column to shade');
      const before = await fullState();
      await menu(path, at);
      // a cosmetic change paints after the query settles: give it the frames it needs
      let changed = await fullState();
      for (let i = 0; i < 10 && firstDifference(before, changed) === null; i += 1) {
        await settle();
        changed = await fullState();
      }
      if (firstDifference(before, changed) === null) {
        throw new Error(`the operation changed nothing at all (colour ${before.colour.slice(0, 120)})`);
      }
      await burger('Undo');
      const after = await fullState();
      const diff = firstDifference(before, after);
      if (diff) throw new Error(`undo left ${diff}`);
      return 'restored exactly';
    });
  }

  // ---- column properties -------------------------------------------------
  await section('column properties');

  await check("changing a column's KIND changes how it aggregates", async () => {
    // The kind is not cosmetic: a dimension takes its unique value
    // when grouped and a measure sums. Only the editor can change it,
    // and nothing had ever driven that -- so this asserts the
    // GENERATED AGGREGATE, not the label in the panel.
    await menu(['Pivot', 'Clear All Vertical Pivots']).catch(() => {});
    await editorTab('Column Properties');
    const opts = await columnChoices();
    if (opts.length === 0) throw new Error('no column list');
    // An INTEGER column, by the compiler's type: numeric, so a measure by default (D3,
    // upstream's rule) -- and the first one is the id, which a person makes a dimension.
    const view = await readView(page);
    const want = opts.find((n) => view.find((c) => c.name === n)?.type === 'Integer');
    if (!want) throw new Error(`no Integer column among ${opts.join(',')}`);
    await chooseColumn(want);
    await settle();
    // the kind is always shown (the user, 2026-09-30): upstream's one ADVANCED setting, no
    // checkbox to open first
    if (await page.locator('.dc-check', { hasText: 'Show advanced settings?' }).count()) {
      throw new Error('a "Show advanced settings?" checkbox is back');
    }

    const kind = page.locator('.dc-field', { hasText: 'Column Kind:' })
      .first().locator('select').first();
    if ((await kind.inputValue()) !== 'measure') {
      throw new Error(`${want} (Integer) defaults to ${await kind.inputValue()}, not measure`);
    }
    await kind.selectOption('dimension');
    await settle();
    await applyEditor();

    // The panel is where a person sees it, so check there too -- but
    // the query is the claim.
    const isMeasure = await page.evaluate((n) =>
      document.querySelector(`.dc-tool-panel-row[data-column="${n}"]`)
        ?.classList.contains('dc-measure') ?? true, want);
    if (isMeasure) throw new Error(`the panel still lists ${want} as a measure`);

    await menu(['Pivot', /^Vertical Pivot on/], { col: GROUP_COL });
    // what DataCube ASKED for, in the query it built -- the same on every planner (each writes
    // its own SQL for it: legend-lite's CASE WHEN COUNT(DISTINCT ..), engine's own)
    const pure = (await state()).pure.replace(/\s+/g, ' ');
    const summed = new RegExp(`${want}:x\\|\\$x\\.${want}:y\\|\\$y->(sum|plus)\\(\\)`).test(pure);
    const unique = new RegExp(`${want}:x\\|\\$x\\.${want}:y\\|\\$y->uniqueValueOnly\\(\\)`).test(pure);
    if (summed || !unique) {
      throw new Error(`${want} is a dimension but ${summed ? 'still sums' : 'does not take its unique value'}`);
    }
    await menu(['Pivot', 'Clear All Vertical Pivots']);
    return `${want}: measure by type, a dimension once set -- its unique value, not a sum`;
  });

  await check('a display name changes the label, not the identity', async () => {
    // The header-identity fault made a label and a `data-column`
    // disagree, so this is the one place they are SUPPOSED to: a
    // renamed column keeps its identity, which is exactly why the
    // invariant compares headers by position rather than by text.
    await editorTab('Column Properties');
    const opts = await columnChoices();
    const want = opts[opts.length - 1];
    await chooseColumn(want);
    await settle();
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

  // ---- saving and opening a cube -----------------------------------------
  await section('saving and opening a cube');
  //
  // Driven end to end -- save, reload the page, open, the same typed values -- by its own
  // harness, `bazel run //datacube:verify_cubes`, which needs a fresh page per step.

  // ---- getting rid of a menu ---------------------------------------------
  await section('getting rid of a menu');
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
    await settle();
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
    await settle();
    const left = await menuOpen();
    if (left) throw new Error(`${left} menu(s) survived Escape`);
    return 'gone';
  });

  await check('the title bar button opens AND closes its menu', async () => {
    await page.click('.dc-titlebar-menu');
    await page.waitForSelector('.dc-menu', { timeout: 5000 });
    await page.click('.dc-titlebar-menu');
    await settle();
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
    await settle();
    const shown = await page.locator('.dc-filter-empty, .dc-filter-tree')
      .count();
    if (!shown) throw new Error('the Filter button opened nothing');
    await page.keyboard.press('Escape');
    await settle();
    return 'opens the editor';
  });

  // ---- the dialogs are windows -------------------------------------------
  await section('the dialogs are windows');

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
    await settle();

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
    await settle();
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
      await settle();
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
      await settle();
      const sized = await box();
      if (sized.w <= moved.w || sized.h <= moved.h) {
        throw new Error(`the south-east grip did not resize:`
          + ` ${moved.w}x${moved.h} -> ${sized.w}x${sized.h}`);
      }

      // Closed and reopened, it comes back where it was left.
      await page.locator('.dc-overlay-close').first().click();
      await settle();
      await menu(['Properties...'], { requery: false });
      const again = await box();
      if (again.x !== sized.x || again.y !== sized.y
        || again.w !== sized.w || again.h !== sized.h) {
        throw new Error(`it jumped on reopening: ${sized.w}x${sized.h} at`
          + ` ${sized.x},${sized.y} became ${again.w}x${again.h} at`
          + ` ${again.x},${again.y}`);
      }
      await page.keyboard.press('Escape');
      await settle();
      return `moved to ${moved.x},${moved.y} and resized to`
        + ` ${sized.w}x${sized.h}`;
    });

  // ---- the chrome ---------------------------------------------------------
  await section('the chrome');

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
    await settle();
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
    await settle();
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
      await settle();
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
    // Window columns too: `extend(over(...), ~[name:{p,w,r|...`.
    return [...pure.matchAll(/extend\(~\[([^:\]]+):|, ~\[([^:\]]+):\{p,w,r\|/g)]
      .map((m) => (m[1] ?? m[2]).replace(/^'|'$/g, ''));
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
  /**
   * The column editor's kind, as a person picks it (src/ui/column-editor.ts): the rail's kind --
   * a formula, a ratio of totals (computed after grouping), a window, the child groups -- then,
   * for a window, over source rows or groups, and for a value of each row, Measure or Dimension.
   */
  const calcKind = async (level, mode = 'expression') => {
    const kind = mode === 'window' ? 'window' : mode === 'children' ? 'children' : level === 'group' ? 'ratio' : 'formula';
    await page.locator(`.dc-coleditor .dc-xc-kind[data-kind="${kind}"]`).click();
    if (kind === 'window') {
      await page.locator(`.dc-coleditor .dc-xc-over [data-value="${level === 'group' ? 'group' : 'row'}"]`).click();
    }
    if (level !== 'group') await page.locator(`.dc-coleditor .dc-xc-use [data-value="${level}"]`).click();
  };

  const addCalc = async (stage, name, expression, kind = 'measure') => {
    await openCalc();
    await page.fill('.dc-coleditor .dc-calc-input-name', name);
    // `kind === null` leaves the editor's own default alone, which is
    // what a user who never touches the kind gets.
    const level = stage === 1 ? 'group' : kind;
    if (level !== null) {
      await calcKind(level);
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
      // GROUPED, set up here: the SUM asserted below is the groupBy's. This check leaned on
      // the grouped cube a saved-view check used to leave behind.
      await menu(['Pivot', /^Vertical Pivot on/], { col: GROUP_COL });
      await addCalc(0, 'uplift', 'x|$x.notional->toOne() * 1.1');
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
      // THE ARITHMETIC. notional x 1.1, the same row's VALUES -- typed as the compiler says
      const notional = await readColumn(page, 'notional');
      const uplift = await readColumn(page, 'uplift');
      if (uplift.type !== 'Float') {
        throw new Error(`uplift is typed ${uplift.type}; notional * 1.1 is a Float`);
      }
      const want = Number(notional.values[0]) * 1.1;
      const got = uplift.values[0];
      if (!closeTyped(got, want, uplift.type)) {
        throw new Error(`uplift is ${String(got)}, expected ${want}`);
      }
      const detail = `${cols.length} columns, uplift = notional x 1.1`
        + ` = ${String(got)} (${uplift.type})`;
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
      await addCalc(1, 'doubled', 'x|$x.notional->toOne() * 2');
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
      // notional x 2, the same row's VALUES
      const notional = await readColumn(page, 'notional');
      const doubled = await readColumn(page, 'doubled');
      const want = Number(notional.values[0]) * 2;
      const got = doubled.values[0];
      if (!closeTyped(got, want, doubled.type)) {
        throw new Error(`doubled is ${String(got)}, expected ${want}`);
      }
      const detail = `extend after groupBy, doubled = ${String(got)} (${doubled.type})`;
      await clearCalcs();
      return detail;
    });

  await check('a calculated column is typed by the COMPILER, and formats by that type',
    async () => {
      // The type is the compiler's answer for the extended relation, asked before the
      // column's first query (T1) -- never learned from a result. Untyped, the aggregate
      // default read no type and a numeric column grouped as `unique`: a blank column.
      await addCalc(0, 'uplift', 'x|$x.notional->toOne() * 1.1');
      await settle();
      try {
      const typed = await page.evaluate(() =>
        (window.__dataCube.snapshot.derived ?? []).find((d) => d.name === 'uplift')?.type ?? null);
      if (typed !== 'Float') {
        throw new Error(`the compiler's type for uplift is ${typed}; notional * 1.1 is a Float`);
      }
      // The type the result reported, as Column Properties shows it: a
      // Float shows the number section with upstream's 2 decimals.
      await page.click('.dc-status-properties');
      await page.locator('.dc-app-overlay .dc-editor-tab', { hasText: 'Column Properties' }).click();
      await page.locator('.dc-app-overlay .dc-pe-col[data-column="uplift"]').click();
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
          + ' its type did not reach the format defaults');
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
      const check = await addCalc(0, 'bogus', 'x|$x.notional->nosuchfunction()');
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

  await check('the GROUP stage offers only columns that compile there', async () => {
    // A groupDerived expression runs after the groupBy. With measures,
    // the source columns are gone and must not be offered; with NONE,
    // the groupBy aggregates every column under its own name (upstream's
    // _groupByAggCols), so they are there. The rule underneath both:
    // nothing offered may be refused. So every offered column that is
    // not a row dimension is compiled, through the product's own check.
    await openCalc();
    await calcKind('group');
    await page.locator('.dc-coleditor .dc-xc-tool[data-insert="column"]').click();
    const offered = await page.locator('.dc-coleditor .dc-calc-item-column'
      + ' .dc-calc-item-label').allTextContents();
    const dims = await dimensionNames();
    const probe = offered.filter((n) => !dims.includes(n)).slice(0, 6);
    const refused = [];
    for (const n of probe) {
      const ref = /^[A-Za-z_][A-Za-z0-9_]*$/.test(n) ? `x|$x.${n}` : `x|$x.'${n}'`;
      await page.fill('.dc-coleditor .dc-calc-input-expr', ref);
      const verdict = await compiledCheck();
      if (verdict.state === 'refused') refused.push(`${n}: ${verdict.text.slice(0, 80)}`);
    }
    await closeCalc();
    if (refused.length > 0) throw new Error(`offered but refused: ${refused.join('; ')}`);
    return `${offered.length} in scope; ${probe.length} non-dimension columns each compile`;
  });

  await check('removing a calculated column takes it out of the query',
    async () => {
      await addCalc(0, 'uplift', 'x|$x.notional->toOne() * 1.1');
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
      await addCalc(0, 'uplift', 'x|$x.notional->toOne() * 1.1');
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
      // default made `$x.notional->toOne() * 1.1` render blank under any row
      // group -- the query took its uniqueValueOnly() -- which is the
      // first thing anyone trying the feature sees.
      await flatten();
      await menu(['Pivot', /^Vertical Pivot on/],
        { col: await needCol('region') });
      await addCalc(0, 'uplift', 'x|$x.notional->toOne() * 1.1', null);
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
    await addCalc(0, 'big', 'x|$x.notional > 500000', 'dimension');
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
    await addCalc(0, 'uplift', 'x|$x.notional->toOne() * 1.1', 'measure');
    await settle();
    await closeCalc();
    // across a TEXT dimension (quarter, String by the asserted schema): `year` is an
    // Integer, so a measure by default (D3), and not offered as a pivot key
    await menu(['Pivot', 'Horizontal Pivot on quarter'],
      { col: await needCol('quarter') });
    const pivot = await pivotOf();
    const cols = await gridColumns();
    await flatten();
    await clearCalcs();
    if (!pivot.some((c) => c.measure === 'uplift')) {
      throw new Error(`the pivot aggregates no uplift: ${pivot.map((c) => c.name).slice(0, 8).join(', ')}`);
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
      // offered "big contains true" on a Boolean. Ordering IS offered:
      // Pure defines it on Boolean (legend-pure's lessThan.pure has the
      // Boolean overloads), and operators are what the compiler accepts
      // (T5) -- so the claim is no TEXT operator, never no ordering.
      await flatten();
      await addCalc(0, 'big', 'x|$x.notional > 500000', 'dimension');
      await settle();
      await closeCalc();
      const items = await menuAt(await needCol('big'));
      await clearCalcs();
      const filters = items.map((i) => i.label)
        .filter((l) => /^Add Filter: big/.test(l));
      if (filters.length === 0) throw new Error('no filter entries for big');
      const textual = filters.filter((l) =>
        /contains|starts with|ends with/.test(l));
      if (textual.length > 0) {
        throw new Error(`text operators on a Boolean: ${textual.join(' / ')}`);
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
      await addCalc(0, 'bogus', 'x|$x.nope * 2', 'measure');
      const form = await page.evaluate(() => ({
        open: document.querySelectorAll('.dc-coleditor').length,
        name: document.querySelector('.dc-coleditor .dc-calc-input-name')?.value ?? null,
        // the formula as written: the lambda's head, shown fixed, then the box
        expr: (document.querySelector('.dc-coleditor .dc-xc-prefix')?.textContent ?? '')
          + (document.querySelector('.dc-coleditor .dc-calc-input-expr')?.value ?? ''),
        problem: document.querySelector('.dc-coleditor .dc-calc-check')?.textContent ?? '',
        prefixes: [...document.querySelectorAll('.dc-coleditor .dc-xc-prefix')].map((e) => e.textContent),
        kind: document.querySelector('.dc-coleditor .dc-xc-kind[aria-pressed="true"], .dc-coleditor .dc-xc-kind.dc-active')?.dataset.kind ?? null,
      }));
      await closeCalc();
      await clearCalcs();
      if (!form.open) throw new Error('the form closed on a refusal');
      if (form.name !== 'bogus' || form.expr !== 'x|$x.nope * 2') {
        throw new Error(`the typed text was lost: ${JSON.stringify(form)}`);
      }
      if (!/nope/.test(form.problem)) {
        throw new Error(`the form does not say why: ${JSON.stringify(form.problem)}`);
      }
      return form.problem.replace(/\s+/g, ' ').slice(0, 80);
    });

  // ---- window columns -------------------------------------------------
  await section('window columns');
  //
  // Every figure below is the DATABASE's, checked against an independent
  // reckoning: the grid's own measure summed in the page, or the raw
  // rows fetched by a plain select and accumulated in JavaScript.
  const addWindow = async ({ name, level, fn, of, partition = [], order = [], frame }) => {
    await openCalc();
    const ed = (sel) => page.locator(`.dc-coleditor ${sel}`);
    await page.fill('.dc-coleditor .dc-calc-input-name', name);
    await calcKind(level, 'window');
    await ed('.dc-win-fn').selectOption(fn);
    if (of) await ed('.dc-win-column').selectOption(of);
    for (const p of partition) await ed(`.dc-win-part-check[value="${p}"]`).check();
    for (const [i, o] of order.entries()) {
      await ed('.dc-win-order-add').click();
      await ed('.dc-win-order-column').nth(i).selectOption(o.column);
      await ed('.dc-win-order-direction').nth(i).selectOption(o.direction);
    }
    if (frame) await ed('.dc-win-frame').selectOption(frame);
    const verdict = await compiledCheck();
    if (verdict.state === 'refused') throw new Error(`refused: ${verdict.text}`);
    const ok = ed('.dc-calc-ok');
    if (await ok.isDisabled()) {
      throw new Error(`OK is disabled: ${await ed('.dc-calc-check').textContent()}`);
    }
    const before = await statusNow();
    await ok.click();
    await settle(before);
    await closeCalc();
  };
  /** The view's columns by name, typed: the assembled table the grid draws. */
  const viewColumns = async (...names) => {
    const view = await readView(page);
    return Object.fromEntries(names.map((n) => [n, view.find((c) => c.name === n) ?? null]));
  };

  await check('window column: a group-level running total and previous value, down the grid', async () => {
    await freshCube();
    try {
      await menu(['Pivot', /^Vertical Pivot on/], { col: await needCol('desk') });
      await addWindow({ name: 'running', level: 'group', fn: 'sum', of: 'notional', frame: 'running' });
      await addWindow({ name: 'previous', level: 'group', fn: 'lag', of: 'notional' });
      const { pure } = await state();
      if (!/extend\(over\(/.test(pure)) throw new Error(`no window in the query: ${pure.slice(0, 200)}`);
      const v = await viewColumns('notional', 'running', 'previous');
      if (!v.notional || !v.running || !v.previous) throw new Error(`columns ${Object.keys(v).filter((k) => !v[k])} missing`);
      // by the compiler's types: a running sum of a Float is a Float, the previous value is
      // the measure's own type -- compared as values of those types
      const bad = [];
      v.notional.values.forEach((_n, i) => {
        const acc = sumTyped(v.notional.values.slice(0, i + 1), v.notional.type);
        if (!closeTyped(v.running.values[i], acc, v.running.type)) {
          bad.push(`row ${i}: running ${String(v.running.values[i])} vs ${String(acc)}`);
        }
        const want = i === 0 ? null : v.notional.values[i - 1];
        if (!sameTyped(v.previous.values[i], want, v.previous.type)) {
          bad.push(`row ${i}: previous ${String(v.previous.values[i])} vs ${String(want)}`);
        }
      });
      if (v.notional.values.length < 2) throw new Error('too few groups to prove anything');
      if (bad.length) throw new Error(bad.slice(0, 3).join('; '));
      return `${v.notional.values.length} desks (${v.running.type}): running ends at`
        + ` ${String(v.running.values.at(-1))}, each previous is the row above`;
    } finally {
      await freshCube();
    }
  });

  await check('window column: a row-level running sum per region matches the raw rows', async () => {
    await freshCube();
    try {
      await addWindow({ name: 'run_qty', level: 'measure', fn: 'sum', of: 'quantity',
        partition: ['region'], order: [{ column: 'trade_id', direction: 'asc' }], frame: 'running' });
      const result = await page.evaluate(async () => {
        const app = window.__dataCube;
        const c = app.controller;
        const raw = (await c.runQuery(await c.parse(`${(await c.print({ _type: 'lambda', parameters: [], body: [app.snapshot.source.query] })).trim()}->select(~[trade_id, region, quantity])`), app.snapshot)).rows;
        const col = (t, n) => t.columns.find((x) => x.name === n).values;
        const ids = col(raw, 'trade_id'); const regions = col(raw, 'region'); const qty = col(raw, 'quantity');
        const order = ids.map((_v, i) => i).sort((a, b) => Number(ids[a]) - Number(ids[b]));
        const want = new Map(); const acc = new Map();
        for (const i of order) {
          const r = String(regions[i]);
          const q = qty[i];
          const prev = acc.has(r) ? acc.get(r) : null;
          const next = q === null ? prev : (prev ?? 0) + Number(q);
          acc.set(r, next);
          want.set(String(ids[i]), next);
        }
        const t = app.view.rows;
        const gotIds = col(t, 'trade_id'); const got = col(t, 'run_qty');
        const bad = [];
        gotIds.forEach((id, i) => {
          const w = want.get(String(id));
          const g = got[i];
          const same = w === null ? g === null : Math.abs(Number(g) - w) <= 1e-6 * Math.max(1, Math.abs(w));
          if (!same) bad.push(`trade ${id}: ${g} vs ${w}`);
        });
        return { compared: gotIds.length, raw: ids.length, bad: bad.slice(0, 3), nbad: bad.length };
      });
      if (result.compared < 100) throw new Error(`only ${result.compared} rows to compare`);
      if (result.nbad) throw new Error(`${result.nbad} wrong: ${result.bad.join('; ')}`);
      return `${result.compared} rows on screen, each equal to the running sum of ${result.raw} raw rows`;
    } finally {
      await freshCube();
    }
  });

  await check('window column: a group-level rank follows the measure', async () => {
    await freshCube();
    try {
      await menu(['Pivot', /^Vertical Pivot on/], { col: await needCol('book') });
      await addWindow({ name: 'rank_notional', level: 'group', fn: 'rank',
        order: [{ column: 'notional', direction: 'desc' }] });
      const v = await viewColumns('notional', 'rank_notional');
      const bad = [];
      const n = v.notional;
      n.values.forEach((x, i) => {
        // rank = 1 + how many are strictly greater, by the measure's type; the rank is an Integer
        const want = 1 + n.values.filter((m) => m !== null && x !== null && compareTyped(m, x, n.type) > 0).length;
        if (!sameTyped(v.rank_notional.values[i], want, v.rank_notional.type)) {
          bad.push(`row ${i}: rank ${String(v.rank_notional.values[i])} vs ${want}`);
        }
      });
      if (n.values.length < 3) throw new Error('too few groups to rank');
      if (bad.length) throw new Error(bad.slice(0, 3).join('; '));
      return `${n.values.length} books ranked by notional (${v.rank_notional.type})`;
    } finally {
      await freshCube();
    }
  });

  // ---- child-group aggregates ------------------------------------------
  await section('child-group aggregates');
  const addChildren = async ({ name, fn, of }) => {
    await openCalc();
    const ed = (sel) => page.locator(`.dc-coleditor ${sel}`);
    await page.fill('.dc-coleditor .dc-calc-input-name', name);
    await calcKind('group', 'children');
    await ed('.dc-child-fn').selectOption(fn);
    await ed('.dc-child-of').selectOption(of);
    const verdict = await compiledCheck();
    if (verdict.state === 'refused') throw new Error(`refused: ${verdict.text}`);
    const ok = ed('.dc-calc-ok');
    if (await ok.isDisabled()) throw new Error(`OK is disabled: ${await ed('.dc-calc-check').textContent()}`);
    const before = await statusNow();
    await ok.click();
    await settle(before);
    await closeCalc();
  };

  await check('child groups: a region shows the smallest of its desks, a desk the smallest of its trades', async () => {
    await freshCube();
    try {
      await menu(['Pivot', /^Vertical Pivot on/], { col: await needCol('region') });
      await menu(['Pivot', 'Add Vertical Pivot on desk'], { col: await needCol('desk') });
      await addChildren({ name: 'weakest', fn: 'min', of: 'notional' });
      await addChildren({ name: 'children', fn: 'count', of: 'notional' });
      // Open the first region onto its desks.
      const opened = await statusNow();
      await page.locator('.dc-row[aria-expanded=false] .dc-chevron').first().click();
      await settle(opened);
      const result = await page.evaluate(async () => {
        const v = window.__dataCube.view;
        const col = (t, n) => t.columns.find((x) => x.name === n)?.values ?? null;
        const notional = col(v.rows, 'notional');
        const weakest = col(v.rows, 'weakest');
        const children = col(v.rows, 'children');
        if (!notional || !weakest || !children) return { error: 'columns missing' };
        const rows = v.treeRows.map((r, i) => ({ path: r.path, level: r.level, i }));
        const region = rows.find((r) => r.level === 1 && rows.some((x) => x.level === 2 && x.path[0] === r.path[0]));
        const desks = rows.filter((x) => x.level === 2 && x.path[0] === region.path[0]);
        // The trades' own minimum per desk, from the raw rows.
        const app = window.__dataCube;
        const c = app.controller;
        const raw = (await c.runQuery(await c.parse(`${(await c.print({ _type: 'lambda', parameters: [], body: [app.snapshot.source.query] })).trim()}->select(~[region, desk, notional])`), app.snapshot)).rows;
        const rr = col(raw, 'region'); const rd = col(raw, 'desk'); const rn = col(raw, 'notional');
        const minTrade = new Map();
        rr.forEach((r, i) => {
          if (r !== region.path[0] || rn[i] === null) return;
          const k = String(rd[i]);
          minTrade.set(k, Math.min(minTrade.get(k) ?? Infinity, Number(rn[i])));
        });
        return {
          region: region.path[0],
          regionWeakest: weakest[region.i],
          regionChildren: children[region.i],
          deskTotals: desks.map((d) => notional[d.i]),
          desks: desks.map((d) => ({ desk: d.path[1], weakest: weakest[d.i], trade: minTrade.get(String(d.path[1])) })),
          truncated: v.truncated.length,
        };
      });
      if (result.error) throw new Error(result.error);
      const near = (a, b) => Math.abs(Number(a) - Number(b)) <= 1e-6 * Math.max(1, Math.abs(Number(b)));
      const smallestDesk = Math.min(...result.deskTotals.map(Number));
      if (!near(result.regionWeakest, smallestDesk)) {
        throw new Error(`${result.region}: weakest ${result.regionWeakest}, its smallest desk total is ${smallestDesk}`);
      }
      if (!result.truncated && Number(result.regionChildren) !== result.deskTotals.length) {
        throw new Error(`${result.region}: ${result.regionChildren} children, ${result.deskTotals.length} desks shown`);
      }
      const bad = result.desks.filter((d) => !near(d.weakest, d.trade));
      if (bad.length) throw new Error(`desk minimum vs its trades: ${stamp(bad.slice(0, 2))}`);
      return `${result.region}: weakest desk ${smallestDesk.toFixed(2)} of ${result.deskTotals.length};`
        + ` each desk's own = its smallest trade`;
    } finally {
      await freshCube();
    }
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
        expr: (document.querySelector('.dc-coleditor .dc-xc-prefix')?.textContent ?? '')
          + (document.querySelector('.dc-coleditor .dc-calc-input-expr')?.value ?? ''),
        kind: document.querySelector('.dc-coleditor .dc-xc-use .dc-on')?.dataset.value,
      }));
      if (seeded.expr !== 'x|$x.notional' || seeded.kind !== 'measure') {
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
      await addCalc(0, 'uplift', 'x|$x.notional->toOne() * 1.1');
      await settle();
      const edit = async () => menu(['Extended Columns', 'Edit Column uplift...'],
        { col: await needCol('uplift'), requery: false });
      await edit();
      await page.fill('.dc-coleditor .dc-calc-input-expr', 'x|$x.notional->toOne() * 9');
      // Reset, as upstream's: back to what the column had.
      await page.locator('.dc-coleditor .dc-calc-reset').click();
      // the formula as written: the lambda's head, shown fixed, then the box
      const expr = (await page.textContent('.dc-coleditor .dc-xc-prefix'))
        + (await page.inputValue('.dc-coleditor .dc-calc-input-expr'));
      await closeCalc();
      await clearCalcs();
      if (expr !== 'x|$x.notional->toOne() * 1.1') throw new Error(`Reset left ${expr}`);
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
    await settle();
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

  // Was a known gap for a long time: a column RENAMED by an earlier
  // check (settled -> "RENAMED") dropped out of the order, because the
  // display name was written into the column's identity path. Found by
  // reading the cube's own state (window.__dataCube) at the failure.
  await check('a panel reorder still reaches the grid after a long run',
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
      const viewsBefore = await page.evaluate(() => window.__dataCubeViews ?? 0);
      const errorsBefore = pageErrors.length;
      await page.locator(`.dc-tool-panel-row[data-column="${last}"]`)
        .dragTo(page.locator(
          `.dc-tool-panel-row[data-column="${before[0]}"]`),
        { timeout: 10_000, targetPosition: { x: 40, y: 2 } });
      await settle(stamp);
      // WAIT FOR THE DROP'S OWN VIEW, not the first one: `settle`
      // returns when the status line changes, and a refresh already in
      // flight (the flatten above) changes it before the drop's
      // refresh -- the one carrying the new order -- has landed.
      const quiet = async () => {
        let seen = await page.evaluate(() => window.__dataCubeViews ?? 0);
        for (let i = 0; i < 50; i += 1) {
          await settle();
          const now = await page.evaluate(() => window.__dataCubeViews ?? 0);
          if (now === seen && i >= 3) return;
          seen = now;
        }
      };
      await quiet();
      const trace = {
        viewsAfterDrag: (await page.evaluate(() => window.__dataCubeViews ?? 0)) - viewsBefore,
        statusBefore: stamp,
        statusAfter: await statusNow(),
        newPageErrors: pageErrors.slice(errorsBefore),
      };
      const grid = (await gridColumns()).filter((c) => c !== '__tree');
      const panel = (await listed()).filter((c) => grid.includes(c));
      const shown = grid.filter((c) => panel.includes(c));
      if (panel.join(',') !== shown.join(',')) {
        // What the CUBE holds, not only what the screen shows.
        const held = await page.evaluate(() => {
          const app = window.__dataCube;
          if (!app) return 'no __dataCube on the page';
          return JSON.stringify({
            columnOrder: app.configuration.columnOrder ?? null,
            snapshotColumns: app.snapshot.columns.map((c) => c.name),
            derived: app.snapshot.derived.map((d) => d.name),
            rows: app.snapshot.rows,
            pivotOn: app.snapshot.pivotOn,
            configured: Object.entries(app.configuration.columns)
              .filter(([, c]) => c.pinned || c.hidden)
              .map(([n, c]) => `${n}:${c.pinned ?? ''}${c.hidden ? 'hidden' : ''}`),
          });
        });
        // Where each header SITS (the order above is by screen x) and
        // where the model PUT it (its grid column).
        const heads = await page.evaluate(() =>
          [...document.querySelectorAll('.dc-th[data-column]')].map((e) =>
            `${e.dataset.column}@${e.style.gridColumn}/x${Math.round(e.getBoundingClientRect().left)}`
            + `${e.classList.contains('dc-pinned') ? '/pinned' : ''}${getComputedStyle(e).position === 'sticky' ? '/sticky' : ''}`));
        throw new Error(`the panel reads ${panel.join(', ')} and the grid`
          + ` reads ${shown.join(', ')} | headers ${heads.join(' ')} | trace ${JSON.stringify(trace)} | cube holds ${held}`
          + ` | query ${(await state()).pure.slice(0, 260)}`);
      }
      return `${last} moved and the grid followed`;
    });

  await check('a column pivot flips to measures first, and back', async () => {
    await flatten();
    await menu(['Pivot', 'Add Vertical Pivot on region'], { col: await needCol('region') });
    await menu(['Pivot', 'Horizontal Pivot on quarter'], { col: await needCol('quarter') });
    const rows = () => page.evaluate(() =>
      [1, 2].map((r) => [...document.querySelectorAll(
        `.dc-head-row[aria-rowindex="${r}"] .dc-th`)].map((e) => e.textContent?.trim() ?? '')));
    // a pivot VALUE, as the database returned it for the key (the view's pivot facts), not
    // a pattern guessed for how one looks
    const keys = new Set((await pivotOf()).flatMap((c) => c.tuple ?? []).map(String));
    const year = (t) => keys.has(t);
    const [top0, second0] = await rows();
    await menu(['Pivot', 'Measures First in Column Headers'], { requery: false });
    await settle();
    const [top1, second1] = await rows();
    await menu(['Pivot', 'Measures First in Column Headers'], { requery: false });
    await settle();
    const [top2] = await rows();
    await menu(['Pivot', 'Clear All Horizontal Pivots']).catch(() => {});
    await menu(['Pivot', 'Clear All Vertical Pivots']).catch(() => {});
    if (!top0.some(year)) throw new Error(`value-first top row has no year: ${top0}`);
    if (top1.some(year)) throw new Error(`measure-first top row still shows years: ${top1}`);
    if (!second1.some(year)) throw new Error(`measure-first second row has no years: ${second1}`);
    const measures = top1.filter((t) => ['notional', 'pnl', 'quantity'].includes(t));
    if (measures.length === 0) throw new Error(`no measure on the top row: ${top1}`);
    if (top2.join() !== top0.join()) throw new Error(`flipping back gave ${top2}, not ${top0}`);
    return `top row ${top0.filter(year).slice(0, 2).join(', ')}… → ${measures.join(', ')}, and back`;
  });

  await check('a pivot zone shows a bar exactly where a dragged column will land', async () => {
    // As the columns list does: a blue bar on the edge of the chip it
    // will land beside -- not a box around the whole zone.
    await flatten();
    for (const c of ['region', 'desk']) {
      await menu(['Pivot', `Add Vertical Pivot on ${c}`], { col: await needCol(c) });
    }
    const chips = page.locator('.dc-tool-panel-zones .dc-zone-rows .dc-chip');
    if ((await chips.count()) < 2) throw new Error('could not set up two row groups');
    const source = page.locator('.dc-tool-panel-row[data-column="book"]');
    const target = await chips.nth(0).boundingBox();
    if (!target || !(await source.count())) throw new Error('nothing to drag');
    // The drag's own events, on the real page: Playwright's mouse does
    // not start an HTML5 drag from this list, and the question is what
    // the zone DRAWS mid-drag, which a completed dragTo never shows.
    const dt = await page.evaluateHandle(() => new DataTransfer());
    await source.dispatchEvent('dragstart', { dataTransfer: dt });
    // The lower half of the FIRST chip: it will land between the two.
    await chips.nth(0).dispatchEvent('dragover', {
      dataTransfer: dt, clientX: target.x + 20, clientY: target.y + target.height * 0.8,
      bubbles: true, cancelable: true,
    });
    await settle();
    const seen = await page.evaluate(() => {
      const zone = document.querySelector('.dc-tool-panel-zones .dc-zone-rows');
      const cs = [...(zone?.querySelectorAll('.dc-chip') ?? [])];
      return {
        marks: cs.map((c) => (c.classList.contains('dc-drop-before') ? 'before'
          : c.classList.contains('dc-drop-after') ? 'after' : '-')),
        bar: cs[1] ? getComputedStyle(cs[1]).boxShadow : '',
        outline: zone ? getComputedStyle(zone).outlineStyle : '',
        zoneClass: zone?.className ?? null,
        dragging: document.querySelector('.dc-dragging, [aria-grabbed=true]') !== null,
      };
    });
    await source.dispatchEvent('dragend', { dataTransfer: dt });
    await menu(['Pivot', 'Clear All Vertical Pivots']).catch(() => {});
    if (seen.marks.join() !== '-,before') throw new Error(`marks ${seen.marks} | ${JSON.stringify(seen)}`);
    if (!/rgb/.test(seen.bar)) throw new Error(`no bar drawn: ${seen.bar}`);
    if (seen.outline === 'dashed') throw new Error('the zone is boxed as well');
    return `bar before chip 2 (${seen.bar.slice(0, 40)}), zone unboxed`;
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
    await settle();
    const hovered = await showing();
    await page.keyboard.press('Escape');
    await settle();
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
    await settle();
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
    await settle();
    const back = await heights();
    if (back.bar < 10) throw new Error('the zones did not come back');
    return `grid ${before.grid} -> ${after.grid}px, and back to ${back.grid}`;
  });

  await check('folding the title bar leaves a lip that restores it', async () => {
    const grid = () => page.evaluate(() => Math.round(
      document.querySelector('.dc-app-middle').getBoundingClientRect().height));
    const before = await grid();
    await page.click('.dc-titlebar-fold');
    await settle();
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
    await settle();
    if (!(await page.locator('.dc-titlebar-menu').count())) {
      throw new Error('the lip did not bring the title bar back');
    }
    return `grid ${before} -> ${after}px, lip ${lipHeight}px`;
  });

  await check('the folds stay in one column at the right: unfolding is where folding was', async () => {
    // User, 2026-09-25: bringing the zones back meant a trip to the far
    // left. Every fold control is now in a column at the right edge,
    // and each way back appears where its fold was (or straight above).
    const cx = (sel) => page.evaluate((q) => {
      const e = document.querySelector(q);
      if (!e) return null;
      const r = e.getBoundingClientRect();
      return Math.round(r.left + r.width / 2);
    }, sel);
    const right = await page.evaluate(() =>
      Math.round(document.querySelector('.dc-titlebar').getBoundingClientRect().right));
    const zoneFold = await cx('.dc-zone-fold');
    if (zoneFold === null || right - zoneFold > 20) throw new Error(`zone fold at ${zoneFold}, bar ends ${right}`);
    await page.click('.dc-zone-fold');
    await settle();
    const back = await cx('.dc-titlebar-zones');
    const titleFold = await cx('.dc-titlebar-fold');
    await page.click('.dc-titlebar-fold');
    await settle();
    const lip = await cx('.dc-titlebar-lip .dc-chevron-icon');
    const backFolded = await cx('.dc-titlebar-zones');
    await page.click('.dc-titlebar-zones');
    await page.click('.dc-titlebar-lip');
    await settle();
    const near = (a, b) => a !== null && b !== null && Math.abs(a - b) <= 2;
    if (!near(back, zoneFold)) throw new Error(`zones back at ${back}, their fold was at ${zoneFold}`);
    if (!near(lip, titleFold)) throw new Error(`lip chevron at ${lip}, the title fold was at ${titleFold}`);
    if (!near(backFolded, zoneFold)) throw new Error(`zones back on the lip at ${backFolded}, not ${zoneFold}`);
    return `zones fold/back at x=${zoneFold}, title fold/lip at x=${titleFold}`;
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
    await settle();
    const shown = () => page.evaluate(() =>
      document.querySelector('.dc-zone-bar')?.hidden === false);
    if (await shown()) throw new Error('the zones did not fold');
    const head = page.locator('.dc-th[data-column]').first();
    await head.dispatchEvent('dragstart', { dataTransfer: null });
    await settle();
    const during = await shown();
    const peeking = await page.evaluate(() =>
      document.querySelector('.dc-zone-bar')?.classList
        .contains('dc-peeking') ?? false);
    await head.dispatchEvent('dragend', { dataTransfer: null });
    await settle();
    const afterwards = await shown();
    await page.click('.dc-titlebar-zones');
    await settle();
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
    await settle();
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
      await settle();
    };
    const properties = async (tab, column) => {
      await reset();
      await page.click('.dc-status-properties');
      await page.waitForSelector(`${O} .dc-editor`, { timeout: 5000 });
      await page.locator(`${O} .dc-editor-tab`, { hasText: tab }).first().click();
      if (column) {
        await page.locator(`${O} .dc-pe-col[data-column="${column}"]`).click();
      }
    };
    const okEditor = async () => {
      const before = await statusNow();
      await page.locator(`${O} .dc-editor-footer button`, { hasText: 'OK' }).click();
      await settle(before);
      await settle();
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
      // the same rows' VALUES and the column's compiler type: whether a cell is negative is
      // a fact about its value, not about its text
      const typed = window.__dataCube.view?.rows.columns.find((c) => c.name === name);
      return {
        texts: cells.map((c) => c?.textContent?.trim() ?? null),
        values: (typed?.values ?? []).slice(0, 6).map((v) => (typeof v === 'bigint' ? String(v) : v)),
        type: typed?.type ?? null,
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
    const openFirst = async () => {
      const s0 = await statusNow();
      await page.locator('.dc-row[aria-expanded=false] .dc-chevron').first().click();
      await settle(s0);
    };
    // On by default, as upstream's: shown ticked, and unticking takes
    // the counts away. By default it counts an OPENED group's next level.
    await control('Show leaf count: shown ticked, untick removes the counts', {
      setup: async () => { await group('region'); await openFirst(); },
      act: general(async () => {
        if (!(await boxOf('Show leaf count').isChecked())) throw new Error('unticked by default');
        await boxOf('Show leaf count').uncheck();
      }),
      expect: (b, a) => (/\(\d+\+?\)$/.test(b.tree[0] ?? '') && !/\(\d+\+?\)$/.test(a.tree[0] ?? '')
        ? null : `tree before ${b.tree[0]} after ${a.tree[0]}`) });
    // Upstream's count, by choice: every group, open or closed, shows
    // how many source rows it holds -- the total's worth between them.
    await control('Count: all rows beneath, on every group', { setup: () => group('region'),
      act: general(() => fieldOf('Count:').locator('select').selectOption('leaves')),
      expect: (b, a) => {
        const counts = a.tree.map((t) => Number(/\((\d+)\)$/.exec(t ?? '')?.[1])).filter(Number.isFinite);
        if (/\(\d+\+?\)$/.test(b.tree[0] ?? '')) return `closed group counted by default: ${b.tree[0]}`;
        return counts.length > 1 ? null : `tree after ${a.tree}`;
      } });
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
      expect: (b, a) => (a.styles.some((x, i) => isNegative(a.values[i] ?? null, a.type) && x?.color !== b.styles[i]?.color) ? null : 'no negative recoloured') });
    await control('Default normal background', { act: general(() => put(sectionOf('Default Colors').locator('input[title="Normal background"]'), '#ffeeaa')),
      expect: (b, a) => (a.styles.some((x, i) => x?.bg !== b.styles[i]?.bg) ? null : 'no background') });
    // (no "Show drag zones" here: the zones fold from their own chevron, the user, 2026-09-30)
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
    await control('Pivot sort direction', { setup: async () => menu(['Pivot', 'Horizontal Pivot on quarter'], { col: await needCol('quarter') }),
      act: column('quarter', () => fieldOf('Pivot sort direction:').locator('select').selectOption('desc')),
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
      expect: (b, a) => (a.styles.some((x, i) => isNegative(a.values[i] ?? null, a.type) && x?.color !== b.styles[i]?.color) ? null : 'no negative recoloured') });
  }

  // ---- Ad Hoc Analysis mode ----------------------------------------------
  await section('Ad Hoc Analysis mode');
  //
  // Against the real planner and engine: every figure below is the
  // database's, and they are checked against EACH OTHER -- a member's
  // total is the sum of its children's, the POV narrows every cell --
  // so no number is written into this file.
  {
    const adhoc = () => page.evaluate(() => {
      const a = window.__dataCube?.adhoc;
      const v = a?.view;
      if (!a) return null;
      return {
        busy: a.busy,
        rows: a.session.grid.rows.map((r) => r.dimension),
        columns: a.session.grid.columns.map((r) => r.dimension),
        pov: a.session.grid.pov,
        labels: v ? v.table.columns[0].values.map((x) => String(x ?? '').trim()) : [],
        // the first measure column's VALUES and compiler type (a bigint crosses as text)
        first: v ? (v.table.columns[v.rowDimensions.length]?.values ?? [])
          .map((x) => (typeof x === 'bigint' ? String(x) : x)) : [],
        firstType: v ? v.table.columns[v.rowDimensions.length]?.type ?? null : null,
      };
    });
    /** Wait until the mode has answered something other than `was`. */
    const changed = async (was) => {
      await page.waitForFunction((prev) => {
        const a = window.__dataCube?.adhoc;
        if (!a || a.busy || !a.view) return false;
        const now = JSON.stringify([a.session.grid, a.view.table.columns.map((c) => c.values)],
          (_k, x) => (typeof x === 'bigint' ? `${x}n` : x));
        return now !== prev;
      }, was, { timeout: 20_000 });
      return adhoc();
    };
    /** What the mode answers now, as text a bigint survives (JSON.stringify throws on one). */
    const answered = () => page.evaluate(() => {
      const a = window.__dataCube?.adhoc;
      return a?.view ? JSON.stringify([a.session.grid, a.view.table.columns.map((c) => c.values)],
        (_k, x) => (typeof x === 'bigint' ? `${x}n` : x)) : '';
    });
    const cellOf = (text) => page.locator('.dc-adhoc-grid .dc-cell', {
      hasText: new RegExp(`^\\s*${text.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\s*$`),
    }).first();
    const enter = async () => {
      await freshCube();
      await burger('Ad Hoc Analysis');
      await page.waitForFunction(() => {
        const a = window.__dataCube?.adhoc;
        return a && !a.busy && a.view;
      }, null, { timeout: 30_000 });
      return adhoc();
    };

    await check('ad hoc analysis: opens on the top member, measures across, the rest on the POV', async () => {
      const a = await enter();
      if (a.rows.length !== 1) throw new Error(`rows ${a.rows}`);
      if (a.columns[0] !== 'Measures') throw new Error(`columns ${a.columns}`);
      if (a.labels.length !== 1 || a.labels[0] !== a.rows[0]) throw new Error(`labels ${a.labels}`);
      const chips = await page.locator('.dc-adhoc-pov-chip').count();
      if (chips !== Object.keys(a.pov).length || chips === 0) throw new Error(`${chips} POV chips`);
      if (!await page.locator('.dc-app-middle').isHidden()) throw new Error("the cube's grid is still shown");
      return `${a.rows[0]} = ${a.first[0]}, ${chips} on the POV`;
    });

    await check('ad hoc analysis: opens on the cube as it stands -- its grouping down the rows', async () => {
      await freshCube();
      const dims = await dimensionNames();
      const on = ['region', 'desk', 'book'].find((n) => dims.includes(n));
      if (!on) throw new Error(`no low-cardinality dimension in ${dims}`);
      await menu(['Pivot', /^Vertical Pivot on/], { col: await needCol(on) });
      const cubeRows = (await state()).rows.length;
      await burger('Ad Hoc Analysis');
      await page.waitForFunction(() => {
        const a = window.__dataCube?.adhoc;
        return a && !a.busy && a.view;
      }, null, { timeout: 30_000 });
      const a = await adhoc();
      if (JSON.stringify(a.rows) !== JSON.stringify([on])) throw new Error(`rows ${a.rows}`);
      if (on in a.pov) throw new Error(`${on} is on the POV as well`);
      // Zoomed once, the members are the groups the cube showed.
      const was = await answered();
      await cellOf(on).dblclick();
      const b = await changed(was);
      return `${on} down the rows; zoomed: ${b.labels.length - 1} members (cube showed ${cubeRows} rows)`;
    });

    await check('ad hoc analysis: double-click zooms in; the children add up to their parent', async () => {
      const a = await enter();
      const was = await answered();
      await cellOf(a.labels[0]).dblclick();
      const b = await changed(was);
      if (b.labels.length < 2) throw new Error(`labels ${b.labels}`);
      // by the measure's compiler type: exact for an Integer or a Decimal
      const total = b.first[0];
      const children = sumTyped(b.first.slice(1), b.firstType);
      if (!closeTyped(total, children, b.firstType)) {
        throw new Error(`top ${String(total)} vs children ${String(children)} (${b.firstType})`);
      }
      return `${b.labels.length - 1} children summing to ${String(total)} (${b.firstType})`;
    });

    await check('ad hoc analysis: Keep Only, Zoom Out and undo from the menu and the keyboard', async () => {
      const a = await enter();
      let was = await answered();
      await cellOf(a.labels[0]).dblclick();
      const b = await changed(was);
      const child = b.labels[1];
      was = await answered();
      await cellOf(child).click({ button: 'right' });
      await page.locator('.dc-menu-item', { hasText: 'Keep Only' }).first().click();
      const c = await changed(was);
      if (JSON.stringify(c.labels) !== JSON.stringify([child])) throw new Error(`kept ${c.labels}`);
      if (!sameTyped(c.first[0], b.first[1], c.firstType)) throw new Error(`${c.first[0]} vs ${b.first[1]}`);
      was = await answered();
      await cellOf(child).click({ button: 'right' });
      await page.locator('.dc-menu-item', { hasText: 'Zoom Out' }).first().click();
      const d = await changed(was);
      if (JSON.stringify(d.labels) !== JSON.stringify([a.labels[0]])) throw new Error(`zoomed out to ${d.labels}`);
      was = await answered();
      await page.keyboard.press(process.platform === 'darwin' ? 'Meta+z' : 'Control+z');
      const e = await changed(was);
      if (JSON.stringify(e.labels) !== JSON.stringify([child])) throw new Error(`undo gave ${e.labels}`);
      return `kept ${child}, zoomed out, undone`;
    });

    await check('ad hoc analysis: a POV member from Member Selection narrows every cell', async () => {
      const a = await enter();
      const chip = page.locator('.dc-adhoc-pov-chip').first();
      const dimension = await chip.getAttribute('data-dimension');
      await chip.click();
      const win = page.locator(`.dc-app-overlay[data-window="Member Selection: ${dimension}"]`);
      await win.waitFor({ timeout: 5000 });
      // The top member is listed first; its first child next, once looked up.
      await win.locator('.dc-adhoc-member').nth(1).waitFor({ timeout: 15_000 });
      const pick = win.locator('.dc-adhoc-member').nth(1);
      const member = (await pick.locator('.dc-adhoc-member-label').textContent())?.trim();
      await pick.locator('.dc-adhoc-member-pick').check();
      const was = await answered();
      await win.locator('.dc-adhoc-members-ok').click();
      const b = await changed(was);
      if (JSON.stringify(b.pov[dimension]) !== JSON.stringify([member])) {
        throw new Error(`POV ${JSON.stringify(b.pov)}`);
      }
      if (!(b.first[0] === null || compareTyped(b.first[0], a.first[0], b.firstType) <= 0)) {
        throw new Error(`${member} gave ${b.first[0]}, more than the whole ${a.first[0]}`);
      }
      const text = await chip.textContent();
      if (!text?.includes(member)) throw new Error(`chip reads ${text}`);
      return `${dimension} = ${member}: ${a.first[0]} -> ${b.first[0]}`;
    });

    await check('ad hoc analysis: Options re-place the answers, and Exit restores the cube', async () => {
      const a = await enter();
      let was = await answered();
      await cellOf(a.labels[0]).dblclick();
      const b = await changed(was);
      await page.locator('.dc-adhoc-tool', { hasText: 'Options...' }).click();
      const win = page.locator('.dc-app-overlay[data-window="Ad Hoc Options"]');
      await win.waitFor({ timeout: 5000 });
      await win.locator('input[name="dc-adhoc-indentation"][value="none"]').check();
      was = await answered();
      await win.locator('.dc-adhoc-options-ok').click();
      const c = await changed(was);
      const raw = await page.evaluate(() => window.__dataCube.adhoc.view.table.columns[0].values);
      if (raw.some((v) => /^\s/.test(String(v)))) throw new Error(`still indented: ${JSON.stringify(raw)}`);
      if (stamp(c.first) !== stamp(b.first)) throw new Error('the figures changed');
      await page.locator('.dc-adhoc-tool', { hasText: 'Exit' }).click();
      await page.waitForFunction(() => !window.__dataCube?.adhoc, null, { timeout: 5000 });
      if (await page.locator('.dc-adhoc').count()) throw new Error('the mode is still on screen');
      if (await page.locator('.dc-app-middle').isHidden()) throw new Error("the cube's grid did not come back");
      const rows = await page.locator('.dc-app-grid .dc-row').count();
      if (!rows) throw new Error('no rows after Exit');
      return `unindented, ${rows} cube rows back`;
    });
  }

  // LAST: it turns the page into a board, which every check above assumes it is not.
  await check('New > Source: an example opens as a grid of its own, at the row limit, said "the first 1,000 of N"', async () => {
    await page.click('.dc-titlebar-menu');
    await page.locator('.dc-menu .dc-menu-item', { has: page.locator(':scope > .dc-menu-label:text-is("New")') }).hover();
    await page.locator('.dc-menu .dc-menu-item', { has: page.locator(':scope > .dc-menu-label:text-is("Data Source\u2026")') }).click();
    await page.locator('.dc-picker').waitFor({ timeout: 5000 });
    await page.locator('.dc-picker-tab[data-section="examples"]').click();
    await page.locator('.dc-picker-card[data-example="trades"]').click();
    await page.fill('.dc-picker-rows', '1500');
    await page.click('.dc-picker-choice .dc-primary');
    await page.locator('.dc-picker').waitFor({ state: 'detached', timeout: 60_000 });
    const tile = page.locator('[data-tile^="grid-"]').first();
    await tile.locator('.dc-row').first().waitFor({ timeout: 60_000 });
    const head = (await tile.locator('.dc-tile-cube').textContent()) ?? '';
    if (!/sample-trades\.csv/.test(head)) throw new Error(`its header says "${head}"`);
    await page.waitForFunction(() => /first 1,000 of 1,500 rows/.test(
      document.querySelector('[data-tile^="grid-"] .dc-status-warning')?.textContent ?? ''), null, { timeout: 30_000 });
    const main = await page.locator('[data-tile="grid"] .dc-row').count();
    if (!main) throw new Error('the cube\'s own grid lost its rows');
    return `"${head.trim()}": the first 1,000 of 1,500 rows; the cube's grid still shows ${main} rows`;
  });

  // A SAVED QUERY (src/saved-queries.ts): the store's records listed, a graph fetch refused in the
  // window with its reason, a data space's query opened as a grid -- its context resolved, its
  // enumeration read as names, the rows its README says (fixtures/saved-queries: Sells, 5 rows).
  await check('New > Data Source > Saved queries: a data space query opens as a grid, its enumeration as names', async () => {
    const pick = async () => {
      await page.click('.dc-titlebar-menu');
      await page.locator('.dc-menu .dc-menu-item', { has: page.locator(':scope > .dc-menu-label:text-is("New")') }).hover();
      await page.locator('.dc-menu .dc-menu-item', { has: page.locator(':scope > .dc-menu-label:text-is("Data Source\u2026")') }).click();
      await page.locator('.dc-picker').waitFor({ timeout: 5000 });
      await page.locator('.dc-picker-tab[data-section="saved"]').click();
      await page.locator('.dc-picker-row[data-query]').first().waitFor({ timeout: 10_000 });
    };
    await pick();
    const listed = await page.$$eval('.dc-picker-row[data-query]', (els) => els.map((e) => e.dataset.query).sort());
    const want = ['fixture-data-space-context', 'fixture-default-parameter-values', 'fixture-explicit-context', 'fixture-graph-fetch'];
    if (listed.join() !== want.join()) throw new Error(`listed ${listed.join(', ')}`);
    await page.click('.dc-picker-row[data-query="fixture-graph-fetch"]');
    await page.locator('.dc-picker-status.dc-failed').waitFor({ timeout: 30_000 });
    const why = (await page.textContent('.dc-picker-status')) ?? '';
    if (!/objects, not rows/.test(why)) throw new Error(`the graph fetch said "${why}"`);
    const tiles = await page.locator('[data-tile^="grid-"]').count();
    await page.click('.dc-picker-row[data-query="fixture-data-space-context"]');
    await page.locator('.dc-picker').waitFor({ state: 'detached', timeout: 60_000 });
    const tile = page.locator('[data-tile^="grid-"]').nth(tiles);
    await tile.locator('.dc-row').first().waitFor({ timeout: 60_000 });
    const head = (await tile.locator('.dc-tile-cube').textContent()) ?? '';
    if (!/Sells/.test(head)) throw new Error(`its header says "${head}"`);
    const cols = (await tile.locator('.dc-th').allTextContents()).map((t) => t.trim());
    if (cols.join() !== 'Trade Id,Side,Quantity') throw new Error(`columns ${cols.join(', ')}`);
    const sides = await tile.locator('.dc-row').evaluateAll((rows) => rows.map((r) => r.querySelectorAll('.dc-cell')[1]?.textContent?.trim()));
    if (sides.length !== 5 || sides.some((v) => v !== 'SELL')) throw new Error(`Side read ${JSON.stringify(sides)}`);
    return `4 listed; the graph fetch refused ("${why.trim()}"); "Sells": 5 rows, Side as SELL`;
  });
} catch (e) {
  record('the run itself', false, String(e.message ?? e).split('\n')[0]);
} finally {
  await browser.close();
  closeServer();
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
// NO_WASM: the in-tab planner was never asked for, or the drop-in claim is false
if (process.env.NO_WASM) {
  if (wasmAsked.length) bad.push({ name: 'the in-tab planner was never needed (NO_WASM)', detail: `asked for ${[...new Set(wasmAsked)].join(', ')} ${wasmAsked.length}x` });
  else console.log(`\nNO_WASM: the in-tab planner's files were never asked for (planner: ${PLANNER || 'local'})`);
}
// CANARY, ENGINE DEFECT S23 (docs/SEMANTICS_REGISTER.md): legend-engine types a BIT column TinyInt,
// and DataCube reads it Boolean (engine-client/src/relation-type.ts). The day engine answers Boolean itself,
// this fails: delete the compensation and the register row.
if (PLANNER === 'engine') {
  const { readFile } = await import('node:fs/promises');
  const engineUrl = JSON.parse(await readFile(new URL('./config.json', import.meta.url), 'utf8')).legendEngine;
  const model = '###Relational\nDatabase c::DB ( Table t ( flag BIT ) )\n';
  const r = await fetch(`${engineUrl}/api/pure/v1/compilation/lambdaRelationType`, {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ lambda: { _type: 'lambda', parameters: [], body: [{ _type: 'classInstance', type: '>', value: { path: ['c::DB', 't'] } }] },
      model: { _type: 'text', code: model } }),
  });
  const said = r.ok ? (await r.json()).columns?.[0]?.genericType?.rawType?.fullPath : `HTTP ${r.status}`;
  if (said === 'meta::pure::precisePrimitives::TinyInt') {
    console.log('\nCANARY S23: legend-engine still types BIT TinyInt -- DataCube\'s compensation still needed');
  } else {
    bad.push({ name: 'CANARY S23: legend-engine types BIT TinyInt', detail: `it now says ${said}: delete DataCube's BIT compensation (src/relation-type.ts) and register row S23` });
  }
}
if (bad.length) {
  console.log(`\nBROKEN (${bad.length}):`);
  for (const r of bad) console.log(`  ${r.name} — ${r.detail}`);
}
// TIMINGS=all: every check's time, not only the slowest twelve
const slow = [...timings].sort((a, b) => b[1] - a[1]).slice(0, process.env.TIMINGS === 'all' ? timings.length : 12);
console.log(`\nslowest checks of ${timings.length}`
  + ` (${Math.round((Date.now() - STARTED) / 1000)}s total):`);
for (const [name, ms] of slow) {
  console.log(`  ${String(ms).padStart(6)}ms  ${name}`);
}

console.log(bad.length
  ? `\n!!! ${bad.length} features are broken !!!`
  : '\n*** every feature checked works ***');
process.exit(bad.length ? 1 : 0);
