// The SHAPE matrix: every sample the product ships, loaded for real.
//
// The feature sweep drives one well-behaved file through many
// operations. This drives many badly-behaved files through a few
// operations, because that is the axis the hand-found bugs actually
// lived on: a flat file with no measure column grouped without a
// GROUP BY, a quoted column name broke every query, a header with a
// `"` in it made a file unusable, epoch milliseconds rendered as
// 1,612,828,800,000. None of those are about which menu entry was
// clicked; they are about what the data looked like.
//
// Each sample is loaded through the source picker's <input type="file">, then the
// grid invariants run, then it is grouped by its first dimension from
// the columns panel -- a different code path from the context menu, on
// purpose -- and the invariants run again.
//
//   bazel run //datacube:verify_smoke
//   ONLY=nulls bazel run //datacube:verify_smoke        (one sample)
//   ROWS=200 bazel run //datacube:verify_smoke          (smaller files, faster)

// first: points Playwright at the Chromium Bazel fetched (as a browser_test; a no-op under bazel run)
import '../../tools/browser/pinned-chromium.mjs';
import { mkdtemp, readFile, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { extname, join } from 'node:path';
import { chromium } from 'playwright';

import { SAMPLES, sampleFileName } from '../src/samples.ts';
import { kindOf } from '../src/snapshot.ts';
import { TEMPORAL_TYPES, gridInvariants } from './grid-invariants.mjs';
import { serve, siteRoot } from './harness.mjs';

const ROOT = siteRoot();
const ONLY = process.env.ONLY;
/**
 * Each sample's OWN size, capped for time -- not a flat small number.
 *
 * SCALE IS PART OF THE SHAPE. The header-crush fault only appears
 * once the body is enormously taller than its container: `flex-shrink`
 * distributes the overflow in proportion to each item's basis, so the
 * 24px header loses almost nothing against a 8,000px body and
 * practically all of itself against a 500,000px one. Running the
 * `tall` sample at 400 rows let a reintroduced crush pass, and at
 * 25,000 rows it was caught immediately.
 *
 * So the default is the sample's declared size, capped only so the
 * 200,000-row case does not dominate the run. `ROWS` overrides it for
 * a quick pass, and says what that costs.
 */
const ROWS = process.env.ROWS ? Number(process.env.ROWS) : undefined;
const ROW_CAP = 30_000;
if (ROWS !== undefined && ROWS < 20_000) {
  console.log(`note: ROWS=${ROWS} — layout faults that need a very tall`
    + ' result (the header crush) will not reproduce at this size');
}

const { port, close: closeServer } = await serve(ROOT);

// under a test, Bazel's own per-test temp directory, never the host's shared one
const dir = await mkdtemp(join(process.env.TEST_TMPDIR ?? tmpdir(), 'dc-smoke-'));
// a test's artifacts (the summary, a screenshot of a failing shape) go where Bazel keeps them
const OUT = process.env.TEST_UNDECLARED_OUTPUTS_DIR;
const wanted = SAMPLES.filter((s) => !ONLY || s.id.includes(ONLY));
if (!wanted.length) {
  console.error(`no sample matches ${ONLY}; have`
    + ` ${SAMPLES.map((s) => s.id).join(', ')}`);
  process.exit(2);
}

const browser = await chromium.launch();
console.log(`browser: ${browser.browserType().name()} ${browser.version()}`
  + ` from ${process.env.PLAYWRIGHT_BROWSERS_PATH ?? "Playwright's default cache"}`);
const context = await browser.newContext({
  viewport: { width: 1400, height: 900 },
});
const page = await context.newPage();
let pageErrors = [];
page.on('pageerror', (e) => pageErrors.push(e.message));

/**
 * The file opened in place of the cube, as a person does now: New ▸ Blank Page, "Add a data source",
 * the picker's Files section and ITS file input (src/ui/source-picker.ts). The page no longer has an
 * input of its own for data files -- the one left is the saved cubes' import.
 */
async function openThroughPicker(file) {
  await page.click('.dc-titlebar-menu');
  await page.locator('.dc-menu .dc-menu-item', { has: page.locator(':scope > .dc-menu-label:text-is("New")') }).hover();
  await page.locator('.dc-menu .dc-menu-item', { has: page.locator(':scope > .dc-menu-label:text-is("Blank Page")') }).click();
  await page.locator('.dc-blank').waitFor({ timeout: 10_000 });
  await page.click('.dc-blank .dc-primary');
  await page.locator('.dc-picker').waitFor({ timeout: 10_000 });
  await page.locator('.dc-picker-tab[data-section="files"]').click();
  await page.setInputFiles('.dc-picker-file', file);
}

const failures = [];
function fail(sample, where, detail) {
  failures.push({ sample, where, detail });
  console.log(`    BAD  ${where}: ${detail}`);
}

/** Run the shared invariants and attribute breaks to this state. */
async function invariants(sample, where) {
  for (const b of await page.evaluate(gridInvariants, TEMPORAL_TYPES)) {
    fail(sample, `${where} — invariant`, b);
  }
  for (const e of pageErrors) {
    fail(sample, `${where} — page error`, e.split('\n')[0]);
  }
  pageErrors = [];
}

const summary = [];
try {
  for (const sample of wanted) {
    // under its OWN format: a JSONL sample written as .csv was sniffed as CSV, so the shape
    // under test was not the one the picker offers
    const file = join(dir, sampleFileName(sample));
    const rows = ROWS ?? Math.min(sample.defaultRows ?? 2000, ROW_CAP);
    await writeFile(file, sample.build(rows), 'utf8');
    console.log(`\n${sample.id} — ${sample.label} (${rows} rows)`);

    // A clean page per sample. Loading one file over another is its
    // own code path, tested in verify-upload; here each shape starts
    // from the same place so a break belongs to that shape.
    pageErrors = [];
    await page.goto(`http://127.0.0.1:${port}/demo/index.html`);
    await page.waitForSelector('.dc-row', { timeout: 90_000 });
    // THE FILE'S answer, not the built-in cube's: its status line already reads "... rows",
    // so waiting for /rows/ alone read a slower ingest (the JSONL sample) before it landed and
    // checked the built-in cube in its place. Wait for the line to CHANGE.
    const before = await page.evaluate(() =>
      document.querySelector('.dc-status-timing')?.textContent ?? '');
    await openThroughPicker(file);
    await page.waitForFunction(
      (was) => {
        const line = document.querySelector('.dc-status-timing')?.textContent ?? '';
        return (line !== was && /rows/.test(line))
          || /could not|error/i.test(document.getElementById('status')?.textContent ?? '');
      },
      before, { timeout: 90_000 },
    ).catch(() => {});
    await page.waitForTimeout(400);

    const loaded = await page.evaluate(() => ({
      status: `${document.querySelector('.dc-status-timing')?.textContent ?? ''}`
        + ` | ${document.getElementById('status')?.textContent ?? ''}`,
      rows: document.querySelectorAll('.dc-row').length,
      headers: [...document.querySelectorAll('.dc-th[data-column]')]
        .map((e) => e.dataset.column),
      panel: [...document.querySelectorAll('.dc-tool-panel-row')]
        .map((e) => e.dataset.column),
    }));
    console.log(`    loaded: ${loaded.status.trim().slice(0, 60)}`);

    // THE FILE MUST OPEN. A sample this product offers and cannot
    // read is a bug in one or the other, never acceptable as "that
    // shape is hard".
    if (!loaded.rows) {
      fail(sample.id, 'loading', `no rows rendered (${loaded.status})`);
      summary.push({ id: sample.id, ok: false });
      continue;
    }
    if (!loaded.headers.length) {
      fail(sample.id, 'loading', 'no column headers');
    }
    await invariants(sample.id, 'loading');

    // THE SOURCE'S TYPES, as the compiler gave them to the cube: printed, so a sniffer
    // change shows in the log, and the ground for every choice below.
    const schema = await page.evaluate(() => window.__dataCube.snapshot.columns
      .map((c) => ({ name: c.name, type: c.type ?? null, kind: c.kind ?? null })));
    console.log(`    types: ${schema.map((c) => `${c.name}:${c.type}`).join(', ')}`);
    for (const c of schema.filter((c) => c.type === null)) {
      fail(sample.id, 'loading', `the compiler did not type column ${c.name}`);
    }

    // GROUP BY THE FIRST DIMENSION, from the panel. Double-click is
    // the keyboard-and-trackpad path and a different route into the
    // same snapshot change than the context menu, so a fault in one
    // does not hide a fault in the other. The dimension is chosen from
    // the typed columns (its kind, which the type defaults), never by
    // reading the panel's classes.
    const dimension = schema.find((c) => kindOf({ name: c.name, type: c.type, ...(c.kind ? { kind: c.kind } : {}) }) === 'dimension');
    const groupable = dimension === undefined ? null
      : page.locator(`.dc-tool-panel-row[data-column="${dimension.name.replace(/["\\]/g, '\\$&')}"]`);
    if (groupable && await groupable.count()) {
      const name = dimension.name;
      await groupable.first().dblclick();
      await page.waitForTimeout(900);
      // the group KEYS as the database returned them (each first-level row's path), not the
      // cells' rendered text: two values a format shows alike are still two groups
      const grouped = await page.evaluate(() => {
        const view = window.__dataCube.view;
        return {
          sql: document.getElementById('sql')?.textContent ?? '',
          pure: document.getElementById('pure')?.textContent ?? '',
          rows: (view?.treeRows ?? []).filter((r) => r.level === 1)
            .map((r) => (r.path[0] === null ? '\u0000null' : String(r.path[0]))),
          headers: document.querySelectorAll('.dc-th[data-column]').length,
        };
      });
      console.log(`    grouped by ${name}: ${grouped.rows.length} rows,`
        + ` ${grouped.headers} columns`);

      // GROUPING MUST GROUP. The exact fault found by hand: the Pure
      // said groupBy, the SQL had no GROUP BY, and the grid showed
      // one row per SOURCE row -- "AMER" repeated down the screen.
      if (!/GROUP BY/i.test(grouped.sql)) {
        fail(sample.id, 'grouping',
          `no GROUP BY in the SQL: ${grouped.sql.slice(0, 90)}`);
      }
      const distinct = new Set(grouped.rows).size;
      if (grouped.rows.length && distinct !== grouped.rows.length) {
        fail(sample.id, 'grouping',
          `${grouped.rows.length} rows but only ${distinct} distinct`
          + ' — it did not group');
      }
      // AND MUST KEEP ITS COLUMNS. Grouping once projected every
      // other column away, turning a twelve-column cube into one
      // blank one.
      if (grouped.headers < 2 && loaded.headers.length >= 2) {
        fail(sample.id, 'grouping',
          `kept ${grouped.headers} of ${loaded.headers.length} columns`);
      }
      await invariants(sample.id, 'grouping');
    }

    // A HORIZONTAL SCROLL, because the header is a separate element
    // and only a scrolled grid can show it drifting.
    const range = await page.evaluate(() => {
      const sc = document.querySelector('.dc-scroller');
      if (!sc) return 0;
      sc.scrollLeft = sc.scrollWidth;
      return Math.round(sc.scrollWidth - sc.clientWidth);
    });
    if (range > 20) {
      await page.evaluate(() => new Promise((r) =>
        requestAnimationFrame(() => requestAnimationFrame(r))));
      await invariants(sample.id, `scrolled ${range}px right`);
    }

    // AND A NARROW WINDOW. Layout faults hide at a comfortable width:
    // the header collapse only showed on a tall result, and the
    // sidebar rail only broke the page once it had room to.
    await page.setViewportSize({ width: 640, height: 720 });
    await page.waitForTimeout(250);
    await invariants(sample.id, 'a 640px window');
    await page.setViewportSize({ width: 1400, height: 900 });
    await page.waitForTimeout(150);

    const broke = failures.filter((f) => f.sample === sample.id).length;
    if (broke && OUT) await page.screenshot({ path: join(OUT, `${sample.id}.png`) });
    summary.push({ id: sample.id, ok: broke === 0 });
  }
} catch (e) {
  fail('the run', 'itself', String(e.message ?? e).split('\n')[0]);
} finally {
  await browser.close();
  closeServer();
}

if (OUT) {
  await writeFile(join(OUT, 'summary.json'),
    JSON.stringify({ summary, failures }, null, 2), 'utf8');
}
console.log('\n---');
for (const s of summary) {
  console.log(`  ${s.ok ? 'ok  ' : 'BAD '} ${s.id}`);
}
const good = summary.filter((s) => s.ok).length;
console.log(`\n${good}/${summary.length} shapes are sound`);
if (failures.length) {
  console.log(`\n${failures.length} break(s):`);
  for (const f of failures) {
    console.log(`  ${f.sample} / ${f.where}\n    ${f.detail}`);
  }
}
console.log(failures.length
  ? `\n!!! ${failures.length} breaks across ${summary.length - good} shapes !!!`
  : '\n*** every shape loads, groups, scrolls and holds its invariants ***');
process.exit(failures.length ? 1 : 0);
