// Open a file the way a user does: pick it in the page.
//
// No query string, no hosting, no model authored by hand. The page
// loads with its demo data, a file is chosen through the real
// <input type="file">, and the cube must come back showing THAT
// file's columns and rows — with the Pure model written from a
// schema DuckDB sniffed, not from anything committed here.
//
// Checked against DuckDB's own answer for the same file, so this
// fails if the inferred model, the planner or the grid is wrong
// rather than only if the page is blank.
//
//   DATA=/abs/path/trades.csv EXPECT_ROWS=50000 npm run verify:upload

import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
import { extname, join, normalize } from 'node:path';
import { chromium } from 'playwright';

const ROOT = new URL('..', import.meta.url).pathname;

// A function declaration, not a const: this is used above its
// definition and a const there is a temporal-dead-zone error
// that masks the very failure it was added to report.
function bad(m) { console.log(`FAIL: ${m}`); failed = true; }
let failed = false;
const DATA = process.env.DATA;
const EXPECT_ROWS = Number(process.env.EXPECT_ROWS ?? 0);
const EXPECT_COLS = Number(process.env.EXPECT_COLS ?? 0);
if (!DATA) {
  console.error('set DATA=/abs/path/to/file.csv (or .parquet)');
  process.exit(2);
}

const TYPES = {
  '.html': 'text/html', '.js': 'text/javascript', '.wasm': 'application/wasm',
  '.pure': 'text/plain', '.css': 'text/css',
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

const browser = await chromium.launch();
const page = await browser.newPage();
const errs = [];
page.on('pageerror', (e) => errs.push(`pageerror: ${e.message}`));
if (process.env.DEBUG) {
  page.on('console', (m) => console.log(`  [page ${m.type()}] ${m.text()}`));
}

try {
  await page.goto(`http://127.0.0.1:${port}/demo/index.html`,
    { waitUntil: 'load', timeout: 120_000 });
  await page.waitForFunction(
    () => document.querySelectorAll('.dc-row').length > 0, { timeout: 120_000 });

  // The control must be VISIBLE, not merely present: an upload
  // feature nobody can reach is not a feature.
  const visible = await page.isVisible('#uploadbar');
  if (!visible) {
    console.log('FAIL: the upload bar is not visible');
    failed = true;
  }

  const before = await page.$$eval('.dc-row', (els) =>
    els.map((el) => el.querySelector('.dc-cell')?.textContent?.trim() ?? ''));
  console.log(`before: ${JSON.stringify(before)}`);

  await page.setInputFiles('#uploadfile', DATA);
  // Wait for an OUTCOME, not for the filename: the in-progress
  // message ("reading trades.csv…") contains it too, so matching on
  // the name reports success while the work is still running.
  await page.waitForFunction(
    () => /rows,|could not open/.test(
      document.getElementById('uploadnote')?.textContent ?? ''),
    undefined, { timeout: 120_000 });

  const note = (await page.textContent('#uploadnote')) ?? '';
  console.log(`note: ${note}`);
  if (/could not open/.test(note)) {
    console.log('FAIL: opening the file errored');
    failed = true;
  }

  // The note is written as soon as the model is swapped; the rebuilt
  // cube's FIRST QUERY is asynchronous, so the grid is still the old
  // one for a moment. Wait for the Pure panel to name the new source.
  if (!/could not open/.test(note)) {
    await page.waitForFunction(
      () => /local::DB/.test(document.getElementById('pure')?.textContent ?? ''),
      undefined, { timeout: 60_000 },
    ).catch(() => { console.log('FAIL: the cube never re-queried'); failed = true; });
  }

  console.log(`status after opening: ${await page.textContent('#status')}`);
  console.log(`#app children: ${await page.$$eval('#app > *',
    (els) => els.map((e) => e.className || e.tagName).join(', '))}`);
  const rows = await page.$$eval('.dc-row', (els) =>
    els.map((el) => [...el.querySelectorAll('.dc-cell')]
      .map((c) => c.textContent?.trim() ?? '')));
  console.log(`rows rendered after opening: ${rows.length}`);
  console.log(`first row: ${JSON.stringify(rows[0]?.slice(0, 8))}`);

  const title = await page.textContent('#pure');
  console.log(`pure now reads from: `
    + `${(title ?? '').split('\n')[0]?.slice(0, 80)}`);

  if (rows.length === 0) {
    console.log('FAIL: no rows after opening the file');
    failed = true;
  }
  // The demo's own AMER/APAC/EMEA must be GONE: same rows would mean
  // the page never actually switched models.
  if (rows.some((r) => /AMER|APAC|EMEA/.test(r[0] ?? ''))) {
    console.log('FAIL: still showing the demo data');
    failed = true;
  }
  // A year must render as 2019, not 2,019. The inference marks
  // key-like numerics as dimensions and the demo turns thousands
  // separators off for them; without that the grid looks broken in a
  // way that reads as bad data.
  if (rows[0]?.some((c) => /^\d,\d{3}$/.test(c))) {
    console.log(`FAIL: a key-like number got thousands separators: `
      + JSON.stringify(rows[0]));
    failed = true;
  }

  // HEADERS MUST BE VISIBLE, not merely present.
  //
  // They were in the DOM with the right labels while the user saw
  // none: .dc-head is a flex item and defaulted to flex-shrink:1, so
  // a tall result crushed it to 0.015625px and `overflow:hidden`
  // clipped every 24px cell. Counting header cells would have passed.
  // The demo cube has three rows and never overflows, which is why
  // only an uploaded file showed it.
  const head = await page.evaluate(() => {
    const h = document.querySelector('.dc-head');
    const c = document.querySelector('.dc-th');
    return {
      headH: h ? Math.round(h.getBoundingClientRect().height) : -1,
      cellH: c ? Math.round(c.getBoundingClientRect().height) : -1,
      labels: [...document.querySelectorAll('[role=columnheader]')]
        .map((e) => e.textContent?.trim() ?? '').filter(Boolean).length,
    };
  });
  console.log(`header: ${head.headH}px tall, ${head.labels} labels`);
  if (head.labels === 0) bad('no column headers at all');
  if (head.headH < head.cellH) {
    bad(`the header is ${head.headH}px but its cells are ${head.cellH}px`
      + ' — collapsed and clipped');
  }

  // A date column must render as a DATE. It arrives as epoch
  // milliseconds and the formatter only date-formats a Date
  // instance, so the failure mode is "1,612,828,800,000" -- and
  // then, once converted at UTC midnight and shown in a western
  // zone, the day before the one in the file.
  const badCells = rows[0]?.filter((c) => /^[\d,]{10,}$/.test(c)
    || /Invalid Date/.test(c)) ?? [];
  if (badCells.length) {
    bad(`a temporal column did not render as a date: `
      + JSON.stringify(badCells));
  }

  if (EXPECT_ROWS) {
    const m = /([\d,]+) rows/.exec(note);
    const got = m ? Number(m[1].replace(/,/g, '')) : -1;
    const ok = got === EXPECT_ROWS;
    console.log(`  ${ok ? 'MATCH ' : 'DIFFER'} row count: `
      + `${got} vs duckdb ${EXPECT_ROWS}`);
    if (!ok) failed = true;
  }
  if (EXPECT_COLS) {
    const m = /(\d+) columns/.exec(note);
    const got = m ? Number(m[1]) : -1;
    const ok = got === EXPECT_COLS;
    console.log(`  ${ok ? 'MATCH ' : 'DIFFER'} column count: `
      + `${got} vs duckdb ${EXPECT_COLS}`);
    if (!ok) failed = true;
  }
} catch (e) {
  console.log(`FAIL: ${e.message.split('\n')[0]}`);
  failed = true;
} finally {
  if (errs.length) {
    console.log(`page errors: ${errs.slice(0, 4).join(' | ')}`);
    failed = true;
  }
  await browser.close();
  server.close();
}

console.log(failed
  ? '\n!!! opening a file did NOT work !!!'
  : '\n*** picked a file in the page, schema inferred, cube rebuilt ***');
process.exit(failed ? 1 : 0);
