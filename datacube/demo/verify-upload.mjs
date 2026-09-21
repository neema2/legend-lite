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

  // OPEN THE DATA PANEL. The page is nothing but the grid now and the
  // upload control lives in a window the title bar menu opens, so
  // reaching it is the first thing a person does and the first thing
  // this does.
  await page.click('.dc-titlebar-menu');
  await page.waitForSelector('.dc-menu', { timeout: 10_000 });
  await page.locator('.dc-menu-item', { hasText: 'Data' }).first().click();
  await page.waitForTimeout(400);

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

  console.log('status after opening: '
    + `${await page.textContent('.dc-status-timing')}`);
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

  // THE HEADER MUST FOLLOW THE COLUMNS SIDEWAYS.
  //
  // The header is a SIBLING of the horizontal scroller, so scrolling
  // the body cannot move it on its own -- the labels sat still while
  // their columns slid away underneath. What is asserted is
  // ALIGNMENT, not movement: the last column's header and the last
  // column's cell must share an x position before and after a
  // horizontal scroll. "The header moved" would also pass if it moved
  // by the wrong amount, which is the same bug one pixel smaller.
  //
  // The viewport is narrowed first so the grid certainly overflows,
  // and the overflow is asserted -- a check that silently finds
  // nothing to scroll proves nothing.
  await page.setViewportSize({ width: 620, height: 720 });
  await page.waitForTimeout(150);

  const hProbe = () => page.evaluate(() => {
    const sc = document.querySelector('.dc-scroller');
    const ths = [...document.querySelectorAll('.dc-th[data-column]')];
    const tds = [...document.querySelectorAll('.dc-row')][0]
      ?.querySelectorAll('.dc-cell') ?? [];
    const i = Math.min(ths.length, tds.length) - 1;
    if (!sc || i < 0) return { range: -1, i };
    return {
      range: Math.round(sc.scrollWidth - sc.clientWidth),
      left: Math.round(sc.scrollLeft),
      i,
      thX: Math.round(ths[i].getBoundingClientRect().left),
      tdX: Math.round(tds[i].getBoundingClientRect().left),
    };
  });

  const hBefore = await hProbe();
  if (hBefore.range < 100) {
    bad(`the grid has only ${hBefore.range}px of horizontal overflow at a`
      + ' 620px viewport, so the scroll check proves nothing');
  } else {
    const shift = Math.min(300, hBefore.range);
    await page.evaluate((d) => {
      document.querySelector('.dc-scroller').scrollLeft = d;
    }, shift);
    // Two frames: the sync runs in a requestAnimationFrame.
    await page.evaluate(() => new Promise((r) =>
      requestAnimationFrame(() => requestAnimationFrame(r))));
    const hAfter = await hProbe();
    console.log(`h-scroll: column ${hAfter.i} header x=${hAfter.thX}`
      + ` cell x=${hAfter.tdX} after scrolling ${hAfter.left}px`
      + ` of ${hAfter.range}px`);
    if (Math.abs(hBefore.thX - hBefore.tdX) > 2) {
      bad(`header and body are misaligned hBefore scrolling:`
        + ` ${hBefore.thX} vs ${hBefore.tdX}`);
    }
    if (hAfter.tdX >= hBefore.tdX - 10) {
      bad(`the body did not scroll: cell x went ${hBefore.tdX} ->`
        + ` ${hAfter.tdX}`);
    }
    if (Math.abs(hAfter.thX - hAfter.tdX) > 2) {
      bad(`the header did not follow the columns: after scrolling`
        + ` ${hAfter.left}px the header cell is at ${hAfter.thX} and its`
        + ` column is at ${hAfter.tdX}`);
    }
  }

  // Widen it again so the drag below fails only for reasons to do
  // with dragging -- but leave the grid SCROLLED SIDEWAYS. Replacing
  // the column model is the second place the header can fall out of
  // step with the body, and it is checked after the drag.
  await page.setViewportSize({ width: 1280, height: 800 });
  await page.waitForTimeout(150);

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

  // GROUPING MUST ACTUALLY GROUP.
  //
  // Dragging a column into the row zone on a cube with no measures
  // emitted a plain select: the serialiser only wrote a groupBy when
  // there was something to aggregate. The grid then showed one row
  // per SOURCE row -- "AMER" repeated down the screen -- with no
  // GROUP BY in the generated SQL, which is wrong data, not a
  // cosmetic fault. Checking the row COUNT is what catches it; the
  // grid looked populated either way.
  const region = page.locator('.dc-th.dc-draggable', { hasText: 'region' });
  if (await region.count()) {
    // THE ROWS HALF, BY NAME. This dropped on `[class*=zone]` first
    // -- which matched the bar's wrapper once the two zones became
    // two halves of one bar, so the drop landed at the boundary and
    // region was PIVOTED rather than grouped. The check reported
    // three failures in the product and the product was right.
    // THE BAR'S zone, said explicitly: the sidebar has one of its
    // own now, so a bare `.dc-zone-rows` matches two elements and
    // Playwright refuses to guess.
    await region.first().dragTo(page.locator('.dc-zone-bar .dc-zone-rows'));
    await page.waitForFunction(
      () => /groupBy|could not/.test(
        document.getElementById('pure')?.textContent ?? ''),
      undefined, { timeout: 30_000 },
    ).catch(() => bad('grouping never re-queried'));
    const grouped = await page.evaluate(() => ({
      pure: document.getElementById('pure')?.textContent ?? '',
      sql: document.getElementById('sql')?.textContent ?? '',
      rows: [...document.querySelectorAll('.dc-row')].map(
        (r) => r.querySelector('.dc-cell')?.textContent?.trim() ?? ''),
      labels: [...document.querySelectorAll('[role=columnheader]')]
        .map((e) => e.textContent?.trim()).filter(Boolean),
    }));
    console.log(`grouped: ${grouped.rows.length} rows `
      + `${JSON.stringify(grouped.rows.slice(0, 4))}`);
    if (!/groupBy\(~\[/.test(grouped.pure)) {
      bad(`no groupBy in the Pure: ${grouped.pure.slice(0, 120)}`);
    }
    if (!/GROUP BY/i.test(grouped.sql)) {
      bad(`no GROUP BY in the SQL: ${grouped.sql.slice(0, 120)}`);
    }
    // AND THE HEADER IS STILL WITH ITS COLUMNS.
    //
    // Grouping replaces the column model, which replaces the header's
    // children -- and that resets the header viewport's own scroll
    // offset while the body stays where the user left it. The offsets
    // must match; the grid is still scrolled sideways from the check
    // above, so 0 === 0 would not be what is being asserted here.
    const offs = await page.evaluate(() => {
      const h = document.querySelector('.dc-head');
      const sc = document.querySelector('.dc-scroller');
      return { head: Math.round(h.scrollLeft), body: Math.round(sc.scrollLeft) };
    });
    console.log(`after rebuild: header offset ${offs.head},`
      + ` body offset ${offs.body}`);
    if (offs.body === 0) {
      bad('the body lost its horizontal scroll across the rebuild, so'
        + ' the header-offset check below proves nothing');
    }
    if (offs.head !== offs.body) {
      bad(`rebuilding the header snapped it back: header at ${offs.head},`
        + ` body at ${offs.body}`);
    }

    const distinct = new Set(grouped.rows).size;
    if (grouped.rows.length !== distinct) {
      bad(`grouped rows repeat: ${grouped.rows.length} rows but only `
        + `${distinct} distinct — it did not group`);
    }
    // GROUPING MUST NOT DROP COLUMNS. DataCube aggregates every
    // selected column that is not a group key (_groupByAggCols) --
    // sum for measures, uniqueValueOnly for the rest -- rather than
    // projecting them away. An earlier fix grouped correctly and
    // left a single blank column, which is not the same product.
    if (grouped.labels.length < 8) {
      bad(`grouping kept only ${grouped.labels.length} columns: `
        + JSON.stringify(grouped.labels));
    }
    if (!/SUM\(/i.test(grouped.sql)) {
      bad('no measure was aggregated after grouping');
    }
    if (!/COUNT\(DISTINCT/i.test(grouped.sql)) {
      bad('no text column took its unique value after grouping');
    }
  }

  // THE SIDEBAR COLLAPSES AND GIVES THE WIDTH TO THE GRID.
  //
  // Measured, not asserted by class name: the whole point of
  // collapsing is the 200px the grid gets back, and a class that
  // toggles while the layout does not move is the same bug as no
  // toggle at all. The rail must also be a real, visible target --
  // a collapsed panel with no way back is one only a reload escapes.
  const side = () => page.evaluate(() => {
    const g = document.querySelector('.dc-app-grid');
    const p = document.querySelector('.dc-app-side');
    const rail = document.querySelector('.dc-tool-panel-rail');
    const r = rail?.getBoundingClientRect();
    const mid = document.querySelector('.dc-app-middle')
      .getBoundingClientRect();
    return {
      grid: Math.round(g.getBoundingClientRect().width),
      panel: Math.round(p.getBoundingClientRect().width),
      rows: document.querySelectorAll('.dc-tool-panel-row').length,
      rail: r ? { w: Math.round(r.width), h: Math.round(r.height) } : null,
      // The page's own geometry, because a sidebar can collapse
      // correctly and still wreck everything around it.
      doc: document.documentElement.scrollHeight,
      midTop: Math.round(mid.top),
      midH: Math.round(mid.height),
    };
  });
  const click = () => page.click('.dc-tool-panel-toggle');

  const open = await side();
  await click();
  const shut = await side();
  await click();
  const again = await side();
  console.log(`sidebar: ${open.panel}px open -> ${shut.panel}px shut`
    + ` (grid ${open.grid} -> ${shut.grid}), rail`
    + ` ${shut.rail ? `${shut.rail.w}x${shut.rail.h}` : 'MISSING'},`
    + ` page ${open.doc} -> ${shut.doc}px`);

  if (open.rows === 0) bad('the columns panel listed nothing to begin with');
  if (shut.panel >= open.panel - 100) {
    bad(`collapsing barely narrowed the panel: ${open.panel} ->`
      + ` ${shut.panel}`);
  }
  if (shut.grid <= open.grid + 100) {
    bad(`the grid did not get the width back: ${open.grid} ->`
      + ` ${shut.grid}`);
  }
  if (shut.rows !== 0) bad(`${shut.rows} column rows survived the collapse`);
  if (!shut.rail || shut.rail.w < 12 || shut.rail.h < 60) {
    bad(`no usable rail to reopen from: ${JSON.stringify(shut.rail)}`);
  }
  if (again.panel !== open.panel || again.rows !== open.rows) {
    bad(`reopening did not restore the panel: ${again.panel}px,`
      + ` ${again.rows} rows vs ${open.panel}px, ${open.rows}`);
  }
  // NOTHING ELSE MAY MOVE. The rail's first version was `height:
  // 100%`, which resolved against a distant ancestor rather than the
  // panel: 800px of button stretched the middle row to 802px, shoved
  // it to y = -1 and grew the document from 888px to 1268px. Every
  // check above passed -- the panel narrowed, the grid widened, the
  // rail was tall and clickable. Only the page's own geometry says
  // that collapsing the sidebar broke the page.
  if (shut.doc !== open.doc) {
    bad(`collapsing changed the page height: ${open.doc} -> ${shut.doc}`);
  }
  if (shut.midTop !== open.midTop || shut.midH !== open.midH) {
    bad(`collapsing moved the grid row: top ${open.midTop} -> `
      + `${shut.midTop}, height ${open.midH} -> ${shut.midH}`);
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
