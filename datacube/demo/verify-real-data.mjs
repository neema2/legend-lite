// The real DataCube, real DuckDB-WASM, real data, no server.
//
// The other harnesses prove the planner produces the right SQL. This
// one proves the whole thing works against DATA YOU BROUGHT: a file
// on a web server, mounted as the table the Pure model already
// names, read over HTTP range requests, and aggregated into the grid.
//
// The totals are checked against DuckDB's own answer for the same
// file, so this fails if any layer -- planner, view mounting,
// pushdown, tree assembly, formatting -- gets it wrong, not merely
// if the page is blank.
//
//   DATA=/abs/path/to/trades.parquet \
//   EXPECT='{"Iberia":74988250}' \
//   npm run verify:realdata
//
// FORMAT=csv (default parquet) for a CSV. The file must carry the
// eight columns demo/trades.pure declares: region, desk, book, year,
// qtr, notional, pnl, qty.

import { createServer } from 'node:http';
import { open, readFile, stat } from 'node:fs/promises';
import { extname, join, normalize } from 'node:path';
import { chromium } from 'playwright';

const ROOT = new URL('..', import.meta.url).pathname;
const DATA = process.env.DATA ?? process.env.PARQUET;
const FORMAT = process.env.FORMAT ?? 'parquet';
if (!DATA) {
  console.error('set DATA=/abs/path/to/trades.parquet (or .csv with FORMAT=csv)');
  process.exit(2);
}
// What DuckDB says about this file, for the grid to be checked against.
const EXPECT = JSON.parse(process.env.EXPECT ?? '{}');

const TYPES = {
  '.html': 'text/html', '.js': 'text/javascript', '.wasm': 'application/wasm',
  '.pure': 'text/plain', '.css': 'text/css',
};

const server = createServer(async (req, res) => {
  const path = (req.url ?? '/').split('?')[0];

  // The Parquet, WITH byte-range support. httpfs reads the footer
  // first and then only the row groups a query needs; a server that
  // ignores Range forces the whole file down every time and hides
  // whether pushdown is working at all.
  if (path === '/data.parquet' || path === '/data.csv') {
    const { size } = await stat(DATA);
    const range = /^bytes=(\d*)-(\d*)$/.exec(req.headers.range ?? '');
    const fh = await open(DATA, 'r');
    try {
      if (range) {
        const start = range[1] ? Number(range[1]) : 0;
        const end = range[2] ? Number(range[2]) : size - 1;
        const len = end - start + 1;
        const buf = Buffer.alloc(len);
        await fh.read(buf, 0, len, start);
        res.writeHead(206, {
          'Content-Type': 'application/octet-stream',
          'Content-Length': String(len),
          'Content-Range': `bytes ${start}-${end}/${size}`,
          'Accept-Ranges': 'bytes',
        });
        res.end(buf);
      } else {
        res.writeHead(200, {
          'Content-Type': 'application/octet-stream',
          'Content-Length': String(size),
          'Accept-Ranges': 'bytes',
        });
        res.end(await readFile(DATA));
      }
    } finally {
      await fh.close();
    }
    return;
  }

  const rel = normalize(path === '/' ? '/demo/index.html' : path)
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
const data = `http://127.0.0.1:${port}/data.${FORMAT}`;
const url = `http://127.0.0.1:${port}/demo/index.html`
  + `?remote=${encodeURIComponent(data)}&format=${FORMAT}`;
console.log(`serving datacube/ and the ${FORMAT} on ${port}`);
console.log(`no legend-lite server is running\n${url}\n`);

const browser = await chromium.launch();
const page = await browser.newPage();
let ranges = 0;
page.on('request', (r) => { if (r.url() === data) ranges++; });
const problems = [];
page.on('pageerror', (e) => problems.push(e.message));

let failed = false;
try {
  await page.goto(url, { waitUntil: 'load', timeout: 120_000 });
  await page.waitForFunction(
    () => document.querySelectorAll('.dc-row').length > 0,
    { timeout: 120_000 },
  );
  const rows = await page.$$eval('.dc-row', (els) =>
    els.map((el) => [...el.querySelectorAll('.dc-cell')]
      .map((c) => c.textContent?.trim() ?? '')));

  console.log(`status: ${await page.textContent('#status')}`);
  console.log(`HTTP requests for the data (range reads): ${ranges}`);
  console.log('grid:');
  for (const r of rows) console.log(`  ${JSON.stringify(r.slice(0, 6))}`);

  // The regions must be the FILE's, not the demo's synthetic
  // AMER/APAC/EMEA -- that difference is what says the cube read the
  // data rather than falling back to generating its own.
  const labels = rows.map((r) => (r[0] ?? '').replace(/^[^A-Za-z]*/, ''));
  for (const [region, total] of Object.entries(EXPECT)) {
    const row = rows[labels.indexOf(region)];
    if (!row) {
      console.log(`FAIL: no row for ${region} (saw ${labels.join(', ')})`);
      failed = true;
      continue;
    }
    // Sum the pivoted year columns and compare with DuckDB's own total.
    const cells = row.slice(1).filter((c) => /^\$/.test(c));
    const sum = cells.reduce((a, c) => a + Number(c.replace(/[$,]/g, '')), 0);
    const off = Math.abs(sum - total);
    const ok = off <= Math.max(2, total * 1e-9);
    console.log(`  ${ok ? 'MATCH ' : 'DIFFER'} ${region}: grid ${sum}`
      + ` vs duckdb ${total}`);
    if (!ok) failed = true;
  }
  if (Object.keys(EXPECT).length === 0) {
    console.log('FAIL: no EXPECT given, so nothing was actually checked');
    failed = true;
  }
} catch (e) {
  console.log(`FAIL: ${e.message}`);
  failed = true;
} finally {
  if (problems.length) {
    console.log(`page errors: ${problems.slice(0, 5).join(' | ')}`);
    failed = true;
  }
  await browser.close();
  server.close();
}

console.log(failed
  ? '\n!!! the cube did NOT render the real data correctly !!!'
  : `\n*** real ${FORMAT}, real DuckDB-WASM, in-browser planner,`
    + ' totals match DuckDB ***');
process.exit(failed ? 1 : 0);
