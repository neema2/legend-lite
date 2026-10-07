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
//   bazel run //datacube:verify_real_data
//
// No DATA: the harness BUILDS a Parquet file with node's DuckDB (as verify-remote does -- a
// committed binary fixture rots silently) and takes EXPECT from that same DuckDB, so it runs
// anywhere, CI included. The answer still comes from a different engine than the one under
// test: DuckDB natively in node, against the in-browser planner and DuckDB-WASM.
//
// FORMAT=csv (default parquet) for a CSV. The file must carry the
// eight columns demo/trades.pure declares: region, desk, book, year,
// qtr, notional, pnl, qty.

// first: points Playwright at the Chromium Bazel fetched (as a browser_test; a no-op under bazel run)
import '../../tools/browser/pinned-chromium.mjs';
import { open, readFile, stat } from 'node:fs/promises';
import { extname } from 'node:path';
import { chromium } from 'playwright';
import { sendFile, serve, siteRoot, tmpDir } from './harness.mjs';

const ROOT = siteRoot();
const GIVEN = process.env.DATA ?? process.env.PARQUET;
const FORMAT = GIVEN ? (process.env.FORMAT ?? 'parquet') : 'parquet';
const fixture = GIVEN ? undefined : await buildFixture();
const DATA = GIVEN ?? fixture.file;
// What DuckDB says about this file, for the grid to be checked against.
const EXPECT = fixture ? fixture.expect : JSON.parse(process.env.EXPECT ?? '{}');

/** A Parquet file of the demo's eight columns, and DuckDB's per-region total of notional. */
async function buildFixture() {
  const { engineClientRequire } = await import('../../engine-client/src/node-require.ts');
  const path = await import('node:path');
  const duckdb = engineClientRequire('@duckdb/duckdb-wasm/blocking');
  const dist = path.dirname(engineClientRequire.resolve('@duckdb/duckdb-wasm/blocking'));
  const db = await duckdb.createDuckDB({
    mvp: { mainModule: path.join(dist, 'duckdb-mvp.wasm'), mainWorker: path.join(dist, 'duckdb-node-mvp.worker.cjs') },
    eh: { mainModule: path.join(dist, 'duckdb-eh.wasm'), mainWorker: path.join(dist, 'duckdb-node-eh.worker.cjs') },
  }, new duckdb.VoidLogger(), duckdb.NODE_RUNTIME);
  await db.instantiate();
  const conn = db.connect();
  // regions the demo never generates, so the grid cannot pass by showing the demo's own rows
  conn.query(`CREATE TABLE t AS SELECT
      CASE i % 3 WHEN 0 THEN 'Iberia' WHEN 1 THEN 'Nordics' ELSE 'Benelux' END AS region,
      CASE (i // 3) % 3 WHEN 0 THEN 'Rates' WHEN 1 THEN 'Credit' ELSE 'FX' END AS desk,
      ('Book ' || (1 + ((i // 9) % 4)))                                      AS book,
      (2021 + (i % 5))                                                       AS year,
      ('Q' || (1 + (i % 4)))                                                 AS qtr,
      ((i * 7919) % 1000000) / 100.0                                         AS notional,
      ((i * 104729) % 200000) / 100.0 - 1000.0                               AS pnl,
      ((i * 31) % 97) + 1                                                    AS qty
    FROM range(30000) t(i)`);
  const expect = Object.fromEntries(conn.query('SELECT region, sum(notional) AS n FROM t GROUP BY region')
    .toArray().map((r) => [String(r.region), Number(r.n)]));
  // in the run's own temp directory, never the working directory (P4-03: under a test, the runfiles tree)
  const dir = await tmpDir('dc-real-');
  const file = path.join(dir, 'trades.parquet').replace(/\\/g, '/'); // DuckDB reads a backslash as an escape
  conn.query(`COPY t TO '${file}' (FORMAT PARQUET)`);
  console.log(`no DATA: built ${file} (30,000 rows); DuckDB says ${JSON.stringify(expect)}`);
  return { file, expect };
}


// the data file WITH byte-range support (harness.sendFile): httpfs reads the footer first and then only the row groups
// a query needs; a server that ignores Range forces the whole file down every time and hides whether pushdown works
const { port, close: closeServer } = await serve(ROOT, {
  route: async (req, res, url) => {
    if (url.pathname !== '/data.parquet' && url.pathname !== '/data.csv') return false; // portable: an HTTP route
    await sendFile(res, DATA, req.headers.range);
    return true;
  },
});
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
  // The same rows, each cell with its column's name: the pivoted years are read BY NAME, so the
  // pivot's Total (which repeats their sum) or a column beside the pivot is never counted in.
  const named = await page.$$eval('.dc-row', (els) =>
    els.map((el) => [...el.querySelectorAll('.dc-cell')]
      .map((c) => ({ column: c.dataset.column ?? '', text: c.textContent?.trim() ?? '' }))));

  console.log(`status: ${await page.textContent('.dc-status-timing')}`);
  console.log(`HTTP requests for the data (range reads): ${ranges}`);
  console.log('grid:');
  for (const r of rows) console.log(`  ${JSON.stringify(r.slice(0, 6))}`);

  // The regions must be the FILE's, not the demo's synthetic
  // AMER/APAC/EMEA -- that difference is what says the cube read the
  // data rather than falling back to generating its own.
  const labels = rows.map((r) => (r[0] ?? '').replace(/^[^A-Za-z]*/, ''));
  for (const [region, total] of Object.entries(EXPECT)) {
    const row = named[labels.indexOf(region)];
    if (!row) {
      console.log(`FAIL: no row for ${region} (saw ${labels.join(', ')})`);
      failed = true;
      continue;
    }
    // Sum the pivoted year columns and compare with DuckDB's own total; the pivot's Total
    // column, when shown, must say the same. Money renders to whole dollars, so each cell is
    // off by under a dollar: the tolerance is a dollar per cell.
    const money = (t) => Number(t.replace(/[$,()]/g, '')) * (/^\(/.test(t) ? -1 : 1);
    const years = row.filter((c) => /^\d{4}__\|__/.test(c.column));
    const sum = years.reduce((a, c) => a + money(c.text), 0);
    const tolerance = Math.max(2, years.length);
    const ok = years.length > 0 && Math.abs(sum - total) <= tolerance;
    console.log(`  ${ok ? 'MATCH ' : 'DIFFER'} ${region}: grid ${sum} over ${years.length} years`
      + ` vs duckdb ${total}`);
    if (!ok) failed = true;
    const shown = row.find((c) => c.column.startsWith('__pivot_total__|__'));
    if (shown && Math.abs(money(shown.text) - total) > tolerance) {
      console.log(`FAIL: ${region}'s Total column says ${shown.text}, duckdb ${total}`);
      failed = true;
    }
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
  closeServer();
}

console.log(failed
  ? '\n!!! the cube did NOT render the real data correctly !!!'
  : `\n*** real ${FORMAT}, real DuckDB-WASM, in-browser planner,`
    + ' totals match DuckDB ***');
process.exit(failed ? 1 : 0);
