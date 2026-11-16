// Does a cube actually read from a remote file?
//
// Everything else about remote sources is testable in node -- which
// statements are emitted, how a URL is escaped, whether a credential
// can reach an error message. This is the part that is not: whether
// the bytes move. The node build of duckdb-wasm answers a synthetic
// 404 for any http:// URL instead of making a request, so only a
// real browser can prove it.
//
// The Parquet file is BUILT here rather than committed: a binary
// fixture rots silently, and generating it means the test also
// proves the writer and the reader agree.
//
// Run: bazel run //datacube:verify_remote
// first: points Playwright at the Chromium Bazel fetched (as a browser_test; a no-op under bazel run)
import '../../tools/browser/pinned-chromium.mjs';
import { startNodeDuckDb } from '../../engine-client/src/node-require.ts';
import { runfilesFromEnv } from '../../tools/js/runfiles.mts';
import { readFile, rm } from 'node:fs/promises';
import { extname } from 'node:path';
import path from 'node:path';

import { chromium } from 'playwright';
import { serve, siteRoot, tmpDir } from './harness.mjs';

const ROOT = siteRoot();

let failed = false;
const check = (name, ok, detail = '') => {
  console.log(`${ok ? 'ok  ' : 'FAIL'}  ${name}${detail ? ` — ${detail}` : ''}`);
  if (!ok) failed = true;
};

// ---- build a Parquet file with node's duckdb ------------------------
const db = await startNodeDuckDb(runfilesFromEnv('DUCKDB_EXTENSIONS'));
const conn = db.connect();
// The fixture carries the SCHEMA THE DEMO'S CUBE IS DEFINED OVER,
// not just the two columns the connection test needs. A narrower
// file passes the connection checks and then fails the app one with
// a binder error -- which is how the first version of this went, and
// a fair result: a cube that pivots on `year` needs a `year`.
conn.query(
  `CREATE TABLE t AS SELECT
     CASE i % 3 WHEN 0 THEN 'AMER' WHEN 1 THEN 'EMEA' ELSE 'APAC' END AS region,
     CASE (i // 3) % 2 WHEN 0 THEN 'Rates' ELSE 'Credit' END          AS desk,
     ('Book ' || (1 + ((i // 6) % 2)))                                AS book,
     (2021 + (i % 2))                                                 AS year,
     ('Q' || (1 + (i % 4)))                                           AS qtr,
     ((i * 7919) % 100000) / 100.0                                    AS notional,
     ((i * 104729) % 20000) / 100.0 - 100.0                           AS pnl,
     ((i * 31) % 97) + 1                                              AS qty
   FROM range(24) t(i)`,
);
// A UNIQUE name per run, and deleted afterwards. duckdb-wasm's node
// runtime writes to the REAL filesystem, and COPY writes a temp file
// then renames -- a rename that does not happen here, so a fixed
// name leaves the PREVIOUS run's file in place and the test happily
// serves stale bytes. That is exactly what happened: the connection
// checks passed against a four-row file while the assertions had
// moved on.
// In the run's own temp directory (harness.tmpDir), never the working directory (Bazel workplan P4-03: under a
// test that is the runfiles tree).
// forward slashes: DuckDB reads a backslash in a quoted path as an escape
const fixture = path.join(await tmpDir('dc-remote-'), `dc-fixture-${process.pid}-${Date.now()}.parquet`).replace(/\\/g, '/');
conn.query(`COPY t TO '${fixture}' (FORMAT PARQUET)`);
const parquet = db.copyFileToBuffer(fixture);
for (const stray of [fixture, path.join(path.dirname(fixture), `tmp_${path.basename(fixture)}`)]) {
  await rm(stray, { force: true }).catch(() => {});
}
check('built a Parquet fixture', parquet.length > 0, `${parquet.length} bytes`);

// ---- serve it, and the harness page, with RANGE support -------------
let rangeRequests = 0;
/** HEAD requests refused on a presigned link (below). */
let signedHeads = 0;

const cors = {
  'Access-Control-Allow-Origin': '*',
  'Cross-Origin-Opener-Policy': 'same-origin',
  'Cross-Origin-Embedder-Policy': 'require-corp',
  'Cross-Origin-Resource-Policy': 'cross-origin',
};
// port 0: any free one, never a fixed number (Bazel workplan P4-01)
const { port: PORT, close: closeServer } = await serve(ROOT, {
  headers: cors,
  route: async (req, res, url) => {
    if (url.pathname === '/data/trades.parquet') { // portable: an HTTP route
      // a link presigned for GET, as object storage signs one, refuses a HEAD (DuckDB-WASM's runtime then logs the link)
      if (req.method === 'HEAD' && url.searchParams.has('X-Amz-Signature')) {
        signedHeads += 1;
        res.writeHead(403, cors);
        res.end();
        return true;
      }
      // Object storage serves ranges, and DuckDB relies on it: it reads
      // the footer first to find the row groups. A server without range
      // support forces a whole-file download, which is the difference
      // between this architecture working and not.
      const range = req.headers.range;
      if (range) {
        rangeRequests += 1;
        const m = /bytes=(\d*)-(\d*)/.exec(range);
        const start = Number(m?.[1] ?? 0);
        const end = m?.[2] ? Number(m[2]) : parquet.length - 1;
        res.writeHead(206, {
          ...cors,
          'Content-Range': `bytes ${start}-${end}/${parquet.length}`,
          'Accept-Ranges': 'bytes',
          'Content-Length': end - start + 1,
        });
        res.end(parquet.subarray(start, end + 1));
        return true;
      }
      res.writeHead(200, {
        ...cors,
        'Accept-Ranges': 'bytes',
        'Content-Length': parquet.length,
      });
      res.end(req.method === 'HEAD' ? undefined : parquet);
      return true;
    }
    return false;
  },
});

// ---- drive it in a real browser -------------------------------------
const browser = await chromium.launch();
const page = await browser.newPage();
const problems = [];
page.on('pageerror', (e) => problems.push(`pageerror: ${e.message}`));

await page.goto(`http://127.0.0.1:${PORT}/demo/remote.html`);
await page.waitForFunction(() => document.body.dataset['ready'] === 'yes', {
  timeout: 60_000,
});
check('the harness booted duckdb-wasm in the browser', true);

const out = await page.evaluate(
  (url) => window.remoteTest({ name: 'TRADES', url }),
  `http://127.0.0.1:${PORT}/data/trades.parquet`,
);

check(
  'a remote Parquet answers a grouped aggregate',
  out.rowCount === 3,
  `${out.rowCount} groups: ${JSON.stringify(out.regions)}`,
);

// Checked against the SAME data read locally, rather than against
// numbers copied into the test. A hand-written expectation only
// proves the fixture was transcribed correctly; this proves the
// remote read agrees with the local one, which is the actual claim.
const localRows = conn.query(
  'SELECT region, sum(notional) AS m FROM t GROUP BY region ORDER BY region',
);
const localTotals = new Map();
for (let i = 0; i < localRows.numRows; i++) {
  localTotals.set(
    String(localRows.getChildAt(0)?.get(i)),
    Number(localRows.getChildAt(1)?.get(i)),
  );
}
const remoteTotals = new Map(
  out.regions.map((r, i) => [String(r), Number(out.totals[i])]),
);
const agree = [...localTotals].every(
  ([k, v]) => Math.abs((remoteTotals.get(k) ?? NaN) - v) < 1e-9,
);
check(
  'and the remote numbers match the same data read locally',
  agree && remoteTotals.size === localTotals.size,
  `remote ${JSON.stringify([...remoteTotals])} local ${JSON.stringify([...localTotals])}`,
);
check(
  'the planner-shaped query never mentions the URL',
  out.columns.join(',') === 'region,m',
  out.columns.join(','),
);
check(
  'DuckDB used range requests rather than pulling the whole file',
  rangeRequests > 0,
  `${rangeRequests} range requests`,
);
check('no page errors', problems.length === 0, problems.join(' | '));

// ---- and now the WHOLE APP against the same remote file -------------
//
// The checks above prove the connection. This proves the product:
// the real demo, the real planner, the real grid, reading a Parquet
// over HTTP. Without it `remote.ts` would be a library only a test
// harness can reach -- which is the exact failure this codebase
// has a guardrail for.
const app = await browser.newPage();
const appProblems = [];
app.on('pageerror', (e) => appProblems.push(`pageerror: ${e.message}`));
const remoteUrl = encodeURIComponent(
  `http://127.0.0.1:${PORT}/data/trades.parquet`,
);
await app.goto(`http://127.0.0.1:${PORT}/demo/index.html?remote=${remoteUrl}`);

let rendered = 0;
try {
  await app.waitForSelector('.dc-row', { timeout: 60_000 });
  rendered = await app.locator('.dc-row').count();
} catch {
  rendered = 0;
}
check(
  'the whole cube renders from a remote Parquet',
  rendered > 0,
  `${rendered} rows — ${(await app.textContent('#status').catch(() => '')) ?? ''}`,
);
check(
  'and it did so without page errors',
  appProblems.length === 0,
  appProblems.slice(0, 2).join(' | '),
);

// ---- and over a PRESIGNED link: its token is printed nowhere ---------
//
// DuckDB-WASM's runtime prints a remote file's URL as it is when a request fails ("HEAD request failed: <url>"), and a
// link presigned for GET refuses a HEAD as a matter of course. The tab's DuckDB runs behind a worker that prints every
// URL with its query string left out (engine-client/vendor/duckdb-redacting.worker.js; 2026-11-16, the TypeScript
// hunt's T-G): no console message of the page or of its workers, and no word on the page, holds the signature.
const signed = await browser.newPage();
const printed = [];
signed.on('console', (m) => printed.push(m.text()));
signed.on('worker', (w) => w.on('console', (m) => printed.push(`[worker] ${m.text()}`)));
const presigned = `http://127.0.0.1:${PORT}/data/trades.parquet?X-Amz-Signature=SENTINEL-SIGNATURE&X-Amz-Expires=600`;
await signed.goto(`http://127.0.0.1:${PORT}/demo/index.html?remote=${encodeURIComponent(presigned)}`);
// the mount is tried: the link refuses DuckDB's HEAD, and the runtime's line about it is printed (or the wait ends)
for (const end = Date.now() + 60_000; Date.now() < end && !(signedHeads > 0 && printed.some((l) => l.includes('trades.parquet')));) {
  await new Promise((done) => setTimeout(done, 100));
}
const said = `${printed.join('\n')}\n${await signed.textContent('body').catch(() => '')}`;
check(
  "a presigned link's signature is printed nowhere: not in the console, not on the page",
  !said.includes('SENTINEL'),
  said.split('\n').filter((l) => l.includes('SENTINEL')).slice(0, 2).join(' | '),
);
check(
  'and the link itself was named in the console, its query string left out',
  printed.some((l) => l.includes('/data/trades.parquet?[query string not shown]')),
  printed.filter((l) => l.includes('trades.parquet')).slice(0, 2).join(' | ') || `${signedHeads} HEAD requests refused; ${printed.length} messages: ${printed.slice(0, 3).join(' | ')}`,
);

await browser.close();
closeServer();
console.log(failed ? '\nREMOTE VERIFY FAILED' : '\nremote sources work');
process.exit(failed ? 1 : 0);
