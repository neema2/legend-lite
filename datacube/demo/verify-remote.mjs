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
import { createRequire } from 'node:module';
import { createServer } from 'node:http';
import { readFile, rm } from 'node:fs/promises';
import { extname, join, normalize } from 'node:path';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { chromium } from 'playwright';

const require = createRequire(import.meta.url);
const ROOT = fileURLToPath(new URL('..', import.meta.url));
const PORT = 8741;

let failed = false;
const check = (name, ok, detail = '') => {
  console.log(`${ok ? 'ok  ' : 'FAIL'}  ${name}${detail ? ` — ${detail}` : ''}`);
  if (!ok) failed = true;
};

// ---- build a Parquet file with node's duckdb ------------------------
const duckdb = require('@duckdb/duckdb-wasm/blocking');
const dist = path.dirname(require.resolve('@duckdb/duckdb-wasm/blocking'));
const db = await duckdb.createDuckDB(
  {
    mvp: {
      mainModule: path.join(dist, 'duckdb-mvp.wasm'),
      mainWorker: path.join(dist, 'duckdb-node-mvp.worker.cjs'),
    },
    eh: {
      mainModule: path.join(dist, 'duckdb-eh.wasm'),
      mainWorker: path.join(dist, 'duckdb-node-eh.worker.cjs'),
    },
  },
  new duckdb.VoidLogger(),
  duckdb.NODE_RUNTIME,
);
await db.instantiate();
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
const fixture = `dc-fixture-${process.pid}-${Date.now()}.parquet`;
conn.query(`COPY t TO '${fixture}' (FORMAT PARQUET)`);
const parquet = db.copyFileToBuffer(fixture);
for (const stray of [fixture, `tmp_${fixture}`]) {
  await rm(stray, { force: true }).catch(() => {});
}
check('built a Parquet fixture', parquet.length > 0, `${parquet.length} bytes`);

// ---- serve it, and the harness page, with RANGE support -------------
let rangeRequests = 0;
const TYPES = {
  '.html': 'text/html',
  '.js': 'text/javascript',
  '.mjs': 'text/javascript',
  '.css': 'text/css',
  '.wasm': 'application/wasm',
  '.json': 'application/json',
};

const server = createServer(async (req, res) => {
  const url = new URL(req.url ?? '/', 'http://x');
  const cors = {
    'Access-Control-Allow-Origin': '*',
    'Cross-Origin-Opener-Policy': 'same-origin',
    'Cross-Origin-Embedder-Policy': 'require-corp',
    'Cross-Origin-Resource-Policy': 'cross-origin',
  };

  if (url.pathname === '/data/trades.parquet') {
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
      return;
    }
    res.writeHead(200, {
      ...cors,
      'Accept-Ranges': 'bytes',
      'Content-Length': parquet.length,
    });
    res.end(req.method === 'HEAD' ? undefined : parquet);
    return;
  }

  try {
    const rel = normalize(decodeURIComponent(url.pathname))
      .replace(/^(\.\.[/\\])+/, '');
    const body = await readFile(join(ROOT, rel));
    res.writeHead(200, {
      ...cors,
      'Content-Type': TYPES[extname(rel)] ?? 'application/octet-stream',
    });
    res.end(body);
  } catch {
    res.writeHead(404, cors).end('not found');
  }
});
await new Promise((r) => server.listen(PORT, r));

// ---- drive it in a real browser -------------------------------------
const browser = await chromium.launch();
const page = await browser.newPage();
const problems = [];
page.on('pageerror', (e) => problems.push(`pageerror: ${e.message}`));

await page.goto(`http://localhost:${PORT}/demo/remote.html`);
await page.waitForFunction(() => document.body.dataset['ready'] === 'yes', {
  timeout: 60_000,
});
check('the harness booted duckdb-wasm in the browser', true);

const out = await page.evaluate(
  (url) => window.remoteTest({ name: 'TRADES', url }),
  `http://localhost:${PORT}/data/trades.parquet`,
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
  `http://localhost:${PORT}/data/trades.parquet`,
);
await app.goto(`http://localhost:${PORT}/demo/index.html?remote=${remoteUrl}`);

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

await browser.close();
server.close();
console.log(failed ? '\nREMOTE VERIFY FAILED' : '\nremote sources work');
process.exit(failed ? 1 : 0);
