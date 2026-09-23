// Step 1 of the build plan: how much slower is DuckDB in WebAssembly
// than the native DuckDB every earlier number was measured on?
//
// Mirrors bench/model/cachehost.py exactly so the two are comparable:
// a 10M-row snap of 6 columns, then a 500-column pivot grouped by book.
// Native single-threaded reference, from that file:
//     build  692ms    pivot  168.4ms    scan  11.2ms
//
// Run: node bench/wasm-penalty.mjs [rows]
import { createRequire } from 'node:module';
import { performance } from 'node:perf_hooks';
import path from 'node:path';

const require = createRequire(import.meta.url);
const duckdb = require('@duckdb/duckdb-wasm/blocking');

const ROWS = Number(process.argv[2] ?? 10_000_000);
const REPS = 3;

// Native reference numbers (single-threaded) from cachehost.py, so the
// penalty is printed rather than left for the reader to compute.
const NATIVE = { build: 692, pivot: 168.4, scan: 11.2 };

const dist = path.dirname(
  require.resolve('@duckdb/duckdb-wasm/blocking'),
);

function best(fn, reps = REPS) {
  let t = null;
  for (let i = 0; i < reps; i++) {
    const t0 = performance.now();
    fn();
    const dt = performance.now() - t0;
    t = t === null ? dt : Math.min(t, dt);
  }
  return t;
}

const SRC = `
  SELECT ((i//500)%2000) AS book, (i%500) AS pk,
         DATE '2020-01-01' + INTERVAL (i%2000) DAY AS trade_date,
         'TRADER-' || (i%1200) AS trader,
         random()*1e6 AS notional,
         random()*1e6 - 5e5 AS pnl
  FROM range(${ROWS}) t(i)
`;

const keys = Array.from({ length: 500 }, (_, i) => i).join(',');
const PIVOT =
  `CREATE OR REPLACE TABLE r AS ` +
  `PIVOT snap ON pk IN (${keys}) USING sum(notional) GROUP BY book`;
const SCAN = 'SELECT count(*), sum(notional), avg(pnl) FROM snap';

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

const version = db.getVersion();
const conn = db.connect();
conn.query('SET threads=1');

console.log(
  `duckdb-wasm ${version}   snap = ${ROWS.toLocaleString()} rows x 6 cols` +
    `   threads=1   min of ${REPS}\n`,
);

const tBuild = best(
  () => conn.query(`CREATE OR REPLACE TABLE snap AS ${SRC}`),
  1,
);
const tPivot = best(() => conn.query(PIVOT));
const tScan = best(() => conn.query(SCAN));

const rows = [
  ['build snap', tBuild, NATIVE.build],
  ['pivot (500 cols)', tPivot, NATIVE.pivot],
  ['full scan', tScan, NATIVE.scan],
];

const pad = (s, n) => String(s).padStart(n);
console.log(
  `${pad('operation', 18)} ${pad('wasm ms', 9)} ${pad('native ms', 10)}` +
    ` ${pad('penalty', 8)}`,
);
console.log('-'.repeat(48));
for (const [name, wasm, native] of rows) {
  console.log(
    `${pad(name, 18)} ${pad(wasm.toFixed(1), 9)} ${pad(native.toFixed(1), 10)}` +
      ` ${pad((wasm / native).toFixed(2) + 'x', 8)}`,
  );
}

const nRows = conn.query('SELECT count(*) AS n FROM r').get(0).n;
console.log(`\npivot produced ${nRows} rows (sanity check)`);
console.log(
  '\nBudget check: the p95 target for expand/sort/filter is 300ms.',
);
console.log(
  `This pivot in the browser: ${tPivot.toFixed(0)}ms ` +
    `(${tPivot <= 300 ? 'INSIDE' : 'OVER'} budget).`,
);

conn.close();
db.terminate?.();
