// The snap ceiling is not a row count.
//
// snap-ceiling.mjs showed pivot time rising only 1.6x while rows rose
// 10x, because the row groups x pivot columns product was held constant
// at 1M cells. That points at output cells, not snap size, as the
// binding constraint -- matching the native cost model in
// bench/model/cellcost.py (constant scan + cells x ~100ns).
//
// This holds rows fixed and sweeps cells, to turn the 300ms interaction
// budget into a number the product can actually enforce.
//
// Run: node bench/cell-budget.mjs
import { createRequire } from 'node:module';
import { performance } from 'node:perf_hooks';
import path from 'node:path';

const require = createRequire(import.meta.url);
const duckdb = require('@duckdb/duckdb-wasm/blocking');

const ROWS = 5_000_000; // fixed
const REPS = 3;
const BUDGET_MS = 300;

// (groups, columns) pairs sweeping the cell count over two orders of
// magnitude, each product reached two different ways where possible.
const CASES = [
  [500, 50],
  [50, 500],
  [1000, 200],
  [200, 1000],
  [2000, 500],
  [500, 2000],
  [4000, 1000],
];

const dist = path.dirname(require.resolve('@duckdb/duckdb-wasm/blocking'));

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
conn.query('SET threads=1');

console.log(
  `duckdb-wasm ${db.getVersion()}  threads=1  ` +
    `snap fixed at ${ROWS.toLocaleString()} rows  min of ${REPS}`,
);
console.log(`budget = ${BUDGET_MS}ms\n`);

const pad = (s, n) => String(s).padStart(n);
console.log(
  `${pad('groups', 7)} ${pad('cols', 6)} ${pad('cells', 10)}` +
    ` ${pad('pivot ms', 9)} ${pad('ns/cell', 8)} ${pad('verdict', 8)}`,
);
console.log('-'.repeat(54));

for (const [groups, cols] of CASES) {
  conn.query(`
    CREATE OR REPLACE TABLE snap AS
    SELECT ((i//${cols})%${groups}) AS book, (i%${cols}) AS pk,
           random()*1e6 AS notional
    FROM range(${ROWS}) t(i)
  `);
  const keys = Array.from({ length: cols }, (_, i) => i).join(',');
  const t = best(() =>
    conn.query(
      `CREATE OR REPLACE TABLE r AS PIVOT snap ON pk IN (${keys}) ` +
        `USING sum(notional) GROUP BY book`,
    ),
  );
  const cells = groups * cols;
  console.log(
    `${pad(groups, 7)} ${pad(cols, 6)} ${pad(cells.toLocaleString(), 10)}` +
      ` ${pad(t.toFixed(0), 9)} ${pad(((t * 1e6) / cells).toFixed(0), 8)}` +
      ` ${pad(t <= BUDGET_MS ? 'OK' : 'OVER', 8)}`,
  );
}

console.log(
  '\nIf equal-cell pairs land close, the product enforces a CELL cap' +
    '\n(row groups x pivot columns) rather than a snap row limit.',
);

conn.close();
db.terminate?.();
