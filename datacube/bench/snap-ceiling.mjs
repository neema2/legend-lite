// What snap size keeps a browser pivot inside the interaction budget?
//
// wasm-penalty.mjs established that DuckDB-WASM costs ~2.1-2.4x native,
// which pushed a 10M-row / 500-column pivot to 360ms -- over the 300ms
// p95 target for expand/sort/filter. This sweeps snap size to find where
// the line actually falls, which revises the "10M row ceiling" that was
// derived from payload size alone.
//
// Run: node bench/snap-ceiling.mjs
import { createRequire } from 'node:module';
import { performance } from 'node:perf_hooks';
import path from 'node:path';

const require = createRequire(import.meta.url);
const duckdb = require('@duckdb/duckdb-wasm/blocking');

const SIZES = [1_000_000, 2_000_000, 5_000_000, 10_000_000];
const PIVOT_COLS = 500;
const GROUPS = 2000;
const REPS = 3;
const BUDGET_MS = 300; // p95 target for expand / sort / filter

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

const keys = Array.from({ length: PIVOT_COLS }, (_, i) => i).join(',');

console.log(
  `duckdb-wasm ${db.getVersion()}  threads=1  ` +
    `${GROUPS} row groups x ${PIVOT_COLS} pivot cols  min of ${REPS}`,
);
console.log(`budget = ${BUDGET_MS}ms p95 for expand/sort/filter\n`);

const pad = (s, n) => String(s).padStart(n);
console.log(
  `${pad('snap rows', 11)} ${pad('build ms', 9)} ${pad('pivot ms', 9)}` +
    ` ${pad('sort ms', 8)} ${pad('verdict', 9)}`,
);
console.log('-'.repeat(50));

for (const rows of SIZES) {
  conn.query(`
    CREATE OR REPLACE TABLE snap AS
    SELECT ((i//${PIVOT_COLS})%${GROUPS}) AS book,
           (i%${PIVOT_COLS}) AS pk,
           'TRADER-' || (i%1200) AS trader,
           random()*1e6 AS notional,
           random()*1e6 - 5e5 AS pnl
    FROM range(${rows}) t(i)
  `);

  const tBuild = best(
    () =>
      conn.query(
        `CREATE OR REPLACE TABLE snap2 AS SELECT * FROM snap`,
      ),
    1,
  );
  const tPivot = best(() =>
    conn.query(
      `CREATE OR REPLACE TABLE r AS PIVOT snap ON pk IN (${keys}) ` +
        `USING sum(notional) GROUP BY book`,
    ),
  );
  // Re-sorting an existing result is the other common interaction.
  const tSort = best(() =>
    conn.query(
      `CREATE OR REPLACE TABLE r2 AS SELECT * FROM r ORDER BY book DESC`,
    ),
  );

  const worst = Math.max(tPivot, tSort);
  const verdict = worst <= BUDGET_MS ? 'OK' : 'OVER';
  console.log(
    `${pad(rows.toLocaleString(), 11)} ${pad(tBuild.toFixed(0), 9)}` +
      ` ${pad(tPivot.toFixed(0), 9)} ${pad(tSort.toFixed(0), 8)}` +
      ` ${pad(verdict, 9)}`,
  );
}

console.log(
  '\nThe pivot is the binding constraint. Snap size above the crossing' +
    '\npoint still works, it just stops feeling instant -- which is the' +
    '\nthing the whole design is for.',
);

conn.close();
db.terminate?.();
