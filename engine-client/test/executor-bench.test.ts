// THE HOST EXECUTOR'S BENCHMARK (docs/HOST_EXECUTORS_PROGRAM_2026_11_02.md, H0: the baseline; manual, never in a gate):
// the tab's engine (DuckDbEngine over DuckDB-WASM, as a Node test starts it) running a planned query of the five
// columns Java's benchmark reads (//core:seam_benchmark: an integer id, a text, a DECIMAL(18,2), a timestamp, a
// nullable integer), at 10 to 1,000,000 rows. Three paths per size:
//   arrow    -- DuckDB's own answer, an Arrow table, no value read: the ceiling of this database here;
//   execute  -- DuckDbEngine.execute, the product's path: the query and every cell decoded to a Scalar, typed by the
//               plan (engine.ts typedByPlan);
//   stream   -- DuckDbEngine.stream, the same a batch at a time.
// Each size: one cold call, then calls timed one by one until BENCH_MEASURE_MS has passed (after BENCH_WARM_MS of
// warm-up); the median and p90 printed, with a SHA-256 of every decoded value so a change to the decode is checked
// to answer the same values. Run:
//   bazel test //engine-client:executor_bench --test_output=streamed
//   (--test_env=BENCH_SIZES=10,1000 --test_env=BENCH_WARM_MS=500 --test_env=BENCH_MEASURE_MS=1000)

import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { describe, it } from 'node:test';

import { DuckDbEngine, type ArrowishConnection } from '../src/duckdb.ts';
import { startNodeDuckDb } from '../src/node-require.ts';
import type { Plan } from '../src/relation-type.ts';
import type { ResultTable } from '../src/result.ts';
import { runfilesFromEnv } from '../../tools/js/runfiles.mts';

const SIZES = (process.env['BENCH_SIZES'] ?? '10,100,1000,10000,100000,1000000').split(',').map(Number);
const WARM_MS = Number(process.env['BENCH_WARM_MS'] ?? 1000);
const MEASURE_MS = Number(process.env['BENCH_MEASURE_MS'] ?? 2000);

interface Raw { query(sql: string): { readonly numRows: number } }

const COLUMNS: Plan['columns'] = [
  { name: 'id', type: 'Integer' },
  { name: 'name', type: 'String' },
  { name: 'amount', type: 'Decimal' },
  { name: 'ts', type: 'DateTime' },
  { name: 'bucket', type: 'Integer' },
];

function plan(rows: number): Plan {
  return { sql: `SELECT id, name, amount, ts, bucket FROM bench ORDER BY id LIMIT ${rows}`, columns: COLUMNS };
}

/** Every decoded value of a result, in column order, hashed: the same values answer the same digest. */
function digest(tables: readonly ResultTable[]): string {
  const h = createHash('sha256');
  for (const t of tables) {
    for (const c of t.columns) {
      h.update(c.name);
      for (const v of c.values) h.update(typeof v === 'bigint' ? `n${v}` : JSON.stringify(v));
    }
  }
  return h.digest('hex').slice(0, 16);
}

async function timed(work: () => Promise<unknown>): Promise<{ median: number; p90: number; runs: number }> {
  await work();
  const warmUntil = performance.now() + WARM_MS;
  while (performance.now() < warmUntil) await work();
  const times: number[] = [];
  const until = performance.now() + MEASURE_MS;
  do {
    const t0 = performance.now();
    await work();
    times.push(performance.now() - t0);
  } while (performance.now() < until || times.length < 3);
  times.sort((a, b) => a - b);
  return { median: times[Math.floor(times.length / 2)]!, p90: times[Math.floor(times.length * 0.9)]!, runs: times.length };
}

const ms = (x: number): string => (x < 10 ? x.toFixed(3) : x.toFixed(1));

describe('the host executor, measured', () => {
  it('DuckDbEngine over DuckDB-WASM, 10 to 1,000,000 rows', async () => {
    const db = await startNodeDuckDb(runfilesFromEnv('DUCKDB_EXTENSIONS'));
    const conn = db.connect();
    const raw = conn as unknown as Raw;
    const engine = new DuckDbEngine(conn as ArrowishConnection);
    const most = Math.max(...SIZES);
    raw.query("SET TimeZone = 'UTC'");
    raw.query(`CREATE TABLE bench AS SELECT i AS id, 'name' || CAST(i AS VARCHAR) AS name,
      CAST(CAST(i AS DECIMAL(18,2)) / 7 AS DECIMAL(18,2)) AS amount,
      TIMESTAMP '2024-01-01 00:00:00' + INTERVAL (i % 86400) SECOND AS ts,
      CASE WHEN i % 10 = 0 THEN NULL ELSE CAST(i % 1000 AS INTEGER) END AS bucket
      FROM range(${most}) t(i)`);
    console.log(`[bench] node ${process.version}; sizes ${SIZES.join(',')}; warm ${WARM_MS} ms, measure ${MEASURE_MS} ms`);
    for (const rows of SIZES) {
      const p = plan(rows);
      const arrow = await timed(async () => assert.equal(raw.query(p.sql).numRows, rows));
      let executed: ResultTable | undefined;
      const execute = await timed(async () => { executed = await engine.execute(p, 0); });
      const chunks: ResultTable[] = [];
      const stream = await timed(async () => { chunks.length = 0; await engine.stream(p, 0, (c) => chunks.push(c)); });
      assert.equal(executed!.rowCount, rows);
      assert.equal(chunks.reduce((n, c) => n + c.rowCount, 0), rows);
      const same = digest([executed!]) === digest(chunks);
      console.log(`[bench] rows=${rows} | arrow ${ms(arrow.median)} ms (p90 ${ms(arrow.p90)}) | execute ${ms(execute.median)} ms`
        + ` (p90 ${ms(execute.p90)}) | stream ${ms(stream.median)} ms (p90 ${ms(stream.p90)}) | values ${digest([executed!])}`
        + `${same ? '' : ' STREAM DIFFERS'} | runs ${arrow.runs}/${execute.runs}/${stream.runs}`);
      assert.ok(same, 'stream and execute answer the same values');
    }
    await db.terminate?.();
  });
});
