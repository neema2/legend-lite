// THE DEMO'S SAMPLE TRADES AS A PARQUET FILE, made at BUILD time (the user's ruling of 2026-11-12, "Demo has to go
// through platform"): the rows src/sample-trades.ts makes -- the one deterministic generator -- written by DuckDB's own
// Parquet writer (DuckDB-WASM in Node, its extensions the pinned ones), to the path given. The site ships the file beside
// the page (BUILD.bazel :sample_trades_parquet, in :site), and the demo opens it exactly as a person's file is opened:
// the planner's planned load of a host's file, run by the hosts' runner (demo/boot.ts loadSample). The same rows and
// types as the generator's: test/sample-trades.test.ts reads the built file and holds it to them.
//
// A build tool, never the page: its one statement is the writer's own (the page spells no SQL: the guardrails scan the
// shipped trees, not tools/).

import { mkdirSync, writeFileSync } from 'node:fs';
import { dirname } from 'node:path';

import { startNodeDuckDb } from '../../../engine-client/src/node-require.ts';
import { runfilesFromEnv } from '../../../tools/js/runfiles.mts';
import { sampleTrades } from '../../src/sample-trades.ts';

interface BlockingConnection {
  insertArrowFromIPCStream(buffer: Uint8Array, options: { name: string; create: boolean }): void;
  query(sql: string): unknown;
}

const out = process.argv[2];
if (!out) throw new Error('usage: emit-parquet <out.parquet>');
const db = await startNodeDuckDb(runfilesFromEnv('DUCKDB_EXTENSIONS'));
const conn = db.connect() as unknown as BlockingConnection;
conn.insertArrowFromIPCStream(sampleTrades(), { name: 'trades', create: true });
// one thread, so one row order and one file whatever the machine
conn.query('SET threads = 1');
conn.query("COPY (SELECT * FROM trades) TO 'sample-trades.parquet' (FORMAT PARQUET)");
mkdirSync(dirname(out), { recursive: true });
writeFileSync(out, (db as unknown as { copyFileToBuffer(name: string): Uint8Array }).copyFileToBuffer('sample-trades.parquet'));
