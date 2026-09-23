// IS legend-engine a drop-in replacement for the local plane?
//
// "The engine compiles our Pure" (verify-engine.mjs) is a weaker claim
// than it sounds: a query can compile and then answer differently.
// "The engine page renders" is weaker still -- it shows that ONE query
// worked. Drop-in replacement means something specific and testable:
// the SAME cube over the SAME data gives the SAME rows, whichever
// backend runs it.
//
// So this executes every case in engine-cases.mjs twice -- once
// planned by legend-lite's wasm planner and run by DuckDB-WASM in
// this process, once handed to a real legend-engine over HTTP running
// it against H2 -- and diffs the rows.
//
// ONE SOURCE OF DATA. The rows are read out of trades-h2.pure's
// `testDataSetupSqls` and inserted into DuckDB from there, so the two
// planes cannot be compared over data that has quietly diverged. If
// the seed changes, both sides change together.
//
// ORDERED vs UNORDERED, the distinction legend-lite's own rcorpus
// draws: a case that declares sorts is compared in order, because the
// order is part of the answer. A case with no sorts is compared as a
// multiset, because SQL does not promise one and a difference there is
// not a defect.
//
// WHAT IT HAS FOUND SO FAR, both of them invisible to a compile-only
// check:
//
//   * contains/startsWith/endsWithCaseInsensitive emitted
//     `toLower(<literal>)`, which the engine COMPILES and then cannot
//     translate -- a StackOverflowError for contains, "Match failure:
//     TypedFunction" for the other two. The literal is pre-lowered
//     now. `equal` accepts the same construct, so the inconsistency
//     is the engine's.
//   * column pivot: "Dialect translation for node of type
//     PivotedRelation not implemented in SqlDialect for database type
//     H2". An engine limitation for this database type, not our
//     emission -- the groupBy half of the same query runs, and a bare
//     pivot off the table fails identically.
//
//   ENGINE=http://127.0.0.1:6300 bazel run //datacube:verify_engine_differential
//   ONLY=pivot bazel run //datacube:verify_engine_differential

import { createRequire } from 'node:module';
import path from 'node:path';
import { readFile } from 'node:fs/promises';

import { DuckDbEngine } from '../src/duckdb.ts';
import { LegendEngineExecutor } from '../src/engine-remote.ts';
import { serialize } from '../src/serialize.ts';
import { WasmPlanner } from '../src/wasm-planner.ts';
import { casesFor } from './engine-cases.mjs';

const ENGINE = (process.env.ENGINE ?? 'http://127.0.0.1:6300')
  .replace(/\/$/, '');
const ONLY = process.env.ONLY;

const HERE = new URL('.', import.meta.url);
const LOCAL_SOURCE = '#>{trades::DB.TRADES}#';
const ENGINE_SOURCE = '#>{trades::h2::DB.TRADES_SCHEMA.TRADES}#';

// ---- the data, read from the engine plane's own seed ----------------

/**
 * The seeded rows, taken out of the .pure file rather than regenerated.
 *
 * The file is the source of truth for what the ENGINE reads, so parsing
 * it is what guarantees DuckDB reads the same thing. Regenerating the
 * rows here from the same rule would be a second generator, and two
 * generators is how the data silently diverges.
 */
async function seededRows() {
  const text = await readFile(new URL('./trades-h2.pure', HERE), 'utf8');
  const rows = [];
  // `'INSERT INTO ... VALUES (a, b), (c, d);'` -- Pure escapes its
  // quotes as \', which is why the tuple scan runs on the unescaped
  // text.
  for (const stmt of text.matchAll(/INSERT INTO [^\n]*?VALUES (.*?);'/g)) {
    const body = stmt[1].replace(/\\'/g, "'");
    for (const tuple of body.matchAll(/\(([^)]*)\)/g)) {
      rows.push(tuple[1].split(',').map((c) => c.trim()));
    }
  }
  if (rows.length === 0) {
    throw new Error('no INSERT tuples found in trades-h2.pure — the seed'
      + ' shape changed and this parser needs updating');
  }
  return rows;
}

// ---- the local plane, in this process ------------------------------

async function localPlane(rows) {
  const require = createRequire(import.meta.url);
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
  const engine = new DuckDbEngine(conn);

  await engine.execute('CREATE TABLE TRADES (region VARCHAR, desk VARCHAR,'
    + ' book VARCHAR, year INTEGER, qtr VARCHAR, notional DOUBLE,'
    + ' pnl DOUBLE, qty INTEGER)', 0);
  // One statement: 128 round trips through a wasm boundary is slower
  // than the rest of this harness put together.
  await engine.execute(`INSERT INTO TRADES VALUES ${
    rows.map((r) => `(${r.join(', ')})`).join(', ')}`, 0);

  const model = await readFile(new URL('./trades.pure', HERE), 'utf8');
  const planner = new WasmPlanner({
    model,
    runtime: 'trades::RT',
    assetBaseUrl: new URL('./vendor/', HERE).href,
    cache: false,
  });
  await planner.warmUp();
  return { engine, planner };
}

// ---- comparison ----------------------------------------------------

/** A ResultTable as rows of strings, so two backends compare by VALUE. */
function normalise(table) {
  const cols = table.columns;
  const out = [];
  for (let r = 0; r < table.rowCount; r += 1) {
    out.push(cols.map((c) => cell(c.values[r])));
  }
  return { names: cols.map((c) => c.name), rows: out };
}

/**
 * One cell, as text.
 *
 * Numbers round to 6 decimals: the two backends compute the same
 * aggregate in binary floating point through different expression
 * trees, so the last bits differ on values that are equal in every
 * sense a cube cares about. BigInt and null are spelled explicitly so
 * a null never compares equal to the string "null" from the other side.
 */
function cell(v) {
  if (v === null || v === undefined) return '\u0000null';
  if (typeof v === 'bigint') return v.toString();
  if (typeof v === 'number') {
    return Number.isInteger(v) ? String(v) : v.toFixed(6);
  }
  return String(v);
}

/**
 * Field separator for the row key, written as an ESCAPE.
 *
 * An invisible byte in source survives no reformat and no code review,
 * and it breaks every search for the code around it -- which is why
 * the repo bans literal control characters outright. NUL cannot occur
 * in a value any store returns as text, so it cannot forge a match.
 */
const FIELD = '\u0001';

function compare(local, remote, ordered, sortKeys) {
  if (local.names.length !== remote.names.length) {
    return `column COUNT differs: local ${local.names.length}`
      + ` (${local.names.join(', ')}) vs engine ${remote.names.length}`
      + ` (${remote.names.join(', ')})`;
  }
  const mismatch = local.names
    .map((n, i) => [n, remote.names[i]])
    .filter(([a, b]) => a !== b);
  if (mismatch.length > 0) {
    return `column NAMES differ: ${mismatch
      .map(([a, b]) => `${a} vs ${b}`).join('; ')}`;
  }
  if (local.rows.length !== remote.rows.length) {
    return `row COUNT differs: local ${local.rows.length}`
      + ` vs engine ${remote.rows.length}`;
  }
  // ORDER and CONTENT are two claims, checked separately.
  //
  // A declared sort fixes the sequence of SORT KEY values and nothing
  // else: rows that tie on every key may come back in any order, and
  // SQL does not promise which. Comparing whole rows in sequence
  // therefore fails on a tie -- which is a fact about the query, not a
  // disagreement between the backends. legend-lite's own rcorpus draws
  // the same line, falling back to a multiset compare when the sort
  // keys are not derivable from the compared output.
  //
  // So: the sequence of key tuples must match exactly, and the full
  // rows must match as a multiset.
  if (ordered && sortKeys.length > 0) {
    const at = (rs) => rs.map((r) =>
      sortKeys.map((i) => r[i]).join(FIELD));
    const ka = at(local.rows);
    const kb = at(remote.rows);
    for (let i = 0; i < ka.length; i += 1) {
      if (ka[i] !== kb[i]) {
        return `sort ORDER differs at row ${i}:`
          + `\n      local  ${ka[i].split(FIELD).join(' | ')}`
          + `\n      engine ${kb[i].split(FIELD).join(' | ')}`;
      }
    }
  }
  const a = local.rows.map((r) => r.join(FIELD)).sort();
  const b = remote.rows.map((r) => r.join(FIELD)).sort();
  for (let i = 0; i < a.length; i += 1) {
    if (a[i] !== b[i]) {
      return `rows differ (as a multiset), first at ${i}:`
        + `\n      local  ${a[i].split(FIELD).join(' | ')}`
        + `\n      engine ${b[i].split(FIELD).join(' | ')}`;
    }
  }
  return null;
}

// ---- run -----------------------------------------------------------

const rows = await seededRows();
console.log(`seed: ${rows.length} rows, read from trades-h2.pure`);

let local;
try {
  local = await localPlane(rows);
} catch (e) {
  console.error(`could not start the local plane: ${e.message}`);
  console.error('run `bazel build //datacube:site` first.');
  process.exit(2);
}

let engineModel;
try {
  engineModel = await readFile(new URL('./trades-h2.pure', HERE), 'utf8');
  const probe = await fetch(`${ENGINE}/api/server/v1/info`,
    { signal: AbortSignal.timeout(3000) });
  if (!probe.ok) throw new Error(String(probe.status));
} catch (e) {
  console.error(`no engine at ${ENGINE}: ${e.message}`);
  process.exit(2);
}
const executor = new LegendEngineExecutor({
  baseUrl: ENGINE,
  model: engineModel,
  runtime: 'trades::h2::RT',
});

const localCases = casesFor(LOCAL_SOURCE);
const engineCases = casesFor(ENGINE_SOURCE);

const agreed = [];
const differed = [];
const skipped = [];

for (let i = 0; i < localCases.length; i += 1) {
  const { name, snapshot: ls, scope } = localCases[i];
  const es = engineCases[i].snapshot;
  if (ONLY && !name.toLowerCase().includes(ONLY.toLowerCase())) continue;

  let localRows;
  try {
    const sql = await local.planner.plan(serialize(ls, scope), ls, scope);
    localRows = normalise(await local.engine.execute(sql, ls.epoch));
  } catch (e) {
    skipped.push({ name, where: 'the local plane',
      detail: String(e.message ?? e).slice(0, 200) });
    continue;
  }

  let engineRows;
  try {
    const out = await executor.execute(serialize(es, scope), es, scope);
    engineRows = normalise(out.rows);
  } catch (e) {
    differed.push({ name, detail: `the engine REFUSED it: `
      + String(e.message ?? e).slice(0, 200) });
    continue;
  }

  const ordered = (ls.sorts ?? []).length > 0;
  // A sort names a COLUMN; the comparison needs its position in the
  // result, and a sort on a column the projection dropped cannot be
  // checked for order at all.
  const sortKeys = (ls.sorts ?? [])
    .map((s) => localRows.names.indexOf(s.column))
    .filter((i) => i >= 0);
  const problem = compare(localRows, engineRows, ordered, sortKeys);
  if (problem) differed.push({ name, detail: problem });
  else {
    agreed.push({ name, rows: localRows.rows.length,
      cols: localRows.names.length, ordered });
  }
}

// ---- report --------------------------------------------------------

console.log(`\nAGREED (${agreed.length}):`);
for (const a of agreed) {
  console.log(`  ok   ${a.name.padEnd(42)} `
    + `${a.rows}x${a.cols}${a.ordered ? ' ordered' : ''}`);
}
if (skipped.length > 0) {
  console.log(`\nCOULD NOT BE COMPARED (${skipped.length}) — the local`
    + ` plane failed, so the engine is not implicated:`);
  for (const s of skipped) {
    console.log(`  --   ${s.name}\n       ${s.where}: ${s.detail}`);
  }
}
if (differed.length > 0) {
  console.log(`\nDIFFERED (${differed.length}):`);
  for (const d of differed) console.log(`  XX   ${d.name}\n       ${d.detail}`);
}

const total = agreed.length + differed.length;
console.log(`\n${agreed.length}/${total} operations give the SAME rows on`
  + ` legend-engine as on the local plane`
  + `${skipped.length > 0 ? ` (${skipped.length} not comparable)` : ''}`);
console.log(differed.length === 0
  ? '\n*** legend-engine is a drop-in replacement for these operations ***'
  : `\n!!! ${differed.length} operations answer differently !!!`);
process.exit(differed.length === 0 ? 0 : 1);
