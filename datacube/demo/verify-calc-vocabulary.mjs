// Does every function the calculated-column editor OFFERS actually
// lower?
//
// A curated vocabulary is only as good as its proof. Offering `median`
// to someone whose backend has no spelling for it produces a refusal
// they cannot act on -- and the editor looked confident when it
// suggested it. So every entry's `example` is compiled here, as a real
// `extend` on a real snapshot, through whichever backends are running:
//
//   the wasm planner   always (it is vendored in the repo)
//   legend-engine      when one is reachable, so the catalogue is
//                      known to work on both planes rather than one
//
// A function that compiles on one and not the other is the most
// valuable thing this can find, because that is invisible to a
// single-backend check -- the same gap verify:engine:diff found for
// the case-insensitive filters.
//
//   bazel run //datacube:verify_calc_vocabulary
//   ENGINE=http://127.0.0.1:6300 bazel run //datacube:verify_calc_vocabulary

import { readFile } from 'node:fs/promises';

import { CALC_FUNCTIONS } from '../src/calc.ts';
import { LegendEngineExecutor } from '../../engine-client/src/engine-remote.ts';
import { levelLambda } from '../src/query.ts';
import { accessor } from '../../pure-protocol/src/index.ts';
import { WasmPlanner } from '../src/wasm-planner.ts';
import { sourceColumns } from '../src/source-columns.ts';
import { TRADES_KINDS, typeDifference } from './engine-cases.mjs';

const HERE = new URL('.', import.meta.url);
const ENGINE = (process.env.ENGINE ?? 'http://127.0.0.1:6300')
  .replace(/\/$/, '');

/**
 * The example as a snapshot: one derived column, nothing else. The example is
 * text a person types, so the plane's compiler parses it (`parse`).
 */
async function snapshotFor(fn, source, columns, parse) {
  return {
    source: { query: source },
    columns,
    derived: [{ name: 'calc', lambda: await parse(`x|${fn.example}`) }],
    rows: [],
    pivotOn: [],
    measures: [],
    sorts: [],
    epoch: 1,
  };
}

const results = [];

// ---- the local plane -----------------------------------------------
let planner = null;
try {
  planner = new WasmPlanner({
    model: await readFile(new URL('./trades.pure', HERE), 'utf8'),
    runtime: 'trades::RT',
    assetBaseUrl: new URL('./vendor/', HERE).href,
    cache: false,
  });
  await planner.warmUp();
} catch (e) {
  console.error(`the wasm planner is not available: ${e.message}`);
  console.error('run `bazel build //datacube:site` first.');
  process.exit(2);
}

// ---- the engine, if one is up --------------------------------------
let executor = null;
try {
  const probe = await fetch(`${ENGINE}/api/server/v1/info`,
    { signal: AbortSignal.timeout(2500) });
  if (!probe.ok) throw new Error(String(probe.status));
  executor = new LegendEngineExecutor({
    baseUrl: ENGINE,
    model: await readFile(new URL('./trades-h2.pure', HERE), 'utf8'),
    runtime: 'trades::h2::RT',
  });
} catch {
  console.log(`no engine at ${ENGINE} — checking the local plane only.`
    + ' Start one to check both.');
}

// Each plane's table as ITS compiler types it; two planes must agree.
const LOCAL_SOURCE = accessor('trades::DB', 'TRADES');
const ENGINE_SOURCE = accessor('trades::h2::DB', 'TRADES_SCHEMA', 'TRADES');
const localColumns = await sourceColumns(planner, LOCAL_SOURCE, TRADES_KINDS);
const engineColumns = executor ? await sourceColumns(executor, ENGINE_SOURCE, TRADES_KINDS) : null;
const typesDiffer = engineColumns ? typeDifference(localColumns, engineColumns) : null;
if (typesDiffer) {
  console.log(`\nTHE PLANES TYPE THE TABLE DIFFERENTLY: ${typesDiffer}`);
}

for (const fn of CALC_FUNCTIONS) {
  const row = { name: fn.name, local: null, engine: null };

  try {
    const localSnap = await snapshotFor(fn, LOCAL_SOURCE, localColumns, (t) => planner.parse(t));
    await planner.plan(levelLambda(localSnap));
  } catch (e) {
    row.local = String(e.message ?? e).replace(/\s+/g, ' ').slice(0, 150);
  }

  if (executor) {
    try {
      const engineSnap = await snapshotFor(
        fn, ENGINE_SOURCE, engineColumns, (t) => executor.parse(t));
      // EXECUTED, not just planned: the case-insensitive filters
      // compiled on the engine and then failed to render, so planning
      // alone is not the question.
      await executor.execute(levelLambda(engineSnap), engineSnap);
    } catch (e) {
      row.engine = String(e.message ?? e).replace(/\s+/g, ' ').slice(0, 150);
    }
  }
  results.push(row);
}

// ---- report --------------------------------------------------------
const ok = results.filter((r) => !r.local && !r.engine);
const localOnly = results.filter((r) => !r.local && r.engine);
const broken = results.filter((r) => r.local);

console.log(`\nWORKS EVERYWHERE (${ok.length}):`);
console.log(`  ${ok.map((r) => r.name).join(', ') || '(none)'}`);

if (localOnly.length > 0) {
  console.log(`\nLOCAL PLANE ONLY (${localOnly.length}) — offered by the`
    + ' editor but refused by legend-engine:');
  for (const r of localOnly) {
    console.log(`  XX  ${r.name}\n      ${r.engine}`);
  }
}
if (broken.length > 0) {
  console.log(`\nDOES NOT LOWER AT ALL (${broken.length}) — remove it from`
    + ' CALC_FUNCTIONS or fix the example:');
  for (const r of broken) {
    console.log(`  XX  ${r.name}\n      ${r.local}`);
  }
}

const bad = localOnly.length + broken.length + (typesDiffer ? 1 : 0);
console.log(`\n${ok.length}/${results.length} offered functions lower`
  + `${executor ? ' on both planes' : ' locally'}`);
console.log(bad === 0
  ? '\n*** the editor only offers what the backends can run ***'
  : `\n!!! ${bad} functions are offered but do not work !!!`);
process.exit(bad === 0 ? 0 : 1);
