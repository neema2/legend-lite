// Does the REAL legend-engine compile everything this cube emits?
//
// Not "does a query work" -- every operation the serialiser can
// produce: each aggregate function, each filter operator, the pivot's
// two-stage cast, a level scope's parent conditions, the grand total,
// the measureless group. One case each, sent to a running upstream
// engine, and the answer recorded per case.
//
// This is the compatibility claim the whole clean-room rewrite rests
// on: that the Pure we emit is Pure that upstream accepts. It was an
// assumption until this file existed.
//
//   ENGINE=http://127.0.0.1:6300 bazel run //datacube:verify_engine
//
// Start an engine first -- see the shaded-jar recipe: no JDK, Maven
// or Docker install is needed on this Mac.

import { readFile } from 'node:fs/promises';

import { LegendEngineExecutor } from '../src/engine-remote.ts';
import { serialize } from '../src/serialize.ts';

const ENGINE = (process.env.ENGINE ?? 'http://127.0.0.1:6300')
  .replace(/\/$/, '');
const API = `${ENGINE}/api/pure/v1`;
const ONLY = process.env.ONLY;
const RUNTIME = 'trades::RT';

/** The demo model's table, as the cube sees it. */
import { ENGINE_COLUMNS, casesFor } from './engine-cases.mjs';

const CASES = casesFor('#>{trades::DB.TRADES}#');

async function post(path, body, text = false) {
  const response = await fetch(`${API}${path}`, {
    method: 'POST',
    headers: { 'Content-Type': text ? 'text/plain' : 'application/json' },
    body: text ? body : JSON.stringify(body),
    signal: AbortSignal.timeout(120_000),
  });
  const raw = await response.text();
  if (!response.ok) {
    let message = raw.slice(0, 300);
    try {
      message = JSON.parse(raw).message ?? message;
    } catch {
      // keep the raw text
    }
    throw new Error(message.replace(/\s+/g, ' ').slice(0, 220));
  }
  return JSON.parse(raw);
}

/** Every SQL an execution plan carries, in order. */
function sqlOf(node, out = []) {
  if (Array.isArray(node)) {
    for (const v of node) sqlOf(v, out);
  } else if (node && typeof node === 'object') {
    if (typeof node.sqlQuery === 'string') out.push(node.sqlQuery);
    for (const v of Object.values(node)) sqlOf(v, out);
  }
  return out;
}

const CONTEXT = {
  _type: 'BaseExecutionContext',
  queryTimeOutInSeconds: 60,
  enableConstraints: true,
};

let model;
const results = [];

try {
  const grammar = await readFile(
    new URL('./trades.pure', import.meta.url), 'utf8');
  model = await post('/grammar/grammarToJson/model', grammar, true);
  console.log(`\nmodel compiled by the engine at ${ENGINE}`);
} catch (e) {
  console.log(`\ncould not reach an engine at ${ENGINE}: ${e.message}`);
  console.log('start one first (the shaded jar needs no install), then'
    + ' re-run.');
  process.exit(2);
}

for (const { name, snapshot, scope } of CASES) {
  if (ONLY && !name.toLowerCase().includes(ONLY.toLowerCase())) continue;
  let pure;
  try {
    // THE RUNTIME NAMED IN THE QUERY. Our planners take it
    // out-of-band; a relation query sent to the engine carries it.
    pure = `${serialize(snapshot, scope)}->from(${RUNTIME})`;
  } catch (e) {
    results.push({ name, ok: false, where: 'our serialiser',
      detail: String(e.message ?? e).slice(0, 200) });
    continue;
  }
  try {
    const lambda = await post('/grammar/grammarToJson/lambda', pure, true);
    const plan = await post('/execution/generatePlan', {
      clientVersion: 'vX_X_X',
      function: lambda,
      model,
      context: CONTEXT,
    });
    const sql = sqlOf(plan);
    if (sql.length === 0) {
      results.push({ name, ok: false, where: 'the plan', pure,
        detail: 'the plan carries no SQL' });
    } else {
      results.push({ name, ok: true, pure, sql: sql.at(-1) });
    }
  } catch (e) {
    results.push({ name, ok: false, where: 'the engine', pure,
      detail: String(e.message ?? e) });
  }
}

// -- SERVER MODE: the engine runs it ---------------------------------
//
// Compiling is not answering. This is the other half of the claim:
// the engine EXECUTES our Pure against a database the browser cannot
// reach -- the H2 it embeds -- and hands back rows, which is what
// upstream's uncached path does (`_runQuery` posts to
// `execution/execute` and renders the TDS).
//
// The figures are asserted, not printed. A query that runs and
// returns the wrong sums is the failure that looks like success.
if (!ONLY || 'server mode'.includes(ONLY.toLowerCase())) {
  try {
    const h2Model = await readFile(
      new URL('./trades-h2.pure', import.meta.url), 'utf8');
    const executor = new LegendEngineExecutor({
      baseUrl: ENGINE,
      model: h2Model,
      runtime: 'trades::h2::RT',
    });
    const snapshot = {
      source: { expression: '#>{trades::h2::DB.TRADES_SCHEMA.TRADES}#' },
      columns: ENGINE_COLUMNS,
      derived: [],
      rows: ['region'],
      pivotOn: [],
      measures: [{ name: 'notional', column: 'notional', fn: 'sum' }],
      sorts: [{ column: 'region', direction: 'asc' }],
      epoch: 3,
    };
    const out = await executor.execute(serialize(snapshot), snapshot);
    const by = Object.fromEntries(out.rows.columns.map((c) => [c.name, c]));
    // Derived from the seed's own shape (notional counts 1..N),
    // not copied from a run -- see tools that generate
    // trades-h2.pure's testDataSetupSqls.
    const seeded = { AMER: 528, APAC: 3088, EMEA: 2712,
      LATAM: 1928 };
    const regions = by['region']?.values ?? [];
    const notional = by['notional']?.values ?? [];
    const wrong = regions
      .map((r, i) => [r, notional[i], seeded[r]])
      .filter(([, got, want]) => Math.abs(Number(got) - want) > 0.001);
    if (out.rows.rowCount !== 4) {
      results.push({ name: 'server mode: the engine executes', ok: false,
        where: 'the engine', detail: `${out.rows.rowCount} rows, expected 4` });
    } else if (wrong.length > 0) {
      results.push({ name: 'server mode: the engine executes', ok: false,
        where: 'the figures',
        detail: wrong.map(([r, got, want]) =>
          `${r}: ${got} not ${want}`).join(', ') });
    } else if (!/^select /i.test(out.sql)) {
      // The SQL comes back as an execution ACTIVITY -- reported, not
      // run here. Without it the SQL pane has nothing true to show.
      results.push({ name: 'server mode: the engine executes', ok: false,
        where: 'the activity', detail: `no SQL reported: ${out.sql}` });
    } else {
      // AND THE `unique` AGGREGATE MEANS THE SAME THING THERE. The
      // engine lowers it to `case when count(distinct x) = 1 then
      // max(x) else null end`, which is exactly what this cube
      // documents it as -- so a group with two desks reads null and
      // one with a single desk reads the desk.
      const desks = Object.fromEntries(
        regions.map((r, i) => [r, (by['desk']?.values ?? [])[i]]));
      // LATAM trades on ONE desk and AMER on two, so unique() has to
      // answer the desk for one and null for the other. With every
      // region multi-desk this check passes while proving nothing.
      const agreed = desks['LATAM'] === 'Rates' && desks['AMER'] === null;
      results.push(agreed
        ? { name: 'server mode: the engine executes', ok: true,
            detail: `4 rows, sums agree, unique agrees` }
        : { name: 'server mode: the engine executes', ok: false,
            where: 'the unique aggregate',
            detail: `LATAM desk ${JSON.stringify(desks['LATAM'])},`
              + ` AMER desk ${JSON.stringify(desks['AMER'])}` });
    }
  } catch (e) {
    results.push({ name: 'server mode: the engine executes', ok: false,
      where: 'the executor', detail: String(e.message ?? e).slice(0, 220) });
  }
}

const ok = results.filter((r) => r.ok);
const bad = results.filter((r) => !r.ok);
for (const r of results) {
  console.log(r.ok
    ? `  ok   ${r.name}`
    : `  BAD  ${r.name} — ${r.where}: ${r.detail}`);
}
console.log(`\n${ok.length}/${results.length} operations compile on the`
  + ' real engine');
if (bad.length) {
  console.log(`\nNOT ACCEPTED (${bad.length}):`);
  for (const r of bad) {
    console.log(`\n  ${r.name}\n    ${r.where}: ${r.detail}`);
    if (r.pure) console.log(`    pure: ${r.pure.slice(0, 200)}`);
  }
}
console.log(bad.length === 0
  ? '\n*** the engine accepts every operation this cube emits ***'
  : `\n!!! ${bad.length} operations the engine does not accept !!!`);
process.exit(bad.length === 0 ? 0 : 1);
