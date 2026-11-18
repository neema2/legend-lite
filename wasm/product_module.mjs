/**
 * THE TAB'S PLANNER MODULE RUNS, AND HOLDS NO EDGE ENTRY (the user's K2 ruling, ~06:00 2026-11-18: "Product root, edge
 * opt-in"; the orchestrator's option A, the same day). //wasm:planner starts at lite's own entry (planner.TabExports):
 * loaded alone, as every app that plans in a tab loads it, it plans the demo's query through lite API v1 (query/parse,
 * then query/plan, as its main does), and exports none of legend-engine's routes -- those are the engine module's
 * (//wasm:engine_planner, wasm/engine_routes.mjs), loaded beside it only where an app is configured
 * `engine: 'legend-engine'`. Its build-time twin is //tools/deps:wasm_planner_closure_test (the module's closure
 * holds no edge target and no com.legend.edge class).
 *
 *   bazel test //wasm:product_module_test
 */
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { fileURLToPath } from 'node:url';
import { runfileDirUrl } from '../tools/js/runfiles.mts';

/** legend-engine's routes in the tab: the engine module's exports, and none of the product module's. */
const ENGINE_ROUTES = ['pureV1OrError', 'executeJsonPlanOrError', 'executeJsonAnswerOrError', 'executeArrowAnswerOrError'];

const MODEL = `###Relational
Database trades::DB
(
    Table TRADES
    (
        region VARCHAR(32), desk VARCHAR(32), notional DOUBLE
    )
)

###Connection
RelationalDatabaseConnection trades::Conn
{
    type: DuckDB;
    specification: DuckDB { };
    auth: Test;
}

###Runtime
Runtime trades::RT
{
    mappings: [];
    connections:
    [
        trades::DB: [ c1: trades::Conn ]
    ];
}
`;
const QUERY = "#>{trades::DB.TRADES}#->filter(x|$x.region == 'EMEA')->select(~[region, desk, notional])";

async function load(variable) {
  const dir = runfileDirUrl(variable);
  const { load: teavmLoad } = await import(new URL('wasm-gc-module-runtime.js', dir).href);
  return teavmLoad(fileURLToPath(new URL('classes.wasm', dir)), {
    stackDeobfuscator: { enabled: false },
    installImports(o) {
      o.teavmConsole = o.teavmConsole || {};
      o.teavmConsole.putcharStdout = () => {};
      o.teavmConsole.putcharStderr = () => {};
    },
  });
}

/** A folded lite answer's text (`OK\n<text>`), or the refusal it folded, named. */
function ok(folded) {
  assert.ok(folded.startsWith('OK\n'), `refused: ${folded.slice(0, 300)}`);
  return folded.slice(3);
}

test('the planner module plans the demo query through lite API v1, alone', async () => {
  const teavm = await load('WASM_PLANNER');
  const parsed = JSON.parse(ok(teavm.exports.queryParseOrError(JSON.stringify({ text: QUERY }))));
  const plan = JSON.parse(ok(teavm.exports.queryPlanOrError(JSON.stringify({ model: MODEL, query: parsed.query, runtime: 'trades::RT' }))));
  assert.match(JSON.stringify(plan), /FROM TRADES AS t0/, 'the plan carries the statement the tab runs');
});

test("the planner module exports none of legend-engine's routes", async () => {
  const teavm = await load('WASM_PLANNER');
  // TeaVM's exports are own properties that do not enumerate (Object.keys sees none): read by their own names
  const exported = Object.getOwnPropertyNames(teavm.exports);
  // the guard looking: the listing holds lite's own operations
  for (const own of ['queryParseOrError', 'queryPlanOrError', 'holdModelOrError', 'modelCheckOrError']) {
    assert.ok(exported.includes(own) && typeof teavm.exports[own] === 'function',
      `the planner module's exports, as read, hold no ${own}: the guard is not looking (${exported.length} read)`);
  }
  assert.deepEqual(exported.filter((e) => ENGINE_ROUTES.includes(e)), [],
    "the planner module exports legend-engine's routes: they are the engine module's (K2, product root, edge opt-in)");
  assert.deepEqual(ENGINE_ROUTES.filter((e) => teavm.exports[e] !== undefined), [],
    "the planner module answers legend-engine's routes by name: they are the engine module's (K2)");
});
