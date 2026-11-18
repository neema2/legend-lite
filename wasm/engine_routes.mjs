/**
 * LEGEND-ENGINE'S ROUTES IN THE TAB, BYTE-EXACT (K2, "product root, edge opt-in", the user's ruling of 2026-11-18): the
 * engine module (//wasm:engine_planner, its entry com.legend.edge.wasm.EngineExports) asked a fixed set of requests on
 * each of its four exports -- pureV1OrError, executeJsonPlanOrError, executeJsonAnswerOrError and
 * executeArrowAnswerOrError -- and each answer's SHA-256 held to the pin engine-routes.json carries. The pins were
 * captured from //wasm:planner BEFORE the split (when the four were its exports), so the split moved no byte of
 * legend-engine's answers. A pin moves only with a dated note in the file's own `note`: an answer that changes is
 * legend-engine's route changing, which the edge's own tests rule on first.
 *
 * Execute's plan half mints its host execution's id (a random UUID, HostAnswers.jsonPlan), so an answer's UUIDs are
 * written as one fixed id before it is hashed, and the plan handed to the answer halves is that fixed text: every other
 * byte is the module's.
 *
 *   bazel test //wasm:engine_routes_test
 *   ENGINE_ROUTES_CAPTURE=<file> WASM_ENGINE_PLANNER=<module dir> RUNFILES_DIR=/ \
 *     node --experimental-wasm-exnref --experimental-strip-types engine_routes.mjs
 *   writes the pins of the module in that directory (to capture them before the split: //wasm:planner_dir, which
 *   exported the four then)
 */
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFileSync, writeFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { runfileDirUrl, runfileFromEnv } from '../tools/js/runfiles.mts';

const sha256 = (s) => createHash('sha256').update(s, 'utf8').digest('hex');
const UUID = /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/g;
const FIXED = '00000000-0000-4000-8000-000000000000';
/** An answer with its minted ids written as the one fixed id. */
const fixed = (s) => s.replace(UUID, FIXED);

const MODEL = `###Relational
Database trades::DB
(
    Table TRADES
    (
        region VARCHAR(32), desk VARCHAR(32), book VARCHAR(32),
        year INTEGER, qtr VARCHAR(8),
        notional DOUBLE, pnl DOUBLE, qty INTEGER
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
const BROKEN = 'Class a::B { x: Strin[1]; }';
const CLASSES = 'Class a::Person\n{\n  name: String[1];\n  age: Integer[0..1];\n}\n\nEnum a::Kind\n{\n  ONE,\n  TWO\n}\n';
const QUERY = "|#>{trades::DB.TRADES}#->filter(x|$x.region == 'EMEA')->select(~[region, desk, notional])"
  + '->groupBy(~[region, desk], ~[total:x|$x.notional:y|$y->sum()])->sort([~region->ascending()])->limit(10)';
const PARAMETERIZED = "{minQty: Integer[1]|#>{trades::DB.TRADES}#->filter(x|$x.qty > $minQty)->select(~[desk, qty])}";
const PURE_V1 = '/api/pure/v1';
const text = { _type: 'text', code: MODEL };
const CONTEXT = { clientVersion: 'vX_X_X', context: { _type: 'BaseExecutionContext' } };

/** A folded pure/v1 answer's body when it is a 200 (`OK\n200\n<type>\n<body>`). */
function okBody(folded) {
  const prefix = 'OK\n200\n';
  assert.ok(folded.startsWith(prefix), `not a 200: ${folded.slice(0, 300)}`);
  return folded.slice(folded.indexOf('\n', prefix.length) + 1);
}

/** The requests, in order: each a name, the export asked and its arguments (a function of the answers before it). */
function requests(ask) {
  const lambda = okBody(ask('pureV1OrError', `${PURE_V1}/grammar/grammarToJson/lambda`, 'returnSourceInformation=false', QUERY));
  const parameterized = okBody(ask('pureV1OrError', `${PURE_V1}/grammar/grammarToJson/lambda`, 'returnSourceInformation=false', PARAMETERIZED));
  const model = okBody(ask('pureV1OrError', `${PURE_V1}/grammar/grammarToJson/model`, 'returnSourceInformation=false', MODEL));
  const classes = okBody(ask('pureV1OrError', `${PURE_V1}/grammar/grammarToJson/model`, 'returnSourceInformation=false', CLASSES));
  const input = (fn, extra = {}) => JSON.stringify({ ...CONTEXT, function: JSON.parse(fn), model: text, runtime: { runtime: 'trades::RT' }, ...extra });
  const typing = (fn) => JSON.stringify({ model: text, lambda: JSON.parse(fn) });
  const plan = fixed(okBody(ask('executeJsonPlanOrError', input(lambda), JSON.stringify([MODEL]))));
  const objects = (n) => JSON.stringify(Array.from({ length: n }, (_, i) => ({ region: 'EMEA', desk: `d${i}`, total: 1.5 * (i + 1) })));
  return [
    // ---- pure/v1: grammar both ways, with and without source information, and its refusals
    ['grammarToJson/model', 'pureV1OrError', `${PURE_V1}/grammar/grammarToJson/model`, 'returnSourceInformation=false', MODEL],
    ['grammarToJson/model, source information', 'pureV1OrError', `${PURE_V1}/grammar/grammarToJson/model`, '', MODEL],
    ['grammarToJson/model, a parse error', 'pureV1OrError', `${PURE_V1}/grammar/grammarToJson/model`, '', 'Class a::B {'],
    ['grammarToJson/lambda', 'pureV1OrError', `${PURE_V1}/grammar/grammarToJson/lambda`, 'returnSourceInformation=false', QUERY],
    ['grammarToJson/lambda, source information', 'pureV1OrError', `${PURE_V1}/grammar/grammarToJson/lambda`, '', QUERY],
    ['grammarToJson/lambda, a parse error', 'pureV1OrError', `${PURE_V1}/grammar/grammarToJson/lambda`, '', '|$x->'],
    ['jsonToGrammar/lambda STANDARD', 'pureV1OrError', `${PURE_V1}/grammar/jsonToGrammar/lambda`, 'renderStyle=STANDARD', lambda],
    ['jsonToGrammar/lambda PRETTY', 'pureV1OrError', `${PURE_V1}/grammar/jsonToGrammar/lambda`, 'renderStyle=PRETTY', lambda],
    ['jsonToGrammar/model STANDARD, classes', 'pureV1OrError', `${PURE_V1}/grammar/jsonToGrammar/model`, 'renderStyle=STANDARD', classes],
    ['jsonToGrammar/model PRETTY, classes', 'pureV1OrError', `${PURE_V1}/grammar/jsonToGrammar/model`, 'renderStyle=PRETTY', classes],
    ['jsonToGrammar/model STANDARD', 'pureV1OrError', `${PURE_V1}/grammar/jsonToGrammar/model`, 'renderStyle=STANDARD', model],
    ['jsonToGrammar/model PRETTY', 'pureV1OrError', `${PURE_V1}/grammar/jsonToGrammar/model`, 'renderStyle=PRETTY', model],
    // ---- pure/v1: compile and typing
    ['compilation/compile', 'pureV1OrError', `${PURE_V1}/compilation/compile`, '', JSON.stringify(text)],
    ['compilation/compile, a model that does not compile', 'pureV1OrError', `${PURE_V1}/compilation/compile`, '',
      JSON.stringify({ _type: 'text', code: BROKEN })],
    ['compilation/lambdaReturnType', 'pureV1OrError', `${PURE_V1}/compilation/lambdaReturnType`, '', typing(lambda)],
    ['compilation/lambdaRelationType', 'pureV1OrError', `${PURE_V1}/compilation/lambdaRelationType`, '', typing(lambda)],
    // ---- pure/v1: plans, the execute the host runs refused, a route there is not
    ['execution/generatePlan', 'pureV1OrError', `${PURE_V1}/execution/generatePlan`, '', input(lambda)],
    ['execution/generatePlan, parameterized', 'pureV1OrError', `${PURE_V1}/execution/generatePlan`, '', input(parameterized)],
    ['execution/execute, refused: the host runs it', 'pureV1OrError', `${PURE_V1}/execution/execute`, 'serializationFormat=DEFAULT', input(lambda)],
    ['a route there is not', 'pureV1OrError', `${PURE_V1}/no/such/route`, '', '{}'],
    // lite's own routes, which the route export answers too (EdgeBoundary.pureV1)
    ['lite API v1 model/check', 'pureV1OrError', '/api/lite/v1/model/check', '', JSON.stringify({ model: MODEL })],
    ['lite API v1 model/check, a model that does not compile', 'pureV1OrError', '/api/lite/v1/model/check', '', JSON.stringify({ model: BROKEN })],
    // ---- execute's plan half
    ['executeJsonPlan', 'executeJsonPlanOrError', input(lambda), JSON.stringify([MODEL])],
    ['executeJsonPlan, parameterized', 'executeJsonPlanOrError',
      input(parameterized, { parameterValues: [{ name: 'minQty', value: { _type: 'integer', value: 6 } }] }), JSON.stringify([MODEL])],
    ['executeJsonPlan, a parameter with no value', 'executeJsonPlanOrError', input(parameterized), JSON.stringify([MODEL])],
    ['executeJsonPlan, a model the host does not run', 'executeJsonPlanOrError', input(lambda), JSON.stringify([])],
    ['executeJsonPlan, models that are not an array', 'executeJsonPlanOrError', input(lambda), '{}'],
    // ---- execute's answer halves, over the plan half's answer (its id fixed)
    ['executeJsonAnswer, three rows', 'executeJsonAnswerOrError', plan, objects(3)],
    ['executeJsonAnswer, no rows', 'executeJsonAnswerOrError', plan, '[]'],
    ['executeJsonAnswer, a null cell', 'executeJsonAnswerOrError', plan, '[{"region":null,"desk":"d","total":null}]'],
    ['executeJsonAnswer, a plan that is not jsonPlan\'s', 'executeJsonAnswerOrError', '{}', '[]'],
    ['executeJsonAnswer, a plan that does not parse', 'executeJsonAnswerOrError', '{', '[]'],
    ['executeArrowAnswer, three rows', 'executeArrowAnswerOrError', plan, objects(3)],
    ['executeArrowAnswer, no rows', 'executeArrowAnswerOrError', plan, '[]'],
    ['executeArrowAnswer, a plan that is not jsonPlan\'s', 'executeArrowAnswerOrError', '{}', '[]'],
  ];
}

async function loadModule() {
  const dir = runfileDirUrl('WASM_ENGINE_PLANNER');
  const { load } = await import(new URL('wasm-gc-module-runtime.js', dir).href);
  return load(fileURLToPath(new URL('classes.wasm', dir)), {
    stackDeobfuscator: { enabled: false },
    installImports(o) {
      o.teavmConsole = o.teavmConsole || {};
      o.teavmConsole.putcharStdout = () => {};
      o.teavmConsole.putcharStderr = () => {};
    },
  });
}

/** Every request's answer, its ids fixed: name -> answer. */
async function answers() {
  const teavm = await loadModule();
  const ask = (exp, ...args) => {
    assert.equal(typeof teavm.exports[exp], 'function', `the engine module exports no ${exp}`);
    return teavm.exports[exp](...args);
  };
  const out = new Map();
  for (const [name, exp, ...args] of requests(ask)) {
    assert.ok(!out.has(name), `two requests named ${name}`);
    out.set(name, fixed(ask(exp, ...args)));
  }
  return out;
}

const capture = process.env['ENGINE_ROUTES_CAPTURE'];
if (capture) {
  const got = await answers();
  const pins = Object.fromEntries([...got].map(([n, a]) => [n, sha256(a)]));
  writeFileSync(capture, JSON.stringify({ note: 'captured by engine_routes.mjs', pins }, null, 2) + '\n');
  for (const [n, a] of got) console.log(`${n}: ${JSON.stringify(a.slice(0, 160))}`);
} else {
  test("legend-engine's routes answer every request as they did before the split, byte for byte", async () => {
    const { pins } = JSON.parse(readFileSync(runfileFromEnv('ENGINE_ROUTES_PINS'), 'utf8'));
    const got = await answers();
    assert.deepEqual([...got.keys()].sort(), Object.keys(pins).sort(), 'the requests are the pins\' requests');
    const differing = [...got].filter(([n, a]) => sha256(a) !== pins[n]).map(([n, a]) => `${n}: ${JSON.stringify(a.slice(0, 300))}`);
    assert.deepEqual(differing, [], `${differing.length} answers differ from their pins`);
  });
}
