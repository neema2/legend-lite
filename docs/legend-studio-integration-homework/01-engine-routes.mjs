// Spike: legend-engine's pure/v1 routes answered by legend-lite's planner module (WebAssembly), in Node.
// usage: node run.mjs <dir holding classes.wasm + wasm-gc-module-runtime.js> <model.pure> [query]
import { readFileSync } from 'node:fs';
import { pathToFileURL } from 'node:url';
import { resolve } from 'node:path';

const [dir, modelPath, queryArg] = process.argv.slice(2);
const { load } = await import(pathToFileURL(resolve(dir, 'wasm-gc-module-runtime.js')).href);
const t0 = performance.now();
const m = await load(resolve(dir, 'classes.wasm'), {
  stackDeobfuscator: { enabled: false },
  installImports(o) { o.teavmConsole = { putcharStdout() {}, putcharStderr() {} }; },
});
const loaded = performance.now() - t0;

function call(path, query, body) {
  const t = performance.now();
  const raw = m.exports.pureV1OrError(path, query, body);
  const ms = performance.now() - t;
  if (!raw.startsWith('OK\n')) return { ok: false, raw, ms };
  const rest = raw.slice(3);
  const i = rest.indexOf('\n'); const j = rest.indexOf('\n', i + 1);
  return { ok: true, status: Number(rest.slice(0, i)), type: rest.slice(i + 1, j), body: rest.slice(j + 1), ms };
}
const show = (name, a, max = 600) => console.log(`\n== ${name}  [${a.ok ? a.status : 'ERR'}] ${a.ms.toFixed(1)} ms\n${(a.ok ? a.body : a.raw).slice(0, max)}`);

const modelText = readFileSync(modelPath, 'utf8');
const query = queryArg ?? "|#>{trades::DB.TRADES}#->filter(x|$x.region == 'EMEA')->groupBy(~[desk], ~[total: x|$x.notional : y|$y->sum()])->sort([~total->descending()])->limit(10)->from(trades::RT)";

console.log(`module loaded in ${loaded.toFixed(0)} ms`);
const pmcd = call('/api/pure/v1/grammar/grammarToJson/model', 'returnSourceInformation=false', modelText);
show('grammarToJson/model', pmcd, 300);
const lambda = call('/api/pure/v1/grammar/grammarToJson/lambda', 'returnSourceInformation=false', query);
show('grammarToJson/lambda', lambda, 300);
if (!pmcd.ok || !lambda.ok || pmcd.status !== 200 || lambda.status !== 200) process.exit(1);

const input = { clientVersion: 'vX_X_X', function: JSON.parse(lambda.body), model: JSON.parse(pmcd.body),
  context: { _type: 'BaseExecutionContext' }, parameterValues: [] };
const rel = call('/api/pure/v1/compilation/lambdaRelationType', '', JSON.stringify({ lambda: input.function, model: input.model }));
show('compilation/lambdaRelationType', rel, 900);
const plan1 = call('/api/pure/v1/execution/generatePlan', '', JSON.stringify(input));
show('execution/generatePlan (cold)', plan1, 2500);
const plan2 = call('/api/pure/v1/execution/generatePlan', '', JSON.stringify(input));
console.log(`\n== execution/generatePlan (warm) ${plan2.ms.toFixed(1)} ms`);
if (plan1.ok && plan1.status === 200) {
  const p = JSON.parse(plan1.body);
  // what legend-studio's DataCube does: the last SQL node's sqlQuery
  const nodes = [];
  (function walk(n) { if (!n || typeof n !== 'object') return; if (n._type) nodes.push(n); for (const c of n.executionNodes ?? []) walk(c); })(p.rootExecutionNode);
  console.log('\nplan keys:', Object.keys(p).join(', '));
  console.log('node types (pre-order):', nodes.map((n) => n._type).join(' > '));
  const sqlNode = (p.rootExecutionNode?.executionNodes ?? []).filter((n) => n._type === 'sql').at(-1);
  console.log('\nSQL as legend-studio would extract it (rootExecutionNode.executionNodes[_type=sql].at(-1).sqlQuery):\n' + (sqlNode?.sqlQuery ?? '<<none found at that position>>'));
}
const text = call('/api/pure/v1/grammar/jsonToGrammar/lambda', 'renderStyle=PRETTY', lambda.body);
show('jsonToGrammar/lambda', text, 300);
