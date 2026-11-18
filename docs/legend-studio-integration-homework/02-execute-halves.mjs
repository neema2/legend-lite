// Spike 2: execute's two halves (the "wasm executor" path) -- plan half only here; reports what the host must run.
import { readFileSync } from 'node:fs';
import { pathToFileURL } from 'node:url';
import { resolve } from 'node:path';
const [dir, modelPath] = process.argv.slice(2);
const { load } = await import(pathToFileURL(resolve(dir, 'wasm-gc-module-runtime.js')).href);
const m = await load(resolve(dir, 'classes.wasm'), { stackDeobfuscator: { enabled: false },
  installImports(o) { o.teavmConsole = { putcharStdout() {}, putcharStderr() {} }; } });
const un = (raw) => raw.startsWith('OK\n') ? { ok: true, body: raw.slice(3) } : { ok: false, body: raw };
const v1 = (path, q, body) => { const r = un(m.exports.pureV1OrError(path, q, body)); if (!r.ok) return r; const i = r.body.indexOf('\n'); const j = r.body.indexOf('\n', i + 1); return { ok: true, status: +r.body.slice(0, i), body: r.body.slice(j + 1) }; };
const modelText = readFileSync(modelPath, 'utf8');
const query = "|#>{trades::DB.TRADES}#->groupBy(~[desk], ~[total: x|$x.notional : y|$y->sum()])->from(trades::RT)";
const lambda = JSON.parse(v1('/api/pure/v1/grammar/grammarToJson/lambda', 'returnSourceInformation=false', query).body);
const pmcd = JSON.parse(v1('/api/pure/v1/grammar/grammarToJson/model', 'returnSourceInformation=false', modelText).body);
const base = { clientVersion: 'vX_X_X', function: lambda, context: { _type: 'BaseExecutionContext' }, parameterValues: [] };
for (const [name, model, models] of [
  ['model as TEXT, listed', { _type: 'text', code: modelText }, [modelText]],
  ['model as PMCD data, none listed', pmcd, []],
  ['model as TEXT, NOT listed', { _type: 'text', code: modelText }, []],
]) {
  const r = un(m.exports.executeJsonPlanOrError(JSON.stringify({ ...base, model }), JSON.stringify(models)));
  console.log(`\n== executeJsonPlan: ${name} -> ${r.ok ? 'OK' : 'ERR'}\n${r.body.slice(0, 700)}`);
}
// execute over HTTP route through the module (expected: refused, the run is the host's)
const ex = v1('/api/pure/v1/execution/execute', '', JSON.stringify({ ...base, model: pmcd }));
console.log(`\n== pureV1 execution/execute -> ${ex.ok ? ex.status : 'ERR'}\n${ex.body.slice(0, 400)}`);
for (const p of ['/api/pure/v1/grammar/grammarToJson/valueSpecification', '/api/pure/v1/grammar/jsonToGrammar/valueSpecification', '/api/pure/v1/compilation/lambdaRelationType/batch', '/api/pure/v1/codeCompletion/completeCode', '/api/pure/v1/compilation/autofix/transformTdsToRelation/lambda']) {
  const r = v1(p, '', '{}'); console.log(`route ${p} -> ${r.ok ? r.status : 'ERR'} ${r.body.slice(0, 120).replace(/\n/g, ' ')}`);
}
