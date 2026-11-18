// Spike 4: the same DuckDB model as Pure TEXT (what legend-lite's own clients send), and the planner's direct exports.
import { pathToFileURL } from 'node:url'; import { resolve } from 'node:path';
const [dir] = process.argv.slice(2);
const { load } = await import(pathToFileURL(resolve(dir, 'wasm-gc-module-runtime.js')).href);
const m = await load(resolve(dir, 'classes.wasm'), { stackDeobfuscator: { enabled: false }, installImports(o) { o.teavmConsole = { putcharStdout() {}, putcharStderr() {} }; } });
const timed = (f) => { const t = performance.now(); const r = f(); return [r, performance.now() - t]; };
const v1 = (path, q, body) => { const raw = m.exports.pureV1OrError(path, q, body); const r = raw.slice(3); const i = r.indexOf('\n'); const j = r.indexOf('\n', i + 1); return { status: +r.slice(0, i), body: r.slice(j + 1) }; };
const model = `###Relational
Database local::db
(
  Schema main
  (
    Table cache1 ( name VARCHAR(16777216), region VARCHAR(16777216), age INTEGER, amount FLOAT, price DOUBLE, d DATE, ts TIMESTAMP, flag BIT )
  )
)
###Connection
RelationalDatabaseConnection local::conn
{
  store: local::db;
  type: DuckDB;
  specification: DuckDB { path: '/tmpFile'; };
  auth: Test;
}
###Runtime
Runtime local::rt
{
  mappings: [];
  connections: [ local::db: [ c0: local::conn ] ];
}`;
const SRC = '#>{local::db.main.cache1}#';
const q1 = `|${SRC}->filter(x|$x.region == 'EMEA')->groupBy(~[region], ~[amount: x|$x.amount : x|$x->sum()])->slice(0, 500)->meta::pure::mapping::from(local::rt)`;
const qPivot = `|${SRC}->select(~[region, name, amount])->pivot(~[region], ~[amount: x|$x.amount : x|$x->sum()])->limit(0)->meta::pure::mapping::from(local::rt)`;
const lam = (q) => v1('/api/pure/v1/grammar/grammarToJson/lambda', 'returnSourceInformation=false', q).body;

// (a) generatePlan with a TEXT model
const g = v1('/api/pure/v1/execution/generatePlan', '', JSON.stringify({ clientVersion: 'vX_X_X', function: JSON.parse(lam(q1)), model: { _type: 'text', code: model }, context: { _type: 'BaseExecutionContext' }, parameterValues: [] }));
console.log(`(a) generatePlan, model as TEXT [${g.status}]: ${g.status === 200 ? JSON.parse(g.body).rootExecutionNode.executionNodes.at(-1).sqlQuery.replace(/\n/g, ' ') : g.body.slice(0, 300)}`);

// (b) the planner's own export: model text + lambda JSON -> {sql, type, plan?}
const [b, bms] = timed(() => m.exports.planJsonOrError(model, lam(q1), 'local::rt'));
console.log(`\n(b) planJsonOrError [${bms.toFixed(1)} ms]: ${b.slice(0, 700)}`);

// (c) hold the model once, plan many times
const [h, hms] = timed(() => m.exports.holdModelOrError(model));
console.log(`\n(c) holdModelOrError [${hms.toFixed(1)} ms]: ${JSON.stringify(h).slice(0, 80)}`);
const handle = Number(h.slice(3).trim());
const times = [];
for (let i = 0; i < 20; i++) { const [, ms] = timed(() => m.exports.planJsonHeldOrError(handle, lam(q1.replace('500', String(500 + i))), 'local::rt')); times.push(ms); }
times.sort((x, y) => x - y);
console.log(`    planJsonHeldOrError x20 distinct queries: median ${times[10].toFixed(2)} ms, max ${times[19].toFixed(2)} ms`);
const [rt, rms] = timed(() => m.exports.relationTypeHeldOrError(handle, `|${SRC}->extend(~[twice: x|$x.amount * 2])`));
console.log(`    relationTypeHeldOrError [${rms.toFixed(1)} ms]: ${rt.slice(0, 260)}`);

// (d) the dynamic pivot through the planner's own export: what a host would have to run
const [p] = timed(() => m.exports.planJsonHeldOrError(handle, lam(qPivot), 'local::rt'));
console.log(`\n(d) dynamic pivot via planJsonHeldOrError: ${p.slice(0, 1500)}`);
