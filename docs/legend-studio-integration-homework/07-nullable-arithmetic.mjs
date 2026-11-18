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


const F = '->meta::pure::mapping::from(local::rt)';
const gp = (q, mdl) => { const l = v1('/api/pure/v1/grammar/grammarToJson/lambda', 'returnSourceInformation=false', q); if (l.status !== 200) return 'PARSE ' + l.body.slice(0, 200);
  const g = v1('/api/pure/v1/execution/generatePlan', '', JSON.stringify({ function: JSON.parse(l.body), model: mdl })); return g.status === 200 ? 'OK  ' + JSON.parse(g.body).rootExecutionNode.executionNodes.at(-1).sqlQuery.replace(/\n\s*/g, ' ') : `ERR ${g.status} ` + g.body.slice(0, 220); };
const text = { _type: 'text', code: model };
const notNull = { _type: 'text', code: model.replace('amount FLOAT', 'amount FLOAT NOT NULL') };
for (const [name, q] of Object.entries({
  'nullable col * literal': `|${SRC}->extend(~[twice: x|$x.amount * 2])${F}`,
  'nullable col + nullable col': `|${SRC}->extend(~[s: x|$x.amount + $x.price])${F}`,
  'toOne() * literal': `|${SRC}->extend(~[twice: x|$x.amount->toOne() * 2])${F}`,
  'nullable string + literal': `|${SRC}->extend(~[n: x|$x.name + '!'])${F}`,
  'nullable col > literal (filter)': `|${SRC}->filter(x|$x.amount > 5)${F}`,
  'if() on nullable': `|${SRC}->extend(~[big: x|if($x.amount > 100, |'big', |'small')])${F}`,
})) console.log(`${name}\n   nullable column: ${gp(q, text)}\n   NOT NULL column: ${gp(q, notNull)}`);
const rt = v1('/api/pure/v1/compilation/lambdaRelationType', '', JSON.stringify({ lambda: JSON.parse(lam('|' + SRC)), model: text }));
console.log('\nmultiplicities of a text-declared table: ' + JSON.parse(rt.body).columns.map((c) => `${c.name}[${c.multiplicity.lowerBound}..${c.multiplicity.upperBound}]`).join(' '));
