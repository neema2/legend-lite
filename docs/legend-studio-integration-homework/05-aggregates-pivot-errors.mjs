// Spike 3: the EXACT model legend-studio's DataCube synthesizes for a DuckDB-backed source (cache / local file),
// and DataCube-shaped queries, through legend-lite's planner module.
import { pathToFileURL } from 'node:url';
import { resolve } from 'node:path';
const [dir] = process.argv.slice(2);
const { load } = await import(pathToFileURL(resolve(dir, 'wasm-gc-module-runtime.js')).href);
const m = await load(resolve(dir, 'classes.wasm'), { stackDeobfuscator: { enabled: false },
  installImports(o) { o.teavmConsole = { putcharStdout() {}, putcharStderr() {} }; } });
const v1 = (path, q, body) => { const t = performance.now(); const raw = m.exports.pureV1OrError(path, q, body); const ms = performance.now() - t;
  if (!raw.startsWith('OK\n')) return { status: 'ERR', body: raw, ms };
  const r = raw.slice(3); const i = r.indexOf('\n'); const j = r.indexOf('\n', i + 1); return { status: +r.slice(0, i), body: r.slice(j + 1), ms }; };

const col = (name, t) => ({ name, type: { _type: t } });
// as probed from legend-studio's serializer: no sizes, no nullable, auth h2Default, datasource duckDB path /tmpFile
const model = { _type: 'data', elements: [
  { _type: 'relational', filters: [], joins: [], name: 'db', package: 'local',
    schemas: [{ name: 'main', tables: [{ columns: [col('name', 'Varchar'), col('region', 'Varchar'), col('age', 'Integer'), col('amount', 'Float'),
      col('price', 'Double'), col('d', 'Date'), col('ts', 'Timestamp'), col('flag', 'Bit'), col('dec', 'Decimal'), col('t', 'TinyInt'), col('s', 'SmallInt'), col('b', 'BigInt')],
      name: 'cache1', primaryKey: [] }], views: [] }] },
  { _type: 'connection', name: 'conn', package: 'local', connectionValue: { _type: 'RelationalDatabaseConnection',
      authenticationStrategy: { _type: process.env.AUTH ?? 'h2Default' }, databaseType: 'DuckDB', datasourceSpecification: { _type: 'duckDB', path: '/tmpFile' },
      element: 'local::db', postProcessorWithParameter: [], type: 'DuckDB' } },
  { _type: 'runtime', name: 'rt', package: 'local', runtimeValue: { _type: 'engineRuntime', connectionStores: [], mappings: [],
    connections: [{ store: { path: 'local::db', type: 'STORE' }, storeConnections: [{ id: 'c0', connection: { _type: 'connectionPointer', connection: 'local::conn' } }] }] } }] };
const SRC = '#>{local::db.main.cache1}#';
const FROM = '->meta::pure::mapping::from(local::rt)';
const A = (n, f) => `${SRC}->groupBy(~[region], ~[${n}: x|$x.${f[0]} : x|$x->${f[1]}])${FROM}`;
const cases = {
  'agg average': A('a', ['amount', 'average()']), 'agg min': A('a', ['amount', 'min()']), 'agg max': A('a', ['amount', 'max()']),
  'agg uniqueValueOnly': A('a', ['name', 'uniqueValueOnly()']), 'agg first': A('a', ['name', 'first()']), 'agg last': A('a', ['name', 'last()']),
  'agg count of column': A('a', ['name', 'count()']), 'agg distinct count?': A('a', ['name', 'distinct()->count()']),
  'sort multi + slice(50,100)': `${SRC}->sort([~region->ascending(), ~amount->descending()])->slice(50, 100)${FROM}`,
  'drilldown filter on null key': `${SRC}->filter(x|$x.region->isEmpty())->groupBy(~[name], ~[amount: x|$x.amount : x|$x->sum()])${FROM}`,
  'pivot + cast, 2 aggs incl count': `${SRC}->select(~[region, name, amount])->sort([~region->ascending()])->pivot(~[region], ~[amount: x|$x.amount : x|$x->sum(), count: x|$x : x|$x->count()])->cast(@meta::pure::metamodel::relation::Relation<(name:String, 'EMEA__|__amount':Float, 'EMEA__|__count':Integer)>)->slice(0, 100)${FROM}`,
};
const lam = (q) => v1('/api/pure/v1/grammar/grammarToJson/lambda', 'returnSourceInformation=false', '|' + q);
let ok = 0, n = 0;
for (const [name, q] of Object.entries(cases)) {
  n++;
  const l = lam(q);
  if (l.status !== 200) { console.log(`\n[PARSE ${l.status}] ${name}\n   ${l.body.slice(0, 300)}`); continue; }
  const input = { clientVersion: 'vX_X_X', function: JSON.parse(l.body), model, context: { _type: 'BaseExecutionContext', queryTimeOutInSeconds: null, enableConstraints: true }, parameterValues: [] };
  const p = v1('/api/pure/v1/execution/generatePlan', '', JSON.stringify(input));
  if (p.status !== 200) { console.log(`\n[PLAN ${p.status}] ${name}\n   ${p.body.slice(0, 400)}`); continue; }
  const plan = JSON.parse(p.body);
  const kids = plan.rootExecutionNode?.executionNodes ?? [];
  const sql = kids.filter((k) => k._type === 'sql').at(-1)?.sqlQuery;
  const shape = `${plan.rootExecutionNode?._type} > [${kids.map((k) => k._type).join(', ')}]`;
  if (sql === undefined) { console.log(`\n[NO SQL AT STUDIO'S POSITION] ${name}\n   shape: ${shape}\n   ${p.body.slice(0, 500)}`); continue; }
  ok++;
  console.log(`\n[OK ${p.ms.toFixed(1)} ms] ${name}   (${shape})\n   ${sql.replace(/\n/g, '\n   ')}`);
}
console.log(`\n${ok}/${n} DataCube-shaped queries planned with SQL where legend-studio reads it`);
// the source's columns, as processSource asks: no from()
const rt = v1('/api/pure/v1/compilation/lambdaRelationType', '', JSON.stringify({ lambda: JSON.parse(lam(SRC).body), model }));
console.log(`\nlambdaRelationType of the bare source [${rt.status}]:`);
if (rt.status === 200) for (const c of JSON.parse(rt.body).columns) console.log(`   ${c.name}: ${c.genericType?.rawType?.fullPath}${c.genericType?.typeVariableValues?.length ? ' ' + JSON.stringify(c.genericType.typeVariableValues) : ''}`);
else console.log('   ' + rt.body.slice(0, 400));
// the synthetic source DataCube uses to type-check an extended column
const syn = lam(`''->cast(@meta::pure::metamodel::relation::Relation<(name:String, amount:Float)>)->extend(~[twice: x|$x.amount * 2])`);
const st = syn.status === 200 ? v1('/api/pure/v1/compilation/lambdaRelationType', '', JSON.stringify({ lambda: JSON.parse(syn.body), model: {} })) : syn;
console.log(`\nlambdaRelationType of cast('', @Relation<...>)->extend(...) with model {} [${st.status}]: ${st.body.slice(0, 300)}`);

{
  const q = `${SRC}->select(~[region, name, amount])->pivot(~[region], ~[amount: x|$x.amount : x|$x->sum()])->limit(0)${FROM}`;
  const input = { clientVersion: 'vX_X_X', function: JSON.parse(lam(q).body), model, context: { _type: 'BaseExecutionContext' }, parameterValues: [] };
  const p = v1('/api/pure/v1/execution/generatePlan', '', JSON.stringify(input));
  const plan = JSON.parse(p.body); delete plan.templateFunctions;
  console.log('\nDYNAMIC PIVOT, full plan (templateFunctions dropped):\n' + JSON.stringify(plan).slice(0, 2600));
  const bad = { ...input, function: JSON.parse(lam(`${SRC}->filter(x|$x.nosuch == 1)${FROM}`).body) };
  const e = v1('/api/pure/v1/execution/generatePlan', '', JSON.stringify(bad));
  console.log(`\nCOMPILE ERROR via generatePlan [${e.status}]: ${e.body.slice(0, 400)}`);
  const e2 = v1('/api/pure/v1/compilation/lambdaRelationType', '', JSON.stringify({ lambda: bad.function, model }));
  console.log(`\nCOMPILE ERROR via lambdaRelationType [${e2.status}]: ${e2.body.slice(0, 400)}`);
  const e3 = v1('/api/pure/v1/grammar/grammarToJson/lambda', 'returnSourceInformation=true', '|1 +');
  console.log(`\nPARSE ERROR via grammarToJson/lambda [${e3.status}]: ${e3.body.slice(0, 400)}`);
}
