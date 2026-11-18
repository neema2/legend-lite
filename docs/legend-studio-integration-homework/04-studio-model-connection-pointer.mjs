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
const cases = {
  'bare source': `${SRC}${FROM}`,
  'filter+select+groupBy+count+sort+slice': `${SRC}->filter(x|$x.region == 'EMEA')->select(~[region, name, age, amount])->groupBy(~[region], ~[amount: x|$x.amount : x|$x->sum(), count: x|$x : x|$x->count()])->sort([~amount->descending()])->slice(0, 500)${FROM}`,
  'leaf extend + limit': `${SRC}->extend(~[twice: x|$x.amount * 2])->limit(100)${FROM}`,
  'group extend after groupBy': `${SRC}->groupBy(~[region], ~[amount: x|$x.amount : x|$x->sum()])->extend(~[share: x|$x.amount / 100])${FROM}`,
  'filters: in, isEmpty, not, and/or': `${SRC}->filter(x|($x.region->in(['EMEA','APAC']) && !($x.name->isEmpty())) || ($x.age >= 30))${FROM}`,
  'filters: case-insensitive contains/startsWith/endsWith': `${SRC}->filter(x|$x.name->toOne()->toLower()->contains('ab'->toLower()) && $x.name->toOne()->startsWith('A') && !($x.name->toOne()->endsWith('z')))${FROM}`,
  'filters: column vs column, dates, today/now': `${SRC}->filter(x|($x.amount > $x.price) && ($x.d >= %2024-01-01) && ($x.d <= today()) && ($x.ts < now()))${FROM}`,
  'aggregates: avg,min,max,uniq,first,last': `${SRC}->groupBy(~[region], ~[a: x|$x.amount : x|$x->average(), mn: x|$x.amount : x|$x->min(), mx: x|$x.amount : x|$x->max(), u: x|$x.name : x|$x->uniqueValueOnly(), f: x|$x.name : x|$x->first(), l: x|$x.name : x|$x->last()])${FROM}`,
  'aggregates: var/std pop+sample, joinStrings': `${SRC}->groupBy(~[region], ~[vp: x|$x.amount : x|$x->variancePopulation(), vs: x|$x.amount : x|$x->varianceSample(), sp: x|$x.amount : x|$x->stdDevPopulation(), ss: x|$x.amount : x|$x->stdDevSample(), j: x|$x.name : x|$x->joinStrings(',')])${FROM}`,
  'pivot discovery (limit 0, no cast)': `${SRC}->select(~[region, name, amount])->sort([~region->ascending()])->pivot(~[region], ~[amount: x|$x.amount : x|$x->sum(), count: x|$x : x|$x->count()])->limit(0)${FROM}`,
  'pivot + cast + groupBy + slice': `${SRC}->select(~[region, name, amount])->sort([~region->ascending()])->pivot(~[region], ~[amount: x|$x.amount : x|$x->sum()])->cast(@meta::pure::metamodel::relation::Relation<(name:String, 'EMEA__|__amount':Float, 'APAC__|__amount':Float)>)->groupBy(~[name], ~['EMEA__|__amount': x|$x.'EMEA__|__amount' : x|$x->sum()])->slice(0, 100)${FROM}`,
  'filler count agg + sum of count (grid drilldown)': `${SRC}->groupBy(~[region, name], ~[INTERNAL__filler_count_agg_column: x|$x : x|$x->count()])->groupBy(~[region], ~[c: x|$x.INTERNAL__filler_count_agg_column : x|$x->sum()])${FROM}`,
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
