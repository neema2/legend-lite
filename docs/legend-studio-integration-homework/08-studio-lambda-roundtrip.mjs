// Spike 5: each DataCube-shaped query is parsed by legend-lite, then re-serialized by LEGEND-STUDIO's own protocol classes
// (legend-graph: V1_deserializeValueSpecification -> V1_serializeValueSpecification), and that JSON is planned by legend-lite.
// usage: node roundtrip.mjs <planner dir> <legend-studio root>
import { pathToFileURL } from 'node:url'; import { resolve } from 'node:path';
const [dir, studio] = process.argv.slice(2);
const G = await import(pathToFileURL(resolve(studio, 'packages/legend-graph/lib/index.js')).href);
const { load } = await import(pathToFileURL(resolve(dir, 'wasm-gc-module-runtime.js')).href);
const m = await load(resolve(dir, 'classes.wasm'), { stackDeobfuscator: { enabled: false }, installImports(o) { o.teavmConsole = { putcharStdout() {}, putcharStderr() {} }; } });
const v1 = (path, q, body) => { const raw = m.exports.pureV1OrError(path, q, body); const r = raw.slice(3); const i = r.indexOf('\n'); const j = r.indexOf('\n', i + 1); return { status: +r.slice(0, i), body: r.slice(j + 1) }; };
const model = `###Relational
Database local::db ( Schema main ( Table cache1 ( name VARCHAR(100) NOT NULL, region VARCHAR(100), age INTEGER, amount FLOAT NOT NULL, price DOUBLE, d DATE, ts TIMESTAMP, flag BIT ) ) )
###Connection
RelationalDatabaseConnection local::conn { store: local::db; type: DuckDB; specification: DuckDB { path: '/tmpFile'; }; auth: Test; }
###Runtime
Runtime local::rt { mappings: []; connections: [ local::db: [ c0: local::conn ] ]; }`;
const handle = Number(m.exports.holdModelOrError(model).slice(3));
const SRC = '#>{local::db.main.cache1}#', FROM = '->meta::pure::mapping::from(local::rt)';
const cases = {
  'bare source': `${SRC}${FROM}`,
  'filter+select+groupBy+count+sort+slice': `${SRC}->filter(x|$x.region == 'EMEA')->select(~[region, name, age, amount])->groupBy(~[region], ~[amount: x|$x.amount : x|$x->sum(), count: x|$x : x|$x->count()])->sort([~amount->descending()])->slice(0, 500)${FROM}`,
  'leaf extend + limit': `${SRC}->extend(~[twice: x|$x.amount * 2])->limit(100)${FROM}`,
  'filters: in, isEmpty, not, and/or': `${SRC}->filter(x|($x.region->in(['EMEA','APAC']) && !($x.name->isEmpty())) || ($x.age >= 30))${FROM}`,
  'filters: case-insensitive': `${SRC}->filter(x|$x.name->toOne()->toLower()->contains('ab'->toLower()))${FROM}`,
  'filters: dates, today/now': `${SRC}->filter(x|($x.d >= %2024-01-01) && ($x.d <= today()) && ($x.ts < now()))${FROM}`,
  'aggregates': `${SRC}->groupBy(~[region], ~[a: x|$x.amount : x|$x->average(), u: x|$x.name : x|$x->uniqueValueOnly(), j: x|$x.name : x|$x->joinStrings(','), sd: x|$x.amount : x|$x->stdDevSample()])${FROM}`,
  'pivot + cast + groupBy + slice': `${SRC}->select(~[region, name, amount])->sort([~region->ascending()])->pivot(~[region], ~[amount: x|$x.amount : x|$x->sum()])->cast(@meta::pure::metamodel::relation::Relation<(name:String, 'EMEA__|__amount':Float, 'APAC__|__amount':Float)>)->groupBy(~[name], ~['EMEA__|__amount': x|$x.'EMEA__|__amount' : x|$x->sum()])->slice(0, 100)${FROM}`,
};
const plan = (lambdaJson) => m.exports.planJsonHeldOrError(handle, lambdaJson, 'local::rt');
const sqlOf = (ans) => ans.startsWith('OK\n') ? JSON.parse(ans.slice(3)).sql : undefined;
let same = 0, n = 0;
for (const [name, q] of Object.entries(cases)) {
  n++;
  const lite = v1('/api/pure/v1/grammar/grammarToJson/lambda', 'returnSourceInformation=false', '|' + q).body;
  let studioJson;
  try {
    // exactly what DataCube does before a call: its protocol objects, serialized by its own serializer
    const vs = G.V1_deserializeValueSpecification(JSON.parse(lite), []);
    studioJson = JSON.stringify(G.V1_serializeValueSpecification(vs, []));
  } catch (e) { console.log(`[STUDIO COULD NOT ROUNDTRIP] ${name}: ${String(e.message).slice(0, 200)}`); continue; }
  const a = plan(lite), b = plan(studioJson);
  const changed = studioJson !== lite;
  if (sqlOf(a) !== undefined && sqlOf(a) === sqlOf(b)) { same++; console.log(`[SAME SQL] ${name}   (studio's JSON ${changed ? 'differs from' : 'equals'} lite's: ${lite.length} -> ${studioJson.length} chars)`); }
  else console.log(`[DIFFERENT] ${name}\n   lite's lambda:   ${a.slice(0, 220).replace(/\n/g, ' ')}\n   studio's lambda: ${b.slice(0, 320).replace(/\n/g, ' ')}`);
}
console.log(`\n${same}/${n} queries plan to the same SQL from legend-studio's serialization of the lambda`);
