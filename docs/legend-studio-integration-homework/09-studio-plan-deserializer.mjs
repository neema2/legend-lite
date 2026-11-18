// Spike 6: legend-lite's generatePlan answer, read by LEGEND-STUDIO's own plan deserializer and DataCube's extraction.
// usage: node deser.mjs <planner dir> <legend-studio root>
import { pathToFileURL } from 'node:url'; import { resolve } from 'node:path';
const [dir, studio] = process.argv.slice(2);
const G = await import(pathToFileURL(resolve(studio, 'packages/legend-graph/lib/index.js')).href);
const { load } = await import(pathToFileURL(resolve(dir, 'wasm-gc-module-runtime.js')).href);
const m = await load(resolve(dir, 'classes.wasm'), { stackDeobfuscator: { enabled: false }, installImports(o) { o.teavmConsole = { putcharStdout() {}, putcharStderr() {} }; } });
const v1 = (path, q, body) => { const raw = m.exports.pureV1OrError(path, q, body); const r = raw.slice(3); const i = r.indexOf('\n'); const j = r.indexOf('\n', i + 1); return { status: +r.slice(0, i), body: r.slice(j + 1) }; };
const model = { _type: 'text', code: `###Relational
Database local::db ( Schema main ( Table cache1 ( name VARCHAR(100), region VARCHAR(100), amount FLOAT ) ) )
###Connection
RelationalDatabaseConnection local::conn { store: local::db; type: DuckDB; specification: DuckDB { path: '/tmpFile'; }; auth: Test; }
###Runtime
Runtime local::rt { mappings: []; connections: [ local::db: [ c0: local::conn ] ]; }` };
const lambda = JSON.parse(v1('/api/pure/v1/grammar/grammarToJson/lambda', 'returnSourceInformation=false',
  "|#>{local::db.main.cache1}#->groupBy(~[region], ~[amount: x|$x.amount : x|$x->sum()])->meta::pure::mapping::from(local::rt)").body);
const answer = v1('/api/pure/v1/execution/generatePlan', '', JSON.stringify({ function: lambda, model }));
// what the app does at startup before any plan is read (V1_PureGraphManager.initialize)
for (const setup of ['V1_setupDatabaseSerialization', 'V1_setupEngineRuntimeSerialization', 'V1_setupLegacyRuntimeSerialization']) { try { G[setup]?.([]); } catch (e) { console.log(`${setup}: ${e.message}`); } }
const plan = G.V1_deserializeExecutionPlan(JSON.parse(answer.body));
// LegendDataCubeDataCubeEngine.ts:1071-1078, verbatim logic
const sql = plan instanceof G.V1_SimpleExecutionPlan
  ? plan.rootExecutionNode.executionNodes.filter((n) => n instanceof G.V1_SQLExecutionNode).at(-1)?.sqlQuery : undefined;
console.log(`plan class: ${plan.constructor.name}; root: ${plan.rootExecutionNode.constructor.name}`);
console.log(`SQL extracted by DataCube's own logic:\n${sql}`);
const rel = v1('/api/pure/v1/compilation/lambdaRelationType', '', JSON.stringify({ lambda, model }));
const cols = G.deserialize ? null : null;
try { const { deserialize } = await import(pathToFileURL(resolve(studio, 'node_modules/serializr/lib/serializr.js')).href).catch(() => ({}));
  if (deserialize && G.V1_relationTypeModelSchema) { const t = deserialize(G.V1_relationTypeModelSchema, JSON.parse(rel.body));
    console.log('relation type read by studio: ' + t.columns.map((c) => `${c.name}:${G.V1_getGenericTypeFullPath ? G.V1_getGenericTypeFullPath(c.genericType) : '?'}`).join(', ')); }
  else console.log('relation type: serializr or schema not importable here (skipped)'); } catch (e) { console.log('relation type read failed: ' + e.message); }
