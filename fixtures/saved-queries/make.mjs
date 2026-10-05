// Writes fixtures/saved-queries/*.json: each a saved Query exactly as legend-lite's /api/pure/v1/query
// answers GET -- created through POST on a server started with --query-store -- and checks each
// content RUNS (its parameters bound from defaultParameterValues, ->from(mapping, runtime)) on the
// demo model, to the row counts README.md states, so a fixture is a real, runnable record. The
// timestamps are set to 0: they are incidental, and the bytes must not depend on the clock.
// //fixtures/saved-queries:gen runs this; `bazel run //fixtures/saved-queries:update_generated`
// writes the result (Bazel workplan P2-06).
//
//   make.mjs --java <java> --server-jar <server_deploy.jar> --model <file> [--model <file>]
//            --store <empty directory> --out <directory>
// Paths are relative to the execution root (JS_BINARY__EXECROOT), as Bazel passes them.

import { spawn } from 'node:child_process';
import { readFileSync, writeFileSync } from 'node:fs';
import { join, resolve } from 'node:path';

const opts = { model: [] };
for (let i = 2; i < process.argv.length; i += 2) {
  const [flag, value] = [process.argv[i], process.argv[i + 1]];
  if (!flag?.startsWith('--') || value === undefined) throw new Error(`usage: see make.mjs; got ${process.argv.slice(2).join(' ')}`);
  const key = flag.slice(2).replace(/-(.)/g, (_, c) => c.toUpperCase());
  if (key === 'model') opts.model.push(value); else opts[key] = value;
}
for (const k of ['java', 'serverJar', 'store', 'out']) if (!opts[k]) throw new Error(`make.mjs: --${k} is required`);
if (opts.model.length === 0) throw new Error('make.mjs: at least one --model');
const root = process.env.JS_BINARY__EXECROOT ?? process.cwd();
const at = (p) => resolve(root, p);

// the server on a port the system picks; it says which
const server = spawn(at(opts.java), ['-jar', at(opts.serverJar), '0', '--query-store', at(opts.store)],
  { stdio: ['ignore', 'pipe', 'inherit'] });
const port = await new Promise((done, fail) => {
  let seen = '';
  server.stdout.on('data', (chunk) => {
    seen += chunk;
    const m = /started on port (\d+)/.exec(seen);
    if (m) done(Number(m[1]));
  });
  server.on('exit', (code) => fail(new Error(`the server exited (${code}) before it started:\n${seen}`)));
});

const api = `http://127.0.0.1:${port}/api`;
const model = { _type: 'text', code: opts.model.map((f) => readFileSync(at(f), 'utf8')).join('\n') };
const call = async (method, path, body, text = false) => {
  const r = await fetch(api + path, { method, headers: { 'Content-Type': text ? 'text/plain' : 'application/json' }, ...(body === undefined ? {} : { body: text ? body : JSON.stringify(body) }) });
  const t = await r.text();
  if (!r.ok) throw new Error(`${method} ${path}: ${r.status} ${t.slice(0, 300)}`);
  return t;
};
// what each runs to (README.md's table)
const EXPECTED = { 'explicit-context': '6 rows', 'data-space-context': '5 rows', 'default-parameter-values': '6 rows', 'graph-fetch': '4 object(s)' };
const base = { groupId: 'demo', artifactId: 'trading', versionId: '0.0.0', stereotypes: [], gridConfig: null };
const records = {
  'explicit-context': { ...base, id: 'fixture-explicit-context', name: 'Big trades',
    executionContext: { _type: 'explicitExecutionContext', mapping: 'demo::trading::TradingMapping', runtime: 'demo::trading::Runtime' },
    content: "|demo::trading::Trade.all()->filter(\n  x|$x.quantity > 1000000\n)->project(\n  ~[\n     'Trade Id': x|$x.tradeId,\n     Quantity: x|$x.quantity\n   ]\n)",
    taggedValues: [], defaultParameterValues: [] },
  'data-space-context': { ...base, id: 'fixture-data-space-context', name: 'Sells',
    executionContext: { _type: 'dataSpaceExecutionContext', dataSpacePath: 'demo::trading::TradingDataSpace', executionKey: 'Production' },
    content: "|demo::trading::Trade.all()->filter(\n  x|$x.side ==\n    demo::trading::Side.SELL\n)->project(\n  ~[\n     'Trade Id': x|$x.tradeId,\n     Side: x|$x.side,\n     Quantity: x|$x.quantity\n   ]\n)",
    taggedValues: [{ tag: { profile: 'meta::pure::profiles::query', value: 'dataSpace' }, value: 'demo::trading::TradingDataSpace' }],
    defaultParameterValues: [] },
  'default-parameter-values': { ...base, id: 'fixture-default-parameter-values', name: 'Trades over a quantity',
    executionContext: { _type: 'explicitExecutionContext', mapping: 'demo::trading::TradingMapping', runtime: 'demo::trading::Runtime' },
    content: "{minQty: Integer[1]|demo::trading::Trade.all()->filter(\n  x|$x.quantity > $minQty\n)->project(\n  ~[\n     'Trade Id': x|$x.tradeId,\n     Quantity: x|$x.quantity\n   ]\n)}",
    taggedValues: [], defaultParameterValues: [{ name: 'minQty', content: '1000000' }] },
  'graph-fetch': { ...base, id: 'fixture-graph-fetch', name: 'Firms as objects',
    executionContext: { _type: 'explicitExecutionContext', mapping: 'demo::trading::TradingMapping', runtime: 'demo::trading::Runtime' },
    content: '|demo::trading::Firm.all()->graphFetch(\n  #{demo::trading::Firm{legalName}}#\n)->serialize(\n  #{demo::trading::Firm{legalName}}#\n)',
    taggedValues: [], defaultParameterValues: [] },
};

try {
for (const [file, record] of Object.entries(records)) {
  await call('POST', '/pure/v1/query', record);
  const answer = JSON.parse(await call('GET', `/pure/v1/query/${record.id}`));
  // runnable: the content, its parameters bound from defaultParameterValues, ->from(mapping, runtime)
  const lambda = JSON.parse(await call('POST', '/pure/v1/grammar/grammarToJson/lambda', answer.content, true));
  const last = lambda.body[lambda.body.length - 1];
  lambda.body[lambda.body.length - 1] = { _type: 'func', function: 'from', parameters: [last,
    { _type: 'packageableElementPtr', fullPath: 'demo::trading::TradingMapping' }, { _type: 'packageableElementPtr', fullPath: 'demo::trading::Runtime' }] };
  const parameterValues = [];
  for (const pv of answer.defaultParameterValues) {
    const v = JSON.parse(await call('POST', '/pure/v1/grammar/grammarToJson/lambda', `|${pv.content}`, true));
    parameterValues.push({ name: pv.name, value: v.body[0] });
  }
  const result = JSON.parse(await call('POST', '/pure/v1/execution/execute', { function: lambda, model, parameterValues, context: { _type: 'BaseExecutionContext' } }));
  const shape = result.result?.rows ? `${result.result.rows.length} rows` : `${Array.isArray(result.values) ? result.values.length : 1} object(s)`;
  if (shape !== EXPECTED[file]) throw new Error(`${file} runs to ${shape}, README.md says ${EXPECTED[file]}`);
  for (const stamp of ['createdAt', 'lastUpdatedAt', 'lastOpenAt']) {
    if (typeof answer[stamp] !== 'number') throw new Error(`${file}: ${stamp} is ${JSON.stringify(answer[stamp])}, not a time`);
    answer[stamp] = 0;
  }
  writeFileSync(join(at(opts.out), `${file}.json`), JSON.stringify(answer, null, 2) + '\n');
}
} finally {
  server.kill();
}
