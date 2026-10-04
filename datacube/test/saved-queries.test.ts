// A saved query as a cube's source, read by the rules Query writes it by
// (fixtures/saved-queries/README.md): every fixture record, through legend-lite's own compiler
// (the WASM module the tab loads) and DuckDB, over the trading project the records belong to --
// the same steps the page takes (demo/boot.ts `openedQuery`).

import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import path from 'node:path';
import { after, before, describe, it } from 'node:test';

import { DuckDbEngine, type ArrowishConnection } from '../src/duckdb.ts';
import {
  contextOf, enumerationsOf, enumsAsStrings, projectOf, sourceOf, unusable, type ModelElement,
} from '../src/saved-queries.ts';
import type { Query } from '../../query-store/src/wire.ts';
import { sourceColumns } from '../src/source-columns.ts';
import { WasmPlanner } from '../src/wasm-planner.ts';
import {
  agg, col, colSpecs, findAll, fn, from, isLambda, lambda, toJson, variable, type ValueSpecification,
} from '../../pure-protocol/src/index.ts';
import { plannerFor } from './catalog-builder.ts';
import { runfileDirUrl, runfileFromEnv, runfileNamed } from '../../tools/js/runfiles.mts';

// the trading project as the site serves it (BUILD.bazel `:projects`, from query/demo/models)
const MODELS = runfileFromEnv('TRADING_PROJECT');
const record = (name: string): Query => JSON.parse(readFileSync(runfileNamed('SAVED_QUERIES', `${name}.json`), 'utf8')) as Query;
const MODEL = readFileSync(`${MODELS}/trading.pure`, 'utf8') + '\n' + readFileSync(`${MODELS}/runtime-duckdb.pure`, 'utf8');

let engine: DuckDbEngine;
let elements: ModelElement[];
const parser = plannerFor(MODEL, 'demo::trading::Runtime');

before(async () => {
  const require = createRequire(import.meta.url);
  const duckdb = require('@duckdb/duckdb-wasm/blocking');
  const dist = path.dirname(require.resolve('@duckdb/duckdb-wasm/blocking'));
  const db = await duckdb.createDuckDB(
    {
      mvp: { mainModule: path.join(dist, 'duckdb-mvp.wasm'), mainWorker: path.join(dist, 'duckdb-node-mvp.worker.cjs') },
      eh: { mainModule: path.join(dist, 'duckdb-eh.wasm'), mainWorker: path.join(dist, 'duckdb-node-eh.worker.cjs') },
    },
    new duckdb.VoidLogger(),
    duckdb.NODE_RUNTIME,
  );
  await db.instantiate();
  engine = new DuckDbEngine(db.connect() as ArrowishConnection);
  for (const line of readFileSync(`${MODELS}/trading-seed.sql`, 'utf8').split('\n')) {
    const sql = line.trim();
    if (sql && !sql.startsWith('--')) await engine.run(sql, 0);
  }
  elements = await parser.modelElements(MODEL) as ModelElement[];
});

after(async () => {
  await engine?.close();
});

/** The page's steps: parse, bind the saved values, read through the context, enumerations as names. */
async function opened(q: Query): Promise<{ source: ValueSpecification; planner: WasmPlanner }> {
  const context = contextOf(q, elements);
  const values = new Map<string, ValueSpecification>();
  for (const v of q.defaultParameterValues ?? []) values.set(v.name, (await parser.parse(`|${v.content}`)).body[0]!);
  let source = sourceOf(await parser.parse(q.content), values);
  const planner = new WasmPlanner({
    model: MODEL, runtime: context.runtime, mapping: context.mapping,
    enumerations: [...enumerationsOf(elements)], assetBaseUrl: runfileDirUrl('WASM_PLANNER'), cache: false,
  });
  const named = new Set((await planner.relationType(lambda([], source))).filter((c) => c.enumeration).map((c) => c.name));
  if (named.size > 0) source = enumsAsStrings(source, named);
  return { source, planner };
}

async function rows(planner: WasmPlanner, query: ValueSpecification): Promise<unknown[][]> {
  const plan = await planner.plan(lambda([], query));
  const table = await engine.run(plan.sql, 0);
  return Array.from({ length: table.rowCount }, (_, i) => table.columns.map((c) => c.values[i]));
}

describe('a saved query as a source: each fixture, as the README says it runs', () => {
  it('an explicit context: TradingMapping and Runtime, 6 rows', async () => {
    const q = record('explicit-context');
    assert.deepEqual(contextOf(q, elements), { mapping: 'demo::trading::TradingMapping', runtime: 'demo::trading::Runtime' });
    const { source, planner } = await opened(q);
    assert.deepEqual((await sourceColumns(planner, source)).map((c) => `${c.name}:${c.type}`), ['Trade Id:Integer', 'Quantity:Integer']);
    assert.equal((await rows(planner, source)).length, 6);
  });

  it('a data space context: its Production context, 5 rows, the enumeration read as its name', async () => {
    const q = record('data-space-context');
    assert.deepEqual(contextOf(q, elements), { mapping: 'demo::trading::TradingMapping', runtime: 'demo::trading::Runtime' });
    const { source, planner } = await opened(q);
    assert.deepEqual((await sourceColumns(planner, source)).map((c) => `${c.name}:${c.type}`),
      ['Trade Id:Integer', 'Side:String', 'Quantity:Integer']);
    const got = await rows(planner, source);
    assert.equal(got.length, 5);
    assert.ok(got.every((r) => r[1] === 'SELL'), JSON.stringify(got));
  });

  it('a parameter: its saved value bound first, 6 rows', async () => {
    const q = record('default-parameter-values');
    const { source, planner } = await opened(q);
    assert.equal((await rows(planner, source)).length, 6);
  });

  it('a graph fetch is refused, and says why', async () => {
    const q = record('graph-fetch');
    assert.match(unusable(await parser.parse(q.content)) ?? '', /objects, not rows/);
    await assert.rejects(opened(q), /objects, not rows/);
  });

  it('a cube query over it: grouped by the enumeration name, a sum per name', async () => {
    const { source, planner } = await opened(record('data-space-context'));
    const all = await rows(planner, source);
    const grouped = from(source).groupBy(['Side'], [agg('q', lambda(['x'], col('x', 'Quantity')), lambda(['y'], fn('sum', variable('y'))))]).node;
    const got = await rows(planner, grouped);
    assert.deepEqual(got.map((r) => [r[0], Number(r[1])]), [['SELL', all.reduce((n, r) => n + Number(r[2]), 0)]]);
  });

  it('a GAV names its project', () => {
    assert.equal(projectOf(record('explicit-context')), 'demo:trading:0.0.0');
  });
});

describe('enumerations as names: only the spec that makes the column', () => {
  it('a projection column gets ->toString(), another column is left alone', () => {
    const project = fn('project', variable('src'), colSpecs([
      { name: 'Side', function1: lambda([variable('x')], variable('x')) },
      { name: 'Qty', function1: lambda([variable('x')], variable('x')) },
    ]));
    // the specs' map bodies, printed as the protocol writes them
    const bodies = findAll(enumsAsStrings(project, new Set(['Side'])), isLambda).map((l) => toJson(l.body[0]));
    assert.deepEqual(bodies, [toJson(fn('toString', variable('x'))), toJson(variable('x'))]);
  });

  it('a column no spec makes is refused by name', () => {
    assert.throws(() => enumsAsStrings(variable('src'), new Set(['Side'])), /'Side' is an enumeration/);
  });
});
