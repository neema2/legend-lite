// A saved query as a cube's source, read by the rules Query writes it by
// (fixtures/saved-queries/README.md): every fixture record, through legend-lite's own compiler
// (the WASM module the tab loads) and DuckDB, over the trading project the records belong to --
// the same steps the page takes (demo/boot.ts `openedQuery`).

import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { after, before, describe, it } from 'node:test';
import { openDatabase, plannerOptions } from '#test-host';

import { DuckDbEngine } from '../../engine-client/src/duckdb.ts';
import {
  contextOf, enumerationsOf, enumsAsStrings, projectOf, savedQueryRead, sourceOf, unusable, type ModelElement,
} from '../src/saved-queries.ts';
import type { Query } from '../../query-store/src/wire.ts';
import { sourceColumns } from '../src/source-columns.ts';
import { LitePlanner } from '../src/lite-planner.ts';
import {
  agg, col, colSpecs, findAll, fn, from, isLambda, lambda, toJson, variable, type ValueSpecification,
} from '../../pure-protocol/src/index.ts';
import { plannerFor } from './catalog-builder.ts';
import { runfileFromEnv, runfileNamed } from '../../tools/js/runfiles.mts';

// the trading project as the site serves it (BUILD.bazel `:projects`, from query/demo/models)
const MODELS = runfileFromEnv('TRADING_PROJECT');
const record = (name: string): Query => JSON.parse(readFileSync(runfileNamed('SAVED_QUERIES', `${name}.json`), 'utf8')) as Query;
const MODEL = ['trading.pure', 'runtime-duckdb.pure', 'trading-data.pure']
  .map((f) => readFileSync(`${MODELS}/${f}`, 'utf8')).join('\n');

let engine: DuckDbEngine;
let elements: ModelElement[];
/** The project's own database, which the queries here run in. */
let projectDb: string;
const parser = plannerFor(MODEL, 'demo::trading::Runtime');

before(async () => {
  const db = await openDatabase();
  engine = new DuckDbEngine(db.connection);
  // the project's rows, its Data element's, in a database of its own as the page makes it (C5, project-data.ts)
  const project = await parser.projectData(MODEL, engine.databaseType);
  projectDb = project.name;
  for (const sql of [...project.open, ...project.use, ...project.fill]) await engine.run(sql, 0);
  elements = await parser.modelElements(MODEL) as ModelElement[];
});

after(async () => {
  await engine?.close();
});

/** The page's steps: parse, bind the saved values, read through the context, enumerations as names. */
async function opened(q: Query): Promise<{ source: ValueSpecification; planner: LitePlanner }> {
  const context = contextOf(q, elements);
  const values = new Map<string, ValueSpecification>();
  for (const v of q.defaultParameterValues ?? []) values.set(v.name, (await parser.parse(`|${v.content}`)).body[0]!);
  let source = sourceOf(await parser.parse(q.content), values);
  const planner = new LitePlanner({
    model: MODEL, runtime: context.runtime, mapping: context.mapping,
    enumerations: [...enumerationsOf(elements)], ...plannerOptions(), cache: false,
  });
  const named = new Set((await planner.relationType(lambda([], source))).filter((c) => c.enumeration).map((c) => c.name));
  if (named.size > 0) source = enumsAsStrings(source, named);
  return { source, planner };
}

async function rows(planner: LitePlanner, query: ValueSpecification): Promise<unknown[][]> {
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
    // query-store's one reader (Query's too): the data space it came from beside the mapping and runtime
    assert.deepEqual(contextOf(q, elements), { mapping: 'demo::trading::TradingMapping', runtime: 'demo::trading::Runtime',
      dataSpace: { path: 'demo::trading::TradingDataSpace', context: 'Production' } });
    const { source, planner } = await opened(q);
    assert.deepEqual((await sourceColumns(planner, source)).map((c) => `${c.name}:${c.type}`),
      ['Trade Id:Integer', 'Side:String', 'Quantity:Integer']);
    const got = await rows(planner, source);
    assert.equal(got.length, 5);
    assert.ok(got.every((r) => r[1] === 'SELL'), JSON.stringify(got));
  });

  it('a data space context: the read carries the data space, its title and its contexts (Run On)', async () => {
    const read = await savedQueryRead(record('data-space-context'), MODEL,
      { elements: async () => elements, parse: (text) => parser.parse(text) });
    assert.deepEqual(read.dataSpace, {
      path: 'demo::trading::TradingDataSpace', title: 'Trading', context: 'Production', defaultContext: 'Production',
      contexts: [{ name: 'Production', title: 'Production' }],
    });
    // an explicit context names none
    const explicit = await savedQueryRead(record('explicit-context'), MODEL,
      { elements: async () => elements, parse: (text) => parser.parse(text) });
    assert.equal(explicit.dataSpace, undefined);
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

// The trading rows' three copies (TRADING_COPIES): the project's Data (trading-data.pure, which the tab fills), the
// warehouse's fixture (trading-seed.sql, one statement per line) and the H2 connection's setup (runtime-h2.pure's
// testDataSetupSqls, Pure strings). Nothing derives one from another, so this holds them equal.
const COPIES = runfileFromEnv('TRADING_COPIES');
const copy = (name: string): string => readFileSync(`${COPIES}/${name}`, 'utf8');
const fixture = (): string[] => copy('trading-seed.sql').split('\n').map((l) => l.trim()).filter((l) => l !== '' && !l.startsWith('--'));
const h2Setup = (): string[] => [...copy('runtime-h2.pure').matchAll(/^\s*'((?:[^'\\]|\\.)*)',?\s*$/gm)]
  .map((m) => m[1]!.replace(/\\(.)/g, '$1'));

describe("the trading rows: the project's Data, the warehouse's fixture and the H2 connection's, row for row", () => {
  it('every table holds the same rows, NULLs included, with the same column types', async () => {
    const others = { trading_fixture: fixture(), trading_h2: h2Setup() };
    assert.ok(others.trading_h2.length >= 10, `the H2 connection's setup read as ${others.trading_h2.length} statements`);
    try {
      for (const [db, statements] of Object.entries(others)) {
        await engine.run(`ATTACH ':memory:' AS ${db}`, 0);
        await engine.run(`USE ${db}`, 0);
        for (const sql of statements) await engine.run(sql, 0);
      }
    } finally {
      await engine.run(`USE "${projectDb}"`, 0);
    }
    const values = async (sql: string): Promise<string[]> =>
      (await engine.run(sql, 0)).columns.map((c) => c.values.map(String)).flatMap((v, i, all) => (i === 0 ? v.map((x, r) => [x, ...all.slice(1).map((o) => o[r])].join(' ')) : []));
    const tables = (db: string): Promise<string[]> => values(
      `SELECT table_name FROM duckdb_tables() WHERE database_name = '${db}' AND schema_name = 'TRADING' ORDER BY table_name`);
    const types = (db: string, table: string): Promise<string[]> => values(`SELECT column_name, data_type FROM duckdb_columns()
      WHERE database_name = '${db}' AND schema_name = 'TRADING' AND table_name = '${table}' ORDER BY column_index`);
    const names = await tables(projectDb);
    assert.deepEqual(names, ['FIRM', 'FIRM_RATING', 'PERSON', 'PRODUCT', 'PRODUCT_LIMIT', 'TRADE']);
    for (const db of Object.keys(others)) {
      assert.deepEqual(await tables(db), names, `${db}'s tables`);
      for (const t of names) {
        assert.deepEqual(await types(db, t), await types(projectDb, t), `${db}.TRADING.${t}'s columns`);
        // every row both ways (EXCEPT ALL keeps duplicates; a set operation reads NULL as equal to NULL)
        const [ours, theirs] = [`"${projectDb}".TRADING.${t}`, `${db}.TRADING.${t}`];
        const differ = await values(`SELECT count(*) FROM ((SELECT * FROM ${ours} EXCEPT ALL SELECT * FROM ${theirs})
          UNION ALL (SELECT * FROM ${theirs} EXCEPT ALL SELECT * FROM ${ours}))`);
        assert.deepEqual(differ, ['0'], `${theirs} and the Data's ${t} differ`);
      }
    }
    // an empty cell of the Data is NULL, as the fixture's NULL is (PERSON 5 has no title, 6 no age, 8 no firm)
    assert.deepEqual(await values(`SELECT PERSON_ID FROM "${projectDb}".TRADING.PERSON
      WHERE TITLE IS NULL OR AGE IS NULL OR FIRM_ID IS NULL ORDER BY PERSON_ID`), ['5', '6', '8']);
  });
});
