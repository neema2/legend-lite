// A model's test data in DuckDB (plan A2): the tables of a relational Data element, as legend-lite's model JSON gives
// them (party-model.json: grammarToJson/model of a Database and a Data element, written by the planner itself), made
// tables typed as the Database declares, and read back.

import assert from 'node:assert/strict';
import path from 'node:path';
import { before, describe, it } from 'node:test';
import { readFileSync } from 'node:fs';

import { DuckDbEngine, type ArrowishConnection } from '../src/duckdb.ts';
import { dataTables, loadDataTables, type FileSink } from '../src/model-data.ts';
import { TabTables } from '../src/tab-data.ts';
import { engineClientRequire } from '../src/node-require.ts';
import { runfileFromEnv } from '../../tools/js/runfiles.mts';

const model = JSON.parse(readFileSync(runfileFromEnv('PARTY_MODEL'), 'utf8')) as { elements: Parameters<typeof dataTables>[0] };

let engine: DuckDbEngine;
let db: {
  registerFileText(name: string, text: string): void;
  registerFileBuffer(name: string, bytes: Uint8Array): void;
  copyFileToBuffer(name: string): Uint8Array;
  connect(): unknown;
  instantiate(): Promise<unknown>;
};
const sink = (): FileSink => ({
  registerFileText: async (name, text) => db.registerFileText(name, text),
  registerFileBuffer: async (name, bytes) => db.registerFileBuffer(name, bytes),
  run: (sql) => engine.run(sql, 0),
});

before(async () => {
  const duckdb = engineClientRequire('@duckdb/duckdb-wasm/blocking');
  const dist = path.dirname(engineClientRequire.resolve('@duckdb/duckdb-wasm/blocking'));
  db = await duckdb.createDuckDB(
    {
      mvp: { mainModule: path.join(dist, 'duckdb-mvp.wasm'), mainWorker: path.join(dist, 'duckdb-node-mvp.worker.cjs') },
      eh: { mainModule: path.join(dist, 'duckdb-eh.wasm'), mainWorker: path.join(dist, 'duckdb-node-eh.worker.cjs') },
    },
    new duckdb.VoidLogger(),
    duckdb.NODE_RUNTIME,
  );
  await db.instantiate();
  engine = new DuckDbEngine(db.connect() as ArrowishConnection);
});

describe("a model's test data, in DuckDB", () => {
  it('reads each table of a relational Data element, typed as its Database declares', () => {
    assert.deepEqual(dataTables(model.elements), [{
      schema: 'PARTY',
      table: 'PARTY',
      columns: [{ name: 'ID', type: 'INTEGER' }, { name: 'NAME', type: 'VARCHAR(200)' }, { name: 'COUNTRY', type: 'VARCHAR(2)' }],
      csv: 'ID,NAME,COUNTRY\n1,Meridian Capital,US\n2,Halberd Securities,GB\n',
      element: 'demo::party::PartyData',
    }]);
  });

  it('loads them where the model says, and they read back typed', async () => {
    await loadDataTables({
      registerFileText: async (name, text) => db.registerFileText(name, text),
      run: (sql) => engine.run(sql, 0),
    }, dataTables(model.elements));
    const t = await engine.run('SELECT ID, NAME, COUNTRY FROM "PARTY"."PARTY" ORDER BY ID', 0);
    assert.deepEqual(t.columns.map((c) => c.name), ['ID', 'NAME', 'COUNTRY']);
    assert.equal(t.rowCount, 2);
    assert.deepEqual(t.columns.map((c) => c.values[1]), [2, 'Halberd Securities', 'GB']);
  });

  it('refuses a table no Database declares; DuckDB refuses a column type it does not know', async () => {
    assert.throws(() => dataTables(model.elements.filter((e) => e._type !== 'relational')), /which no Database of the model declares/);
    const odd = structuredClone(model.elements) as { _type: string; schemas?: { tables: { columns: { type: { _type: string } }[] }[] }[] }[];
    odd[0]!.schemas![0]!.tables[0]!.columns[0]!.type = { _type: 'Semistructured' };
    await assert.rejects(loadDataTables({
      registerFileText: async (name, text) => db.registerFileText(name, text),
      run: (sql) => engine.run(sql, 0),
    }, dataTables(odd as never)), /SEMISTRUCTURED/i);
  });
});

describe("a table's rows from a person's file (plan A2)", () => {
  const rows = async (): Promise<unknown[][]> => {
    const t = await engine.run('SELECT ID, NAME, COUNTRY FROM "PARTY"."PARTY" ORDER BY ID', 0);
    return Array.from({ length: t.rowCount }, (_, i) => t.columns.map((c) => c.values[i]));
  };
  const enc = (s: string): Uint8Array => new TextEncoder().encode(s);

  it("says where each declared table's rows are from, and how many", async () => {
    const tabs = new TabTables(sink(), engine);
    await tabs.load(model.elements);
    assert.deepEqual((await tabs.tables(model.elements)).map((t) => [t.database, `${t.schema}.${t.table}`, t.source, t.rows]),
      [['demo::party::PartyDB', 'PARTY.PARTY', { kind: 'test', element: 'demo::party::PartyData' }, 2]]);
  });

  it('fills a table from a CSV, kept over the test data until reset', async () => {
    const tabs = new TabTables(sink(), engine);
    await tabs.load(model.elements);
    await tabs.put(model.elements, 'PARTY', 'PARTY', { name: 'parties.csv', bytes: enc('ID,NAME,COUNTRY\n7,Osprey Fund,NZ\n') });
    assert.deepEqual(await rows(), [[7, 'Osprey Fund', 'NZ']]);
    await tabs.load(model.elements);      // a run loads the test data again: the file's rows stay
    assert.deepEqual(await rows(), [[7, 'Osprey Fund', 'NZ']]);
    assert.deepEqual((await tabs.tables(model.elements))[0]!.source, { kind: 'file', name: 'parties.csv' });
    await tabs.reset(model.elements, 'PARTY', 'PARTY');
    assert.equal((await rows()).length, 2);
    assert.deepEqual((await tabs.tables(model.elements))[0]!.source, { kind: 'test', element: 'demo::party::PartyData' });
  });

  it('fills a table from Parquet, each declared column by name, cast to its type', async () => {
    await engine.run("COPY (SELECT 'AU' AS COUNTRY, 9 AS ID, 'Wattle Capital' AS NAME, true AS EXTRA) TO 'made.parquet' (FORMAT PARQUET)", 0);
    const tabs = new TabTables(sink(), engine);
    await tabs.put(model.elements, 'PARTY', 'PARTY', { name: 'parties.parquet', bytes: db.copyFileToBuffer('made.parquet') });
    assert.deepEqual(await rows(), [[9, 'Wattle Capital', 'AU']]);
  });

  it('refuses a file of another kind, a table no Database declares, and a file without a declared column', async () => {
    const tabs = new TabTables(sink(), engine);
    await assert.rejects(tabs.put(model.elements, 'PARTY', 'PARTY', { name: 'parties.json', bytes: enc('[]') }), /a \.csv or a \.parquet file/);
    await assert.rejects(tabs.put(model.elements, 'PARTY', 'NOPE', { name: 'x.csv', bytes: enc('A\n1\n') }), /no Database of the model declares PARTY\.NOPE/);
    await engine.run("COPY (SELECT 1 AS ID) TO 'thin.parquet' (FORMAT PARQUET)", 0);
    await assert.rejects(tabs.put(model.elements, 'PARTY', 'PARTY', { name: 'thin.parquet', bytes: db.copyFileToBuffer('thin.parquet') }), /NAME/);
  });
});
