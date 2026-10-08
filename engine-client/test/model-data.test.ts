// A model's test data in DuckDB (plan A2): the tables of a relational Data element, as legend-lite's model JSON gives
// them (party-model.json: grammarToJson/model of a Database and a Data element, written by the planner itself), made
// tables typed as the Database declares, and read back.

import assert from 'node:assert/strict';
import path from 'node:path';
import { before, describe, it } from 'node:test';
import { readFileSync } from 'node:fs';

import { DuckDbEngine, type ArrowishConnection } from '../src/duckdb.ts';
import { dataTables, loadDataTables, sqlType, TestData, type FileSink } from '../src/model-data.ts';
import { TabTables, withTestData } from '../src/tab-data.ts';
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

/** A table's rows, ordered by ID. */
const rowsOf = async (table: string, on: { run(sql: string, epoch: number): ReturnType<DuckDbEngine['run']> } = engine): Promise<unknown[][]> => {
  const t = await on.run(`SELECT ID, NAME, COUNTRY FROM "PARTY"."${table}" ORDER BY ID`, 0);
  return Array.from({ length: t.rowCount }, (_, i) => t.columns.map((c) => c.values[i]));
};

type Editable = { _type: string; schemas?: { tables: { name: string }[] }[]; data?: { tables?: { schema: string; table: string; values: string }[] } };

/** The party model as another version of it: its Data element's rows `csv`, and with `desk`, a second table it fills. */
const version = (csv: string, desk?: string): Parameters<typeof dataTables>[0] => {
  const elements = structuredClone(model.elements) as unknown as Editable[];
  const database = elements.find((e) => e._type === 'relational')!;
  const data = elements.find((e) => e._type === 'dataElement')!;
  data.data!.tables![0]!.values = csv;
  if (desk !== undefined) {
    database.schemas![0]!.tables.push({ ...database.schemas![0]!.tables[0]!, name: 'DESK' });
    data.data!.tables!.push({ schema: 'PARTY', table: 'DESK', values: desk });
  }
  return elements as unknown as Parameters<typeof dataTables>[0];
};
const V1 = 'ID,NAME,COUNTRY\n1,Meridian Capital,US\n2,Halberd Securities,GB\n';
const V2 = 'ID,NAME,COUNTRY\n3,Corvid Partners,FR\n';
const DESK = 'ID,NAME,COUNTRY\n5,Rates Desk,US\n';

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

  it('refuses a table no Database declares, and a column type with no DDL, in the server\'s words', () => {
    assert.throws(() => dataTables(model.elements.filter((e) => e._type !== 'relational')), /which no Database of the model declares/);
    const odd = structuredClone(model.elements) as unknown as { _type: string; schemas?: { tables: { columns: { type: { _type: string } }[] }[] }[] }[];
    odd[0]!.schemas![0]!.tables[0]!.columns[0]!.type = { _type: 'Other' };
    assert.throws(() => dataTables(odd as never), /no DDL spelling for declared column type OTHER/);
  });

  // THE COPY (docs/STUDIO_FULL_PLAN_2026_10_04.md, "The tab's table types"): the server's DuckDB spelling of every
  // column kind its model knows (core: FromProtocol.dataType, StoreCompiler.declaredType, DuckDb.ddlType,
  // DdlSpelling.h2Type). A change there is a change here, until the planner hands the tab the server's own statements.
  it("spells every column type as the server's DuckDB tables do", () => {
    const spelled = (_type: string, more: object = {}): string => sqlType({ _type, ...more });
    assert.deepEqual(
      ['BigInt', 'SmallInt', 'TinyInt', 'Integer', 'Float', 'Double', 'Real', 'Bit', 'Timestamp', 'Date', 'SemiStructured', 'Json'].map((k) => spelled(k)),
      ['BIGINT', 'SMALLINT', 'TINYINT', 'INTEGER', 'DOUBLE', 'DOUBLE', 'REAL', 'BOOLEAN', 'TIMESTAMP', 'DATE', 'JSON', 'JSON']);
    assert.deepEqual(['Varchar', 'Char', 'Binary', 'Varbinary'].map((k) => spelled(k, { size: 20 })), ['VARCHAR(20)', 'CHAR(20)', 'BINARY(20)', 'VARBINARY(20)']);
    assert.deepEqual(['Decimal', 'Numeric'].map((k) => spelled(k, { precision: 10, scale: 2 })), ['DECIMAL(10, 2)', 'NUMERIC(10, 2)']);
    assert.throws(() => spelled('Distinct'), /no DDL spelling for declared column type DISTINCT/);
    assert.throws(() => spelled('Boolean'), /no model data type for protocol kind 'Boolean'/);
    assert.throws(() => spelled('toString'), /no model data type for protocol kind 'toString'/);
  });

  it('a Float column keeps its value exactly and a Bit column reads true and false, as on the server', async () => {
    const typed = structuredClone(model.elements) as unknown as { _type: string; schemas?: { tables: { columns: { name: string; type: { _type: string } }[] }[] }[]; data?: { tables?: { values: string }[] } }[];
    const columns = typed.find((e) => e._type === 'relational')!.schemas![0]!.tables[0]!.columns;
    columns[1] = { name: 'SCORE', type: { _type: 'Float' } };
    columns[2] = { name: 'ACTIVE', type: { _type: 'Bit' } };
    typed.find((e) => e._type === 'dataElement')!.data!.tables![0]!.values = 'ID,SCORE,ACTIVE\n1,0.1,true\n2,2.675,false\n';
    await loadDataTables({ registerFileText: async (name, text) => db.registerFileText(name, text), run: (sql) => engine.run(sql, 0) },
      dataTables(typed as never));
    const t = await engine.run('SELECT ID, SCORE, ACTIVE, SCORE = 0.1 AS EXACT FROM "PARTY"."PARTY" ORDER BY ID', 0);
    assert.deepEqual(t.columns.map((c) => c.values[0]), [1, 0.1, true, true]);
    assert.deepEqual(t.columns.map((c) => c.values[1]), [2, 2.675, false, false]);
  });
});

describe("one model's test data at a time, versions of one project filling the same tables (TestData)", () => {
  const textSink = () => ({ registerFileText: async (name: string, text: string) => db.registerFileText(name, text), run: (sql: string) => engine.run(sql, 0) });

  it("opening an earlier version again reads its own rows, not the later one's", async () => {
    const data = new TestData(textSink());
    await data.use('p:1.0.0', dataTables(version(V1)));
    assert.deepEqual((await rowsOf('PARTY')).map((r) => r[0]), [1, 2]);
    await data.use('p:HEAD', dataTables(version(V2)));
    assert.deepEqual((await rowsOf('PARTY')).map((r) => r[0]), [3]);
    await data.use('p:1.0.0', dataTables(version(V1)));
    assert.deepEqual((await rowsOf('PARTY')).map((r) => r[0]), [1, 2]);
  });

  it('drops a table an earlier version made that this one does not fill', async () => {
    const data = new TestData(textSink());
    await data.use('a', dataTables(version(V1, DESK)));
    assert.deepEqual(await rowsOf('DESK'), [[5, 'Rates Desk', 'US']]);
    await data.use('b', dataTables(version(V2)));
    await assert.rejects(rowsOf('DESK'), /DESK/);
  });

  it("runs each query in the same turn as its version's rows, whatever another query asked for meanwhile", async () => {
    const data = new TestData(textSink());
    const one = withTestData(engine, data, 'p:1.0.0', dataTables(version(V1)));
    const head = withTestData(engine, data, 'p:HEAD', dataTables(version(V2)));
    const [a, b, c] = await Promise.all([rowsOf('PARTY', one), rowsOf('PARTY', head), rowsOf('PARTY', one)]);
    assert.deepEqual([a, b, c].map((rows) => rows.map((r) => r[0])), [[1, 2], [3], [1, 2]]);
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

  it("drops a table the model's test data no longer fills (its Data element changed), and keeps a file's", async () => {
    const tabs = new TabTables(sink(), engine);
    await tabs.load(version(V1, DESK));
    await tabs.put(version(V1, DESK), 'PARTY', 'PARTY', { name: 'parties.csv', bytes: enc('ID,NAME,COUNTRY\n7,Osprey Fund,NZ\n') });
    await tabs.load(version(V1));
    await assert.rejects(rowsOf('DESK'), /DESK/);
    assert.deepEqual(await rows(), [[7, 'Osprey Fund', 'NZ']]);
    assert.deepEqual((await tabs.tables(version(V1))).map((t) => [t.table, t.source.kind]), [['PARTY', 'file']]);
    await tabs.reset(version(V1), 'PARTY', 'PARTY');
  });

  it("fills a table from a file whose name DuckDB would read as a pattern (sales[2024].csv)", async () => {
    const tabs = new TabTables(sink(), engine);
    await tabs.put(model.elements, 'PARTY', 'PARTY', { name: 'parties[2024]*?.csv', bytes: enc('ID,NAME,COUNTRY\n8,Kestrel Bank,DE\n') });
    assert.deepEqual(await rows(), [[8, 'Kestrel Bank', 'DE']]);
    assert.deepEqual((await tabs.tables(model.elements))[0]!.source, { kind: 'file', name: 'parties[2024]*?.csv' });
    await tabs.reset(model.elements, 'PARTY', 'PARTY');
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
