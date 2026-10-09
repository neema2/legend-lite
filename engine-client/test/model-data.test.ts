// A model's test data in DuckDB (plan A2): the tables of a relational Data element, made and filled by the statements
// the server seeds a database with, which legend-lite's planner (WebAssembly, WASM_PLANNER, called here as the tab's
// worker calls it) gives for the model's text -- and read back. The party model is the demo's (studio/demo/projects/
// party), written here so each test can make the version it needs.

import assert from 'node:assert/strict';
import path from 'node:path';
import { before, describe, it } from 'node:test';
import { fileURLToPath } from 'node:url';

import { DuckDbEngine, type ArrowishConnection } from '../src/duckdb.ts';
import { WasmGrammar } from '../src/legend/wasm-grammar.ts';
import { answer, type PlannerModule, type PlannerRequest } from '../src/legend/planner-answer.ts';
import { dataTables, loadDataTables, seeded, TestData, type FileSink } from '../src/model-data.ts';
import { TabTables, withTestData, type Model } from '../src/tab-data.ts';
import { engineClientRequire } from '../src/node-require.ts';
import { runfileDirUrl } from '../../tools/js/runfiles.mts';

type Elements = Parameters<typeof dataTables>[0];

let engine: DuckDbEngine;
let db: {
  registerFileBuffer(name: string, bytes: Uint8Array): void;
  copyFileToBuffer(name: string): Uint8Array;
  connect(): unknown;
  instantiate(): Promise<unknown>;
};
let planner: WasmGrammar;
const sink = (): FileSink => ({
  registerFileBuffer: async (name, bytes) => db.registerFileBuffer(name, bytes),
  run: (sql) => engine.run(sql, 0),
});

/** A table's rows, ordered by ID. */
const rowsOf = async (table: string, on: { run(sql: string, epoch: number): ReturnType<DuckDbEngine['run']> } = engine): Promise<unknown[][]> => {
  const t = await on.run(`SELECT ID, NAME, COUNTRY FROM "PARTY"."${table}" ORDER BY ID`, 0);
  return Array.from({ length: t.rowCount }, (_, i) => t.columns.map((c) => c.values[i]));
};

const PARTY_COLUMNS = 'ID INTEGER PRIMARY KEY, NAME VARCHAR(200), COUNTRY VARCHAR(2)';
/** A CSV as a Pure string's body. */
const pure = (csv: string): string => csv.replace(/\\/g, '\\\\').replace(/'/g, "\\'").replace(/\n/g, '\\n');

/** The party model's text: its Database (a table PARTY, a table DESK too with `desk`) and its Data element's rows. */
function partyText(csv: string, desk?: string, columns = PARTY_COLUMNS): string {
  return `###Relational
Database demo::party::PartyDB
(
  Schema PARTY
  (
    Table PARTY (${columns})${desk === undefined ? '' : `\n    Table DESK (${PARTY_COLUMNS})`}
  )
)

###Data
Data demo::party::PartyData
{
  Relational
  #{
    PARTY.PARTY:
      '${pure(csv)}';${desk === undefined ? '' : `\n    PARTY.DESK:\n      '${pure(desk)}';`}
  }#
}
`;
}

/** A model: its text and the elements the planner reads it to. */
async function modelOf(text: string): Promise<Model> {
  return { text, elements: (await planner.modelJson(text)).elements as unknown as Elements };
}
/** The party model as another version of it: its Data element's rows `csv`, and with `desk`, a second table it fills. */
const version = (csv: string, desk?: string): Promise<Model> => modelOf(partyText(csv, desk));
/** A version's test data with the server's statements. */
const tablesOf = async (m: Model) => seeded(planner, m.text, dataTables(m.elements));

const V1 = 'ID,NAME,COUNTRY\n1,Meridian Capital,US\n2,Halberd Securities,GB\n';
const V2 = 'ID,NAME,COUNTRY\n3,Corvid Partners,FR\n';
const DESK = 'ID,NAME,COUNTRY\n5,Rates Desk,US\n';

/** legend-lite's planner, in this process, answering as the tab's worker does. */
async function loadPlanner(): Promise<WasmGrammar> {
  const dir = new URL(runfileDirUrl('WASM_PLANNER'));
  const runtime = await import(new URL('wasm-gc-module-runtime.js', dir).href) as {
    load(src: string, options: unknown): Promise<PlannerModule>;
  };
  const m = await runtime.load(fileURLToPath(new URL('classes.wasm', dir)), {
    stackDeobfuscator: { enabled: false },
    installImports(i: Record<string, unknown>) {
      i.teavmConsole = { putcharStdout() {}, putcharStderr() {} };
    },
  });
  return new WasmGrammar({
    async ask(r: PlannerRequest): Promise<string> {
      return answer(m, r);
    },
  });
}

before(async () => {
  planner = await loadPlanner();
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
  it('reads each table of a relational Data element, with the Database that declares it', async () => {
    assert.deepEqual(dataTables((await version(V1)).elements), [{
      database: 'demo::party::PartyDB',
      schema: 'PARTY',
      table: 'PARTY',
      csv: V1,
      element: 'demo::party::PartyData',
    }]);
  });

  it("makes and fills them with the server's statements, and they read back", async () => {
    const v1 = await version(V1);
    const tables = await tablesOf(v1);
    assert.ok(tables[0]!.seed.sql.some((s) => /^CREATE TABLE/i.test(s)), tables[0]!.seed.sql.join('\n'));
    await loadDataTables(sink(), tables);
    assert.deepEqual(await rowsOf('PARTY'), [[1, 'Meridian Capital', 'US'], [2, 'Halberd Securities', 'GB']]);
  });

  it('refuses a table no Database declares, and one two Databases declare: which one would be a guess', async () => {
    const v1 = await version(V1);
    assert.throws(() => dataTables(v1.elements.filter((e) => e._type !== 'relational')), /which no Database of the model declares/);
    const two = await modelOf(`${partyText(V1)}
###Relational
Database demo::party::OtherDB
(
  Schema PARTY
  (
    Table PARTY (${PARTY_COLUMNS})
  )
)
`);
    assert.throws(() => dataTables(two.elements), /more than one Database declares \(demo::party::PartyDB, demo::party::OtherDB\)/);
  });

  it("makes each column the server's type: a Float column DOUBLE, keeping its value exactly; a Bit column BOOLEAN", async () => {
    const typed = await modelOf(partyText('ID,SCORE,ACTIVE\n1,0.1,true\n2,2.675,false\n', undefined, 'ID INTEGER PRIMARY KEY, SCORE FLOAT, ACTIVE BIT'));
    await loadDataTables(sink(), await tablesOf(typed));
    const types = await engine.run("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'PARTY' AND table_name = 'PARTY' ORDER BY ordinal_position", 0);
    assert.deepEqual(Array.from({ length: types.rowCount }, (_, i) => types.columns.map((c) => c.values[i])),
      [['ID', 'INTEGER'], ['SCORE', 'DOUBLE'], ['ACTIVE', 'BOOLEAN']]);
    const t = await engine.run('SELECT ID, SCORE, ACTIVE, SCORE = 0.1 AS EXACT FROM "PARTY"."PARTY" ORDER BY ID', 0);
    assert.deepEqual(t.columns.map((c) => c.values[0]), [1, 0.1, true, true]);
    assert.deepEqual(t.columns.map((c) => c.values[1]), [2, 2.675, false, false]);
  });
});

describe("one model's test data at a time, versions of one project filling the same tables (TestData)", () => {
  it("opening an earlier version again reads its own rows, not the later one's", async () => {
    const data = new TestData(sink());
    const one = await tablesOf(await version(V1));
    const head = await tablesOf(await version(V2));
    await data.use('p:1.0.0', one);
    assert.deepEqual((await rowsOf('PARTY')).map((r) => r[0]), [1, 2]);
    await data.use('p:HEAD', head);
    assert.deepEqual((await rowsOf('PARTY')).map((r) => r[0]), [3]);
    await data.use('p:1.0.0', one);
    assert.deepEqual((await rowsOf('PARTY')).map((r) => r[0]), [1, 2]);
  });

  it('drops a table an earlier version made that this one does not fill', async () => {
    const data = new TestData(sink());
    await data.use('a', await tablesOf(await version(V1, DESK)));
    assert.deepEqual(await rowsOf('DESK'), [[5, 'Rates Desk', 'US']]);
    await data.use('b', await tablesOf(await version(V2)));
    await assert.rejects(rowsOf('DESK'), /DESK/);
  });

  it("runs each query in the same turn as its version's rows, whatever another query asked for meanwhile", async () => {
    const data = new TestData(sink());
    const one = withTestData(engine, data, 'p:1.0.0', await tablesOf(await version(V1)));
    const head = withTestData(engine, data, 'p:HEAD', await tablesOf(await version(V2)));
    const [a, b, c] = await Promise.all([rowsOf('PARTY', one), rowsOf('PARTY', head), rowsOf('PARTY', one)]);
    assert.deepEqual([a, b, c].map((rows) => rows.map((r) => r[0])), [[1, 2], [3], [1, 2]]);
  });
});

describe("a table's rows from a person's file (plan A2)", () => {
  const rows = (): Promise<unknown[][]> => rowsOf('PARTY');
  const enc = (s: string): Uint8Array => new TextEncoder().encode(s);

  it("says where each declared table's rows are from, and how many", async () => {
    const v1 = await version(V1);
    const tabs = new TabTables(sink(), engine, planner);
    await tabs.load(v1);
    assert.deepEqual((await tabs.tables(v1)).map((t) => [t.database, `${t.schema}.${t.table}`, t.source, t.rows]),
      [['demo::party::PartyDB', 'PARTY.PARTY', { kind: 'test', element: 'demo::party::PartyData' }, 2]]);
  });

  it('fills a table from a CSV, kept over the test data until reset', async () => {
    const v1 = await version(V1);
    const tabs = new TabTables(sink(), engine, planner);
    await tabs.load(v1);
    await tabs.put(v1, 'PARTY', 'PARTY', { name: 'parties.csv', bytes: enc('ID,NAME,COUNTRY\n7,Osprey Fund,NZ\n') });
    assert.deepEqual(await rows(), [[7, 'Osprey Fund', 'NZ']]);
    await tabs.load(v1);      // a run loads the test data again: the file's rows stay
    assert.deepEqual(await rows(), [[7, 'Osprey Fund', 'NZ']]);
    assert.deepEqual((await tabs.tables(v1))[0]!.source, { kind: 'file', name: 'parties.csv' });
    await tabs.reset(v1, 'PARTY', 'PARTY');
    assert.equal((await rows()).length, 2);
    assert.deepEqual((await tabs.tables(v1))[0]!.source, { kind: 'test', element: 'demo::party::PartyData' });
  });

  it("drops a table the model's test data no longer fills (its Data element changed), and keeps a file's", async () => {
    const withDesk = await version(V1, DESK);
    const v1 = await version(V1);
    const tabs = new TabTables(sink(), engine, planner);
    await tabs.load(withDesk);
    await tabs.put(withDesk, 'PARTY', 'PARTY', { name: 'parties.csv', bytes: enc('ID,NAME,COUNTRY\n7,Osprey Fund,NZ\n') });
    await tabs.load(v1);
    await assert.rejects(rowsOf('DESK'), /DESK/);
    assert.deepEqual(await rows(), [[7, 'Osprey Fund', 'NZ']]);
    assert.deepEqual((await tabs.tables(v1)).map((t) => [t.table, t.source.kind]), [['PARTY', 'file']]);
    await tabs.reset(v1, 'PARTY', 'PARTY');
  });

  it("fills a table from a file whose name DuckDB would read as a pattern (sales[2024].csv)", async () => {
    const v1 = await version(V1);
    const tabs = new TabTables(sink(), engine, planner);
    await tabs.put(v1, 'PARTY', 'PARTY', { name: 'parties[2024]*?.csv', bytes: enc('ID,NAME,COUNTRY\n8,Kestrel Bank,DE\n') });
    assert.deepEqual(await rows(), [[8, 'Kestrel Bank', 'DE']]);
    assert.deepEqual((await tabs.tables(v1))[0]!.source, { kind: 'file', name: 'parties[2024]*?.csv' });
    await tabs.reset(v1, 'PARTY', 'PARTY');
  });

  it('fills a table from Parquet, each declared column by name, converted to its type', async () => {
    await engine.run("COPY (SELECT 'AU' AS COUNTRY, 9 AS ID, 'Wattle Capital' AS NAME, true AS EXTRA) TO 'made.parquet' (FORMAT PARQUET)", 0);
    const tabs = new TabTables(sink(), engine, planner);
    await tabs.put(await version(V1), 'PARTY', 'PARTY', { name: 'parties.parquet', bytes: db.copyFileToBuffer('made.parquet') });
    assert.deepEqual(await rows(), [[9, 'Wattle Capital', 'AU']]);
  });

  it('refuses a file of another kind, a table no Database declares, a file without a declared column, and a value its column cannot take', async () => {
    const v1 = await version(V1);
    const tabs = new TabTables(sink(), engine, planner);
    await assert.rejects(tabs.put(v1, 'PARTY', 'PARTY', { name: 'parties.json', bytes: enc('[]') }), /a \.csv or a \.parquet file/);
    await assert.rejects(tabs.put(v1, 'PARTY', 'NOPE', { name: 'x.csv', bytes: enc('A\n1\n') }), /a file fills PARTY\.NOPE, which no Database of the model declares/);
    await engine.run("COPY (SELECT 1 AS ID) TO 'thin.parquet' (FORMAT PARQUET)", 0);
    await assert.rejects(tabs.put(v1, 'PARTY', 'PARTY', { name: 'thin.parquet', bytes: db.copyFileToBuffer('thin.parquet') }), /NAME/);
    await assert.rejects(tabs.put(v1, 'PARTY', 'PARTY', { name: 'bad.csv', bytes: enc('ID,NAME,COUNTRY\nseven,Osprey Fund,NZ\n') }), /seven/);
  });

  it('a refused file leaves the table as it was: its rows, and where they are from', async () => {
    const v1 = await version(V1);
    const tabs = new TabTables(sink(), engine, planner);
    await tabs.load(v1);
    await assert.rejects(tabs.put(v1, 'PARTY', 'PARTY', { name: 'bad.csv', bytes: enc('ID,NAME,COUNTRY\nseven,Osprey Fund,NZ\n') }), /seven/);
    assert.deepEqual((await rows()).map((r) => r[0]), [1, 2]);
    assert.deepEqual((await tabs.tables(v1)).map((t) => [t.source.kind, t.rows]), [['test', 2]]);
  });

  it('refuses a file for a table two Databases declare: which one would be a guess', async () => {
    const two = await modelOf(`${partyText(V1)}
###Relational
Database demo::party::OtherDB
(
  Schema PARTY
  (
    Table DESK (${PARTY_COLUMNS})
  )
)

###Relational
Database demo::party::ThirdDB
(
  Schema PARTY
  (
    Table DESK (${PARTY_COLUMNS})
  )
)
`);
    const tabs = new TabTables(sink(), engine, planner);
    await assert.rejects(tabs.put(two, 'PARTY', 'DESK', { name: 'desks.csv', bytes: enc('ID,NAME,COUNTRY\n5,Rates Desk,US\n') }),
      /a file fills PARTY\.DESK, which more than one Database declares \(demo::party::OtherDB, demo::party::ThirdDB\)/);
  });
});

// The names are the planner's (the audit of this change, 2026-10-08): a table with no Schema is made with no schema,
// and a quoted column keeps its quotes -- so the tab drops, counts and fills them by the names the statements made.
describe("a table's names, as the server's statements make them", () => {
  const plain = (data: string): string => `###Relational
Database demo::plain::PlainDB
(
  Table T (ID INTEGER PRIMARY KEY, "first name" VARCHAR(20))
)

###Data
Data demo::plain::PlainData
{
  Relational
  #{
    default.T:
      '${pure(data)}';
  }#
}
`;
  const tRows = async (): Promise<unknown[][]> => {
    const t = await engine.run('SELECT ID, "first name" FROM T ORDER BY ID', 0);
    return Array.from({ length: t.rowCount }, (_, i) => t.columns.map((c) => c.values[i]));
  };
  const enc = (s: string): Uint8Array => new TextEncoder().encode(s);

  it('a table with no Schema: made, counted, filled from a file, put back, and dropped when no longer filled', async () => {
    const m = await modelOf(plain('ID,first name\n1,Ada\n'));
    const tabs = new TabTables(sink(), engine, planner);
    await tabs.load(m);
    assert.deepEqual(await tRows(), [[1, 'Ada']]);
    assert.deepEqual((await tabs.tables(m)).map((t) => [`${t.schema}.${t.table}`, t.source.kind, t.rows]), [['default.T', 'test', 1]]);
    await tabs.put(m, 'default', 'T', { name: 'people.csv', bytes: enc('ID,first name\n2,Grace\n3,Edsger\n') });
    assert.deepEqual(await tRows(), [[2, 'Grace'], [3, 'Edsger']]);
    assert.deepEqual((await tabs.tables(m)).map((t) => [t.source.kind, t.rows]), [['file', 2]]);
    await tabs.reset(m, 'default', 'T');
    assert.deepEqual(await tRows(), [[1, 'Ada']]);
    const data = new TestData(sink());
    await data.use('a', await tablesOf(m));
    await data.use('b', []);
    await assert.rejects(tRows(), /T/);
  });
});
