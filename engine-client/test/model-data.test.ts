// A model's test data in DuckDB (plan A2): the tables of a relational Data element, made and filled by the statements
// the server seeds a database with, which legend-lite's planner (WebAssembly, WASM_PLANNER, called here as the tab's
// worker calls it) gives for the model's text -- and read back. The party model is the demo's (studio/demo/projects/
// party), written here so each test can make the version it needs.

import assert from 'node:assert/strict';
import { afterEach, before, describe, it } from 'node:test';

import { DuckDbEngine, type ArrowishConnection } from '../src/duckdb.ts';
import { runLoad } from '../src/plan-runner.ts';
import type { WasmGrammar } from '../src/legend/wasm-grammar.ts';
import { loadPlanner } from './planner.ts';
import { dataTables, loadDataTables, seeded, type FileSink } from '../src/model-data.ts';
import { ProjectDatabases, ProjectRunner } from '../src/project-data.ts';
import { TabTables, type Model } from '../src/tab-data.ts';
import { startNodeDuckDb, type NodeDuckDb } from '../src/node-require.ts';
import { runfilesFromEnv } from '../../tools/js/runfiles.mts';

type Elements = Parameters<typeof dataTables>[0];

let engine: DuckDbEngine;
let db: NodeDuckDb;
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

before(async () => {
  planner = await loadPlanner();
  db = await startNodeDuckDb(runfilesFromEnv('DUCKDB_EXTENSIONS'));
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

/** Every keeper a test made: each test drops its databases (the cases share one DuckDB). */
const keepers: ProjectDatabases[] = [];
/** The tab's project databases over this file's DuckDB: each on a connection of its own, as the tab's start opens one. */
const projectsOf = (): ProjectDatabases => {
  const made = new ProjectDatabases({
    databaseType: 'DuckDB',
    run: (sql) => engine.run(sql, 0),
    connect: async () => new DuckDbEngine(db.connect() as ArrowishConnection),
  }, planner);
  keepers.push(made);
  return made;
};
afterEach(async () => {
  for (const k of keepers.splice(0)) await k.dropAll();
});

describe("each version of a project in a database of its own (C5, ProjectDatabases)", () => {
  it("answers each version's own rows at once -- none swapped for another -- and the tab's own connection sees none", async () => {
    const projects = projectsOf();
    const [one, head] = await Promise.all([projects.engineFor(partyText(V1), engine), projects.engineFor(partyText(V2), engine)]);
    const [a, b, c] = await Promise.all([rowsOf('PARTY', one.engine), rowsOf('PARTY', head.engine), rowsOf('PARTY', one.engine)]);
    assert.deepEqual([a, b, c].map((rows) => rows.map((r) => r[0])), [[1, 2], [3], [1, 2]]);
    await assert.rejects(rowsOf('PARTY'), /PARTY/);
  });

  it('makes one database for one content asked for twice at once, and keeps it: the same engine every time', async () => {
    const projects = projectsOf();
    let made = 0;
    const count = async (): Promise<void> => { made++; };
    const [a, b] = await Promise.all([projects.engineFor(partyText(V1), engine, count), projects.engineFor(partyText(V1), engine, count)]);
    assert.equal(a.name, b.name);
    assert.equal(a.engine, b.engine);
    assert.equal((await projects.engineFor(partyText(V1), engine, count)).engine, a.engine);
    assert.equal(made, 1);
    assert.deepEqual(projects.names, [a.name]);
  });

  it('names a database by what it holds: an edited row another, the same rows the same whatever else changed', async () => {
    const projects = projectsOf();
    const v1 = await projects.engineFor(partyText(V1), engine);
    const edited = await projects.engineFor(partyText(V1.replace('Halberd', 'Kestrel')), engine);
    const withAClass = await projects.engineFor(`${partyText(V1)}\n###Pure\nClass demo::party::Other { x: Integer[1]; }\n`, engine);
    assert.notEqual(edited.name, v1.name);
    assert.equal(withAClass.name, v1.name);
    assert.deepEqual((await rowsOf('PARTY', edited.engine)).map((r) => r[1]), ['Meridian Capital', 'Kestrel Securities']);
    // the first one's rows as they were: a SNAPSHOT republished with new rows is never answered with the old, nor the old with the new
    assert.deepEqual((await rowsOf('PARTY', v1.engine)).map((r) => r[1]), ['Meridian Capital', 'Halberd Securities']);
  });

  it("runs a model with no Data rows on the tab's own connection: no database of its own", async () => {
    const projects = projectsOf();
    const bare = partyText(V1).replace(/###Data[\s\S]*$/, '');
    const answer = await projects.engineFor(bare, engine);
    assert.equal(answer.engine, engine);
    assert.equal(answer.name, undefined);
    assert.deepEqual(projects.names, []);
  });

  it("gives a model with no Data rows a database of its own when asked; its owner's drop lets it go, made again after", async () => {
    const projects = projectsOf();
    const bare = partyText(V1).replace(/###Data[\s\S]*$/, '');
    const own = await projects.engineFor(bare, engine, undefined, { own: true });
    assert.notEqual(own.engine, engine);
    assert.equal(projects.names.length, 1);
    await projects.drop(own.name!);
    assert.deepEqual(projects.names, []);
    const again = await projects.engineFor(partyText(V1), engine);
    await projects.drop(again.name!);
    const third = await projects.engineFor(partyText(V1), engine);
    assert.deepEqual((await rowsOf('PARTY', third.engine)).map((r) => r[0]), [1, 2]);
  });

  it('forgets a make that failed, and makes it again when next asked', async () => {
    const projects = projectsOf();
    await assert.rejects(projects.engineFor(partyText(V1), engine, async () => { throw new Error('the person\'s file failed'); }), /failed/);
    assert.deepEqual(projects.names, []);
    const again = await projects.engineFor(partyText(V1), engine);
    assert.deepEqual((await rowsOf('PARTY', again.engine)).map((r) => r[0]), [1, 2]);
  });
});

/** Studio's tables over this file's DuckDB: the model's database, and the runner the screens run on. */
/**
 * Files as the browser's DuckDB-WASM takes them: the buffer handed over (transferred to its worker, the caller's view
 * left empty), and let go of by name (Studio's review: Node's blocking bindings copy, which hid a person's file going
 * empty the second time it went in). The names registered now, to see that none is left behind.
 */
const registered = new Set<string>();
const asTheBrowserTakesThem = {
  registerFileBuffer: async (name: string, bytes: Uint8Array): Promise<void> => {
    const taken = structuredClone(bytes, { transfer: [bytes.buffer] });
    db.registerFileBuffer(name, taken);
    registered.add(name);
  },
  dropFile: async (name: string): Promise<void> => {
    registered.delete(name);
  },
};

function tabTables(): { tabs: TabTables; runner: ProjectRunner; projects: ProjectDatabases } {
  const runner = new ProjectRunner(engine);
  const projects = projectsOf();
  return { tabs: new TabTables(projects, runner, engine, asTheBrowserTakesThem, planner), runner, projects };
}

describe("a table's rows from a person's file (plan A2)", () => {
  let runner: ProjectRunner;
  /** The workspace's tables, with `runner` the engine its screens run on. */
  const tables = (): TabTables => {
    const made = tabTables();
    runner = made.runner;
    return made.tabs;
  };
  const rows = (): Promise<unknown[][]> => rowsOf('PARTY', runner);
  const enc = (s: string): Uint8Array => new TextEncoder().encode(s);

  it("says where each declared table's rows are from, and how many", async () => {
    const v1 = await version(V1);
    const tabs = tables();
    await tabs.load(v1);
    assert.deepEqual((await tabs.tables(v1)).map((t) => [t.database, `${t.schema}.${t.table}`, t.source, t.rows]),
      [['demo::party::PartyDB', 'PARTY.PARTY', { kind: 'test', element: 'demo::party::PartyData' }, 2]]);
  });

  it('fills a table from a CSV, kept over the test data until reset', async () => {
    const v1 = await version(V1);
    const tabs = tables();
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
    const tabs = tables();
    await tabs.load(withDesk);
    await tabs.put(withDesk, 'PARTY', 'PARTY', { name: 'parties.csv', bytes: enc('ID,NAME,COUNTRY\n7,Osprey Fund,NZ\n') });
    await tabs.load(v1);
    await assert.rejects(rowsOf('DESK', runner), /DESK/);
    assert.deepEqual(await rows(), [[7, 'Osprey Fund', 'NZ']]);
    assert.deepEqual((await tabs.tables(v1)).map((t) => [t.table, t.source.kind]), [['PARTY', 'file']]);
    await tabs.reset(v1, 'PARTY', 'PARTY');
  });

  it("a database the workspace comes back to holds the person's files as they are now (an undo; a reset elsewhere)", async () => {
    const a = await version(V1);
    const b = await version(V1.replace('Halberd', 'Kestrel'));
    const tabs = tables();
    await tabs.load(a);
    await tabs.load(b);
    await tabs.put(b, 'PARTY', 'PARTY', { name: 'parties.csv', bytes: enc('ID,NAME,COUNTRY\n7,Osprey Fund,NZ\n') });
    // the Data row's edit undone: A again, which was made before the file
    await tabs.load(a);
    assert.deepEqual(await rows(), [[7, 'Osprey Fund', 'NZ']]);
    assert.deepEqual((await tabs.tables(a))[0]!.source, { kind: 'file', name: 'parties.csv' });
    // reset while on B; back on A, which held the file too: its own rows again
    await tabs.load(b);
    await tabs.reset(b, 'PARTY', 'PARTY');
    assert.deepEqual((await rows()).map((r) => r[1]), ['Meridian Capital', 'Kestrel Securities']);
    await tabs.load(a);
    assert.deepEqual((await rows()).map((r) => r[1]), ['Meridian Capital', 'Halberd Securities']);
    assert.deepEqual((await tabs.tables(a))[0]!.source, { kind: 'test', element: 'demo::party::PartyData' });
  });

  it("puts a person's file in again after the browser's DuckDB took its bytes, and leaves no registration behind", async () => {
    const tabs = tables();
    const v1 = await version(V1);
    await tabs.put(v1, 'PARTY', 'PARTY', { name: 'parties.csv', bytes: enc('ID,NAME,COUNTRY\n7,Osprey Fund,NZ\n') });
    // a Data row edited: the database made again, the file put in again from the bytes kept
    const edited = await version(V1.replace('Halberd', 'Kestrel'));
    await tabs.load(edited);
    assert.deepEqual(await rows(), [[7, 'Osprey Fund', 'NZ']]);
    assert.deepEqual((await tabs.tables(edited))[0]!.source, { kind: 'file', name: 'parties.csv' });
    assert.deepEqual([...registered], []);
  });

  it("keeps a file the model no longer takes out of the database, the rest of the workspace running (Studio's review)", async () => {
    const withDesk = await version(V1, DESK);
    const tabs = tables();
    await tabs.load(withDesk);
    await tabs.put(withDesk, 'PARTY', 'DESK', { name: 'desks.csv', bytes: enc('ID,NAME,COUNTRY\n6,Swaps Desk,GB\n') });
    // DESK removed from the model: its file has no table to go into, and the rest runs
    const v1 = await version(V1);
    await tabs.load(v1);
    assert.deepEqual((await tabs.tables(v1)).map((t) => [t.table, t.source.kind]), [['PARTY', 'test']]);
    assert.deepEqual((await rows()).map((r) => r[0]), [1, 2]);
    // DESK back: the file goes in again
    await tabs.load(withDesk);
    assert.deepEqual((await tabs.tables(withDesk)).find((t) => t.table === 'DESK')!.source, { kind: 'file', name: 'desks.csv' });
    assert.deepEqual((await rowsOf('DESK', runner)).map((r) => r[1]), ['Swaps Desk']);
    // a column the file lacks: it no longer loads; the table keeps the model's own rows, and says why
    await tabs.put(withDesk, 'PARTY', 'PARTY', { name: 'parties.csv', bytes: enc('ID,NAME,COUNTRY\n7,Osprey Fund,NZ\n') });
    const wider = await modelOf(partyText(V1, DESK, `${PARTY_COLUMNS}, RATING VARCHAR(4)`));
    await tabs.load(wider);
    const party = (await tabs.tables(wider)).find((t) => t.table === 'PARTY')!;
    assert.deepEqual([party.source.kind, party.refusedFile?.name, party.rows], ['test', 'parties.csv', 2]);
    assert.match(party.refusedFile!.why, /RATING/i);
  });

  it('holds one database at a time: what fills it changed, the one before dropped; a class edited, the same one', async () => {
    const made = tabTables();
    runner = made.runner;
    const a = await version(V1);
    await made.tabs.load(a);
    const first = made.projects.names;
    await made.tabs.load(await modelOf(`${partyText(V1)}\n###Pure\nClass demo::party::Other { x: Integer[1]; }\n`));
    assert.deepEqual(made.projects.names, first);
    await made.tabs.load(await version(V2));
    assert.equal(made.projects.names.length, 1);
    assert.notDeepEqual(made.projects.names, first);
    assert.deepEqual((await rows()).map((r) => r[0]), [3]);
  });

  it("fills a table from a file whose name DuckDB would read as a pattern (sales[2024].csv)", async () => {
    const v1 = await version(V1);
    const tabs = tables();
    await tabs.put(v1, 'PARTY', 'PARTY', { name: 'parties[2024]*?.csv', bytes: enc('ID,NAME,COUNTRY\n8,Kestrel Bank,DE\n') });
    await tabs.load(v1);
    assert.deepEqual(await rows(), [[8, 'Kestrel Bank', 'DE']]);
    assert.deepEqual((await tabs.tables(v1))[0]!.source, { kind: 'file', name: 'parties[2024]*?.csv' });
    await tabs.reset(v1, 'PARTY', 'PARTY');
  });

  it('fills a table from Parquet, each declared column by name, converted to its type', async () => {
    await engine.run("COPY (SELECT 'AU' AS COUNTRY, 9 AS ID, 'Wattle Capital' AS NAME, true AS EXTRA) TO 'made.parquet' (FORMAT PARQUET)", 0);
    const tabs = tables();
    const v1 = await version(V1);
    await tabs.put(v1, 'PARTY', 'PARTY', { name: 'parties.parquet', bytes: db.copyFileToBuffer('made.parquet') });
    await tabs.load(v1);
    assert.deepEqual(await rows(), [[9, 'Wattle Capital', 'AU']]);
  });

  it('refuses a file of another kind, a table no Database declares, a file without a declared column, and a value its column cannot take', async () => {
    const v1 = await version(V1);
    const tabs = tables();
    await assert.rejects(tabs.put(v1, 'PARTY', 'PARTY', { name: 'parties.json', bytes: enc('[]') }), /a \.csv or a \.parquet file/);
    await assert.rejects(tabs.put(v1, 'PARTY', 'NOPE', { name: 'x.csv', bytes: enc('A\n1\n') }), /a file fills PARTY\.NOPE, which no Database of the model declares/);
    await engine.run("COPY (SELECT 1 AS ID) TO 'thin.parquet' (FORMAT PARQUET)", 0);
    await assert.rejects(tabs.put(v1, 'PARTY', 'PARTY', { name: 'thin.parquet', bytes: db.copyFileToBuffer('thin.parquet') }), /NAME/);
    await assert.rejects(tabs.put(v1, 'PARTY', 'PARTY', { name: 'bad.csv', bytes: enc('ID,NAME,COUNTRY\nseven,Osprey Fund,NZ\n') }), /seven/);
  });

  it('a refused file leaves the table as it was: its rows, and where they are from', async () => {
    const v1 = await version(V1);
    const tabs = tables();
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
    const tabs = tables();
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
  let at: ProjectRunner;
  const tRows = async (): Promise<unknown[][]> => {
    const t = await at.run('SELECT ID, "first name" FROM T ORDER BY ID', 0);
    return Array.from({ length: t.rowCount }, (_, i) => t.columns.map((c) => c.values[i]));
  };
  const enc = (s: string): Uint8Array => new TextEncoder().encode(s);

  it('a table with no Schema: made, counted, filled from a file, put back, and dropped when no longer filled', async () => {
    const m = await modelOf(plain('ID,first name\n1,Ada\n'));
    const made = tabTables();
    const tabs = made.tabs;
    at = made.runner;
    await tabs.load(m);
    assert.deepEqual(await tRows(), [[1, 'Ada']]);
    assert.deepEqual((await tabs.tables(m)).map((t) => [`${t.schema}.${t.table}`, t.source.kind, t.rows]), [['default.T', 'test', 1]]);
    await tabs.put(m, 'default', 'T', { name: 'people.csv', bytes: enc('ID,first name\n2,Grace\n3,Edsger\n') });
    assert.deepEqual(await tRows(), [[2, 'Grace'], [3, 'Edsger']]);
    assert.deepEqual((await tabs.tables(m)).map((t) => [t.source.kind, t.rows]), [['file', 2]]);
    await tabs.reset(m, 'default', 'T');
    assert.deepEqual(await tRows(), [[1, 'Ada']]);
    // the model edited so its Data element fills T no longer: its own database, where there is no T
    await tabs.load(await modelOf(plain('ID,first name\n1,Ada\n').replace(/###Data[\s\S]*$/, '')));
    await assert.rejects(tRows(), /T/);
  });
});

describe("a project's own database (C5), the planner's statements run in DuckDB-WASM", () => {
  it("holds the project's rows in a database of their own, beside the tab's table of the same name, untouched", async () => {
    const project = await planner.projectData(partyText(V1), 'DuckDB');
    assert.match(project.name, /^__project_[0-9a-f]{16}$/);
    // the tab's own table, of the very name the project declares
    await engine.run('CREATE SCHEMA IF NOT EXISTS "PARTY"', 0);
    await engine.run('CREATE OR REPLACE TABLE "PARTY"."PARTY" (ID INTEGER, NAME VARCHAR, COUNTRY VARCHAR)', 0);
    await engine.run("INSERT INTO \"PARTY\".\"PARTY\" VALUES (9, 'the tab''s own', 'XX')", 0);
    for (const sql of project.open) await engine.run(sql, 0);
    const own = new DuckDbEngine(db.connect() as ArrowishConnection);
    for (const sql of project.use) await own.run(sql, 0);
    // its rows: the plan's ONCE load, run by the hosts' runner (B3: the load a server's project plan carries)
    assert.ok(project.load !== undefined, 'a project with Data rows carries their load');
    await runLoad(JSON.stringify(project.load), (sql) => own.run(sql, 0));
    assert.deepEqual(await rowsOf('PARTY', own), [[1, 'Meridian Capital', 'US'], [2, 'Halberd Securities', 'GB']]);
    assert.deepEqual(await rowsOf('PARTY'), [[9, "the tab's own", 'XX']]);
    await own.close();
    for (const sql of project.close) await engine.run(sql, 0);
    const left = await engine.run('SELECT database_name FROM duckdb_databases()', 0);
    assert.ok(!left.columns[0]!.values.includes(project.name), String(left.columns[0]!.values));
    await engine.run('DROP TABLE "PARTY"."PARTY"', 0);
  });

  it('names a database by what it holds: the same rows the same name, other rows another', async () => {
    const [a, b] = await Promise.all([planner.projectData(partyText(V1), 'DuckDB'), planner.projectData(partyText(V1), 'DuckDB')]);
    assert.equal(a.name, b.name);
    assert.notEqual((await planner.projectData(partyText(V2), 'DuckDB')).name, a.name);
  });

  it('is refused by name on a database without it', async () => {
    await assert.rejects(planner.projectData(partyText(V1), 'Postgres'), /a project's own database on Postgres is not built/);
  });
});
