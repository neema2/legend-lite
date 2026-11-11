// A write's execution plan run by the host's plan runner (plan-runner.ts; docs/DATACUBE_FLOW_MODE_2026_10_24.md M6, an
// Output into a table): legend-lite's planner (WebAssembly) plans a program that writes -- `write`, `delete` then
// `write`, `merge` by a key -- and the runner runs its steps on DuckDB-WASM in one transaction, every value the
// database's: the rows the table holds after, the count it answers.

import assert from 'node:assert/strict';
import { before, describe, it } from 'node:test';

import { startNodeDuckDb } from '../src/node-require.ts';
import { runfilesFromEnv } from '../../tools/js/runfiles.mts';
import { DuckDbEngine, type ArrowishConnection } from '../src/duckdb.ts';
import { answer, type PlannerModule } from '../src/legend/planner-answer.ts';
import { establish, hostSession, PlanRefusal, release, requestsOwn, runLoad, runPlan, targetKey, type PlanSession } from '../src/plan-runner.ts';
import { readFileSync } from 'node:fs';
import { runfileFromEnv } from '../../tools/js/runfiles.mts';
import { loadPlanner, loadPlannerModule } from './planner.ts';
import { accessor, eq, fn, lambda, lit, property, toJson, variable, type ValueSpecification } from '../../pure-protocol/src/index.ts';
import type { WasmGrammar } from '../src/legend/wasm-grammar.ts';

const MODEL = `###Relational
Database w::DB
(
    Table SRC ( id INTEGER PRIMARY KEY, name VARCHAR(32), amount DOUBLE )
    Table T ( id INTEGER PRIMARY KEY, name VARCHAR(32), amount DOUBLE )
)

###Connection
RelationalDatabaseConnection w::Conn
{
    type: DuckDB;
    specification: DuckDB { };
    auth: Test;
}

###Runtime
Runtime w::RT
{
    mappings: [];
    connections:
    [
        w::DB: [ c1: w::Conn ]
    ];
}
`;

let m: PlannerModule;
let grammar: WasmGrammar;
let engine: DuckDbEngine;
let handle: number;

const ok = (tagged: string): string => {
  const [tag, ...rest] = tagged.split('\n');
  if (tag !== 'OK') throw new Error(rest.join('\n'));
  return rest.join('\n');
};
/** A program planned as an execution plan (the planner protocol's executionPlan kind): its statements in order. */
const planned = (...body: ValueSpecification[]): string =>
  ok(answer(m, { kind: 'executionPlan', handle, lambda: toJson(lambda([], ...body)), runtime: 'w::RT' }));
const src = (): ValueSpecification => accessor('w::DB', 'SRC');
const target = (): ValueSpecification => accessor('w::DB', 'T');
/** The program run on the tab's engine, held for it, its transaction the planner's. */
const run = (plan: string): Promise<string> => engine.exclusive(async (held) =>
  runPlan(plan, await hostSession(engine.databaseType, held.rows, grammar)));
const rows = async (): Promise<string> => {
  const t = await engine.exclusive((held) => held.rows('SELECT id, name, amount FROM T ORDER BY id', []));
  return t.map((r) => r.map(String).join(':')).join(' ');
};

before(async () => {
  m = await loadPlannerModule();
  grammar = await loadPlanner();
  const db = await startNodeDuckDb(runfilesFromEnv('DUCKDB_EXTENSIONS'));
  engine = new DuckDbEngine(db.connect() as ArrowishConnection);
  await engine.run('CREATE TABLE SRC (id INTEGER PRIMARY KEY, name VARCHAR, amount DOUBLE)', 0);
  await engine.run("INSERT INTO SRC VALUES (1, 'a', 1.5), (2, 'b', 2.5)", 0);
  await engine.run('CREATE TABLE T (id INTEGER PRIMARY KEY, name VARCHAR, amount DOUBLE)', 0);
  handle = Number(ok(answer(m, { kind: 'hold', model: MODEL })));
});

describe("a write's plan, run by the host's runner on DuckDB-WASM", () => {
  it('appends: its rows written, its count the database\'s, answered once it has committed', async () => {
    const plan = planned(fn('write', src(), target()));
    assert.match(plan, /"_type":"sequence"/);
    assert.match(plan, /"_type":"update"/);
    // its answer the count as the database answered it, a typed relation of one row the runner writes as JSON rows
    assert.match(plan, /"_type":"tdsResult"/);
    assert.deepEqual(JSON.parse(await run(plan)), [{ value: 2 }]);
    assert.equal(await rows(), '1:a:1.5 2:b:2.5');
  });

  it('replaces: its rows deleted, then written -- one transaction', async () => {
    await engine.run("UPDATE SRC SET amount = amount * 10", 0);
    const plan = planned(fn('delete', target(), lambda(['r'], lit.boolean(true))), fn('write', src(), target()));
    assert.deepEqual(JSON.parse(await run(plan)), [{ value: 2 }]);
    assert.equal(await rows(), '1:a:15 2:b:25');
  });

  it('a failure takes every change back: a key written twice, the table as it was', async () => {
    // row 1 deleted, then SRC's rows written: T holds row 2's key still, so the write fails -- and the delete with it
    const plan = planned(fn('delete', target(), lambda(['r'], eq(property(variable('r'), 'id'), lit.integer(1)))), fn('write', src(), target()));
    await assert.rejects(() => run(plan), /PRIMARY KEY|Duplicate key|constraint/i);
    assert.equal(await rows(), '1:a:15 2:b:25', 'as it was');
    // the connection is the engine's again, outside any transaction
    assert.deepEqual(await engine.exclusive((held) => held.rows('SELECT 1', [])), [[1]]);
  });

  it('answers a value the program wrote with nothing sent for it: the constant node', async () => {
    // the program's last statement a String literal: the plan's own value (the test-is-the-product plan's M2), the
    // writes before it run in the one transaction
    const plan = planned(fn('delete', target(), lambda(['r'], lit.boolean(true))), fn('write', src(), target()),
      lit.string('done'));
    assert.match(plan, /"_type":"constant"/);
    assert.deepEqual(JSON.parse(await run(plan)), [{ value: 'done' }]);
    assert.equal(await rows(), '1:a:15 2:b:25');
    // a constant alone: no statement sent, the session never asked
    const alone = planned(lit.boolean(true));
    const untouched: PlanSession = { databaseType: 'DuckDB', run: async () => { throw new Error('ran'); }, transaction: async () => { throw new Error('ran'); } };
    assert.deepEqual(JSON.parse(await runPlan(alone, untouched)), [{ value: true }]);
  });

  it('refuses by name what a host never plans, nothing run', async () => {
    const plan = JSON.parse(planned(fn('write', src(), target()))) as Record<string, unknown>;
    const session: PlanSession = { databaseType: 'Postgres', run: async () => { throw new Error('ran'); }, transaction: async () => { throw new Error('ran'); } };
    await assert.rejects(() => runPlan(JSON.stringify(plan), session), (e: unknown) => e instanceof PlanRefusal && /not run on this Postgres one/.test(e.message));
    await assert.rejects(() => runPlan(JSON.stringify({ ...plan, parameters: [{ name: 'p' }] }), session), /takes parameters/);
    await assert.rejects(() => runPlan('{"format":"other"}', session), /not a legend-lite execution plan/);
    // a plan of another version, and a statement only a dynamic pivot's probe plan writes (version 19): refused, never run (the version the runner reads, PLAN_VERSION: the A checkpoint's audit made it 42, 2026-11-10)
    await assert.rejects(() => runPlan(JSON.stringify({ ...plan, version: 5 }), session), /not a legend-lite execution plan of version 47/);
    const root = plan.root as { steps: { sql: Record<string, unknown> }[] };
    const regioned = { ...plan, root: { ...root, steps: root.steps.map((step) => ({ ...step, sql: { ...step.sql, regions: [{ start: 0 }] } })) } };
    const here: PlanSession = { ...session, databaseType: 'DuckDB' };
    await assert.rejects(() => runPlan(JSON.stringify(regioned), here), /per-key regions/);
    // a relation of no declared column (a raw grid's: the columns its result reports): refused, never run as rows of
    // no cell (the seam program's S3c, 2026-10-30)
    const reported = { ...plan, root: { ...root, steps: root.steps.map((step) => ((step as { _type?: string })._type === 'tdsResult'
      ? { ...step, shape: 'RELATION', columns: [] } : step)) } };
    await assert.rejects(() => runPlan(JSON.stringify(reported), here), /the columns its result reports/);
    // a program's seed step and a statement binding its own execution-trace stamp: a test's program's alone (the
    // test-is-the-product plan's M3), refused by name, never run
    const steps = root.steps as unknown as { _type: string; sql: { target: unknown } }[];
    const seeded = { ...plan, root: { ...root, steps: [{ _type: 'setup', key: 'k', steps: [], target: steps[0]!.sql.target }, ...steps] } };
    await assert.rejects(() => runPlan(JSON.stringify(seeded), here), /not a setup/);
    // a target that loads data first (its declared test data, a project's Data: the plan's load section, L2/L3)
    const loading = { ...plan, root: { ...root, steps: root.steps.map((step) => ({ ...step, sql: { ...step.sql, target: { ...(step.sql.target as object), loads: ['load:k'] } } })) } };
    await assert.rejects(() => runPlan(JSON.stringify(loading), here), /loads data into its database first/);
    const stamped = { ...plan, root: { ...root, steps: root.steps.map((step) => ({ ...step, sql: { ...step.sql, stamp: '#stamp0' } })) } };
    await assert.rejects(() => runPlan(JSON.stringify(stamped), here), /its own execution-trace stamp/);
  });
});

// THE LOADS (B3, 2026-11-11): a plan whose target loads data -- a connection's declared test data, a data: source's
// session table -- runs on a database the host made for it, established there once (its loads in one transaction, each
// content blob checked against its digest), and released after (a session table dropped).
const LOADED = `###Relational
Database x::DB ( Table T (ID INTEGER PRIMARY KEY, NAME VARCHAR(20)) )

###Connection
RelationalDatabaseConnection x::Conn { store: x::DB; type: DuckDB; specification: LocalH2 { testDataSetupCSV: 'default\\nT\\nID,NAME\\n1,a\\n2,b\\n'; }; auth: DefaultH2; }
RelationalDatabaseConnection x::Bare { store: x::DB; type: DuckDB; specification: DuckDB { }; auth: Test; }

###Runtime
Runtime x::RT { mappings: []; connections: [ x::DB: [ c: x::Conn ] ]; }
Runtime x::BareRT { mappings: []; connections: [ x::DB: [ c: x::Bare ] ]; }
`;

describe("a plan's loads, established on a database the host made for its target", () => {
  let loaded: number;
  /** A text query planned over the held model: the execution plan its text renders from (Boundary.planned's `plan`). */
  const executionPlanOf = (query: string, runtime: string): string => {
    const planned = JSON.parse(ok(answer(m, { kind: 'plan', handle: loaded, query, runtime }))) as { plan?: unknown };
    assert.ok(planned.plan !== undefined, 'a text that does not run alone carries its execution plan');
    return JSON.stringify(planned.plan);
  };
  /** A text query's execution plan, asked as the planner's executionPlan kind (its lambda read from the text). */
  const executionPlanOfLambda = (query: string, runtime: string): string => {
    // the http answer: its status, its media type, then the body
    const [status, , ...body] = ok(answer(m, { kind: 'pureV1', path: '/api/pure/v1/grammar/grammarToJson/lambda', query: '', body: query })).split('\n');
    assert.equal(status, '200', body.join('\n'));
    const lambdaJson = body.join('\n');
    return ok(answer(m, { kind: 'executionPlan', handle: loaded, lambda: lambdaJson, runtime }));
  };
  /** A database of its own, as a host makes one for a target: a fresh DuckDB-WASM. */
  const fresh = async (): Promise<{ readonly engine: DuckDbEngine; readonly on: <T>(work: (s: PlanSession) => Promise<T>) => Promise<T> }> => {
    const db = await startNodeDuckDb(runfilesFromEnv('DUCKDB_EXTENSIONS'));
    const own = new DuckDbEngine(db.connect() as ArrowishConnection);
    return { engine: own, on: (work) => own.exclusive(async (held) => work(await hostSession(own.databaseType, held.rows, grammar))) };
  };
  const tables = (e: DuckDbEngine): Promise<string> => e.exclusive(async (held) =>
    (await held.rows("SELECT string_agg(table_name, ',' ORDER BY table_name) FROM duckdb_tables()", [])).map((r) => String(r[0])).join(''));

  before(() => {
    loaded = Number(ok(answer(m, { kind: 'hold', model: LOADED })));
  });

  it("a connection's declared test data: established once, the query run on it", async () => {
    const plan = executionPlanOf('|#>{x::DB.T}#->select(~[ID, NAME])->sort(~ID->ascending())', 'x::RT');
    assert.ok(targetKey(plan) !== null, 'a target that loads is kept by its key');
    assert.equal(requestsOwn(plan), false);
    const db = await fresh();
    // on a session the host did not establish for it, refused by name: never run against tables no load made
    await assert.rejects(() => db.on((s) => runPlan(plan, s)), /runs on a database the host made for it and established/);
    await db.on((s) => establish(plan, s));
    assert.deepEqual(JSON.parse(await db.on((s) => runPlan(plan, s, { established: true }))), [{ ID: 1, NAME: 'a' }, { ID: 2, NAME: 'b' }]);
  });

  it("a content blob that is not the data planned is refused, naming it, and its load rolled back", async () => {
    const plan = JSON.parse(executionPlanOf('|#>{x::DB.T}#->select(~[ID, NAME])', 'x::RT')) as { loads: { content: Record<string, string> }[] };
    const [load] = plan.loads;
    const [digest] = Object.keys(load!.content);
    load!.content[digest!] = load!.content[digest!]!.replace("'a'", "'z'");
    const db = await fresh();
    await assert.rejects(() => db.on((s) => establish(JSON.stringify(plan), s)),
      (e: unknown) => e instanceof PlanRefusal && /the data of the table T is not the data it was planned against/.test(e.message));
    assert.equal(await tables(db.engine), 'null', 'the load rolled back: no table made');
  });

  it("a data: source's session table: made and filled when established, dropped at the release", async () => {
    const plan = executionPlanOfLambda("|sourceUrl('data:application/json,[{\"ID\":1,\"NAME\":\"a\"},{\"ID\":2,\"NAME\":\"b\"}]')"
      + "->extend(~[ID: r|cast(get($r.data, 'ID'), @meta::pure::metamodel::type::Integer), NAME: r|cast(get($r.data, 'NAME'), @meta::pure::metamodel::type::String)])"
      + "->select(~[ID, NAME])->sort(~ID->ascending())->from(x::BareRT)", 'x::BareRT');
    assert.equal(requestsOwn(plan), true, "the request's own tables: never a database kept for another");
    const db = await fresh();
    await db.on((s) => establish(plan, s));
    assert.deepEqual(JSON.parse(await db.on((s) => runPlan(plan, s, { established: true }))), [{ ID: 1, NAME: 'a' }, { ID: 2, NAME: 'b' }]);
    const temporary = (): Promise<number> => db.engine.exclusive(async (held) =>
      Number((await held.rows('SELECT count(*) FROM duckdb_tables() WHERE temporary', []))[0]![0]));
    assert.equal(await temporary(), 1, 'its session table');
    await db.on((s) => release(plan, s));
    assert.equal(await temporary(), 0, 'dropped at the release');
  });

  it('a pivot whose columns the data names: its keys read first, its body written per key and run', async () => {
    const plan = executionPlanOfLambda("{|#TDS{\ndept, city, name\nA, NYC, x\nA, SAN, y\nB, NYC, z\n}#"
      + "->pivot(~[city], ~[names:r|$r.name:y|$y->joinStrings(',')])->sort(~dept->ascending())->from(x::BareRT)}", 'x::BareRT');
    assert.match(plan, /"_type":"keyed"/);
    assert.equal(targetKey(plan), null, "it loads nothing: the host's own session serves it");
    // each key's column named as the plan's keys statement names it (Java's runner reads the same name)
    assert.deepEqual(JSON.parse(await run(plan)), [
      { dept: 'A', "'NYC__|__names'": 'x', "'SAN__|__names'": 'y' },
      { dept: 'B', "'NYC__|__names'": 'z', "'SAN__|__names'": null },
    ]);
  });
});

// THE LOAD RUNNERS' CONFORMANCE (B3, 2026-11-11): runLoad and Python's projects.run_load run one fixture -- the same
// plan's load, the same rows, the same tampered blob refused in the same words (python/tests/test_engine.py)
describe("the load runners' shared conformance fixture", () => {
  interface Fixture { model: string; databaseType: string; check: string; rows: unknown[][]; tamper: { from: string; to: string }; refused: string }
  const fixture = JSON.parse(readFileSync(runfileFromEnv('RUN_LOAD_FIXTURE'), 'utf8')) as Fixture;
  const planOf = (): { open: string[]; use: string[]; load: { root: { content: Record<string, string> } } } =>
    JSON.parse(ok(answer(m, { kind: 'projectData', model: fixture.model, databaseType: fixture.databaseType })));

  it('gives the rows the fixture names', async () => {
    const project = planOf();
    const db = await startNodeDuckDb(runfilesFromEnv('DUCKDB_EXTENSIONS'));
    const own = new DuckDbEngine(db.connect() as ArrowishConnection);
    for (const sql of [...project.open, ...project.use]) await own.run(sql, 0);
    await runLoad(JSON.stringify(project.load), (sql) => own.run(sql, 0));
    const rows = await own.exclusive((held) => held.rows(fixture.check, []));
    assert.deepEqual(rows.map((r) => r.map((v) => (typeof v === 'bigint' ? Number(v) : v))), fixture.rows);
  });

  it('refuses the tampered blob in the fixture\'s words, nothing run', async () => {
    const project = planOf();
    const content = project.load.root.content;
    const [digest] = Object.keys(content);
    content[digest!] = content[digest!]!.replace(fixture.tamper.from, fixture.tamper.to);
    const sent: string[] = [];
    await assert.rejects(() => runLoad(JSON.stringify(project.load), async (sql) => { sent.push(sql); }),
      (e: unknown) => e instanceof PlanRefusal && e.message === fixture.refused || assert.fail(String(e instanceof Error ? e.message : e)));
    assert.ok(!sent.some((s) => /INSERT/i.test(s)), sent.join('; '));
  });
});

