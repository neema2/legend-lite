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
import { checkParameters, ConnectionSettings, establish, PLAN_VERSION, expandKeyed, inLoadsTransaction, ParameterRefusal, PlanRefusal, runAuthored, runLoad, runPlan, targetKey, transacted, type PlanSession } from '../src/plan-runner.ts';
import { currentRun, frameKept, inRun } from '../src/run-frame.ts';
import { DatabaseError, failure, PlanRaise } from '../src/raised.ts';
import { declareStatementSites, type Site } from '../src/declared-raises.ts';
import { heldTexts, redactHeld } from '../src/connection-settings.ts';
import { PlanParamsRefusal, planParams, statementParams } from '../src/plan-params.ts';
import type { PlanColumn } from '../src/relation-type.ts';
import { statementOf, TabEngine } from '../src/legend/browser-engine.ts';
import { BrowserEngine } from '../src/legend/edge-engine.ts';
import { QueryError } from '../src/engine.ts';
import { SecretStore } from '../src/host-operations.ts';
import { readFileSync } from 'node:fs';
import { runfileFromEnv } from '../../tools/js/runfiles.mts';
import { loadPlanner, loadPlannerModule, loadPlannerPort } from './planner.ts';
import { loadEnginePort } from './engine.ts';
import { WasmEdgeGrammar } from '../src/legend/wasm-edge.ts';
import { accessor, eq, fn, lambda, lit, property, toJson, variable, type ValueSpecification } from '../../pure-protocol/src/index.ts';
import type { WasmGrammar } from '../src/legend/wasm-grammar.ts';
import { arrowIpcStream } from '../src/arrow-ipc.ts';
import type { Receipt } from '../src/receipt.ts';
import { unplanned, unplannedPlan, asTest } from './unplanned.ts';
import type { PlannedSql, PlanText } from '../src/planned-sql.ts';

/** This test reads a JSON text loosely, its fields probed as it goes: since the C final audit's H5 (2026-11-12)
 *  JSON.parse is `unknown` everywhere (engine-client/src/json-unknown.ts), so the product reads none unchecked. */
const looseJson = (text: string): any => JSON.parse(text);

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
/** query/plan's answer (`{"plan":<PlanJson>}`) as its plan's own text, its numbers' digits whole. */
const planJsonOf = (answered: string): string => {
  assert.ok(answered.startsWith('{"plan":') && answered.endsWith('}'), answered.slice(0, 120));
  return answered.slice('{"plan":'.length, -1);
};
/** `lambdaJson` planned over the model held as `h` (lite API v1's query/plan, held): its plan's text. */
const heldPlan = (h: number, lambdaJson: string, runtime: string): string =>
  planJsonOf(ok(answer(m, { kind: 'queryPlanHeld', handle: h, request: `{"query":${lambdaJson},"runtime":${JSON.stringify(runtime)}}` })));
/** A program planned as an execution plan (the planner protocol's queryPlanHeld kind): its statements in order. */
const planned = (...body: ValueSpecification[]): PlanText =>
  unplannedPlan(heldPlan(handle, toJson(lambda([], ...body)), 'w::RT'));
const src = (): ValueSpecification => accessor('w::DB', 'SRC');
const target = (): ValueSpecification => accessor('w::DB', 'T');
/** A host's session over a held connection: its statements sent one at a time, its transactions the plan's (S8). */
const sessionOn = (databaseType: string, held: { rows: PlanSession['run'] }): PlanSession => ({ databaseType, run: held.rows });
/** The program run on the tab's engine, held for it, its transaction its plan's -- in runPlan's OWN run frame, never a
 *  test's (the C5-T audit's nit: wrapped in asTest, a runPlan that lost its frame went unseen; unwrapped, its held
 *  connection's sends are refused outside one). */
const run = (plan: string): Promise<string> => engine.exclusive(async (held) =>
  runPlan(unplannedPlan(plan), sessionOn(engine.databaseType, held)));
const rows = async (): Promise<string> => {
  const t = await inRun('a test', () => asTest(engine).exclusive((held) => held.rows(unplanned('SELECT id, name, amount FROM T ORDER BY id'), [])));
  return t.map((r) => r.map(String).join(':')).join(' ');
};

/**
 * Every statement a plan's JSON carries for its steps, in the order it is written -- a node's `statement` or `sql`
 * text, a list of them, a statements step's blob (read by its digest) -- its typed trees and its target passed over:
 * what a run of it sends after its target's session, and in that order (the C final audit's H8, 2026-11-12).
 */
const statementsOf = (node: unknown, content: Readonly<Record<string, string>> = {}): string[] => {
  if (Array.isArray(node)) return node.flatMap((n) => statementsOf(n, content));
  if (node === null || typeof node !== 'object') return [];
  const here = node as Readonly<Record<string, unknown>>;
  const blobs = typeof here['content'] === 'object' && here['content'] !== null ? here['content'] as Record<string, string> : content;
  if (here['_type'] === 'statements') {
    const digest = (here['ref'] as { readonly digest: string }).digest;
    return looseJson(blobs[digest]!) as string[];
  }
  const out: string[] = [];
  for (const [key, value] of Object.entries(here)) {
    if (['tree', 'content', 'regions', 'target'].includes(key)) continue;
    if ((key === 'statement' || key === 'sql') && typeof value === 'string') out.push(value);
    else if (Array.isArray(value) && value.length > 0 && value.every((v) => typeof v === 'string')) out.push(...(value as string[]));
    else out.push(...statementsOf(value, blobs));
  }
  return out;
};

/** A plan's target's session statements (the first target it names): what a run sends first, once. */
const sessionOf = (node: unknown): string[] => {
  if (node === null || typeof node !== 'object') return [];
  const here = node as Readonly<Record<string, unknown>>;
  const target = here['target'] as { readonly session?: readonly string[] } | undefined;
  if (target?.session !== undefined) return [...target.session];
  for (const value of Object.values(here)) {
    const found = sessionOf(value);
    if (found.length > 0) return found;
  }
  return [];
};

/** The transaction the plan's target carries (version 50, the re-audit's S8): what the runner opens one with. */
const transactionOf = (node: unknown): { readonly begin: readonly string[]; readonly commit: readonly string[] } => {
  const find = (n: unknown): { readonly begin: readonly string[]; readonly commit: readonly string[] } | undefined => {
    if (n === null || typeof n !== 'object') return undefined;
    const here = n as Readonly<Record<string, unknown>>;
    const t = here['target'] as { readonly transaction?: { readonly begin: readonly string[]; readonly commit: readonly string[] } } | undefined;
    return t?.transaction ?? Object.values(here).map(find).find((x) => x !== undefined);
  };
  const found = find(node);
  if (found === undefined) throw new Error('a plan whose target carries no transaction');
  return found;
};

/** The database the write plans run on: another connection to it is a witness's (`witnessed`). */
let database: Awaited<ReturnType<typeof startNodeDuckDb>>;

/**
 * THE UNFILTERED WITNESS (the C final audit's H8 and H13, 2026-11-12): an engine over a connection that records every
 * text DuckDB-WASM itself is handed -- each query, each statement prepared or sent, each Arrow import (by its target's
 * name) -- not what the runner says it sent. A plan's run must hand it its plan's statements and nothing else.
 */
const witnessed = (raw: ArrowishConnection): { readonly engine: DuckDbEngine; readonly received: string[]; readonly runs: (string | undefined)[] } => {
  const received: string[] = [];
  // the run each text arrived inside (run-frame.ts, the witness): a plan's statements are sent inside its run
  const runs: (string | undefined)[] = [];
  const took = (text: string): void => {
    received.push(text);
    runs.push(currentRun());
  };
  const connection: ArrowishConnection = {
    query: (sql) => { took(sql); return raw.query(sql); },
    ...(raw.send === undefined ? {} : { send: (sql: PlannedSql) => { took(sql); return raw.send!(sql); } }),
    ...(raw.prepare === undefined ? {} : { prepare: (sql: PlannedSql) => { took(sql); return raw.prepare!(sql); } }),
    ...(raw.insertArrowFromIPCStream === undefined ? {} : {
      insertArrowFromIPCStream: (buffer: Uint8Array, options: { name: string; schema?: string; create?: boolean }) => {
        took(`<an Arrow stream into ${options.name}>`);
        return raw.insertArrowFromIPCStream!(buffer, options);
      },
    }),
  };
  return { engine: new DuckDbEngine(connection), received, runs };
};

before(async () => {
  m = await loadPlannerModule();
  grammar = await loadPlanner();
  const db = await startNodeDuckDb(runfilesFromEnv('DUCKDB_EXTENSIONS'));
  database = db;
  engine = new DuckDbEngine(db.connect() as ArrowishConnection);
  await inRun('a test', () => asTest(engine).run(unplanned('CREATE TABLE SRC (id INTEGER PRIMARY KEY, name VARCHAR, amount DOUBLE)'), 0));
  await inRun('a test', () => asTest(engine).run(unplanned("INSERT INTO SRC VALUES (1, 'a', 1.5), (2, 'b', 2.5)"), 0));
  await inRun('a test', () => asTest(engine).run(unplanned('CREATE TABLE T (id INTEGER PRIMARY KEY, name VARCHAR, amount DOUBLE)'), 0));
  handle = Number(ok(answer(m, { kind: 'hold', model: MODEL })));
});

describe("a write's plan, run by the host's runner on DuckDB-WASM", () => {
  it('appends: its rows written, its count the database\'s, answered once it has committed', async () => {
    const plan = planned(fn('write', src(), target()));
    assert.match(plan, /"_type":"sequence"/);
    assert.match(plan, /"_type":"update"/);
    // its answer the count as the database answered it, a typed relation of one row the runner writes as JSON rows
    assert.match(plan, /"_type":"tdsResult"/);
    assert.deepEqual(looseJson(await run(plan)), [{ value: 2 }]);
    assert.equal(await rows(), '1:a:1.5 2:b:2.5');
  });

  it('replaces: its rows deleted, then written -- one transaction', async () => {
    await inRun('a test', () => asTest(engine).run(unplanned("UPDATE SRC SET amount = amount * 10"), 0));
    const plan = planned(fn('delete', target(), lambda(['r'], lit.boolean(true))), fn('write', src(), target()));
    assert.deepEqual(looseJson(await run(plan)), [{ value: 2 }]);
    assert.equal(await rows(), '1:a:15 2:b:25');
  });

  it('a failure takes every change back: a key written twice, the table as it was', async () => {
    // row 1 deleted, then SRC's rows written: T holds row 2's key still, so the write fails -- and the delete with it
    const plan = planned(fn('delete', target(), lambda(['r'], eq(property(variable('r'), 'id'), lit.integer(1)))), fn('write', src(), target()));
    await assert.rejects(() => run(plan), /PRIMARY KEY|Duplicate key|constraint/i);
    assert.equal(await rows(), '1:a:15 2:b:25', 'as it was');
    // the connection is the engine's again, outside any transaction
    assert.deepEqual(await inRun('a test', () => asTest(engine).exclusive((held) => held.rows(unplanned('SELECT 1'), []))), [[1]]);
  });

  it('answers a value the program wrote with nothing sent for it: the constant node', async () => {
    // the program's last statement a String literal: the plan's own value (the test-is-the-product plan's M2), the
    // writes before it run in the one transaction
    const plan = planned(fn('delete', target(), lambda(['r'], lit.boolean(true))), fn('write', src(), target()),
      lit.string('done'));
    assert.match(plan, /"_type":"constant"/);
    assert.deepEqual(looseJson(await run(plan)), [{ value: 'done' }]);
    assert.equal(await rows(), '1:a:15 2:b:25');
    // a constant alone: no statement sent, the session never asked
    const alone = planned(lit.boolean(true));
    const untouched: PlanSession = { databaseType: 'DuckDB', run: async () => { throw new Error('ran'); } };
    assert.deepEqual(looseJson(await runPlan(alone, untouched)), [{ value: true }]);
  });

  it("hands the database exactly the plan's statements, in order, inside the plan's own transaction (the C final audit's H8; the re-audit's S8)", async () => {
    await inRun('a test', () => asTest(engine).run(unplanned('DELETE FROM T'), 0));
    const plan = planned(fn('write', src(), target()));
    // the witness at the database itself: every text DuckDB-WASM's connection is handed during the run (H13) -- the
    // transaction's statements the plan's target's, never asked of the planner at run (S8: no planner is handed in)
    const { engine: own, received, runs } = witnessed(database.connect() as ArrowishConnection);
    await inRun('a test', () => own.exclusive(async (held) => runPlan(plan, sessionOn(own.databaseType, held))));
    const read: unknown = looseJson(plan);
    const { begin, commit } = transactionOf(read);
    assert.deepEqual(begin, ['BEGIN TRANSACTION']);
    assert.deepEqual(received, [...sessionOf(read), ...begin, ...statementsOf((read as { root: unknown }).root), ...commit]);
    // each received inside a run: the session's statements in the plan's, the rest in its transaction's, nested in it
    assert.deepEqual(runs, [...sessionOf(read).map(() => 'a plan'), ...received.slice(sessionOf(read).length).map(() => 'a transaction')]);
  });

  // the witness (run-frame.ts, 2026-11-13): each of the runner's roads is a run, so a planner entry it reaches is refused
  it('runs each of its roads as a run, which plans nothing', async () => {
    const plan = planned(fn('write', src(), target()));
    const seen: (string | undefined)[] = [];
    const seeing: PlanSession = { databaseType: 'DuckDB', run: async () => { seen.push(currentRun()); return []; } };
    await establish(plan, seeing);
    assert.ok(seen.length > 0 && seen.every((r) => r === "a target's establishing"), String(seen));
    assert.equal(await transacted({ begin: [], commit: [], rollback: [] }, async () => [], async () => currentRun()), 'a transaction');
    const written = unplannedPlan(ok(answer(m, { kind: 'authored', request: JSON.stringify({ databaseType: 'DuckDB', text: 'SELECT 1' }) })));
    assert.equal(await runAuthored(written, { databaseType: 'DuckDB', run: async () => currentRun() }), 'an authored statement');
    if (frameKept) {
      await assert.rejects(transacted({ begin: [], commit: [], rollback: [] }, async () => [], async () => answer(m, { kind: 'hold', model: MODEL })),
        /the planner was asked 'hold' inside a run \(a transaction\)/);
    }
    assert.equal(currentRun(), undefined, 'each run ended with its road');
  });

  it("refuses by name a write whose target carries no transaction, nothing run (the re-audit's S8)", async () => {
    const read = looseJson(planned(fn('write', src(), target()))) as { root: unknown };
    const strip = (node: unknown): void => {
      if (node === null || typeof node !== 'object') return;
      const here = node as Record<string, unknown>;
      if (typeof here['target'] === 'object' && here['target'] !== null) delete (here['target'] as Record<string, unknown>)['transaction'];
      Object.values(here).forEach(strip);
    };
    strip(read);
    const sent: string[] = [];
    const session: PlanSession = { databaseType: 'DuckDB', run: async (sql) => { sent.push(sql); return []; } };
    await assert.rejects(() => runPlan(unplannedPlan(JSON.stringify(read)), session),
      (e: unknown) => e instanceof PlanRefusal && /carries its target's transaction, and this one has none/.test(e.message));
    assert.deepEqual(sent, sessionOf(read), 'nothing of its writes run');
  });

  it("refuses by name a plan missing what its version carries, nothing run (the C final audit's H10)", async () => {
    /** The plan with `key` taken out of every target it names. */
    const without = (key: string): string => {
      const read: unknown = looseJson(planned(fn('write', src(), target())));
      const strip = (node: unknown): void => {
        if (node === null || typeof node !== 'object') return;
        const here = node as Record<string, unknown>;
        if (typeof here['target'] === 'object' && here['target'] !== null) delete (here['target'] as Record<string, unknown>)[key];
        Object.values(here).forEach(strip);
      };
      strip(read);
      return JSON.stringify(read);
    };
    const sent: string[] = [];
    const session: PlanSession = { databaseType: 'DuckDB', run: async (sql) => { sent.push(sql); return []; } };
    await assert.rejects(() => runPlan(unplannedPlan(without('session')), session),
      (e: unknown) => e instanceof PlanRefusal && /carries its target's session, and this one has none/.test(e.message));
    assert.deepEqual(sent, [], 'nothing run');
    assert.throws(() => targetKey(unplannedPlan(without('loads'))),
      (e: unknown) => e instanceof PlanRefusal && /carries its target's loads, and this one has none/.test(e.message));
  });

  it('refuses by name what a host never plans, nothing run', async () => {
    const plan = looseJson(planned(fn('write', src(), target()))) as Record<string, unknown>;
    const session: PlanSession = { databaseType: 'Postgres', run: async () => { throw new Error('ran'); } };
    await assert.rejects(() => runPlan(unplannedPlan(JSON.stringify(plan)), session), (e: unknown) => e instanceof PlanRefusal && /not run on this Postgres one/.test(e.message));
    // a parameter's declaration without its type and multiplicity (2026-11-17, the edge program's H3: a plan's parameters
    // are bound at run, so their declarations are read -- until then any plan that took parameters was refused whole)
    await assert.rejects(() => runPlan(unplannedPlan(JSON.stringify({ ...plan, parameters: [{ name: 'p' }] })), session),
      (e: unknown) => e instanceof PlanRefusal && e.message === 'a plan parameter declared without its name, type, multiplicity and enumeration\'s values: {"name":"p"}');
    await assert.rejects(() => runPlan(unplannedPlan('{"format":"other"}'), session), /not a legend-lite execution plan/);
    // a plan of another version, and a statement only a dynamic pivot's probe plan writes (version 19): refused, never run (the version the runner reads, PLAN_VERSION: the A checkpoint's audit made it 42, 2026-11-10)
    await assert.rejects(() => runPlan(unplannedPlan(JSON.stringify({ ...plan, version: 5 })), session),
      new RegExp(`not a legend-lite execution plan of version ${PLAN_VERSION}:`));
    const root = plan.root as { steps: { sql: Record<string, unknown> }[] };
    const regioned = { ...plan, root: { ...root, steps: root.steps.map((step) => ({ ...step, sql: { ...step.sql, regions: [{ start: 0 }] } })) } };
    const here: PlanSession = { ...session, databaseType: 'DuckDB' };
    await assert.rejects(() => runPlan(unplannedPlan(JSON.stringify(regioned)), here), /per-key regions/);
    // a relation of no declared column (a raw grid's: the columns its result reports): refused, never run as rows of
    // no cell (the seam program's S3c, 2026-10-30)
    const reported = { ...plan, root: { ...root, steps: root.steps.map((step) => ((step as { _type?: string })._type === 'tdsResult'
      ? { ...step, shape: 'RELATION', columns: [] } : step)) } };
    await assert.rejects(() => runPlan(unplannedPlan(JSON.stringify(reported)), here), /the columns its result reports/);
    // a program's seed step and a statement binding its own execution-trace stamp: a test's program's alone (the
    // test-is-the-product plan's M3), refused by name, never run
    const steps = root.steps as unknown as { _type: string; sql: { target: unknown } }[];
    const seeded = { ...plan, root: { ...root, steps: [{ _type: 'setup', key: 'k', steps: [], target: steps[0]!.sql.target }, ...steps] } };
    await assert.rejects(() => runPlan(unplannedPlan(JSON.stringify(seeded)), here), /not a setup/);
    // a target that loads data first (its declared test data, a project's Data: the plan's load section, L2/L3)
    const loading = { ...plan, root: { ...root, steps: root.steps.map((step) => ({ ...step, sql: { ...step.sql, target: { ...(step.sql.target as object), loads: ['load:k'] } } })) } };
    await assert.rejects(() => runPlan(unplannedPlan(JSON.stringify(loading)), here), /loads data into its database first/);
    const stamped = { ...plan, root: { ...root, steps: root.steps.map((step) => ({ ...step, sql: { ...step.sql, stamp: '#stamp0' } })) } };
    await assert.rejects(() => runPlan(unplannedPlan(JSON.stringify(stamped)), here), /its own execution-trace stamp/);
  });
});

// THE LOADS (B3, 2026-11-11): a plan whose target loads data -- a connection's declared test data -- runs on a database
// the host made for it, established there once (its loads in one transaction, each content blob checked against its
// digest). A data: source loads nothing since the JSON pass: its payload is bound.
const LOADED = `###Relational
Database x::DB ( Table T (ID INTEGER PRIMARY KEY, NAME VARCHAR(20)) )

###Connection
RelationalDatabaseConnection x::Conn { store: x::DB; type: DuckDB; specification: LocalH2 { testDataSetupCSV: 'default\\nT\\nID,NAME\\n1,a\\n2,b\\n'; }; auth: DefaultH2; }
RelationalDatabaseConnection x::Bare { store: x::DB; type: DuckDB; specification: DuckDB { }; auth: Test; }

###Runtime
Runtime x::RT { mappings: []; connections: [ x::DB: [ c: x::Conn ] ]; }
Runtime x::BareRT { mappings: []; connections: [ x::DB: [ c: x::Bare ] ]; }
`;

describe("lite's query/run in the tab on lite's own plan, its parameters bound at run (lite's API v1 spec §2, §4)", () => {
  // a query of the tab's DuckDB: SRC's rows past an id, the id a parameter the plan declares and the run binds
  // (its id and name: an earlier test's write changes SRC's amounts)
  const PAST = '{n: Integer[1]|#>{w::DB.SRC}#->filter(r|$r.id > $n)->select(~[id, name])->sort(~id->ascending())->from(w::RT)}';

  it("answers the database's rows typed by the plan's columns, the same plan bound to each run's value", async () => {
    const tab = new TabEngine(grammar, asTest(engine), engine.databaseType);
    const query = await grammar.queryParse(PAST);
    const one = await tab.queryRun(MODEL, query, 'w::RT', { n: 1 });
    assert.equal(one.shape, 'RELATION');
    assert.deepEqual(one.columns, [{ name: 'id', type: 'Integer' }, { name: 'name', type: 'String' }]);
    assert.deepEqual(one.rows, [[2, 'b']]);
    assert.deepEqual((await tab.queryRun(MODEL, query, 'w::RT', { n: 0 })).rows, [[1, 'a'], [2, 'b']]);
    // an Integer parameter given as a bigint (spec §4): within 2^53 bound exactly; past it the tab's DuckDB, which binds
    // through JSON, refuses it by name before anything is sent (a server binds it exactly: lite-api-server.test.ts)
    assert.deepEqual((await tab.queryRun(MODEL, query, 'w::RT', { n: 1n })).rows, [[2, 'b']]);
    await assert.rejects(tab.queryRun(MODEL, query, 'w::RT', { n: 9223372036854775806n }), (e: unknown) =>
      e instanceof RangeError && e.message.includes('9223372036854775806 is not bound (a legend-lite server binds it exactly)'));
    // the value is bound, never written: query/plan's statement carries its placeholder
    const sql = statementOf(await tab.queryPlan(MODEL, query, 'w::RT'));
    assert.match(sql, /\?/);
    assert.ok(!sql.includes('9223372036854775806'), sql);
  });

  it('refuses by name, before anything is sent, a parameter with no value and a value its type does not take', async () => {
    const seen = witnessed(database.connect() as ArrowishConnection);
    const tab = new TabEngine(grammar, asTest(seen.engine), seen.engine.databaseType);
    const query = await grammar.queryParse(PAST);
    await assert.rejects(tab.queryRun(MODEL, query, 'w::RT'), (e: unknown) => e instanceof ParameterRefusal
      && e.message === 'no value for the parameter(s) n:Integer[1]');
    await assert.rejects(tab.queryRun(MODEL, query, 'w::RT', { n: 'one' }), (e: unknown) => e instanceof ParameterRefusal
      && e.message === 'values their parameters do not take: [parameter \'n\' (Integer[1]): given "one" (string): an Integer is a whole number: a safe integer, or a bigint]');
    // a whole number past 2^53 as a number has lost its digits: refused by name, never bound as the nearest double
    await assert.rejects(tab.queryRun(MODEL, query, 'w::RT', { n: 2 ** 60 }), (e: unknown) => e instanceof ParameterRefusal
      && e.message.includes('is a whole number past 2^53, which a number cannot hold exactly: give it as a bigint'));
    await assert.rejects(tab.queryRun(MODEL, query, 'w::RT', { n: 2n ** 63n }), (e: unknown) => e instanceof ParameterRefusal
      && e.message.includes('9223372036854775808n is beyond the 64-bit range an Integer holds'));
    assert.deepEqual(seen.received, [], 'nothing sent for a refused run');
  });
});

describe("a run's parameter values checked against the plan's declarations, as Java's PlanParameters checks them", () => {
  const declared = (name: string, type: string, lower: number, upper?: number, enumValues: readonly string[] = []) =>
    ({ name, type, multiplicity: upper === undefined ? { lower } : { lower, upper }, enumValues });

  it('converts each value for binding: a date-time to its text at the nanosecond, a list, an optional absence', () => {
    const checked = checkParameters([
      declared('at', 'DateTime', 1, 1), declared('day', 'StrictDate', 1, 1), declared('d', 'Decimal', 1, 1),
      declared('ids', 'Integer', 0), declared('maybe', 'String', 0, 1), declared('c', 'p::Colour', 1, 1, ['RED', 'GREEN']),
    ], { at: '2024-06-01T09:30:00', day: '2024-06-01', d: '1.25', ids: [1, 2], c: 'RED' });
    assert.deepEqual(Object.fromEntries(checked), {
      at: { _type: 'one', value: '2024-06-01 09:30:00.000000000' }, day: { _type: 'one', value: '2024-06-01' },
      d: { _type: 'one', value: '1.25' }, ids: { _type: 'many', values: [1, 2] }, maybe: { _type: 'none' },
      c: { _type: 'one', value: 'RED' },
    });
  });

  it('names every problem at once: the missing first, then each value its parameter does not take', () => {
    assert.throws(() => checkParameters([declared('a', 'Integer', 1, 1), declared('b', 'p::Colour', 1, 1, ['RED']),
      declared('c', 'StrictDate', 1, 1), declared('d', 'Integer', 1, 1)], { b: 'BLUE', c: '2024-13', d: [1, 2] }),
    (e: unknown) => e instanceof ParameterRefusal && e.message === 'no value for the parameter(s) a:Integer[1]; values their'
      + ' parameters do not take: [parameter \'b\' (p::Colour[1]): "BLUE" is not a value of p::Colour [RED];'
      + ' parameter \'c\' (StrictDate[1]): given "2024-13" (string): a StrictDate is its text YYYY-MM-DD;'
      + ' parameter \'d\' (Integer[1]): takes one value, given a list of 2]');
  });
});

describe("a plan's loads, established on a database the host made for its target", () => {
  let loaded: number;
  /** A text query's tree, as lite API v1 reads one (query/parse), its JSON whole. */
  const parsed = (query: string): string => {
    const answered = ok(answer(m, { kind: 'queryParse', request: JSON.stringify({ text: query }) }));
    return answered.slice('{"query":'.length, -1);
  };
  /** A text query planned over the held model (query/parse, then query/plan held: lite API v1, no text entry). */
  const executionPlanOf = (query: string, runtime: string): PlanText => unplannedPlan(heldPlan(loaded, parsed(query), runtime));
  /** The same, asked as a host asks it (the two were one road's two entries until lite API v1). */
  const executionPlanOfLambda = executionPlanOf;
  /** A database of its own, as a host makes one for a target: a fresh DuckDB-WASM. */
  const fresh = async (): Promise<{ readonly engine: DuckDbEngine; readonly on: <T>(work: (s: PlanSession) => Promise<T>) => Promise<T> }> => {
    const db = await startNodeDuckDb(runfilesFromEnv('DUCKDB_EXTENSIONS'));
    const own = new DuckDbEngine(db.connect() as ArrowishConnection);
    return { engine: own, on: (work) => own.exclusive(async (held) => work(sessionOn(own.databaseType, held))) };
  };
  const tables = (e: DuckDbEngine): Promise<string> => asTest(e).exclusive(async (held) =>
    (await held.rows(unplanned("SELECT string_agg(table_name, ',' ORDER BY table_name) FROM duckdb_tables()"), [])).map((r) => String(r[0])).join(''));

  before(() => {
    loaded = Number(ok(answer(m, { kind: 'hold', model: LOADED })));
  });

  it("a connection's declared test data: established once, the query run on it", async () => {
    const plan = executionPlanOf('|#>{x::DB.T}#->select(~[ID, NAME])->sort(~ID->ascending())', 'x::RT');
    assert.ok(targetKey(plan) !== null, 'a target that loads is kept by its key');
    const db = await fresh();
    // on a session the host did not establish for it, refused by name: never run against tables no load made
    await assert.rejects(() => db.on((s) => runPlan(plan, s)), /runs on a database the host made for it and established/);
    await db.on((s) => establish(plan, s));
    assert.deepEqual(looseJson(await db.on((s) => runPlan(plan, s, { established: true }))), [{ ID: 1, NAME: 'a' }, { ID: 2, NAME: 'b' }]);
  });

  it("a content blob that is not the data planned is refused, naming it, and its load rolled back", async () => {
    const plan = looseJson(executionPlanOf('|#>{x::DB.T}#->select(~[ID, NAME])', 'x::RT')) as { loads: { content: Record<string, string> }[] };
    const [load] = plan.loads;
    const [digest] = Object.keys(load!.content);
    load!.content[digest!] = load!.content[digest!]!.replace("'a'", "'z'");
    const db = await fresh();
    await assert.rejects(() => db.on((s) => establish(unplannedPlan(JSON.stringify(plan)), s)),
      (e: unknown) => e instanceof PlanRefusal && /the data of the table T is not the data it was planned against/.test(e.message));
    assert.equal(await tables(db.engine), 'null', 'the load rolled back: no table made');
  });

  it("a data: source's payload is bound, not loaded: no load and no release, and this runner binds it from the plan", async () => {
    // the JSON pass (2026-11-11): DuckDB binds a data: payload as one parameter its statement splits -- no session
    // table, so no load makes one and no release drops it; the hosts' runner binds it from the statement's content by
    // its digest (the tenets audit's M3, 2026-11-11: until then refused by name)
    const plan = executionPlanOfLambda("|sourceUrl('data:application/json,[{\"ID\":1,\"NAME\":\"a\"},{\"ID\":2,\"NAME\":\"b\"}]')"
      + "->extend(~[ID: r|cast(get($r.data, 'ID'), @meta::pure::metamodel::type::Integer), NAME: r|cast(get($r.data, 'NAME'), @meta::pure::metamodel::type::String)])"
      + "->select(~[ID, NAME])->sort(~ID->ascending())->from(x::BareRT)", 'x::BareRT');
    const read = looseJson(plan) as { release?: unknown; loads?: readonly unknown[] };
    assert.equal(read.release, undefined, 'no plan carries a release');
    assert.equal((read.loads ?? []).length, 0, 'no load makes a session table');
    assert.equal(targetKey(plan), null, "it loads nothing: the host's own session serves it");
    const db = await fresh();
    assert.deepEqual(looseJson(await db.on((s) => runPlan(plan, s))), [{ ID: 1, NAME: 'a' }, { ID: 2, NAME: 'b' }]);
    // a payload that is not its digest is refused by name, nothing run
    const tampered = plan.replace('"NAME\\":\\"b\\"', '"NAME\\":\\"B\\"');
    assert.notEqual(tampered, plan);
    await assert.rejects(() => db.on((s) => runPlan(unplannedPlan(tampered), s)), /the data: source's payload is not the data it was planned against/);
  });

  it('a pivot whose columns the data names: its keys read first, its body written per key and run', async () => {
    const plan = executionPlanOfLambda("{|#TDS{\ndept, city, name\nA, NYC, x\nA, SAN, y\nB, NYC, z\n}#"
      + "->pivot(~[city], ~[names:r|$r.name:y|$y->joinStrings(',')])->sort(~dept->ascending())->from(x::BareRT)}", 'x::BareRT');
    assert.match(plan, /"_type":"keyed"/);
    assert.equal(targetKey(plan), null, "it loads nothing: the host's own session serves it");
    // each key's column named as the plan's keys statement names it, bare (D1.4, 2026-11-15; Java's runner reads the
    // same name)
    assert.deepEqual(looseJson(await run(plan)), [
      { dept: 'A', 'NYC__|__names': 'x', 'SAN__|__names': 'y' },
      { dept: 'B', 'NYC__|__names': 'z', 'SAN__|__names': null },
    ]);
    // H8: exactly the plan's statements, as the database itself received them (the unfiltered witness, H13) -- its
    // target's session, then its keys statement and its body written for the keys read (expandKeyed, the one form a
    // statement is written at run: test/fixtures/keyed-expansion.json holds it to Java's and Python's), in its
    // snapshot's transaction
    const keyed = (looseJson(plan).root) as { keys: { statement: string }; body: Parameters<typeof expandKeyed>[0] };
    const keyRows = await inRun('a test', () => asTest(engine).exclusive((held) => held.rows(unplanned(keyed.keys.statement), [])));
    const { engine: own, received } = witnessed(database.connect() as ArrowishConnection);
    await inRun('a test', () => own.exclusive(async (held) => runPlan(plan, sessionOn(own.databaseType, held))));
    const { begin, commit } = transactionOf(looseJson(plan));
    assert.deepEqual(received, [...sessionOf(looseJson(plan)), ...begin, keyed.keys.statement,
      expandKeyed(keyed.body, keyRows.map((r) => r.map(String))).statement, ...commit]);
  });
});

// THE LOAD RUNNERS' CONFORMANCE (B3, 2026-11-11): runLoad and Python's projects.run_load run one fixture -- the same
// plan's load, the same rows, the same tampered blob refused in the same words (python/tests/test_engine.py)
describe("a remote file's mount, a planned load whose bearer URL the host binds on each connection (B3's Q3)", () => {
  it('binds the held URL by name, keeps the setting, and a second connection reads the view once it is replayed', async () => {
    const db = await startNodeDuckDb(runfilesFromEnv('DUCKDB_EXTENSIONS'));
    db.registerFileText('held.csv', 'id,name\n1,a\n2,b\n');
    const first = new DuckDbEngine(db.connect() as ArrowishConnection);
    // the planner is asked names alone: the bearer URL by the name the host holds it under, its address shown
    const plan = ok(answer(m, { kind: 'remoteMount', request: JSON.stringify({
      databaseType: 'DuckDB', view: 'remote_t', location: { bearer: 'datacube_url_t', shown: 'https://h/t.csv' }, format: 'csv',
    }) }));
    assert.ok(!plan.includes('held.csv') && plan.includes('"_type":"held","name":"datacube_url_t","field":"URL"'), plan);
    // the host's store: the URL it holds (a registered file here: a Node test makes no HTTP request)
    const store = new SecretStore();
    store.hold('datacube_url_t', new Map([['URL', 'held.csv']]));
    const route = (e: DuckDbEngine) => (sql: string, params: readonly (string | number | bigint | boolean | null)[]): Promise<unknown> =>
      (params.length === 0 ? e.run(unplanned(sql), 0) : e.exclusive((held) => held.rows(unplanned(sql), params)));
    const { settings } = await runLoad(unplannedPlan(plan), route(first), { secrets: store });
    const count = (e: DuckDbEngine): Promise<string> => asTest(e).exclusive(async (held) => String((await held.rows(unplanned('SELECT count(*) FROM remote_t'), []))[0]![0]));
    assert.equal(await count(first), '2');
    const viewText = await inRun('a test', () => asTest(first).exclusive(async (held) => String((await held.rows(unplanned("SELECT sql FROM duckdb_views() WHERE view_name = 'remote_t'"), []))[0]![0])));
    assert.ok(!viewText.includes('held.csv') && viewText.includes('getvariable'), viewText);
    // a second connection to the same database: the variable is the first's, so it reads nothing until the setting is
    // replayed on it, bound again from the store
    const second = new DuckDbEngine(db.connect() as ArrowishConnection);
    await assert.rejects(() => count(second), /NULL/);
    const kept = new ConnectionSettings();
    kept.keep('remote_t', settings, store);
    await inRun('a test', () => kept.replay(route(second)));
    assert.equal(await count(second), '2');
    // a bound statement this run was given no store for is refused by name
    await assert.rejects(() => runLoad(unplannedPlan(plan), route(first)), /binds what a host holds, and this run was given no host's store/);
  });
});

// the B checkpoint's tenets audit, M2 (2026-11-11): a database repeats a bound value in its own error -- an expired
// presigned link's read names the whole URL -- so the engine says a failure with every value its kept settings bind
// taken out, and without the database's own error, which still holds it
describe("a failure's words, the values a host holds taken out at the engine's QueryError boundary (M2)", () => {
  const SIGNATURE = 'SENTINEL0signature0123456789abcdef';
  const CREDENTIAL = 'SENTINEL-AKIA-credential%2F20261111';
  const URL = `https://bucket.example/t.csv?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=${CREDENTIAL}`
    + `&X-Amz-Expires=3600&X-Amz-Signature=${SIGNATURE}`;

  it('a held URL is taken out whole, as its query string and as each parameter long enough to be a token', () => {
    const texts = heldTexts(URL);
    for (const t of [URL, URL.slice(URL.indexOf('?') + 1), SIGNATURE, CREDENTIAL, decodeURIComponent(CREDENTIAL)]) {
      assert.ok(texts.includes(t), t);
    }
    // a short value (an expiry's seconds) is no token: the message keeps its own words and numbers
    assert.ok(!texts.includes('3600'), texts.join(' | '));
    assert.equal(redactHeld(`expired: ${URL}; signed ${SIGNATURE}; by ${decodeURIComponent(CREDENTIAL)} for 3600 s`, [URL]),
      'expired: ***; signed ***; by *** for 3600 s');
  });

  it("the engine's failure, run as a query or on the held connection, names no held value and carries no raw error", async () => {
    const db = await startNodeDuckDb(runfilesFromEnv('DUCKDB_EXTENSIONS'));
    const store = new SecretStore();
    store.hold('datacube_url_t', new Map([['URL', URL]]));
    const settings = new ConnectionSettings();
    settings.keep('remote_t', [{ sql: unplanned('SET VARIABLE lite_url_t = ?'), binds: [{ _type: 'held', name: 'datacube_url_t', field: 'URL' }] }], store);
    const engine = new DuckDbEngine(db.connect() as ArrowishConnection, { settings });
    // DuckDB's own error repeating the URL, its query string and one parameter, as a failed read's does
    const failing = `SELECT error('HTTP 403 reading "${URL}" (${URL.slice(URL.indexOf('?') + 1)}; ${SIGNATURE})')`;
    const said = (e: unknown): boolean => {
      assert.ok(e instanceof QueryError, String(e));
      assert.ok(!e.message.includes('SENTINEL'), e.message);
      assert.match(e.message, /HTTP 403 reading/);
      assert.equal(e.cause, undefined, 'the raw error, which holds the URL, is not carried');
      return true;
    };
    await assert.rejects(() => asTest(engine).run(unplanned(failing), 0), said);
    await assert.rejects(() => asTest(engine).exclusive((held) => held.rows(unplanned(failing), [])), said);
    // a failure that names no held value is said as it is, its cause kept
    await assert.rejects(() => asTest(engine).run(unplanned("SELECT error('plain')"), 0), (e: unknown) =>
      e instanceof QueryError && /plain/.test(e.message) && e.cause !== undefined);
  });
});

// the tenets audit's M3 (2026-11-11, the orchestrator's go; the user's ruling "make json work everywhere"): a JSON
// connection's frame is a data: source whose payload DuckDB binds as one parameter (the JSON pass); the tab reads the
// payload from the plan it is given beside the text, by digest, verified once, and binds it -- the text keeps its `?`
describe("a JSON connection's query in the tab: its payload bound from the plan, its records read back exactly (M3)", () => {
  // a quote, a backslash, a line break, in an order no sort would give
  const RECORDS = [{ name: "it's" }, { name: 'back\\slash' }, { name: 'two\nlines' }, { name: 'a' }];
  const jsonModel = (records: readonly unknown[]): string => {
    // the payload as JSON, written into the Pure string literal of its data: URL ('\' and '\'' escaped)
    const url = `data:application/json,${JSON.stringify(records)}`.replace(/\\/g, '\\\\').replace(/'/g, "\\'");
    return `Class m::Raw { name: String[1]; }
Class m::P { name: String[1]; }
###Mapping
Mapping m::MM ( *m::P: Pure { ~src m::Raw name: $src.name } )
###Runtime
Runtime m::RT { mappings: [m::MM]; connections: [ ModelStore: [ json: #{ JsonModelConnection {
    class: m::Raw; url: '${url}'; } }# ] ]; }
`;
  };
  const QUERY = '|m::P.all()->project(~[name: p|$p.name])';
  /** query/plan (lite API v1, unheld): its statement, its columns, and its plan (which carries the payload). */
  const planOf = async (model: string): Promise<{ sql: string; columns: PlanColumn[]; plan: { root: { sql: { content: Record<string, string> } } } }> => {
    const plan = looseJson(planJsonOf(ok(answer(m, { kind: 'queryPlan', request: `{"model":${JSON.stringify(model)},"query":${toJson(await grammar.lambda(QUERY))},"runtime":"m::RT"}` })))) as {
      root: { columns: PlanColumn[]; sql: { statement: string; content: Record<string, string> } } };
    return { sql: plan.root.sql.statement, columns: plan.root.columns.map((c) => ({ name: c.name, type: c.type })), plan };
  };

  it('the text plan binds the payload: read from its content by digest, bound, its records back in payload order', async () => {
    const body = await planOf(jsonModel(RECORDS));
    assert.ok(body.sql.includes('?') && !body.sql.includes('back') && !body.sql.includes('data:'), body.sql);
    const params = planParams(body.plan);
    assert.deepEqual(params.map((p) => looseJson(p) as unknown), [RECORDS]);
    const answered = await inRun('a test', () => asTest(engine).execute({ sql: unplanned(body.sql), columns: body.columns, params }, 0));
    assert.deepEqual(answered.columns[0]!.values, RECORDS.map((r) => r.name));
    // an empty array: no record, no row
    const empty = await planOf(jsonModel([]));
    const none = await inRun('a test', () => asTest(engine).execute({ sql: unplanned(empty.sql), columns: empty.columns, params: planParams(empty.plan) }, 0));
    assert.equal(none.rowCount, 0);
  });

  it("a payload that is not its digest is refused by name, nothing run", async () => {
    const body = await planOf(jsonModel(RECORDS));
    const content = body.plan!.root.sql.content;
    const [digest] = Object.keys(content);
    content[digest!] = content[digest!]!.replace('back', 'BACK');
    assert.throws(() => planParams(body.plan), (e: unknown) => e instanceof PlanParamsRefusal
      && /the data: source's payload is not the data it was planned against/.test(e.message));
  });

  it("the tab's pure/v1 execute (BrowserEngine) binds it too: the answer's rows are the records", async () => {
    const browser = new BrowserEngine(new WasmEdgeGrammar(await loadPlannerPort(), await loadEnginePort()), engine, 'tester');
    const text = await browser.executeText({
      clientVersion: 'vX_X_X', function: await grammar.lambda(QUERY),
      model: { _type: 'text', code: jsonModel(RECORDS) } as never, runtime: { runtime: 'm::RT' },
      context: { _type: 'BaseExecutionContext' },
    });
    const answered = looseJson(text) as { result: { rows: { values: unknown[] }[] }; activities: { sql: string }[] };
    assert.deepEqual(answered.result.rows.map((r) => r.values[0]), RECORDS.map((r) => r.name));
    // the activity's SQL is the statement's text: its `?`, never the payload
    assert.ok(!JSON.stringify(answered.activities).includes('back'), JSON.stringify(answered.activities));
  });
});

// THE KEYED EXPANSION'S ONE FIXTURE (the C final audit's H8, 2026-11-12): the tab's expandKeyed writes each case as
// Java's PlanRunner.expand and Python's _keyed_text do (KeyedExpansionConformanceTest, test_planned.py)
describe("the keyed expansion's one fixture", () => {
  interface Case {
    name: string; body: Parameters<typeof expandKeyed>[0]; keys: string[][];
    statement?: string; params?: string[]; refused?: string;
  }
  const cases = (looseJson(readFileSync(runfileFromEnv('KEYED_FIXTURE'), 'utf8')) as { cases: Case[] }).cases;

  it('writes every case as the fixture says, and refuses the one it refuses in its words', () => {
    assert.ok(cases.length > 3);
    for (const c of cases) {
      if (c.refused !== undefined) {
        assert.throws(() => expandKeyed(c.body, c.keys), (e: unknown) => e instanceof PlanRefusal && e.message === c.refused, c.name);
      } else {
        assert.deepEqual(expandKeyed(c.body, c.keys), { statement: c.statement, params: c.params }, c.name);
      }
    }
  });
});

describe("the load runners' shared conformance fixture", () => {
  interface Fixture {
    model: string; databaseType: string; check: string; rows: unknown[][]; tamper: { from: string; to: string }; refused: string;
    version: { given: number; refused: string };
  }
  const fixture = looseJson(readFileSync(runfileFromEnv('RUN_LOAD_FIXTURE'), 'utf8')) as Fixture;
  const planOf = (): { open: string[]; use: string[]; load: { root: { content: Record<string, string> } } } =>
    looseJson(ok(answer(m, { kind: 'projectData', model: fixture.model, databaseType: fixture.databaseType })));

  it('gives the rows the fixture names', async () => {
    const project = planOf();
    const db = await startNodeDuckDb(runfilesFromEnv('DUCKDB_EXTENSIONS'));
    const own = new DuckDbEngine(db.connect() as ArrowishConnection);
    for (const sql of [...project.open, ...project.use]) await inRun('a test', () => own.run(unplanned(sql), 0));
    await runLoad(unplannedPlan(JSON.stringify(project.load)), (sql) => own.run(sql, 0));
    const rows = await inRun('a test', () => own.exclusive((held) => held.rows(unplanned(fixture.check), [])));
    assert.deepEqual(rows.map((r) => r.map((v) => (typeof v === 'bigint' ? Number(v) : v))), fixture.rows);
  });

  it("hands the database exactly the load's statements, in order (the C final audit's H8)", async () => {
    const project = planOf();
    const db = await startNodeDuckDb(runfilesFromEnv('DUCKDB_EXTENSIONS'));
    // the witness at the database itself (H13): what it receives from the load's run alone
    const { engine: own, received, runs } = witnessed(db.connect() as ArrowishConnection);
    for (const sql of [...project.open, ...project.use]) await inRun('a test', () => own.run(unplanned(sql), 0));
    received.length = 0;
    runs.length = 0;
    await runLoad(unplannedPlan(JSON.stringify(project.load)), (sql) => own.run(sql, 0));
    assert.ok(received.length > 1, received.join('; '));
    assert.deepEqual(received, statementsOf(project.load.root));
    assert.deepEqual(runs, received.map(() => 'a load'), 'each received inside the load\'s run');
  });

  it('refuses the tampered blob in the fixture\'s words, nothing run', async () => {
    const project = planOf();
    const content = project.load.root.content;
    const [digest] = Object.keys(content);
    content[digest!] = content[digest!]!.replace(fixture.tamper.from, fixture.tamper.to);
    const sent: string[] = [];
    await assert.rejects(() => runLoad(unplannedPlan(JSON.stringify(project.load)), async (sql) => { sent.push(sql); }),
      (e: unknown) => e instanceof PlanRefusal && e.message === fixture.refused || assert.fail(String(e instanceof Error ? e.message : e)));
    assert.ok(!sent.some((s) => /INSERT/i.test(s)), sent.join('; '));
  });

  it("refuses a plan of another format version in the fixture's words, nothing run", async () => {
    const load = planOf().load as unknown as { version: number };
    const words = fixture.version.refused.replace('{VERSION}', String(load.version));
    load.version = fixture.version.given;
    const sent: string[] = [];
    await assert.rejects(() => runLoad(unplannedPlan(JSON.stringify(load)), async (sql) => { sent.push(sql); }),
      (e: unknown) => e instanceof PlanRefusal && e.message === words || assert.fail(String(e instanceof Error ? e.message : e)));
    assert.deepEqual(sent, []);
  });
});


// A SNAPSHOT FROM A REMOTE PLANE, A PLANNED LOAD WHOSE SOURCE IS A STREAM (the user's ruling of 2026-11-11, "This is the
// right plan"; no hash, the orchestrator's simplification of 2026-11-12): the planner names the plane, its statement and
// the table; the runner streams the plane's Arrow chunks in by DuckDB-WASM's own Arrow import in ONE transaction, kept
// only when the stream completed and the table holds the rows the plane counted -- else rolled back, nothing left
describe("a snapshot's stream, planned and run on DuckDB-WASM in one transaction", () => {
  // the request carries the PULL -- its model, runtime and query -- which the planner plans in the call (the user's
  // ruling of 2026-11-12, "that is a real off platform problem"): never the statement the plane runs
  const PULL = { model: MODEL, runtime: 'w::RT', lambda: toJson(lambda([], src())) } as const;
  const chunkOf = (from: number, rows: number): Uint8Array => arrowIpcStream(rows, {
    id: { integers: (i) => from + i },
    notional: { doubles: (i) => (from + i) / 4 },
    region: { text: ['EMEA', 'AMER', 'APAC'], pick: (i) => (from + i) % 3 },
  });
  /** A stand-in remote plane: its done step's receipt (the plane's count) before its chunks, as the warehouse's is. */
  const plane = (chunks: readonly Uint8Array[], options: { readonly counted?: number; readonly cutAfter?: number; readonly silent?: boolean } = {}) => ({
    asked: [] as string[],
    async *arrowChunks(sql: string, _signal?: AbortSignal, onReceipt?: (r: Receipt) => void): AsyncIterable<Uint8Array> {
      this.asked.push(sql);
      if (!options.silent) onReceipt?.({ plane: 'warehouse', where: 'a stand-in warehouse', statementId: 's-1', serverRows: options.counted ?? 7 });
      for (const [i, chunk] of chunks.entries()) {
        if (options.cutAfter !== undefined && i >= options.cutAfter) throw new Error('the connection was reset');
        yield chunk;
      }
    },
  });
  const planOf = (table: string, schema?: string): PlanText => unplannedPlan(ok(answer(m, { kind: 'snapStream', request: JSON.stringify({
    databaseType: 'DuckDB', plane: 'warehouse', ...PULL, ...(schema ? { schema } : {}), table }) })));
  const tableRows = (t: string): Promise<string> => asTest(engine).exclusive(async (held) =>
    (await held.rows(unplanned(`SELECT id, notional, region FROM ${t} ORDER BY id`), [])).map((r) => r.map(String).join(':')).join(' '));
  const columns = (t: string): Promise<string> => asTest(engine).exclusive(async (held) =>
    (await held.rows(unplanned(`SELECT column_name, data_type FROM duckdb_columns() WHERE table_name = '${t}' ORDER BY column_index`), []))
      .map((r) => r.join(' ')).join(', '));
  const exists = (t: string): Promise<number> => asTest(engine).exclusive(async (held) =>
    Number((await held.rows(unplanned(`SELECT count(*) FROM duckdb_tables() WHERE table_name = '${t}'`), []))[0]![0]));
  const snap = (plan: string, p: ReturnType<typeof plane>) => asTest(engine).exclusive((held) =>
    runLoad(unplannedPlan(plan), (sql, params) => held.rows(sql, params), { streams: { planes: { warehouse: p }, held } }));

  it('lands every chunk exactly, rows and types, the table counted by the database, the receipt the plane\'s', async () => {
    const plan = planOf('snapped', 'copies');
    // the plan names the plane, its statement and the table: never a row
    const step = (looseJson(plan) as { root: { steps: Record<string, unknown>[] } }).root.steps[0]!;
    assert.deepEqual(Object.keys(step).sort(), ['_type', 'count', 'plane', 'prepare', 'schema', 'sql', 'table', 'transaction']);
    const source = plane([chunkOf(1, 4), chunkOf(5, 3)]);
    const { landed } = await snap(plan, source);
    // the plane's statement is the planner's own plan of the pull, made in the call that answered the plan
    const pulled = looseJson(heldPlan(handle, PULL.lambda, PULL.runtime)) as { root: { sql: { statement: string } } };
    assert.equal(step['sql'], pulled.root.sql.statement);
    assert.deepEqual(source.asked, [step['sql']], 'the plane ran the plan\'s statement');
    assert.equal(landed.length, 1);
    assert.equal(landed[0]!.rows, 7);
    assert.equal(landed[0]!.receipt.statementId, 's-1');
    assert.equal(await columns('snapped'), 'id BIGINT, notional DOUBLE, region VARCHAR');
    assert.equal(await tableRows('copies.snapped'), '1:0.25:AMER 2:0.5:APAC 3:0.75:EMEA 4:1:AMER 5:1.25:APAC 6:1.5:EMEA 7:1.75:AMER');
  });

  it("hands the database exactly the stream's statements and its chunks, nothing else (the C final audit's H8, H13)", async () => {
    const plan = planOf('witnessed');
    const step = (looseJson(plan) as { root: { steps: { transaction: { begin: string[]; commit: string[] }; prepare: string[]; count: string }[] } })
      .root.steps[0]!;
    // the witness at the database itself: each statement and each Arrow import it is handed during the load's run
    const { engine: own, received } = witnessed(database.connect() as ArrowishConnection);
    await inRun('a test', () => own.exclusive((held) => runLoad(plan, (sql, params) => held.rows(sql, params),
      { streams: { planes: { warehouse: plane([chunkOf(1, 4), chunkOf(5, 3)]) }, held } })));
    assert.deepEqual(received, [...step.transaction.begin, ...step.prepare, '<an Arrow stream into witnessed>',
      '<an Arrow stream into witnessed>', step.count, ...step.transaction.commit]);
  });

  it('a stream cut short rolls back: no table is left, and one there before is as it was', async () => {
    // a fresh name: nothing of the stream is left
    await assert.rejects(() => snap(planOf('cut'), plane([chunkOf(1, 4), chunkOf(5, 3)], { cutAfter: 1 })), /the connection was reset/);
    assert.equal(await exists('cut'), 0);
    // a table of that name there before: its drop is the stream's transaction's, rolled back with it
    await inRun('a test', () => asTest(engine).run(unplanned("CREATE TABLE kept AS SELECT 1 AS id, CAST(0.5 AS DOUBLE) AS notional, 'EMEA' AS region"), 0));
    await assert.rejects(() => snap(planOf('kept'), plane([chunkOf(1, 4), chunkOf(5, 3)], { cutAfter: 1 })), /reset/);
    assert.equal(await tableRows('kept'), '1:0.5:EMEA');
  });

  it('a tampered stream -- fewer rows than the plane counted -- rolls back, refused by name', async () => {
    await assert.rejects(() => snap(planOf('tampered'), plane([chunkOf(1, 4), chunkOf(5, 2)])),
      (e: unknown) => e instanceof PlanRefusal && /landed 6 rows in tampered, where its plane counted 7: nothing of it is kept/.test(e.message));
    assert.equal(await exists('tampered'), 0);
    // a stream whose plane never said it was done is not kept either
    await assert.rejects(() => snap(planOf('unsaid'), plane([chunkOf(1, 7)], { silent: true })), /ended before its plane said it was done/);
    assert.equal(await exists('unsaid'), 0);
  });

  it('a request carrying a statement is refused by name: the planner plans the pull, never a host\'s text', () => {
    for (const given of [{ sql: 'DROP TABLE t' }, { ...PULL, sql: 'SELECT 1' }]) {
      assert.throws(() => ok(answer(m, { kind: 'snapStream', request: JSON.stringify({ databaseType: 'DuckDB', plane: 'warehouse',
        ...given, table: 't' }) })), /a snapshot's stream has no key sql/);
    }
  });

  it('a rollback that fails too is said with the failure, never swallowed', async () => {
    // G7 (the final audit's item 4, 2026-11-12): `.catch(() => {})` dropped the rollback's failure, the stream's said alone
    const failing = async (sql: string): Promise<unknown[][]> => {
      if (sql.toUpperCase().startsWith('ROLLBACK')) throw new Error('the rollback was refused');
      return [];
    };
    await assert.rejects(() => runLoad(planOf('unrolled'), failing,
      { streams: { planes: { warehouse: plane([chunkOf(1, 4)], { cutAfter: 0 }) }, held: { rows: failing, insertArrow: async () => {} } } }),
    (e: unknown) => e instanceof Error
      && e.message === 'the connection was reset -- and rolling its transaction back failed: the rollback was refused'
      && e.cause instanceof Error && e.cause.message === 'the connection was reset');
  });

  it('a plane this host does not hold, and a host that streams none, are refused by name, nothing run', async () => {
    const sent: string[] = [];
    const record = async (sql: string): Promise<unknown[][]> => { sent.push(sql); return []; };
    await assert.rejects(() => runLoad(planOf('t'), record, { streams: { planes: {}, held: { rows: record, insertArrow: async () => {} } } }),
      /'warehouse' is not a remote plane this host holds: nothing is streamed into t/);
    await assert.rejects(() => runLoad(planOf('t'), record), /streams the remote plane 'warehouse' into t, and this host streams no plane's rows/);
    assert.deepEqual(sent, []);
  });
});


// A SNAPSHOT'S LOCAL COPY (the final audit's item 1(b), 2026-11-12): the WASM planner answers the pull and a table with ONE
// plan of writes (Boundary.snapCopy) -- the table there before dropped, the table made from the pull -- in its target's
// transaction, which the hosts' runner runs (runPlan) and answers nothing; never the pull's statement handed back
describe("a snapshot's local copy, one plan of writes run in its target's transaction", () => {
  const PULL = { model: MODEL, runtime: 'w::RT', lambda: toJson(lambda([], src())) } as const;
  const copyOf = (table: string): PlanText => unplannedPlan(ok(answer(m, { kind: 'snapCopy', request: JSON.stringify({
    databaseType: 'DuckDB', ...PULL, table }) })));
  type Copy = { root: { steps: { _type: string; sql: { statement: string; target: { session: string[];
    transaction: { begin: string[]; commit: string[]; rollback: string[] } } } }[] } };
  const rowsIn = (t: string): Promise<string> => asTest(engine).exclusive(async (held) =>
    (await held.rows(unplanned(`SELECT id, name, amount FROM ${t} ORDER BY id`), [])).map((r) => r.map(String).join(':')).join(' '));

  it("hands the database exactly the plan's statements, in its transaction, and the copy holds the pull's rows", async () => {
    const plan = copyOf('copied');
    const { root } = looseJson(plan) as Copy;
    assert.deepEqual(root.steps.map((s) => s._type), ['effect', 'effect']);
    const t = root.steps[0]!.sql.target;
    // the witness at the database itself: every statement it is handed during the plan's run
    const { engine: own, received } = witnessed(database.connect() as ArrowishConnection);
    const said = await inRun('a test', () => own.exclusive((held) => runPlan(plan, { databaseType: 'DuckDB', run: (sql, params) => held.rows(sql, params) })));
    assert.equal(said, '[]', 'a plan of writes alone answers no row');
    assert.deepEqual(received, [...t.session, ...t.transaction.begin, ...root.steps.map((s) => s.sql.statement), ...t.transaction.commit]);
    assert.match(root.steps[1]!.sql.statement, /^CREATE TABLE "copied" AS SELECT/);
    assert.equal(await rowsIn('copied'), await rowsIn('SRC'));
  });

  it('a copy that fails after its drop rolls back by the plan\'s own statement: the table there before is as it was', async () => {
    await inRun('a test', () => asTest(engine).run(unplanned("CREATE TABLE kept_copy AS SELECT 9 AS id, 'z' AS name, CAST(9.5 AS DOUBLE) AS amount"), 0));
    const plan = copyOf('kept_copy');
    const sent: string[] = [];
    await assert.rejects(() => asTest(engine).exclusive((held) => runPlan(plan, { databaseType: 'DuckDB', run: async (sql, params) => {
      sent.push(sql);
      if (/^CREATE TABLE/.test(sql)) throw new Error('the copy was refused');
      return held.rows(sql, params);
    } })), /the copy was refused/);
    assert.equal(sent.at(-1), 'ROLLBACK');
    assert.equal(await rowsIn('kept_copy'), '9:z:9.5');
  });

  it('a request carrying a statement is refused by name: the planner plans the pull, never a host\'s text', () => {
    assert.throws(() => ok(answer(m, { kind: 'snapCopy', request: JSON.stringify({ databaseType: 'DuckDB', ...PULL, sql: 'SELECT 1', table: 't' }) })),
      /a snapshot's copy has no key sql/);
  });
});


// ROWS THE DATABASE MAKES, A PLANNED LOAD (SYNTHESIZE's simple slice, the user's ruling of 2026-11-12, register S303):
// the WASM planner answers a declared table and a count with one HOST_SESSION load -- the table made as the model
// declares it, empty, then ONE INSERT ... SELECT over range(count) whose every value the database computes -- and the
// hosts' runner runs it on DuckDB-WASM; the plan holds no row
describe("a synthesized table, planned by the WASM planner and run on DuckDB-WASM", () => {
  const SYNTH = `###Relational
Database s::DB
(
    Schema demo
    (
        Table TRADES ( id INTEGER, region VARCHAR(8), book VARCHAR(16), notional DECIMAL(12,2), day DATE, live BIT )
    )
)
`;
  const KNOBS = { region: { values: ['EMEA', 'AMER', 'APAC'] }, notional: { range: [0, 9999.99], scale: 2 } };
  const request = (count: number, databaseType = 'DuckDB'): string => JSON.stringify({
    databaseType, model: SYNTH, database: 's::DB', schema: 'demo', table: 'TRADES', count, columns: KNOBS });
  // the test's own questions of the table the plan made (C3: typed as a plan's by name, unplanned.ts)
  const one = (sql: string): Promise<unknown[]> => asTest(engine).exclusive(async (held) => (await held.rows(unplanned(sql), []))[0]!);

  it('fills the declared table with the rows the database made, the plan holding none of them', async () => {
    const plan = ok(answer(m, { kind: 'synthesize', request: request(100_000) }));
    // the plan's size is the count's digits: "100000" against "10"
    assert.equal(plan.length, ok(answer(m, { kind: 'synthesize', request: request(10) })).length + 4);
    const steps = (looseJson(plan) as { root: { cadence: string; steps: { _type: string }[] } }).root;
    assert.equal(steps.cadence, 'HOST_SESSION');
    assert.ok(steps.steps.every((s) => s._type === 'statement'), JSON.stringify(steps.steps.map((s) => s._type)));
    await inRun('a test', () => asTest(engine).exclusive((held) => runLoad(unplannedPlan(plan), (sql, params) => held.rows(sql, params))));
    assert.deepEqual((await one('SELECT count(*), count(DISTINCT id), CAST(min(day) AS VARCHAR), CAST(max(day) AS VARCHAR) FROM demo.TRADES')).map(String),
      ['100000', '10000', '2021-01-01', '2025-12-31']);
    assert.deepEqual((await one("SELECT count(*) FILTER (WHERE region = 'EMEA'), count(*) FILTER (WHERE region = 'AMER'),"
      + " count(*) FILTER (WHERE region = 'APAC'), count(DISTINCT book), count(*) FILTER (WHERE live),"
      + ' min(notional) >= 0, max(notional) <= 9999.99, count(*) FILTER (WHERE id IS NULL OR book IS NULL) FROM demo.TRADES')).map(String),
      ['33334', '33333', '33333', '5', '50005', 'true', 'true', '0']);
    // planned again, run again: the table made afresh, its rows the same
    await inRun('a test', () => asTest(engine).exclusive((held) => runLoad(unplannedPlan(plan), (sql, params) => held.rows(sql, params))));
    assert.equal(String((await one('SELECT count(*) FROM demo.TRADES'))[0]), '100000');
  });

  it("a load run in one transaction opens it with its plan's own statements, and a failure rolls it back (the re-audit's S8)", async () => {
    const ten = unplannedPlan(ok(answer(m, { kind: 'synthesize', request: request(10) })));
    const { begin, commit } = transactionOf(looseJson(ten));
    const sent: string[] = [];
    const recorded = (held: { rows: Parameters<typeof runLoad>[1] }): Parameters<typeof runLoad>[1] => (sql, params) => {
      sent.push(sql);
      return held.rows(sql, params);
    };
    await inRun('a test', () => asTest(engine).exclusive((held) => inLoadsTransaction(ten, recorded(held), () => runLoad(ten, recorded(held)))));
    assert.deepEqual([sent.slice(0, begin.length), sent.slice(-commit.length)], [begin, commit], "the plan's transaction around its load");
    assert.equal(String((await one('SELECT count(*) FROM demo.TRADES'))[0]), '10');
    // a failure after the load ran: rolled back by the plan's own statement, the table as it was
    const twenty = unplannedPlan(ok(answer(m, { kind: 'synthesize', request: request(20) })));
    sent.length = 0;
    await assert.rejects(() => asTest(engine).exclusive((held) => inLoadsTransaction(twenty, recorded(held), async () => {
      await runLoad(twenty, recorded(held));
      throw new Error('refused after the load');
    })), /refused after the load/);
    assert.deepEqual(sent.at(-1), 'ROLLBACK');
    assert.equal(String((await one('SELECT count(*) FROM demo.TRADES'))[0]), '10');
    // a load whose target carries no transaction is refused by name, nothing sent
    const bare = looseJson(ten) as { root: { target: Record<string, unknown> } };
    delete bare.root.target['transaction'];
    sent.length = 0;
    await assert.rejects(() => asTest(engine).exclusive((held) => inLoadsTransaction(unplannedPlan(JSON.stringify(bare)), recorded(held), async () => undefined)),
      (e: unknown) => e instanceof PlanRefusal && /carries its target's transaction, and this one has none/.test(e.message));
    assert.deepEqual(sent, []);
  });

  it('any other database is refused by name, planning nothing', () => {
    assert.throws(() => ok(answer(m, { kind: 'synthesize', request: request(10, 'Postgres') })), /Synthesize on/);
  });

  it('a count that is not a whole number, and a scale beside values, are refused by name, never read past', () => {
    assert.throws(() => ok(answer(m, { kind: 'synthesize', request: request(2.9) })), /a whole number of rows, not 2.9/);
    const scaled = JSON.stringify({ databaseType: 'DuckDB', model: SYNTH, database: 's::DB', schema: 'demo',
      table: 'TRADES', count: 10, columns: { region: { values: ['EMEA'], scale: 2 } } });
    assert.throws(() => ok(answer(m, { kind: 'synthesize', request: scaled })), /scale is its range's, and region gives values/);
  });
});


// A STATEMENT A PERSON WROTE, AS A PLAN (Studio's SQL playground; the user's ruling of 2026-11-12, "Yes do 5 and 6"):
// the WASM planner carries the person's one text as written -- never parsed, rewritten or split -- as a plan's one
// statement, and the hosts' runner sends exactly that statement on DuckDB-WASM
describe("a person's own statement, carried by the WASM planner as written and run on DuckDB-WASM", () => {
  const authored = (text: string): PlanText => unplannedPlan(ok(answer(m, { kind: 'authored', request: JSON.stringify({ databaseType: 'DuckDB', text }) })));

  it('the plan carries the text exactly, and the runner sends that one statement, its answer the database\'s', async () => {
    const text = "CREATE OR REPLACE TABLE playground_t AS SELECT 1 AS n  -- a person's own; as typed\n";
    const plan = authored(text);
    const root = (looseJson(plan) as { root: { _type: string; shape: string; columns: unknown[]; sql: { statement: string; slots: unknown[] } } }).root;
    assert.deepEqual([root._type, root.shape, root.columns, root.sql.statement, root.sql.slots], ['tdsResult', 'RELATION', [], text, []]);
    const sent: string[] = [];
    const recording = { databaseType: 'DuckDB', run: async (sql: PlannedSql) => { sent.push(sql); return asTest(engine).run(sql, 0); } };
    await runAuthored(plan, recording, { established: true });
    assert.deepEqual(sent, [text], 'the one statement, as the person wrote it');
    const read = await runAuthored(authored('SELECT n, n + 1 AS m FROM playground_t'), { databaseType: 'DuckDB', run: (sql) => asTest(engine).run(sql, 0) });
    assert.deepEqual(read.columns.map((c) => [c.name, c.values.map(String)]), [['n', ['1']], ['m', ['2']]]);
  });

  it("a text the database cannot take is the database's own error; an empty one is refused by name", async () => {
    await assert.rejects(() => runAuthored(authored('SELEC 1'), { databaseType: 'DuckDB', run: (sql) => asTest(engine).run(sql, 0) }), /syntax error/i);
    assert.throws(() => authored('   '), /an authored statement is the person's text, and this one is empty/);
  });

  it("is run as every plan is: its target's session first, its database's type checked, its columns required (the C final audit's H9)", async () => {
    const plan = authored('SELECT 1 AS one');
    const target = (looseJson(plan) as { root: { sql: { target: { session: string[] } } } }).root.sql.target;
    assert.ok(target.session.length > 0, 'a DuckDB target sets its session up (Boundary.sessionSetup)');
    const sent: string[] = [];
    await runAuthored(plan, { databaseType: 'DuckDB', run: async (sql: PlannedSql) => { sent.push(sql); return asTest(engine).run(sql, 0); } });
    assert.deepEqual(sent, [...target.session, 'SELECT 1 AS one'], "the target's session statements, then the person's text");
    const none: string[] = [];
    await assert.rejects(() => runAuthored(plan, { databaseType: 'Postgres', run: async (sql: PlannedSql) => { none.push(sql); } }),
      (e: unknown) => e instanceof PlanRefusal && /a plan of a DuckDB database is not run on this Postgres one/.test(e.message));
    const unstated = looseJson(plan) as { root: { columns?: unknown } };
    delete unstated.root.columns;
    await assert.rejects(() => runAuthored(unplannedPlan(JSON.stringify(unstated)), { databaseType: 'DuckDB', run: async (sql: PlannedSql) => { none.push(sql); } }),
      (e: unknown) => e instanceof PlanRefusal && /carries its result's columns, and this one has none/.test(e.message));
    assert.deepEqual(none, [], 'nothing sent for a plan refused');
  });

  it('a plan that is not an authored statement is refused by name, nothing sent', async () => {
    const sent: string[] = [];
    const load = ok(answer(m, { kind: 'snapStream', request: JSON.stringify({ databaseType: 'DuckDB', plane: 'warehouse',
      model: MODEL, runtime: 'w::RT', lambda: toJson(lambda([], src())), table: 't' }) }));
    await assert.rejects(() => runAuthored(unplannedPlan(load), { databaseType: 'DuckDB', run: async (sql: PlannedSql) => { sent.push(sql); } }), /an authored statement's plan answers the relation its database reports, not a load/);
    assert.deepEqual(sent, []);
  });
});

// G7 (the re-audit's S7, 2026-11-12): what the planner always writes is required, never defaulted
describe("a statement's slots and a payload's digest, required", () => {
  it('refuses a statement with no slots, a payload with no digest, and one whose statement carries no content, by name', () => {
    const refused = (statement: Parameters<typeof statementParams>[0], words: RegExp): void =>
      assert.throws(() => statementParams(statement), (e: unknown) => e instanceof PlanParamsRefusal && words.test(e.message));
    refused({}, /this plan's statement does not carry its slots/);
    // no digest, and no content: either way the payload is not carried
    refused({ slots: [{ parameter: 'p', binding: { _type: 'payload' } }], content: {} }, /'p' binds a payload its statement does not carry/);
    refused({ slots: [{ parameter: 'p', binding: { _type: 'payload', digest: 'ab' } }] }, /'p' binds a payload its statement does not carry/);
    assert.deepEqual(statementParams({ slots: [] }), []);
  });
});

// a product raise in lite's words (#104's M2(b), 2026-11-15; phase D's Phase 2, raises as SITES, 2026-11-16): the
// statement raises a bare token LG#<n>, read through the sites its plan declares for its text (declared as the plan
// is read), its words planned -- at the tab's ONE send (DuckDbEngine's failure), so every route says the same words;
// the envelope's cases one fixture Java's and Python's readers read too
describe('a raise, worded', () => {
  it('reads each envelope of the shared fixture as Java and Python read it', () => {
    const fixture = looseJson(readFileSync(runfileFromEnv('RAISE_FIXTURE'), 'utf8')) as {
      statement: string; sites: Site[];
      cases: { name: string; message: string; words?: string; kind?: string; where?: string; refused?: boolean }[] };
    // the sites are the statement's (PlanJson v56): declared first, as a plan read does
    declareStatementSites(fixture.statement, fixture.sites, 'the fixture');
    const cases = fixture.cases;
    assert.ok(cases.length > 5);
    for (const c of cases) {
      const e = failure(c.message, fixture.statement, {});
      assert.ok(e instanceof QueryError, c.name);
      if (c.refused === true) {
        assert.ok(!(e instanceof PlanRaise), c.name);
        assert.match(e.message, /which the statement does not declare/, c.name);
        continue;
      }
      assert.equal(e.message, c.words, c.name);
      assert.equal(e instanceof PlanRaise || e instanceof DatabaseError ? e.kind : undefined, c.kind, c.name);
      assert.equal(e instanceof PlanRaise ? e.where : undefined, c.where, c.name);
    }
  });

  it('refuses a token its statement does not declare by name, the database error kept as its cause', () => {
    const db = new Error('Invalid Input Error: \u001f\u001cLG#1\u001f');
    const e = failure(db.message, 'SELECT a statement no plan declared', { cause: db });
    assert.ok(e instanceof QueryError && !(e instanceof PlanRaise));
    assert.match(e.message, /LG#1, which the statement does not declare/);
    assert.equal(e.cause, db);
  });

  it('words a raise at the send, on every route: a statement run, and a plan run', async () => {
    // a planned query dividing by zero, run by runPlan over the engine's held session: worded from its plan's raises
    const plan = planned(fn('divide', lit.integer(1), lit.integer(0)));
    // the kinds its statements can send, no words (PlanJson v59, stage B's B1): the host words each from its table
    assert.deepEqual((JSON.parse(plan) as { raises: unknown }).raises, ['DIVIDE_BY_ZERO']);
    await assert.rejects(() => inRun('a test', () => engine.exclusive((held) => runPlan(plan, sessionOn('DuckDB', held)))),
      (e: unknown) => e instanceof PlanRaise && e.message === 'Division by zero');
    // the envelope a product statement raises, sent as the database's own error() writes it: read through the sites
    // the plans the tab read declared for that text
    const sent = "SELECT error(chr(31) || chr(28) || 'LG#1' || chr(31))";
    declareStatementSites(sent, [{ n: 1, kind: 'DIVIDE_BY_ZERO', where: null }], 'a test');
    await assert.rejects(() => inRun('a test', () => asTest(engine).run(unplanned(sent), 0)),
      (e: unknown) => e instanceof PlanRaise && e.message === 'Division by zero' && e.kind === 'DIVIDE_BY_ZERO');
  });
});
