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
import { hostSession, PlanRefusal, runPlan, type PlanSession } from '../src/plan-runner.ts';
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

  it('refuses by name what a host never plans, nothing run', async () => {
    const plan = JSON.parse(planned(fn('write', src(), target()))) as Record<string, unknown>;
    const session: PlanSession = { databaseType: 'Postgres', run: async () => { throw new Error('ran'); }, transaction: async () => { throw new Error('ran'); } };
    await assert.rejects(() => runPlan(JSON.stringify(plan), session), (e: unknown) => e instanceof PlanRefusal && /not run on this Postgres one/.test(e.message));
    await assert.rejects(() => runPlan(JSON.stringify({ ...plan, parameters: [{ name: 'p' }] }), session), /takes parameters/);
    await assert.rejects(() => runPlan('{"format":"other"}', session), /not a legend-lite execution plan/);
    // a plan of another version, and a statement only a dynamic pivot's probe plan writes (version 19): refused, never run
    await assert.rejects(() => runPlan(JSON.stringify({ ...plan, version: 5 }), session), /not a legend-lite execution plan of version 19/);
    const root = plan.root as { steps: { sql: Record<string, unknown> }[] };
    const regioned = { ...plan, root: { ...root, steps: root.steps.map((step) => ({ ...step, sql: { ...step.sql, regions: [{ start: 0 }] } })) } };
    const here: PlanSession = { ...session, databaseType: 'DuckDB' };
    await assert.rejects(() => runPlan(JSON.stringify(regioned), here), /per-key regions/);
  });
});
