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

  it('refuses by name a plan it does not read or a database it is not on, nothing run', async () => {
    // what a host's runner runs is decided at planning (HostPlans.program, D6: core's HostPlansTest pins each refusal);
    // the runner reads the plan's format and version, refuses a step it does not know, and runs on its own database only
    const plan = JSON.parse(planned(fn('write', src(), target()))) as Record<string, unknown>;
    const session: PlanSession = { databaseType: 'Postgres', run: async () => { throw new Error('ran'); }, transaction: async () => { throw new Error('ran'); } };
    await assert.rejects(() => runPlan(JSON.stringify(plan), session), (e: unknown) => e instanceof PlanRefusal && /not run on this Postgres one/.test(e.message));
    await assert.rejects(() => runPlan('{"format":"other"}', session), /not a legend-lite execution plan/);
    await assert.rejects(() => runPlan(JSON.stringify({ ...plan, version: 5 }), session), /not a legend-lite execution plan of version 20/);
    const here: PlanSession = { ...session, databaseType: 'DuckDB' };
    await assert.rejects(() => runPlan(JSON.stringify({ ...plan, root: { _type: 'keyed' } }), here), /root is a sequence or a typed result, not a keyed/);
    const root = plan.root as { steps: Record<string, unknown>[] };
    await assert.rejects(() => runPlan(JSON.stringify({ ...plan, root: { ...root, steps: [{ _type: 'effect' }, ...root.steps] } }), here),
      /an update, a check or a typed result, not a effect/);
  });
});

describe('every plan door a host asks answers the one plan record (R2: docs/HOST_EXECUTORS_PROGRAM_2026_11_02.md, D11)', () => {
  const query = '|#>{w::DB.SRC}#->select(~[id, amount])->from(w::RT)';
  const record = (tagged: string): { format: string; version: number; root: { _type: string; sql: { statement: string } } } =>
    JSON.parse(ok(tagged)) as { format: string; version: number; root: { _type: string; sql: { statement: string } } };

  it('a query\'s rows, planned over a model held or given as text, as Pure text or a tree; a write; an execute', async () => {
    const tree = toJson(await grammar.lambda(query));
    const body = JSON.stringify({ clientVersion: 'vX_X_X', function: JSON.parse(tree), model: { _type: 'text', code: MODEL },
      context: { _type: 'BaseExecutionContext' } });
    const doors: Record<string, string> = {
      plan: answer(m, { kind: 'plan', handle, query, runtime: 'w::RT' }),
      planJson: answer(m, { kind: 'planJson', handle, lambda: tree, runtime: 'w::RT' }),
      planModel: answer(m, { kind: 'planModel', model: MODEL, lambda: tree, runtime: 'w::RT' }),
      executionPlan: answer(m, { kind: 'executionPlan', handle, lambda: toJson(lambda([], fn('write', src(), target()))), runtime: 'w::RT' }),
      executePlan: answer(m, { kind: 'executePlan', body, models: JSON.stringify([MODEL]) }).replace(/^OK\n200\n[^\n]*\n/, 'OK\n'),
    };
    for (const [door, tagged] of Object.entries(doors)) {
      const read = record(tagged);
      assert.deepEqual([read.format, read.version], ['legend-lite-plan', 20], door);
      assert.match(read.root._type, door === 'executionPlan' ? /^sequence$/ : /^tdsResult$/, door);
    }
  });

  it('the census: every export of the planner whose name says it plans is one of these, each answering the record or named here', () => {
    // a new plan door fails here until it is listed: the record (the doors above), or why it answers something else
    const doors: Record<string, string> = {
      planOrError: 'the record (the plan door)',
      planHeldOrError: 'the record (the plan door)',
      planJsonOrError: 'the record (the planModel door)',
      planJsonHeldOrError: 'the record (the planJson door)',
      executionPlanJsonHeldOrError: 'the record (the executionPlan door)',
      executeJsonPlanOrError: 'the record (the executePlan door)',
      plan: 'the SQL alone, shown (Boundary.planSql: a Pure text query\'s statement)',
    };
    // the module's export object keeps its names off its own enumerable keys: every name along its chain
    const names = new Set<string>();
    for (let o: object | null = m.exports as object; o !== null && o !== Object.prototype; o = Object.getPrototypeOf(o) as object | null) {
      for (const name of Object.getOwnPropertyNames(o)) names.add(name);
    }
    const planning = [...names].filter((name) => /plan/i.test(name)).sort();
    assert.deepEqual(planning, Object.keys(doors).sort());
  });

  it('the grid\'s doors refuse what a host does not run, by name; Show SQL shows the planner\'s statement, as before R2', async () => {
    // a plain query: Show SQL's statement is the plan record's
    const tree = toJson(await grammar.lambda(query));
    const shown = ok(answer(m, { kind: 'sqlModel', model: MODEL, lambda: tree, runtime: 'w::RT' }));
    assert.equal(shown, record(answer(m, { kind: 'planModel', model: MODEL, lambda: tree, runtime: 'w::RT' })).root.sql.statement);
    // a pivot whose columns its data names: no grid's (refused at planning), and its SQL still shown
    const pivot = toJson(await grammar.lambda('|#>{w::DB.SRC}#->select(~[name, amount])->pivot(~[name], ~[t: x|$x.amount: y|$y->sum()])->from(w::RT)'));
    assert.match(answer(m, { kind: 'planModel', model: MODEL, lambda: pivot, runtime: 'w::RT' }), /^ERR\n[^\n]*\n.*Keyed/s);
    assert.match(ok(answer(m, { kind: 'sqlModel', model: MODEL, lambda: pivot, runtime: 'w::RT' })), /select/i);
    // a plan's write is no grid's: the grid's door refuses it at planning
    const write = answer(m, { kind: 'planJson', handle, lambda: toJson(lambda([], fn('write', src(), target()))), runtime: 'w::RT' });
    assert.match(write, /^ERR\n/);
  });
});
