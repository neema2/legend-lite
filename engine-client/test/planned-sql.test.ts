// THE ONE TEXT A HOST SENDS (engine-client/src/planned-sql.ts; the total wall's C3, 2026-11-12): the engines and the
// hosts' plan runner take a PlannedSql, and a host's hand-written string is not one. The `@ts-expect-error` lines are
// the proof: each is a send of a plain string, and the typecheck (//datacube:typecheck_test, which reads this file)
// fails if any of them compiles -- if the brand, or a send site's type, is ever loosened. Where the type is made is
// pinned by tools/guards/PlannedSqlTest.java; Python's twin is python/tests/test_planned.py.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import type { ArrowishConnection, DuckDbEngine, HeldConnection } from '../src/duckdb.ts';
import type { TabFiles } from '../src/duckdb-tab.ts';
import { runLoad, runPlan, targetKey, type PlanSession } from '../src/plan-runner.ts';
import type { AsyncDuckDB } from '../src/duckdb-wasm.ts';
import type { QueryEngine } from '../src/engine.ts';
import type { HostOperation, HostSource, SnapStreamRequest } from '../src/host-operations.ts';
import { allFromPlan, fromPlan, type PlannedSql } from '../src/planned-sql.ts';
import type { Plan } from '../src/relation-type.ts';
import { allUnplanned, unplanned } from './unplanned.ts';

/** Never called: each line is a send of a string no plan carries, which must not compile. */
export function handWrittenSqlDoesNotCompile(engine: DuckDbEngine, query: QueryEngine, held: HeldConnection,
  connection: ArrowishConnection): void {
  // @ts-expect-error a string is not a plan's statement
  const statement: PlannedSql = 'SELECT 1';
  // @ts-expect-error the tab's engine runs a plan's statement alone
  void engine.run('SELECT 1', 0);
  // @ts-expect-error every engine does
  void query.run('SELECT 1', 0);
  // @ts-expect-error a plan's own SQL is a plan's
  const plan: Plan = { sql: 'SELECT 1', columns: [] };
  // @ts-expect-error the connection held for a plan runs its statements alone
  void held.rows('DROP TABLE t', []);
  // @ts-expect-error and its tables
  void held.table('SELECT 1');
  // @ts-expect-error the driver's own connection, as the engine holds it
  void connection.query('SELECT 1');
  // a statement edited after it was read is a string again
  // @ts-expect-error concatenated, it is no plan's
  void engine.run(fromPlan('SELECT 1') + ' UNION ALL SELECT 2', 0);
  // a host operation's planner text is the planner's (the C final audit's H2, 2026-11-12): its source query,
  // @ts-expect-error a host's own query is not
  const source: HostSource = { query: 'SELECT 1' };
  const relation: HostOperation = {
    op: 'relation', kind: 'table', target: { name: 't' }, from: { relation: { name: 's' } },
    // @ts-expect-error its select list,
    selectList: '*',
  };
  const replaced: HostOperation = {
    op: 'relation', kind: 'table', target: { name: 't' }, from: { relation: { name: 's' } },
    // @ts-expect-error and each replacement's expression
    replacing: [{ column: 'a', sql: 'a' }],
  };
  // @ts-expect-error a snapshot's request carries the pull, never a statement (H3; the user's ruling of 2026-11-12)
  const snap: SnapStreamRequest = { databaseType: 'DuckDB', plane: 'warehouse', sql: 'DROP TABLE t', table: 't' };
  void statement;
  void plan;
  void [source, relation, replaced, snap];
}

/** Never called: a plan the host wrote is no plan (the re-audit's M1, 2026-11-12): the runners take what the planner
 *  answered (PlanText), so a hand-written plan -- its statements a host's own -- does not compile. */
export async function aHandWrittenPlanDoesNotRun(session: PlanSession): Promise<void> {
  const handWritten = '{"format":"legend-lite-plan","version":52,"parameters":[],"root":{"_type":"sequence","steps":[]}}';
  // @ts-expect-error a string is not a plan the planner answered
  await runPlan(handWritten, session);
  // @ts-expect-error nor for a load
  await runLoad(`{"format":"legend-lite-plan"}`, async () => []);
  // @ts-expect-error nor its target's key
  void targetKey(handWritten);
}

/** Never called: the tab's file registry is no database (duckdb-tab.ts TabFiles; the C final audit's H11, 2026-11-12):
 *  the raw database, whose connections run any text, is never handed out as one. */
export function theDatabaseIsNoFileRegistry(db: AsyncDuckDB): TabFiles {
  // @ts-expect-error the database itself is not the tab's file registry
  return db;
}

/** Never called: JSON is read as `unknown` (engine-client/src/json-unknown.ts; the C final audit's H5, 2026-11-12), so a
 *  parsed text is never a plan, nor anything holding a plan's statement, unread. */
export async function jsonIsReadAsUnknown(response: Response): Promise<void> {
  // @ts-expect-error a parsed text is unknown, never a plan
  const parsed: Plan = JSON.parse('{"sql":"DROP TABLE t","columns":[]}');
  // @ts-expect-error and a response's JSON is too
  const fetched: Plan = await response.json();
  // and a value found to be an array holds unknowns, never `any` (the re-audit's S6, 2026-11-12)
  const found: unknown = JSON.parse('["DROP TABLE t"]');
  if (Array.isArray(found)) {
    // @ts-expect-error an element of it is unknown
    const element: PlannedSql = found[0];
    void element;
  }
  void [parsed, fetched];
}

describe("a plan's statement", () => {
  it('is its text, typed and never edited', () => {
    assert.equal(fromPlan('SELECT 1'), 'SELECT 1');
    assert.deepEqual(allFromPlan(['a', 'b']), ['a', 'b']);
    assert.equal(unplanned('SELECT 2'), 'SELECT 2');
    assert.deepEqual(allUnplanned(['c']), ['c']);
  });
});
