// DATACUBE'S SMALL DOORS (C5-T, 2026-11-15: the TypeScript split of plan and run): each asks the planner outside any
// run, then runs its answer in a run frame (run-frame.ts: a run plans nothing). The grid's own door is runner.ts
// (localHost); these are the app's one-off questions, which until then planned and ran inline. A module of its own, so
// the screens that ask them (a join's summary counts) fetch it with themselves (the bundle budgets).

import type { QueryEngine, RawTable } from '../../engine-client/src/engine.ts';
import { inRun } from '../../engine-client/src/run-frame.ts';
import type { Lambda } from '../../pure-protocol/src/index.ts';
import type { Planner } from './planner-types.ts';

/** A query planned, then its plan's statement run once, its rows as the database answered them (untyped: a count). */
export async function planAndRunOnce(planner: Pick<Planner, 'plan'>, engine: Pick<QueryEngine, 'run'>, query: Lambda): Promise<RawTable> {
  const p = await planner.plan(query);
  return inRun('a query, once', () => engine.run(p.sql, 0, undefined, p.params));
}
