// THE PLAYGROUND'S DOOR (Studio's SQL playground; the C5-T audit's MUST 2, 2026-11-15: out of
// studio/src/backend/run.ts, where the app asked the planner and sent the answer itself): the person's statement asked
// of the planner -- carried as written, a plan (Boundary.authored) -- outside any run, then run on the playground's
// database as the plan says (plan-runner.ts runAuthored, inside its own run frame).

import type { QueryEngine, RawTable } from '../engine.ts';
import { runAuthored } from '../plan-runner.ts';
import type { PlanText } from '../planned-sql.ts';

/** `statement`, as the person typed it, planned by `planner` and run on `database`'s engine. */
export async function runAuthoredStatement(
  statement: string,
  planner: { authored(text: string): Promise<PlanText> },
  database: { readonly engine: Pick<QueryEngine, 'run'>; readonly databaseType: string },
): Promise<RawTable> {
  const plan = await planner.authored(statement);
  return runAuthored(plan, { databaseType: database.databaseType, run: (sql) => database.engine.run(sql, 0) });
}
