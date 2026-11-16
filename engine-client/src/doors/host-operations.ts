// THE DOOR TO A HOST'S NAMED OPERATIONS (C5-T, 2026-11-15: the TypeScript split of plan and run): the planner asked for
// the operations' statements (Boundary.hostOperations, D5) outside any run, then the statements run on the host's own
// database inside a run frame (run-frame.ts: a run plans nothing). The page spells no SQL; a database without an
// operation is refused by the planner, by name.

import type { PlannedSql } from '../planned-sql.ts';
import type { HostOperationsSource } from '../planner-sources.ts';
import type { HostOperation } from '../requests.ts';
import { inRun } from '../run-frame.ts';

/** Each of `operations` planned by `planner` for `engine`'s database, then its statements run on `engine`, in order. */
export async function runHostOperations(planner: HostOperationsSource,
  engine: { readonly databaseType: string; run(sql: PlannedSql, epoch: number): Promise<unknown> },
  operations: readonly HostOperation[], epoch = 0): Promise<void> {
  const statements = (await planner.hostOperations(engine.databaseType, operations)).flat();
  await inRun('host operations', async () => {
    for (const sql of statements) await engine.run(sql, epoch);
  });
}
