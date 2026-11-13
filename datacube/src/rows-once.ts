// A QUERY'S ROWS READ ONCE (the engine contract's step 4a, 2026-11-13): its own module, importing the runner's types
// alone, so a reader of one fact (a sample, a key's uniqueness) does not bring the runners' code into the page's start.

import type { Lambda } from '../../pure-protocol/src/index.ts';
import type { QueryRunner, RunOutcome } from './runner.ts';
import type { CubeSnapshot } from './snapshot.ts';

/**
 * A QUERY'S ROWS READ ONCE, outside any grid's view (a model action's sample or fact, a join's question, grids opened
 * together, a class's first read): through the runner as every grid's rows are -- never a host's planner or database
 * reached past it (the engine contract's step 4a, 2026-11-13). Its snapshot is the query's alone: its source the query,
 * no level of a grid's, epoch 0.
 */
export function rowsOnce(runner: QueryRunner, query: Lambda, signal?: AbortSignal): Promise<RunOutcome> {
  const alone: CubeSnapshot = { source: { query }, columns: [], derived: [], rows: [], pivotOn: [], sorts: [], epoch: 0 };
  return runner.run(query, alone, undefined, signal);
}
