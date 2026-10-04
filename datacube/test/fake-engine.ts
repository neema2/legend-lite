// A test double for an ENGINE: it answers the SQL it is given with a table the test
// builds, types included -- a stand-in for the database AND the plan's typing, which
// the real engines do through engine.ts `typedByPlan`. A planned query arrives as its
// plan; the double answers its SQL.

import type { QueryEngine, RawTable } from '../../engine-client/src/engine.ts';
import type { Plan } from '../../engine-client/src/relation-type.ts';
import type { ResultTable } from '../../engine-client/src/result.ts';

export abstract class FakeEngine implements QueryEngine {
  abstract readonly name: string;

  /** The table this double returns for `sql`. */
  abstract answer(sql: string, epoch: number, signal?: AbortSignal): Promise<ResultTable>;

  execute(plan: Plan, epoch: number, signal?: AbortSignal): Promise<ResultTable> {
    return this.answer(plan.sql, epoch, signal);
  }

  run(sql: string, epoch: number, signal?: AbortSignal): Promise<RawTable> {
    return this.answer(sql, epoch, signal);
  }

  /** The answer as one chunk: a double does not stream. */
  async stream(plan: Plan, epoch: number, onChunk: (chunk: ResultTable) => void, signal?: AbortSignal): Promise<void> {
    onChunk(await this.answer(plan.sql, epoch, signal));
  }

  async close(): Promise<void> {}
}
