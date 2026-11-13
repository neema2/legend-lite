// A CLASS'S SAMPLE ROWS: its first rows as Create a Class writes them into its project (model-actions.ts), each value
// as the grid's executor gave it, a bigint as text. Moved out of model-actions.ts, which opens the windows, so code with
// no screen writes them too: Python's classes, headless (headless/grid.ts gridClass;
// docs/PYTHON_ASKS_DATACUBE_2026_10_15.md, item 7).

import type { Lambda } from '../../pure-protocol/src/index.ts';
import type { QueryRunner } from './runner.ts';
import { rowsOnce } from './rows-once.ts';

/** A class's sample: its columns' names and its rows, each a value per column. */
export interface SampleRows {
  readonly columns: string[];
  readonly rows: (string | number | boolean | null)[][];
}

/** A sample DataCube does not write, and why: a value the route's JSON has no spelling for, named by its place. */
export class SampleRefusal extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'SampleRefusal';
  }
}

/**
 * A class's first rows as the route's sample takes them (a bigint as text), planned and run where its grid's are. A
 * NaN or an infinity is refused, naming its column and row, before anything is written: the sample goes to the route
 * as JSON, which has neither, and JSON.stringify would write `null` -- a value the data never had (AGENTS.md invariant
 * 4; found 2026-10-15, refused 2026-10-16).
 */
export async function sampleOf(runner: QueryRunner, sample: Lambda): Promise<SampleRows> {
  const { rows } = await rowsOnce(runner, sample);
  return {
    columns: rows.columns.map((c) => c.name),
    rows: [...Array(rows.rowCount).keys()].map((r) => rows.columns.map((c) => {
      const v = c.values[r] ?? null;
      if (typeof v === 'number' && !Number.isFinite(v)) {
        throw new SampleRefusal(`the sample's ${c.name} in row ${r + 1} is ${String(v)}, which a class's data cannot `
          + 'hold: its rows are written as JSON, which has no NaN or infinity');
      }
      return typeof v === 'bigint' ? String(v) : v;
    })),
  };
}
