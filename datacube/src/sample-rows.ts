// A CLASS'S SAMPLE ROWS: its first rows as Create a Class writes them into its project (model-actions.ts), each value
// as the grid's executor gave it, a bigint as text. Moved out of model-actions.ts, which opens the windows, so code with
// no screen writes them too: Python's classes, headless (headless/grid.ts gridClass;
// docs/PYTHON_ASKS_DATACUBE_2026_10_15.md, item 7).

import type { Lambda } from '../../pure-protocol/src/index.ts';
import type { PlanAndRunHost } from './runner.ts';

/** A class's sample: its columns' names and its rows, each a value per column. */
export interface SampleRows {
  readonly columns: string[];
  readonly rows: (string | number | boolean | null)[][];
}

/** A class's first rows as the route's sample takes them (a bigint as text), planned and run where its grid's are. */
export async function sampleOf(host: PlanAndRunHost, sample: Lambda): Promise<SampleRows> {
  const { rows } = await host.planAndRun(sample, 0);
  return {
    columns: rows.columns.map((c) => c.name),
    rows: [...Array(rows.rowCount).keys()].map((r) => rows.columns.map((c) => {
      const v = c.values[r] ?? null;
      return typeof v === 'bigint' ? String(v) : v;
    })),
  };
}
