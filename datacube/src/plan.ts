// THE CUBE PLAN: the queries a view needs, in the order they have to run
// (docs/DATACUBE_CUBE_PLAN_DESIGN_2026_09_27.md).
//
// Its first piece is the pivot. A pivot's columns depend on the data --
// which years are present -- so a compiler cannot type a pivoted query in
// one pass without learning types from a result. It is two steps instead,
// each an ordinary Pure query with a static type: find the values
// (`pivotValuesQuery`), then write every level's groupBy with them
// (`serialize`). This module runs the first and hands its answer on.
//
// Before either, the cube's columns are TYPED BY THE COMPILER (step 0,
// docs/DATACUBE_TYPED_VALUES_DESIGN_2026_09_27.md): the source and its
// calculated columns, one compile-only ask, so the aggregate each column
// defaults to is right on the first query and nothing is learned from a result.

import type { ResultTable } from '../../engine-client/src/result.ts';
import type { QueryRunner } from './runner.ts';
import type { Lambda } from '../../pure-protocol/src/index.ts';
import {
  levelLambda,
  pivotValuesLambda,
  sourceWithDerived,
  MAX_PIVOT_VALUES,
  pinnedPivotFacts,
  pivotColumns,
  type LevelScope,
  type PivotColumn,
  type PivotFacts,
} from './query.ts';
import { CubeRefusal, type CubeSnapshot } from './snapshot.ts';
import { groupValue } from './treeview.ts';
import type { GroupKey } from './tree.ts';

/** A source column whose declared type is not what the compiler says it is now. */
export interface SchemaChange {
  readonly column: string;
  readonly was: string;
  /** The compiler's type, or null when the source no longer has the column. */
  readonly now: string | null;
}

/**
 * Step 0: the cube's source and row-stage calculated columns, as the COMPILER
 * types them -- `source->extend(...)`, the calculated columns in the order they
 * are applied, asked once per refresh through the runner's compile-only call
 * (upstream `lambdaRelationType`; the planners cache it by grammar, so an
 * unchanged cube costs no second compile). The snapshot adopts the answer
 * (option B: its types are the compiler's cache); a declared source type that no
 * longer matches is a SCHEMA CHANGE, reported, never silently kept. Group-stage
 * calculated columns exist only after a groupBy: the level query's plan types
 * them. A runner that types nothing (a test double) leaves the snapshot as is.
 */
export async function typeColumns(
  snapshot: CubeSnapshot,
  runner: QueryRunner,
  signal?: AbortSignal,
): Promise<{ readonly snapshot: CubeSnapshot; readonly changes: readonly SchemaChange[] }> {
  const typed = await runner.relationType(sourceWithDerived(snapshot).lambda(), signal);
  if (typed.length === 0) return { snapshot, changes: [] };
  const types = new Map(typed.map((c) => [c.name, c.type]));
  const changes: SchemaChange[] = [];
  let changed = false;
  const columns = snapshot.columns.map((c) => {
    const now = types.get(c.name);
    if (now === c.type) return c;
    changes.push({ column: c.name, was: c.type, now: now ?? null });
    if (now === undefined) return c;
    changed = true;
    return { ...c, type: now };
  });
  const derived = snapshot.derived.map((d) => {
    const now = types.get(d.name);
    if (now === undefined || now === d.type) return d;
    changed = true;
    return { ...d, type: now };
  });
  return { snapshot: changed ? { ...snapshot, columns, derived } : snapshot, changes };
}

/** A pivoted cube's first step, answered: its values and its columns. */
export interface PivotPlan {
  readonly facts: PivotFacts;
  /** Every column the pivot makes, cells then Totals, with what each IS. */
  readonly columns: readonly PivotColumn[];
  /** The values query and its SQL; null when the values were pinned. */
  readonly query: Lambda | null;
  readonly sql: string | null;
}

/**
 * Step 1 of a pivoted cube, run through the cube's own runner -- the same
 * planner and the same engine as every other query, so the values come
 * from exactly the data the cells will. Undefined when the cube does not
 * pivot. Run once per refresh: a filter that removes a value removes its
 * column, and new data brings new columns, with nothing learned to go
 * stale in between.
 */
export async function planPivot(
  snapshot: CubeSnapshot,
  runner: QueryRunner,
  signal?: AbortSignal,
): Promise<PivotPlan | undefined> {
  if (snapshot.pivotOn.length === 0) return undefined;
  const pinned = pinnedPivotFacts(snapshot);
  if (pinned) {
    return { facts: pinned, columns: pivotColumns(snapshot, pinned), query: null, sql: null };
  }
  const query = pivotValuesLambda(snapshot);
  if (query === null) throw new Error('a pivoted cube has no values query');
  const { rows, sql } = await runner.run(query, snapshot, undefined, signal);
  const facts = pivotFacts(rows, snapshot);
  return { facts, columns: pivotColumns(snapshot, facts), query, sql };
}

/**
 * Step 1's rows as the pivot's value combinations, in the database's
 * order. Past the cap it is REFUSED, naming the key: a grid of thousands
 * of columns freezes the tab, and silently keeping the first few hundred
 * would show a table with values missing and nothing saying so.
 */
export function pivotFacts(rows: ResultTable, snapshot: CubeSnapshot): PivotFacts {
  const on = snapshot.pivotOn;
  if (rows.rowCount > MAX_PIVOT_VALUES) {
    throw new CubeRefusal(
      `pivoting on ${on.join(', ')} finds more than ${MAX_PIVOT_VALUES} values, `
      + 'which would make a column for each: filter the cube first, or pivot '
      + 'on a column with fewer values',
    );
  }
  const tuples: GroupKey[][] = [];
  for (let i = 0; i < rows.rowCount; i++) {
    tuples.push(on.map((_, k) => groupValue(rows.columns[k]?.values[i] ?? null)));
  }
  return { tuples };
}

/**
 * One level's query for a caller that runs queries itself (the demo's
 * verification harnesses): step 1 through `runValues` when the cube
 * pivots, then the level written with its answer.
 */
export async function levelWithValues(
  snapshot: CubeSnapshot,
  scope: LevelScope | undefined,
  runValues: (query: Lambda) => Promise<ResultTable>,
): Promise<Lambda> {
  if (snapshot.pivotOn.length === 0) return levelLambda(snapshot, scope);
  const pinned = pinnedPivotFacts(snapshot);
  const values = pivotValuesLambda(snapshot);
  const facts = pinned ?? (values === null ? { tuples: [] } : pivotFacts(await runValues(values), snapshot));
  return levelLambda(snapshot, scope, facts);
}
