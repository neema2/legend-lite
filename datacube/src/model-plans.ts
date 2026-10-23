// A MODEL ACTION'S PLAN, ON EVERY HOST (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md Z4-Z6): what Create a Class…, Create an
// Association… and Create a Data Space… make of the page before they ask the writer -- a grid's rows as a class's (its
// class action, each table's first rows its samples), an association's key facts read where the rows are -- one
// function each, which the windows (create-class.ts, model-actions.ts) and DataCube's code headless (headless/model.ts:
// Python's actions) call alike. No DOM, no window.

import type { Lambda, ValueSpecification } from '../../pure-protocol/src/index.ts';
import type { ClassAction, ColumnChoice, GridSource, Joinable } from './model-writer.ts';
import type { ClassBasis } from './query.ts';
import type { PlanAndRun } from './runner.ts';
import { sampleOf, type SampleRows } from './sample-rows.ts';

/**
 * Each table a class is made over, its first rows as the window writes them into the project (sample-rows.ts sampleOf),
 * read where the grid's rows are: a NaN or an infinity the sample cannot carry refused in words (SampleRefusal).
 */
export async function classSamples(rows: PlanAndRun, basis: ClassBasis): Promise<Record<string, SampleRows>> {
  const samples: Record<string, SampleRows> = {};
  for (const t of basis.tables) samples[t.table] = await sampleOf(rows.host, t.sample);
  return samples;
}

/** What a person names and chooses of a class: its name, its documentation, and their choices for its columns. */
export interface ClassNamed {
  readonly className: string;
  readonly documentation?: string;
  readonly columns?: Readonly<Record<string, ColumnChoice>>;
}

/**
 * A GRID'S ROWS AS A CLASS (the writer's class action): its rows at their finest level over its source, the Database
 * they read at `place` (`Browser`, `Python`) with each table's sample, under the name and choices a person gave.
 */
export function classAction(made: { readonly basis: ClassBasis; readonly source: GridSource }, place: string,
  samples: Readonly<Record<string, SampleRows>>, named: ClassNamed): ClassAction {
  return {
    query: made.basis.query,
    source: made.source,
    name: named.className,
    ...(named.documentation ? { documentation: named.documentation } : {}),
    ...(named.columns !== undefined ? { columns: named.columns } : {}),
    where: { from: made.basis.database, as: place, samples },
  };
}

/** Whether the planned query answers no row: the column is unique over the rows; undefined when it cannot be run. */
export async function noRow(rows: PlanAndRun, query: Lambda): Promise<boolean | undefined> {
  try {
    return (await rows.host.planAndRun(query, 0)).rows.rowCount === 0;
  } catch {
    // the rows are not where this host reads them (a class made in another browser, its table not opened here): asked,
    // not guessed
    return undefined;
  }
}

/** The query builders a key's facts are read with (query.ts duplicateLambda; a relation function's call), handed in. */
export interface KeyQueries {
  readonly duplicate: (relation: ValueSpecification, column: string) => Lambda;
  readonly call: (fn: string) => ValueSpecification;
}

/**
 * WHETHER A CLASS'S KEY IS UNIQUE OVER ITS ROWS (Create an Association…'s preset of how many of an end, Z5): read
 * through the class's relation function over the project -- the writer's `joinable` names it and the key's column --
 * as the query for a value held twice: no row, and it is. Undefined when it cannot be read (no function, no column, the
 * rows not here): left for the writer to refuse in words, as the window leaves it for the person.
 */
export function keyUnique(facts: PlanAndRun, joinable: readonly Joinable[], cls: string, on: string, q: KeyQueries):
  Promise<boolean | undefined> {
  const j = joinable.find((x) => x.class === cls);
  const column = j?.keys.find((k) => k.name === on)?.column;
  if (j?.function === undefined || column === undefined) return Promise.resolve(undefined);
  return noRow(facts, q.duplicate(q.call(j.function), column));
}
