// A MEASURE AS THE SCREEN'S OWN STEPS (the user, 2026-10-22: one API the screen's gestures and a program call):
// Python's grid.measure and page.grid(measures=...) make a measure by the commands a person makes it by -- Column
// Properties' Aggregation (`column`), or the column editor's calculated column of its own name reading the column
// (`calculate`) and then its Aggregation -- each by the one rule (grid-edits.ts measurePlan, which Show Grid's measures
// are made by too). No command kind of its own: what a page open on it is told is what a person's steps tell.

import { applyGridCommands, type GridCommand } from './grid-commands.ts';
import { measurePlan, type GridDraft } from './grid-edits.ts';
import type { Measure } from './snapshot.ts';

/**
 * The commands that make `measures` the grid's, in order, each judged against what the ones before it made (a column
 * given an Aggregation once is taken: another measure of it is a calculated column of its own name). Refused in their
 * words (CubeRefusal), nothing made.
 */
export function measureCommands(draft: GridDraft, measures: readonly Measure[]): GridCommand[] {
  const commands: GridCommand[] = [];
  const taken = new Set<string>();
  let d = draft;
  for (const m of measures) {
    const plan = measurePlan(d, m, taken);
    const made: GridCommand[] = plan.copy === undefined
      ? [{ kind: 'column', name: m.column, change: plan.change }]
      : [{ kind: 'calculate', level: 'row', column: plan.copy }, { kind: 'column', name: plan.copy.name, change: plan.change }];
    if (plan.copy === undefined) taken.add(m.column);
    d = applyGridCommands(d, made);
    commands.push(...made);
  }
  return commands;
}
