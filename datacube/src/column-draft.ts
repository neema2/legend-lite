// A CALCULATED COLUMN'S DRAFT, as the column editor (ui/column-editor.ts) makes one into the cube's columns: no DOM, so
// code with no screen adds a column by the editor's own steps -- Python's, headless (headless/grid.ts gridCalculate;
// docs/PYTHON_ASKS_DATACUBE_2026_10_15.md, item 5). Plain functions of the cube as it is now, the column's name when
// editing began (undefined when adding), the draft, and its lambda as the compiler parsed what was typed.

import type { Lambda } from '../../pure-protocol/src/index.ts';
import type { CalcStage } from './calc.ts';
import {
  WINDOW_FUNCTIONS,
  renameColumnReferences,
  type ChildAggregate,
  type CubeSnapshot,
  type DerivedColumn,
  type WindowSpec,
} from './snapshot.ts';

/** Upstream's DataCubeExtendedColumnKind. */
export type ColumnLevel = 'measure' | 'dimension' | 'group';

/** A calculated column as the editor's form holds it. */
export interface ColumnDraft {
  name: string;
  level: ColumnLevel;
  /**
   * An expression over one row, a window over the rows around it, or
   * (group level) an aggregate of the child groups' figures.
   */
  mode: 'expression' | 'window' | 'children';
  /** The whole lambda as the person edits it, `x|$x.a * 2` (upstream's editor shows the same). */
  expression: string;
  /** EXPLODE: the expression is a collection, and each row repeats once per element (`DerivedColumn.unnest`). */
  unnest: boolean;
  window: WindowSpec;
  child: ChildAggregate;
}

/** A window to start from: a running sum, order to be chosen. */
export const NEW_WINDOW: WindowSpec = { fn: 'sum', partition: [], order: [], frame: 'running' };
/** A child-group aggregate to start from: the smallest child. */
export const NEW_CHILD: ChildAggregate = { fn: 'min', of: '' };

/** A new column's draft: an expression, at `level` -- a MEASURE unless told, as upstream starts one
 *  (DataCubeNewColumnState). */
export function newDraft(name: string, expression: string, level: ColumnLevel = 'measure'): ColumnDraft {
  return { name, level, mode: 'expression', expression, unnest: false, window: NEW_WINDOW, child: NEW_CHILD };
}

/** Where the draft's column is computed: before the grouping, or after it (a group-level column). */
export function draftStage(draft: ColumnDraft): CalcStage {
  return draft.level === 'group' ? 'group' : 'row';
}

/** A window, tidied: only what its function takes. */
export function draftWindow(w: WindowSpec): WindowSpec {
  const meta = WINDOW_FUNCTIONS.find((f) => f.fn === w.fn);
  return {
    fn: w.fn,
    ...(meta?.column && w.column ? { column: w.column } : {}),
    partition: [...w.partition],
    order: w.order.map((o) => ({ ...o })),
    ...(meta?.framed && w.frame !== undefined ? { frame: w.frame } : {}),
    ...((w.fn === 'lag' || w.fn === 'lead') && w.offset !== undefined ? { offset: w.offset } : {}),
    ...(w.fn === 'ntile' ? { buckets: w.buckets ?? 4 } : {}),
  };
}

/** The column the draft makes, its lambda the compiler's parse of its expression. */
export function draftColumn(draft: ColumnDraft, lambda?: Lambda): DerivedColumn {
  return {
    name: draft.name.trim(),
    ...(draft.mode === 'expression' && lambda ? { lambda } : {}),
    // explode: row stage only (after grouping there are no source rows to repeat)
    ...(draft.mode === 'expression' && draft.unnest && draft.level !== 'group' ? { unnest: true } : {}),
    ...(draft.mode === 'window' ? { window: draftWindow(draft.window) } : {}),
    ...(draft.mode === 'children' ? { childAggregate: { ...draft.child } } : {}),
    // A group-level column is already past the aggregation, so it has
    // no measure-or-dimension to decide; upstream draws the same line.
    ...(draft.level === 'group' ? {} : { kind: draft.level }),
  };
}

/** The cube's two lists of calculated columns with the draft's column in place of `original` (added when undefined). */
export function draftLists(s: CubeSnapshot, original: string | undefined, draft: ColumnDraft, lambda?: Lambda):
  { row: DerivedColumn[]; group: DerivedColumn[] } {
  const without = (list: readonly DerivedColumn[]): DerivedColumn[] => list.filter((d) => d.name !== original);
  const row = without(s.derived);
  const group = without(s.groupDerived ?? []);
  const next = draftColumn(draft, lambda);
  // In its old place when it stays at its stage, so an edit does not
  // reorder the cube's columns.
  const target = draft.level === 'group' ? group : row;
  const was = (draft.level === 'group' ? s.groupDerived ?? [] : s.derived).findIndex((x) => x.name === original);
  if (was >= 0) target.splice(was, 0, next);
  else target.push(next);
  return { row, group };
}

/** The rename the draft makes of `original`, if it renames it. */
export function draftRename(original: string | undefined, draft: ColumnDraft):
  { readonly from: string; readonly to: string } | undefined {
  const to = draft.name.trim();
  return original !== undefined && original !== to ? { from: original, to } : undefined;
}

/** The cube with the draft's column in place of `original`, what read a renamed column reading it by its new name. */
export function draftCandidate(s: CubeSnapshot, original: string | undefined, draft: ColumnDraft, lambda?: Lambda):
  CubeSnapshot {
  const { row, group } = draftLists(s, original, draft, lambda);
  const next: CubeSnapshot = { ...s, derived: row, groupDerived: group };
  const rename = draftRename(original, draft);
  return rename ? renameColumnReferences(next, rename.from, rename.to) : next;
}
