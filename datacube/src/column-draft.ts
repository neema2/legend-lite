// A CALCULATED COLUMN'S DRAFT, as the column editor (ui/column-editor.ts) makes one into the cube's columns: no DOM, so
// code with no screen adds a column by the editor's own steps -- Python's, headless (headless/grid.ts gridColumn;
// docs/PYTHON_ASKS_DATACUBE_2026_10_15.md, item 5). Plain functions of the cube as it is now, the column's name when
// editing began (undefined when adding), the draft, and its lambda as the compiler parsed what was typed.

import { defaultKind } from '../../engine-client/src/types.ts';
import type { Lambda } from '../../pure-protocol/src/index.ts';
import type { CalcStage } from './calc.ts';
import {
  WINDOW_FUNCTIONS,
  withCalculated,
  type ChildAggregate,
  type CubeSnapshot,
  type DerivedColumn,
  type Grain,
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
   * (group level) an aggregate of the child groups' figures or a measure
   * at its parent's or the grand total's grain.
   */
  mode: 'expression' | 'window' | 'children' | 'grain';
  /** The whole lambda as the person edits it, `x|$x.a * 2` (upstream's editor shows the same). */
  expression: string;
  /** EXPLODE: the expression is a collection, and each row repeats once per element (`DerivedColumn.unnest`). */
  unnest: boolean;
  window: WindowSpec;
  child: ChildAggregate;
  grain: Grain;
}

/** A window to start from: a running sum, order to be chosen. */
export const NEW_WINDOW: WindowSpec = { fn: 'sum', partition: [], order: [], frame: 'running' };
/** A child-group aggregate to start from: the smallest child. */
export const NEW_CHILD: ChildAggregate = { fn: 'min', of: '' };
/** A measure at a grain to start from: at its parent's, measure to be chosen. */
export const NEW_GRAIN: Grain = { of: '', at: 'parent' };

/** A new column's draft: an expression, at `level` -- a MEASURE unless told, as upstream starts one
 *  (DataCubeNewColumnState). */
export function newDraft(name: string, expression: string, level: ColumnLevel = 'measure'): ColumnDraft {
  return { name, level, mode: 'expression', expression, unnest: false, window: NEW_WINDOW, child: NEW_CHILD, grain: NEW_GRAIN };
}

/**
 * An existing calculated column as a draft -- what Edit Column opens on, and what a host's edit of it starts from (the
 * S1 audit's 4: its kind, its level, its explode kept unless told otherwise) -- and its lambda to print into it (the
 * draft's text waits for the compiler's print), or undefined if it has gone.
 */
export function existingDraft(s: CubeSnapshot, name: string): { readonly draft: ColumnDraft; readonly lambda?: Lambda } | undefined {
  const row = s.derived.find((d) => d.name === name);
  if (row) {
    return {
      draft: {
        name,
        // A column saved before kinds were declared behaves as its type.
        level: row.kind ?? defaultKind(row.type),
        mode: row.window ? 'window' : 'expression',
        expression: '',
        unnest: row.unnest === true,
        window: row.window ?? NEW_WINDOW,
        child: NEW_CHILD,
        grain: NEW_GRAIN,
      },
      ...(row.lambda ? { lambda: row.lambda } : {}),
    };
  }
  const group = (s.groupDerived ?? []).find((d) => d.name === name);
  return group
    ? {
      draft: {
        name, level: 'group',
        mode: group.childAggregate ? 'children' : group.grain ? 'grain' : group.window ? 'window' : 'expression',
        expression: '', unnest: false, window: group.window ?? NEW_WINDOW,
        child: group.childAggregate ?? NEW_CHILD, grain: group.grain ?? NEW_GRAIN,
      },
      ...(group.lambda ? { lambda: group.lambda } : {}),
    }
    : undefined;
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
    ...(draft.mode === 'grain' ? { grain: { ...draft.grain } } : {}),
    // A group-level column is already past the aggregation, so it has
    // no measure-or-dimension to decide; upstream draws the same line.
    ...(draft.level === 'group' ? {} : { kind: draft.level }),
  };
}

/** The cube with the draft's column in place of `original`, what read a renamed column reading it by its new name. */
export function draftCandidate(s: CubeSnapshot, original: string | undefined, draft: ColumnDraft, lambda?: Lambda):
  CubeSnapshot {
  return withCalculated(s, original, draftStage(draft), draftColumn(draft, lambda));
}
