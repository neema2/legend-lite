// What every editor panel is built from.
//
// Separate from editor.ts so the panels and the shell can import the
// same scaffolding without a cycle: the shell needs the panels to
// render them and the panels need the context type, and putting both
// in one module makes the import graph depend on evaluation order.

import type { Dimension } from '../dimensions.ts';
import {
  columnConfig,
  type CubeConfiguration,
} from '../config.ts';
import { rowColumns, type CubeSnapshot, type SortDirection } from '../snapshot.ts';
import { ColumnsSelector, type SelectorColumn } from './columns-selector.ts';

/** Everything the editor edits, as one value. */
export interface CubeDraft {
  readonly snapshot: CubeSnapshot;
  readonly config: CubeConfiguration;
  readonly dimensions: readonly Dimension[];
}

/** What a panel is handed. */
export interface PanelContext {
  readonly doc: Document;
  /**
   * Scratch space that survives a refresh, owned by the editor.
   *
   * A panel needs somewhere to keep which column it is showing and
   * whether its advanced block is open -- state that belongs to the
   * VIEW rather than to the draft, and must not be saved with the
   * cube. Module-level variables would do it in one editor and
   * cross-talk between two.
   */
  readonly state: Record<string, unknown>;
  draft(): CubeDraft;
  setSnapshot(snapshot: CubeSnapshot): void;
  setConfig(config: CubeConfiguration): void;
  setDimensions(dimensions: readonly Dimension[]): void;
  /**
   * Rebuild the current panel.
   *
   * Explicit rather than automatic on every change: a rebuild moves
   * focus, so a checkbox must NOT trigger one and a "choose column"
   * dropdown -- which rebinds every other control in the panel --
   * must.
   */
  refresh(): void;
}

export type PanelBuilder = (ctx: PanelContext) => HTMLElement;

export const SORT_DIRECTIONS: readonly {
  value: SortDirection;
  label: string;
}[] = [
  { value: 'asc', label: 'Ascending' },
  { value: 'desc', label: 'Descending' },
];

export function panelShell(
  doc: Document,
  title: string,
  ...children: readonly HTMLElement[]
): HTMLElement {
  const root = doc.createElement('div');
  root.className = 'dc-panel';
  const head = doc.createElement('div');
  head.className = 'dc-panel-title';
  head.textContent = title;
  root.append(head, ...children);
  return root;
}

/** Every column the cube knows, including the derived ones. */
export function allColumns(draft: CubeDraft): SelectorColumn[] {
  const s = draft.snapshot;
  return [
    ...s.columns.map((c) => ({ name: c.name, type: c.type })),
    ...s.derived.map((d) => ({ name: d.name, type: 'Derived' })),
    ...(s.groupDerived ?? []).map((d) => ({ name: d.name, type: 'Derived' })),
  ];
}

/**
 * Columns a pivot or a row grouping may use.
 *
 * Measures are excluded, matching DataCube's `enableRowGroup: kind
 * === DIMENSION`. Grouping by a notional produces one group per
 * distinct amount, which is never what anyone meant and is expensive
 * to discover.
 */
export function groupableColumns(draft: CubeDraft): SelectorColumn[] {
  const out: SelectorColumn[] = [];
  // Row-stage calculated dimensions are groupable like any other. Their
  // kind is the one declared in the calculated-column editor; the
  // configuration's override applies to source columns only.
  for (const c of rowColumns(draft.snapshot)) {
    const kind = c.derived
      ? c.kind
      : columnConfig(draft.config, c.name).kind ?? c.kind;
    if (kind === 'dimension') {
      out.push({ name: c.name, type: c.type ?? 'Derived' });
    }
  }
  return out;
}

/** A selector filling a panel's body. */
export function selectorInto(
  ctx: PanelContext,
  state: { all: readonly SelectorColumn[]; selected: readonly string[] },
  onChange: (selected: readonly string[]) => void,
  extra: {
    actionFor?: (name: string) => HTMLElement | null;
    labelFor?: (name: string) => string;
    hintFor?: (name: string) => string | null;
    availableLabel?: string;
    selectedLabel?: string;
  } = {},
): HTMLElement {
  const host = ctx.doc.createElement('div');
  host.className = 'dc-panel-body';
  new ColumnsSelector(host, state, { onChange, ...extra });
  return host;
}
