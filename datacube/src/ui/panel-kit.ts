// What every editor panel is built from.
//
// Separate from editor.ts so the panels and the shell can import the
// same scaffolding without a cycle: the shell needs the panels to
// render them and the panels need the context type, and putting both
// in one module makes the import graph depend on evaluation order.

import type { Dimension } from '../dimensions.ts';
import type { CubeConfiguration } from '../config.ts';
import { SORT_DIRECTIONS, type CubeSnapshot, type SortDirection } from '../snapshot.ts';
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
  /** The Dimensions tab's actions, the draft applied first (EditorOptions.onUseDimension, onAdHoc); absent, not offered. */
  readonly useDimension?: (name: string) => void;
  readonly adHoc?: () => void;
}

export type PanelBuilder = (ctx: PanelContext) => HTMLElement;

const DIRECTION_LABELS: Readonly<Record<SortDirection, string>> = { asc: 'Ascending', desc: 'Descending' };
export const SORT_DIRECTION_OPTIONS: readonly { value: SortDirection; label: string }[] =
  SORT_DIRECTIONS.map((value) => ({ value, label: DIRECTION_LABELS[value] }));

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

/** Columns, as a selector lists them: the offers themselves are grid-edits.ts's (allColumns, groupableColumns), the
 *  one home of what a grid may be grouped, pivoted and sorted by -- for the screen's panels and Python's calls alike. */
export function selectable(names: readonly string[]): SelectorColumn[] {
  return names.map((name) => ({ name }));
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
