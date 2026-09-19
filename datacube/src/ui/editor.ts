// The editor: seven tabs over one draft.
//
// Tab set, tab ORDER and tab labels are DataCube's own, taken from
// its DataCubeEditorTab enum, so a user who knows one product can
// find a setting in the other without hunting. The footer is its
// three buttons with its semantics: Cancel discards, Apply keeps the
// editor open, OK applies and closes.
//
// The draft is the whole point. Every panel edits ONE object and
// nothing reaches the cube until Apply, so a half-built pivot never
// issues a query, and Cancel is a discard rather than an undo log.
// That also lets the panels stay dumb: they read the draft and write
// the draft, and none of them knows the controller exists.
//
// On the Columns tab: DataCube has two flags where this has one.
// Its `isSelected` decides whether a column is PROJECTED and its
// `hideFromView` whether a projected column is rendered. Here the
// query already projects only what the rows, measures and derived
// columns reference, so for an aggregated cube the two collapse --
// an unselected column that nothing references was never in the SQL
// to begin with. So this tab edits visibility and order, and Column
// Properties' "Hide from view" is the same flag from the other side.
// Stated rather than silently conflated.

import type { Dimension } from '../dimensions.ts';
import {
  DEFAULT_CONFIGURATION,
  applyToSnapshot,
  columnConfig,
  labelFor,
  withColumn,
  type CubeConfiguration,
} from '../config.ts';
import type { CubeSnapshot, SortSpec } from '../snapshot.ts';
import { PIVOT_SEPARATOR } from '../grid/columns.ts';
import { button, dropdown } from './form.ts';
import {
  SORT_DIRECTIONS,
  allColumns,
  groupableColumns,
  panelShell,
  selectorInto,
  type CubeDraft,
  type PanelBuilder,
  type PanelContext,
} from './panel-kit.ts';
import { generalPropertiesPanel } from './panel-general.ts';
import { columnPropertiesPanel } from './panel-column.ts';
import { dimensionsPanel } from './panel-dimensions.ts';

export type { CubeDraft, PanelBuilder, PanelContext };

export type EditorTab =
  | 'Columns'
  | 'Horizontal Pivots'
  | 'Vertical Pivots'
  | 'Dimensions'
  | 'Sorts'
  | 'General Properties'
  | 'Column Properties';

/** DataCube's own order, which is not its enum's declaration order. */
export const EDITOR_TABS: readonly EditorTab[] = [
  'Columns',
  'Horizontal Pivots',
  'Vertical Pivots',
  'Dimensions',
  'Sorts',
  'General Properties',
  'Column Properties',
];

export interface EditorOptions {
  readonly onApply: (draft: CubeDraft) => void;
  readonly onClose: () => void;
  readonly initialTab?: EditorTab;
}

export class CubeEditor {
  readonly #doc: Document;
  readonly #options: EditorOptions;
  readonly #tabStrip: HTMLElement;
  readonly #body: HTMLElement;
  #draft: CubeDraft;
  #tab: EditorTab;
  /** Per-editor view state, handed to whichever panel is showing. */
  readonly #panelState: Record<string, unknown> = {};
  /** The draft as it was when the editor opened, for Cancel. */
  readonly #opened: CubeDraft;

  constructor(root: HTMLElement, draft: CubeDraft, options: EditorOptions) {
    this.#doc = root.ownerDocument;
    this.#draft = draft;
    this.#opened = draft;
    this.#options = options;
    this.#tab = options.initialTab ?? 'Columns';

    root.classList.add('dc-editor');
    root.setAttribute('role', 'dialog');
    root.setAttribute('aria-label', 'Cube properties');

    this.#tabStrip = this.#doc.createElement('div');
    this.#tabStrip.className = 'dc-editor-tabs';
    this.#tabStrip.setAttribute('role', 'tablist');

    this.#body = this.#doc.createElement('div');
    this.#body.className = 'dc-editor-body';
    this.#body.setAttribute('role', 'tabpanel');

    root.replaceChildren(this.#tabStrip, this.#body, this.#footer());
    this.#renderTabs();
    this.refresh();
  }

  get tab(): EditorTab {
    return this.#tab;
  }

  get draft(): CubeDraft {
    return this.#draft;
  }

  setTab(tab: EditorTab): void {
    if (tab === this.#tab) return;
    this.#tab = tab;
    this.#renderTabs();
    this.refresh();
  }

  /** Discard every change and close. */
  cancel(): void {
    this.#draft = this.#opened;
    this.#options.onClose();
  }

  /**
   * Hand the draft to the cube.
   *
   * The snapshot is reconciled with the configuration HERE rather
   * than in each panel, so a setting that shapes the query -- a
   * column's kind, the row cap -- cannot be applied by one panel and
   * forgotten by another.
   */
  apply(options: { close?: boolean } = {}): void {
    const snapshot = applyToSnapshot(this.#draft.snapshot, this.#draft.config);
    this.#draft = { ...this.#draft, snapshot };
    this.#options.onApply(this.#draft);
    if (options.close) this.#options.onClose();
  }

  refresh(): void {
    const build = PANELS[this.#tab];
    this.#body.replaceChildren(build(this.#context()));
  }

  #context(): PanelContext {
    return {
      doc: this.#doc,
      state: this.#panelState,
      draft: () => this.#draft,
      setSnapshot: (snapshot) => {
        this.#draft = { ...this.#draft, snapshot };
      },
      setConfig: (config) => {
        this.#draft = { ...this.#draft, config };
      },
      setDimensions: (dimensions) => {
        this.#draft = { ...this.#draft, dimensions };
      },
      refresh: () => this.refresh(),
    };
  }

  #renderTabs(): void {
    const tabs = EDITOR_TABS.map((tab) => {
      const b = this.#doc.createElement('button');
      b.type = 'button';
      b.className = 'dc-editor-tab';
      b.textContent = tab;
      b.setAttribute('role', 'tab');
      const current = tab === this.#tab;
      b.setAttribute('aria-selected', String(current));
      b.classList.toggle('dc-on', current);
      // Roving tabindex: the strip is ONE stop and arrows move within
      // it. Seven tab stops in front of the panel is how a keyboard
      // user gives up on a settings dialog.
      b.tabIndex = current ? 0 : -1;
      b.addEventListener('click', () => this.setTab(tab));
      b.addEventListener('keydown', (event) => this.#tabKey(event, tab));
      return b;
    });
    this.#tabStrip.replaceChildren(...tabs);
  }

  #tabKey(event: KeyboardEvent, tab: EditorTab): void {
    const step =
      event.key === 'ArrowRight' ? 1 : event.key === 'ArrowLeft' ? -1 : 0;
    if (step === 0) return;
    event.preventDefault();
    const i = EDITOR_TABS.indexOf(tab);
    const next = EDITOR_TABS[
      (i + step + EDITOR_TABS.length) % EDITOR_TABS.length
    ] as EditorTab;
    this.setTab(next);
    (this.#tabStrip.children[EDITOR_TABS.indexOf(next)] as HTMLElement).focus();
  }

  #footer(): HTMLElement {
    const bar = this.#doc.createElement('div');
    bar.className = 'dc-editor-footer';
    bar.append(
      button(this.#doc, 'Cancel', () => this.cancel()),
      button(this.#doc, 'Apply', () => this.apply()),
      button(this.#doc, 'OK', () => this.apply({ close: true }), {
        className: 'dc-primary',
      }),
    );
    return bar;
  }
}

// --------------------------------------------------------------------
// The structural panels. The two property panels are their own files
// only because they are long.
// --------------------------------------------------------------------

const columnsPanel: PanelBuilder = (ctx) => {
  const draft = ctx.draft();
  const all = allColumns(draft);
  const names = all.map((c) => c.name);
  const order = draft.config.columnOrder ?? names;
  // Order first, then filter: a column the configuration orders but
  // the source no longer has must not appear, and one the order
  // forgot must still show up.
  const ordered = [
    ...order.filter((n) => names.includes(n)),
    ...names.filter((n) => !order.includes(n)),
  ];
  const selected = ordered.filter(
    (n) => !columnConfig(draft.config, n).hidden,
  );

  const body = selectorInto(
    ctx,
    { all, selected },
    (next) => {
      const shown = new Set(next);
      let config = ctx.draft().config;
      for (const name of names) {
        config = withColumn(config, name, {
          hidden: shown.has(name) ? undefined : true,
        });
      }
      // The selected pane's order IS the display order. Removed
      // columns keep their relative place behind it, so putting one
      // back does not send it to the end.
      const tail = ordered.filter((n) => !shown.has(n));
      ctx.setConfig({ ...config, columnOrder: [...next, ...tail] });
      ctx.refresh();
    },
    {
      labelFor: (n) => labelFor(ctx.draft().config, n),
      hintFor: (n) => {
        const s = ctx.draft().snapshot;
        if (s.derived.some((d) => d.name === n)) return 'Extended (Leaf Level)';
        if ((s.groupDerived ?? []).some((d) => d.name === n))
          return 'Extended (Group Level)';
        return null;
      },
    },
  );

  const warning = ctx.doc.createElement('div');
  warning.className = 'dc-warning';
  warning.textContent = 'No columns selected';
  warning.hidden = selected.length > 0;

  return panelShell(ctx.doc, 'Columns', body, warning);
};

const horizontalPivotsPanel: PanelBuilder = (ctx) => {
  const draft = ctx.draft();
  const body = selectorInto(
    ctx,
    { all: groupableColumns(draft), selected: draft.snapshot.pivotOn },
    (pivotOn) => {
      const s = ctx.draft().snapshot;
      ctx.setSnapshot({ ...s, pivotOn: [...pivotOn] });
    },
    {
      actionFor: (name) =>
        dropdown(
          ctx.doc,
          columnConfig(ctx.draft().config, name).pivotSortDirection ?? 'asc',
          SORT_DIRECTIONS,
          (direction) =>
            ctx.setConfig(
              withColumn(ctx.draft().config, name, {
                pivotSortDirection: direction,
              }),
            ),
          { width: 110 },
        ),
    },
  );
  return panelShell(ctx.doc, 'Horizontal Pivots', body);
};

const verticalPivotsPanel: PanelBuilder = (ctx) => {
  const draft = ctx.draft();
  const body = selectorInto(
    ctx,
    { all: groupableColumns(draft), selected: draft.snapshot.rows },
    (rows) => {
      const s = ctx.draft().snapshot;
      ctx.setSnapshot({ ...s, rows: [...rows] });
    },
  );
  return panelShell(ctx.doc, 'Vertical Pivots', body);
};

/**
 * A pivoted column's name is its dimension values joined by the
 * pivot separator; raw, that is unreadable, so it reads back as
 * "2023 / EMEA" -- DataCube's own presentation.
 */
export function sortLabel(name: string): string {
  return name.split(PIVOT_SEPARATOR).join(' / ');
}

const sortsPanel: PanelBuilder = (ctx) => {
  const draft = ctx.draft();
  const directions = new Map(
    draft.snapshot.sorts.map((s) => [s.column, s.direction]),
  );
  const body = selectorInto(
    ctx,
    {
      all: allColumns(draft),
      selected: draft.snapshot.sorts.map((s) => s.column),
    },
    (columns) => {
      const s = ctx.draft().snapshot;
      const was = new Map(s.sorts.map((x) => [x.column, x.direction]));
      const sorts: SortSpec[] = columns.map((column) => ({
        column,
        // A column dragged out and back keeps the direction it had,
        // which is what a user reordering sorts expects.
        direction: was.get(column) ?? 'asc',
      }));
      ctx.setSnapshot({ ...s, sorts });
    },
    {
      labelFor: sortLabel,
      actionFor: (name) =>
        dropdown(
          ctx.doc,
          directions.get(name) ?? 'asc',
          SORT_DIRECTIONS,
          (direction) => {
            const s = ctx.draft().snapshot;
            ctx.setSnapshot({
              ...s,
              sorts: s.sorts.map((x) =>
                x.column === name
                  ? { column: x.column, direction: direction ?? 'asc' }
                  : x,
              ),
            });
          },
          { width: 110 },
        ),
    },
  );
  return panelShell(ctx.doc, 'Sorts', body);
};

const PANELS: Readonly<Record<EditorTab, PanelBuilder>> = {
  Columns: columnsPanel,
  'Horizontal Pivots': horizontalPivotsPanel,
  'Vertical Pivots': verticalPivotsPanel,
  Dimensions: dimensionsPanel,
  Sorts: sortsPanel,
  'General Properties': generalPropertiesPanel,
  'Column Properties': columnPropertiesPanel,
};

/** A starting draft for a cube that has never been configured. */
export function draftFor(
  snapshot: CubeSnapshot,
  config: CubeConfiguration = DEFAULT_CONFIGURATION,
  dimensions: readonly Dimension[] = [],
): CubeDraft {
  return { snapshot, config, dimensions };
}
