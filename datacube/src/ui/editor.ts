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
import { allColumns, groupableColumns, setGrouping, setPivot, setSorts } from '../grid-edits.ts';
import {
  SORT_DIRECTION_OPTIONS,
  panelShell,
  selectable,
  selectorInto,
  type CubeDraft,
  type PanelBuilder,
  type PanelContext,
} from './panel-kit.ts';
import { generalPropertiesPanel } from './panel-general.ts';
import { columnPanelUi, columnPropertiesPanel } from './panel-column.ts';
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

/**
 * The rail's sections, in its order (the user, 2026-10-01): the LAYOUT -- named as the drop zones
 * name it, Row Groups and Column Labels, not upstream's Vertical / Horizontal Pivots -- then the
 * PROPERTIES. The ids stay upstream's tab names.
 */
export const EDITOR_GROUPS: readonly { readonly title: string; readonly tabs: readonly EditorTab[] }[] = [
  { title: 'Layout', tabs: ['Columns', 'Vertical Pivots', 'Horizontal Pivots', 'Sorts', 'Dimensions'] },
  { title: 'Properties', tabs: ['Column Properties', 'General Properties'] },
];

/** Every section, in the rail's order. */
export const EDITOR_TABS: readonly EditorTab[] = EDITOR_GROUPS.flatMap((g) => g.tabs);

/** What the rail calls a section. */
export const TAB_LABELS: Readonly<Record<EditorTab, string>> = {
  'Columns': 'Columns',
  'Vertical Pivots': 'Row Groups',
  'Horizontal Pivots': 'Column Labels',
  'Sorts': 'Sorts',
  'Dimensions': 'Dimensions',
  'Column Properties': 'Column Properties',
  'General Properties': 'General Properties',
};

export interface EditorOptions {
  /**
   * `base` is the draft as it stood when the editor opened (or was
   * last applied): what the user CHANGED is the difference, and only
   * that should reach a cube other windows may have moved since.
   */
  /**
   * Whether the cube TOOK the draft. Upstream compiles the whole query
   * before publishing it; a refused draft applies nothing, and the
   * editor stays open on it.
   */
  readonly onApply: (draft: CubeDraft, base: CubeDraft) => boolean | Promise<boolean>;
  readonly onClose: () => void;
  /**
   * The Dimensions tab's own actions (the user, 2026-10-23: started here, not from a menu), each once the draft is
   * applied and the editor closed -- a refused draft starts neither: the grid's rows grouped by dimension `name`
   * (its first level), and Ad Hoc Analysis of the cube. Absent, the tab does not offer them.
   */
  readonly onUseDimension?: (name: string) => void;
  readonly onAdHoc?: () => void;
  readonly initialTab?: EditorTab;
  /** Open Column Properties on this column (from its header's menu). */
  readonly initialColumn?: string;
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
  /** The draft as it was when opened or last applied: Cancel, and the base of a merge. */
  #opened: CubeDraft;
  /** An Apply is running: another press waits for it, it does not start a second. */
  #applying = false;

  constructor(root: HTMLElement, draft: CubeDraft, options: EditorOptions) {
    this.#doc = root.ownerDocument;
    this.#draft = draft;
    this.#opened = draft;
    this.#options = options;
    this.#tab = options.initialColumn !== undefined
      ? 'Column Properties'
      : options.initialTab ?? 'Columns';
    if (options.initialColumn !== undefined) {
      columnPanelUi(this.#panelState).chosen = options.initialColumn;
    }

    root.classList.add('dc-editor');
    root.setAttribute('role', 'dialog');
    root.setAttribute('aria-label', 'Cube properties');

    this.#tabStrip = this.#doc.createElement('div');
    this.#tabStrip.className = 'dc-editor-tabs dc-pe-rail';
    this.#tabStrip.setAttribute('role', 'tablist');
    this.#tabStrip.setAttribute('aria-orientation', 'vertical');

    this.#body = this.#doc.createElement('div');
    this.#body.className = 'dc-editor-body';
    this.#body.setAttribute('role', 'tabpanel');

    const main = this.#doc.createElement('div');
    main.className = 'dc-pe-main';
    main.append(this.#tabStrip, this.#body);
    root.classList.add('dc-pe');
    root.replaceChildren(main, this.#footer());
    this.#renderTabs();
    this.refresh();
  }

  get tab(): EditorTab {
    return this.#tab;
  }

  get draft(): CubeDraft {
    return this.#draft;
  }

  /**
   * Show Column Properties for one column -- upstream's Properties...
   * from a column header. Keeps every other edit in the draft.
   */
  focusColumn(name: string): void {
    columnPanelUi(this.#panelState).chosen = name;
    this.#tab = 'Column Properties';
    this.#renderTabs();
    this.refresh();
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
  async apply(options: { close?: boolean } = {}): Promise<boolean> {
    // ONE AT A TIME: a double-click on OK started two overlapping applies.
    if (this.#applying) return false;
    const snapshot = applyToSnapshot(this.#draft.snapshot, this.#draft.config);
    // WHAT IS SENT is what becomes the base once it lands -- not the draft
    // as it is after the wait. The panels stay live while the query runs,
    // and an edit made meanwhile was recorded as already applied, so the
    // next Apply sent nothing of it (P2-170).
    const sent: CubeDraft = { ...this.#draft, snapshot };
    this.#draft = sent;
    this.#applying = true;
    let took: boolean;
    try {
      took = await this.#options.onApply(sent, this.#opened);
    } finally {
      this.#applying = false;
    }
    // Refused: the draft stays in the editor, the editor stays open.
    if (!took) return false;
    this.#opened = sent;
    if (options.close) this.#options.onClose();
    return true;
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
      ...(this.#options.onUseDimension ? { useDimension: (name: string) => this.#appliedThen(() => this.#options.onUseDimension?.(name)) } : {}),
      ...(this.#options.onAdHoc ? { adHoc: () => this.#appliedThen(() => this.#options.onAdHoc?.()) } : {}),
    };
  }

  /** The draft applied and the editor closed, then `then`: what the Dimensions tab starts acts on the cube as applied. */
  #appliedThen(then: () => void): void {
    void this.apply({ close: true }).then((took) => {
      if (took) then();
    });
  }

  #renderTabs(): void {
    const parts: HTMLElement[] = [];
    for (const group of EDITOR_GROUPS) {
      const title = this.#doc.createElement('div');
      title.className = 'dc-pe-rail-title';
      title.textContent = group.title;
      parts.push(title);
      for (const tab of group.tabs) parts.push(this.#tabButton(tab));
    }
    this.#tabStrip.replaceChildren(...parts);
  }

  #tabButton(tab: EditorTab): HTMLElement {
    const b = this.#doc.createElement('button');
    b.type = 'button';
    b.className = 'dc-editor-tab';
    b.dataset['tab'] = tab;
    b.textContent = TAB_LABELS[tab];
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
  }

  #tabKey(event: KeyboardEvent, tab: EditorTab): void {
    const step =
      event.key === 'ArrowDown' || event.key === 'ArrowRight' ? 1
        : event.key === 'ArrowUp' || event.key === 'ArrowLeft' ? -1 : 0;
    if (step === 0) return;
    event.preventDefault();
    const i = EDITOR_TABS.indexOf(tab);
    const next = EDITOR_TABS[
      (i + step + EDITOR_TABS.length) % EDITOR_TABS.length
    ] as EditorTab;
    this.setTab(next);
    this.#tabStrip.querySelector<HTMLElement>(`.dc-editor-tab[data-tab="${next}"]`)?.focus();
  }

  #footer(): HTMLElement {
    const bar = this.#doc.createElement('div');
    bar.className = 'dc-editor-footer';
    bar.append(
      button(this.#doc, 'Cancel', () => this.cancel()),
      button(this.#doc, 'Apply', () => void this.apply()),
      button(this.#doc, 'OK', () => void this.apply({ close: true }), {
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
  const names = allColumns(draft);
  const all = selectable(names);
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
    { all: selectable(groupableColumns(draft)), selected: draft.snapshot.pivotOn },
    (pivotOn) => ctx.setSnapshot(setPivot(ctx.draft(), pivotOn).snapshot),
    {
      actionFor: (name) =>
        dropdown(
          ctx.doc,
          columnConfig(ctx.draft().config, name).pivotSortDirection ?? 'asc',
          SORT_DIRECTION_OPTIONS,
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
  return panelShell(ctx.doc, TAB_LABELS['Horizontal Pivots'], body);
};

const verticalPivotsPanel: PanelBuilder = (ctx) => {
  const draft = ctx.draft();
  const body = selectorInto(
    ctx,
    { all: selectable(groupableColumns(draft)), selected: draft.snapshot.rows },
    (rows) => ctx.setSnapshot(setGrouping(ctx.draft(), rows).snapshot),
  );
  return panelShell(ctx.doc, TAB_LABELS['Vertical Pivots'], body);
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
  // READ LIVE on every render of a row: a map built with the panel showed a
  // direction the draft no longer held once a sort was removed and added
  // back, or another row was clicked (P2-171).
  const directionOf = (name: string): SortSpec['direction'] =>
    ctx.draft().snapshot.sorts.find((s) => s.column === name)?.direction ?? 'asc';
  const body = selectorInto(
    ctx,
    {
      all: selectable(allColumns(draft)),
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
      ctx.setSnapshot(setSorts(ctx.draft(), sorts).snapshot);
    },
    {
      labelFor: sortLabel,
      actionFor: (name) =>
        dropdown(
          ctx.doc,
          directionOf(name),
          SORT_DIRECTION_OPTIONS,
          (direction) => {
            const s = ctx.draft().snapshot;
            ctx.setSnapshot(setSorts(ctx.draft(), s.sorts.map((x) =>
              x.column === name
                ? { column: x.column, direction: direction ?? 'asc' }
                : x,
            )).snapshot);
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
