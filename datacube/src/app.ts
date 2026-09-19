// The application: the piece that makes the rest reachable.
//
// An audit of this codebase found that most of it was built, tested
// and unreachable -- the context menu, the heatmap, the HTML and
// Excel exporters, drill-through, selection statistics, saved views
// and named dimensions each had exactly one user, the file that
// defined it. They were libraries, not features. This is where they
// become features, and it lives in src/ rather than in demo/ for
// precisely that reason: a feature only a demo can reach is a
// feature nobody has.
//
// It owns the assembly and nothing else. The controller still owns
// the query, the editor still owns the draft, the grid still owns
// its DOM; this wires them to each other and to a toolbar, and every
// decision it makes is about WHEN to call them, never about what
// they mean.

import { CubeController, type CubeView, type Planner } from './cube.ts';
import {
  DEFAULT_CONFIGURATION,
  applyToSnapshot,
  columnConfig,
  fromSnapshot,
  labelFor,
  toColumnAppearance,
  toColumnLayout,
  toFormats,
  withColumn,
  type CubeConfiguration,
} from './config.ts';
import type { Dimension } from './dimensions.ts';
import { availableDimensions, useDimension } from './dimensions.ts';
import { drillQuery } from './drill.ts';
import type { QueryEngine } from './engine.ts';
import { toCsv } from './export.ts';
import { toHtml, toSpreadsheetML } from './export-rich.ts';
import { FormatterCache, type ColumnFormat } from './format.ts';
import { DataGrid } from './grid/grid.ts';
import {
  FloatingFilterRow,
  type FloatingFilterColumn,
} from './grid/floating-filter.ts';
import {
  PIVOT_SEPARATOR,
  TREE_COLUMN,
  buildColumnModel,
  type ColumnLayout,
} from './grid/columns.ts';
import { load, save, toJson, treeOf } from './persist.ts';
import { selectionStats, selectionTable, type CellRange } from './selection.ts';
import type { Scalar } from './result.ts';
import { kindOf, type CubeSnapshot, type FilterNode } from './snapshot.ts';
import { columnRange, heatColour } from './style.ts';
import type { HeatmapRange, HeatmapSpec } from './style.ts';
import { parsePathKey, pathKey, type TreeRow } from './tree.ts';
import { CubeEditor, draftFor, type CubeDraft } from './ui/editor.ts';
import { FilterEditor } from './ui/filter-editor.ts';
import { applyMenuAction, buildMenu, type MenuItem } from './ui/menu.ts';
import { MenuView } from './ui/menu-view.ts';
import { PivotPanel, type Zone } from './ui/pivot-panel.ts';
import { ColumnsToolPanel } from './ui/columns-panel.ts';

export interface CubeAppOptions {
  readonly engine: QueryEngine;
  readonly planner: Planner;
  /** Named hierarchies offered in the toolbar and the editor. */
  readonly dimensions?: readonly Dimension[];
  /**
   * The configuration to open on.
   *
   * A host knows things the snapshot does not -- that `notional` is
   * money and `qty` is a count -- and a grid that renders a trade
   * count as $10,005 is wrong in the way that looks plausible.
   */
  readonly configuration?: CubeConfiguration;
  readonly onStatus?: (text: string, kind: 'ok' | 'warn' | 'error') => void;
  /** Every view the controller produces, for a host's own chrome. */
  readonly onView?: (view: CubeView) => void;
  /** Called whenever the snap state changes, for a plane badge. */
  readonly onPlane?: () => void;
  /** Where saved views live. Absent means they are not offered. */
  readonly storage?: Pick<Storage, 'getItem' | 'setItem' | 'removeItem'>;
  readonly writeClipboard?: (text: string) => void | Promise<void>;
  /**
   * Hand a file to the user.
   *
   * Injected because a download is a host concern -- a page does it
   * with an anchor, an embedder may want a save dialog -- and
   * because a test must not depend on a browser writing to disk.
   */
  readonly download?: (name: string, mime: string, text: string) => void;
  /** Show the column drag zone. Off matches DataCube exactly. */
  readonly showColumnZone?: boolean;
}

const VIEW_KEY = 'datacube.savedView';

/** DataCube's --ag-row-height. Kept beside the CSS token in theme.css. */
const DATACUBE_ROW_HEIGHT = 20;

export class CubeApp {
  readonly #doc: Document;
  readonly #options: CubeAppOptions;
  readonly #controller: CubeController;
  readonly #grid: DataGrid;
  readonly #pivots: PivotPanel;
  readonly #menu: MenuView;
  readonly #filterRow: FloatingFilterRow;
  readonly #formatters = new FormatterCache();
  /**
   * Handed to the grid ONCE and mutated in place.
   *
   * The grid holds the reference and reads it per render, so
   * replacing the object would leave it formatting against the
   * configuration as it was when the grid was built -- which is how
   * every measure silently rendered unformatted the first time.
   */
  readonly #formats: Record<string, ColumnFormat> = {};
  /** Measured heatmap scales, by leaf index. See `#refreshHeatmaps`. */
  readonly #heatmaps = new Map<
    number,
    { spec: HeatmapSpec; byDepth: Map<number, HeatmapRange> }
  >();
  readonly #columnsPanel: ColumnsToolPanel;
  readonly #els: {
    toolbar: HTMLElement;
    grid: HTMLElement;
    overlay: HTMLElement;
    stats: HTMLElement;
  };
  #snapshot: CubeSnapshot;
  #config: CubeConfiguration = DEFAULT_CONFIGURATION;
  #view: CubeView | null = null;
  #treeRows: readonly TreeRow[] = [];
  #selection: CellRange | null = null;

  constructor(
    root: HTMLElement,
    snapshot: CubeSnapshot,
    options: CubeAppOptions,
  ) {
    this.#doc = root.ownerDocument;
    this.#options = options;
    this.#snapshot = snapshot;
    // Read the snapshot back rather than starting from defaults: a
    // cube can arrive from a saved view or a colleague's link, and
    // the editor must open on what is actually running.
    this.#config = fromSnapshot(snapshot, options.configuration);
    Object.assign(this.#formats, toFormats(this.#config));

    root.classList.add('dc-app');
    this.#els = {
      toolbar: this.#div(root, 'dc-app-toolbar'),
      grid: this.#doc.createElement('div'),
      overlay: this.#doc.createElement('div'),
      stats: this.#doc.createElement('div'),
    };
    this.#els.grid.className = 'dc-app-grid';
    this.#els.overlay.className = 'dc-app-overlay';
    this.#els.stats.className = 'dc-app-stats';

    // The zones sit between the toolbar and the grid, where ag-Grid
    // puts them, so a column dragged upward has somewhere obvious to
    // land. The tool panel sits to the right of the grid, where
    // DataCube's sidebar is.
    const zones = this.#div(root, 'dc-pivot-panel');
    const middle = this.#div(root, 'dc-app-middle');
    middle.append(this.#els.grid);
    const side = this.#doc.createElement('div');
    side.className = 'dc-app-side';
    middle.append(side);
    root.append(this.#els.overlay, this.#els.stats);
    this.#els.overlay.hidden = true;

    this.#columnsPanel = new ColumnsToolPanel(side, {
      labelFor: (c) => labelFor(this.#config, c),
      onPick: (c) => this.#onZoneChange('rows', [...this.#snapshot.rows, c]),
    });

    this.#pivots = new PivotPanel(zones, {
      canGroup: (c) => this.#isDimension(c),
      labelFor: (c) => labelFor(this.#config, c),
      onChange: (zone, columns) => this.#onZoneChange(zone, columns),
      ...(options.showColumnZone !== undefined
        ? { showColumnZone: options.showColumnZone }
        : {}),
    });

    this.#filterRow = new FloatingFilterRow(this.#doc, {
      onChange: (filter) => this.#setFilter(filter),
    });

    this.#menu = new MenuView(this.#doc, {
      onSelect: (item) => this.#onMenuAction(item),
    });

    this.#grid = new DataGrid(this.#els.grid, this.#formatters, {
      // Their --ag-row-height. The CSS token said 20 and this said
      // 24, and the JS wins because it sets the row's inline height
      // -- so the grid rendered four pixels too tall per row while
      // the stylesheet claimed otherwise.
      rowHeight: DATACUBE_ROW_HEIGHT,
      formats: this.#formats,
      appearance: this.#config.appearance,
      columnAppearance: toColumnAppearance(this.#config),
      floatingFilter: this.#filterRow,
      floatingFilterFor: (leaf) => this.#filterFor(leaf),
      canGroup: (c) => this.#isDimension(c),
      cellBackground: (leaf, row, value) => {
        const heat = this.#heatmaps.get(leaf.index);
        if (!heat) return null;
        return heatColour(
          value,
          heat.spec,
          this.#heatRange(leaf.index, row) ?? null,
        );
      },
      rowMeta: (abs) => this.#rowMeta(abs),
      onToggleExpand: (key) => {
        void this.#controller.toggle(parsePathKey(key));
      },
      onSelectionChange: (range) => this.#onSelectionChange(range),
      onActivateCell: (row) => {
        void this.#drillThrough(row);
      },
      ...(options.writeClipboard
        ? { writeClipboard: options.writeClipboard }
        : {}),
    });

    this.#controller = new CubeController(options.engine, options.planner, {
      onView: (view) => this.#onView(view),
      onError: (e) =>
        this.#status(
          e instanceof Error ? e.message : String(e),
          'error',
        ),
    });

    this.#wireContextMenu();
    this.#buildToolbar();
  }

  get controller(): CubeController {
    return this.#controller;
  }
  get configuration(): CubeConfiguration {
    return this.#config;
  }
  get snapshot(): CubeSnapshot {
    return this.#snapshot;
  }

  async open(): Promise<void> {
    await this.#refresh();
  }

  // -- assembly ------------------------------------------------------

  #div(parent: HTMLElement, className: string): HTMLElement {
    const el = this.#doc.createElement('div');
    el.className = className;
    parent.appendChild(el);
    return el;
  }

  #status(text: string, kind: 'ok' | 'warn' | 'error' = 'ok'): void {
    this.#options.onStatus?.(text, kind);
  }

  #isDimension(column: string): boolean {
    const spec = this.#snapshot.columns.find((c) => c.name === column);
    if (!spec) return false;
    return (columnConfig(this.#config, column).kind ?? kindOf(spec)) ===
      'dimension';
  }

  async #refresh(): Promise<void> {
    // Fold the configuration in HERE, once, so a setting that shapes
    // the query cannot reach the engine through one path and not
    // another.
    const next = applyToSnapshot(this.#snapshot, this.#config);
    this.#snapshot = next;
    this.#filterRow.setFilter(next.filter);
    this.#pivots.setColumns(next.rows, next.pivotOn);
    this.#refreshFormats();
    this.#refreshToolPanel();
    await this.#controller.update({ ...next, epoch: next.epoch + 1 });
  }

  /** Re-fill the format map in place. See `#formats`. */
  #refreshFormats(): void {
    for (const key of Object.keys(this.#formats)) delete this.#formats[key];
    const byColumn = toFormats(this.#config);
    Object.assign(this.#formats, byColumn);
    // A pivoted leaf is named after its MEASURE, not its source
    // column, so a format set on `notional` has to be copied onto
    // every `2021__|__notional` the pivot produced -- otherwise the
    // formats apply to a flat cube and silently stop at the first
    // pivot.
    const view = this.#view;
    if (!view) return;
    for (const leaf of view.columns.leaves) {
      if (leaf.isDimension) continue;
      const measure = leaf.path[leaf.path.length - 1];
      const format = measure !== undefined ? byColumn[measure] : undefined;
      if (format) this.#formats[leaf.name] = format;
    }
  }

  /**
   * The box a leaf column gets, if any.
   *
   * The tree column gets ONE box that matches any row dimension --
   * it holds a different dimension at every level, so no single
   * column could be its subject. A leaf that IS a source column
   * gets its own box. A pivoted measure gets none: its name is a
   * path the engine has never heard of, and filtering the measure's
   * source column instead would quietly mean something else.
   */
  #filterFor(leaf: {
    readonly name: string;
    readonly path: readonly string[];
  }): FloatingFilterColumn | null {
    if (leaf.name === TREE_COLUMN) {
      const rows = this.#snapshot.rows;
      return rows.length === 0
        ? null
        : { name: TREE_COLUMN, type: 'String', filterable: true, mode: 'tree', rows };
    }
    if (leaf.path.length > 1) return null;
    // A MEASURE's output often shares its source column's name --
    // `notional` is sum(notional) -- and a box there would filter
    // the rows feeding the aggregate rather than the aggregate
    // itself. Same name, different thing, so: no box.
    if (this.#snapshot.measures.some((m) => m.name === leaf.name)) return null;
    if ((this.#snapshot.groupDerived ?? []).some((d) => d.name === leaf.name)) {
      return null;
    }
    const spec = this.#snapshot.columns.find((c) => c.name === leaf.name);
    if (!spec) return null;
    return {
      name: spec.name,
      type: spec.type,
      filterable: true,
      mode: 'column',
    };
  }

  #refreshToolPanel(): void {
    const rows = new Set(this.#snapshot.rows);
    const cols = new Set(this.#snapshot.pivotOn);
    this.#columnsPanel.setColumns(
      this.#snapshot.columns.map((c) => ({
        name: c.name,
        type: c.type,
        groupable: this.#isDimension(c.name),
        ...(rows.has(c.name)
          ? { usedAs: 'rows' as const }
          : cols.has(c.name)
            ? { usedAs: 'columns' as const }
            : {}),
      })),
    );
  }

  #onView(view: CubeView): void {
    this.#view = view;
    this.#options.onView?.(view);
    this.#treeRows = view.treeRows;
    this.#snapshot = view.snapshot;

    const model = buildColumnModel(
      view.rows,
      view.snapshot.rows,
      view.snapshot.measures.map((m) => m.name),
      toColumnLayout(this.#config) as ColumnLayout,
    );
    // Refreshed AFTER the view lands, because the pivot's leaf names
    // are only known once the engine has answered.
    this.#refreshFormats();
    this.#refreshHeatmaps(view);
    this.#grid.setColumns(model);
    this.#grid.setRows(view.rows, 0, view.rows.rowCount);

    const base =
      `${view.rows.rowCount.toLocaleString()} rows × ` +
      `${model.leaves.length} cols in ${view.rows.elapsedMs.toFixed(0)}ms`;
    if (view.truncated.length > 0 && this.#config.showTruncationWarning) {
      // Saying WHICH level was cut matters: "some rows are missing"
      // sends someone hunting through the whole cube.
      this.#status(
        `${base} — showing the first ${this.#config.maxRows.toLocaleString()} of ` +
          `${view.truncated.length} level${view.truncated.length > 1 ? 's' : ''}`,
        'warn',
      );
    } else {
      this.#status(base, 'ok');
    }
  }

  #rowMeta(abs: number): {
    level: number;
    key: string;
    expanded?: boolean;
    isTotal?: boolean;
  } {
    const row = this.#treeRows[abs];
    if (!row) return { level: 1, key: String(abs) };
    return {
      level: row.depth,
      key: pathKey(row.path),
      ...(row.isGroup ? { expanded: row.expanded } : {}),
      ...(row.isTotal || row.level === 0 ? { isTotal: true } : {}),
    };
  }

  /**
   * Measure each heatmapped column, once per view, PER TREE DEPTH.
   *
   * Per column is not fine enough in a tree. A column holds the
   * grand total, its subtotals and its leaves all at once, and those
   * differ by orders of magnitude: measured together, the total
   * takes the strongest colour and every leaf washes out to nothing.
   * The screenshot of the first version showed exactly that -- a red
   * total row above a sheet of white. style.ts already gives the
   * reason per column beats per grid; the same argument carried one
   * level further gives per depth.
   *
   * So a cell is coloured against its SIBLINGS, which is the
   * comparison a reader is actually making.
   *
   * The range comes from the whole column, never the visible window:
   * deriving it from what happens to be on screen makes the colours
   * change as the user scrolls.
   */
  #refreshHeatmaps(view: CubeView): void {
    this.#heatmaps.clear();
    for (const leaf of view.columns.leaves) {
      const spec = this.#heatmapFor(leaf.name);
      if (!spec) continue;
      const values = view.rows.columns[leaf.index]?.values ?? [];

      if (spec.range) {
        // An explicitly fixed scale is the user's decision and is
        // applied whole, at every depth.
        this.#heatmaps.set(leaf.index, {
          spec,
          byDepth: new Map([[-1, spec.range]]),
        });
        continue;
      }

      const atDepth = new Map<number, Scalar[]>();
      values.forEach((value, row) => {
        const depth = this.#treeRows[row]?.depth ?? 0;
        const bucket = atDepth.get(depth);
        if (bucket) bucket.push(value);
        else atDepth.set(depth, [value]);
      });

      const byDepth = new Map<number, HeatmapRange>();
      for (const [depth, bucket] of atDepth) {
        const range = columnRange(bucket);
        if (range) byDepth.set(depth, range);
      }
      if (byDepth.size > 0) this.#heatmaps.set(leaf.index, { spec, byDepth });
    }
  }

  /** The scale a cell is measured against: its own depth's. */
  #heatRange(leafIndex: number, row: number): HeatmapRange | undefined {
    const heat = this.#heatmaps.get(leafIndex);
    if (!heat) return undefined;
    return (
      heat.byDepth.get(-1) ?? heat.byDepth.get(this.#treeRows[row]?.depth ?? 0)
    );
  }

  /**
   * A leaf's heatmap: its own, else its measure's.
   *
   * A pivoted leaf is named for its measure, so a setting made on
   * `notional` has to reach every `2021__|__notional` the pivot
   * produced -- otherwise the feature works on a flat cube and
   * silently stops at the first pivot, which is the bug this
   * replaced.
   */
  #heatmapFor(leafName: string): HeatmapSpec | undefined {
    const own = columnConfig(this.#config, leafName).heatmap;
    if (own) return own;
    const measure = leafName.split(PIVOT_SEPARATOR).pop();
    return measure !== undefined && measure !== leafName
      ? columnConfig(this.#config, measure).heatmap
      : undefined;
  }

  // -- the drag zones -------------------------------------------------

  #onZoneChange(zone: Zone, columns: readonly string[]): void {
    this.#snapshot =
      zone === 'rows'
        ? { ...this.#snapshot, rows: [...columns] }
        : { ...this.#snapshot, pivotOn: [...columns] };
    void this.#refresh();
  }

  #setFilter(filter: FilterNode | undefined): void {
    this.#snapshot = filter
      ? { ...this.#snapshot, filter }
      : (({ filter: _drop, ...rest }) => rest)(this.#snapshot);
    void this.#refresh();
  }

  // -- the context menu ------------------------------------------------

  #wireContextMenu(): void {
    this.#els.grid.addEventListener('contextmenu', (event) => {
      const target = event.target;
      if (!(target && 'closest' in (target as object))) return;
      const el = target as Element;
      const header = el.closest<HTMLElement>('[data-column]');
      const column = header?.dataset['column'];
      event.preventDefault();
      const groups = buildMenu({
        snapshot: this.#snapshot,
        ...(column !== undefined ? { column } : {}),
        isRowDimension: column
          ? this.#snapshot.rows.includes(column)
          : false,
        hasSelection: this.#selection !== null,
        hasExpanded: this.#controller.tree.openPaths.length > 0,
      });
      this.#menu.show(groups, event.clientX, event.clientY);
    });
  }

  #onMenuAction(item: MenuItem): void {
    // The query actions go through applyMenuAction, which returns the
    // SAME snapshot when nothing changed; the rest are layout,
    // clipboard and export, which never touch the query.
    const next = applyMenuAction(this.#snapshot, item);
    if (next !== this.#snapshot) {
      this.#snapshot = next;
      void this.#refresh();
      return;
    }
    const column = item.column;
    switch (item.id) {
      case 'tree.collapseAll':
        void this.#controller.setTree(this.#controller.tree.collapseAll());
        return;
      case 'column.hide':
        if (column) this.#patchColumn(column, { hidden: true });
        return;
      case 'column.pinLeft':
        if (column) this.#patchColumn(column, { pinned: 'left' });
        return;
      case 'column.pinRight':
        if (column) this.#patchColumn(column, { pinned: 'right' });
        return;
      case 'column.unpin':
        if (column) this.#patchColumn(column, { pinned: undefined });
        return;
      case 'column.unpinAll':
        for (const name of Object.keys(this.#config.columns)) {
          this.#config = withColumn(this.#config, name, { pinned: undefined });
        }
        void this.#refresh();
        return;
      case 'copy.selection':
        this.#copy(this.#selectionCsv());
        return;
      case 'copy.column':
        if (column) this.#copy(this.#columnCsv(column));
        return;
      case 'export.csv':
        this.#export('csv');
        return;
      case 'export.excel':
        this.#export('excel');
        return;
      case 'export.specification':
        this.#export('specification');
        return;
      case 'filter.column':
        this.openFilters();
        return;
      default:
        return;
    }
  }

  #patchColumn(
    column: string,
    patch: Parameters<typeof withColumn>[2],
  ): void {
    const next = withColumn(this.#config, column, patch);
    if (next === this.#config) return;
    this.#config = next;
    void this.#refresh();
  }

  // -- selection -------------------------------------------------------

  #onSelectionChange(range: CellRange | null): void {
    this.#selection = range;
    const table = this.#view?.rows;
    if (!range || !table || !this.#config.showSelectionStats) {
      this.#els.stats.textContent = '';
      return;
    }
    const s = selectionStats(table, range);
    // Blanks are reported rather than folded into the count, because
    // an average over a pivot region that treated empty combinations
    // as zero would be wrong in the direction of looking plausible.
    this.#els.stats.textContent =
      s.numeric === 0
        ? `${s.cells} cells, none numeric`
        : `sum ${fmt(s.sum)} · avg ${fmt(s.average)} · min ${fmt(s.min)} · ` +
          `max ${fmt(s.max)} · ${s.numeric} of ${s.cells} numeric` +
          (s.blank > 0 ? ` · ${s.blank} blank` : '');
  }

  #selectionCsv(): string {
    const table = this.#view?.rows;
    if (!table || !this.#selection) return '';
    return toCsv(selectionTable(table, this.#selection));
  }

  #columnCsv(column: string): string {
    const view = this.#view;
    if (!view) return '';
    const leaf = view.columns.leaves.find((l) => l.name === column);
    if (!leaf) return '';
    const src = view.rows.columns[leaf.index];
    if (!src) return '';
    return toCsv({
      columns: [src],
      rowCount: view.rows.rowCount,
      epoch: view.rows.epoch,
      elapsedMs: 0,
    });
  }

  #copy(text: string): void {
    if (text === '') return;
    void this.#options.writeClipboard?.(text);
    this.#status('copied', 'ok');
  }

  // -- drill-through ----------------------------------------------------

  /**
   * The rows behind one aggregate.
   *
   * A group row drills to its own group; a leaf drills to itself.
   * The query is built from the row's PATH rather than from its
   * rendered labels, because a formatted cell says "$1.2m" and the
   * engine needs the key.
   */
  async #drillThrough(row: number): Promise<void> {
    const view = this.#view;
    const meta = this.#treeRows[row];
    if (!view || !meta) return;
    const pure = drillQuery(view.snapshot, { path: meta.path });
    const sql = await this.#options.planner.plan(pure, view.snapshot);
    const table = await this.#options.engine.execute(
      sql,
      view.snapshot.epoch,
    );
    this.#showOverlay('Drill-through', (host) => {
      const pre = this.#doc.createElement('pre');
      pre.className = 'dc-drill';
      pre.textContent = toCsv(table);
      host.append(pre);
    });
  }

  // -- export -----------------------------------------------------------

  #export(kind: 'csv' | 'excel' | 'html' | 'specification'): void {
    const view = this.#view;
    if (!view) return;
    const title = this.#config.reportTitle ?? 'cube';
    const download = this.#options.download;
    if (!download) {
      this.#status('no download handler', 'error');
      return;
    }
    switch (kind) {
      case 'csv':
        download(`${title}.csv`, 'text/csv', toCsv(view.rows));
        return;
      case 'excel':
        // SpreadsheetML rather than CSV so numbers arrive as numbers;
        // a CSV of "1,234" opens as text in every locale that uses a
        // comma for the decimal point.
        download(
          `${title}.xls`,
          'application/vnd.ms-excel',
          toSpreadsheetML(view.rows, { title }),
        );
        return;
      case 'html':
        download(`${title}.html`, 'text/html', toHtml(view.rows, { title }));
        return;
      case 'specification':
        download(
          `${title}.json`,
          'application/json',
          toJson(
            save({
              name: title,
              snapshot: view.snapshot,
              tree: this.#controller.tree,
              columns: {
                ...(this.#config.columnOrder
                  ? { order: this.#config.columnOrder }
                  : {}),
                formats: toFormats(this.#config),
              },
            }),
          ),
        );
        return;
    }
  }

  // -- saved views --------------------------------------------------------

  saveView(name: string): void {
    const storage = this.#options.storage;
    if (!storage) return;
    storage.setItem(
      VIEW_KEY,
      toJson(
        save({
          name,
          snapshot: this.#snapshot,
          tree: this.#controller.tree,
          columns: {
            ...(this.#config.columnOrder
              ? { order: this.#config.columnOrder }
              : {}),
            formats: toFormats(this.#config),
          },
        }),
      ),
    );
    this.#status(`saved "${name}"`, 'ok');
  }

  async loadView(): Promise<void> {
    const storage = this.#options.storage;
    const raw = storage?.getItem(VIEW_KEY);
    if (!raw) {
      this.#status('no saved view', 'warn');
      return;
    }
    // A malformed or future view is reported, not thrown past the
    // user: a saved view is the one artefact a colleague hands over,
    // and "nothing happened" is the worst response to a bad one.
    try {
      const view = load(raw);
      this.#snapshot = view.snapshot;
      this.#config = fromSnapshot(view.snapshot, this.#config);
      await this.#controller.setTree(treeOf(view));
      await this.#refresh();
      this.#status(`loaded "${view.name}"`, 'ok');
    } catch (e) {
      this.#status(e instanceof Error ? e.message : String(e), 'error');
    }
  }

  // -- dimensions ----------------------------------------------------------

  useDimension(dimension: Dimension): void {
    this.#snapshot = useDimension(this.#snapshot, dimension);
    void this.#refresh();
  }

  // -- the dialogs ----------------------------------------------------------

  openEditor(): void {
    this.#showOverlay('Properties', (host) => {
      new CubeEditor(
        host,
        draftFor(this.#snapshot, this.#config, this.#options.dimensions ?? []),
        {
          onApply: (draft) => this.#applyDraft(draft),
          onClose: () => this.#closeOverlay(),
        },
      );
    });
  }

  openFilters(): void {
    this.#showOverlay('Filters', (host) => {
      new FilterEditor(host, {
        columns: this.#snapshot.columns.map((c) => c.name),
        ...(this.#snapshot.filter ? { value: this.#snapshot.filter } : {}),
        onChange: (filter) => this.#setFilter(filter),
      });
    });
  }

  #applyDraft(draft: CubeDraft): void {
    this.#snapshot = draft.snapshot;
    this.#config = draft.config;
    void this.#refresh();
  }

  #showOverlay(title: string, build: (host: HTMLElement) => void): void {
    const overlay = this.#els.overlay;
    overlay.hidden = false;
    overlay.replaceChildren();
    const head = this.#div(overlay, 'dc-overlay-head');
    const h = this.#doc.createElement('span');
    h.textContent = title;
    const close = this.#doc.createElement('button');
    close.type = 'button';
    close.className = 'dc-overlay-close';
    close.textContent = '×';
    close.setAttribute('aria-label', 'Close');
    close.addEventListener('click', () => this.#closeOverlay());
    head.append(h, close);
    build(this.#div(overlay, 'dc-overlay-body'));
    // Escape closes, because a modal a keyboard user cannot dismiss
    // is a trap, and the panels below already stop their own Escape
    // from reaching here.
    overlay.addEventListener('keydown', (event) => {
      if (event.key === 'Escape') this.#closeOverlay();
    });
  }

  #closeOverlay(): void {
    this.#els.overlay.hidden = true;
    this.#els.overlay.replaceChildren();
  }

  // -- the toolbar ------------------------------------------------------------

  #buildToolbar(): void {
    const bar = this.#els.toolbar;
    const add = (label: string, onClick: () => void, title?: string): void => {
      const b = this.#doc.createElement('button');
      b.type = 'button';
      b.className = 'dc-tool';
      b.textContent = label;
      if (title !== undefined) b.title = title;
      b.addEventListener('click', onClick);
      bar.append(b);
    };

    add('Properties…', () => this.openEditor(), 'Edit the cube (Ctrl+E)');

    // Snap belongs on the toolbar rather than in a demo page: it is
    // the product's own mode switch, and rule one of snap mode is
    // that what you are looking at is never inferable.
    const snap = this.#doc.createElement('button');
    snap.type = 'button';
    snap.className = 'dc-tool';
    const paint = (): void => {
      snap.textContent = this.#controller.snaps.isSnapped
        ? 'Release snap'
        : 'Snap';
    };
    snap.addEventListener('click', () => {
      snap.disabled = true;
      const done = (): void => {
        snap.disabled = false;
        paint();
        this.#options.onPlane?.();
      };
      const work = this.#controller.snaps.isSnapped
        ? this.#controller.release()
        : this.#controller.snap();
      work.then(done, (e: unknown) => {
        this.#status(e instanceof Error ? e.message : String(e), 'error');
        done();
      });
    });
    paint();
    bar.append(snap);
    add('Filters…', () => this.openFilters());
    add('Collapse all', () => {
      void this.#controller.setTree(this.#controller.tree.collapseAll());
    });
    add('Totals', () => {
      const tree = this.#controller.tree;
      void this.#controller.setTree(tree.withTotals(!tree.showTotals));
    });

    for (const [label, kind] of [
      ['CSV', 'csv'],
      ['Excel', 'excel'],
      ['HTML', 'html'],
      ['Spec', 'specification'],
    ] as const) {
      add(label, () => this.#export(kind), `Export as ${label}`);
    }

    if (this.#options.storage) {
      add('Save view', () =>
        this.saveView(this.#config.reportTitle ?? 'view'),
      );
      add('Load view', () => {
        void this.loadView();
      });
    }

    const dimensions = availableDimensions(
      this.#snapshot,
      this.#options.dimensions ?? [],
    );
    for (const d of dimensions) {
      add(d.name, () => this.useDimension(d), `Drill ${d.name}`);
    }

    this.#doc.addEventListener('keydown', (event) => {
      if ((event.ctrlKey || event.metaKey) && event.key === 'e') {
        event.preventDefault();
        this.openEditor();
      }
    });
  }
}

function fmt(n: number): string {
  if (!Number.isFinite(n)) return '—';
  return n.toLocaleString(undefined, { maximumFractionDigits: 2 });
}
