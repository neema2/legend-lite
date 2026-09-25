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

import type { QueryRunner } from './runner.ts';
import {
  CubeController,
  type CubeControllerOptions,
  type CubeView,
  type Planner,
} from './cube.ts';
import {
  DEFAULT_CONFIGURATION,
  applyToSnapshot,
  columnConfig,
  mergeColumnOrder,
  fromSnapshot,
  labelFor,
  toColumnAppearance,
  toColumnLayout,
  renderFormats,
  toFormats,
  DEFAULT_MAX_ROWS,
  renameColumnConfig,
  withColumn,
  withSettings,
  type CubeConfiguration,
  type Patch,
} from './config.ts';
import type { Dimension } from './dimensions.ts';
import { availableDimensions, useDimension } from './dimensions.ts';
import { drillQuery } from './drill.ts';
import type { QueryEngine } from './engine.ts';
import { exportFileName, toCsv, toEml } from './export.ts';
import { ALERT_WINDOW, buildAlert, type AlertOptions } from './ui/alert.ts';
import { toHtml, toSpreadsheetML } from './export-rich.ts';
import { toPdf, toPlainText } from './export-doc.ts';
import { toBarChart, toTreemap } from './chart.ts';
import { FormatterCache, type ColumnFormat } from './format.ts';
import { DataGrid } from './grid/grid.ts';
import {
  PIVOT_SEPARATOR,
  TREE_COLUMN,
  buildColumnModel,
  type ColumnLayout,
  type ColumnModel,
  type LeafColumn,
} from './grid/columns.ts';
import { load, save, toJson, treeOf } from './persist.ts';
import { isPivotTotalColumn, pivotTotalColumn } from './treeview.ts';
import { selectionStats, selectionTable, type CellRange } from './selection.ts';
import type { Scalar } from './result.ts';
import {
  renameColumnReferences,
  rowColumns,
  type ColumnKind,
  type CubeSnapshot,
  type DerivedColumn,
  type FilterNode,
  type FilterValue,
} from './snapshot.ts';
import { columnRange, heatColour } from './style.ts';
import type { HeatmapRange, HeatmapSpec } from './style.ts';
import { TreeState, parsePathKey, pathKey, type TreeRow } from './tree.ts';
import { ColumnEditor, type ColumnEditorStart } from './ui/column-editor.ts';
import { columnRef } from './calc.ts';
import { CubeEditor, draftFor, type CubeDraft } from './ui/editor.ts';
import { FilterEditor } from './ui/filter-editor.ts';
import {
  applyMenuAction,
  buildMenu,
  calcStageOf,
  type MenuItem,
} from './ui/menu.ts';
import { MenuView } from './ui/menu-view.ts';
import { makeWindow, type WindowOptions, type WindowSpec } from './ui/window.ts';
import {
  PivotPanel,
  currentHeaderDrag,
  type Zone,
} from './ui/pivot-panel.ts';
import {
  ColumnsToolPanel,
  type ColumnsPanelChild,
} from './ui/columns-panel.ts';

/**
 * How this cube turns Pure into rows -- one of two arrangements.
 *
 * A planner and a local engine, which is both browser planes; or a
 * runner, which is how the remote-engine plane is built
 * (`new RemoteRun(executor)`). Spelled as a union so the two forms
 * are visible in the type rather than enforced by a comment, and so
 * a caller cannot pass half of each.
 */
export type CubeAppQuerySource =
  | {
    readonly engine: QueryEngine;
    readonly planner: Planner;
    readonly runner?: undefined;
  }
  | {
    readonly runner: QueryRunner;
    readonly engine?: undefined;
    readonly planner?: undefined;
  };

export interface CubeAppBaseOptions {
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
  /**
   * The host's own readout, at the right of the STATUS bar.
   *
   * It used to sit in the title bar, which put a host's progress
   * text and a moving row count in the one strip that should say
   * what you are looking at. The status bar is where a readout
   * belongs, so that is where the slot is. Called on every render
   * with the element to append to -- the bar is rebuilt each time,
   * so the host appends the same node again and it moves.
   */
  readonly hostStatus?: (slot: HTMLElement) => void;
  /**
   * The host's own entries, at the foot of the title bar menu.
   *
   * Built on each open, so a label can reflect state. Their ids are
   * the host's to choose, and `onHostMenu` receives whichever was
   * picked -- anything the cube does not recognise is routed there
   * rather than silently ignored.
   */
  readonly hostMenu?: () => readonly MenuItem[];
  readonly onHostMenu?: (item: MenuItem) => void;
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
  /**
   * Hand a message with an attachment to the host's mail client.
   *
   * A host concern, and unavoidably so: a browser cannot attach a
   * file to a mailto: link, so there is no in-page implementation to
   * fall back to. Absent means the Email entries are DISABLED rather
   * than missing -- the capability is legible either way.
   */
  readonly email?: (message: {
    readonly subject: string;
    readonly body: string;
    readonly attachment: {
      readonly name: string;
      readonly mime: string;
      readonly content: string;
    };
  }) => void | Promise<void>;
  /** Show the column drag zone. Off matches DataCube exactly. */
  readonly showColumnZone?: boolean;
  /**
   * Where a snap materialises, when the source is a model relation.
   *
   * While snapped the query is still planned as Pure, so the frozen
   * relation has to be one the model declares -- a generated
   * `dc_snap_1` is a SQL identifier and means nothing to a compiler.
   */
  readonly snapTarget?: { readonly table: string; readonly expression: string };
}

/**
 * Everything the app needs, and exactly one way of getting rows.
 *
 * The intersection is what makes "a planner and an engine" and "a
 * runner" both complete and mutually exclusive: pass half of each and
 * it does not compile.
 */
export type CubeAppOptions = CubeAppBaseOptions & CubeAppQuerySource;

const VIEW_KEY = 'datacube.savedView';

/** DataCube's --ag-row-height. Kept beside the CSS token in theme.css. */
const DATACUBE_ROW_HEIGHT = 20;

/**
 * Drop a pivot cast that belongs to a different pivot.
 *
 * The cast names columns a particular pivot produced, so changing
 * what the cube pivots on invalidates it -- and naming columns the
 * next pivot will not produce is a query the engine refuses. Keyed on
 * the pivot columns rather than cleared everywhere, so a change that
 * leaves the pivot alone (a filter, a sort, a new row dimension)
 * keeps the cast and does not cost an extra round trip.
 */
function forgetStaleCast(
  from: CubeSnapshot,
  next: CubeSnapshot,
): CubeSnapshot {
  if (next === from || next.pivotCast === undefined) return next;
  if (next.pivotOn.join('\u0000') === from.pivotOn.join('\u0000')) return next;
  const { pivotCast: _drop, ...rest } = next;
  return rest;
}

/** Room for the cell's own padding and its border. */
const AUTO_SIZE_PAD = 8;
/** Narrow enough to be honest, wide enough to still be grabbable. */
const AUTO_SIZE_MIN = 48;
/** One long free-text column must not push the rest off screen. */
const AUTO_SIZE_MAX = 480;
/** Upstream's DEFAULT_COLUMN_MIN_WIDTH: what Minimize shrinks to. */
const MINIMIZED_WIDTH = 50;

/**
 * What the result is and what it cost, in one line.
 *
 * Shared by the status bar and by `onStatus`, so the figure a host
 * reports and the figure on screen cannot drift apart.
 */
function timingText(view: CubeView, cols: number): string {
  return (
    `${view.rows.rowCount.toLocaleString()} rows × ` +
    `${cols} cols in ${view.rows.elapsedMs.toFixed(0)}ms`
  );
}

export class CubeApp {
  readonly #doc: Document;
  readonly #options: CubeAppOptions;
  readonly #controller: CubeController;
  readonly #grid: DataGrid;
  readonly #pivots: PivotPanel;
  /** The same zones again, as a list in the sidebar. */
  readonly #sideZones: PivotPanel;
  readonly #menu: MenuView;
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
    root: HTMLElement;
    /** The strip holding the zones and the control that folds it. */
    zoneBar: HTMLElement;
    grid: HTMLElement;
    stats: HTMLElement;
  };
  /**
   * The zones, shown for the length of a drag that needs them.
   *
   * Folded away they are not merely invisible, they are not there --
   * so a person who folds them and then drags a column header has
   * nowhere to drop it. The bar comes back while the drag is in
   * flight and folds itself again afterwards, which means folding it
   * takes nothing away.
   */
  #zonePeek = false;
  /**
   * Where each dialog was last left, by title.
   *
   * Reopening one should find it where you put it; a window that
   * jumps back to the middle every time is one you have to move
   * again every time.
   */
  readonly #windows = new Map<string, WindowSpec>();
  /** The open Properties editor, to move it to a column. */
  #editor: CubeEditor | null = null;
  /** The windows on screen, by title. See `#showOverlay`. */
  readonly #open = new Map<string, HTMLElement>();
  /** The z-index the most recently touched window was given. */
  #zTop = 20;
  /** Where the grid was scrolled when the context menu opened. */
  #menuScroll: { top: number; left: number } | null = null;
  /** The open calculated-column editors, by window key. */
  readonly #columnEditors = new Map<string, ColumnEditor>();
  #newColumns = 0;
  /** Alerts are many and untitled, so each gets its own window key. */
  #alerts = 0;
  #snapshot: CubeSnapshot;
  #config: CubeConfiguration = DEFAULT_CONFIGURATION;
  #view: CubeView | null = null;
  #treeRows: readonly TreeRow[] = [];
  #selection: CellRange | null = null;
  /** Where selection statistics are written, inside the status bar. */
  #statsSlot: HTMLElement | null = null;
  /** Repaints the plane toggle, which is now the only plane badge. */
  #paintSnap: (() => void) | null = null;

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
    Object.assign(this.#formats, renderFormats(this.#config, this.#snapshot));

    root.classList.add('dc-app');
    this.#els = {
      // The window container: a dialog is positioned inside the app,
      // not inside the document, so it cannot wander off over the
      // host's own page furniture.
      root,
      toolbar: this.#div(root, 'dc-titlebar'),
      // Replaced immediately below, once the toolbar exists to sit
      // above it. Declared here so the map has one shape.
      zoneBar: this.#doc.createElement('div'),
      grid: this.#doc.createElement('div'),
      stats: this.#doc.createElement('div'),
    };
    this.#els.grid.className = 'dc-app-grid';
    this.#els.stats.className = 'dc-app-stats';

    // The zones sit between the toolbar and the grid, where ag-Grid
    // puts them, so a column dragged upward has somewhere obvious to
    // land. The tool panel sits to the right of the grid, where
    // DataCube's sidebar is.
    // A STRIP, holding the zones and the one control that folds
    // them. The zones themselves belong to PivotPanel, which
    // replaces its children on every render, so the control cannot
    // live inside it -- and should not: what is on screen is the
    // app's business, what is in a zone is the panel's.
    const zoneBar = this.#div(root, 'dc-zone-bar');
    const zones = this.#div(zoneBar, 'dc-pivot-panel');
    this.#els.zoneBar = zoneBar;
    zoneBar.append(this.#zoneFold());
    const middle = this.#div(root, 'dc-app-middle');
    middle.append(this.#els.grid);
    const side = this.#doc.createElement('div');
    side.className = 'dc-app-side';
    middle.append(side);
    root.append(this.#els.stats);

    this.#columnsPanel = new ColumnsToolPanel(side, {
      labelFor: (c) => labelFor(this.#config, c),
      onPick: (c) => this.#onZoneChange('rows', [...this.#snapshot.rows, c]),
      onVisibility: (c, visible) => this.#patchColumn(c, { hidden: !visible }),
      // REORDERING WHERE THE LIST IS, and the same merge the grid's
      // own header drag goes through: the panel reports the columns
      // it lists, which is not all of them either.
      onReorder: (order) => {
        void this.applyConfiguration({
          columnOrder: mergeColumnOrder(this.#columnOrder(), order),
        });
      },
      // Dragged out of a zone and back to the list: off that axis.
      onRemoveFromZone: (zone, column) => {
        const next = (zone === 'rows'
          ? this.#snapshot.rows
          : this.#snapshot.pivotOn).filter((c) => c !== column);
        this.#onZoneChange(zone, next);
      },
    });

    // THE SAME ZONES, DOWN THE SIDEBAR.
    //
    // Three sections of one surface -- row groups, column labels and
    // the columns themselves -- so the whole shape of the cube can
    // be dragged into place in one place. They are a second
    // RENDERING of `PivotPanel`, not a second implementation: both
    // read the same snapshot and both mean the same thing by a drop,
    // which two copies of this logic would not stay agreed on.
    this.#sideZones = new PivotPanel(this.#columnsPanel.zones, {
      canGroup: (c) => this.#isDimension(c),
      labelFor: (c) => labelFor(this.#config, c),
      onChange: (zone, columns) => this.#onZoneChange(zone, columns),
      orientation: 'list',
      showColumnZone: true,
    });

    this.#pivots = new PivotPanel(zones, {
      canGroup: (c) => this.#isDimension(c),
      labelFor: (c) => labelFor(this.#config, c),
      onChange: (zone, columns) => this.#onZoneChange(zone, columns),
      ...(options.showColumnZone !== undefined
        ? { showColumnZone: options.showColumnZone }
        : {}),
    });

    this.#menu = new MenuView(this.#doc, {
      onSelect: (item) => this.#onMenuAction(item),
    });
    // Upstream's menu goes when the grid scrolls: it names the cell
    // under the pointer, and a scroll moves another cell there. Scroll
    // does not bubble, so this listens on the way down.
    //
    // Only when the grid's VIEW moved since the menu opened -- the
    // body scroller's position, whatever element fired. The right-click
    // that opened the menu can scroll on its own (a half-visible cell
    // brought into view), and that event lands a frame AFTER the menu
    // is up; the header, which follows the body sideways by setting
    // its own scrollLeft, fires one more. Either closed the menu the
    // user had just asked for (2026-09-25 harness, twice).
    this.#els.grid.addEventListener('scroll', () => {
      if (!this.#menu.open) return;
      const at = this.#menuScroll;
      const scroller = this.#els.grid.querySelector<HTMLElement>('.dc-scroller');
      if (at && scroller && scroller.scrollTop === at.top
        && scroller.scrollLeft === at.left) return;
      this.#menu.close();
    }, { capture: true });

    this.#grid = new DataGrid(this.#els.grid, this.#formatters, {
      // Their --ag-row-height. The CSS token said 20 and this said
      // 24, and the JS wins because it sets the row's inline height
      // -- so the grid rendered four pixels too tall per row while
      // the stylesheet claimed otherwise.
      rowHeight: DATACUBE_ROW_HEIGHT,
      formats: this.#formats,
      appearance: this.#config.appearance,
      columnAppearance: toColumnAppearance(this.#config),
      canGroup: (c) => this.#isDimension(c),
      onReorder: (order, added) => {
        // A COLUMN DRAGGED OUT OF THE PANEL INTO THE GRID IS A
        // REQUEST TO SHOW IT. The grid reports the arrival because
        // it cannot know the column was hidden -- it was never in
        // the model -- and unhiding is the configuration's to do.
        if (added !== undefined) {
          this.#config = withColumn(this.#config, added, { hidden: false });
        }
        // MERGED, not written over. The grid reports only what it is
        // showing, so writing its report straight in dropped every
        // grouped, pivoted and hidden column out of the order -- and
        // the panel, which sorts by it, threw them to the end of the
        // list. One drag and the grouped columns jumped.
        void this.applyConfiguration({
          columnOrder: mergeColumnOrder(this.#columnOrder(), order),
        });
      },
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
      onHeaderSort: (column) => this.#sortByHeader(column),
      // A dragged edge is a width like Auto-size's -- resizable again,
      // unlike an explicit Fixed one.
      onResizeColumn: (column, width) => this.#patchColumn(column, { width }),
      headerTitle: (path, leaf) => this.#headerTitle(path, leaf),
      treeTitle: (_row, label) => (label === '' ? '' : `Group Value = ${label}`),
      ...(options.writeClipboard
        ? { writeClipboard: options.writeClipboard }
        : {}),
    });

    // ONE SET OF DEPENDENCIES, then the arrangement that runs the
    // queries. Written once rather than per form: two copies of this
    // object would drift, and the half that drifted would be the one
    // nobody's plane exercised.
    const deps: CubeControllerOptions = {
      ...(options.snapTarget ? { snapTarget: options.snapTarget } : {}),
      onView: (view) => this.#onView(view),
      // Upstream's "Loading..." overlay while a query runs.
      onBusy: (busy) => this.#grid.setBusy(busy),
      onError: (e) =>
        this.#status(
          e instanceof Error ? e.message : String(e),
          'error',
        ),
      // The configuration is the host's half of the undoable state.
      // Without this pair, undo reverts the query and leaves the
      // pins, widths, colours and row cap where they were -- and for
      // a setting that shapes the query, the stale config is folded
      // back in on the next refresh, undoing the undo.
      captureHost: () => this.#config,
      restoreHost: (host) => {
        this.#config = host as CubeConfiguration;
        this.#refreshFormats();
        this.#refreshToolPanel();
      },
    };
    // NARROWED BY THE UNION, spelled out so the reader sees the two
    // forms as the type does: a runner, or a planner with a local
    // engine. There is no third state.
    this.#controller = options.runner !== undefined
      ? new CubeController(options.runner, deps)
      : new CubeController(
        options.engine as QueryEngine,
        options.planner as Planner,
        deps,
      );
    // The tree starts as the CONFIGURATION says, not as the defaults:
    // a host that asks for the root total or an expand level got
    // neither until the editor was applied once.
    this.#controller.adoptTree(TreeState.empty(this.#config.showRootAggregation)
      .withExpandTo(this.#config.initialExpandToLevel ?? 0));

    this.#wireContextMenu();
    this.#buildToolbar();
    this.#applyChrome();
    // A DRAG BRINGS THE ZONES BACK. Both listeners are on the app's
    // own root, so a header drag from the grid and a chip drag from
    // the bar are the same event to this.
    root.addEventListener('dragstart', () => {
      // WHETHER IT CAN LAND AT ALL, for the length of the drag. A
      // measure cannot be grouped by, and the zones used to refuse
      // it in silence -- so dragging notional into Row Groups looked
      // like a product that does not support dragging.
      const drag = currentHeaderDrag();
      root.classList.toggle(
        'dc-drag-nogroup',
        drag !== null && !this.#isDimension(drag.column),
      );
      if (this.#config.showDragZones) return;
      this.#zonePeek = true;
      this.#applyChrome();
    });
    const unpeek = (): void => {
      root.classList.remove('dc-drag-nogroup');
      if (!this.#zonePeek) return;
      this.#zonePeek = false;
      this.#applyChrome();
    };
    root.addEventListener('dragend', unpeek);
    root.addEventListener('drop', unpeek);
    // ADOPT THE HOST'S READOUT NOW, not on the first render: a cube
    // whose first query FAILS never renders a status bar, and that
    // is exactly when the host has something to say. The bar is
    // rebuilt per render and re-adopts it there.
    this.#adoptHostStatus();
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
    return this.#kindOf(column) === 'dimension';
  }

  /**
   * A column's kind, over source AND row-stage calculated columns.
   *
   * Undefined for anything that does not exist before aggregation --
   * a group-stage calculated column, a pivot's generated column --
   * which is therefore neither groupable nor extendable. A calculated
   * column's kind is the one declared in its editor; the
   * configuration's override is for source columns.
   */
  #kindOf(column: string): ColumnKind | undefined {
    const c = rowColumns(this.#snapshot).find((x) => x.name === column);
    if (!c) return undefined;
    return c.derived
      ? c.kind
      : columnConfig(this.#config, column).kind ?? c.kind;
  }

  async #refresh(): Promise<void> {
    // Fold the configuration in HERE, once, so a setting that shapes
    // the query cannot reach the engine through one path and not
    // another.
    const next = applyToSnapshot(this.#snapshot, this.#config);
    this.#snapshot = next;
    this.#pivots.setColumns(next.rows, next.pivotOn);
    this.#sideZones.setColumns(next.rows, next.pivotOn);
    this.#refreshFormats();
    // Fonts, colours, grid lines and row highlights: the grid held the
    // appearance it was built with until this existed.
    this.#grid.setAppearance(this.#config.appearance, toColumnAppearance(this.#config));
    this.#refreshToolPanel();
    await this.#controller.update({ ...next, epoch: next.epoch + 1 });
  }

  /**
   * Refresh, and put a failure where the user can see it.
   *
   * `void this.#refresh()` was a FLOATING promise at six call sites,
   * and the query it starts can legitimately refuse -- a pivot with
   * nothing to aggregate, a filter the type checker rejects. A
   * refusal there became an uncaught error in the console: the grid
   * went on showing the previous answer with no hint that the click
   * had failed.
   *
   * Worse, the snapshot that caused it STAYED. Every later query
   * carried the same bad shape and threw the same refusal, so a
   * single click on one menu entry wedged the cube until a reload.
   * That is why `previous` is not optional at the call sites that
   * changed the query: rolling back is what keeps one refused action
   * from ending the session.
   */
  #refreshOr(previous: CubeSnapshot | null): void {
    this.#refresh().catch((error: unknown) => {
      this.#status(
        error instanceof Error ? error.message : String(error),
        'error',
      );
      if (previous) this.#snapshot = previous;
    });
  }

  /**
   * Learn the pivot's own column names, and re-run once with them.
   *
   * A cube that is both grouped and pivoted needs two stages --
   * pivot, then a groupBy naming the pivot's output columns -- and
   * those names (`2021__|__notional`) do not exist until the pivot
   * has run. So the first query goes without the outer groupBy and
   * its RESULT supplies the names; this then re-runs once with them,
   * and the carried columns appear. DataCube does the same thing
   * through `pivot.castColumns`.
   *
   * The names come from the column MODEL rather than from the raw
   * result, because the model has already worked out which leaves
   * are pivoted and which measure each came from -- including for
   * the engine's own `2021_notional` spelling, where splitting on
   * the separator would find nothing.
   *
   * It cannot loop: the second query's pivot stage is identical, so
   * it produces the same names and the comparison below stops.
   */
  #syncPivotCast(model: ColumnModel): void {
    const snapshot = this.#snapshot;
    if (snapshot.pivotOn.length === 0 || snapshot.rows.length === 0) return;
    // EVERY leaf the result carried, not the visible ones: the cast
    // describes the shape of the ANSWER, and reading it off the grid
    // made hiding a pivot column narrow the next query -- which took
    // the column out of the data, where nothing could get it back.
    // Not the pivot TOTAL: it is joined in from a second query, and the
    // pivot does not produce it -- a cast naming it is refused.
    const cast = model.all
      .filter((l) => !l.isDimension && l.path.length > 1
        && !isPivotTotalColumn(l.name))
      .map((l) => ({
        name: l.name,
        measure: l.path[l.path.length - 1] ?? l.name,
      }));
    if (cast.length === 0) return;
    const key = (c: readonly { name: string; measure: string }[]): string =>
      c.map((x) => `${x.name}\u0000${x.measure}`).join('|');
    if (key(cast) === key(snapshot.pivotCast ?? [])) return;

    // The PREVIOUS snapshot is handed to the refresh so that a query
    // the engine rejects takes the cast back out with it. Otherwise a
    // cast that does not match the data -- the pivot column changed,
    // say -- would be retried on every refresh and the cube would be
    // stuck reporting the same failure.
    this.#snapshot = { ...snapshot, pivotCast: cast };
    this.#refreshOr(snapshot);
  }

  /** Re-fill the format map in place. See `#formats`. */
  #refreshFormats(): void {
    for (const key of Object.keys(this.#formats)) delete this.#formats[key];
    const byColumn = renderFormats(this.#config, this.#snapshot);
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
   * The order of EVERY column, not just the ones on screen.
   *
   * The configured order where there is one, and the cube's declared
   * order behind it, so a column the order predates still has a
   * place rather than being treated as unlisted.
   */
  #columnOrder(): string[] {
    const declared = this.#snapshot.columns.map((c) => c.name);
    const configured = this.#config.columnOrder ?? [];
    return [
      ...configured.filter((n) => declared.includes(n)),
      ...declared.filter((n) => !configured.includes(n)),
    ];
  }

  #refreshToolPanel(): void {
    const rows = new Set(this.#snapshot.rows);
    const cols = new Set(this.#snapshot.pivotOn);
    // IN THE GRID'S ORDER. The panel listed the cube's declared
    // columns, so dragging a header to reorder moved the column on
    // screen and left the panel beside it saying something else --
    // and the panel is the list people read to find a column. Same
    // rule the grid uses: what the order names comes first, in that
    // order, and anything it does not keeps declared order behind.
    const order = this.#config.columnOrder;
    // Calculated columns are columns: listed so they can be ticked,
    // dragged into a zone, and found. A group-stage one is listed too
    // -- it is on screen -- and `#isDimension` keeps it out of the
    // zones, since it only exists after the groupBy.
    const known = [
      ...rowColumns(this.#snapshot).map((c) => ({
        name: c.name, type: c.type ?? 'Derived' })),
      ...(this.#snapshot.groupDerived ?? []).map((d) => ({
        name: d.name, type: d.type ?? 'Derived' })),
    ];
    const listed = order
      ? [...known].sort((a, b) => {
          const ia = order.indexOf(a.name);
          const ib = order.indexOf(b.name);
          if (ia === -1 && ib === -1) return 0;
          if (ia === -1) return 1;
          if (ib === -1) return -1;
          return ia - ib;
        })
      : known;
    // THE PIVOT'S OWN COLUMNS, under the measure they came from.
    //
    // A pivoted measure is not one column in the grid: it is one per
    // value of the pivot key, and the panel said "notional" once
    // while the grid showed five of it. `pivotCast` is the list, and
    // each entry already carries its measure, so nothing here has to
    // know how a pivot name is spelled.
    const cast = this.#snapshot.pivotCast ?? [];
    // THE MEASURE'S SOURCE COLUMN, not the measure's name. A measure
    // is `{ name: 'total', column: 'notional' }` as often as it is
    // `notional` twice over, and the cast carries the NAME -- so
    // matching the panel's source columns against it found nothing
    // whenever a cube named its measures.
    const sourceOf = (measure: string): string =>
      this.#snapshot.measures.find((m) => m.name === measure)?.column
        ?? measure;
    // Several measures can come off one column -- a sum and an
    // average of notional -- and then the values alone name two
    // children the same. Their measure tells them apart.
    const perColumn = new Map<string, number>();
    for (const m of new Set(cast.map((c) => c.measure))) {
      const column = sourceOf(m);
      perColumn.set(column, (perColumn.get(column) ?? 0) + 1);
    }
    // Each measure's pivot TOTAL is one more of its columns on screen,
    // listed last so it can be hidden like any other.
    const totalsShown = new Set((this.#view?.columns.all ?? [])
      .filter((l) => isPivotTotalColumn(l.name))
      .map((l) => l.name));
    const totalLabel = this.#config.pivotStatisticColumnName ?? 'Total';
    const childrenOf = (column: string): ColumnsPanelChild[] => [
      ...cast
        .filter((c) => sourceOf(c.measure) === column)
        .map((c) => {
          const suffix = `${PIVOT_SEPARATOR}${c.measure}`;
          const values = c.name.endsWith(suffix)
            ? c.name
              .slice(0, -suffix.length)
              .split(PIVOT_SEPARATOR)
              .join(' \u203a ')
            : c.name;
          return {
            name: c.name,
            // The values alone where the measure is the row above,
            // and the measure too where the row above covers more
            // than one.
            label: (perColumn.get(column) ?? 1) > 1
              ? `${values} \u00b7 ${c.measure}`
              : values,
            visible: columnConfig(this.#config, c.name).hidden !== true,
          };
        }),
      ...[...new Set(cast.map((c) => c.measure))]
        .filter((m) => sourceOf(m) === column
          && totalsShown.has(pivotTotalColumn(m)))
        .map((m) => ({
          name: pivotTotalColumn(m),
          label: (perColumn.get(column) ?? 1) > 1
            ? `${totalLabel} \u00b7 ${m}`
            : totalLabel,
          visible: columnConfig(this.#config, pivotTotalColumn(m)).hidden !== true,
        })),
    ];

    // THE COLUMNS SECTION IS THE GRID'S COLUMNS, and the axes have
    // sections of their own now.
    //
    // A pivot key's values ARE the column headers, so it cannot also
    // be a column; a row dimension's values are the tree's. Listing
    // them here meant a tick box that could only lie -- it was
    // disabled and labelled with the reason, which is a worse answer
    // than putting the column where it actually lives. A row group
    // KEPT as a column is a real column, so it appears in both,
    // which is exactly what the setting means.
    const keptGrouped = this.#config.showGroupedColumns;
    this.#columnsPanel.setColumns(
      listed
        .filter((c) => !cols.has(c.name))
        .filter((c) => keptGrouped || !rows.has(c.name))
        .map((c) => {
          const children = childrenOf(c.name);
          const hidden = columnConfig(this.#config, c.name).hidden === true;
          return {
            name: c.name,
            type: c.type,
            groupable: this.#isDimension(c.name),
            visible: !hidden,
            ...(children.length > 0 ? { children } : {}),
            ...(rows.has(c.name) ? { usedAs: 'rows' as const } : {}),
          };
        }),
    );
  }

  #onView(view: CubeView): void {
    this.#view = view;
    this.#options.onView?.(view);
    this.#treeRows = view.treeRows;
    const changed = this.#snapshot !== view.snapshot;
    this.#snapshot = view.snapshot;
    // Open column editors compile against the cube as it is now.
    if (changed) for (const editor of this.#columnEditors.values()) editor.recheck();
    if (this.#syncCalcTypes(view)) return;

    const model = buildColumnModel(
      view.rows,
      view.snapshot.rows,
      view.snapshot.measures.map((m) => m.name),
      {
        ...(toColumnLayout(this.#config) as ColumnLayout),
        // THE CUBE'S ORDER, not the answer's.
        //
        // Leaves were ordered by their position in the RESULT, and a
        // pivoted cube's result puts the pivot's own columns before
        // the ones it carried through -- upstream's `_groupByAggCols`
        // emits them in that order too. So pivoting threw the
        // columns into a new order: every year block first, then
        // trade_id, quarter and the rest behind them.
        //
        // DataCube does not have the problem because its grid never
        // takes order from the query: `columnDefs:
        // generateColumnDefs(snapshot, configuration)` builds them in
        // CONFIGURATION order, and the result's column order is
        // nobody's business but the engine's. So the declared order
        // is the default here, and an explicit reorder still wins.
        order: this.#config.columnOrder
          ?? view.snapshot.columns.map((c) => c.name),
        // Horizontal Pivots > each key's sort direction: the order of
        // that key's VALUES across the header. Written by the editor
        // and read by nothing until the 2026-09-25 sweep found it.
        pivotDirections: view.snapshot.pivotOn.map((name) =>
          columnConfig(this.#config, name).pivotSortDirection ?? 'asc'),
      },
      view.snapshot.pivotOn.length,
    );
    // Refreshed AFTER the view lands, because the pivot's leaf names
    // are only known once the engine has answered.
    this.#refreshFormats();
    this.#refreshHeatmaps(view);
    this.#grid.setColumns(model);
    this.#grid.setSorts(view.snapshot.sorts);
    this.#grid.setRows(view.rows, 0, view.rows.rowCount);

    this.#syncPivotCast(model);
    // The panel is refreshed from the view, not only from a
    // configuration change: a pivot's own column names are only
    // known once a result has come back, and the panel lists them.
    this.#refreshToolPanel();
    this.#renderStatusBar(view, model.leaves.length);
    // The snapshot's row count is in the toggle's tooltip, and a
    // fresh snap changes it.
    this.#paintSnap?.();

    const base = timingText(view, model.leaves.length);
    if (view.truncated.length > 0 && this.#config.showTruncationWarning) {
      // Saying WHICH level was cut matters: "some rows are missing"
      // sends someone hunting through the whole cube.
      this.#status(
        `${base} — showing the first ${(this.#config.maxRows ?? DEFAULT_MAX_ROWS).toLocaleString()} of ` +
          `${view.truncated.length} level${view.truncated.length > 1 ? 's' : ''}`,
        'warn',
      );
    } else {
      this.#status(base, 'ok');
    }
  }

  /**
   * Their status bar: a 20px strip, right-aligned, carrying the row
   * count in a monospaced face and the truncation warning in orange.
   *
   * Monospaced because the number changes as you expand the tree,
   * and a proportional figure jitters its neighbours every time it
   * does. The separators are theirs too -- a 1px by 12px neutral
   * rule between groups rather than padding alone.
   */
  #renderStatusBar(view: CubeView, cols: number): void {
    const doc = this.#doc;
    const bar = this.#els.stats;
    bar.replaceChildren();

// WHAT YOU CAN DO ON THE LEFT, WHAT IS TRUE ON THE RIGHT.
    //
    // DataCube's own bar is `justify-between` with two link buttons
    // at the left -- a settings icon and an underlined "Properties",
    // then a filter icon and an underlined "Filter" -- and its
    // readouts at the right. Ours had one link and every figure
    // crowded after it.
    //
    // Both editors were reachable only through the grid's
    // right-click menu, two levels down, and a person looking for
    // them did not find them.
    const left = doc.createElement('div');
    left.className = 'dc-status-actions';
    const right = doc.createElement('div');
    right.className = 'dc-status-readout';
    bar.append(left, right);

    const properties = doc.createElement('button');
    properties.type = 'button';
    properties.className = 'dc-status-link dc-status-properties';
    properties.textContent = '\u2699 Properties';
    properties.title = 'The cube\u2019s settings (Ctrl+E)';
    properties.addEventListener('click', () => this.openEditor());
    left.append(properties);

    // The filter SAYS whether one is in force. A cube showing a
    // subset with nothing on screen to indicate it is how someone
    // reads a filtered total as the whole book.
    const filtered = this.#snapshot.filter !== undefined;
    const filter = doc.createElement('button');
    filter.type = 'button';
    filter.className = filtered
      ? 'dc-status-link dc-status-filter dc-on'
      : 'dc-status-link dc-status-filter';
    filter.textContent = filtered ? '⧨ Filter (on)' : '⧨ Filter';
    filter.title = filtered
      ? 'A filter is in force. Click to edit it.'
      : 'Filter the cube';
    filter.addEventListener('click', () => this.openFilters());
    left.append(filter);

    // THE RESULT, AND WHAT IT COST, in the one bar that is already
    // about the result.
    //
    // This said "Rows: 29" while the same figure -- plus the column
    // count and the elapsed time -- went to the host through
    // `onStatus` and was rendered ABOVE the grid, in the strip that
    // should say what you are looking at. Two readouts of one fact,
    // the fuller one in the wrong place. The cube states it here
    // itself, so a host gets a timing readout without wiring one,
    // and `onStatus` still fires for hosts that want their own.
    const rows = doc.createElement('div');
    rows.className = 'dc-status-rows dc-status-timing';
    rows.textContent = timingText(view, cols);
    rows.title = 'Rows and columns in the result, and how long the '
      + 'query took.';
    right.append(rows);

    if (view.truncated.length > 0 && this.#config.showTruncationWarning) {
      right.append(this.#statusSeparator());
      const warn = doc.createElement('div');
      warn.className = 'dc-status-warning';
      warn.textContent =
        `⚠ Results truncated to fit within row limit ` +
        `(${(this.#config.maxRows ?? DEFAULT_MAX_ROWS).toLocaleString()})`;
      right.append(warn);
    }

    const stats = doc.createElement('div');
    stats.className = 'dc-status-stats';
    right.append(this.#statusSeparator(), stats);
    this.#statsSlot = stats;
    this.#renderSelectionStats();

    // The host's own readout, last.
    this.#adoptHostStatus();
  }

  /**
   * Give the host its slot at the end of the status bar.
   *
   * Called on every render because the bar is rebuilt on every
   * render: the host appends the same node again, which MOVES it
   * rather than cloning it, so whatever wrote to that node keeps
   * writing to the one on screen.
   */
  #adoptHostStatus(): void {
    const fill = this.#options.hostStatus;
    if (!fill) return;
    const slot = this.#doc.createElement('div');
    slot.className = 'dc-status-host';
    // AT THE FAR RIGHT, among the readouts -- with the figures,
    // because that is what it is: which backend answered.
    const readout = this.#els.stats
      .querySelector<HTMLElement>('.dc-status-readout');
    const host = readout ?? this.#els.stats;
    if (host.childElementCount > 0) host.append(this.#statusSeparator());
    host.append(slot);
    fill(slot);
  }

  /**
   * Put the chrome flags on screen.
   *
   * Called after every change to them, and after a rebuild of the
   * title bar -- which is how the bar comes back as a LIP rather
   * than as nothing.
   */
  #applyChrome(): void {
    const zones = this.#config.showDragZones || this.#zonePeek;
    this.#els.zoneBar.hidden = !zones;
    this.#els.zoneBar.classList.toggle('dc-peeking', !this.#config
      .showDragZones && this.#zonePeek);
    this.#els.toolbar.classList.toggle(
      'dc-collapsed',
      !this.#config.showTitleBar,
    );
  }

  /** Fold the zones away, or bring them back. */
  #setChrome(patch: {
    readonly showDragZones?: boolean;
    readonly showTitleBar?: boolean;
  }): void {
    this.#config = { ...this.#config, ...patch };
    this.#renderChrome();
  }

  /**
   * Put the configuration's chrome on screen, whatever set it.
   *
   * Every path that replaces the whole configuration -- the
   * properties editor, a loaded view -- has to come through here, or
   * the flags and the DOM drift apart: the bar stays folded while
   * the configuration says it is shown, and the toggle that should
   * unfold it folds it instead.
   */
  #renderChrome(): void {
    this.#buildToolbar();
    this.#applyChrome();
  }

  /**
   * The control that folds the zone bar.
   *
   * The same shape as the columns panel's: one button, in the bar it
   * folds, and when the bar is gone the title bar carries the twin
   * that brings it back. Never nothing to click -- a bar that
   * vanishes without leaving a way back is a bar the user has lost.
   */
  #zoneFold(): HTMLElement {
    const button = this.#doc.createElement('button');
    button.type = 'button';
    button.className = 'dc-zone-fold';
    button.textContent = '\u2303';
    button.title = 'Hide the drag zones';
    button.setAttribute('aria-label', 'Hide the drag zones');
    button.setAttribute('aria-expanded', 'true');
    button.addEventListener('click', () => {
      this.#setChrome({ showDragZones: false });
    });
    return button;
  }

  #statusSeparator(): HTMLElement {
    const sep = this.#doc.createElement('div');
    sep.className = 'dc-status-sep';
    sep.setAttribute('aria-hidden', 'true');
    return sep;
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
    const previous = this.#snapshot;
    this.#snapshot = forgetStaleCast(this.#snapshot,
      zone === 'rows'
        ? { ...this.#snapshot, rows: [...columns] }
        : { ...this.#snapshot, pivotOn: [...columns] });
    this.#refreshOr(previous);
  }

  /**
   * Take the editor's two lists.
   *
   * A bad expression is not caught here -- `#refreshOr` runs the query
   * and, if the planner refuses, shows its message and puts the
   * previous snapshot back. So the cube never sits in a state it
   * cannot render, and the error the user sees is the planner's own
   * rather than a paraphrase.
   */
  async #setCalc(
    row: readonly DerivedColumn[],
    group: readonly DerivedColumn[],
    rename?: { readonly from: string; readonly to: string },
  ): Promise<string | null> {
    const previous = this.#snapshot;
    const previousConfig = this.#config;
    const next: CubeSnapshot = {
      ...this.#snapshot,
      derived: [...row],
      ...(group.length > 0
        ? { groupDerived: [...group] }
        : { groupDerived: [] }),
    };
    // A RENAME carries through: grouped, pivoted, sorted or filtered
    // by the old name, and its settings (format, width, display name)
    // follow it. Before, the rename left those naming a column that
    // no longer existed, and the planner refused it.
    this.#snapshot = rename
      ? renameColumnReferences(next, rename.from, rename.to)
      : next;
    if (rename) {
      this.#config = renameColumnConfig(this.#config, rename.from, rename.to);
    }
    // AWAITED, and the refusal RETURNED, rather than `#refreshOr`'s
    // fire-and-forget: the editor has to know. Reverting the snapshot
    // alone took the user's column -- and the text they typed -- with
    // it, and put the reason on the status line, away from the form
    // it was about. The editor keeps the form open and shows it there.
    try {
      await this.#refresh();
      return null;
    } catch (error: unknown) {
      const message = error instanceof Error ? error.message : String(error);
      this.#status(message, 'error');
      this.#snapshot = previous;
      this.#config = previousConfig;
      return message;
    }
  }

  // -- the context menu ------------------------------------------------

  #wireContextMenu(): void {
    this.#els.grid.addEventListener('contextmenu', (event) => {
      const target = event.target;
      if (!(target && 'closest' in (target as object))) return;
      const el = target as Element;
      const named = el.closest<HTMLElement>('[data-column]');
      let column = named?.dataset['column'];
      event.preventDefault();

      // The clicked VALUE, which is what makes the filter entries
      // one-click rather than a door to a dialog. A cell in the
      // TREE column belongs to whichever row dimension sits at that
      // row's level -- right-clicking EMEA under region filters
      // region, and the desk beneath it filters desk. Their menu
      // resolves it the same way, from the node's level.
      let value: FilterValue | null | undefined;
      let columnType: string | undefined;
      const cell = el.closest<HTMLElement>('.dc-cell');
      const row = cell?.closest<HTMLElement>('.dc-row');
      if (cell && row && this.#view) {
        const abs =
          Number(row.getAttribute('aria-rowindex') ?? '0') -
          this.#view.columns.depth -
          1;
        const meta = this.#treeRows[abs];
        if (column === TREE_COLUMN) {
          // The row's PATH says which dimension it is, not its
          // depth: depth shifts by one when the grand total is
          // shown, so a depth-based reading resolved AMER -- a
          // region -- to `desk`, and then found no value there.
          // A path of length n is the nth row dimension; the grand
          // total's path is empty and offers no value filter.
          const path = meta?.path ?? [];
          column = this.#snapshot.rows[path.length - 1];
          value = path.length > 0 ? (path[path.length - 1] ?? null) : undefined;
        } else if (column !== undefined) {
          const leaf = this.#view.columns.leaves.find(
            (l) => l.name === column,
          );
          const raw =
            leaf === undefined
              ? null
              : (this.#view.rows.columns[leaf.index]?.values[abs] ?? null);
          value = raw as FilterValue | null;
        }
        // A pivot TOTAL is not a column any query can filter on.
        if (column !== undefined && isPivotTotalColumn(column)) {
          value = undefined;
        }
        if (column !== undefined) {
          columnType = rowColumns(this.#snapshot).find(
            (c) => c.name === column,
          )?.type;
        }
        // A group-stage calculated column exists only AFTER the
        // groupBy, and every filter runs before it -- so a value
        // filter on one could only ever be refused.
        if (column !== undefined
          && (this.#snapshot.groupDerived ?? []).some((d) => d.name === column)) {
          value = undefined;
        }
      }
      // FROM A HEADER, Properties... opens on that column (its measure,
      // for a pivot result or a pivot total), as upstream's does.
      const facts = column !== undefined ? this.#columnFacts(column) : {};
      const onHeader = el.closest('.dc-th') !== null;
      const propertiesColumn = onHeader && column !== undefined
        && column !== TREE_COLUMN
        ? facts.pivotBase
          ?? this.#snapshot.measures.find((m) => m.name === column)?.column
          ?? column
        : undefined;
      const groups = buildMenu({
        snapshot: this.#snapshot,
        ...(propertiesColumn !== undefined ? { propertiesColumn } : {}),
        ...(column !== undefined ? { column } : {}),
        isRowDimension: column
          ? this.#snapshot.rows.includes(column)
          : false,
        hasSelection: this.#selection !== null,
        hasExpanded: this.#controller.tree.openPaths.length > 0,
        hasHeatmap:
          column !== undefined && this.#heatmapFor(column) !== undefined,
        canGroup: column === undefined || this.#isDimension(column),
        // A host mailer, or upstream's way: a .eml draft to download.
        canEmail: this.#options.email !== undefined
          || this.#options.download !== undefined,
        ...(column !== undefined && this.#config.columns[column]?.pinned
          ? { pinned: this.#config.columns[column]?.pinned as 'left' | 'right' }
          : {}),
        ...(column !== undefined && isPivotTotalColumn(column)
          ? { pivotTotal: true }
          : {}),
        ...(column !== undefined && this.#kindOf(column) !== undefined
          ? { extendable: true }
          : {}),
        ...(column !== undefined ? calcStageOf(this.#snapshot, column) : {}),
        ...facts,
        ...(value !== undefined ? { value } : {}),
        ...(columnType !== undefined ? { columnType } : {}),
      });
      this.#menu.show(groups, event.clientX, event.clientY);
      const scroller = this.#els.grid.querySelector<HTMLElement>('.dc-scroller');
      this.#menuScroll = scroller
        ? { top: scroller.scrollTop, left: scroller.scrollLeft }
        : null;
    });
  }

  #onMenuAction(item: MenuItem): void {
    if (this.#onHostAction(item)) return;
    // The query actions go through applyMenuAction, which returns the
    // SAME snapshot when nothing changed; the rest are layout,
    // clipboard and export, which never touch the query.
    const next = forgetStaleCast(this.#snapshot, applyMenuAction(
      this.#snapshot, item));
    if (next !== this.#snapshot) {
      const previous = this.#snapshot;
      this.#snapshot = next;
      this.#refreshOr(previous);
      return;
    }
    const column = item.column;
    switch (item.id) {
      case 'tree.collapseAll':
        void this.#controller.setTree(this.#controller.tree.collapseAll());
        return;
      case 'column.autoSize':
        if (column) this.#autoSize([column]);
        return;
      case 'column.autoSizeAll':
        this.#autoSize(null);
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
        this.#refreshOr(null);
        return;
      case 'copy.selection':
        this.#copy(this.#selectionCsv());
        return;
      case 'copy.column':
        if (column) this.#copy(this.#columnCsv(column));
        return;
      case 'view.properties':
        this.openEditor(item.column);
        return;
      case 'copy.rows':
        this.#copy(this.#rowsCsv());
        return;
      case 'column.minimize':
        if (column) this.#minimize([column]);
        return;
      case 'column.minimizeAll':
        this.#minimize(null);
        return;
      case 'grid.sizeToFit':
        this.#sizeToFit();
        return;
      case 'pivot.exclude':
        if (column) this.#patchColumn(column, { excludedFromPivot: true });
        return;
      case 'pivot.include':
        if (column) this.#patchColumn(column, { excludedFromPivot: false });
        return;
      // Upstream's Extended Columns entries. EXTEND seeds the new
      // column with a reference to the one clicked and inherits its
      // kind, as DataCubeNewColumnState does.
      case 'calc.add':
        this.openColumnEditor({});
        return;
      case 'calc.extend':
        if (column) {
          this.openColumnEditor({
            expression: columnRef(column),
            level: this.#kindOf(column) ?? 'measure',
          });
        }
        return;
      case 'calc.edit':
        if (column) this.openColumnEditor({ edit: column });
        return;
      case 'calc.delete':
        if (column) this.#deleteCalc(column);
        return;
      case 'view.zones':
        this.#setChrome({ showDragZones: !this.#config.showDragZones });
        return;
      case 'view.titleBar':
        this.#setChrome({ showTitleBar: !this.#config.showTitleBar });
        return;
      case 'heatmap.add':
        if (column) this.#setHeatmap(column, true);
        return;
      case 'heatmap.remove':
        if (column) this.#setHeatmap(column, false);
        return;
      case 'export.html':
        this.#confirmExport(() => this.#export('html'));
        return;
      case 'export.csv':
        this.#confirmExport(() => this.#export('csv'));
        return;
      case 'export.excel':
        this.#confirmExport(() => this.#export('excel'));
        return;
      case 'export.text':
        this.#confirmExport(() => this.#export('text'));
        return;
      case 'export.pdf':
        this.#confirmExport(() => this.#export('pdf'));
        return;
      case 'export.specification':
        this.#export('specification');
        return;
      case 'email.html':
        this.#confirmExport(() => void this.#email('html'));
        return;
      case 'email.excel':
        this.#confirmExport(() => void this.#email('excel'));
        return;
      case 'email.csv':
        this.#confirmExport(() => void this.#email('csv'));
        return;
      case 'email.text':
        this.#confirmExport(() => void this.#email('text'));
        return;
      case 'email.pdf':
        this.#confirmExport(() => void this.#email('pdf'));
        return;
      case 'chart.plot':
        this.#chart('plot');
        return;
      case 'chart.treemap':
        this.#chart('treemap');
        return;
      case 'filter.column':
        this.openFilters();
        return;
      default:
        return;
    }
  }

  /**
   * Fit columns to their content.
   *
   * These two entries -- "Auto-size to Fit Content" and "Auto-size
   * All Columns" -- were on the menu with NO handler behind them. The
   * dispatch ends in `default: return`, so clicking either did
   * nothing at all, silently, and a census of emitted-versus-handled
   * menu ids is what found them rather than anyone using the product.
   *
   * A floor and a ceiling because auto-size is a convenience, not a
   * licence: a column of long free text would otherwise push every
   * other column off the screen, and an empty one would collapse to
   * nothing and be impossible to grab again.
   *
   * Widths are keyed by COLUMN, and a pivoted leaf is named after the
   * pivot value it sits under (`2021__|__notional`), so on a pivoted
   * cube these size the columns they can name and leave the rest.
   */
  #autoSize(columns: readonly string[] | null): void {
    const measured = this.#grid.measureColumns(columns ?? undefined);
    let config = this.#config;
    let changed = 0;
    for (const [name, content] of Object.entries(measured)) {
      const width = Math.min(AUTO_SIZE_MAX,
        Math.max(AUTO_SIZE_MIN, content + AUTO_SIZE_PAD));
      const next = withColumn(config, name, { width });
      if (next !== config) changed += 1;
      config = next;
    }
    if (changed === 0) {
      this.#status('nothing to resize', 'warn');
      return;
    }
    void this.#setConfiguration(config);
  }

  /**
   * Turn a heatmap on or off for a column.
   *
   * Set on the MEASURE rather than the leaf, so it reaches every
   * `2021__|__notional` the pivot produced -- a heatmap applied to
   * one pivoted leaf and not its siblings is worse than none.
   */
  #setHeatmap(leafName: string, on: boolean): void {
    const measure = leafName.split(PIVOT_SEPARATOR).pop() ?? leafName;
    this.#patchColumn(measure, {
      heatmap: on ? { from: '#ffffff', to: '#ff8a65' } : undefined,
    });
  }

  /**
   * A header click: upstream's sort-on-click, with multi-sort always on.
   * Off, ascending, descending, off again -- each column in its own
   * place among the sorts. The tree column orders the GROUPS, so it
   * flips the tree's direction; a pivot total is no query's column.
   */
  #sortByHeader(column: string): void {
    if (isPivotTotalColumn(column)) {
      this.#status('a pivot total cannot be sorted on', 'warn');
      return;
    }
    if (column === TREE_COLUMN) {
      void this.applyConfiguration({
        treeColumnSort: this.#config.treeColumnSort === 'asc' ? 'desc' : 'asc',
      });
      return;
    }
    const sorts = this.#snapshot.sorts;
    const at = sorts.findIndex((x) => x.column === column);
    const now = sorts[at];
    const next = now === undefined
      ? [...sorts, { column, direction: 'asc' as const }]
      : now.direction === 'asc'
        ? sorts.map((x, i) => (i === at ? { column, direction: 'desc' as const } : x))
        : sorts.filter((_x, i) => i !== at);
    const previous = this.#snapshot;
    this.#snapshot = { ...this.#snapshot, sorts: next };
    this.#refreshOr(previous);
  }

  /**
   * A header's tooltip, as upstream's: the column and its own name;
   * under a pivot, the values it sits under.
   */
  #headerTitle(path: readonly string[], leaf?: LeafColumn): string {
    const keys = this.#snapshot.pivotOn.map((k) => labelFor(this.#config, k));
    const named = (name: string): string => {
      const label = labelFor(this.#config, name);
      return label === name ? name : `${label} (${name})`;
    };
    if (leaf?.name === TREE_COLUMN) return '';
    if (leaf && isPivotTotalColumn(leaf.name)) {
      const measure = leaf.path[leaf.path.length - 1] ?? leaf.name;
      return `Column = ${named(measure)} ~ [ ${keys.join(', ')}: all values ]`;
    }
    if (leaf && leaf.path.length > 1) {
      const measure = leaf.path[leaf.path.length - 1] ?? leaf.name;
      const values = leaf.path.slice(0, -1);
      return `Column = ${named(measure)} ~ [ ${values
        .map((v, i) => `${keys[i] ?? '?'} = ${v}`).join(', ')} ]`;
    }
    if (leaf) return `Column = ${named(leaf.name)}`;
    if (path[0] === (this.#config.pivotStatisticColumnName ?? 'Total')) return '';
    return `[ ${path.map((v, i) => `${keys[i] ?? '?'} = ${v}`).join(', ')} ]`;
  }

  #patchColumn(
    column: string,
    patch: Parameters<typeof withColumn>[2],
  ): void {
    const next = withColumn(this.#config, column, patch);
    if (next === this.#config) return;
    void this.#setConfiguration(next);
  }

  /**
   * Change the configuration and re-run, as one step.
   *
   * The single funnel for presentation changes, so every one of them
   * lands on the undo stack. Config used to be assigned in four
   * places, each followed by its own refresh, which is how half of
   * them ended up outside the history.
   */
  async applyConfiguration(
    patch: Patch<CubeConfiguration>,
  ): Promise<void> {
    // `columns` is split out rather than passed through: withSettings
    // prunes undefined keys, so handing it `columns: undefined` does
    // not mean "leave columns alone", it DELETES the column map.
    const { columns, ...settings } = patch;
    let next = Object.keys(settings).length > 0
      ? withSettings(this.#config, settings)
      : this.#config;
    for (const [name, cfg] of Object.entries(columns ?? {})) {
      next = withColumn(next, name, cfg);
    }
    await this.#setConfiguration(next);
  }

  async #setConfiguration(next: CubeConfiguration): Promise<void> {
    if (next === this.#config) return;
    this.#config = next;
    await this.#refresh();
  }

  // -- selection -------------------------------------------------------

  #onSelectionChange(range: CellRange | null): void {
    this.#selection = range;
    this.#renderSelectionStats();
  }

  #renderSelectionStats(): void {
    const slot = this.#statsSlot;
    if (!slot) return;
    const table = this.#view?.rows;
    const range = this.#selection;
    if (!range || !table || !this.#config.showSelectionStats) {
      slot.textContent = '';
      return;
    }
    const s = selectionStats(table, range);
    // Blanks are reported rather than folded into the count, because
    // an average over a pivot region that treated empty combinations
    // as zero would be wrong in the direction of looking plausible.
    slot.textContent =
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

  /**
   * Every shown column across the selected ROWS: upstream's "Selected
   * Rows as Plain Text". Through the column model, so hidden columns
   * and the tree's machinery stay out.
   */
  #rowsCsv(): string {
    const view = this.#view;
    const sel = this.#selection;
    if (!view || !sel) return '';
    const top = Math.min(sel.anchor.row, sel.focus.row);
    const bottom = Math.max(sel.anchor.row, sel.focus.row);
    const columns = view.columns.leaves.flatMap((l) => {
      const c = view.rows.columns[l.index];
      return c ? [{ ...c, values: c.values.slice(top, bottom + 1) }] : [];
    });
    return toCsv({
      columns,
      rowCount: bottom - top + 1,
      epoch: view.rows.epoch,
      elapsedMs: 0,
    });
  }

  /**
   * What the menu needs to know about the column under the pointer:
   * the measure a pivot result came from, whether it is a measure,
   * whether it is kept out of the pivot, and whether its width is
   * fixed (Minimize leaves a fixed width alone, as upstream does).
   */
  #columnFacts(column: string): {
    pivotBase?: string;
    isMeasure?: boolean;
    excludedFromPivot?: boolean;
    fixedWidth?: boolean;
  } {
    const leaf = this.#view?.columns.leaves.find((l) => l.name === column);
    const measure = leaf && leaf.path.length > 1
      ? leaf.path[leaf.path.length - 1]
      : undefined;
    const base = measure === undefined
      ? column
      : (this.#snapshot.measures.find((m) => m.name === measure)?.column
        ?? measure);
    const excluded = columnConfig(this.#config, base).excludedFromPivot === true
      || this.#snapshot.columns.some((c) => c.name === base && c.excludedFromPivot);
    return {
      ...(measure !== undefined ? { pivotBase: base } : {}),
      ...(this.#kindOf(base) === 'measure' ? { isMeasure: true } : {}),
      ...(excluded ? { excludedFromPivot: true } : {}),
      ...(columnConfig(this.#config, column).widthMode === 'fixed'
        ? { fixedWidth: true }
        : {}),
    };
  }

  /**
   * Resize > Minimize: a column to its own minimum width, or upstream's
   * DEFAULT_COLUMN_MIN_WIDTH (50). `null` minimizes every shown column.
   * A column whose width is FIXED keeps it.
   */
  #minimize(columns: readonly string[] | null): void {
    const names = columns
      ?? (this.#view?.columns.leaves ?? [])
        .map((l) => l.name)
        .filter((n) => n !== TREE_COLUMN);
    let config = this.#config;
    for (const name of names) {
      const c = columnConfig(config, name);
      if (c.widthMode === 'fixed') continue;
      config = withColumn(config, name, { width: c.minWidth ?? MINIMIZED_WIDTH });
    }
    void this.#setConfiguration(config);
  }

  /**
   * Resize > Size Grid to Fit Screen: every shown column scaled by the
   * same factor so together they fill the grid's width -- ag-Grid's
   * `sizeColumnsToFit`, which is what upstream calls. A fixed width
   * keeps its size and the rest share what is left.
   */
  #sizeToFit(): void {
    const widths = this.#grid.renderedWidths();
    const viewport = this.#grid.viewportWidth;
    const fixed = Object.keys(widths)
      .filter((n) => columnConfig(this.#config, n).widthMode === 'fixed');
    const flexible = Object.keys(widths).filter((n) => !fixed.includes(n));
    const taken = fixed.reduce((sum, n) => sum + (widths[n] ?? 0), 0);
    const current = flexible.reduce((sum, n) => sum + (widths[n] ?? 0), 0);
    if (viewport <= 0 || current <= 0 || flexible.length === 0) return;
    const factor = Math.max(0, viewport - taken) / current;
    let config = this.#config;
    for (const name of flexible) {
      config = withColumn(config, name, {
        width: Math.max(MINIMIZED_WIDTH, Math.floor((widths[name] ?? 0) * factor)),
      });
    }
    void this.#setConfiguration(config);
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
    // THROUGH THE CONTROLLER'S RUNNER, not a planner and an engine of
    // its own: on the plane where a remote engine executes there is
    // no local engine here to call, and a drill-through that works on
    // two planes out of three is a broken feature on the third.
    const { rows: table } = await this.#controller.runQuery(
      pure,
      view.snapshot,
    );
    this.#showOverlay('Drill-through', (host) => {
      const pre = this.#doc.createElement('pre');
      pre.className = 'dc-drill';
      pre.textContent = toCsv(table);
      host.append(pre);
    }, { replace: true });
  }

  // -- export -----------------------------------------------------------

  /**
   * Header text per column, where the user renamed one.
   *
   * The document exports are READ by people, so they must say what
   * the screen says -- an export whose headings are the raw generated
   * names is a different document from the one on screen.
   */
  #labels(): Record<string, string> {
    const out: Record<string, string> = {};
    for (const c of this.#view?.rows.columns ?? []) {
      const label = labelFor(this.#config, c.name);
      if (label !== c.name) out[c.name] = label;
    }
    return out;
  }

  /**
   * Show the current view as a picture.
   *
   * Drawn from the ROWS ON SCREEN rather than from a fresh query, so
   * the chart cannot disagree with the grid behind it -- every
   * filter, pivot and sort is already baked into what it is given.
   */
  #chart(kind: 'plot' | 'treemap'): void {
    const view = this.#view;
    if (!view) return;
    const title = this.#config.reportTitle ?? 'cube';
    const svg = kind === 'plot'
      ? toBarChart(view.rows, { title })
      : toTreemap(view.rows, { title });
    this.#showOverlay(kind === 'plot' ? 'Plot' : 'Treemap', (host) => {
      const box = this.#doc.createElement('div');
      box.className = 'dc-chart';
      // The SVG is composed here, from values this code escaped, so
      // there is no untrusted markup in it.
      box.innerHTML = svg;
      host.append(box);
    }, { replace: true });
  }

  /**
   * One rendering, shared by download and email, named as upstream
   * names an export: the title and the moment
   * (`exportFileName`), so a second export never overwrites the first.
   */
  #render(kind: 'csv' | 'excel' | 'html' | 'text' | 'pdf'): {
    name: string;
    mime: string;
    content: string;
  } | null {
    const view = this.#view;
    if (!view) return null;
    const title = this.#config.reportTitle ?? 'cube';
    const base = exportFileName(title, new Date());
    const doc = {
      title,
      formatters: this.#formatters,
      formats: this.#formats,
      labels: this.#labels(),
    };
    switch (kind) {
      case 'csv':
        return { name: `${base}.csv`, mime: 'text/csv', content: toCsv(view.rows) };
      case 'excel':
        // SpreadsheetML rather than CSV so numbers arrive as numbers;
        // a CSV of "1,234" opens as text in every locale that uses a
        // comma for the decimal point.
        return {
          name: `${base}.xls`,
          mime: 'application/vnd.ms-excel',
          content: toSpreadsheetML(view.rows, { title }),
        };
      case 'html':
        return { name: `${base}.html`, mime: 'text/html', content: toHtml(view.rows, { title }) };
      case 'text':
        // Formatted, not raw: plain text exists to be READ -- pasted
        // into a message or a ticket -- so it should say what the
        // screen says. CSV is the one that stays raw so it can be
        // computed on again.
        return { name: `${base}.txt`, mime: 'text/plain', content: toPlainText(view.rows, doc) };
      case 'pdf':
        return { name: `${base}.pdf`, mime: 'application/pdf', content: toPdf(view.rows, doc) };
    }
  }

  /**
   * Upstream's warning before any data leaves the cube: the user
   * attests to the leakage risk, and Decline (the default, focused
   * first) does nothing. Upstream asks for exports; ours asks for an
   * email too, which carries the same rows.
   */
  #confirmExport(onAccept: () => void): void {
    this.#alert({
      type: 'warning',
      message: 'Confirm you want to proceed with export',
      text: 'I attest that I am aware of the sensitive data leakage risk when'
        + ' exporting queried data. The data I export will only be used by me.',
      actions: [
        { label: 'Decline', handler: () => {} },
        { label: 'Accept', handler: onAccept },
      ],
    });
  }

  /** Open an alert window (upstream's DataCubeAlertService.alert). */
  #alert(options: AlertOptions): void {
    this.#alerts += 1;
    this.#showOverlay(options.title ?? '', (host, close) => buildAlert(host, options, close), {
      key: `alert:${this.#alerts}`,
      size: ALERT_WINDOW,
    });
  }

  /**
   * Email the current view as an attachment: through the host's
   * mailer when it has one, else upstream's way -- an unsent `.eml`
   * draft, downloaded, that opens in the user's own mail client.
   */
  async #email(kind: 'csv' | 'excel' | 'html' | 'text' | 'pdf'): Promise<void> {
    const rendered = this.#render(kind);
    if (!rendered) return;
    const send = this.#options.email;
    const download = this.#options.download;
    if (!send) {
      if (!download) {
        this.#status('this build cannot send email', 'warn');
        return;
      }
      const draft = rendered.name.replace(/\.[^.]+$/, '.eml');
      download(draft, 'message/rfc822', toEml(rendered));
      this.#status(`email draft ${draft}`, 'ok');
      return;
    }
    const title = this.#config.reportTitle ?? 'cube';
    try {
      await send({
        subject: title,
        body: `${title} — ${this.#view?.rows.rowCount ?? 0} rows`,
        attachment: rendered,
      });
      this.#status(`emailed ${rendered.name}`, 'ok');
    } catch (e) {
      this.#status(e instanceof Error ? e.message : String(e), 'error');
    }
  }

  #export(
    kind: 'csv' | 'excel' | 'html' | 'text' | 'pdf' | 'specification',
  ): void {
    const view = this.#view;
    if (!view) return;
    const download = this.#options.download;
    if (!download) {
      this.#status('no download handler', 'error');
      return;
    }
    if (kind !== 'specification') {
      const rendered = this.#render(kind);
      if (rendered) download(rendered.name, rendered.mime, rendered.content);
      return;
    }
    const title = this.#config.reportTitle ?? 'cube';
    download(
      `${exportFileName(title, new Date())}.json`,
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
      this.#renderChrome();
      // ADOPT, do not set: `setTree` refreshes, and that refresh runs
      // the controller's own snapshot -- the one being replaced --
      // then pushes it back through `onView`, which reassigns
      // `this.#snapshot`. The load reported success and restored
      // nothing, because the query that followed used the shape the
      // user had just abandoned. One refresh, both halves in place.
      this.#controller.adoptTree(treeOf(view));
      await this.#refresh();
      this.#status(`loaded "${view.name}"`, 'ok');
    } catch (e) {
      this.#status(e instanceof Error ? e.message : String(e), 'error');
    }
  }

  // -- dimensions ----------------------------------------------------------

  useDimension(dimension: Dimension): void {
    const previous = this.#snapshot;
    this.#snapshot = useDimension(this.#snapshot, dimension);
    this.#refreshOr(previous);
  }

  // -- the dialogs ----------------------------------------------------------

  /**
   * The Properties editor; on `column`, open at Column Properties for
   * it -- upstream's Properties... from a column header. An editor
   * already open is raised and moved to that column, keeping its draft.
   */
  openEditor(column?: string): void {
    const open = this.#editor;
    if (open && this.#open.has('Properties')) {
      this.#showOverlay('Properties', () => {});
      if (column !== undefined) open.focusColumn(column);
      return;
    }
    this.#showOverlay('Properties', (host, close) => {
      this.#editor = new CubeEditor(
        host,
        draftFor(this.#snapshot, this.#config, this.#options.dimensions ?? []),
        {
          onApply: (draft, base) => this.#applyDraft(draft, base),
          onClose: close,
          ...(column !== undefined ? { initialColumn: column } : {}),
        },
      );
    });
  }

  /**
   * A calculated column's editor, ONE PER WINDOW as upstream's: any
   * number of "Add New Column" windows, and one "Edit Column" per
   * column -- editing a column already open brings its window forward.
   * Each compiles its draft as it is typed (compile only, never run) and
   * compiles again whenever the cube changes under it.
   */
  openColumnEditor(start: ColumnEditorStart): void {
    const editing = 'edit' in start ? start.edit : undefined;
    const key = editing !== undefined
      ? `column:${editing}`
      : `column:new:${(this.#newColumns += 1)}`;
    this.#showOverlay(editing !== undefined ? 'Edit Column' : 'Add New Column', (host, close) => {
      this.#columnEditors.set(key, new ColumnEditor(host, {
        snapshot: () => this.#snapshot,
        start,
        compile: (candidate, signal) => this.#controller.compile(candidate, signal),
        apply: (row, group, rename) => this.#setCalc(row, group, rename),
        onClose: close,
      }));
    }, {
      key,
      // Upstream's column editor window, a little taller for our
      // completion list.
      size: { x: 50, y: 50, width: 500, height: 420, minWidth: 300, minHeight: 200, center: false },
    });
  }

  /** Take one calculated column out, whichever stage it is in. */
  #deleteCalc(name: string): void {
    void this.#setCalc(
      this.#snapshot.derived.filter((d) => d.name !== name),
      (this.#snapshot.groupDerived ?? []).filter((d) => d.name !== name),
    );
  }

  openFilters(): void {
    this.#showOverlay('Filters', (host, close) => {
      new FilterEditor(host, {
        // Row-stage calculated columns filter like any other, and each
        // column brings its TYPE: it decides the operators offered and
        // the value editor shown.
        columns: rowColumns(this.#snapshot).map((c) => ({
          name: c.name,
          type: c.type ?? 'String',
        })),
        ...(this.#snapshot.filter ? { value: this.#snapshot.filter } : {}),
        onApply: (filter) => this.#applyFilter(filter),
        onClose: close,
      });
    }, {
      // Upstream's Filter window: 750 x 400, near the top right --
      // wide enough for a condition on a date and time to the second.
      size: {
        width: 750, height: 400, minWidth: 300, minHeight: 200,
        x: -50, y: 50, center: false,
      },
    });
  }

  /**
   * The Filter window's Apply: run it, and on a refusal put the cube
   * back and hand the reason to the window that asked.
   */
  async #applyFilter(filter: FilterNode | undefined): Promise<string | null> {
    const previous = this.#snapshot;
    this.#snapshot = filter
      ? { ...this.#snapshot, filter }
      : (({ filter: _drop, ...rest }) => rest)(this.#snapshot);
    try {
      await this.#refresh();
      return null;
    } catch (error: unknown) {
      const message = error instanceof Error ? error.message : String(error);
      this.#status(message, 'error');
      this.#snapshot = previous;
      return message;
    }
  }

  #applyDraft(edited: CubeDraft, base: CubeDraft): void {
    // ONLY WHAT THE EDITOR CHANGED. Other windows stay open beside the
    // editor now -- a filter applied, a column pinned from the menu,
    // a calculated column added -- and applying a draft taken before
    // them wholesale would silently put the cube back. So the editor's
    // edits (the difference between its draft and what it opened on)
    // land on the cube as it is NOW.
    const draft = mergeDraft(
      { snapshot: this.#snapshot, config: this.#config, dimensions: edited.dimensions },
      base,
      edited,
    );
    const wasRoot = this.#config.showRootAggregation;
    // A REFUSED DRAFT PUTS EVERYTHING BACK. It used to stay: the
    // refusal went to the status line and the draft remained the
    // cube's snapshot and configuration, so every later query repeated
    // it -- one Apply wedged the cube until a reload. Upstream compiles
    // the whole query before publishing; running it and restoring on
    // refusal gives the same guarantee.
    const previous = this.#snapshot;
    const previousConfig = this.#config;
    const rollback = (error: unknown): void => {
      this.#status(error instanceof Error ? error.message : String(error), 'error');
      this.#snapshot = previous;
      this.#config = previousConfig;
      this.#renderChrome();
      this.#refreshFormats();
      this.#grid.setAppearance(this.#config.appearance,
        toColumnAppearance(this.#config));
      this.#refreshToolPanel();
    };
    this.#snapshot = draft.snapshot;
    this.#config = draft.config;
    this.#renderChrome();
    // "Show root aggregation" is a SETTING in their General
    // Properties, and it decides whether the level-0 query is issued
    // at all. It was never connected to the tree, so the checkbox
    // moved and the grand total stayed exactly where it was.
    // "Initially expand to level" decides which groups open as they
    // load (upstream's isServerSideGroupOpenByDefault). It was written
    // to the configuration and read by nothing (census §2).
    const tree = this.#controller.tree;
    const expandTo = draft.config.initialExpandToLevel ?? 0;
    if (draft.config.showRootAggregation !== wasRoot
      || expandTo !== tree.expandTo) {
      void this.#controller
        .setTree(tree
          .withTotals(draft.config.showRootAggregation)
          .withExpandTo(expandTo))
        .then(() => this.#refresh())
        .catch((error: unknown) => {
          this.#controller.adoptTree(tree);
          rollback(error);
        });
      return;
    }
    this.#refresh().catch(rollback);
  }

  /**
   * Record what type each calculated column turned out to have.
   *
   * The type is a PLAN FACT, not something to infer from the
   * expression: `$x.a * 2` is a Float and `$x.a->toUpper()` a String,
   * and deciding which by reading the text would be writing the type
   * checker the planner already is. The result carries it, so it is
   * copied onto the snapshot here -- the same way the snapshot
   * already learns the pivot's generated column names.
   *
   * It no longer decides whether the column SUMS -- the editor asks
   * for that (`DerivedColumn.kind`), as upstream does, so the
   * aggregate is right on the first query instead of one query late.
   * What still needs the type is the pivot's
   * `cast(@Relation<(...)>)`, which has to declare one, and the
   * type-based fallback for a column with no declared kind.
   */
  #learnCalcTypes(snapshot: CubeSnapshot, view: CubeView): CubeSnapshot {
    const seen = new Map(view.rows.columns.map((c) => [c.name, c.type]));
    let changed = false;
    const learn = (list: readonly DerivedColumn[]): DerivedColumn[] =>
      list.map((d) => {
        const type = seen.get(d.name);
        if (type === undefined || type === d.type) return d;
        changed = true;
        return { ...d, type };
      });
    const derived = learn(snapshot.derived);
    const group = learn(snapshot.groupDerived ?? []);
    if (!changed) return snapshot;
    return { ...snapshot, derived, groupDerived: group };
  }

  /**
   * Adopt the learned types, and re-run once if they change the query.
   *
   * The aggregate DEFAULT reads a column's type: a numeric one sums,
   * anything else takes its unique value. A calculated column has no
   * type until a result has landed, so the first query over a GROUPED
   * cube aggregates it as `unique` -- which renders as a blank column
   * rather than an error, and would fix itself only on the user's
   * next interaction.
   *
   * So this does what `#syncPivotCast` does for the pivot's generated
   * names: takes the fact out of the result, puts it in the snapshot,
   * and runs once more. It cannot loop, because the second result
   * reports the same types and nothing changes.
   *
   * Only when it MATTERS. On a flat cube nothing is aggregated and the
   * type cannot change the query, so a second round trip would buy
   * nothing.
   *
   * @returns whether a re-run was started, so the caller stops.
   */
  #syncCalcTypes(view: CubeView): boolean {
    const previous = this.#snapshot;
    const learned = this.#learnCalcTypes(previous, view);
    if (learned === previous) return false;
    this.#snapshot = learned;
    // ONLY WHEN THE TYPE CAN STILL CHANGE THE QUERY. Two cases are
    // left now that the kind is declared: a pivot has to name a type
    // in its cast, and a column with no declared kind still falls
    // back to the type-based default. Anything else would be a second
    // round trip that buys nothing.
    const castsATypes = previous.pivotOn.length > 0;
    const undeclared = [...previous.derived,
      ...(previous.groupDerived ?? [])].some((d) => d.kind === undefined);
    if (!castsATypes && !(undeclared && previous.rows.length > 0)) {
      return false;
    }
    // The PREVIOUS snapshot goes with it, so a query the planner
    // rejects takes the learned types back out rather than leaving the
    // cube retrying a shape it cannot render.
    this.#refreshOr(previous);
    return true;
  }

  /**
   * Open a WINDOW, or bring it forward if it is already open.
   *
   * Several at once, as upstream's layout: the Filter window beside
   * the Properties editor beside a column editor, each consulted
   * against the others and against the grid. There used to be ONE
   * overlay, and opening anything replaced whatever was in it.
   *
   * Keyed by title. Reopening an open window raises it rather than
   * rebuilding it -- a half-edited draft is not thrown away because
   * the entry that opened it was clicked again -- unless `replace`
   * says the content is new (a different drill-through, a new chart).
   *
   * `build` receives the body and the one function that closes THIS
   * window; a window closes itself, never "whichever is open".
   */
  #showOverlay(
    title: string,
    build: (host: HTMLElement, close: () => void) => void,
    options: {
      readonly replace?: boolean;
      readonly size?: WindowOptions;
      /** What identifies the window, when its title does not: an alert. */
      readonly key?: string;
    } = {},
  ): HTMLElement {
    const key = options.key ?? title;
    const open = this.#open.get(key);
    if (open && !options.replace) {
      this.#raise(open);
      return open;
    }
    const win = open ?? this.#doc.createElement('div');
    if (!open) {
      win.className = 'dc-app-overlay';
      win.dataset['window'] = key;
      this.#els.root.append(win);
      this.#open.set(key, win);
      // Whichever window is touched comes to the front.
      win.addEventListener('pointerdown', () => this.#raise(win));
      // Escape closes the window it is pressed in, because a window a
      // keyboard user cannot dismiss is a trap; the panels inside stop
      // their own Escape from reaching here.
      win.addEventListener('keydown', (event) => {
        if (event.key === 'Escape') this.#closeWindow(key);
      });
    }
    win.hidden = false;
    win.replaceChildren();
    win.removeAttribute('style');
    const close = (): void => this.#closeWindow(key);
    const head = this.#div(win, 'dc-overlay-head');
    const h = this.#doc.createElement('span');
    h.textContent = title;
    const shut = this.#doc.createElement('button');
    shut.type = 'button';
    shut.className = 'dc-overlay-close';
    shut.textContent = '\u00d7';
    shut.setAttribute('aria-label', `Close ${title}`);
    shut.addEventListener('click', close);
    head.append(h, shut);
    build(this.#div(win, 'dc-overlay-body'), close);

    // Dragged by the header, resized from any edge, and REMEMBERED BY
    // TITLE: reopening Properties finds it where it was left, while
    // the Filter window keeps its own place.
    const remembered = this.#windows.get(key);
    this.#windows.set(
      key,
      makeWindow(win, head, this.#els.root, {
        ...(options.size ?? {}),
        ...(remembered ? { spec: remembered } : {}),
        onChange: (spec) => this.#windows.set(key, spec),
      }),
    );
    this.#raise(win);
    return win;
  }

  /** Put a window above every other. */
  #raise(win: HTMLElement): void {
    this.#zTop += 1;
    win.style.zIndex = String(this.#zTop);
  }

  /** Close one window, by its title. */
  #closeWindow(key: string): void {
    const win = this.#open.get(key);
    if (!win) return;
    win.remove();
    this.#open.delete(key);
    // An alert's position is not worth remembering: each is new.
    if (key.startsWith('alert:')) this.#windows.delete(key);
    this.#columnEditors.get(key)?.dispose();
    this.#columnEditors.delete(key);
    if (key === 'Properties') this.#editor = null;
  }

  // -- the toolbar ------------------------------------------------------------

  /**
   * Their title bar, not a toolbar.
   *
   * DataCube has no row of buttons over the grid. It has a 28px bar
   * carrying a cube glyph, the report title, whatever the HOST wants
   * to add, and a hamburger -- and everything else lives in the
   * grid's right-click menu, which is where its users look for it.
   * The toolbar this replaced was thirteen buttons that existed only
   * because the features behind them had nowhere else to be reached
   * from; now they do.
   */
  #buildToolbar(): void {
    const bar = this.#els.toolbar;
    const doc = this.#doc;
    bar.replaceChildren();

    // FOLDED, THE BAR IS A LIP, not nothing. The hamburger lives
    // here, so a bar that vanished outright would take the menu with
    // it and leave a person no way back except a reload. The lip is
    // 12px and carries one control; the grid's right-click menu
    // carries the same toggle, so there are always two ways back.
    if (!this.#config.showTitleBar) {
      const open = doc.createElement('button');
      open.type = 'button';
      open.className = 'dc-titlebar-lip';
      open.textContent = '\u2304';
      open.title = 'Show the title bar';
      open.setAttribute('aria-label', 'Show the title bar');
      open.setAttribute('aria-expanded', 'false');
      open.addEventListener('click', () => {
        this.#setChrome({ showTitleBar: true });
      });
      bar.append(open);
      return;
    }

    // THE REPORT'S NAME, and nothing else on the left.
    //
    // This was a cube glyph and the word "DataCube" -- a brand on a
    // bar 28px tall, above a grid that wanted every pixel -- with the
    // report title as an optional replacement. It is the other way
    // round now: the bar carries the name of the thing you are
    // looking at, and when the cube has no name it carries nothing.
    // Naming the product here says nothing a person did not know
    // from opening it.
    const reportTitle = this.#config.reportTitle;
    if (reportTitle !== undefined && reportTitle !== '') {
      const title = doc.createElement('span');
      title.className = 'dc-titlebar-title';
      title.textContent = reportTitle;
      bar.append(title);
    }

    // Snap is legend-lite's own idea rather than DataCube's, but it
    // is a MODE, and a mode belongs in the bar rather than two
    // levels down a menu.
    const host = this.#div(bar, 'dc-titlebar-host');
    const snap = doc.createElement('button');
    snap.type = 'button';
    snap.className = 'dc-titlebar-toggle';
    // THE ONLY PLANE INDICATOR, so it carries the whole truth.
    //
    // A banner over the grid said "trades - frozen at 14:02:11 - 29
    // rows" while the button beside it said "Snapped": two controls
    // for one fact, one of them costing a full row of the viewport.
    // The banner is gone and its detail moved into the tooltip --
    // rule 1 of snap mode is that what you are looking at is never
    // inferable, and WHEN it was frozen and HOW MANY rows it holds
    // is the part a label cannot carry.
    const paint = (): void => {
      const state = this.#controller.snaps.state;
      const snapped = state.mode === 'snapped';
      snap.textContent = snapped ? 'Snapped' : 'Live';
      snap.classList.toggle('dc-on', snapped);
      if (state.mode === 'snapped') {
        const taken = state.snap.takenAt.toLocaleTimeString();
        snap.title =
          `${state.snap.label} — frozen at ${taken}, ` +
          `${state.snap.rowCount.toLocaleString()} rows. ` +
          `Click to go live.`;
      } else {
        snap.title = 'Live data, which may move while you work. '
          + 'Click to snap.';
      }
    };
    this.#paintSnap = paint;
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
    host.append(snap);

    // THE ZONES' WAY BACK, in the bar that is still on screen. Shown
    // only when they are folded: a control that is always there but
    // does nothing half the time is worse than one that appears when
    // it has something to do.
    if (!this.#config.showDragZones) {
      const show = doc.createElement('button');
      show.type = 'button';
      show.className = 'dc-titlebar-zones';
      show.textContent = '\u2304 Zones';
      show.title = 'Show the drag zones';
      show.setAttribute('aria-label', 'Show the drag zones');
      show.setAttribute('aria-expanded', 'false');
      show.addEventListener('click', () => {
        this.#setChrome({ showDragZones: true });
      });
      host.append(show);
    }

    // AND THE BAR FOLDS ITSELF, beside the menu it carries.
    const fold = doc.createElement('button');
    fold.type = 'button';
    fold.className = 'dc-titlebar-fold';
    fold.textContent = '\u2303';
    fold.title = 'Hide the title bar';
    fold.setAttribute('aria-label', 'Hide the title bar');
    fold.setAttribute('aria-expanded', 'true');
    fold.addEventListener('click', () => {
      this.#setChrome({ showTitleBar: false });
    });
    bar.append(fold);

    // The hamburger. Theirs carries host-level entries -- View
    // Source, Settings, About -- so ours carries the equivalents:
    // the saved view, and the named hierarchies this cube was given.
    const burger = doc.createElement('button');
    burger.type = 'button';
    burger.className = 'dc-titlebar-menu';
    burger.setAttribute('aria-label', 'Menu');
    burger.textContent = '\u2261';
    burger.addEventListener('click', (event) => {
      if (this.#menu.open) {
        this.#menu.close();
        return;
      }
      // Undo and Redo are DISABLED rather than hidden when there is
      // nothing to go back to -- the same choice the grid's own menu
      // makes everywhere else, and the reason the entries exist at
      // all: a keyboard shortcut that silently does nothing gives a
      // person no way to tell "there is no undo here" from "undo is
      // broken". A disabled entry answers that before they press it.
      const items: MenuItem[] = [
        {
          id: 'view.undo',
          label: 'Undo',
          ...(this.#controller.canUndo ? {} : { disabled: true }),
        },
        {
          id: 'view.redo',
          label: 'Redo',
          ...(this.#controller.canRedo ? {} : { disabled: true }),
        },
        { id: 'view.properties', label: 'Properties...' },
        {
          id: 'view.zones',
          label: this.#config.showDragZones
            ? 'Hide Drag Zones'
            : 'Show Drag Zones',
        },
        { id: 'view.titleBar', label: 'Hide Title Bar' },
      ];
      if (this.#options.storage) {
        items.push(
          { id: 'view.save', label: 'Save View' },
          { id: 'view.load', label: 'Load View' },
        );
      }
      for (const d of availableDimensions(
        this.#snapshot,
        this.#options.dimensions ?? [],
      )) {
        items.push({ id: 'view.dimension', label: d.name, column: d.name });
      }
      // The HOST's own entries last, so its additions never push the
      // cube's own actions around as they come and go.
      for (const item of this.#options.hostMenu?.() ?? []) {
        items.push(item);
      }
      // A second press on the hamburger SHUTS it. Without this the
      // outside-press dismissal closes the menu and the click that
      // follows reopens it, so the button appears to do nothing and
      // the menu cannot be dismissed from the control that opened it.
      this.#menu.show([{ label: '', items }], event.clientX, event.clientY,
        burger);
    });
    bar.append(burger);

    this.#doc.addEventListener('keydown', (event) => {
      if (!(event.ctrlKey || event.metaKey)) return;
      const key = event.key.toLowerCase();

      if (key === 'e') {
        event.preventDefault();
        this.openEditor();
        return;
      }

      // NEVER take undo away from a text field. Inside a filter value
      // or the editor, Cmd-Z means "undo my typing", and stealing it
      // to roll back the whole cube would be the single most
      // destructive misfire in the product.
      if (isTextEntry(event.target)) return;

      if (key === 'z' && !event.shiftKey) {
        event.preventDefault();
        void this.#undo();
        return;
      }
      // Both spellings: Cmd-Shift-Z on macOS, Ctrl-Y on Windows.
      if ((key === 'z' && event.shiftKey) || key === 'y') {
        event.preventDefault();
        void this.#redo();
      }
    });
  }

  /**
   * Undo, with an answer either way.
   *
   * Three outcomes a person can tell apart: it worked, there was
   * nothing to undo, or it could not be applied. The last one matters
   * most -- the controller puts the cube back exactly as it was and
   * keeps the step, so the honest message is that nothing moved and
   * it can be tried again, not a stack trace.
   *
   * The rejection is caught HERE rather than left to `void`, which
   * does not catch and would surface an engine outage as an unhandled
   * promise rejection in the console.
   */
  async #undo(): Promise<void> {
    if (!this.#controller.canUndo) {
      this.#status('Nothing to undo', 'warn');
      return;
    }
    try {
      await this.#controller.undo();
    } catch {
      // refresh() already reported the cause through onError.
      this.#status('Could not undo — the cube is unchanged', 'error');
    }
  }

  async #redo(): Promise<void> {
    if (!this.#controller.canRedo) {
      this.#status('Nothing to redo', 'warn');
      return;
    }
    try {
      await this.#controller.redo();
    } catch {
      this.#status('Could not redo — the cube is unchanged', 'error');
    }
  }

  /** Entries the title bar menu adds on top of the grid's own. */
  #onHostAction(item: MenuItem): boolean {
    switch (item.id as string) {
      case 'view.undo':
        void this.#undo();
        return true;
      case 'view.redo':
        void this.#redo();
        return true;
      case 'view.save':
        this.saveView(this.#config.reportTitle ?? 'view');
        return true;
      case 'view.load':
        void this.loadView();
        return true;
      case 'view.dimension': {
        const found = (this.#options.dimensions ?? []).find(
          (d) => d.name === item.column,
        );
        if (found) this.useDimension(found);
        return true;
      }
      default: {
        // ANYTHING THE CUBE DOES NOT KNOW belongs to whoever put it
        // there. Without this a host could add an entry to the menu
        // and watch it do nothing, which is the dead-button fault
        // `menu-ids.test.ts` exists to prevent -- one layer up.
        const host = this.#options.onHostMenu;
        const mine = this.#options.hostMenu?.() ?? [];
        if (host && item.id !== undefined
          && mine.some((m) => m.id === item.id)) {
          host(item);
          return true;
        }
        return false;
      }
    }
  }

}

/**
 * Whether the event landed in something the user types into.
 *
 * contenteditable counts: it is a text field that simply is not an
 * input element, and a check that only looked at tag names would
 * hand the cube's undo to someone mid-word.
 */
function isTextEntry(target: EventTarget | null): boolean {
  if (!target || typeof (target as Element).closest !== 'function') {
    return false;
  }
  const el = target as HTMLElement;
  const tag = el.tagName;
  return (
    tag === 'INPUT'
    || tag === 'TEXTAREA'
    || tag === 'SELECT'
    || el.isContentEditable === true
    || el.closest('[contenteditable="true"]') !== null
  );
}

function fmt(n: number): string {
  if (!Number.isFinite(n)) return '—';
  return n.toLocaleString(undefined, { maximumFractionDigits: 2 });
}

/**
 * A three-way merge of an editor's draft: what changed between `base`
 * and `edited`, laid over `current`.
 *
 * The editor owns three things in the snapshot -- the row and column
 * pivots and the sorts -- and the whole configuration. Each is taken
 * from the edit only where the edit differs from its base, key by
 * key down through the configuration's objects, so two windows that
 * touched different settings both keep what they did.
 */
export function mergeDraft(current: CubeDraft, base: CubeDraft, edited: CubeDraft):
CubeDraft {
  const same = (a: unknown, b: unknown): boolean =>
    JSON.stringify(a) === JSON.stringify(b);
  const plain = (v: unknown): v is Record<string, unknown> =>
    typeof v === 'object' && v !== null && !Array.isArray(v);
  const merge = (now: unknown, was: unknown, next: unknown): unknown => {
    if (same(was, next)) return now;
    if (!plain(now) || !plain(was) || !plain(next)) return next;
    const out: Record<string, unknown> = { ...now };
    for (const key of new Set([...Object.keys(was), ...Object.keys(next)])) {
      const merged = merge(now[key], was[key], next[key]);
      if (merged === undefined) delete out[key];
      else out[key] = merged;
    }
    return out;
  };
  const snapshot: CubeSnapshot = {
    ...current.snapshot,
    rows: merge(current.snapshot.rows, base.snapshot.rows,
      edited.snapshot.rows) as CubeSnapshot['rows'],
    pivotOn: merge(current.snapshot.pivotOn, base.snapshot.pivotOn,
      edited.snapshot.pivotOn) as CubeSnapshot['pivotOn'],
    sorts: merge(current.snapshot.sorts, base.snapshot.sorts,
      edited.snapshot.sorts) as CubeSnapshot['sorts'],
  };
  return {
    snapshot: applyToSnapshot(snapshot,
      merge(current.config, base.config, edited.config) as CubeConfiguration),
    config: merge(current.config, base.config, edited.config) as CubeConfiguration,
    dimensions: edited.dimensions,
  };
}
