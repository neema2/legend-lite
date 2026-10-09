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

import { UI_LOCALE } from '../../engine-client/src/locale.ts';
import { hostOf, receiptLabel, receiptLines } from '../../engine-client/src/receipt.ts';
import { sessionExpired, WarehouseEngine } from '../../engine-client/src/warehouse.ts';
import { isQueryFailure, type QueryRunner } from './runner.ts';
import {
  CubeController,
  pivotHeaderPaths,
  type CubeControllerOptions,
  type CubeView,
  type Planner,
} from './cube.ts';
import {
  applyToSnapshot,
  columnConfig,
  mergeColumnOrder,
  fromSnapshot,
  labelFor,
  toColumnAppearance,
  toColumnLayout,
  renderFormats,
  leafFormats,
  DEFAULT_CONFIGURATION,
  DEFAULT_MAX_ROWS,
  renameColumnConfig,
  withColumn,
  withSettings,
  type CubeConfiguration,
  type Patch,
} from './config.ts';
import type { Dimension } from './dimensions.ts';
import { availableDimensions, useDimension } from './dimensions.ts';
import { AdHocMode } from './adhoc/mode.ts';
import { carryOver } from './adhoc/outline.ts';
import { AdHocSession } from './adhoc/session.ts';
import { drillLambda, levelLambda } from './query.ts';
import { findAll, type AppliedProperty, type ValueSpecification } from '../../pure-protocol/src/index.ts';
import { isPivotTotalColumn, withoutConditions } from './snapshot.ts';
import type { Lambda } from '../../pure-protocol/src/index.ts';
import type { QueryEngine } from '../../engine-client/src/engine.ts';
import { describePlane, type RemoteSource } from '../../engine-client/src/snap.ts';
import type { SnapTarget } from './cube.ts';
import { exportCsv, exportFileName, toCsv, toEml } from './export.ts';
import {
  ALERT_WINDOW,
  CODE_CHECK_WINDOW,
  EXECUTION_ERROR_WINDOW,
  buildAlert,
  buildCodeCheckAlert,
  buildExecutionErrorAlert,
  signInAgain,
  type AlertOptions,
} from './ui/alert.ts';
import { pivotLabel, type PivotColumn } from './query.ts';
import { toHtml } from './export-rich.ts';
import { toPdf, toPlainText } from './export-doc.ts';
import { exportTable, type ExportPage } from './export-model.ts';
import { toXlsx, XLSX_MIME } from './export-xlsx.ts';
import { cubeScopeOf, newCubeScope } from './ui/scope.ts';
import type { CubePage, ChartSource, SpawnedGrid, SpawnOptions } from './page/cube-page.ts';
import { PAGE_CUBE, pageToJson, writePage, type ChartView, type PageDocument, type PageViews } from './page-document.ts';
import { FormatterCache, type ColumnFormat } from './format.ts';
import { DataGrid } from './grid/grid.ts';
import {
  TREE_COLUMN,
  buildColumnModel,
  type ColumnLayout,
  type LeafColumn,
} from './grid/columns.ts';
import { writeCube, type CubeDocument, type CubeSource } from './cube-document.ts';
import { selectionStats, selectionTable, type CellRange } from './selection.ts';
import type { ResultTable, Scalar } from '../../engine-client/src/result.ts';
import type { JsonColumnReader } from './ui/json-fields.ts';
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
import { CubeStateOwner, type ChangeOptions, type CubeState, type OwnerEvent, type Outcome } from './cube-state.ts';
import { ColumnEditor, type ColumnEditorStart, type ColumnPreview } from './ui/column-editor.ts';
import {
  booleanSetting,
  numericSetting,
  readSettings,
  type SettingKey,
  type SettingValues,
} from './settings.ts';
import { SETTINGS_WINDOW, buildSettingsPanel } from './ui/settings-panel.ts';
import { buildDocumentation, isDocKey, type DocKey } from './ui/docs.ts';
import { columnRef } from './calc.ts';
import { CubeEditor, draftFor, type CubeDraft } from './ui/editor.ts';
import { FilterEditor } from './ui/filter-editor.ts';
import {
  applyMenuAction,
  buildMenu,
  calcStageOf,
  emailItems,
  exportItems,
  type MenuGroup,
  type MenuItem,
} from './ui/menu.ts';
import { MenuView } from './ui/menu-view.ts';
import { makeWindow, type WindowOptions, type WindowSpec } from './ui/window.ts';
import {
  PivotPanel,
  currentHeaderDrag,
  type Zone,
  type ZoneLayout,
} from './ui/pivot-panel.ts';
import {
  ColumnsToolPanel,
  type ColumnsPanelChild,
} from './ui/columns-panel.ts';
import { isVariant } from '../../engine-client/src/types.ts';


/** Rows sampled to infer what a JSON column holds. */
const JSON_SAMPLE_ROWS = 1000;
/** The row-count column of a JSON column's count query (`#jsonReader`). */
const JSON_ROWS = '__json_rows';

/**
 * How this cube turns Pure into rows -- one of two arrangements.
 *
 * A planner and a local engine, which is both browser planes; or a
 * runner, which is how the remote-engine plane is built
 * (`new RemoteRun(executor)`). Spelled as a union so the two forms
 * are visible in the type rather than enforced by a comment, and so
 * a caller cannot pass half of each.
 */
/**
 * A grid over another source (New ▸ Source…): what it shows first, and what runs it -- its own
 * engine and its own planner over its own model (a planner's `withModel`), so no two sources'
 * models meet.
 */
export interface GridSource {
  readonly snapshot: CubeSnapshot;
  readonly configuration?: CubeConfiguration;
  readonly place: CubeAppQuerySource;
  readonly snapTarget?: SnapTarget;
  readonly heldCopy?: HeldCopy;
  readonly cubeSource?: CubeSource;
  /** What its header says it reads: a file's name, `sales.orders`, a saved query's name. */
  readonly label: string;
}

export type CubeAppQuerySource =
  | {
    readonly engine: QueryEngine;
    readonly planner: Planner;
    /**
     * A remote LIVE engine (the warehouse). Queries run there while live;
     * Snap copies the user's rows into `engine` and queries run here.
     */
    readonly live?: QueryEngine & RemoteSource;
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
  /** Every view that lands, for a host's own chrome. */
  readonly onView?: (view: CubeView) => void;
  /**
   * The cube's state changed: a view landed, or a presentation change (a
   * colour, a width) that runs no query. What "changed since saved" listens to.
   */
  readonly onChange?: () => void;
  /** The groups to open on (a saved cube's), instead of the configuration's expand level. */
  readonly tree?: TreeState;
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
  /**
   * Where the cube's rows come from, as a saved cube names it (a file by its fingerprint).
   * Given, the cube can write itself down (`cubeDocument`, Export > Cube File); absent, it
   * cannot -- a cube over a model waits for the model home.
   */
  readonly cubeSource?: CubeSource;
  /**
   * Where the cube's windows (dialogs) float, and what they are kept inside: a page of several
   * cubes gives its whole area, so a dialog opened from a small tile is not squeezed into the
   * tile (plan F6, blocker #4). It must be positioned (relative or absolute). Default: the cube.
   */
  readonly windowHost?: HTMLElement;
  /** A small cube in a tile (a chart's editing grid): its columns panel starts folded away. */
  readonly compact?: boolean;
  /** What the grid reads, in the host's own words (its header says it); else the cube works it out. */
  readonly sourceLabel?: string;
  /**
   * A grid on a page (page/cube-page.ts): its "+ Chart" and "New grid" go to the page, which
   * puts them on its board, instead of a board of this cube's own.
   */
  readonly onChart?: () => void;
  readonly onNewGrid?: () => void;
  /**
   * ANOTHER SOURCE, for New ▸ Source…: the host asks the person which (its source picker) and
   * answers what reads it, or undefined when they chose nothing. Absent: no New ▸ Source….
   */
  readonly openSource?: () => Promise<GridSource | undefined>;
  /** New ▸ Blank Page: the host clears the page -- every grid and chart -- for a first data source. */
  readonly onBlankPage?: () => void;
  /** A grid on a page: its New ▸ Data Source… goes to the page, as `onNewGrid` does. */
  readonly onNewSource?: (make: (host: HTMLElement, options: SpawnOptions) => SpawnedGrid) => void;
  readonly writeClipboard?: (text: string) => void | Promise<void>;
  /**
   * Hand a file to the user.
   *
   * Injected because a download is a host concern -- a page does it
   * with an anchor, an embedder may want a save dialog -- and
   * because a test must not depend on a browser writing to disk.
   */
  readonly download?: (name: string, mime: string, content: string | Uint8Array) => void;
  /**
   * Hand a message with an attachment to the host's mail client.
   *
   * A host concern, and unavoidably so: a browser cannot attach a
   * file to a mailto: link. Absent, Email downloads an unsent `.eml` draft
   * (upstream's way) through `download` instead. The attachment is BYTES for
   * a binary format (a PDF, a workbook) and text for the others.
   */
  readonly email?: (message: {
    readonly subject: string;
    readonly body: string;
    readonly attachment: {
      readonly name: string;
      readonly mime: string;
      readonly content: string | Uint8Array;
    };
  }) => void | Promise<void>;
  /**
   * Settings the host kept, by upstream's keys (`dataCube.grid.rowBuffer`
   * and so on) -- upstream's `settingsData.values`. Unknown keys are
   * ignored.
   */
  readonly settings?: Readonly<Record<string, unknown>>;
  /** Settings were saved: upstream's `onSettingsChanged`, for a host to keep them. */
  readonly onSettingsChanged?: (values: SettingValues) => void;
  /** Show the column drag zone. Off matches DataCube exactly. */
  readonly showColumnZone?: boolean;
  /**
   * Open with the cube's controls hidden -- title bar, drag zones, columns panel, status bar --
   * the grid alone, for the most room (a host's results area). The grid's right-click menu
   * brings them back ("Show Controls", its last entry). View state: not saved with the cube.
   */
  readonly controlsHidden?: boolean;
  /**
   * Where a snap materialises: a relation the model declares, so a snapped
   * cube is planned as a live one is. Without one the cube cannot snap.
   */
  readonly snapTarget?: SnapTarget;
  /**
   * The rows are ALREADY a copy in this tab: a file opened into its DuckDB,
   * rows generated there. Nothing can move them while the person works, so
   * the cube is snapped from the moment it opens -- and snapping again would
   * copy a copy. The plane indicator says Snapped, with when and how many,
   * and offers nothing to click. What decides it is where the rows ARE, not
   * which engine reads them: a remote file mounted in DuckDB-WASM is read
   * afresh by every query, so it is live and can be snapped.
   */
  readonly heldCopy?: HeldCopy;
}

/** A source that is a copy in this tab from the start (`heldCopy`). */
export interface HeldCopy {
  /** What the rows are, as the person would name them (a file's name). */
  readonly label: string;
  /** When they were copied into the tab. */
  readonly takenAt: Date;
  readonly rowCount: number;
}

/**
 * Everything the app needs, and exactly one way of getting rows.
 *
 * The intersection is what makes "a planner and an engine" and "a
 * runner" both complete and mutually exclusive: pass half of each and
 * it does not compile.
 */
export type CubeAppOptions = CubeAppBaseOptions & CubeAppQuerySource;

/** DataCube's --ag-row-height. Kept beside the CSS token in theme.css. */
const DATACUBE_ROW_HEIGHT = 20;

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
    `${view.rows.rowCount.toLocaleString(UI_LOCALE)} rows × ` +
    `${cols} cols in ${view.rows.elapsedMs.toFixed(0)}ms`
  );
}

/**
 * What a cube tells whoever listens (`CubeApp#on`): a page, a pinned chart and the host can all
 * listen at once, where the options' callbacks had room for one (plan F6, blocker #5).
 */
export interface CubeEvents {
  /** A view landed (after the cube took it in). */
  readonly view: readonly [view: CubeView];
  /** The cube's state changed: a view landed, or a presentation change that runs no query. */
  readonly change: readonly [];
  /** A status line. */
  readonly status: readonly [text: string, kind: 'ok' | 'warn' | 'error'];
  /** The snap state changed. */
  readonly plane: readonly [];
}

/** The z-index windows start from, above the cube's own layers. */
const WINDOW_Z_BASE = 20;
/** The top of each window host's stack of windows (`CubeApp#raise`). */
const windowStack = new WeakMap<HTMLElement, number>();

/**
 * Which cube on a document a keystroke from OUTSIDE every cube belongs to: the one last
 * clicked or focused. With several cubes on a page (plan F6), Ctrl-Z pressed on the page's
 * body must undo one cube, not all of them; with one, it still undoes that one.
 */
const lastTouched = new WeakMap<Document, object>();

/** A change's refusal in the words the user reads, or null when it was not refused. */
function refusal(out: Outcome): string | null {
  if (out.kind !== 'refused') return null;
  return out.error instanceof Error ? out.error.message : String(out.error);
}

/**
 * THE PAGE'S MODULE (page/cube-page.ts: the board, its layouts, the charts' panel), fetched the first time a cube gets
 * a page -- a chart, a second grid -- and once for every cube in the tab. A grid alone never downloads it (the bundle's
 * budget, test/bundle-budget.test.ts).
 */
let pageModule: Promise<typeof import('./page/cube-page.ts')> | undefined;
function loadPage(): Promise<typeof import('./page/cube-page.ts')> {
  pageModule ??= import('./page/cube-page.ts');
  return pageModule;
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
  /** The title bar's menu button, while the bar shows: where the page's layouts open from (Arrange...). */
  #burger: HTMLElement | null = null;
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
    { spec: HeatmapSpec; byDepth: Map<number, HeatmapRange>; type: string | undefined }
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
  /** What is running, for the status bar's progress (upstream's TaskService). */
  readonly #tasks: { readonly description: string }[] = [];
  /** Work a change waits on before its query runs, still out (busy). */
  #checking = 0;
  #endFetch: (() => void) | null = null;
  readonly #progress: HTMLElement;
  /** Where the grid was scrolled when the context menu opened. */
  #menuScroll: { top: number; left: number } | null = null;
  /** The controls hidden, the grid alone (`controlsHidden`); view state, never saved. */
  #controlsHidden = false;
  /** Settings > ...: defaults under what the host kept. */
  #settings: SettingValues;
  /** The open calculated-column editors, by window key. */
  readonly #columnEditors = new Map<string, ColumnEditor>();
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
  /** The board of the grid and its charts (page/cube-page.ts), once a chart is open; until then the grid sits alone. */
  #page: { readonly page: CubePage; readonly host: HTMLElement } | null = null;
  /** The grid's part of its tile's header (`tileHead`). */
  readonly #tileHeadEl: HTMLElement;
  /** Where the grid lives when there is no board. */
  #middle: HTMLElement | null = null;
  #newColumns = 0;
  /** Alerts are many and untitled, so each gets its own window key. */
  #alerts = 0;
  /**
   * THE CUBE'S STATE -- snapshot, configuration, open groups, history -- and
   * its one owner (cube-state.ts, Leg B). Nothing in this class holds a copy:
   * presentation paints `current`, the grid shows the owner's view, and every
   * change goes through `#change`. The guardrail test (state-guardrail.test.ts)
   * holds that nothing outside cube-state.ts assigns cube state.
   */
  readonly #owner: CubeStateOwner;
  /** What the title bar was last built from (`#paintState`). */
  #chrome = '';
  /** Stops the owner's events reaching this app: see `dispose`. */
  readonly #unsubscribe: () => void;
  /** Set by `dispose`: nothing reaches the host, the screen or a window after it. */
  #disposed = false;
  /** The open Filters window, told whenever the cube's filter changes (P2-144). */
  #filters: FilterEditor | null = null;
  /** Drill-throughs, latest wins: an earlier one answering late is dropped (P2-131). */
  #drills = 0;
  #selection: CellRange | null = null;
  /** Where selection statistics are written, inside the status bar. */
  #statsSlot: HTMLElement | null = null;
  /** Repaints the plane toggle, which is now the only plane badge. */
  #paintSnap: (() => void) | null = null;
  /** Switch plane as the button does (a retry after signing in again); null with no toggle. */
  #planeToggle: (() => void) | null = null;
  /** The Receipts window's body while it is open (`#paintReceipts`). */
  #receiptsBody: HTMLElement | null = null;
  /** Ad Hoc Analysis mode while it is on; the cube's own grid is hidden then. */
  #adhoc: AdHocMode | null = null;
  /** The shortcuts this app listens for on the DOCUMENT: see `dispose`. */
  #onDocKey: ((event: KeyboardEvent) => void) | null = null;
  /** Marks this cube as the document's last touched (see `lastTouched`). */
  readonly #onTouch = (): void => { lastTouched.set(this.#doc, this); };
  readonly #listeners = new Map<keyof CubeEvents, Set<unknown>>();

  constructor(
    root: HTMLElement,
    snapshot: CubeSnapshot,
    options: CubeAppOptions,
  ) {
    this.#doc = root.ownerDocument;
    this.#progress = this.#doc.createElement('div');
    this.#progress.className = 'dc-status-progress';
    this.#progress.setAttribute('role', 'progressbar');
    this.#options = options;
    this.#settings = readSettings(options.settings);
    // Read the snapshot back rather than starting from defaults: a
    // cube can arrive from a saved cube or a colleague's link, and
    // the editor must open on what is actually running. The tree starts
    // as the CONFIGURATION says (the root total, the expand level) unless
    // the host hands the groups a saved cube had open.
    const configuration = fromSnapshot(snapshot, options.configuration);
    this.#owner = new CubeStateOwner({
      snapshot,
      configuration,
      tree: options.tree ?? TreeState.empty(configuration.showRootAggregation)
        .withExpandTo(configuration.initialExpandToLevel ?? 0),
    }, (state) => this.#controller.run(state), {
      historyLimit: numericSetting(this.#settings, 'dataCube.editor.maxHistoryStackSize'),
      abort: () => this.#controller.cancel(),
    });
    Object.assign(this.#formats, renderFormats(this.#config, this.#snapshot));

    root.classList.add('dc-app');
    this.#tileHeadEl = this.#doc.createElement('span');
    this.#tileHeadEl.className = 'dc-tile-cube';
    // a grid in a page's tile: the page has the title bar
    root.classList.toggle('dc-compact', this.#options.compact === true);
    this.#controlsHidden = this.#options.controlsHidden === true;
    root.classList.toggle('dc-controls-hidden', this.#controlsHidden);
    // which cube this is, on a page of several: its drags land only on it (ui/scope.ts)
    root.dataset['dcCube'] = newCubeScope();
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
    this.#middle = middle;
    middle.append(this.#els.grid);
    const side = this.#doc.createElement('div');
    side.className = 'dc-app-side';
    middle.append(side);
    root.append(this.#els.stats);

    this.#columnsPanel = new ColumnsToolPanel(side, {
      ...(this.#options.compact ? { collapsed: true } : {}),
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
      onChange: (layout) => this.#onLayout(layout),
      orientation: 'list',
      showColumnZone: true,
    });

    this.#pivots = new PivotPanel(zones, {
      canGroup: (c) => this.#isDimension(c),
      labelFor: (c) => labelFor(this.#config, c),
      onChange: (layout) => this.#onLayout(layout),
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
      overscan: numericSetting(this.#settings, 'dataCube.grid.rowBuffer'),
      // Upstream fits every column to its content after each fetch.
      autoFit: true,
      formats: this.#formats,
      appearance: this.#config.appearance,
      columnAppearance: toColumnAppearance(this.#config),
      canGroup: (c) => this.#isDimension(c),
      onReorder: (order, added) => {
        // A COLUMN DRAGGED OUT OF THE PANEL INTO THE GRID IS A
        // REQUEST TO SHOW IT. The grid reports the arrival because
        // it cannot know the column was hidden -- it was never in
        // the model -- and unhiding is the configuration's to do.
        // MERGED, not written over. The grid reports only what it is
        // showing, so writing its report straight in dropped every
        // grouped, pivoted and hidden column out of the order -- and
        // the panel, which sorts by it, threw them to the end of the
        // list. One drag and the grouped columns jumped. One change:
        // the unhide and the order together.
        void this.#configure((c) => withSettings(
          added !== undefined ? withColumn(c, added, { hidden: false }) : c,
          { columnOrder: mergeColumnOrder(this.#columnOrder(), order) },
        ), 'reorder columns');
      },
      cellBackground: (leaf, row, value) => this.#heatFor(leaf, row, value),
      rowMeta: (abs) => this.#rowMeta(abs),
      // SET, never toggled: the grid says which way the person asked, and
      // asking for what already is changes nothing (P2-127).
      onToggleExpand: (key, expanded) => {
        void this.#change((s) => ({ ...s, tree: s.tree.setOpen(parsePathKey(key), expanded) }),
          { label: expanded ? 'expand' : 'collapse' });
      },
      onSelectionChange: (range) => this.#onSelectionChange(range),
      onActivateCell: (row, column) => {
        void this.#drillThrough(row, column);
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
      ...(options.runner === undefined && options.live ? { live: options.live } : {}),
      onQueryFailure: (error) => this.#offerSignIn(error),
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
    this.#unsubscribe = this.#owner.subscribe((event) => this.#onState(event));
    this.#listenForKeys();

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
      const drag = currentHeaderDrag(root);
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
  /** The cube's state and its history: read it, or change it through `change`. */
  get state(): CubeStateOwner {
    return this.#owner;
  }
  get configuration(): CubeConfiguration {
    return this.#config;
  }
  get snapshot(): CubeSnapshot {
    return this.#snapshot;
  }
  /** The open groups. */
  get tree(): TreeState {
    return this.#owner.current.tree;
  }
  /** The view on screen. */
  get view(): CubeView | null {
    return this.#view;
  }
  /**
   * A change is in flight: the work it waits on before its query runs (#check: the Filter and Properties windows'
   * compile checks, the column window's parse and live check, Snap's copy) or its query has not answered yet. The
   * harnesses wait on this; the Loading overlay is the owner's alone.
   */
  get busy(): boolean {
    return this.#owner.busy || this.#checking > 0;
  }
  get canUndo(): boolean {
    return this.#owner.canUndo;
  }
  get canRedo(): boolean {
    return this.#owner.canRedo;
  }
  /** Undo, as the menu's Undo: a refusal is said, never thrown. */
  async undo(): Promise<void> {
    await this.#undo();
  }
  async redo(): Promise<void> {
    await this.#redo();
  }

  /** Are the cube's controls hidden (the grid alone)? */
  get controlsHidden(): boolean {
    return this.#controlsHidden;
  }

  /**
   * Hide the cube's controls -- title bar, drag zones, columns panel, status bar -- for the grid
   * alone, or bring them back. View state: no query, no undo step, not saved with the cube. The
   * grid re-measures itself (it watches its own size).
   */
  setControlsHidden(hidden: boolean): void {
    this.#controlsHidden = hidden;
    this.#els.root.classList.toggle('dc-controls-hidden', hidden);
  }

  /** Run the cube and show it: the first query. Not an undo step. */
  async open(): Promise<void> {
    await this.#owner.refresh();
  }

  /**
   * Change the cube: `update` derives the next state from the current one.
   * Presentation (a colour, a width) commits at once and runs no query; a
   * change to the query is one transaction -- it commits as the engine
   * returned it, or is refused and everything repaints from what was on
   * screen, the refusal said where the user reads it. Never throws.
   */
  change(update: (state: CubeState) => CubeState, options?: ChangeOptions): Promise<Outcome> {
    return this.#change(update, options);
  }

  // -- the cube's state, read ----------------------------------------------

  /** What presentation paints: the change in flight, else what was accepted. */
  get #snapshot(): CubeSnapshot {
    return this.#owner.current.snapshot;
  }

  get #config(): CubeConfiguration {
    return this.#owner.current.configuration;
  }

  /** The view on screen, and its rows. */
  get #view(): CubeView | null {
    return this.#owner.view;
  }

  /**
   * The snapshot of the view ON SCREEN. Anything read off the rows a person
   * is looking at -- which column a clicked row groups by, what a cell is --
   * reads this, never the change still in flight: during a regroup the
   * menu named the pending grouping for the old rows (P2-110).
   */
  get #shown(): CubeSnapshot {
    return this.#owner.rendered.snapshot;
  }

  get #treeRows(): readonly TreeRow[] {
    return this.#owner.view?.treeRows ?? [];
  }

  // -- the cube's state, changed -------------------------------------------

  #change(update: (state: CubeState) => CubeState, options?: ChangeOptions): Promise<Outcome> {
    return this.#owner.change(update, options);
  }

  /** A change of the configuration alone. */
  #configure(update: (config: CubeConfiguration) => CubeConfiguration, label: string): Promise<Outcome> {
    return this.#change((s) => ({ ...s, configuration: update(s.configuration) }), { label });
  }

  /** A change of the snapshot alone. */
  #query(update: (snapshot: CubeSnapshot) => CubeSnapshot, label: string): Promise<Outcome> {
    return this.#change((s) => ({ ...s, snapshot: update(s.snapshot) }), { label });
  }

  /**
   * EVERY change of the cube's state reaches the screen HERE, and only here:
   * pending (paint what was asked for), committed (a view landed),
   * presentation (repaint the view with the new settings), refused or
   * cancelled (repaint from what was on screen, and say why). One place
   * paints from the state, so an undo, a refusal and a change all put back
   * the same things -- zones, formats, appearance, title bar, panels
   * (P2-102, P2-108).
   */
  #onState(event: OwnerEvent): void {
    this.#setBusy(this.#owner.busy);
    this.#paintState();
    // the Filters window follows the filter the cube HAS
    this.#filters?.rebase(this.#owner.committed.snapshot.filter);
    switch (event.kind) {
      case 'pending':
        return;
      case 'committed':
        this.#onView(event.view);
        this.#emit('change');
        return;
      case 'presentation':
        if (this.#view) this.#paintView(this.#view);
        this.#emit('change');
        return;
      case 'refused':
        if (this.#view) this.#paintView(this.#view);
        this.#reportFailure(event.error);
        if (event.reverted.length > 1) {
          this.#status(`${event.reverted.length} changes were undone: ${event.reverted.join(', ')} -- ${
            event.error instanceof Error ? event.error.message : String(event.error)}`, 'error');
        }
        return;
      case 'cancelled':
        if (this.#view) this.#paintView(this.#view);
        return;
    }
  }

  /** Upstream's "Loading..." overlay and its own task, while a change is in flight. */
  #setBusy(busy: boolean): void {
    this.#grid.setBusy(busy);
    if (busy && !this.#endFetch) this.#endFetch = this.#startTask('Fetching data...');
    if (!busy) {
      this.#endFetch?.();
      this.#endFetch = null;
    }
  }

  /** What is drawn from the state alone: zones, formats, appearance, panels, chrome. */
  #paintState(): void {
    const s = this.#snapshot;
    this.#pivots.setColumns(s.rows, s.pivotOn);
    this.#sideZones.setColumns(s.rows, s.pivotOn);
    this.#refreshFormats();
    // Fonts, colours, grid lines and row highlights: the grid held the
    // appearance it was built with until this existed.
    this.#grid.setAppearance(this.#config.appearance, toColumnAppearance(this.#config));
    this.#refreshToolPanel();
    // The title bar is REBUILT only when what it shows changed (its title, its folds): it was
    // rebuilt on every event, a query starting included, which could close its own menu.
    const c = this.#config;
    const chrome = JSON.stringify([c.reportTitle ?? null, c.showDragZones, c.showTitleBar]);
    if (chrome !== this.#chrome) {
      this.#chrome = chrome;
      this.#renderChrome();
    } else {
      this.#applyChrome();
    }
  }

  // -- assembly ------------------------------------------------------

  #div(parent: HTMLElement, className: string): HTMLElement {
    const el = this.#doc.createElement('div');
    el.className = className;
    parent.appendChild(el);
    return el;
  }

  #status(text: string, kind: 'ok' | 'warn' | 'error' = 'ok'): void {
    this.#emit('status', text, kind);
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
  #kindOf(column: string, snapshot: CubeSnapshot = this.#snapshot): ColumnKind | undefined {
    const c = rowColumns(snapshot).find((x) => x.name === column);
    if (!c) return undefined;
    return c.derived
      ? c.kind
      : columnConfig(this.#config, column).kind ?? c.kind;
  }

  /** Re-fill the format map in place. See `#formats`. */
  #refreshFormats(): void {
    for (const key of Object.keys(this.#formats)) delete this.#formats[key];
    Object.assign(this.#formats, renderFormats(this.#config, this.#snapshot));
    // What is ON SCREEN renders by its own leaf's type: a measure's result type, which
    // aggregation changes (the average of an Integer is a Float), never its source's.
    const view = this.#view;
    if (!view) return;
    // and by its KIND: a numeric dimension reads as written (no thousands separators)
    const kinds = new Map(rowColumns(this.#shown).map((c) => [c.name, c.kind]));
    const byLeaf = leafFormats(this.#config, view.columns.leaves.map((leaf) => ({
      ...leaf, type: view.rows.columns[leaf.index]?.type, kind: kinds.get(leaf.name),
    })));
    for (const [name, format] of Object.entries(byLeaf)) {
      if (format) this.#formats[name] = format;
      else delete this.#formats[name];
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
        name: c.name, ...(c.type === undefined ? {} : { type: c.type }) })),
      ...(this.#snapshot.groupDerived ?? []).map((d) => ({
        name: d.name, ...(d.type === undefined ? {} : { type: d.type }) })),
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
    // while the grid showed five of it. The view's PLAN is the list:
    // each column carries its measure and its values, so nothing here
    // reads a pivot name back apart.
    const planned: readonly PivotColumn[] = this.#view?.pivot?.columns ?? [];
    // THE MEASURE'S SOURCE COLUMN, not the measure's name: a measure is
    // `{ name: 'total', column: 'notional' }` as often as it is
    // `notional` twice over.
    // Several measures can come off one column -- a sum and an
    // average of notional -- and then the values alone name two
    // children the same. Their measure tells them apart.
    const perColumn = new Map<string, number>();
    for (const m of new Set(planned.map((c) => c.measure.name))) {
      const column = planned.find((c) => c.measure.name === m)?.measure.column ?? m;
      perColumn.set(column, (perColumn.get(column) ?? 0) + 1);
    }
    const totalLabel = this.#config.pivotStatisticColumnName ?? 'Total';
    const leafType = (name: string): string | undefined =>
      this.#view?.rows.columns.find((r) => r.name === name)?.type;
    // Each measure's pivot TOTAL is one more of its columns on screen,
    // listed last so it can be hidden like any other.
    const childrenOf = (column: string): ColumnsPanelChild[] => [
      ...planned.filter((c) => c.tuple !== null && c.measure.column === column),
      ...planned.filter((c) => c.tuple === null && c.measure.column === column),
    ].map((c) => {
      const values = c.tuple === null ? totalLabel : c.tuple.map(pivotLabel).join(' \u203a ');
      return {
        name: c.name,
        // The values alone where the measure is the row above, and the
        // measure too where the row above covers more than one.
        label: (perColumn.get(column) ?? 1) > 1 ? `${values} \u00b7 ${c.measure.name}` : values,
        visible: columnConfig(this.#config, c.name).hidden !== true,
        // the leaf's own type: the level query's plan typed it
        ...(leafType(c.name) === undefined ? {} : { type: leafType(c.name) as string }),
      };
    });

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
            ...(c.type === undefined ? {} : { type: c.type }),
            groupable: this.#isDimension(c.name),
            visible: !hidden,
            ...(children.length > 0 ? { children } : {}),
            ...(rows.has(c.name) ? { usedAs: 'rows' as const } : {}),
          };
        }),
    );
  }

  /** A view LANDED (the owner committed it): take it in, then paint it. */
  #onView(view: CubeView): void {
    this.#debug('query', { query: view.query, sql: view.sql, rows: view.rows.rowCount,
      ms: view.rows.elapsedMs, snapshot: view.snapshot });
    // Open column editors compile against the cube as it is now.
    for (const editor of this.#columnEditors.values()) editor.recheck();
    // Open charts follow the cube: a new filter or calculated column redraws them.
    this.#page?.page.refresh();
    this.#page?.page.reconcile(this.#snapshot.filter);
    this.#paintView(view);
    this.#paintReceipts();
    this.#reportSchemaChanges(view);
    // The host LAST, once the app has taken the view in: told first, it read the app one view
    // behind (the snapshot was still the previous one), so "changed since saved" missed the
    // change that had just landed.
    this.#emit('view', view);
  }

  /**
   * The view on screen, laid out with the CURRENT settings: after it lands,
   * and again after a presentation change (a width, a pin, a colour), which
   * runs no query.
   */
  #paintView(view: CubeView): void {
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
      pivotHeaderPaths(view.pivot?.columns),
    );
    // Refreshed AFTER the view lands, because the pivot's leaf names
    // are only known once the engine has answered.
    this.#refreshFormats();
    this.#refreshHeatmaps(view);
    this.#grid.setColumns(model);
    this.#grid.setSorts(view.snapshot.sorts);
    this.#grid.setRows(view.rows, 0, view.rows.rowCount);

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
        `${base} — showing the first ${(this.#config.maxRows ?? DEFAULT_MAX_ROWS).toLocaleString(UI_LOCALE)} of ` +
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
    // Ad Hoc owns the bar while it is on (`#renderAdHocStatus`)
    if (this.#adhoc) return;
    const doc = this.#doc;

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
    const { left, right } = this.#statusFrame();

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
    const rows = this.#readout(right, timingText(view, cols));
    rows.title = 'Rows and columns in the result, and how long the '
      + 'query took.';
    this.#receiptChip(right, view);

    if (view.truncated.length > 0 && this.#config.showTruncationWarning) {
      right.append(this.#statusSeparator());
      const warn = doc.createElement('div');
      warn.className = 'dc-status-warning';
      const cap = (this.#config.maxRows ?? DEFAULT_MAX_ROWS).toLocaleString(UI_LOCALE);
      // a flat cube says how many there are in all (cube.ts `totalRows`): the first 1,000 of 48,213
      warn.textContent = view.totalRows !== undefined
        ? `⚠ Showing the first ${cap} of ${view.totalRows.toLocaleString(UI_LOCALE)} rows (row limit)`
        : `⚠ Results truncated to fit within row limit (${cap})`;
      right.append(warn);
    }

    const stats = doc.createElement('div');
    stats.className = 'dc-status-stats';
    right.append(this.#statusSeparator(), stats);
    this.#statsSlot = stats;
    this.#renderSelectionStats();

    this.#statusTail(right);
  }

  /**
   * WHAT ANSWERED THIS VIEW, as the engines that ran it said: the receipt of the query behind
   * the rows (receipt.ts). The plane button is the tab's own account; this is the engine's --
   * the warehouse's statement id, the SQL legend-engine reports, or "nothing left this tab".
   */
  #receiptChip(right: HTMLElement, view: CubeView): void {
    const main = view.receipts.at(-1);
    if (main === undefined) return;
    const chip = this.#doc.createElement('button');
    chip.type = 'button';
    chip.className = `dc-status-receipt dc-receipt-${main.copy ? 'copy' : main.plane}`;
    const more = view.receipts.length > 1 ? ` (+${view.receipts.length - 1})` : '';
    chip.textContent = `${receiptLabel(main)}${more}`;
    chip.title = `${receiptLines(main).join('\n')}\n\nClick for every query's receipt`
      + (main.check ? ', and to check it with the server.' : '.');
    chip.addEventListener('click', () => this.openReceipts());
    right.append(this.#statusSeparator(), chip);
  }

  /** Every receipt behind the view on screen, each checkable with its server where it can be. */
  openReceipts(): void {
    this.#showOverlay('Receipts', (host) => {
      host.classList.add('dc-receipts');
      this.#receiptsBody = host;
      this.#paintReceipts();
    }, { replace: true });
  }

  /**
   * The Receipts window's contents, for the view on screen NOW: repainted in place as each view
   * lands, so an open window never shows the receipts of rows no longer displayed (it went on
   * saying "the warehouse" over a snap's rows).
   */
  #paintReceipts(): void {
    const host = this.#receiptsBody;
    if (!host?.isConnected || host.closest<HTMLElement>('.dc-app-overlay')?.hidden) return;
    host.replaceChildren();
    const receipts = this.#view?.receipts ?? [];
    {
      if (receipts.length === 0) {
        host.textContent = 'No query has answered this view yet.';
        return;
      }
      receipts.forEach((r, i) => {
        const box = this.#doc.createElement('div');
        box.className = 'dc-receipt';
        const head = this.#doc.createElement('div');
        head.className = 'dc-receipt-head';
        head.textContent = `Query ${i + 1} of ${receipts.length}: ${receiptLabel(r)}`;
        box.append(head);
        for (const line of receiptLines(r)) {
          const p = this.#doc.createElement('div');
          p.className = 'dc-receipt-line';
          p.textContent = line;
          box.append(p);
        }
        const check = r.check;
        if (check) {
          const button = this.#doc.createElement('button');
          button.type = 'button';
          button.className = 'dc-receipt-check';
          button.textContent = 'Check with the server';
          const answer = this.#doc.createElement('div');
          answer.className = 'dc-receipt-answer';
          button.addEventListener('click', () => {
            button.disabled = true;
            answer.textContent = 'asking the server…';
            check().then((text) => {
              answer.textContent = text;
            }, (e: unknown) => {
              answer.textContent = `The server could not be asked: ${e instanceof Error ? e.message : String(e)}`;
            }).finally(() => {
              button.disabled = false;
            });
          });
          box.append(button, answer);
        } else if (r.plane === 'engine') {
          const note = this.#doc.createElement('div');
          note.className = 'dc-receipt-line';
          note.textContent = 'This server issues no statement id and keeps no history to check against: '
            + 'its receipt is what its reply reported.';
          box.append(note);
        }
        host.append(box);
      });
    }
  }

  /** The status bar emptied, and its two sides: what you can do, what is true. */
  #statusFrame(): { left: HTMLElement; right: HTMLElement } {
    const bar = this.#els.stats;
    bar.replaceChildren();
    const left = this.#doc.createElement('div');
    left.className = 'dc-status-actions';
    const right = this.#doc.createElement('div');
    right.className = 'dc-status-readout';
    bar.append(left, right);
    return { left, right };
  }

  /** The row and column readout, at the start of the right side. */
  #readout(right: HTMLElement, text: string): HTMLElement {
    const rows = this.#doc.createElement('div');
    rows.className = 'dc-status-rows dc-status-timing';
    rows.textContent = text;
    right.append(rows);
    return rows;
  }

  /**
   * The end of every status bar: upstream's task progress (a bar while anything runs, what is
   * running in its tooltip; the same element every rebuild, so a task that outlives a view
   * keeps showing), then the host's own readout.
   */
  #statusTail(right: HTMLElement): void {
    right.append(this.#statusSeparator(), this.#progress);
    this.#adoptHostStatus();
  }

  /**
   * The status bar in Ad Hoc Analysis: its own counts, and none of the
   * cube's links -- the cube's Filter said "(on)" over numbers that ignore
   * it (P2-287), and the counts were the hidden cube's (P2-290).
   */
  #renderAdHocStatus(rows: number, cols: number, stale: boolean): void {
    const { right } = this.#statusFrame();
    this.#readout(right, `${rows.toLocaleString(UI_LOCALE)} rows \u00d7 ${cols} cols${stale ? ' (not refreshed)' : ''}`);
    this.#statusTail(right);
  }

  /**
   * Work a change waits on before its query runs, counted from the moment it starts until it settles: the cube is
   * busy while it is out, not only once the query starts. (The column window's live check starts after its typing
   * pause; it is counted from then.)
   */
  #check<T>(call: Promise<T>): Promise<T> {
    this.#checking += 1;
    return call.finally(() => {
      this.#checking -= 1;
    });
  }

  /** A task on the status bar's progress, until the returned end is called. */
  #startTask(description: string): () => void {
    const task = { description };
    this.#tasks.push(task);
    this.#paintProgress();
    return () => {
      const at = this.#tasks.indexOf(task);
      if (at >= 0) this.#tasks.splice(at, 1);
      this.#paintProgress();
    };
  }

  #paintProgress(): void {
    const tasks = this.#tasks;
    this.#progress.classList.toggle('dc-busy', tasks.length > 0);
    this.#progress.title = tasks.length > 1
      ? tasks.map((t, i) => `Task ${i + 1}/${tasks.length}: ${t.description}`).join('\n')
      : tasks[0]?.description ?? '';
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
    // WHERE THE PLANNER RUNS, changed where it is read (the user, 2026-09-30): the readout
    // opens the host's planes, when it has more than the one it is on.
    if (this.#hostItems('plane').length > 1) {
      slot.classList.add('dc-status-host-pick');
      slot.setAttribute('role', 'button');
      slot.tabIndex = 0;
      slot.title = 'Where the planner runs: click to change';
      const open = (): void => {
        if (this.#menu.open) {
          this.#menu.close();
          return;
        }
        const at = slot.getBoundingClientRect();
        this.#menu.show([{ label: '', items: this.#hostItems('plane') }], at.left, at.top, slot);
      };
      slot.addEventListener('click', open);
      slot.addEventListener('keydown', (e) => {
        if (e.key === 'Enter' || e.key === ' ') {
          e.preventDefault();
          open();
        }
      });
    }
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
    // Ad Hoc Analysis mode has its own axes; the cube's zones wait.
    this.#els.zoneBar.hidden = !zones || this.#adhoc !== null;
    this.#els.zoneBar.classList.toggle('dc-peeking', !this.#config
      .showDragZones && this.#zonePeek);
    this.#els.toolbar.classList.toggle(
      'dc-collapsed',
      !this.#config.showTitleBar,
    );
  }

  /** Fold the zones or the title bar away, or bring them back. */
  #setChrome(patch: {
    readonly showDragZones?: boolean;
    readonly showTitleBar?: boolean;
  }): void {
    // Presentation: no query, one undo step, painted by `#onState`.
    void this.#configure((c) => ({ ...c, ...patch }),
      patch.showTitleBar !== undefined ? 'title bar' : 'drag zones');
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
    this.#paintTileHead();
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
    button.append(this.#chevron('up'));
    button.title = 'Hide the drag zones';
    button.setAttribute('aria-label', 'Hide the drag zones');
    button.setAttribute('aria-expanded', 'true');
    button.addEventListener('click', () => {
      this.#setChrome({ showDragZones: false });
    });
    return button;
  }

  /** The zones' way back, in whichever title bar is on screen. */
  #zonesBack(): HTMLElement {
    const show = this.#doc.createElement('button');
    show.type = 'button';
    show.className = 'dc-titlebar-zones';
    show.append(this.#chevron('down'));
    show.title = 'Show the drag zones';
    show.setAttribute('aria-label', 'Show the drag zones');
    show.setAttribute('aria-expanded', 'false');
    show.addEventListener('click', () => {
      this.#setChrome({ showDragZones: true });
    });
    return show;
  }


  /**
   * A fold's chevron, drawn rather than typed: the ⌃ / ⌄ glyphs sat
   * off-centre and changed size with the font.
   */
  #chevron(direction: 'up' | 'down'): SVGSVGElement {
    const ns = 'http://www.w3.org/2000/svg';
    const svg = this.#doc.createElementNS(ns, 'svg');
    svg.setAttribute('class', 'dc-chevron-icon');
    svg.setAttribute('viewBox', '0 0 12 12');
    svg.setAttribute('width', '12');
    svg.setAttribute('height', '12');
    svg.setAttribute('aria-hidden', 'true');
    const path = this.#doc.createElementNS(ns, 'path');
    path.setAttribute('d', direction === 'up' ? 'M2.5 7.5 6 4l3.5 3.5' : 'M2.5 4.5 6 8l3.5-3.5');
    path.setAttribute('fill', 'none');
    path.setAttribute('stroke', 'currentColor');
    path.setAttribute('stroke-width', '1.5');
    path.setAttribute('stroke-linecap', 'round');
    path.setAttribute('stroke-linejoin', 'round');
    svg.append(path);
    return svg;
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
      // placed on the scale by the leaf's COMPILER type: a date or a text column has none
      const type = view.rows.columns[leaf.index]?.type;

      if (spec.range) {
        // An explicitly fixed scale is the user's decision and is
        // applied whole, at every depth.
        this.#heatmaps.set(leaf.index, {
          spec,
          byDepth: new Map([[-1, spec.range]]),
          type,
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
        const range = columnRange(bucket, type);
        if (range) byDepth.set(depth, range);
      }
      if (byDepth.size > 0) this.#heatmaps.set(leaf.index, { spec, byDepth, type });
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
    const measure = this.#pivotMeasureOf(leafName);
    return measure !== undefined
      ? columnConfig(this.#config, measure).heatmap
      : undefined;
  }

  /**
   * The measure a pivot column spreads (or totals), from the view's
   * plan -- never read out of the column's name. Undefined for any
   * other column.
   */
  #pivotMeasureOf(leafName: string): string | undefined {
    return this.#view?.pivot?.columns.find((c) => c.name === leafName)?.measure.name;
  }

  // -- the drag zones -------------------------------------------------

  /** A drag in the zones: the whole new layout, one change (P2-103). */
  #onLayout(layout: ZoneLayout): void {
    void this.#query((s) => ({ ...s, rows: [...layout.rows], pivotOn: [...layout.columns] }), 'move in the zones');
  }

  #onZoneChange(zone: Zone, columns: readonly string[]): void {
    void this.#query((s) => (zone === 'rows'
      ? { ...s, rows: [...columns] }
      : { ...s, pivotOn: [...columns] }), zone === 'rows' ? 'row groups' : 'column pivots');
  }

  /**
   * Take the editor's two lists, as one transaction: if the planner
   * refuses, the cube is put back whole and the refusal is RETURNED to the
   * editor, which keeps the form open and shows it there -- the error the
   * user sees is the planner's own rather than a paraphrase.
   */
  async #setCalc(
    row: readonly DerivedColumn[],
    group: readonly DerivedColumn[],
    rename?: { readonly from: string; readonly to: string },
  ): Promise<string | null> {
    const out = await this.#change((s) => {
      const next: CubeSnapshot = { ...s.snapshot, derived: [...row], groupDerived: [...group] };
      // A RENAME carries through: grouped, pivoted, sorted or filtered
      // by the old name, and its settings (format, width, display name)
      // follow it. Before, the rename left those naming a column that
      // no longer existed, and the planner refused it.
      return rename
        ? {
            ...s,
            snapshot: renameColumnReferences(next, rename.from, rename.to),
            configuration: renameColumnConfig(s.configuration, rename.from, rename.to),
          }
        : { ...s, snapshot: next };
    }, { label: 'calculated columns' });
    return refusal(out);
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
          column = this.#shown.rows[path.length - 1];
          value = path.length > 0 ? (path[path.length - 1] ?? null) : undefined;
        } else if (column !== undefined) {
          const leaf = this.#view.columns.leaves.find(
            (l) => l.name === column,
          );
          const raw =
            leaf === undefined
              ? null
              : (this.#view.rows.columns[leaf.index]?.values[abs] ?? null);
          // a big integer as its digits: a filter value is saved as JSON, which has no
          // bigint; `literal` spells the digits by the column's type
          value = (typeof raw === 'bigint' ? raw.toString() : raw) as FilterValue | null;
        }
        // A PIVOT'S OWN COLUMN -- a cell or a Total -- exists only after
        // the pivot, and every filter runs before it: a value filter on
        // one could only be refused (P2-134). Its row keys and its pivot
        // values are what drill-through pins instead.
        if (column !== undefined
          && (isPivotTotalColumn(column) || this.#pivotMeasureOf(column) !== undefined)) {
          value = undefined;
        }
        if (column !== undefined) {
          columnType = rowColumns(this.#shown).find(
            (c) => c.name === column,
          )?.type;
        }
        // a JSON cell or key is its JSON text; as a filter value it is a document
        // (`fromJson`), as query.ts reads a JSON group key
        if (isVariant(columnType) && typeof value === 'string') value = { json: value };
        // A group-stage calculated column exists only AFTER the
        // groupBy, and every filter runs before it -- so a value
        // filter on one could only ever be refused.
        if (column !== undefined
          && (this.#shown.groupDerived ?? []).some((d) => d.name === column)) {
          value = undefined;
        }
        // A GROUP or TOTAL row shows aggregates, and every filter runs on
        // source rows, before them. A measure's cell there is its sum
        // (or average...): `notional = <the group's sum>` ran, and kept
        // the trades whose own notional happened to equal it -- almost
        // always none. A dimension's cell is the group's one shared value
        // (a fine filter), or blank for "several", which is not "empty".
        // The group keys themselves are the tree column's, above.
        if (column !== undefined && column !== TREE_COLUMN && meta !== undefined
          && !meta.isDetail && (this.#shown.rows.length > 0 || meta.isTotal)
          && !this.#shown.rows.includes(column)
          && (this.#kindOf(column, this.#shown) === 'measure' || value === null)) {
          value = undefined;
        }
      }
      // the facts of the rows ON SCREEN, as everything this menu reads (P2-110)
      const facts = column !== undefined ? this.#columnFacts(column, this.#shown) : {};
      // FROM A COLUMN -- its header OR any of its cells (the user, 2026-09-30) -- Properties...
      // opens on that column's Column Properties, it chosen (its measure, for a pivot result or a
      // pivot total).
      const propertiesColumn = column !== undefined
        && column !== TREE_COLUMN
        ? facts.pivotBase
          ?? this.#shown.measures.find((m) => m.name === column)?.column
          ?? column
        : undefined;
      const groups = buildMenu({
        snapshot: this.#shown,
        ...(propertiesColumn !== undefined ? { propertiesColumn } : {}),
        ...(column !== undefined ? { column } : {}),
        isRowDimension: column
          ? this.#shown.rows.includes(column)
          : false,
        hasSelection: this.#selection !== null,
        hasExpanded: this.#owner.current.tree.openPaths.length > 0,
        hasHeatmap:
          column !== undefined && this.#heatmapFor(column) !== undefined,
        canGroup: column === undefined || this.#kindOf(column, this.#shown) === 'dimension',
        ...(this.#config.pivotMeasuresFirst ? { measuresFirst: true } : {}),
        // A host mailer, or upstream's way: a .eml draft to download.
        canEmail: this.#options.email !== undefined
          || this.#options.download !== undefined,
        canSaveCube: this.#options.cubeSource !== undefined,
        ...(this.#controlsHidden ? { controlsHidden: true } : {}),
        ...(column !== undefined && this.#config.columns[column]?.pinned
          ? { pinned: this.#config.columns[column]?.pinned as 'left' | 'right' }
          : {}),
        ...(column !== undefined && isPivotTotalColumn(column)
          ? { pivotTotal: true }
          : {}),
        ...(column !== undefined && this.#kindOf(column, this.#shown) !== undefined
          ? { extendable: true }
          : {}),
        ...(column !== undefined ? calcStageOf(this.#shown, column) : {}),
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
    if (applyMenuAction(this.#snapshot, item) !== this.#snapshot) {
      void this.#query((s) => applyMenuAction(s, item), item.label ?? item.id ?? 'menu');
      return;
    }
    const column = item.column;
    switch (item.id) {
      case 'tree.collapseAll':
        void this.#change((s) => ({ ...s, tree: s.tree.collapseAll() }), { label: 'collapse all' });
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
      case 'pivot.measuresFirst':
        void this.applyConfiguration({
          pivotMeasuresFirst: this.#config.pivotMeasuresFirst ? undefined : true,
        });
        return;
      case 'column.unpinAll':
        void this.#configure((c) => Object.keys(c.columns)
          .reduce((next, name) => withColumn(next, name, { pinned: undefined }), c), 'unpin all');
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
      case 'view.controls':
        this.setControlsHidden(!this.#controlsHidden);
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
          // A JSON column opens on its fields: what to extend it by is
          // what is in it.
          const json = isVariant(rowColumns(this.#snapshot)
            .find((c) => c.name === column)?.type);
          this.openColumnEditor(json
            ? { json: column, level: 'dimension' }
            : { expression: `x|${columnRef(column)}`, level: this.#kindOf(column) ?? 'measure' });
        }
        return;
      case 'calc.edit':
        if (column) this.openColumnEditor({ edit: column });
        return;
      case 'calc.delete':
        if (column) this.#deleteCalc(column);
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
        void this.openChart();
        return;
      case 'source.new':
        void this.newSource();
        return;
      case 'page.blank':
        this.#options.onBlankPage?.();
        return;
      case 'page.arrange':
        this.#page?.page.showLayouts(this.#burger?.isConnected ? this.#burger : this.#els.root);
        return;
      case 'page.editLayout':
        this.#page?.page.setEditing(!this.#page.page.editing);
        return;
      case 'page.undoLayout':
        this.#page?.page.undoLayout();
        return;
      case 'page.redoLayout':
        this.#page?.page.redoLayout();
        return;
      case 'grid.new':
        void this.newGrid();
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
    const widths: [string, number][] = [];
    for (const [name, content] of Object.entries(measured)) {
      const width = Math.min(AUTO_SIZE_MAX,
        Math.max(AUTO_SIZE_MIN, content + AUTO_SIZE_PAD));
      const next = withColumn(config, name, { width });
      if (next !== config) changed += 1;
      config = next;
      widths.push([name, width]);
    }
    if (changed === 0) {
      this.#status('nothing to resize', 'warn');
      return;
    }
    void this.#configure((c) => widths.reduce((next, [name, width]) => withColumn(next, name, { width }), c),
      'auto-size');
  }

  /**
   * Turn a heatmap on or off for a column.
   *
   * Set on the MEASURE rather than the leaf, so it reaches every
   * `2021__|__notional` the pivot produced -- a heatmap applied to
   * one pivoted leaf and not its siblings is worse than none.
   */
  #setHeatmap(leafName: string, on: boolean): void {
    const measure = this.#pivotMeasureOf(leafName) ?? leafName;
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
    void this.#query((s) => {
      const sorts = s.sorts;
      const at = sorts.findIndex((x) => x.column === column);
      const now = sorts[at];
      const next = now === undefined
        ? [...sorts, { column, direction: 'asc' as const }]
        : now.direction === 'asc'
          ? sorts.map((x, i) => (i === at ? { column, direction: 'desc' as const } : x))
          : sorts.filter((_x, i) => i !== at);
      return { ...s, sorts: next };
    }, `sort ${column}`);
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
    // MEASURE-FIRST: a spanning cell starts with its measure.
    if (this.#config.pivotMeasuresFirst && path.length > 0) {
      const [measure, ...values] = path;
      return `Column = ${named(measure as string)}${values.length > 0
        ? ` ~ [ ${values.map((v, i) => `${keys[i] ?? '?'} = ${v}`).join(', ')} ]` : ''}`;
    }
    if (path[0] === (this.#config.pivotStatisticColumnName ?? 'Total')) return '';
    return `[ ${path.map((v, i) => `${keys[i] ?? '?'} = ${v}`).join(', ')} ]`;
  }

  #patchColumn(
    column: string,
    patch: Parameters<typeof withColumn>[2],
  ): void {
    void this.#configure((c) => withColumn(c, column, patch), `column ${column}`);
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
    await this.#configure((c) => {
      let next = Object.keys(settings).length > 0 ? withSettings(c, settings) : c;
      for (const [name, cfg] of Object.entries(columns ?? {})) next = withColumn(next, name, cfg);
      return next;
    }, 'settings');
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
    const s = selectionStats(table, this.#grid.columns?.leaves ?? [], range);
    // Read as the grid reads them: in the one format the numeric cells' columns share
    // (a 4-place rate stays at 4 places), else as plain numbers. Min and max are cells,
    // so they take their column's type; the sum is exact decimal text, the average a
    // double -- each rendered by its own type.
    const formats = new Set(s.columns.map((c) => this.#formats[c]));
    const format: ColumnFormat = formats.size === 1
      ? { ...([...formats][0] ?? STATS_FORMAT), kind: 'number' } : STATS_FORMAT;
    const show = (v: Scalar, type: string | undefined): string =>
      this.#formatters.format(v, format, type);
    // Blanks are reported rather than folded into the count, because
    // an average over a pivot region that treated empty combinations
    // as zero would be wrong in the direction of looking plausible.
    slot.textContent =
      s.numeric === 0
        ? `${s.cells} cells, none numeric`
        : `sum ${show(s.sum, 'Decimal')} · avg ${show(s.average, 'Float')} · ` +
          `min ${show(s.min, s.type ?? 'Number')} · max ${show(s.max, s.type ?? 'Number')} · ` +
          `${s.numeric} of ${s.cells} numeric` +
          (s.blank > 0 ? ` · ${s.blank} blank` : '');
  }

  #selectionCsv(): string {
    const table = this.#view?.rows;
    if (!table || !this.#selection) return '';
    return toCsv(selectionTable(table, this.#grid.columns?.leaves ?? [], this.#selection));
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
  #columnFacts(column: string, snapshot: CubeSnapshot): {
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
      : (snapshot.measures.find((m) => m.name === measure)?.column
        ?? measure);
    const excluded = columnConfig(this.#config, base).excludedFromPivot === true
      || snapshot.columns.some((c) => c.name === base && c.excludedFromPivot);
    return {
      ...(measure !== undefined ? { pivotBase: base } : {}),
      ...(this.#kindOf(base, snapshot) === 'measure' ? { isMeasure: true } : {}),
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
    void this.#configure((config) => names.reduce((next, name) => {
      const c = columnConfig(next, name);
      return c.widthMode === 'fixed' ? next : withColumn(next, name, { width: c.minWidth ?? MINIMIZED_WIDTH });
    }, config), 'minimize');
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
    void this.#configure((config) => flexible.reduce((next, name) => withColumn(next, name, {
      width: Math.max(MINIMIZED_WIDTH, Math.floor((widths[name] ?? 0) * factor)),
    }), config), 'size to fit');
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
  async #drillThrough(row: number, column?: number): Promise<void> {
    const drill = (this.#drills += 1);
    const view = this.#view;
    const meta = this.#treeRows[row];
    if (!view || !meta) return;
    // A PIVOT CELL drills to its own value as well as its row: the 2021
    // notional of EMEA is EMEA's 2021 trades, not all of EMEA's. The
    // cell's values come from the plan that made the column (P2-128).
    const leaf = column === undefined ? undefined : view.columns.leaves[column];
    const planned = leaf === undefined
      ? undefined
      : view.pivot?.columns.find((c) => c.name === leaf.name);
    const pivotPath = planned?.tuple ?? undefined;
    const query = drillLambda(view.snapshot, {
      path: meta.path,
      ...(pivotPath ? { pivotPath } : {}),
    });
    // THROUGH THE CONTROLLER'S RUNNER, not a planner and an engine of
    // its own: on the plane where a remote engine executes there is
    // no local engine here to call, and a drill-through that works on
    // two planes out of three is a broken feature on the third.
    let table: ResultTable;
    try {
      ({ rows: table } = await this.#controller.runQuery(query, view.snapshot));
    } catch (error: unknown) {
      if (drill === this.#drills && !this.#disposed) this.#reportFailure(error);
      return;
    }
    // LATEST WINS: two drills in quick succession answered in either
    // order, and whichever answered last took the window (P2-131).
    if (drill !== this.#drills || this.#disposed) return;
    this.#showOverlay('Drill-through', (host) => {
      const pre = this.#doc.createElement('pre');
      pre.className = 'dc-drill';
      pre.textContent = toCsv(table);
      host.append(pre);
    }, { replace: true });
  }

  // -- export -----------------------------------------------------------

  /**
   * A chart of the cube on the board, below the grid (page/cube-page.ts): it follows the grid
   * until frozen; a click on a mark filters the cube to it.
   */
  async openChart(restore?: ChartView): Promise<void> {
    if (this.#options.onChart && !restore) {
      this.#options.onChart();
      return;
    }
    (await this.#ensurePage())?.openChart(restore);
  }

  /** Another grid on the page, starting as this one is (page/cube-page.ts `addGrid`). */
  async newGrid(): Promise<void> {
    if (this.#options.onNewGrid) {
      this.#options.onNewGrid();
      return;
    }
    (await this.#ensurePage())?.addGrid();
  }

  /**
   * New ▸ Source…: the host's picker chooses one, and a grid over it joins the page -- this
   * cube's board, or the page this grid is on.
   */
  async newSource(): Promise<void> {
    const source = await this.#options.openSource?.();
    if (source === undefined) return;
    const make = this.#gridOver(source);
    if (this.#options.onNewSource) {
      this.#options.onNewSource(make);
      return;
    }
    (await this.#ensurePage())?.addGridOver(make);
  }

  /** A cube over `source`, made by the page in a tile: a grid like any other on it. */
  #gridOver(source: GridSource): (host: HTMLElement, options: SpawnOptions) => SpawnedGrid {
    return (host, spawned) => new CubeApp(host, source.snapshot, {
      ...source.place,
      ...(source.snapTarget ? { snapTarget: source.snapTarget } : {}),
      ...(source.heldCopy ? { heldCopy: source.heldCopy } : {}),
      ...(source.cubeSource ? { cubeSource: source.cubeSource } : {}),
      sourceLabel: source.label,
      // a new source starts as its rows, up to the row limit (the user, 2026-10-01: "showing the max
      // rows configured (1000 default)"); Properties changes it
      configuration: source.configuration ?? { ...DEFAULT_CONFIGURATION, reportTitle: source.label, maxRows: DEFAULT_MAX_ROWS },
      windowHost: this.#options.windowHost ?? this.#els.root,
      compact: true,
      // its drop zones are this grid's: Column Labels too, where this one shows it
      ...(this.#options.showColumnZone !== undefined ? { showColumnZone: this.#options.showColumnZone } : {}),
      ...(this.#options.openSource ? { openSource: this.#options.openSource } : {}),
      ...(spawned.onChart ? { onChart: spawned.onChart } : {}),
      ...(spawned.onNewGrid ? { onNewGrid: spawned.onNewGrid } : {}),
      ...(spawned.onNewSource ? { onNewSource: spawned.onNewSource } : {}),
    });
  }

  /**
   * The board, made on first use: the grid moves into its first tile, charts beside or below it. The last chart gone,
   * the grid goes back where it was and the board with it. Its module -- the board, the layouts, the charts' panel -- is
   * fetched then too (`loadPage`): a grid alone never downloads it. Undefined when the cube went meanwhile.
   */
  /** The page as it is now (a method, so a read after an await is not taken for the read before it). */
  #pageNow(): CubePage | undefined {
    return this.#page?.page;
  }

  async #ensurePage(): Promise<CubePage | undefined> {
    if (this.#page) return this.#page.page;
    const { CubePage } = await loadPage();
    // made meanwhile by another call (read afresh: the await may have let one in), or the cube gone while it came
    const made = this.#pageNow();
    if (made) return made;
    if (this.#disposed) return undefined;
    const root = this.#els.root;
    const host = this.#doc.createElement('div');
    host.className = 'dc-board-host';
    // THE GRID'S TILE HOLDS THE WHOLE GRID -- its zones, its columns panel and its status bar --
    // as an added grid's does (the user, 2026-09-30: no grid looks special). The title bar stays
    // above the board: it is the page's (its name, Snapped or Live, the menu).
    const parts = [this.#els.zoneBar, this.#middle, this.#els.stats]
      .filter((el): el is HTMLElement => el !== null && el.parentElement === root);
    root.insertBefore(host, parts[0] ?? null);
    const body = this.#doc.createElement('div');
    body.className = 'dc-grid-body';
    body.append(...parts);
    const page = new CubePage({
      host,
      grid: { element: body, source: this.chartSource(), head: this.#tileHeadEl },
      onChange: () => this.#pageChanged(),
      onEmpty: () => {
        // back where they were, the board gone: the title bar carries the source and pill again
        for (const el of parts) root.insertBefore(el, host);
        page.dispose();
        host.remove();
        this.#page = null;
        this.#renderChrome();
      },
    });
    this.#page = { page, host };
    // on the board: the grid's source and pill move into its tile's header
    this.#renderChrome();
    return page;
  }

  /** What a chart of this cube needs from it (page/cube-page.ts `ChartSource`). */
  chartSource(): ChartSource {
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const app = this;
    return {
      get snapshot() { return app.#snapshot; },
      run: async (query, snapshot, signal) =>
        (await this.#controller.runQuery(query, snapshot, undefined, signal)).rows,
      format: (value, column, type) => this.#formatters.format(value, this.#formats[column], type),
      label: (column) => labelFor(this.#config, column),
      shown: () => {
        const view = this.#view;
        return view ? {
          rows: view.rows,
          tree: view.treeRows,
          leaves: view.columns.leaves.map((l) => ({ name: l.name, type: l.type, ...(l.label !== undefined ? { label: l.label } : {}) })),
          levels: view.snapshot.rows,
        } : null;
      },
      refilter: (old, add, label) => {
        void this.#query((s) => {
          const kept = withoutConditions(s.filter, old) ?? undefined;
          const children = [...(kept === undefined ? [] : kept.kind === 'and' ? kept.children : [kept]), ...add];
          const { filter: _drop, ...rest } = s;
          return children.length === 0 ? rest
            : { ...rest, filter: children.length === 1 ? children[0]! : { kind: 'and', children } };
        }, label);
      },
      spawn: (host, snapshot, spawned) => {
        const o = this.#options;
        // the same place to run as this cube: its engine and planner (and live warehouse), or its runner
        const source: CubeAppQuerySource = o.runner
          ? { runner: o.runner }
          : { engine: o.engine, planner: o.planner, ...(o.live ? { live: o.live } : {}) };
        return new CubeApp(host, snapshot, {
          ...source,
          ...(o.snapTarget ? { snapTarget: o.snapTarget } : {}),
          // what its rows ARE, as this grid knows it: the same copy in the tab, the same file, the
          // host's own name for it -- so its header says the same source, and the same plane
          ...(o.heldCopy ? { heldCopy: o.heldCopy } : {}),
          ...(o.cubeSource ? { cubeSource: o.cubeSource } : {}),
          ...(o.sourceLabel !== undefined ? { sourceLabel: o.sourceLabel } : {}),
          configuration: this.#config,
          // its windows float where this cube's do, not inside the small tile
          windowHost: o.windowHost ?? this.#els.root,
          compact: true,
          // its drop zones are this grid's: Column Labels too, where this one shows it
          ...(o.showColumnZone !== undefined ? { showColumnZone: o.showColumnZone } : {}),
          ...(o.openSource ? { openSource: o.openSource } : {}),
          ...(spawned?.onChart ? { onChart: spawned.onChart } : {}),
          ...(spawned?.onNewGrid ? { onNewGrid: spawned.onNewGrid } : {}),
          ...(spawned?.onNewSource ? { onNewSource: spawned.onNewSource } : {}),
        });
      },
    };
  }

  /** The views and their layout changed: "changed since saved" re-reads the page. */
  #pageChanged(): void {
    this.#emit('change');
  }

  /**
   * What the cube shows around itself, as a saved page keeps it: the grid, each chart (its
   * title, spec, and the mark it filters to) and the layout. A cube with no charts is a page
   * of its grid alone, the whole board.
   */
  pageViews(): PageViews {
    const page = this.#page?.page;
    if (!page || page.charts === 0) {
      return {
        views: [{ id: 'grid', kind: 'grid', cube: PAGE_CUBE }],
        layout: { kind: 'bands', fit: false, bands: [{ height: 1, node: { tile: 'grid' } }] },
      };
    }
    return page.views();
  }

  /**
   * The cube and everything around it as the ONE thing that is saved: a page (page-document.ts)
   * wrapping the cube's own document. Undefined when the host gave no source.
   */
  pageDocument(name: string, unknown?: {
    readonly cube?: Readonly<Record<string, unknown>>;
    readonly page?: Readonly<Record<string, unknown>>;
  }): PageDocument | undefined {
    const cube = this.cubeDocument(name, unknown?.cube);
    if (!cube) return undefined;
    return writePage({ name, cube, views: this.pageViews(), ...(unknown?.page ? { unknown: unknown.page } : {}) });
  }

  /**
   * The page's layout editable (tiles move, dividers drag) or locked (view mode: nothing moves by accident) -- a page
   * opened from a share link opens locked. A cube with no page of tiles has nothing to lock.
   */
  setLayoutEditing(editing: boolean): void {
    this.#page?.page.setEditing(editing);
  }

  /** Whether the page's layout can be arranged now (true for a cube with no page of tiles: nothing is locked). */
  get layoutEditing(): boolean {
    return this.#page?.page.editing ?? true;
  }

  /** Put a saved page's views back around the cube: its charts, their titles, its layout. */
  async restoreViews(page: PageViews): Promise<void> {
    if (!page.views.some((v) => v.kind === 'chart')) return;
    (await this.#ensurePage())?.restore(page);
  }



  /**
   * One rendering, shared by download and email, named as upstream
   * names an export: the title and the moment
   * (`exportFileName`), so a second export never overwrites the first.
   */
  #render(kind: 'csv' | 'excel' | 'html' | 'text' | 'pdf'): {
    name: string;
    mime: string;
    content: string | Uint8Array;
  } | null {
    const view = this.#view;
    const model = this.#grid.columns;
    if (!view || !model) return null;
    const title = this.#config.reportTitle ?? 'cube';
    const at = new Date();
    const base = exportFileName(title, at);
    // WHAT THE GRID SHOWS, once, for every format (export-model.ts): its leaves in its order
    // under its headers, the tree's depth, blurred columns REDACTED, the notes a reader needs.
    const table = exportTable({
      title,
      rows: view.rows,
      model,
      treeRows: view.treeRows,
      groupLabels: view.snapshot.rows.map((name) => labelFor(this.#config, name)),
      truncated: view.truncated.length > 0,
      ...(this.#config.maxRows !== undefined ? { maxRows: this.#config.maxRows } : {}),
      // and how the grid DRAWS it: the same appearance and heatmaps, so the file looks like it
      appearance: this.#config.appearance,
      columnAppearance: toColumnAppearance(this.#config),
      cellBackground: (leaf, row, value) => this.#heatFor(leaf, row, value),
    });
    // THE WHOLE PAGE: with charts on the board, each format carries them where the board puts
    // them, and the whole grid (the user: "it just always exports whole page by default")
    const page = this.#exportPage();
    const doc = { formatters: this.#formatters, formats: this.#formats, ...(page ? { page } : {}) };
    switch (kind) {
      case 'csv':
        // raw values: CSV is the one that is computed on again
        return { name: `${base}.csv`, mime: 'text/csv', content: exportCsv(table) };
      case 'excel':
        // a real workbook: numbers as numbers in the column's own format, the tree as outlines
        return { name: `${base}.xlsx`, mime: XLSX_MIME, content: toXlsx(table, { formats: this.#formats, at, ...(page ? { page } : {}) }) };
      case 'html':
        // Formatted: a page exists to be read, so it says what the screen says.
        return { name: `${base}.html`, mime: 'text/html', content: toHtml(table, doc) };
      case 'text':
        // Formatted, not raw: plain text exists to be READ -- pasted into a message or a ticket.
        return { name: `${base}.txt`, mime: 'text/plain', content: toPlainText(table, doc) };
      case 'pdf':
        return { name: `${base}.pdf`, mime: 'application/pdf', content: toPdf(table, doc) };
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
      download(draft, 'message/rfc822', toEml({
        subject: this.#config.reportTitle ?? 'cube',
        text: this.#emailSummary(rendered.name),
        attachment: rendered,
      }));
      this.#status(`email draft ${draft}`, 'ok');
      return;
    }
    const title = this.#config.reportTitle ?? 'cube';
    try {
      await send({
        subject: title,
        body: this.#emailSummary(rendered.name),
        attachment: rendered,
      });
      this.#status(`emailed ${rendered.name}`, 'ok');
    } catch (e) {
      this.#status(e instanceof Error ? e.message : String(e), 'error');
    }
  }

  /** The board as an export carries it: each tile where it is, each chart as its picture; none without charts. */
  #exportPage(): ExportPage | undefined {
    const page = this.#page?.page;
    return page && page.charts > 0 ? page.exportPage() : undefined;
  }

  /** A cell's heatmap colour, for the grid and for an export alike; null when it has none. */
  #heatFor(leaf: LeafColumn, row: number, value: Scalar): string | null {
    const heat = this.#heatmaps.get(leaf.index);
    if (!heat) return null;
    return heatColour(value, heat.spec, this.#heatRange(leaf.index, row) ?? null, heat.type);
  }

  /** What an email's body says about its attachment: the title, the rows, when, and any note. */
  #emailSummary(file: string): string {
    const title = this.#config.reportTitle ?? 'cube';
    const rows = this.#view?.rows.rowCount ?? 0;
    const receipt = this.#view?.receipts.at(-1);
    return [
      `${title}: ${rows.toLocaleString(UI_LOCALE)} row${rows === 1 ? '' : 's'}, exported ${new Date().toLocaleString(UI_LOCALE)}.`,
      `Attached: ${file}`,
      ...(receipt ? [`Answered by ${receipt.where}${receipt.as ? ` as ${receipt.as}` : ''}`
        + `${receipt.statementId ? ` (statement ${receipt.statementId})` : ''}.`] : []),
    ].join('\n');
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
    // the same one thing Save writes: the page, its cube inside
    const page = this.pageDocument(title);
    if (!page) {
      this.#status('this cube does not know its source, so it cannot be written down', 'warn');
      return;
    }
    download(`${exportFileName(title, new Date())}.json`, 'application/json', pageToJson(page));
  }

  // -- the saved cube -------------------------------------------------------

  /**
   * The cube as a saved-cube document (docs/DATACUBE_SAVE_SHARE_2026_09_28.md): its
   * definition, what the user set, the open rows, and its source by identity -- never its
   * data. Undefined when the host gave no source.
   */
  cubeDocument(name: string, unknown?: Readonly<Record<string, unknown>>): CubeDocument | undefined {
    const source = this.#options.cubeSource;
    if (!source) return undefined;
    // What was ACCEPTED, never a change still in flight.
    const state = this.#owner.committed;
    return writeCube({
      name,
      source,
      snapshot: state.snapshot,
      configuration: state.configuration,
      tree: state.tree,
      ...(unknown ? { unknown } : {}),
    });
  }

  /**
   * Stop listening on the document. A host that builds a new cube in the
   * same element (opening a file does) left the old one's shortcuts
   * live: Ctrl-Z undid a cube no longer on screen, and said "Could not
   * undo" over the one that was (2026-09-25 harness).
   */
  dispose(): void {
    if (this.#disposed) return;
    // FIRST, so nothing below -- and nothing that answers late -- reaches
    // the host: a cube it no longer has kept calling onView, onChange and
    // onStatus, and a late answer painted into a page that had moved on
    // (P2-105).
    this.#disposed = true;
    this.#unsubscribe();
    // The change in flight is cancelled and its query stopped.
    this.#owner.cancel();
    this.#setBusy(false);
    if (this.#onDocKey) this.#doc.removeEventListener('keydown', this.#onDocKey);
    this.#onDocKey = null;
    this.#els.root.removeEventListener('pointerdown', this.#onTouch, true);
    this.#els.root.removeEventListener('focusin', this.#onTouch, true);
    if (lastTouched.get(this.#doc) === this) lastTouched.delete(this.#doc);
    this.exitAdHoc();
    this.#menu.close();
    for (const key of [...this.#open.keys()]) this.#closeWindow(key);
    // EVERYTHING it built, torn down (plan F6): a page that removes a tile must not leave its
    // grid's and board's resize observers, its charts (their ECharts instances and queries) or
    // its column editors alive behind it.
    this.#page?.page.dispose();
    this.#page = null;
    for (const editor of this.#columnEditors.values()) editor.dispose();
    this.#columnEditors.clear();
    this.#grid.destroy();
  }

  // -- Ad Hoc Analysis mode -------------------------------------------------

  /** Whether Ad Hoc Analysis mode is on, and its session. */
  get adhoc(): AdHocMode | null {
    return this.#adhoc;
  }

  /**
   * Switch to Ad Hoc Analysis mode: the cube's named dimensions as its
   * hierarchies, its measures as the Measures dimension, the opening
   * grid queried through this cube's own runner. The cube's grid, zones
   * and sidebar are hidden, not torn down, so leaving finds them as
   * they were.
   */
  async enterAdHoc(): Promise<void> {
    if (this.#adhoc) return;
    // OPEN ON WHAT THE USER WAS LOOKING AT: its row groups down the
    // rows, its pivots across, a filter pinning a member as that member.
    // From the cube ON SCREEN, never a change still in flight: a pending
    // filter the cube then refused was carried in as a pinned member, and
    // every Ad Hoc query failed (P2-291).
    const { cube, grid } = carryOver(this.#owner.committed.snapshot, this.#dimensions());
    if (cube.outline.dimensions.length === 0) {
      this.#status('Ad Hoc Analysis needs at least one dimension column', 'warn');
      return;
    }
    const session = new AdHocSession(cube, async (snapshot, scope) => {
      const rows = await this.#controller.level(snapshot, scope);
      // Settings > Debug Mode logs Ad Hoc's queries too (P2-283)
      this.#debug('adhoc query', { snapshot, scope, rows: rows.rowCount });
      return rows;
    }, grid, { historyLimit: numericSetting(this.#settings, 'dataCube.editor.maxHistoryStackSize') });
    const middle = this.#els.grid.parentElement as HTMLElement;
    const host = this.#doc.createElement('div');
    middle.before(host);
    const mode = new AdHocMode(host, session, {
      formatters: this.#formatters,
      rowHeight: DATACUBE_ROW_HEIGHT,
      showWindow: (title, build, size) => {
        this.#showOverlay(title, build, size ? { size } : {});
      },
      startTask: (description) => this.#startTask(description),
      status: (text, kind) => this.#status(text, kind),
      reportFailure: (error) => this.#reportFailure(error),
      onExit: () => this.exitAdHoc(),
      overscan: numericSetting(this.#settings, 'dataCube.grid.rowBuffer'),
      // THE STATUS BAR IS AD HOC'S while it is on: its counts, and none of
      // the cube's links (P2-287, P2-290)
      onView: (view, stale) => this.#renderAdHocStatus(view.table.rowCount, view.columnTuples.length, stale),
      ...(this.#options.writeClipboard ? { writeClipboard: this.#options.writeClipboard } : {}),
    });
    this.#adhoc = mode;
    this.#els.root.classList.add('dc-adhoc-on');
    middle.hidden = true;
    this.#els.zoneBar.hidden = true;
    await mode.refresh();
  }

  /** Back to the cube's own grid, as it was left. */
  exitAdHoc(): void {
    const mode = this.#adhoc;
    if (!mode) return;
    this.#adhoc = null;
    const host = this.#els.root.querySelector<HTMLElement>(':scope > .dc-adhoc');
    mode.destroy();
    host?.remove();
    for (const key of [...this.#open.keys()]) {
      if (key.startsWith('Member Selection: ') || key === 'Ad Hoc Options') this.#closeWindow(key);
    }
    this.#els.root.classList.remove('dc-adhoc-on');
    (this.#els.grid.parentElement as HTMLElement).hidden = false;
    this.#applyChrome();
    // the cube's own status bar back
    if (this.#view) this.#renderStatusBar(this.#view, this.#view.columns.leaves.length);
    this.#status('Back to the cube');
  }

  /** The named dimensions: the Dimensions tab's, else the host's. */
  #dimensions(): readonly Dimension[] {
    return this.#config.dimensions ?? this.#options.dimensions ?? [];
  }

  // -- dimensions ----------------------------------------------------------

  useDimension(dimension: Dimension): void {
    if (this.#cubeOnly()) return;
    void this.#query((s) => useDimension(s, dimension), `dimension ${dimension.name}`);
  }

  // -- the dialogs ----------------------------------------------------------

  /**
   * The Properties editor; on `column`, open at Column Properties for
   * it -- upstream's Properties... from a column header. An editor
   * already open is raised and moved to that column, keeping its draft.
   */
  /**
   * An action of the cube's own, asked while Ad Hoc Analysis is on: said,
   * and not done -- it changed the hidden cube and nothing on screen
   * (P2-289). True when refused.
   */
  #cubeOnly(): boolean {
    if (!this.#adhoc) return false;
    this.#status('That acts on the cube: leave Ad Hoc Analysis first', 'warn');
    return true;
  }

  /**
   * Why the cube cannot be saved right now, or undefined. In Ad Hoc
   * Analysis a save would keep the hidden cube, not the layout on screen,
   * and say "saved" (P2-288).
   */
  saveRefusal(): string | undefined {
    return this.#adhoc
      ? 'Saving keeps the cube, not the Ad Hoc Analysis layout on screen: leave Ad Hoc Analysis to save the cube.'
      : undefined;
  }

  openEditor(column?: string): void {
    if (this.#cubeOnly()) return;
    const open = this.#editor;
    if (open && this.#open.has('Properties')) {
      this.#showOverlay('Properties', () => {});
      if (column !== undefined) open.focusColumn(column);
      return;
    }
    this.#showOverlay('Properties', (host, close) => {
      this.#editor = new CubeEditor(
        host,
        draftFor(this.#snapshot, this.#config, this.#dimensions()),
        {
          onApply: (draft, base) => this.#applyDraft(draft, base),
          onClose: close,
          ...(column !== undefined ? { initialColumn: column } : {}),
        },
      );
    }, {
      // the rail of sections, and Column Properties' columns beside its form
      size: { width: 980, height: 660, minWidth: 640, minHeight: 400 },
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
    if (this.#cubeOnly()) return;
    const editing = 'edit' in start ? start.edit : undefined;
    const key = editing !== undefined
      ? `column:${editing}`
      : `column:new:${(this.#newColumns += 1)}`;
    this.#showOverlay(editing !== undefined ? 'Edit Column' : 'Add New Column', (host, close) => {
      this.#columnEditors.set(key, new ColumnEditor(host, {
        snapshot: () => this.#snapshot,
        pivotColumns: () => this.#view?.pivot?.columns ?? [],
        start,
        parse: (text) => this.#check(this.#controller.parse(text)),
        print: (query) => this.#controller.print(query),
        compile: (candidate, signal) => this.#check(this.#controller.compile(
          { snapshot: candidate, tree: this.#owner.current.tree }, this.#view, signal)),
        apply: (row, group, rename) => this.#setCalc(row, group, rename),
        readJson: (column) => this.#jsonReader(column),
        // the rail's choice: another calculated column, in this window's place
        openOther: (next) => {
          close();
          this.openColumnEditor(next);
        },
        preview: (candidate, column, signal) => this.#previewColumn(candidate, column, signal),
        onClose: close,
      }));
    }, {
      key,
      // one page: the rail, the builder and the preview side by side
      size: { x: 50, y: 40, width: 860, height: 640, minWidth: 560, minHeight: 360, center: false },
    });
  }

  /**
   * A calculated column's PREVIEW (the column editor's): the first rows of the cube with the draft
   * in it, the new column beside what it reads -- through the cube's own query path, so on every
   * planner. Computed per source row, the source's first rows; after grouping, the first level's
   * first groups. The cells are written as the grid writes them.
   */
  async #previewColumn(candidate: CubeSnapshot, column: string, signal: AbortSignal): Promise<ColumnPreview> {
    const PREVIEW_ROWS = 10;
    const group = (candidate.groupDerived ?? []).find((d) => d.name === column);
    const scope = { level: 1, parent: [] as never[], limit: PREVIEW_ROWS } as const;
    let snapshot: CubeSnapshot;
    let shown: string[];
    if (group) {
      if (candidate.pivotOn.length > 0) throw new Error('No preview while the cube pivots its columns');
      snapshot = { ...candidate, groupDerived: (candidate.groupDerived ?? []).filter((d) => !d.childAggregate || d.name === column) };
      shown = [...candidate.rows.slice(0, 1), ...readsOf(group).filter((c) => c !== column).slice(0, 3), column];
    } else {
      const at = candidate.derived.findIndex((d) => d.name === column);
      const d = candidate.derived[at];
      if (!d) throw new Error(`no calculated column '${column}'`);
      // the source with the calculated columns up to this one: one may read another before it
      snapshot = {
        source: candidate.source,
        columns: candidate.columns,
        derived: candidate.derived.slice(0, at + 1),
        rows: [],
        pivotOn: [],
        measures: [],
        sorts: [],
        epoch: candidate.epoch,
      };
      shown = [...readsOf(d).filter((c) => c !== column).slice(0, 4), column];
    }
    const query = levelLambda(snapshot, group && candidate.rows.length === 0 ? undefined : scope);
    const { rows } = await this.#controller.runQuery(query, snapshot, group && candidate.rows.length === 0 ? undefined : scope, signal);
    const columns = shown.map((name) => rows.columns.find((c) => c.name === name)).filter((c): c is NonNullable<typeof c> => c !== undefined);
    const count = Math.min(PREVIEW_ROWS, rows.rowCount);
    return {
      columns: columns.map((c) => ({ name: c.name, type: c.type })),
      rows: Array.from({ length: count }, (_, i) => columns.map((c) =>
        this.#formatters.format(c.values[i] ?? null, this.#formats[c.name], c.type))),
    };
  }

  /**
   * A JSON column's cells, through the cube's own query path so every plane: the column
   * alone (a calculated one with the calculated columns before it), unfiltered -- the shape
   * of the data, not of the current view. A sample of its first rows, with the column's row
   * count; or every row in ONE streamed query, each chunk observed and let go (flat memory,
   * one scan, every row exactly once).
   */
  #jsonReader(column: string): JsonColumnReader {
    const s = this.#snapshot;
    const at = s.derived.findIndex((d) => d.name === column);
    const flat: CubeSnapshot = {
      source: s.source,
      columns: at >= 0 ? [] : s.columns.filter((c) => c.name === column),
      derived: at >= 0 ? s.derived.slice(0, at + 1) : [],
      rows: [],
      pivotOn: [],
      measures: [],
      sorts: [],
      epoch: s.epoch,
    };
    const cellsOf = (rows: ResultTable): readonly unknown[] =>
      rows.columns.find((c) => c.name === column)?.values ?? [];
    // the rows the column has: one count over the same relation
    const counted: CubeSnapshot = { ...flat, measures: [{ name: JSON_ROWS, column, fn: 'count' }] };
    return {
      sample: async (signal) => {
        const scope = { level: 0, parent: [], limit: JSON_SAMPLE_ROWS };
        const [sampled, total] = await Promise.all([
          this.#controller.runQuery(levelLambda(flat, scope), flat, scope, signal),
          this.#controller.runQuery(levelLambda(counted), counted, undefined, signal),
        ]);
        const n = total.rows.columns.find((c) => c.name === JSON_ROWS)?.values[0];
        return { cells: cellsOf(sampled.rows), total: Number(n ?? 0) };
      },
      all: (onChunk, signal) => this.#controller.streamQuery(
        levelLambda(flat), flat, (chunk) => onChunk(cellsOf(chunk)), signal),
    };
  }

  /** Take one calculated column out, whichever stage it is in. */
  #deleteCalc(name: string): void {
    void this.#setCalc(
      this.#snapshot.derived.filter((d) => d.name !== name),
      (this.#snapshot.groupDerived ?? []).filter((d) => d.name !== name),
    );
  }

  openFilters(): void {
    if (this.#cubeOnly()) return;
    this.#showOverlay('Filters', (host, close) => {
      this.#filters = new FilterEditor(host, {
        // Row-stage calculated columns filter like any other, and each
        // column brings its TYPE: it decides the operators offered and
        // the value editor shown.
        // a column the compiler has not typed yet is not offered: its
        // operators and editor would be a guess
        columns: rowColumns(this.#snapshot).flatMap((c) =>
          c.type === undefined ? [] : [{ name: c.name, type: c.type }]),
        // the filter the cube HAS, as every rebase after
        ...(this.#owner.committed.snapshot.filter ? { value: this.#owner.committed.snapshot.filter } : {}),
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
   * The Filter window's Apply: COMPILED FIRST, as a calculated column is --
   * the compiler judges each value (a date that is not a day, a time that
   * is not a time) and a refusal applies nothing, in the compiler's words.
   * Then run it, and on a refusal put the cube back and hand the reason to
   * the window that asked. A plane that cannot compile without running
   * falls to the run-and-restore.
   */
  async #applyFilter(filter: FilterNode | undefined): Promise<string | null> {
    const withFilter = (snapshot: CubeSnapshot): CubeSnapshot => (filter
      ? { ...snapshot, filter }
      : (({ filter: _drop, ...rest }) => rest)(snapshot));
    try {
      const checked = await this.#check(this.#controller.compile(
        { snapshot: withFilter(this.#snapshot), tree: this.#owner.current.tree }, this.#view));
      if (checked && checked.refusal !== null) {
        this.#status(checked.refusal, 'error');
        return checked.refusal;
      }
    } catch (error: unknown) {
      const message = error instanceof Error ? error.message : String(error);
      this.#status(message, 'error');
      return message;
    }
    return refusal(await this.#query(withFilter, 'filter'));
  }

  async #applyDraft(edited: CubeDraft, base: CubeDraft): Promise<boolean> {
    // ONLY WHAT THE EDITOR CHANGED. Other windows stay open beside the
    // editor now -- a filter applied, a column pinned from the menu,
    // a calculated column added -- and applying a draft taken before
    // them wholesale would silently put the cube back. So the editor's
    // edits (the difference between its draft and what it opened on)
    // land on the cube as it is NOW.
    const merge = (s: CubeState): CubeState => {
      const draft = mergeDraft(
        { snapshot: s.snapshot, config: s.configuration, dimensions: edited.dimensions },
        base,
        edited,
      );
      // "Show root aggregation" is a SETTING in their General Properties,
      // and it decides whether the level-0 query is issued at all;
      // "Initially expand to level" decides which groups open as they
      // load (upstream's isServerSideGroupOpenByDefault). Both reach the
      // TREE in the same transaction as the rest of the draft: they used
      // to be a refresh of their own before the draft's, which lost the
      // draft's rows, pivots and sorts on a refusal (P2-169).
      return {
        snapshot: draft.snapshot,
        configuration: draft.config,
        tree: s.tree
          .withTotals(draft.config.showRootAggregation)
          .withExpandTo(draft.config.initialExpandToLevel ?? 0),
      };
    };
    // COMPILED FIRST, as upstream's editor does: the whole query the
    // draft makes, planned and not run. A refusal applies nothing,
    // shows the query with the place the compiler named, and leaves the
    // editor open on the draft. A plane that cannot compile without
    // running falls to the transaction's own refusal, never to a guess.
    const endValidate = this.#startTask('Validating query...');
    const target = merge(this.#owner.current);
    let checked: Awaited<ReturnType<CubeController['compile']>>;
    try {
      checked = await this.#check(this.#controller.compile({
        snapshot: applyToSnapshot(target.snapshot, target.configuration),
        tree: target.tree,
      }, this.#view));
    } catch (error: unknown) {
      // A compile that could not run at all is said, never swallowed: the
      // Apply button's promise has nobody waiting on it (P2-152).
      this.#reportFailure(error);
      return false;
    } finally {
      endValidate();
    }
    if (checked && checked.refusal !== null) {
      const refused = checked.refusal;
      this.#status(refused, 'error');
      this.#codeCheckAlert(
        "Query Validation Failure: Can't safely apply changes. Check the query code below for more details.",
        refused, checked.query ? await this.#queryText(checked.query) : '');
      return false;
    }
    // ONE TRANSACTION: it lands whole, or is refused and everything --
    // snapshot, configuration, tree, chrome -- repaints from what was on
    // screen. The draft is merged again onto the cube as it is when it is
    // sent, not as it was before the compile.
    const out = await this.#change(merge, { label: 'Properties' });
    return out.kind === 'applied' || out.kind === 'nothing';
  }

  /**
   * A failure where the user can see it: the status line, and -- for a
   * query that was sent and failed -- upstream's execution-error alert,
   * with the query behind "Show debug info?". One at a time: a tree
   * whose every level fails is one problem, not a stack of windows.
   */
  #reportFailure(error: unknown): void {
    const message = error instanceof Error ? error.message : String(error);
    this.#status(message, 'error');
    this.#debug('failure', error);
    if (!isQueryFailure(error)) return;
    const download = this.#options.download;
    const signIn = this.#signInOffer(error);
    void this.#queryText(error.query).then((pure) => this.#showOverlay('Error', (host, close) => buildExecutionErrorAlert(host, {
      message: "Data Fetch Failure: Can't execute query.",
      text: `Error: ${message}`,
      pure,
      ...(error.sql !== undefined ? { sql: error.sql } : {}),
      ...(download ? { download } : {}),
      ...(signIn ? { signIn } : {}),
    }, close), {
      key: 'alert:execution',
      replace: true,
      size: signIn ? { ...EXECUTION_ERROR_WINDOW, height: EXECUTION_ERROR_WINDOW.height + 60 } : EXECUTION_ERROR_WINDOW,
    }));
  }

  /**
   * An expired warehouse sign-in, answered where it is reported: sign in again as the same
   * user at the same warehouse, then do again what failed. Snapped, only going live can have
   * reached the warehouse, so that is retried; live, the refused change is applied again (a
   * sort that failed is sorted, not dropped). Null for any other
   * failure, or a cube whose live plane cannot sign in again.
   */
  #signInOffer(error: unknown): { who: string; where: string; submit: (password: string) => Promise<void> } | null {
    const expired = sessionExpired(error);
    const live = this.#options.runner === undefined ? this.#options.live : undefined;
    if (!expired || !(live instanceof WarehouseEngine)) return null;
    return {
      who: expired.principal,
      where: `the warehouse at ${hostOf(expired.baseUrl)}`,
      submit: async (password) => {
        await live.signInAgain(password);
        this.#closeWindow('Sign in');
        this.#status(`signed in again as ${expired.principal}`, 'ok');
        // what failed runs again: the view (going live, or the refused change) and every chart
        if (this.#controller.snaps.isSnapped) this.#planeToggle?.();
        else void this.#owner.retryRefused();
        this.#page?.page.refresh();
      },
    };
  }

  /**
   * A query the cube's own view did not run (a chart, a drill-through, an export) found the
   * warehouse sign-in expired: offer to sign in again, in a window of its own -- those report
   * their failures where they are drawn, which had no way to offer it. One window, however many
   * queries failed.
   */
  #offerSignIn(error: unknown): void {
    const signIn = this.#signInOffer(error);
    if (!signIn) return;
    this.#showOverlay('Sign in', (host, close) => {
      buildAlert(host, {
        type: 'warning',
        message: `The sign-in to ${signIn.where} has expired.`,
        text: 'Sign in again and what failed runs again.',
      }, close);
      host.append(signInAgain(this.#doc, signIn, close));
    }, { size: { ...ALERT_WINDOW, height: ALERT_WINDOW.height + 40 } });
  }

  /**
   * A query as a person reads it: as the compiler prints it (E4). A print
   * that fails says so in its place.
   */
  async #queryText(query: Lambda): Promise<string> {
    try {
      return await this.#controller.print(query);
    } catch (error: unknown) {
      return `(the query could not be printed: ${error instanceof Error ? error.message : String(error)})`;
    }
  }

  /** Upstream's documentation panel, on the entry a (?) named. */
  openDocumentation(key: DocKey): void {
    this.#showOverlay('Documentation', (host) => buildDocumentation(host, key), {
      replace: true,
      size: { width: 400, height: 300, minWidth: 200, minHeight: 150, center: true },
    });
  }

  /** The Settings window (upstream's, from the title bar's menu). */
  openSettings(): void {
    this.#showOverlay('Settings', (host, close) => buildSettingsPanel(host, {
      values: this.#settings,
      onSave: (values) => this.#applySettings(values),
      onAction: (key) => this.#settingAction(key),
      onClose: close,
    }), { size: SETTINGS_WINDOW });
  }

  /** The settings, in effect: each one reaches what it controls. */
  #applySettings(values: SettingValues): void {
    this.#settings = values;
    const limit = numericSetting(values, 'dataCube.editor.maxHistoryStackSize');
    const buffer = numericSetting(values, 'dataCube.grid.rowBuffer');
    this.#owner.setHistoryLimit(limit);
    this.#grid.setOverscan(buffer);
    // and Ad Hoc's, which kept 100 steps and the default buffer (P2-283)
    this.#adhoc?.session.setHistoryLimit(limit);
    this.#adhoc?.setOverscan(buffer);
    this.#options.onSettingsChanged?.(values);
  }

  #settingAction(key: SettingKey): void {
    // Reload what is ON SCREEN: Ad Hoc's grid while it is on
    if (key === 'dataCube.debugger.action.reload') void (this.#adhoc ? this.#adhoc.refresh() : this.#owner.refresh());
  }

  /** Settings > Debug Mode: what ran, what it made, what failed. */
  #debug(event: string, data: unknown): void {
    if (!booleanSetting(this.#settings, 'dataCube.debugger.enableDebugMode')) return;
    console.debug(`[DataCube] ${event}`, data);
  }

  /** Upstream's code-check alert: the refused query, the place marked. */
  #codeCheckAlert(message: string, refusal: string, code: string): void {
    this.#alerts += 1;
    this.#showOverlay('Error', (host, close) => buildCodeCheckAlert(host, {
      message, text: `Error: ${refusal}`, code,
    }, close), { key: `alert:${this.#alerts}`, size: CODE_CHECK_WINDOW });
  }

  /** A schema change the compiler reports on a refresh, said where the user reads it. */
  #reportSchemaChanges(view: CubeView): void {
    const changes = view.schemaChanges ?? [];
    if (changes.length === 0) return;
    const said = changes.map((c) => (c.now === null
      ? `'${c.column}' (${c.was}) is no longer in the source`
      : `'${c.column}' is now ${c.now}, was ${c.was}`));
    this.#status(`The source changed: ${said.join('; ')}.`, 'warn');
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
      // still this cube's, wherever it floats: its drags land here, its keys are this cube's
      win.dataset['dcCube'] = this.#els.root.dataset['dcCube'] ?? '';
      const host = this.#options.windowHost ?? this.#els.root;
      // outside the cube it does not inherit the cube's type and colours, so it carries them
      if (host !== this.#els.root) win.classList.add('dc-app-floating');
      host.append(win);
      this.#open.set(key, win);
      // Whichever window is touched comes to the front.
      win.addEventListener('pointerdown', () => this.#raise(win));
      // A (?) in this window asks for its documentation. On the WINDOW,
      // which this app owns, not on the host's element: a host that
      // rebuilds the cube in the same element (opening a file does)
      // kept the old app listening there, and both opened a window.
      win.addEventListener('dc-doc', (event) => {
        const key = (event as CustomEvent<unknown>).detail;
        if (isDocKey(key)) this.openDocumentation(key);
      });
      // Escape closes the window it is pressed in, because a window a
      // keyboard user cannot dismiss is a trap; the panels inside stop
      // their own Escape from reaching here.
      // Escape closes the window -- except in a text field, where it
      // belongs to the field: it closed the whole window and dropped the
      // draft being typed (P2-221).
      win.addEventListener('keydown', (event) => {
        if (event.key === 'Escape' && !isTextEntry(event.target)) this.#closeWindow(key);
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
      makeWindow(win, head, this.#options.windowHost ?? this.#els.root, {
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
    // one stack per place windows float: several cubes sharing a `windowHost` share it too,
    // so the window touched last is on top whichever cube opened it
    const host = this.#options.windowHost ?? this.#els.root;
    const top = (windowStack.get(host) ?? WINDOW_Z_BASE) + 1;
    windowStack.set(host, top);
    win.style.zIndex = String(top);
  }

  /** Close one window, by its title. */
  #closeWindow(key: string): void {
    if (key === 'Filters') this.#filters = null;
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
    //
    // THE FOLDS LIVE ON THE RIGHT, one column per bar, so folding and
    // unfolding is a flick of the pointer rather than a trip across
    // the screen (user, 2026-09-25): the zones' fold is the zone
    // bar's last control, and their way back appears in the title
    // bar directly above it; the lip keeps its chevron where the
    // title bar's fold was, the whole strip still clickable.
    if (!this.#config.showTitleBar) {
      const open = doc.createElement('button');
      open.type = 'button';
      open.className = 'dc-titlebar-lip';
      open.append(this.#chevron('down'));
      open.title = 'Show the title bar';
      open.setAttribute('aria-label', 'Show the title bar');
      open.setAttribute('aria-expanded', 'false');
      open.addEventListener('click', () => {
        this.#setChrome({ showTitleBar: true });
      });
      bar.append(open);
      if (!this.#config.showDragZones && !this.#inTile) bar.append(this.#zonesBack());
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

    // ONE GRID ALONE: the bar names its source and carries its Live/Snapped pill. On a board
    // each grid carries its own, in its tile's header (`tileHead`, the user 2026-09-30).
    if (!this.#inTile) {
      const source = this.#sourceName();
      if (source !== undefined) bar.append(this.#sourceTag(source));
      bar.append(this.#snapPill());
    }

    // AND THE BAR FOLDS ITSELF: after the menu, in the fold column
    // (appended below, once the hamburger is in).
    const fold = doc.createElement('button');
    fold.type = 'button';
    fold.className = 'dc-titlebar-fold';
    fold.append(this.#chevron('up'));
    fold.title = 'Hide the title bar';
    fold.setAttribute('aria-label', 'Hide the title bar');
    fold.setAttribute('aria-expanded', 'true');
    fold.addEventListener('click', () => {
      this.#setChrome({ showTitleBar: false });
    });

    // THE MENU, under the hamburger: like the grid's right-click menu -- a short list, the rest a
    // level down (the user, 2026-09-30). Page-wide things live here; what belongs to a column or
    // a cell stays in the right-click menu, in its order.
    const burger = doc.createElement('button');
    this.#burger = burger;
    burger.type = 'button';
    burger.className = 'dc-titlebar-menu';
    burger.setAttribute('aria-label', 'Menu');
    burger.setAttribute('aria-haspopup', 'menu');
    burger.textContent = '\u2261';
    burger.addEventListener('click', () => {
      // A second press on the hamburger SHUTS it: the outside-press dismissal would close the
      // menu and the click that follows reopen it, so the button would appear to do nothing.
      if (this.#menu.open) {
        this.#menu.close();
        return;
      }
      // BELOW THE BUTTON, not at the pointer: at the pointer the menu covered the button it came
      // from, so a second press picked the first entry instead of shutting the menu.
      const at = burger.getBoundingClientRect();
      this.#menu.show(this.#mainMenu(), at.left, at.bottom, burger);
    });
    // THE MENU ON THE LEFT, the folds alone on the right (user, 2026-09-25)
    bar.prepend(burger);
    bar.append(fold);
    // THE ZONES' WAY BACK, at the far right: directly above where their own fold was. Only while
    // they are folded -- a control that is always there but does nothing half the time is worse
    // than one that appears when it has something to do. On a board it is in the tile's header.
    if (!this.#config.showDragZones && !this.#inTile) bar.append(this.#zonesBack());
  }

  /**
   * The Live/Snapped pill: where this grid's rows come from, and -- where both planes exist -- the
   * toggle between them. In the title bar of a grid alone, or in the grid's tile header on a
   * board: one per grid either way.
   */
  #snapPill(): HTMLElement {
    const doc = this.#doc;
    // Snap is legend-lite's own idea rather than DataCube's, but it
    // is a MODE, and a mode belongs in the bar rather than two
    // levels down a menu.
    const host = doc.createElement('div');
    host.className = 'dc-titlebar-host';
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
    //
    // It is a TOGGLE only where both planes exist: a live source elsewhere
    // (a warehouse, a remote file) and a store in this tab to snap into.
    // Rows already held in the tab are a snap from the start; a plane with
    // no local store (the engine) is live with no snap on offer. Either
    // way it still states the plane, and a click does nothing -- a Live
    // that only answers with a refusal, or a Snap of a copy, was a lie.
    const held = this.#options.heldCopy;
    const canSnap = this.#options.runner === undefined && this.#options.snapTarget !== undefined;
    if (held !== undefined || !canSnap) {
      snap.textContent = held !== undefined ? 'Snapped' : 'Live';
      snap.classList.toggle('dc-on', held !== undefined);
      snap.classList.add('dc-fixed');
      snap.setAttribute('aria-disabled', 'true');
      snap.title = held !== undefined
        ? `${held.label} — copied into this tab at ${held.takenAt.toLocaleTimeString(UI_LOCALE)}, ` +
          `${held.rowCount.toLocaleString(UI_LOCALE)} rows. Nothing can change it while you work.`
        : describePlane({ mode: 'live' }, { remote: false, toggles: false }).title;
      host.append(snap);
      this.#paintSnap = null;
    } else {
      const paint = (): void => {
        const state = this.#controller.snaps.state;
        snap.classList.toggle('dc-on', state.mode === 'snapped');
        // Where the data is, when Live is a warehouse: the plane is a place (the wording every app shares)
        const remote = this.#options.runner === undefined && this.#options.live !== undefined;
        const described = describePlane(state, { remote, toggles: true });
        snap.textContent = described.text;
        snap.title = described.title;
      };
      this.#paintSnap = paint;
      this.#planeToggle = () => snap.click();
      snap.addEventListener('click', () => {
        snap.disabled = true;
        const done = (): void => {
          snap.disabled = false;
          paint();
          this.#emit('plane');
        };
        // The PLANE changes, the cube's state does not: freeze (or release)
        // what is on screen, then the owner re-runs it -- not an undo step.
        // The new plane holds only once it has ANSWERED: a refused re-run puts
        // the old plane back, so the badge never names a plane the rows are
        // not from (a dead warehouse left a dropped snap's rows under "Live").
        const rerun = () => this.#owner.refresh();
        // the copy into the tab comes before the re-run, which is when the owner turns busy: counted from the click
        const work = this.#check(this.#controller.snaps.isSnapped
          ? this.#controller.goLive(rerun)
          : this.#controller.snapAndRun(this.#owner.committed.snapshot, rerun));
        work.then(done, (e: unknown) => {
          this.#status(e instanceof Error ? e.message : String(e), 'error');
          done();
        });
      });
      paint();
      host.append(snap);
    }
    return host;
  }

  /** What this grid reads, as a person names it: a file, a table, a copy in this tab. */
  #sourceName(): string | undefined {
    const o = this.#options;
    if (o.sourceLabel !== undefined) return o.sourceLabel;
    if (o.heldCopy) return o.heldCopy.label;
    if (o.cubeSource) return o.cubeSource.name;
    const path = (this.#snapshot.source.query as { value?: { path?: readonly string[] } }).value?.path;
    const table = path && path.length > 1 ? path.slice(1).join('.') : undefined;
    if (table === undefined) return undefined;
    // where the rows are, when not in this tab: run by an engine, or live on a warehouse
    if (o.runner !== undefined) return `${table} (on the engine)`;
    return o.live !== undefined ? `${table} (warehouse)` : table;
  }

  #sourceTag(text: string): HTMLElement {
    const el = this.#doc.createElement('span');
    el.className = 'dc-source-tag';
    el.textContent = text;
    el.title = `This grid reads ${text}`;
    return el;
  }

  /** In a tile on a board (this cube's own, or a page's), rather than alone on the page. */
  get #inTile(): boolean {
    return this.#page !== null || this.#options.compact === true;
  }

  /**
   * The grid's own part of its TILE'S HEADER, on a board: its source, its Live/Snapped pill, and
   * -- while its zones are folded -- their way back. The page puts it in the tile's header; it is
   * repainted with the rest of the chrome.
   */
  tileHead(): HTMLElement {
    return this.#tileHeadEl;
  }

  #paintTileHead(): void {
    const head = this.#tileHeadEl;
    head.replaceChildren();
    if (!this.#inTile) return;
    const source = this.#sourceName();
    if (source !== undefined) head.append(this.#sourceTag(source));
    head.append(this.#snapPill());
    if (!this.#config.showDragZones) head.append(this.#zonesBack());
    head.append(this.#gridMenuButton());
  }

  /** The grid's own menu button, in its tile's header (`#gridMenu`); a second press shuts the menu, as the bar's does. */
  #gridMenuButton(): HTMLElement {
    const button = this.#doc.createElement('button');
    button.type = 'button';
    button.className = 'dc-tile-button dc-tile-menu';
    button.textContent = '\u2261';
    button.title = 'This grid\'s menu';
    button.setAttribute('aria-label', 'Grid menu');
    button.setAttribute('aria-haspopup', 'menu');
    button.addEventListener('click', () => {
      if (this.#menu.open) {
        this.#menu.close();
        return;
      }
      const at = button.getBoundingClientRect();
      this.#menu.show(this.#gridMenu(), at.left, at.bottom, button);
    });
    return button;
  }

  /**
   * The hamburger's menu, built as it opens (Undo knows whether there is anything to undo):
   * Data...; Undo and Redo; the cube as a file (Save, Save As, Open, Share, Export, Email); View
   * and Insert a level down; Settings. Where the planner runs is the status bar's readout. A host's entries go where their `section` says, File unless
   * named; an empty group or submenu is not shown.
   */
  /** A host's entries for one place (`MenuItem.section`, the file group unless named). */
  #hostItems(section: 'file' | 'view' | 'data' | 'plane'): MenuItem[] {
    return (this.#options.hostMenu?.() ?? []).filter((i) => (i.section ?? 'file') === section);
  }

  #mainMenu(): MenuGroup[] {
    const host = (section: 'file' | 'view' | 'data'): MenuItem[] => this.#hostItems(section);
    // The cube's own entries are not offered while Ad Hoc is on: they changed the hidden cube
    // and nothing visible (P2-289).
    const cubeOnly = this.#adhoc ? { disabled: true } : {};
    const submenu = (label: string, items: MenuItem[]): MenuItem[] =>
      (items.length > 0 ? [{ label, submenu: items }] : []);
    const groups: MenuGroup[] = [
      // NEW, first (the user, 2026-10-01): a data source -- through the host's picker -- a
      // visualization or a copy of this grid, all beside what is there; or a blank page, apart,
      // which replaces everything
      { label: '', items: submenu('New', [
        ...(this.#options.openSource ? [{ id: 'source.new' as const, label: 'Data Source\u2026', ...cubeOnly }] : []),
        { id: 'chart.plot', label: 'Visualization', ...cubeOnly },
        { id: 'grid.new', label: 'Copy of Grid', ...cubeOnly },
        ...(this.#options.onBlankPage ? [{ id: 'page.blank' as const, label: 'Blank Page', separated: true }] : []),
      ]) },
      // THE PAGE'S LAYOUT, while there is a page of tiles: its layouts, and locking it so nothing moves by accident
      ...(this.#page ? [{ label: '', items: [
        { id: 'page.arrange' as const, label: 'Arrange\u2026' },
        { id: 'page.undoLayout' as const, label: 'Undo Layout', ...(this.#page.page.canUndoLayout ? {} : { disabled: true }) },
        { id: 'page.redoLayout' as const, label: 'Redo Layout', ...(this.#page.page.canRedoLayout ? {} : { disabled: true }) },
        { id: 'page.editLayout' as const, label: 'Edit Layout', checked: this.#page.page.editing },
      ] }] : []),
      { label: '', items: host('data') },
      { label: '', items: this.#undoItems() },
      { label: '', items: [...host('file'), ...this.#outputItems()] },
      { label: '', items: [...submenu('View', [...this.#viewItems(), ...host('view')])] },
      { label: '', items: [{ id: 'view.settings', label: 'Settings...' }] },
    ];
    return groups.filter((g) => g.items.length > 0);
  }

  /**
   * A GRID'S OWN MENU, under the menu button in its tile's header on a page (the design's §3.1: each grid's header
   * carries only that grid's things, the same for the first grid and the fifth): a chart or a copy of it, its Undo,
   * its Export and Email, its Properties, Ad Hoc and Dimensions.
   */
  #gridMenu(): MenuGroup[] {
    const cubeOnly = this.#adhoc ? { disabled: true } : {};
    return [
      { label: '', items: [{ label: 'New', submenu: [
        { id: 'chart.plot', label: 'Visualization', ...cubeOnly },
        { id: 'grid.new', label: 'Copy of Grid', ...cubeOnly },
      ] }] },
      { label: '', items: this.#undoItems() },
      { label: '', items: this.#outputItems() },
      { label: '', items: this.#viewItems() },
    ];
  }

  /** Undo and Redo, of what this grid shows. */
  #undoItems(): MenuItem[] {
    return [
      // WHAT UNDO ACTS ON: Ad Hoc's session while it is on, never the hidden cube's history
      // (P2-284). Disabled rather than hidden when there is nothing to go back to: a shortcut
      // that silently does nothing cannot be told from one that is broken.
      { id: 'view.undo', label: 'Undo',
        ...((this.#adhoc ? this.#adhoc.session.canUndo : this.#owner.canUndo) ? {} : { disabled: true }) },
      { id: 'view.redo', label: 'Redo',
        ...((this.#adhoc ? this.#adhoc.session.canRedo : this.#owner.canRedo) ? {} : { disabled: true }) },
    ];
  }

  /** Export and Email, as the right-click menu has them. */
  #outputItems(): MenuItem[] {
    const canEmail = this.#options.email !== undefined || this.#options.download !== undefined;
    return [
      { label: 'Export', submenu: exportItems(this.#options.cubeSource !== undefined) },
      { label: 'Email', submenu: emailItems(canEmail) },
    ];
  }

  /** Properties, Ad Hoc Analysis, and the named hierarchies (Dimensions) when there are any. */
  #viewItems(): MenuItem[] {
    // The cube's own entries are not offered while Ad Hoc is on: they changed the hidden cube
    // and nothing visible (P2-289).
    const cubeOnly = this.#adhoc ? { disabled: true } : {};
    const dimensions = availableDimensions(this.#snapshot, this.#dimensions())
      .map((d): MenuItem => ({ id: 'view.dimension', label: d.name, column: d.name, ...cubeOnly }));
    return [
      { id: 'view.properties', label: 'Properties...', ...cubeOnly },
      // the other way to work the cube: checked while it is on; choosing it again leaves
      { id: 'view.adhoc', label: 'Ad Hoc Analysis', ...(this.#adhoc ? { checked: true } : {}) },
      ...(dimensions.length > 0 ? [{ label: 'Dimensions', submenu: dimensions }] : []),
    ];
  }

  /**
   * The cube's shortcuts, on the DOCUMENT, registered ONCE. They were added
   * by every title bar rebuild and never removed, so after a few rebuilds one
   * Ctrl-Z undid several steps (P2-220); `dispose` takes this one away.
   */
  #listenForKeys(): void {
    if (!lastTouched.has(this.#doc)) lastTouched.set(this.#doc, this);
    this.#els.root.addEventListener('pointerdown', this.#onTouch, true);
    this.#els.root.addEventListener('focusin', this.#onTouch, true);
    this.#onDocKey = (event: KeyboardEvent): void => {
      if (!(event.ctrlKey || event.metaKey)) return;
      if (!this.#ownsKey(event.target)) return;
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
    };
    this.#doc.addEventListener('keydown', this.#onDocKey);
  }

  /**
   * Listen to what the cube tells (`CubeEvents`); returns the way to stop. Any number of
   * listeners, beside the options' callbacks; none are called once the cube is disposed.
   */
  on<K extends keyof CubeEvents>(event: K, fn: (...args: CubeEvents[K]) => void): () => void {
    let set = this.#listeners.get(event);
    if (!set) this.#listeners.set(event, set = new Set());
    set.add(fn);
    return () => { set.delete(fn); };
  }

  /** Tell the options' callback, then every listener; nothing once disposed. */
  #emit<K extends keyof CubeEvents>(event: K, ...args: CubeEvents[K]): void {
    if (this.#disposed) return;
    const o = this.#options;
    switch (event) {
      case 'view': o.onView?.(...(args as CubeEvents['view'])); break;
      case 'change': o.onChange?.(); break;
      case 'status': o.onStatus?.(...(args as CubeEvents['status'])); break;
      case 'plane': o.onPlane?.(); break;
    }
    for (const fn of [...(this.#listeners.get(event) ?? [])]) (fn as (...a: CubeEvents[K]) => void)(...args);
  }

  /**
   * Whether a keystroke from `target` is this cube's: from inside it (its root, or one of its
   * windows), or from outside every cube while this is the one last touched. A keystroke inside
   * ANOTHER cube is never this one's.
   */
  #ownsKey(target: EventTarget | null): boolean {
    const node = target && typeof (target as Node).nodeType === 'number' ? target as Node : null;
    if (!node) return lastTouched.get(this.#doc) === this;
    // inside ANOTHER cube -- one on this cube's own board (a chart's editing grid) included
    const scope = cubeScopeOf(node);
    if (scope !== null && scope !== this.#els.root.dataset['dcCube']) return false;
    if (this.#els.root.contains(node)) return true;
    for (const win of this.#open.values()) if (win.contains(node)) return true;
    const el = node.nodeType === 1 ? node as Element : node.parentElement;
    if (el?.closest('.dc-app')) return false;
    return lastTouched.get(this.#doc) === this;
  }

  /**
   * Undo, with an answer either way.
   *
   * Outcomes a person can tell apart: it worked, there was nothing to
   * undo, the change still running was cancelled, or it could not be
   * applied. The last one matters most -- the owner leaves the cube
   * exactly as it was and keeps the step, so the honest message is that
   * nothing moved and it can be tried again, not a stack trace. Never
   * throws: an engine outage is a refusal, said, not an unhandled promise.
   */
  async #undo(): Promise<void> {
    if (this.#adhoc) {
      await this.#adhoc.undo();
      return;
    }
    this.#sayHistory(await this.#owner.undo(), 'undo');
  }

  async #redo(): Promise<void> {
    if (this.#adhoc) {
      await this.#adhoc.redo();
      return;
    }
    this.#sayHistory(await this.#owner.redo(), 'redo');
  }

  #sayHistory(out: Outcome, what: 'undo' | 'redo'): void {
    if (out.kind === 'nothing') this.#status(`Nothing to ${what}`, 'warn');
    else if (out.kind === 'cancelled') this.#status('The change still running was cancelled', 'ok');
    else if (out.kind === 'refused') this.#status(`Could not ${what} — the cube is unchanged`, 'error');
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
      case 'view.settings':
        this.openSettings();
        return true;
      case 'view.adhoc':
        if (this.#adhoc) this.exitAdHoc();
        else void this.enterAdHoc();
        return true;
      case 'view.dimension': {
        const found = this.#dimensions().find((d) => d.name === item.column);
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

/** The selection statistics' numbers where their columns share no format. */
const STATS_FORMAT: ColumnFormat = { kind: 'number', maximumFractionDigits: 2 };

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
  // The Dimensions tab's edit is KEPT, on the configuration: it went
  // nowhere before, so a hierarchy defined there was gone on Apply.
  const merged = merge(current.config, base.config, edited.config) as CubeConfiguration;
  const config: CubeConfiguration = same(base.dimensions, edited.dimensions)
    ? merged
    : { ...merged, dimensions: edited.dimensions };
  return {
    snapshot: applyToSnapshot(snapshot, config),
    config,
    dimensions: edited.dimensions,
  };
}



/** The columns a calculated column reads: what its preview shows beside it. */
function readsOf(d: DerivedColumn): string[] {
  const out: string[] = [];
  const add = (c: string | undefined): void => { if (c !== undefined && c !== '' && !out.includes(c)) out.push(c); };
  if (d.lambda) {
    for (const p of findAll(d.lambda as ValueSpecification, (n): n is AppliedProperty => n._type === 'property')) {
      if (p.parameters[0]?._type === 'var') add(p.property);
    }
  }
  if (d.window) {
    add(d.window.column);
    d.window.partition.forEach(add);
    d.window.order.forEach((o) => add(o.column));
  }
  if (d.childAggregate) add(d.childAggregate.of);
  return out;
}
