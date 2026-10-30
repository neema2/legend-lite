// A PAGE OF TILES: grids and the charts made from them, on one board, laid out as bands (layout/band-board.ts;
// docs/DATACUBE_PAGES_DESIGN_2026_10_09.md). Every tile is placed, moved and arranged alike: a new chart or grid goes
// beside the tile it came from while its band has room (smart placement), else below it, and the layout picker
// arranges the whole page.
//
// SHEETS (the design's §7.2): a page is one or more sheets, each a whole screen of tiles with its own board -- its
// bands, its fit, its layout undo -- one shown at a time. Every tile is on one sheet; a chart follows its grid on any
// sheet, so a grid on a sheet not shown keeps running for the charts that read it. A cube alone has one sheet.
//
// Two owners, one board. A CUBE ALONE (a CubeApp embedded in a host: the Query app, a notebook) gets a board when its
// first chart opens, its own grid the first tile and not removable (`grid`). A PAGE OF ITS OWN (page/page-app.ts,
// DataCube's app) has no such grid: every grid is added alike (`addGridOver`), the first one removable as the fifth.
//
// THE BOARD IS PRODUCED BY THE COMMANDS (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md S3). Both owners keep the page's
// state (page-state.ts); every gesture here tells its page commands (onPageEdit), the owner applies them by the one step
// (page-commands.ts applyToState) and puts the board at the page they make (`sync`). The board keeps the gestures, the
// previews and the drawing -- what a gesture makes (a grid's engine, a chart's panel) waits until its command places
// it, and what no page holds (a chart's editing grid, the tile a stack shows in front) is its own -- and changes nothing
// of the page itself (test/state-guardrail.test.ts).
//
// Moved out of CubeApp so a grid and a chart are the same kind of thing -- a tile on a board --
// and a page can come to hold several grids (the user, 2026-09-30: "the grid is the source").
// A chart reaches its grid only through `ChartSource`: the grid's query as it is now, a way to
// run a query of its own, the grid's formats, and the grid's filter for click-to-filter.
//
// A chart FOLLOWS ITS GRID until frozen -- it re-draws as the grid is pivoted, grouped and
// filtered, badged so -- and any number may follow at once. A frozen chart keeps its grouping
// (its query and spec, not its data: it still refreshes and answers the grid's filter); Follow the
// grid makes it follow again. Show Grid shows a chart's grid: the one it follows, or -- frozen or
// detached -- a grid of its own grouping and query, made in one step, which it follows from then on (D7).

import { ChartPanel } from '../ui/chart-panel.ts';
import type { RunOutcome } from '../runner.ts';
import { MenuView } from '../ui/menu-view.ts';
import { createItems, type MenuItem } from '../ui/menu.ts';
import { chromeIcon, closeMark, paintState, stateButton } from '../ui/tile-icons.ts';
import { BandBoard, type BandTile, type LayoutMove } from '../layout/band-board.ts';
import { type Bands, NEW_SHEET, add as addTile, bringToFront, cells, leaves, places, remove as removeTile, tiles } from '../layout/bands.ts';
import { FlowBoard, runStoppers, type FlowPoint, type LineEnd, type RunLog } from '../layout/flow-board.ts';
import { liveGrids, relinked } from '../live-inputs.ts';
import type { Profile, TopValue } from '../profile.ts';
import { CONTAINER_FRAME, EMPTY_FLOW, flowTiles, messageProblem, messageSaid, outputWords, runStages, type FlowBox, type Frame, type Output } from '../layout/flow.ts';
import type { FlowTool, StepEditor } from '../flow-boxes.ts';
import { MAX_RULES } from '../dynamic-replace.ts';
import { MACRO_BOXES, macroOf, macroProblem, macroWords, type PageMacro } from '../macros.ts';
import type { PageParameter } from '../page-parameters.ts';
import type { InputPick } from '../dynamic-input.ts';
import type { GestureArgs } from '../grid-gestures.ts';
import type { LayoutPicker } from '../ui/layout-picker.ts';
import { CHART_STATES, chartGrid, MAP_MARKS, showGridWay, type ChartSpec, type ChartState } from '../chart-spec.ts';
import { chartPicked, type PageFilter } from '../page-filters.ts';
import type { CubeConfiguration } from '../config.ts';
import type { GridShown } from '../chart-option.ts';
import type { ExportPage, ExportTile } from '../export-model.ts';
import {
  FIRST_SHEET, PAGE_CUBE, type ChartView, type GridView, type PageLayout, type PageSheet, type PageView, type PageViews, type TextView,
  type TimeseriesView as TimeseriesPageView,
} from '../page-document.ts';
import {
  duplicateCommands, layoutCommand, movedCommand, moveSheetCommand, moveToSheetCommands, pageCommand, renameSheetCommand, savedLayout, settingsCommands,
  showGridCommands,
  takeOffCommands,
} from '../page-gestures.ts';
import { chartTitle, gridTitle, nextPageId, textTitle, timeseriesTitle } from '../page-ids.ts';
import type { PageCommand } from '../page-commands.ts';
import type { CubeDocument, CubeSource, InputPort } from '../cube-document.ts';
import type { GridCopy, PageDocument } from '../page-document.ts';
import { CubeRefusal, sameValue, withoutConditions, type CubeSnapshot, type FilterNode, type FilterValue } from '../snapshot.ts';
import type { SettingValues } from '../settings.ts';
import type { ResultTable, Scalar } from '../../../engine-client/src/result.ts';
import type { Lambda } from '../../../pure-protocol/src/index.ts';
import { lazily } from '../lazily.ts';
import { TS_STATES, tsStateOf, type TimeseriesSpec, type TsState } from '../timeseries-spec.ts';
import type { TimeseriesView, TsGrid, TsTogether } from '../ui/timeseries-view.ts';
import { sha256 } from '../cube-document.ts';
import { isFileHandle, pickTextFiles, type FileHandle, type HandleRead } from '../file-handles.ts';
import { TEXT_FILE_EXTENSIONS, TEXT_FILE_SHOWN, bytesWords, csvSeparator, pickedRefusal, textFileFormat, type TextFile } from '../text-files.ts';

/** How many arrangements Undo Layout goes back through. */
const LAYOUT_STEPS = 50;
/** The grid's tile, until renamed: the cube's name is already above the board. */
export const GRID_TILE_TITLE = 'Grid';
const GRID = 'grid';

/** A chart's states as its pill says them: its word, and what it does, in a line (chart-spec.ts chartState). */
const STATE_WORDS: Readonly<Record<ChartState, { readonly label: string; readonly says: string }>> = {
  dynamic: { label: 'Dynamic', says: 'it follows the grid: its grouping, its filters and the page\'s clicks' },
  pinned: { label: 'Pinned', says: 'it keeps its own grouping, and follows the grid\'s filters and the page\'s clicks' },
  frozen: { label: 'Frozen', says: 'it keeps its grouping and the grid\'s filter as they are now' },
};

/** A timeseries tile's states as its button says them, in a chart's words (timeseries-spec.ts TsState: no Pinned, its
 *  grouping always its own). */
const SERIES_STATE_WORDS: Readonly<Record<TsState, { readonly label: string; readonly says: string }>> = {
  dynamic: { label: 'Dynamic', says: 'it follows its grids\' filters and the page\'s clicks' },
  frozen: { label: 'Frozen', says: 'it keeps its grids\' filters as they are now' },
};

/** What a chart needs from the grid it is made from (a CubeApp). */
export interface ChartSource {
  /** The grid's query as it is now. */
  readonly snapshot: CubeSnapshot;
  /** The grid's configuration as it is now: its Column Properties among it. */
  readonly configuration: CubeConfiguration;
  /** Run a query the grid did not plan, through the grid's own runner: its rows, and the SQL the database ran. */
  run(query: Lambda, snapshot: CubeSnapshot, signal: AbortSignal): Promise<RunOutcome>;
  /** A query as the compiler prints it, for a person to read (a timeseries' Query window). */
  print(query: Lambda): Promise<string>;
  /** A value as the grid writes it; `headline`: scaled, a KPI's number (format.ts `headlineFormat`). */
  format(value: Scalar, column: string, type: string | undefined, headline?: boolean): string;
  /** A column's unit as the grid formats it (`currency|USD|`), or undefined: two units, two value axes. */
  unit?(column: string): string | undefined;
  /** A column as the grid names it. */
  label(column: string): string;
  /** What the grid shows now (its rows, their tree, its visible columns): a treemap draws it. */
  shown(): GridShown | null;
  /** Take `old` off the grid's filter and put `add` on, as one change named `label`. */
  refilter(old: readonly FilterNode[], add: readonly FilterNode[], label: string): void;
  /**
   * A chart's selection `old` taken off the grid's filter as a page step took it already (removeTile, followGrid:
   * page-commands.ts unselected). `theirs`: another editor's step, followed as its change is -- never told back, no step
   * of the grid's own (the S7 audit's P-1); else the person's own (a Remove, a Show Grid, a page undo): a step of the
   * grid's own history, told only after an edit of the grid's own still running -- the step said the rest (the S7
   * confirmation audit's G2-2, B2-3: a removal followed as another editor's wiped the grid's undo, and a click still
   * running then told its selection after the removal had taken it off).
   */
  unselected(old: readonly FilterNode[], theirs: boolean): void;
  /**
   * A grid of its own over the same source, in `host`: a chart's editing grid, or another grid on
   * the page. `onChart` is where its own "+ Chart" goes (the page), rather than a board of its own.
   */
  spawn(host: HTMLElement, snapshot: CubeSnapshot, options?: SpawnOptions): SpawnedGrid;
}

/** Where a grid on the page sends its own "+ Chart", "New grid" and the rest: to the page. */
/** A step a flow's New Box After makes (docs/DATACUBE_FLOW_MODE_2026_10_24.md FL9): the grid's gesture, and its name. */
export interface FlowStep {
  /** New Box After (FL9): the gesture made in the box made after. */
  readonly gesture?: { readonly name: string; readonly args: GestureArgs };
  /** Import Pure (FL12): the box made a Pure box of this function, and a box after it of each of `next`, in turn. */
  readonly pure?: Lambda;
  readonly next?: readonly Lambda[];
  /** Import Pure: the box taken apart, and the boxes made after it so far -- the last made, what read it reads the last
   *  (page-gestures.ts importedCommands). */
  readonly from?: string;
  readonly made?: readonly string[];
  readonly label: string;
  /** The box's name, given as it is made (a macro used: its name). */
  readonly title?: string;
}

export interface SpawnOptions {
  /** Its tile's id on the page (a page of its own: its host keeps what it knows of the grid by it). */
  readonly id?: string;
  readonly onChart?: () => void;
  readonly onNewGrid?: () => void;
  /** Its menus' Create ▸ Text Box: a text tile on its sheet. */
  readonly onText?: () => void;
  /** Its menus' Create ▸ Timeseries: a timeseries tile of it, under it. */
  readonly onTimeseries?: () => void;
  /** Its New ▸ Source…: a grid over another source, made by `make`, on this page. */
  readonly onNewSource?: (make: (host: HTMLElement, options: SpawnOptions) => SpawnedGrid) => void;
  /** Its menu's Remove from Page: the page takes its tile off, as the tile's own remove button does. */
  readonly onRemove?: () => void;
  /** The page around it, for its export: every chart where the board puts it, this grid as the table. */
  readonly exportPage?: () => ExportPage | undefined;
  /** Its Settings saved: the page's to apply to every grid (each reads them, the host keeps them). */
  readonly onSettingsChanged?: (values: SettingValues) => void;
  /** The host's readout in its status bar (a page of its own: its first grid's only), on each of its renders. */
  readonly hostStatus?: (slot: HTMLElement) => void;
  /** Its page's bar is folded: its title bar setting starts hidden, as every grid on the page says. */
  readonly titleBarHidden?: boolean;
  /**
   * For its menu's Move to Sheet: the page's other sheets, by what their tabs say, and whether the page is locked
   * (nothing moves: the entry is offered, disabled).
   */
  readonly sheets?: () => { readonly locked: boolean; readonly others: readonly { readonly id: string; readonly label: string }[] };
  /** Its Move to Sheet: onto `sheet`, or -- null -- onto a new one. */
  readonly onMoveToSheet?: (sheet: string | null) => void;
  /** Its configuration, when not the spawning grid's: Show Grid's, a chart's values as its Column Properties. */
  readonly configuration?: CubeConfiguration;
  /** Where its windows that set it dock (CubeAppOptions.dock): a flow's box, its flow's settings pane. */
  readonly dock?: HTMLElement;
  /** A flow's box's New Box After (CubeAppOptions.onStepAfter, FL9): the gesture made in a box after it. */
  readonly onStepAfter?: (gesture: { readonly name: string; readonly args: GestureArgs }, label: string) => void;
  /** A flow's Pure box set by its window (CubeAppOptions.onSetPure, FL12): told as the page's setPure. */
  readonly onSetPure?: (pure: Lambda | null) => boolean;
  /** A flow's Dynamic Replace's boxes to read rules from, and one read (CubeAppOptions.ruleBoxes, onReadRules). */
  readonly ruleBoxes?: () => readonly { readonly id: string; readonly label: string }[];
  readonly onReadRules?: (box: string, expressions: boolean) => void;
}

/** A grid on a page (a CubeApp). */
/** How a step's grid is made again by its owner (its host's maker, starting where it was when it went -- or, `document`
 *  given, as that document says). Undefined: its maker is gone. */
export type Remake = (grid: string, document?: CubeDocument) => ((host: HTMLElement, options: SpawnOptions) => SpawnedGrid) | undefined;

export interface SpawnedGrid {
  readonly snapshot: CubeSnapshot;
  open(): Promise<void>;
  dispose(): void;
  /** What its charts need from it. */
  chartSource(): ChartSource;
  /** Told each time a view lands ('view'), each time its state changes, a view or a width ('change'), each time where
   *  its rows are changes, a snap or going live ('plane'), and -- before that -- when it moved them itself ('moved'). */
  on(event: 'view' | 'change' | 'plane' | 'moved', fn: () => void): () => void;
  /** The copy it reads (a snap), as its page writes it down; undefined: it reads its rows live. */
  readonly copy?: GridCopy | undefined;
  /** Its part of its tile's header, at its right: its zones' way back, while they are folded. */
  tileHead(): HTMLElement;
  /** Its state, left of its tile's title: Live or Snapped (a lone grid's, in its sheet's tab). */
  tileState(): HTMLElement;
  /** What it reads, said on its tile's title's hover: "Reads trades.csv". */
  tileAbout(): string | undefined;
  /** The grid as a saved cube, named `name`: undefined when it cannot say its source. */
  cubeDocument?(name: string): CubeDocument | undefined;
  /** The window that sets a flow's step (flow-boxes.ts TOOL_EDITORS), opened -- docked where its options dock it. */
  openStep?(editor: StepEditor): void;
  /** Its view as it landed last: its rows, and a flat grid's total when the row cap cut them -- a flow's box's count, and
   *  a Test box's counts (its columns' first values). */
  readonly view?: {
    readonly rows: { readonly rowCount: number; readonly columns: readonly { readonly name: string; readonly values: readonly unknown[] }[] };
    readonly totalRows?: number;
  } | null;
  /** Its rows' profile (a flow's box's Profile: profile.ts), asked of the database when it is shown. */
  profile?(): Promise<Profile>;
  /** A column's most frequent values among its rows (a profile's column opened), `column` its name among them. */
  topValues?(column: string): Promise<readonly TopValue[]>;
  /** Its rows -- every one, the columns it shows -- written to a CSV file named `name` (a flow's Output). */
  writeOutput?(output: Output): Promise<void>;
  /** Its rows as it hands them on, at most `limit`, read by the database (a Dynamic Replace's rules: dynamic-replace.ts). */
  handedOnRows?(limit: number): Promise<ResultTable>;
  /** Its own steps as a function of its input's rows (a standard macro's step: macros.ts boxFunction). */
  ownFunction?(): Promise<Lambda>;
  /** The query it ran last, as the compiler prints it, and its SQL (a flow's box's Code). */
  code?(): Promise<{ readonly pure: string; readonly sql: string }>;
  /** Text onto its host's clipboard (a flow's box's Code copied). */
  copyText?(text: string): Promise<void>;
  /** A Cross Tab's values pinned as its rows hold them now, its columns fixed (FL2), or `pin` false its values its rows'
   *  again: the refusal in words, or null. */
  pinPivotValues?(pin: boolean): Promise<string | null>;
  /** A Cross Tab's values its pin lacks, each a column of its own after its columns (FL8's "Add to the cast"). */
  addPivotValues?(values: readonly (FilterValue | null)[]): Promise<string | null>;
  /** Whether its rows are a copy held in this page's database from the start (a file read into the tab): nothing to copy. */
  readonly heldHere?: boolean;
  /** Its snap's copy handed over as it closes (a cached box: FL10), or undefined when it holds none of its own. */
  handOverCopy?(): { readonly copy: GridCopy; readonly discard: () => Promise<void> } | undefined;
  /** Its rows copied into the tab (its Snap), unless they are already or its plane cannot move. */
  snapNow?(): Promise<void>;
  liveNow?(): Promise<void>;
  /** A gesture made on it as its menu makes one (a flow's New Box After: the new box's one step, FL9). */
  gesture?(name: string, args: GestureArgs, label: string): Promise<string | null>;
  /** A Cross Tab's values its pinned columns lack, asked of the database (FL3): none when it pins nothing. */
  lackingPivotValues?(): Promise<readonly (FilterValue | null)[]>;
}

export interface CubePageOptions {
  /** Where the board goes. */
  readonly host: HTMLElement;
  /** Where the page's windows float, above its tiles: a chart's options among its grids' windows. Default: `host`. */
  readonly windowHost?: HTMLElement;
  /**
   * The cube's own grid, when the board is a cube's (a CubeApp alone): its first tile, not removable -- the cube is the
   * host's. A page of its own has none: every grid is added alike.
   */
  readonly grid?: {
    readonly element: HTMLElement;
    readonly source: ChartSource;
    /** The grid's own part of its tile's header: its zones' way back. */
    readonly head?: HTMLElement;
    /** Its state, left of its tile's title: Live or Snapped. */
    readonly state?: HTMLElement;
    /** What it reads, said on its title's hover. */
    readonly about?: string;
  };
  /** The page changed: a tile, its title, its layout, a chart's spec or selection, a grid's own state. */
  readonly onChange: () => void;
  /**
   * Nothing left of what the board is for: a cube's own, its last chart gone (the grid goes back alone); a page of its
   * own, its last tile gone (it offers a source).
   */
  readonly onEmpty: () => void;
  /** A tile came or went: a page of its own shows a lone grid in its bar. */
  readonly onTiles?: () => void;
  /** A grid's Settings saved: every grid on the page takes them (a page of its own). */
  readonly onSettingsChanged?: (values: SettingValues) => void;
  /**
   * A text box's file read again, by its fingerprint (text-files.ts: a page points to its files): from the handle the
   * host kept for this page in this browser (file-handles.ts) -- `click` saying whether a person's click asks, the only
   * place the browser lets a page ask. Absent, or nothing kept: the text box asks for the file again.
   */
  readonly textFile?: (sha256: string, click: boolean) => Promise<HandleRead | undefined>;
  /** A text box's file picked again (its own name's Open…) with the handle the browser gave: kept by the host now, for
   *  the saved page it is -- the page unchanged, no save would keep it (the 2026-10-25 audit). */
  readonly keepTextFile?: (sha256: string, handle: FileHandle) => void;
  /** What the board refuses before it tells a command (a file too big to read, of a kind a text box does not show), said
   *  as the page's refusals are; absent, on its board. */
  readonly refuse?: (words: string) => void;
  /** Create ▸ Data Source… from a chart's or a text box's right-click: the host's picker, a grid over what is picked
   *  beside `near` (a page of its own: page-app.ts addSource); absent, a tile's Create ▸ offers none, as a grid's. */
  readonly addSource?: (near: string) => void;
  /**
   * How a grid like `from` -- over its source -- is made, starting at `snapshot` (and `configuration`, given; else
   * `from`'s): Copy of Grid, and the grid of a chart's own grouping Show Grid makes. A page of its own makes every grid
   * through its host (page/page-app.ts); without this, the grid's own `spawn`.
   */
  readonly gridLike?: (from: string, snapshot: CubeSnapshot, configuration?: CubeConfiguration) =>
    (host: HTMLElement, options: SpawnOptions) => SpawnedGrid;
  /**
   * What a sheet is called, as its tab says it (a page of its own names a sheet after its first grid until someone
   * names it). Given, the page offers Move to Sheet in its tiles' menus.
   */
  readonly sheetLabel?: (id: string) => string;
  /** The sheets came or went, were renamed or reordered, or another is shown: a page of its own paints its tabs. */
  readonly onSheets?: () => void;
  /**
   * The sheets' tabs, for a tile dragged onto one (moved to that sheet): the sheet whose tab is under the pointer (its
   * position in the window), and the tab to mark while it is (undefined: none).
   */
  readonly sheetTabs?: {
    readonly at: (x: number, y: number) => string | undefined;
    readonly hover: (sheet: string | undefined) => void;
  };
  /**
   * EACH EDIT OF THE PAGE'S STRUCTURE, as the page commands it is (page-commands.ts: the commands Python's page edits
   * build) and what it is called -- one step of the page's history (page-state.ts): a sheet added, renamed, moved or
   * removed; a tile added, renamed, moved to another sheet or removed; a chart's spec or selection changed by hand; a
   * sheet's layout changed by hand (a drag, a resize, a stack, an arrangement, a layout undone). THE BOARD IS PRODUCED BY
   * THE COMMANDS (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md S3): the owner applies them to the page's state by the one
   * rule (page-commands.ts applyToState) -- refused, nothing changes -- and puts the board at the page they make
   * (`sync`). A gesture changes nothing of the page itself: what it makes (a grid's engine, a chart's panel) waits until
   * its command places it, and a drag or a rename in place is the board's own drawing until then. True when applied.
   */
  readonly onPageEdit: (edit: PageEdit) => boolean;
  /**
   * Every id the page's state has held (page-commands.ts takenIds): a tile or a sheet a gesture makes takes the next id
   * after them and after every one on the board now (page-ids.ts nextPageId).
   */
  readonly taken: () => readonly string[];
  /** The tag of the ids this board's tiles are made under (page-ids.ts nextPageId), or undefined: plain ids. */
  readonly idTag?: () => string | undefined;
  /** A grid's cube document, as its page names it, for the command that carries one (`addGrid`):
   *  undefined when it cannot say its source. Absent, the grid's own (a cube alone's board: named as the cube is). */
  readonly documentOf?: (grid: string) => CubeDocument | undefined;
  /** Ctrl+Z (Ctrl+Shift+Z: `redo`) on a tile's frame: the page's own undo (a page of its own); absent, Undo Layout. */
  readonly onUndo?: (redo: boolean) => void;
  /** What the page's Undo and Redo would undo and redo now, by name: a flow's Undo and Redo say it. */
  readonly history?: () => { readonly undo?: string | undefined; readonly redo?: string | undefined };
  /**
   * What a chart's click on a mark of grid `grid` is decided on (page-filters.ts chartPicked): the grid's source -- on a
   * grid over a class, the conditions on its properties are a page filter (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md
   * §16.2) -- and the page's filters now. Absent (a cube alone), the click is its grid's alone.
   */
  readonly pickOn?: (grid: string) => { readonly source: CubeSource | undefined; readonly filters: readonly PageFilter[] };
  /**
   * A FLOW'S BOX SHOWN (docs/DATACUBE_FLOW_MODE_2026_10_24.md M1's design, 3): how its grid is made over `document`, its
   * cube as the page has it now -- its source opened by its host, its live inputs read as the page has them now;
   * undefined when the person gave up. Absent, a page has no flow sheets to show a box of.
   */
  readonly openBox?: (box: string, document: CubeDocument) => Promise<((host: HTMLElement, options: SpawnOptions) => SpawnedGrid) | undefined>;
  /** A file dropped onto flow `sheet`'s canvas: an Input box over it, added by the page's owner. */
  readonly onFile?: (file: File, sheet: string) => void;
  /** A tool of a flow's palette picked: a box of it added to flow `sheet`, after box `after` (none: on its own), its
   *  settings (`editor`) opened once it is shown -- dropped on a line, `into` the box that line goes into, which reads
   *  the new box from then on. */
  readonly onTool?: (tool: FlowTool, after: string | undefined, sheet: string, editor: StepEditor | undefined, into?: LineEnd,
    port?: InputPort, at?: FlowPoint, step?: FlowStep) => void;
  /** A flow's Join or Union box's settings asked for (docs/DATACUBE_FLOW_MODE_2026_10_24.md M2): the Join window over its
   *  inputs, opened by the page's owner -- a window over the page, never docked. */
  readonly onJoinBox?: (box: string, sheet: string) => void;
  /** A flow's palette's Parameter: the page's Add Parameter (page-app.ts addParameter). */
  readonly onParameter?: () => void;
  /** The page's macros (macros.ts) and parameters, as its state holds them: what a flow's Macro offers, and in what
   *  words -- and what a Dynamic Input is picked by. */
  readonly macros?: () => readonly PageMacro[];
  readonly parameters?: () => readonly PageParameter[];
  /** A flow's Union set to keep the rows of one input, picked by a parameter (a Dynamic Input: dynamic-input.ts), or of
   *  every input again: set by the page -- the refusal in words, or null. */
  readonly onPick?: (box: string, pick: InputPick, called: (grid: string) => string) => string | null;
  /** Tidy Up on flow `sheet`: its boxes placed by the flow's own rule, by the page's owner (page-gestures.ts tidyCommands). */
  readonly onTidy?: (sheet: string) => void;
  /**
   * The grids of `cubes` opened together over one model of their tables, for a timeseries series that combines them
   * (docs/DATACUBE_TIMESERIES_DESIGN_2026_10_25.md §5.2: join-open.ts openSources) -- a page of its own over this tab's
   * database. Absent, such a series is refused in words.
   */
  readonly together?: (cubes: readonly string[]) => Promise<TsTogether>;
  /** A cube the page holds as it saved it, its source not opened (page-app.ts: P9), by its id: what a timeseries tile
   *  freezing keeps of it (its filter), as Python's freeze reads the page's cube. Absent: none such. */
  readonly unopenedCube?: (cube: string) => CubeDocument | undefined;
}

/** An edit of the page's structure: its page commands, and what it is called (the page's Undo names it). */
export interface PageEdit {
  readonly label: string;
  readonly commands: readonly PageCommand[];
  /** False: no step of the page's history (a snap or going live: where the rows are, not what the page is). */
  readonly step?: false;
}

/** A sheet as the page has it: its id, its name when someone gave it one, its tiles in reading order, and whether it is
 *  a flow (its boxes on a canvas: docs/DATACUBE_FLOW_MODE_2026_10_24.md FL4). */
export interface SheetInfo {
  readonly id: string;
  readonly name?: string;
  readonly tiles: readonly string[];
  readonly flow?: true;
}

/** A SHEET: a whole screen of tiles, on a board of its own -- its bands, its fit, its layout undo. */
interface BoardSheet {
  readonly kind: 'bands';
  readonly id: string;
  /** Its name, when someone gave it one. */
  name: string | undefined;
  readonly host: HTMLElement;
  readonly board: BandBoard;
  /** Each arrangement by hand is one step (the design's §3.3): the layouts before the steps done, and after those undone. */
  readonly steps: { undone: Bands[]; redone: Bands[] };
}

/** A FLOW SHEET (docs/DATACUBE_FLOW_MODE_2026_10_24.md FL4): its boxes on a canvas, the grid of the one shown below it. */
interface FlowSheet {
  readonly kind: 'flow';
  readonly id: string;
  name: string | undefined;
  readonly host: HTMLElement;
  readonly board: FlowBoard;
}

type Sheet = BoardSheet | FlowSheet;

/** A sheet's tiles in reading order: a board's by its bands, a flow's in the order its boxes were placed. */
const sheetTiles = (sheet: Sheet): string[] => (sheet.kind === 'flow' ? flowTiles(sheet.board.layout) : tiles(sheet.board.layout));

/** A layout's tiles, either kind. */
const layoutTilesOf = (layout: PageLayout): string[] => (layout.kind === 'flow' ? flowTiles(layout) : tiles(layout));

/** A FLOW'S BOX NOT SHOWN: no grid made for it, its view and its cube as the page has them (M1's design, 3). */
/** A box or a note a flow's Ctrl+C kept: its cube (or its words), and where it was. */
type ClipItem = { readonly tile: string; readonly x: number; readonly y: number }
  & ({ readonly document: CubeDocument; readonly box: FlowBox } | { readonly text: string });

/** How far down and right of where they were boxes are pasted, given no place: their copies seen beside them. */
const PASTE_OFFSET = 32;

interface ClosedBox {
  view: GridView;
  document: CubeDocument;
}

/** A grid on the page: the cube's own, or another one added to it. */
interface GridTile {
  readonly source: ChartSource;
  /** An added grid: its cube, and how to stop listening to it. */
  readonly added?: { readonly cube: SpawnedGrid; readonly stop: () => void };
}

/**
 * Where a chart's rows come from: the grid it belongs to, or -- DETACHED, its grid removed while it was frozen -- its
 * own copy of that grid's query (source, filter, calculated columns), run by that grid, kept off the board for it
 * (`#kept`) until its last such chart goes.
 */
interface ChartLink {
  grid: string | null;
  detached?: { readonly snapshot: CubeSnapshot; readonly from: string };
}

/** A text tile's words when Create ▸ Text Box makes one, its editor open on them. */
const STARTING_TEXT = 'Double-click to write here.';

/** A text tile on the board: its words as the page has them, its body, and whether its editor is open. */
interface TextTile {
  text: string;
  /** The files it shows beside its words (text-files.ts), as the page has them. */
  files: readonly TextFile[];
  /** The file its tabs show, by its fingerprint; null: its own words. */
  shown: string | null;
  /** Its words as last drawn, shown or not (a file's tab in front): what an export writes. */
  drawn: HTMLElement | null;
  readonly body: HTMLElement;
  /** Its pencil, left of its title: a click edits its words. */
  readonly edit: HTMLButtonElement;
  editing: boolean;
}

/** A timeseries tile on the board (ui/timeseries-view.ts, a chunk of its own fetched with the first drawn): its spec as
 *  the page has it, its body, and its viewer once fetched. */
interface SeriesTile {
  spec: TimeseriesSpec;
  readonly body: HTMLElement;
  view: TimeseriesView | null;
  /** Its state button, as it now is (dynamic, frozen, detached). */
  readonly paint: () => void;
  /** A grid it reads landed a view while its sheet was not shown: it asks again when its sheet is. */
  stale: boolean;
}

interface ChartTile {
  readonly panel: ChartPanel;
  readonly link: ChartLink;
  /** Its buttons, as it now is (following, frozen, detached). */
  readonly paint: () => void;
  readonly chip: HTMLButtonElement;
  /** The conditions this chart's last click put on its grid's filter. */
  conditions: FilterNode[];
  /** Its spec as last told (`#chartChanged`), as JSON: a change told once however many callbacks say it. */
  told: string;
  /** Whether the page has it: a chart made with no spec yet has not, until its first one (#tellSpec). */
  added: boolean;
}

/**
 * A drawn text block as an export's lines (page-text.ts textElement): a list's items each after its mark (a task's box
 * as `[x]`), those under an item indented; code a line a line; a quote's lines after `>`; a table's rows, their cells
 * between `|`; a rule `---`; any other block its words.
 */
export function textLines(block: Element): string[] {
  const nested = (el: Element): boolean => /^(UL|OL|BLOCKQUOTE|PRE|DIV|P|HR)$/.test(el.tagName);
  switch (block.tagName) {
    case 'UL':
    case 'OL': {
      const start = Number(block.getAttribute('start') ?? '1');
      return [...block.children].flatMap((li, i) => {
        const box = li.querySelector<HTMLInputElement>(':scope > input[type=checkbox]');
        const mark = `${block.tagName === 'OL' ? `${start + i}.` : '-'}${box ? (box.checked ? ' [x]' : ' [ ]') : ''}`;
        const words = [...li.childNodes].filter((n) => n.nodeType !== 1 || !nested(n as Element)).map((n) => n.textContent ?? '').join('');
        return [`${mark} ${words}`, ...[...li.children].filter(nested).flatMap(textLines).map((l) => `  ${l}`)];
      });
    }
    case 'PRE':
      return (block.textContent ?? '').split('\n');
    case 'BLOCKQUOTE':
      return [...block.children].flatMap(textLines).map((l) => `> ${l}`);
    case 'HR':
      return ['---'];
    case 'DIV':
      return [...block.querySelectorAll('tr')].map((tr) => [...tr.children].map((c) => c.textContent ?? '').join(' | '));
    default:
      return [block.textContent ?? ''];
  }
}

export class CubePage {
  readonly #doc: Document;
  readonly #options: CubePageOptions;
  /** The sheets, in order; one is shown. */
  readonly #sheets: Sheet[] = [];
  #shown: Sheet;
  /** The tiles a gesture made, waiting for the command that places them (`sync`): a grid's, a chart's. */
  readonly #pending = new Map<string, BandTile>();
  /** Inside `sync`: the board emptied by it is said empty once it has done (`#afterRemove`). */
  #syncing = false;
  #emptied = false;
  /**
   * EACH SHEET'S LAYOUT AS THE PAGE HAS IT (P7), set only when the board is put at a page (`sync`, `restore`, a new
   * sheet): what `views` writes. The board's own drawing may differ -- a chart with no spec yet placed beside its grid,
   * the tile a stack shows in front -- and never reaches the page.
   */
  readonly #pageLayouts = new Map<string, Bands>();
  /** While an edit tells the command it is, the edits it is made of tell none (a sheet removed with its tiles). */
  #quiet = 0;
  /** A saved page's tiles, each to go on the sheet it was saved on as it is added (`setSheets`, `restore`). */
  readonly #planned = new Map<string, Sheet>();
  /** Each tile as it was put on its board: put on another sheet's as it is (Move to Sheet). */
  readonly #specs = new Map<string, BandTile>();
  #editing = true;
  /** The layouts, fetched the first time they are opened (a page that is never arranged never downloads them). */
  #picker: LayoutPicker | undefined;
  #disposed = false;
  readonly #charts = new Map<string, ChartTile>();
  readonly #grids = new Map<string, GridTile>();
  /** The text tiles (page-text.ts): each one's words as the page has them, its body, whether its editor is open. */
  readonly #texts = new Map<string, TextTile>();
  /** What a flow's Ctrl+C kept to paste: boxes' cubes and notes' words, each where it was, in the order they read each
   *  other. */
  #clipboard: readonly ClipItem[] | undefined;
  /** A flow's boxes not shown, by id: documents, no grid made for them until one is (`#showBox`). */
  readonly #boxes = new Map<string, ClosedBox>();
  /** The boxes whose grids are being made (their sources opening), each once. */
  readonly #showing = new Set<string>();
  /** The timeseries tiles (ui/timeseries-view.ts): each one's spec as the page has it, its body, its viewer. */
  readonly #series = new Map<string, SeriesTile>();
  /** The text boxes' files read this session, by fingerprint, and the handle each was picked with where the browser
   *  gave one: shown without asking again, and their handles kept when the page is saved (`textFilesNow`). */
  readonly #textFiles = new Map<string, { readonly file: File; readonly handle?: FileHandle }>();
  /** Grids removed from the board while a frozen chart still reads them: kept, off the board, to run its query. */
  readonly #kept = new Map<string, SpawnedGrid>();
  /** Each grid's title as it was made, so a saved page writes only a title someone gave it. */
  readonly #titles = new Map<string, string>();
  /**
   * WHAT A SAVED PAGE HAS THAT THIS ONE COULD NOT OPEN (a grid whose source was not opened, a chart reading one): each
   * view kept as its page saved it, a tile saying why in its place -- written back, never dropped (P9).
   */
  readonly #unavailable = new Map<string, PageView>();
  /** The order the charts came in, those it could not open among them: views() writes them so. */
  readonly #came = new Map<string, number>();
  #cameNext = 0;
  /** Each added grid's first open (its first view), for `opened`. */
  readonly #opening = new Map<string, Promise<void>>();
  /** Each cached box's copy the page keeps while it is not shown (FL10: its grid's Snap, handed over as it closed), for
   *  what it was made of then (`key`, its chain): what the boxes after it read until that changes (cacheCopies). */
  readonly #cacheKept = new Map<string, { readonly copy: GridCopy; readonly key: string; readonly discard: () => Promise<void> }>();
  /** A cached box's snap under way, once it is shown: a run waits for it before the box after it is shown. */
  readonly #caching = new Map<string, Promise<void>>();
  /** Each Cross Tab's question of the values its columns lack, while it is asked (#askLacking): what a run waits for. */
  readonly #lackingAsked = new Map<string, Promise<void>>();
  /** Each Cross Tab's question the database refused, last asked, in its words (#askLacking): why a run it stops stops. */
  readonly #lackingRefused = new Map<string, string>();
  /** This page's crosshair group (chart-render.ts setGroup): its own, so two pages on one screen do not share. */
  readonly #crosshair = `page-${(crosshairs += 1)}`;
  /** A chart's right-click menu: its Options, Show Grid, Remove. */
  readonly #menu: MenuView;
  #menuFor: ((item: MenuItem) => void) | null = null;

  constructor(options: CubePageOptions) {
    this.#options = options;
    this.#doc = options.host.ownerDocument;
    this.#menu = new MenuView(this.#doc, { onSelect: (item) => this.#menuFor?.(item) });
    const first = this.#newBands(FIRST_SHEET);
    this.#shown = first;
    first.host.hidden = false;
    const own = options.grid;
    if (!own) return;
    // a chart or another grid is added from the menus (right-click or the hamburger, Insert), not
    // from buttons on the tile (the user, 2026-09-30)
    this.#grids.set(GRID, { source: own.source });
    this.#titles.set(GRID, GRID_TILE_TITLE);
    this.#place({
      id: GRID,
      // the cube's own name is the page's title already, above the board
      title: GRID_TILE_TITLE,
      element: own.element,
      actions: own.head ? [own.head] : [],
      ...(own.state ? { state: own.state } : {}),
      ...(own.about !== undefined ? { about: own.about } : {}),
      // the cube is the host's: the page's document is this grid's cube
      removable: false,
    });
    // a screenful: the grid had the whole page until now
    first.board.setLayout({ fit: true, bands: [{ height: 1, node: { tile: GRID } }] });
    this.#pageLayouts.set(FIRST_SHEET, first.board.layout);
  }

  /** A sheet, after the others, not shown: a board of its own in an element of its own -- a flow's canvas (`flow`), else
   *  bands. */
  #newSheet(id: string, name?: string, flow = false): Sheet {
    return flow ? this.#newFlow(id, name) : this.#newBands(id, name);
  }

  /** A sheet's element, after the others', hidden. */
  #sheetHost(id: string): HTMLElement {
    const host = this.#doc.createElement('div');
    host.className = 'dc-sheet';
    host.dataset['sheet'] = id;
    host.hidden = true;
    this.#options.host.append(host);
    return host;
  }

  #newBands(id: string, name?: string): BoardSheet {
    const host = this.#sheetHost(id);
    const steps: BoardSheet['steps'] = { undone: [], redone: [] };
    const tabs = this.#options.sheetTabs;
    // the sheet a dragged tile is over the tab of: another one than its own
    const other = (x: number, y: number): string | undefined => {
      const at = tabs?.at(x, y);
      return at !== undefined && at !== id ? at : undefined;
    };
    const board = new BandBoard(host, {
      onRemove: (tileId) => this.#removeTile(tileId),
      onChange: (_layout, before, move) => {
        steps.undone = [...steps.undone, before].slice(-LAYOUT_STEPS);
        steps.redone = [];
        this.#tellMove(sheet, before, move);
        this.#options.onChange();
      },
      onRename: (tile, title) => {
        // the title as typed: a grid given its made title back keeps none of its own, by renameTile's rule (views()
        // writes none either); an editing grid, which a page does not save, is no tile of the page's to tell
        if (this.#saved(tile)) this.#tell('Rename Tile', pageCommand('renameTile', { tile, title }));
        this.#options.onChange();
      },
      onLayout: (tileId, anchor) => this.showLayouts(anchor, tileId),
      onUndo: (redo) => {
        const undo = this.#options.onUndo;
        if (undo) undo(redo);
        else if (redo) this.redoLayout();
        else this.undoLayout();
      },
      ...(tabs ? {
        outside: {
          over: (x, y) => {
            const at = other(x, y);
            tabs.hover(at);
            return at !== undefined;
          },
          drop: (tile, x, y) => {
            const at = other(x, y);
            if (at !== undefined) this.moveToSheet(tile, at);
          },
          leave: () => tabs.hover(undefined),
        },
      } : {}),
    });
    // a new sheet's board (bands.ts NEW_SHEET): it fits its window, and scrolls once its bands are too many to fit
    board.setLayout(NEW_SHEET);
    this.#pageLayouts.set(id, NEW_SHEET);
    board.setEditing(this.#editing);
    const sheet: BoardSheet = { kind: 'bands', id, name, host, board, steps };
    this.#sheets.push(sheet);
    return sheet;
  }

  /**
   * A FLOW SHEET'S BOARD (layout/flow-board.ts): what the hand does there told as the page's commands -- a box let go
   * somewhere else as its place (placeBox), its × as its removal, its name as its title -- a box clicked shown (its
   * grid made, `#showBox`), and a tool of the palette added by the page's owner (`onTool`).
   */
  #newFlow(id: string, name: string | undefined): FlowSheet {
    const host = this.#sheetHost(id);
    const board = new FlowBoard(host, {
      onMove: (tile, x, y) => {
        this.#tell('Move Box', pageCommand('placeBox', { tile, x, y }));
        this.#options.onChange();
      },
      onMoveMany: (moves, label) => {
        this.#tell(label ?? 'Move Boxes', ...moves.map((m) => pageCommand('placeBox', { tile: m.id, x: m.x, y: m.y })));
        this.#options.onChange();
      },
      onFrame: (tile, x, y, frame) => {
        const was = this.#sheetOf(tile)?.board.layout;
        const before = was !== undefined && 'boxes' in was ? was.boxes.find((b) => b.tile === tile)?.frame : undefined;
        const label = before !== undefined && (before.collapsed === true) !== (frame.collapsed === true)
          ? (frame.collapsed === true ? 'Fold Container' : 'Unfold Container') : 'Size Container';
        this.#tell(label, pageCommand('placeBox', { tile, x, y, frame }));
        this.#options.onChange();
      },
      onContainer: (at, frame) => this.#addContainer(id, at, frame),
      onEditText: (tile) => this.editText(tile),
      onOutput: (tile, output) => {
        this.#tell(output === null ? 'Write No File' : 'Set Output', pageCommand('setOutput', { grid: tile, output }));
        this.#options.onChange();
      },
      onCache: (tile, cache) => {
        this.#tell(cache ? 'Cache Rows' : 'Stop Caching', pageCommand('setCache', { grid: tile, cache }));
        // cached while it is shown: copied now; no longer cached, the copy kept for it let go (cacheCopies' rule)
        if (cache) this.#cacheShown(tile, board);
        else {
          this.#letCacheGo(tile);
          // shown: its own grid read live again, as Python's cache(False) goes live (the roadmap audit of 2026-10-26: it
          // stayed snapped, the boxes after it reading the copy)
          void this.#grids.get(tile)?.added?.cube.liveNow?.();
        }
        this.#options.onChange();
      },
      cachedCopy: (tile) => {
        const grid = this.#grids.get(tile)?.added?.cube;
        return this.#cacheKept.get(tile)?.copy ?? grid?.copy ?? (grid?.heldHere === true ? 'here' : undefined);
      },
      // BLOCK UNTIL DONE and a MESSAGE (M8): a box's settings, each the page's command
      onBlocks: (tile, blocks) => {
        this.#tell(blocks ? 'Block Until Done' : 'Stop Blocking', pageCommand('setBlocks', { grid: tile, blocks }));
        this.#options.onChange();
      },
      onMessage: (tile, message) => {
        // judged by the flow's own rule before the page is told (layout/flow.ts messageProblem): its words said here
        const why = message === null ? undefined : messageProblem(message);
        if (why !== undefined) return why;
        const told = this.#tell(message === null ? 'Say Nothing' : 'Set Message', pageCommand('setMessage', { grid: tile, message }));
        this.#options.onChange();
        return told ? null : 'the page did not take it';
      },
      onStops: (tile, stops) => {
        this.#tell(stops ? 'Stop Run On Failure' : 'Warn On Failure', pageCommand('setStops', { grid: tile, stops }));
        this.#options.onChange();
      },
      onPinValues: async (tile, pin) => {
        // its grid made first, when it is not shown: its values are its rows'
        if (this.#grids.get(tile)?.added?.cube === undefined) await this.#showBox(tile);
        const grid = this.#grids.get(tile)?.added?.cube;
        if (grid?.pinPivotValues === undefined) {
          const on = this.#sheetOf(tile);
          return (on?.kind === 'flow' ? on.board.refusal(tile) : undefined) ?? 'its grid could not be opened';
        }
        return grid.pinPivotValues(pin);
      },
      // A STANDARD MACRO (M8, macros.ts): the page's, each in words; one used after a box; a box's steps saved as one
      macros: () => (this.#options.macros?.() ?? []).map((m) => ({ name: m.name, words: macroWords(m, this.#options.parameters?.() ?? []) })),
      onMacro: (name, after) => this.#useMacro(id, name, after),
      onSaveMacro: (tile, from, name) => this.#saveMacro(tile, from, name),
      // A DYNAMIC INPUT (M8): a Union's pick, by a parameter of the page's
      parameters: () => this.#options.parameters?.() ?? [],
      ...(this.#options.onPick ? { onPick: (tile: string, pick: InputPick, called: (grid: string) => string) => this.#options.onPick!(tile, pick, called) } : {}),
      onAddPinned: async (tile, values) => {
        const grid = this.#grids.get(tile)?.added?.cube;
        if (grid?.addPivotValues === undefined) return 'its grid is not open';
        return grid.addPivotValues(values);
      },
      onWrite: (tile) => this.#writeBox(tile),
      onSelect: (tile, editor) => void this.#showBox(tile, editor),
      onRemove: (tile) => this.#removeTile(tile),
      onRemoveMany: (tiles) => this.#removeTiles(tiles),
      onRename: (tile, title) => {
        if (this.#saved(tile)) this.#tell('Rename Tile', pageCommand('renameTile', { tile, title }));
        this.#options.onChange();
      },
      // a Comment is the page's own (a text tile), any other tool its owner's to make
      onTool: (tool, after, editor, into, port, at) => (tool === 'comment' ? this.#addComment(id, at)
        // a Parameter is the page's own (page-parameters.ts): its bar's Add form
        : tool === 'parameter' ? this.#options.onParameter?.()
          // a Macro asked by name (the command palette): the page's one, else which is the palette's to pick
          : tool === 'macro' ? this.#macroAsked(id, after)
        // a Container clicked in the palette, or dropped on a box: one of its own size, below the flow's boxes
        : tool === 'container' ? this.#addContainer(id, at ?? this.#belowFlow(id), CONTAINER_FRAME)
          : this.#options.onTool?.(tool, after, id, editor, into, port, at)),
      // a line drawn, taken off, or set in a box's settings: what the box reads (docs/DATACUBE_FLOW_MODE_2026_10_24.md M2)
      onConnect: (tile, side, from, port) => {
        this.#tell('Link Box', pageCommand('setInput', { grid: tile, side, input: { grid: from, ...(port !== undefined ? { port } : {}) } }));
        this.#options.onChange();
      },
      onFreeze: (tile, side) => {
        this.#tell('Freeze Box', pageCommand('setInput', { grid: tile, side, input: null }));
        this.#options.onChange();
      },
      onTakeOff: (tile, side) => {
        const source = (this.#boxes.get(tile)?.document ?? this.documentOf(tile))?.source;
        if (source === undefined) return;
        this.#tell('Take Off Input', ...takeOffCommands(tile, source, side));
        this.#options.onChange();
      },
      onUndo: (redo) => this.#options.onUndo?.(redo),
      ...(this.#options.history ? { history: this.#options.history } : {}),
      onFile: (file) => this.#options.onFile?.(file, id),
      documentOf: (tile) => this.#boxes.get(tile)?.document ?? this.documentOf(tile),
      onTidy: () => this.#options.onTidy?.(id),
      onRun: (dry) => void this.#runFlow(id, dry === true),
      onDuplicate: (tile) => this.#duplicateBox(id, tile),
      onCopy: (tiles, cut) => this.#copyBoxes(tiles, cut),
      profileOf: (tile) => {
        const grid = this.#grids.get(tile)?.added?.cube;
        return grid?.profile === undefined ? Promise.reject(new Error(`${tile} is not shown`)) : grid.profile();
      },
      topValuesOf: (tile, column) => {
        const grid = this.#grids.get(tile)?.added?.cube;
        return grid?.topValues === undefined ? Promise.reject(new Error(`${tile} is not shown`)) : grid.topValues(column);
      },
      firstRowOf: (tile) => {
        const v = this.#grids.get(tile)?.added?.cube.view;
        if (v === undefined || v === null) return undefined;
        // no row (a Test of no rows): every column's value undefined
        return (column) => (v.rows.rowCount === 0 ? undefined : v.rows.columns.find((c) => c.name === column)?.values[0]);
      },
      codeOf: (tile) => {
        const grid = this.#grids.get(tile)?.added?.cube;
        return grid?.code === undefined ? Promise.reject(new Error(`${tile} is not shown`)) : grid.code();
      },
      copyText: (tile, text) => {
        const grid = this.#grids.get(tile)?.added?.cube;
        return grid?.copyText === undefined ? Promise.reject(new Error(`${tile} is not shown`)) : grid.copyText(text);
      },
      // its rows drawn as a chart tile draws its grid's (a mark clicked filters nothing: a box's query is its step)
      chartOf: (tile, host) => {
        const grid = this.#grids.get(tile)?.added?.cube;
        if (grid === undefined) throw new Error(`${tile} is not shown`);
        const source = (): ChartSource => grid.chartSource();
        const panel = new ChartPanel(host, {
          snapshot: () => source().snapshot,
          run: async (query, snapshot, signal) => (await source().run(query, snapshot, signal)).rows,
          label: (value, column, type, headline) => source().format(value, column, type, headline),
          unit: (column) => source().unit?.(column),
          shown: () => source().shown(),
          onPick: () => undefined,
          formOpen: false,
        });
        // its Options, as a chart tile's: a window beside it, put away again (ui/chart-panel.ts openForm)
        return { toggleForm: () => { if (panel.formOpen) panel.closeForm(); else panel.openForm(); }, dispose: () => panel.dispose() };
      },
      onPaste: (at) => this.#pasteBox(id, at),
    });
    board.setEditing(this.#editing);
    const sheet: FlowSheet = { kind: 'flow', id, name, host, board };
    this.#sheets.push(sheet);
    return sheet;
  }

  /** The next id of `kind` (page-ids.ts nextPageId): after every id the page's state has held and every one on the board
   *  now -- a tile a gesture made that its command has not placed yet among them. */
  #nextId(kind: 'grid' | 'chart' | 'text' | 'timeseries' | 'sheet', reserved: Iterable<string> = []): string {
    const board = [...this.#sheets.map((s) => s.id), ...this.#grids.keys(), ...this.#charts.keys(), ...this.#kept.keys(),
      ...this.#unavailable.keys(), ...this.#pending.keys(), ...this.#texts.keys(), ...this.#series.keys(), ...this.#boxes.keys(), ...reserved];
    return nextPageId([...this.#options.taken(), ...board], kind, this.#options.idTag?.());
  }

  /** A page edit told as its commands, one step named `label` (onPageEdit: applied, and the board put at what it makes),
   *  unless it is part of one told already. True when applied. */
  #tell(label: string, ...commands: PageCommand[]): boolean {
    return this.#quiet === 0 && this.#options.onPageEdit({ label, commands });
  }

  /** Whether a tile is one the page saves (views() writes it): not a chart's editing grid, a moment's work. */
  #saved(tile: string): boolean {
    return this.views((g) => g).views.some((v) => v.id === tile);
  }

  /** The tiles the page saves (views() writes them): not a chart's editing grid, a moment's work. */
  #savedTiles(): ReadonlySet<string> {
    return new Set(this.views((g) => g).views.map((v) => v.id));
  }

  /** A sheet's layout told whole, as views() writes it: its saved tiles alone, as a page saves them
   *  (page-gestures.ts layoutCommand). */
  #tellLayout(sheet: BoardSheet): void {
    if (this.#quiet > 0) return;
    this.#tell('Layout Change', layoutCommand(sheet.id, sheet.board.layout, this.#savedTiles()));
  }

  /** A sheet's layout changed by hand, told as what the hand did (page-gestures.ts movedCommand: a stack, a preset
   *  picked, else the layout whole). `before`: the layout the hand changed. */
  #tellMove(sheet: BoardSheet, before: Bands, move: LayoutMove | undefined): void {
    if (this.#quiet > 0) return;
    this.#tell('Layout Change', movedCommand(sheet.id, before, move, sheet.board.layout, this.#savedTiles()));
  }

  /** Commands told by `edit` are left untold: the edit tells its own. */
  #quietly(edit: () => void): void {
    this.#quiet += 1;
    try {
      edit();
    } finally {
      this.#quiet -= 1;
    }
  }

  /** The sheet a tile is on. */
  #sheetOf(tile: string): Sheet | undefined {
    return this.#sheets.find((s) => s.board.title(tile) !== undefined);
  }

  /**
   * A tile on its sheet: the one a saved page put it on (`setSheets`), else the sheet of `near` -- under it (bands.ts
   * add) -- else the sheet shown.
   */
  #place(spec: BandTile, near?: string): void {
    const by = near !== undefined ? this.#sheetOf(near) : undefined;
    const sheet = this.#planned.get(spec.id) ?? by ?? this.#shown;
    this.#specs.set(spec.id, spec);
    if (sheet.kind === 'flow') {
      // a comment (a text tile: M7), drawn as a note where the flow places it
      if (this.#texts.has(spec.id)) {
        sheet.board.addNote(spec, by === sheet ? near : undefined);
        return;
      }
      // a flow's boxes are grids, one shown at a time: the one shown before closes (`#closeBox`)
      if (!this.#grids.has(spec.id)) throw new Error(`${spec.id} is no grid: a flow sheet holds boxes and comments alone`);
      this.#closeBox(sheet);
      this.#boxes.delete(spec.id);
      sheet.board.add(spec, by === sheet ? near : undefined);
      return;
    }
    sheet.board.add(spec, by === sheet ? near : undefined, this.#charts.has(spec.id) ? this.#chartsOnly : undefined);
  }

  // -- a flow's boxes (docs/DATACUBE_FLOW_MODE_2026_10_24.md M1's design, 3) --------------------------------------

  /** A box of flow `sheet` not shown, drawn from `document` (its cube as the page has it): put there, or -- there
   *  already -- its view and its cube as the step has them. */
  #putBox(sheet: FlowSheet, view: GridView, document: CubeDocument): void {
    const title = view.title ?? gridTitle(view.id);
    const box = this.#boxes.get(view.id);
    if (box !== undefined) {
      box.view = view;
      box.document = document;
      return;
    }
    this.#boxes.set(view.id, { view, document });
    this.#titles.set(view.id, gridTitle(view.id));
    sheet.board.addBox(view.id, title);
  }

  /** A box not shown off its flow, as a step leaves it. */
  #dropBox(id: string): void {
    if (!this.#boxes.delete(id)) return;
    this.#take(id);
  }

  /**
   * THE PAGE'S PARAMETERS CHANGED (page-parameters.ts): each flow's shown box opened again, so its base -- its inputs'
   * rows, its own Pure -- reads their values now (live-inputs.ts resolvedSource binds them where it resolves a source),
   * as its own steps read them through its lens.
   */
  parametersChanged(): void {
    for (const sheet of this.#sheets) {
      if (sheet.kind !== 'flow') continue;
      const shown = sheet.board.shown;
      if (shown === undefined || this.#grids.get(shown)?.added === undefined) continue;
      // a cached box's copy is of the value it was copied with: let go with its grid -- its box live on the page again,
      // so nothing reads the copy dropped -- and copied again as it opens (the roadmap audit of 2026-10-26)
      const cached = sheet.board.layout.boxes.find((b) => b.tile === shown)?.cache === true;
      const snapped = cached && this.#grids.get(shown)?.added?.cube.copy !== undefined;
      this.#closeBox(sheet, undefined, !cached);
      if (snapped) this.#options.onPageEdit({ label: 'Go Live', step: false, commands: [pageCommand('live', { grid: shown })] });
      void this.#showBox(shown);
    }
  }

  /**
   * THE BOX SHOWN ON `sheet` CLOSED: its grid let go -- its cube kept as a document (`document`, else its grid's as it is
   * now), its view as the page saves it -- and the box left on the canvas, drawn from that document. No step of the page:
   * which box is shown is the screen's (§3.4).
   */
  #closeBox(sheet: FlowSheet, document?: CubeDocument, handOver = true): void {
    const id = sheet.board.shown;
    if (id === undefined) return;
    const grid = this.#grids.get(id);
    const doc = document ?? this.documentOf(id);
    if (!grid?.added || doc === undefined) throw new Error(`box ${id} cannot be closed: it cannot say its cube`);
    const title = this.title(id);
    const view: GridView = { id, kind: 'grid', cube: id, ...(title !== undefined && title !== this.#titles.get(id) ? { title } : {}) };
    // a cached box (FL10): its copy handed to the page, which the boxes after it read until what it is made of changes
    if (handOver && sheet.board.layout.boxes.find((b) => b.tile === id)?.cache === true) {
      const handed = grid.added.cube.handOverCopy?.();
      if (handed !== undefined) this.#cacheKept.set(id, { ...handed, key: sheet.board.chain(id) });
    }
    grid.added.stop();
    grid.added.cube.dispose();
    this.#grids.delete(id);
    this.#specs.delete(id);
    sheet.board.close(id);
    this.#boxes.set(id, { view, document: doc });
    // its owner told the tiles changed by whoever closed it, once the box shown in its place is on the board: told now,
    // the grid made for that box, not yet placed, would be forgotten as gone (PageApp #onTiles)
  }

  /**
   * A BOX SHOWN (a click on it): its grid made by the page's owner over its cube as the page has it now (openBox) --
   * its live inputs read as they are now -- in the results pane, the box shown before closed. Its source not opened
   * (refused, or the person gave up), the box stays a document, and why is said.
   */
  async #showBox(id: string, editor?: StepEditor): Promise<void> {
    // a Join box's settings: the Join window, its owner's to open over the page
    if (editor?.window === 'join') {
      const on = this.#sheetOf(id);
      if (on?.kind === 'flow') this.#options.onJoinBox?.(id, on.id);
      return;
    }
    // shown already: its settings, docked
    const shownGrid = this.#grids.get(id)?.added?.cube;
    if (shownGrid !== undefined) {
      if (editor !== undefined) shownGrid.openStep?.(editor);
      return;
    }
    const box = this.#boxes.get(id);
    const open = this.#options.openBox;
    const sheet = this.#sheetOf(id);
    if (box === undefined || open === undefined || sheet?.kind !== 'flow' || this.#showing.has(id)) return;
    // a cached box shown: copied again as its grid lands (below), the copy kept for it let go
    this.#letCacheGo(id);
    const document = box.document;
    this.#showing.add(id);
    let make: ((host: HTMLElement, options: SpawnOptions) => SpawnedGrid) | undefined;
    try {
      make = await open(id, document);
    } catch (e) {
      // its source not opened: drawn red, why said in its settings and its results, until it is shown
      const why = e instanceof Error ? e.message : String(e);
      sheet.board.refuse(id, why);
      sheet.board.note(`${this.title(id) ?? id}: ${why}`);
      return;
    } finally {
      this.#showing.delete(id);
    }
    // gone meanwhile, or shown another way: as it is
    if (make === undefined || this.#disposed || this.#boxes.get(id) !== box) return;
    // its cube changed while it opened (a step's): opened again over the page's now
    if (box.document !== document) {
      void this.#showBox(id, editor);
      return;
    }
    this.#planned.set(id, sheet);
    try {
      this.addGridOver(make, { id, ...(box.view.title !== undefined ? { title: box.view.title } : {}) });
    } finally {
      this.#planned.delete(id);
    }
    if (editor !== undefined) this.#grids.get(id)?.added?.cube.openStep?.(editor);
    if (sheet.board.layout.boxes.find((b) => b.tile === id)?.cache === true) this.#cacheShown(id, sheet.board);
  }

  /** A cached box shown (FL10): its rows copied into this page's database once they land -- its Snap -- a run waiting
   *  for it (#caching). Why it could not be, said on its flow. */
  #cacheShown(id: string, board: FlowBoard): void {
    const shown = this.#grids.get(id)?.added?.cube;
    if (shown?.snapNow === undefined) return;
    const snapping = (async () => {
      await this.#opening.get(id)?.catch(() => undefined);
      if (this.#grids.get(id)?.added?.cube === shown) await shown.snapNow!();
      // its box and its settings say it is copied
      board.repaint();
    })().catch((e: unknown) => board.note(`${this.title(id) ?? id}: its rows were not cached: ${e instanceof Error ? e.message : String(e)}`));
    this.#caching.set(id, snapping);
    void snapping.finally(() => { if (this.#caching.get(id) === snapping) this.#caching.delete(id); });
  }

  /**
   * THE COPIES THE PAGE KEEPS FOR ITS CACHED BOXES not shown (FL10), by box: each what the boxes after it read while it is
   * made of what it was when its copy was made -- one whose box is gone, no longer caches, or is made of something else
   * now let go here (FL11: a cache lasts its session, and is of what it was copied from).
   */
  cacheCopies(): ReadonlyMap<string, GridCopy> {
    this.staleCaches();
    return new Map([...this.#cacheKept].map(([id, kept]) => [id, kept.copy]));
  }

  /** The kept copies of cached boxes not shown that are of what their box is no more -- its box gone, no longer
   *  caching, or made of something else now (its chain: an upstream edit, a parameter's value) -- each let go, its box
   *  returned: what the page is told reads live again (page-app.ts), so no box after it reads the dropped copy. */
  staleCaches(): string[] {
    const gone: string[] = [];
    for (const [id, kept] of [...this.#cacheKept]) {
      const sheet = this.#sheetOf(id);
      const box = sheet?.kind === 'flow' ? sheet.board.layout.boxes.find((b) => b.tile === id) : undefined;
      if (box?.cache !== true || sheet?.kind !== 'flow' || sheet.board.chain(id) !== kept.key) {
        this.#letCacheGo(id);
        gone.push(id);
      }
    }
    return gone;
  }

  /** The copy kept for cached box `id` let go, if one is: its table dropped. */
  #letCacheGo(id: string): void {
    const kept = this.#cacheKept.get(id);
    if (kept === undefined) return;
    this.#cacheKept.delete(id);
    void kept.discard().catch(() => {});
  }

  /**
   * RUN (the canvas's button: docs/DATACUBE_FLOW_MODE_2026_10_24.md M5, M6): each box of flow `sheet` shown in turn, left
   * to right -- its grid made over the page as it is, its rows counted as they land -- then the box shown before shown
   * again. The screen's, no step of the page's: which box is shown is the screen's. A DRY RUN (`dry`, M6) counts and
   * judges every box as a run does and writes no Output: each says what it would write, or why it would not.
   *
   * IN STAGES (Block Until Done, M8: layout/flow.ts runStages): each stage's boxes shown, then its Outputs written --
   * none once a box that stops a run has failed, in this stage or before (FL8), or a Message has said an error -- before
   * the next stage's boxes are shown, so a box after a block reads what the Outputs before it wrote. A Message (M8) says
   * its words in the log when its rows are as it asks.
   */
  async #runFlow(sheetId: string, dry = false): Promise<void> {
    const sheet = this.#sheets.find((s) => s.id === sheetId);
    if (sheet?.kind !== 'flow' || sheet.board.running !== undefined) return;
    const board = sheet.board;
    // its boxes, not its comments, stage by stage
    const reads = (id: string): readonly string[] => {
      const doc = this.#boxes.get(id)?.document ?? this.documentOf(id);
      return doc === undefined ? [] : liveGrids(doc.source);
    };
    const stages = runStages(board.layout, reads, (id) => board.noteOf(id) === undefined);
    const order = stages.flat();
    const was = board.shown;
    const started = performance.now();
    type Logged = RunLog['boxes'][number];
    const logged: Logged[] = [];
    // a Message's error, said: what stops the run as a failing Test does
    const errors: { readonly id: string; readonly why: string }[] = [];
    let stopped: { readonly id: string; readonly why: string }[] = [];
    let done = 0;
    board.setRunning({ done: 0, of: order.length });
    try {
      for (const [k, stage] of stages.entries()) {
        // each Output of the stage written once its boxes have run: none when a box that stops a run fails (FL8's refuse)
        const writes: { readonly id: string; readonly name: string; readonly at: number }[] = [];
        for (const id of stage) {
          if (this.#disposed) return;
          if (board.layout.boxes.some((b) => b.tile === id)) {
            const t0 = performance.now();
            await this.#showBox(id);
            await this.#opening.get(id)?.catch(() => undefined);
            // a cached box's copy made before the box after it reads it (FL10)
            await this.#caching.get(id);
            // a Cross Tab's values its columns lack, asked as its rows land (FL3): known before any Output is written
            const shown = this.#grids.get(id)?.added?.cube;
            if (shown !== undefined) this.#askLacking(id, shown, board);
            await this.#lackingAsked.get(id);
            // its log: its rows, how long it took, or why it was refused
            const rows = board.count(id);
            const refused = board.refusal(id);
            const box = board.layout.boxes.find((b) => b.tile === id);
            // an Output writes its file after its stage's boxes have run, its rows counted now
            if (box?.output !== undefined && refused === undefined) writes.push({ id, name: outputWords(box.output), at: logged.length });
            // a Message: its words, when its rows are as it asks -- an error stopping the run, as it does when it cannot
            // be judged (its rows refused or not counted: a stop that cannot be judged stops, the review's B7)
            const said = box?.message !== undefined && refused === undefined ? messageSaid(box.message, rows) : undefined;
            if (said !== undefined && box?.message?.level === 'error') errors.push({ id, why: `${board.title(id) ?? id} said ${said}` });
            else if (box?.message?.level === 'error' && (refused !== undefined || (rows === undefined && box.message.when !== 'always'))) {
              errors.push({ id, why: `${board.title(id) ?? id} could not be judged${refused !== undefined ? ` (${refused})` : ''}` });
            }
            logged.push({ id, ms: Math.round(performance.now() - t0), ...(rows !== undefined ? { rows } : {}), ...(refused !== undefined ? { refused } : {}),
              ...(said !== undefined ? { said, level: box!.message!.level } : {}) });
          }
          done += 1;
          board.setRunning({ done, of: order.length });
        }
        // a box that stops a run, failing -- or never judged (its rows refused, its question unanswered: the review of
        // 2026-10-26's B7, a stop that cannot be judged stops) -- in this stage or before, or a Message's error: no Output
        // written from here on, each saying why
        stopped = [...runStoppers(board, stages.slice(0, k + 1).flat(), (id) => this.#lackingRefused.get(id)), ...errors];
        const stoppers = stopped.map((s) => s.why).join(', ');
        for (const w of writes) {
          if (this.#disposed) return;
          // a Dry Run writes none: each Output says what it would write, or why it would not
          if (dry) {
            logged[w.at] = stopped.length > 0 ? { ...logged[w.at]!, refused: `would not be written: ${stoppers}` }
              : { ...logged[w.at]!, wouldWrite: w.name };
            continue;
          }
          const why = stopped.length > 0 ? `not written: ${stoppers}` : await this.#writeBox(w.id);
          logged[w.at] = why === null ? { ...logged[w.at]!, wrote: w.name }
            : { ...logged[w.at]!, refused: stopped.length > 0 ? why : `could not write ${w.name}: ${why}` };
        }
      }
      if (was !== undefined && board.shown !== was && board.layout.boxes.some((b) => b.tile === was)) await this.#showBox(was);
      board.setLog({ at: new Date(), ms: Math.round(performance.now() - started), boxes: logged,
        ...(stopped.length > 0 ? { stopped: stopped.map((s) => s.id) } : {}), ...(stages.length > 1 ? { stages: stages.length } : {}),
        ...(dry ? { dry: true } : {}) });
    } finally {
      board.setRunning(undefined);
    }
  }

  /**
   * NEW BOX AFTER (docs/DATACUBE_FLOW_MODE_2026_10_24.md FL9): a gesture of box `id`'s menu -- a value filtered by, a
   * column sorted or grouped by -- made in a new box after it, as the palette's tool is added after a box (its owner's
   * onTool), the gesture that box's one step; the box shown keeps doing its one thing.
   */
  /** IMPORT PURE'S BOXES (FL12: pure-import.ts): each step of a Pure box's after its first, a Pure box after the one
   *  before -- made by the host as the palette's Pure is, in turn. The refusal, or null. */
  async #importAfter(id: string, steps: readonly Lambda[]): Promise<string | null> {
    const sheet = this.#sheetOf(id);
    if (sheet?.kind !== 'flow') return `${id} is no box of a flow`;
    if (this.#options.onTool === undefined || steps.length === 0) return 'this page makes no box';
    this.#options.onTool('pure', id, sheet.id, undefined, undefined, undefined, undefined, { pure: steps[0]!, next: steps.slice(1), from: id, made: [], label: 'Import Pure' });
    return null;
  }

  #stepAfter(id: string, gesture: NonNullable<FlowStep['gesture']>, label: string): void {
    const sheet = this.#sheetOf(id);
    if (sheet?.kind !== 'flow') return;
    const tool: FlowTool = gesture.name.startsWith('sort.') ? 'sort' : gesture.name.startsWith('pivot.') ? 'summarize' : 'filter';
    this.#options.onTool?.(tool, id, sheet.id, undefined, undefined, undefined, undefined, { gesture, label });
  }

  /**
   * A CROSS TAB'S VALUES ITS COLUMNS LACK (FL3), asked of the database once its rows land for what it is now -- once
   * for each thing it is (its chain), one question at a time -- and told the board, which marks the box and holds a
   * stopping one's run (FL8). A grid that pins nothing lacks nothing. A run waits for the question (#runFlow).
   */
  #askLacking(id: string, cube: SpawnedGrid, board: FlowBoard): void {
    if ((cube.snapshot.pivotValues ?? []).length === 0 || cube.lackingPivotValues === undefined) return;
    if (board.lacking(id) !== undefined || this.#lackingAsked.has(id)) return;
    this.#lackingRefused.delete(id);
    const asked = cube.lackingPivotValues()
      .then((values) => { if (!this.#disposed) board.setLacking(id, values); })
      // a question the database refused: its box not judged, which stops a run it stops (#runFlow), in these words
      .catch((e: unknown) => { this.#lackingRefused.set(id, e instanceof Error ? e.message : String(e)); })
      .finally(() => this.#lackingAsked.delete(id));
    this.#lackingAsked.set(id, asked);
  }

  /**
   * A DYNAMIC REPLACE'S RULES READ (dynamic-replace.ts): box `rules` shown, its rows as it hands them on read by the
   * database -- as many as a Dynamic Replace takes and one more, so a box of too many is said -- then box `id` shown
   * again, its window opened on them, or on why they could not be read. A flow shows one box at a time, as a Run shows
   * each in turn.
   */
  async #readRules(id: string, rules: string, expressions = false): Promise<void> {
    let read: { readonly rows?: ResultTable; readonly problem?: string };
    try {
      if (this.#grids.get(rules)?.added === undefined) {
        await this.#showBox(rules);
        await this.#opening.get(rules)?.catch(() => undefined);
      }
      const grid = this.#grids.get(rules)?.added?.cube;
      const on = this.#sheetOf(rules);
      read = grid?.handedOnRows === undefined
        ? { problem: (on?.kind === 'flow' ? on.board.refusal(rules) : undefined) ?? 'its rows could not be read' }
        : { rows: await grid.handedOnRows(MAX_RULES + 1) };
    } catch (e) {
      read = { problem: e instanceof Error ? e.message : String(e) };
    }
    if (this.#disposed || !this.#boxes.has(id)) return;
    await this.#showBox(id, { window: 'dynamicReplace', rules: { box: rules, ...read, ...(expressions ? { expressions } : {}) } });
  }

  /** A MACRO USED (M8, macros.ts): a Pure box of its function after box `after`, named after it -- made by the host as
   *  the palette's Pure is, as Import Pure's boxes are. */
  #useMacro(sheet: string, name: string, after: string): void {
    const macro = (this.#options.macros?.() ?? []).find((m) => m.name === name);
    if (macro === undefined) {
      // taken off meanwhile (another editor's step): said, as Python's use_macro refuses it
      const on = this.#sheets.find((s) => s.id === sheet);
      if (on?.kind === 'flow') on.board.note(`'${name}' is no macro of the page now`);
      return;
    }
    this.#options.onTool?.('pure', after, sheet, undefined, undefined, undefined, undefined, { pure: macro.pure, label: 'Use Macro', title: name });
  }

  /** A Macro asked for by its tool's name alone (the command palette): the page's one used after box `after`; none, or
   *  several, said -- the palette's Macro picks one. */
  #macroAsked(sheet: string, after: string | undefined): void {
    const macros = this.#options.macros?.() ?? [];
    const on = this.#sheets.find((s) => s.id === sheet);
    if (after !== undefined && macros.length === 1) this.#useMacro(sheet, macros[0]!.name, after);
    else if (on?.kind === 'flow') {
      on.board.note(macros.length === 0 ? "No macro yet: a box's menu, Save as Macro…"
        : after === undefined ? 'Show a box first: a macro is used after it' : "Pick one from the palette's Macro");
    }
  }

  /**
   * SAVE AS MACRO (M8, macros.ts): box `id`'s own steps and those of the boxes after box `from` up to it, upstream first,
   * each read as a function of its input's rows by its grid (ownFunction: a flow shows one box at a time, so each is
   * shown in turn, as a Run shows them), composed and kept as macro `name` -- one step, Save Macro; box `id` shown again.
   * The refusal, in words, or null.
   */
  async #saveMacro(id: string, from: string, name: string): Promise<string | null> {
    // the boxes from after `from` to `id`, each over the one before it
    const chain: string[] = [];
    for (let at: string | undefined = id; at !== from;) {
      if (at === undefined) return `its steps do not start after ${this.title(from) ?? from}: it is no box before it`;
      chain.unshift(at);
      if (chain.length > MACRO_BOXES) return `a macro is made of ${MACRO_BOXES} boxes at most`;
      const source: CubeSource | undefined = (this.#boxes.get(at)?.document ?? this.documentOf(at))?.source;
      // a box's own steps are its rows as it hands them on: one reading another port (a Filter's False, a Join's L or R)
      // reads rows no step of it says (the roadmap audit of 2026-10-26: its True rows were taken)
      if (source?._type === 'rows' && 'grid' in source.input && source.input.port !== undefined) {
        return `${this.title(at) ?? at} reads ${this.title(source.input.grid) ?? source.input.grid}'s ${source.input.port} rows: a macro's boxes each read the rows the one before hands on`;
      }
      at = source?._type === 'rows' && 'grid' in source.input ? source.input.grid : undefined;
    }
    const steps: Lambda[] = [];
    try {
      for (const box of chain) {
        if (this.#grids.get(box)?.added === undefined) {
          await this.#showBox(box);
          await this.#opening.get(box)?.catch(() => undefined);
        }
        const grid = this.#grids.get(box)?.added?.cube;
        const on = this.#sheetOf(box);
        if (grid?.ownFunction === undefined) return (on?.kind === 'flow' ? on.board.refusal(box) : undefined) ?? `${this.title(box) ?? box} could not be opened`;
        steps.push(await grid.ownFunction());
      }
    } catch (e) {
      return e instanceof Error ? e.message : String(e);
    } finally {
      if (!this.#disposed && this.#boxes.has(id) && this.#grids.get(id)?.added === undefined) await this.#showBox(id);
    }
    let pure: Lambda;
    try {
      pure = macroOf(steps);
    } catch (e) {
      if (e instanceof CubeRefusal) return e.message;
      throw e;
    }
    const problem = macroProblem({ name, pure }, (this.#options.macros?.() ?? []).filter((m) => m.name !== name));
    if (problem !== undefined) return problem;
    const told = this.#tell('Save Macro', pageCommand('setMacro', { name, pure }));
    this.#options.onChange();
    return told ? null : 'the page did not keep it';
  }

  /** AN OUTPUT WRITTEN (its Write now, a Run: docs/DATACUBE_FLOW_MODE_2026_10_24.md M6): the box shown, its grid made,
   *  every row it gives written to its file -- or why not, in words. */
  async #writeBox(tile: string): Promise<string | null> {
    const sheet = this.#sheetOf(tile);
    if (sheet?.kind !== 'flow') return `${tile} is no box of a flow`;
    const output = sheet.board.layout.boxes.find((b) => b.tile === tile)?.output;
    if (output === undefined) return `${tile} writes no file`;
    if (this.#grids.get(tile)?.added === undefined) {
      await this.#showBox(tile);
      await this.#opening.get(tile)?.catch(() => undefined);
    }
    const grid = this.#grids.get(tile)?.added?.cube;
    if (grid?.writeOutput === undefined) return sheet.board.refusal(tile) ?? 'its rows could not be read';
    try {
      await grid.writeOutput(output);
      return null;
    } catch (e) {
      return e instanceof Error ? e.message : String(e);
    }
  }

  /** A COMMENT ON FLOW `sheet` (the palette's Comment: docs/DATACUBE_FLOW_MODE_2026_10_24.md M7): a text tile, where it
   *  was let go (`at`) or below the boxes, one step -- its editor open on it at once. */
  #addComment(sheet: string, at?: FlowPoint): void {
    const id = this.#nextId('text');
    const told = this.#tell('Add Comment', pageCommand('addText', { id, sheet, text: 'A comment', title: null, columns: null }),
      ...(at !== undefined ? [pageCommand('placeBox', { tile: id, x: at.x, y: at.y })] : []));
    this.#options.onChange();
    if (told && this.#texts.has(id)) this.editText(id);
  }

  /** Where a thing made with no place goes on flow `sheet`: at its margin, below every box. */
  #belowFlow(sheet: string): FlowPoint {
    const s = this.#sheets.find((x) => x.id === sheet);
    const boxes = s?.kind === 'flow' ? s.board.layout.boxes : [];
    return { x: 40, y: boxes.length === 0 ? 40 : Math.max(...boxes.map((b) => b.y + (b.frame?.h ?? 60))) + 32 };
  }

  /** A CONTAINER (docs/DATACUBE_FLOW_MODE_2026_10_24.md M7, Alteryx's Tool Container): a comment of its name, framed at
   *  `at` -- one step, Add Container; double-click its head to name it. */
  #addContainer(sheet: string, at: FlowPoint, frame: Frame): void {
    const id = this.#nextId('text');
    this.#tell('Add Container', pageCommand('addText', { id, sheet, text: 'Container', title: null, columns: null }),
      pageCommand('placeBox', { tile: id, x: Math.round(at.x), y: Math.round(at.y), frame }));
    this.#options.onChange();
  }

  /** A BOX DUPLICATED (Ctrl+D, its menu: docs/DATACUBE_FLOW_MODE_2026_10_24.md M7): a copy of its cube as the page has
   *  it -- reading what it reads -- placed after the box it reads first, a document until it is shown, as every box not
   *  shown is (page-gestures.ts duplicateCommands). */
  #duplicateBox(sheet: string, tile: string): void {
    const document = this.#boxes.get(tile)?.document ?? this.documentOf(tile);
    if (document === undefined) return;
    // the one rule Python's grid.copy of a box duplicates by (page-gestures.ts duplicateCommands): its modes with it
    const on = this.#sheets.find((s) => s.id === sheet);
    const box = on?.kind === 'flow' ? on.board.layout.boxes.find((b) => b.tile === tile) : undefined;
    this.#tell('Duplicate Box', ...duplicateCommands(document, tile, sheet, null, box !== undefined ? { id: this.#nextId('grid'), box } : undefined));
    this.#options.onChange();
  }

  /**
   * BOXES COPIED (Ctrl+C on a box, or on the boxes picked together; Ctrl+X to cut them: docs/DATACUBE_FLOW_MODE_2026_10_24.md
   * M7): each one's cube as the page has it -- reading what it reads -- or a note's words, and its place, kept by the
   * page to paste on any of its flows, in the order they read each other; cut, taken off as their × takes them (a step of
   * its own). The page's alone: what a box reads is a grid of this page.
   */
  #copyBoxes(tiles: readonly string[], cut: boolean): void {
    const sheet = this.#sheetOf(tiles[0]!);
    if (sheet?.kind !== 'flow') return;
    const placeOf = new Map(sheet.board.layout.boxes.map((b) => [b.tile, b] as const));
    const items: ClipItem[] = [];
    for (const tile of tiles) {
      const at = placeOf.get(tile);
      if (at === undefined) continue;
      const text = this.#texts.get(tile);
      const document = text === undefined ? this.#boxes.get(tile)?.document ?? this.documentOf(tile) : undefined;
      if (text !== undefined) items.push({ tile, text: text.text, x: at.x, y: at.y });
      else if (document !== undefined) items.push({ tile, document, box: at, x: at.x, y: at.y });
    }
    if (items.length === 0) return;
    // a box before the boxes that read it, so a paste adds each after what it reads
    const copied = new Set(items.map((i) => i.tile));
    const ordered: ClipItem[] = [];
    const visit = (item: ClipItem, seen: ReadonlySet<string>): void => {
      if (ordered.includes(item) || seen.has(item.tile)) return;
      const reads = 'document' in item ? liveGrids(item.document.source).filter((g) => copied.has(g)) : [];
      for (const g of reads) visit(items.find((i) => i.tile === g)!, new Set([...seen, item.tile]));
      ordered.push(item);
    };
    for (const item of items) visit(item, new Set());
    this.#clipboard = ordered;
    const name = items.length === 1 ? sheet.board.title(items[0]!.tile) ?? items[0]!.tile : `${items.length} boxes`;
    if (cut) this.#removeTiles(items.map((i) => i.tile));
    sheet.board.say(`${cut ? 'Cut' : 'Copied'}: ${name}.`);
  }

  /**
   * WHAT WAS COPIED, PASTED on flow `sheet` (Ctrl+V), one step: each box a grid of its cube -- a box reading another of
   * them reading its pasted copy, any other box what it read -- and each note a comment of its words, placed as they
   * were placed together, the first of them at `at` (where the pointer is; none, a little down and right of where they
   * were). One box with nowhere given goes after the box it reads, as a box added does. A box whose input is no longer
   * on the page is refused, as any is.
   */
  #pasteBox(sheet: string, at: FlowPoint | undefined): void {
    const clip = this.#clipboard;
    if (clip === undefined || clip.length === 0) return;
    const taken = new Set<string>();
    const fresh = (kind: 'grid' | 'text'): string => {
      const id = this.#nextId(kind, taken);
      taken.add(id);
      return id;
    };
    const ids = new Map(clip.map((i) => [i.tile, fresh('text' in i ? 'text' : 'grid')] as const));
    const left = Math.min(...clip.map((i) => i.x));
    const top = Math.min(...clip.map((i) => i.y));
    const placed = (i: ClipItem): { x: number; y: number } | undefined => {
      if (at !== undefined) return { x: at.x + i.x - left, y: at.y + i.y - top };
      return clip.length === 1 ? undefined : { x: i.x + PASTE_OFFSET, y: i.y + PASTE_OFFSET };
    };
    const commands: PageCommand[] = [];
    for (const i of clip) {
      const id = ids.get(i.tile)!;
      if ('text' in i) commands.push(pageCommand('addText', { id, sheet, text: i.text, title: null, columns: null }));
      else {
        const source = relinked(i.document.source, (g) => ids.get(g) ?? g);
        commands.push(pageCommand('addGrid', {
          id, document: { ...i.document, source }, sheet, near: liveGrids(source)[0] ?? null, title: null, columns: null,
        }), ...settingsCommands(id, i.box));
      }
      const p = placed(i);
      if (p !== undefined) commands.push(pageCommand('placeBox', { tile: id, x: p.x, y: p.y }));
    }
    const only = clip.length === 1 ? clip[0]! : undefined;
    const told = this.#tell(only === undefined ? `Paste ${clip.length} Boxes` : 'text' in only ? 'Paste Comment' : 'Paste Box', ...commands);
    this.#options.onChange();
    // what was pasted picked together, so a drag moves it, Delete takes it off, Ctrl+C copies it again
    const on = this.#sheets.find((s) => s.id === sheet);
    if (told && on?.kind === 'flow') on.board.pick(ids.values());
  }

  /** Boxes taken off a flow together (Delete on the ones picked): one step, each removed as its × removes it -- a box
   *  reading one of them reading a copy of it from then on (FL6). */
  #removeTiles(tiles: readonly string[]): void {
    const saved = tiles.filter((t) => this.#saved(t));
    for (const t of tiles) if (!saved.includes(t)) this.#removeTile(t);
    if (saved.length === 1) this.#removeTile(saved[0]!);
    else if (saved.length > 1) this.#tell(`Remove ${saved.length} Boxes`, ...saved.map((tile) => pageCommand('removeTile', { tile })));
  }

  /** A box added from a flow's palette as `tool` (PageApp.addedAs): drawn as one until its grid does something. */
  boxAddedAs(id: string, tool: FlowTool): void {
    const sheet = this.#sheetOf(id);
    if (sheet?.kind === 'flow') sheet.board.addAs(id, tool);
  }

  /** A tile the step puts on another sheet than it is on, moved there as it is: a box between flows (shown, the flow's
   *  box shown; else a document still), any other tile between boards. */
  #moveOnto(id: string, from: Sheet, on: Sheet, title: string | undefined): void {
    const spec = this.#specs.get(id);
    if (on.kind === 'flow') {
      if (this.#boxes.has(id)) {
        from.board.remove(id);
        on.board.addBox(id, title ?? gridTitle(id));
      } else if (spec) {
        from.board.remove(id);
        this.#closeBox(on);
        on.board.add({ ...spec, title: title ?? spec.title });
      }
      return;
    }
    if (!spec) return;
    from.board.remove(id);
    on.board.add({ ...spec, title: title ?? spec.title });
  }

  /** Whether these tiles are all charts: a band a new chart joins (bands.ts add), beside its grid's other charts. */
  readonly #chartsOnly = (ids: readonly string[]): boolean => ids.every((t) => this.#charts.has(t));

  /** A saved view this page cannot open, kept in its place as its page saved it: a tile that says `why`. */
  placeUnavailable(view: PageView, why: string): void {
    this.#unavailable.set(view.id, view);
    if (view.kind === 'chart' && !this.#came.has(view.id)) this.#came.set(view.id, this.#cameNext++);
    const body = this.#doc.createElement('div');
    body.className = 'dc-tile-unavailable';
    body.textContent = why;
    this.#place({ id: view.id, title: view.kind === 'chart' ? view.title : view.title ?? gridTitle(view.id), element: body, actions: [] });
  }

  /** The charts in the order they came, those it could not open among them. */
  #chartIds(): string[] {
    const lost = [...this.#unavailable.values()].filter((v) => v.kind === 'chart').map((v) => v.id);
    if (lost.length === 0) return [...this.#charts.keys()];
    const at = (id: string): number => this.#came.get(id) ?? Number.MAX_SAFE_INTEGER;
    return [...this.#charts.keys(), ...lost].sort((x, y) => at(x) - at(y));
  }

  /**
   * A tile it could not open taken off, as a step leaves it (`sync`): what the step did to the rest -- a grid's charts
   * gone with it, a frozen one's selection cleared -- is the step's own views, which `sync` takes as they are (the S7
   * audit's P-8: a copy of removeTile's rule was here).
   */
  #dropUnavailable(id: string): void {
    if (!this.#unavailable.delete(id)) return;
    this.#take(id);
  }

  /** A tile off its sheet (its element detached, not destroyed). */
  #take(id: string): void {
    this.#sheetOf(id)?.board.remove(id);
    this.#specs.delete(id);
  }

  /** A tile brought into view: its sheet shown, then the tile scrolled to. */
  #reveal(id: string): void {
    const sheet = this.#sheetOf(id);
    if (!sheet) return;
    this.showSheet(sheet.id);
    sheet.board.reveal(id);
  }

  // -- sheets ------------------------------------------------------------------------------------------------------

  /** The sheets, in order. */
  get sheets(): readonly SheetInfo[] {
    return this.#sheets.map((s) => ({
      id: s.id, ...(s.name !== undefined ? { name: s.name } : {}), tiles: sheetTiles(s), ...(s.kind === 'flow' ? { flow: true as const } : {}),
    }));
  }

  /** The sheet shown. */
  get shownSheet(): string {
    return this.#shown.id;
  }

  /** Said by the sheet shown, to a screen reader (an undo: what it undid). */
  say(text: string): void {
    this.#shown.board.say(text);
  }

  /**
   * THE PAGE PUT AT A STEP OF ITS HISTORY (page-state.ts; docs/DATACUBE_PAGE_STATE_2026_10_18.md): from `from`, the page
   * as its state last kept it, to `to` -- each change made as the page's own edits make it, and told as none. A tile
   * neither page holds (a chart with no spec yet) is drawn where the board draws it (`#drawn`). A grid `to` has and the
   * screen does not is the one a gesture made for it, else made by `remake` (its host's maker, starting where it was
   * when it went). A chart reads the grid the step says (a removal undone, a Show Grid). A chart's selection is drawn as
   * the step has it; a grid's filter that lost it meanwhile is its owner's to put back (G8). The board left empty by the
   * step is said so once it has done.
   */
  sync(to: PageViews & { readonly cubes?: PageDocument['cubes'] }, from: PageViews, remake: Remake,
    unopened: (cube: string) => string | undefined = () => undefined, theirs = false, refilter = true): void {
    this.#syncing = true;
    try {
      this.#synced(to, from, remake, unopened, theirs, refilter);
    } finally {
      this.#syncing = false;
    }
    if (this.#emptied) {
      this.#emptied = false;
      if (this.#isEmpty()) this.#options.onEmpty();
    }
  }

  #synced(to: PageViews & { readonly cubes?: PageDocument['cubes'] }, from: PageViews, remake: Remake,
    unopened: (cube: string) => string | undefined, theirs: boolean, refilter = true): void {
    const wanted = new Map(to.views.map((v) => [v.id, v]));
    const known = new Set(from.views.map((v) => v.id));
    const sheetFor = new Map(to.sheets.flatMap((s) => layoutTilesOf(s.layout).map((t) => [t, s.id] as const)));
    const sheetsBefore = new Set(this.#sheets.map((s) => s.id));
    const sheetOrder = this.#sheets.map((s) => s.id);
    const board = (id: string | undefined): Sheet | undefined => this.#sheets.find((s) => s.id === id);
    this.#quietly(() => {
      // sheets it has and the screen does not
      for (const s of to.sheets) if (!board(s.id)) this.#newSheet(s.id, s.name, s.layout.kind === 'flow');
      // tiles a gesture made, placed where their command put them (a grid's engine, a chart's panel: made already)
      for (const [id, spec] of [...this.#pending]) {
        if (!wanted.has(id)) continue;
        const on = board(sheetFor.get(id));
        if (on) this.#planned.set(id, on);
        this.#pending.delete(id);
        this.#place(spec);
      }
      // tiles the page held and the step does not: charts first (a grid would take its following ones, or detach them)
      for (const id of [...this.#charts.keys()]) if (known.has(id) && !wanted.has(id)) this.#dropChart(id, theirs, refilter);
      for (const id of [...this.#grids.keys()]) if (known.has(id) && !wanted.has(id)) this.#dropGrid(id);
      for (const id of [...this.#unavailable.keys()]) if (known.has(id) && !wanted.has(id)) this.#dropUnavailable(id);
      for (const id of [...this.#texts.keys()]) if (known.has(id) && !wanted.has(id)) this.#dropText(id);
      for (const id of [...this.#series.keys()]) if (known.has(id) && !wanted.has(id)) this.#dropTimeseries(id);
      for (const id of [...this.#boxes.keys()]) if (!wanted.has(id)) this.#dropBox(id);
      // a flow's boxes it has: each a document as the step has it, no grid made for it (the one shown keeps its own)
      for (const view of to.views) {
        if (view.kind !== 'grid' || this.#grids.has(view.id)) continue;
        const on = board(sheetFor.get(view.id));
        if (on?.kind !== 'flow') continue;
        const document = to.cubes?.find((c) => c.id === view.cube)?.cube ?? this.#boxes.get(view.id)?.document;
        if (document === undefined) throw new Error(`box ${view.id} has no cube of the page's to be drawn from`);
        this.#putBox(on, view, document);
      }
      // text tiles it has and the screen does not, each on its sheet
      for (const view of to.views) {
        if (view.kind !== 'text' || this.#texts.has(view.id)) continue;
        const on = board(sheetFor.get(view.id));
        if (on) this.#planned.set(view.id, on);
        this.#placeText(view);
      }
      // a cube a timeseries tile reads that no grid of the page shows (its grid removed while the tile read it: the
      // review's G4) kept off the board for it, made as the page `to` holds it -- as a detached chart's is, below
      for (const view of to.views) {
        if (view.kind !== 'timeseries') continue;
        for (const { cube } of view.spec.sources) {
          if (this.#grids.has(cube) || this.#kept.has(cube) || to.views.some((v) => v.kind === 'grid' && v.cube === cube)) continue;
          const make = remake(cube, to.cubes?.find((c) => c.id === cube)?.cube);
          if (!make) throw new Error(`timeseries ${view.id} cannot be drawn again: grid ${cube}'s maker is gone`);
          this.keepGrid(cube, make);
        }
      }
      // timeseries tiles it has and the screen does not, each on its sheet (each reads its grids when it draws)
      for (const view of to.views) {
        if (view.kind !== 'timeseries' || this.#series.has(view.id)) continue;
        const on = board(sheetFor.get(view.id));
        if (on) this.#planned.set(view.id, on);
        this.#placeTimeseries(view);
      }
      // what it could not open, back: kept as the step has it, in its place
      for (const view of to.views) {
        if (view.kind === 'text' || view.kind === 'timeseries' || this.#boxes.has(view.id)) continue;
        if (this.#unavailable.has(view.id) || this.#grids.has(view.id) || this.#charts.has(view.id)) {
          if (this.#unavailable.has(view.id)) this.#unavailable.set(view.id, view);
          continue;
        }
        const why = unopened(view.cube);
        if (why === undefined) continue;
        const on = board(sheetFor.get(view.id));
        if (on) this.#planned.set(view.id, on);
        this.placeUnavailable(view, why);
      }
      // grids it has and the screen does not: made again, each on its sheet; one kept off the board for a detached chart
      // is replaced by the one made again, and its detached charts follow it once more
      for (const view of to.views) {
        if (view.kind !== 'grid' || this.#grids.has(view.id) || this.#unavailable.has(view.id) || this.#boxes.has(view.id)) continue;
        // from the cube the page `to` holds for it, when it says one (a keeper's step or put-back: the grid as the page
        // has it, not as this screen last saw it -- a grid made again under the id its keeper gave it started from the
        // screen's state, its sent commands then applied twice: the D19 audit's fourth round, 3)
        const make = remake(view.id, to.cubes?.find((c) => c.id === view.cube)?.cube);
        if (!make) throw new Error(`grid ${view.id} cannot be made again: its maker is gone`);
        const kept = this.#kept.get(view.id);
        this.#kept.delete(view.id);
        const on = board(sheetFor.get(view.id));
        if (on) this.#planned.set(view.id, on);
        this.addGridOver(make, { id: view.id, ...(view.title !== undefined ? { title: view.title } : {}) });
        for (const chart of this.#charts.values()) {
          if (chart.link.detached?.from !== view.id) continue;
          chart.link.grid = view.id;
          delete chart.link.detached;
          chart.paint();
        }
        kept?.dispose();
      }
      // charts it has and the screen does not: opened as a saved page's are, on their sheets -- a detached one's grid
      // kept off the board made again for it -- each selection put back on its grid's filter
      for (const view of to.views) {
        if (view.kind !== 'chart' || this.#charts.has(view.id) || this.#unavailable.has(view.id)) continue;
        if (!this.#grids.has(view.cube) && !this.#kept.has(view.cube)) {
          const make = remake(view.cube, to.cubes?.find((c) => c.id === view.cube)?.cube);
          if (!make) throw new Error(`chart ${view.id} cannot be opened again: grid ${view.cube}'s maker is gone`);
          this.keepGrid(view.cube, make);
        }
        const on = board(sheetFor.get(view.id));
        if (on) this.#planned.set(view.id, on);
        this.openChart(view, view.cube);
      }
      // what it says of the tiles both have: each one's title, its spec, its sheet
      for (const view of to.views) {
        const sheet = this.#sheetOf(view.id);
        if (!sheet) continue;
        const title = view.kind === 'grid' ? view.title ?? this.#titles.get(view.id)
          : view.kind === 'text' ? view.title ?? textTitle(view.id)
            : view.kind === 'timeseries' ? view.title ?? timeseriesTitle(view.id) : view.title;
        if (title !== undefined && sheet.board.title(view.id) !== title) sheet.board.rename(view.id, title);
        // a text tile's words and files as the step has them, drawn unless its editor is open
        const text = this.#texts.get(view.id);
        if (text && view.kind === 'text') {
          const files = view.files ?? [];
          const changed = text.text !== view.text || JSON.stringify(text.files) !== JSON.stringify(files);
          text.text = view.text;
          text.files = files;
          if (text.shown !== null && !files.some((f) => f.sha256 === text.shown)) text.shown = null;
          if (changed && !text.editing) this.#drawText(view.id);
        }
        // a timeseries tile's spec as the step has it: drawn from the rows it has, or asked again (its viewer's rule)
        const series = this.#series.get(view.id);
        if (series && view.kind === 'timeseries' && JSON.stringify(series.spec) !== JSON.stringify(view.spec)) {
          series.spec = view.spec;
          series.view?.update(view.spec);
        }
        const chart = this.#charts.get(view.id);
        if (chart && view.kind === 'chart') {
          // reading another grid there than here (a grid's removal undone; a Show Grid, or one undone or redone): it reads
          // that grid
          if (chart.link.grid !== view.cube && this.#grids.has(view.cube)) {
            // moved off a grid with a selection (a followGrid): off that grid as the step took it off
            const moved = chart.link.grid !== null && chart.conditions.length > 0;
            if (moved) {
              this.#sourceOf(chart.link).unselected(chart.conditions, theirs);
              chart.conditions = [];
            }
            chart.link.grid = view.cube;
            delete chart.link.detached;
            chart.paint();
            if (moved) this.#paintSelection(view.id);
          }
          if (JSON.stringify(chart.panel.spec) !== JSON.stringify(view.spec)) {
            chart.told = JSON.stringify(view.spec);
            chart.panel.setSpec(view.spec);
            chart.paint();
          }
          // its selection as the step has it (a frozen chart's, cleared when its grid went, comes back with it): a grid's
          // filter that lost it meanwhile is its owner's to put back, by that grid's command (G8)
          const selection = view.selection ?? [];
          if (chart.link.grid !== null && JSON.stringify(selection) !== JSON.stringify(chart.conditions)) {
            chart.conditions = [...selection];
            this.#paintSelection(view.id);
          }
        }
        const on = board(sheetFor.get(view.id));
        if (on && on !== sheet) this.#moveOnto(view.id, sheet, on, title);
      }
      // sheets it does not have, with what is left on them (no tile it holds: a moment's work)
      for (const sheet of [...this.#sheets]) {
        if (to.sheets.some((s) => s.id === sheet.id)) continue;
        const on = (): string[] => sheetTiles(sheet);
        for (const tile of on().filter((t) => this.#grids.has(t))) this.#drop(tile);
        for (const tile of on()) this.#drop(tile);
        if (sheet === this.#shown) {
          // the sheet after it, else the one before it, as Delete Sheet leaves the tabs
          const at = sheetOrder.indexOf(sheet.id);
          const kept = (id: string): boolean => to.sheets.some((s) => s.id === id);
          const next = sheetOrder.slice(at + 1).find(kept) ?? sheetOrder.slice(0, at).reverse().find(kept) ?? to.sheets[0]!.id;
          sheet.host.hidden = true;
          this.#shown = board(next)!;
          this.#shown.host.hidden = false;
        }
        this.#sheets.splice(this.#sheets.indexOf(sheet), 1);
        this.#pageLayouts.delete(sheet.id);
        sheet.board.dispose();
        sheet.host.remove();
      }
      // the sheets' names and order, and each one's layout
      for (const s of to.sheets) board(s.id)!.name = s.name;
      this.#sheets.sort((a, b) => to.sheets.findIndex((s) => s.id === a.id) - to.sheets.findIndex((s) => s.id === b.id));
      // the boards' elements in the sheets' order -- moved only when it changed: a moved element loses its focus
      const shown = [...this.#options.host.children].filter((el) => this.#sheets.some((s) => s.host === el));
      if (shown.some((el, i) => el !== this.#sheets[i]!.host)) this.#options.host.append(...this.#sheets.map((s) => s.host));
      for (const s of to.sheets) {
        const sheet = board(s.id)!;
        // a sheet is a board or a flow from its making to its removal (page-commands.ts layout)
        if (sheet.kind !== s.layout.kind) throw new Error(`sheet ${s.id} is a ${sheet.kind === 'flow' ? 'flow' : 'board'} here, not in the step`);
        if (sheet.kind === 'flow' || s.layout.kind === 'flow') {
          const { kind: _flow, ...flow } = s.layout.kind === 'flow' ? s.layout : { kind: 'flow' as const, ...EMPTY_FLOW };
          if (sheet.kind === 'flow' && JSON.stringify(flow) !== JSON.stringify(sheet.board.layout)) sheet.board.setLayout(flow);
          continue;
        }
        const { kind: _kind, ...bands } = s.layout;
        // drawn again only when it moves: a drag in progress on a sheet the step leaves as it is goes on (the S3 audit's
        // 11)
        const drawn = this.#drawn(sheet, bands);
        if (JSON.stringify(drawn) !== JSON.stringify(sheet.board.layout)) sheet.board.setLayout(drawn);
        // the page's: what the step holds, of the tiles on the board
        this.#pageLayouts.set(s.id, without(bands, (id) => sheet.board.title(id) === undefined));
      }
      // the charts in its order (views() writes them in the order they came: one opened again comes where it was)
      const order = (id: string): number => (wanted.has(id) ? to.views.findIndex((v) => v.id === id) : to.views.length);
      const charts = [...this.#charts].sort(([x], [y]) => order(x) - order(y));
      this.#charts.clear();
      for (const [id, chart] of charts) this.#charts.set(id, chart);
      const came = this.#chartIds().sort((x, y) => order(x) - order(y));
      came.forEach((id, i) => this.#came.set(id, i));
      this.#cameNext = came.length;
      this.#planned.clear();
    });
    // each flow drawn from its documents as the step has them; a box shown whose source the step changed (a link to a
    // box removed, frozen: FL6) shown again over it, its grid made anew
    for (const sheet of this.#sheets) {
      if (sheet.kind !== 'flow') continue;
      const shown = sheet.board.shown;
      const cube = shown === undefined ? undefined : to.cubes?.find((c) => c.id === wanted.get(shown)?.cube)?.cube;
      const now = shown === undefined ? undefined : this.documentOf(shown);
      if (shown !== undefined && cube !== undefined && now !== undefined && !sameValue(cube.source, now.source)) {
        this.#closeBox(sheet, cube);
        void this.#showBox(shown);
      }
      sheet.board.repaint();
    }
    // a sheet brought back is shown: where the change is
    const back = this.#sheets.find((s) => !sheetsBefore.has(s.id));
    if (back) this.showSheet(back.id);
    this.#drawShownSeries();
    this.#releaseKept();
    for (const tile of this.#series.values()) tile.paint();
    this.#options.onSheets?.();
    this.#options.onTiles?.();
    this.#options.onChange();
  }

  /** The sheet a tile is on. */
  sheetOf(tile: string): string | undefined {
    return this.#sheetOf(tile)?.id;
  }

  /** Show another sheet: its board drawn, the one shown before hidden (its grids still running). */
  showSheet(id: string): void {
    const next = this.#sheets.find((s) => s.id === id);
    if (!next || next === this.#shown) return;
    this.#menu.close();
    this.#picker?.close();
    this.#shown.host.hidden = true;
    next.host.hidden = false;
    this.#shown = next;
    this.#drawShownSeries();
    this.#options.onSheets?.();
  }

  /** A sheet after the last, shown (`sync` shows a sheet that comes). */
  addSheet(): string {
    const id = this.#nextId('sheet');
    this.#tell('Add Sheet', pageCommand('addSheet', { id, name: null }));
    return id;
  }

  /** A FLOW SHEET after the last, shown (docs/DATACUBE_FLOW_MODE_2026_10_24.md FL4): an empty canvas, its palette beside
   *  it. */
  addFlow(): string {
    const id = this.#nextId('sheet');
    this.#tell('Add Flow', pageCommand('addFlow', { id, name: null }));
    return id;
  }

  /** A sheet's name: its own (trimmed), or -- empty or undefined -- named again after what it shows. */
  renameSheet(id: string, name: string | undefined): void {
    const sheet = this.#sheets.find((s) => s.id === id);
    if (!sheet) return;
    // the names' one rule (page-gestures.ts renameSheetCommand, page-names.ts givenName): trimmed, a blank none of its
    // own (named after its first grid); none, the name it has
    const command = renameSheetCommand(id, name, sheet.name, this.#options.sheetLabel?.(id));
    if (command !== null) this.#tell('Rename Sheet', command);
  }

  /** A sheet moved to `to` in the order (counted with it taken out). */
  moveSheet(id: string, to: number): void {
    const at = this.#sheets.findIndex((s) => s.id === id);
    if (at < 0) return;
    const command = moveSheetCommand(id, at, to, this.#sheets.length);
    if (command !== null) this.#tell('Move Sheet', command);
  }

  /**
   * A sheet deleted, and its tiles with it -- each as its own remove button would (a grid's following charts go, on
   * whichever sheet; its frozen ones stay, detached). The last sheet stays: a page has one at least.
   */
  removeSheet(id: string): void {
    const sheet = this.#sheets.find((s) => s.id === id);
    if (!sheet || this.#sheets.length === 1) return;
    this.#tell('Delete Sheet', pageCommand('removeSheet', { sheet: id }));
  }

  /**
   * MOVE TO SHEET: a tile onto another sheet (at its bottom), or -- null -- onto a new one after the last. The sheet
   * shown stays shown; the tile's own sheet says where it went.
   */
  moveToSheet(tile: string, to: string | null): void {
    // a locked page: nothing moves (the design's §7.2)
    if (!this.#editing) return;
    const from = this.#sheetOf(tile);
    const spec = this.#specs.get(tile);
    if (!from || !spec) return;
    const target = to === null ? undefined : this.#sheets.find((s) => s.id === to);
    if (to !== null && (!target || target === from)) return;
    const shown = this.#shown.id;
    const title = from.board.title(tile) ?? spec.title;
    // one step: a new sheet and the tile onto it -- the sheet shown stays shown (`sync` shows a sheet that comes)
    const sheet = to ?? this.#nextId('sheet');
    const steps = moveToSheetCommands(tile, sheet, to === null, target?.board.columns ?? null, from.kind === 'flow');
    if (this.#saved(tile)) {
      this.#tell('Move to Sheet', ...steps);
    } else {
      // a tile the page does not save (a chart with no spec yet): its new sheet the page's, the move the board's alone
      const added = steps.filter((c) => c.kind === 'addSheet' || c.kind === 'addFlow');
      if (added.length > 0) this.#tell('Move to Sheet', ...added);
      const onto = this.#sheets.find((s) => s.id === sheet);
      if (!onto) return;
      from.board.remove(tile);
      onto.board.add({ ...spec, title });
      this.#options.onTiles?.();
    }
    if (this.#shown.id !== shown) this.showSheet(shown);
    if (this.#sheetOf(tile) !== from) from.board.say(`${title} moved to ${this.#options.sheetLabel?.(sheet) ?? sheet}.`);
  }

  /**
   * A SAVED PAGE'S SHEETS, made before its tiles are added (a page of its own adds its grids first, `restore` then
   * puts its charts and layouts): each in its saved order, under its saved id and name, and each tile then added goes
   * on the sheet it was saved on. A sheet the page has and the saved one does not, left empty, goes.
   */
  setSheets(saved: readonly PageSheet[]): void {
    for (const s of saved) {
      const flow = s.layout.kind === 'flow';
      let sheet = this.#sheets.find((x) => x.id === s.id);
      // a sheet of the other kind under its id (a board's first sheet, the saved page's a flow): made again, as its
      // saved one is, while nothing is on it
      if (sheet !== undefined && (sheet.kind === 'flow') !== flow) {
        if (sheet.board.size > 0) throw new Error(`sheet ${s.id} holds tiles: it is not made a ${flow ? 'flow' : 'board'}`);
        const at = this.#sheets.indexOf(sheet);
        const was = sheet;
        sheet.board.dispose();
        sheet.host.remove();
        this.#sheets.splice(at, 1);
        this.#pageLayouts.delete(sheet.id);
        sheet = this.#newSheet(s.id, undefined, flow);
        if (this.#shown === was) {
          this.#shown = sheet;
          sheet.host.hidden = false;
        }
      }
      sheet ??= this.#newSheet(s.id, undefined, flow);
      sheet.name = s.name;
      for (const tile of layoutTilesOf(s.layout)) this.#planned.set(tile, sheet);
    }
    // a tile already on the page (a cube's own grid) onto its saved sheet
    for (const [tile, sheet] of this.#planned) {
      const on = this.#sheetOf(tile);
      if (on && on !== sheet) this.#moveOnto(tile, on, sheet, on.board.title(tile));
    }
    const ids = new Set(saved.map((s) => s.id));
    for (const sheet of [...this.#sheets]) {
      if (ids.has(sheet.id) || sheet.board.size > 0) continue;
      if (sheet === this.#shown) {
        sheet.host.hidden = true;
        this.#shown = this.#sheets.find((x) => ids.has(x.id))!;
        this.#shown.host.hidden = false;
      }
      this.#sheets.splice(this.#sheets.indexOf(sheet), 1);
      this.#pageLayouts.delete(sheet.id);
      sheet.board.dispose();
      sheet.host.remove();
    }
    this.#sheets.sort((a, b) => saved.findIndex((s) => s.id === a.id) - saved.findIndex((s) => s.id === b.id));
    this.#options.host.append(...this.#sheets.map((s) => s.host));
  }

  /** The grids on the board, by tile id, each with its cube (none for a cube's own grid: it is the host's). */
  get grids(): ReadonlyMap<string, SpawnedGrid | undefined> {
    return new Map([...this.#grids].map(([id, g]) => [id, g.added?.cube]));
  }

  /** A flow's boxes not shown, by their cubes: each one's cube as the page has it -- what the page writes for them, no
   *  grid saying it (PageApp #seen). */
  get boxes(): ReadonlyMap<string, CubeDocument> {
    return new Map([...this.#boxes.values()].map((b) => [b.view.cube, b.document]));
  }

  /** The grids kept off the board for the frozen charts that still read them. */
  get kept(): ReadonlyMap<string, SpawnedGrid> {
    return this.#kept;
  }

  /** Every tile on the page, sheet by sheet, each sheet's in reading order. */
  get tileIds(): readonly string[] {
    return this.#sheets.flatMap(sheetTiles);
  }

  /** A tile's title, as shown (renamed or not). */
  title(id: string): string | undefined {
    return this.#sheetOf(id)?.board.title(id);
  }

  /** The title a person gave a tile; undefined while it has the one it was made with (as views() saves none). */
  givenTitle(id: string): string | undefined {
    const title = this.title(id);
    return title !== undefined && title !== this.#titles.get(id) ? title : undefined;
  }

  /**
   * A LONE GRID IN THE PAGE'S BAR (a page of its own; the design's §3.1): while the board holds one tile and it is a
   * grid, that tile is drawn without its frame and its header's part -- its source, its pill, its menu -- moves into
   * `slot`, the right of the page's bar; otherwise (or `slot` null) every header is its tile's.
   */
  showAlone(slot: HTMLElement | null, stateSlot: HTMLElement | null = null): void {
    // the sheet shown: the others' tiles are each in its own frame
    for (const sheet of this.#sheets) if (sheet !== this.#shown && sheet.kind === 'bands') sheet.board.setAlone(null, null);
    // a flow's box is never alone: its canvas is the sheet
    const shown = this.#shown;
    if (shown.kind === 'flow') return;
    const ids = tiles(shown.board.layout);
    const lone = slot !== null && ids.length === 1 && this.#grids.has(ids[0]!) ? ids[0]! : null;
    shown.board.setAlone(lone, slot, stateSlot);
  }

  /**
   * The layouts (ui/layout-picker.ts) by `anchor`, each arranging every tile on the page: from Arrange... in reading
   * order, or -- `first`, from a tile's own layout button -- with that tile in the first slot (the user, 2026-10-09: a
   * tile's layouts arranging only its band showed two tiles of four, and 2 x 2 came out 2, 1, 1).
   */
  showLayouts(anchor: HTMLElement, first?: string): void {
    // a locked page offers no layouts: nothing moves by accident; a flow's boxes are placed on its canvas
    if (!this.#editing || this.#shown.kind === 'flow') return;
    if (this.#picker) {
      this.#showPicker(this.#picker, anchor, first);
      return;
    }
    void lazily(() => import('../ui/layout-picker.ts')).then(({ LayoutPicker }) => {
      if (this.#disposed) return;
      this.#picker ??= new LayoutPicker(this.#options.host);
      this.#showPicker(this.#picker, anchor, first);
    });
  }

  #showPicker(picker: LayoutPicker, anchor: HTMLElement, first?: string): void {
    // the sheet shown: its tiles, arranged on it
    const shown = this.#shown;
    if (shown.kind === 'flow') return;
    const board = shown.board;
    picker.show(anchor, {
      // its places: a stack of tiles is one
      tiles: places(board.layout),
      ...(first !== undefined ? { first } : {}),
      fit: board.layout.fit,
      onPreview: (preset) => board.preview(preset, first),
      onPick: (preset) => board.arrange(preset, first),
      onFit: (fit) => board.setFit(fit),
      onEvenOut: () => board.evenOut(),
    });
  }

  /**
   * The last arrangement undone (Undo Layout; Ctrl+Z on a tile's frame): the page's tiles placed as they were before
   * it. A tile added or removed since is not brought back or taken away: only where the tiles are is undone.
   */
  undoLayout(): void {
    const shown = this.#shown;
    if (shown.kind === 'flow') return;
    const { board, steps } = shown;
    const before = steps.undone.at(-1);
    if (before === undefined || !this.#editing) return;
    steps.undone = steps.undone.slice(0, -1);
    steps.redone = [...steps.redone, board.layout];
    board.setLayout(before);
    this.#tellLayout(shown);
    board.say('Layout change undone.');
    this.#options.onChange();
  }

  redoLayout(): void {
    const shown = this.#shown;
    if (shown.kind === 'flow') return;
    const { board, steps } = shown;
    const after = steps.redone.at(-1);
    if (after === undefined || !this.#editing) return;
    steps.redone = steps.redone.slice(0, -1);
    steps.undone = [...steps.undone, board.layout];
    board.setLayout(after);
    this.#tellLayout(shown);
    board.say('Layout change redone.');
    this.#options.onChange();
  }

  /** Whether Undo Layout would do something: a step to undo on the sheet shown, on a page that is not locked. */
  get canUndoLayout(): boolean {
    return this.#shown.kind === 'bands' && this.#shown.steps.undone.length > 0 && this.#editing;
  }

  get canRedoLayout(): boolean {
    return this.#shown.kind === 'bands' && this.#shown.steps.redone.length > 0 && this.#editing;
  }

  /** Edit mode (tiles move, dividers drag) or view mode (nothing moves by accident): every sheet alike. */
  get editing(): boolean {
    return this.#editing;
  }

  setEditing(editing: boolean): void {
    this.#editing = editing;
    for (const sheet of this.#sheets) sheet.board.setEditing(editing);
    this.#options.onSheets?.();
  }

  /** How many tiles are on the page besides the cube's own grid: charts and added grids. */
  get charts(): number {
    return this.#charts.size + this.#grids.size + this.#boxes.size - (this.#options.grid ? 1 : 0);
  }

  /** How many text tiles it holds. */
  get texts(): number {
    return this.#texts.size;
  }

  /** How many timeseries tiles it holds. */
  get timeseries(): number {
    return this.#series.size;
  }

  /**
   * ANOTHER GRID on the page: a cube of its own over the same source, starting as `from` is now,
   * in a tile like any chart's -- moved, resized, renamed and removed the same way -- with charts
   * of its own. Removing it removes the charts that follow it; its frozen charts stay, DETACHED:
   * each keeps its own copy of the grid's query.
   */
  addGrid(from: string = GRID): string {
    const source = this.#grids.get(from)?.source;
    if (!source) throw new Error(`no grid ${from} on the page to copy`);
    return this.addGridOver(this.#gridLike(from, source, source.snapshot), { near: from });
  }

  /** A grid like `from` (over `source`), starting at `snapshot`: made by the page's owner when it makes them. */
  #gridLike(from: string, source: ChartSource, snapshot: CubeSnapshot, configuration?: CubeConfiguration):
    (host: HTMLElement, options: SpawnOptions) => SpawnedGrid {
    return this.#options.gridLike?.(from, snapshot, configuration)
      ?? ((host, options) => source.spawn(host, snapshot, { ...options, ...(configuration ? { configuration } : {}) }));
  }

  /**
   * A grid over ANOTHER SOURCE (New ▸ Source…): `make` builds it -- its own engine and planner,
   * over its own model -- in the tile's element. Then it is a grid like any other: its own charts,
   * its own New ▸ Grid (over its source), moved and removed the same way. It goes beside `near`, the grid it was asked
   * from, while that band has room (else at the bottom). A saved page's grid comes back under its own `id` and `title`.
   */
  addGridOver(
    make: (host: HTMLElement, options: SpawnOptions) => SpawnedGrid,
    how: {
      readonly near?: string; readonly id?: string; readonly title?: string;
      /** The step it is part of (Show Grid): its name, and the commands told with its addGrid, before and after it. */
      readonly label?: string; readonly before?: readonly PageCommand[]; readonly after?: (id: string) => readonly PageCommand[];
    } = {},
  ): string {
    const id = how.id ?? this.#nextId('grid');
    if (this.#grids.has(id) || this.#charts.has(id) || this.#kept.has(id)) throw new Error(`a tile ${id} is already on the page`);
    const host = this.#doc.createElement('div');
    host.className = 'dc-grid-tile';
    // a flow's box: the windows that set it dock in its flow's settings pane
    const lands = this.#planned.get(id) ?? (how.near !== undefined ? this.#sheetOf(how.near) : undefined) ?? this.#shown;
    const cube = make(host, { ...this.#spawnOptions(id), ...(lands.kind === 'flow' ? {
      dock: lands.board.dock,
      // New Box After (FL9): its menu's gesture made in a box after it, as the palette's tool is
      onStepAfter: (gesture: NonNullable<FlowStep['gesture']>, label: string) => this.#stepAfter(id, gesture, label),
      // Import Pure (FL12): a Pure box's other steps, a box after it each, in turn
      onImportPure: async (steps: readonly Lambda[]) => this.#importAfter(id, steps),
      // Dynamic Replace: its rules a row each of another box of its flow, read by the page
      ruleBoxes: () => lands.board.layout.boxes.filter((b) => b.tile !== id && this.#boxes.has(b.tile))
        .map((b) => ({ id: b.tile, label: this.title(b.tile) ?? b.tile })),
      onReadRules: (box: string, expressions: boolean) => void this.#readRules(id, box, expressions),
      // a Pure box's function (FL12): its source's, so the page's to set -- the box then shown again over it
      onSetPure: (pure: Lambda | null) => {
        const told = this.#tell(pure === null ? 'Take Pure Off' : 'Set Pure', pageCommand('setPure', { grid: id, pure }));
        if (told) this.#options.onChange();
        return told;
      },
    } : {}) });
    const stops = [
      cube.on('view', () => {
        this.#refreshGrid(id);
        this.#reconcileGrid(id, cube.snapshot.filter);
        // a flow's box counted as its rows land (M5): what it gave, for its chain as it is
        const on = this.#sheetOf(id);
        const v = cube.view;
        if (on?.kind === 'flow' && v) on.board.counted(id, v.totalRows ?? v.rows.rowCount);
        if (on?.kind === 'flow' && v) this.#askLacking(id, cube, on.board);
      }),
      // its own changes are the page's: "changed since saved" re-reads the page
      cube.on('change', () => {
        this.#options.onChange();
        // a flow's box shown draws what its grid now does (flow-boxes.ts boxSays), and its Undo the change the page kept
        const sheet = this.#sheetOf(id);
        if (sheet?.kind === 'flow') sheet.board.repaint();
      }),
      // where its rows are is the page's too: its copies (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §18.8)
      // its own snap or going live told as the page's command (the copy it now reads, or none)
      cube.on('moved', () => {
        // a grid whose cube the page cannot hold (it cannot say its source) has no copies of the page's
        if (this.#quiet > 0 || !this.#saved(id) || this.documentOf(id) === undefined) return;
        const copy = cube.copy;
        this.#options.onPageEdit({ label: copy ? 'Snap' : 'Go Live', step: false,
          commands: [copy ? pageCommand('snap', { grid: id, copy }) : pageCommand('live', { grid: id })] });
      }),
      cube.on('plane', () => {
        // its rows read from elsewhere now (a snap, going live): its timeseries ask again
        this.#refreshSeries(id);
        this.#options.onChange();
      }),
    ];
    this.#grids.set(id, { source: cube.chartSource(), added: { cube, stop: () => stops.forEach((stop) => stop()) } });
    // numbered by its id, as the page's one rule names a grid with no title of its own (page-commands.ts renameTile,
    // B2): on a cube alone's board too, whose own grid is "Grid" (the S3 audit's 9)
    const made = gridTitle(id);
    this.#titles.set(id, made);
    const near = how.near ?? (this.#options.grid ? GRID : undefined);
    // a flow's box after the box it came from, shown or not; beside a grid, a chart or a text box (a text box's
    // Create ▸ Data Source…: the 2026-10-25 audit)
    const beside = near !== undefined && (this.#grids.has(near) || this.#charts.has(near) || this.#texts.has(near) || this.#boxes.has(near))
      ? near : undefined;
    const about = cube.tileAbout();
    const tile: BandTile = { id, title: how.title ?? made, element: host, actions: [cube.tileHead()], state: cube.tileState(),
      ...(about !== undefined ? { about } : {}) };
    this.#opening.set(id, cube.open());
    // a saved page's grid, or one a step makes again, comes under its own id: placed by its sheet's layout (`restore`,
    // `sync`)
    if (how.id !== undefined) {
      this.#place(tile, beside);
      this.#options.onChange();
      this.#options.onTiles?.();
      return id;
    }
    // a new one: told, placed where its command puts it (`sync`), and brought into view
    const sheet = (beside !== undefined ? this.#sheetOf(beside) : undefined) ?? this.#shown;
    this.#pending.set(id, tile);
    const applied = this.#tell(how.label ?? 'Add Grid', ...(how.before ?? []), pageCommand('addGrid', {
      id, document: this.documentOf(id) ?? null, sheet: sheet.id, near: beside ?? null,
      title: how.title ?? null, columns: sheet.board.columns ?? null,
    }), ...(how.after?.(id) ?? []));
    if (!applied) {
      this.#pending.delete(id);
      // refused (its owner said why): never placed, let go -- its owner told it is gone -- and no tile has its id
      this.#grids.get(id)?.added?.stop();
      this.#grids.delete(id);
      this.#titles.delete(id);
      this.#opening.delete(id);
      cube.dispose();
      this.#options.onTiles?.();
      return id;
    }
    this.#reveal(id);
    return id;
  }

  /** A grid's cube document as its page names it (CubePageOptions.documentOf) -- absent that, the grid's own: one on
   *  the board, or one kept off it for a detached chart. Undefined when it cannot say its source. */
  documentOf(id: string): CubeDocument | undefined {
    if (this.#options.documentOf) return this.#options.documentOf(id);
    return (this.#grids.get(id)?.added?.cube ?? this.#kept.get(id))?.cubeDocument?.('');
  }

  /** Once every grid added so far has opened (its first view landed, or its first query refused). */
  async opened(): Promise<void> {
    await Promise.allSettled([...this.#opening.values()]);
  }

  /**
   * A grid KEPT OFF THE BOARD for the frozen charts that read it (a saved page's grid removed while they were frozen),
   * or the timeseries tiles (the review's G4): made, opened and listened to by nothing, until the last of them goes. Its
   * charts come back detached (`restore`).
   */
  keepGrid(id: string, make: (host: HTMLElement, options: SpawnOptions) => SpawnedGrid): void {
    if (this.#grids.has(id) || this.#kept.has(id)) throw new Error(`a grid ${id} is already on the page`);
    const cube = make(this.#doc.createElement('div'), { id });
    this.#kept.set(id, cube);
    this.#opening.set(id, cube.open());
  }

  /**
   * What a grid on this page sends to it: its charts, its copies and other sources go on this board; Remove from Page
   * takes it off; its export is the page's, with it as the table.
   */
  #spawnOptions(id: string): SpawnOptions {
    return {
      id,
      // a flow's box makes none of them: its rows are a step of the flow, and a chart, a note or a timeseries goes on a
      // board sheet
      onChart: () => (this.#onFlow(id) ? this.#boardsOnly(id, 'A chart') : this.openChart(undefined, id)),
      onText: () => (this.#onFlow(id) ? this.#boardsOnly(id, 'A text box') : this.addText(id)),
      onTimeseries: () => (this.#onFlow(id) ? this.#boardsOnly(id, 'A timeseries') : this.addTimeseries(id)),
      onNewGrid: () => this.addGrid(id),
      onNewSource: (other) => this.addGridOver(other, { near: id }),
      onRemove: () => this.removeGrid(id),
      // a page around the grid's table only when there is a chart to put beside it: the other grids are not in it
      exportPage: () => (this.#chartsBeside(id) ? this.exportPage(id) : undefined),
      ...(this.#options.onSettingsChanged ? { onSettingsChanged: this.#options.onSettingsChanged } : {}),
      ...(this.#options.sheetLabel ? {
        sheets: () => ({ locked: !this.#editing, others: this.#otherSheets(id) }),
        onMoveToSheet: (sheet: string | null) => this.moveToSheet(id, sheet),
      } : {}),
    };
  }

  /** Whether a chart -- a timeseries among them -- is on grid `id`'s sheet: its export then lays out that sheet, the grid
   *  its table. */
  #chartsBeside(id: string): boolean {
    const sheet = this.#sheetOf(id);
    return [...this.#charts.keys(), ...this.#series.keys()].some((chart) => this.#sheetOf(chart) === sheet);
  }

  /**
   * A TILE'S CREATE ▸, as a grid's (ui/menu.ts createItems: the same words in every menu): a data source beside it, a
   * text box beside it, and -- of the grid it reads (`grid`) -- a copy of that grid or another chart of it. With no grid
   * (a text box, a chart whose grid is gone) those two are offered off, `why` said on their hover.
   */
  #tileCreate(grid: string | null, why: string): MenuItem {
    const needsGrid = new Set(['grid.new', 'chart.plot', 'timeseries.new']);
    return { label: 'Create', submenu: createItems({ source: this.#options.addSource !== undefined, host: [] })
      .map((item) => (grid === null && needsGrid.has(item.id ?? '') ? { ...item, disabled: true, hint: why } : item)) };
  }

  /** What a tile's Create ▸ entry picked does (#tileCreate): beside tile `id`, of the grid it reads. */
  #onTileCreate(id: string, grid: string | null, item: MenuItem): void {
    if (item.id === 'source.new') this.#options.addSource?.(id);
    if (item.id === 'text.new') this.addText(id);
    if (grid === null) return;
    if (item.id === 'grid.new') this.addGrid(grid);
    if (item.id === 'chart.plot') this.openChart(undefined, grid);
    // a timeseries tile of the grid it reads, its first series DataCube's rule over the grid's columns, as the grid's
    // own Create ▸ Timeseries makes one (main's tile Create ▸, merged with the timeseries tile, 2026-10-26)
    if (item.id === 'timeseries.new') this.addTimeseries(grid);
  }

  /**
   * A TILE'S VIEW ▸, the last entry of a chart's and a text box's right-click as of a grid's (docs/DATACUBE_UI_REVIEW_
   * 2026_10_24.md, the menus): how it is shown and where -- filling the page or back to it, onto another sheet or a new
   * one (on a page of sheets; locked, offered disabled and why), off the page.
   */
  #tileView(id: string): MenuItem {
    // a board's tile fills its page; a flow's note is on its canvas, which has no such place
    const on = this.#sheetOf(id);
    const filling = on?.kind === 'bands' && on.board.maximised === id;
    const sheets = this.#otherSheets(id);
    return { label: 'View', submenu: [
      ...(on?.kind === 'flow' ? [] : [{ id: 'tile.maximise' as const, label: filling ? 'Restore' : 'Maximise' }]),
      ...(this.#options.sheetLabel ? [{ label: 'Move to Sheet', separated: true, ...(this.#editing ? { submenu: [
        ...sheets.map((s) => ({ id: `sheet.to.${s.id}` as const, label: s.label })),
        { id: 'sheet.toNew' as const, label: 'New Sheet', ...(sheets.length > 0 ? { separated: true } : {}) },
      ] } : { disabled: true, hint: 'Edit Layout first' }) }] : []),
      // off while the page is locked, as its x is (the 2026-10-25 audit)
      { id: 'tile.remove', label: 'Remove from Page', ...(this.#options.sheetLabel ? {} : { separated: true }),
        ...(this.#editing ? {} : { disabled: true, hint: 'Edit Layout first' }) },
    ] };
  }

  /** What a tile's View ▸ entry picked does (#tileView). */
  #onTileView(id: string, item: MenuItem): void {
    if (item.id === 'tile.maximise') {
      const on = this.#sheetOf(id);
      if (on?.kind === 'bands') on.board.maximise(on.board.maximised === id ? null : id);
    }
    if (item.id === 'tile.remove') this.#removeTile(id);
    if (item.id === 'sheet.toNew') this.moveToSheet(id, null);
    else if (item.id?.startsWith('sheet.to.')) this.moveToSheet(id, item.id.slice('sheet.to.'.length));
  }

  /** The sheets a tile is not on, as their tabs say them: where its Move to Sheet can take it -- a box another flow, a
   *  tile of a board another board (page-commands.ts moveTile). */
  #otherSheets(tile: string): { id: string; label: string }[] {
    const label = this.#options.sheetLabel;
    const on = this.#sheetOf(tile);
    return label ? this.#sheets.filter((s) => s !== on && s.kind === on?.kind).map((s) => ({ id: s.id, label: label(s.id) })) : [];
  }

  /** Whether a tile is a flow's box. */
  #onFlow(tile: string): boolean {
    return this.#sheetOf(tile)?.kind === 'flow';
  }

  /** What a box's grid offers that a flow holds none of, said where its rows are. */
  #boardsOnly(box: string, what: string): void {
    this.#sheetOf(box)?.board.say(`${what} goes on a board sheet: a flow's boxes are its steps. Move this box's grid there with Copy of Grid, or make one on a board.`);
  }

  /**
   * A chart of a grid (the cube's own, unless named) on the board: following, or as a saved page had it -- or, `grid`
   * a grid kept off the board, detached, reading that grid's query.
   */
  openChart(restore?: ChartView, grid: string = GRID): void {
    const kept = this.#kept.get(grid);
    if (!this.#grids.has(grid) && !kept) throw new Error(`no grid ${grid} on the page for a chart to follow`);
    const link: ChartLink = kept ? { grid: null, detached: { snapshot: kept.snapshot, from: grid } } : { grid };
    const source = (): ChartSource => this.#sourceOf(link);
    const id = restore?.id ?? this.#nextId('chart');
    const doc = this.#doc;
    const body = doc.createElement('div');
    body.className = 'dc-chart-tile';
    // what the chart is filtering the grid to, with a way out: shown only while it does
    const chip = doc.createElement('button');
    chip.type = 'button';
    chip.className = 'dc-tile-chip';
    chip.hidden = true;
    chip.addEventListener('click', () => this.#select(id, null));
    // THE CHART'S HEADER (the user, 2026-10-25: one header for every tile): its state as an icon left of its title --
    // quiet while it follows its grid, accented when it keeps something of its own -- its title, the filter chip while
    // a click is filtering its grid, and the board's buttons. Everything else is a right-click away. Its states, one
    // word each (the user, 2026-10-23: frozen charts kept their grouping but not their filter): DYNAMIC follows the
    // grid; PINNED keeps its own grouping (a hand on its axes pins it) and follows the grid's filters and the page's
    // clicks; FROZEN keeps its grouping and the grid's filter as they were.
    const pill = stateButton(doc);
    const paint = (): void => {
      const state = panel.state;
      const detached = link.grid === null;
      // a map is placed by its own columns (chart-spec.ts live): the grid's grouping leaves it as it is
      const map = panel.spec !== null && MAP_MARKS.has(panel.spec.mark) && state === 'dynamic';
      paintState(pill, detached ? 'detached' : state, detached ? 'Detached' : STATE_WORDS[state].label, detached
        ? 'Detached: its grid was removed, and this chart keeps its own copy of that grid\'s query. Right-click > Show Grid to show it in a grid of its own.'
        : `${STATE_WORDS[state].label}: ${map ? 'it re-draws as the grid is filtered; a map is placed by its own columns, not the grid\'s grouping' : STATE_WORDS[state].says}. Click to change.`,
      { menu: !detached });
      pill.disabled = detached;
    };
    const panel = new ChartPanel(body, {
      // its spec changed by hand -- its options, a drill, its state -- is a step of the page's
      onState: () => this.#chartChanged(id, 'Chart Change'),
      onSpec: () => this.#chartChanged(id, 'Chart Change'),
      ...(restore ? { initial: restore.spec } : {}),
      snapshot: () => source().snapshot,
      run: async (query, snapshot, signal) => (await source().run(query, snapshot, signal)).rows,
      label: (value, column, type, headline) => source().format(value, column, type, headline),
      unit: (column) => source().unit?.(column),
      // the page's charts with the same column across share a crosshair
      crosshair: this.#crosshair,
      shown: () => source().shown(),
      // a detached chart has no grid's filter to put a click on
      onPick: (mark) => { if (link.grid !== null) this.#select(id, mark); },
      // its options a window among the page's windows, titled by its tile's name
      windowHost: this.#options.windowHost ?? this.#options.host,
      title: () => this.title(id) ?? chartTitle(id),
    });
    // its states offered, the one it is in checked, each said in a line
    pill.addEventListener('click', () => {
      if (link.grid === null) return;
      this.#menuFor = (item) => {
        const state = CHART_STATES.find((s) => item.id === `tile.state.${s}`);
        if (state !== undefined) panel.setState(state);
      };
      const at = pill.getBoundingClientRect();
      // a pick among three: the one it is in checked, what each does said on its hover
      this.#menu.show([{ label: '', items: CHART_STATES.map((s) => ({
        id: `tile.state.${s}` as const, label: STATE_WORDS[s].label, hint: `It ${STATE_WORDS[s].says.replace(/^it /, '')}`, checked: panel.state === s,
      })) }], at.left, at.bottom);
    });
    // RIGHT-CLICK: the chart's own entries
    body.addEventListener('contextmenu', (event) => {
      event.preventDefault();
      const spec = panel.spec;
      // SHOW GRID (D7; the user, 2026-10-23: on every chart): the grid it reads shown, or one of its own made
      // (chart-spec.ts showGridWay) -- off, and why said on the entry, only where no grid of its own can be made
      const way = spec === null ? null : showGridWay(spec, link.grid !== null);
      const refused = way === 'own' ? this.#ownGridRefusal() : null;
      this.#menuFor = (item) => {
        // opens its properties' window beside it, or brings it forward: its × or Escape puts it away
        if (item.id === 'tile.properties') panel.openForm();
        if (item.id === 'tile.grid') this.#showGrid(id);
        this.#onTileCreate(id, link.grid, item);
        this.#onTileView(id, item);
      };
      // the menus' one skeleton (docs/DATACUBE_UI_REVIEW_2026_10_24.md, the menus): its data, its window, then how it
      // is shown and where
      this.#menu.show([
        { label: '', items: [
          { id: 'tile.grid', label: 'Show Grid', ...(way !== null && refused === null ? {} : { disabled: true, ...(refused ? { hint: refused } : {}) }) },
        ] },
        { label: '', items: [this.#tileCreate(link.grid, 'Its grid is gone: it reads its own copy of the query')] },
        { label: '', items: [{ id: 'tile.properties', label: 'Properties\u2026' }] },
        { label: '', items: [this.#tileView(id)] },
      ], event.clientX, event.clientY);
    });
    paint();
    if (!this.#came.has(id)) this.#came.set(id, this.#cameNext++);
    this.#charts.set(id, {
      panel,
      link,
      paint,
      chip,
      conditions: restore?.selection ? [...restore.selection] : [],
      told: JSON.stringify(panel.spec),
      added: restore !== undefined,
    });
    if (restore?.selection) this.#paintSelection(id);
    const actions = [chip];
    if (restore) {
      // on its saved sheet, placed by that sheet's layout once every view is on the page (`restore`, `sync`)
      this.#place({ id, title: restore.title, element: body, actions, state: pill });
      return;
    }
    const tile: BandTile = { id, title: chartTitle(id), element: body, actions, state: pill };
    const spec = panel.spec;
    if (spec && link.grid !== null) {
      if (this.#tellChart(id, tile, spec, link.grid)) this.#reveal(id);
      else this.#dropChart(id);
      return;
    }
    // a chart with no spec yet (nothing to suggest one): no tile the page saves -- the board's, beside its grid, until
    // its spec makes it the page's (#tellSpec)
    this.#place(tile, link.grid ?? undefined);
    this.#reveal(id);
    this.#options.onChange();
    this.#options.onTiles?.();
  }

  /**
   * A chart told as the page's, with its spec (addChart): placed beside its grid where the command puts it (`sync`) --
   * one made while it had no spec, on the board already, kept where it is. False when refused.
   */
  #tellChart(id: string, tile: BandTile, spec: ChartSpec, grid: string): boolean {
    const chart = this.#charts.get(id)!;
    const placed = this.#sheetOf(id) !== undefined;
    if (!placed) this.#pending.set(id, tile);
    chart.told = JSON.stringify(spec);
    const applied = this.#tell('Add Chart', pageCommand('addChart', {
      id, near: grid, spec, title: null, columns: this.#sheetOf(grid)?.board.columns ?? null,
    }));
    if (!applied) this.#pending.delete(id);
    chart.added = applied;
    return applied;
  }

  /** A chart's spec changed by hand: told (once, however many of its panel's callbacks say it), its pill painted. */
  #chartChanged(id: string, label: string): void {
    const chart = this.#charts.get(id);
    if (!chart) return;
    const spec = chart.panel.spec;
    if (spec) this.#tellSpec(id, spec, label);
    chart.paint();
    this.#options.onChange();
  }

  /** A chart's spec told as its `setChart`, unless it is the spec told last -- or, the first it has, as the chart the
   *  page then has (addChart). */
  #tellSpec(id: string, spec: ChartSpec, label: string): void {
    const chart = this.#charts.get(id);
    const text = JSON.stringify(spec);
    if (!chart || chart.told === text) return;
    if (!chart.added) {
      const tile = this.#specs.get(id);
      if (tile && chart.link.grid !== null) this.#tellChart(id, tile, spec, chart.link.grid);
      return;
    }
    chart.told = text;
    this.#tell(label, pageCommand('setChart', { chart: id, spec }));
  }

  /** The cube's own grid changed (a view landed, a sign-in): its charts draw again, a following one regrouped. */
  refresh(): void {
    this.#refreshGrid(GRID);
  }

  /**
   * The cube's own grid's filter changed: a selection whose conditions are no longer all in it
   * (cleared or edited in the filter window, undone) is no longer the chart's to take off.
   */
  reconcile(filter: FilterNode | undefined): void {
    this.#reconcileGrid(GRID, filter);
  }

  #refreshGrid(grid: string): void {
    for (const chart of this.#charts.values()) if (chart.link.grid === grid) chart.panel.refresh();
    this.#refreshSeries(grid);
  }

  /** The timeseries tiles reading grid `grid` told its view landed: each asks again, as a chart does. */
  #refreshSeries(grid: string): void {
    const cube = this.#cubeOf(grid);
    for (const [id, series] of this.#series) {
      if (!series.spec.sources.some((s) => s.cube === cube)) continue;
      // one on a sheet not shown asks nothing now -- it asks when its sheet is (`#drawShownSeries`): a grid sorted on
      // its own sheet asked a hidden tile's every query again (the second audit's P-1; docs/TENETS.md lazy Work)
      if (this.#sheetOf(id) === this.#shown) series.view?.refresh();
      else if (series.view) series.stale = true;
    }
  }

  #reconcileGrid(grid: string, filter: FilterNode | undefined): void {
    for (const [id, chart] of this.#charts) {
      if (chart.link.grid !== grid || chart.conditions.length === 0) continue;
      if (withoutConditions(filter, chart.conditions, true) === null) {
        chart.conditions = [];
        this.#paintSelection(id);
      }
    }
  }

  /**
   * What the page shows, as a saved page keeps it: each grid, each chart (its title, spec, and the mark it filters to)
   * and the layout -- not an editing grid, a moment's work. `cubeOf` names the cube each grid writes in the page's
   * document (a grid kept off the board for a detached chart included); a grid it names none for is left out, with the
   * charts that read it. A cube's own board writes its grid alone (its cube, PAGE_CUBE), as it always has.
   */
  views(cubeOf: (grid: string) => string | undefined = (grid) => (grid === GRID ? PAGE_CUBE : undefined)): PageViews {
    const views: PageView[] = [];
    // the grids in the layout's reading order, sheet by sheet: the order a reopened page adds them in
    for (const id of this.tileIds.filter((t) => this.#grids.has(t) || this.#boxes.has(t) || this.#unavailable.get(t)?.kind === 'grid')) {
      const lost = this.#unavailable.get(id);
      if (lost) {
        views.push(lost);
        continue;
      }
      // a flow's box not shown: its view as the page has it, under the title it has now
      const box = this.#boxes.get(id);
      if (box) {
        const { title: _title, ...view } = box.view;
        const title = this.title(id);
        views.push({ ...view, ...(title !== undefined && title !== this.#titles.get(id) ? { title } : {}) });
        continue;
      }
      const cube = cubeOf(id);
      if (cube === undefined) continue;
      const title = this.title(id);
      views.push({ id, kind: 'grid', cube, ...(title !== undefined && title !== this.#titles.get(id) ? { title } : {}) });
    }
    for (const id of this.#chartIds()) {
      const lost = this.#unavailable.get(id);
      if (lost) {
        views.push(lost);
        continue;
      }
      const chart = this.#charts.get(id)!;
      const spec = chart.panel.spec;
      const reads = chart.link.grid ?? chart.link.detached?.from;
      const cube = reads === undefined ? undefined : cubeOf(reads);
      if (!spec || cube === undefined) continue;
      views.push({
        id,
        kind: 'chart',
        cube,
        title: this.title(id) ?? id,
        spec,
        ...(chart.conditions.length > 0 ? { selection: chart.conditions } : {}),
      });
    }
    // the text tiles in reading order, each its own title only when someone gave it one
    for (const id of this.tileIds.filter((t) => this.#texts.has(t))) {
      const title = this.title(id);
      const text = this.#texts.get(id)!;
      views.push({
        id, kind: 'text', text: text.text, ...(title !== undefined && title !== textTitle(id) ? { title } : {}),
        ...(text.files.length > 0 ? { files: text.files } : {}),
      });
    }
    // the timeseries tiles in reading order, each with its spec as the page has it -- its sources by the cubes they read,
    // kept when a grid it reads is removed (it says so; an undo brings the grid back) -- and its own title only when
    // someone gave it one
    // -- but not one reading a grid this page does not write (a cube alone's added grid, as that grid's charts are left
    // out above: the second audit's A-B1, its page file refused by the reader for a cube it did not hold)
    const unwritten = new Set([...this.#grids.keys(), ...this.#kept.keys()].filter((g) => cubeOf(g) === undefined).map((g) => this.#cubeOf(g)));
    for (const id of this.tileIds.filter((t) => this.#series.has(t))) {
      const title = this.title(id);
      if (this.#series.get(id)!.spec.sources.some((s) => unwritten.has(s.cube))) continue;
      views.push({
        id, kind: 'timeseries', spec: this.#series.get(id)!.spec, ...(title !== undefined && title !== timeseriesTitle(id) ? { title } : {}),
      });
    }
    // each sheet's layout of what is saved: the other tiles' places closed up, as if they had been removed
    // (page-gestures.ts savedLayout, the rule a layout is told by too)
    const saved = new Set(views.map((v) => v.id));
    return {
      views,
      sheets: this.#sheets.map((s) => ({
        id: s.id,
        ...(s.name !== undefined ? { name: s.name } : {}),
        // as the page has it (an editing grid never touches it: P7); a stack as it is saved, reopening on its first tab
        // (the design's §5, 7); a flow's boxes where they are
        layout: s.kind === 'flow' ? { kind: 'flow' as const, boxes: s.board.layout.boxes.filter((b) => saved.has(b.tile)) }
          : savedLayout(this.#layoutOf(s), saved),
      })),
    };
  }

  /**
   * Put a saved page's views back: the grids' titles, the charts -- each following the grid `gridOf` finds for its
   * cube, or detached when that grid is kept off the board -- and the layout. The grids are on the page already (a
   * cube's own; a page's, added under their saved ids).
   */
  restore(page: PageViews & { readonly cubes?: PageDocument['cubes'] },
    gridOf: (cube: string) => string | undefined = (cube) => (cube === PAGE_CUBE ? GRID : undefined),
    unopened: (cube: string) => string | undefined = () => undefined): void {
    // its sheets, if the grids were not put on them as they were added (a cube alone's own grid)
    this.setSheets(page.sheets);
    for (const view of page.views) {
      if (view.kind === 'grid' && view.title && this.#grids.has(view.id)) this.#sheetOf(view.id)?.board.rename(view.id, view.title);
    }
    const sheetFor = new Map(page.sheets.flatMap((s) => layoutTilesOf(s.layout).map((t) => [t, s.id] as const)));
    // a flow's boxes, documents until one is shown (none is, as a page opens): each drawn from its cube
    for (const view of page.views) {
      const on = this.#sheets.find((s) => s.id === sheetFor.get(view.id));
      if (view.kind !== 'grid' || on?.kind !== 'flow' || this.#grids.has(view.id)) continue;
      const document = page.cubes?.find((c) => c.id === view.cube)?.cube;
      if (document === undefined) throw new Error(`box ${view.id} has no cube of the page's to be drawn from`);
      this.#putBox(on, view, document);
    }
    for (const view of page.views) {
      if (view.kind !== 'text' || this.#texts.has(view.id)) continue;
      const on = this.#sheets.find((s) => s.id === sheetFor.get(view.id));
      if (on) this.#planned.set(view.id, on);
      this.#placeText(view);
    }
    for (const view of page.views) {
      if (view.kind !== 'timeseries' || this.#series.has(view.id)) continue;
      const on = this.#sheets.find((s) => s.id === sheetFor.get(view.id));
      if (on) this.#planned.set(view.id, on);
      this.#placeTimeseries(view);
    }
    const charts = page.views.filter((v): v is ChartView => v.kind === 'chart');
    for (const chart of charts) {
      const grid = gridOf(chart.cube);
      if (grid !== undefined && (this.#grids.has(grid) || this.#kept.has(grid))) this.openChart(chart, grid);
      else {
        // its grid's source not opened: kept as saved, in its place
        const why = unopened(chart.cube);
        if (why !== undefined) this.placeUnavailable(chart, why);
      }
    }
    for (const saved of page.sheets) {
      const sheet = this.#sheets.find((s) => s.id === saved.id);
      if (sheet === undefined) continue;
      if (sheet.kind === 'flow') {
        if (saved.layout.kind === 'flow') sheet.board.setLayout({ boxes: saved.layout.boxes });
        continue;
      }
      if (saved.layout.kind !== 'bands') throw new Error(`sheet ${saved.id} is a board here, not in the page`);
      const { kind: _kind, ...bands } = saved.layout;
      sheet.board.setLayout(bands);
      this.#pageLayouts.set(sheet.id, sheet.board.layout);
    }
    this.#planned.clear();
    // a page reopens on its first sheet (what is looked at is not saved: the design's §5, 7)
    this.showSheet(this.#sheets[0]!.id);
    this.#drawShownSeries();
    // a kept grid no restored chart reads (its charts not opened) goes now, not at the next removal
    this.#releaseKept();
  }

  /**
   * The tiles of grid `table`'s sheet as an export of it lays them out: where each is, a chart as its picture, that
   * grid as its table. The other grids are not in it (an export holds one table); every chart on that sheet is.
   */
  exportPage(table: string = GRID): ExportPage {
    // the sheet as the page has it (an editing grid open on it is no tile of the page's: P7), each stack exporting the
    // tile it shows in front -- the grid exported in front of its own
    const sheet = this.#sheetOf(table) ?? this.#shown;
    // a flow's box: its grid alone, the page's width (a flow has no charts beside its boxes)
    if (sheet.kind === 'flow') {
      return { cols: 24, tiles: [{ id: table, kind: 'grid', title: this.title(table) ?? GRID_TILE_TITLE, x: 0, y: 0, w: 24, h: 1 }] };
    }
    const grid = cells(bringToFront(without(fronted(this.#layoutOf(sheet), sheet.board.layout),
      (id) => this.#grids.has(id) && id !== table), table));
    const tiles = grid.tiles.map((t): ExportTile => {
      const text = this.#texts.get(t.id);
      if (text) return { id: t.id, kind: 'text', title: this.title(t.id) ?? textTitle(t.id), x: t.x, y: t.y, w: t.w, h: t.h, text: this.#exportedText(text) };
      // a timeseries tile exports as a chart does: its picture as drawn
      const series = this.#series.get(t.id);
      if (series) {
        const drawn = series.view?.picture() ?? null;
        return {
          id: t.id, kind: 'chart', title: this.title(t.id) ?? timeseriesTitle(t.id), x: t.x, y: t.y, w: t.w, h: t.h,
          ...(drawn ? { picture: drawn } : {}),
        };
      }
      const chart = this.#charts.get(t.id);
      const picture = chart?.panel.picture() ?? null;
      return {
        id: t.id,
        kind: chart ? 'chart' : 'grid',
        title: this.title(t.id) ?? (chart ? t.id : GRID_TILE_TITLE),
        x: t.x, y: t.y, w: t.w, h: t.h,
        ...(picture ? { picture } : {}),
      };
    });
    return { cols: grid.cols, tiles };
  }

  dispose(): void {
    this.#disposed = true;
    // each cached box's copy the page keeps let go with it (FL11: a cache lasts its session)
    for (const id of [...this.#cacheKept.keys()]) this.#letCacheGo(id);
    this.#menu.close();
    this.#picker?.close();
    for (const chart of this.#charts.values()) chart.panel.dispose();
    this.#charts.clear();
    this.#texts.clear();
    this.#boxes.clear();
    for (const series of this.#series.values()) series.view?.dispose();
    this.#series.clear();
    for (const grid of this.#grids.values()) {
      grid.added?.stop();
      grid.added?.cube.dispose();
    }
    this.#grids.clear();
    for (const cube of this.#kept.values()) cube.dispose();
    this.#kept.clear();
    for (const sheet of this.#sheets) {
      sheet.board.dispose();
      sheet.host.remove();
    }
  }

  // -- charts ---------------------------------------------------------------

  /**
   * Where a chart's rows come from now: its grid, or -- detached -- its own copy of the query, run by the grid it came
   * from, kept off the board for it (each grid has its own engine and planner, over its own model).
   */
  #sourceOf(link: ChartLink): ChartSource {
    if (link.grid !== null) {
      const source = this.#grids.get(link.grid)?.source;
      if (!source) throw new Error(`no grid ${link.grid} on the page`);
      return source;
    }
    const detached = link.detached;
    const kept = detached ? this.#kept.get(detached.from) : undefined;
    if (!detached || !kept) throw new Error('a detached chart lost the grid that runs its query');
    const runs = kept.chartSource();
    return {
      snapshot: detached.snapshot,
      get configuration() { return runs.configuration; },
      run: (query, s, signal) => runs.run(query, s, signal),
      print: (query) => runs.print(query),
      format: (value, column, type, headline) => runs.format(value, column, type, headline),
      unit: (column) => runs.unit?.(column),
      label: (column) => runs.label(column),
      // detached, a treemap keeps the rows it last drew
      shown: () => null,
      refilter: () => {},
      unselected: () => {},
      spawn: (host, s, options) => runs.spawn(host, s, options),
    };
  }

  /**
   * A TILE'S REMOVE (its x, its menu's Remove, a grid's Remove from Page): a tile of the page's is told as its removal,
   * and `sync` takes it off (#drop). A chart's selection comes off its grid's filter by removeTile's own rule (the
   * page's unselected), as Python's removeTile takes it off: one removal, one command (the S1 audit's 5); the grid on
   * the board follows the step, a step of its own history, told only after an edit of its own still running
   * (ChartSource.unselected; the S7 audit's P-1, the S7 confirmation audit's G2-2, B2-3).
   */
  #removeTile(id: string): void {
    if (!this.#saved(id)) {
      // a chart with no spec yet: the board's alone
      if (this.#charts.has(id)) this.#dropChart(id);
      return;
    }
    this.#tell(`Remove ${this.title(id) ?? id}`, pageCommand('removeTile', { tile: id }));
  }

  /** A tile off the board as a step leaves it (`sync`): a grid, a tile it could not open, a chart. */
  #drop(id: string): void {
    if (this.#grids.get(id)?.added) this.#dropGrid(id);
    else if (this.#unavailable.has(id)) {
      this.#dropUnavailable(id);
      this.#options.onChange();
      this.#afterRemove();
    } else if (this.#charts.has(id)) this.#dropChart(id);
    else if (this.#texts.has(id)) this.#dropText(id);
    else if (this.#boxes.has(id)) {
      this.#dropBox(id);
      this.#options.onChange();
      this.#afterRemove();
    }
    else if (this.#series.has(id)) this.#dropTimeseries(id);
  }

  /** A chart off the board: its selection off its grid's filter as the step took it off (`theirs`: another editor's,
   *  followed, not told), a grid kept only for it let go. */
  #dropChart(id: string, theirs = false, refilter = true): void {
    const chart = this.#charts.get(id);
    if (!chart) return;
    // its selection off its grid as the step that took it off the board took it off the grid's filter (the page's
    // unselected) -- unless the grid is put back whole by its caller (a converge's put-back: PageApp.putBack)
    if (refilter && chart.link.grid !== null && chart.conditions.length > 0) this.#sourceOf(chart.link).unselected(chart.conditions, theirs);
    chart.panel.dispose();
    this.#charts.delete(id);
    this.#take(id);
    this.#releaseKept();
    this.#options.onChange();
    this.#afterRemove();
  }

  /** A grid kept off the board that no chart, and no timeseries, reads any more goes. */
  #releaseKept(): void {
    for (const [id, cube] of [...this.#kept]) {
      if ([...this.#charts.values()].some((c) => c.link.detached?.from === id)) continue;
      if (this.#seriesRead(id)) continue;
      cube.dispose();
      this.#kept.delete(id);
    }
  }

  /** Whether a timeseries tile reads grid `id`'s cube: what keeps a removed grid's cube on the page (the review's G4). */
  #seriesRead(id: string): boolean {
    const cube = this.#cubeOf(id);
    return [...this.#series.values()].some((t) => t.spec.sources.some((s) => s.cube === cube));
  }

  /**
   * A tile is gone (its neighbours closed over its place): a cube's own board with only its grid left is no more; a
   * page of its own with no tile left is empty.
   */
  #afterRemove(): void {
    this.#options.onTiles?.();
    if (!this.#isEmpty()) return;
    // inside `sync`, said once it has done: its owner may take the board down (the S3 audit's 2)
    if (this.#syncing) this.#emptied = true;
    else this.#options.onEmpty();
  }

  /** Nothing left of what the board is for: a cube's own with no chart, a page of its own with no tile. */
  #isEmpty(): boolean {
    return this.charts === 0 && this.#texts.size === 0 && this.#series.size === 0
      && (this.#options.grid !== undefined || (this.#grids.size === 0 && this.#boxes.size === 0));
  }

  // -- text tiles ------------------------------------------------------------------------------------------------------

  /**
   * CREATE ▸ TEXT BOX: a text tile on `near`'s sheet (else the sheet shown), told as the page's (addText) -- a band of its
   * own at the sheet's bottom -- its editor then open on its starting words.
   */
  addText(near?: string): void {
    const sheet = (near !== undefined ? this.#sheetOf(near) : undefined) ?? this.#shown;
    const id = this.#nextId('text');
    if (!this.#tell('Add Text', pageCommand('addText', { id, sheet: sheet.id, text: STARTING_TEXT, title: null, columns: sheet.board.columns ?? null }))) return;
    this.#reveal(id);
    this.editText(id);
  }

  /** A text tile on the board, its words drawn (page-text.ts textElement); a double-click, or its Edit, edits them. */
  #placeText(view: TextView): void {
    const doc = this.#doc;
    const body = doc.createElement('div');
    body.className = 'dc-text-host';
    // its state, left of its title as every tile's (the user, 2026-10-25): a pencil, quiet while its words are read,
    // accented while they are edited -- a click edits them; in view mode it goes (app.css), nothing is edited there
    const edit = stateButton(doc);
    edit.classList.add('dc-text-edit');
    this.#texts.set(view.id, { text: view.text, files: view.files ?? [], shown: null, drawn: null, body, edit, editing: false });
    this.#paintTextState(view.id);
    this.#drawText(view.id);
    // FILES DROPPED ON IT (docs/DATACUBE_UI_REVIEW_2026_10_24.md §8): shown beside its words, pointed to (text-files.ts)
    const carriesFiles = (e: DragEvent): boolean => [...(e.dataTransfer?.types ?? [])].includes('Files');
    body.addEventListener('dragover', (e) => {
      if (!carriesFiles(e) || this.#texts.get(view.id)?.editing) return;
      e.preventDefault();
      body.classList.add('dc-text-drop');
    });
    body.addEventListener('dragleave', () => body.classList.remove('dc-text-drop'));
    body.addEventListener('drop', (e) => {
      body.classList.remove('dc-text-drop');
      if (!carriesFiles(e) || this.#texts.get(view.id)?.editing || !e.dataTransfer) return;
      e.preventDefault();
      // each file's handle asked for now, inside the drop: the items are gone once it returns
      const items = [...e.dataTransfer.items].filter((i) => i.kind === 'file');
      const picked = items.map((i) => ({
        file: i.getAsFile(),
        handle: (i as { getAsFileSystemHandle?: () => Promise<unknown> }).getAsFileSystemHandle?.(),
      }));
      void Promise.all(picked.map(async (p) => {
        const handle = await p.handle?.catch(() => null);
        return p.file === null ? null : { file: p.file, ...(isFileHandle(handle) ? { handle } : {}) };
      })).then((got) => this.addTextFiles(view.id, got.filter((g): g is { file: File; handle?: FileHandle } => g !== null)));
    });
    // its words double-clicked: a file's tab or its preview is not its words (the 2026-10-25 audit: a word selected in a
    // CSV opened the editor over it)
    body.addEventListener('dblclick', (e) => {
      if ((e.target as Element | null)?.closest?.('.dc-text-files, .dc-text-file, .dc-text-file-missing')) return;
      this.editText(view.id);
    });
    edit.addEventListener('click', () => this.editText(view.id));
    // RIGHT-CLICK: its window, then how it is shown and where, as a chart's (#tileView); its editor's own words keep the
    // browser's (copy, paste)
    body.addEventListener('contextmenu', (event) => {
      if (this.#texts.get(view.id)?.editing) return;
      event.preventDefault();
      this.#menuFor = (item) => {
        if (item.id === 'tile.editText') this.editText(view.id);
        if (item.id === 'tile.openFiles') void this.openTextFiles(view.id);
        this.#onTileCreate(view.id, null, item);
        this.#onTileView(view.id, item);
      };
      this.#menu.show([
        { label: '', items: [this.#tileCreate(null, 'A text box reads no grid')] },
        { label: '', items: [{ id: 'tile.editText', label: 'Edit Text\u2026' }, { id: 'tile.openFiles', label: 'Open Files\u2026' }] },
        { label: '', items: [this.#tileView(view.id)] },
      ], event.clientX, event.clientY);
    });
    this.#place({ id: view.id, title: view.title ?? textTitle(view.id), element: body, actions: [], state: edit });
  }

  /** A text tile's pencil: being edited, or there to edit it. */
  #paintTextState(id: string): void {
    const text = this.#texts.get(id);
    if (!text) return;
    paintState(text.edit, text.editing ? 'editing' : 'viewing', text.editing ? 'Editing' : 'Edit',
      text.editing ? 'Its words are being edited: OK keeps them, Cancel or Escape leaves them as they were' : 'Edit the text');
  }

  /** A text tile's words drawn (page-text.ts, a chunk of its own fetched with the first drawn), unless its editor is
   *  open by the time they are. */
  #drawText(id: string, focusTab = false): void {
    this.#paintTextState(id);
    void lazily(() => import('../page-text.ts')).then((chunk) => {
      const text = this.#texts.get(id);
      if (!text || text.editing) return;
      const words = chunk.textElement(this.#doc, chunk.readText(text.text));
      text.drawn = words;
      if (text.files.length === 0) {
        text.body.replaceChildren(words);
        return;
      }
      // its files beside its words: a tab each, the one shown below them
      const pane = this.#doc.createElement('div');
      pane.className = 'dc-text-pane';
      const strip = this.#fileTabs(id, text);
      text.body.replaceChildren(strip, pane);
      // a tab pressed by the keyboard keeps it: the strip is drawn again (the 2026-10-25 audit)
      if (focusTab) strip.querySelector<HTMLElement>('[aria-pressed="true"]')?.focus();
      const shown = text.files.find((f) => f.sha256 === text.shown);
      if (shown === undefined) pane.append(words);
      else void this.#drawFile(id, shown, pane, chunk.fileElement, false);
    });
  }

  /**
   * A text box's tabs: its own words, then each of its files with its x, the one shown pressed -- buttons, each pressed
   * or not (a tab list holds tabs alone, and each file's x is beside its name: the 2026-10-25 audit).
   */
  #fileTabs(id: string, text: TextTile): HTMLElement {
    const doc = this.#doc;
    const strip = doc.createElement('div');
    strip.className = 'dc-text-files';
    strip.setAttribute('role', 'group');
    strip.setAttribute('aria-label', 'What the text box shows');
    const tab = (label: string, sha: string | null): HTMLElement => {
      const holder = doc.createElement('span');
      holder.className = 'dc-text-file-tab';
      const button = doc.createElement('button');
      button.type = 'button';
      button.className = 'dc-text-file-name';
      button.setAttribute('aria-pressed', String(text.shown === sha));
      button.textContent = label;
      button.addEventListener('click', () => {
        text.shown = sha;
        this.#drawText(id, true);
      });
      holder.append(button);
      strip.append(holder);
      return holder;
    };
    tab('Text', null);
    for (const file of text.files) {
      const holder = tab(file.name, file.sha256);
      holder.title = `${file.name}, ${bytesWords(file.size)}`;
      const off = closeMark(doc.createElement('button'), `Take ${file.name} off`);
      off.type = 'button';
      off.className = 'dc-text-file-close';
      off.setAttribute('aria-label', `Take ${file.name} off`);
      // by its fingerprint: the files a step draws again are equal, not the same objects (the 2026-10-25 audit)
      off.addEventListener('click', () => {
        this.#tell(`Take ${file.name} Off`, pageCommand('setTextFiles', { tile: id, files: text.files.filter((f) => f.sha256 !== file.sha256) }));
      });
      holder.append(off);
    }
    return strip;
  }

  /**
   * A text box's file drawn in `pane` (page-text.ts fileElement): read from this session's copy, else again from where it
   * was picked (the host's kept handle); the browser asking first, a click of the person's lets it -- missing, or not
   * read, it is asked for again by its name. Read as it is now: changed since it was added, that is said. Only its first
   * TEXT_FILE_SHOWN bytes are drawn: a copy already fingerprinted is read no further.
   */
  async #drawFile(id: string, file: TextFile, pane: HTMLElement,
    draw: (doc: Document, format: NonNullable<ReturnType<typeof textFileFormat>>, text: string, separator: ',' | '\t', cut?: string) => HTMLElement,
    click: boolean): Promise<void> {
    const doc = this.#doc;
    const kept = this.#textFiles.get(file.sha256);
    const read: HandleRead = kept ? { state: 'file', file: kept.file }
      : (await this.#options.textFile?.(file.sha256, click).catch((e: unknown) => ({ state: 'unavailable' as const, why: e instanceof Error ? e.message : String(e) })))
        ?? { state: 'unavailable', why: 'this browser keeps nothing of it' };
    // moved on meanwhile: another tab shown, the box redrawn
    const away = (): boolean => this.#texts.get(id)?.shown !== file.sha256 || !pane.isConnected;
    if (away()) return;
    if (read.state === 'needs-click') {
      pane.replaceChildren(this.#fileAsk(`The browser asks before this page reads ${file.name} again.`, `Allow reading ${file.name}`,
        () => void this.#drawFile(id, file, pane, draw, true)));
      return;
    }
    const missing = (why: string): void => {
      pane.replaceChildren(this.#fileAsk(`${file.name} is not here (${why}): a page points to its files, never copies them.`,
        `Open ${file.name}\u2026`, () => void this.#openAgain(id, file)));
    };
    if (read.state === 'unavailable') {
      missing(read.why);
      return;
    }
    const format = textFileFormat(file.name);
    if (format === undefined) throw new Error(`${file.name} is a kind of file a text box does not show, yet it is on one`);
    let shownBytes: ArrayBuffer;
    let changed = false;
    try {
      if (kept) shownBytes = await read.file.slice(0, TEXT_FILE_SHOWN).arrayBuffer();
      else {
        // read whole once, to know it is the file added: kept for this session only then (the 2026-10-25 audit: a
        // changed file kept under the old fingerprint was shown, the next time, as the one added)
        const bytes = await read.file.arrayBuffer();
        changed = (await sha256(bytes)) !== file.sha256;
        if (!changed) this.#textFiles.set(file.sha256, { file: read.file });
        shownBytes = bytes.slice(0, TEXT_FILE_SHOWN);
      }
    } catch (e) {
      // moved, or changed on disk since it was picked (the browser's NotReadableError): asked for again
      missing(e instanceof Error ? e.message : String(e));
      return;
    }
    if (away()) return;
    const cut = read.file.size > TEXT_FILE_SHOWN ? `its first ${bytesWords(TEXT_FILE_SHOWN)}` : undefined;
    const shown = draw(doc, format, new TextDecoder().decode(shownBytes), csvSeparator(file.name), cut);
    const said: string[] = [];
    if (changed) said.push(`${file.name} has changed since it was added to this text box.`);
    if (cut !== undefined) said.push(`The first ${bytesWords(TEXT_FILE_SHOWN)} of ${file.name} (${bytesWords(read.file.size)}) are shown.`);
    for (const words of said.reverse()) {
      const note = doc.createElement('p');
      note.className = 'dc-text-file-note';
      note.textContent = words;
      shown.prepend(note);
    }
    pane.replaceChildren(shown);
  }

  /** What a text box says in place of a file it cannot show, and the one button that would show it. */
  #fileAsk(words: string, act: string, go: () => void): HTMLElement {
    const box = this.#doc.createElement('div');
    box.className = 'dc-text-file-missing';
    const said = this.#doc.createElement('p');
    said.textContent = words;
    const button = this.#doc.createElement('button');
    button.type = 'button';
    button.className = 'dc-button';
    button.textContent = act;
    button.addEventListener('click', go);
    box.append(said, button);
    return box;
  }

  /** What the board refuses before telling a command, said as the page's refusals are (`refuse`). */
  #refuse(words: string): void {
    if (this.#options.refuse) this.#options.refuse(words);
    else this.say(words);
  }

  /** Each picked file's fingerprint, its bytes read whole; undefined when one is not read -- a folder dropped, a file
   *  moved or changed since -- refused in words (`#refuse`). */
  async #fingerprinted(picked: readonly { readonly file: File }[]): Promise<string[] | undefined> {
    const out: string[] = [];
    for (const p of picked) {
      try {
        out.push(await sha256(await p.file.arrayBuffer()));
      } catch (e) {
        this.#refuse(`${p.file.name} could not be read (${e instanceof Error ? e.message : String(e)}): a folder, or a file moved since it was picked`);
        return undefined;
      }
    }
    return out;
  }

  /** A missing file picked again: the same one (its fingerprint) shown, its handle kept by the host; another one in its
   *  place, told as the box's files. */
  async #openAgain(id: string, file: TextFile): Promise<void> {
    const [got] = await pickTextFiles(this.#doc, TEXT_FILE_EXTENSIONS, false);
    if (!got) return;
    // judged by name and size before it is read, by the page's own rule (text-files.ts pickedRefusal)
    const refused = pickedRefusal([{ name: got.file.name, size: got.file.size }]);
    if (refused !== undefined) {
      this.#refuse(refused);
      return;
    }
    const [sha] = (await this.#fingerprinted([got])) ?? [];
    if (sha === undefined) return;
    this.#textFiles.set(sha, got);
    if (got.handle) this.#options.keepTextFile?.(sha, got.handle);
    const text = this.#texts.get(id);
    if (!text) return;
    if (sha === file.sha256) {
      this.#drawText(id);
      return;
    }
    const other: TextFile = { name: got.file.name, size: got.file.size, sha256: sha };
    const was = text.shown;
    text.shown = sha;
    const files = text.files.map((f) => (f.sha256 === file.sha256 ? other : f));
    if (!this.#tell(`Replace ${file.name}`, pageCommand('setTextFiles', { tile: id, files }))) text.shown = was;
  }

  /** Open Files… on a text box's right-click: the files picked, shown beside its words (`addTextFiles`). */
  async openTextFiles(id: string): Promise<void> {
    const picked = await pickTextFiles(this.#doc, TEXT_FILE_EXTENSIONS);
    if (picked.length > 0) await this.addTextFiles(id, picked);
  }

  /**
   * FILES ON A TEXT BOX (docs/DATACUBE_UI_REVIEW_2026_10_24.md §8): each fingerprinted and added to its files, told as the
   * page's command (setTextFiles: its rule refuses, in its words, a kind it does not show, too many, too big -- asked by
   * name and size before any is read) -- one it shows already left as it is; the first new one shown. Their bytes stay
   * this session's, their handles kept when the page is saved; the page keeps only where they are.
   */
  async addTextFiles(id: string, picked: readonly { readonly file: File; readonly handle?: FileHandle }[]): Promise<void> {
    if (!this.#texts.has(id)) return;
    const refused = pickedRefusal(picked.map((p) => ({ name: p.file.name, size: p.file.size })));
    if (refused !== undefined) {
      this.#refuse(refused);
      return;
    }
    const shas = await this.#fingerprinted(picked);
    if (shas === undefined) return;
    const added = picked.map((p, i): TextFile => ({ name: p.file.name, size: p.file.size, sha256: shas[i]! }));
    picked.forEach((p, i) => this.#textFiles.set(shas[i]!, p));
    const text = this.#texts.get(id);
    if (!text) return;
    const fresh = added.filter((a, i) => !text.files.some((f) => f.sha256 === a.sha256) && added.findIndex((b) => b.sha256 === a.sha256) === i);
    if (fresh.length === 0) return;
    const was = text.shown;
    text.shown = fresh[0]!.sha256;
    const label = fresh.length === 1 ? `Add ${fresh[0]!.name}` : `Add ${fresh.length} Files`;
    if (!this.#tell(label, pageCommand('setTextFiles', { tile: id, files: [...text.files, ...fresh] }))) text.shown = was;
  }

  /** Each file on the page's text boxes, with the handle it was picked with this session where the browser gave one:
   *  kept by the host when the page is saved -- one with none here, the handle the page it was saved as kept (Save As:
   *  the 2026-10-25 audit) -- so opening it again reads each from there (file-handles.ts handleKey). */
  textFilesNow(): { readonly sha256: string; readonly handle?: FileHandle }[] {
    const on = new Set([...this.#texts.values()].flatMap((t) => t.files.map((f) => f.sha256)));
    return [...on].map((sha) => {
      const handle = this.#textFiles.get(sha)?.handle;
      return handle ? { sha256: sha, handle } : { sha256: sha };
    });
  }

  /**
   * A TEXT TILE'S EDITOR, in its place (page-text.ts textEditor): OK tells its words (setText -- refused by the page, the
   * editor stays with what was typed); Cancel or Escape leaves them as they were.
   */
  editText(id: string): void {
    const text = this.#texts.get(id);
    if (!text || text.editing) return;
    text.editing = true;
    this.#paintTextState(id);
    void lazily(() => import('../page-text.ts')).then(({ textEditor }) => {
      if (!this.#texts.has(id)) return;
      const close = (): void => {
        text.editing = false;
        this.#drawText(id);
      };
      const editor = textEditor(this.#doc, {
        words: text.text,
        onCancel: close,
        onOk: (words) => {
          if (words === text.text) return close();
          text.editing = false;
          // the step draws it (sync); refused, the editor stays with what was typed
          if (!this.#tell('Edit Text', pageCommand('setText', { tile: id, text: words }))) text.editing = true;
        },
      });
      text.body.replaceChildren(...editor.elements);
      editor.area.focus();
      editor.area.select();
    });
  }

  /** A text tile as an export writes it: as the board drew it -- its HTML, a line a block -- or, not yet drawn, its
   *  words as typed. */
  #exportedText(text: TextTile): { readonly html: string; readonly lines: readonly string[] } {
    // its words as drawn, a file's tab in front or not (the 2026-10-25 audit: an export depended on the tab shown)
    const drawn = text.drawn ?? text.body.querySelector('.dc-text-tile');
    if (drawn === null) {
      const pre = this.#doc.createElement('pre');
      pre.textContent = text.text;
      return { html: pre.outerHTML, lines: text.text.split('\n') };
    }
    return { html: drawn.outerHTML, lines: [...drawn.children].flatMap(textLines) };
  }

  /** A text tile off the board, as a step leaves it. */
  #dropText(id: string): void {
    if (!this.#texts.delete(id)) return;
    this.#take(id);
  }

  // -- timeseries tiles ------------------------------------------------------------------------------------------------

  /** A grid tile's cube as the page names it: a cube alone's own grid is its page's cube (PAGE_CUBE); every other grid's
   *  is its tile's id. */
  #cubeOf(grid: string): string {
    return grid === GRID && this.#options.grid ? PAGE_CUBE : grid;
  }

  /** The grid on the board that shows `cube` (#cubeOf's inverse), or null: none does (its grid removed). */
  #gridOfCube(cube: string): string | null {
    const grid = cube === PAGE_CUBE && this.#options.grid ? GRID : cube;
    return this.#grids.has(grid) ? grid : null;
  }

  /** A grid of the page as a timeseries reads it, by the cube the page names it by: its query, its filter, its runner and
   *  its formats as they are now; undefined when no grid on the board shows that cube (removed). */
  #tsGrid(cube: string): TsGrid | undefined {
    const grid = cube === PAGE_CUBE && this.#options.grid ? GRID : cube;
    // a grid removed while the tile reads it: its cube kept off the board, read as it was (the review's G4)
    const kept = this.#grids.has(grid) ? undefined : this.#kept.get(grid);
    const source = this.#grids.get(grid)?.source ?? kept?.chartSource();
    if (!source) return undefined;
    return {
      cube,
      ...(kept ? { kept: true } : {}),
      title: this.title(grid) ?? this.#titles.get(grid) ?? grid,
      get snapshot() { return source.snapshot; },
      get filter() { return source.snapshot.filter; },
      run: (query, snapshot, signal) => source.run(query, snapshot, signal),
      format: (value, column, type) => source.format(value, column, type),
      print: (query) => source.print(query),
    };
  }

  /**
   * CREATE ▸ TIMESERIES (docs/DATACUBE_TIMESERIES_DESIGN_2026_10_25.md §7): a timeseries tile of grid `near` -- else the
   * first grid on the sheet shown, else the page's first -- under it, its first series DataCube's rule over the grid's
   * columns (ui/timeseries-view.ts firstTimeseries, fetched with the viewer), told as the page's (addTimeseries). A grid
   * with no date to read along is said so.
   */
  addTimeseries(near?: string): void {
    const shown = sheetTiles(this.#shown).filter((t) => this.#grids.has(t));
    const grid = near !== undefined && this.#grids.has(near) ? near : shown[0] ?? [...this.#grids.keys()][0];
    if (grid === undefined) return;
    void lazily(() => import('../ui/timeseries-view.ts')).then(({ firstTimeseries }) => {
      const read = this.#tsGrid(this.#cubeOf(grid));
      if (this.#disposed || !read) return;
      const spec = firstTimeseries(read);
      if (typeof spec === 'string') return this.#refuse(spec);
      const sheet = this.#sheetOf(grid) ?? this.#shown;
      const id = this.#nextId('timeseries');
      if (this.#tell('Add Timeseries', pageCommand('addTimeseries', {
        id, sheet: sheet.id, near: grid, spec, title: null, columns: sheet.board.columns ?? null,
      }))) this.#reveal(id);
    });
  }

  /** A timeseries tile on the board, its viewer fetched and drawn (`#drawTimeseries`) once its sheet is shown. */
  #placeTimeseries(view: TimeseriesPageView): void {
    const body = this.#doc.createElement('div');
    body.className = 'dc-ts-host';
    // ITS HEADER'S STATE (the user, 2026-10-26: a timeseries' states are a chart's, where they mean something): DYNAMIC
    // follows its grids' filters and the page's clicks; FROZEN keeps each grid's filter as it was; DETACHED, a grid it
    // reads removed, read as it was -- its state still its to change, as Python's Timeseries.state changes it (one rule:
    // the third review's note -- the screen alone turned it off when every grid was gone)
    const pill = stateButton(this.#doc);
    const paint = (): void => {
      const tile = this.#series.get(view.id);
      if (!tile) return;
      const state = tsStateOf(tile.spec);
      const gone = tile.spec.sources.filter((s) => this.#tsGrid(s.cube)?.kept === true);
      const words = SERIES_STATE_WORDS[state];
      const names = gone.map((s) => this.#tsGrid(s.cube)!.title).join(', ');
      paintState(pill, gone.length > 0 ? 'detached' : state, gone.length > 0 ? 'Detached' : words.label, gone.length > 0
        ? `Detached: ${names}${gone.length === 1 ? '\'s grid was' : '\'s grids were'} removed, and this tile reads ${gone.length === 1 ? 'its' : 'their'} query as it was. ${words.label}: ${words.says}. Click to change.`
        : `${words.label}: ${words.says}. Click to change.`, { menu: true });
    };
    pill.addEventListener('click', () => {
      const tile = this.#series.get(view.id);
      if (!tile?.view) return;
      const shown = tile.view;
      this.#menuFor = (item) => {
        const state = TS_STATES.find((s) => item.id === `tile.state.${s}`);
        if (state !== undefined) shown.setState(state);
      };
      const at = pill.getBoundingClientRect();
      const now = tsStateOf(tile.spec);
      this.#menu.show([{ label: '', items: TS_STATES.map((s) => ({
        id: `tile.state.${s}` as const, label: SERIES_STATE_WORDS[s].label, hint: `It ${SERIES_STATE_WORDS[s].says.replace(/^it /, '')}`, checked: now === s,
      })) }], at.left, at.bottom);
    });
    // RIGHT-CLICK (PR #73's one shape, a chart's): its Settings as its Properties, Create ▸ beside it of the grid it
    // reads first (its grid gone, what needs one off and why), View ▸ -- the viewer's own toolbar the rest
    body.addEventListener('contextmenu', (event) => {
      const tile = this.#series.get(view.id);
      if (!tile) return;
      event.preventDefault();
      // the first of its grids still on the board (the third review's note: the first source's alone, its grid gone, turned
      // Create's grid entries off though another it reads was there)
      const grid = tile.spec.sources.map((s) => this.#gridOfCube(s.cube)).find((g) => g !== null) ?? null;
      this.#menuFor = (item) => {
        if (item.id === 'tile.properties') tile.view?.openSettings();
        this.#onTileCreate(view.id, grid, item);
        this.#onTileView(view.id, item);
      };
      this.#menu.show([
        { label: '', items: [this.#tileCreate(grid, 'Its grids are gone: it reads their queries as they were')] },
        { label: '', items: [{ id: 'tile.properties', label: 'Properties\u2026', ...(tile.view ? {} : { disabled: true, hint: 'Show its sheet first' }) }] },
        { label: '', items: [this.#tileView(view.id)] },
      ], event.clientX, event.clientY);
    });
    this.#series.set(view.id, { spec: view.spec, body, view: null, paint, stale: false });
    this.#place({ id: view.id, title: view.title ?? timeseriesTitle(view.id), element: body, actions: [], state: pill });
    paint();
    this.#drawTimeseries(view.id);
  }

  /**
   * A timeseries tile's viewer (ui/timeseries-view.ts, a chunk of its own fetched with the first drawn) over the page's
   * grids: each source's by its cube, several opened together for a series that combines them (CubePageOptions.together),
   * and a change of its spec told as the page's (setTimeseries) -- the step then puts it back as the page has it (`sync`).
   * A tile on a sheet not shown is drawn when its sheet is (`#drawShownSeries`): until then it asks its grids nothing --
   * a page of many sheets of them (the timeseries gallery's) asks only what is looked at (docs/TENETS.md: lazy Work).
   */
  #drawTimeseries(id: string): void {
    if (this.#sheetOf(id) !== this.#shown) return;
    void lazily(() => import('../ui/timeseries-view.ts')).then(({ TimeseriesView }) => {
      const tile = this.#series.get(id);
      if (!tile || tile.view || this.#disposed || this.#sheetOf(id) !== this.#shown) return;
      tile.view = new TimeseriesView(tile.body, {
        grid: (cube) => this.#tsGrid(cube),
        // each source's filter as the page holds its cube: its grid's (on the board or kept off it), else as the page
        // saved it, its source not opened -- the rule Python's freeze reads the page's cubes by (headless timeseriesEdit)
        heldFilter: (cube) => {
          const grid = this.#tsGrid(cube);
          if (grid) return { filter: grid.filter };
          const saved = this.#options.unopenedCube?.(cube);
          return saved ? { filter: saved.query.filter } : undefined;
        },
        // the grids of its own sheet first (a board's, as the page has it), then the rest
        grids: () => ((on) => (on.kind === 'bands' ? tiles(this.#layoutOf(on)) : sheetTiles(on)))(this.#sheetOf(id) ?? this.#shown)
          .filter((t) => this.#grids.has(t))
          .concat([...this.#grids.keys()].filter((t) => this.#sheetOf(t) !== this.#sheetOf(id)))
          .flatMap((t) => this.#tsGrid(this.#cubeOf(t)) ?? []),
        together: (cubes) => {
          const together = this.#options.together;
          if (!together) return Promise.reject(new Error('a series of several grids is made on a page of DataCube\'s app, whose grids read this tab\'s database'));
          return together(cubes);
        },
        setSpec: (spec) => {
          if (JSON.stringify(spec) === JSON.stringify(tile.spec)) return;
          if (!this.#tell('Timeseries Change', pageCommand('setTimeseries', { tile: id, spec }))) tile.view?.update(tile.spec);
        },
        // its editors' windows among the page's, titled by its tile's name -- a chart's options' rule
        windowHost: this.#options.windowHost ?? this.#options.host,
        title: () => this.title(id) ?? timeseriesTitle(id),
        // as the code that builds it names it: its own title, each source's grid by its title and where its rows come
        // from (its cube's source: the program opens a grid of its own over the same rows)
        codeOf: (spec) => {
          const grids: Record<string, { readonly title: string; readonly source?: CubeSource }> = {};
          for (const s of spec.sources) {
            const g = this.#gridOfCube(s.cube);
            const source = g === null ? undefined : this.documentOf(g)?.source;
            if (g !== null) grids[s.cube] = { title: this.title(g) ?? this.#titles.get(g) ?? g, ...(source !== undefined ? { source } : {}) };
          }
          return { title: this.givenTitle(id) ?? null, grids };
        },
      }, tile.spec);
    });
  }

  /** The timeseries tiles of the sheet shown not yet drawn, drawn: a sheet shown, a step that leaves one shown. */
  #drawShownSeries(): void {
    for (const [id, tile] of this.#series) {
      if (!tile.view) this.#drawTimeseries(id);
      else if (tile.stale && this.#sheetOf(id) === this.#shown) {
        tile.stale = false;
        tile.view.refresh(0);
      }
    }
  }

  /** A timeseries tile off the board, as a step leaves it. */
  #dropTimeseries(id: string): void {
    const tile = this.#series.get(id);
    if (!tile) return;
    tile.view?.dispose();
    this.#series.delete(id);
    this.#take(id);
    // a removed grid's cube it alone read goes with it
    this.#releaseKept();
    this.#options.onChange();
    this.#afterRemove();
  }

  /** Take a grid off the page as its own tile's remove button does: a grid's menu's Remove from Page. */
  removeGrid(id: string): void {
    if (this.#grids.get(id)?.added) this.#removeTile(id);
  }

  /**
   * A GRID OFF THE BOARD as a step leaves it (`sync`): the charts following it are gone with it (the step has none);
   * each frozen one stays, detached -- with its own copy of the grid's query as it is now, so it still draws and
   * refreshes.
   */
  #dropGrid(id: string): void {
    const grid = this.#grids.get(id);
    if (!grid?.added) return;
    const query = grid.source.snapshot;
    let keeps = false;
    for (const [chartId, chart] of [...this.#charts]) {
      if (chart.link.grid !== id) continue;
      if (!chart.panel.frozen) {
        chart.panel.dispose();
        this.#charts.delete(chartId);
        this.#take(chartId);
        continue;
      }
      chart.conditions = [];
      this.#paintSelection(chartId);
      chart.link.detached = { snapshot: query, from: id };
      chart.link.grid = null;
      chart.paint();
      keeps = true;
    }
    grid.added.stop();
    // a frozen chart or a timeseries still reads it: kept off the board, to run their queries, until the last of them goes
    // (the page keeps its cube as long: page-document.ts readsCube) -- the timeseries drawing on from it
    if (keeps || this.#seriesRead(id)) this.#kept.set(id, grid.added.cube);
    else grid.added.cube.dispose();
    this.#grids.delete(id);
    this.#take(id);
    this.#refreshSeries(id);
    // its timeseries say they read it as it was
    for (const tile of this.#series.values()) tile.paint();
    this.#options.onChange();
    this.#afterRemove();
  }

  /**
   * SHOW GRID (D7; Python's chart.grid(), by the same rule): a chart's grid shown -- the one it follows, the page as it
   * is; or, frozen or detached, a grid of its own grouping and query (chart-spec.ts chartGrid: its column across and its
   * split the Rows, its values the Column Properties, its selection off the filter), made beside it in one step that
   * has the chart follow it: the grid added, the chart on its cube (`followGrid`: its selection off its old grid's filter
   * and its page filter off, as a click on nothing takes them -- the rule's, its old grid following the step), following
   * it as it was drawn (`setChart`).
   */
  #showGrid(chartId: string): void {
    const chart = this.#charts.get(chartId);
    const spec = chart?.panel.spec;
    if (!chart || !spec) return;
    if (chart.link.grid !== null && showGridWay(spec, true) === 'reveal') {
      this.#reveal(chart.link.grid);
      return;
    }
    if (this.#ownGridRefusal() !== null) return;
    const source = this.#sourceOf(chart.link);
    const made = chartGrid({ snapshot: source.snapshot, config: source.configuration }, spec, chart.conditions);
    const from = chart.link.grid ?? chart.link.detached?.from;
    if (from === undefined) return;
    this.addGridOver(this.#gridLike(from, source, made.snapshot, made.configuration), {
      near: chartId, label: 'Show Grid',
      // the one rule Python's chart.grid() asks too (page-gestures.ts showGridCommands)
      after: (grid) => showGridCommands(chartId, grid, made.spec),
    });
  }

  /**
   * Why a chart gets no grid of its own here, in words, or null. A cube alone's board saves its own grid and its charts
   * alone (app.ts pageViews): a chart moved onto another grid there would be lost when the cube is saved, so it is never
   * moved -- Show Grid only shows the grid a chart reads.
   */
  #ownGridRefusal(): string | null {
    return this.#options.grid !== undefined ? "A cube on its own keeps its one grid: a chart's own grid needs a page." : null;
  }

  /** A sheet's layout as the page has it (P7): set whenever the board is put at a page. */
  #layoutOf(sheet: BoardSheet): Bands {
    const layout = this.#pageLayouts.get(sheet.id);
    if (layout === undefined) throw new Error(`sheet ${sheet.id} has no layout of the page's`);
    return layout;
  }

  /**
   * A sheet's layout of the page's as its board draws it (P7): each chart with no spec yet (no page's tile) under its
   * grid, as a new chart is placed (bands.ts add), and the tile each stack shows in front as the board shows it (what is looked at, never
   * saved: the design's §5, 7) -- every tile kept as it is on the board, none made again.
   */
  #drawn(sheet: BoardSheet, layout: Bands): Bands {
    let drawn = layout;
    const on = new Set(tiles(sheet.board.layout));
    for (const [id, chart] of this.#charts) {
      if (!tiles(drawn).includes(id) && on.has(id) && !chart.added) {
        drawn = addTile(drawn, id, chart.link.grid ?? undefined, sheet.board.columns, this.#chartsOnly);
      }
    }
    return fronted(drawn, sheet.board.layout);
  }

  // -- selections: click-to-filter ----------------------------------------------

  /**
   * A chart's SELECTION: the mark clicked last, as conditions on the grid's filter (one per
   * column the mark names, ANDed on), owned by that chart. A click on another mark replaces them
   * rather than piling more on (two clicks must not filter to nothing); a click on the same mark,
   * or the chip naming it in the chart's title bar, takes them off. Each is ONE change -- one undo
   * step, refused as a whole -- by the one rule Python's chart.select asks too (page-filters.ts chartPicked, toggled as
   * a click is): on a grid over a class, the conditions on its properties the chart's page filter.
   */
  #select(id: string, conditions: readonly FilterNode[] | null): void {
    const chart = this.#charts.get(id);
    if (!chart || chart.link.grid === null) return;
    const on = this.#options.pickOn?.(chart.link.grid);
    // the chart said the mark as conditions (chart-spec.ts `markConditions`): a value, a period's or a bin's range
    const picked = chartPicked({ source: on?.source, chart: id, conditions, old: chart.conditions, filters: on?.filters ?? [], toggle: true });
    if (picked.refilter === undefined && picked.page.length === 0) return;
    const label = picked.page.every((c) => c.kind === 'removePageFilter' || (c.kind === 'select' && c.conditions.length === 0))
      ? 'clear chart selection' : 'filter to chart mark';
    // its conditions on its grid's filter, that grid's (its refilter, told by the grid); the page filter and the chart's
    // selection, the page's (painted by `sync`)
    if (picked.refilter?.kind === 'refilter') this.#sourceOf(chart.link).refilter(picked.refilter.remove, picked.refilter.add, label);
    if (picked.page.length > 0) this.#tell(label, ...picked.page);
  }

  #paintSelection(id: string): void {
    const chart = this.#charts.get(id);
    if (!chart) return;
    const source = this.#sourceOf(chart.link);
    // a value as `region: EMEA`; a range's ends (a period, a bin, a brush) as `sold ≥ 2024-03-01`
    const sign: Partial<Record<string, string>> = { greaterThanEqual: '\u2265', greaterThan: '>', lessThan: '<', lessThanEqual: '\u2264' };
    const words = chart.conditions.map((c) => c.kind === 'condition'
      ? (c.operator === 'isEmpty' ? `${source.label(c.column)}: (empty)`
        : sign[c.operator] !== undefined ? `${source.label(c.column)} ${sign[c.operator]} ${String(c.value)}`
          : `${source.label(c.column)}: ${String(c.value)}`)
      : '');
    chart.chip.hidden = words.length === 0;
    chart.chip.replaceChildren(this.#doc.createTextNode(words.join(', ')), chromeIcon(this.#doc, 'close'));
    chart.chip.title = 'Filtered to this; click to clear';
    chart.chip.setAttribute('aria-label', `Clear the filter to ${words.join(', ')}`);
  }
}

// the page's rules and state, for a cube alone's board (app.ts): fetched with the board, never with a grid alone
export { applyToState, idsOf, takenIds } from '../page-commands.ts';
export { PageState } from '../page-state.ts';

/** Pages made so far: each its own crosshair group. */
let crosshairs = 0;

/** `layout` with each stack's tile in front as `shown` has it, where that stack is still one: which tab is in front is
 *  what is looked at, never saved (the design's §5, 7), and a step keeps it. */
function fronted(layout: Bands, shown: Bands): Bands {
  return leaves(shown).reduce((at, leaf) => ('stack' in leaf && leaf.front !== undefined ? bringToFront(at, leaf.front) : at), layout);
}

/** `layout` without the tiles `leave` names: each one's place closed by its neighbours, as a removal closes it. */
function without(layout: Bands, leave: (id: string) => boolean): Bands {
  return tiles(layout).filter(leave).reduce((rest, id) => removeTile(rest, id), layout);
}
