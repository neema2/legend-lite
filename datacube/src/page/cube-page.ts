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
import { MenuView } from '../ui/menu-view.ts';
import type { MenuItem } from '../ui/menu.ts';
import { BandBoard, type BandTile, type LayoutMove } from '../layout/band-board.ts';
import { type Bands, NEW_SHEET, add as addTile, bringToFront, cells, leaves, places, remove as removeTile, tiles } from '../layout/bands.ts';
import type { LayoutPicker } from '../ui/layout-picker.ts';
import { CHART_STATES, chartGrid, MAP_MARKS, showGridWay, type ChartSpec, type ChartState } from '../chart-spec.ts';
import { chartPicked, type PageFilter } from '../page-filters.ts';
import type { CubeConfiguration } from '../config.ts';
import type { GridShown } from '../chart-option.ts';
import type { ExportPage, ExportTile } from '../export-model.ts';
import {
  FIRST_SHEET, PAGE_CUBE, type ChartView, type PageSheet, type PageView, type PageViews,
} from '../page-document.ts';
import {
  layoutCommand, movedCommand, moveSheetCommand, moveToSheetCommands, pageCommand, renameSheetCommand, savedLayout, showGridCommands,
} from '../page-gestures.ts';
import { chartTitle, gridTitle, nextPageId } from '../page-ids.ts';
import type { PageCommand } from '../page-commands.ts';
import type { CubeDocument, CubeSource } from '../cube-document.ts';
import type { GridCopy, PageDocument } from '../page-document.ts';
import { withoutConditions, type CubeSnapshot, type FilterNode } from '../snapshot.ts';
import type { SettingValues } from '../settings.ts';
import type { ResultTable, Scalar } from '../../../engine-client/src/result.ts';
import type { Lambda } from '../../../pure-protocol/src/index.ts';
import { lazily } from '../lazily.ts';

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

/** What a chart needs from the grid it is made from (a CubeApp). */
export interface ChartSource {
  /** The grid's query as it is now. */
  readonly snapshot: CubeSnapshot;
  /** The grid's configuration as it is now: its Column Properties among it. */
  readonly configuration: CubeConfiguration;
  /** Run a query the grid did not plan, through the grid's own runner. */
  run(query: Lambda, snapshot: CubeSnapshot, signal: AbortSignal): Promise<ResultTable>;
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
export interface SpawnOptions {
  /** Its tile's id on the page (a page of its own: its host keeps what it knows of the grid by it). */
  readonly id?: string;
  readonly onChart?: () => void;
  readonly onNewGrid?: () => void;
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
  /** Its part of its tile's header: its source, its Live/Snapped pill, its menu. */
  tileHead(): HTMLElement;
  /** The grid as a saved cube, named `name`: undefined when it cannot say its source. */
  cubeDocument?(name: string): CubeDocument | undefined;
}

export interface CubePageOptions {
  /** Where the board goes. */
  readonly host: HTMLElement;
  /**
   * The cube's own grid, when the board is a cube's (a CubeApp alone): its first tile, not removable -- the cube is the
   * host's. A page of its own has none: every grid is added alike.
   */
  readonly grid?: {
    readonly element: HTMLElement;
    readonly source: ChartSource;
    /** The grid's own part of its tile's header: its source, its Live/Snapped pill. */
    readonly head?: HTMLElement;
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
  /**
   * What a chart's click on a mark of grid `grid` is decided on (page-filters.ts chartPicked): the grid's source -- on a
   * grid over a class, the conditions on its properties are a page filter (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md
   * §16.2) -- and the page's filters now. Absent (a cube alone), the click is its grid's alone.
   */
  readonly pickOn?: (grid: string) => { readonly source: CubeSource | undefined; readonly filters: readonly PageFilter[] };
}

/** An edit of the page's structure: its page commands, and what it is called (the page's Undo names it). */
export interface PageEdit {
  readonly label: string;
  readonly commands: readonly PageCommand[];
  /** False: no step of the page's history (a snap or going live: where the rows are, not what the page is). */
  readonly step?: false;
}

/** A sheet as the page has it: its id, its name when someone gave it one, and its tiles in reading order. */
export interface SheetInfo {
  readonly id: string;
  readonly name?: string;
  readonly tiles: readonly string[];
}

/** A SHEET: a whole screen of tiles, on a board of its own -- its bands, its fit, its layout undo. */
interface Sheet {
  readonly id: string;
  /** Its name, when someone gave it one. */
  name: string | undefined;
  readonly host: HTMLElement;
  readonly board: BandBoard;
  /** Each arrangement by hand is one step (the design's §3.3): the layouts before the steps done, and after those undone. */
  readonly steps: { undone: Bands[]; redone: Bands[] };
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
  /** This page's crosshair group (chart-render.ts setGroup): its own, so two pages on one screen do not share. */
  readonly #crosshair = `page-${(crosshairs += 1)}`;
  /** A chart's right-click menu: its Options, Show Grid, Remove. */
  readonly #menu: MenuView;
  #menuFor: ((item: MenuItem) => void) | null = null;

  constructor(options: CubePageOptions) {
    this.#options = options;
    this.#doc = options.host.ownerDocument;
    this.#menu = new MenuView(this.#doc, { onSelect: (item) => this.#menuFor?.(item) });
    this.#shown = this.#newSheet(FIRST_SHEET);
    this.#shown.host.hidden = false;
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
      // the cube is the host's: the page's document is this grid's cube
      removable: false,
    });
    // a screenful: the grid had the whole page until now
    this.#shown.board.setLayout({ fit: true, bands: [{ height: 1, node: { tile: GRID } }] });
    this.#pageLayouts.set(FIRST_SHEET, this.#shown.board.layout);
  }

  /** A sheet, after the others, not shown: a board of its own in an element of its own. */
  #newSheet(id: string, name?: string): Sheet {
    const host = this.#doc.createElement('div');
    host.className = 'dc-sheet';
    host.dataset['sheet'] = id;
    host.hidden = true;
    this.#options.host.append(host);
    const steps: Sheet['steps'] = { undone: [], redone: [] };
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
    const sheet: Sheet = { id, name, host, board, steps };
    this.#sheets.push(sheet);
    return sheet;
  }

  /** The next id of `kind` (page-ids.ts nextPageId): after every id the page's state has held and every one on the board
   *  now -- a tile a gesture made that its command has not placed yet among them. */
  #nextId(kind: 'grid' | 'chart' | 'sheet'): string {
    const board = [...this.#sheets.map((s) => s.id), ...this.#grids.keys(), ...this.#charts.keys(), ...this.#kept.keys(),
      ...this.#unavailable.keys(), ...this.#pending.keys()];
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
  #tellLayout(sheet: Sheet): void {
    if (this.#quiet > 0) return;
    this.#tell('Layout Change', layoutCommand(sheet.id, sheet.board.layout, this.#savedTiles()));
  }

  /** A sheet's layout changed by hand, told as what the hand did (page-gestures.ts movedCommand: a stack, a preset
   *  picked, else the layout whole). `before`: the layout the hand changed. */
  #tellMove(sheet: Sheet, before: Bands, move: LayoutMove | undefined): void {
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
    sheet.board.add(spec, by === sheet ? near : undefined, this.#charts.has(spec.id) ? this.#chartsOnly : undefined);
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
    return this.#sheets.map((s) => ({ id: s.id, ...(s.name !== undefined ? { name: s.name } : {}), tiles: tiles(s.board.layout) }));
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
    const sheetFor = new Map(to.sheets.flatMap((s) => tiles(s.layout).map((t) => [t, s.id] as const)));
    const sheetsBefore = new Set(this.#sheets.map((s) => s.id));
    const sheetOrder = this.#sheets.map((s) => s.id);
    const board = (id: string | undefined): Sheet | undefined => this.#sheets.find((s) => s.id === id);
    this.#quietly(() => {
      // sheets it has and the screen does not
      for (const s of to.sheets) if (!board(s.id)) this.#newSheet(s.id, s.name);
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
      // what it could not open, back: kept as the step has it, in its place
      for (const view of to.views) {
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
        if (view.kind !== 'grid' || this.#grids.has(view.id) || this.#unavailable.has(view.id)) continue;
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
        const title = view.kind === 'grid' ? view.title ?? this.#titles.get(view.id) : view.title;
        if (title !== undefined && sheet.board.title(view.id) !== title) sheet.board.rename(view.id, title);
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
        const spec = this.#specs.get(view.id);
        if (on && on !== sheet && spec) {
          sheet.board.remove(view.id);
          on.board.add({ ...spec, title: title ?? spec.title });
        }
      }
      // sheets it does not have, with what is left on them (no tile it holds: a moment's work)
      for (const sheet of [...this.#sheets]) {
        if (to.sheets.some((s) => s.id === sheet.id)) continue;
        const on = (): string[] => tiles(sheet.board.layout);
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
        const { kind: _kind, ...bands } = s.layout;
        const sheet = board(s.id)!;
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
    // a sheet brought back is shown: where the change is
    const back = this.#sheets.find((s) => !sheetsBefore.has(s.id));
    if (back) this.showSheet(back.id);
    this.#releaseKept();
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
    this.#options.onSheets?.();
  }

  /** A sheet after the last, shown (`sync` shows a sheet that comes). */
  addSheet(): string {
    const id = this.#nextId('sheet');
    this.#tell('Add Sheet', pageCommand('addSheet', { id, name: null }));
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
    const steps = moveToSheetCommands(tile, sheet, to === null, target?.board.columns ?? null);
    if (this.#saved(tile)) {
      this.#tell('Move to Sheet', ...steps);
    } else {
      // a tile the page does not save (a chart with no spec yet): its new sheet the page's, the move the board's alone
      const added = steps.filter((c) => c.kind === 'addSheet');
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
      const sheet = this.#sheets.find((x) => x.id === s.id) ?? this.#newSheet(s.id);
      sheet.name = s.name;
      for (const tile of tiles(s.layout)) this.#planned.set(tile, sheet);
    }
    // a tile already on the page (a cube's own grid) onto its saved sheet
    for (const [tile, sheet] of this.#planned) {
      const on = this.#sheetOf(tile);
      if (on && on !== sheet) {
        const spec = this.#specs.get(tile)!;
        on.board.remove(tile);
        sheet.board.add({ ...spec, title: on.board.title(tile) ?? spec.title });
      }
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

  /** The grids kept off the board for the frozen charts that still read them. */
  get kept(): ReadonlyMap<string, SpawnedGrid> {
    return this.#kept;
  }

  /** Every tile on the page, sheet by sheet, each sheet's in reading order. */
  get tileIds(): readonly string[] {
    return this.#sheets.flatMap((s) => tiles(s.board.layout));
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
  showAlone(slot: HTMLElement | null): void {
    // the sheet shown: the others' tiles are each in its own frame
    for (const sheet of this.#sheets) if (sheet !== this.#shown) sheet.board.setAlone(null, null);
    const ids = tiles(this.#shown.board.layout);
    const lone = slot !== null && ids.length === 1 && this.#grids.has(ids[0]!) ? ids[0]! : null;
    this.#shown.board.setAlone(lone, slot);
  }

  /**
   * The layouts (ui/layout-picker.ts) by `anchor`, each arranging every tile on the page: from Arrange... in reading
   * order, or -- `first`, from a tile's own layout button -- with that tile in the first slot (the user, 2026-10-09: a
   * tile's layouts arranging only its band showed two tiles of four, and 2 x 2 came out 2, 1, 1).
   */
  showLayouts(anchor: HTMLElement, first?: string): void {
    // a locked page offers no layouts: nothing moves by accident
    if (!this.#editing) return;
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
    const board = this.#shown.board;
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
    const { board, steps } = this.#shown;
    const before = steps.undone.at(-1);
    if (before === undefined || !this.#editing) return;
    steps.undone = steps.undone.slice(0, -1);
    steps.redone = [...steps.redone, board.layout];
    board.setLayout(before);
    this.#tellLayout(this.#shown);
    board.say('Layout change undone.');
    this.#options.onChange();
  }

  redoLayout(): void {
    const { board, steps } = this.#shown;
    const after = steps.redone.at(-1);
    if (after === undefined || !this.#editing) return;
    steps.redone = steps.redone.slice(0, -1);
    steps.undone = [...steps.undone, board.layout];
    board.setLayout(after);
    this.#tellLayout(this.#shown);
    board.say('Layout change redone.');
    this.#options.onChange();
  }

  /** Whether Undo Layout would do something: a step to undo on the sheet shown, on a page that is not locked. */
  get canUndoLayout(): boolean {
    return this.#shown.steps.undone.length > 0 && this.#editing;
  }

  get canRedoLayout(): boolean {
    return this.#shown.steps.redone.length > 0 && this.#editing;
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
    return this.#charts.size + this.#grids.size - (this.#options.grid ? 1 : 0);
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
    const cube = make(host, this.#spawnOptions(id));
    const stops = [
      cube.on('view', () => {
        this.#refreshGrid(id);
        this.#reconcileGrid(id, cube.snapshot.filter);
      }),
      // its own changes are the page's: "changed since saved" re-reads the page
      cube.on('change', () => this.#options.onChange()),
      // where its rows are is the page's too: its copies (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §18.8)
      // its own snap or going live told as the page's command (the copy it now reads, or none)
      cube.on('moved', () => {
        // a grid whose cube the page cannot hold (it cannot say its source) has no copies of the page's
        if (this.#quiet > 0 || !this.#saved(id) || this.documentOf(id) === undefined) return;
        const copy = cube.copy;
        this.#options.onPageEdit({ label: copy ? 'Snap' : 'Go Live', step: false,
          commands: [copy ? pageCommand('snap', { grid: id, copy }) : pageCommand('live', { grid: id })] });
      }),
      cube.on('plane', () => this.#options.onChange()),
    ];
    this.#grids.set(id, { source: cube.chartSource(), added: { cube, stop: () => stops.forEach((stop) => stop()) } });
    // numbered by its id, as the page's one rule names a grid with no title of its own (page-commands.ts renameTile,
    // B2): on a cube alone's board too, whose own grid is "Grid" (the S3 audit's 9)
    const made = gridTitle(id);
    this.#titles.set(id, made);
    const near = how.near ?? (this.#options.grid ? GRID : undefined);
    const beside = near !== undefined && (this.#grids.has(near) || this.#charts.has(near)) ? near : undefined;
    const tile: BandTile = { id, title: how.title ?? made, element: host, actions: [cube.tileHead()] };
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
   * A grid KEPT OFF THE BOARD for the frozen charts that read it (a saved page's grid removed while they were frozen):
   * made, opened and listened to by nothing, until its last chart goes. Its charts come back detached (`restore`).
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
      onChart: () => this.openChart(undefined, id),
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

  /** Whether a chart is on grid `id`'s sheet: its export then lays out that sheet, the grid its table. */
  #chartsBeside(id: string): boolean {
    const sheet = this.#sheetOf(id);
    return [...this.#charts.keys()].some((chart) => this.#sheetOf(chart) === sheet);
  }

  /** The sheets a tile is not on, as their tabs say them: where its Move to Sheet can take it. */
  #otherSheets(tile: string): { id: string; label: string }[] {
    const label = this.#options.sheetLabel;
    const on = this.#sheetOf(tile);
    return label ? this.#sheets.filter((s) => s !== on).map((s) => ({ id: s.id, label: label(s.id) })) : [];
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
    // THE CHART'S HEADER, simple (the user, 2026-09-30): its title, a pill -- as a grid's Live / Snapped -- that says
    // its state and changes it, the filter chip while a click is filtering its grid, and its x. Everything else is a
    // right-click away. Its states, one word each (the user, 2026-10-23: frozen charts kept their grouping but not
    // their filter): DYNAMIC follows the grid; PINNED keeps its own grouping (a hand on its axes pins it) and follows
    // the grid's filters and the page's clicks; FROZEN keeps its grouping and the grid's filter as they were.
    const pill = doc.createElement('button');
    pill.type = 'button';
    pill.className = 'dc-titlebar-toggle';
    pill.setAttribute('aria-haspopup', 'menu');
    const paint = (): void => {
      const state = panel.state;
      const detached = link.grid === null;
      pill.textContent = detached ? 'Detached' : STATE_WORDS[state].label;
      pill.classList.toggle('dc-on', state === 'dynamic' && !detached);
      pill.classList.toggle('dc-fixed', detached);
      pill.disabled = detached;
      // a map is placed by its own columns (chart-spec.ts live): the grid's grouping leaves it as it is
      const map = panel.spec !== null && MAP_MARKS.has(panel.spec.mark) && state === 'dynamic';
      pill.title = detached
        ? 'Its grid was removed: this chart keeps its own copy of that grid\'s query. Right-click > Show Grid to show it in a grid of its own.'
        : `${STATE_WORDS[state].label}: ${map ? 'it re-draws as the grid is filtered; a map is placed by its own columns, not the grid\'s grouping' : STATE_WORDS[state].says}. Click to change.`;
    };
    const panel = new ChartPanel(body, {
      // its spec changed by hand -- its options, a drill, its state -- is a step of the page's
      onState: () => this.#chartChanged(id, 'Chart Change'),
      onSpec: () => this.#chartChanged(id, 'Chart Change'),
      ...(restore ? { initial: restore.spec } : {}),
      snapshot: () => source().snapshot,
      run: (query, snapshot, signal) => source().run(query, snapshot, signal),
      label: (value, column, type, headline) => source().format(value, column, type, headline),
      unit: (column) => source().unit?.(column),
      // the page's charts with the same column across share a crosshair
      crosshair: this.#crosshair,
      shown: () => source().shown(),
      // a detached chart has no grid's filter to put a click on
      onPick: (mark) => { if (link.grid !== null) this.#select(id, mark); },
      formOpen: false,
    });
    // its states offered, the one it is in checked, each said in a line
    pill.addEventListener('click', () => {
      if (link.grid === null) return;
      this.#menuFor = (item) => {
        const state = CHART_STATES.find((s) => item.id === `tile.state.${s}`);
        if (state !== undefined) panel.setState(state);
      };
      const at = pill.getBoundingClientRect();
      this.#menu.show([{ label: '', items: CHART_STATES.map((s) => ({
        id: `tile.state.${s}` as const, label: `${STATE_WORDS[s].label} \u2014 ${STATE_WORDS[s].says}`, checked: panel.state === s,
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
        if (item.id === 'tile.options') panel.toggleForm();
        if (item.id === 'tile.grid') this.#showGrid(id);
        if (item.id === 'tile.remove') this.#removeTile(id);
        if (item.id === 'sheet.toNew') this.moveToSheet(id, null);
        else if (item.id?.startsWith('sheet.to.')) this.moveToSheet(id, item.id.slice('sheet.to.'.length));
      };
      const sheets = this.#otherSheets(id);
      this.#menu.show([
        { label: '', items: [
          { id: 'tile.options', label: 'Options...' },
          { id: 'tile.grid', label: 'Show Grid', ...(way !== null && refused === null ? {} : { disabled: true, ...(refused ? { hint: refused } : {}) }) },
        ] },
        // a page of sheets: this chart onto another, or a new one (its grid stays where it is, and it follows it)
        // locked: offered disabled, with nothing under it
        ...(this.#options.sheetLabel ? [{ label: '', items: [{ label: 'Move to Sheet', ...(this.#editing ? { submenu: [
          ...sheets.map((s) => ({ id: `sheet.to.${s.id}` as const, label: s.label })),
          { id: 'sheet.toNew' as const, label: 'New Sheet', ...(sheets.length > 0 ? { separated: true } : {}) },
        ] } : { disabled: true }) }] }] : []),
        { label: '', items: [{ id: 'tile.remove', label: 'Remove' }] },
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
    const actions = [pill, chip];
    if (restore) {
      // on its saved sheet, placed by that sheet's layout once every view is on the page (`restore`, `sync`)
      this.#place({ id, title: restore.title, element: body, actions });
      return;
    }
    const tile: BandTile = { id, title: chartTitle(id), element: body, actions };
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
    for (const id of this.tileIds.filter((t) => this.#grids.has(t) || this.#unavailable.get(t)?.kind === 'grid')) {
      const lost = this.#unavailable.get(id);
      if (lost) {
        views.push(lost);
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
    // each sheet's layout of what is saved: the other tiles' places closed up, as if they had been removed
    // (page-gestures.ts savedLayout, the rule a layout is told by too)
    const saved = new Set(views.map((v) => v.id));
    return {
      views,
      sheets: this.#sheets.map((s) => ({
        id: s.id,
        ...(s.name !== undefined ? { name: s.name } : {}),
        // as the page has it (an editing grid never touches it: P7); a stack as it is saved, reopening on its first tab
        // (the design's §5, 7)
        layout: savedLayout(this.#layoutOf(s), saved),
      })),
    };
  }

  /**
   * Put a saved page's views back: the grids' titles, the charts -- each following the grid `gridOf` finds for its
   * cube, or detached when that grid is kept off the board -- and the layout. The grids are on the page already (a
   * cube's own; a page's, added under their saved ids).
   */
  restore(page: PageViews, gridOf: (cube: string) => string | undefined = (cube) => (cube === PAGE_CUBE ? GRID : undefined),
    unopened: (cube: string) => string | undefined = () => undefined): void {
    // its sheets, if the grids were not put on them as they were added (a cube alone's own grid)
    this.setSheets(page.sheets);
    for (const view of page.views) {
      if (view.kind === 'grid' && view.title && this.#grids.has(view.id)) this.#sheetOf(view.id)?.board.rename(view.id, view.title);
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
      const { kind: _kind, ...bands } = saved.layout;
      const sheet = this.#sheets.find((s) => s.id === saved.id);
      sheet?.board.setLayout(bands);
      if (sheet) this.#pageLayouts.set(sheet.id, sheet.board.layout);
    }
    this.#planned.clear();
    // a page reopens on its first sheet (what is looked at is not saved: the design's §5, 7)
    this.showSheet(this.#sheets[0]!.id);
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
    const grid = cells(bringToFront(without(fronted(this.#layoutOf(sheet), sheet.board.layout),
      (id) => this.#grids.has(id) && id !== table), table));
    const tiles = grid.tiles.map((t): ExportTile => {
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
    this.#menu.close();
    this.#picker?.close();
    for (const chart of this.#charts.values()) chart.panel.dispose();
    this.#charts.clear();
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

  /** A grid kept off the board that no chart reads any more goes. */
  #releaseKept(): void {
    for (const [id, cube] of [...this.#kept]) {
      if ([...this.#charts.values()].some((c) => c.link.detached?.from === id)) continue;
      cube.dispose();
      this.#kept.delete(id);
    }
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
    return this.charts === 0 && (this.#options.grid !== undefined || this.#grids.size === 0);
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
    // a frozen chart still reads it: kept off the board, to run its query, until that chart goes
    if (keeps) this.#kept.set(id, grid.added.cube);
    else grid.added.cube.dispose();
    this.#grids.delete(id);
    this.#take(id);
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
  #layoutOf(sheet: Sheet): Bands {
    const layout = this.#pageLayouts.get(sheet.id);
    if (layout === undefined) throw new Error(`sheet ${sheet.id} has no layout of the page's`);
    return layout;
  }

  /**
   * A sheet's layout of the page's as its board draws it (P7): each chart with no spec yet (no page's tile) under its
   * grid, as a new chart is placed (bands.ts add), and the tile each stack shows in front as the board shows it (what is looked at, never
   * saved: the design's §5, 7) -- every tile kept as it is on the board, none made again.
   */
  #drawn(sheet: Sheet, layout: Bands): Bands {
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
    chart.chip.textContent = `${words.join(', ')} ×`;
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
