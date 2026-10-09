// A PAGE OF TILES: grids and the charts made from them, on one board, laid out as bands (layout/band-board.ts;
// docs/DATACUBE_PAGES_DESIGN_2026_10_09.md). Every tile is placed, moved and arranged alike: a new chart or grid goes
// beside the tile it came from while its band has room (smart placement), else below it, and the layout picker
// arranges the whole page.
//
// Two owners, one board. A CUBE ALONE (a CubeApp embedded in a host: the Query app, a notebook) gets a board when its
// first chart opens, its own grid the first tile and not removable (`grid`). A PAGE OF ITS OWN (page/page-app.ts,
// DataCube's app) has no such grid: every grid is added alike (`addGridOver`), the first one removable as the fifth.
//
// Moved out of CubeApp so a grid and a chart are the same kind of thing -- a tile on a board --
// and a page can come to hold several grids (the user, 2026-09-30: "the grid is the source").
// A chart reaches its grid only through `ChartSource`: the grid's query as it is now, a way to
// run a query of its own, the grid's formats, and the grid's filter for click-to-filter.
//
// A chart FOLLOWS ITS GRID until frozen -- it re-draws as the grid is pivoted, grouped and
// filtered, badged so -- and any number may follow at once. A frozen chart keeps its grouping
// (its query and spec, not its data: it still refreshes and answers the grid's filter); Follow the
// grid makes it follow again. Open in grid edits a frozen chart's grouping in a grid of its own,
// on the board beside it, leaving its grid as it is; Update writes it back.

import { ChartPanel } from '../ui/chart-panel.ts';
import { MenuView } from '../ui/menu-view.ts';
import type { MenuItem } from '../ui/menu.ts';
import { BandBoard } from '../layout/band-board.ts';
import { type Bands, cells, remove as removeTile, tiles } from '../layout/bands.ts';
import type { LayoutPicker } from '../ui/layout-picker.ts';
import { followCube, measureName } from '../chart-spec.ts';
import type { GridShown, MarkKey } from '../chart-option.ts';
import type { ExportPage, ExportTile } from '../export-model.ts';
import { PAGE_CUBE, type ChartView, type PageView, type PageViews } from '../page-document.ts';
import { withoutConditions, type CubeSnapshot, type FilterNode, type Measure } from '../snapshot.ts';
import type { SettingValues } from '../settings.ts';
import type { ResultTable, Scalar } from '../../../engine-client/src/result.ts';
import type { Lambda } from '../../../pure-protocol/src/index.ts';

/** How many arrangements Undo Layout goes back through. */
const LAYOUT_STEPS = 50;
/** The grid's tile, until renamed: the cube's name is already above the board. */
export const GRID_TILE_TITLE = 'Grid';
const GRID = 'grid';

/** What a chart needs from the grid it is made from (a CubeApp). */
export interface ChartSource {
  /** The grid's query as it is now. */
  readonly snapshot: CubeSnapshot;
  /** Run a query the grid did not plan, through the grid's own runner. */
  run(query: Lambda, snapshot: CubeSnapshot, signal: AbortSignal): Promise<ResultTable>;
  /** A value as the grid writes it. */
  format(value: Scalar, column: string, type: string | undefined): string;
  /** A column as the grid names it. */
  label(column: string): string;
  /** What the grid shows now (its rows, their tree, its visible columns): a treemap draws it. */
  shown(): GridShown | null;
  /** Take `old` off the grid's filter and put `add` on, as one change named `label`. */
  refilter(old: readonly FilterNode[], add: readonly FilterNode[], label: string): void;
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
}

/** A grid on a page (a CubeApp). */
export interface SpawnedGrid {
  readonly snapshot: CubeSnapshot;
  open(): Promise<void>;
  dispose(): void;
  /** What its charts need from it. */
  chartSource(): ChartSource;
  /** Told each time a view lands ('view'), and each time its state changes, a view or a width ('change'). */
  on(event: 'view' | 'change', fn: () => void): () => void;
  /** Its part of its tile's header: its source, its Live/Snapped pill, its menu. */
  tileHead(): HTMLElement;
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
  key: string;
  /** Its editing grid, while open: its tile and its cube. */
  editor?: { readonly tile: string; readonly grid: SpawnedGrid };
}

export class CubePage {
  readonly #doc: Document;
  readonly #options: CubePageOptions;
  readonly #board: BandBoard;
  /** The layouts, fetched the first time they are opened (a page that is never arranged never downloads them). */
  #picker: LayoutPicker | undefined;
  #disposed = false;
  readonly #charts = new Map<string, ChartTile>();
  readonly #grids = new Map<string, GridTile>();
  /** Grids removed from the board while a frozen chart still reads them: kept, off the board, to run its query. */
  readonly #kept = new Map<string, SpawnedGrid>();
  /** Each grid's title as it was made, so a saved page writes only a title someone gave it. */
  readonly #titles = new Map<string, string>();
  /** Each added grid's first open (its first view), for `opened`. */
  readonly #opening = new Map<string, Promise<void>>();
  #chartCount = 0;
  #gridCount = 0;
  /** Each arrangement by hand is one step (the design's §3.3): the layouts before the steps done, and after those undone. */
  #undone: Bands[] = [];
  #redone: Bands[] = [];
  /** A chart's right-click menu: its Options, Open in grid, Remove. */
  readonly #menu: MenuView;
  #menuFor: ((item: MenuItem) => void) | null = null;

  constructor(options: CubePageOptions) {
    this.#options = options;
    this.#doc = options.host.ownerDocument;
    this.#menu = new MenuView(this.#doc, { onSelect: (item) => this.#menuFor?.(item) });
    this.#board = new BandBoard(options.host, {
      onRemove: (tileId) => this.#removeTile(tileId),
      onChange: (_layout, before) => {
        this.#undone = [...this.#undone, before].slice(-LAYOUT_STEPS);
        this.#redone = [];
        options.onChange();
      },
      onRename: () => options.onChange(),
      onLayout: (tileId, anchor) => this.showLayouts(anchor, tileId),
      onUndo: (redo) => (redo ? this.redoLayout() : this.undoLayout()),
    });
    // a new page fits its window (the user, 2026-10-09: fit by default) -- and scrolls once its bands are too many to
    // fit (bands.ts draw's floor)
    this.#board.setLayout({ fit: true, bands: [] });
    const own = options.grid;
    if (!own) return;
    // a chart or another grid is added from the menus (right-click or the hamburger, Insert), not
    // from buttons on the tile (the user, 2026-09-30)
    this.#grids.set(GRID, { source: own.source });
    this.#titles.set(GRID, GRID_TILE_TITLE);
    this.#board.add({
      id: GRID,
      // the cube's own name is the page's title already, above the board
      title: GRID_TILE_TITLE,
      element: own.element,
      actions: own.head ? [own.head] : [],
      // the cube is the host's: the page's document is this grid's cube
      removable: false,
    });
    // a screenful: the grid had the whole page until now
    this.#board.setLayout({ fit: true, bands: [{ height: 1, node: { tile: GRID } }] });
  }

  /** The grids on the board, by tile id, each with its cube (none for a cube's own grid: it is the host's). */
  get grids(): ReadonlyMap<string, SpawnedGrid | undefined> {
    return new Map([...this.#grids].map(([id, g]) => [id, g.added?.cube]));
  }

  /** The grids kept off the board for the frozen charts that still read them. */
  get kept(): ReadonlyMap<string, SpawnedGrid> {
    return this.#kept;
  }

  /** Every tile on the board, in reading order. */
  get tileIds(): readonly string[] {
    return tiles(this.#board.layout);
  }

  /** A tile's title, as shown (renamed or not). */
  title(id: string): string | undefined {
    return this.#board.title(id);
  }

  /**
   * A LONE GRID IN THE PAGE'S BAR (a page of its own; the design's §3.1): while the board holds one tile and it is a
   * grid, that tile is drawn without its frame and its header's part -- its source, its pill, its menu -- moves into
   * `slot`, the right of the page's bar; otherwise (or `slot` null) every header is its tile's.
   */
  showAlone(slot: HTMLElement | null): void {
    const ids = this.tileIds;
    const lone = slot !== null && ids.length === 1 && this.#grids.has(ids[0]!) ? ids[0]! : null;
    this.#board.setAlone(lone, slot);
  }

  /**
   * The layouts (ui/layout-picker.ts) by `anchor`, each arranging every tile on the page: from Arrange... in reading
   * order, or -- `first`, from a tile's own layout button -- with that tile in the first slot (the user, 2026-10-09: a
   * tile's layouts arranging only its band showed two tiles of four, and 2 x 2 came out 2, 1, 1).
   */
  showLayouts(anchor: HTMLElement, first?: string): void {
    // a locked page offers no layouts: nothing moves by accident
    if (!this.#board.editing) return;
    if (this.#picker) {
      this.#showPicker(this.#picker, anchor, first);
      return;
    }
    void import('../ui/layout-picker.ts').then(({ LayoutPicker }) => {
      if (this.#disposed) return;
      this.#picker ??= new LayoutPicker(this.#options.host);
      this.#showPicker(this.#picker, anchor, first);
    });
  }

  #showPicker(picker: LayoutPicker, anchor: HTMLElement, first?: string): void {
    const board = this.#board;
    picker.show(anchor, {
      tiles: tiles(board.layout),
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
    const before = this.#undone.at(-1);
    if (before === undefined || !this.#board.editing) return;
    this.#undone = this.#undone.slice(0, -1);
    this.#redone = [...this.#redone, this.#board.layout];
    this.#board.setLayout(before);
    this.#board.say('Layout change undone.');
    this.#options.onChange();
  }

  redoLayout(): void {
    const after = this.#redone.at(-1);
    if (after === undefined || !this.#board.editing) return;
    this.#redone = this.#redone.slice(0, -1);
    this.#undone = [...this.#undone, this.#board.layout];
    this.#board.setLayout(after);
    this.#board.say('Layout change redone.');
    this.#options.onChange();
  }

  /** Whether Undo Layout would do something: a step to undo, on a page that is not locked. */
  get canUndoLayout(): boolean {
    return this.#undone.length > 0 && this.#board.editing;
  }

  get canRedoLayout(): boolean {
    return this.#redone.length > 0 && this.#board.editing;
  }

  /** Edit mode (tiles move, dividers drag) or view mode (nothing moves by accident). */
  get editing(): boolean {
    return this.#board.editing;
  }

  setEditing(editing: boolean): void {
    this.#board.setEditing(editing);
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
    return this.addGridOver((host, options) => source.spawn(host, source.snapshot, options), { near: from });
  }

  /**
   * A grid over ANOTHER SOURCE (New ▸ Source…): `make` builds it -- its own engine and planner,
   * over its own model -- in the tile's element. Then it is a grid like any other: its own charts,
   * its own New ▸ Grid (over its source), moved and removed the same way. It goes beside `near`, the grid it was asked
   * from, while that band has room (else at the bottom). A saved page's grid comes back under its own `id` and `title`.
   */
  addGridOver(
    make: (host: HTMLElement, options: SpawnOptions) => SpawnedGrid,
    how: { readonly near?: string; readonly id?: string; readonly title?: string } = {},
  ): string {
    const id = how.id ?? this.#freshGridId();
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
    ];
    this.#grids.set(id, { source: cube.chartSource(), added: { cube, stop: () => stops.forEach((stop) => stop()) } });
    // the cube's own grid is "Grid"; a page's grids are numbered as they come
    const made = this.#options.grid ? `Grid ${this.#grids.size}` : `Grid ${this.#gridNumber(id)}`;
    this.#titles.set(id, made);
    const near = how.near ?? (this.#options.grid ? GRID : undefined);
    this.#board.add({ id, title: how.title ?? made, element: host, actions: [cube.tileHead()] },
      near !== undefined && this.#grids.has(near) ? near : undefined);
    this.#board.reveal(id);
    this.#opening.set(id, cube.open());
    this.#options.onChange();
    this.#options.onTiles?.();
    return id;
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
    const cube = make(this.#doc.createElement('div'), {});
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
      onRemove: () => this.#removeGrid(id),
      exportPage: () => (this.charts > 0 || this.#grids.size > 1 ? this.exportPage(id) : undefined),
      ...(this.#options.onSettingsChanged ? { onSettingsChanged: this.#options.onSettingsChanged } : {}),
    };
  }

  /** A grid id not on the page, and its number. */
  #freshGridId(): string {
    let n = Math.max(this.#gridCount + 1, 1);
    while (this.#grids.has(`grid-${n}`) || this.#kept.has(`grid-${n}`) || this.#board.title(`grid-${n}`) !== undefined) n += 1;
    this.#gridCount = n;
    return `grid-${n}`;
  }

  #gridNumber(id: string): number {
    const n = /^grid-(\d+)$/.exec(id)?.[1];
    if (n !== undefined) this.#gridCount = Math.max(this.#gridCount, Number(n));
    return n !== undefined ? Number(n) : this.#grids.size;
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
    this.#chartCount += 1;
    const id = restore?.id ?? this.#freshChartId();
    const doc = this.#doc;
    const body = doc.createElement('div');
    body.className = 'dc-chart-tile';
    // what the chart is filtering the grid to, with a way out: shown only while it does
    const chip = doc.createElement('button');
    chip.type = 'button';
    chip.className = 'dc-tile-chip';
    chip.hidden = true;
    chip.addEventListener('click', () => this.#select(id, null));
    // THE CHART'S HEADER, simple (the user, 2026-09-30): its title, a Dynamic / Frozen pill -- as
    // a grid's Live / Snapped -- that says which and toggles it, the filter chip while a click is
    // filtering its grid, and its x. Everything else is a right-click away.
    const pill = doc.createElement('button');
    pill.type = 'button';
    pill.className = 'dc-titlebar-toggle';
    const paint = (): void => {
      const frozen = panel.frozen;
      const detached = link.grid === null;
      pill.textContent = detached ? 'Detached' : frozen ? 'Frozen' : 'Dynamic';
      pill.classList.toggle('dc-on', !frozen && !detached);
      pill.classList.toggle('dc-fixed', detached);
      pill.disabled = detached;
      pill.title = detached
        ? 'Its grid was removed: this chart keeps its own copy of that grid\'s query. Right-click > Open in grid to change it.'
        : frozen
          ? 'Frozen: it keeps its grouping as the grid is pivoted. Click to follow the grid again.'
          : 'Dynamic: it re-draws as the grid is pivoted, grouped and filtered. Click to freeze it as it is.';
    };
    const panel = new ChartPanel(body, {
      onFrozen: () => {
        paint();
        this.#options.onChange();
      },
      onSpec: () => this.#options.onChange(),
      ...(restore ? { initial: restore.spec } : {}),
      snapshot: () => source().snapshot,
      run: (query, snapshot, signal) => source().run(query, snapshot, signal),
      label: (value, column, type) => source().format(value, column, type),
      shown: () => source().shown(),
      // a detached chart has no grid's filter to put a click on
      onPick: (mark) => { if (link.grid !== null) this.#select(id, mark); },
      formOpen: false,
    });
    pill.addEventListener('click', () => {
      if (link.grid === null) return;
      // following again, it takes the grid's grouping: an open editing grid has nothing to edit
      if (panel.frozen) this.#closeEditor(id);
      panel.setFrozen(!panel.frozen);
    });
    // RIGHT-CLICK: the chart's own entries
    body.addEventListener('contextmenu', (event) => {
      event.preventDefault();
      const spec = panel.spec;
      const editable = panel.frozen && spec?.mark !== 'scatter' && spec?.mark !== 'treemap';
      this.#menuFor = (item) => {
        if (item.id === 'tile.options') panel.toggleForm();
        if (item.id === 'tile.edit') this.#openEditor(id);
        if (item.id === 'tile.remove') this.#removeTile(id);
      };
      this.#menu.show([
        { label: '', items: [
          { id: 'tile.options', label: 'Options...' },
          { id: 'tile.edit', label: 'Open in grid', ...(editable ? {} : { disabled: true }) },
        ] },
        { label: '', items: [{ id: 'tile.remove', label: 'Remove' }] },
      ], event.clientX, event.clientY);
    });
    paint();
    this.#charts.set(id, {
      panel,
      link,
      paint,
      chip,
      conditions: restore?.selection ? [...restore.selection] : [],
      key: restore?.selection ? JSON.stringify(restore.selection) : '',
    });
    if (restore?.selection) this.#paintSelection(id);
    const actions = [pill, chip];
    if (restore) {
      // placed by the page's layout, once every view is on the board (`restore`)
      this.#board.add({ id, title: restore.title, element: body, actions });
      return;
    }
    // beside its grid while that band has room, else in a band below it
    this.#board.add({ id, title: `Chart ${this.#chartCount}`, element: body, actions }, link.grid ?? undefined);
    this.#board.reveal(id);
    this.#options.onChange();
    this.#options.onTiles?.();
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
        chart.key = '';
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
    for (const id of this.#grids.keys()) {
      const cube = cubeOf(id);
      if (cube === undefined) continue;
      const title = this.#board.title(id);
      views.push({ id, kind: 'grid', cube, ...(title !== undefined && title !== this.#titles.get(id) ? { title } : {}) });
    }
    for (const [id, chart] of this.#charts) {
      const spec = chart.panel.spec;
      const reads = chart.link.grid ?? chart.link.detached?.from;
      const cube = reads === undefined ? undefined : cubeOf(reads);
      if (!spec || cube === undefined) continue;
      views.push({
        id,
        kind: 'chart',
        cube,
        title: this.#board.title(id) ?? id,
        spec,
        ...(chart.conditions.length > 0 ? { selection: chart.conditions } : {}),
      });
    }
    // the layout of what is saved: the other tiles' places closed up, as if they had been removed
    const saved = new Set(views.map((v) => v.id));
    return { views, layout: { kind: 'bands', ...without(this.#board.layout, (id) => !saved.has(id)) } };
  }

  /**
   * Put a saved page's views back: the grids' titles, the charts -- each following the grid `gridOf` finds for its
   * cube, or detached when that grid is kept off the board -- and the layout. The grids are on the page already (a
   * cube's own; a page's, added under their saved ids).
   */
  restore(page: PageViews, gridOf: (cube: string) => string | undefined = (cube) => (cube === PAGE_CUBE ? GRID : undefined)): void {
    for (const view of page.views) {
      if (view.kind === 'grid' && view.title && this.#grids.has(view.id)) this.#board.rename(view.id, view.title);
    }
    const charts = page.views.filter((v): v is ChartView => v.kind === 'chart');
    for (const chart of charts) {
      const grid = gridOf(chart.cube);
      if (grid !== undefined && (this.#grids.has(grid) || this.#kept.has(grid))) this.openChart(chart, grid);
    }
    const { kind: _kind, ...bands } = page.layout;
    this.#board.setLayout(bands);
    this.#chartCount = Math.max(this.#chartCount, ...charts.map((c) => Number(/^chart-(\d+)$/.exec(c.id)?.[1] ?? 0)));
  }

  /**
   * The page's tiles as an export of grid `table` lays them out: where each is, a chart as its picture, that grid as
   * its table. The other grids are not in it (an export holds one table); every chart is.
   */
  exportPage(table: string = GRID): ExportPage {
    const grid = cells(without(this.#pageLayout(), (id) => this.#grids.has(id) && id !== table));
    const tiles = grid.tiles.map((t): ExportTile => {
      const chart = this.#charts.get(t.id);
      const picture = chart?.panel.picture() ?? null;
      return {
        id: t.id,
        kind: chart ? 'chart' : 'grid',
        title: this.#board.title(t.id) ?? (chart ? t.id : GRID_TILE_TITLE),
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
    for (const chart of this.#charts.values()) {
      chart.editor?.grid.dispose();
      chart.panel.dispose();
    }
    this.#charts.clear();
    for (const grid of this.#grids.values()) {
      grid.added?.stop();
      grid.added?.cube.dispose();
    }
    this.#grids.clear();
    for (const cube of this.#kept.values()) cube.dispose();
    this.#kept.clear();
    this.#board.dispose();
  }

  // -- charts ---------------------------------------------------------------

  #button(text: string, title: string): HTMLButtonElement {
    const el = this.#doc.createElement('button');
    el.type = 'button';
    el.className = 'dc-tile-button';
    el.textContent = text;
    el.title = title;
    return el;
  }

  /** A chart id not on the board (a restored page's ids may run ahead of the count). */
  #freshChartId(): string {
    let n = this.#chartCount;
    while (this.#charts.has(`chart-${n}`)) n += 1;
    this.#chartCount = n;
    return `chart-${n}`;
  }

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
      run: (query, s, signal) => runs.run(query, s, signal),
      format: (value, column, type) => runs.format(value, column, type),
      label: (column) => runs.label(column),
      // detached, a treemap keeps the rows it last drew
      shown: () => null,
      refilter: () => {},
      spawn: (host, s, options) => runs.spawn(host, s, options),
    };
  }

  /** The page's layout without the charts' editing grids (a moment's work, not the page). */
  #pageLayout(): Bands {
    const editing = new Set([...this.#charts.values()].flatMap((c) => (c.editor ? [c.editor.tile] : [])));
    return without(this.#board.layout, (id) => editing.has(id));
  }

  /** Take a tile off the board: an editing grid (its chart left as it was), or a chart and its selection. */
  #removeTile(id: string): void {
    for (const [chartId, chart] of this.#charts) {
      if (chart.editor?.tile === id) {
        this.#closeEditor(chartId);
        return;
      }
    }
    if (this.#grids.get(id)?.added) {
      this.#removeGrid(id);
      return;
    }
    const chart = this.#charts.get(id);
    if (!chart) return;
    if (chart.conditions.length) this.#select(id, null);
    this.#closeEditor(id);
    chart.panel.dispose();
    this.#charts.delete(id);
    this.#board.remove(id);
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
    if (this.charts === 0 && (this.#options.grid !== undefined || this.#grids.size === 0)) this.#options.onEmpty();
  }

  /** Take a grid off the page as its own tile's remove button does: a grid's menu's Remove from Page. */
  removeGrid(id: string): void {
    this.#removeGrid(id);
  }

  /**
   * REMOVE A GRID: the charts following it go with it; each frozen one stays, detached -- with its
   * own copy of the grid's query as it is now, so it still draws and refreshes.
   */
  #removeGrid(id: string): void {
    const grid = this.#grids.get(id);
    if (!grid?.added) return;
    const query = grid.source.snapshot;
    let keeps = false;
    for (const [chartId, chart] of [...this.#charts]) {
      if (chart.link.grid !== id) continue;
      if (!chart.panel.frozen) {
        this.#closeEditor(chartId);
        chart.panel.dispose();
        this.#charts.delete(chartId);
        this.#board.remove(chartId);
        continue;
      }
      chart.conditions = [];
      chart.key = '';
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
    this.#board.remove(id);
    this.#options.onChange();
    this.#afterRemove();
  }

  /**
   * OPEN IN GRID: a frozen chart's grouping in a grid of its own, in a tile beside the chart --
   * the chart's column across and its split as the row groups, its measures as the grid's, over
   * the same source and filter. The chart's grid is not touched. Update writes the editing grid's
   * grouping back into the chart; removing its tile throws it away.
   */
  #openEditor(chartId: string): void {
    const chart = this.#charts.get(chartId);
    if (!chart) return;
    if (chart.editor) {
      this.#board.reveal(chart.editor.tile);
      return;
    }
    const spec = chart.panel.spec;
    if (!spec || spec.mark === 'scatter' || spec.mark === 'treemap' || spec.x === undefined) return;
    const source = this.#sourceOf(chart.link);
    const tile = `edit-${chartId}`;
    const host = this.#doc.createElement('div');
    host.className = 'dc-chart-editor';
    const used = new Set<string>();
    const measures = spec.y.map((m): Measure => {
      // the grid's own measure of that column and aggregate keeps its name (its format)
      const own = source.snapshot.measures.find((c) => c.column === m.column && c.fn === m.fn);
      const name = own?.name ?? (used.has(m.column) ? measureName(m) : m.column);
      used.add(name);
      return { name, column: m.column, fn: m.fn };
    });
    const grid = source.spawn(host, {
      ...source.snapshot,
      rows: [spec.x, ...(spec.split !== undefined ? [spec.split] : [])],
      pivotOn: [],
      measures,
    });
    const chartTitle = this.#board.title(chartId) ?? 'the chart';
    const update = this.#button(`Update ${chartTitle}`, 'Give the chart this grid\'s grouping, and close this grid.');
    update.addEventListener('click', () => {
      const current = chart.panel.spec;
      if (!current) return;
      // the chart regrouped the way the editing grid is -- as a following chart would follow it --
      // then frozen again: its mark and options are its own
      const { frozen: _f, ...following } = current;
      // detached, it takes the editing grid's whole query (its filter too), run by a grid of its own kept off the board
      // -- another chart detached from the same grid keeps the query it had
      if (chart.link.grid === null && chart.link.detached) {
        const own = `${chartId}-query`;
        this.#kept.get(own)?.dispose();
        this.#kept.delete(own);
        const cube = source.spawn(this.#doc.createElement('div'), grid.snapshot);
        this.#kept.set(own, cube);
        void cube.open();
        chart.link.detached = { snapshot: grid.snapshot, from: own };
        this.#releaseKept();
      }
      chart.panel.setSpec({ ...followCube(following, grid.snapshot), frozen: true });
      this.#closeEditor(chartId);
      this.#options.onChange();
    });
    chart.editor = { tile, grid };
    // beside its chart while that band has room
    this.#board.add({ id: tile, title: `Editing ${chartTitle}`, element: host, actions: [update] }, chartId);
    this.#board.reveal(tile);
    void grid.open();
  }

  /** Throw a chart's editing grid away, if it has one. */
  #closeEditor(chartId: string): void {
    const chart = this.#charts.get(chartId);
    const editor = chart?.editor;
    if (!chart || !editor) return;
    delete chart.editor;
    editor.grid.dispose();
    this.#board.remove(editor.tile);
  }

  // -- selections: click-to-filter ----------------------------------------------

  /**
   * A chart's SELECTION: the mark clicked last, as conditions on the grid's filter (one per
   * column the mark names, ANDed on), owned by that chart. A click on another mark replaces them
   * rather than piling more on (two clicks must not filter to nothing); a click on the same mark,
   * or the chip naming it in the chart's title bar, takes them off. Each is ONE change -- one undo
   * step, refused as a whole.
   */
  #select(id: string, mark: MarkKey | null): void {
    const chart = this.#charts.get(id);
    if (!chart) return;
    const next: FilterNode[] = mark === null ? [] : Object.entries(mark).map(([column, raw]) => {
      // a big integer as its digits, as the context menu's value filters do
      const value = typeof raw === 'bigint' ? raw.toString() : raw;
      return value === null
        ? { kind: 'condition', column, operator: 'isEmpty' }
        : { kind: 'condition', column, operator: 'equal', value };
    });
    const key = JSON.stringify(next);
    const add = key === chart.key ? [] : next;
    const old = chart.conditions;
    if (old.length === 0 && add.length === 0) return;
    this.#sourceOf(chart.link).refilter(old, add, add.length === 0 ? 'clear chart selection' : 'filter to chart mark');
    chart.conditions = add;
    chart.key = add.length === 0 ? '' : key;
    this.#paintSelection(id);
    this.#options.onChange();
  }

  #paintSelection(id: string): void {
    const chart = this.#charts.get(id);
    if (!chart) return;
    const source = this.#sourceOf(chart.link);
    const words = chart.conditions.map((c) => c.kind === 'condition'
      ? `${source.label(c.column)}: ${c.operator === 'isEmpty' ? '(empty)' : String(c.value)}`
      : '');
    chart.chip.hidden = words.length === 0;
    chart.chip.textContent = `${words.join(', ')} ×`;
    chart.chip.title = 'Filtered to this; click to clear';
    chart.chip.setAttribute('aria-label', `Clear the filter to ${words.join(', ')}`);
  }
}

/** `layout` without the tiles `leave` names: each one's place closed by its neighbours, as a removal closes it. */
function without(layout: Bands, leave: (id: string) => boolean): Bands {
  return tiles(layout).filter(leave).reduce((rest, id) => removeTile(rest, id), layout);
}
