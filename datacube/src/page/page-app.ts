// A PAGE OF ITS OWN (docs/DATACUBE_PAGES_DESIGN_2026_10_09.md §3.1, §6): DataCube's app, above its grids.
//
// The page has a thin bar that is not a tile -- its menu, its name in a box, its sheets' tabs as a browser's (§7.1),
// its fold -- and below it the board (page/cube-page.ts) of grids and charts. Every grid is a tile holding a compact CubeApp, made the same way
// whether it is the first or the fifth (`addGrid`), so any of them can be removed; with none left the page is empty
// and shows the host's choices of a source. The page's menu holds the page's things only (New, Open, Save, Share,
// Arrange, Settings, the host's own entries); each grid's own menu, in its tile's header, holds the grid's.
//
// SHEETS (§7.2): the page's tiles on one or more sheets, one shown; the bar's tabs show, add, rename, reorder and delete
// them, and a tile is moved to another from its menu or by dragging it onto that sheet's tab. A sheet is named after
// its first grid's report title (its source) until someone names it.
//
// ONE GRID ALONE on the sheet shown fills it with no frame, and its header -- its source, Live/Snapped, its menu -- sits at the
// right of the bar's strip: one strip, as a grid alone looks today (the user, 2026-10-09). A second tile moves it back.
//
// EVERY GRID IS THE HOST'S: made by a maker the host gave (`addGrid`, `restore`) -- a copy of one (Copy of Grid) by the
// same maker, starting where that grid is now -- so the host knows each grid on the page, and the page knows each one.
//
// THE PAGE IS WHAT IS SAVED: one cube document per grid (each over its own source), a view per tile, the sheets
// (`document`); reopening adds each grid again under its saved id, through the host's own way of opening its source
// (`restore`). A grid removed while a frozen chart still reads it is kept off the board for that chart, and saved as a
// cube with no grid view.
//
// ONE PAGE STATE (page-state.ts; docs/DATACUBE_PAGE_STATE_2026_10_18.md): the page as it is saved, held with its
// history. THE BOARD IS PRODUCED BY THE COMMANDS (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md S3): each structural edit
// the board tells is applied to the state by the one rule (page-commands.ts applyToState), a step of its history, and
// the board put at the page it makes (CubePage.sync); what the grids change of themselves (a view, a chart following
// it) is kept as they leave it. Undo and Redo put the board at a step the same way.

import { CubePage, type PageEdit, type SheetInfo, type SpawnOptions, type SpawnedGrid } from './cube-page.ts';
import { MenuView } from '../ui/menu-view.ts';
import { SheetTabs } from '../ui/sheet-tabs.ts';
import type { MenuGroup, MenuItem } from '../ui/menu.ts';
import { tiles } from '../layout/bands.ts';
import {
  filtersFor, keptFor, modelAcross, newPage, NOTHING_TO_SAVE, pageFile, sheetLabel, writePageOf, type PageDocument, type PageView,
  type PageViews,
} from '../page-document.ts';
import { conditionWords, filterWords, gridLens, type PageFilter } from '../page-filters.ts';
import type { BarClass, BarFilter, PageFilterBar } from '../ui/page-filter-bar.ts';
import type { QueryLens } from '../runner.ts';
import { CubeRefusal, type FilterNode } from '../snapshot.ts';
import {
  applyPageCommands, applyToState, idsOf, selectionRefilters, takenIds, type PageCommand, type PageDraft,
} from '../page-commands.ts';
import { pageCommand } from '../page-gestures.ts';
import { PageState } from '../page-state.ts';
import { givenName } from '../page-names.ts';
import type { PageModelRef } from '../page-model.ts';
import { configurationOf, openCube, type CubeDocument, type CubeSource } from '../cube-document.ts';
import type { GridCopy } from '../page-document.ts';
import type { CubeSnapshot } from '../snapshot.ts';
import type { CubeConfiguration } from '../config.ts';
import type { SettingValues } from '../settings.ts';
import type { TreeState } from '../tree.ts';
import type { GridCommand } from '../grid-commands.ts';

/** What the page needs of a grid on it (a CubeApp in a tile). */
export interface PageGrid extends SpawnedGrid {
  /** The grid as a saved cube: its source, its view, its configuration; undefined when it cannot say its source. */
  cubeDocument(name: string, unknown?: Readonly<Record<string, unknown>>): CubeDocument | undefined;
  /** Why the grid cannot be saved now (an edit in progress, a source with no identity), if it cannot. */
  saveRefusal(): string | undefined;
  /** Its Settings window; and settings saved elsewhere, in effect here too. */
  openSettings(): void;
  useSettings(values: SettingValues): void;
  /** Its configuration: a lone grid's report title names the page, and its title bar setting folds the page's bar. */
  readonly configuration: CubeConfiguration;
  /** Its open groups: what it starts with again when an undo makes it again. */
  readonly tree: TreeState;
  setChrome(patch: { readonly showTitleBar?: boolean }): void;
  /** Its status bar's readout from the host asked for again: the page's first grid changed. */
  refreshHostStatus(): void;
  /** Where its rows come from, as it is saved: a class's grid is one the page's filters reach. */
  readonly cubeSource?: CubeSource | undefined;
  /** Every query it runs rewritten by the page (its filters on the grid's class), `said` in words; `run`: run again now. */
  setLens?(lens: QueryLens | undefined, run: boolean, said?: string): Promise<void>;
  /** The copy it reads (Snap: docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §18.8), as the page writes it down; none, live. */
  readonly copy?: GridCopy | undefined;
  /** The page's snap command from its other editor: the copy that editor's host made, read from now on. */
  followSnap?(copy: GridCopy): Promise<void>;
  /** The page's live command from its other editor: its live rows again. */
  followLive?(): Promise<void>;
  /** Its edit by commands, as a host makes one (grid-commands.ts): told, one step of its own history; the refusal or null. */
  edit(commands: readonly GridCommand[], label: string): Promise<string | null>;
}

/**
 * Where a copy of a grid starts: that grid's query and configuration as they are now -- or, with its open groups
 * (`tree`), where the SAME grid starts when an undo makes it again: its own, not a copy's.
 */
export interface GridStart {
  readonly snapshot: CubeSnapshot;
  readonly configuration: CubeConfiguration;
  readonly tree?: TreeState;
}

/**
 * How the host makes a grid on the page, in `host`, wired to the page by `options` (its id, its charts, copies,
 * removal) -- as the host opened it, or, `start`, a copy starting where another grid of the same source is now.
 */
export type GridMaker = (host: HTMLElement, options: SpawnOptions, start?: GridStart) => PageGrid;

export interface PageAppOptions {
  /** Where the page goes: it fills it. */
  readonly host: HTMLElement;
  /** The page's name, in its bar's name box, until `setTitle`. */
  readonly title?: string;
  /**
   * The name box renamed (Enter in its field): the host keeps the name -- the page's, saved by Save -- and gives it
   * back with `setTitle`. Absent, the box only says the name.
   */
  readonly onRename?: (name: string) => void;
  /** A question to answer yes or no (a sheet deleted with its tiles): the browser's `confirm` unless given. */
  readonly confirm?: (question: string) => boolean;
  /** Each edit of the page's structure as its page commands (page/cube-page.ts onPageEdit; page-commands.ts). */
  readonly onPageCommand?: (command: PageCommand) => void;
  /** The page put back whole by an undo or a redo, which no command says: written whole, a restore its keeper takes only
   *  at the version it was made at (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md N2, P5). */
  readonly onRestore?: () => void;
  /** The host's own menu entries (Save, Open, Share, where the planner runs), by their `section`. */
  readonly hostMenu?: () => readonly MenuItem[];
  readonly onHostMenu?: (item: MenuItem) => void;
  /** What an empty page shows -- the host's choices of a source -- in `slot`. */
  readonly empty: (slot: HTMLElement) => void;
  /** New ▸ Data Source…: the host's picker, its grid's maker; undefined when nothing was chosen. */
  readonly openSource?: () => Promise<GridMaker | undefined>;
  /** New ▸ Blank Page: the host clears the page (asking first about unsaved changes). */
  readonly onBlankPage?: () => void;
  /** Something on the page changed: a grid's state, a chart, the layout, a title. */
  readonly onChange?: () => void;
  /** A grid's Settings saved: in effect on every grid, and for the host to keep. */
  readonly onSettingsChanged?: (values: SettingValues) => void;
  /** Hand a file to the user: Export ▸ Page File. */
  readonly download?: (name: string, mime: string, content: string) => void;
  /**
   * The page named `name` as its host keeps it, when it keeps more than the board does -- what a newer writer put in it,
   * its model -- for Export ▸ Page File, which writes it by DataCube's one rule (page-document.ts pageFile).
   */
  readonly pageNow?: (name: string) => PageDocument | undefined;
  /** Say a refusal to the person (the host's way: a page of its own alerts). */
  readonly refuse?: (text: string) => void;
  /**
   * The host's readout (where the planner runs, what went wrong), at the right of the page's FIRST grid's status bar
   * -- it moves when another grid becomes the first -- on each of that bar's renders.
   */
  readonly hostStatus?: (slot: HTMLElement) => void;
}

export class PageApp {
  readonly #options: PageAppOptions;
  readonly #doc: Document;
  readonly #root: HTMLElement;
  /** The name box: the page's name, a dot when it changed since it was saved. */
  readonly #title: HTMLElement;
  readonly #sheetTabs: SheetTabs;
  /** What a sheet with nothing on it says, on a page that has tiles elsewhere. */
  readonly #emptySheet: HTMLElement;
  /** Changed since it was saved (the host says): the name box's dot. */
  #changed = false;
  readonly #burger: HTMLButtonElement;
  readonly #bar: HTMLElement;
  readonly #status: HTMLElement;
  readonly #fold: HTMLButtonElement;
  readonly #lip: HTMLButtonElement;
  readonly #alone: HTMLElement;
  readonly #boardHost: HTMLElement;
  readonly #empty: HTMLElement;
  readonly #menu: MenuView;
  /**
   * Every grid made on the page, by its id, with the maker that made it (a copy is made by the same one). Read only
   * for the ids the board still has (`#live`): one it let go is gone.
   */
  readonly #grids = new Map<string, { readonly grid: PageGrid; readonly make: GridMaker }>();
  #page: CubePage;
  /** The page as it is saved, and its history: the page on screen, kept after every change (page-state.ts). */
  #state!: PageState;
  /** While the screen is put at a step of the history: what it changes is that step's, not a change of its own. */
  #syncing = false;
  /** Each grid the page let go, by its id: its maker, and where it was when it went -- made again by an undo. */
  readonly #gone = new Map<string, { readonly make: GridMaker; readonly start: GridStart }>();
  #name: string;
  #disposed = false;
  /** The grid whose status bar holds the host's readout. */
  #first: PageGrid | undefined;
  /** THE PAGE'S FILTERS (page-filters.ts, §16.2), as its state holds them: set by page commands, put on its grids'
   *  queries by each one's lens. */
  #filters: readonly PageFilter[] = [];
  /** The page's model (where its classes are: page-model.ts), as its state has it -- made and kept by its model actions'
   *  commands, its own and its other editor's (P15). */
  #model: PageModelRef | undefined;
  /** Each grid's lens as last given it, by what made it (page-filters.ts gridLens's key): the same, not run again. */
  readonly #lensKeys = new Map<string, string>();
  /** The views of a page being restored: whose chart a click filter came from, before the charts are on the board. */
  #restoring: readonly PageView[] | undefined;
  /** The filter edits, one after another: each decided on the page as the one before left it (two quick clicks of one
   *  chart replace its filter, never add two). */
  #filterQueue: Promise<unknown> = Promise.resolve();
  /** The filter bar's place, below the page's bar; the bar itself fetched with a page's first class grid or filter
   *  (ui/page-filter-bar.ts: a grid-only page never downloads it). */
  readonly #filterSlot: HTMLElement;
  #filterBar: PageFilterBar | undefined;
  #filterBarLoading = false;

  constructor(options: PageAppOptions) {
    this.#options = options;
    this.#doc = options.host.ownerDocument;
    this.#name = options.title ?? '';
    const doc = this.#doc;
    const root = doc.createElement('div');
    root.className = 'dc-page';
    // the bar: the page's menu, its name box, its sheets' tabs, its fold, and a lone grid's header at the right
    const bar = doc.createElement('div');
    bar.className = 'dc-titlebar dc-page-bar';
    this.#burger = doc.createElement('button');
    this.#burger.type = 'button';
    this.#burger.className = 'dc-titlebar-menu';
    this.#burger.textContent = '≡';
    this.#burger.title = 'The page\'s menu';
    this.#burger.setAttribute('aria-label', 'Page menu');
    this.#burger.setAttribute('aria-haspopup', 'menu');
    this.#burger.setAttribute('aria-expanded', 'false');
    this.#burger.addEventListener('click', () => this.#toggleMenu());
    this.#title = doc.createElement('span');
    this.#title.className = 'dc-page-name';
    if (options.onRename) {
      this.#title.tabIndex = 0;
      this.#title.setAttribute('role', 'button');
      this.#title.title = 'The page\'s name: click to rename it';
      this.#title.addEventListener('click', () => this.#renamePage());
      this.#title.addEventListener('keydown', (e) => {
        if (e.key !== 'Enter' && e.key !== ' ' && e.key !== 'F2') return;
        e.preventDefault();
        this.#renamePage();
      });
    }
    this.#sheetTabs = new SheetTabs(doc, {
      onShow: (id) => this.#page.showSheet(id),
      onAdd: () => this.#page.addSheet(),
      onRename: (id, name) => this.#page.renameSheet(id, name),
      onMove: (id, to) => this.#page.moveSheet(id, to),
      onRemove: (id) => this.#removeSheet(id),
    });
    // the space between the tabs and the right end (the host's readout is in its first grid's status bar)
    const status = doc.createElement('span');
    status.className = 'dc-page-status';
    this.#status = status;
    this.#alone = doc.createElement('span');
    this.#alone.className = 'dc-page-alone dc-tile-cube';
    // THE BAR FOLDS (the user, 2026-09-25: the folds live in one column at the right): its fold just left of a lone
    // grid's header, whose last control -- the zones' way back, when they are folded -- stays at the far right, above
    // the zone bar's own fold; folded, a lip, its chevron where the fold was, the whole strip a button
    this.#fold = doc.createElement('button');
    this.#fold.type = 'button';
    this.#fold.className = 'dc-titlebar-fold';
    this.#fold.append(chevron(doc, 'up'));
    this.#fold.title = 'Hide the bar';
    this.#fold.setAttribute('aria-label', 'Hide the bar');
    this.#fold.setAttribute('aria-expanded', 'true');
    this.#fold.addEventListener('click', () => {
      this.setBarFolded(true);
      this.#lip.focus();
    });
    this.#lip = doc.createElement('button');
    this.#lip.type = 'button';
    this.#lip.className = 'dc-titlebar-lip';
    this.#lip.append(chevron(doc, 'down'));
    this.#lip.title = 'Show the bar';
    this.#lip.setAttribute('aria-label', 'Show the bar');
    this.#lip.setAttribute('aria-expanded', 'false');
    this.#lip.addEventListener('click', () => {
      this.setBarFolded(false);
      this.#fold.focus();
    });
    bar.append(this.#burger, this.#title, this.#sheetTabs.element, status, this.#fold, this.#alone);
    this.#bar = bar;
    this.#boardHost = doc.createElement('div');
    this.#boardHost.className = 'dc-board-host';
    this.#empty = doc.createElement('div');
    this.#empty.className = 'dc-page-empty';
    this.#emptySheet = this.#newEmptySheet();
    this.#filterSlot = doc.createElement('div');
    this.#filterSlot.className = 'dc-page-filters';
    this.#filterSlot.hidden = true;
    root.append(bar, this.#filterSlot, this.#boardHost, this.#emptySheet, this.#empty);
    options.host.replaceChildren(root);
    this.#root = root;
    this.#menu = new MenuView(doc, {
      onSelect: (item) => this.#onMenu(item),
      onClose: () => this.#burger.setAttribute('aria-expanded', 'false'),
    });
    options.empty(this.#empty);
    this.#page = this.#newBoard();
    this.#state = new PageState(this.#seen(undefined));
    this.#paintTitle();
    this.#paintSheets();
    this.#paintEmpty();
  }

  /** An empty sheet's card: it says so, with a data source to add to it and the way to move a tile here. */
  #newEmptySheet(): HTMLElement {
    const doc = this.#doc;
    const el = doc.createElement('div');
    el.className = 'dc-sheet-empty';
    el.hidden = true;
    const say = doc.createElement('p');
    say.textContent = 'This sheet is empty. Move a tile here from its menu (Move to Sheet) or by dragging it onto this sheet\'s tab';
    el.append(say);
    if (this.#options.openSource) {
      say.textContent += ', or add a data source.';
      const add = doc.createElement('button');
      add.type = 'button';
      add.className = 'dc-picker-button dc-primary';
      add.textContent = 'Add a data source';
      add.addEventListener('click', () => void this.addSource());
      el.append(add);
    } else {
      say.textContent += '.';
    }
    return el;
  }

  /**
   * A grid's state changed: the bar says the first grid's report title, and is folded while every grid on the page
   * says its title bar is hidden -- a grid alone as a cube alone always did; several, as the bar was folded by hand.
   */
  #onGridChange(): void {
    this.#paintTitle();
    // a sheet named after its first grid's report title: that title may have changed
    this.#paintSheets();
    const grids = this.#board();
    if (grids.length > 0) this.#foldBar(grids.every((g) => !g.configuration.showTitleBar));
  }

  /** The grids on the board, in reading order (not those kept off it). */
  #board(): PageGrid[] {
    return this.grids.map((id) => this.grid(id)).filter((g): g is PageGrid => g !== undefined);
  }

  /** The page's own element: where its grids' windows float, above every tile. */
  get root(): HTMLElement {
    return this.#root;
  }

  /**
   * The bar folded to a lip (more room for the grids) or shown. Folded, the page's menu is not on the page at all --
   * the lip brings it back -- and a lone grid's header keeps only the zones' way back, where it was.
   */
  setBarFolded(folded: boolean): void {
    // kept in every grid's title bar setting (saved with it, as a cube alone's always was): whichever grids are left
    // later say the same, and a grid made while it is folded starts so (`#maker`)
    for (const grid of this.#board()) {
      if (grid.configuration.showTitleBar === folded) grid.setChrome({ showTitleBar: !folded });
    }
    this.#foldBar(folded);
  }

  #foldBar(folded: boolean): void {
    if (folded === this.#bar.classList.contains('dc-collapsed')) return;
    this.#menu.close();
    // focus in the bar stays in it (its control is taken off by the fold); elsewhere it is left where it is
    const focused = this.#bar.contains(this.#doc.activeElement);
    this.#bar.classList.toggle('dc-collapsed', folded);
    if (folded) this.#bar.replaceChildren(this.#lip, this.#alone);
    else this.#bar.replaceChildren(this.#burger, this.#title, this.#sheetTabs.element, this.#status, this.#fold, this.#alone);
    if (focused && !this.#bar.contains(this.#doc.activeElement)) (folded ? this.#lip : this.#fold).focus();
  }

  get barFolded(): boolean {
    return this.#bar.classList.contains('dc-collapsed');
  }

  /** The page's own name ('' before it has one). */
  get title(): string {
    return this.#name;
  }

  /** What to call the page when it has no name of its own yet (Save's suggestion): its first sheet's name. */
  get suggestedName(): string {
    const first = this.#page.sheets[0];
    return this.#name || (first ? this.#sheetLabel(first) : '');
  }

  /** The page's own name (a saved page's, or one given in the name box): '' for none yet. */
  setTitle(name: string): void {
    this.#name = name;
    this.#paintTitle();
    // its host's to name: no step of the page's history
    this.#state.keep(this.#seen());
  }

  /**
   * THE PAGE RENAMED BY ITS EDITOR (the name box, Save under a name): its renamePage command applied by the one rule and
   * told -- no step of its history (the host's to name), but an edit another editor follows (the S7 audit's P-5: it was
   * set, then told). A name it has already tells nothing.
   */
  rename(name: string): void {
    if (name === this.#name) return;
    this.#applyUnstepped([pageCommand('renamePage', { name })], 'Rename Page');
  }

  /**
   * THE PAGE SAVED AS THE PAGE STORE'S RECORD `record` by its host's Save (null: that record deleted, in Open…): its
   * savedAs command applied by the one rule and told -- no step of its history, as a rename is none -- so its other
   * editor saves over the same record (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md B13). The page's state holds the record
   * (`record`), no host beside it (the S7 audit's H-4).
   */
  savedAs(record: string | null): void {
    if ((record ?? undefined) === this.record) return;
    this.#applyUnstepped([pageCommand('savedAs', { record })], 'Saved');
  }

  /** The page's state, or none while the board is still being made (the constructor's first `#seen`). */
  get #stateNow(): PageState | undefined {
    return this.#state as PageState | undefined;
  }

  /** The page store's record the page is saved as, as its state has it (the savedAs command's), or none. */
  get record(): string | undefined {
    return this.#state.draft.page.record;
  }

  /** Commands of the page's own applied by the one function and told, no step of its history: what its host does to it
   *  (a rename, a save, a model action). Refused in their words, nothing changed. */
  #applyUnstepped(commands: readonly PageCommand[], label: string): boolean {
    this.#state.keep(this.#seen());
    return this.#edit({ label, commands, step: false });
  }

  /** Where the page's classes are (page-model.ts), as its state has it: none before its first model action. */
  get model(): PageModelRef | undefined {
    return this.#model;
  }

  /**
   * A MODEL ACTION'S RECORD OF THE PAGE (Create a Class…, an Association, a Data Space: page-commands.ts modelInto,
   * modelKeep, modelPin), applied by the one function and told -- no step of the page's history (D5, P15): what it made in
   * its project cannot be undone. Refused in the commands' words, nothing changed.
   */
  applyModel(commands: readonly PageCommand[]): void {
    if (this.#disposed || commands.length === 0) return;
    if (this.#applyUnstepped(commands, 'a model action')) this.#options.onChange?.();
  }

  /** Changed since it was saved, or not (the host knows): the name box says so. */
  setChanged(changed: boolean): void {
    if (changed === this.#changed) return;
    this.#changed = changed;
    this.#paintTitle();
  }

  /** The name box: the page's name, "Untitled page" before it has one, a dot when it changed since it was saved. */
  #paintTitle(): void {
    if (this.#title.querySelector('input')) return;
    this.#title.textContent = this.#name || 'Untitled page';
    this.#title.classList.toggle('dc-page-name-untitled', this.#name === '');
    this.#title.classList.toggle('dc-page-name-changed', this.#changed);
    this.#title.setAttribute('aria-label', `Page: ${this.#name || 'untitled'}${this.#changed ? ', changed since saved' : ''}`);
  }

  /** The name box, renamed in place: Enter keeps the name (the host's to keep), Escape leaves it as it was. */
  #renamePage(): void {
    if (this.#title.querySelector('input')) return;
    const field = this.#doc.createElement('input');
    field.className = 'dc-page-name-field';
    field.value = this.#name;
    field.placeholder = this.suggestedName || 'Untitled page';
    field.setAttribute('aria-label', 'Page name');
    let done = false;
    const finish = (keep: boolean): void => {
      if (done) return;
      done = true;
      // the one rule (page-names.ts givenName): trimmed, a blank refused -- the page keeps the name it had
      const given = givenName('page', field.value);
      field.remove();
      this.#paintTitle();
      this.#title.focus();
      const name = 'name' in given ? given.name : undefined;
      if (keep && name !== undefined && name !== this.#name) this.#options.onRename?.(name);
    };
    field.addEventListener('keydown', (e) => {
      e.stopPropagation();
      if (e.key === 'Enter') finish(true);
      if (e.key === 'Escape') finish(false);
    });
    field.addEventListener('blur', () => finish(true));
    field.addEventListener('click', (e) => e.stopPropagation());
    this.#title.replaceChildren(field);
    field.focus();
    field.select();
  }

  // -- sheets ------------------------------------------------------------------------------------------------------

  /**
   * What a sheet's tab says: its own name, else its first grid's report title (what that grid reads -- so a page of
   * one sheet looks as it did, its source's name in the one tab), else "Sheet N".
   */
  #sheetLabel(sheet: SheetInfo): string {
    // the one rule (page-document.ts sheetLabel), asked of the board: a tile is a grid when the board holds it as one,
    // or a grid the page could not open, as its page saved it -- the page's grid still, as the rule counts it (the S7
    // confirmation audit's B2-4: a rename to the label shown was dropped as the label the rule computes)
    return sheetLabel(sheet, this.#page.sheets.findIndex((s) => s.id === sheet.id), (t) => {
      if (this.grid(t) !== undefined && this.#page.grids.has(t)) return { title: this.grid(t)?.configuration.reportTitle };
      const left = this.#unopened.get(t);
      return left ? { title: configurationOf(left).reportTitle } : undefined;
    });
  }

  /** The sheets, in order (their ids), and the one shown. */
  get sheets(): readonly string[] {
    return this.#page.sheets.map((s) => s.id);
  }

  get shownSheet(): string {
    return this.#page.shownSheet;
  }

  /** Show a sheet, as its tab's click does (a page opened again on the sheet it showed). */
  showSheet(id: string): void {
    this.#page.showSheet(id);
  }

  /** What a sheet's tab says. */
  sheetLabel(id: string): string {
    const sheet = this.#page.sheets.find((s) => s.id === id);
    return sheet ? this.#sheetLabel(sheet) : id;
  }

  #paintSheets(): void {
    const sheets = this.#page.sheets;
    this.#sheetTabs.paint(sheets.map((s) => ({ id: s.id, label: this.#sheetLabel(s) })), this.#page.shownSheet, this.#page.editing);
  }

  /** A sheet's tab's Delete: asked first when it has tiles, which go with it. */
  #removeSheet(id: string): void {
    const sheet = this.#page.sheets.find((s) => s.id === id);
    if (!sheet) return;
    const ask = this.#options.confirm ?? ((q: string) => this.#doc.defaultView?.confirm(q) ?? false);
    const n = sheet.tiles.length;
    if (n > 0 && !ask(`Delete the sheet "${this.#sheetLabel(sheet)}" and the ${n === 1 ? 'tile' : `${n} tiles`} on it?`)) return;
    this.#page.removeSheet(id);
  }

  /** The sheets changed, or another is shown: the tabs, the lone grid, the empty sheet, the host's readout. */
  #onSheets(): void {
    this.#paintSheets();
    this.#paintEmpty();
  }

  /** The grids on the page, sheet by sheet in reading order (not those kept off it for a detached chart). */
  get grids(): readonly string[] {
    const on = new Set(this.#page.grids.keys());
    return this.#page.tileIds.filter((id) => on.has(id));
  }

  /** Every grid the page saves a cube for: on the board in reading order, then those kept off it for a detached chart. */
  get cubes(): readonly string[] {
    return [...this.grids, ...this.#page.kept.keys()];
  }

  /** A grid on the page, by its id (on the board or kept off it). */
  grid(id: string): PageGrid | undefined {
    return this.#page.grids.has(id) || this.#page.kept.has(id) ? this.#grids.get(id)?.grid : undefined;
  }

  /** The title a person gave a tile, by its id; undefined while it has the one it was made with. */
  givenTitle(id: string): string | undefined {
    return this.#page.givenTitle(id);
  }

  /** The grids on the page now, on the board or kept off it. */
  #live(): PageGrid[] {
    return this.cubes.map((id) => this.grid(id)).filter((g): g is PageGrid => g !== undefined);
  }

  /** `make`, made to say what it made: every grid on the page is made through here, under its tile's id. `own`: a grid
   *  this tab makes (a gesture's), which a folded bar folds; never one another editor's command or an undo makes from
   *  the page's document, whose own setting it keeps (the S7 audit's G-3). */
  #maker(make: GridMaker, start?: GridStart, own = true): (host: HTMLElement, options: SpawnOptions) => PageGrid {
    return (host, options) => {
      const id = options.id;
      const hostStatus = this.#options.hostStatus;
      const grid = make(host, {
        ...options,
        // the host's readout, in this grid's bar while it is the first on the sheet shown
        ...(hostStatus && id !== undefined ? { hostStatus: (slot: HTMLElement) => { if (this.#firstShown() === id) hostStatus(slot); } } : {}),
        // made while the bar is folded: it says so too, or the bar would come back once it is the only grid
        ...(own && this.barFolded ? { titleBarHidden: true } : {}),
      }, start);
      if (id !== undefined) {
        this.#grids.set(id, { grid, make });
        // the page's filters on its class, before its first query: it opens filtered
        const lens = this.#lensOf(id, grid);
        this.#lensKeys.set(id, lens.key);
        if (lens.lens) void grid.setLens?.(lens.lens, false, this.#lensWords(lens.taken));
      }
      return grid;
    };
  }

  /** The first grid on the sheet shown, by its id: where the host's readout is. */
  #firstShown(): string | undefined {
    const shown = this.#page.sheets.find((s) => s.id === this.#page.shownSheet);
    return shown?.tiles.find((t) => this.#page.grids.has(t));
  }

  /** The host's readout moved to the first grid on the sheet shown, when that is another grid now. */
  #rehomeStatus(): void {
    const first = this.#firstShown();
    const grid = first !== undefined ? this.grid(first) : undefined;
    if (grid === this.#first) return;
    const was = this.#first;
    this.#first = grid;
    // the one it leaves drops its slot first; the readout's node then moves to the new one's
    if (was && this.#live().includes(was)) was.refreshHostStatus();
    grid?.refreshHostStatus();
  }

  /** Nothing on the page: no grid, no chart. */
  get empty(): boolean {
    return this.#page.tileIds.length === 0;
  }

  /**
   * A GRID ON THE PAGE, made by `make` in a tile like any other: beside `near` while its band has room, else below;
   * a saved page's under its saved `id` and `title`. Returns its id.
   */
  addGrid(make: GridMaker, how: { readonly id?: string; readonly title?: string; readonly near?: string } = {}): string {
    const id = this.#page.addGridOver(this.#maker(make), how);
    this.#paintEmpty();
    return id;
  }

  /** Once every grid on the page has opened (its first view landed, or its first query refused). */
  async ready(): Promise<void> {
    await this.#page.opened();
  }

  /** New ▸ Data Source…: the host's picker, and the grid it makes beside the others. */
  async addSource(near?: string): Promise<string | undefined> {
    const make = await this.#options.openSource?.();
    if (!make || this.#disposed) return undefined;
    return this.addGrid(make, near !== undefined ? { near } : {});
  }

  /** Every tile gone -- grids, charts, kept grids -- and the new page shown, unnamed (Blank Page; opening in place). */
  clear(): void {
    // a new page, as DataCube starts one (page-document.ts newPage, the one skeleton: D11, B9)
    const fresh = newPage();
    this.#page.dispose();
    this.#grids.clear();
    this.#gone.clear();
    this.#unopened.clear();
    this.#unopenedCopies.clear();
    this.#filters = [];
    this.#model = undefined;
    this.#lensKeys.clear();
    this.#page = this.#newBoard();
    this.#page.setSheets(fresh.sheets);
    this.#name = fresh.name;
    this.#paintTitle();
    this.#state = new PageState(this.#seen(undefined));
    this.#paintSheets();
    this.#paintEmpty();
    this.#options.onChange?.();
  }

  /** The page's layout editable (tiles move, dividers drag) or locked (view mode); a shared page opens locked. */
  setLayoutEditing(editing: boolean): void {
    this.#page.setEditing(editing);
  }

  get layoutEditing(): boolean {
    return this.#page.editing;
  }

  /**
   * What the page shows, as its document keeps it: every grid's view (its cube its own id), every chart and the grid
   * it reads (a kept one for a detached chart), the sheets and their layouts.
   */
  views(): PageViews {
    return this.#page.views((grid) => (this.grid(grid) ? grid : undefined));
  }

  /**
   * THE PAGE AS IT IS SAVED (page-document.ts, version 5): one cube per grid -- on the board, then kept off it for a
   * detached chart -- each the grid's own cube document, under the grid's id; the views and the sheets; the page's
   * model (where its classes are), as its state has it (P15); the saved page it is (`record`), as its state has it.
   * `unknown`: fields a newer writer put in the page and in each cube, written back as they were. Undefined when a grid cannot say its source, or there is nothing on the page -- unless
   * `empty`: the page written live to its keeper, a page of no grid one too.
   */
  document(name: string, unknown?: {
    readonly page?: Readonly<Record<string, unknown>>;
    readonly cubes?: ReadonlyMap<string, Readonly<Record<string, unknown>>>;
  }, empty = false): PageDocument | undefined {
    const ids = this.cubes;
    // a page of no grid is no page saved; written live, it is one (`empty`: docs/DATACUBE_PAGE_KEEPER_2026_10_20.md)
    if (ids.length === 0 && !empty) return undefined;
    const cubes: { id: string; cube: CubeDocument }[] = [];
    for (const id of ids) {
      const cube = this.grid(id)?.cubeDocument(name, unknown?.cubes?.get(id));
      if (!cube) return undefined;
      cubes.push({ id, cube });
    }
    const views = this.views();
    const written = this.#withUnopened(cubes, views);
    return writePageOf({
      name, cubes: written, views, filters: this.#filters, copies: this.#copies(ids, written),
      ...(this.#model ? { model: keptFor(this.#model, written) } : {}),
      ...(this.record !== undefined ? { record: this.record } : {}),
      ...(unknown?.page ? { unknown: unknown.page } : {}),
    });
  }

  /**
   * THE CUBES IT COULD NOT OPEN (a saved page's whose source was not opened: P9), each as its page saved it -- written
   * back while a view reads it, in its grid's place.
   */
  readonly #unopened = new Map<string, CubeDocument>();
  /** The copy each of them read as its page saved it (a snapped grid's): written back with it (the S1 audit's 11). */
  readonly #unopenedCopies = new Map<string, GridCopy>();

  /** Why a cube's views are kept unopened, or undefined: it is no cube the page could not open. */
  #unopenedWhy(cube: string): string | undefined {
    const doc = this.#unopened.get(cube);
    return doc === undefined ? undefined
      : `${doc.source.name} could not be opened: this tile is kept as the page saved it, to open when its source can be.`;
  }

  /** The page's cubes as it writes them: its grids', then those it could not open that a view still reads, each in
   *  its grid's reading place (as page-commands.ts writes a page), the rest as they came. */
  #withUnopened(cubes: readonly { id: string; cube: CubeDocument }[], views: PageViews): { id: string; cube: CubeDocument }[] {
    if (this.#unopened.size === 0) return [...cubes];
    const read = new Set(views.views.map((v) => v.cube));
    const all = [...cubes, ...[...this.#unopened].filter(([id]) => read.has(id)).map(([id, cube]) => ({ id, cube }))];
    const grids = views.views.filter((v) => v.kind === 'grid').map((v) => v.cube);
    const place = (id: string): number => (grids.includes(id) ? grids.indexOf(id) : grids.length);
    return all.map((c, i) => ({ c, i })).sort((x, y) => place(x.c.id) - place(y.c.id) || x.i - y.i).map((x) => x.c);
  }

  /** The copy each of `ids`' grids reads, by its cube (§18.8): the page's record of where its snapped grids' rows are --
   *  and of `written`'s cubes the page could not open, each as its page saved it. */
  #copies(ids: readonly string[], written: readonly { id: string }[]): Readonly<Record<string, GridCopy>> {
    const kept = written.flatMap(({ id }) => {
      const copy = ids.includes(id) ? undefined : this.#unopenedCopies.get(id);
      return copy ? [[id, copy] as const] : [];
    });
    return Object.fromEntries([...ids.flatMap((id) => {
      const copy = this.grid(id)?.copy;
      return copy ? [[id, copy] as const] : [];
    }), ...kept]);
  }

  /** Why the page cannot be saved now, if it cannot: nothing on it, or a grid's own reason, named. */
  saveRefusal(): string | undefined {
    const ids = this.cubes;
    if (ids.length === 0) return NOTHING_TO_SAVE;
    for (const id of ids) {
      const grid = this.grid(id);
      const refused = grid ? grid.saveRefusal() : 'a grid on it does not know its source.';
      if (refused) return ids.length > 1 ? `${this.#page.title(id) ?? id}: ${refused}` : refused;
    }
    return undefined;
  }

  /**
   * A SAVED PAGE PUT BACK: its grids, each made by its cube's maker (the host opened each cube's source) under its
   * saved id and title, in the saved layout's reading order; the cubes no grid view names kept off the board for their
   * detached charts; then the charts and the sheets' layouts. A cube with no maker (its source not opened) is left out, with
   * the views that read it. The page is emptied first. `held`: every id the page has held where it is kept (its keeper's,
   * B12), none of which is given out again here.
   */
  restore(page: PageDocument, makers: ReadonlyMap<string, GridMaker>, held: readonly string[] = []): void {
    this.#page.dispose();
    this.#grids.clear();
    this.#gone.clear();
    this.#unopened.clear();
    this.#unopenedCopies.clear();
    // its filters first: each grid opens with them (a chart's click, by the views it will have)
    this.#filters = page.filters ?? [];
    this.#model = page.model;
    this.#lensKeys.clear();
    this.#restoring = page.views;
    this.#page = this.#newBoard();
    // its grids say how its bar is, as they were saved: not as the page before it had its bar
    this.#foldBar(false);
    // its sheets first: each grid then goes on the sheet it was saved on
    this.#page.setSheets(page.sheets);
    // in the sheets' reading order; a grid no sheet has a place for, last
    const order = page.sheets.flatMap((sheet) => tiles(sheet.layout));
    const at = (id: string): number => (order.includes(id) ? order.indexOf(id) : order.length);
    const grids = page.views.filter((v) => v.kind === 'grid').sort((a, b) => at(a.id) - at(b.id));
    const tileOf = new Map<string, string>();
    // a cube whose source was not opened is kept as the page saved it, its views in their places (P9)
    for (const { id, cube } of page.cubes) {
      if (makers.has(id)) continue;
      this.#unopened.set(id, cube);
      const copy = page.copies?.[id];
      if (copy) this.#unopenedCopies.set(id, copy);
    }
    for (const view of grids) {
      const make = makers.get(view.cube);
      if (tileOf.has(view.cube)) continue;
      if (!make) {
        this.#page.placeUnavailable(view, this.#unopenedWhy(view.cube)!);
        continue;
      }
      tileOf.set(view.cube, this.addGrid(make, { id: view.id, ...(view.title ? { title: view.title } : {}) }));
    }
    for (const { id } of page.cubes) {
      const make = makers.get(id);
      if (tileOf.has(id) || !make) continue;
      this.#page.keepGrid(id, this.#maker(make));
    }
    this.#page.restore(page, (cube) => tileOf.get(cube) ?? (this.#page.kept.has(cube) ? cube : undefined),
      (cube) => this.#unopenedWhy(cube));
    this.#restoring = undefined;
    // a click filter whose chart was left out (its grid's source not opened) goes with it
    this.#filters = filtersFor(this.#filters, this.views().views);
    this.#applyLenses();
    // a page opened has no history: its first step is the first edit made on it
    const seen = this.#seen(page.record);
    this.#state = new PageState({ ...seen, held: [...new Set([...seen.held, ...held])] });
    this.#paintSheets();
    this.#paintEmpty();
    this.#onGridChange();
    // a change of the page, as `clear` is: its host re-reads it (a kept grid let go just now, its table with it)
    this.#options.onChange?.();
  }

  dispose(): void {
    if (this.#disposed) return;
    this.#disposed = true;
    this.#menu.close();
    this.#page.dispose();
    this.#sheetTabs.dispose();
    this.#grids.clear();
    this.#root.remove();
  }

  // -- the board ------------------------------------------------------------------

  #newBoard(): CubePage {
    return new CubePage({
      host: this.#boardHost,
      onChange: () => {
        // a chart removed takes its click's filter with it (page-document.ts filtersFor); a chart or a grid come or
        // gone changes what each grid's lens is -- not while a page is restored, its charts not yet on the board
        if (!this.#restoring) {
          this.#filters = filtersFor(this.#filters, this.views().views);
          this.#applyLenses();
        }
        // what the board's owners change of themselves (a grid's own edits, a chart following it) kept, no step -- not
        // while the board is put at the state (the commands' changes are the state's already)
        if (!this.#syncing) this.#state.keep(this.#seen());
        // a grid let go that no step can bring back any more is forgotten (its host lets its source go)
        const reachable = this.#state.reachable;
        for (const id of [...this.#gone.keys()]) if (!reachable.has(id)) this.#gone.delete(id);
        // a layout change can make another grid the first
        this.#rehomeStatus();
        this.#onGridChange();
        this.#options.onChange?.();
      },
      onEmpty: () => this.#paintEmpty(),
      onTiles: () => this.#onTiles(),
      onSettingsChanged: (values) => {
        for (const grid of this.#live()) grid.useSettings(values);
        this.#options.onSettingsChanged?.(values);
      },
      // a copy (Copy of Grid; the grid of a chart's own grouping Show Grid makes) is made by the maker of the grid it
      // copies -- one kept off the board for a detached chart too
      gridLike: (from, snapshot, configuration) => {
        const was = this.#grids.get(from);
        if (!was || !this.grid(from)) throw new Error(`no grid ${from} on the page to copy`);
        return this.#maker(was.make, { snapshot, configuration: configuration ?? was.grid.configuration });
      },
      sheetLabel: (id) => this.sheetLabel(id),
      onSheets: () => this.#onSheets(),
      sheetTabs: { at: (x, y) => this.#sheetTabs.at(x, y), hover: (id) => this.#sheetTabs.hover(id) },
      // each structural edit applied to the page's state by the one rule, a step of its history, the board put at what
      // it makes; its commands told
      onPageEdit: (edit) => this.#edit(edit),
      taken: () => takenIds(this.#state.draft),
      // any grid it made: one the board waits to place too
      documentOf: (grid) => this.#grids.get(grid)?.grid.cubeDocument(this.#name),
      onUndo: (redo) => (redo ? this.redo() : this.undo()),
      // a chart's click decided on its grid's source and the page's filters (page-filters.ts chartPicked)
      pickOn: (grid) => ({ source: this.grid(grid)?.cubeSource, filters: this.#filters }),
    });
  }

  /** The page as it is on screen, as its state holds it: what `document` writes (each grid that can say its source) --
   *  and the saved page it is (`record`), which only the page's savedAs command changes (the S7 audit's H-4). */
  #seen(record: string | undefined = this.#stateNow?.draft.page.record): PageDraft {
    const cubes: { id: string; cube: CubeDocument }[] = [];
    for (const id of this.cubes) {
      const cube = this.grid(id)?.cubeDocument(this.#name);
      if (cube) cubes.push({ id, cube });
    }
    const views = this.views();
    const written = this.#withUnopened(cubes, views);
    const page = writePageOf({
      name: this.#name, cubes: written, views, filters: this.#filters, copies: this.#copies(this.cubes, written),
      ...(this.#model ? { model: keptFor(this.#model, written) } : {}),
      ...(record !== undefined ? { record } : {}),
    });
    return { page, held: idsOf(page) };
  }

  /**
   * A PAGE EDIT (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md S3: the board produced by the commands): its commands applied
   * to the page's state by the one rule (page-commands.ts applyPageCommands) -- refused in their words, nothing changed
   * -- a step of the page's history (but a snap or going live: where its rows are, not what the page is), the commands
   * as applied told, and the board put at the page they make (`#syncTo`). True when applied.
   */
  #edit(edit: PageEdit): boolean {
    const from = this.#state.draft.page;
    let applied: PageCommand[];
    try {
      applied = applyToState(this.#state, edit.commands, edit.label, edit.step !== false);
    } catch (e) {
      if (!(e instanceof CubeRefusal)) throw e;
      this.#refused(e);
      return false;
    }
    for (const command of applied) this.#options.onPageCommand?.(command);
    this.#syncTo(this.#state.draft.page, from);
    return true;
  }

  /**
   * The board put at `to` from `from` (CubePage.sync), what it changes no change of the page's own: its filters as `to`
   * has them (a grid it makes opens with them), a grid it makes made by its maker -- the one that made it, or `makers`'
   * (another editor's) -- and each grid's lens given it again.
   */
  #syncTo(to: PageDocument, from: PageDocument, makers: ReadonlyMap<string, GridMaker> = new Map(), theirs = false): void {
    // its name as the step has it (a renamePage: its editor's, or another's)
    if (to.name !== this.#name) {
      this.#name = to.name;
      this.#paintTitle();
    }
    this.#filters = to.filters ?? [];
    this.#model = to.model;
    this.#restoring = to.views;
    this.#syncing = true;
    try {
      this.#page.sync({ views: to.views, sheets: to.sheets, cubes: to.cubes }, { views: from.views, sheets: from.sheets }, (grid, document) => {
        const make = makers.get(grid);
        return this.#remake(grid, document) ?? (make ? this.#maker(make, undefined, false) : undefined);
      }, (cube) => this.#unopenedWhy(cube), theirs);
    } finally {
      this.#syncing = false;
      this.#restoring = undefined;
    }
    this.#applyLenses();
    this.#paintSheets();
    this.#paintEmpty();
  }

  // -- undo and redo ----------------------------------------------------------------------------------------------

  /** The page's last step undone (its menu's Undo; Ctrl+Z on a tile's frame): the screen put at the page before it. */
  undo(): void {
    this.#move('undo');
  }

  /** The last step undone done again. */
  redo(): void {
    this.#move('redo');
  }

  /** What Undo would undo, by name (undefined: nothing, or a locked page: nothing moves). */
  get undoLabel(): string | undefined {
    return this.#page.editing ? this.#state.undoLabel : undefined;
  }

  get redoLabel(): string | undefined {
    return this.#page.editing ? this.#state.redoLabel : undefined;
  }

  /**
   * The grids the page let go that an undo or a redo can bring back: a host keeps what each reads (its table, its
   * planner) while they are, as it keeps a grid's on the page.
   */
  get recoverable(): readonly string[] {
    const reachable = this.#state.reachable;
    // its maker known: let go already, or -- the change that let it go still telling its host -- about to be
    return [...new Set([...this.#gone.keys(), ...this.#grids.keys()])].filter((id) => reachable.has(id));
  }

  /** The page as its state holds it (page-state.ts): the page on screen, with its history. */
  get state(): PageDraft {
    return this.#state.draft;
  }

  /**
   * ANOTHER EDITOR'S CHANGE (Python's, on a page it serves: docs/DATACUBE_LIVE_COMMANDS_2026_10_18.md): its page commands
   * applied to the page as it is open -- what was changed here meanwhile kept -- and the screen put at the page they
   * make, a grid they add made by its maker in `makers` (its host's, for its cube document). The editor's change is no
   * step of this page's own: its history starts again from it (an undo past it would take it back unseen). Refused in
   * the commands' words (CubeRefusal) when they do not apply here -- a tile closed meanwhile, an id given since -- with
   * nothing changed.
   */
  async follow(commands: readonly PageCommand[], makers: ReadonlyMap<string, GridMaker>): Promise<void> {
    if (this.#disposed) return;
    const from = this.#state.draft.page;
    const made = applyPageCommands(this.#state.draft, commands).draft;
    const to = made.page;
    // its name too (a page renamed in Python), by #syncTo: the host reads it back (`title`)
    this.#syncTo(to, from, makers, true);
    // the page the commands made, whole -- the ids it has held, its cubes -- the start of its history again: before the
    // grids read their rows where the commands say, so an edit made meanwhile is a step on top of it (the S3 audit's 5)
    this.#state = new PageState(made);
    // where each grid's rows are as the commands leave them (§18.8), in their order: a snap made by the other editor
    // read from its host, a live command its live rows again
    for (const command of commands) {
      if (command.kind !== 'snap' && command.kind !== 'live') continue;
      const cube = to.views.find((v) => v.id === command.grid)?.cube;
      const grid = cube === undefined ? undefined : this.grid(cube);
      if (!grid?.followSnap || !grid.followLive) throw new Error(`${command.grid} cannot read its other editor's copy here`);
      if (command.kind === 'snap') await grid.followSnap(command.copy);
      else await grid.followLive();
      if (this.#disposed) return;
    }
  }

  #move(way: 'undo' | 'redo'): void {
    // a locked page: nothing moves
    if (!this.#page.editing || this.#disposed) return;
    const from = this.#state.draft.page;
    const step = way === 'undo' ? this.#state.undo() : this.#state.redo();
    if (!step) return;
    this.#options.onRestore?.();
    // what the page's model actions wrote stays, no step of its history (D5): a grid brought back brings its own; and
    // its name and the saved page it is (renamePage, savedAs: no steps either) -- an undo past a Save saves over the
    // same record, under the name it has (the S7 confirmation audit's H2-2)
    const { model: was, name: _named, record: _saved, ...back } = step.draft.page;
    const model = modelAcross(was, from.model, back.cubes);
    const across = { ...back, name: from.name, ...(from.record !== undefined ? { record: from.record } : {}) };
    this.#syncTo(model ? { ...across, model } : across, from);
    // its grids keep their own history (G8): a chart brought back with a selection its grid's filter lost meanwhile has
    // it put back by that grid's own refilter, as a click puts it there
    for (const { grid, command } of selectionRefilters(step.draft.page, (cube) => this.grid(cube)?.snapshot.filter)) {
      void this.grid(grid)?.edit([command], 'filter to chart mark');
    }
    // the screen is the page: what it now shows is the state's (a grid made again writes its own cube)
    this.#state.keep(this.#seen(from.record));
    this.#page.say(`${way === 'undo' ? 'Undone' : 'Redone'}: ${step.label}.`);
  }

  /** A grid made again, by the maker that made it, starting where it was (on the board, kept, or let go) -- or, a
   *  `document` given, as it says (a detached chart's query as a step holds it), over the same source. */
  #remake(id: string, document?: CubeDocument): ((host: HTMLElement, options: SpawnOptions) => PageGrid) | undefined {
    const live = this.#grids.get(id);
    const was = live ? { make: live.make, start: startOf(live.grid) } : this.#gone.get(id);
    if (!was) return undefined;
    this.#gone.delete(id);
    if (document === undefined) return this.#maker(was.make, was.start, false);
    const opened = openCube(document, was.start.snapshot.source, was.start.snapshot.columns);
    return this.#maker(was.make, { snapshot: opened.snapshot, configuration: opened.configuration, tree: opened.tree }, false);
  }

  /** A tile came or went: grids taken off forgotten, a lone grid shown in the bar, the empty page when it is. */
  #onTiles(): void {
    const live = new Set(this.cubes);
    for (const [id, { grid, make }] of [...this.#grids]) {
      if (live.has(id)) continue;
      // let go: kept to be made again by an undo, starting where it was
      this.#gone.set(id, { make, start: startOf(grid) });
      this.#grids.delete(id);
    }
    this.#paintEmpty();
    this.#onGridChange();
    // a sheet named after its first grid: that grid came or went
    this.#paintSheets();
  }

  /** The empty page (no tile on any sheet: the host's choices), or an empty sheet's card, or the sheet shown. */
  #paintEmpty(): void {
    this.#rehomeStatus();
    const empty = this.empty;
    const bare = !empty && (this.#page.sheets.find((s) => s.id === this.#page.shownSheet)?.tiles.length ?? 0) === 0;
    // an empty sheet's card in the board's place (its board, with nothing on it, has nothing to draw)
    this.#boardHost.hidden = empty || bare;
    this.#empty.hidden = !empty;
    this.#emptySheet.hidden = !bare;
    this.#root.classList.toggle('dc-page-is-empty', empty);
    this.#page.showAlone(empty ? null : this.#alone);
    this.#paintFilters();
  }

  // -- the page's filters (page-filters.ts; docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §16.2) --------------------------

  /** The page's filters, as its state holds them. */
  get filters(): readonly PageFilter[] {
    return this.#filters;
  }

  /**
   * A FILTER OF THE PAGE SET -- the one of its id replaced, else added -- as one step named `label`, by the page
   * commands' one rule (page-commands.ts setPageFilter, Python's too): refused in its words, nothing changed. Every grid
   * over its class is filtered. Resolves to its id.
   */
  async setFilter(f: { readonly id?: string; readonly class: string; readonly condition: FilterNode; readonly from?: string },
    label = 'Filter'): Promise<string> {
    const applied = await this.#filterCommand(() => pageCommand('setPageFilter', {
      ...(f.id !== undefined ? { id: f.id } : {}), class: f.class, condition: f.condition, from: f.from ?? null,
    }), label);
    const made = applied[0];
    if (made?.kind !== 'setPageFilter' || made.id === undefined) throw new Error('the page filter was set with no id');
    return made.id;
  }

  /** A filter of the page taken off, as one step: every grid it reached unfiltered by it. */
  async removeFilter(id: string, label = 'Remove Filter'): Promise<void> {
    await this.#filterCommand(() => pageCommand('removePageFilter', { filter: id }), label);
  }

  /**
   * A filter edit, after the ones before it: `make` decides its command on the page as they left it (undefined: nothing
   * to do now), applied by the page commands' rule as one step named `label`.
   */
  #filterCommand(make: () => PageCommand | undefined, label: string): Promise<PageCommand[]> {
    const run = this.#filterQueue.then(() => this.#applyFilterCommand(make, label));
    // the queue goes on past a refusal: its caller hears it
    this.#filterQueue = run.catch(() => undefined);
    return run;
  }

  async #applyFilterCommand(make: () => PageCommand | undefined, label: string): Promise<PageCommand[]> {
    if (this.#disposed) return [];
    const command = make();
    if (command === undefined) return [];
    this.#state.keep(this.#seen());
    const applied = applyToState(this.#state, [command], label);
    this.#filters = this.#state.draft.page.filters ?? [];
    this.#applyLenses();
    this.#paintFilters();
    for (const c of applied) this.#options.onPageCommand?.(c);
    this.#options.onChange?.();
    return applied;
  }

  #refused(e: unknown): void {
    const said = e instanceof Error ? e.message : String(e);
    if (this.#options.refuse) this.#options.refuse(said);
    else this.#page.say(said);
  }

  /** The page's views as a lens reads them: a page being restored's, else the board's. */
  #viewsNow(): readonly PageView[] {
    return this.#restoring ?? this.views().views;
  }

  /** What the page's filters make of grid `id`'s queries, `views` the page's: a click filter is not its own chart's grid's. */
  #lensOf(id: string, grid: PageGrid, views: readonly PageView[] = this.#viewsNow()): ReturnType<typeof gridLens> {
    const charts = new Set(views.filter((v) => v.kind === 'chart' && v.cube === id).map((v) => v.id));
    return gridLens(this.#filters, grid.cubeSource, charts);
  }

  /** Each grid given the lens the page's filters make for it now: one whose lens changed runs again. */
  #applyLenses(): void {
    const views = this.#viewsNow();
    for (const id of this.cubes) {
      const grid = this.grid(id);
      if (!grid?.setLens) continue;
      const lens = this.#lensOf(id, grid, views);
      if (this.#lensKeys.get(id) === lens.key) continue;
      this.#lensKeys.set(id, lens.key);
      void grid.setLens(lens.lens, true, this.#lensWords(lens.taken));
    }
    this.#paintFilters();
  }

  /** The filters a grid takes, in words, its class named once: `region = EMEA; notional > 100`. */
  #lensWords(taken: readonly string[]): string {
    return this.#filters.filter((f) => taken.includes(f.id)).map((f) => conditionWords(f.condition)).join('; ');
  }

  /** The filter bar: each filter in words, what it reaches; Add a Filter on the classes the page's grids read. */
  #paintFilters(): void {
    const views = this.#viewsNow();
    const classes = new Map<string, BarClass>();
    const reach = new Map<string, { tiles: number; not: string[] }>(this.#filters.map((f) => [f.id, { tiles: 0, not: [] }]));
    // the grids on the board (a grid kept off it for a detached chart reads as its chart does, but is no tile to count)
    for (const id of this.grids) {
      const grid = this.grid(id);
      const source = grid?.cubeSource;
      if (!grid || source?._type !== 'class') continue;
      if (!classes.has(source.class)) classes.set(source.class, { path: source.class, name: source.name, properties: source.columns });
      const lens = this.#lensOf(id, grid, views);
      for (const f of lens.taken) reach.get(f)!.tiles += 1;
      for (const r of lens.refused) reach.get(r.id)?.not.push(`not ${this.#page.title(id) ?? id} (${r.why})`);
    }
    const shown: BarFilter[] = this.#filters.map((f) => {
      const r = reach.get(f.id)!;
      const tiles = `${r.tiles} ${r.tiles === 1 ? 'grid' : 'grids'}`;
      return { id: f.id, words: filterWords(f), reach: r.not.length > 0 ? `${tiles}; ${r.not.join('; ')}` : tiles, fromChart: f.from !== undefined };
    });
    if (this.#filterBar) {
      this.#filterBar.render(shown, [...classes.values()]);
      return;
    }
    // no bar yet, and none needed: a page with no class grid and no filter
    if (shown.length === 0 && classes.size === 0) return;
    if (this.#filterBarLoading) return;
    this.#filterBarLoading = true;
    void import('../ui/page-filter-bar.ts').then(({ PageFilterBar: Bar }) => {
      if (this.#disposed) return;
      const made = new Bar(this.#doc, {
        // its x clicked twice before it was painted again: the second finds nothing to take off
        remove: (id) => void this.#filterCommand(() => (this.#filters.some((f) => f.id === id)
          ? pageCommand('removePageFilter', { filter: id }) : undefined), 'Remove Filter').catch((e: unknown) => this.#refused(e)),
        add: (cls, condition) => void this.setFilter({ class: cls, condition }).catch((e: unknown) => this.#refused(e)),
      });
      this.#filterSlot.replaceWith(made.element);
      this.#filterBar = made;
      // painted as the page is now, which may have moved on while the bar was fetched
      this.#paintFilters();
    }).catch((e: unknown) => {
      // not fetched (the network): said, and asked for again at the next change
      this.#filterBarLoading = false;
      this.#refused(new Error(`the page's filter bar did not load: ${e instanceof Error ? e.message : String(e)}`));
    });
  }


  // -- the page's menu --------------------------------------------------------------

  #toggleMenu(): void {
    if (this.#menu.open) {
      this.#menu.close();
      return;
    }
    const at = this.#burger.getBoundingClientRect();
    this.#menu.show(this.#menuGroups(), at.left, at.bottom, this.#burger);
    if (this.#menu.open) this.#burger.setAttribute('aria-expanded', 'true');
  }

  /** A host's entries for one place (`MenuItem.section`, the file group unless named). */
  #hostItems(section: 'file' | 'view' | 'data'): MenuItem[] {
    return (this.#options.hostMenu?.() ?? []).filter((i) => (i.section ?? 'file') === section);
  }

  /**
   * THE PAGE'S MENU: the page's things only -- New (a data source, a blank page), the host's data entries, the page's
   * layout, the host's file entries (Save, Open, Share) and Export of the page, the host's View entries, Settings.
   */
  #menuGroups(): MenuGroup[] {
    const page = this.#page;
    const arranged = !this.empty && page.editing;
    const submenu = (label: string, items: MenuItem[]): MenuItem[] => (items.length > 0 ? [{ label, submenu: items }] : []);
    const groups: MenuGroup[] = [
      { label: '', items: submenu('New', [
        ...(this.#options.openSource ? [{ id: 'source.new' as const, label: 'Data Source…' }] : []),
        { id: 'sheet.new' as const, label: 'Sheet', ...(page.editing ? {} : { disabled: true }) },
        ...(this.#options.onBlankPage ? [{ id: 'page.blank' as const, label: 'Blank Page', separated: true }] : []),
      ]) },
      { label: '', items: this.#hostItems('data') },
      { label: '', items: [
        { id: 'page.arrange', label: 'Arrange…', ...(arranged ? {} : { disabled: true }) },
        // the page's history: each step by its name, as a grid's own Undo names its
        { id: 'page.undo', label: this.undoLabel !== undefined ? `Undo ${this.undoLabel}` : 'Undo', ...(this.undoLabel !== undefined ? {} : { disabled: true }) },
        { id: 'page.redo', label: this.redoLabel !== undefined ? `Redo ${this.redoLabel}` : 'Redo', ...(this.redoLabel !== undefined ? {} : { disabled: true }) },
        { id: 'page.editLayout', label: 'Edit Layout', checked: page.editing },
      ] },
      { label: '', items: [
        ...this.#hostItems('file'),
        ...submenu('Export', this.#options.download
          ? [{ id: 'page.export' as const, label: 'Page File (JSON)', ...(this.saveRefusal() === undefined ? {} : { disabled: true }) }]
          : []),
      ] },
      { label: '', items: submenu('View', this.#hostItems('view')) },
      { label: '', items: [{ id: 'view.settings', label: 'Settings...', ...(this.grids.length > 0 ? {} : { disabled: true }) }] },
    ];
    return groups.filter((g) => g.items.length > 0);
  }

  #onMenu(item: MenuItem): void {
    switch (item.id) {
      case 'source.new':
        void this.addSource();
        return;
      case 'page.blank':
        this.#options.onBlankPage?.();
        return;
      case 'sheet.new':
        this.#page.addSheet();
        return;
      case 'page.arrange':
        this.#page.showLayouts(this.#burger.isConnected ? this.#burger : this.#root);
        return;
      case 'page.undo':
        this.undo();
        return;
      case 'page.redo':
        this.redo();
        return;
      case 'page.editLayout':
        this.#page.setEditing(!this.#page.editing);
        return;
      case 'page.export': {
        // the page as its host keeps it, when it keeps more than the board (a newer writer's fields, its model), as a
        // page file by DataCube's one rule (pageFile: Python's save(path) asks it too), or refused in its words
        // named by pageFile's rule (Python's save(path) asks it too), the file after it
        const doc = this.#options.pageNow ? this.#options.pageNow(this.#name) : this.document(this.#name);
        if (!doc) return;
        const made = pageFile(doc);
        if ('refused' in made) {
          this.#options.refuse?.(made.refused);
          return;
        }
        this.#options.download?.(`${made.name}.page.json`, 'application/json', made.file);
        return;
      }
      case 'view.settings': {
        const first = this.grids[0];
        if (first !== undefined) this.grid(first)?.openSettings();
        return;
      }
      default:
        this.#options.onHostMenu?.(item);
    }
  }
}

/** Where a grid starts again when an undo makes it again: its query, its configuration and its open groups. */
function startOf(grid: PageGrid): GridStart {
  return { snapshot: grid.snapshot, configuration: grid.configuration, tree: grid.tree };
}

/** A fold's chevron, drawn rather than typed, as the cube's own folds draw theirs. */
function chevron(doc: Document, direction: 'up' | 'down'): SVGSVGElement {
  const ns = 'http://www.w3.org/2000/svg';
  const svg = doc.createElementNS(ns, 'svg');
  svg.setAttribute('class', 'dc-chevron-icon');
  svg.setAttribute('viewBox', '0 0 12 12');
  svg.setAttribute('width', '12');
  svg.setAttribute('height', '12');
  svg.setAttribute('aria-hidden', 'true');
  const path = doc.createElementNS(ns, 'path');
  path.setAttribute('d', direction === 'up' ? 'M2.5 7.5 6 4l3.5 3.5' : 'M2.5 4.5 6 8l3.5-3.5');
  path.setAttribute('fill', 'none');
  path.setAttribute('stroke', 'currentColor');
  path.setAttribute('stroke-width', '1.5');
  path.setAttribute('stroke-linecap', 'round');
  path.setAttribute('stroke-linejoin', 'round');
  svg.append(path);
  return svg;
}

