// A PAGE OF ITS OWN (docs/DATACUBE_PAGES_DESIGN_2026_10_09.md §3.1, §6): DataCube's app, above its grids.
//
// The page has a thin bar that is not a tile -- its menu, its name, its fold -- and below it the board (page/cube-page.ts) of grids and charts. Every grid is a tile holding a compact CubeApp, made the same way
// whether it is the first or the fifth (`addGrid`), so any of them can be removed; with none left the page is empty
// and shows the host's choices of a source. The page's menu holds the page's things only (New, Open, Save, Share,
// Arrange, Settings, the host's own entries); each grid's own menu, in its tile's header, holds the grid's.
//
// ONE GRID ALONE fills the page with no frame, and its header -- its source, Live/Snapped, its menu -- sits at the
// right of the bar's strip: one strip, as a grid alone looks today (the user, 2026-10-09). A second tile moves it back.
//
// EVERY GRID IS THE HOST'S: made by a maker the host gave (`addGrid`, `restore`) -- a copy of one (Copy of Grid) by the
// same maker, starting where that grid is now -- so the host knows each grid on the page, and the page knows each one.
//
// THE PAGE IS WHAT IS SAVED: one cube document per grid (each over its own source), a view per tile, the layout
// (`document`); reopening adds each grid again under its saved id, through the host's own way of opening its source
// (`restore`). A grid removed while a frozen chart still reads it is kept off the board for that chart, and saved as a
// cube with no grid view.

import { CubePage, type SpawnOptions, type SpawnedGrid } from './cube-page.ts';
import { MenuView } from '../ui/menu-view.ts';
import type { MenuGroup, MenuItem } from '../ui/menu.ts';
import { tiles } from '../layout/bands.ts';
import { pageToJson, writePageOf, type PageDocument, type PageViews } from '../page-document.ts';
import type { CubeDocument } from '../cube-document.ts';
import type { CubeSnapshot } from '../snapshot.ts';
import type { CubeConfiguration } from '../config.ts';
import type { SettingValues } from '../settings.ts';

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
  setChrome(patch: { readonly showTitleBar?: boolean }): void;
  /** Its status bar's readout from the host asked for again: the page's first grid changed. */
  refreshHostStatus(): void;
}

/** Where a copy of a grid starts: that grid's query and configuration as they are now. */
export interface GridStart {
  readonly snapshot: CubeSnapshot;
  readonly configuration: CubeConfiguration;
}

/**
 * How the host makes a grid on the page, in `host`, wired to the page by `options` (its id, its charts, copies,
 * removal) -- as the host opened it, or, `start`, a copy starting where another grid of the same source is now.
 */
export type GridMaker = (host: HTMLElement, options: SpawnOptions, start?: GridStart) => PageGrid;

export interface PageAppOptions {
  /** Where the page goes: it fills it. */
  readonly host: HTMLElement;
  /** The page's name, in its bar, until `setTitle`. */
  readonly title?: string;
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
   * The host's readout (where the planner runs, what went wrong), at the right of the page's FIRST grid's status bar
   * -- it moves when another grid becomes the first -- on each of that bar's renders.
   */
  readonly hostStatus?: (slot: HTMLElement) => void;
}

export class PageApp {
  readonly #options: PageAppOptions;
  readonly #doc: Document;
  readonly #root: HTMLElement;
  readonly #title: HTMLElement;
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
  #name: string;
  #disposed = false;
  /** The grid whose status bar holds the host's readout. */
  #first: PageGrid | undefined;

  constructor(options: PageAppOptions) {
    this.#options = options;
    this.#doc = options.host.ownerDocument;
    this.#name = options.title ?? '';
    const doc = this.#doc;
    const root = doc.createElement('div');
    root.className = 'dc-page';
    // the bar: the page's menu, its name, its fold, and a lone grid's header at the right
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
    this.#title.className = 'dc-titlebar-title';
    this.#title.textContent = this.#name;
    // the space between the name and the right end (the host's readout is in its first grid's status bar)
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
    bar.append(this.#burger, this.#title, status, this.#fold, this.#alone);
    this.#bar = bar;
    this.#boardHost = doc.createElement('div');
    this.#boardHost.className = 'dc-board-host';
    this.#empty = doc.createElement('div');
    this.#empty.className = 'dc-page-empty';
    root.append(bar, this.#boardHost, this.#empty);
    options.host.replaceChildren(root);
    this.#root = root;
    this.#menu = new MenuView(doc, {
      onSelect: (item) => this.#onMenu(item),
      onClose: () => this.#burger.setAttribute('aria-expanded', 'false'),
    });
    options.empty(this.#empty);
    this.#page = this.#newBoard();
    this.#paintEmpty();
  }

  /**
   * A grid's state changed: the bar says the first grid's report title, and is folded while every grid on the page
   * says its title bar is hidden -- a grid alone as a cube alone always did; several, as the bar was folded by hand.
   */
  #onGridChange(): void {
    this.#paintTitle();
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
    else this.#bar.replaceChildren(this.#burger, this.#title, this.#status, this.#fold, this.#alone);
    if (focused && !this.#bar.contains(this.#doc.activeElement)) (folded ? this.#lip : this.#fold).focus();
  }

  get barFolded(): boolean {
    return this.#bar.classList.contains('dc-collapsed');
  }

  /** The page's name, in its bar. */
  get title(): string {
    return this.#title.textContent ?? '';
  }

  /** The page's own name (a saved page's): '' leaves the bar to say the first grid's report title. */
  setTitle(name: string): void {
    this.#name = name;
    this.#paintTitle();
  }

  /** The bar's name: the page's own, else its first grid's report title (what a grid alone was called). */
  #paintTitle(): void {
    const first = this.grids[0];
    this.#title.textContent = this.#name || (first !== undefined ? this.grid(first)?.configuration.reportTitle ?? '' : '');
  }

  /** The grids on the board, in reading order (not those kept off it for a detached chart). */
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

  /** The grids on the page now, on the board or kept off it. */
  #live(): PageGrid[] {
    return this.cubes.map((id) => this.grid(id)).filter((g): g is PageGrid => g !== undefined);
  }

  /** `make`, made to say what it made: every grid on the page is made through here, under its tile's id. */
  #maker(make: GridMaker, start?: GridStart): (host: HTMLElement, options: SpawnOptions) => PageGrid {
    return (host, options) => {
      const id = options.id;
      const hostStatus = this.#options.hostStatus;
      const grid = make(host, {
        ...options,
        // the host's readout, in this grid's bar while it is the page's first
        ...(hostStatus && id !== undefined ? { hostStatus: (slot: HTMLElement) => { if (this.grids[0] === id) hostStatus(slot); } } : {}),
        // made while the bar is folded: it says so too, or the bar would come back once it is the only grid
        ...(this.barFolded ? { titleBarHidden: true } : {}),
      }, start);
      if (id !== undefined) this.#grids.set(id, { grid, make });
      return grid;
    };
  }

  /** The host's readout moved to the page's first grid, when that is another grid now. */
  #rehomeStatus(): void {
    const first = this.grids[0];
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

  /** Every tile gone -- grids, charts, kept grids -- and the empty page shown (Blank Page; opening in place). */
  clear(): void {
    this.#page.dispose();
    this.#grids.clear();
    this.#page = this.#newBoard();
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
   * it reads (a kept one for a detached chart), the layout.
   */
  views(): PageViews {
    return this.#page.views((grid) => (this.grid(grid) ? grid : undefined));
  }

  /**
   * THE PAGE AS IT IS SAVED (page-document.ts, version 2): one cube per grid -- on the board, then kept off it for a
   * detached chart -- each the grid's own cube document, under the grid's id; the views and the layout. `unknown`:
   * fields a newer writer put in the page and in each cube, written back as they were. Undefined when a grid cannot
   * say its source, or there is nothing on the page.
   */
  document(name: string, unknown?: {
    readonly page?: Readonly<Record<string, unknown>>;
    readonly cubes?: ReadonlyMap<string, Readonly<Record<string, unknown>>>;
  }): PageDocument | undefined {
    const ids = this.cubes;
    if (ids.length === 0) return undefined;
    const cubes: { id: string; cube: CubeDocument }[] = [];
    for (const id of ids) {
      const cube = this.grid(id)?.cubeDocument(name, unknown?.cubes?.get(id));
      if (!cube) return undefined;
      cubes.push({ id, cube });
    }
    return writePageOf({ name, cubes, views: this.views(), ...(unknown?.page ? { unknown: unknown.page } : {}) });
  }

  /** Why the page cannot be saved now, if it cannot: nothing on it, or a grid's own reason, named. */
  saveRefusal(): string | undefined {
    const ids = this.cubes;
    if (ids.length === 0) return 'There is nothing on this page to save: add a data source first.';
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
   * detached charts; then the charts and the layout. A cube with no maker (its source not opened) is left out, with
   * the views that read it. The page is emptied first.
   */
  restore(page: PageDocument, makers: ReadonlyMap<string, GridMaker>): void {
    this.#page.dispose();
    this.#grids.clear();
    this.#page = this.#newBoard();
    // its grids say how its bar is, as they were saved: not as the page before it had its bar
    this.#foldBar(false);
    // in the layout's reading order; a grid the layout has no place for, last
    const order = tiles(page.layout);
    const at = (id: string): number => (order.includes(id) ? order.indexOf(id) : order.length);
    const grids = page.views.filter((v) => v.kind === 'grid').sort((a, b) => at(a.id) - at(b.id));
    const tileOf = new Map<string, string>();
    for (const view of grids) {
      const make = makers.get(view.cube);
      if (!make || tileOf.has(view.cube)) continue;
      tileOf.set(view.cube, this.addGrid(make, { id: view.id, ...(view.title ? { title: view.title } : {}) }));
    }
    for (const { id } of page.cubes) {
      const make = makers.get(id);
      if (tileOf.has(id) || !make) continue;
      this.#page.keepGrid(id, this.#maker(make));
    }
    this.#page.restore(page, (cube) => tileOf.get(cube) ?? (this.#page.kept.has(cube) ? cube : undefined));
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
    this.#grids.clear();
    this.#root.remove();
  }

  // -- the board ------------------------------------------------------------------

  #newBoard(): CubePage {
    return new CubePage({
      host: this.#boardHost,
      onChange: () => {
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
      // a copy (Copy of Grid; a detached chart's own grid) is made by the maker of the grid it copies
      gridLike: (from, snapshot) => {
        const was = this.#grids.get(from);
        if (!was || !this.grid(from)) throw new Error(`no grid ${from} on the page to copy`);
        return this.#maker(was.make, { snapshot, configuration: was.grid.configuration });
      },
    });
  }

  /** A tile came or went: grids taken off forgotten, a lone grid shown in the bar, the empty page when it is. */
  #onTiles(): void {
    const live = new Set(this.cubes);
    for (const id of [...this.#grids.keys()]) if (!live.has(id)) this.#grids.delete(id);
    this.#paintEmpty();
    this.#onGridChange();
  }

  #paintEmpty(): void {
    this.#rehomeStatus();
    const empty = this.empty;
    this.#boardHost.hidden = empty;
    this.#empty.hidden = !empty;
    this.#root.classList.toggle('dc-page-is-empty', empty);
    this.#page.showAlone(empty ? null : this.#alone);
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
        ...(this.#options.onBlankPage ? [{ id: 'page.blank' as const, label: 'Blank Page', separated: true }] : []),
      ]) },
      { label: '', items: this.#hostItems('data') },
      { label: '', items: [
        { id: 'page.arrange', label: 'Arrange…', ...(arranged ? {} : { disabled: true }) },
        { id: 'page.undoLayout', label: 'Undo Layout', ...(page.canUndoLayout ? {} : { disabled: true }) },
        { id: 'page.redoLayout', label: 'Redo Layout', ...(page.canRedoLayout ? {} : { disabled: true }) },
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
      case 'page.arrange':
        this.#page.showLayouts(this.#burger.isConnected ? this.#burger : this.#root);
        return;
      case 'page.undoLayout':
        this.#page.undoLayout();
        return;
      case 'page.redoLayout':
        this.#page.redoLayout();
        return;
      case 'page.editLayout':
        this.#page.setEditing(!this.#page.editing);
        return;
      case 'page.export': {
        const name = this.#name || 'page';
        const doc = this.document(name);
        if (doc) this.#options.download?.(`${name}.page.json`, 'application/json', pageToJson(doc));
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

