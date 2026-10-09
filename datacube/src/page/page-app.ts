// A PAGE OF ITS OWN (docs/DATACUBE_PAGES_DESIGN_2026_10_09.md §3.1, §6): DataCube's app, above its grids.
//
// The page has a thin bar that is not a tile -- its name, the page's menu, the host's status line -- and below it the
// board (page/cube-page.ts) of grids and charts. Every grid is a tile holding a compact CubeApp, made the same way
// whether it is the first or the fifth (`addGrid`), so any of them can be removed; with none left the page is empty
// and shows the host's choices of a source. The page's menu holds the page's things only (New, Open, Save, Share,
// Arrange, Settings, the host's own entries); each grid's own menu, in its tile's header, holds the grid's.
//
// ONE GRID ALONE fills the page with no frame, and its header -- its source, Live/Snapped, its menu -- sits at the
// right of the bar's strip: one strip, as a grid alone looks today (the user, 2026-10-09). A second tile moves it back.
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
}

/** How the host makes a grid on the page, in `host`, wired to the page by `options` (its charts, copies, removal). */
export type GridMaker = (host: HTMLElement, options: SpawnOptions) => PageGrid;

export interface PageAppOptions {
  /** Where the page goes: it fills it. */
  readonly host: HTMLElement;
  /** The page's name, in its bar, until `setTitle`. */
  readonly title?: string;
  /** The host's own menu entries (Save, Open, Share, where the planner runs), by their `section`. */
  readonly hostMenu?: () => readonly MenuItem[];
  readonly onHostMenu?: (item: MenuItem) => void;
  /** The host's status line, in the page's bar (planner progress, an error): `slot` is its place. */
  readonly hostStatus?: (slot: HTMLElement) => void;
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
}

export class PageApp {
  readonly #options: PageAppOptions;
  readonly #doc: Document;
  readonly #root: HTMLElement;
  readonly #title: HTMLElement;
  readonly #burger: HTMLButtonElement;
  readonly #alone: HTMLElement;
  readonly #boardHost: HTMLElement;
  readonly #empty: HTMLElement;
  readonly #menu: MenuView;
  /** Every grid on the page, on the board or kept off it for a detached chart, by its id. */
  readonly #grids = new Map<string, PageGrid>();
  #page: CubePage;
  #name: string;
  #disposed = false;

  constructor(options: PageAppOptions) {
    this.#options = options;
    this.#doc = options.host.ownerDocument;
    this.#name = options.title ?? '';
    const doc = this.#doc;
    const root = doc.createElement('div');
    root.className = 'dc-page';
    // the bar: the page's menu, its name, the host's status, and a lone grid's header at the right
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
    const status = doc.createElement('span');
    status.className = 'dc-page-status';
    this.#alone = doc.createElement('span');
    this.#alone.className = 'dc-page-alone dc-tile-cube';
    bar.append(this.#burger, this.#title, status, this.#alone);
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
    options.hostStatus?.(status);
    options.empty(this.#empty);
    this.#page = this.#newBoard();
    this.#paintEmpty();
  }

  /** The page's own element: where its grids' windows float, above every tile. */
  get root(): HTMLElement {
    return this.#root;
  }

  /** The page's name, in its bar. */
  get title(): string {
    return this.#name;
  }

  setTitle(name: string): void {
    this.#name = name;
    this.#title.textContent = name;
  }

  /** The grids on the board, in reading order (not those kept off it for a detached chart). */
  get grids(): readonly string[] {
    const on = new Set(this.#page.grids.keys());
    return this.#page.tileIds.filter((id) => on.has(id));
  }

  /** A grid on the page, by its id (on the board or kept off it). */
  grid(id: string): PageGrid | undefined {
    return this.#grids.get(id);
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
    let made: PageGrid | undefined;
    const id = this.#page.addGridOver((host, options) => (made = make(host, options)), how);
    if (made) this.#grids.set(id, made);
    this.#paintEmpty();
    return id;
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
    return this.#page.views((grid) => (this.#grids.has(grid) ? grid : undefined));
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
    const ids = [...this.grids, ...this.#page.kept.keys()];
    if (ids.length === 0) return undefined;
    const cubes: { id: string; cube: CubeDocument }[] = [];
    for (const id of ids) {
      const cube = this.#grids.get(id)?.cubeDocument(name, unknown?.cubes?.get(id));
      if (!cube) return undefined;
      cubes.push({ id, cube });
    }
    return writePageOf({ name, cubes, views: this.views(), ...(unknown?.page ? { unknown: unknown.page } : {}) });
  }

  /** Why the page cannot be saved now, if it cannot: nothing on it, or a grid's own reason, named. */
  saveRefusal(): string | undefined {
    const ids = [...this.grids, ...this.#page.kept.keys()];
    if (ids.length === 0) return 'There is nothing on this page to save: add a data source first.';
    for (const id of ids) {
      const refused = this.#grids.get(id)?.saveRefusal();
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
    const order = tiles(page.layout);
    const grids = page.views.filter((v) => v.kind === 'grid')
      .sort((a, b) => order.indexOf(a.id) - order.indexOf(b.id));
    const tileOf = new Map<string, string>();
    for (const view of grids) {
      const make = makers.get(view.cube);
      if (!make || tileOf.has(view.cube)) continue;
      tileOf.set(view.cube, this.addGrid(make, { id: view.id, ...(view.title ? { title: view.title } : {}) }));
    }
    for (const { id } of page.cubes) {
      const make = makers.get(id);
      if (tileOf.has(id) || !make) continue;
      let made: PageGrid | undefined;
      this.#page.keepGrid(id, (host, options) => (made = make(host, options)));
      if (made) this.#grids.set(id, made);
    }
    this.#page.restore(page, (cube) => tileOf.get(cube) ?? (this.#page.kept.has(cube) ? cube : undefined));
    this.#paintEmpty();
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
      onChange: () => this.#options.onChange?.(),
      onEmpty: () => this.#paintEmpty(),
      onTiles: () => this.#onTiles(),
      onSettingsChanged: (values) => {
        for (const grid of this.#grids.values()) grid.useSettings(values);
        this.#options.onSettingsChanged?.(values);
      },
    });
  }

  /** A tile came or went: grids taken off forgotten, a lone grid shown in the bar, the empty page when it is. */
  #onTiles(): void {
    const live = new Set([...this.#page.grids.keys(), ...this.#page.kept.keys()]);
    for (const id of [...this.#grids.keys()]) if (!live.has(id)) this.#grids.delete(id);
    this.#paintEmpty();
  }

  #paintEmpty(): void {
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
        if (first !== undefined) this.#grids.get(first)?.openSettings();
        return;
      }
      default:
        this.#options.onHostMenu?.(item);
    }
  }
}
