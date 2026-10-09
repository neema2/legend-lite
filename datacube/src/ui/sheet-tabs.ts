// A PAGE'S SHEET TABS, in its bar, as a browser's tabs (docs/DATACUBE_PAGES_DESIGN_2026_10_09.md §7.1; the user,
// 2026-10-09): the sheet shown raised, a click shows another, a double click renames one in place, the + adds one, a
// tab's × deletes it (on the tab shown and the tab under the pointer; none for the last sheet, or on a locked page), its
// right-click menu has Rename, Move Left, Move Right and Delete, and a tab dragged along the strip reorders.
// Many sheets shrink their tabs, then the strip scrolls, with a list of every sheet at its end.
//
// THE KEYBOARD (the APG tabs pattern): one tab in the Tab order, the shown one; Left and Right (Home, End) move to
// another and show it; F2 renames; the context-menu key opens its menu. No page-wide shortcut: the browser keeps
// Ctrl+PgUp and Ctrl+PgDn for its own tabs.
//
// What a sheet IS is the page's (page/cube-page.ts); this strip only says the sheets and asks for changes.

import { MenuView } from './menu-view.ts';
import type { MenuGroup, MenuItem } from './menu.ts';

/** A sheet as its tab says it. */
export interface SheetTab {
  readonly id: string;
  readonly label: string;
}

export interface SheetTabsOptions {
  readonly onShow: (id: string) => void;
  readonly onAdd: () => void;
  /** A tab renamed: its new name, or '' to be named after what it shows again. */
  readonly onRename: (id: string, name: string) => void;
  /** A tab moved to `to` in the order (counted with it taken out). */
  readonly onMove: (id: string, to: number) => void;
  /** Its menu's Delete: the page asks first when the sheet has tiles. */
  readonly onRemove: (id: string) => void;
}

/** How far a pressed tab moves before it is dragged rather than clicked, in pixels. */
const DRAG_PX = 4;

export class SheetTabs {
  readonly #doc: Document;
  readonly #options: SheetTabsOptions;
  readonly #root: HTMLElement;
  readonly #strip: HTMLElement;
  readonly #add: HTMLButtonElement;
  readonly #list: HTMLButtonElement;
  readonly #menu: MenuView;
  readonly #observer: ResizeObserver | undefined;
  #tabs: readonly SheetTab[] = [];
  #shown = '';
  #editing = true;
  /** The tab a dragged tile is over (marked; page/cube-page.ts drops it there). */
  #hovered: string | undefined;
  /** A tab being renamed, while its field is open. */
  #renaming: string | undefined;
  /** Each sheet's tab, kept from paint to paint: a click that shows a sheet leaves its tab, so a double click renames it. */
  readonly #els = new Map<string, HTMLElement>();

  constructor(doc: Document, options: SheetTabsOptions) {
    this.#doc = doc;
    this.#options = options;
    this.#root = doc.createElement('div');
    this.#root.className = 'dc-sheet-tabs';
    this.#strip = doc.createElement('div');
    this.#strip.className = 'dc-sheet-strip';
    this.#strip.setAttribute('role', 'tablist');
    this.#strip.setAttribute('aria-label', 'Sheets');
    this.#add = doc.createElement('button');
    this.#add.type = 'button';
    this.#add.className = 'dc-sheet-add';
    this.#add.textContent = '+';
    this.#add.title = 'A new sheet';
    this.#add.setAttribute('aria-label', 'New sheet');
    this.#add.addEventListener('click', () => this.#options.onAdd());
    // every sheet, when they are more than the strip shows
    this.#list = doc.createElement('button');
    this.#list.type = 'button';
    this.#list.className = 'dc-sheet-list';
    this.#list.textContent = '▾';
    this.#list.title = 'Every sheet';
    this.#list.setAttribute('aria-label', 'Every sheet');
    this.#list.setAttribute('aria-haspopup', 'menu');
    this.#list.hidden = true;
    this.#list.addEventListener('click', () => {
      const at = this.#list.getBoundingClientRect();
      this.#showMenu([{ label: '', items: this.#tabs.map((t) => ({ id: `sheet.show.${t.id}` as const, label: t.label, checked: t.id === this.#shown })) }],
        at.left, at.bottom, this.#list);
    });
    this.#root.append(this.#strip, this.#add, this.#list);
    this.#menu = new MenuView(doc, { onSelect: (item) => this.#onMenu(item) });
    const View = doc.defaultView;
    this.#observer = View && 'ResizeObserver' in View
      ? new (View as unknown as { ResizeObserver: typeof ResizeObserver }).ResizeObserver(() => this.#paintOverflow())
      : undefined;
    this.#observer?.observe(this.#strip);
  }

  /** The strip: its tabs, its +, its list of every sheet. */
  get element(): HTMLElement {
    return this.#root;
  }

  /**
   * The sheets as they are: `shown` raised, and -- `editing` false, a locked page -- no +, no renaming, moving or
   * deleting (the sheets still switch).
   */
  paint(tabs: readonly SheetTab[], shown: string, editing: boolean): void {
    this.#tabs = tabs;
    this.#shown = shown;
    this.#editing = editing;
    this.#add.hidden = !editing;
    const focused = this.#strip.contains(this.#doc.activeElement);
    const ids = new Set(tabs.map((t) => t.id));
    for (const [id, el] of [...this.#els]) {
      if (ids.has(id)) continue;
      el.remove();
      this.#els.delete(id);
    }
    for (const t of tabs) {
      let el = this.#els.get(t.id);
      if (!el) {
        el = this.#tab(t.id);
        this.#els.set(t.id, el);
      }
      this.#paintTab(el, t);
    }
    // in the sheets' order (an element already in place is not moved: its focus and its pointer stay)
    tabs.forEach((t, i) => {
      const el = this.#els.get(t.id)!;
      if (this.#strip.children[i] !== el) this.#strip.insertBefore(el, this.#strip.children[i] ?? null);
    });
    const raised = this.#tabAt(shown);
    raised?.scrollIntoView?.({ block: 'nearest', inline: 'nearest' });
    // a tab being renamed keeps its field, and the field its focus
    if (focused && this.#renaming === undefined) raised?.focus();
    this.#paintOverflow();
  }

  /** The sheet whose tab is at a point of the window, if any. */
  at(x: number, y: number): string | undefined {
    for (const tab of this.#strip.querySelectorAll<HTMLElement>('.dc-sheet-tab')) {
      const r = tab.getBoundingClientRect();
      if (x >= r.left && x <= r.right && y >= r.top && y <= r.bottom) return tab.dataset['sheet'];
    }
    return undefined;
  }

  /** Mark the tab a dragged tile is over (undefined: none). */
  hover(id: string | undefined): void {
    if (id === this.#hovered) return;
    this.#hovered = id;
    for (const tab of this.#strip.querySelectorAll<HTMLElement>('.dc-sheet-tab')) {
      tab.classList.toggle('dc-sheet-tab-target', tab.dataset['sheet'] === id);
    }
  }

  /** Open a tab's name for editing (a double click, F2, its menu's Rename). */
  rename(id: string): void {
    const tab = this.#tabAt(id);
    const sheet = this.#tabs.find((t) => t.id === id);
    if (!tab || !sheet || !this.#editing) return;
    this.#renaming = id;
    const field = this.#doc.createElement('input');
    field.className = 'dc-sheet-name';
    field.value = sheet.label;
    field.setAttribute('aria-label', 'Sheet name');
    // the tab's own label and ×, back in place when the field goes
    const label = tab.querySelector('.dc-sheet-label')!;
    const close = tab.querySelector('.dc-sheet-close');
    let done = false;
    const finish = (keep: boolean): void => {
      if (done) return;
      done = true;
      this.#renaming = undefined;
      tab.replaceChildren(label, ...(close ? [close] : []));
      if (keep && field.value.trim() !== sheet.label) this.#options.onRename(id, field.value.trim());
      this.#tabAt(id)?.focus();
    };
    field.addEventListener('keydown', (e) => {
      e.stopPropagation();
      if (e.key === 'Enter') finish(true);
      if (e.key === 'Escape') finish(false);
    });
    field.addEventListener('blur', () => finish(true));
    tab.replaceChildren(field);
    field.focus();
    field.select();
  }

  dispose(): void {
    this.#menu.close();
    this.#observer?.disconnect();
    this.#root.remove();
  }

  // -- a tab ---------------------------------------------------------------------------------------------------

  /** A sheet's tab, its handlers reading where it is now (`paint` keeps it while its sheet lasts). */
  #tab(id: string): HTMLElement {
    const doc = this.#doc;
    const tab = doc.createElement('div');
    tab.className = 'dc-sheet-tab';
    tab.dataset['sheet'] = id;
    tab.setAttribute('role', 'tab');
    const label = doc.createElement('span');
    label.className = 'dc-sheet-label';
    // its ×: deleted as its menu's Delete does (the page asks first when it has tiles)
    const close = doc.createElement('button');
    close.type = 'button';
    close.className = 'dc-sheet-close';
    close.textContent = '\u00d7';
    close.tabIndex = -1;
    close.addEventListener('pointerdown', (e) => e.stopPropagation());
    close.addEventListener('dblclick', (e) => e.stopPropagation());
    close.addEventListener('click', (e) => {
      e.stopPropagation();
      this.#options.onRemove(id);
    });
    tab.append(label, close);
    tab.addEventListener('dblclick', () => this.rename(id));
    tab.addEventListener('contextmenu', (e) => {
      e.preventDefault();
      this.#tabMenu(id, e.clientX, e.clientY, tab);
    });
    tab.addEventListener('keydown', (e) => this.#onKey(e, id, tab));
    tab.addEventListener('pointerdown', (e) => this.#press(e, id, tab));
    return tab;
  }

  #paintTab(tab: HTMLElement, sheet: SheetTab): void {
    const shown = sheet.id === this.#shown;
    tab.setAttribute('aria-selected', String(shown));
    tab.tabIndex = shown ? 0 : -1;
    tab.classList.toggle('dc-sheet-tab-shown', shown);
    tab.classList.toggle('dc-sheet-tab-target', sheet.id === this.#hovered);
    tab.title = sheet.label;
    const label = tab.querySelector('.dc-sheet-label');
    if (label) label.textContent = sheet.label;
    const close = tab.querySelector<HTMLButtonElement>('.dc-sheet-close');
    if (close) {
      // the last sheet stays, and a locked page deletes none
      close.hidden = !this.#editing || this.#tabs.length < 2;
      close.title = `Delete ${sheet.label}`;
      close.setAttribute('aria-label', `Delete the sheet ${sheet.label}`);
    }
  }

  #tabAt(id: string): HTMLElement | null {
    return this.#els.get(id) ?? null;
  }

  #onKey(e: KeyboardEvent, id: string, tab: HTMLElement): void {
    if (this.#renaming !== undefined) return;
    const index = this.#tabs.findIndex((t) => t.id === id);
    const last = this.#tabs.length - 1;
    const to = e.key === 'ArrowLeft' ? index - 1 : e.key === 'ArrowRight' ? index + 1
      : e.key === 'Home' ? 0 : e.key === 'End' ? last : undefined;
    if (to !== undefined) {
      e.preventDefault();
      const next = this.#tabs[Math.max(0, Math.min(last, to))];
      if (next && next.id !== id) this.#options.onShow(next.id);
      this.#tabAt(next?.id ?? id)?.focus();
      return;
    }
    if (e.key === 'F2') {
      e.preventDefault();
      this.rename(id);
      return;
    }
    if (e.key === 'ContextMenu' || (e.key === 'F10' && e.shiftKey)) {
      e.preventDefault();
      const r = tab.getBoundingClientRect();
      this.#tabMenu(id, r.left, r.bottom, tab);
    }
  }

  /**
   * A tab pressed: a click shows it (on release, so a double click can rename it); dragged past a few pixels, it moves
   * along the strip -- its place marked -- and is put there on release. Escape or a lost pointer leaves it where it was.
   */
  #press(e: PointerEvent, id: string, tab: HTMLElement): void {
    if (e.button !== 0 || this.#renaming !== undefined) return;
    const startX = e.clientX;
    let dragging = false;
    let to = -1;
    tab.setPointerCapture?.(e.pointerId);
    const others = (): HTMLElement[] => [...this.#strip.querySelectorAll<HTMLElement>('.dc-sheet-tab')].filter((t) => t !== tab);
    const move = (ev: PointerEvent): void => {
      if (!dragging && (!this.#editing || Math.abs(ev.clientX - startX) < DRAG_PX)) return;
      dragging = true;
      tab.classList.add('dc-sheet-tab-dragging');
      // its place: before the first other tab whose middle is right of the pointer
      const rest = others();
      to = rest.findIndex((t) => {
        const r = t.getBoundingClientRect();
        return ev.clientX < r.left + r.width / 2;
      });
      if (to < 0) to = rest.length;
      rest.forEach((t, i) => {
        t.classList.toggle('dc-sheet-tab-before', i === to);
        t.classList.toggle('dc-sheet-tab-after', to === rest.length && i === rest.length - 1);
      });
    };
    const end = (apply: boolean): void => {
      tab.removeEventListener('pointermove', move);
      tab.removeEventListener('pointerup', up);
      tab.removeEventListener('pointercancel', cancel);
      tab.removeEventListener('lostpointercapture', cancel);
      this.#doc.removeEventListener('keydown', escape, true);
      tab.classList.remove('dc-sheet-tab-dragging');
      for (const t of others()) t.classList.remove('dc-sheet-tab-before', 'dc-sheet-tab-after');
      if (!apply) return;
      if (dragging) {
        const from = this.#tabs.findIndex((t) => t.id === id);
        if (to >= 0 && to !== from) this.#options.onMove(id, to);
      } else if (id !== this.#shown) {
        this.#options.onShow(id);
      }
    };
    const up = (): void => end(true);
    const cancel = (): void => end(false);
    const escape = (ev: KeyboardEvent): void => {
      if (ev.key !== 'Escape') return;
      ev.stopPropagation();
      end(false);
    };
    tab.addEventListener('pointermove', move);
    tab.addEventListener('pointerup', up);
    tab.addEventListener('pointercancel', cancel);
    tab.addEventListener('lostpointercapture', cancel);
    this.#doc.addEventListener('keydown', escape, true);
  }

  #tabMenu(id: string, x: number, y: number, trigger: HTMLElement): void {
    const at = this.#tabs.findIndex((t) => t.id === id);
    const locked = this.#editing ? {} : { disabled: true };
    this.#showMenu([
      { label: '', items: [{ id: `sheet.rename.${id}` as const, label: 'Rename', ...locked }] },
      { label: '', items: [
        { id: `sheet.left.${id}` as const, label: 'Move Left', ...(this.#editing && at > 0 ? {} : { disabled: true }) },
        { id: `sheet.right.${id}` as const, label: 'Move Right', ...(this.#editing && at < this.#tabs.length - 1 ? {} : { disabled: true }) },
      ] },
      // the last sheet stays: a page has one at least
      { label: '', items: [{ id: `sheet.delete.${id}` as const, label: 'Delete', ...(this.#editing && this.#tabs.length > 1 ? {} : { disabled: true }) }] },
    ], x, y, trigger);
  }

  #showMenu(groups: readonly MenuGroup[], x: number, y: number, trigger: HTMLElement): void {
    this.#menu.show(groups, x, y, trigger);
  }

  #onMenu(item: MenuItem): void {
    const id = item.id ?? '';
    const of = (verb: string): string | undefined => (id.startsWith(`sheet.${verb}.`) ? id.slice(`sheet.${verb}.`.length) : undefined);
    const show = of('show');
    if (show !== undefined) this.#options.onShow(show);
    const rename = of('rename');
    if (rename !== undefined) this.rename(rename);
    const left = of('left');
    const right = of('right');
    const moved = left ?? right;
    if (moved !== undefined) {
      const at = this.#tabs.findIndex((t) => t.id === moved);
      this.#options.onMove(moved, left !== undefined ? at - 1 : at + 1);
    }
    const remove = of('delete');
    if (remove !== undefined) this.#options.onRemove(remove);
  }

  /** The list of every sheet, while the strip cannot show them all. */
  #paintOverflow(): void {
    this.#list.hidden = this.#strip.scrollWidth <= this.#strip.clientWidth + 1;
  }
}
