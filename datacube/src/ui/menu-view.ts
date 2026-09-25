// Rendering the context menu.
//
// Thin on purpose: the menu's content is decided in menu.ts and
// tested as data, so this only has to place it, keep it on screen,
// and make it operable from the keyboard.
//
// The APG menu pattern, in the parts that matter: role=menu with
// role=menuitem children, arrow keys to move, Escape to close, focus
// returned to wherever it came from. A context menu that traps focus
// or loses it is worse than no context menu, because the user cannot
// get back to the grid.

import type { MenuGroup, MenuItem } from './menu.ts';

export interface MenuViewOptions {
  readonly onSelect: (item: MenuItem) => void;
  readonly onClose?: () => void;
}

export class MenuView {
  readonly #doc: Document;
  readonly #options: MenuViewOptions;
  #el: HTMLElement | null = null;
  #returnFocus: Element | null = null;

  constructor(doc: Document, options: MenuViewOptions) {
    this.#doc = doc;
    this.#options = options;
  }

  get open(): boolean {
    return this.#el !== null;
  }

  /** What opened the menu; a press on it must not dismiss. */
  #trigger: HTMLElement | null = null;

  /** Items currently rendered, in order. For tests. */
  get items(): HTMLElement[] {
    return this.#el
      ? [...this.#el.querySelectorAll<HTMLElement>('[role="menuitem"], [role="menuitemcheckbox"]')]
      : [];
  }

  /**
   * @param trigger the control that opened the menu, if any. A
   *   pointer press on it does NOT dismiss: that press belongs to
   *   the click which will toggle the menu shut, and closing here
   *   first would let it reopen instead -- which is precisely how
   *   the title bar menu became impossible to get rid of.
   */
  show(
    groups: readonly MenuGroup[],
    x: number,
    y: number,
    trigger?: HTMLElement,
  ): void {
    this.close();
    if (groups.length === 0) return;
    this.#trigger = trigger ?? null;

    this.#returnFocus = this.#doc.activeElement;
    const menu = this.#doc.createElement('div');
    menu.className = 'dc-menu';
    // Focusable, but not in the tab order: focus is put here
    // deliberately when the menu opens.
    menu.tabIndex = -1;
    menu.setAttribute('role', 'menu');
    menu.tabIndex = -1;

    groups.forEach((group, gi) => {
      if (gi > 0) {
        const sep = this.#doc.createElement('div');
        sep.className = 'dc-menu-sep';
        // Decoration between groups, not an item: announcing it would
        // make the menu read as twice as long as it is.
        sep.setAttribute('role', 'separator');
        menu.appendChild(sep);
      }
      for (const item of group.items) {
        menu.appendChild(this.#item(item));
      }
    });

    menu.addEventListener('keydown', this.#onKeyDown);
    // ON THE DOCUMENT, not on the menu.
    //
    // The menu had no dismissal at all beyond choosing an entry: a
    // click anywhere else left it standing, and Escape only worked
    // while focus was still inside it -- which one click elsewhere
    // ends. So a person who opened it and then looked away had no
    // way to close it short of reloading.
    //
    // Pointerdown rather than click, so the menu is gone before the
    // press it was dismissed by can act on whatever is underneath;
    // and captured, so a handler that stops propagation cannot keep
    // the menu alive.
    this.#doc.addEventListener('pointerdown', this.#onOutside, true);
    this.#doc.addEventListener('keydown', this.#onEscape, true);
    this.#doc.body.appendChild(menu);
    this.#el = menu;
    this.#place(x, y);
    // THE MENU, NOT ITS FIRST ITEM.
    //
    // CSS opens a submenu on hover AND on focus-within, so focusing
    // the first entry flew its submenu open the moment the menu
    // appeared: every right-click on the grid arrived with the whole
    // Export list already unfurled beside it, and hovering anything
    // else left TWO submenus on screen, because one was held open by
    // focus and the other by the pointer. Focus lands on the menu
    // instead -- the keyboard still works, since ArrowDown from
    // nowhere goes to the first entry -- and a submenu now opens
    // only when someone asks for it.
    menu.focus();
  }

  /**
   * One entry, with its submenu if it has one.
   *
   * The submenu is nested INSIDE its parent element rather than
   * positioned as a second popup, so it inherits the parent's
   * lifetime and cannot survive the menu closing -- an orphaned
   * submenu floating over the grid is the classic bug here. CSS
   * opens it on hover and on focus-within, so it works from the
   * keyboard as well as the mouse.
   */
  #item(item: MenuItem): HTMLElement {
    const doc = this.#doc;
    const el = doc.createElement('div');
    el.className = 'dc-menu-item';
    el.setAttribute('role', 'menuitem');
    el.tabIndex = -1;

    if (item.checked !== undefined) {
      // A check mark, and the role that lets a screen reader say it.
      el.setAttribute('role', 'menuitemcheckbox');
      el.setAttribute('aria-checked', String(item.checked));
      const mark = doc.createElement('span');
      mark.className = 'dc-menu-check';
      mark.textContent = item.checked ? '✓' : '';
      el.appendChild(mark);
    }
    const label = doc.createElement('span');
    label.className = 'dc-menu-label';
    label.textContent = item.label;
    el.appendChild(label);

    if (item.disabled) {
      // Shown but dead. The menu keeps its shape so it can be
      // learned; aria-disabled says so rather than leaving a screen
      // reader to discover it by clicking.
      el.classList.add('dc-disabled');
      el.setAttribute('aria-disabled', 'true');
    }

    if (item.submenu && item.submenu.length > 0) {
      el.classList.add('dc-has-submenu');
      el.setAttribute('aria-haspopup', 'menu');
      const chevron = doc.createElement('span');
      chevron.className = 'dc-menu-chevron';
      chevron.setAttribute('aria-hidden', 'true');
      chevron.textContent = '\u203a';
      el.appendChild(chevron);

      const sub = doc.createElement('div');
      sub.className = 'dc-submenu';
      sub.setAttribute('role', 'menu');
      for (const child of item.submenu) sub.appendChild(this.#item(child));
      el.appendChild(sub);
    }

    // A submenu parent has no action of its own; clicking it must
    // not close the menu the user is still navigating.
    if (item.id !== undefined && !item.disabled) {
      el.addEventListener('click', (event) => {
        event.stopPropagation();
        this.#options.onSelect(item);
        this.close();
      });
    } else {
      el.addEventListener('click', (event) => event.stopPropagation());
    }
    return el;
  }

  #onOutside = (event: Event): void => {
    const target = event.target;
    if (!(target && 'nodeType' in (target as object))) return;
    const node = target as Node;
    if (this.#el?.contains(node)) return;
    if (this.#trigger?.contains(node)) return;
    this.close();
  };

  #onEscape = (event: KeyboardEvent): void => {
    if (event.key !== 'Escape' || !this.#el) return;
    event.preventDefault();
    this.close();
  };

  close(): void {
    if (!this.#el) return;
    this.#doc.removeEventListener('pointerdown', this.#onOutside, true);
    this.#doc.removeEventListener('keydown', this.#onEscape, true);
    this.#trigger = null;
    this.#el.removeEventListener('keydown', this.#onKeyDown);
    this.#el.remove();
    this.#el = null;
    // Focus goes back where it came from, or the user is stranded
    // outside the grid with no way back to it by keyboard.
    //
    // Duck-typed rather than `instanceof HTMLElement`: the
    // constructor belongs to the document's realm, so in an iframe
    // -- or a test DOM -- the check is false and focus is silently
    // dropped. This is the second place that bit; it is a property of
    // cross-realm DOM, not of any one call site.
    const back = this.#returnFocus as { focus?: unknown } | null;
    if (back && typeof back.focus === 'function') {
      (back as HTMLElement).focus();
    }
    this.#returnFocus = null;
    this.#options.onClose?.();
  }

  /**
   * Keep the menu on screen.
   *
   * Flipped rather than clamped: a menu clamped to the bottom edge
   * sits under the pointer and the first item is whatever the user
   * happens to be hovering, which is how a wrong action gets clicked.
   */
  #place(x: number, y: number): void {
    const el = this.#el;
    if (!el) return;
    const view = this.#doc.defaultView;
    const vw = view?.innerWidth ?? 0;
    const vh = view?.innerHeight ?? 0;
    const rect = el.getBoundingClientRect();
    const left = vw > 0 && x + rect.width > vw ? Math.max(0, x - rect.width) : x;
    const top = vh > 0 && y + rect.height > vh ? Math.max(0, y - rect.height) : y;
    el.style.left = `${left}px`;
    el.style.top = `${top}px`;
  }

  #onKeyDown = (event: KeyboardEvent): void => {
    const items = this.items;
    if (items.length === 0) return;
    const current = items.indexOf(
      this.#doc.activeElement as HTMLElement,
    );

    switch (event.key) {
      case 'ArrowDown':
        event.preventDefault();
        items[(current + 1) % items.length]?.focus();
        break;
      case 'ArrowUp':
        event.preventDefault();
        items[(current - 1 + items.length) % items.length]?.focus();
        break;
      case 'Home':
        event.preventDefault();
        items[0]?.focus();
        break;
      case 'End':
        event.preventDefault();
        items[items.length - 1]?.focus();
        break;
      case 'Escape':
        event.preventDefault();
        this.close();
        break;
      case 'Enter':
      case ' ':
        event.preventDefault();
        (items[current] ?? items[0])?.click();
        break;
      default:
        break;
    }
  };
}
