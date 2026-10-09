// THE LAYOUT PICKER (docs/DATACUBE_PAGES_DESIGN_2026_10_09.md §3.3, 1): layouts as thumbnails, opened from a tile's
// layout button or the page's Arrange. Each thumbnail is the page's own tiles arranged that way (bands.ts `arrange`,
// drawn small), the tile it was opened from marked, so what it shows is what a click gives. Pointing at a thumbnail,
// or reaching it with the keyboard, previews it on the page; leaving puts the page back; a click arranges it.
//
// The user, 2026-10-09: "default 8-9 shapes with option to see more and option to do custom". First the standard
// shapes, the same nine in the same order for any number of tiles; "More layouts" opens every other shape, under its
// kind's heading; "Custom" builds rows by hand (how many tiles in each), previewed on the page as it is built. Below
// them: the page fitting its window or scrolling, and Even out.
//
// The picker goes in the container it is given (the page), not the document's body, so the page's styles reach it
// inside a shadow root (marimo's) as they reach the page.

import {
  type Bands,
  type LayoutGroup,
  type OfferedLayout,
  type Preset,
  EMPTY,
  arrange,
  draw,
  fitted,
  gridRows,
  layoutsFor,
} from '../layout/bands.ts';
import { focusedElement } from '../focus.ts';

/** A thumbnail's size, in pixels. */
const THUMB_W = 64;
const THUMB_H = 44;
const THUMB_GAP = 2;

/** The headings the layouts are shown under (bands.ts layoutsFor's groups). */
const GROUPS: Readonly<Record<LayoutGroup, string>> = { rows: 'Rows', large: 'One and the rest', columns: 'Columns' };

export interface LayoutPickerChoices {
  /** The page's tiles in reading order; `first`, the one the picker was opened from (the preset's first slot). */
  readonly tiles: readonly string[];
  readonly first?: string;
  /** Whether the page fits its window now. */
  readonly fit: boolean;
  /** A layout pointed at or reached by the keyboard (null: none, the page as it is). */
  readonly onPreview: (preset: Preset | null) => void;
  /** A layout chosen. */
  readonly onPick: (preset: Preset) => void;
  /** The page set to fit its window, or to scroll. */
  readonly onFit: (fit: boolean) => void;
  /** Every split evened out. */
  readonly onEvenOut: () => void;
}

export class LayoutPicker {
  readonly #container: HTMLElement;
  readonly #doc: Document;
  #el: HTMLElement | null = null;
  #trigger: HTMLElement | null = null;
  #returnFocus: Element | null = null;
  #choices: LayoutPickerChoices | null = null;
  #previewing: Preset | null = null;
  /** Thumbnails to a row: four, six while More layouts is open (the picker wider, to show more at once). */
  #perRow = 4;
  #anchor: HTMLElement | null = null;

  constructor(container: HTMLElement) {
    this.#container = container;
    this.#doc = container.ownerDocument;
  }

  get open(): boolean {
    return this.#el !== null;
  }

  /** The thumbnails, in order. For tests. */
  get options(): HTMLButtonElement[] {
    return this.#el ? [...this.#el.querySelectorAll<HTMLButtonElement>('.dc-layout-option')] : [];
  }

  /** Open by `anchor` (the button pressed); open already from the same anchor, close (the button toggles). */
  show(anchor: HTMLElement, choices: LayoutPickerChoices): void {
    if (this.#el && this.#trigger === anchor) {
      this.close();
      return;
    }
    this.close();
    const doc = this.#doc;
    this.#choices = choices;
    this.#trigger = anchor;
    this.#returnFocus = focusedElement(doc);
    const el = doc.createElement('div');
    el.className = 'dc-layout-picker';
    el.setAttribute('role', 'dialog');
    el.setAttribute('aria-label', 'Layouts');
    el.tabIndex = -1;
    const count = order(choices).length;
    const offered = layoutsFor(count);
    // the thumbnails: the standard shapes, and -- More layouts -- the others; the page as it is again when the pointer
    // or the keyboard leaves them
    const lists = doc.createElement('div');
    lists.className = 'dc-layout-lists';
    const featured = doc.createElement('div');
    featured.className = 'dc-layout-options';
    for (const layout of offered.filter((l) => l.featured)) featured.append(this.#option(layout, 'featured', choices));
    lists.append(featured);
    lists.addEventListener('pointerleave', () => this.#preview(null));
    lists.addEventListener('focusout', (e) => {
      if (!lists.contains(e.relatedTarget as Node | null)) this.#preview(null);
    });
    const others = offered.filter((l) => !l.featured);
    const toggles = doc.createElement('div');
    toggles.className = 'dc-layout-toggles';
    if (others.length > 0) {
      // open, the picker grows -- six to a row, taller -- so more show at once; never so big it hides the page whose
      // preview it shows (the user, 2026-10-09: "make it fatter and longer both to fit more options")
      toggles.append(this.#toggle('More layouts', () => this.#more(others, choices), lists, (open) => {
        this.#perRow = open ? 6 : 4;
        el.classList.toggle('dc-layout-picker-wide', open);
        if (this.#anchor) this.#place(this.#anchor);
      }));
    }
    if (count >= 2) toggles.append(this.#toggle('Custom\u2026', () => this.#custom(count, choices), null));
    const fit = doc.createElement('label');
    fit.className = 'dc-layout-fit';
    const box = doc.createElement('input');
    box.type = 'checkbox';
    box.checked = choices.fit;
    box.addEventListener('change', () => choices.onFit(box.checked));
    fit.append(box, doc.createTextNode(' Fit to window'));
    fit.title = 'The page fills the window and nothing scrolls; unticked, bands keep their heights and the page scrolls';
    const even = doc.createElement('button');
    even.type = 'button';
    even.className = 'dc-layout-even';
    even.textContent = 'Even out';
    even.title = 'Every tile in a row as wide as the others, every band as tall';
    even.addEventListener('click', () => {
      choices.onEvenOut();
      this.close();
    });
    const foot = doc.createElement('div');
    foot.className = 'dc-layout-foot';
    foot.append(fit, even);
    el.append(lists, toggles, foot);
    el.addEventListener('keydown', this.#onKey);
    doc.addEventListener('pointerdown', this.#onOutside, true);
    doc.addEventListener('keydown', this.#onEscape, true);
    this.#container.append(el);
    this.#el = el;
    this.#anchor = anchor;
    this.#perRow = 4;
    this.#place(anchor);
    // the picker, not its first thumbnail: focusing that would preview it the moment the picker opens (an arrow key
    // reaches the thumbnails)
    el.focus();
  }

  close(): void {
    if (!this.#el) return;
    this.#preview(null);
    this.#doc.removeEventListener('pointerdown', this.#onOutside, true);
    this.#doc.removeEventListener('keydown', this.#onEscape, true);
    this.#el.remove();
    this.#el = null;
    this.#trigger = null;
    this.#anchor = null;
    this.#choices = null;
    const back = this.#returnFocus as { focus?: unknown } | null;
    if (back && typeof back.focus === 'function') (back as HTMLElement).focus();
    this.#returnFocus = null;
  }

  /**
   * A button that opens a part of the picker (More layouts, Custom) and closes it again: the part made when opened,
   * into `into` (the thumbnails' lists), or after the toggles.
   */
  #toggle(text: string, make: () => HTMLElement, into: HTMLElement | null, onToggle?: (open: boolean) => void): HTMLButtonElement {
    const button = this.#doc.createElement('button');
    button.type = 'button';
    button.className = 'dc-layout-toggle';
    button.textContent = text;
    button.setAttribute('aria-expanded', 'false');
    let part: HTMLElement | null = null;
    button.addEventListener('click', () => {
      if (part) {
        part.remove();
        part = null;
        this.#preview(null);
      } else {
        part = make();
        if (into) into.append(part);
        else button.parentElement?.after(part);
      }
      button.setAttribute('aria-expanded', String(part !== null));
      onToggle?.(part !== null);
    });
    return button;
  }

  /** Every other layout, under its kind's heading. */
  #more(layouts: readonly OfferedLayout[], choices: LayoutPickerChoices): HTMLElement {
    const doc = this.#doc;
    const grid = doc.createElement('div');
    grid.className = 'dc-layout-options dc-layout-more';
    let group: LayoutGroup | undefined;
    for (const layout of layouts) {
      if (layout.group !== group) {
        group = layout.group;
        const heading = doc.createElement('div');
        heading.className = 'dc-layout-group';
        heading.textContent = GROUPS[group];
        grid.append(heading);
      }
      grid.append(this.#option(layout, layout.group, choices));
    }
    return grid;
  }

  /**
   * CUSTOM ROWS: how many tiles in each row, row by row, each count a step up or down, rows added and taken away;
   * previewed on the page whenever every tile has its place, and applied by Apply. It starts as the grid (bands.ts
   * gridRows).
   */
  #custom(count: number, choices: LayoutPickerChoices): HTMLElement {
    const doc = this.#doc;
    let counts = gridRows(count);
    const panel = doc.createElement('div');
    panel.className = 'dc-layout-custom';
    panel.setAttribute('role', 'group');
    panel.setAttribute('aria-label', 'Custom rows');
    const rows = doc.createElement('div');
    rows.className = 'dc-layout-custom-rows';
    const status = doc.createElement('span');
    status.className = 'dc-layout-custom-status';
    status.setAttribute('aria-live', 'polite');
    const button = (text: string, label: string, onClick: () => void, disabled = false): HTMLButtonElement => {
      const b = doc.createElement('button');
      b.type = 'button';
      b.className = 'dc-layout-step';
      b.textContent = text;
      b.title = label;
      b.setAttribute('aria-label', label);
      b.disabled = disabled;
      b.addEventListener('click', onClick);
      return b;
    };
    const id = (): Preset => (counts.length === 1 ? 'side-by-side' : `rows:${counts.join('-')}`);
    const apply = doc.createElement('button');
    apply.type = 'button';
    apply.className = 'dc-layout-even dc-layout-apply';
    apply.textContent = 'Apply';
    apply.addEventListener('click', () => {
      this.#previewing = null;
      choices.onPick(id());
      this.close();
    });
    const paint = (): void => {
      const placed = counts.reduce((sum, c) => sum + c, 0);
      rows.replaceChildren(...counts.map((c, i) => {
        const row = doc.createElement('div');
        row.className = 'dc-layout-custom-row';
        const name = doc.createElement('span');
        name.className = 'dc-layout-custom-name';
        name.textContent = `Row ${i + 1}`;
        const value = doc.createElement('span');
        value.className = 'dc-layout-custom-count';
        value.textContent = String(c);
        const set = (next: number[]): void => {
          counts = next;
          paint();
        };
        row.append(
          name,
          button('\u2212', `One fewer tile in row ${i + 1}`, () => set(counts.map((x, j) => (j === i ? x - 1 : x))), c <= 1),
          value,
          button('+', `One more tile in row ${i + 1}`, () => set(counts.map((x, j) => (j === i ? x + 1 : x))), placed >= count),
          button('\u00d7', `Take row ${i + 1} away`, () => set(counts.filter((_, j) => j !== i)), counts.length <= 1),
        );
        return row;
      }));
      const add = button('+ Row', 'Add a row of one tile', () => {
        counts = [...counts, 1];
        paint();
      }, placed >= count);
      add.classList.add('dc-layout-add-row');
      rows.append(add);
      status.textContent = placed === count ? `${count} tiles in ${counts.length} ${counts.length === 1 ? 'row' : 'rows'}`
        : placed < count ? `${placed} of ${count} tiles placed: ${count - placed} more to place` : `${placed} of ${count}`;
      apply.disabled = placed !== count;
      // the page as it would be, while every tile has its place
      if (placed === count) this.#preview(id());
    };
    const foot = doc.createElement('div');
    foot.className = 'dc-layout-custom-foot';
    foot.append(status, apply);
    panel.append(rows, foot);
    paint();
    return panel;
  }

  /** One layout: its thumbnail and its name, previewed while pointed at or focused, arranged on a click. */
  #option(layout: OfferedLayout, group: LayoutGroup | 'featured', choices: LayoutPickerChoices): HTMLButtonElement {
    const { id: preset, label } = layout;
    const doc = this.#doc;
    const button = doc.createElement('button');
    button.type = 'button';
    button.className = 'dc-layout-option';
    button.dataset['preset'] = preset;
    button.dataset['group'] = group;
    button.title = label;
    button.setAttribute('aria-label', label);
    button.append(thumbnail(doc, arrange(EMPTY, preset, order(choices)), choices.first));
    button.addEventListener('pointerenter', () => this.#preview(preset));
    button.addEventListener('focus', () => this.#preview(preset));
    button.addEventListener('click', () => {
      // the preview is the page as it will be: no flash back to the old layout between the two
      this.#previewing = null;
      choices.onPick(preset);
      this.close();
    });
    return button;
  }

  #preview(preset: Preset | null): void {
    if (preset === this.#previewing) return;
    this.#previewing = preset;
    this.#choices?.onPreview(preset);
  }

  /** Below the anchor, its right edges lined up; flipped above, or left, when it would leave the window. */
  #place(anchor: HTMLElement): void {
    const el = this.#el;
    if (!el) return;
    const view = this.#doc.defaultView;
    const vw = view?.innerWidth ?? 0;
    const vh = view?.innerHeight ?? 0;
    const at = anchor.getBoundingClientRect();
    const own = el.getBoundingClientRect();
    let left = at.right - own.width;
    if (left < 0) left = Math.min(at.left, Math.max(0, vw - own.width));
    const top = vh > 0 && at.bottom + own.height > vh ? Math.max(0, at.top - own.height) : at.bottom;
    el.style.left = `${Math.max(0, left)}px`;
    el.style.top = `${top}px`;
  }

  /**
   * The arrow keys move between the thumbnails as they are drawn -- four to a row (six while More layouts is open),
   * each kind under its heading: Left
   * and Right to the one before and after, Up and Down to the one above and below (into the kind before or after at
   * the same place in its row, or its last) -- and Home and End to the ends.
   */
  #onKey = (event: KeyboardEvent): void => {
    const options = this.options;
    const current = options.indexOf(focusedElement(this.#doc) as HTMLButtonElement);
    if (current < 0 && focusedElement(this.#doc) !== this.#el) return;
    let next: number | undefined;
    if (current < 0 && ['ArrowLeft', 'ArrowRight', 'ArrowUp', 'ArrowDown'].includes(event.key)) next = 0;
    else if (event.key === 'ArrowLeft') next = Math.max(0, current - 1);
    else if (event.key === 'ArrowRight') next = Math.min(options.length - 1, current + 1);
    else if (event.key === 'ArrowUp' || event.key === 'ArrowDown') {
      next = vertical(options, current, event.key === 'ArrowDown' ? 1 : -1, this.#perRow);
    }
    else if (event.key === 'Home') next = 0;
    else if (event.key === 'End') next = options.length - 1;
    if (next === undefined) return;
    event.preventDefault();
    options[next]?.focus();
  };

  #onOutside = (event: Event): void => {
    // the element pressed, inside whatever shadow root holds it (as menu-view.ts)
    const target = event.composedPath?.()[0] ?? event.target;
    if (!(target && 'nodeType' in (target as object))) return;
    const node = target as Node;
    if (this.#el?.contains(node) || this.#trigger?.contains(node)) return;
    this.close();
  };

  #onEscape = (event: KeyboardEvent): void => {
    if (event.key !== 'Escape' || !this.#el) return;
    event.preventDefault();
    event.stopPropagation();
    this.close();
  };
}

/** The tiles in reading order, the one the picker was opened from first. */
function order(choices: LayoutPickerChoices): string[] {
  const { tiles, first } = choices;
  const ids = tiles.length > 0 ? [...tiles] : ['a', 'b'];
  return first !== undefined && ids.includes(first) ? [first, ...ids.filter((id) => id !== first)] : ids;
}

/** A layout drawn small: every band fitted to the thumbnail, each tile a box, `mark`'s filled. */
function thumbnail(doc: Document, layout: Bands, mark: string | undefined): HTMLElement {
  const el = doc.createElement('span');
  el.className = 'dc-layout-thumb';
  el.setAttribute('aria-hidden', 'true');
  const drawn = draw(fitted(layout, true), THUMB_W, THUMB_H, THUMB_GAP, 0);
  for (const [id, box] of drawn.tiles) {
    const cell = doc.createElement('span');
    cell.className = id === mark ? 'dc-layout-cell dc-layout-cell-mark' : 'dc-layout-cell';
    cell.style.left = `${box.x}px`;
    cell.style.top = `${box.y}px`;
    cell.style.width = `${box.w}px`;
    cell.style.height = `${box.h}px`;
    el.append(cell);
  }
  return el;
}

/** The thumbnail above (`by` -1) or below (+1) option `at`, its kind's options `perRow` to a row: the same column, or the last. */
function vertical(options: readonly HTMLElement[], at: number, by: 1 | -1, perRow: number): number {
  const groups: number[][] = [];
  options.forEach((o, i) => {
    const group = o.dataset['group'];
    if (i === 0 || options[i - 1]!.dataset['group'] !== group) groups.push([]);
    groups[groups.length - 1]!.push(i);
  });
  // the rows as drawn: each kind's options in rows of four
  const rows = groups.flatMap((g) => Array.from({ length: Math.ceil(g.length / perRow) }, (_, r) => g.slice(r * perRow, r * perRow + perRow)));
  const row = rows.findIndex((r) => r.includes(at));
  const target = rows[row + by];
  if (row < 0 || target === undefined) return at;
  const column = rows[row]!.indexOf(at);
  return target[Math.min(column, target.length - 1)]!;
}

