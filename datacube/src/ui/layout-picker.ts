// THE LAYOUT PICKER (docs/DATACUBE_PAGES_DESIGN_2026_10_09.md §3.3, 1): the common layouts as thumbnails, opened from a
// tile's layout button or the page's Arrange. Each thumbnail is the page's own tiles arranged that way (bands.ts
// `arrange`, drawn small), the tile it was opened from marked, so what it shows is what a click gives. Pointing at a
// thumbnail, or reaching it with the keyboard, previews it on the page; leaving puts the page back; a click arranges
// it. Below them: the page fitting its window or scrolling, and Even out.
//
// The picker goes in the container it is given (the page), not the document's body, so the page's styles reach it
// inside a shadow root (marimo's) as they reach the page.

import {
  type Bands,
  type Preset,
  EMPTY,
  PRESETS,
  arrange,
  draw,
  fitted,
} from '../layout/bands.ts';
import { focusedElement } from '../focus.ts';

/** A thumbnail's size, in pixels. */
const THUMB_W = 64;
const THUMB_H = 44;
const THUMB_GAP = 2;

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
    const grid = doc.createElement('div');
    grid.className = 'dc-layout-options';
    for (const { id, label } of PRESETS) grid.append(this.#option(id, label, choices));
    grid.addEventListener('pointerleave', () => this.#preview(null));
    // the keyboard leaving the thumbnails (Tab to Fit to window): the page as it is again
    grid.addEventListener('focusout', (e) => {
      if (!grid.contains(e.relatedTarget as Node | null)) this.#preview(null);
    });
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
    el.append(grid, foot);
    el.addEventListener('keydown', this.#onKey);
    doc.addEventListener('pointerdown', this.#onOutside, true);
    doc.addEventListener('keydown', this.#onEscape, true);
    this.#container.append(el);
    this.#el = el;
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
    this.#choices = null;
    const back = this.#returnFocus as { focus?: unknown } | null;
    if (back && typeof back.focus === 'function') (back as HTMLElement).focus();
    this.#returnFocus = null;
  }

  /** One layout: its thumbnail and its name, previewed while pointed at or focused, arranged on a click. */
  #option(preset: Preset, label: string, choices: LayoutPickerChoices): HTMLButtonElement {
    const doc = this.#doc;
    const button = doc.createElement('button');
    button.type = 'button';
    button.className = 'dc-layout-option';
    button.dataset['preset'] = preset;
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

  /** The arrow keys move between the thumbnails (a row of four), Home and End to the ends. */
  #onKey = (event: KeyboardEvent): void => {
    const options = this.options;
    const current = options.indexOf(focusedElement(this.#doc) as HTMLButtonElement);
    if (current < 0 && focusedElement(this.#doc) !== this.#el) return;
    const step: Record<string, number> = { ArrowLeft: -1, ArrowRight: 1, ArrowUp: -4, ArrowDown: 4 };
    let next: number | undefined;
    // from the picker itself, any arrow reaches the first thumbnail
    if (event.key in step) next = current < 0 ? 0 : Math.min(options.length - 1, Math.max(0, current + step[event.key]!));
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
