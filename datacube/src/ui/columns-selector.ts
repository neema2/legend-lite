// The two-pane column selector: DataCube's drag-to-pivot.
//
// This is the widget every structural editor tab is built from --
// Columns, Horizontal Pivots, Vertical Pivots and Sorts are all one
// of these with a different list and a different trailing control.
// Reading legend-studio's DataCubeEditorColumnsSelector settled four
// behaviours that are not guessable:
//
//  - A move is SCOPED BY THE SEARCH. Selecting six rows, typing a
//    filter that hides four, then pressing add moves two, not six.
//    Without this a search plus a stale selection quietly moves
//    columns the user cannot see.
//  - Double-click moves one row; double-clicking the `[All Columns]`
//    header moves everything the search currently matches.
//  - The available pane is NOT reorderable (its order is the
//    source's) and the selected pane is, because for pivots and
//    sorts the order IS the meaning.
//  - The header carries a match count, so a search that hides
//    everything is distinguishable from an empty list.
//
// The drag payload lives in a module-level variable rather than in
// `dataTransfer`. That is not a shortcut: `dataTransfer.getData` is
// deliberately unreadable during `dragover` in every browser, so a
// drop target cannot decide whether to accept a drag from it. The
// transfer is still populated, for drags that leave the document.

export interface SelectorColumn {
  readonly name: string;
  readonly type?: string;
}

/** What is currently being dragged, within this document. */
interface DragPayload {
  readonly from: 'available' | 'selected';
  readonly names: readonly string[];
}

let dragging: DragPayload | null = null;

/** Exposed for tests and for a host that needs to cancel a drag. */
export function currentDrag(): DragPayload | null {
  return dragging;
}
export function setDrag(payload: DragPayload | null): void {
  dragging = payload;
}

// --------------------------------------------------------------------
// The pure operations. No DOM, so the awkward cases are testable.
// --------------------------------------------------------------------

/** Case-insensitive substring match, as ag-grid's quick filter. */
export function matches(name: string, search: string): boolean {
  const q = search.trim().toLowerCase();
  return q === '' || name.toLowerCase().includes(q);
}

export function filterColumns(
  names: readonly string[],
  search: string,
): string[] {
  return names.filter((n) => matches(n, search));
}

/**
 * Insert names into a list at an index, removing them from wherever
 * they were.
 *
 * Removing first and then inserting is what makes a reorder and a
 * cross-pane move the same operation, so there is one implementation
 * to get the index arithmetic right in rather than two.
 */
export function insertAt(
  list: readonly string[],
  names: readonly string[],
  index: number,
): string[] {
  const moving = new Set(names);
  const kept = list.filter((n) => !moving.has(n));
  // The index was measured against the list BEFORE removal, so it
  // has to shift back past every moved item that sat above it.
  const removedAbove = list
    .slice(0, index)
    .filter((n) => moving.has(n)).length;
  const at = Math.max(0, Math.min(kept.length, index - removedAbove));
  return [...kept.slice(0, at), ...names, ...kept.slice(at)];
}

export function without(
  list: readonly string[],
  names: readonly string[],
): string[] {
  const gone = new Set(names);
  return list.filter((n) => !gone.has(n));
}

/**
 * Where a pointer sits in a list of rows, as an insertion index.
 *
 * Measured against each row's MIDPOINT so the gap the user is aiming
 * at is the one they get. The obvious alternative -- nearest row --
 * makes the position after the last row unreachable, which for a
 * pivot list is the position most often wanted.
 */
export function indexForY(
  boxes: readonly { readonly top: number; readonly height: number }[],
  y: number,
): number {
  for (let i = 0; i < boxes.length; i++) {
    const box = boxes[i] as { top: number; height: number };
    if (y < box.top + box.height / 2) return i;
  }
  return boxes.length;
}

export interface SelectorState {
  /** Every column the selector may offer, in source order. */
  readonly all: readonly SelectorColumn[];
  /** The chosen ones, in the order they were chosen. */
  readonly selected: readonly string[];
}

/** The available pane's contents: everything not selected, in source order. */
export function availableOf(state: SelectorState): string[] {
  const chosen = new Set(state.selected);
  return state.all.map((c) => c.name).filter((n) => !chosen.has(n));
}

// --------------------------------------------------------------------
// The widget.
// --------------------------------------------------------------------

export interface ColumnsSelectorOptions {
  /** Called whenever the selected list changes. */
  readonly onChange: (selected: readonly string[]) => void;
  /** A trailing control per selected row, e.g. a sort direction. */
  readonly actionFor?: (name: string) => HTMLElement | null;
  /** Override the row's text, e.g. to show a display name. */
  readonly labelFor?: (name: string) => string;
  /** Extra text after the label, e.g. the column's type. */
  readonly hintFor?: (name: string) => string | null;
  readonly availableLabel?: string;
  readonly selectedLabel?: string;
}

interface Pane {
  readonly which: 'available' | 'selected';
  readonly root: HTMLElement;
  readonly list: HTMLElement;
  readonly search: HTMLInputElement;
  readonly count: HTMLElement;
  /** Rows the user has highlighted in THIS pane. */
  picked: Set<string>;
}

export class ColumnsSelector {
  readonly #root: HTMLElement;
  readonly #doc: Document;
  readonly #options: ColumnsSelectorOptions;
  #state: SelectorState;
  #panes: { available: Pane; selected: Pane } | null = null;
  #addButton: HTMLButtonElement | null = null;
  #removeButton: HTMLButtonElement | null = null;
  /** Anchor for shift-click, per pane. */
  #anchor: { available: string | null; selected: string | null } = {
    available: null,
    selected: null,
  };

  constructor(
    root: HTMLElement,
    state: SelectorState,
    options: ColumnsSelectorOptions,
  ) {
    this.#root = root;
    this.#doc = root.ownerDocument;
    this.#state = state;
    this.#options = options;
    this.#build();
    this.render();
  }

  get selected(): readonly string[] {
    return this.#state.selected;
  }

  setState(state: SelectorState): void {
    this.#state = state;
    this.render();
  }

  #emit(selected: readonly string[]): void {
    this.#state = { ...this.#state, selected };
    this.#options.onChange(selected);
    this.render();
  }

  // ---- construction ----

  #build(): void {
    this.#root.classList.add('dc-selector');
    this.#root.replaceChildren();

    const available = this.#buildPane(
      'available',
      this.#options.availableLabel ?? 'Available columns:',
    );
    const middle = this.#buildMiddle();
    const selected = this.#buildPane(
      'selected',
      this.#options.selectedLabel ?? 'Selected columns:',
    );
    this.#panes = { available, selected };
    this.#root.append(available.root, middle, selected.root);
  }

  #buildPane(which: Pane['which'], label: string): Pane {
    const doc = this.#doc;
    const root = doc.createElement('div');
    root.className = `dc-selector-pane dc-pane-${which}`;

    const caption = doc.createElement('div');
    caption.className = 'dc-selector-caption';
    caption.textContent = label;
    root.append(caption);

    const box = doc.createElement('div');
    box.className = 'dc-selector-box';

    const searchRow = doc.createElement('div');
    searchRow.className = 'dc-selector-search';
    const icon = doc.createElement('span');
    icon.className = 'dc-selector-search-icon';
    icon.setAttribute('aria-hidden', 'true');
    icon.textContent = '⌕';
    const search = doc.createElement('input');
    search.type = 'text';
    search.className = 'dc-selector-search-input';
    search.placeholder = 'Search columns...';
    search.setAttribute('aria-label', `Search ${which} columns`);
    const clear = doc.createElement('button');
    clear.type = 'button';
    clear.className = 'dc-selector-search-clear';
    clear.textContent = '×';
    clear.title = 'Clear search [Esc]';
    clear.addEventListener('click', () => {
      search.value = '';
      search.focus();
      this.render();
    });
    search.addEventListener('input', () => this.render());
    search.addEventListener('keydown', (event) => {
      if (event.key === 'Escape') {
        // Stop here: the editor dialog also listens for Escape, and
        // clearing a search should not also close the editor.
        event.stopPropagation();
        search.value = '';
        this.render();
      }
    });
    searchRow.append(icon, search, clear);
    box.append(searchRow);

    const header = doc.createElement('button');
    header.type = 'button';
    header.className = 'dc-selector-header';
    header.title =
      which === 'available'
        ? 'Double-click to add all columns'
        : 'Double-click to remove all columns';
    const headerLabel = doc.createElement('span');
    headerLabel.textContent = '[All Columns]';
    const count = doc.createElement('span');
    count.className = 'dc-selector-count';
    header.append(headerLabel, count);
    header.addEventListener('dblclick', () => this.#moveAll(which));
    box.append(header);

    const list = doc.createElement('div');
    list.className = 'dc-selector-list';
    list.setAttribute('role', 'listbox');
    list.setAttribute('aria-multiselectable', 'true');
    list.setAttribute('aria-label', `${label} list`);
    this.#wireDropZone(list, which);
    box.append(list);

    root.append(box);
    return { which, root, list, search, count, picked: new Set() };
  }

  #buildMiddle(): HTMLElement {
    const doc = this.#doc;
    const middle = doc.createElement('div');
    middle.className = 'dc-selector-middle';

    const add = doc.createElement('button');
    add.type = 'button';
    add.className = 'dc-selector-move';
    add.textContent = '›';
    add.title = 'Add selected column(s)';
    add.addEventListener('click', () => this.#movePicked('available'));

    const remove = doc.createElement('button');
    remove.type = 'button';
    remove.className = 'dc-selector-move';
    remove.textContent = '‹';
    remove.title = 'Remove selected column(s)';
    remove.addEventListener('click', () => this.#movePicked('selected'));

    this.#addButton = add;
    this.#removeButton = remove;
    middle.append(add, remove);
    return middle;
  }

  // ---- moves ----

  /** Names visible in a pane right now, after its search. */
  #visible(which: Pane['which']): string[] {
    const pane = this.#panes?.[which];
    const search = pane?.search.value ?? '';
    const source =
      which === 'available' ? availableOf(this.#state) : this.#state.selected;
    return filterColumns(source, search);
  }

  #moveAll(which: Pane['which']): void {
    const names = this.#visible(which);
    if (names.length === 0) return;
    this.#move(which, names);
  }

  #movePicked(which: Pane['which']): void {
    const pane = this.#panes?.[which];
    if (!pane) return;
    // Scoped by the search: a hidden row is not moved even if it is
    // still highlighted from before the search was typed.
    const visible = new Set(this.#visible(which));
    const names = [...pane.picked].filter((n) => visible.has(n));
    if (names.length === 0) return;
    pane.picked.clear();
    this.#move(which, names);
  }

  #move(from: Pane['which'], names: readonly string[]): void {
    if (from === 'available') {
      this.#emit([...this.#state.selected, ...names]);
    } else {
      this.#emit(without(this.#state.selected, names));
    }
  }

  // ---- drag and drop ----

  #wireDropZone(list: HTMLElement, which: Pane['which']): void {
    list.addEventListener('dragover', (event) => {
      const drag = currentDrag();
      if (!drag) return;
      // Dropping back into the pane it came from is a reorder, which
      // only the selected pane allows; the available pane's order is
      // the source's and is not the user's to change.
      if (which === 'available' && drag.from === 'available') return;
      event.preventDefault();
      if (event.dataTransfer) event.dataTransfer.dropEffect = 'move';
      list.classList.add('dc-drop-target');
    });
    list.addEventListener('dragleave', () =>
      list.classList.remove('dc-drop-target'),
    );
    list.addEventListener('drop', (event) => {
      list.classList.remove('dc-drop-target');
      const drag = currentDrag();
      if (!drag) return;
      event.preventDefault();
      this.#drop(which, drag, this.#indexAt(list, event));
    });
  }

  #indexAt(list: HTMLElement, event: MouseEvent): number {
    const boxes = [...list.children].map((row) =>
      (row as HTMLElement).getBoundingClientRect(),
    );
    return indexForY(boxes, event.clientY);
  }

  #drop(into: Pane['which'], drag: DragPayload, index: number): void {
    setDrag(null);
    if (into === 'selected') {
      this.#emit(insertAt(this.#state.selected, drag.names, index));
    } else if (drag.from === 'selected') {
      this.#emit(without(this.#state.selected, drag.names));
    }
  }

  // ---- rendering ----

  render(): void {
    if (!this.#panes) return;
    this.#renderPane(this.#panes.available, availableOf(this.#state));
    this.#renderPane(this.#panes.selected, this.#state.selected);
    if (this.#addButton) {
      this.#addButton.disabled = this.#panes.available.picked.size === 0;
    }
    if (this.#removeButton) {
      this.#removeButton.disabled = this.#panes.selected.picked.size === 0;
    }
  }

  #renderPane(pane: Pane, source: readonly string[]): void {
    const names = filterColumns(source, pane.search.value);
    // A search that hides everything must not look like an empty
    // list, so the count is always shown against the total.
    pane.count.textContent =
      names.length === source.length
        ? `${source.length}`
        : `${names.length} of ${source.length}`;

    // Drop rows whose column vanished, so a stale highlight cannot
    // resurrect a column by being counted in a later move.
    const live = new Set(source);
    for (const n of [...pane.picked]) if (!live.has(n)) pane.picked.delete(n);

    const rows = names.map((name, i) => this.#row(pane, name, i, names));
    pane.list.replaceChildren(...rows);
  }

  #row(
    pane: Pane,
    name: string,
    index: number,
    visible: readonly string[],
  ): HTMLElement {
    const doc = this.#doc;
    const row = doc.createElement('div');
    row.className = 'dc-selector-row';
    row.setAttribute('role', 'option');
    row.dataset['column'] = name;
    row.draggable = true;
    const picked = pane.picked.has(name);
    row.setAttribute('aria-selected', String(picked));
    row.classList.toggle('dc-picked', picked);
    row.tabIndex = index === 0 ? 0 : -1;

    const label = doc.createElement('span');
    label.className = 'dc-selector-row-label';
    label.textContent = this.#options.labelFor?.(name) ?? name;
    row.append(label);

    const hint = this.#options.hintFor?.(name);
    if (hint) {
      const h = doc.createElement('span');
      h.className = 'dc-selector-row-hint';
      h.textContent = hint;
      row.append(h);
    }

    if (pane.which === 'selected') {
      const action = this.#options.actionFor?.(name);
      if (action) {
        // The action is a control in its own right; a click on it
        // must not also select or move the row beneath it.
        action.addEventListener('click', (e) => e.stopPropagation());
        action.addEventListener('dblclick', (e) => e.stopPropagation());
        row.append(action);
      }
    }

    row.title = `[${name}]\nDouble-click to ${
      pane.which === 'available' ? 'add' : 'remove'
    } column`;

    row.addEventListener('click', (event) =>
      this.#pick(pane, name, visible, event),
    );
    row.addEventListener('dblclick', () => {
      pane.picked.clear();
      this.#move(pane.which, [name]);
    });
    row.addEventListener('keydown', (event) => {
      if (event.key === 'Enter' || event.key === ' ') {
        event.preventDefault();
        this.#move(pane.which, [name]);
      }
    });
    row.addEventListener('dragstart', (event) => {
      // Dragging an unhighlighted row drags that row alone, which is
      // what a user means; dragging a highlighted one takes the whole
      // highlight with it.
      const names = pane.picked.has(name) ? [...pane.picked] : [name];
      setDrag({ from: pane.which, names });
      if (event.dataTransfer) {
        event.dataTransfer.effectAllowed = 'move';
        event.dataTransfer.setData('text/plain', names.join('\n'));
      }
    });
    row.addEventListener('dragend', () => setDrag(null));
    return row;
  }

  #pick(
    pane: Pane,
    name: string,
    visible: readonly string[],
    event: MouseEvent,
  ): void {
    const anchor = this.#anchor[pane.which];
    if (event.shiftKey && anchor !== null) {
      const from = visible.indexOf(anchor);
      const to = visible.indexOf(name);
      if (from >= 0 && to >= 0) {
        const [lo, hi] = from <= to ? [from, to] : [to, from];
        pane.picked = new Set(visible.slice(lo, hi + 1));
      }
    } else if (event.ctrlKey || event.metaKey) {
      if (pane.picked.has(name)) pane.picked.delete(name);
      else pane.picked.add(name);
      this.#anchor[pane.which] = name;
    } else {
      pane.picked = new Set([name]);
      this.#anchor[pane.which] = name;
    }
    this.render();
  }
}
