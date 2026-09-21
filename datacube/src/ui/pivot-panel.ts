// The drag zones above the grid.
//
// ag-Grid calls this the row group panel and DataCube turns it on
// with `rowGroupPanelShow: 'always'`; it is the strip that reads
// "Drag here to set row groups" and fills with a chip per grouping
// column. Dragging a column header into it groups by that column.
//
// DataCube deliberately does NOT enable ag-Grid's matching PIVOT
// panel. Its own comment says why: pivot mode "comes with many
// restrictions/opinionated behaviors on column grouping... it
// disallows full control of column definitions, so we couldn't
// display dimension columns which are not part of pivot while
// pivoting", so it drives pivots from column settings instead and
// opts out of the GUI. That restriction is ag-Grid's, not the
// concept's -- this grid owns its own column model and has no such
// limit -- so both zones are here, and the column zone is the one
// place this goes beyond DataCube rather than matching it.
//
// Only DIMENSION columns are draggable, matching DataCube's
// `enableRowGroup: column.kind === DIMENSION`. Grouping by a notional
// yields one group per distinct amount: never meant, and expensive
// to find out.

export type Zone = 'rows' | 'columns';

/** What is being dragged, and where from. */
export interface HeaderDrag {
  readonly column: string;
  /**
   * Where the drag started.
   *
   * A zone, so a chip moved between zones leaves the first; or the
   * COLUMNS PANEL, which is not a zone and holds no membership to
   * leave -- and which may be carrying a column the grid is not
   * showing at all.
   */
  readonly from?: Zone | 'panel';
}

let dragging: HeaderDrag | null = null;

export function currentHeaderDrag(): HeaderDrag | null {
  return dragging;
}
export function setHeaderDrag(drag: HeaderDrag | null): void {
  dragging = drag;
}

export interface PivotPanelOptions {
  /** Called with the new membership of a zone. */
  readonly onChange: (zone: Zone, columns: readonly string[]) => void;
  /** Whether a column may be dropped at all. Measures may not. */
  readonly canGroup: (column: string) => boolean;
  /** Header text for a column, where it differs from its name. */
  readonly labelFor?: (column: string) => string;
  /** Show the column zone. Off matches DataCube exactly. */
  readonly showColumnZone?: boolean;
}

const PROMPTS: Readonly<Record<Zone, string>> = {
  rows: 'Drag here to set row groups',
  columns: 'Drag here to set column labels',
};

const TITLES: Readonly<Record<Zone, string>> = {
  rows: 'Row Groups',
  columns: 'Column Labels',
};

/**
 * Move a column within, into or out of a list.
 *
 * Returns the SAME array when nothing changed, so a drop that lands
 * where the chip already was does not re-run the query.
 */
export function placeColumn(
  list: readonly string[],
  column: string,
  index: number | null,
): readonly string[] {
  const at = list.indexOf(column);
  const without = list.filter((c) => c !== column);
  let next: readonly string[];
  if (index === null) {
    next = without;
  } else {
    // The index was measured against the list as DISPLAYED, so it
    // shifts back by one when the chip being moved sat above the gap.
    const target = at !== -1 && at < index ? index - 1 : index;
    const to = Math.max(0, Math.min(without.length, target));
    next = [...without.slice(0, to), column, ...without.slice(to)];
  }
  return same(list, next) ? list : next;
}

/** Identity, so a drop that lands where the chip was costs no query. */
function same(a: readonly string[], b: readonly string[]): boolean {
  return a.length === b.length && a.every((x, i) => x === b[i]);
}

export class PivotPanel {
  readonly #root: HTMLElement;
  readonly #doc: Document;
  readonly #options: PivotPanelOptions;
  #state: Readonly<Record<Zone, readonly string[]>> = {
    rows: [],
    columns: [],
  };

  constructor(root: HTMLElement, options: PivotPanelOptions) {
    this.#root = root;
    this.#doc = root.ownerDocument;
    this.#options = options;
    root.classList.add('dc-pivot-panel');
    this.render();
  }

  setColumns(rows: readonly string[], columns: readonly string[]): void {
    this.#state = { rows, columns };
    this.render();
  }

  render(): void {
    const zones: Zone[] = this.#options.showColumnZone
      ? ['rows', 'columns']
      : ['rows'];
    this.#root.replaceChildren(...zones.map((z) => this.#zone(z)));
  }

  #zone(zone: Zone): HTMLElement {
    const doc = this.#doc;
    const el = doc.createElement('div');
    el.className = `dc-zone dc-zone-${zone}`;
    el.dataset['zone'] = zone;
    el.setAttribute('role', 'group');
    el.setAttribute('aria-label', TITLES[zone]);

    const title = doc.createElement('span');
    title.className = 'dc-zone-title';
    title.textContent = TITLES[zone];
    el.append(title);

    const columns = this.#state[zone];
    if (columns.length === 0) {
      const prompt = doc.createElement('span');
      prompt.className = 'dc-zone-prompt';
      prompt.textContent = PROMPTS[zone];
      el.append(prompt);
    } else {
      columns.forEach((column, i) => {
        if (i > 0) {
          const arrow = doc.createElement('span');
          arrow.className = 'dc-zone-arrow';
          arrow.setAttribute('aria-hidden', 'true');
          // The arrow is not decoration: it says the order is a
          // hierarchy, outermost first, rather than a set.
          arrow.textContent = '›';
          el.append(arrow);
        }
        el.append(this.#chip(zone, column));
      });
    }

    el.addEventListener('dragover', (event) => {
      const drag = currentHeaderDrag();
      if (!drag || !this.#options.canGroup(drag.column)) return;
      event.preventDefault();
      if (event.dataTransfer) event.dataTransfer.dropEffect = 'move';
      el.classList.add('dc-drop-target');
    });
    el.addEventListener('dragleave', () => el.classList.remove('dc-drop-target'));
    el.addEventListener('drop', (event) => {
      el.classList.remove('dc-drop-target');
      const drag = currentHeaderDrag();
      if (!drag) return;
      event.preventDefault();
      this.#drop(zone, drag, this.#indexAt(el, event));
    });
    return el;
  }

  #chip(zone: Zone, column: string): HTMLElement {
    const doc = this.#doc;
    const chip = doc.createElement('span');
    chip.className = 'dc-chip';
    chip.dataset['column'] = column;
    chip.draggable = true;

    const label = doc.createElement('span');
    label.className = 'dc-chip-label';
    label.textContent = this.#options.labelFor?.(column) ?? column;
    chip.append(label);

    const remove = doc.createElement('button');
    remove.type = 'button';
    remove.className = 'dc-chip-remove';
    remove.textContent = '×';
    remove.title = `Remove ${column}`;
    remove.setAttribute('aria-label', `Remove ${column}`);
    remove.addEventListener('click', () => this.#set(zone, placeColumn(this.#state[zone], column, null)));
    chip.append(remove);

    chip.addEventListener('dragstart', (event) => {
      setHeaderDrag({ column, from: zone });
      if (event.dataTransfer) {
        event.dataTransfer.effectAllowed = 'move';
        event.dataTransfer.setData('text/plain', column);
      }
    });
    chip.addEventListener('dragend', () => setHeaderDrag(null));

    // Keyboard: a zone a mouse can fill must be emptiable without
    // one, so Delete on a focused chip removes it.
    chip.tabIndex = 0;
    chip.addEventListener('keydown', (event) => {
      if (event.key === 'Delete' || event.key === 'Backspace') {
        event.preventDefault();
        this.#set(zone, placeColumn(this.#state[zone], column, null));
      }
    });
    return chip;
  }

  #indexAt(zone: HTMLElement, event: MouseEvent): number {
    const chips = [...zone.querySelectorAll('.dc-chip')] as HTMLElement[];
    for (let i = 0; i < chips.length; i++) {
      const box = (chips[i] as HTMLElement).getBoundingClientRect();
      if (event.clientX < box.left + box.width / 2) return i;
    }
    return chips.length;
  }

  #drop(zone: Zone, drag: HeaderDrag, index: number): void {
    setHeaderDrag(null);
    if (!this.#options.canGroup(drag.column)) return;
    // Dragging a chip from one zone to the other has to LEAVE the
    // first, or the same column groups rows and labels columns at
    // once, which produces a cube with the dimension on both axes.
    // A panel drag has no zone to leave.
    if ((drag.from === 'rows' || drag.from === 'columns')
      && drag.from !== zone) {
      const other = drag.from;
      this.#state = {
        ...this.#state,
        [other]: placeColumn(this.#state[other], drag.column, null),
      };
      this.#options.onChange(other, this.#state[other]);
    }
    this.#set(zone, placeColumn(this.#state[zone], drag.column, index));
  }

  #set(zone: Zone, columns: readonly string[]): void {
    if (columns === this.#state[zone]) return;
    this.#state = { ...this.#state, [zone]: columns };
    this.#options.onChange(zone, columns);
    this.render();
  }
}

/**
 * Make a grid header cell draggable into the zones.
 *
 * Applied by the grid to each leaf header; a measure is left alone
 * rather than made draggable and then refused, because a drag that
 * can never land is worse than no drag handle at all.
 */
export function makeHeaderDraggable(
  el: HTMLElement,
  column: string,
  canGroup: boolean,
  from?: 'panel',
): void {
  // A PANEL ROW DRAGS WHETHER OR NOT IT CAN BE GROUPED BY. A measure
  // has nowhere to land in the zones -- they refuse it -- but it has
  // somewhere to land in the GRID, which is what upstream's
  // `allowDragFromColumnsToolPanel` is for. Only the zones care
  // about `canGroup`, and they check it themselves.
  if (!canGroup && from !== 'panel') return;
  el.draggable = true;
  el.classList.add('dc-draggable');
  el.addEventListener('dragstart', (event) => {
    setHeaderDrag(from ? { column, from } : { column });
    if (event.dataTransfer) {
      event.dataTransfer.effectAllowed = 'move';
      event.dataTransfer.setData('text/plain', column);
    }
  });
  el.addEventListener('dragend', () => setHeaderDrag(null));
}
