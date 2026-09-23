// The columns tool panel: the drag SOURCE for the pivot zones.
//
// This exists because the browser run exposed a hole in the obvious
// design. "Drag a column header into the row-group zone" assumes the
// column you want has a header on screen, and in a pivoted cube it
// does not: the row dimensions collapse into ONE tree column with a
// deliberately blank header, and every other header is a pivot value
// or a measure -- exactly the columns that must not be grouped by.
// So header-dragging alone can only ever move columns that are
// already grouped, which is the opposite of useful.
//
// DataCube has the same problem and the same answer: a Columns tool
// panel in a right-hand sidebar, with `allowDragFromColumnsToolPanel:
// true`. Every column the SOURCE has is listed, whether or not it is
// on screen, and that list is what you drag from.
//
// Measures are listed but not draggable, for the reason the zones
// already give: grouping by a notional yields one group per distinct
// amount. They are shown rather than hidden so the panel reads as
// the model's column list rather than as a mystery subset.

import {
  currentHeaderDrag,
  makeHeaderDraggable,
  setHeaderDrag,
} from './pivot-panel.ts';

export interface ColumnsPanelColumn {
  readonly name: string;
  readonly type: string;
  readonly groupable: boolean;
  /** Where it is used now, for the badge. */
  readonly usedAs?: 'rows' | 'columns';
  /** Whether the grid is showing it. Absent counts as shown. */
  readonly visible?: boolean;
  /**
   * The columns a pivot made of this one.
   *
   * A pivoted measure is not one column in the grid, it is one per
   * value of the pivot key -- `2021__|__notional` through
   * `2025__|__notional` -- and the panel said "notional" once. So it
   * lists them, under the measure they came from, which is how
   * ag-grid's own tool panel shows a pivot (upstream generates a
   * column group per value and the panel nests the leaves inside
   * it).
   */
  readonly children?: readonly ColumnsPanelChild[];
}

/** One column a pivot produced, under the measure it came from. */
export interface ColumnsPanelChild {
  /** The generated name, e.g. `2021__|__notional`. */
  readonly name: string;
  /** What to call it here: the pivot values, e.g. `2021`. */
  readonly label: string;
  readonly visible: boolean;
}

export interface ColumnsPanelOptions {
  readonly labelFor?: (column: string) => string;
  /** Clicking a groupable column adds it to the row groups. */
  readonly onPick?: (column: string) => void;
  /**
   * Show or hide a column in the grid.
   *
   * The tick box is what upstream's columns tool panel is mostly
   * FOR (`agColumnsToolPanel`, with values and pivot mode
   * suppressed): a list of every column with a checkbox each. Ours
   * listed them and could not turn one off, so the panel was a
   * reference card next to a grid that hid columns from a menu
   * three levels down.
   */
  readonly onVisibility?: (column: string, visible: boolean) => void;
  /**
   * A new order for the columns this panel lists.
   *
   * Reordering by dragging a HEADER works on the columns that are on
   * screen; this works on the list, which is where a person goes
   * looking for a column in the first place -- and is the only way
   * to order one the grid is not showing.
   */
  readonly onReorder?: (order: readonly string[]) => void;
  /**
   * Take a column off an axis, by dragging it back to the list.
   *
   * The counterpart to dropping one INTO a zone: the two zones and
   * this list are three sections of one surface, and a drag between
   * them is how the cube is configured.
   */
  readonly onRemoveFromZone?: (zone: 'rows' | 'columns', column: string) => void;
  /** Start collapsed. The grid is the point; the panel is a tool. */
  readonly collapsed?: boolean;
}

const BADGES: Readonly<Record<'rows' | 'columns', string>> = {
  rows: 'Row',
  columns: 'Col',
};

/**
 * How many pivot columns a measure may list before arriving folded.
 *
 * One pivot key over five years is a list worth reading; two keys
 * over five years and four quarters is twenty rows of `Q3 \u203a
 * 2024` between one measure and the next.
 */
const FOLD_CHILDREN_OVER = 8;

export class ColumnsToolPanel {
  readonly #root: HTMLElement;
  readonly #doc: Document;
  /**
   * Where the row/column zones live, as a LIST rather than a bar.
   *
   * Created once and re-appended on every render: the panel replaces
   * its children, and whatever is mounted here owns its own. That
   * keeps the zones' drag handling in one place -- `PivotPanel` --
   * instead of a second copy of "what a drop means".
   */
  readonly zones: HTMLElement;
  readonly #options: ColumnsPanelOptions;
  #columns: readonly ColumnsPanelColumn[] = [];
  /**
   * Measures whose pivot columns are folded away.
   *
   * A pivot on one key makes five of a measure; a pivot on two makes
   * twenty, and the list became a wall of `Q3 \u203a 2024`. So a
   * block past `FOLD_CHILDREN_OVER` arrives folded -- by its CURRENT
   * size, not by whether the measure has been seen before: pivoting
   * on a second key takes five columns to twenty, and a measure
   * remembered from when it was small stayed unfolded at twenty.
   *
   * A person's own toggle wins over that and is remembered.
   */
  readonly #folded = new Set<string>();
  readonly #chosen = new Set<string>();
  #search = '';
  #collapsed: boolean;

  constructor(root: HTMLElement, options: ColumnsPanelOptions = {}) {
    this.#root = root;
    this.#doc = root.ownerDocument;
    this.#options = options;
    this.#collapsed = options.collapsed ?? false;
    this.zones = this.#doc.createElement('div');
    this.zones.className = 'dc-tool-panel-zones';
    root.classList.add('dc-tool-panel');
    this.render();
  }

  setColumns(columns: readonly ColumnsPanelColumn[]): void {
    this.#columns = columns;
    for (const column of columns) {
      const children = column.children ?? [];
      if (children.length === 0 || this.#chosen.has(column.name)) continue;
      if (children.length > FOLD_CHILDREN_OVER) this.#folded.add(column.name);
      else this.#folded.delete(column.name);
    }
    this.render();
  }

  /** Whether the panel is showing its list. */
  get collapsed(): boolean {
    return this.#collapsed;
  }

  /**
   * Fold the panel away, or bring it back.
   *
   * Collapsed it becomes a RAIL, not nothing. A panel that vanishes
   * without leaving something to click is a panel the user has lost,
   * and the only way back would be a reload -- so the rail carries
   * the same button, and the 200px it gives up goes to the grid.
   * This is the shape ag-grid's side bar has, where clicking the
   * active tab collapses to the tab strip.
   */
  setCollapsed(collapsed: boolean): void {
    if (collapsed === this.#collapsed) return;
    this.#collapsed = collapsed;
    this.render();
  }

  render(): void {
    const doc = this.#doc;
    this.#root.classList.toggle('dc-collapsed', this.#collapsed);

    if (this.#collapsed) {
      // The rail, and nothing else. The search box and the list are
      // not merely hidden: a `display: none` subtree still holds
      // focusable controls that tab order walks through, so a
      // collapsed panel would still be reachable by keyboard while
      // being invisible.
      this.#root.replaceChildren(this.#toggle());
      return;
    }

    const head = doc.createElement('div');
    head.className = 'dc-tool-panel-head';
    const title = doc.createElement('span');
    title.textContent = 'Columns';
    head.append(title, this.#toggle());

    const search = doc.createElement('input');
    search.type = 'text';
    search.className = 'dc-tool-panel-search';
    search.placeholder = 'Search columns...';
    search.value = this.#search;
    search.setAttribute('aria-label', 'Search columns');
    search.addEventListener('input', () => {
      this.#search = search.value;
      this.render();
      // Re-render replaces the box, so put the caret back where the
      // user left it rather than at the start of what they typed.
      const next = this.#root.querySelector<HTMLInputElement>(
        '.dc-tool-panel-search',
      );
      next?.focus();
      next?.setSelectionRange(next.value.length, next.value.length);
    });

    const list = doc.createElement('div');
    list.className = 'dc-tool-panel-list';
    list.setAttribute('role', 'list');
    list.setAttribute('aria-label', 'Grid Columns');

    // DROPPED BACK OUT OF A ZONE: a chip dragged from Row Groups or
    // Column Labels onto this list leaves that axis. The two zones
    // and this list are three sections of one surface.
    list.addEventListener('dragover', (event) => {
      const drag = currentHeaderDrag();
      if (!drag || (drag.from !== 'rows' && drag.from !== 'columns')) return;
      event.preventDefault();
      if (event.dataTransfer) event.dataTransfer.dropEffect = 'move';
      list.classList.add('dc-drop-target');
    });
    list.addEventListener('dragleave', () =>
      list.classList.remove('dc-drop-target'));
    list.addEventListener('drop', (event) => {
      list.classList.remove('dc-drop-target');
      const drag = currentHeaderDrag();
      if (!drag || (drag.from !== 'rows' && drag.from !== 'columns')) return;
      event.preventDefault();
      setHeaderDrag(null);
      this.#options.onRemoveFromZone?.(drag.from, drag.column);
    });

    const section = doc.createElement('div');
    section.className = 'dc-tool-panel-section';
    section.textContent = 'Grid Columns';

    const q = this.#search.trim().toLowerCase();
    for (const column of this.#columns) {
      if (q !== '' && !column.name.toLowerCase().includes(q)) continue;
      list.append(this.#row(column));
      // The pivot's own columns, under the measure they came from.
      if (this.#folded.has(column.name)) continue;
      for (const child of column.children ?? []) {
        list.append(this.#child(column, child));
      }
    }

    this.#root.replaceChildren(head, search, this.zones, section, list);
  }

  /**
   * The one control that folds the panel and unfolds it.
   *
   * The SAME button in both states, so there is nothing to find:
   * where it sits changes, what it does does not. Collapsed it is
   * the whole panel and carries the word "Columns" down its side, so
   * the rail says what it opens rather than being an anonymous
   * sliver.
   */
  #toggle(): HTMLElement {
    const button = this.#doc.createElement('button');
    button.type = 'button';
    button.className = 'dc-tool-panel-toggle';
    button.setAttribute('aria-expanded', String(!this.#collapsed));
    if (this.#collapsed) {
      button.classList.add('dc-tool-panel-rail');
      button.textContent = 'Columns';
      button.title = 'Show the columns panel';
      button.setAttribute('aria-label', 'Show the columns panel');
    } else {
      // A chevron pointing the way the panel will go, which is the
      // convention every collapsible sidebar uses.
      button.textContent = '\u203a';
      button.title = 'Hide the columns panel';
      button.setAttribute('aria-label', 'Hide the columns panel');
    }
    button.addEventListener('click', () => {
      this.setCollapsed(!this.#collapsed);
      // Keep the focus on the control the user just pressed: it is
      // replaced by the re-render, so without this the focus falls
      // back to the document and a keyboard user loses their place.
      this.#root.querySelector<HTMLElement>('.dc-tool-panel-toggle')?.focus();
    });
    return button;
  }

  #row(column: ColumnsPanelColumn): HTMLElement {
    const doc = this.#doc;
    const row = doc.createElement('div');
    row.className = 'dc-tool-panel-row';
    row.setAttribute('role', 'listitem');
    row.dataset['column'] = column.name;
    row.classList.toggle('dc-measure', !column.groupable);

    const children = column.children ?? [];
    if (children.length === 0) {
      // THE DISCLOSURE'S SLOT, KEPT EMPTY. Without it a measure the
      // pivot has spread sits 12px to the right of every other row,
      // and the three sections stop reading as one list.
      const spacer = doc.createElement('span');
      spacer.className = 'dc-tool-panel-twist dc-tool-panel-spacer';
      spacer.setAttribute('aria-hidden', 'true');
      row.append(spacer);
    }
    if (children.length > 0) {
      // The disclosure, first, where every tree puts it.
      const twist = doc.createElement('button');
      twist.type = 'button';
      twist.className = 'dc-tool-panel-twist';
      const folded = this.#folded.has(column.name);
      twist.textContent = folded ? '\u203a' : '\u2304';
      twist.title = folded
        ? `Show the ${children.length} columns of ${column.name}`
        : `Fold the ${children.length} columns of ${column.name}`;
      twist.setAttribute('aria-label', twist.title);
      twist.setAttribute('aria-expanded', String(!folded));
      twist.draggable = false;
      twist.addEventListener('pointerdown', (e: Event) => e.stopPropagation());
      twist.addEventListener('click', (e: Event) => {
        e.stopPropagation();
        this.#chosen.add(column.name);
        if (folded) this.#folded.delete(column.name);
        else this.#folded.add(column.name);
        this.render();
      });
      row.append(twist);
    }
    // A pivoted measure is shown by ITS PARTS: the parent is ticked
    // when any of them is, and toggling it moves all of them, which
    // is the only thing "hide notional" can mean once the pivot has
    // made five of it.
    const visible = children.length > 0
      ? children.some((c) => c.visible)
      : column.visible !== false;
    row.classList.toggle('dc-hidden-column', !visible);
    if (this.#options.onVisibility) {
      const box = doc.createElement('input');
      box.type = 'checkbox';
      box.className = 'dc-tool-panel-show';
      box.checked = visible;
      box.title = visible
        ? `Hide ${column.name} from the grid`
        : `Show ${column.name} in the grid`;
      box.setAttribute('aria-label', box.title);
      // The row is draggable, and a press on the box must tick it
      // rather than start a drag of the row underneath.
      box.draggable = false;
      box.addEventListener('pointerdown', (e) => e.stopPropagation());
      box.addEventListener('click', (e) => e.stopPropagation());
      box.addEventListener('change', () => {
        if (children.length > 0) {
          for (const child of children) {
            this.#options.onVisibility?.(child.name, box.checked);
          }
          return;
        }
        this.#options.onVisibility?.(column.name, box.checked);
      });
      row.append(box);
    }

    const label = doc.createElement('span');
    label.className = 'dc-tool-panel-label';
    label.textContent = this.#options.labelFor?.(column.name) ?? column.name;
    row.append(label);

    if (column.usedAs) {
      const badge = doc.createElement('span');
      badge.className = 'dc-tool-panel-badge';
      badge.textContent = BADGES[column.usedAs];
      row.append(badge);
    }

    const type = doc.createElement('span');
    type.className = 'dc-tool-panel-type';
    type.textContent = column.type;
    row.append(type);

    makeHeaderDraggable(row, column.name, column.groupable, 'panel');
    if (column.groupable && this.#options.onPick) {
      // Double-click is the keyboard-and-trackpad path to the same
      // thing: a panel that can only be operated by dragging is a
      // panel some people cannot operate.
      row.title = `[${column.name}]\nDrag into a zone to group by it, or`
        + ` into the grid to place it. Double-click to group.`;
      row.tabIndex = 0;
      row.addEventListener('dblclick', () =>
        this.#options.onPick?.(column.name),
      );
      row.addEventListener('keydown', (event) => {
        if (event.key === 'Enter') {
          event.preventDefault();
          this.#options.onPick?.(column.name);
        }
      });
    } else {
      row.title = `[${column.name}]\nDrag into the grid to place it.`
        + ` Measures cannot be grouped by.`;
    }
    this.#reorderable(row, column.name);
    return row;
  }

  /**
   * Let a row be dropped between two others.
   *
   * REORDERING WHERE THE LIST IS. Dragging a header does it for the
   * columns that are on screen; this does it for every column the
   * cube has, which is the only way to place one the grid is not
   * showing -- and the list is where a person looks for a column
   * anyway.
   *
   * Above or below the halfway line, like the grid's own header
   * drop: without the two halves a column can never be placed last.
   */
  #reorderable(row: HTMLElement, column: string): void {
    if (!this.#options.onReorder) return;
    const side = (event: MouseEvent): 'before' | 'after' => {
      const box = row.getBoundingClientRect();
      return event.clientY > box.top + box.height / 2 ? 'after' : 'before';
    };
    const held = (): string | null => {
      const drag = currentHeaderDrag();
      // From this list only: a chip dragged out of a zone is a
      // REMOVAL, which the list itself handles, and a grid header
      // drag is already the grid's own reorder.
      if (!drag || drag.from !== 'panel') return null;
      return drag.column === column ? null : drag.column;
    };
    row.addEventListener('dragover', (event) => {
      if (!held()) return;
      event.preventDefault();
      if (event.dataTransfer) event.dataTransfer.dropEffect = 'move';
      this.#clearDropMarks();
      row.classList.add(`dc-drop-${side(event)}`);
    });
    row.addEventListener('dragleave', () =>
      row.classList.remove('dc-drop-before', 'dc-drop-after'));
    row.addEventListener('drop', (event) => {
      const moved = held();
      this.#clearDropMarks();
      if (!moved) return;
      // Stop the list's own handler: this is a reorder, not a
      // removal, and both are listening.
      event.stopPropagation();
      event.preventDefault();
      const listed = [...this.#root.querySelectorAll('.dc-tool-panel-row')]
        .map((r) => (r as HTMLElement).dataset['column'] ?? '')
        .filter((n) => n !== '');
      const next = listed.filter((n) => n !== moved);
      const at = next.indexOf(column) + (side(event) === 'after' ? 1 : 0);
      next.splice(at, 0, moved);
      setHeaderDrag(null);
      this.#options.onReorder?.(next);
    });
  }

  #clearDropMarks(): void {
    for (const e of this.#root.querySelectorAll(
      '.dc-drop-before, .dc-drop-after')) {
      e.classList.remove('dc-drop-before', 'dc-drop-after');
    }
  }

  /**
   * One column a pivot produced.
   *
   * Indented under its measure and labelled by the pivot values
   * alone -- under "notional", the rows read 2021, 2022, 2023 --
   * because the measure is already named by the row above and
   * `2021__|__notional` says it twice.
   */
  #child(parent: ColumnsPanelColumn, child: ColumnsPanelChild): HTMLElement {
    const doc = this.#doc;
    const row = doc.createElement('div');
    row.className = 'dc-tool-panel-row dc-tool-panel-child';
    row.setAttribute('role', 'listitem');
    row.dataset['column'] = child.name;
    row.classList.toggle('dc-hidden-column', !child.visible);

    if (this.#options.onVisibility) {
      const box = doc.createElement('input');
      box.type = 'checkbox';
      box.className = 'dc-tool-panel-show';
      box.checked = child.visible;
      box.title = child.visible
        ? `Hide ${child.label} ${parent.name} from the grid`
        : `Show ${child.label} ${parent.name} in the grid`;
      box.setAttribute('aria-label', box.title);
      box.draggable = false;
      box.addEventListener('pointerdown', (e) => e.stopPropagation());
      box.addEventListener('click', (e) => e.stopPropagation());
      box.addEventListener('change', () => {
        this.#options.onVisibility?.(child.name, box.checked);
      });
      row.append(box);
    }

    const label = doc.createElement('span');
    label.className = 'dc-tool-panel-label';
    label.textContent = child.label;
    row.append(label);

    const type = doc.createElement('span');
    type.className = 'dc-tool-panel-type';
    type.textContent = parent.type;
    row.append(type);

    // Draggable like any other column: a pivoted leaf reorders its
    // MEASURE, in every value block at once, which is the only
    // outcome the configuration can express.
    makeHeaderDraggable(row, child.name, false, 'panel');
    row.title = `[${child.name}]\nDrag into the grid to place`
      + ` ${parent.name}.`;
    return row;
  }
}
