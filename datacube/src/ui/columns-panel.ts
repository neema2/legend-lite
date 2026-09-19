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

import { makeHeaderDraggable } from './pivot-panel.ts';

export interface ColumnsPanelColumn {
  readonly name: string;
  readonly type: string;
  readonly groupable: boolean;
  /** Where it is used now, for the badge. */
  readonly usedAs?: 'rows' | 'columns';
}

export interface ColumnsPanelOptions {
  readonly labelFor?: (column: string) => string;
  /** Clicking a groupable column adds it to the row groups. */
  readonly onPick?: (column: string) => void;
}

const BADGES: Readonly<Record<'rows' | 'columns', string>> = {
  rows: 'Row',
  columns: 'Col',
};

export class ColumnsToolPanel {
  readonly #root: HTMLElement;
  readonly #doc: Document;
  readonly #options: ColumnsPanelOptions;
  #columns: readonly ColumnsPanelColumn[] = [];
  #search = '';

  constructor(root: HTMLElement, options: ColumnsPanelOptions = {}) {
    this.#root = root;
    this.#doc = root.ownerDocument;
    this.#options = options;
    root.classList.add('dc-tool-panel');
    this.render();
  }

  setColumns(columns: readonly ColumnsPanelColumn[]): void {
    this.#columns = columns;
    this.render();
  }

  render(): void {
    const doc = this.#doc;
    const head = doc.createElement('div');
    head.className = 'dc-tool-panel-head';
    head.textContent = 'Columns';

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

    const q = this.#search.trim().toLowerCase();
    for (const column of this.#columns) {
      if (q !== '' && !column.name.toLowerCase().includes(q)) continue;
      list.append(this.#row(column));
    }

    this.#root.replaceChildren(head, search, list);
  }

  #row(column: ColumnsPanelColumn): HTMLElement {
    const doc = this.#doc;
    const row = doc.createElement('div');
    row.className = 'dc-tool-panel-row';
    row.setAttribute('role', 'listitem');
    row.dataset['column'] = column.name;
    row.classList.toggle('dc-measure', !column.groupable);

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

    makeHeaderDraggable(row, column.name, column.groupable);
    if (column.groupable && this.#options.onPick) {
      // Double-click is the keyboard-and-trackpad path to the same
      // thing: a panel that can only be operated by dragging is a
      // panel some people cannot operate.
      row.title = `[${column.name}]\nDrag into a zone, or double-click to group`;
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
      row.title = `[${column.name}]\nMeasures cannot be grouped by`;
    }
    return row;
  }
}
