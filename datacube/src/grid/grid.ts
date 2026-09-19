// The grid: a windowed, keyboard-navigable, screen-reader-legible
// treegrid.
//
// Built on the W3C ARIA APG treegrid pattern rather than invented,
// because the research was unambiguous: no analytical grid in the field
// has complete screen-reader support for a pivot, and the reason is
// structural -- pivoting continuously redefines what a "row" is, which
// is exactly what breaks announcements. The APG pattern is a fully
// specified, standards-track model for hierarchical tabular data, and
// building on it is the cheapest route to the one differentiator
// nobody else has. Retrofitting it later is what fails.
//
// Roles and properties used here, all from that pattern:
//   role=treegrid on the container
//   role=row with aria-level, aria-expanded, aria-posinset, aria-setsize
//   role=columnheader / role=gridcell
//   aria-rowcount / aria-rowindex, so a WINDOWED grid still announces
//     "row 4,312 of 2,000,000" rather than "row 12 of 40"
//
// That last one is the bit virtualised grids habitually get wrong: the
// DOM holds only the visible band, so without explicit rowcount and
// rowindex a screen reader announces positions within the band and the
// user has no idea where they are.

import type { ColumnModel } from './columns.ts';
import { computeRowWindow, isCovered, type RowWindow } from './viewport.ts';
import type { ColumnFormat, FormatterCache } from '../format.ts';
import { DEFAULT_FORMAT } from '../format.ts';
import type { ResultTable, Scalar } from '../result.ts';

export interface GridRowMeta {
  /** Depth in the row-group tree; 1 for a top-level row. */
  readonly level: number;
  /** Present only for a group row that can expand. */
  readonly expanded?: boolean;
  /** Stable identity, used to key view state across refreshes. */
  readonly key: string;
  /** True for a subtotal or grand-total row. */
  readonly isTotal?: boolean;
}

export interface GridOptions {
  readonly rowHeight?: number;
  readonly overscan?: number;
  /** Per-column display format, by leaf column name. */
  readonly formats?: Readonly<Record<string, ColumnFormat>>;
  /** Metadata per absolute row index, for grouping and totals. */
  readonly rowMeta?: (absoluteRow: number) => GridRowMeta;
  readonly onToggleExpand?: (key: string, expanded: boolean) => void;
  readonly onActivateCell?: (row: number, column: number) => void;
}

const DEFAULT_ROW_HEIGHT = 24;

interface Focus {
  row: number;
  col: number;
}

/**
 * A grid bound to one container element.
 *
 * Deliberately not a framework component. The grid owns its DOM and
 * updates it directly, so there is no reconciliation layer between a
 * scroll event and the cells that must change -- the measured hazard
 * being that a fast engine gets squandered in the UI framework, which
 * is what happened to at least one comparable product.
 */
export class DataGrid {
  readonly #root: HTMLElement;
  readonly #head: HTMLElement;
  readonly #body: HTMLElement;
  readonly #spacer: HTMLElement;
  readonly #scroller: HTMLElement;
  readonly #formatters: FormatterCache;
  readonly #options: GridOptions;

  #model: ColumnModel | null = null;
  #table: ResultTable | null = null;
  /** Absolute index of the first row present in `#table`. */
  #blockOffset = 0;
  #totalRows = 0;
  #rendered: RowWindow | null = null;
  #focus: Focus = { row: 0, col: 0 };
  #frame = 0;

  constructor(
    container: HTMLElement,
    formatters: FormatterCache,
    options: GridOptions = {},
  ) {
    this.#root = container;
    this.#formatters = formatters;
    this.#options = options;

    const doc = container.ownerDocument;
    this.#root.classList.add('dc-grid');
    this.#root.setAttribute('role', 'treegrid');
    this.#root.tabIndex = 0;

    this.#head = doc.createElement('div');
    this.#head.className = 'dc-head';
    this.#head.setAttribute('role', 'rowgroup');

    this.#scroller = doc.createElement('div');
    this.#scroller.className = 'dc-scroller';

    this.#spacer = doc.createElement('div');
    this.#spacer.className = 'dc-spacer';
    // Presentational: the spacer only sizes the scrollbar and must not
    // appear to assistive technology as a row.
    this.#spacer.setAttribute('role', 'presentation');

    this.#body = doc.createElement('div');
    this.#body.className = 'dc-body';
    this.#body.setAttribute('role', 'rowgroup');

    this.#spacer.appendChild(this.#body);
    this.#scroller.appendChild(this.#spacer);
    this.#root.appendChild(this.#head);
    this.#root.appendChild(this.#scroller);

    this.#scroller.addEventListener('scroll', this.#onScroll);
    this.#root.addEventListener('keydown', this.#onKeyDown);
  }

  /** Replace the column model. Resets the header. */
  setColumns(model: ColumnModel): void {
    this.#model = model;
    this.#renderHeader();
    this.#rendered = null;
  }

  /**
   * Supply a block of rows.
   *
   * `totalRows` is the full result size, not the block's size, so the
   * scrollbar and the announced row count reflect the whole cube.
   */
  setRows(table: ResultTable, blockOffset: number, totalRows: number): void {
    this.#table = table;
    this.#blockOffset = blockOffset;
    this.#totalRows = totalRows;
    this.#root.setAttribute('aria-rowcount', String(totalRows));
    this.#rendered = null;
    this.#render();
  }

  /** Current window, for tests and for deciding what to fetch. */
  get window(): RowWindow {
    return computeRowWindow({
      scrollTop: this.#scroller.scrollTop,
      viewportHeight: this.#scroller.clientHeight,
      rowHeight: this.#options.rowHeight ?? DEFAULT_ROW_HEIGHT,
      totalRows: this.#totalRows,
      ...(this.#options.overscan !== undefined
        ? { overscan: this.#options.overscan }
        : {}),
    });
  }

  destroy(): void {
    this.#scroller.removeEventListener('scroll', this.#onScroll);
    this.#root.removeEventListener('keydown', this.#onKeyDown);
    if (this.#frame) cancelAnimationFrame(this.#frame);
    this.#root.replaceChildren();
  }

  // -- rendering ---------------------------------------------------

  #renderHeader(): void {
    const model = this.#model;
    if (!model) return;
    const doc = this.#root.ownerDocument;
    this.#head.replaceChildren();

    // One CSS grid for the whole header, with every cell placed
    // explicitly. Flexbox cannot express a cell spanning two rows, so
    // a ragged header laid out in document order puts the lower row's
    // cells under the dimension columns instead of under their values.
    this.#head.style.gridTemplateColumns = this.#templateColumns(model);
    this.#head.style.gridTemplateRows =
      `repeat(${model.depth}, var(--dc-row-height))`;

    model.headerRows.forEach((cells, level) => {
      // A row element per level keeps role=row correct for assistive
      // technology; `display: contents` lets its children take part in
      // the grid directly, so semantics and layout do not fight.
      const row = doc.createElement('div');
      row.setAttribute('role', 'row');
      row.className = 'dc-head-row';
      // Header rows are rows too, and numbering them from 1 keeps
      // aria-rowindex continuous with the body below.
      row.setAttribute('aria-rowindex', String(level + 1));

      for (const cell of cells) {
        const el = doc.createElement('div');
        el.setAttribute('role', 'columnheader');
        el.className = 'dc-th';
        el.textContent = cell.label;
        el.style.gridColumn = `${cell.colStart + 1} / span ${cell.colSpan}`;
        el.style.gridRow = `${level + 1} / span ${cell.rowSpan}`;
        if (cell.colSpan > 1) {
          el.setAttribute('aria-colspan', String(cell.colSpan));
        }
        if (cell.rowSpan > 1) {
          el.setAttribute('aria-rowspan', String(cell.rowSpan));
        }
        row.appendChild(el);
      }
      this.#head.appendChild(row);
    });
  }

  /**
   * Column widths, shared by the header grid and the body rows.
   *
   * Both must derive from the SAME leaf list, or the header drifts out
   * of line with the data -- which is what happened when only the
   * header's first cell was given dimension width while every
   * dimension cell in the body took it.
   */
  #templateColumns(model: ColumnModel): string {
    return model.leaves
      .map((l) =>
        l.isDimension ? 'var(--dc-dim-width)' : 'var(--dc-col-width)',
      )
      .join(' ');
  }

  #onScroll = (): void => {
    // Coalesce to one render per frame. Scroll fires far more often
    // than the window changes, and this is what bounds the work.
    if (this.#frame) return;
    this.#frame = requestAnimationFrame(() => {
      this.#frame = 0;
      this.#render();
    });
  };

  #render(): void {
    const model = this.#model;
    const table = this.#table;
    if (!model || !table) return;

    const wanted = this.window;
    if (this.#rendered && isCovered(this.#rendered, wanted)) return;

    const doc = this.#root.ownerDocument;
    const rowHeight = this.#options.rowHeight ?? DEFAULT_ROW_HEIGHT;
    this.#spacer.style.height = `${wanted.totalHeight}px`;
    this.#body.style.transform = `translateY(${wanted.offsetTop}px)`;

    const frag = doc.createDocumentFragment();
    const headerLevels = model.headerRows.length;
    const template = this.#templateColumns(model);

    for (let abs = wanted.start; abs < wanted.end; abs++) {
      const local = abs - this.#blockOffset;
      const row = doc.createElement('div');
      row.setAttribute('role', 'row');
      row.className = 'dc-row';
      row.style.height = `${rowHeight}px`;
      row.style.gridTemplateColumns = template;
      // Absolute position in the whole result, offset past the header
      // rows. Without this a virtualised grid announces the position
      // within the rendered band, which is meaningless to the user.
      row.setAttribute('aria-rowindex', String(headerLevels + abs + 1));

      const meta = this.#options.rowMeta?.(abs);
      if (meta) {
        row.setAttribute('aria-level', String(meta.level));
        if (meta.expanded !== undefined) {
          row.setAttribute('aria-expanded', String(meta.expanded));
        }
        row.dataset['key'] = meta.key;
        if (meta.isTotal) row.classList.add('dc-total');
      }

      const loaded = local >= 0 && local < table.rowCount;
      for (let c = 0; c < model.leaves.length; c++) {
        const leaf = model.leaves[c]!;
        const cell = doc.createElement('div');
        cell.setAttribute('role', 'gridcell');
        cell.className = leaf.isDimension ? 'dc-cell dc-dim' : 'dc-cell';
        cell.setAttribute('aria-colindex', String(c + 1));

        if (loaded) {
          const value: Scalar =
            table.columns[leaf.index]?.values[local] ?? null;
          cell.textContent = this.#formatters.format(
            value,
            this.#options.formats?.[leaf.name] ?? DEFAULT_FORMAT,
          );
        } else {
          // Not fetched yet. Rendered as a placeholder rather than
          // blocking the scroll, and marked busy so a screen reader
          // says so instead of reading an empty cell as an empty value.
          cell.classList.add('dc-pending');
          cell.setAttribute('aria-busy', 'true');
        }

        if (abs === this.#focus.row && c === this.#focus.col) {
          cell.tabIndex = 0;
          cell.classList.add('dc-focus');
        } else {
          cell.tabIndex = -1;
        }
        row.appendChild(cell);
      }
      frag.appendChild(row);
    }

    this.#body.replaceChildren(frag);
    this.#rendered = wanted;
  }

  // -- keyboard ----------------------------------------------------

  #onKeyDown = (event: KeyboardEvent): void => {
    const model = this.#model;
    if (!model) return;
    const lastCol = model.leaves.length - 1;
    const lastRow = Math.max(0, this.#totalRows - 1);
    let { row, col } = this.#focus;
    let handled = true;

    switch (event.key) {
      case 'ArrowDown':
        row = Math.min(lastRow, row + 1);
        break;
      case 'ArrowUp':
        row = Math.max(0, row - 1);
        break;
      case 'ArrowRight': {
        // APG treegrid: on a collapsed group row, Right expands rather
        // than moving. Only then does it move to the next column.
        const meta = this.#options.rowMeta?.(row);
        if (meta?.expanded === false) {
          this.#options.onToggleExpand?.(meta.key, true);
        } else {
          col = Math.min(lastCol, col + 1);
        }
        break;
      }
      case 'ArrowLeft': {
        const meta = this.#options.rowMeta?.(row);
        if (meta?.expanded === true && col === 0) {
          this.#options.onToggleExpand?.(meta.key, false);
        } else {
          col = Math.max(0, col - 1);
        }
        break;
      }
      case 'Home':
        if (event.ctrlKey) row = 0;
        col = 0;
        break;
      case 'End':
        if (event.ctrlKey) row = lastRow;
        col = lastCol;
        break;
      case 'PageDown':
        row = Math.min(lastRow, row + this.#pageSize());
        break;
      case 'PageUp':
        row = Math.max(0, row - this.#pageSize());
        break;
      case 'Enter':
      case ' ':
        this.#options.onActivateCell?.(row, col);
        break;
      default:
        handled = false;
    }

    if (!handled) return;
    event.preventDefault();
    this.#focus = { row, col };
    this.#scrollFocusIntoView();
    this.#rendered = null;
    this.#render();
    this.#focusCell();
  };

  #pageSize(): number {
    const rowHeight = this.#options.rowHeight ?? DEFAULT_ROW_HEIGHT;
    return Math.max(1, Math.floor(this.#scroller.clientHeight / rowHeight));
  }

  #scrollFocusIntoView(): void {
    const rowHeight = this.#options.rowHeight ?? DEFAULT_ROW_HEIGHT;
    const top = this.#focus.row * rowHeight;
    const bottom = top + rowHeight;
    const viewTop = this.#scroller.scrollTop;
    const viewBottom = viewTop + this.#scroller.clientHeight;
    if (top < viewTop) this.#scroller.scrollTop = top;
    else if (bottom > viewBottom) {
      this.#scroller.scrollTop = bottom - this.#scroller.clientHeight;
    }
  }

  #focusCell(): void {
    const el = this.#body.querySelector<HTMLElement>('.dc-focus');
    el?.focus();
  }

  /** Focused cell position, for tests. */
  get focus(): Readonly<Focus> {
    return this.#focus;
  }
}
