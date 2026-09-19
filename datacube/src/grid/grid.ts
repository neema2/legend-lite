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

import { TREE_COLUMN, type ColumnModel } from './columns.ts';
import { computeRowWindow, isCovered, type RowWindow } from './viewport.ts';
import type { FloatingFilterRow } from './floating-filter.ts';
import { makeHeaderDraggable } from '../ui/pivot-panel.ts';
import type { ColumnFormat, FormatterCache } from '../format.ts';
import { DEFAULT_FORMAT } from '../format.ts';
import type { ResultTable, Scalar } from '../result.ts';
import {
  contains,
  extend as extendRange,
  selectionTable,
  single,
  type CellRange,
} from '../selection.ts';
import { toClipboard } from '../export.ts';
import {
  cellStyle,
  gridVariables,
  isAlternateRow,
  mergeAppearance,
  type CellAppearance,
  type GridAppearance,
} from '../style.ts';

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
  /** Grid-wide appearance: fonts, grid lines, alternating rows. */
  readonly appearance?: GridAppearance;
  /** Per-column appearance, merged over the grid's, by column name. */
  readonly columnAppearance?: Readonly<Record<string, CellAppearance>>;
  /** Metadata per absolute row index, for grouping and totals. */
  readonly rowMeta?: (absoluteRow: number) => GridRowMeta;
  readonly onToggleExpand?: (key: string, expanded: boolean) => void;
  readonly onActivateCell?: (row: number, column: number) => void;
  /** Called whenever the selected rectangle changes. */
  readonly onSelectionChange?: (range: CellRange | null) => void;
  /**
   * Write text to the clipboard. Injected rather than calling the
   * navigator directly, because clipboard access is permission-gated
   * and a test must not depend on a browser granting it.
   */
  readonly writeClipboard?: (text: string) => void | Promise<void>;
  /**
   * A filter box under each column header.
   *
   * Supplying the row turns it on; the grid places one cell per leaf
   * and the row owns the filter tree. ag-Grid's idiom, not
   * DataCube's -- see floating-filter.ts.
   */
  readonly floatingFilter?: FloatingFilterRow;
  /**
   * Whether a column header may be dragged into the pivot zones.
   *
   * Returning false leaves the header undraggable rather than
   * letting a drag start and refusing the drop, because a drag that
   * can never land is worse than no drag handle at all.
   */
  readonly canGroup?: (column: string) => boolean;
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
  #selection: CellRange | null = null;
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
    // Appearance rides CSS custom properties rather than per-cell
    // styles wherever it can: one declaration for the whole grid
    // instead of a style attribute on every cell.
    for (const [k, v] of Object.entries(gridVariables(options.appearance ?? {}))) {
      this.#root.style.setProperty(k, v);
    }
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
    this.#body.addEventListener('click', this.#onClick);
  }

  /** Replace the column model. Resets the header. */
  setColumns(model: ColumnModel): void {
    this.#model = model;
    this.#renderHeader();
    this.#announceRowCount();
    this.#rendered = null;
  }

  /**
   * Header rows above the data, INCLUDING the floating filter row.
   *
   * Every aria-rowindex in the body is offset by this, so the filter
   * row appearing or not must move the data rows with it -- a body
   * row announced as row 4 while the header occupies rows 1-4 is a
   * grid a screen-reader user cannot navigate.
   */
  #headerLevels(): number {
    return (
      (this.#model?.headerRows.length ?? 0) +
      (this.#options.floatingFilter ? 1 : 0)
    );
  }

  /**
   * aria-rowcount counts the header rows too.
   *
   * It has to, because aria-rowindex does: the last data row
   * announces as headerLevels + totalRows, and a count of totalRows
   * alone makes every row read as "row N of fewer-than-N".
   */
  #announceRowCount(): void {
    this.#root.setAttribute(
      'aria-rowcount',
      String(this.#headerLevels() + this.#totalRows),
    );
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
    this.#announceRowCount();
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
    this.#body.removeEventListener('click', this.#onClick);
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
      `repeat(${this.#headerLevels()}, var(--dc-row-height))`;

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
        // Only a cell sitting directly over ONE leaf names a column;
        // a pivot value spanning four leaves is not a column and
        // dragging it would have to mean four things at once.
        const leaf =
          cell.leafIndex !== undefined ? model.leaves[cell.leafIndex] : undefined;
        if (leaf) {
          el.dataset['column'] = leaf.name;
          makeHeaderDraggable(
            el,
            leaf.name,
            this.#options.canGroup?.(leaf.name) ?? false,
          );
        }
        row.appendChild(el);
      }
      this.#head.appendChild(row);
    });

    const filterRow = this.#options.floatingFilter;
    if (filterRow) {
      const row = doc.createElement('div');
      row.setAttribute('role', 'row');
      row.className = 'dc-head-row dc-floating-row';
      row.setAttribute('aria-rowindex', String(model.depth + 1));
      model.leaves.forEach((leaf, i) => {
        const cell = filterRow.cell({
          name: leaf.name,
          type: leaf.type,
          // A row dimension is filtered on its source values, a
          // pivoted measure on nothing a box can express -- its
          // name is a path, not a column the engine knows.
          filterable: leaf.path.length <= 1,
        });
        cell.style.gridColumn = `${i + 1} / span 1`;
        cell.style.gridRow = `${model.depth + 1} / span 1`;
        row.appendChild(cell);
      });
      this.#head.appendChild(row);
    }
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
        l.width !== undefined
          ? `${l.width}px`
          : l.isDimension
            ? 'var(--dc-dim-width)'
            : 'var(--dc-col-width)',
      )
      .join(' ');
  }

  /**
   * Clicking a disclosure chevron toggles its group.
   *
   * Delegated from the body rather than bound per cell: the body is
   * rebuilt on every render, and per-cell listeners would have to be
   * torn down with it.
   */
  #onClick = (event: MouseEvent): void => {
    // Duck-typed rather than `instanceof Element`: the constructor
    // belongs to the document's realm, so an iframe -- or a test DOM
    // -- fails the check and the handler silently does nothing.
    const target = event.target as { closest?: unknown } | null;
    if (!target || typeof target.closest !== 'function') return;
    const el = target as unknown as Element;

    const cell = el.closest<HTMLElement>('.dc-cell');
    const chevron = el.closest('.dc-chevron');
    if (cell && !chevron) {
      const row = cell.closest<HTMLElement>('.dc-row');
      const cells = row ? [...row.children] : [];
      const col = cells.indexOf(cell);
      const abs = Number(row?.getAttribute('aria-rowindex') ?? '0') -
        this.#headerLevels() - 1;
      if (col >= 0 && abs >= 0) {
        // Shift extends from the ANCHOR, so growing a selection works
        // from where it started rather than from the last cell
        // touched.
        this.#selection =
          event.shiftKey && this.#selection
            ? extendRange(this.#selection, { row: abs, col })
            : single(abs, col);
        this.#focus = { row: abs, col };
        this.#options.onSelectionChange?.(this.#selection);
        this.#rendered = null;
        this.#render();
      }
      return;
    }

    if (!chevron || chevron.classList.contains('dc-chevron-empty')) return;
    const row = chevron.closest<HTMLElement>('.dc-row');
    const key = row?.dataset['key'];
    if (key === undefined) return;
    const expanded = row?.getAttribute('aria-expanded') === 'true';
    event.preventDefault();
    this.#options.onToggleExpand?.(key, !expanded);
  };

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
    const headerLevels = this.#headerLevels();
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

      // Banding is by absolute row, so it does not flicker as the
      // window scrolls past.
      if (
        this.#options.appearance?.alternateRows &&
        isAlternateRow(abs, this.#options.appearance.alternateRowsCount ?? 1)
      ) {
        row.classList.add('dc-alt');
      }

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
        const classes = ['dc-cell'];
        if (leaf.isDimension) classes.push('dc-dim');
        if (leaf.pinned) classes.push(`dc-pin-${leaf.pinned}`);
        // Obscured until hovered, for a figure that should not be
        // readable over a shoulder or in a screen share.
        if (leaf.blurred) classes.push('dc-blur');
        cell.className = classes.join(' ');
        cell.setAttribute('aria-colindex', String(c + 1));

        if (loaded) {
          const value: Scalar =
            table.columns[leaf.index]?.values[local] ?? null;
          const text = this.#formatters.format(
            value,
            this.#options.formats?.[leaf.name] ?? DEFAULT_FORMAT,
          );

          // Colour follows the VALUE, not the column: a scale across
          // a nested pivot compares a subtotal against a leaf and
          // puts the strongest colour on whatever aggregates most.
          const appearance = mergeAppearance(
            this.#options.appearance ?? {},
            this.#options.columnAppearance?.[leaf.name],
          );
          for (const [k, v] of Object.entries(cellStyle(appearance, value))) {
            cell.style.setProperty(k, v);
          }

          if (leaf.name === TREE_COLUMN) {
            // Depth is shown by indentation rather than by a column
            // per dimension, so the grid stays one width however deep
            // the cube goes. Level 1 sits flush; each level indents.
            cell.classList.add('dc-tree');
            cell.style.setProperty(
              '--dc-indent',
              String(Math.max(0, (meta?.level ?? 1) - 1)),
            );
            if (meta?.expanded !== undefined) {
              const chevron = doc.createElement('span');
              chevron.className = 'dc-chevron';
              // The row already carries aria-expanded, so the glyph is
              // decoration; announcing it again would say "collapsed"
              // twice.
              chevron.setAttribute('aria-hidden', 'true');
              chevron.textContent = meta.expanded ? '\u25be' : '\u25b8';
              cell.appendChild(chevron);
            } else {
              // A leaf still needs the chevron's width, or its label
              // fails to line up under its siblings' labels.
              const spacer = doc.createElement('span');
              spacer.className = 'dc-chevron dc-chevron-empty';
              spacer.setAttribute('aria-hidden', 'true');
              cell.appendChild(spacer);
            }
            const label = doc.createElement('span');
            label.className = 'dc-tree-label';
            label.textContent = text;
            cell.appendChild(label);
          } else {
            cell.textContent = text;
          }
        } else {
          // Not fetched yet. Rendered as a placeholder rather than
          // blocking the scroll, and marked busy so a screen reader
          // says so instead of reading an empty cell as an empty value.
          cell.classList.add('dc-pending');
          cell.setAttribute('aria-busy', 'true');
        }

        if (this.#selection && contains(this.#selection, abs, c)) {
          cell.classList.add('dc-selected');
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

    // Re-rendering destroys the element that had DOM focus, which
    // silently drops the keyboard user onto <body> -- every expand
    // would eject them from the grid, and the next arrow key would go
    // nowhere. Restore focus, but only if it was ours to begin with,
    // so a background refresh never steals it from elsewhere.
    const hadFocus =
      doc.activeElement !== null && this.#root.contains(doc.activeElement);
    this.#body.replaceChildren(frag);
    this.#rendered = wanted;
    if (hadFocus) this.#focusCell();
  }

  // -- keyboard ----------------------------------------------------

  #onKeyDown = (event: KeyboardEvent): void => {
    const model = this.#model;
    if (!model) return;
    const lastCol = model.leaves.length - 1;
    const lastRow = Math.max(0, this.#totalRows - 1);
    let { row, col } = this.#focus;
    let handled = true;

    // Copy before the movement switch, so it does not also move.
    if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === 'c') {
      event.preventDefault();
      this.copySelection();
      return;
    }

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
    const moved = row !== this.#focus.row || col !== this.#focus.col;
    this.#focus = { row, col };
    if (moved) {
      // Shift+arrow grows the selection; a bare arrow replaces it,
      // which is what every grid and spreadsheet does.
      this.#selection =
        event.shiftKey && this.#selection
          ? extendRange(this.#selection, { row, col })
          : single(row, col);
      this.#options.onSelectionChange?.(this.#selection);
    }
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

  /** The selected rectangle, if any. */
  get selection(): CellRange | null {
    return this.#selection;
  }

  select(range: CellRange | null): void {
    this.#selection = range;
    this.#options.onSelectionChange?.(range);
    this.#rendered = null;
    this.#render();
  }

  /**
   * Copy the selection as TSV, which is what a spreadsheet expects on
   * the clipboard. Reuses the exporter, so escaping cannot drift
   * between a copy and a download.
   */
  copySelection(): string | null {
    const table = this.#table;
    const range = this.#selection;
    if (!table || !range) return null;
    const text = toClipboard(selectionTable(table, range));
    void this.#options.writeClipboard?.(text);
    return text;
  }
}
