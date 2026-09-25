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

import {
  TREE_COLUMN,
  linkFor,
  type ColumnModel,
  type LeafColumn,
} from './columns.ts';
import { computeRowWindow, isCovered, type RowWindow } from './viewport.ts';
import {
  currentHeaderDrag,
  makeHeaderDraggable,
  setHeaderDrag,
} from '../ui/pivot-panel.ts';
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
  highlightBand,
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
   * Whether a column header may be dragged into the pivot zones.
   *
   * Returning false leaves the header undraggable rather than
   * letting a drag start and refusing the drop, because a drag that
   * can never land is worse than no drag handle at all.
   */
  readonly canGroup?: (column: string) => boolean;
  /**
   * A new left-to-right order, from dragging one header onto another.
   *
   * The whole order rather than a move, because the grid is what
   * knows the order that is on screen; handing over a pair would
   * make the host reconstruct it.
   */
  /**
   * A new column order, and the column that arrived from outside it.
   *
   * `added` is set when the drag came from the columns panel
   * carrying a column the grid was not showing -- upstream's
   * `allowDragFromColumnsToolPanel: true`, where a column dragged
   * out of the panel lands in the grid at the point it was dropped.
   * The grid cannot unhide it on its own; whoever owns the
   * configuration does that.
   */
  readonly onReorder?: (
    order: readonly string[],
    added?: string,
  ) => void;
  /**
   * A per-cell background, for a heatmap.
   *
   * A hook rather than a pass over the DOM afterwards, because the
   * grid is VIRTUALISED: anything painted onto the cells that exist
   * now is gone the moment a scroll rebuilds them. Returning null
   * leaves the cell's own colours alone.
   */
  readonly cellBackground?: (
    leaf: LeafColumn,
    absoluteRow: number,
    value: Scalar,
  ) => string | null;
  /**
   * A header was clicked: sort by it. Upstream sorts on a header click
   * with multi-sort always on; the host decides what a click means.
   */
  readonly onHeaderSort?: (column: string) => void;
  /** A column's right edge was dragged to a new width. */
  readonly onResizeColumn?: (column: string, width: number) => void;
  /**
   * The tooltip of a header cell, from the header path above and
   * including it; `leaf` names the column when the cell is one.
   */
  readonly headerTitle?: (path: readonly string[], leaf?: LeafColumn) => string;
  /** The tooltip of a tree cell -- "Group Value = EMEA (12)". */
  readonly treeTitle?: (absoluteRow: number, label: string) => string;
}

/** A sort as the header shows it. */
export interface HeaderSort {
  readonly column: string;
  readonly direction: 'asc' | 'desc';
}

/** Upstream's DEFAULT_COLUMN_MIN_WIDTH: no drag makes a column narrower. */
const RESIZE_MIN_WIDTH = 50;

/**
 * A cell's tooltip -- upstream's `tooltipValueGetter`: the raw value,
 * so a formatted "1.2m" can be read as the number it is, and a blank
 * cell says it is MISSING rather than empty.
 */
export function valueTitle(value: Scalar): string {
  if (value === null || value === undefined) return 'Missing Value';
  if (value === '') return "Value = ''";
  if (value === true) return 'Value = TRUE';
  if (value === false) return 'Value = FALSE';
  if (value instanceof Date) return `Value = ${value.toISOString()}`;
  return `Value = ${String(value)}`;
}

const DEFAULT_ROW_HEIGHT = 24;

/** How many gradients the pivot group header rotates through. */
const PIVOT_GROUP_COLOURS = 5;

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
  readonly #headGrid: HTMLElement;
  #resize: ResizeObserver | null = null;
  readonly #body: HTMLElement;
  readonly #spacer: HTMLElement;
  readonly #scroller: HTMLElement;
  readonly #formatters: FormatterCache;
  #options: GridOptions;
  /** The custom properties `gridVariables` last set on the root. */
  #vars: string[] = [];

  #model: ColumnModel | null = null;
  #table: ResultTable | null = null;
  /** Absolute index of the first row present in `#table`. */
  #blockOffset = 0;
  #totalRows = 0;
  #rendered: RowWindow | null = null;
  #focus: Focus = { row: 0, col: 0 };
  #selection: CellRange | null = null;
  #frame = 0;
  /** The sorts the header marks. */
  #sorts: readonly HeaderSort[] = [];
  /** Live widths while a column edge is being dragged. */
  readonly #dragWidths = new Map<string, number>();
  /** The scroll readout, and when it next hides. */
  readonly #hint: HTMLElement;
  #hintTimer: ReturnType<typeof setTimeout> | undefined;
  /** "Loading..." / "0 rows", over the body. */
  readonly #overlay: HTMLElement;
  #busy = false;
  #busyTimer: ReturnType<typeof setTimeout> | undefined;

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
    this.#applyVariables(options.appearance ?? {});
    this.#root.setAttribute('role', 'treegrid');
    this.#root.tabIndex = 0;

    // The header is TWO elements, and it has to be. It must clip at
    // the grid's width while its own content is as wide as every
    // column, because that is the only way it can be scrolled to
    // follow the body: a single `width: max-content` element is
    // exactly as wide as its content and so has no scroll range at
    // all. The outer element is the viewport, the inner one the grid.
    this.#head = doc.createElement('div');
    this.#head.className = 'dc-head';
    // Presentational, so the rowgroup inside it is still owned by the
    // treegrid -- the same shape `#spacer` uses around `#body`.
    this.#head.setAttribute('role', 'presentation');

    this.#headGrid = doc.createElement('div');
    this.#headGrid.className = 'dc-head-grid';
    this.#headGrid.setAttribute('role', 'rowgroup');
    this.#head.appendChild(this.#headGrid);

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

    // Upstream's scroll readout: "start-end/total" while scrolling.
    this.#hint = doc.createElement('div');
    this.#hint.className = 'dc-scroll-hint';
    this.#hint.setAttribute('aria-hidden', 'true');
    this.#hint.hidden = true;
    // Upstream's overlays: "Loading..." while a query runs, "0 rows"
    // when one comes back empty.
    this.#overlay = doc.createElement('div');
    this.#overlay.className = 'dc-grid-overlay';
    this.#overlay.setAttribute('role', 'status');
    this.#overlay.hidden = true;
    this.#root.append(this.#hint, this.#overlay);

    this.#scroller.addEventListener('scroll', this.#onScroll);
    // A RESIZE MOVES THE BODY WITHOUT A SCROLL EVENT.
    //
    // Widening the grid -- collapsing the sidebar does it -- shrinks
    // the scroll range, so the browser clamps `scrollLeft` down to
    // fit. The header is a separate scroller driven from the scroll
    // handler, and no scroll event need fire for that clamp, so it
    // kept its old offset: every column's header sat exactly the
    // clamped distance to the left of its own cells. Measured at
    // 100px after one sidebar collapse.
    //
    // Guarded because jsdom has no ResizeObserver, and a grid that
    // cannot observe its own size still has to build.
    const Observer = (this.#root.ownerDocument.defaultView as
      { ResizeObserver?: typeof ResizeObserver } | null)?.ResizeObserver;
    if (Observer) {
      // ONLY ON AN ACTUAL WIDTH CHANGE. A callback that touches the
      // DOM every time it runs keeps the page in motion, and
      // Playwright -- like a person -- waits for an element to stop
      // moving before clicking it; several checks timed out waiting
      // for a grid that never settled. The vertical axis cannot
      // affect a horizontal offset, so height changes are ignored.
      let seen = -1;
      this.#resize = new Observer(() => {
        const width = Math.round(this.#scroller.clientWidth);
        if (width === seen) return;
        seen = width;
        this.#syncHeaderOffset();
      });
      this.#resize.observe(this.#scroller);
    }
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
   * Header rows above the data.
   *
   * Every aria-rowindex in the body is offset by this -- a body row
   * announced as row 4 while the header occupies rows 1-2 is a grid
   * a screen-reader user cannot navigate.
   */
  #headerLevels(): number {
    return this.#model?.headerRows.length ?? 0;
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
    this.#paintOverlay();
  }

  /** The sorts to mark on the header: an arrow, and a position when several. */
  setSorts(sorts: readonly HeaderSort[]): void {
    this.#sorts = sorts;
    this.#renderHeader();
  }

  /**
   * A query is running. "Loading..." shows only if it takes a moment,
   * so a fast answer does not flicker the grid.
   */
  setBusy(busy: boolean): void {
    this.#busy = busy;
    if (this.#busyTimer !== undefined) clearTimeout(this.#busyTimer);
    this.#busyTimer = undefined;
    if (busy) {
      this.#busyTimer = setTimeout(() => {
        this.#busyTimer = undefined;
        this.#paintOverlay();
      }, 250);
    } else {
      this.#paintOverlay();
    }
  }

  #paintOverlay(): void {
    const loading = this.#busy && this.#busyTimer !== undefined
      ? false : this.#busy;
    const empty = !this.#busy && this.#table !== null && this.#totalRows === 0;
    this.#overlay.hidden = !(loading || empty);
    this.#overlay.textContent = loading ? 'Loading...' : empty ? '0 rows' : '';
    this.#overlay.classList.toggle('dc-loading', loading);
  }

  /**
   * New appearance, because the cube's settings changed.
   *
   * The grid took its appearance once, at construction, and had no way
   * to take another -- so every font, colour, grid-line and highlight
   * setting changed the configuration and left the screen as it was
   * (found by the 2026-09-25 controls sweep). Number formats never had
   * the problem: the app mutates the one object the grid holds.
   */
  setAppearance(
    appearance: GridAppearance,
    columnAppearance: Readonly<Record<string, CellAppearance>>,
  ): void {
    this.#options = { ...this.#options, appearance, columnAppearance };
    this.#applyVariables(appearance);
    this.#rendered = null;
    this.#render();
  }

  #applyVariables(appearance: GridAppearance): void {
    for (const k of this.#vars) this.#root.style.removeProperty(k);
    const vars = gridVariables(appearance);
    for (const [k, v] of Object.entries(vars)) this.#root.style.setProperty(k, v);
    this.#vars = Object.keys(vars);
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
    this.#resize?.disconnect();
    this.#resize = null;
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
    this.#headGrid.replaceChildren();

    // One CSS grid for the whole header, with every cell placed
    // explicitly. Flexbox cannot express a cell spanning two rows, so
    // a ragged header laid out in document order puts the lower row's
    // cells under the dimension columns instead of under their values.
    this.#headGrid.style.gridTemplateColumns = this.#templateColumns(model);
    // Header rows are 24px where body rows are 20px -- their
    // --ag-header-height and --ag-row-height differ, and laying the
    // header out on the body's height makes every header cell 4px
    // short of the real thing.
    this.#headGrid.style.gridTemplateRows =
      `repeat(${this.#headerLevels()}, var(--dc-head-height))`;

    let pivotGroup = 0;
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
        // A pivot VALUE group -- the `2021` spanning its measures --
        // takes one of five rotating gradients, as DataCube does.
        // It is not decoration: with several measures under each
        // pivot value, the bands are what tell you where one value's
        // block ends and the next begins.
        if (level === 0 && cell.rowSpan === 1 && model.depth > 1) {
          el.classList.add(`dc-pivot-group-${pivotGroup % PIVOT_GROUP_COLOURS}`);
          pivotGroup += 1;
        }

        // Only a cell sitting directly over ONE leaf names a column;
        // a pivot value spanning four leaves is not a column and
        // dragging it would have to mean four things at once.
        const leaf =
          cell.leafIndex !== undefined ? model.leaves[cell.leafIndex] : undefined;
        const title = this.#options.headerTitle?.(
          (model.leaves[cell.colStart]?.path ?? []).slice(0, level + 1), leaf);
        if (title) el.title = title;
        if (leaf) {
          this.#sortable(el, leaf);
          this.#resizable(el, leaf);
          this.#reorderable(el, leaf, model);
          el.dataset['column'] = leaf.name;
          // A STICKY COLUMN'S HEADER HAS TO BE STICKY TOO.
          //
          // Only the body cells carried these, so a pinned column --
          // and the row-dimension tree column, which is pinned by the
          // same rule -- kept its data in place while its header slid
          // away with the scroll. Measured on the 60-column sample:
          // the tree cells sat at x=21 and their header at x=-5163.
          //
          // It is the fault the user reported, mirrored: there the
          // header failed to move with its column, here it moves when
          // its column does not. Both are the same property -- a
          // header belongs over its column -- which is why one
          // invariant now covers them.
          if (leaf.isDimension) el.classList.add('dc-dim');
          if (leaf.pinned) el.classList.add(`dc-pin-${leaf.pinned}`);
          makeHeaderDraggable(
            el,
            leaf.name,
            this.#options.canGroup?.(leaf.name) ?? false,
          );
        }
        row.appendChild(el);
      }
      this.#headGrid.appendChild(row);
    });

  }

  /**
   * Slide the header to wherever the body is scrolled horizontally.
   *
   * The header cannot simply live inside the scroller: it is
   * `position: sticky` against the grid, and a sticky element inside
   * the same box that scrolls horizontally sticks in BOTH axes, so
   * the labels would stay put while their columns moved. So it is a
   * sibling with its own overflow, driven from here -- which is what
   * ag-grid does, for the same reason.
   *
   * ONE call site, in the scroll handler. Rebuilding the header's
   * children does not need its own sync: replacing them inside one
   * frame keeps the viewport's offset, and a column model narrow
   * enough to clamp the body's offset fires a scroll event that
   * lands here anyway. Both were checked by removing this call and
   * watching verify:upload stay green across a regroup.
   *
   * `scrollLeft` rather than a transform: `overflow: hidden` still
   * makes a scroll container, it just refuses the user's gestures,
   * and scrolling it keeps the sticky positioning working instead of
   * establishing a containing block that would break it.
   */
  #syncHeaderOffset(): void {
    const x = this.#scroller.scrollLeft;
    // The IN-RANGE part goes through scrollLeft, as before.
    const max = Math.max(0, this.#head.scrollWidth - this.#head.clientWidth);
    const clamped = Math.max(0, Math.min(x, max));
    this.#head.scrollLeft = clamped;

    // The OVERSHOOT cannot: a scroll container refuses to scroll past
    // its own range, so during an elastic overscroll the body
    // rubber-bands and the header stays put, ending up a few pixels
    // out of line -- which is exactly where a user notices it, since
    // the bounce draws the eye. macOS reports those out-of-range
    // offsets rather than hiding them (see viewport.ts, which clamps
    // a negative scrollTop for the same reason), so the remainder is
    // known and can be applied as a transform, which has no range.
    //
    // At rest the overshoot is zero and no transform is set at all,
    // so the ordinary case is untouched -- including the sticky
    // pinned header cells, which a permanent transform on their
    // container would have put at risk.
    const over = x - clamped;
    this.#head.style.transform = over === 0
      ? ''
      : `translateX(${-over}px)`;
  }

  /**
   * Dragging a header onto another reorders the columns.
   *
   * Only leaves that NAME ONE SOURCE COLUMN take part. A pivoted
   * leaf is `2021__|__notional` -- a value crossed with a measure --
   * and the order the configuration holds is a list of source
   * columns, so there is nothing to move a pivot block to. The tree
   * column is the row dimensions and is not a column of the data at
   * all.
   *
   * Which HALF of the target decides the side, as every column
   * reorder does: the left half puts the dragged column before it,
   * the right half after. Without that, dragging rightwards can
   * never place a column last.
   */
  #reorderable(el: HTMLElement, leaf: LeafColumn, model: ColumnModel): void {
    const onReorder = this.#options.onReorder;
    if (!onReorder) return;
    // A PIVOTED LEAF REORDERS ITS MEASURE. `2021__|__notional` is a
    // value crossed with a measure, and the order a configuration
    // holds is a list of source columns -- so dragging it moves
    // `notional` among the measures, in every value block at once.
    // That is the only outcome the configuration can express, and
    // refusing the drag instead (which this first did) leaves a
    // pivoted cube with no way to reorder anything at all.
    const rank = (l: LeafColumn): string =>
      l.path[l.path.length - 1] ?? l.name;
    const order: string[] = [];
    for (const l of model.leaves) {
      if (l.name === TREE_COLUMN) continue;
      const key = rank(l);
      if (!order.includes(key)) order.push(key);
    }
    const self = rank(leaf);
    if (!order.includes(self)) return;

    // Draggable even when it cannot be GROUPED: a measure is
    // reorderable, it just has nowhere to be dropped in the zones.
    el.draggable = true;
    el.classList.add('dc-reorderable');
    el.addEventListener('dragstart', (event) => {
      setHeaderDrag({ column: leaf.name });
      if (event.dataTransfer) {
        event.dataTransfer.effectAllowed = 'move';
        event.dataTransfer.setData('text/plain', leaf.name);
      }
    });
    el.addEventListener('dragend', () => {
      setHeaderDrag(null);
      this.#clearDropMarks();
    });

    const side = (event: DragEvent): 'before' | 'after' => {
      const box = el.getBoundingClientRect();
      return event.clientX > box.left + box.width / 2 ? 'after' : 'before';
    };
    const held = (): string | null => {
      const drag = currentHeaderDrag();
      if (!drag) return null;
      // The MEASURE behind what is being dragged, so a pivoted leaf
      // is compared with this one on the same footing. Two leaves of
      // the same measure in different value blocks have nothing to
      // reorder between them.
      const moved = model.leaves.find((l) => l.name === drag.column);
      const key = moved ? rank(moved) : drag.column;
      if (key === self) return null;
      if (order.includes(key)) return key;
      // NOT IN THE GRID AT ALL, which is a drop from the columns
      // panel carrying a hidden column. Every other drag is between
      // things already on screen, so anything else unknown here is
      // refused as it was.
      return drag.from === 'panel' ? key : null;
    };

    el.addEventListener('dragover', (event) => {
      if (!held()) return;
      // Only a prevented dragover makes an element a drop target.
      event.preventDefault();
      if (event.dataTransfer) event.dataTransfer.dropEffect = 'move';
      this.#clearDropMarks();
      el.classList.add(`dc-drop-${side(event)}`);
    });
    el.addEventListener('dragleave', () => {
      el.classList.remove('dc-drop-before', 'dc-drop-after');
    });
    el.addEventListener('drop', (event) => {
      const moved = held();
      this.#clearDropMarks();
      if (!moved) return;
      event.preventDefault();
      const where = side(event);
      const arrived = !order.includes(moved);
      const next = order.filter((n) => n !== moved);
      const at = next.indexOf(self) + (where === 'after' ? 1 : 0);
      next.splice(at, 0, moved);
      setHeaderDrag(null);
      onReorder(next, arrived ? moved : undefined);
    });
  }

  /**
   * A header click sorts, and the header says how: an arrow, and the
   * column's place in the sort when there are several.
   */
  #sortable(el: HTMLElement, leaf: LeafColumn): void {
    const at = this.#sorts.findIndex((x) => x.column === leaf.name);
    const sort = this.#sorts[at];
    el.setAttribute('aria-sort', sort === undefined ? 'none'
      : sort.direction === 'asc' ? 'ascending' : 'descending');
    if (sort !== undefined) {
      const mark = el.ownerDocument.createElement('span');
      mark.className = 'dc-sort-mark';
      mark.setAttribute('aria-hidden', 'true');
      mark.textContent = (sort.direction === 'asc' ? '\u2191' : '\u2193')
        + (this.#sorts.length > 1 ? String(at + 1) : '');
      el.append(mark);
    }
    const onSort = this.#options.onHeaderSort;
    if (!onSort) return;
    el.classList.add('dc-sortable');
    el.addEventListener('click', (event) => {
      const target = event.target as { closest?: unknown } | null;
      if (target && typeof target.closest === 'function'
        && (target as Element).closest('.dc-col-resize')) return;
      onSort(leaf.name);
    });
  }

  /**
   * A grip on the header's right edge resizes the column: live while
   * dragging, committed on release. Never on a fixed width -- upstream
   * makes those unresizable -- and never below upstream's minimum.
   */
  #resizable(el: HTMLElement, leaf: LeafColumn): void {
    const onResize = this.#options.onResizeColumn;
    if (!onResize || leaf.fixed === true) return;
    const doc = el.ownerDocument;
    const grip = doc.createElement('span');
    grip.className = 'dc-col-resize';
    grip.setAttribute('aria-hidden', 'true');
    grip.draggable = false;
    grip.addEventListener('mousedown', (e) => e.stopPropagation());
    grip.addEventListener('click', (e) => e.stopPropagation());
    grip.addEventListener('dragstart', (e) => { e.preventDefault(); e.stopPropagation(); });
    grip.addEventListener('pointerdown', (event) => {
      if (event.button !== 0) return;
      event.preventDefault();
      event.stopPropagation();
      const startX = event.clientX;
      const from = Math.round(el.getBoundingClientRect().width);
      const width = (e: PointerEvent): number => Math.max(
        leaf.minWidth ?? RESIZE_MIN_WIDTH,
        Math.min(leaf.maxWidth ?? Infinity, from + (e.clientX - startX)),
      );
      const move = (e: PointerEvent): void => {
        this.#dragWidths.set(leaf.name, width(e));
        this.#applyTemplate();
      };
      const up = (e: PointerEvent): void => {
        grip.removeEventListener('pointermove', move);
        grip.removeEventListener('pointerup', up);
        grip.removeEventListener('pointercancel', up);
        const final = this.#dragWidths.get(leaf.name) ?? width(e);
        this.#dragWidths.delete(leaf.name);
        if (final !== from) onResize(leaf.name, final);
        else this.#applyTemplate();
      };
      grip.setPointerCapture?.(event.pointerId);
      grip.addEventListener('pointermove', move);
      grip.addEventListener('pointerup', up);
      grip.addEventListener('pointercancel', up);
    });
    el.append(grip);
  }

  /** Re-lay the header and the rendered rows on the current widths. */
  #applyTemplate(): void {
    const model = this.#model;
    if (!model) return;
    const template = this.#templateColumns(model);
    this.#headGrid.style.gridTemplateColumns = template;
    for (const row of this.#body.querySelectorAll<HTMLElement>('.dc-row')) {
      row.style.gridTemplateColumns = template;
    }
  }

  #clearDropMarks(): void {
    for (const e of this.#head.querySelectorAll('.dc-drop-before,'
      + ' .dc-drop-after')) {
      e.classList.remove('dc-drop-before', 'dc-drop-after');
    }
  }

  /**
   * The content width of each column, as currently rendered.
   *
   * `scrollWidth` rather than any text measurement: a cell clips with
   * `overflow: hidden`, and scrollWidth is exactly the width its
   * content wanted. That also means this measures WHAT IS ON SCREEN
   * -- the grid is virtualised, so a column whose widest value is
   * ten thousand rows down does not count. ag-grid's own
   * auto-size-to-fit-content has the same property, so matching it
   * is the faithful behaviour rather than a shortcut.
   *
   * The header is included: a column auto-sized to its values alone
   * can end up too narrow to read its own name.
   */
  measureColumns(names?: readonly string[]): Record<string, number> {
    const model = this.#model;
    if (!model) return {};
    const want = names ? new Set(names) : null;
    const out: Record<string, number> = {};
    for (const leaf of model.leaves) {
      if (want && !want.has(leaf.name)) continue;
      let max = 0;
      const cells = this.#root.querySelectorAll<HTMLElement>(
        `[data-column="${CSS.escape(leaf.name)}"]`,
      );
      for (const cell of cells) max = Math.max(max, cell.scrollWidth);
      if (max > 0) out[leaf.name] = max;
    }
    return out;
  }

  /** Each shown column's width as rendered, by its header. */
  renderedWidths(): Record<string, number> {
    const out: Record<string, number> = {};
    for (const th of this.#root.querySelectorAll<HTMLElement>('.dc-th[data-column]')) {
      const name = th.dataset['column'];
      if (name !== undefined) out[name] = Math.round(th.getBoundingClientRect().width);
    }
    return out;
  }

  /** The width the columns have to share. */
  get viewportWidth(): number {
    return this.#scroller.clientWidth;
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
        this.#dragWidths.has(l.name)
          ? `${this.#dragWidths.get(l.name)}px`
          : l.width !== undefined
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
    // MARKED PENDING BEFORE the request, not from its return value.
    // Taking the id back from `requestAnimationFrame` assumes the
    // callback runs later; where it runs synchronously the callback
    // clears the flag first and the id is assigned afterwards, so the
    // flag is left set for ever and every later scroll returns early.
    // A scroll handler that stops firing after the first event is a
    // bad thing to have riding on that assumption.
    this.#frame = 1;
    requestAnimationFrame(() => {
      this.#frame = 0;
      this.#showScrollHint();
      // Before the render, and OUTSIDE it: `#render` returns early
      // when the row window has not moved, which is exactly what a
      // purely horizontal scroll does.
      this.#syncHeaderOffset();
      this.#render();
    });
  };

  /**
   * Upstream's scroll readout: which rows are on screen, of how many,
   * shown while scrolling and gone a moment after.
   */
  #showScrollHint(): void {
    if (this.#totalRows === 0) return;
    const rowHeight = this.#options.rowHeight ?? DEFAULT_ROW_HEIGHT;
    const top = this.#scroller.scrollTop;
    const first = Math.max(1, Math.ceil(top / rowHeight) + 1);
    const last = Math.min(this.#totalRows,
      Math.floor((top + this.#scroller.clientHeight) / rowHeight));
    this.#hint.textContent = `${first}-${Math.max(first, last)}/${this.#totalRows}`;
    this.#hint.hidden = false;
    if (this.#hintTimer !== undefined) clearTimeout(this.#hintTimer);
    this.#hintTimer = setTimeout(() => { this.#hint.hidden = true; }, 800);
  }

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
      const band = highlightBand(this.#options.appearance ?? {});
      if (band > 0 && isAlternateRow(abs, band)) {
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
        // The context menu reads this. Without it a right-click on a
        // CELL produced a menu with every column-specific entry
        // missing, which is most of the menu.
        cell.dataset['column'] = leaf.name;

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

          // After the value colours, so a heatmap wins over the
          // normal/negative background it would otherwise fight.
          const heat = this.#options.cellBackground?.(leaf, abs, value);
          if (heat) cell.style.backgroundColor = heat;

          cell.title = leaf.name === TREE_COLUMN
            ? (this.#options.treeTitle?.(abs, text) ?? '')
            : valueTitle(value);
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
            const link = leaf.linkLabelParameter === undefined
              ? null
              : linkFor(value, leaf.linkLabelParameter);
            if (link) {
              const a = doc.createElement('a');
              a.className = 'dc-link';
              a.href = link.href;
              a.target = '_blank';
              a.rel = 'noreferrer noopener';
              a.textContent = link.label;
              cell.appendChild(a);
            } else {
              cell.textContent = text;
            }
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
