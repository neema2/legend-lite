// Row windowing: which rows to build DOM for, and how tall to pretend
// the scroll area is.
//
// Pure arithmetic, deliberately. This is the logic that decides whether
// a scroll shows the right rows, and it is far easier to get right --
// and to keep right -- when it can be tested without a browser.
//
// Only the ROW axis is windowed. Column windowing was measured and
// dropped: it pays 1.2x at 200 pivot columns and only becomes dramatic
// past a thousand, while costing a three-query split, a row-membership
// hazard, column overscan and a window cache. Every column of the
// current result is therefore already in memory, which is why
// horizontal scrolling in this grid issues no queries at all.

export interface ViewportMetrics {
  /** Pixels scrolled from the top of the scrollable area. */
  readonly scrollTop: number;
  /** Visible height of the scroll container, in pixels. */
  readonly viewportHeight: number;
  /** Uniform row height. Pivot rows are uniform by construction. */
  readonly rowHeight: number;
  /** Total rows in the result, not just the loaded ones. */
  readonly totalRows: number;
  /**
   * Extra rows rendered above and below the visible band, so a small
   * scroll reveals already-built DOM instead of a blank gap.
   */
  readonly overscan?: number;
}

export interface RowWindow {
  /** First row index to render, inclusive. */
  readonly start: number;
  /** Last row index to render, exclusive. */
  readonly end: number;
  /** Pixels to offset the rendered block by, so it lands correctly. */
  readonly offsetTop: number;
  /** Full scrollable height, for the spacer that sizes the scrollbar. */
  readonly totalHeight: number;
}

const DEFAULT_OVERSCAN = 8;

/**
 * Clamp helper. Written out rather than inlined because every one of
 * these bounds has a way of being wrong at an edge, and naming them
 * makes the edges visible.
 */
function clamp(v: number, lo: number, hi: number): number {
  return v < lo ? lo : v > hi ? hi : v;
}

export function computeRowWindow(m: ViewportMetrics): RowWindow {
  const overscan = Math.max(0, m.overscan ?? DEFAULT_OVERSCAN);
  const rowHeight = Math.max(1, m.rowHeight);
  const totalRows = Math.max(0, m.totalRows);
  const totalHeight = totalRows * rowHeight;

  if (totalRows === 0) {
    return { start: 0, end: 0, offsetTop: 0, totalHeight: 0 };
  }

  // A negative scrollTop is real: elastic overscroll on macOS reports
  // it, and it must not produce a negative start index.
  const scrollTop = clamp(m.scrollTop, 0, Math.max(0, totalHeight - 1));
  const firstVisible = Math.floor(scrollTop / rowHeight);

  // Height can be 0 while the element is hidden or before layout; that
  // must yield an empty-but-valid window rather than NaN.
  const visibleCount =
    m.viewportHeight <= 0
      ? 0
      : Math.ceil(m.viewportHeight / rowHeight) + 1;

  const start = clamp(firstVisible - overscan, 0, totalRows);
  const end = clamp(firstVisible + visibleCount + overscan, start, totalRows);

  return { start, end, offsetTop: start * rowHeight, totalHeight };
}

/**
 * Whether a newly computed window is already covered by what is
 * rendered, so a scroll event can be ignored entirely.
 *
 * Scroll events fire far more often than the window actually changes;
 * with overscan, most of them move within the rendered band. Skipping
 * those is what keeps scrolling at frame rate.
 */
export function isCovered(rendered: RowWindow, wanted: RowWindow): boolean {
  return wanted.start >= rendered.start && wanted.end <= rendered.end;
}

/**
 * The slice of a fetched block that satisfies a window, or null when
 * the block does not cover it and a fetch is required.
 */
export function sliceFor(
  block: { offset: number; rowCount: number },
  wanted: RowWindow,
): { from: number; to: number } | null {
  const blockEnd = block.offset + block.rowCount;
  if (wanted.start < block.offset || wanted.end > blockEnd) return null;
  return { from: wanted.start - block.offset, to: wanted.end - block.offset };
}
