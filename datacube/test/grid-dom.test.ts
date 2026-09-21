// DOM tests for the grid, with particular attention to the ARIA the
// research says every other analytical grid gets wrong.

import assert from 'node:assert/strict';
import { beforeEach, describe, it } from 'node:test';
import { JSDOM } from 'jsdom';

import { buildColumnModel } from '../src/grid/columns.ts';
import { DataGrid, type GridRowMeta } from '../src/grid/grid.ts';
import { FormatterCache } from '../src/format.ts';
import type { ResultTable } from '../src/result.ts';

const ROW_HEIGHT = 20;
const VIEW_HEIGHT = 200;

let dom: JSDOM;
let container: HTMLElement;
let grid: DataGrid;

function makeTable(rows: number): ResultTable {
  const region: (string | null)[] = [];
  const y2023: (number | null)[] = [];
  const y2024: (number | null)[] = [];
  for (let i = 0; i < rows; i++) {
    region.push(`R${i}`);
    y2023.push(i * 1.5);
    y2024.push(i % 7 === 0 ? null : i * 2.5);
  }
  return {
    columns: [
      { name: 'region', type: 'String', values: region },
      { name: '2023__|__total', type: 'Float', values: y2023 },
      { name: '2024__|__total', type: 'Float', values: y2024 },
    ],
    rowCount: rows,
    epoch: 1,
    elapsedMs: 0,
  };
}

/** jsdom reports zero layout, so the sizes the grid reads are stubbed. */
function stubLayout(el: Element, height: number): void {
  Object.defineProperty(el, 'clientHeight', {
    value: height,
    configurable: true,
  });
}

beforeEach(() => {
  dom = new JSDOM('<!doctype html><div id="g"></div>');
  const g = dom.window.document.getElementById('g');
  assert.ok(g);
  container = g as unknown as HTMLElement;
  // The grid calls requestAnimationFrame; jsdom has it, but run it
  // synchronously so tests do not have to wait a frame.
  (dom.window as unknown as { requestAnimationFrame: unknown })
    .requestAnimationFrame = (cb: FrameRequestCallback) => {
      cb(0);
      return 1;
    };
  globalThis.requestAnimationFrame = (dom.window as unknown as {
    requestAnimationFrame: typeof requestAnimationFrame;
  }).requestAnimationFrame;
  globalThis.cancelAnimationFrame = () => {};
});

function build(rows: number, total = rows, meta?: (r: number) => GridRowMeta) {
  const table = makeTable(rows);
  grid = new DataGrid(container, new FormatterCache(), {
    rowHeight: ROW_HEIGHT,
    overscan: 2,
    formats: { '2023__|__total': { kind: 'number', locale: 'en-US' } },
    ...(meta ? { rowMeta: meta } : {}),
  });
  const scroller = container.querySelector('.dc-scroller');
  assert.ok(scroller);
  stubLayout(scroller, VIEW_HEIGHT);
  grid.setColumns(buildColumnModel(table, ['region']));
  grid.setRows(table, 0, total);
  return { table, scroller: scroller as HTMLElement };
}

describe('DataGrid DOM', () => {
  it('exposes treegrid roles', () => {
    build(50);
    assert.equal(container.getAttribute('role'), 'treegrid');
    assert.equal(
      container.querySelectorAll('[role="rowgroup"]').length,
      2,
      'header and body are both rowgroups',
    );
    assert.ok(container.querySelector('[role="columnheader"]'));
    assert.ok(container.querySelector('[role="gridcell"]'));
  });

  it('renders the ragged pivot header with spans', () => {
    build(10);
    const level0 = container.querySelectorAll('.dc-head-row')[0];
    const cells = level0?.querySelectorAll('[role="columnheader"]') ?? [];
    assert.equal(cells.length, 3); // region, 2023, 2024
    // The row dimension spans both header levels.
    assert.equal(cells[0]?.getAttribute('aria-rowspan'), '2');
    assert.equal(cells[0]?.textContent, 'region');
  });

  it('renders only a window of rows, not the whole result', () => {
    build(10_000);
    const rendered = container.querySelectorAll('.dc-row').length;
    // ~10 visible + 1 partial + 2 overscan, nowhere near 10,000.
    assert.ok(rendered > 0 && rendered < 30, `rendered ${rendered}`);
  });

  it('announces absolute position in the whole result, not the band', () => {
    // The mistake virtualised grids make: without these, a reader says
    // "row 3 of 12" when the user is at row 5,000 of a million.
    const { scroller } = build(10_000);
    // The count includes the header rows, because the INDEX does:
    // the last data row announces as headerLevels + totalRows, so a
    // count of totalRows alone makes every row read as "row N of
    // fewer-than-N".
    assert.equal(container.getAttribute('aria-rowcount'), '10002');

    scroller.scrollTop = 100_000; // row 5,000
    scroller.dispatchEvent(new dom.window.Event('scroll'));

    const first = container.querySelector('.dc-row');
    const idx = Number(first?.getAttribute('aria-rowindex'));
    // 2 header rows precede the body, and rowindex is 1-based.
    assert.ok(idx > 4900 && idx < 5100, `aria-rowindex was ${idx}`);
  });

  it('formats through the cache and renders null as empty', () => {
    build(20);
    const rows = container.querySelectorAll('.dc-row');
    const firstRowCells = rows[0]?.querySelectorAll('.dc-cell') ?? [];
    assert.equal(firstRowCells[0]?.textContent, 'R0');
    // row 0 of 2024 is null -> empty string, never the word "null"
    assert.equal(firstRowCells[2]?.textContent, '');
  });

  it('marks unfetched cells busy rather than showing a false empty', () => {
    // A block covering rows 0-9 while the result claims 1,000.
    const table = makeTable(10);
    grid = new DataGrid(container, new FormatterCache(), {
      rowHeight: ROW_HEIGHT,
      overscan: 2,
    });
    const scroller = container.querySelector('.dc-scroller');
    stubLayout(scroller!, VIEW_HEIGHT);
    grid.setColumns(buildColumnModel(table, ['region']));
    grid.setRows(table, 0, 1000);

    const pending = container.querySelectorAll('.dc-pending');
    assert.ok(pending.length > 0, 'rows past the block are placeholders');
    assert.equal(pending[0]?.getAttribute('aria-busy'), 'true');
  });

  it('carries aria-level and aria-expanded for group rows', () => {
    build(20, 20, (r) =>
      // Even rows are collapsible groups; odd rows are leaves and must
      // carry no aria-expanded at all, which exactOptionalPropertyTypes
      // enforces by refusing an explicit undefined.
      r % 2 === 0
        ? { level: 1, expanded: false, key: `k${r}`, isTotal: r === 0 }
        : { level: 2, key: `k${r}` },
    );
    const rows = container.querySelectorAll('.dc-row');
    assert.equal(rows[0]?.getAttribute('aria-level'), '1');
    assert.equal(rows[0]?.getAttribute('aria-expanded'), 'false');
    assert.equal(rows[1]?.getAttribute('aria-level'), '2');
    assert.equal(rows[1]?.hasAttribute('aria-expanded'), false);
    assert.ok(rows[0]?.classList.contains('dc-total'));
  });

  it('keeps the spacer out of the accessibility tree', () => {
    build(10);
    assert.equal(
      container.querySelector('.dc-spacer')?.getAttribute('role'),
      'presentation',
    );
  });
});

describe('DataGrid selection', () => {
  function press(key: string, init: KeyboardEventInit = {}) {
    container.dispatchEvent(
      new dom.window.KeyboardEvent('keydown', {
        key,
        bubbles: true,
        ...init,
      }) as unknown as KeyboardEvent,
    );
  }

  function clickCell(row: number, col: number, shift = false) {
    const cells = container
      .querySelectorAll('.dc-row')
      [row]!.querySelectorAll('.dc-cell');
    cells[col]!.dispatchEvent(
      new dom.window.MouseEvent('click', { bubbles: true, shiftKey: shift }),
    );
  }

  it('selects a single cell on click', () => {
    build(50);
    clickCell(2, 1);
    assert.deepEqual(grid.selection?.anchor, { row: 2, col: 1 });
    assert.equal(container.querySelectorAll('.dc-selected').length, 1);
  });

  it('extends from the ANCHOR on shift-click', () => {
    // Growing from the last cell touched rather than the anchor is
    // the classic shift-click bug.
    build(50);
    clickCell(1, 0);
    clickCell(3, 2, true);
    assert.deepEqual(grid.selection?.anchor, { row: 1, col: 0 });
    assert.equal(container.querySelectorAll('.dc-selected').length, 9);

    // Shift-clicking back must still measure from the anchor.
    clickCell(0, 0, true);
    assert.deepEqual(grid.selection?.anchor, { row: 1, col: 0 });
    assert.equal(container.querySelectorAll('.dc-selected').length, 2);
  });

  it('shift-arrow grows and a bare arrow replaces', () => {
    build(50);
    clickCell(0, 0);
    press('ArrowDown', { shiftKey: true });
    assert.equal(container.querySelectorAll('.dc-selected').length, 2);
    press('ArrowDown');
    assert.equal(
      container.querySelectorAll('.dc-selected').length,
      1,
      'a bare arrow collapses the selection to one cell',
    );
  });

  it('reports every selection change', () => {
    const seen: unknown[] = [];
    const table = makeTable(20);
    grid = new DataGrid(container, new FormatterCache(), {
      rowHeight: ROW_HEIGHT,
      onSelectionChange: (r) => seen.push(r),
    });
    stubLayout(container.querySelector('.dc-scroller')!, VIEW_HEIGHT);
    grid.setColumns(buildColumnModel(table, ['region']));
    grid.setRows(table, 0, 20);
    grid.select({ anchor: { row: 0, col: 0 }, focus: { row: 1, col: 1 } });
    assert.equal(seen.length, 1);
  });

  it('copies the selection as TSV through the injected writer', () => {
    // Clipboard access is permission-gated, so the writer is injected
    // rather than reached for on the navigator.
    const written: string[] = [];
    const table = makeTable(20);
    grid = new DataGrid(container, new FormatterCache(), {
      rowHeight: ROW_HEIGHT,
      writeClipboard: (t) => {
        written.push(t);
      },
    });
    stubLayout(container.querySelector('.dc-scroller')!, VIEW_HEIGHT);
    grid.setColumns(buildColumnModel(table, ['region']));
    grid.setRows(table, 0, 20);
    grid.select({ anchor: { row: 0, col: 0 }, focus: { row: 1, col: 1 } });

    const text = grid.copySelection();
    assert.equal(written.length, 1);
    assert.equal(written[0], text);
    assert.match(text ?? '', /^region\t2023__\|__total\nR0\t0\nR1\t1\.5\n$/);
  });

  it('copies nothing when nothing is selected', () => {
    build(20);
    assert.equal(grid.copySelection(), null);
  });
});

describe('DataGrid appearance', () => {
  function appearanceGrid(opts: Record<string, unknown>) {
    const table = makeTable(30);
    grid = new DataGrid(container, new FormatterCache(), {
      rowHeight: ROW_HEIGHT,
      overscan: 2,
      ...opts,
    });
    stubLayout(container.querySelector('.dc-scroller')!, VIEW_HEIGHT);
    grid.setColumns(buildColumnModel(table, ['region']));
    grid.setRows(table, 0, 30);
    return table;
  }

  it('colours a cell by its value, not by its column', () => {
    appearanceGrid({
      appearance: { normalForeground: 'rgb(17, 17, 17)' },
      columnAppearance: {
        '2023__|__total': { negativeForeground: 'rgb(220, 20, 60)' },
      },
    });
    const cells = container.querySelectorAll('.dc-row .dc-cell:not(.dc-dim)');
    // Every value in the fixture is non-negative, so the normal
    // colour applies; the negative slot is configured but unused.
    assert.equal(
      (cells[0] as HTMLElement).style.color,
      'rgb(17, 17, 17)',
    );
  });

  it('bands rows by absolute index, so scrolling does not flicker', () => {
    appearanceGrid({ appearance: { alternateRows: true } });
    const rows = [...container.querySelectorAll('.dc-row')];
    assert.equal(rows[0]?.classList.contains('dc-alt'), false);
    assert.equal(rows[1]?.classList.contains('dc-alt'), true);
    assert.equal(rows[2]?.classList.contains('dc-alt'), false);
  });

  it('honours a band size greater than one', () => {
    appearanceGrid({
      appearance: { alternateRows: true, alternateRowsCount: 2 },
    });
    const banded = [...container.querySelectorAll('.dc-row')].map((r) =>
      r.classList.contains('dc-alt'),
    );
    assert.deepEqual(banded.slice(0, 6), [false, false, true, true, false, false]);
  });

  it('sets grid-line variables on the container', () => {
    appearanceGrid({ appearance: { showHorizontalGridLines: false } });
    assert.equal(container.style.getPropertyValue('--dc-hgrid'), '0');
    assert.equal(container.style.getPropertyValue('--dc-vgrid'), '1');
  });
});

describe('DataGrid keyboard', () => {
  function press(key: string, init: KeyboardEventInit = {}) {
    container.dispatchEvent(
      new dom.window.KeyboardEvent('keydown', {
        key,
        bubbles: true,
        ...init,
      }) as unknown as KeyboardEvent,
    );
  }

  it('moves with the arrow keys', () => {
    build(100);
    press('ArrowDown');
    press('ArrowDown');
    press('ArrowRight');
    assert.deepEqual(grid.focus, { row: 2, col: 1 });
    press('ArrowUp');
    press('ArrowLeft');
    assert.deepEqual(grid.focus, { row: 1, col: 0 });
  });

  it('clamps at the edges instead of running off', () => {
    build(3);
    press('ArrowUp');
    assert.deepEqual(grid.focus, { row: 0, col: 0 });
    for (let i = 0; i < 10; i++) press('ArrowDown');
    assert.equal(grid.focus.row, 2);
    for (let i = 0; i < 10; i++) press('ArrowRight');
    assert.equal(grid.focus.col, 2);
  });

  it('expands a collapsed group with ArrowRight, per the APG pattern', () => {
    const toggles: [string, boolean][] = [];
    const table = makeTable(10);
    grid = new DataGrid(container, new FormatterCache(), {
      rowHeight: ROW_HEIGHT,
      rowMeta: (r) => ({ level: 1, expanded: r === 0 ? false : true, key: `k${r}` }),
      onToggleExpand: (k, e) => toggles.push([k, e]),
    });
    stubLayout(container.querySelector('.dc-scroller')!, VIEW_HEIGHT);
    grid.setColumns(buildColumnModel(table, ['region']));
    grid.setRows(table, 0, 10);

    press('ArrowRight'); // on a collapsed row: expands, does not move
    assert.deepEqual(toggles, [['k0', true]]);
    assert.equal(grid.focus.col, 0, 'focus must not move when expanding');

    press('ArrowDown'); // row 1 is expanded
    press('ArrowLeft'); // at col 0 on an expanded row: collapses
    assert.deepEqual(toggles[1], ['k1', false]);
  });

  it('supports Home, End and paging', () => {
    build(500);
    press('End', { ctrlKey: true });
    assert.equal(grid.focus.row, 499);
    press('Home', { ctrlKey: true });
    assert.equal(grid.focus.row, 0);
    press('PageDown');
    assert.equal(grid.focus.row, Math.floor(VIEW_HEIGHT / ROW_HEIGHT));
    press('PageUp');
    assert.equal(grid.focus.row, 0);
  });

  it('activates a cell on Enter', () => {
    const hits: [number, number][] = [];
    const table = makeTable(10);
    grid = new DataGrid(container, new FormatterCache(), {
      rowHeight: ROW_HEIGHT,
      onActivateCell: (r, c) => hits.push([r, c]),
    });
    stubLayout(container.querySelector('.dc-scroller')!, VIEW_HEIGHT);
    grid.setColumns(buildColumnModel(table, ['region']));
    grid.setRows(table, 0, 10);
    press('Enter');
    assert.deepEqual(hits, [[0, 0]]);
  });

  it('keeps DOM focus after a re-render', () => {
    // Regression: re-rendering destroys the focused element, so DOM
    // focus fell back to <body> and the next arrow key went nowhere.
    // In the browser this ejected the keyboard user from the grid on
    // every expand, while the .dc-focus class still looked correct.
    const { table } = build(100);
    press('ArrowDown');
    const focused = container.querySelector('.dc-cell.dc-focus');
    assert.equal(
      dom.window.document.activeElement,
      focused,
      'the focused cell holds DOM focus',
    );

    // A refresh, as a controller would issue after expanding a group.
    grid.setRows(table, 0, 100);
    assert.equal(
      dom.window.document.activeElement,
      container.querySelector('.dc-cell.dc-focus'),
      'focus follows the cell across a re-render',
    );
  });

  it('does not steal focus it never had', () => {
    const { table } = build(100);
    const outside = dom.window.document.createElement('button');
    dom.window.document.body.appendChild(outside);
    outside.focus();

    grid.setRows(table, 0, 100);
    assert.equal(
      dom.window.document.activeElement,
      outside,
      'a background refresh must not grab focus from the page',
    );
  });

  it('keeps exactly one cell in the tab order', () => {
    build(100);
    press('ArrowDown');
    const focusable = container.querySelectorAll('.dc-cell[tabindex="0"]');
    assert.equal(focusable.length, 1, 'roving tabindex, per the APG pattern');
  });
});

describe('the header follows a horizontal ELASTIC OVERSCROLL', () => {
  // Reported from the product: scrolling sideways gives a bounce at
  // the end, and the header does not bounce with it, so the two end
  // up a few pixels apart -- right where the eye is, because the
  // bounce is what drew it there.
  //
  // A scroll container refuses to scroll past its own range, so
  // `head.scrollLeft = scroller.scrollLeft` can never follow an
  // overscroll. macOS reports the out-of-range offset rather than
  // hiding it, which `computeRowWindow` already relies on for the
  // vertical axis, so the remainder is known and goes on as a
  // transform -- which has no range.
  //
  // jsdom keeps whatever `scrollLeft` it is given, having no layout
  // to clamp against, which is what makes the arithmetic testable
  // here. What cannot be tested headlessly is the VISUAL match, since
  // the bounce is a compositor effect that does not occur.
  const scrollTo = (x: number): HTMLElement => {
    const scroller = container.querySelector('.dc-scroller');
    const head = container.querySelector('.dc-head');
    assert.ok(scroller && head);
    // A real header: 600px of columns inside a 400px viewport, so the
    // in-range part has somewhere to go.
    Object.defineProperty(head, 'scrollWidth', {
      value: 600, configurable: true,
    });
    Object.defineProperty(head, 'clientWidth', {
      value: 400, configurable: true,
    });
    (scroller as HTMLElement).scrollLeft = x;
    scroller.dispatchEvent(new dom.window.Event('scroll'));
    return head as unknown as HTMLElement;
  };

  it('sits flush with no transform while in range', () => {
    build(50);
    const head = scrollTo(120);
    assert.equal(head.scrollLeft, 120);
    assert.equal(head.style.transform, '',
      'a transform at rest would put the sticky pinned headers at risk');
  });

  it('follows an overscroll to the LEFT, where scrollLeft cannot', () => {
    build(50);
    const head = scrollTo(-30);
    // Clamped to the range, as the browser would.
    assert.equal(head.scrollLeft, 0);
    // The body has visually moved 30px to the right; so must this.
    assert.equal(head.style.transform, 'translateX(30px)');
  });

  it('follows an overscroll past the RIGHT end', () => {
    build(50);
    const head = scrollTo(200 + 45);
    assert.equal(head.scrollLeft, 200, 'the in-range part still scrolls');
    assert.equal(head.style.transform, 'translateX(-45px)');
  });

  it('clears the transform when the bounce settles', () => {
    build(50);
    scrollTo(-30);
    const head = scrollTo(80);
    assert.equal(head.style.transform, '');
    assert.equal(head.scrollLeft, 80);
  });
});
