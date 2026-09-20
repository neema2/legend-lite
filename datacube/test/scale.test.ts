// Too much data, and far too many columns.
//
// The row cap (DEFAULT_MAX_ROWS) bounds what a level can return, so
// the grid's worst case is a full cap of rows against a cube wide
// enough to be silly. The failure that matters at that size is not
// slowness, it is a grid that puts every row in the DOM: a thousand
// rows times two hundred columns is two hundred thousand cells, and
// building them is the difference between a grid that scrolls and a
// tab that stops responding.
//
// So the assertion is VIRTUALISATION, not speed -- speed on a
// contended machine means nothing, while "did it render the rows it
// cannot show" is a fact that holds on any machine.

import assert from 'node:assert/strict';
import { beforeEach, describe, it } from 'node:test';
import { JSDOM } from 'jsdom';

import { buildColumnModel } from '../src/grid/columns.ts';
import { DataGrid } from '../src/grid/grid.ts';
import { FormatterCache } from '../src/format.ts';
import type { ResultTable } from '../src/result.ts';

const ROW_HEIGHT = 20;
const VIEW_HEIGHT = 400;
const MEASURES = ['alpha', 'beta', 'gamma', 'delta'];

let dom: JSDOM;
let container: HTMLElement;

/** `values` pivot values x four measures, plus a dimension column. */
function wideTable(rows: number, values: number): ResultTable {
  const dim: (string | null)[] = [];
  for (let i = 0; i < rows; i++) dim.push(`R${i}`);
  const columns: ResultTable['columns'][number][] = [
    { name: 'region', type: 'String', values: dim },
  ];
  for (let v = 0; v < values; v++) {
    for (const m of MEASURES) {
      const col: (number | null)[] = [];
      for (let i = 0; i < rows; i++) col.push(i % 11 === 0 ? null : i * (v + 1));
      columns.push({ name: `v${v}__|__${m}`, type: 'Float', values: col });
    }
  }
  return { columns, rowCount: rows, epoch: 1, elapsedMs: 0 };
}

function stubLayout(el: Element, height: number): void {
  Object.defineProperty(el, 'clientHeight', { value: height, configurable: true });
}

beforeEach(() => {
  dom = new JSDOM('<!doctype html><div id="g"></div>');
  const g = dom.window.document.getElementById('g');
  assert.ok(g);
  container = g as unknown as HTMLElement;
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

function build(rows: number, values: number) {
  const table = wideTable(rows, values);
  const grid = new DataGrid(container, new FormatterCache(), {
    rowHeight: ROW_HEIGHT,
    overscan: 2,
  });
  const scroller = container.querySelector('.dc-scroller');
  assert.ok(scroller);
  stubLayout(scroller, VIEW_HEIGHT);
  grid.setColumns(buildColumnModel(table, ['region'], MEASURES, {}, 1));
  grid.setRows(table, 0, rows);
  return { table, grid, scroller: scroller as HTMLElement };
}

describe('a full cap of rows against a very wide cube', () => {
  it('renders only the rows the viewport can show', () => {
    // 1000 rows x 201 columns. A grid that materialised all of them
    // would put 201,000 cells in the DOM.
    const { scroller } = build(1000, 50);
    const rendered = container.querySelectorAll('.dc-row').length;
    const visible = Math.ceil(VIEW_HEIGHT / ROW_HEIGHT);
    assert.ok(
      rendered <= visible + 8,
      `rendered ${rendered} rows for a ${visible}-row viewport`,
    );
    assert.ok(rendered > 0, 'but it did render something');
    assert.ok(scroller.scrollHeight >= 0);
  });

  it('builds the header for 200 pivot columns without losing any', () => {
    build(100, 50);
    // The header is RAGGED: the region dimension is emitted once in
    // the top row with a rowSpan covering both levels, so the bottom
    // row carries only the 200 measure leaves rather than 201 cells.
    // Expecting 201 here is the classic misreading of a ragged
    // header, and it is what this assertion originally got wrong.
    const bottom = container.querySelectorAll(
      '.dc-head-row:last-child [role="columnheader"]',
    );
    assert.equal(bottom.length, 50 * MEASURES.length);

    const top = container.querySelectorAll(
      '.dc-head-row:first-child [role="columnheader"]',
    );
    assert.equal(top.length, 1 + 50, 'the dimension plus one cell per value');
  });

  it('groups each pivot value over exactly its four measures', () => {
    build(100, 50);
    const top = container.querySelectorAll('.dc-head-row')[0];
    const cells = [...(top?.querySelectorAll('[role="columnheader"]') ?? [])];
    // The dimension spans the header height; each value spans 4.
    const spans = cells
      .map((c) => Number(c.getAttribute('aria-colspan') ?? '1'))
      .filter((n) => n > 1);
    assert.equal(spans.length, 50, 'one cell per pivot value');
    assert.ok(spans.every((s) => s === MEASURES.length), `spans: ${[...new Set(spans)]}`);
  });

  it('still renders a window after scrolling to the bottom', () => {
    const { scroller } = build(1000, 20);
    scroller.scrollTop = 1000 * ROW_HEIGHT - VIEW_HEIGHT;
    // The grid re-renders from its own scroll listener; there is no
    // imperative refresh to call.
    scroller.dispatchEvent(new dom.window.Event('scroll'));
    const rendered = container.querySelectorAll('.dc-row').length;
    assert.ok(rendered > 0, 'the bottom of a long grid still has rows');
    assert.ok(
      rendered <= Math.ceil(VIEW_HEIGHT / ROW_HEIGHT) + 8,
      `still windowed at the bottom: ${rendered}`,
    );
  });

  it('handles a cube that is far wider than it is tall', () => {
    // 5 rows, 800 columns -- the shape a heavily pivoted cube takes.
    const { table } = build(5, 200);
    assert.equal(table.columns.length, 801);
    const rendered = container.querySelectorAll('.dc-row').length;
    assert.equal(rendered, 5, 'every row fits, so every row renders');
  });
});
