// The grid's everyday interactions: a header click sorts (and says so),
// Shift-click selects columns, an edge drag resizes, a cell and
// a header explain themselves on hover, and an empty or loading grid
// says which it is.

import assert from 'node:assert/strict';
import { beforeEach, describe, it } from 'node:test';
import { JSDOM } from 'jsdom';

import { buildColumnModel } from '../src/grid/columns.ts';
import { DataGrid, valueTitle } from '../src/grid/grid.ts';
import { FormatterCache } from '../src/format.ts';
import type { ResultTable } from '../../engine-client/src/result.ts';

let dom: JSDOM;
let container: HTMLElement;

const TABLE: ResultTable = {
  columns: [
    { name: 'region', type: 'String', values: ['EMEA', ''] },
    { name: 'notional', type: 'Float', values: [12.5, null] },
    { name: 'settled', type: 'Boolean', values: [true, false] },
  ],
  rowCount: 2,
  epoch: 1,
  elapsedMs: 0,
};

beforeEach(() => {
  dom = new JSDOM('<!doctype html><div id="g"></div>');
  container = dom.window.document.getElementById('g') as unknown as HTMLElement;
  globalThis.requestAnimationFrame = ((cb: FrameRequestCallback) => {
    cb(0);
    return 1;
  }) as typeof requestAnimationFrame;
  globalThis.cancelAnimationFrame = () => {};
  Object.defineProperty(dom.window.HTMLElement.prototype, 'clientHeight', {
    configurable: true, get: () => 200,
  });
});

function grid(options: ConstructorParameters<typeof DataGrid>[2] = {},
  layout: Parameters<typeof buildColumnModel>[3] = {}): DataGrid {
  const g = new DataGrid(container, new FormatterCache(), { rowHeight: 20, ...options });
  g.setColumns(buildColumnModel(TABLE, [], [], layout));
  g.setRows(TABLE, 0, TABLE.rowCount);
  return g;
}
const header = (name: string): HTMLElement =>
  container.querySelector(`.dc-th[data-column="${name}"]`) as HTMLElement;

const click = (el: HTMLElement, keys: { altKey?: boolean; shiftKey?: boolean; ctrlKey?: boolean; metaKey?: boolean } = {}): void => {
  el.dispatchEvent(new dom.window.MouseEvent('click', { bubbles: true, ...keys }));
};

describe('a header click sorts; Ctrl or Cmd chains it; Shift-click selects (the user, 2026-10-18)', () => {
  it('a plain click sorts by the column alone; Ctrl, or Cmd on a Mac, chains it after the others', () => {
    const clicked: string[] = [];
    const g = grid({ onHeaderSort: (c, chained) => clicked.push(`${c}${chained ? ' chained' : ''}`) });
    click(header('notional'));
    click(header('region'), { ctrlKey: true });
    click(header('settled'), { metaKey: true });
    assert.deepEqual(clicked, ['notional', 'region chained', 'settled chained']);
    assert.equal(g.selection, null, 'a sorting click selects nothing');
  });

  it('a grid that does not sort (Ad Hoc\'s) selects the column on a plain click, as it did', () => {
    const g = grid();
    click(header('notional'));
    assert.deepEqual(g.selection, { anchor: { row: 0, col: 1 }, focus: { row: 1, col: 1 } });
  });

  it('the header marks each sort with an arrow, and its place when there are several', () => {
    const g = grid({ onHeaderSort: () => {} });
    g.setSorts([
      { column: 'region', direction: 'asc' },
      { column: 'notional', direction: 'desc' },
    ]);
    // aria-sort names the first sort alone, as ARIA asks of a table; the marks show the chain
    assert.equal(header('region').getAttribute('aria-sort'), 'ascending');
    assert.equal(header('notional').getAttribute('aria-sort'), 'none');
    assert.equal(header('notional').querySelector('.dc-sort-mark')?.textContent, '↓2');
    assert.equal(header('region').querySelector('.dc-sort-mark')?.textContent, '↑1');
    assert.equal(header('settled').getAttribute('aria-sort'), 'none');
    assert.equal(header('settled').querySelector('.dc-sort-mark'), null);
  });

  it('a lone sort shows no position number', () => {
    const g = grid({ onHeaderSort: () => {} });
    g.setSorts([{ column: 'region', direction: 'asc' }]);
    assert.equal(header('region').querySelector('.dc-sort-mark')?.textContent, '↑');
  });

  it('Shift-click selects every row of the column, and never sorts', () => {
    const clicked: string[] = [];
    const g = grid({ onHeaderSort: (c) => clicked.push(c) });
    click(header('notional'), { shiftKey: true });
    assert.deepEqual(clicked, []);
    assert.deepEqual(g.selection, { anchor: { row: 0, col: 1 }, focus: { row: 1, col: 1 } });
    assert.equal(header('notional').classList.contains('dc-th-selected'), true);
    assert.equal(header('region').classList.contains('dc-th-selected'), false);
  });

  it('a second Shift-click selects every column from the first to it, either way', () => {
    const g = grid();
    click(header('region'), { shiftKey: true });
    click(header('settled'), { shiftKey: true });
    assert.deepEqual(g.selection, { anchor: { row: 0, col: 0 }, focus: { row: 1, col: 2 } });
    // back to the middle: the range shrinks to it, from the first column still
    click(header('notional'), { shiftKey: true });
    assert.deepEqual(g.selection, { anchor: { row: 0, col: 0 }, focus: { row: 1, col: 1 } });
  });
});

describe('a header edge resizes', () => {
  it('every column has a grip but a FIXED one', () => {
    grid({ onResizeColumn: () => {} }, { fixed: ['settled'] });
    assert.ok(header('notional').querySelector('.dc-col-resize'));
    assert.equal(header('settled').querySelector('.dc-col-resize'), null);
  });

  it('a click on the grip neither sorts nor selects', () => {
    const clicked: string[] = [];
    const g = grid({ onHeaderSort: (c) => clicked.push(c), onResizeColumn: () => {} });
    click(header('notional').querySelector('.dc-col-resize') as HTMLElement, { shiftKey: true });
    click(header('notional').querySelector('.dc-col-resize') as HTMLElement);
    assert.deepEqual(clicked, []);
    assert.equal(g.selection, null);
  });
});

describe('tooltips', () => {
  it('a cell names its RAW value, and a blank says it is missing', () => {
    assert.equal(valueTitle(12.5), 'Value = 12.5');
    assert.equal(valueTitle(''), "Value = ''");
    assert.equal(valueTitle(true), 'Value = TRUE');
    assert.equal(valueTitle(null), 'Missing Value');
    grid();
    const cells = [...container.querySelectorAll('.dc-cell[data-column="notional"]')]
      .map((c) => (c as HTMLElement).title);
    assert.deepEqual(cells, ['Value = 12.5', 'Missing Value']);
  });

  it('a header takes the title the host composes', () => {
    grid({ headerTitle: (path) => `Column = ${path.join('/')}` });
    assert.equal(header('region').title, 'Column = region');
  });
});

// (its headers stay centred: grid.css's rule, which no DOM without the stylesheet reads -- not claimed here)
describe('a cell\'s alignment when none is set (the user, 2026-10-25): figures right, the rest left', () => {
  const justify = (name: string): string =>
    (container.querySelector(`.dc-cell[data-column="${name}"]`) as HTMLElement).style.getPropertyValue('justify-content');

  it('a measure\'s numbers sit right; a text and a boolean left', () => {
    grid();
    assert.deepEqual(['region', 'notional', 'settled'].map(justify), ['flex-start', 'flex-end', 'flex-start']);
  });
  it('a number the cube calls a dimension sits left, as a label', () => {
    grid({ isMeasure: (leaf) => leaf.name !== 'notional' });
    assert.equal(justify('notional'), 'flex-start');
  });
  it('an alignment set wins, the grid\'s or the column\'s', () => {
    grid({ appearance: { textAlign: 'center' }, columnAppearance: { region: { textAlign: 'right' } } });
    assert.deepEqual(['region', 'notional'].map(justify), ['flex-end', 'center']);
  });
});

describe('the overlays', () => {
  it('an empty result says "0 rows"', () => {
    const g = new DataGrid(container, new FormatterCache(), {});
    g.setColumns(buildColumnModel(TABLE));
    g.setRows({ ...TABLE, rowCount: 0,
      columns: TABLE.columns.map((c) => ({ ...c, values: [] })) }, 0, 0);
    const o = container.querySelector('.dc-grid-overlay') as HTMLElement;
    assert.equal(o.hidden, false);
    assert.equal(o.textContent, '0 rows');
  });

  it('"Loading..." waits a moment, so a fast answer does not flicker', (t) => {
    // the test's own clock (Bazel workplan P3-16): the delay is advanced, not slept through
    t.mock.timers.enable({ apis: ['setTimeout'] });
    const g = grid();
    const o = container.querySelector('.dc-grid-overlay') as HTMLElement;
    g.setBusy(true);
    assert.equal(o.hidden, true);
    t.mock.timers.tick(300);
    assert.equal(o.hidden, false);
    assert.equal(o.textContent, 'Loading...');
    g.setBusy(false);
    assert.equal(o.hidden, true);
  });
});
