// The grid's everyday interactions, as upstream's ag-grid gives them:
// a header click sorts (and says so), an edge drag resizes, a cell and
// a header explain themselves on hover, and an empty or loading grid
// says which it is.

import assert from 'node:assert/strict';
import { beforeEach, describe, it } from 'node:test';
import { JSDOM } from 'jsdom';

import { buildColumnModel } from '../src/grid/columns.ts';
import { DataGrid, valueTitle } from '../src/grid/grid.ts';
import { FormatterCache } from '../src/format.ts';
import type { ResultTable } from '../src/result.ts';

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

describe('a header click sorts', () => {
  it('reports the column, and marks the sort with its place', () => {
    const clicked: string[] = [];
    const g = grid({ onHeaderSort: (c) => clicked.push(c) });
    header('notional').click();
    assert.deepEqual(clicked, ['notional']);
    g.setSorts([
      { column: 'region', direction: 'asc' },
      { column: 'notional', direction: 'desc' },
    ]);
    assert.equal(header('notional').getAttribute('aria-sort'), 'descending');
    assert.equal(header('notional').querySelector('.dc-sort-mark')?.textContent,
      '↓2');
    assert.equal(header('settled').getAttribute('aria-sort'), 'none');
  });

  it('a lone sort shows no position number', () => {
    const g = grid({ onHeaderSort: () => {} });
    g.setSorts([{ column: 'region', direction: 'asc' }]);
    assert.equal(header('region').querySelector('.dc-sort-mark')?.textContent,
      '↑');
  });
});

describe('a header edge resizes', () => {
  it('every column has a grip but a FIXED one', () => {
    grid({ onResizeColumn: () => {} }, { fixed: ['settled'] });
    assert.ok(header('notional').querySelector('.dc-col-resize'));
    assert.equal(header('settled').querySelector('.dc-col-resize'), null);
  });

  it('a click on the grip does not sort', () => {
    const clicked: string[] = [];
    grid({ onHeaderSort: (c) => clicked.push(c), onResizeColumn: () => {} });
    (header('notional').querySelector('.dc-col-resize') as HTMLElement).click();
    assert.deepEqual(clicked, []);
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

  it('"Loading..." waits a moment, so a fast answer does not flicker', async () => {
    const g = grid();
    const o = container.querySelector('.dc-grid-overlay') as HTMLElement;
    g.setBusy(true);
    assert.equal(o.hidden, true);
    await new Promise((r) => setTimeout(r, 300));
    assert.equal(o.hidden, false);
    assert.equal(o.textContent, 'Loading...');
    g.setBusy(false);
    assert.equal(o.hidden, true);
  });
});
