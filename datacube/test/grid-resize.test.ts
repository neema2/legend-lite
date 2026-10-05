// A column resize ends cleanly whatever lands mid-drag (Leg B / B3, P2-210): a result arriving
// during the drag rebuilds the header, the grip the pointer was captured by is gone, its pointerup
// never comes -- and the dragged width stuck to the column for good.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { JSDOM } from 'jsdom';

import { buildColumnModel } from '../src/grid/columns.ts';
import { DataGrid } from '../src/grid/grid.ts';
import { FormatterCache } from '../src/format.ts';
import type { ResultTable } from '../../engine-client/src/result.ts';

const TABLE: ResultTable = {
  columns: [
    { name: 'region', type: 'String', values: ['EMEA', 'AMER'] },
    { name: 'total', type: 'Float', values: [1, 2] },
  ],
  rowCount: 2,
  epoch: 1,
  elapsedMs: 0,
};

describe('a resize drag and a result landing mid-drag (P2-210)', () => {
  it('the drag ends with the rebuild: what was dragged is kept, and the column is not stuck', () => {
    const dom = new JSDOM('<!doctype html><div id="g"></div>');
    const g = globalThis as unknown as Record<string, unknown>;
    g['window'] = dom.window;
    g['document'] = dom.window.document;
    g['requestAnimationFrame'] = (cb: FrameRequestCallback) => { cb(0); return 1; };
    const container = dom.window.document.getElementById('g') as unknown as HTMLElement;
    const resized: [string, number][] = [];
    const grid = new DataGrid(container, new FormatterCache(), {
      rowHeight: 20,
      onResizeColumn: (column, width) => resized.push([column, width]),
    });
    const model = (width: number) => buildColumnModel(TABLE, [], ['total'], { widths: { region: width } });
    grid.setColumns(model(100));
    grid.setRows(TABLE, 0, TABLE.rowCount);
    const grip = container.querySelector('.dc-col-resize') as HTMLElement;
    const pointer = (type: string, x: number) => {
      const e = new dom.window.MouseEvent(type, { bubbles: true, cancelable: true, clientX: x, button: 0 });
      Object.defineProperty(e, 'pointerId', { value: 1 });
      grip.dispatchEvent(e);
    };
    pointer('pointerdown', 0);
    pointer('pointermove', 40);
    // a result lands: the header is rebuilt, the old grip is gone
    grid.setColumns(model(100));
    assert.equal(resized.length, 1, 'the drag ended with the rebuild');
    // the settings then say 180: the column shows 180, not a width left over from the drag
    grid.setColumns(model(180));
    const template = (container.querySelector('.dc-head-grid') as HTMLElement | null)?.style.gridTemplateColumns
      ?? (container.querySelector('.dc-row') as HTMLElement).style.gridTemplateColumns;
    assert.match(template, /^180px/, template);
  });
});
