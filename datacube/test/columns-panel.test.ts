import assert from 'node:assert/strict';
import { beforeEach, describe, it } from 'node:test';
import { JSDOM } from 'jsdom';

import { ColumnsToolPanel } from '../src/ui/columns-panel.ts';
import { currentHeaderDrag, setHeaderDrag } from '../src/ui/pivot-panel.ts';

const COLUMNS = [
  { name: 'region', type: 'String', groupable: true, usedAs: 'rows' as const },
  { name: 'desk', type: 'String', groupable: true },
  { name: 'year', type: 'Integer', groupable: true, usedAs: 'columns' as const },
  { name: 'notional', type: 'Float', groupable: false },
];

describe('the columns tool panel', () => {
  let dom: JSDOM;
  let root: HTMLElement;
  let panel: ColumnsToolPanel;
  let picked: string[];

  beforeEach(() => {
    dom = new JSDOM('<!doctype html><body><div id="r"></div></body>');
    root = dom.window.document.getElementById('r') as HTMLElement;
    picked = [];
    panel = new ColumnsToolPanel(root, { onPick: (c) => picked.push(c) });
    panel.setColumns(COLUMNS);
    setHeaderDrag(null);
  });

  const rows = (): HTMLElement[] =>
    [...root.querySelectorAll('.dc-tool-panel-row')] as HTMLElement[];

  it('lists every source column, measures included', () => {
    // Shown rather than hidden, so the panel reads as the model's
    // column list rather than as a mystery subset.
    assert.deepEqual(
      rows().map((r) => r.dataset['column']),
      ['region', 'desk', 'year', 'notional'],
    );
  });

  it('makes dimensions draggable and measures not', () => {
    const draggable = rows().filter((r) => r.draggable);
    assert.deepEqual(
      draggable.map((r) => r.dataset['column']),
      ['region', 'desk', 'year'],
    );
  });

  it('marks which columns are already in use, and how', () => {
    const badges = [...root.querySelectorAll('.dc-tool-panel-badge')].map(
      (b) => b.textContent,
    );
    assert.deepEqual(badges, ['Row', 'Col']);
  });

  it('starts a header drag the zones will accept', () => {
    const region = rows()[0] as HTMLElement;
    region.dispatchEvent(new dom.window.Event('dragstart', { bubbles: true }));
    assert.deepEqual(currentHeaderDrag(), { column: 'region' });
  });

  it('offers a non-drag path to the same thing', () => {
    // A panel that can only be operated by dragging is a panel some
    // people cannot operate.
    (rows()[1] as HTMLElement).dispatchEvent(
      new dom.window.MouseEvent('dblclick', { bubbles: true }),
    );
    assert.deepEqual(picked, ['desk']);

    (rows()[1] as HTMLElement).dispatchEvent(
      new dom.window.KeyboardEvent('keydown', { key: 'Enter', bubbles: true }),
    );
    assert.deepEqual(picked, ['desk', 'desk']);
  });

  it('a measure is not pickable either', () => {
    (rows()[3] as HTMLElement).dispatchEvent(
      new dom.window.MouseEvent('dblclick', { bubbles: true }),
    );
    assert.deepEqual(picked, []);
  });

  it('searches', () => {
    const search = root.querySelector(
      '.dc-tool-panel-search',
    ) as HTMLInputElement;
    search.value = 'es';
    search.dispatchEvent(new dom.window.Event('input'));
    assert.deepEqual(
      rows().map((r) => r.dataset['column']),
      ['desk'],
    );
  });
});
