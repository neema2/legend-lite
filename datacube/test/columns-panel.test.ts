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

  const toggle = (): HTMLButtonElement =>
    root.querySelector('.dc-tool-panel-toggle') as HTMLButtonElement;

  const click = (el: HTMLElement): void => {
    el.dispatchEvent(new dom.window.MouseEvent('click', { bubbles: true }));
  };

  it('collapses to a rail and comes back', () => {
    assert.equal(panel.collapsed, false);
    click(toggle());
    assert.equal(panel.collapsed, true);
    click(toggle());
    assert.equal(panel.collapsed, false);
    assert.equal(rows().length, 4, 'the list came back with it');
  });

  it('collapsed, the panel is the rail and NOTHING else', () => {
    // Not hidden -- removed. A `display: none` subtree still holds
    // focusable controls that tab order walks through, so a hidden
    // panel is one a keyboard user can land inside while seeing
    // nothing. Counting rows is what catches that; checking a class
    // would not.
    click(toggle());
    assert.equal(rows().length, 0);
    assert.equal(root.querySelector('.dc-tool-panel-search'), null);
    assert.equal(root.querySelectorAll('button').length, 1);
    assert.ok(toggle().classList.contains('dc-tool-panel-rail'));
    assert.match(toggle().textContent ?? '', /Columns/,
      'the rail must say what it opens');
  });

  it('the rail is what marks the container collapsed', () => {
    // The width lives in CSS on this class, so if it is not on the
    // root the panel still occupies its 200px while showing nothing.
    click(toggle());
    assert.ok(root.classList.contains('dc-collapsed'));
    click(toggle());
    assert.ok(!root.classList.contains('dc-collapsed'));
  });

  it('says which state it is in, and keeps the focus', () => {
    assert.equal(toggle().getAttribute('aria-expanded'), 'true');
    toggle().focus();
    click(toggle());
    assert.equal(toggle().getAttribute('aria-expanded'), 'false');
    // The re-render replaces the button, so without a deliberate
    // refocus the keyboard user is dropped back to the document.
    assert.equal(dom.window.document.activeElement, toggle());
  });

  it('a collapse survives new columns arriving', () => {
    // Every snapshot calls setColumns, which re-renders. If the flag
    // did not live on the instance the panel would spring open on
    // the next query.
    click(toggle());
    panel.setColumns([...COLUMNS].slice(0, 2));
    assert.equal(panel.collapsed, true);
    assert.equal(rows().length, 0);
  });

  it('can start collapsed', () => {
    const p2 = new ColumnsToolPanel(root, { collapsed: true });
    p2.setColumns(COLUMNS);
    assert.equal(p2.collapsed, true);
    assert.equal(rows().length, 0);
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
