import assert from 'node:assert/strict';
import { afterEach, beforeEach, describe, it } from 'node:test';
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

  it('makes EVERY row draggable, measures included', () => {
    // A measure has nowhere to land in the zones -- they refuse it,
    // and grouping by a notional means one group per amount -- but
    // it has somewhere to land in the GRID, which is what
    // `allowDragFromColumnsToolPanel` is for upstream. Refusing the
    // drag outright left a measure in this panel with nothing at all
    // it could do.
    assert.deepEqual(
      rows().filter((r) => r.draggable).map((r) => r.dataset['column']),
      ['region', 'desk', 'year', 'notional'],
    );
  });

  it('marks which columns are already in use, and how', () => {
    const badges = [...root.querySelectorAll('.dc-tool-panel-badge')].map(
      (b) => b.textContent,
    );
    assert.deepEqual(badges, ['Row', 'Col']);
  });

  it('starts a drag that SAYS it came from the panel', () => {
    // The source matters at the far end: a chip dragged out of a
    // zone leaves that zone, a panel row has no zone to leave, and
    // the grid accepts a column it is not showing only from here.
    const region = rows()[0] as HTMLElement;
    region.dispatchEvent(new dom.window.Event('dragstart', { bubbles: true }));
    assert.deepEqual(currentHeaderDrag(), { column: 'region', from: 'panel' });

    const notional = rows()[3] as HTMLElement;
    notional.dispatchEvent(new dom.window.Event('dragstart', { bubbles: true }));
    assert.deepEqual(currentHeaderDrag(),
      { column: 'notional', from: 'panel' });
  });

  it('shows and hides a column with a tick box', () => {
    // The thing upstream's columns tool panel mostly IS: a list of
    // every column with a checkbox each. This panel listed them and
    // could not turn one off, so it was a reference card beside a
    // grid that hid columns from a menu three levels down.
    const toggled: [string, boolean][] = [];
    const host = dom.window.document.createElement('div');
    dom.window.document.body.append(host);
    const withBoxes = new ColumnsToolPanel(host, {
      onVisibility: (c, v) => toggled.push([c, v]),
    });
    withBoxes.setColumns([
      ...COLUMNS.slice(0, 3),
      { name: 'notional', type: 'Float', groupable: false, visible: false },
    ]);
    const boxes: HTMLInputElement[] = [
      ...host.querySelectorAll<HTMLInputElement>('.dc-tool-panel-show'),
    ];
    assert.equal(boxes.length, 4);
    assert.deepEqual(boxes.map((b) => b.checked), [true, true, true, false]);
    // A hidden column stays LISTED -- the list is how you find it
    // again -- and says it is hidden.
    const hidden = host.querySelector('.dc-tool-panel-row.dc-hidden-column');
    assert.equal((hidden as HTMLElement | null)?.dataset['column'], 'notional');

    boxes[0]!.checked = false;
    boxes[0]!.dispatchEvent(new dom.window.Event('change'));
    boxes[3]!.checked = true;
    boxes[3]!.dispatchEvent(new dom.window.Event('change'));
    assert.deepEqual(toggled, [['region', false], ['notional', true]]);
  });

  it('offers no tick box when the host wires no visibility', () => {
    // The panel does not pretend to a control the host has not
    // connected: a checkbox that toggles nothing is the dead-button
    // fault in miniature.
    assert.equal(root.querySelectorAll('.dc-tool-panel-show').length, 0);
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

describe('the panel as three sections of one surface', () => {
  let dom: JSDOM;
  let root: HTMLElement;
  let panel: ColumnsToolPanel;
  let reordered: string[][];
  let removed: [string, string][];

  const PIVOTED = [
    { name: 'region', type: 'String', groupable: true },
    {
      name: 'notional',
      type: 'Float',
      groupable: false,
      children: [
        { name: 'A__|__notional', label: 'A', visible: true },
        { name: 'B__|__notional', label: 'B', visible: true },
      ],
    },
  ];

  beforeEach(() => {
    dom = new JSDOM('<!doctype html><body><div id="r"></div></body>');
    root = dom.window.document.getElementById('r') as HTMLElement;
    reordered = [];
    removed = [];
    panel = new ColumnsToolPanel(root, {
      onReorder: (order) => reordered.push([...order]),
      onRemoveFromZone: (zone, column) => removed.push([zone, column]),
      onVisibility: () => {},
    });
    panel.setColumns([
      { name: 'region', type: 'String', groupable: true },
      { name: 'desk', type: 'String', groupable: true },
      { name: 'notional', type: 'Float', groupable: false },
    ]);
    setHeaderDrag(null);
  });

  afterEach(() => setHeaderDrag(null));

  const row = (name: string): HTMLElement =>
    root.querySelector(`.dc-tool-panel-row[data-column="${name}"]`) as
      HTMLElement;

  /** Drop `from` on `to`, in whichever half. */
  const dropOn = (from: string, to: string, half: 'top' | 'bottom'): void => {
    const target = row(to);
    Object.defineProperty(target, 'getBoundingClientRect', {
      value: () => ({ top: 100, height: 20, bottom: 120, left: 0,
        width: 100, right: 100, x: 0, y: 100, toJSON: () => ({}) }),
      configurable: true,
    });
    setHeaderDrag({ column: from, from: 'panel' });
    for (const type of ['dragover', 'drop']) {
      target.dispatchEvent(new dom.window.MouseEvent(type, {
        bubbles: true, cancelable: true,
        clientY: half === 'top' ? 105 : 115,
      }));
    }
  };

  it('keeps a stable slot for the zones', () => {
    // The panel replaces its children on every render, and the zones
    // mounted in that slot own theirs -- so the slot has to be the
    // same element each time or whatever is in it is destroyed.
    const first = panel.zones;
    panel.setColumns([{ name: 'region', type: 'String', groupable: true }]);
    assert.equal(panel.zones, first);
    assert.equal(root.contains(panel.zones), true);
  });

  it('reorders by dragging one row onto another', () => {
    // Dragging a HEADER reorders the columns that are on screen;
    // this reorders the list, which is where a person looks for a
    // column -- and is the only way to place one the grid is not
    // showing.
    dropOn('notional', 'region', 'top');
    assert.deepEqual(reordered.at(-1), ['notional', 'region', 'desk']);
    dropOn('region', 'notional', 'bottom');
    assert.deepEqual(reordered.at(-1), ['desk', 'notional', 'region']);
  });

  it('takes a column OFF an axis when a chip is dropped on the list', () => {
    // The counterpart to dropping one into a zone: the two zones and
    // this list are three sections of one surface, and a drag
    // between them is how the cube is configured.
    const list = root.querySelector('.dc-tool-panel-list') as HTMLElement;
    setHeaderDrag({ column: 'region', from: 'rows' });
    list.dispatchEvent(new dom.window.MouseEvent('drop', {
      bubbles: true, cancelable: true,
    }));
    assert.deepEqual(removed, [['rows', 'region']]);
  });

  it('ignores a drop that came from the list itself', () => {
    // Otherwise a reorder would also be read as a removal: both
    // handlers are listening on the same drop.
    const list = root.querySelector('.dc-tool-panel-list') as HTMLElement;
    setHeaderDrag({ column: 'region', from: 'panel' });
    list.dispatchEvent(new dom.window.MouseEvent('drop', {
      bubbles: true, cancelable: true,
    }));
    assert.deepEqual(removed, []);
  });

  it('keeps one left gutter for every row, children or not', () => {
    // The three sections are meant to read as one list, and a row
    // whose measure a pivot has spread carries a disclosure the
    // others do not -- so without an empty slot in its place, every
    // other label sits 12px to the left of it.
    panel.setColumns(PIVOTED);
    const rows = [...root.querySelectorAll('.dc-tool-panel-row')]
      .filter((r) => !r.classList.contains('dc-tool-panel-child'));
    for (const row of rows) {
      const slots = row.querySelectorAll('.dc-tool-panel-twist');
      assert.equal(slots.length, 1,
        `${(row as HTMLElement).dataset['column']} has ${slots.length}`
        + ' slots where the disclosure goes');
    }
    // The one with children has a real control; the other an inert
    // spacer.
    const parent = root.querySelector(
      '.dc-tool-panel-row[data-column="notional"] .dc-tool-panel-twist');
    const plain = root.querySelector(
      '.dc-tool-panel-row[data-column="region"] .dc-tool-panel-twist');
    assert.equal(parent?.tagName, 'BUTTON');
    assert.equal(plain?.tagName, 'SPAN');
    assert.equal(plain?.classList.contains('dc-tool-panel-spacer'), true);
  });

  it('FOLDS a pivoted measure that made too many columns', () => {
    // A pivot on one key makes five of a measure; on two it makes
    // twenty, and the list became a wall of `Q3 . 2024`.
    const many = (n: number) => [{
      name: 'notional',
      type: 'Float',
      groupable: false,
      children: Array.from({ length: n }, (_, i) => ({
        name: `v${i}__|__notional`,
        label: `v${i}`,
        visible: true,
      })),
    }];
    panel.setColumns(PIVOTED);
    assert.equal(root.querySelectorAll('.dc-tool-panel-child').length, 2,
      'a small block should be open');

    // BY ITS CURRENT SIZE: pivoting on a second key takes five
    // columns to twenty, and a measure remembered from when it was
    // small stayed unfolded at twenty.
    panel.setColumns(many(20));
    assert.equal(root.querySelectorAll('.dc-tool-panel-child').length, 0);
    const twist = root.querySelector('.dc-tool-panel-twist') as
      HTMLButtonElement;
    assert.notEqual(twist, null);
    twist.click();
    assert.equal(root.querySelectorAll('.dc-tool-panel-child').length, 20);

    // And the choice sticks: the next render must not fold it again.
    panel.setColumns(many(20));
    assert.equal(root.querySelectorAll('.dc-tool-panel-child').length, 20);
  });
});
