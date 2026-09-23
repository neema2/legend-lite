import assert from 'node:assert/strict';
import { afterEach, beforeEach, describe, it } from 'node:test';
import { JSDOM } from 'jsdom';

import {
  ColumnsSelector,
  availableOf,
  filterColumns,
  indexForY,
  insertAt,
  setDrag,
  without,
  type SelectorColumn,
} from '../src/ui/columns-selector.ts';

const ALL: SelectorColumn[] = [
  { name: 'region', type: 'String' },
  { name: 'country', type: 'String' },
  { name: 'city', type: 'String' },
  { name: 'desk', type: 'String' },
  { name: 'notional', type: 'Float' },
];

describe('insertAt', () => {
  it('reorders within a list, accounting for the removal', () => {
    // The drop index was measured against the list BEFORE the item
    // was pulled out, so moving down by one has to shift back past
    // itself or it becomes a no-op.
    assert.deepEqual(insertAt(['a', 'b', 'c'], ['a'], 2), ['b', 'a', 'c']);
    assert.deepEqual(insertAt(['a', 'b', 'c'], ['a'], 3), ['b', 'c', 'a']);
    assert.deepEqual(insertAt(['a', 'b', 'c'], ['c'], 0), ['c', 'a', 'b']);
  });

  it('moves a block, keeping its internal order', () => {
    assert.deepEqual(insertAt(['a', 'b', 'c', 'd'], ['a', 'b'], 4), [
      'c',
      'd',
      'a',
      'b',
    ]);
  });

  it('inserts from outside the list', () => {
    assert.deepEqual(insertAt(['a', 'b'], ['x'], 1), ['a', 'x', 'b']);
  });

  it('clamps an index past either end', () => {
    assert.deepEqual(insertAt(['a'], ['x'], 99), ['a', 'x']);
    assert.deepEqual(insertAt(['a'], ['x'], -3), ['x', 'a']);
  });
});

describe('indexForY', () => {
  const rows = [
    { top: 0, height: 20 },
    { top: 20, height: 20 },
    { top: 40, height: 20 },
  ];

  it('picks the gap by the row MIDPOINT', () => {
    assert.equal(indexForY(rows, 5), 0);
    assert.equal(indexForY(rows, 15), 1);
    assert.equal(indexForY(rows, 25), 1);
    assert.equal(indexForY(rows, 35), 2);
  });

  it('makes the position after the last row reachable', () => {
    // Nearest-row would clamp this to 2, and for a pivot list the
    // end is the position most often wanted.
    assert.equal(indexForY(rows, 55), 3);
  });

  it('an empty list is index zero', () => {
    assert.equal(indexForY([], 100), 0);
  });
});

describe('search and derivation', () => {
  it('matches case-insensitively on a substring', () => {
    assert.deepEqual(filterColumns(['region', 'Desk'], 'E'), [
      'region',
      'Desk',
    ]);
    assert.deepEqual(filterColumns(['region', 'desk'], '  '), [
      'region',
      'desk',
    ]);
  });

  it('available is everything unselected, in SOURCE order', () => {
    // Not in the order they were removed: the available pane mirrors
    // the model, and a user looking for a column expects it where it
    // has always been.
    assert.deepEqual(availableOf({ all: ALL, selected: ['city', 'region'] }), [
      'country',
      'desk',
      'notional',
    ]);
  });

  it('without drops every named column', () => {
    assert.deepEqual(without(['a', 'b', 'c'], ['b', 'z']), ['a', 'c']);
  });
});

describe('the widget', () => {
  let dom: JSDOM;
  let root: HTMLElement;
  let changes: string[][];
  let selector: ColumnsSelector;

  beforeEach(() => {
    dom = new JSDOM('<!doctype html><body><div id="r"></div></body>');
    root = dom.window.document.getElementById('r') as HTMLElement;
    changes = [];
    selector = new ColumnsSelector(
      root,
      { all: ALL, selected: ['region'] },
      { onChange: (s) => changes.push([...s]) },
    );
  });

  afterEach(() => setDrag(null));

  const pane = (which: 'available' | 'selected'): HTMLElement =>
    root.querySelector(`.dc-pane-${which}`) as HTMLElement;
  const rows = (which: 'available' | 'selected'): HTMLElement[] =>
    [...pane(which).querySelectorAll('.dc-selector-row')] as HTMLElement[];
  const names = (which: 'available' | 'selected'): string[] =>
    rows(which).map((r) => r.dataset['column'] as string);
  const search = (which: 'available' | 'selected'): HTMLInputElement =>
    pane(which).querySelector('.dc-selector-search-input') as HTMLInputElement;
  const click = (el: Element, init: MouseEventInit = {}): void => {
    el.dispatchEvent(
      new dom.window.MouseEvent('click', { bubbles: true, ...init }),
    );
  };
  const dblclick = (el: Element): void => {
    el.dispatchEvent(new dom.window.MouseEvent('dblclick', { bubbles: true }));
  };

  it('splits the columns into the two panes', () => {
    assert.deepEqual(names('available'), [
      'country',
      'city',
      'desk',
      'notional',
    ]);
    assert.deepEqual(names('selected'), ['region']);
  });

  it('double-click moves one column across', () => {
    dblclick(rows('available')[0] as HTMLElement);
    assert.deepEqual(changes.at(-1), ['region', 'country']);
    assert.deepEqual(names('selected'), ['region', 'country']);
  });

  it('double-clicking the header moves everything the search matches', () => {
    search('available').value = 'c';
    search('available').dispatchEvent(new dom.window.Event('input'));
    assert.deepEqual(names('available'), ['country', 'city']);

    const header = pane('available').querySelector(
      '.dc-selector-header',
    ) as HTMLElement;
    dblclick(header);
    assert.deepEqual(changes.at(-1), ['region', 'country', 'city']);
  });

  it('a move is SCOPED BY THE SEARCH', () => {
    // Highlight three, then filter so only one is visible. Pressing
    // add must move the visible one only -- otherwise a stale
    // highlight moves columns the user cannot see.
    click(rows('available')[0] as HTMLElement);
    click(rows('available')[1] as HTMLElement, { ctrlKey: true });
    click(rows('available')[2] as HTMLElement, { ctrlKey: true });

    search('available').value = 'desk';
    search('available').dispatchEvent(new dom.window.Event('input'));

    const add = root.querySelector('.dc-selector-move') as HTMLButtonElement;
    click(add);
    assert.deepEqual(changes.at(-1), ['region', 'desk']);
  });

  it('shift-click picks a range, ctrl-click toggles one', () => {
    click(rows('available')[0] as HTMLElement);
    click(rows('available')[2] as HTMLElement, { shiftKey: true });
    assert.deepEqual(
      rows('available')
        .filter((r) => r.getAttribute('aria-selected') === 'true')
        .map((r) => r.dataset['column']),
      ['country', 'city', 'desk'],
    );

    click(rows('available')[1] as HTMLElement, { ctrlKey: true });
    assert.equal(
      (rows('available')[1] as HTMLElement).getAttribute('aria-selected'),
      'false',
    );
  });

  it('the move buttons are disabled with nothing highlighted', () => {
    const [add, remove] = [
      ...root.querySelectorAll('.dc-selector-move'),
    ] as HTMLButtonElement[];
    assert.equal(add?.disabled, true);
    assert.equal(remove?.disabled, true);
    click(rows('available')[0] as HTMLElement);
    assert.equal(add?.disabled, false);
  });

  it('shows the match count against the total', () => {
    // A search that hides everything must not read as an empty list.
    const count = pane('available').querySelector(
      '.dc-selector-count',
    ) as HTMLElement;
    assert.equal(count.textContent, '4');
    search('available').value = 'zzz';
    search('available').dispatchEvent(new dom.window.Event('input'));
    assert.equal(count.textContent, '0 of 4');
  });

  it('clears the search on Escape without closing the dialog', () => {
    let escaped = false;
    root.addEventListener('keydown', () => (escaped = true));
    const s = search('available');
    s.value = 'desk';
    s.dispatchEvent(
      new dom.window.KeyboardEvent('keydown', {
        key: 'Escape',
        bubbles: true,
      }),
    );
    assert.equal(s.value, '');
    assert.equal(escaped, false, 'the dialog must not also see it');
  });

  it('drops a dragged column into the selected pane', () => {
    setDrag({ from: 'available', names: ['desk'] });
    const list = pane('selected').querySelector(
      '.dc-selector-list',
    ) as HTMLElement;
    list.dispatchEvent(new dom.window.Event('drop', { bubbles: true }));
    assert.deepEqual(changes.at(-1), ['region', 'desk']);
  });

  it('dragging OUT of selected removes the column', () => {
    selector.setState({ all: ALL, selected: ['region', 'desk'] });
    setDrag({ from: 'selected', names: ['region'] });
    const list = pane('available').querySelector(
      '.dc-selector-list',
    ) as HTMLElement;
    list.dispatchEvent(new dom.window.Event('drop', { bubbles: true }));
    assert.deepEqual(changes.at(-1), ['desk']);
  });

  it('will not reorder the available pane', () => {
    // Its order is the source's, not the user's, so a drag that
    // starts and ends there must change nothing at all.
    const list = pane('available').querySelector(
      '.dc-selector-list',
    ) as HTMLElement;
    const before = changes.length;
    setDrag({ from: 'available', names: ['city'] });
    list.dispatchEvent(new dom.window.Event('drop', { bubbles: true }));
    assert.equal(changes.length, before);
  });

  it('renders a trailing action only in the selected pane', () => {
    const r2 = root.ownerDocument.createElement('div');
    new ColumnsSelector(
      r2,
      { all: ALL, selected: ['region'] },
      {
        onChange: () => {},
        actionFor: (name) => {
          const b = r2.ownerDocument.createElement('button');
          b.className = 'act';
          b.textContent = name;
          return b;
        },
      },
    );
    assert.equal(r2.querySelectorAll('.dc-pane-selected .act').length, 1);
    assert.equal(r2.querySelectorAll('.dc-pane-available .act').length, 0);
  });

  it('forgets a highlight whose column has gone', () => {
    // Otherwise a later "add" resurrects a column that is no longer
    // in the pane at all.
    click(rows('available')[0] as HTMLElement);
    selector.setState({ all: ALL, selected: ['region', 'country'] });
    const add = root.querySelector('.dc-selector-move') as HTMLButtonElement;
    assert.equal(add.disabled, true);
  });
});
