import assert from 'node:assert/strict';
import { afterEach, beforeEach, describe, it } from 'node:test';
import { JSDOM } from 'jsdom';

import {
  PivotPanel,
  makeHeaderDraggable,
  placeColumn,
  setHeaderDrag,
  type Zone,
} from '../src/ui/pivot-panel.ts';

describe('placeColumn', () => {
  it('appends a new column', () => {
    assert.deepEqual(placeColumn(['a'], 'b', 1), ['a', 'b']);
  });

  it('reorders, accounting for the chip leaving its old place', () => {
    // The index was measured against the list as DISPLAYED, so
    // moving right by one is otherwise a no-op.
    assert.deepEqual(placeColumn(['a', 'b', 'c'], 'a', 2), ['b', 'a', 'c']);
    assert.deepEqual(placeColumn(['a', 'b', 'c'], 'c', 0), ['c', 'a', 'b']);
  });

  it('removes on a null index', () => {
    assert.deepEqual(placeColumn(['a', 'b'], 'a', null), ['b']);
  });

  it('returns the SAME array when nothing moved', () => {
    // A drop that lands where the chip already was must not re-run
    // the query.
    const list = ['a', 'b'];
    assert.equal(placeColumn(list, 'a', 0), list);
    assert.equal(placeColumn(list, 'c', null), list);
  });

  it('clamps past the end', () => {
    assert.deepEqual(placeColumn(['a'], 'b', 99), ['a', 'b']);
  });
});

describe('the drag zones', () => {
  let dom: JSDOM;
  let root: HTMLElement;
  let changes: [Zone, string[]][];
  let panel: PivotPanel;

  const DIMENSIONS = new Set(['region', 'desk', 'year']);

  beforeEach(() => {
    dom = new JSDOM('<!doctype html><body><div id="r"></div></body>');
    root = dom.window.document.getElementById('r') as HTMLElement;
    changes = [];
    panel = new PivotPanel(root, {
      onChange: (zone, cols) => changes.push([zone, [...cols]]),
      canGroup: (c) => DIMENSIONS.has(c),
      showColumnZone: true,
    });
  });

  afterEach(() => setHeaderDrag(null));

  const zone = (z: Zone): HTMLElement =>
    root.querySelector(`.dc-zone-${z}`) as HTMLElement;
  const chips = (z: Zone): string[] =>
    [...zone(z).querySelectorAll('.dc-chip')].map(
      (c) => (c as HTMLElement).dataset['column'] as string,
    );
  const drop = (z: Zone): void => {
    zone(z).dispatchEvent(new dom.window.Event('drop', { bubbles: true }));
  };

  it("prompts with ag-Grid's own words when empty", () => {
    assert.equal(
      zone('rows').querySelector('.dc-zone-prompt')?.textContent,
      'Drag here to set row groups',
    );
  });

  it('accepts a dimension dragged from a header', () => {
    setHeaderDrag({ column: 'region' });
    drop('rows');
    assert.deepEqual(changes.at(-1), ['rows', ['region']]);
    assert.deepEqual(chips('rows'), ['region']);
  });

  it('REFUSES a measure', () => {
    // Grouping by a notional yields one group per distinct amount.
    setHeaderDrag({ column: 'notional' });
    drop('rows');
    assert.equal(changes.length, 0);
  });

  it('shows the order as a hierarchy, not a set', () => {
    panel.setColumns(['region', 'desk'], []);
    assert.equal(zone('rows').querySelectorAll('.dc-zone-arrow').length, 1);
  });

  it('a chip dragged to the other zone LEAVES the first', () => {
    // Otherwise the same dimension groups rows and labels columns at
    // once, which is a cube with the dimension on both axes.
    panel.setColumns(['region'], []);
    setHeaderDrag({ column: 'region', from: 'rows' });
    drop('columns');
    assert.deepEqual(chips('rows'), []);
    assert.deepEqual(chips('columns'), ['region']);
    assert.deepEqual(changes.at(-2), ['rows', []]);
    assert.deepEqual(changes.at(-1), ['columns', ['region']]);
  });

  it('removes a chip with its button', () => {
    panel.setColumns(['region', 'desk'], []);
    (
      zone('rows').querySelector('.dc-chip-remove') as HTMLButtonElement
    ).click();
    assert.deepEqual(changes.at(-1), ['rows', ['desk']]);
  });

  it('removes a chip from the KEYBOARD', () => {
    // A zone a mouse can fill must be emptiable without one.
    panel.setColumns(['region'], []);
    (zone('rows').querySelector('.dc-chip') as HTMLElement).dispatchEvent(
      new dom.window.KeyboardEvent('keydown', {
        key: 'Delete',
        bubbles: true,
      }),
    );
    assert.deepEqual(changes.at(-1), ['rows', []]);
  });

  it('hides the column zone by default, matching DataCube', () => {
    const other = dom.window.document.createElement('div');
    new PivotPanel(other, { onChange: () => {}, canGroup: () => true });
    assert.equal(other.querySelectorAll('.dc-zone').length, 1);
  });
});

describe('makeHeaderDraggable', () => {
  let dom: JSDOM;

  beforeEach(() => {
    dom = new JSDOM('<!doctype html><body></body>');
    setHeaderDrag(null);
  });

  it('leaves a measure header alone rather than refusing its drop', () => {
    // A drag that can never land is worse than no drag handle.
    const el = dom.window.document.createElement('div');
    makeHeaderDraggable(el, 'notional', false);
    assert.equal(el.draggable, false);
    assert.equal(el.classList.contains('dc-draggable'), false);
  });

  it('marks a dimension header draggable', () => {
    const el = dom.window.document.createElement('div');
    makeHeaderDraggable(el, 'region', true);
    assert.equal(el.draggable, true);
    assert.equal(el.classList.contains('dc-draggable'), true);
  });
});

describe('the same zones, down a list', () => {
  let dom: JSDOM;
  let root: HTMLElement;
  let changes: [Zone, string[]][];

  beforeEach(() => {
    dom = new JSDOM('<!doctype html><body><div id="r"></div></body>');
    root = dom.window.document.getElementById('r') as HTMLElement;
    changes = [];
    const panel = new PivotPanel(root, {
      onChange: (zone, cols) => changes.push([zone, [...cols]]),
      // A MEASURE IS NOT GROUPABLE, as in the product: grouping by a
      // notional means one group per amount. A fixture that says
      // everything can be grouped cannot test the refusal.
      canGroup: (c) => c !== 'notional',
      showColumnZone: true,
      orientation: 'list',
    });
    panel.setColumns(['region', 'desk'], []);
  });

  afterEach(() => setHeaderDrag(null));

  it('says which layout it is in, so the CSS can lay it out', () => {
    assert.equal(root.classList.contains('dc-pivot-panel-list'), true);
  });

  it('leaves the other axis, even for a drag from the PANEL', () => {
    // A column on both axes groups the rows and labels the columns
    // in the same query. A chip dragged between zones always left
    // the first; a column dragged out of the COLUMNS PANEL left
    // nothing, because the rule asked where the drag came from
    // rather than where the column already was -- and with "keep
    // grouped columns in the grid" on, a row dimension is listed in
    // that panel too.
    const panel2 = new PivotPanel(root, {
      onChange: (zone, cols) => changes.push([zone, [...cols]]),
      canGroup: () => true,
      showColumnZone: true,
      orientation: 'list',
    });
    panel2.setColumns(['region', 'desk'], ['year']);
    setHeaderDrag({ column: 'region', from: 'panel' });
    (root.querySelector('.dc-zone-columns') as HTMLElement)
      .dispatchEvent(new dom.window.MouseEvent('drop', {
        bubbles: true, cancelable: true,
      }));
    assert.deepEqual(changes, [
      ['rows', ['desk']],
      ['columns', ['year', 'region']],
    ]);
  });

  it('renders no chain arrows, because the list IS the order', () => {
    // The bar reads "region > desk > book", and the arrow says the
    // order is a hierarchy rather than a set. A list says that by
    // running downwards, so arrows there are marks a screen reader
    // reads out for nothing.
    assert.equal(root.querySelectorAll('.dc-zone-arrow').length, 0);
    assert.equal(root.querySelectorAll('.dc-chip').length, 2);
  });

  it('MARKS a drag it cannot take, rather than ignoring it', () => {
    // Grouping by a notional means one group per amount, so a
    // measure is refused -- upstream does not offer it either. The
    // refusal was silent, and the measures are the first thing
    // anyone drags: dropping one in a zone did nothing at all, which
    // reads as a product that does not support dragging.
    const zone = root.querySelector('.dc-zone-rows') as HTMLElement;
    setHeaderDrag({ column: 'notional', from: 'panel' });
    zone.dispatchEvent(new dom.window.MouseEvent('dragover', {
      bubbles: true, cancelable: true,
    }));
    assert.equal(zone.classList.contains('dc-refuse'), true);
    assert.equal(zone.classList.contains('dc-drop-target'), false);
    // A dimension over the same zone clears the mark rather than
    // carrying both: `dragleave` is not guaranteed to have arrived,
    // and a zone marked two ways says nothing. Checked BEFORE any
    // drop -- a drop clears both marks, which masked this entirely.
    setHeaderDrag({ column: 'year', from: 'panel' });
    zone.dispatchEvent(new dom.window.MouseEvent('dragover', {
      bubbles: true, cancelable: true,
    }));
    assert.equal(zone.classList.contains('dc-refuse'), false);
    assert.equal(zone.classList.contains('dc-drop-target'), true);

    // And the refused one lands nothing.
    setHeaderDrag({ column: 'notional', from: 'panel' });
    zone.dispatchEvent(new dom.window.MouseEvent('drop', {
      bubbles: true, cancelable: true,
    }));
    assert.deepEqual(changes, []);
  });

  it('finds the drop index DOWN the zone, not across it', () => {
    // A list laid out downwards but measured across gives every drop
    // the same index, so a chip dragged anywhere in a vertical zone
    // landed in the same place. jsdom has no layout, so the chips'
    // boxes are stubbed: which gap the pointer is in is the whole
    // question, and zero-height boxes cannot express it.
    const zone = root.querySelector('.dc-zone-rows') as HTMLElement;
    const chips = [...zone.querySelectorAll('.dc-chip')] as HTMLElement[];
    chips.forEach((chip, i) => {
      Object.defineProperty(chip, 'getBoundingClientRect', {
        value: () => ({
          top: i * 20, height: 20, bottom: i * 20 + 20,
          left: 0, width: 100, right: 100,
          x: 0, y: i * 20, toJSON: () => ({}),
        }),
        configurable: true,
      });
    });
    setHeaderDrag({ column: 'year' });
    zone.dispatchEvent(new dom.window.MouseEvent('drop', {
      bubbles: true, cancelable: true, clientY: 5, clientX: 50,
    }));
    // Dropped in the top half of the FIRST chip: first place.
    assert.deepEqual(changes.at(-1), ['rows', ['year', 'region', 'desk']]);
  });
});
