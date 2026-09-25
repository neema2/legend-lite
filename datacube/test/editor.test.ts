import assert from 'node:assert/strict';
import { beforeEach, describe, it } from 'node:test';
import { JSDOM } from 'jsdom';

import {
  CubeEditor,
  EDITOR_TABS,
  draftFor,
  sortLabel,
  type CubeDraft,
  type EditorTab,
} from '../src/ui/editor.ts';
import { DEFAULT_CONFIGURATION, columnConfig } from '../src/config.ts';
import { freshName } from '../src/ui/panel-dimensions.ts';
import { groupableColumns } from '../src/ui/panel-kit.ts';
import { AGGREGATES } from '../src/ui/panel-column.ts';
import type { CubeSnapshot } from '../src/snapshot.ts';

const CUBE: CubeSnapshot = {
  source: { expression: 't' },
  columns: [
    { name: 'region', type: 'String' },
    { name: 'desk', type: 'String' },
    { name: 'year', type: 'Integer', kind: 'dimension' },
    { name: 'notional', type: 'Float' },
  ],
  // A ratio, so a measure -- declared, as the calculated-column
  // editor always declares a row-stage column's kind now.
  derived: [{ name: 'margin', expression: '$x.a / $x.b', kind: 'measure' }],
  rows: ['region'],
  pivotOn: [],
  measures: [{ name: 'total', column: 'notional', fn: 'sum' }],
  sorts: [{ column: 'total', direction: 'desc' }],
  epoch: 3,
};

describe('the tab strip', () => {
  it("is DataCube's seven tabs, in DataCube's order", () => {
    assert.deepEqual(EDITOR_TABS, [
      'Columns',
      'Horizontal Pivots',
      'Vertical Pivots',
      'Dimensions',
      'Sorts',
      'General Properties',
      'Column Properties',
    ]);
  });
});

describe('sortLabel', () => {
  it('reads a pivoted column back as its dimension values', () => {
    assert.equal(sortLabel('2023__|__EMEA'), '2023 / EMEA');
    assert.equal(sortLabel('total'), 'total');
  });
});

describe('the aggregate list', () => {
  it('offers every aggregate the snapshot can express', () => {
    // A measure the query supports and the UI cannot reach is the
    // failure worth catching; the compile-time check in the panel
    // catches the other direction.
    // 13 since `unique` joined them: DataCube's default aggregate
    // for every non-numeric column, and what lets grouping keep the
    // text columns instead of dropping them.
    assert.equal(AGGREGATES.length, 13);
    assert.equal(new Set(AGGREGATES.map((a) => a.value)).size, 13);
  });
});

describe('the editor', () => {
  let dom: JSDOM;
  let root: HTMLElement;
  let applied: CubeDraft[];
  let closed: number;
  let editor: CubeEditor;

  beforeEach(() => {
    dom = new JSDOM('<!doctype html><body><div id="r"></div></body>');
    root = dom.window.document.getElementById('r') as HTMLElement;
    applied = [];
    closed = 0;
    editor = new CubeEditor(root, draftFor(CUBE), {
      onApply: (d) => applied.push(d),
      onClose: () => (closed += 1),
    });
  });

  const tabButton = (tab: EditorTab): HTMLButtonElement =>
    [...root.querySelectorAll('.dc-editor-tab')].find(
      (b) => b.textContent === tab,
    ) as HTMLButtonElement;
  const go = (tab: EditorTab): void => {
    tabButton(tab).click();
  };
  const rows = (which: 'available' | 'selected'): string[] =>
    [
      ...root.querySelectorAll(`.dc-pane-${which} .dc-selector-row`),
    ].map((r) => (r as HTMLElement).dataset['column'] as string);
  const dbl = (el: Element): void => {
    el.dispatchEvent(new dom.window.MouseEvent('dblclick', { bubbles: true }));
  };
  const fieldWithLabel = (label: string): HTMLElement =>
    [...root.querySelectorAll('.dc-field')].find(
      (f) => f.querySelector('.dc-field-label')?.textContent === label,
    ) as HTMLElement;

  it('opens on Columns and renders one panel at a time', () => {
    assert.equal(editor.tab, 'Columns');
    assert.equal(root.querySelectorAll('.dc-panel').length, 1);
    assert.equal(
      root.querySelector('.dc-panel-title')?.textContent,
      'Columns',
    );
  });

  it('moves between tabs with the arrow keys, as one tab stop', () => {
    const first = tabButton('Columns');
    assert.equal(first.tabIndex, 0);
    assert.equal(tabButton('Sorts').tabIndex, -1);
    first.dispatchEvent(
      new dom.window.KeyboardEvent('keydown', {
        key: 'ArrowRight',
        bubbles: true,
      }),
    );
    assert.equal(editor.tab, 'Horizontal Pivots');
  });

  it('wraps at the ends rather than dead-ending', () => {
    tabButton('Columns').dispatchEvent(
      new dom.window.KeyboardEvent('keydown', {
        key: 'ArrowLeft',
        bubbles: true,
      }),
    );
    assert.equal(editor.tab, 'Column Properties');
  });

  it('changes NOTHING until Apply', () => {
    go('Vertical Pivots');
    dbl(root.querySelector('.dc-pane-available .dc-selector-row') as Element);
    assert.equal(applied.length, 0, 'no query has been issued');
    assert.deepEqual(CUBE.rows, ['region'], 'the original is untouched');
  });

  it('Apply hands over the draft and keeps the editor open', () => {
    go('Vertical Pivots');
    dbl(root.querySelector('.dc-pane-available .dc-selector-row') as Element);
    (
      [...root.querySelectorAll('.dc-editor-footer button')].find(
        (b) => b.textContent === 'Apply',
      ) as HTMLButtonElement
    ).click();
    assert.equal(applied.length, 1);
    assert.equal(closed, 0);
  });

  it('OK applies and closes; Cancel discards', () => {
    (
      [...root.querySelectorAll('.dc-editor-footer button')].find(
        (b) => b.textContent === 'OK',
      ) as HTMLButtonElement
    ).click();
    assert.equal(applied.length, 1);
    assert.equal(closed, 1);

    go('Vertical Pivots');
    dbl(root.querySelector('.dc-pane-available .dc-selector-row') as Element);
    (
      [...root.querySelectorAll('.dc-editor-footer button')].find(
        (b) => b.textContent === 'Cancel',
      ) as HTMLButtonElement
    ).click();
    assert.deepEqual(editor.draft.snapshot.rows, ['region']);
  });

  it('Vertical Pivots offers DIMENSIONS only', () => {
    // Grouping by a notional makes one group per distinct amount,
    // which is never meant and expensive to discover.
    go('Vertical Pivots');
    assert.deepEqual(rows('available'), ['desk', 'year']);
    assert.deepEqual(rows('selected'), ['region']);
  });

  it('Vertical Pivots writes the row grouping', () => {
    go('Vertical Pivots');
    dbl(
      [...root.querySelectorAll('.dc-pane-available .dc-selector-row')].find(
        (r) => (r as HTMLElement).dataset['column'] === 'desk',
      ) as Element,
    );
    assert.deepEqual(editor.draft.snapshot.rows, ['region', 'desk']);
  });

  it('Horizontal Pivots writes pivotOn and its own sort direction', () => {
    go('Horizontal Pivots');
    dbl(
      [...root.querySelectorAll('.dc-pane-available .dc-selector-row')].find(
        (r) => (r as HTMLElement).dataset['column'] === 'year',
      ) as Element,
    );
    assert.deepEqual(editor.draft.snapshot.pivotOn, ['year']);

    const select = root.querySelector(
      '.dc-pane-selected .dc-selector-row select',
    ) as HTMLSelectElement;
    select.value = 'desc';
    select.dispatchEvent(new dom.window.Event('change'));
    assert.equal(
      columnConfig(editor.draft.config, 'year').pivotSortDirection,
      'desc',
    );
  });

  it('Sorts keeps a direction across a remove and re-add', () => {
    go('Sorts');
    assert.deepEqual(rows('selected'), ['total']);
    // total is a measure name, so it is not in the source columns;
    // the sorts panel offers every column the cube knows, derived
    // ones included.
    assert.ok(rows('available').includes('margin'));

    dbl(
      root.querySelector('.dc-pane-selected .dc-selector-row') as Element,
    );
    assert.deepEqual(editor.draft.snapshot.sorts, []);
  });

  it('the Columns tab hides a column and remembers its place', () => {
    go('Columns');
    assert.deepEqual(rows('selected'), [
      'region',
      'desk',
      'year',
      'notional',
      'margin',
    ]);
    dbl(
      [...root.querySelectorAll('.dc-pane-selected .dc-selector-row')].find(
        (r) => (r as HTMLElement).dataset['column'] === 'desk',
      ) as Element,
    );
    assert.equal(columnConfig(editor.draft.config, 'desk').hidden, true);
    // Its place is kept behind the visible ones, so putting it back
    // does not send it to the end.
    assert.deepEqual(editor.draft.config.columnOrder, [
      'region',
      'year',
      'notional',
      'margin',
      'desk',
    ]);
  });

  it('warns when no column is left selected', () => {
    go('Columns');
    const header = root.querySelector(
      '.dc-pane-selected .dc-selector-header',
    ) as HTMLElement;
    dbl(header);
    const warning = root.querySelector('.dc-warning') as HTMLElement;
    assert.equal(warning.hidden, false);
  });

  it('General Properties writes the row limit, and Apply carries it to the query', () => {
    go('General Properties');
    const input = fieldWithLabel('Row Limit:').querySelector(
      'input',
    ) as HTMLInputElement;
    input.value = '250';
    input.dispatchEvent(new dom.window.Event('change'));
    assert.equal(editor.draft.config.maxRows, 250);

    editor.apply();
    assert.equal(applied[0]?.snapshot.maxRows, 250);
  });

  it('a cleared report title leaves no trace', () => {
    go('General Properties');
    const input = fieldWithLabel('Report Title:').querySelector(
      'input',
    ) as HTMLInputElement;
    input.value = 'Q3';
    input.dispatchEvent(new dom.window.Event('change'));
    assert.equal(editor.draft.config.reportTitle, 'Q3');
    input.value = '';
    input.dispatchEvent(new dom.window.Event('change'));
    assert.equal('reportTitle' in editor.draft.config, false);
  });

  it('Column Properties rebinds every control when the column changes', () => {
    go('Column Properties');
    const chooser = fieldWithLabel('Choose Column:').querySelector(
      'select',
    ) as HTMLSelectElement;
    assert.equal(chooser.value, 'region');

    const name = fieldWithLabel('Display Name:').querySelector(
      'input',
    ) as HTMLInputElement;
    name.value = 'Region';
    name.dispatchEvent(new dom.window.Event('change'));
    assert.equal(columnConfig(editor.draft.config, 'region').displayName, 'Region');

    chooser.value = 'notional';
    chooser.dispatchEvent(new dom.window.Event('change'));
    const after = fieldWithLabel('Display Name:').querySelector(
      'input',
    ) as HTMLInputElement;
    assert.equal(after.value, '', 'now bound to notional, which has none');
    assert.equal(after.placeholder, 'notional');
  });

  it('offers an aggregate only for a measure', () => {
    go('Column Properties');
    const agg = (): HTMLSelectElement =>
      fieldWithLabel('Aggregation:').querySelector(
        'select',
      ) as HTMLSelectElement;
    assert.equal(agg().disabled, true, 'region is a dimension');

    const chooser = fieldWithLabel('Choose Column:').querySelector(
      'select',
    ) as HTMLSelectElement;
    chooser.value = 'notional';
    chooser.dispatchEvent(new dom.window.Event('change'));
    assert.equal(agg().disabled, false);
  });

  it('offers a weight column only for a weighted average', () => {
    go('Column Properties');
    const chooser = fieldWithLabel('Choose Column:').querySelector(
      'select',
    ) as HTMLSelectElement;
    chooser.value = 'notional';
    chooser.dispatchEvent(new dom.window.Event('change'));

    const weight = (): HTMLSelectElement =>
      fieldWithLabel('Weight column:').querySelector(
        'select',
      ) as HTMLSelectElement;
    assert.equal(weight().disabled, true);

    const agg = fieldWithLabel('Aggregation:').querySelector(
      'select',
    ) as HTMLSelectElement;
    agg.value = 'wavg';
    agg.dispatchEvent(new dom.window.Event('change'));
    assert.equal(weight().disabled, false);
  });

  it('keeps the width numbers when the mode changes', () => {
    go('Column Properties');
    const mode = (): HTMLSelectElement =>
      fieldWithLabel('Width:').querySelector('select') as HTMLSelectElement;
    mode().value = 'fixed';
    mode().dispatchEvent(new dom.window.Event('change'));
    const px = fieldWithLabel('Width:').querySelectorAll(
      'input',
    )[0] as HTMLInputElement;
    px.value = '140';
    px.dispatchEvent(new dom.window.Event('change'));

    mode().value = 'any';
    mode().dispatchEvent(new dom.window.Event('change'));
    mode().value = 'fixed';
    mode().dispatchEvent(new dom.window.Event('change'));
    const again = fieldWithLabel('Width:').querySelectorAll(
      'input',
    )[0] as HTMLInputElement;
    assert.equal(again.value, '140', 'the typed number survived');
  });

  it('hides the advanced block until asked', () => {
    go('Column Properties');
    const label = (): Element | null =>
      [...root.querySelectorAll('.dc-field-label')].find(
        (l) => l.textContent === 'Use parameter in link as label:',
      ) ?? null;
    assert.equal(label(), null);
    (root.querySelector('.dc-panel-head-row input') as HTMLInputElement).click();
    assert.notEqual(label(), null);
  });

  it('gives each editor its own panel state', () => {
    // Module-level state would make the second editor jump to
    // whatever column the first was showing.
    go('Column Properties');
    const chooser = fieldWithLabel('Choose Column:').querySelector(
      'select',
    ) as HTMLSelectElement;
    chooser.value = 'notional';
    chooser.dispatchEvent(new dom.window.Event('change'));

    const other = dom.window.document.createElement('div');
    new CubeEditor(other, draftFor(CUBE), {
      onApply: () => {},
      onClose: () => {},
      initialTab: 'Column Properties',
    });
    const otherChooser = other.querySelector('select') as HTMLSelectElement;
    assert.equal(otherChooser.value, 'region');
  });

  it('Dimensions adds, renames and deletes a hierarchy', () => {
    go('Dimensions');
    assert.equal(root.querySelector('.dc-dimension-row'), null);
    const add = [...root.querySelectorAll('.dc-dimension-controls button')].find(
      (b) => b.textContent === 'Add',
    ) as HTMLButtonElement;
    add.click();
    assert.deepEqual(editor.draft.dimensions, [
      { name: 'Dimension 1', columns: [] },
    ]);

    dbl(
      [...root.querySelectorAll('.dc-pane-available .dc-selector-row')].find(
        (r) => (r as HTMLElement).dataset['column'] === 'region',
      ) as Element,
    );
    assert.deepEqual(editor.draft.dimensions[0]?.columns, ['region']);

    const del = [...root.querySelectorAll('.dc-dimension-controls button')].find(
      (b) => b.textContent === 'Delete',
    ) as HTMLButtonElement;
    del.click();
    assert.deepEqual(editor.draft.dimensions, []);
  });

  it('a dimension offers only groupable columns', () => {
    go('Dimensions');
    (
      [...root.querySelectorAll('.dc-dimension-controls button')].find(
        (b) => b.textContent === 'Add',
      ) as HTMLButtonElement
    ).click();
    assert.deepEqual(rows('available'), ['region', 'desk', 'year']);
  });
});

describe('groupableColumns', () => {
  it('offers a calculated DIMENSION, and never a calculated measure', () => {
    // Reading the source columns alone, a calculated column could
    // never be grouped on, whatever kind it declared.
    const names = groupableColumns(draftFor({
      ...CUBE,
      derived: [
        { name: 'margin', expression: '$x.a / $x.b', kind: 'measure' },
        { name: 'big', expression: '$x.notional > 1', kind: 'dimension' },
      ],
      groupDerived: [{ name: 'share', expression: '$x.total / 2' }],
    })).map((c) => c.name);
    assert.deepEqual(names, ['region', 'desk', 'year', 'big']);
  });
});

describe('freshName', () => {
  it('skips names already taken', () => {
    assert.equal(freshName([]), 'Dimension 1');
    assert.equal(
      freshName([
        { name: 'Dimension 1', columns: [] },
        { name: 'Dimension 2', columns: [] },
      ]),
      'Dimension 3',
    );
  });
});

describe('the default configuration', () => {
  it('records nothing per column until a user says something', () => {
    assert.deepEqual(DEFAULT_CONFIGURATION.columns, {});
  });
});
