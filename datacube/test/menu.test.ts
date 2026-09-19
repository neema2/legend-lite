import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
  applyMenuAction,
  buildMenu,
  menuItems,
  type MenuActionId,
} from '../src/ui/menu.ts';
import type { CubeSnapshot } from '../src/snapshot.ts';

const CUBE: CubeSnapshot = {
  source: { expression: 't' },
  columns: [
    { name: 'region', type: 'String' },
    { name: 'desk', type: 'String' },
    { name: 'year', type: 'Integer' },
    { name: 'notional', type: 'Float' },
  ],
  derived: [],
  rows: ['region'],
  pivotOn: ['year'],
  measures: [{ name: 'total', column: 'notional', fn: 'sum' }],
  sorts: [],
  epoch: 1,
};

const ids = (ctx: Parameters<typeof buildMenu>[0]): MenuActionId[] =>
  menuItems(buildMenu(ctx)).map((i) => i.id);

describe('buildMenu', () => {
  it('names the target in every column-specific label', () => {
    // A context menu is read after the right-click, when which column
    // was clicked is already fading.
    const items = menuItems(buildMenu({ snapshot: CUBE, column: 'desk' }));
    const pivot = items.find((i) => i.id === 'pivot.vertical');
    assert.equal(pivot?.label, 'Vertical Pivot on desk');
  });

  it('opens the filter editor UNSCOPED, as DataCube does', () => {
    // Their entry is plain "Filters...". There is no per-column
    // filter dialog, because the filter is one tree over the whole
    // cube rather than a set of per-column widgets to reconcile.
    const items = menuItems(buildMenu({ snapshot: CUBE, column: 'desk' }));
    assert.equal(
      items.find((i) => i.id === 'filter.column')?.label,
      'Filters...',
    );
    // Still offered with no column under the pointer.
    assert.ok(
      menuItems(buildMenu({ snapshot: CUBE })).some(
        (i) => i.id === 'filter.column',
      ),
    );
  });

  it('will not offer to pivot BY a pivoted measure', () => {
    // "Vertical Pivot on 2021 / notional" is an action with no
    // meaning. The caller says whether a column can be grouped.
    const pivoted = '2021__|__notional';
    const ids2 = ids({ snapshot: CUBE, column: pivoted, canGroup: false });
    assert.equal(
      ids2.some((i) => i.startsWith('pivot.')),
      false,
      ids2.join(', '),
    );
  });

  it('still offers LAYOUT actions on a column it cannot group by', () => {
    // These sat inside the pivot block, so gating that block took
    // hide, pin and resize away with it -- caught by a screenshot,
    // not by a test, which is why this one exists.
    const ids2 = ids({
      snapshot: CUBE,
      column: '2021__|__notional',
      canGroup: false,
    });
    for (const id of [
      'column.hide',
      'column.pinLeft',
      'column.autoSize',
    ] as const) {
      assert.ok(ids2.includes(id), `${id} missing from ${ids2.join(', ')}`);
    }
  });

  it('reads a pivoted column name back readably', () => {
    const items = menuItems(
      buildMenu({ snapshot: CUBE, column: '2021__|__notional' }),
    );
    assert.equal(
      items.find((i) => i.id === 'column.hide')?.label,
      'Hide 2021 / notional',
    );
  });

  it('omits an action that cannot apply rather than disabling it', () => {
    // 'desk' is not a vertical pivot, so "Remove Vertical Pivot on
    // desk" would be a lie.
    const forDesk = ids({ snapshot: CUBE, column: 'desk' });
    assert.equal(forDesk.includes('pivot.removeVertical'), false);
    assert.equal(forDesk.includes('pivot.vertical'), true);

    // 'region' IS one, so the pair flips.
    const forRegion = ids({ snapshot: CUBE, column: 'region' });
    assert.equal(forRegion.includes('pivot.removeVertical'), true);
    assert.equal(forRegion.includes('pivot.vertical'), false);
  });

  it('offers Add only when there is something to add to', () => {
    const noRows = ids({ snapshot: { ...CUBE, rows: [] }, column: 'desk' });
    assert.equal(noRows.includes('pivot.addVertical'), false);
    assert.equal(
      ids({ snapshot: CUBE, column: 'desk' }).includes('pivot.addVertical'),
      true,
    );
  });

  it('offers Clear Sort only for a column that is sorted', () => {
    const unsorted = ids({ snapshot: CUBE, column: 'desk' });
    assert.equal(unsorted.includes('sort.clearColumn'), false);

    const sorted = ids({
      snapshot: { ...CUBE, sorts: [{ column: 'desk', direction: 'asc' }] },
      column: 'desk',
    });
    assert.equal(sorted.includes('sort.clearColumn'), true);
    assert.equal(sorted.includes('sort.addAsc'), true, 'and Add becomes real');
  });

  it('offers Clear All Filters only when a filter exists', () => {
    assert.equal(ids({ snapshot: CUBE }).includes('filter.clearAll'), false);
    assert.equal(
      ids({
        snapshot: {
          ...CUBE,
          filter: { kind: 'condition', column: 'region', operator: 'isEmpty' },
        },
      }).includes('filter.clearAll'),
      true,
    );
  });

  it('gates copy on a selection and collapse on an open group', () => {
    const bare = ids({ snapshot: CUBE });
    assert.equal(bare.includes('copy.selection'), false);
    assert.equal(bare.includes('tree.collapseAll'), false);

    const rich = ids({ snapshot: CUBE, hasSelection: true, hasExpanded: true });
    assert.equal(rich.includes('copy.selection'), true);
    assert.equal(rich.includes('tree.collapseAll'), true);
  });

  it('drops a group with nothing in it', () => {
    // A right-click on empty space must not open a menu of headings.
    const groups = buildMenu({ snapshot: CUBE });
    assert.equal(groups.some((g) => g.items.length === 0), false);
    assert.equal(groups.some((g) => g.label === 'Sort'), false);
    assert.equal(groups.some((g) => g.label === 'Export'), true);
  });
});

describe('applyMenuAction', () => {
  const run = (id: MenuActionId, column?: string, direction?: 'asc' | 'desc') =>
    applyMenuAction(CUBE, {
      id,
      label: '',
      ...(column ? { column } : {}),
      ...(direction ? { direction } : {}),
    });

  it('replaces the sort, or adds to it', () => {
    // Two different intentions, so two actions rather than one that
    // guesses which was meant.
    const replaced = applyMenuAction(
      { ...CUBE, sorts: [{ column: 'region', direction: 'asc' }] },
      { id: 'sort.desc', label: '', column: 'desk', direction: 'desc' },
    );
    assert.deepEqual(replaced.sorts, [{ column: 'desk', direction: 'desc' }]);

    const added = applyMenuAction(
      { ...CUBE, sorts: [{ column: 'region', direction: 'asc' }] },
      { id: 'sort.addDesc', label: '', column: 'desk', direction: 'desc' },
    );
    assert.deepEqual(added.sorts, [
      { column: 'region', direction: 'asc' },
      { column: 'desk', direction: 'desc' },
    ]);
  });

  it('re-adding a sorted column moves it rather than duplicating', () => {
    const s = applyMenuAction(
      { ...CUBE, sorts: [{ column: 'desk', direction: 'asc' }] },
      { id: 'sort.addDesc', label: '', column: 'desk', direction: 'desc' },
    );
    assert.deepEqual(s.sorts, [{ column: 'desk', direction: 'desc' }]);
  });

  it('pivots vertically and horizontally', () => {
    assert.deepEqual(run('pivot.vertical', 'desk').rows, ['desk']);
    assert.deepEqual(run('pivot.addVertical', 'desk').rows, ['region', 'desk']);
    assert.deepEqual(run('pivot.removeVertical', 'region').rows, []);
    assert.deepEqual(run('pivot.horizontal', 'desk').pivotOn, ['desk']);
    assert.deepEqual(run('pivot.clearHorizontal').pivotOn, []);
  });

  it('does not add a dimension twice', () => {
    assert.deepEqual(run('pivot.addVertical', 'region').rows, ['region']);
  });

  it('removes the filter key entirely rather than setting undefined', () => {
    // An explicit undefined would serialise into a saved view as a
    // null filter, which reads as "a filter that matches nothing".
    const filtered: CubeSnapshot = {
      ...CUBE,
      filter: { kind: 'condition', column: 'region', operator: 'isEmpty' },
    };
    const cleared = applyMenuAction(filtered, {
      id: 'filter.clearAll',
      label: '',
    });
    assert.equal('filter' in cleared, false);
  });

  it('leaves the query alone for layout, copy and export', () => {
    for (const id of [
      'column.hide',
      'column.pinLeft',
      'copy.selection',
      'export.csv',
    ] as MenuActionId[]) {
      assert.equal(run(id, 'desk'), CUBE, `${id} must not touch the query`);
    }
  });
});
