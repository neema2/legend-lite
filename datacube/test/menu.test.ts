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
  menuItems(buildMenu(ctx))
    .map((i) => i.id)
    .filter((id): id is MenuActionId => id !== undefined);

/** Ids of the entries a user can actually invoke. */
const enabled = (ctx: Parameters<typeof buildMenu>[0]): MenuActionId[] =>
  menuItems(buildMenu(ctx))
    .filter((i) => !i.disabled)
    .map((i) => i.id)
    .filter((id): id is MenuActionId => id !== undefined);

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

  it('will not let you pivot BY a pivoted measure', () => {
    // "Vertical Pivot on 2021 / notional" is an action with no
    // meaning. Present, as their menu keeps its shape, but dead.
    const ctx = {
      snapshot: CUBE,
      column: '2021__|__notional',
      canGroup: false,
    };
    assert.equal(
      enabled(ctx).some((i) => i.startsWith('pivot.') && i !== 'pivot.clearVertical' && i !== 'pivot.clearHorizontal'),
      false,
      enabled(ctx).join(', '),
    );
  });

  it('NESTS, as theirs does', () => {
    // A flat list of the same entries is a different product:
    // sixteen top-level items scan as a wall where eight verbs with
    // their variants underneath scan as a sentence.
    const top = buildMenu({ snapshot: CUBE, column: 'desk' }).flatMap((g) => [
      ...g.items,
    ]);
    const withSub = top.filter((i) => i.submenu);
    assert.deepEqual(
      withSub.map((i) => i.label),
      ['Export', 'Email', 'Copy', 'Sort', 'Filter', 'Pivot', 'Resize', 'Pin',
        'Heatmap', 'Layout'],
    );
    // A submenu parent does nothing itself.
    assert.ok(withSub.every((i) => i.id === undefined));
  });

  it('disables Email when the host cannot send it', () => {
    // A browser cannot attach a file to a mailto:, so emailing is
    // the host's to provide. Disabled rather than hidden: "this
    // build cannot email" is information, and a menu that silently
    // lacks an entry a colleague's build has is confusing.
    const email = buildMenu({ snapshot: CUBE, column: 'desk' })
      .flatMap((g) => g.items)
      .find((i) => i.label === 'Email');
    assert.ok(email?.submenu);
    assert.ok(
      email.submenu.every((i) => i.disabled === true),
      'every entry disabled with no host handler',
    );
  });

  it('enables Email when the host can send it', () => {
    const email = buildMenu({ snapshot: CUBE, column: 'desk', canEmail: true })
      .flatMap((g) => g.items)
      .find((i) => i.label === 'Email');
    assert.ok(email?.submenu);
    assert.ok(email.submenu.every((i) => i.disabled !== true));
    assert.deepEqual(
      email.submenu.map((i) => i.id),
      ['email.html', 'email.excel', 'email.csv', 'email.text', 'email.pdf'],
    );
  });

  it('offers the clicked VALUE as a one-click filter', () => {
    // The most useful thing in their menu, and the part a
    // labels-only reading misses.
    const items = menuItems(
      buildMenu({
        snapshot: CUBE,
        column: 'region',
        columnType: 'String',
        value: 'EMEA',
      }),
    );
    assert.ok(
      items.some((i) => i.label === 'Add Filter: region = EMEA'),
      items.map((i) => i.label).join(' | '),
    );
  });

  it('puts every OTHER operator one level down, typed to the column', () => {
    const forString = buildMenu({
      snapshot: CUBE,
      column: 'region',
      columnType: 'String',
      value: 'EMEA',
    });
    const more = menuItems(forString).find((i) =>
      i.label.startsWith('More Filters on'),
    );
    assert.notEqual(more, undefined);
    // A string takes the comparisons AND the text predicates; the
    // equality it leads with is not repeated.
    assert.equal(more?.submenu?.length, 11);
    assert.ok(more?.submenu?.some((i) => i.operator === 'startsWith'));

    // A number takes the comparisons only -- "starts with 5" is not
    // a question anyone asks.
    const forNumber = menuItems(
      buildMenu({
        snapshot: CUBE,
        column: 'notional',
        columnType: 'Float',
        value: 5,
      }),
    ).find((i) => i.label.startsWith('More Filters on'));
    assert.equal(forNumber?.submenu?.length, 5);
    assert.equal(
      forNumber?.submenu?.some((i) => i.operator === 'contains'),
      false,
    );
  });

  it('a BLANK cell offers the null predicates instead', () => {
    // "= " against nothing is not a filter anyone means.
    const items = menuItems(
      buildMenu({
        snapshot: CUBE,
        column: 'region',
        columnType: 'String',
        value: null,
      }),
    );
    assert.ok(items.some((i) => i.operator === 'isEmpty'));
    assert.ok(items.some((i) => i.operator === 'isNotEmpty'));
    assert.equal(
      items.some((i) => i.operator === 'equal'),
      false,
    );
  });

  it('offers no value filter from a HEADER, where there is no value', () => {
    const items = menuItems(
      buildMenu({ snapshot: CUBE, column: 'region', columnType: 'String' }),
    );
    assert.equal(
      items.some((i) => i.id === 'filter.add'),
      false,
    );
    // The dialog is still one click away.
    assert.ok(items.some((i) => i.id === 'filter.column'));
  });

  it('offers no value filter on a type with no operators', () => {
    const items = menuItems(
      buildMenu({
        snapshot: CUBE,
        column: 'flag',
        columnType: 'Boolean',
        value: true,
      }),
    );
    assert.equal(
      items.some((i) => i.id === 'filter.add'),
      false,
    );
  });

  it('still offers LAYOUT actions on a column it cannot group by', () => {
    // Hiding or pinning `2021 / notional` is meaningful where
    // grouping by it is not. These once sat inside the pivot block,
    // so gating that block took them away with it.
    const live = enabled({
      snapshot: CUBE,
      column: '2021__|__notional',
      canGroup: false,
    });
    for (const id of [
      'column.hide',
      'column.pinLeft',
      'column.autoSize',
    ] as const) {
      assert.ok(live.includes(id), `${id} missing from ${live.join(', ')}`);
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

  it('DISABLES an action that cannot apply rather than omitting it', () => {
    // This reverses an earlier decision here. DataCube is right: a
    // menu whose entries move depending on context cannot be
    // learned, and the value of a context menu is that the third
    // time you use it you no longer read it.
    const forDesk = ids({ snapshot: CUBE, column: 'desk' });
    assert.equal(forDesk.includes('pivot.removeVertical'), true, 'present');
    assert.equal(
      enabled({ snapshot: CUBE, column: 'desk' }).includes(
        'pivot.removeVertical',
      ),
      false,
      'but not actionable: desk is not a vertical pivot',
    );
    assert.equal(
      enabled({ snapshot: CUBE, column: 'desk' }).includes('pivot.vertical'),
      true,
    );

    // 'region' IS one, so which of the pair is live flips -- while
    // both stay in the same place in the menu.
    const forRegion = enabled({ snapshot: CUBE, column: 'region' });
    assert.equal(forRegion.includes('pivot.removeVertical'), true);
    assert.equal(forRegion.includes('pivot.addVertical'), false);
  });

  it('greys Add Vertical Pivot for a column already pivoted', () => {
    const already = enabled({ snapshot: CUBE, column: 'region' });
    assert.equal(already.includes('pivot.addVertical'), false);
    assert.equal(
      enabled({ snapshot: CUBE, column: 'desk' }).includes(
        'pivot.addVertical',
      ),
      true,
    );
  });

  it('greys Clear Sort for a column that is not sorted', () => {
    assert.equal(
      enabled({ snapshot: CUBE, column: 'desk' }).includes('sort.clearColumn'),
      false,
    );
    const sorted = {
      ...CUBE,
      sorts: [{ column: 'desk', direction: 'asc' as const }],
    };
    assert.equal(
      enabled({ snapshot: sorted, column: 'desk' }).includes(
        'sort.clearColumn',
      ),
      true,
    );
  });

  it('greys Clear All Filters when there is no filter', () => {
    assert.equal(
      enabled({ snapshot: CUBE }).includes('filter.clearAll'),
      false,
    );
    const filtered = {
      ...CUBE,
      filter: {
        kind: 'condition' as const,
        column: 'region',
        operator: 'equal' as const,
        value: 'EMEA',
      },
    };
    assert.equal(
      enabled({ snapshot: filtered }).includes('filter.clearAll'),
      true,
    );
  });

  it('gates copy on a selection and collapse on an open group', () => {
    const bare = enabled({ snapshot: CUBE });
    assert.equal(bare.includes('copy.selection'), false);
    assert.equal(bare.includes('tree.collapseAll'), false);
    const live = enabled({
      snapshot: CUBE,
      hasSelection: true,
      hasExpanded: true,
    });
    assert.equal(live.includes('copy.selection'), true);
    assert.equal(live.includes('tree.collapseAll'), true);
  });

  it('keeps its SHAPE with no column under the pointer', () => {
    // The menu is the same menu wherever it opens; what changes is
    // which entries are live. That is the whole point of disabling
    // rather than omitting.
    const bare = buildMenu({ snapshot: CUBE }).flatMap((g) => [...g.items]);
    const onColumn = buildMenu({ snapshot: CUBE, column: 'desk' }).flatMap(
      (g) => [...g.items],
    );
    // Same entries, same order; only the target names and which
    // ones are live differ.
    assert.deepEqual(
      bare.map((i) => i.id ?? i.label),
      onColumn.map((i) => i.id ?? i.label),
    );
    assert.equal(
      bare.some((g) => g.label === 'Export'),
      true,
    );
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
