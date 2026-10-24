import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
  buildMenu,
  createItems,
  menuGesture,
  menuItems,
  valueLabel,
  type MenuActionId,
  type MenuItem,
} from '../src/ui/menu.ts';
import { applyGridCommands, type GridCommand } from '../src/grid-commands.ts';
import { gridGestureGiven } from '../src/grid-gestures.ts';
import { pivotTotalColumn, type CubeSnapshot } from '../src/snapshot.ts';
import { DEFAULT_CONFIGURATION } from '../src/config.ts';
import { element } from '../../pure-protocol/src/index.ts';

/** A menu entry made as the grid makes it (app.ts #onMenuAction): its gesture (menuGesture, grid-gestures.ts gridGesture,
 *  the one Python's grid.gesture asks), its commands applied -- the very same grid when it makes none. */
const menuCommands = (s: CubeSnapshot, item: MenuItem): GridCommand[] | null => {
  const gesture = menuGesture(item);
  return gesture === null ? null
    : gridGestureGiven(gesture.name, { snapshot: s, config: DEFAULT_CONFIGURATION, dimensions: [], view: null }, gesture.args);
};
const applyMenuAction = (s: CubeSnapshot, item: MenuItem): CubeSnapshot => {
  const commands = menuCommands(s, item);
  return commands === null || commands.length === 0 ? s
    : applyGridCommands({ snapshot: s, config: DEFAULT_CONFIGURATION }, commands).snapshot;
};

const CUBE: CubeSnapshot = {
  source: { query: element('t') },
  columns: [
    { name: 'region', type: 'String' },
    { name: 'desk', type: 'String' },
    { name: 'year', type: 'Integer' },
    { name: 'notional', type: 'Float' },
  ],
  derived: [],
  rows: ['region'],
  pivotOn: ['year'],
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
      // The clears and the header flip are about the cube, not about
      // pivoting BY this column.
      enabled(ctx).some((i) => i.startsWith('pivot.') && i !== 'pivot.clearVertical'
        && i !== 'pivot.clearHorizontal' && i !== 'pivot.measuresFirst'),
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
      ['Export', 'Email', 'Edit', 'Sort', 'Filter', 'Pivot',
        'Extended Columns', 'Resize', 'Pin', 'Heatmap', 'Create'],
    );
    // A submenu parent does nothing itself.
    assert.ok(withSub.every((i) => i.id === undefined));
  });

  it('ends with the switch for the controls, the order before it untouched', () => {
    // The right-click order stays as people know it (the user); the one switch for the controls
    // -- hidden for the grid alone, and the only way back (the user, 2026-10-01: "right click
    // only") -- comes after everything, Properties... included.
    const top = (hidden: boolean): (string | undefined)[] =>
      buildMenu({ snapshot: CUBE, column: 'desk', ...(hidden ? { controlsHidden: true } : {}) })
        .flatMap((g) => g.items).map((i) => i.label);
    const shown = top(false);
    assert.deepEqual(shown.slice(-2), ['Properties...', 'Hide Controls']);
    assert.deepEqual(top(true).slice(-2), ['Properties...', 'Show Controls']);
  });

  // the user, 2026-10-23: the grid's menu button gone, its Move to Sheet and Remove from Page "nested with Hide Controls"
  it('on a page, ends with Tile ▸: the controls, Move to Sheet, Remove from Page', () => {
    const tile = (ctx: Partial<Parameters<typeof buildMenu>[0]>) => {
      const items = buildMenu({ snapshot: CUBE, column: 'desk', ...ctx }).flatMap((g) => g.items);
      return items[items.length - 1]!;
    };
    const sheets: MenuItem[] = [{ id: 'sheet.to.s2', label: 'Sheet 2' }, { id: 'sheet.toNew', label: 'New Sheet', separated: true }];
    const both = tile({ sheets, canRemove: true });
    assert.equal(both.label, 'Tile');
    assert.deepEqual(both.submenu?.map((i) => `${i.separated ? '| ' : ''}${i.label}${i.disabled ? ' [off]' : ''}`),
      ['Hide Controls', 'Move to Sheet', '| Remove from Page']);
    assert.equal(both.submenu?.[1]?.submenu, sheets, 'Move to Sheet: the other sheets, then a new one');
    assert.equal(both.submenu?.[2]?.id, 'grid.remove');
    assert.deepEqual(tile({ sheets: 'locked', controlsHidden: true }).submenu?.map((i) => `${i.label}${i.disabled ? ' [off]' : ''}`),
      ['Show Controls', 'Move to Sheet [off]'], 'locked: offered, disabled, nothing under it');
    assert.deepEqual(tile({ canRemove: true }).submenu?.map((i) => i.label), ['Hide Controls', 'Remove from Page']);
    assert.equal(tile({}).id, 'view.controls', 'off a page: the switch alone, no Tile ▸ of one entry');
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
      items.some((i) => i.label === "Add Filter: region = 'EMEA'"),
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

  it('offers a JSON document what the compiler takes: equality, no order, no text', () => {
    const labels = menuItems(
      buildMenu({
        snapshot: CUBE,
        column: 'payload',
        columnType: 'Variant',
        value: { json: '{"a":1}' },
      }),
    ).filter((i) => i.id === 'filter.add').map((i) => i.label);
    assert.deepEqual(labels,
      ['Add Filter: payload = {"a":1}', 'Add Filter: payload != {"a":1}']);
  });

  it('filters a BOOLEAN by equality: no ordering against a value', () => {
    // a Boolean column -- a source flag or a calculated one -- gets a value filter, = and !=
    // as upstream DataCube; legend-lite would order false before true, legend-engine refuses
    // it over a BIT column (ENGINE DEFECT S23, offers.ts)
    const labels = menuItems(
      buildMenu({
        snapshot: CUBE,
        column: 'flag',
        columnType: 'Boolean',
        value: true,
      }),
    ).filter((i) => i.id === 'filter.add').map((i) => i.label);
    assert.deepEqual(labels, [
      'Add Filter: flag = true', 'Add Filter: flag != true',
    ]);
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

describe('a menu entry made as its gesture', () => {
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

  it('leaves the query alone for layout, copy and export: presentation a configure, the rest the screen\'s own', () => {
    for (const id of ['column.hide', 'column.pinLeft'] as MenuActionId[]) {
      assert.deepEqual(menuCommands(CUBE, { id, label: '', column: 'desk' })?.map((c) => c.kind), ['configure'], id);
    }
    for (const id of ['copy.selection', 'export.csv'] as MenuActionId[]) {
      assert.equal(menuCommands(CUBE, { id, label: '', column: 'desk' }), null, `${id} makes no command`);
    }
  });
});

describe('menuGesture: a query entry is its grid gesture (grid-gestures.ts), the one Python\'s grid.gesture asks', () => {
  const command = (id: MenuActionId, column?: string, s: CubeSnapshot = CUBE) =>
    menuCommands(s, { id, label: '', ...(column ? { column } : {}) });

  it('makes the edit as Python would send it', () => {
    assert.deepEqual(menuCommands(CUBE, { id: 'sort.addDesc', label: '', column: 'desk', direction: 'desc' }),
      [{ kind: 'addSort', column: 'desk', direction: 'desc' }]);
    assert.deepEqual(command('pivot.addVertical', 'desk'), [{ kind: 'group', rows: ['region', 'desk'] }]);
    assert.deepEqual(command('pivot.horizontal', 'desk'), [{ kind: 'pivot', columns: ['desk'] }]);
    assert.deepEqual(command('filter.clearAll'), [{ kind: 'filter', filter: null }]);
    assert.deepEqual(menuCommands(CUBE, { id: 'filter.add', label: '', column: 'desk', operator: 'equal', value: 'FX' }),
      [{ kind: 'addFilter', condition: { kind: 'condition', column: 'desk', operator: 'equal', value: 'FX' } }]);
    // the entry's gesture and what it carries of what the gesture takes: its column, not its direction (the name's)
    assert.deepEqual(menuGesture({ id: 'sort.desc', label: '', column: 'desk', direction: 'desc' }),
      { name: 'sort.desc', args: { column: 'desk' } });
  });

  it('makes none for an entry off the query, or one that would change nothing; refuses one with no column', () => {
    for (const id of ['copy.selection', 'export.csv'] as MenuActionId[]) {
      assert.equal(command(id, 'desk'), null, id);
    }
    assert.deepEqual(command('pivot.addVertical', 'region'), [], 'a row group the grid has');
    assert.throws(() => command('sort.asc'), /'sort.asc' needs column/, 'no column under it');
  });
});

describe("upstream's remaining entries", () => {
  const labels = (ctx: Parameters<typeof buildMenu>[0]) =>
    menuItems(buildMenu(ctx)).map((i) => `${i.label}${i.disabled ? ' [off]' : ''}`);

  it('Edit > Copy Selected Rows, disabled with no selection', () => {
    assert.ok(labels({ snapshot: CUBE, column: 'desk' }).includes('Copy Selected Rows [off]'));
    assert.ok(labels({ snapshot: CUBE, column: 'desk', hasSelection: true })
      .includes('Copy Selected Rows'));
  });

  // the user, 2026-10-23: Copy ▸ became "Edit with undo/redo and copy" -- the grid's own history, its menu button gone
  it('Edit ▸: Undo and Redo (disabled with nothing to undo or redo), then the copies', () => {
    const edit = (ctx: Partial<Parameters<typeof buildMenu>[0]>) => (buildMenu({ snapshot: CUBE, column: 'desk', ...ctx })
      .flatMap((g) => g.items).find((i) => i.label === 'Edit')?.submenu ?? [])
      .map((i) => `${i.separated ? '| ' : ''}${i.id}:${i.label}${i.disabled ? ' [off]' : ''}`);
    assert.deepEqual(edit({ hasSelection: true }), [
      'view.undo:Undo [off]', 'view.redo:Redo [off]', '| copy.selection:Copy Selected Cells', 'copy.rows:Copy Selected Rows',
      'copy.column:Copy Column desk',
    ]);
    assert.deepEqual(edit({ canUndo: true, canRedo: true }).slice(0, 2), ['view.undo:Undo', 'view.redo:Redo']);
  });

  it('Resize > Minimize, All, and Size Grid to Fit; Minimize spares a fixed width', () => {
    const l = labels({ snapshot: CUBE, column: 'desk' });
    for (const want of ['Minimize Column', 'Minimize All Columns', 'Size Grid to Fit Screen']) {
      assert.ok(l.includes(want), want);
    }
    assert.ok(labels({ snapshot: CUBE, column: 'desk', fixedWidth: true })
      .includes('Minimize Column [off]'));
  });

  it('Pivot > Exclude from a pivoted column, Include from an excluded measure', () => {
    const onPivoted = labels({ snapshot: CUBE, column: '2021__|__notional',
      canGroup: false, pivotBase: 'notional', isMeasure: true });
    assert.ok(onPivoted.includes('Exclude Column notional from Horizontal Pivot'));
    const onExcluded = labels({ snapshot: CUBE, column: 'notional',
      isMeasure: true, excludedFromPivot: true });
    assert.ok(onExcluded.includes('Include Column notional in Horizontal Pivot'));
    // Neither without an active pivot.
    const flat = labels({ snapshot: { ...CUBE, pivotOn: [] }, column: 'notional',
      isMeasure: true, excludedFromPivot: true });
    assert.ok(!flat.some((x) => /Horizontal Pivot$/.test(x) && /Include|Exclude/.test(x)));
  });
});

describe('valueLabel', () => {
  it('names a date as its text, and TODAY/NOW by name', () => {
    assert.equal(valueLabel({ relative: 'today' }), 'TODAY');
    // Text quoted, as upstream's labels are; numbers and booleans bare.
    assert.equal(valueLabel('EMEA'), "'EMEA'");
    assert.equal(valueLabel(12), '12');
    assert.equal(valueLabel(true), 'true');
  });
});

describe('a pivot Total column', () => {
  // A total is a column of the level's query like any other, so it sorts; filters apply to source rows, before any
  // total exists, so it offers none. The context is the app's for a total: its measure the pivot's base, no dimension.
  const total = pivotTotalColumn('notional');
  const ctx = { snapshot: { ...CUBE, pivotTotal: { placement: 'right' as const } }, column: total, pivotBase: 'notional', canGroup: false, isMeasure: true };

  it('offers Sort on the total', () => {
    const on = enabled(ctx);
    for (const id of ['sort.asc', 'sort.desc', 'sort.addAsc', 'sort.addDesc'] as const) {
      assert.ok(on.includes(id), `${id}: ${on.join(' ')}`);
    }
    const next = applyMenuAction(ctx.snapshot, menuItems(buildMenu(ctx)).find((i) => i.id === 'sort.desc')!);
    assert.deepEqual(next.sorts, [{ column: total, direction: 'desc' }]);
  });

  it('offers no filter on it, and nothing about it as a dimension', () => {
    const items = menuItems(buildMenu(ctx));
    assert.ok(!items.some((i) => i.id === 'filter.add'));
    assert.ok(!enabled(ctx).includes('pivot.vertical'));
  });
});

describe('the Pin and Sort entries report state, as upstream', () => {
  const find = (groups: ReturnType<typeof buildMenu>, label: string) =>
    menuItems(groups).find((i) => i.label === label);

  it('checks, and disables, the placement that holds; Unpin needs a pin', () => {
    const left = buildMenu({ snapshot: CUBE, column: 'region', pinned: 'left' });
    assert.equal(find(left, 'Pin Left')?.checked, true);
    assert.equal(find(left, 'Pin Left')?.disabled, true);
    assert.equal(find(left, 'Pin Right')?.checked, false);
    assert.equal(find(left, 'Pin Right')?.disabled, false);
    const none = buildMenu({ snapshot: CUBE, column: 'region' });
    assert.equal(find(none, 'Unpin')?.disabled, true);
  });

  it('a pivot result column cannot be pinned, as upstream', () => {
    const groups = buildMenu({ snapshot: CUBE, column: '2021__|__notional', pivotBase: 'notional' });
    assert.equal(find(groups, 'Pin Left')?.disabled, true);
    assert.equal(find(groups, 'Pin Right')?.disabled, true);
  });

  it('offers the measure-first header flip, checked when on, only with a column pivot', () => {
    const pivoted = { ...CUBE, pivotOn: ['region'] };
    assert.equal(find(buildMenu({ snapshot: pivoted, measuresFirst: true }),
      'Measures First in Column Headers')?.checked, true);
    assert.equal(find(buildMenu({ snapshot: pivoted }),
      'Measures First in Column Headers')?.checked, false);
    assert.equal(find(buildMenu({ snapshot: { ...CUBE, pivotOn: [] } }),
      'Measures First in Column Headers')?.disabled, true);
  });

  it('disables an Add that would change nothing', () => {
    const sorted = { ...CUBE, sorts: [{ column: 'region', direction: 'asc' as const }] };
    const groups = buildMenu({ snapshot: sorted, column: 'region' });
    assert.equal(find(groups, 'Add Ascending')?.disabled, true);
    assert.equal(find(groups, 'Add Descending')?.disabled, false);
  });
});

describe('a JSON column in the menu', () => {
  const withJson: CubeSnapshot = {
    ...CUBE,
    columns: [...CUBE.columns, { name: 'payload', type: 'Variant' }],
  };

  it('is extended through Add Column, not a menu of its own', () => {
    // The app opens the column editor on its fields for calc.extend.
    const got = ids({ snapshot: withJson, column: 'payload', extendable: true });
    assert.ok(got.includes('calc.extend'));
    assert.ok(!got.some((id) => String(id).startsWith('json.')));
  });
});

describe('the chart\'s presets in the grid\'s Extended Columns (docs/DATACUBE_CHARTS_DESIGN_2026_10_09.md §3.2)', () => {
  const DATED: CubeSnapshot = {
    ...CUBE,
    columns: [...CUBE.columns, { name: 'trade_date', type: 'StrictDate' }],
    pivotOn: [],
  };
  const items = (ctx: Parameters<typeof buildMenu>[0]) => menuItems(buildMenu(ctx));

  it('a source date by its period, a source number by its bins', () => {
    const dated = items({ snapshot: DATED, column: 'trade_date', extendable: true });
    assert.deepEqual(dated.filter((i) => i.id === 'calc.grain').map((i) => i.grain), ['day', 'week', 'month', 'quarter', 'year']);
    assert.ok(!dated.some((i) => i.id === 'calc.bins'));
    const number = items({ snapshot: DATED, column: 'notional', extendable: true });
    assert.ok(number.some((i) => i.id === 'calc.bins' && i.label === 'Bins of notional...'));
    assert.ok(!number.some((i) => i.id === 'calc.grain'));
    // a column a row-stage expression cannot see (a pivot's, a group-stage one) is not extendable
    assert.ok(!items({ snapshot: DATED, column: 'trade_date' }).some((i) => i.id === 'calc.grain'));
  });

  it('a measure of grouped rows shown as something computed from it; not on a pivot', () => {
    const shown = items({ snapshot: DATED, column: 'notional', isMeasure: true });
    assert.deepEqual(shown.filter((i) => i.id === 'calc.showAs').map((i) => i.showAs),
      ['runningTotal', 'percentOfTotal', 'difference', 'percentChange', 'movingAverage', 'rank']);
    assert.ok(!items({ snapshot: CUBE, column: 'notional', isMeasure: true }).some((i) => i.id === 'calc.showAs'));
    assert.ok(!items({ snapshot: { ...DATED, rows: [] }, column: 'notional', isMeasure: true }).some((i) => i.id === 'calc.showAs'));
  });
});

// the user, 2026-10-23: "Insert" became "Create", one word in every menu; a Join ▸ beside Filter
describe('Create ▸ and Join ▸', () => {
  const labels = (items: readonly MenuItem[]) => items.map((i) => `${i.separated ? '| ' : ''}${i.label}`);

  it('Create ▸ is the same everywhere: a data source, a copy of this grid, a chart; the host\'s a line apart', () => {
    const host: MenuItem[] = [{ id: 'host.createClass', label: 'Class\u2026' }, { id: 'host.createDataSpace', label: 'Data Space\u2026' }];
    assert.deepEqual(labels(createItems({ source: true, host })), ['Data Source\u2026', 'Copy of This Grid', 'Visualization', '| Class\u2026', 'Data Space\u2026']);
    assert.deepEqual(labels(createItems({ source: false, host: [] })), ['Copy of This Grid', 'Visualization']);
    assert.ok(createItems({ source: true, host, disabled: true }).slice(0, 3).every((i) => i.disabled), 'the grid\'s own off in Ad Hoc');
    assert.ok(createItems({ source: true, host, disabled: true }).slice(3).every((i) => !i.disabled), 'the host\'s its own to say');
    const create = buildMenu({ snapshot: CUBE, column: 'desk', canOpenSource: true, create: host }).flatMap((g) => g.items).find((i) => i.label === 'Create');
    assert.deepEqual(labels(create?.submenu ?? []), labels(createItems({ source: true, host })), 'the right-click\'s, the same');
  });

  it("Join ▸ is the host's, beside Filter, only when it gives entries", () => {
    const join: MenuItem[] = [{ id: 'host.join.with.0', label: 'With customers on desk' }];
    const top = (ctx: Parameters<typeof buildMenu>[0]) => buildMenu(ctx).flatMap((g) => g.items).map((i) => i.label);
    const withJoin = top({ snapshot: CUBE, column: 'desk', join });
    assert.equal(withJoin.indexOf('Join'), withJoin.indexOf('Filter') + 1);
    assert.equal(buildMenu({ snapshot: CUBE, column: 'desk', join }).flatMap((g) => g.items).find((i) => i.label === 'Join')?.submenu, join);
    assert.ok(!top({ snapshot: CUBE, column: 'desk' }).includes('Join'));
    assert.ok(!top({ snapshot: CUBE, column: 'desk', join: [] }).includes('Join'));
  });
});
