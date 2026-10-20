// The grid's right-click menu, the rest of it (ui/menu.ts): a calculated column's own entries, the entries that cannot
// apply -- disabled, never omitted, each still naming what it would act on -- and what applying an entry does to the
// grid: sorts, filters by the clicked value, pivots, each set by grid-edits.ts, and an entry its rules refuse said in
// their words (docs/DATACUBE_ONE_GRID_EDIT_PATH_2026_10_17.md §2, §3.2, §4).

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
  applyMenuAction,
  buildMenu,
  calcStageOf,
  menuItems,
  valueLabel,
  type MenuContext,
  type MenuItem,
} from '../src/ui/menu.ts';
import { CubeRefusal, pivotTotalColumn, type CubeSnapshot, type FilterNode } from '../src/snapshot.ts';
import { DEFAULT_CONFIGURATION } from '../src/config.ts';
import { element, fn, lambda, property, variable } from '../../pure-protocol/src/index.ts';

// grouped by region, pivoted by year: the menu test's own cube
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

/** A row-stage calculated column (before grouping) and a group-stage one (after). */
const CALCULATED: CubeSnapshot = {
  ...CUBE,
  derived: [{ name: 'big', lambda: lambda(['x'], fn('times', property(variable('x'), 'notional'), property(variable('x'), 'year'))), type: 'Float' }],
  groupDerived: [{ name: 'margin', lambda: lambda(['x'], fn('divide', property(variable('x'), 'notional'), property(variable('x'), 'year'))), type: 'Float' }],
};

/** The cube with a day and a time of day beside its columns. */
const DATED: CubeSnapshot = {
  ...CUBE,
  columns: [...CUBE.columns, { name: 'trade_date', type: 'StrictDate' }, { name: 'cutoff', type: 'StrictTime' }],
};

const items = (ctx: MenuContext): MenuItem[] => menuItems(buildMenu(ctx));
const find = (ctx: MenuContext, label: string): MenuItem | undefined => items(ctx).find((i) => i.label === label);
/** Each entry's label, `[off]` after a disabled one. */
const labels = (list: readonly MenuItem[]): string[] => list.map((i) => `${i.label}${i.disabled ? ' [off]' : ''}`);
/** The submenu a top-level verb opens. */
const submenu = (ctx: MenuContext, verb: string): MenuItem[] =>
  [...(buildMenu(ctx).flatMap((g) => g.items).find((i) => i.label === verb)?.submenu ?? [])];

const apply = (s: CubeSnapshot, item: Omit<MenuItem, 'label'>): CubeSnapshot =>
  applyMenuAction(s, { label: '', ...item }, DEFAULT_CONFIGURATION);

describe('a calculated column\'s own entries (Extended Columns)', () => {
  it('a calculated column says its stage; a source column, or none the cube has, says none', () => {
    assert.deepEqual(calcStageOf(CALCULATED, 'big'), { calcStage: 'row' });
    assert.deepEqual(calcStageOf(CALCULATED, 'margin'), { calcStage: 'group' });
    assert.deepEqual(calcStageOf(CALCULATED, 'notional'), {});
    assert.deepEqual(calcStageOf(CUBE, 'margin'), {}, 'a cube with no group-stage columns');
  });

  it('a row-stage column is extended, binned, edited and deleted from its own header, each entry naming it', () => {
    const ctx: MenuContext = { snapshot: CALCULATED, column: 'big', extendable: true, ...calcStageOf(CALCULATED, 'big') };
    assert.deepEqual(labels(submenu(ctx, 'Extended Columns')),
      ['Add New Column...', 'Extend Column big...', 'Bins of big...', 'Edit Column big...', 'Delete Column big']);
    const del = find(ctx, 'Delete Column big');
    assert.equal(del?.id, 'calc.delete');
    assert.equal(del?.column, 'big');
    assert.equal(find(ctx, 'Edit Column big...')?.id, 'calc.edit');
  });

  it('a group-stage column is edited and deleted, never extended: a row-stage expression cannot see it', () => {
    // the app spreads extendable only over a column that exists before aggregation (app.ts #kindOf)
    const ctx: MenuContext = { snapshot: CALCULATED, column: 'margin', ...calcStageOf(CALCULATED, 'margin') };
    assert.deepEqual(labels(submenu(ctx, 'Extended Columns')),
      ['Add New Column...', 'Edit Column margin...', 'Delete Column margin']);
  });

  it('a source column has no Edit or Delete: it is not the editor\'s to change', () => {
    const ctx: MenuContext = { snapshot: CALCULATED, column: 'notional', extendable: true, ...calcStageOf(CALCULATED, 'notional') };
    const ids = items(ctx).map((i) => i.id);
    assert.ok(ids.includes('calc.extend'));
    assert.ok(!ids.includes('calc.edit') && !ids.includes('calc.delete'), ids.join(' '));
  });

  it('with no column under the pointer, Extended Columns offers a new column alone', () => {
    assert.deepEqual(labels(submenu({ snapshot: CALCULATED }, 'Extended Columns')), ['Add New Column...']);
  });
});

describe('entries that cannot apply are disabled, never omitted, and say what they would act on', () => {
  it('with no column under the pointer, every column entry is there, unnamed and disabled; the grid\'s own stay live', () => {
    const bare = items({ snapshot: CUBE });
    const off = labels(bare).filter((l) => l.endsWith(' [off]'));
    for (const want of [
      'Selected Column as Plain Text', 'Ascending', 'Descending', 'Add Ascending', 'Add Descending',
      'Vertical Pivot', 'Add Vertical Pivot', 'Remove Vertical Pivot', 'Horizontal Pivot', 'Add Horizontal Pivot',
      'Remove Horizontal Pivot', 'Auto-size to Fit Content', 'Minimize Column', 'Pin Left', 'Pin Right', 'Unpin', 'Hide',
      'Add Heatmap', 'Remove Heatmap',
    ]) {
      assert.ok(off.includes(`${want} [off]`), `${want}: ${off.join(' | ')}`);
    }
    // none of them carries a column to act on
    assert.ok(bare.filter((i) => i.disabled).every((i) => i.column === undefined));
    for (const live of ['Auto-size All Columns', 'Minimize All Columns', 'Size Grid to Fit Screen', 'Remove All Pinnings',
      'Filters...', 'Add New Column...', 'Visualization', 'Copy of Grid', 'Properties...']) {
      assert.ok(labels(bare).includes(live), live);
    }
  });

  it('a heatmap is added to a column that has none and removed from one that has', () => {
    assert.deepEqual(labels(submenu({ snapshot: CUBE, column: 'notional' }, 'Heatmap')),
      ['Add Heatmap to notional', 'Remove Heatmap [off]']);
    assert.deepEqual(labels(submenu({ snapshot: CUBE, column: 'notional', hasHeatmap: true }, 'Heatmap')),
      ['Add Heatmap to notional [off]', 'Remove Heatmap']);
  });

  it('the cube itself is exported as a file only when the host knows its source; the grid\'s exports always', () => {
    assert.deepEqual(labels(submenu({ snapshot: CUBE, column: 'desk' }, 'Export')),
      ['HTML', 'Excel (Grid)', 'CSV (Grid)', 'Plain Text', 'PDF', 'Cube File (JSON) [off]']);
    assert.equal(find({ snapshot: CUBE, column: 'desk', canSaveCube: true }, 'Cube File (JSON)')?.disabled, undefined);
  });

  it('the clears are live only when there is something to clear', () => {
    const flat: CubeSnapshot = { ...CUBE, rows: [], pivotOn: [] };
    assert.equal(find({ snapshot: flat }, 'Clear All Vertical Pivots')?.disabled, true);
    assert.equal(find({ snapshot: flat }, 'Clear All Horizontal Pivots')?.disabled, true);
    assert.equal(find({ snapshot: flat }, 'Clear All Sorts')?.disabled, true);
    const busy: CubeSnapshot = { ...CUBE, sorts: [{ column: 'desk', direction: 'asc' }] };
    assert.equal(find({ snapshot: busy }, 'Clear All Vertical Pivots')?.disabled, false);
    assert.equal(find({ snapshot: busy }, 'Clear All Horizontal Pivots')?.disabled, false);
    assert.equal(find({ snapshot: busy }, 'Clear All Sorts')?.disabled, false);
    // Clear Sort is the column's: live on the sorted one, greyed on another
    assert.equal(find({ snapshot: busy, column: 'desk' }, 'Clear Sort')?.disabled, false);
    assert.equal(find({ snapshot: busy, column: 'region' }, 'Clear Sort')?.disabled, true);
  });

  it('a column pinned right is checked there, Pin Left and Unpin live', () => {
    const pin = submenu({ snapshot: CUBE, column: 'desk', pinned: 'right' }, 'Pin');
    assert.deepEqual(pin.map((i) => [i.label, i.checked === true, i.disabled === true]), [
      ['Pin Left', false, false], ['Pin Right', true, true], ['Unpin', false, false], ['Remove All Pinnings', false, false],
    ]);
  });

  it('a pivot total is offered what a pivot\'s cell is: hidden, copied, sized and a heatmap of its own; never pinned', () => {
    // the app's context for a total (2026-10-18): its measure the pivot's base, no dimension to group by
    const total = pivotTotalColumn('notional');
    const ctx: MenuContext = { snapshot: { ...CUBE, pivotTotal: { placement: 'right' } }, column: total, pivotBase: 'notional',
      canGroup: false, isMeasure: true };
    for (const label of ['Hide Total / notional', 'Column Total / notional as Plain Text', 'Add Heatmap to Total / notional',
      'Minimize Column', 'Auto-size to Fit Content']) {
      assert.equal(find(ctx, label)?.disabled, false, label);
      assert.equal(find(ctx, label)?.column, total, label);
    }
    // a pivot's result is not pinned, as upstream's
    for (const label of ['Pin Left', 'Pin Right']) assert.equal(find(ctx, label)?.disabled, true, label);
    // with its own heatmap on (the app's hasHeatmap: heatmapSetting, a total's alone), its menu offers to take it off
    assert.equal(find({ ...ctx, hasHeatmap: true }, 'Remove Heatmap')?.disabled, false);
    // named as its header names it: Properties' Pivot Statistic Column Name, when renamed
    assert.equal(find({ ...ctx, totalLabel: 'All years' }, 'Hide All years / notional')?.column, total);
    // nothing about it as a calculated column (the app finds no stage for it: calcStageOf), nor as a dimension
    assert.deepEqual(calcStageOf(ctx.snapshot, total), {});
    assert.deepEqual(labels(submenu(ctx, 'Extended Columns')), ['Add New Column...']);
    assert.ok(labels(submenu(ctx, 'Pivot')).every((l) => !/on Total/.test(l) || l.endsWith('[off]')), labels(submenu(ctx, 'Pivot')).join(' | '));
  });

  it('a value on a column whose type is not known yet offers no value filter: the operators are the type\'s, never guessed', () => {
    // a calculated column the compiler has not typed yet (the app passes no columnType)
    const filter = submenu({ snapshot: CALCULATED, column: 'big', value: 5 }, 'Filter');
    assert.deepEqual(labels(filter), ['Filters...', 'Clear All Filters [off]']);
  });

  it('a date\'s value filters compare it; a relative date is named TODAY or NOW, a JSON document as written', () => {
    const today = submenu({ snapshot: DATED, column: 'trade_date', columnType: 'StrictDate', value: { relative: 'today' } }, 'Filter');
    assert.equal(today[0]?.label, 'Add Filter: trade_date = TODAY');
    assert.deepEqual(today[1]?.submenu?.map((i) => i.label), [
      'Add Filter: trade_date != TODAY', 'Add Filter: trade_date < TODAY', 'Add Filter: trade_date <= TODAY',
      'Add Filter: trade_date > TODAY', 'Add Filter: trade_date >= TODAY',
    ]);
    assert.equal(valueLabel({ relative: 'now' }), 'NOW');
    assert.equal(valueLabel({ json: '[1,2]' }), '[1,2]');
    // a time of day is compared for equality only
    const time = submenu({ snapshot: DATED, column: 'cutoff', columnType: 'StrictTime', value: '17:30:00' }, 'Filter');
    assert.deepEqual(labels(time), [
      'Add Filter: cutoff = \'17:30:00\'', 'More Filters on cutoff...', 'Filters...', 'Clear All Filters [off]',
    ]);
  });
});

describe('applying an entry to the grid (applyMenuAction)', () => {
  const SORTED: CubeSnapshot = {
    ...CUBE, sorts: [{ column: 'region', direction: 'asc' }, { column: 'desk', direction: 'desc' }, { column: 'notional', direction: 'asc' }],
  };

  it('Clear Sort takes the column\'s sort away, the others kept in order; Clear All Sorts takes every one', () => {
    assert.deepEqual(apply(SORTED, { id: 'sort.clearColumn', column: 'desk' }).sorts,
      [{ column: 'region', direction: 'asc' }, { column: 'notional', direction: 'asc' }]);
    assert.deepEqual(apply(SORTED, { id: 'sort.clearAll' }).sorts, []);
  });

  it('a column entry with no column changes nothing: the very same grid, so nothing is asked again', () => {
    for (const id of ['sort.asc', 'sort.addDesc', 'sort.clearColumn', 'pivot.vertical', 'pivot.addVertical', 'pivot.removeVertical',
      'pivot.horizontal', 'pivot.addHorizontal', 'pivot.removeHorizontal', 'filter.add'] as const) {
      assert.equal(apply(SORTED, { id }), SORTED, id);
    }
    // a filter with no operator is no filter
    assert.equal(apply(SORTED, { id: 'filter.add', column: 'region', value: 'EMEA' }), SORTED);
  });

  it('Add Filter adds: alone it is the filter; beside one it is AND-ed; to an AND it joins it, flat', () => {
    const emea = { id: 'filter.add', column: 'region', operator: 'equal', value: 'EMEA' } as const;
    const isEmea: FilterNode = { kind: 'condition', column: 'region', operator: 'equal', value: 'EMEA' };
    assert.deepEqual(apply(CUBE, emea).filter, isEmea);
    const fx: FilterNode = { kind: 'condition', column: 'desk', operator: 'equal', value: 'FX' };
    assert.deepEqual(apply({ ...CUBE, filter: fx }, emea).filter, { kind: 'and', children: [fx, isEmea] });
    const both: FilterNode = { kind: 'and', children: [fx, { kind: 'condition', column: 'year', operator: 'greaterThan', value: 2020 }] };
    assert.deepEqual(apply({ ...CUBE, filter: both }, emea).filter, { kind: 'and', children: [...both.children, isEmea] });
    // an OR is the filter editor's: the new condition is AND-ed with the whole of it
    const either: FilterNode = { kind: 'or', children: both.children };
    assert.deepEqual(apply({ ...CUBE, filter: either }, emea).filter, { kind: 'and', children: [either, isEmea] });
  });

  it('the entry a clicked cell offers filters on what its label says; a blank cell\'s carries no value', () => {
    const offered = items({ snapshot: CUBE, column: 'region', columnType: 'String', value: 'EMEA' })
      .find((i) => i.label === 'Add Filter: region does not start with \'EMEA\'');
    assert.ok(offered);
    assert.deepEqual(applyMenuAction(CUBE, offered, DEFAULT_CONFIGURATION).filter,
      { kind: 'condition', column: 'region', operator: 'notStartsWith', value: 'EMEA' });
    const blank = items({ snapshot: CUBE, column: 'region', columnType: 'String', value: null })
      .find((i) => i.label === 'Add Filter: region is null');
    assert.ok(blank);
    const filter = applyMenuAction(CUBE, blank, DEFAULT_CONFIGURATION).filter;
    assert.deepEqual(filter, { kind: 'condition', column: 'region', operator: 'isEmpty' });
    assert.ok(filter && !('value' in filter), 'no value key: a null predicate takes none');
  });

  it('pivots: every row group cleared; a column pivot added last, once; taken away', () => {
    assert.deepEqual(apply(CUBE, { id: 'pivot.clearVertical' }).rows, []);
    assert.deepEqual(apply(CUBE, { id: 'pivot.addHorizontal', column: 'desk' }).pivotOn, ['year', 'desk']);
    assert.equal(apply(CUBE, { id: 'pivot.addHorizontal', column: 'year' }), CUBE, 'pivoted already: nothing changes');
    assert.deepEqual(apply({ ...CUBE, pivotOn: ['year', 'desk'] }, { id: 'pivot.removeHorizontal', column: 'year' }).pivotOn, ['desk']);
  });

  it('the header menu sorts by a pivot\'s header, as it sorts by the header clicked', () => {
    assert.deepEqual(apply(CUBE, { id: 'sort.desc', column: '2021__|__notional', direction: 'desc' }).sorts,
      [{ column: '2021__|__notional', direction: 'desc' }]);
  });

  it('taking the column pivot away takes a sort of one of its headers with it; the grid\'s own sorts stay', () => {
    // the flat grid has no such column, and its query was refused (§4)
    const s: CubeSnapshot = {
      ...CUBE, sorts: [{ column: '2021__|__notional', direction: 'desc' }, { column: 'region', direction: 'asc' }],
    };
    for (const item of [{ id: 'pivot.removeHorizontal', column: 'year' } as const, { id: 'pivot.clearHorizontal' } as const]) {
      const flat = apply(s, item);
      assert.deepEqual(flat.pivotOn, [], item.id);
      assert.deepEqual(flat.sorts, [{ column: 'region', direction: 'asc' }], item.id);
    }
  });

  it('an entry the grid\'s rules refuse is refused in their words (the app says them, never drops them)', () => {
    const refused = (s: CubeSnapshot, item: Omit<MenuItem, 'label'>, words: string): void => {
      assert.throws(() => apply(s, item), (e: unknown) => e instanceof CubeRefusal && e.message === words, words);
    };
    refused(CUBE, { id: 'pivot.vertical', column: 'notional' },
      '\'notional\' is a measure, and the row groups take dimensions: Column Properties > Column Kind makes it one');
    refused(CUBE, { id: 'pivot.addHorizontal', column: 'book' },
      '\'book\' is not a column the pivot can take: the grid\'s dimensions are desk, region');
    refused(CUBE, { id: 'sort.asc', column: 'book', direction: 'asc' },
      'the sort names \'book\', which the grid does not have: one of its columns (desk, notional, region, year), or a pivot\'s header');
    // a header of a column the pivot does not spread (region is a row group, not a measure) is no header of the grid's
    refused(CUBE, { id: 'sort.addAsc', column: '2021__|__region', direction: 'asc' },
      'the sort names \'2021__|__region\', which the grid does not have: one of its columns (desk, notional, region, year), or a pivot\'s header');
  });
});
