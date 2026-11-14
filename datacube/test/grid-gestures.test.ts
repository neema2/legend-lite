// A GRID'S GESTURES AS THEIR COMMANDS (src/grid-gestures.ts; docs/DATACUBE_ONE_API_PLAN_2026_10_21.md S8): each gesture
// a person makes on a grid means what its one function says -- the function the screen's control calls and Python's
// grid.gesture asks (headless gridGesture) -- so these hold what each means, and that it is refused in words when it is
// given what it does not take.

import assert from 'node:assert/strict';
import { DATA_BAR, RULE_STYLES } from '../src/conditional.ts';
import { describe, it } from 'node:test';

import { element } from '../../pure-protocol/src/index.ts';
import { DEFAULT_CONFIGURATION, type CubeConfiguration } from '../src/config.ts';
import { applyGridCommands, readGridCommand } from '../src/grid-commands.ts';
import {
  GRID_GESTURES, columnOrderOf, gestureReadsView, gridGesture, gridGestureGiven, isGridGesture, type GestureGrid, type GestureView,
} from '../src/grid-gestures.ts';
import { CubeRefusal, pivotTotalColumn, type CubeSnapshot } from '../src/snapshot.ts';
import { TREE_COLUMN } from '../src/treeview.ts';

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
  pivotOn: [],
  sorts: [],
  epoch: 1,
};
const VIEW: GestureView = {
  headers: [{ name: TREE_COLUMN, type: 'String' }, { name: 'desk', type: 'String' }, { name: 'notional', type: 'Float' }],
  pivot: [{ name: '2021__|__notional', measure: { name: 'notional' } }],
};
const grid = (s: Partial<CubeSnapshot> = {}, config: CubeConfiguration = DEFAULT_CONFIGURATION, view: GestureView | null = VIEW): GestureGrid =>
  ({ snapshot: { ...CUBE, ...s }, config, dimensions: [{ name: 'Book', columns: ['region', 'desk'] }], view });
const withColumns = (columns: CubeConfiguration['columns']): CubeConfiguration => ({ ...DEFAULT_CONFIGURATION, columns });
const refused = (f: () => unknown, words: RegExp): void => {
  assert.throws(f, (e: unknown) => e instanceof CubeRefusal && words.test(e.message), String(words));
};

describe('the row groups and the pivot', () => {
  it("a column added last, once -- added twice, nothing changes -- or taken out, the rest in order", () => {
    assert.deepEqual(gridGesture('pivot.addVertical', grid(), { column: 'desk' }), [{ kind: 'group', rows: ['region', 'desk'] }]);
    assert.deepEqual(gridGesture('pivot.addVertical', grid(), { column: 'region' }), [], 'grouped by already');
    assert.deepEqual(gridGesture('pivot.removeVertical', grid({ rows: ['region', 'desk'] }), { column: 'region' }),
      [{ kind: 'group', rows: ['desk'] }]);
    assert.deepEqual(gridGesture('pivot.addHorizontal', grid({ pivotOn: ['year'] }), { column: 'desk' }),
      [{ kind: 'pivot', columns: ['year', 'desk'] }]);
    assert.deepEqual(gridGesture('pivot.removeHorizontal', grid({ pivotOn: ['year'] }), { column: 'year' }),
      [{ kind: 'pivot', columns: [] }]);
  });

  it('a drag in the zones sets both, whole, in one change', () => {
    assert.deepEqual(gridGesture('zones.set', grid(), { rows: ['desk'], columns: ['year'] }),
      [{ kind: 'group', rows: ['desk'] }, { kind: 'pivot', columns: ['year'] }]);
  });

  it("Measures First turns the setting the other way; an excluded column says so as Column Properties' setting", () => {
    assert.deepEqual(gridGesture('pivot.measuresFirst', grid(), {}), [{ kind: 'configure', patch: { pivotMeasuresFirst: true } }]);
    assert.deepEqual(gridGesture('pivot.measuresFirst', grid({}, { ...DEFAULT_CONFIGURATION, pivotMeasuresFirst: true }), {}),
      [{ kind: 'configure', patch: { pivotMeasuresFirst: null } }]);
    assert.deepEqual(gridGesture('pivot.exclude', grid(), { column: 'notional' }),
      [{ kind: 'configure', patch: { columns: { notional: { excludedFromPivot: true } } } }]);
  });
});

describe("a Cross Tab's values pinned (FL2: pivot.pin, grid-edits.ts pinPivot): its columns fixed", () => {
  const pivoted = (s: Partial<CubeSnapshot> = {}): GestureGrid => grid({ pivotOn: ['year'], ...s });
  it('pins the values as the database answered them, each typed as its column is; null takes them off', () => {
    // a big integer answered as its digits: a number again; one past a double's exact range kept as its text
    assert.deepEqual(gridGesture('pivot.pin', pivoted(), { values: ['2023', 2024, '90071992547409930'] }),
      [{ kind: 'pinPivot', values: [2023, 2024, '90071992547409930'] }]);
    assert.deepEqual(gridGesture('pivot.pin', grid({ pivotOn: ['desk'] }), { values: ['FX', 'Rates'] }), [{ kind: 'pinPivot', values: ['FX', 'Rates'] }]);
    assert.deepEqual(gridGesture('pivot.pin', pivoted({ pivotValues: [2023] }), { values: [2023] }), [], 'pinned so already');
    assert.deepEqual(gridGesture('pivot.pin', pivoted({ pivotValues: [2023] }), { values: null }), [{ kind: 'pinPivot', values: null }]);
    assert.deepEqual(gridGesture('pivot.pin', pivoted(), { values: null }), [], 'nothing pinned to take off');
    refused(() => gridGesture('pivot.pin', pivoted(), { values: [2023, null] }), /an empty 'year', which names no column/);
  });

  it("adds the values its pin lacks after those it has, typed so, each once (FL8's Add to the cast: pivot.addValues)", () => {
    assert.deepEqual(gridGesture('pivot.addValues', pivoted({ pivotValues: [2023] }), { values: ['2025', 2023, 2024, '2025', null] }),
      [{ kind: 'pinPivot', values: [2023, 2025, 2024] }], 'an empty value names no column: skipped');
    refused(() => gridGesture('pivot.addValues', pivoted({ pivotValues: [2023] }), { values: ['2023', null] }), /a column for each of them already/);
    refused(() => gridGesture('pivot.addValues', pivoted(), { values: [2023] }), /its columns are not fixed: Load values fixes them/);
    refused(() => gridGestureGiven('pivot.addValues', pivoted({ pivotValues: [2023] }), { values: 2024 }), /a pivot's values are a list/);
  });

  it("a Decimal's values kept as their text, every digit (the review of 2026-10-26's B1); a Float's a number", () => {
    const priced = grid({ columns: [...CUBE.columns, { name: 'px', type: 'Decimal' }, { name: 'rate', type: 'Float' }], pivotOn: ['px'] });
    assert.deepEqual(gridGesture('pivot.pin', priced, { values: ['12.10', '12345678901234.567'] }),
      [{ kind: 'pinPivot', values: ['12.10', '12345678901234.567'] }]);
    assert.deepEqual(gridGesture('pivot.pin', { ...priced, snapshot: { ...priced.snapshot, pivotOn: ['rate'] } }, { values: ['0.25'] }),
      [{ kind: 'pinPivot', values: [0.25] }]);
  });

  it('applied: its values kept while it pivots on that column, taken off by another pivot; refused in words', () => {
    const applied = (s: Partial<CubeSnapshot>, commands: readonly unknown[]): CubeSnapshot =>
      applyGridCommands({ snapshot: { ...CUBE, ...s }, config: DEFAULT_CONFIGURATION }, commands as never).snapshot;
    assert.deepEqual(applied({ pivotOn: ['year'] }, [{ kind: 'pinPivot', values: [2023, 2024] }]).pivotValues, [2023, 2024]);
    assert.equal(applied({ pivotOn: ['year'], pivotValues: [2023] }, [{ kind: 'pinPivot', values: null }]).pivotValues, undefined);
    assert.deepEqual(applied({ pivotOn: ['year'], pivotValues: [2023] }, [{ kind: 'pivot', columns: ['year'] }]).pivotValues, [2023], 'the same pivot');
    assert.equal(applied({ pivotOn: ['year'], pivotValues: [2023] }, [{ kind: 'pivot', columns: ['desk'] }]).pivotValues, undefined, "another's values none of its");
    refused(() => applied({}, [{ kind: 'pinPivot', values: [1] }]), /pivots on no column/);
    refused(() => applied({ pivotOn: ['year', 'desk'] }, [{ kind: 'pinPivot', values: [1] }]), /a pivot on one column has its values pinned, not on 2/);
    refused(() => applied({ pivotOn: ['desk'] }, [{ kind: 'pinPivot', values: [] }]), /one value of 'desk' at least/);
    refused(() => applied({ pivotOn: ['desk'] }, [{ kind: 'pinPivot', values: ['FX', 'FX'] }]), /'FX' is pinned twice/);
    refused(() => applied({ pivotOn: ['desk'] }, [{ kind: 'pinPivot', values: [''] }]), /an empty text names no column/);
    assert.deepEqual(readGridCommand({ kind: 'pinPivot', values: ['FX', 2] }), { kind: 'pinPivot', values: ['FX', 2] });
    assert.deepEqual(readGridCommand({ kind: 'pinPivot', values: null }), { kind: 'pinPivot', values: null });
    refused(() => readGridCommand({ kind: 'pinPivot', values: [{ x: 1 }] }), /a list of texts, numbers or true and false, or null/);
  });
});

describe("the columns' presentation", () => {
  it("a setting a column has already, or a group it is not in taken out, is nothing to tell (the S8 audit's 6)", () => {
    const config = withColumns({ desk: { pinned: 'left', hidden: true, width: 120 }, notional: { heatmap: { from: '#ffffff', to: '#ff8a65' } } });
    assert.deepEqual(gridGesture('column.pinLeft', grid({}, config), { column: 'desk' }), []);
    assert.deepEqual(gridGesture('column.hide', grid({}, config), { column: 'desk' }), []);
    assert.deepEqual(gridGesture('column.resize', grid({}, config), { column: 'desk', width: 120 }), []);
    assert.deepEqual(gridGesture('column.unpin', grid(), { column: 'desk' }), [], 'no pin to take off');
    assert.deepEqual(gridGesture('heatmap.remove', grid(), { column: 'notional' }), [], 'no heatmap to take off');
    assert.deepEqual(gridGesture('heatmap.add', grid({}, config), { column: 'notional' }), [], 'the heatmap it has');
    assert.deepEqual(gridGesture('pivot.removeVertical', grid(), { column: 'desk' }), [], 'not a row group');
    assert.deepEqual(gridGesture('pivot.removeHorizontal', grid(), { column: 'year' }), [], 'not pivoted on');
    // and one it has not, told
    assert.deepEqual(gridGesture('column.pinRight', grid({}, config), { column: 'desk' }),
      [{ kind: 'configure', patch: { columns: { desk: { pinned: 'right' } } } }]);
  });

  it('Unpin All unpins every pinned column, and is nothing when none is', () => {
    assert.deepEqual(gridGesture('column.unpinAll', grid({}, withColumns({ desk: { pinned: 'left' }, year: { pinned: 'right' }, region: {} })), {}),
      [{ kind: 'configure', patch: { columns: { desk: { pinned: null }, year: { pinned: null } } } }]);
    assert.deepEqual(gridGesture('column.unpinAll', grid(), {}), []);
  });

  it("a reorder is merged into the grid's whole order, a column dragged in from the panel shown with it", () => {
    // the grid reports only what it shows: region, grouped, keeps its slot
    assert.deepEqual(columnOrderOf(CUBE, DEFAULT_CONFIGURATION), ['region', 'desk', 'year', 'notional']);
    assert.deepEqual(gridGesture('columns.reorder', grid(), { order: ['notional', 'desk'] }),
      [{ kind: 'configure', patch: { columnOrder: ['region', 'notional', 'year', 'desk'] } }]);
    assert.deepEqual(gridGesture('columns.reorder', grid(), { order: ['notional', 'desk'], added: 'year' }),
      [{ kind: 'configure', patch: { columnOrder: ['region', 'notional', 'year', 'desk'], columns: { year: { hidden: false } } } }]);
  });

  it('Auto-size sizes each column measured between its floor and its ceiling; none changed, nothing', () => {
    assert.deepEqual(gridGesture('columns.autoSize', grid(), { widths: { desk: 10, notional: 100, region: 900 } }),
      [{ kind: 'configure', patch: { columns: { desk: { width: 48 }, notional: { width: 108 }, region: { width: 480 } } } }]);
    assert.deepEqual(gridGesture('columns.autoSize', grid({}, withColumns({ desk: { width: 108 } })), { widths: { desk: 100 } }), []);
  });

  it("Minimize takes a column to its own minimum, or upstream's 50; a fixed width keeps it; All is every header shown", () => {
    const config = withColumns({ desk: { minWidth: 70 }, notional: { widthMode: 'fixed', width: 200 } });
    assert.deepEqual(gridGesture('column.minimize', grid({}, config), { column: 'desk' }),
      [{ kind: 'configure', patch: { columns: { desk: { width: 70 } } } }]);
    assert.deepEqual(gridGesture('column.minimize', grid({}, config), { column: 'notional' }), [], 'fixed');
    assert.deepEqual(gridGesture('column.minimizeAll', grid({}, config), {}),
      [{ kind: 'configure', patch: { columns: { desk: { width: 70 } } } }], 'the tree column and a fixed one left');
  });

  it('Size to Fit scales every flexible column by one factor to fill the grid, a fixed one kept; none fits, nothing', () => {
    const config = withColumns({ notional: { widthMode: 'fixed' } });
    assert.deepEqual(gridGesture('columns.sizeToFit', grid({}, config), { widths: { desk: 100, year: 50, notional: 100 }, viewport: 400 }),
      [{ kind: 'configure', patch: { columns: { desk: { width: 200 }, year: { width: 100 } } } }]);
    assert.deepEqual(gridGesture('columns.sizeToFit', grid(), { widths: { desk: 100 }, viewport: 0 }), []);
  });

  it("a heatmap goes on a pivoted column's measure, so every value's column has it; a total's is its own", () => {
    assert.deepEqual(gridGesture('heatmap.add', grid({ pivotOn: ['year'] }), { column: '2021__|__notional' }),
      [{ kind: 'configure', patch: { columns: { notional: { heatmap: { from: '#ffffff', to: '#ff8a65' } } } } }]);
    const total = pivotTotalColumn('notional');
    const heated = withColumns({ [total]: { heatmap: { from: '#ffffff', to: '#ff8a65' } } });
    assert.deepEqual(gridGesture('heatmap.remove', grid({ pivotOn: ['year'] }, heated), { column: total }),
      [{ kind: 'configure', patch: { columns: { [total]: { heatmap: null } } } }]);
  });

  it('the chrome folded or shown, each as it is given', () => {
    assert.deepEqual(gridGesture('view.chrome', grid(), { showTitleBar: false }), [{ kind: 'configure', patch: { showTitleBar: false } }]);
  });
});

describe('sorts, the tree and a dimension', () => {
  it("a header's click by the one rule (header-sort.ts), its first direction its header's type; the tree column's the groups'", () => {
    assert.deepEqual(gridGesture('header.sort', grid(), { column: 'notional', chained: false }),
      [{ kind: 'clickSort', column: 'notional', first: 'desc', chained: false }]);
    assert.deepEqual(gridGesture('header.sort', grid(), { column: null, chained: true }), [{ kind: 'clickTreeSort', chained: true }]);
    assert.deepEqual(gridGesture('header.sort', grid(), { column: TREE_COLUMN, chained: false }), [{ kind: 'clickTreeSort', chained: false }]);
    refused(() => gridGesture('header.sort', grid(), { column: 'year', chained: false }), /'year' is no header of the grid/);
  });

  it('a group opened is set, never toggled', () => {
    assert.deepEqual(gridGesture('tree.open', grid(), { path: ['EMEA'], open: true }), [{ kind: 'open', path: ['EMEA'], open: true }]);
  });

  it('a named dimension used groups by its first level, drilled one deeper or back; one the grid has not, refused', () => {
    assert.deepEqual(gridGesture('view.dimension', grid({ rows: [] }), { name: 'Book', move: 'use' }), [{ kind: 'group', rows: ['region'] }]);
    assert.deepEqual(gridGesture('view.dimension', grid(), { name: 'Book', move: 'down' }), [{ kind: 'group', rows: ['region', 'desk'] }]);
    refused(() => gridGesture('view.dimension', grid(), { name: 'Geo', move: 'use' }), /the grid has no dimension 'Geo' \(its dimensions: Book\)/);
    refused(() => gridGestureGiven('view.dimension', grid(), { name: 'Book', move: 'sideways' }), /used, drilled down or drilled up/);
  });
});

describe('the value list and date ranges: a column\'s own conditions replaced', () => {
  const emea = { kind: 'condition' as const, column: 'region', operator: 'equal' as const, value: 'EMEA' };
  const big = { kind: 'condition' as const, column: 'notional', operator: 'greaterThan' as const, value: 100 };

  it('the values kept in place of the column\'s own conditions; every value takes them off; nothing kept refused', () => {
    const filtered = grid({ filter: { kind: 'and', children: [emea, big] } });
    assert.deepEqual(gridGesture('filter.values', filtered, { column: 'region', values: ['EMEA', 'AMER', null] }), [{
      kind: 'refilter', remove: [emea],
      add: [{ kind: 'or', children: [
        { kind: 'condition', column: 'region', operator: 'in', value: ['EMEA', 'AMER'] },
        { kind: 'condition', column: 'region', operator: 'isEmpty' },
      ] }],
    }]);
    // applied: the other column's condition stays as it was
    const [command] = gridGesture('filter.values', filtered, { column: 'region', values: ['AMER'] });
    const after = applyGridCommands({ snapshot: filtered.snapshot, config: DEFAULT_CONFIGURATION }, [command!]);
    assert.deepEqual(after.snapshot.filter,
      { kind: 'and', children: [big, { kind: 'condition', column: 'region', operator: 'equal', value: 'AMER' }] });
    assert.deepEqual(gridGesture('filter.values', filtered, { column: 'region', values: null }),
      [{ kind: 'refilter', remove: [emea], add: [] }], 'every value: its own conditions off');
    assert.deepEqual(gridGesture('filter.values', grid(), { column: 'region', values: null }), [], 'none to take off');
    refused(() => gridGesture('filter.values', grid(), { column: 'region', values: [] }), /keep at least one value of 'region'/);
    refused(() => gridGesture('filter.values', grid(), { column: 'region', values: 'EMEA' as never }), /the values kept are a list/);
  });

  it('a date range from its start to its end, in place of the column\'s own conditions', () => {
    const [command] = gridGesture('filter.dateRange', grid(), { column: 'year', range: 'thisYear' });
    assert.deepEqual(command, { kind: 'refilter', remove: [], add: [
      { kind: 'condition', column: 'year', operator: 'greaterThanEqual', value: { relative: 'today', startOf: 'year' } },
      { kind: 'condition', column: 'year', operator: 'lessThan',
        value: { relative: 'today', startOf: 'year', offset: 1, unit: 'YEARS' } },
    ] });
    refused(() => gridGesture('filter.dateRange', grid(), { column: 'year', range: 'soon' as never }), /a date range is today/);
  });

  it("a Date Filter's dates: a range or the days between two, both kept, its filter alone; refused in words", () => {
    const [command] = gridGesture('filter.dates', grid(), { column: 'year', from: '2024-01-01', to: '2024-06-30' });
    const days = [
      { kind: 'condition', column: 'year', operator: 'greaterThanEqual', value: '2024-01-01' },
      { kind: 'condition', column: 'year', operator: 'lessThanEqual', value: '2024-06-30' },
    ] as const;
    assert.deepEqual(command, { kind: 'refilter', remove: [], add: days });
    // in place of every condition it had: another column's, a range's
    const had = { kind: 'condition', column: 'region', operator: 'equal', value: 'EMEA' } as const;
    const filtered = { ...grid(), snapshot: { ...grid().snapshot, filter: had } };
    assert.deepEqual(gridGesture('filter.dates', filtered, { column: 'year', range: 'thisYear' })[0],
      { kind: 'refilter', remove: [had], add: gridGesture('filter.dateRange', grid(), { column: 'year', range: 'thisYear' })
        .flatMap((c) => (c.kind === 'refilter' ? c.add : [])) });
    const set = { ...grid(), snapshot: { ...grid().snapshot, filter: { kind: 'and', children: days } as const } };
    assert.deepEqual(gridGesture('filter.dates', set, { column: 'year', from: '2024-01-01', to: '2024-06-30' }), [], 'so already');
    refused(() => gridGesture('filter.dates', grid(), { column: 'year', from: '2024-02-30', to: '2024-06-30' }),
      /its first day is a day, as 2024-06-30, not "2024-02-30"/);
    refused(() => gridGesture('filter.dates', grid(), { column: 'year', from: '2024-07-01', to: '2024-06-30' }),
      /its first day, 2024-07-01, is after its last, 2024-06-30/);
    refused(() => gridGesture('filter.dates', grid(), { column: 'year', range: 'thisYear', from: '2024-07-01', to: '2024-08-01' }),
      /a range of dates, or the days from one to another: one of them/);
    refused(() => gridGesture('filter.dates', grid(), { column: 'year' }), /one of them/);
  });
});

describe('top or bottom N', () => {
  it('by the column, the largest or the smallest; taken off when set', () => {
    assert.deepEqual(gridGesture('filter.topN', grid(), { column: 'notional', n: 10 }),
      [{ kind: 'topN', topN: { by: 'notional', n: 10 } }]);
    assert.deepEqual(gridGesture('filter.topN', grid(), { column: 'notional', n: 5, bottom: true }),
      [{ kind: 'topN', topN: { by: 'notional', n: 5, bottom: true } }]);
    assert.deepEqual(gridGesture('filter.topNOff', grid({ topN: { by: 'notional', n: 5 } }), {}), [{ kind: 'topN', topN: null }]);
    assert.deepEqual(gridGesture('filter.topNOff', grid(), {}), [], 'none to take off');
    refused(() => gridGesture('filter.topN', grid(), { column: 'notional', n: 2.5 }), /whole number from 1/);
  });
});

describe('what a gesture is given', () => {
  it('named by text (a menu entry, Python), an argument it does not take, or one it needs and is not given, is refused in words', () => {
    refused(() => gridGestureGiven('column.pinLeft', grid(), { column: 'desk', side: 'left' }), /'column.pinLeft' takes column, not side/);
    refused(() => gridGestureGiven('column.pinLeft', grid(), {}), /'column.pinLeft' needs column/);
    refused(() => gridGestureGiven('sort.clearAll', grid(), { column: 'desk' }), /'sort.clearAll' takes nothing, not column/);
    refused(() => gridGestureGiven('column.resize', grid(), { column: 'desk', width: 'wide' }), /a column's width is a number of pixels/);
    refused(() => gridGestureGiven('nope', grid(), {}), /'nope' is no gesture of a grid's: sort\.asc, /);
    refused(() => gridGestureGiven('sort.clearAll', grid(), ['x']), /a gesture's arguments are given by name/);
    // the same table as a control's: what one makes, the other does
    assert.deepEqual(gridGestureGiven('pivot.addVertical', grid(), { column: 'desk' }), gridGesture('pivot.addVertical', grid(), { column: 'desk' }));
  });

  it("given from JSON, its commands are read by the commands' own reader, as Python's are (headless gridGesture)", () => {
    const read = (name: string, args: Record<string, unknown>) => gridGestureGiven(name, grid(), args).map(readGridCommand);
    refused(() => read('filter.add', { column: 'desk', operator: 'nearly', value: 'FX' }), /a condition's operator is one of/);
    refused(() => read('tree.open', { path: 'EMEA', open: true }), /a group's path is its keys/);
    refused(() => gridGestureGiven('tree.open', grid(), { path: ['EMEA'], open: 'yes' }), /opened is true or false/);
  });

  it('a gesture that reads the view is said to -- the heatmap only where a pivoted leaf can be named; one given no view refuses to guess', () => {
    const pivoted: CubeSnapshot = { ...CUBE, pivotOn: ['year'] };
    assert.deepEqual(GRID_GESTURES.filter((n) => gestureReadsView(n, pivoted, { column: '2021__|__notional' })),
      ['header.sort', 'column.minimizeAll', 'heatmap.add', 'heatmap.remove', 'format.dataBar', 'format.colorScale', 'format.iconSet',
        'format.addRule', 'format.code', 'format.clear']);
    // a grid not pivoted, a column of the grid's own, a pivot's total: no view to read, the grid not run for it
    assert.equal(gestureReadsView('heatmap.add', CUBE, { column: 'notional' }), false);
    assert.equal(gestureReadsView('heatmap.add', pivoted, { column: 'notional' }), false);
    assert.equal(gestureReadsView('heatmap.add', pivoted, { column: pivotTotalColumn('notional') }), false);
    assert.deepEqual(gridGesture('heatmap.add', grid({}, DEFAULT_CONFIGURATION, null), { column: 'notional' }),
      [{ kind: 'configure', patch: { columns: { notional: { heatmap: { from: '#ffffff', to: '#ff8a65' } } } } }]);
    assert.throws(() => gridGesture('heatmap.add', grid({ pivotOn: ['year'] }, DEFAULT_CONFIGURATION, null), { column: '2021__|__notional' }), /given none/);
    assert.equal(isGridGesture('copy.selection'), false, 'the clipboard is the screen\'s own');
  });

  it("a gesture's commands apply: what it makes, the grid takes", () => {
    const d = { snapshot: CUBE, config: DEFAULT_CONFIGURATION };
    assert.deepEqual(applyGridCommands(d, gridGesture('pivot.addVertical', grid(), { column: 'desk' })).snapshot.rows, ['region', 'desk']);
  });
});

describe("conditional formats (conditional.ts): on what a heatmap is on", () => {
  it('data bars on and off, a colour scale, icons, a rule added once, all cleared', () => {
    const patch = (columns: unknown): unknown[] => [{ kind: 'configure', patch: { columns } }];
    assert.deepEqual(gridGesture('format.dataBar', grid(), { column: 'notional', on: true }), patch({ notional: { dataBar: DATA_BAR } }));
    assert.deepEqual(gridGesture('format.dataBar', grid(), { column: 'notional', on: false }), [], 'no bars to take off');
    assert.deepEqual(gridGesture('format.colorScale', grid(), { column: 'notional', scale: { from: '#f8696b', mid: '#ffeb84', to: '#63be7b' } }),
      patch({ notional: { heatmap: { from: '#f8696b', mid: '#ffeb84', to: '#63be7b' } } }));
    assert.deepEqual(gridGesture('format.iconSet', grid(), { column: 'notional', set: 'flags', reverse: true }),
      patch({ notional: { iconSet: { set: 'flags', reverse: true } } }));
    const rule = { when: { op: 'greaterThan', value: 100 }, style: 'yellow' } as const;
    const kept = { when: { op: 'greaterThan', value: 100 }, style: RULE_STYLES.yellow };
    assert.deepEqual(gridGesture('format.addRule', grid(), { column: 'notional', rule }), patch({ notional: { rules: [kept] } }));
    const ruled = withColumns({ notional: { rules: [kept as never] } });
    assert.deepEqual(gridGesture('format.addRule', grid({}, ruled), { column: 'notional', rule }), [], 'the rule it has');
    assert.deepEqual(gridGesture('format.addRule', grid({}, ruled), { column: 'notional', rule: { when: { op: 'blank' }, style: 'bold' } as const }),
      patch({ notional: { rules: [kept, { when: { op: 'blank' }, style: RULE_STYLES.bold }] } }), 'after those it has');
    assert.deepEqual(gridGesture('format.clear', grid({}, ruled), { column: 'notional' }),
      patch({ notional: { heatmap: null, dataBar: null, iconSet: null, rules: null } }));
    refused(() => gridGesture('format.iconSet', grid(), { column: 'notional', set: 'stars' as never }), /one of arrows, trafficLights, symbols, flags/);
    refused(() => gridGesture('format.addRule', grid(), { column: 'notional', rule: { when: { op: 'top' }, style: 'yellow' } as never }), /count is missing/);
    refused(() => gridGesture('format.colorScale', grid(), { column: 'notional', scale: { from: 'red', to: '#fff' } as never }), /#rrggbb, not "red"/);
  });

  it("on a pivoted column's measure, so every value's column has it", () => {
    assert.deepEqual(gridGesture('format.iconSet', grid({ pivotOn: ['year'] }), { column: '2021__|__notional', set: 'arrows' }),
      [{ kind: 'configure', patch: { columns: { notional: { iconSet: { set: 'arrows' } } } } }]);
  });
});

describe("an Excel number format code on a column (format-code.ts)", () => {
  it('set beside its other format settings, judged as given, taken off by null', () => {
    const patch = (columns: unknown): unknown[] => [{ kind: 'configure', patch: { columns } }];
    assert.deepEqual(gridGesture('format.code', grid(), { column: 'notional', code: '#,##0.0,,"M"' }),
      patch({ notional: { format: { kind: 'auto', code: '#,##0.0,,"M"' } } }));
    const formatted = withColumns({ notional: { format: { kind: 'number', decimals: 2, code: '0.0%' } } });
    assert.deepEqual(gridGesture('format.code', grid({}, formatted), { column: 'notional', code: null }),
      patch({ notional: { format: { kind: 'number', decimals: 2 } } }), 'its other settings kept');
    assert.deepEqual(gridGesture('format.code', grid({}, withColumns({ notional: { format: { kind: 'auto', code: '0' } } })), { column: 'notional', code: null }),
      patch({ notional: { format: null } }), 'nothing left: the format taken off');
    assert.deepEqual(gridGesture('format.code', grid({}, formatted), { column: 'notional', code: '0.0%' }), [], 'the code it has');
    refused(() => gridGesture('format.code', grid(), { column: 'notional', code: '[>100]0' }), /\[>100\] \(a condition or an elapsed time\)/);
  });
});
