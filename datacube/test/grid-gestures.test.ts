// A GRID'S GESTURES AS THEIR COMMANDS (src/grid-gestures.ts; docs/DATACUBE_ONE_API_PLAN_2026_10_21.md S8): each gesture
// a person makes on a grid means what its one function says -- the function the screen's control calls and Python's
// grid.gesture asks (headless gridGesture) -- so these hold what each means, and that it is refused in words when it is
// given what it does not take.

import assert from 'node:assert/strict';
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
      ['header.sort', 'column.minimizeAll', 'heatmap.add', 'heatmap.remove']);
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
