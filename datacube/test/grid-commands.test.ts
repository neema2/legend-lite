// A GRID EDIT AS A VALUE (src/grid-commands.ts; docs/DATACUBE_COMMANDS_STAGE1_2026_10_18.md): each command gives what
// the screen's own edit gives (grid-edits.ts, config.ts), so building a command changes no edit; a command read from
// JSON (Python's) is the same command; and a kind or a field DataCube does not know is refused in words, never passed
// over.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { PIVOT_SEPARATOR } from '../../engine-client/src/generated/lite-facts.ts';
import { element } from '../../pure-protocol/src/index.ts';
import { DEFAULT_CONFIGURATION, withColumn, withConfiguration } from '../src/config.ts';
import {
  GRID_COMMAND_KINDS, applyGridCommand, applyGridCommands, draftCommands, readGridCommand, readMeasure, type GridCommand,
} from '../src/grid-commands.ts';
import {
  addSort, clearSort, clickSort, clickTreeSort, measureAsColumn, setColumn, setGrouping, setPivot, setSorts, spreadMeasureNames,
  type GridDraft,
} from '../src/grid-edits.ts';
import { CubeRefusal, pivotTotalColumn, type CubeSnapshot, type DerivedColumn } from '../src/snapshot.ts';
import { TREE_COLUMN } from '../src/treeview.ts';
import { headerSortCommand } from '../src/header-sort.ts';
import { measureCommands } from '../src/grid-measures.ts';
import { DETAIL_ROW, TreeState, parsePathKey } from '../src/tree.ts';
import { stand } from './fake-planner.ts';

const CUBE: CubeSnapshot = {
  source: { query: element('t') },
  columns: [
    { name: 'region', type: 'String' },
    { name: 'desk', type: 'String' },
    { name: 'traded', type: 'StrictDate' },
    { name: 'notional', type: 'Float' },
    { name: 'qty', type: 'Integer' },
  ],
  derived: [],
  rows: ['region'],
  pivotOn: [],
  sorts: [],
  epoch: 1,
};
const draft = (s: Partial<CubeSnapshot> = {}): GridDraft => ({ snapshot: { ...CUBE, ...s }, config: DEFAULT_CONFIGURATION });

const refused = (f: () => unknown, words: RegExp): void => {
  assert.throws(f, (e: unknown) => e instanceof CubeRefusal && words.test(e.message), String(words));
};

const double: DerivedColumn = { name: 'double', lambda: stand('$x.qty * 2'), kind: 'measure' };


describe("a measure made by the steps a person makes it by: no command of its own (the user, 2026-10-22: one API)", () => {
  const d = draft();
  const applied = (ms: Parameters<typeof measureCommands>[1]): GridDraft => applyGridCommands(d, measureCommands(d, ms));
  it("its own column's Aggregation: Column Properties' command, the grid measureAsColumn makes", () => {
    assert.deepEqual(measureCommands(d, [{ name: 'qty', column: 'qty', fn: 'max' }]),
      [{ kind: 'column', name: 'qty', change: { aggregate: 'max' } }]);
    assert.deepEqual(applied([{ name: 'qty', column: 'qty', fn: 'max' }]), measureAsColumn(d, { name: 'qty', column: 'qty', fn: 'max' }).draft);
  });
  it("a second of a column taken, or another name: the column editor's calculated column, then its Aggregation", () => {
    const made = measureCommands(d, [{ name: 'qty', column: 'qty', fn: 'max' }, { name: 'qty_min', column: 'qty', fn: 'min' }]);
    assert.deepEqual(made.map((c) => c.kind), ['column', 'calculate', 'column']);
    const once = measureAsColumn(d, { name: 'qty', column: 'qty', fn: 'max' }).draft;
    const want = measureAsColumn(once, { name: 'qty_min', column: 'qty', fn: 'min' }, new Set(['qty'])).draft;
    assert.deepEqual(applyGridCommands(d, made).snapshot.derived, want.snapshot.derived);
    assert.deepEqual(applyGridCommands(d, made).config, want.config);
    // through JSON, as Python's are read
    for (const c of made) assert.deepEqual(readGridCommand(JSON.parse(JSON.stringify(c))), c, c.kind);
  });
  it('refused in the words measureAsColumn refuses in, nothing made', () => {
    assert.throws(() => measureCommands(d, [{ name: 'x', column: 'nope', fn: 'sum' }]), (e: unknown) => e instanceof CubeRefusal
      && /'nope' is not a column of the grid/.test(e.message));
    assert.throws(() => readMeasure({ name: 'x', column: 'qty' }), CubeRefusal);
    assert.deepEqual(readMeasure({ name: ' w ', column: 'notional', fn: 'wavg', weight: 'qty' }),
      { name: 'w', column: 'notional', fn: 'wavg', weight: 'qty' });
  });
});

describe("a header's sort click, the one rule the screen's click and Python's toggle_sort ask (the user, 2026-10-22)", () => {
  const headers = [{ name: 'notional', type: 'Float' }, { name: 'desk', type: 'String' }, { name: '2024__|__qty', type: 'Integer' }];
  it('a number first from the largest, text from A, by the type its header has; the tree column turns the groups', () => {
    assert.deepEqual(headerSortCommand(headers, 'notional', false), { kind: 'clickSort', column: 'notional', first: 'desc', chained: false });
    assert.deepEqual(headerSortCommand(headers, 'desk', true), { kind: 'clickSort', column: 'desk', first: 'asc', chained: true });
    assert.deepEqual(headerSortCommand(headers, '2024__|__qty', false), { kind: 'clickSort', column: '2024__|__qty', first: 'desc', chained: false });
    assert.deepEqual(headerSortCommand(headers, TREE_COLUMN, true), { kind: 'clickTreeSort', chained: true });
  });
  it('a column the grid shows no header of is refused, naming those it does', () => {
    assert.throws(() => headerSortCommand(headers, 'nope', false), (e: unknown) => e instanceof CubeRefusal
      && e.message === "'nope' is no header of the grid (notional, desk, 2024__|__qty)");
  });
});

describe('each command gives what the screen\'s own edit gives', () => {
  const d = draft();
  const cases: [string, GridCommand, GridDraft][] = [
    ['group', { kind: 'group', rows: ['region', 'desk'] }, setGrouping(d, ['region', 'desk'])],
    ['pivot', { kind: 'pivot', columns: ['desk'] }, setPivot(d, ['desk'])],
    ['sorts', { kind: 'sorts', sorts: [{ column: 'qty', direction: 'asc' }] }, setSorts(d, [{ column: 'qty', direction: 'asc' }])],
    ['addSort', { kind: 'addSort', column: 'notional', direction: 'desc' }, addSort(d, 'notional', 'desc')],
    // a header click starts where the screen says, from the type the compiler gave the header: a number from the
    // largest, text from A -- and a column of text the grid counts is a number on the screen, though not in the query
    ['clickSort on a number', { kind: 'clickSort', column: 'notional', first: 'desc', chained: false }, clickSort(d, 'notional', 'desc', false)],
    ['clickSort on text', { kind: 'clickSort', column: 'desk', first: 'asc', chained: true }, clickSort(d, 'desk', 'asc', true)],
    ['clickSort on counted text', { kind: 'clickSort', column: 'desk', first: 'desc', chained: false }, clickSort(d, 'desk', 'desc', false)],
    ['clickTreeSort', { kind: 'clickTreeSort', chained: false }, clickTreeSort(d, false)],
    ['column', { kind: 'column', name: 'qty', change: { aggregate: 'average' } }, setColumn(d, 'qty', { aggregate: 'average' })],
    ['configure', { kind: 'configure', patch: { reportTitle: 'Trades', columns: { qty: { hidden: true } } } },
      { ...d, config: withConfiguration(d.config, { reportTitle: 'Trades', columns: { qty: { hidden: true } } }) }],
  ];
  for (const [name, command, want] of cases) {
    it(name, () => {
      assert.deepEqual(applyGridCommand(d, command), want);
    });
  }

  it('configure: null clears a setting; a column\'s Aggregation goes by Column Properties\' rule (setColumn)', () => {
    const titled = applyGridCommand(d, { kind: 'configure', patch: { reportTitle: 'Trades' } });
    assert.equal(applyGridCommand(titled, { kind: 'configure', patch: { reportTitle: null } }).config.reportTitle, undefined);
    assert.deepEqual(applyGridCommand(d, { kind: 'configure', patch: { columns: { qty: { aggregateFn: 'max' } } } }),
      setColumn(d, 'qty', { aggregate: 'max' }));
  });

  it('configure moves the tree with the settings that decide it: the grand total and the expand level (G3)', () => {
    const open = { ...d, tree: TreeState.fromPaths([['EMEA']]).collapse(['EMEA']).expand(['AMER']) };
    const totals = applyGridCommand(open, { kind: 'configure', patch: { showRootAggregation: true } });
    assert.equal(totals.tree!.showTotals, true, 'the grand total shown');
    assert.equal(totals.tree!.isOpen(['AMER']), true, 'the open groups kept');
    const levelled = applyGridCommand(totals, { kind: 'configure', patch: { initialExpandToLevel: 2 } });
    assert.equal(levelled.tree!.expandTo, 2);
    assert.equal(levelled.tree!.isOpen(['EMEA'], 3), true, 'the level opens what it reaches');
    // cleared, the level opens nothing; a patch of neither leaves the tree as it is
    assert.equal(applyGridCommand(levelled, { kind: 'configure', patch: { initialExpandToLevel: null } }).tree!.expandTo, 0);
    assert.equal(applyGridCommand(levelled, { kind: 'configure', patch: { reportTitle: 'T' } }).tree, levelled.tree);
    // an editor's draft holds no open groups: the settings are set, nothing more
    assert.equal(applyGridCommand(d, { kind: 'configure', patch: { showRootAggregation: true } }).config.showRootAggregation, true);
  });

  it("configure giving a tree's setting again as it is moves nothing: a group closed under the level stays closed (F3)", () => {
    const levelled = applyGridCommand({ ...d, tree: TreeState.fromPaths([]) }, { kind: 'configure', patch: { initialExpandToLevel: 1 } });
    const closed = { ...levelled, tree: levelled.tree!.collapse(['EMEA']) };
    assert.equal(closed.tree.isOpen(['EMEA'], 2), false, 'closed by hand under the level');
    // Python's configure names every setting it is given; the screen's Properties sends only what changed
    const again = applyGridCommand(closed, { kind: 'configure', patch: { initialExpandToLevel: 1, showRootAggregation: closed.config.showRootAggregation } });
    assert.equal(again.tree, closed.tree, 'the tree as it was');
    assert.equal(again.tree!.isOpen(['EMEA'], 2), false);
  });

  it('configure refuses a setting DataCube has not, a column the grid has not, and a total\'s own Aggregation', () => {
    refused(() => applyGridCommand(d, { kind: 'configure', patch: { maxRow: 5 } }), /'maxRow' is not a grid's setting/);
    refused(() => applyGridCommand(d, { kind: 'configure', patch: { columns: { nope: { hidden: true } } } }),
      /'nope' is not a column of the grid/);
    const total = pivotTotalColumn('notional');
    refused(() => applyGridCommand(d, { kind: 'configure', patch: { columns: { [total]: { aggregateFn: 'max' } } } }),
      /is the pivot total of 'notional'/);
  });

  it('configure sets a pivot column\'s and the tree column\'s presentation (the menu hides them), never a kind or Aggregation', () => {
    const pivoted = setPivot(d, ['desk']);
    const header = `FX${PIVOT_SEPARATOR}notional`;
    const hide = (name: string): GridCommand => ({ kind: 'configure', patch: { columns: { [name]: { hidden: true } } } });
    assert.deepEqual(applyGridCommand(pivoted, hide(header)).config.columns[header], { hidden: true });
    refused(() => applyGridCommand(pivoted, { kind: 'configure', patch: { columns: { [header]: { aggregateFn: 'max' } } } }),
      /is a pivot column of 'notional'/);
    // not pivoted, no pivot's header is a column of the grid
    refused(() => applyGridCommand(d, hide(header)), /is not a column of the grid/);
    assert.deepEqual(applyGridCommand(d, hide(TREE_COLUMN)).config.columns[TREE_COLUMN], { hidden: true });
    refused(() => applyGridCommand(d, { kind: 'configure', patch: { columns: { [TREE_COLUMN]: { kind: 'measure' } } } }),
      /the grid's tree column: it has no kind/);
  });

  it('configure sets the presentation of the count a pivot of no measure fills with, and its total\'s (the menu\'s heatmap, hide)', () => {
    const dims = setPivot(draft({ columns: [{ name: 'region', type: 'String' }, { name: 'desk', type: 'String' }] }), ['desk']);
    assert.deepEqual(spreadMeasureNames(dims), ['count'], 'a pivot of no measure fills with a count');
    const heatmap = { from: '#ffffff', to: '#ff8a65' };
    assert.deepEqual(applyGridCommand(dims, { kind: 'configure', patch: { columns: { count: { heatmap } } } }).config.columns['count'],
      { heatmap });
    const total = pivotTotalColumn('count');
    assert.deepEqual(applyGridCommand(dims, { kind: 'configure', patch: { columns: { [total]: { hidden: true } } } }).config.columns[total],
      { hidden: true });
    refused(() => applyGridCommand(dims, { kind: 'configure', patch: { columns: { count: { aggregateFn: 'max' } } } }),
      /the count a pivot of no measure fills with: it has no kind/);
  });

  it('clearSort takes one column\'s sort off, the others kept in order; one the grid is not sorted by is refused', () => {
    const sorted = draft({ sorts: [{ column: 'region', direction: 'asc' }, { column: 'qty', direction: 'desc' },
      { column: 'notional', direction: 'asc' }] });
    assert.deepEqual(applyGridCommand(sorted, { kind: 'clearSort', column: 'qty' }), clearSort(sorted, 'qty'));
    assert.deepEqual(applyGridCommand(sorted, { kind: 'clearSort', column: 'qty' }).snapshot.sorts,
      [{ column: 'region', direction: 'asc' }, { column: 'notional', direction: 'asc' }]);
    refused(() => applyGridCommand(sorted, { kind: 'clearSort', column: 'desk' }),
      /the grid is not sorted by 'desk' \(it is sorted by region, qty, notional\)/);
    refused(() => applyGridCommand(draft(), { kind: 'clearSort', column: 'qty' }), /not sorted by 'qty' \(it has no sorts\)/);
  });

  it('filter sets the filter whole, and null takes it off', () => {
    const one = { kind: 'condition' as const, column: 'region', operator: 'equal' as const, value: 'EMEA' };
    const on = applyGridCommand(d, { kind: 'filter', filter: one });
    assert.deepEqual(on.snapshot.filter, one);
    assert.equal('filter' in applyGridCommand(on, { kind: 'filter', filter: null }).snapshot, false);
  });

  it('addFilter and-s a condition with the filter the grid has, as a cell\'s menu does', () => {
    const a = { kind: 'condition' as const, column: 'region', operator: 'equal' as const, value: 'EMEA' };
    const b = { kind: 'condition' as const, column: 'desk', operator: 'equal' as const, value: 'FX' };
    const c = { kind: 'condition' as const, column: 'qty', operator: 'lessThan' as const, value: 3 };
    const one = applyGridCommand(d, { kind: 'addFilter', condition: a });
    assert.deepEqual(one.snapshot.filter, a);
    const two = applyGridCommand(one, { kind: 'addFilter', condition: b });
    assert.deepEqual(two.snapshot.filter, { kind: 'and', children: [a, b] });
    assert.deepEqual(applyGridCommand(two, { kind: 'addFilter', condition: c }).snapshot.filter, { kind: 'and', children: [a, b, c] });
  });

  it('calculate adds the column at its level, both lists written as the column editor writes them', () => {
    const row = applyGridCommand(d, { kind: 'calculate', level: 'row', column: double });
    assert.deepEqual(row.snapshot.derived, [double]);
    assert.deepEqual(row.snapshot.groupDerived, []);
    const group = applyGridCommand(d, { kind: 'calculate', level: 'group', column: { ...double, name: 'g' } });
    assert.deepEqual(group.snapshot.derived, []);
    assert.deepEqual(group.snapshot.groupDerived, [{ ...double, name: 'g' }]);
    // a name the grid has is refused, in the editor's words
    refused(() => applyGridCommand(d, { kind: 'calculate', level: 'row', column: { ...double, name: 'qty' } }), /qty/);
  });

  it('a top N set, replaced and taken off; refused on a pivot, whose figures are its values\' columns, and a pivot under it', () => {
    const top = applyGridCommand(d, { kind: 'topN', topN: { by: 'notional', n: 10 } });
    assert.deepEqual(top.snapshot.topN, { by: 'notional', n: 10 });
    assert.deepEqual(applyGridCommand(top, { kind: 'topN', topN: { by: 'qty', n: 3, bottom: true } }).snapshot.topN,
      { by: 'qty', n: 3, bottom: true });
    assert.equal('topN' in applyGridCommand(top, { kind: 'topN', topN: null }).snapshot, false);
    const pivoted = { ...d, snapshot: { ...d.snapshot, pivotOn: ['region'] } };
    refused(() => applyGridCommand(pivoted, { kind: 'topN', topN: { by: 'notional', n: 10 } }), /take the pivot off first/);
    // the other way round (2026-10-24, found by the undo's random walks: a pivot set under a top N was a state no command
    // makes again)
    refused(() => applyGridCommand(top, { kind: 'pivot', columns: ['region'] }), /take the top N off first/);
    assert.deepEqual(applyGridCommand(top, { kind: 'pivot', columns: [] }).snapshot.pivotOn, [], 'no pivot is no pivot');
  });

  // 2026-10-24, found by the round-trip tests once the random walks set format codes: a null code was taken, and the
  // saved cube's reader refused the grid written with it
  it('a format code is text: null is refused, as the saved cube\'s reader refuses it; a format without one has none', () => {
    refused(() => applyGridCommand(d, { kind: 'configure', patch: { columns: { notional: { format: { kind: 'auto', code: null } } } } } as never),
      /a format code is text, not null/);
    const coded = applyGridCommand(d, { kind: 'configure', patch: { columns: { notional: { format: { kind: 'auto', code: '#,##0' } } } } });
    assert.equal(coded.config.columns['notional']?.format?.code, '#,##0');
    assert.equal(applyGridCommand(coded, { kind: 'configure', patch: { columns: { notional: { format: null } } } }).config.columns['notional']?.format, undefined);
  });

  it('a group opened and closed is the tree the chevron leaves; Collapse All the menu\'s (G1, G2)', () => {
    const shown = { ...draft({ rows: ['region', 'desk'] }), tree: TreeState.empty().withExpandTo(1) };
    const opened = applyGridCommand(shown, { kind: 'open', path: ['EMEA', 'FX'], open: true });
    assert.deepEqual(opened.tree.openPaths.map(parsePathKey), [['EMEA'], ['EMEA', 'FX']], 'its parents open with it');
    // SET, never toggled: asked twice, the same tree (P2-127)
    assert.equal(applyGridCommand(opened, { kind: 'open', path: ['EMEA', 'FX'], open: true }).tree, opened.tree);
    const closed = applyGridCommand(opened, { kind: 'open', path: ['EMEA'], open: false });
    assert.deepEqual(closed.tree.openPaths, [], 'the groups under it close with it');
    assert.equal(closed.tree.isOpen(['APAC']), true, 'the expand level opens the others');
    const all = applyGridCommand(closed, { kind: 'collapseAll' });
    assert.equal(all.tree.isOpen(['APAC']), false, "Collapse All closes the expand level's groups too");
    // Expand All: every group above the detail rows
    const every = applyGridCommand(all, { kind: 'expandAll' });
    assert.equal(every.tree.isOpen(['APAC'], 2), true);
    assert.equal(every.tree.isOpen(['APAC', 'FX'], 2), false, 'the deepest groups open onto detail rows: not asked');
    // a path deeper than the groups (another editor regrouped since the click) opens nothing, and is no refusal
    assert.equal(applyGridCommand(shown, { kind: 'open', path: ['EMEA', 'FX', 'x'], open: true }), shown);
    refused(() => applyGridCommand(shown, { kind: 'open', path: [], open: true }), /the grand total is no group/);
    refused(() => applyGridCommand(shown, { kind: 'open', path: ['EMEA', `${DETAIL_ROW}0`], open: true }), /a detail row/);
    // an editor's draft holds no open groups: a bug, not a person's edit
    assert.throws(() => applyGridCommand(draft(), { kind: 'collapseAll' }), (e: unknown) => !(e instanceof CubeRefusal));
  });

  it('a calculated column edited in its place, renamed with what reads it, and taken out (G4, G5)', () => {
    const withTwo = applyGridCommands(d, [
      { kind: 'calculate', level: 'row', column: double },
      { kind: 'calculate', level: 'row', column: { ...double, name: 'triple', lambda: stand('$x.qty * 3') } },
      { kind: 'sorts', sorts: [{ column: 'double', direction: 'desc' }] },
      { kind: 'configure', patch: { columns: { double: { displayName: 'Twice qty' } } } },
    ]);
    const edited = applyGridCommand(withTwo, { kind: 'editCalculated', name: 'double', level: 'row',
      column: { ...double, name: 'twice', lambda: stand('$x.qty + $x.qty') } });
    assert.deepEqual(edited.snapshot.derived.map((c) => c.name), ['twice', 'triple'], 'in its place');
    assert.deepEqual(edited.snapshot.sorts, [{ column: 'twice', direction: 'desc' }], 'its sort follows it');
    assert.equal(edited.config.columns['twice']?.displayName, 'Twice qty', 'its settings follow it');
    assert.equal(edited.config.columns['double'], undefined);
    // another level: last of it
    const moved = applyGridCommand(withTwo, { kind: 'editCalculated', name: 'double', level: 'group',
      column: { name: 'double', childAggregate: { fn: 'min', of: 'qty' } } });
    assert.deepEqual([moved.snapshot.derived.map((c) => c.name), moved.snapshot.groupDerived?.map((c) => c.name)],
      [['triple'], ['double']]);
    refused(() => applyGridCommand(withTwo, { kind: 'editCalculated', name: 'qty', level: 'row', column: double }),
      /'qty' is not a calculated column of the grid \(double, triple\)/);
    refused(() => applyGridCommand(withTwo, { kind: 'editCalculated', name: 'double', level: 'row',
      column: { ...double, name: 'triple' } }), /'triple' is already a column/);
    const removed = applyGridCommand(withTwo, { kind: 'removeCalculated', name: 'triple' });
    assert.deepEqual(removed.snapshot.derived.map((c) => c.name), ['double']);
    refused(() => applyGridCommand(d, { kind: 'removeCalculated', name: 'region' }), /'region' is not a calculated column/);
  });

  it("a chart's mark: its old conditions taken off where they stand, the new ANDed on (G7)", () => {
    const desk = { kind: 'condition', column: 'desk', operator: 'equal', value: 'FX' } as const;
    const emea = { kind: 'condition', column: 'region', operator: 'equal', value: 'EMEA' } as const;
    const apac = { kind: 'condition', column: 'region', operator: 'equal', value: 'APAC' } as const;
    const filtered = applyGridCommand(d, { kind: 'filter', filter: { kind: 'and', children: [desk, emea] } });
    const moved = applyGridCommand(filtered, { kind: 'refilter', remove: [emea], add: [apac] });
    assert.deepEqual(moved.snapshot.filter, { kind: 'and', children: [desk, apac] }, 'the rest of the filter kept');
    const cleared = applyGridCommand(applyGridCommand(d, { kind: 'refilter', remove: [], add: [emea] }),
      { kind: 'refilter', remove: [emea], add: [] });
    assert.equal(cleared.snapshot.filter, undefined, 'a selection cleared leaves no filter of its own');
  });

  it('a list applies in order and stops at the first refusal', () => {
    const done = applyGridCommands(d, [{ kind: 'pivot', columns: ['desk'] }, { kind: 'addSort', column: 'qty', direction: 'asc' }]);
    assert.deepEqual(done.snapshot.pivotOn, ['desk']);
    assert.deepEqual(done.snapshot.sorts, [{ column: 'qty', direction: 'asc' }]);
    refused(() => applyGridCommands(d, [{ kind: 'group', rows: ['nope'] }, { kind: 'pivot', columns: ['desk'] }]), /nope/);
  });
});

describe("an editor's draft as commands (the Properties window's Apply: draftCommands)", () => {
  const d = draft();
  const patched = (x: GridDraft, patch: Parameters<typeof withConfiguration>[1]): GridDraft =>
    ({ ...x, config: withConfiguration(x.config, patch) });

  it('builds nothing for a draft unchanged', () => {
    assert.deepEqual(draftCommands(d, d), []);
  });

  it('builds only what the draft changed, and lands it on the grid as it is now: another window\'s edit stays', () => {
    const edited = patched(setGrouping(d, ['region', 'desk']), { reportTitle: 'Trades', appearance: { fontSize: 13 } });
    const commands = draftCommands(d, edited);
    assert.deepEqual(commands, [
      { kind: 'group', rows: ['region', 'desk'] },
      { kind: 'configure', patch: { reportTitle: 'Trades', appearance: { fontSize: 13 } } },
    ]);
    // meanwhile: a row limit set, a column pinned, a sort clicked -- each kept, as the three-way merge kept them
    const now = clickSort(patched(d, { maxRows: 50, columns: { qty: { pinned: 'left' } } }), 'qty', 'desc', false);
    const landed = applyGridCommands(now, commands);
    assert.deepEqual(landed.snapshot.rows, ['region', 'desk']);
    assert.deepEqual(landed.snapshot.sorts, [{ column: 'qty', direction: 'desc' }]);
    assert.equal(landed.config.maxRows, 50);
    assert.equal(landed.config.columns['qty']?.pinned, 'left');
    assert.equal(landed.config.appearance.fontSize, 13);
  });

  it('a column made a dimension comes before the grouping on it; one ungrouped, before it is made a measure', () => {
    const grouped = setGrouping(setColumn(d, 'qty', { kind: 'dimension' }), ['region', 'qty']);
    assert.deepEqual(draftCommands(d, grouped), [
      { kind: 'column', name: 'qty', change: { kind: 'dimension' } },
      { kind: 'group', rows: ['region', 'qty'] },
    ]);
    assert.deepEqual(applyGridCommands(d, draftCommands(d, grouped)), grouped);
    const back = setColumn(setGrouping(grouped, ['region']), 'qty', { kind: 'measure' });
    assert.deepEqual(draftCommands(grouped, back), [
      { kind: 'group', rows: ['region'] },
      { kind: 'column', name: 'qty', change: { kind: 'measure' } },
    ]);
    assert.deepEqual(applyGridCommands(grouped, draftCommands(grouped, back)), back);
  });

  it("Column Properties is the column command alone -- what setColumn sets with it (a kind's pivot exclusion) is not said again", () => {
    assert.deepEqual(draftCommands(d, setColumn(d, 'desk', { kind: 'measure' })),
      [{ kind: 'column', name: 'desk', change: { kind: 'measure' } }]);
    const wavg = setColumn(setColumn(d, 'notional', { aggregate: 'wavg' }), 'notional', { weight: 'qty' });
    assert.deepEqual(draftCommands(d, wavg), [{ kind: 'column', name: 'notional', change: { aggregate: 'wavg', weight: 'qty' } }]);
    // and cleared, as the panel's "none" clears them
    assert.deepEqual(draftCommands(wavg, d), [{ kind: 'column', name: 'notional', change: { aggregate: null, weight: null } }]);
  });

  it('a setting cleared is null in the patch, an appearance and a column key by key, the sorts last', () => {
    const was = patched(d, { reportTitle: 'Trades', columns: { qty: { width: 90, hidden: true } } });
    const cleared = patched(was, { reportTitle: undefined });
    const edited = setSorts({ ...cleared, config: withColumn(cleared.config, 'qty', { width: undefined }) },
      [{ column: 'qty', direction: 'asc' }]);
    assert.deepEqual(draftCommands(was, edited), [
      { kind: 'configure', patch: { reportTitle: null, columns: { qty: { width: null } } } },
      { kind: 'sorts', sorts: [{ column: 'qty', direction: 'asc' }] },
    ]);
    assert.deepEqual(applyGridCommands(was, draftCommands(was, edited)), edited);
  });
});

describe('a command read from JSON is the same command, and what DataCube does not know is refused', () => {
  const every: GridCommand[] = [
    { kind: 'group', rows: ['region'] },
    { kind: 'pivot', columns: [] },
    { kind: 'sorts', sorts: [{ column: 'qty', direction: 'desc' }] },
    { kind: 'addSort', column: 'qty', direction: 'asc' },
    { kind: 'clearSort', column: 'qty' },
    { kind: 'clickSort', column: 'qty', first: 'desc', chained: true },
    { kind: 'clickTreeSort', chained: false },
    { kind: 'column', name: 'qty', change: { kind: 'dimension', aggregate: null, weight: null } },
    { kind: 'configure', patch: { columns: { qty: { heatmap: null } } } },
    { kind: 'filter', filter: null },
    { kind: 'filter', filter: { kind: 'and', children: [
      { kind: 'condition', column: 'region', operator: 'in', value: ['EMEA', 'APAC'] },
      { kind: 'not', child: { kind: 'condition', column: 'qty', operator: 'greaterThanColumn', rightColumn: 'notional' } },
      { kind: 'condition', column: 'desk', operator: 'isEmpty' },
    ] } },
    { kind: 'addFilter', condition: { kind: 'condition', column: 'region', operator: 'equal', value: 'EMEA' } },
    { kind: 'refilter', remove: [{ kind: 'condition', column: 'region', operator: 'equal', value: 'EMEA' }],
      add: [{ kind: 'condition', column: 'region', operator: 'equal', value: 'APAC' }] },
    { kind: 'calculate', level: 'row', column: double },
    // every column kind the screen's editor and its presets make (Show As: a window; a share: a child aggregate; explode)
    { kind: 'calculate', level: 'row', column: { name: 'running', window: { fn: 'sum', column: 'notional', partition: ['desk'],
      order: [{ column: 'region', direction: 'asc' }], frame: 'running' } } },
    { kind: 'calculate', level: 'row', column: { name: 'moving', window: { fn: 'average', column: 'notional', partition: [],
      order: [{ column: 'region', direction: 'desc' }], frame: { lastRows: 3 } } } },
    { kind: 'calculate', level: 'row', column: { name: 'previous', window: { fn: 'lag', column: 'qty', partition: [], order: [], offset: 2 } } },
    { kind: 'calculate', level: 'row', column: { name: 'bucket', window: { fn: 'ntile', partition: [], order: [], buckets: 4 } } },
    { kind: 'calculate', level: 'group', column: { name: 'smallest', childAggregate: { fn: 'min', of: 'notional' }, kind: 'measure' } },
    { kind: 'calculate', level: 'row', column: { ...double, name: 'each', unnest: true } },
    { kind: 'editCalculated', name: 'double', level: 'group', column: { ...double, name: 'twice' } },
    // in its place: before another of its level (an undo's: D19)
    { kind: 'calculate', level: 'row', column: { ...double, name: 'first' }, before: 'double' },
    { kind: 'editCalculated', name: 'double', level: 'row', column: double, before: 'twice' },
    { kind: 'removeCalculated', name: 'double' },
    { kind: 'open', path: ['EMEA', null], open: true },
    { kind: 'collapseAll' },
    { kind: 'expandAll' },
    { kind: 'topN', topN: { by: 'notional', n: 3, bottom: true } },
    // a Cross Tab's values pinned, and taken off (FL2)
    { kind: 'pinPivot', values: ['EMEA', 'APAC'] },
    { kind: 'pinPivot', values: null },
    { kind: 'tree', open: [['EMEA'], ['EMEA', 'FX']], showTotals: true, expandTo: 1, closed: [['AMER']] },
  ];

  it('every kind, through JSON and back', () => {
    assert.deepEqual([...new Set(every.map((c) => c.kind))].sort(), [...GRID_COMMAND_KINDS].sort(), 'every kind is here');
    for (const c of every) assert.deepEqual(readGridCommand(JSON.parse(JSON.stringify(c))), c, c.kind);
  });

  it('an unknown kind, a misspelt field and a field of the wrong type, each in words', () => {
    refused(() => readGridCommand({ kind: 'regroup', rows: [] }), /'regroup' is not a grid's command/);
    refused(() => readGridCommand({ kind: 'open', path: ['EMEA', 2021], open: true }), /each text or null, not \["EMEA",2021\]/);
    refused(() => readGridCommand({ kind: 'open', path: 'EMEA', open: true }), /a group's path is its keys/);
    refused(() => readGridCommand({ kind: 'open', path: ['EMEA'] }), /open is true or false/);
    refused(() => readGridCommand({ kind: 'collapseAll', path: [] }), /a 'collapseAll' command takes nothing more, not path/);
    refused(() => readGridCommand({ kind: 'expandAll', level: 2 }), /'expandAll' command takes nothing more, not level/);
    refused(() => readGridCommand({ kind: 'topN', topN: { by: 'notional', n: 0 } }), /a top N's n is a whole number from 1/);
    refused(() => readGridCommand({ kind: 'topN', topN: { by: 'notional', n: 2, top: true } }), /a top N takes by, n and bottom, not top/);
    refused(() => readGridCommand({ kind: 'editCalculated', name: 'double', level: 'cell', column: double }), /'row' or 'group'/);
    refused(() => readGridCommand({ kind: 'removeCalculated', name: 7 }), /the calculated column's name is text/);
    refused(() => readGridCommand({ kind: 'group', row: ['region'] }), /a 'group' command takes rows, not row/);
    refused(() => readGridCommand({ kind: 'addSort', column: 'qty', direction: 'up' }), /a sort is 'asc' or 'desc'/);
    refused(() => readGridCommand({ kind: 'clickSort', column: 'qty', first: 'desc' }), /chained or not/);
    refused(() => readGridCommand({ kind: 'clickSort', column: 'qty', chained: false }), /the direction it starts with/);
    refused(() => readGridCommand({ kind: 'column', name: 'qty', change: { aggregateFn: 'max' } }),
      /a column's properties are kind, aggregate and weight, not aggregateFn/);
    refused(() => readGridCommand({ kind: 'column', name: 'qty', change: { aggregate: 'mean' } }), /"mean" is not one of DataCube's aggregates/);
    refused(() => readGridCommand({ kind: 'addFilter', condition: { kind: 'and', children: [] } }), /one condition/);
    // a filter, field by field: a field misspelt, an operator DataCube has not, an operand the operator does not take
    const condition = (c: Record<string, unknown>): unknown => ({ kind: 'filter', filter: { kind: 'condition', ...c } });
    refused(() => readGridCommand(condition({ colum: 'region', operator: 'equal', value: 'EMEA' })),
      /a condition takes column, operator, value, rightColumn, not colum/);
    refused(() => readGridCommand(condition({ column: 'region', operator: 'bogus', value: 'EMEA' })), /a condition's operator is one of/);
    refused(() => readGridCommand(condition({ column: 'region', operator: 'equal' })), /'equal' takes a value/);
    refused(() => readGridCommand(condition({ column: 'region', operator: 'in', value: 'EMEA' })), /'in' takes a list of values/);
    refused(() => readGridCommand(condition({ column: 'region', operator: 'equal', value: { at: 1 } })), /a filter's value is/);
    refused(() => readGridCommand({ kind: 'filter', filter: { kind: 'xor', children: [] } }), /a filter is a condition, an and/);
    // a calculated column, a formula's fields: its lambda the protocol's, its kind one of DataCube's, its name trimmed
    const calc = (c: Record<string, unknown>): unknown => ({ kind: 'calculate', level: 'row', column: c });
    refused(() => readGridCommand(calc({ name: 'x', lambda: 42 })), /lambda is the protocol's/);
    refused(() => readGridCommand(calc({ name: 'x', lambda: double.lambda, kind: 'bogus' })), /a column's kind is/);
    refused(() => readGridCommand(calc({ name: 'x', lambda: double.lambda, window: {} })),
      /a calculated column is one of a lambda, a window, a child aggregate, a grain, a related property or value groups, not lambda and window/);
    refused(() => readGridCommand(calc({ name: 'x' })), /a calculated column is one of a lambda, a window, a child aggregate, a grain, a related property or value groups, not none/);
    refused(() => readGridCommand(calc({ name: 'x', lambda: double.lambda, colour: 'red' })),
      /a calculated column from JSON takes name, lambda, window, childAggregate, grain, related, groups, unnest, kind, type, not colour/);
    // a measure at a grain: its measure, and its parent's or the grand total's
    const grain = (g: unknown): unknown => ({ kind: 'calculate', level: 'group', column: { name: 'g', grain: g } });
    assert.deepEqual(readGridCommand(grain({ of: 'notional', at: 'all' })),
      { kind: 'calculate', level: 'group', column: { name: 'g', grain: { of: 'notional', at: 'all' } } });
    refused(() => readGridCommand(grain({ of: 'notional', at: 'grandparent' })), /its parent's \('parent'\) or the grand total's \('all'\), not "grandparent"/);
    refused(() => readGridCommand(grain({ of: 'notional', at: 'all', by: 'region' })), /a measure at a grain takes of, at, not by/);
    // a window, field by field
    const win = (w: Record<string, unknown>): unknown => calc({ name: 'w', window: { partition: [], order: [], ...w } });
    refused(() => readGridCommand(win({ fn: 'median' })), /a window's function is one of sum, average/);
    refused(() => readGridCommand(win({ fn: 'sum', column: 'qty', frame: 'rolling' })), /a window's frame is 'running', 'partition' or \{lastRows\}/);
    refused(() => readGridCommand(win({ fn: 'sum', column: 'qty', frame: { lastRows: 0 } })), /lastRows is a whole number of rows, 1 or more/);
    refused(() => readGridCommand(win({ fn: 'lag', column: 'qty', offset: 1.5 })), /a window's offset is a whole number, 1 or more/);
    refused(() => readGridCommand(win({ fn: 'sum', column: 'qty', order: [{ column: 'qty' }] })), /a sort is 'asc' or 'desc'/);
    refused(() => readGridCommand(win({ fn: 'sum', column: 'qty', ordr: [] })), /a window takes fn, column, partition, order, frame, offset, buckets, not ordr/);
    // a child aggregate, and explode
    refused(() => readGridCommand(calc({ name: 'c', childAggregate: { fn: 'mode', of: 'qty' } })), /a child aggregate's function is one of min, max/);
    refused(() => readGridCommand(calc({ name: 'c', childAggregate: { fn: 'min' } })), /a child aggregate's column is text/);
    refused(() => readGridCommand(calc({ name: 'e', lambda: double.lambda, unnest: 'yes' })), /unnest is true or false/);
    refused(() => readGridCommand(calc({ name: 'e', childAggregate: { fn: 'min', of: 'qty' }, unnest: true })), /explode is a formula's/);
    const read = readGridCommand(calc({ name: '  pad  ', lambda: double.lambda }));
    assert.equal(read.kind === 'calculate' ? read.column.name : undefined, 'pad');
    refused(() => readGridCommand(['group']), /a grid's command is \{kind, \.\.\.\}/);
  });
});
