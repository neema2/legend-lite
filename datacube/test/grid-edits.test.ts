import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { PIVOT_SEPARATOR } from '../../engine-client/src/generated/lite-facts.ts';
import { DEFAULT_CONFIGURATION, applyToSnapshot, columnConfig, withColumn } from '../src/config.ts';
import {
  addSort, allColumns, clickSort, clickTreeSort, columnKind, copiedColumn, firstSortDirection, groupableColumns,
  headerSorts, measureAsColumn, measuresAsColumns, offeredAggregates, setColumn, setGrouping, setPivot, setSorts, type GridDraft,
} from '../src/grid-edits.ts';
import {
  AGGREGATE_FNS, CubeRefusal, pivotTotalColumn, refuseMeasuresList, type CubeSnapshot, type QuerySnapshot,
} from '../src/snapshot.ts';
import { element, lambda, property, variable } from '../../pure-protocol/src/index.ts';
import { stand } from './fake-planner.ts';

// ONE WAY TO EDIT A GRID (docs/DATACUBE_ONE_GRID_EDIT_PATH_2026_10_17.md): the screen's offers, as one module the
// screen's panels, its header menu, its drag zones and Python's calls (headless gridEdit) all call.

const CUBE: CubeSnapshot = {
  source: { query: element('t') },
  columns: [
    { name: 'region', type: 'String' },
    { name: 'desk', type: 'String' },
    { name: 'year', type: 'Integer' },
    { name: 'notional', type: 'Float' },
    { name: 'qty', type: 'Integer' },
  ],
  derived: [
    { name: 'big', lambda: stand('$x.notional > 1'), kind: 'dimension', type: 'Boolean' },
    { name: 'fee', lambda: stand('$x.notional * 2'), kind: 'measure', type: 'Float' },
  ],
  groupDerived: [{ name: 'share', lambda: stand('$x.notional / 2'), type: 'Float' }],
  rows: ['region'],
  pivotOn: [],
  sorts: [],
  epoch: 1,
};

const draft = (s: Partial<CubeSnapshot> = {}, config = DEFAULT_CONFIGURATION): GridDraft =>
  ({ snapshot: { ...CUBE, ...s }, config });

const refused = (f: () => unknown, words: RegExp): void => {
  assert.throws(f, (e: unknown) => e instanceof CubeRefusal && words.test(e.message));
};

describe('the offers', () => {
  it('groups by dimensions: a source column by its kind as set, a calculated one by its editor\'s', () => {
    assert.deepEqual(groupableColumns(draft()), ['region', 'desk', 'big']);
    // Column Properties makes an integer a dimension
    const year = withColumn(DEFAULT_CONFIGURATION, 'year', { kind: 'dimension' });
    assert.deepEqual(groupableColumns(draft({}, year)), ['region', 'desk', 'year', 'big']);
    // a calculated column's kind set in Column Properties is no query's: its editor's stands
    const fee = withColumn(DEFAULT_CONFIGURATION, 'fee', { kind: 'dimension' });
    assert.equal(columnKind(draft({}, fee), 'fee'), 'measure');
    assert.equal(columnKind(draft(), 'share'), undefined);
  });

  it('lists every column, its calculated ones too', () => {
    assert.deepEqual(allColumns(draft()), ['region', 'desk', 'year', 'notional', 'qty', 'big', 'fee', 'share']);
  });

  it('offers a column the aggregates of its own family, every one before it is typed', () => {
    assert.ok(offeredAggregates('Float').includes('sum'));
    assert.ok(!offeredAggregates('String').includes('sum'));
    // a count is an Integer: a text column is not offered one (a group's rows are counted by "Show leaf count")
    assert.ok(!offeredAggregates('String').includes('count'));
    assert.deepEqual(offeredAggregates(undefined), [...AGGREGATE_FNS]);
  });
});

describe('grouping and pivoting', () => {
  it('sets dimensions, outermost first', () => {
    assert.deepEqual(setGrouping(draft(), ['region', 'desk']).snapshot.rows, ['region', 'desk']);
    assert.deepEqual(setPivot(draft(), ['big']).snapshot.pivotOn, ['big']);
  });

  it('refuses a measure, a column the grid has not, and one named twice, in the screen\'s words', () => {
    refused(() => setGrouping(draft(), ['notional']), /'notional' is a measure, and the row groups take dimensions/);
    refused(() => setPivot(draft(), ['nope']), /'nope' is not a column the pivot can take: .*region/);
    refused(() => setGrouping(draft(), ['desk', 'desk']), /the row groups name 'desk' twice/);
    // a group-level column exists only after grouping
    refused(() => setGrouping(draft(), ['share']), /'share' is not a column the row groups can take/);
  });

  it('keeps or takes away a group the grid has, whatever it is: the offers judge what is added', () => {
    // a saved cube can hold a key its offers would not add now (old Python grouped by any column)
    const held = draft({ rows: ['qty', 'region'] });
    assert.deepEqual(setGrouping(held, ['qty']).snapshot.rows, ['qty']);
    assert.deepEqual(setGrouping(held, ['qty', 'region', 'desk']).snapshot.rows, ['qty', 'region', 'desk']);
  });
});

describe('sorting', () => {
  it('sorts by a column the grid has, a calculated one too', () => {
    const s = setSorts(draft(), [{ column: 'notional', direction: 'desc' }, { column: 'share', direction: 'asc' }]);
    assert.deepEqual(s.snapshot.sorts, [{ column: 'notional', direction: 'desc' }, { column: 'share', direction: 'asc' }]);
  });

  it('pivoted, sorts by a pivot\'s header -- its values, then a column it spreads -- and a pivot total', () => {
    const pivoted = draft({ pivotOn: ['desk'] });
    const header = `FX${PIVOT_SEPARATOR}notional`;
    assert.deepEqual(setSorts(pivoted, [{ column: header, direction: 'asc' }]).snapshot.sorts,
      [{ column: header, direction: 'asc' }]);
    assert.deepEqual(setSorts(pivoted, [{ column: pivotTotalColumn('notional'), direction: 'desc' }]).snapshot.sorts,
      [{ column: pivotTotalColumn('notional'), direction: 'desc' }]);
    // not pivoted, a header is no column of the grid's
    refused(() => setSorts(draft(), [{ column: header, direction: 'asc' }]), /which the grid does not have/);
  });

  it('unpivoted, lets a pivot header\'s sort go with its headers, and keeps a column\'s', () => {
    const sorts = [{ column: `FX${PIVOT_SEPARATOR}notional`, direction: 'asc' as const },
      { column: 'notional', direction: 'desc' as const }];
    const pivoted = draft({ pivotOn: ['desk'], sorts });
    assert.deepEqual(setPivot(pivoted, ['region']).snapshot.sorts, sorts);
    assert.deepEqual(setPivot(pivoted, []).snapshot.sorts, [{ column: 'notional', direction: 'desc' }]);
  });

  it('refuses a column the grid has not, one named twice and a direction of no sort', () => {
    refused(() => setSorts(draft(), [{ column: 'nope', direction: 'asc' }]),
      /the sort names 'nope', which the grid does not have: one of its columns \(big, desk/);
    refused(() => setSorts(draft(), [{ column: 'desk', direction: 'asc' }, { column: 'desk', direction: 'desc' }]),
      /the sorts name 'desk' twice/);
    refused(() => setSorts(draft(), [{ column: 'desk', direction: 'up' as 'asc' }]), /a sort is 'asc' or 'desc', not 'up'/);
  });

  it('pivoted, takes the headers the pivot writes: one made unique, the count a pivot of no measure fills with', () => {
    const pivoted = draft({ pivotOn: ['desk'] });
    const unique = `EMEA${PIVOT_SEPARATOR}notional~2`;
    assert.deepEqual(setSorts(pivoted, [{ column: unique, direction: 'desc' }]).snapshot.sorts, [{ column: unique, direction: 'desc' }]);
    // a grid of dimensions only: its pivot spreads a count
    const dims = draft({ pivotOn: ['desk'], rows: ['region'], columns: CUBE.columns.slice(0, 2), derived: [], groupDerived: [] });
    const filler = `EMEA${PIVOT_SEPARATOR}count`;
    assert.deepEqual(setSorts(dims, [{ column: filler, direction: 'asc' }]).snapshot.sorts, [{ column: filler, direction: 'asc' }]);
    // a header of no measure the pivot spreads is a typo, refused
    refused(() => setSorts(pivoted, [{ column: `FX${PIVOT_SEPARATOR}notionl`, direction: 'asc' }]), /which the grid does not have/);
    refused(() => setSorts(pivoted, [{ column: filler, direction: 'asc' }]), /which the grid does not have/);
  });

  it('adds a sort last, as the menu\'s Add Ascending: the column\'s sort before taken away; one it has changes nothing', () => {
    const two = addSort(addSort(draft(), 'qty', 'asc'), 'desk', 'desc');
    assert.deepEqual(two.snapshot.sorts, [{ column: 'qty', direction: 'asc' }, { column: 'desk', direction: 'desc' }]);
    assert.deepEqual(addSort(two, 'qty', 'desc').snapshot.sorts,
      [{ column: 'desk', direction: 'desc' }, { column: 'qty', direction: 'desc' }]);
    assert.equal(addSort(two, 'qty', 'asc'), two, 'an add that would change nothing');
    refused(() => addSort(draft(), 'nope', 'asc'), /the sort names 'nope'/);
  });

  it('keeps a sort the grid has, and changes its direction, whatever it names', () => {
    const held = draft({ sorts: [{ column: `FX${PIVOT_SEPARATOR}notional`, direction: 'asc' }] });
    const flipped = setSorts(held, [{ column: `FX${PIVOT_SEPARATOR}notional`, direction: 'desc' }]);
    assert.deepEqual(flipped.snapshot.sorts, [{ column: `FX${PIVOT_SEPARATOR}notional`, direction: 'desc' }]);
  });
});

describe('a header click (the user, 2026-10-18): first, the other way, none; alone, or chained with Ctrl or Cmd', () => {
  const sorts = (d: GridDraft): string[] => d.snapshot.sorts.map((x) => `${x.column} ${x.direction}`);

  it('sorts a number or a date from the largest, the latest, first; anything else from A', () => {
    for (const t of ['Integer', 'Float', 'Decimal', 'Number', 'StrictDate', 'DateTime', 'Date']) {
      assert.equal(firstSortDirection(t), 'desc', t);
    }
    for (const t of ['String', 'Boolean', 'Variant', undefined]) assert.equal(firstSortDirection(t), 'asc', String(t));
  });

  it('a plain click cycles its column -- first, the other way, none -- and sorts by it alone', () => {
    let d = draft();
    d = clickSort(d, 'notional', 'desc', false);
    assert.deepEqual(sorts(d), ['notional desc']);
    d = clickSort(d, 'notional', 'desc', false);
    assert.deepEqual(sorts(d), ['notional asc']);
    d = clickSort(d, 'notional', 'desc', false);
    assert.deepEqual(sorts(d), []);
    // text starts from A
    d = clickSort(d, 'region', 'asc', false);
    assert.deepEqual(sorts(d), ['region asc']);
    d = clickSort(d, 'region', 'asc', false);
    assert.deepEqual(sorts(d), ['region desc']);
  });

  it('a plain click on another column takes every other sort off; on a sorted one it turns that one, alone', () => {
    const two = draft({ sorts: [{ column: 'region', direction: 'asc' }, { column: 'notional', direction: 'desc' }] });
    assert.deepEqual(sorts(clickSort(two, 'qty', 'desc', false)), ['qty desc']);
    assert.deepEqual(sorts(clickSort(two, 'notional', 'desc', false)), ['notional asc']);
    // its other way clicked once more: no sort at all
    const asc = draft({ sorts: [{ column: 'region', direction: 'asc' }, { column: 'notional', direction: 'asc' }] });
    assert.deepEqual(sorts(clickSort(asc, 'notional', 'desc', false)), []);
  });

  it('a chained click adds its column last, then turns it in its place, then takes it off -- the others kept', () => {
    let d = draft({ sorts: [{ column: 'region', direction: 'asc' }] });
    d = clickSort(d, 'notional', 'desc', true);
    assert.deepEqual(sorts(d), ['region asc', 'notional desc']);
    d = clickSort(d, 'qty', 'desc', true);
    assert.deepEqual(sorts(d), ['region asc', 'notional desc', 'qty desc']);
    d = clickSort(d, 'notional', 'desc', true);
    assert.deepEqual(sorts(d), ['region asc', 'notional asc', 'qty desc']);
    d = clickSort(d, 'notional', 'desc', true);
    assert.deepEqual(sorts(d), ['region asc', 'qty desc']);
  });

  it('a pivot total and a pivot header sort by a click as by the menu', () => {
    const pivoted = draft({ pivotOn: ['year'] });
    const total = pivotTotalColumn('notional');
    assert.deepEqual(sorts(clickSort(pivoted, total, 'desc', false)), [`${total} desc`]);
    const header = `2024${PIVOT_SEPARATOR}notional`;
    assert.deepEqual(sorts(clickSort(pivoted, header, 'desc', true)), [`${header} desc`]);
  });

  it('a click on a column the grid does not have is refused, as every sort is', () => {
    refused(() => clickSort(draft(), 'nope', 'asc', false), /the sort names 'nope'/);
  });

  it('the tree column orders the groups: it turns, never "none"; a plain click makes the groups the whole sort', () => {
    const asc = draft();
    assert.equal(asc.config.treeColumnSort, 'asc');
    // the groups already the whole sort: a plain click turns them
    const turned = clickTreeSort(asc, false);
    assert.equal(turned.config.treeColumnSort, 'desc');
    assert.equal(clickTreeSort(turned, false).config.treeColumnSort, 'asc');
    // a measure orders the groups (the tree shows nothing): a plain click takes it off, the groups from A
    const sorted = draft({ sorts: [{ column: 'notional', direction: 'desc' }] }, { ...DEFAULT_CONFIGURATION, treeColumnSort: 'desc' });
    const alone = clickTreeSort(sorted, false);
    assert.deepEqual(sorts(alone), []);
    assert.equal(alone.config.treeColumnSort, 'asc');
    // with no sort applied there is nothing to chain after: a chained click is a plain one
    assert.equal(clickTreeSort(asc, true).config.treeColumnSort, 'desc');
  });

  it('a plain tree click turns the order the tree shows, even when a sort of the first group shows it (the audit of #57)', () => {
    // region sorted Z-A before it was grouped: the groups come back Z-A whatever treeColumnSort says
    const byRegion = draft({ sorts: [{ column: 'region', direction: 'desc' }] }, { ...DEFAULT_CONFIGURATION, treeColumnSort: 'desc' });
    const turned = clickTreeSort(byRegion, false);
    assert.deepEqual(sorts(turned), []);
    assert.equal(turned.config.treeColumnSort, 'asc', 'it was desc: kept, the click would have changed nothing on screen');
  });

  it('a chained tree click chains the first group, as a click chains any column: added last, turned, taken off', () => {
    let d = draft({ sorts: [{ column: 'notional', direction: 'desc' }] });
    d = clickTreeSort(d, true);
    assert.deepEqual(sorts(d), ['notional desc', 'region asc']);
    d = clickTreeSort(d, true);
    assert.deepEqual(sorts(d), ['notional desc', 'region desc']);
    d = clickTreeSort(d, true);
    assert.deepEqual(sorts(d), ['notional desc']);
    assert.equal(d.config.treeColumnSort, 'asc', 'the tree\'s own direction never moved');
  });

  it('a grid with no row groups has no tree to order', () => {
    refused(() => clickTreeSort(draft({ rows: [] }), false), /no row groups/);
  });
});

describe('the headers\' sort marks: what orders the rows, as the query orders them (the audit of #57)', () => {
  const TREE = '__tree';
  const headed = ['__tree', 'notional', 'qty', pivotTotalColumn('notional'), 'weakest'];
  const folded = (s: Partial<CubeSnapshot>): CubeSnapshot =>
    applyToSnapshot({ ...CUBE, ...s }, DEFAULT_CONFIGURATION);

  it('with no sort applied, the tree shows its own direction: it orders the groups', () => {
    assert.deepEqual(headerSorts(folded({}), headed, TREE), [{ column: TREE, direction: 'asc' }]);
    const desc = applyToSnapshot(CUBE, { ...DEFAULT_CONFIGURATION, treeColumnSort: 'desc' });
    assert.deepEqual(headerSorts(desc, headed, TREE), [{ column: TREE, direction: 'desc' }]);
  });

  it('a sort of the first group shows on the tree, its header being the tree; the tree\'s own direction does not', () => {
    const s = folded({ sorts: [{ column: 'region', direction: 'desc' }] });
    assert.deepEqual(headerSorts(s, headed, TREE), [{ column: TREE, direction: 'desc' }]);
  });

  it('a measure ordering the groups shows on its header, and the tree shows nothing', () => {
    const s = folded({ sorts: [{ column: 'notional', direction: 'desc' }] });
    assert.deepEqual(headerSorts(s, headed, TREE), [{ column: 'notional', direction: 'desc' }]);
  });

  it('a sort the query leaves out is neither marked nor counted: a total\'s with totals off, a child aggregate\'s', () => {
    const total = pivotTotalColumn('notional');
    const { pivotStatisticColumnPlacement: _totals, ...noTotals } = DEFAULT_CONFIGURATION;
    const s = applyToSnapshot({
      ...CUBE,
      pivotOn: ['desk'],
      groupDerived: [{ name: 'weakest', childAggregate: { fn: 'min', of: 'notional' } }],
      sorts: [{ column: total, direction: 'desc' }, { column: 'weakest', direction: 'asc' }, { column: 'qty', direction: 'desc' }],
    }, noTotals);
    // qty is the first sort the rows have: its mark is alone, so it carries no position
    assert.deepEqual(headerSorts(s, headed, TREE), [{ column: 'qty', direction: 'desc' }]);
  });

  it('a group column with a header of its own (kept beside the tree) is marked on it', () => {
    const s = folded({ sorts: [{ column: 'region', direction: 'desc' }] });
    assert.deepEqual(headerSorts(s, [...headed, 'region'], TREE), [{ column: 'region', direction: 'desc' }]);
  });

  it('a chain keeps the query\'s order, the first group\'s place on the tree', () => {
    const s = folded({ sorts: [{ column: 'notional', direction: 'desc' }, { column: 'region', direction: 'asc' }] });
    assert.deepEqual(headerSorts(s, headed, TREE),
      [{ column: 'notional', direction: 'desc' }, { column: TREE, direction: 'asc' }]);
  });
});

describe('a sort a child group\'s aggregate cannot have (P2-15)', () => {
  const sorts = (d: GridDraft): string[] => d.snapshot.sorts.map((x) => `${x.column} ${x.direction}`);
  const children = (sorts: CubeSnapshot['sorts'] = []): GridDraft =>
    draft({ groupDerived: [{ name: 'weakest', childAggregate: { fn: 'min', of: 'notional' } }], sorts });

  it('is refused, in words, by a click as by the menu', () => {
    refused(() => clickSort(children(), 'weakest', 'desc', false), /'weakest' is worked out from each group's child rows/);
    refused(() => setSorts(children(), [{ column: 'weakest', direction: 'asc' }]), /cannot be sorted by it/);
  });

  it('one a grid already has is kept or taken off as it is', () => {
    const had = children([{ column: 'weakest', direction: 'asc' }]);
    assert.deepEqual(sorts(setSorts(had, [])), []);
    assert.deepEqual(sorts(setSorts(had, [{ column: 'weakest', direction: 'asc' }])), ['weakest asc']);
  });
});

describe('a column\'s Column Properties', () => {
  it('sets a source column\'s kind, the pivot\'s exclusion with it, as the panel does', () => {
    const year = setColumn(draft(), 'year', { kind: 'dimension' });
    assert.deepEqual(columnConfig(year.config, 'year'), { kind: 'dimension', excludedFromPivot: true });
    const back = setColumn(year, 'year', { kind: 'measure' });
    assert.equal(columnConfig(back.config, 'year').excludedFromPivot, false);
  });

  it('refuses a kind the grid could not read: a calculated column\'s, a grouped column\'s, a word of no kind', () => {
    refused(() => setColumn(draft(), 'fee', { kind: 'dimension' }), /'fee' is a calculated column: its kind is set in its editor/);
    refused(() => setColumn(draft(), 'region', { kind: 'measure' }), /'region' is grouped or pivoted on/);
    refused(() => setColumn(draft(), 'qty', { kind: 'level' as 'measure' }), /a column's kind is 'dimension' or 'measure'/);
  });

  it('takes a kind a column has already as no change, grouped too (the panel sets none)', () => {
    const d = draft();
    assert.equal(setColumn(d, 'region', { kind: 'dimension' }).config, d.config);
  });

  it('sets a measure\'s Aggregation, one offered for its type, and clears it', () => {
    const avg = setColumn(draft(), 'notional', { aggregate: 'average' });
    assert.equal(columnConfig(avg.config, 'notional').aggregateFn, 'average');
    // a row-level calculated measure is aggregated like any other
    assert.equal(columnConfig(setColumn(draft(), 'fee', { aggregate: 'max' }).config, 'fee').aggregateFn, 'max');
    const cleared = setColumn(avg, 'notional', { aggregate: null });
    assert.equal(columnConfig(cleared.config, 'notional').aggregateFn, undefined);
  });

  it('refuses an Aggregation of a dimension, of a group-level column, and one its type is not offered', () => {
    refused(() => setColumn(draft(), 'desk', { aggregate: 'joinStrings' }), /'desk' is a dimension, and only a measure takes an Aggregation/);
    refused(() => setColumn(draft(), 'share', { aggregate: 'sum' }), /'share' is a group-level column, computed after grouping/);
    // Column Properties makes desk a measure: then it is offered its own aggregates, never a sum
    const measure = setColumn(draft({ rows: [] }), 'desk', { kind: 'measure' });
    refused(() => setColumn(measure, 'desk', { aggregate: 'sum' }), /'sum' is not offered on desk, a String column/);
    assert.equal(columnConfig(setColumn(measure, 'desk', { aggregate: 'joinStrings' }).config, 'desk').aggregateFn,
      'joinStrings');
  });

  it('keeps the Aggregation a column has, offered or not: a cube saved elsewhere can carry one', () => {
    const held = withColumn(DEFAULT_CONFIGURATION, 'qty', { aggregateFn: 'joinStrings' });
    assert.equal(columnConfig(setColumn(draft({}, held), 'qty', { aggregate: 'joinStrings' }).config, 'qty').aggregateFn,
      'joinStrings');
  });

  it('takes a weight for a weighted average only, a column of the grid\'s', () => {
    const wavg = setColumn(draft(), 'notional', { aggregate: 'wavg', weight: 'qty' });
    assert.deepEqual(columnConfig(wavg.config, 'notional'), { aggregateFn: 'wavg', aggregationParameters: ['qty'] });
    refused(() => setColumn(draft(), 'notional', { weight: 'qty' }), /only a weighted average \(wavg\) takes a weight/);
    refused(() => setColumn(wavg, 'notional', { weight: 'nope' }), /notional's weight names 'nope', not a column/);
    // a group-level column exists only after grouping: no row of the grid has it to weigh by
    refused(() => setColumn(wavg, 'notional', { weight: 'share' }), /notional's weight names 'share', not a column the rows have/);
    const unweighted = setColumn(wavg, 'notional', { weight: null });
    assert.equal(columnConfig(unweighted.config, 'notional').aggregationParameters, undefined);
  });

  it('refuses a column the grid has not', () => {
    refused(() => setColumn(draft(), 'nope', { aggregate: 'sum' }), /'nope' is not a column of the grid/);
  });
});

describe('a measure as the screen\'s model (Python\'s grid.measure, a saved measures list, Open in Grid)', () => {
  it('is its own column\'s Aggregation, named as its column and the column no key', () => {
    const one = measureAsColumn(draft(), { name: 'notional', column: 'notional', fn: 'average' });
    assert.equal(columnConfig(one.draft.config, 'notional').aggregateFn, 'average');
    assert.deepEqual(one.made, { measure: { name: 'notional', column: 'notional', fn: 'average' }, column: 'notional' });
    assert.equal(one.draft.snapshot.derived.length, CUBE.derived.length, 'no column made');
    // asked of a grid, a dimension's Aggregation is refused, as Column Properties refuses it
    const deskMeasure = { name: 'desk', column: 'desk', fn: 'joinStrings' } as const;
    refused(() => measureAsColumn(draft({ rows: [] }), deskMeasure), /'desk' is a dimension, and only a measure takes/);
    // converting what a cube held (a saved list, a chart's values), a dimension is made a measure first
    const desk = measureAsColumn(draft({ rows: [] }), deskMeasure, new Set(), true);
    assert.deepEqual(columnConfig(desk.draft.config, 'desk'),
      { kind: 'measure', excludedFromPivot: false, aggregateFn: 'joinStrings' });
    assert.match(desk.note, /desk is now a measure/);
  });

  it('is a calculated column of its own name when named for another, or of a grouped column', () => {
    const named = measureAsColumn(draft(), { name: 'top', column: 'notional', fn: 'max' });
    const top = named.draft.snapshot.derived.at(-1);
    assert.deepEqual([top?.name, top?.kind, top?.type], ['top', 'measure', 'Float']);
    assert.match(JSON.stringify(top?.lambda), /"property":"notional"/);
    assert.equal(columnConfig(named.draft.config, 'top').aggregateFn, 'max');
    assert.equal(named.made.column, 'top');
    // a weighted average with its weight
    const w = measureAsColumn(draft(), { name: 'w', column: 'notional', fn: 'wavg', weight: 'qty' });
    assert.deepEqual(columnConfig(w.draft.config, 'w'), { aggregateFn: 'wavg', aggregationParameters: ['qty'] });
  });

  it('refuses, in the screen\'s words: a column the grid has not, a name taken, an aggregate its type is not offered', () => {
    refused(() => measureAsColumn(draft(), { name: 'n', column: 'nope', fn: 'sum' }), /'nope' is not a column of the grid/);
    refused(() => measureAsColumn(draft(), { name: 'n', column: 'share', fn: 'sum' }),
      /'share' is a group-level column, computed after grouping: it takes no Aggregation/);
    refused(() => measureAsColumn(draft(), { name: 'desk', column: 'qty', fn: 'sum' }), /'desk' is already a column/);
    // region is grouped: its measure is a column of its own, and a count of text is not offered
    refused(() => measureAsColumn(draft(), { name: 'region', column: 'region', fn: 'count' }),
      /'count' is not offered on region, a String column: min, max, joinStrings, unique \(Show leaf count/);
    refused(() => measureAsColumn(draft(), { name: 'n', column: 'notional', fn: 'sum', weight: 'qty' }),
      /only a weighted average \(wavg\) takes a weight/);
  });

  it('makes a list in order, a column taken once, and leaves out what it cannot hold, saying why', () => {
    const made = measuresAsColumns(draft(), [
      { name: 'notional', column: 'notional', fn: 'sum' },
      { name: 'notional', column: 'notional', fn: 'max' },
      { name: 'peak', column: 'notional', fn: 'max' },
    ]);
    assert.deepEqual(made.made.map((m) => m.column), ['notional', 'peak']);
    assert.deepEqual(made.notes, [
      "measure notional is now notional's Aggregation, sum",
      "left out measure notional: 'notional' is already a column",
      'measure peak is now a calculated column of notional, its Aggregation max',
    ]);
  });
});

describe('what a grid is', () => {
  it('a calculated column that copies a column as it is, and no other, says which', () => {
    const copy = { name: 'peak', lambda: lambda(['x'], property(variable('x'), 'notional')) };
    assert.equal(copiedColumn(copy), 'notional');
    assert.equal(copiedColumn(CUBE.derived[1]!), undefined, 'a formula is no copy');
    assert.equal(copiedColumn({ ...copy, window: { fn: 'rank', partition: [], order: [] } as never }), undefined);
  });

  it('carries no measures list: one a JavaScript host hands it is refused, never read', () => {
    const smuggled: QuerySnapshot = { ...CUBE, measures: [{ name: 'n', column: 'notional', fn: 'sum' }] };
    assert.throws(() => refuseMeasuresList(smuggled), /a grid carries no measures list/);
    refuseMeasuresList(CUBE);
  });
});
