// A saved cube and a saved page, read back as a person's file would be (src/cube-document.ts, src/page-document.ts):
// every setting saved and reopened, a setting CLEARED saved and reopened, the pivot sort, the pivot total and the row
// cap folded into the query and read back on open, each refusal of a malformed cube or page said in its own words
// (one assertion per message, the message whole), and openCube's notes for each kind of drift between a cube and its
// source as it is now.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
  CUBE_KIND,
  CubeDocumentError,
  configurationOf,
  cubeToJson,
  fileSource,
  frameSource,
  openCube,
  readCube,
  rebuilt,
  treeOf,
  writeCube,
  type CubeDocument,
  type FileSource,
} from '../src/cube-document.ts';
import {
  APPEARANCE_SETTINGS,
  COLUMN_SETTINGS,
  CUBE_SETTINGS,
  DEFAULT_CONFIGURATION,
  applyToSnapshot,
  columnConfig,
  fromSnapshot,
  withAppearance,
  withColumn,
  withConfiguration,
  withSettings,
  type ColumnConfiguration,
  type CubeConfiguration,
  type Patch,
} from '../src/config.ts';
import {
  PAGE_KIND,
  PageDocumentError,
  oneSheet,
  pageToJson,
  readPage,
  readSaved,
  writePage,
  writePageOf,
  type PageViews,
} from '../src/page-document.ts';
import type { ChartSpec } from '../src/chart-spec.ts';
import type { ColumnSpec, CubeSnapshot } from '../src/snapshot.ts';
import type { GridAppearance } from '../src/style.ts';
import { TreeState } from '../src/tree.ts';
import { pivotTotalColumn } from '../src/snapshot.ts';
import { accessor, col, lambda, lit, times } from '../../pure-protocol/src/index.ts';

const RELATION = { query: accessor('local::DB', 'trades') };

const COLUMNS: readonly ColumnSpec[] = [
  { name: 'region', type: 'String' },
  { name: 'desk', type: 'String' },
  { name: 'book', type: 'String' },
  { name: 'notional', type: 'Float' },
  { name: 'qty', type: 'Integer' },
];

/** The file a cube of these columns was saved over. */
const sourceOf = (columns: readonly ColumnSpec[]): FileSource => ({
  _type: 'file', name: 'trades.csv', format: 'csv', size: 120, sha256: 'ab'.repeat(32),
  columns: columns.map((c) => ({ name: c.name, type: c.type })),
});

const SNAPSHOT: CubeSnapshot = {
  source: RELATION,
  columns: COLUMNS,
  derived: [{ name: 'scaled', type: 'Float', lambda: lambda(['x'], times(col('x', 'qty'), lit.integer(2))) }],
  rows: ['region'],
  pivotOn: [],
  sorts: [],
  epoch: 1,
};

/**
 * A cube saved as the screen saves one -- its query the COMMITTED one, the configuration folded in (state-owner.ts
 * commits `fold(state)`) -- and read back through its JSON text, as a file round-trips.
 */
function saved(o: {
  snapshot?: CubeSnapshot; configuration?: CubeConfiguration; tree?: TreeState;
} = {}): CubeDocument {
  const configuration = o.configuration ?? DEFAULT_CONFIGURATION;
  const snapshot = applyToSnapshot(o.snapshot ?? SNAPSHOT, configuration);
  return readCube(cubeToJson(writeCube({
    name: 'Trades', source: sourceOf(snapshot.columns), snapshot, configuration, tree: o.tree ?? TreeState.fromPaths([]),
  })));
}

/**
 * The settings a saved cube reopens on, as the screen opens it over an unchanged source: openCube, then what its query
 * carries read back over its configuration (app.ts's constructor, headless grid.ts `opened`).
 */
function reopened(doc: CubeDocument): CubeConfiguration {
  const opened = openCube(doc, RELATION, doc.source.columns.map((c) => ({ name: c.name, type: c.type })));
  assert.deepEqual(opened.notes, [], 'nothing drifted');
  return fromSnapshot(opened.snapshot, opened.configuration);
}

/** The source's columns now: those named, as the compiler types them. */
const now = (...names: string[]): ColumnSpec[] => COLUMNS.filter((c) => names.includes(c.name));

// ------------------------------------------------------------------------------------------------ every setting

const EVERY_APPEARANCE: Required<GridAppearance> = {
  fontFamily: 'Georgia', fontSize: 13, bold: true, italic: true, underline: 'dotted', strikethrough: false,
  textAlign: 'right', fontCase: 'uppercase', normalForeground: '#111111', negativeForeground: '#aa0000',
  zeroForeground: '#999999', errorForeground: '#0000aa', normalBackground: '#ffffff', negativeBackground: '#ffeeee',
  zeroBackground: '#f5f5f5', errorBackground: '#eeeeff', showHorizontalGridLines: true, showVerticalGridLines: false,
  gridLineColor: '#cccccc', alternateRowsStandardMode: false, alternateRows: true, alternateRowsColor: '#eef2f7',
  alternateRowsCount: 2,
};

const EVERY_COLUMN_SETTING: Required<ColumnConfiguration> = {
  displayName: 'Notional (USD)', kind: 'measure', aggregateFn: 'wavg', aggregationParameters: ['qty'], hidden: false,
  blurred: true, pinned: 'right', widthMode: 'range', width: 120, minWidth: 80, maxWidth: 240,
  format: { kind: 'currency', currency: 'USD', locale: 'en-US', maximumFractionDigits: 0, numberScale: 'millions' },
  appearance: { bold: true, normalBackground: '#fafafa' },
  heatmap: { from: '#ffffff', to: '#ff8a65', range: { min: 0, max: 100 } },
  excludedFromPivot: true, pivotSortDirection: 'desc', pivotStatisticColumnFunction: 'max', displayAsLink: true,
  linkLabelParameter: 'title',
};

/** EVERY setting a grid has, each set off its default: `Required<>` refuses this fixture the day one is added. */
const EVERY_SETTING: Required<CubeConfiguration> = {
  reportTitle: 'Q3 review', showRootAggregation: true, showGroupedColumns: true, showDragZones: false,
  showTitleBar: false, showLeafCount: false, leafCountMode: 'leaves', treeColumnSort: 'desc', initialExpandToLevel: 2,
  maxRows: 500, showTruncationWarning: false, appearance: EVERY_APPEARANCE, showSelectionStats: true,
  pivotStatisticColumnName: 'All desks', pivotStatisticColumnPlacement: 'left', pivotMeasuresFirst: true,
  dimensions: [{ name: 'Geography', columns: ['region', 'desk'] }],
  columns: { notional: EVERY_COLUMN_SETTING, desk: { hidden: true } },
  columnOrder: ['desk', 'region', 'notional', 'qty', 'book'],
};

describe('every setting, saved and reopened', () => {
  it('the fixture sets every setting the host lists name (config.ts CUBE_SETTINGS, COLUMN_SETTINGS, APPEARANCE_SETTINGS)', () => {
    assert.deepEqual(Object.keys(EVERY_SETTING).sort(), Object.keys(CUBE_SETTINGS).sort());
    assert.deepEqual(Object.keys(EVERY_COLUMN_SETTING).sort(), Object.keys(COLUMN_SETTINGS).sort());
    assert.deepEqual(Object.keys(EVERY_APPEARANCE).sort(), Object.keys(APPEARANCE_SETTINGS).sort());
  });

  it('comes back exactly, through the JSON text, on the screen that reopens it', () => {
    const doc = saved({ configuration: EVERY_SETTING });
    assert.deepEqual(reopened(doc), EVERY_SETTING);
    assert.deepEqual(configurationOf(doc), EVERY_SETTING, 'the saved configuration over the product defaults');
  });

  it('a host patching every setting at once reaches each of them', () => {
    assert.deepEqual(withConfiguration(DEFAULT_CONFIGURATION, EVERY_SETTING), EVERY_SETTING);
  });

  it('a cube at the product defaults writes none of them down', () => {
    const doc = writeCube({
      name: 'Trades', source: sourceOf(COLUMNS), snapshot: SNAPSHOT, configuration: DEFAULT_CONFIGURATION,
      tree: TreeState.fromPaths([]),
    });
    assert.deepEqual(doc.configuration, {});
    assert.deepEqual(reopened(readCube(cubeToJson(doc))), DEFAULT_CONFIGURATION);
  });
});

describe('a setting cleared, saved and reopened', () => {
  it('the pivot total switched off stays off: saved as unset, reopened with none, its query with none', () => {
    const off = withConfiguration(DEFAULT_CONFIGURATION, { pivotStatisticColumnPlacement: undefined });
    const doc = saved({ configuration: off });
    assert.equal(doc.configuration['pivotStatisticColumnPlacement'], null, 'a default unset is written as null');
    const opened = openCube(doc, RELATION, now(...COLUMNS.map((c) => c.name)));
    assert.equal(opened.configuration.pivotStatisticColumnPlacement, undefined);
    // the screen opens on the configuration with what its query carries read back (app.ts, headless grid.ts)
    const shown = fromSnapshot(opened.snapshot, opened.configuration);
    assert.equal(shown.pivotStatisticColumnPlacement, undefined);
    assert.equal('pivotTotal' in applyToSnapshot(opened.snapshot, shown), false);
  });

  it('an appearance default cleared is saved as cleared, and the others stay the defaults', () => {
    const cleared = withAppearance(DEFAULT_CONFIGURATION, { negativeForeground: undefined });
    const doc = saved({ configuration: cleared });
    assert.deepEqual(doc.configuration, { appearance: { negativeForeground: null } });
    const { negativeForeground: _n, ...rest } = DEFAULT_CONFIGURATION.appearance;
    assert.deepEqual(reopened(doc).appearance, rest);
  });

  it('a row cap set and cleared, and a column configured and cleared, leave nothing in the file', () => {
    let c = withSettings(DEFAULT_CONFIGURATION, { maxRows: 50 });
    c = withColumn(c, 'notional', { pinned: 'left', displayName: 'Notional' });
    // as a host's JSON patch arrives (headless grid.ts): a column's part is a patch too, which the whole's type cannot say
    c = withConfiguration(c, { maxRows: undefined, columns: { notional: { pinned: undefined, displayName: undefined } } } as
      unknown as Patch<CubeConfiguration>);
    const doc = saved({ configuration: c });
    assert.deepEqual(doc.configuration, {});
    assert.equal(reopened(doc).maxRows, undefined, 'no limit, as a new cube has none');
    assert.equal('maxRows' in doc.query, false, 'and its query is not capped');
  });

  it("a column's own setting cleared is gone on reopening, its other settings kept", () => {
    const c = withColumn(withColumn(DEFAULT_CONFIGURATION, 'notional', { pinned: 'left', hidden: true }), 'notional',
      { pinned: undefined });
    assert.deepEqual(columnConfig(reopened(saved({ configuration: c })), 'notional'), { hidden: true });
  });
});

describe('the pivot sort, the pivot total and the row cap, folded into the saved query and read back on open', () => {
  const pivoted: CubeSnapshot = { ...SNAPSHOT, rows: ['region'], pivotOn: ['desk'] };
  let config = withSettings(DEFAULT_CONFIGURATION, { maxRows: 300, pivotStatisticColumnPlacement: 'left' });
  config = withColumn(config, 'desk', { pivotSortDirection: 'desc' });
  config = withColumn(config, 'notional', { pivotStatisticColumnFunction: 'average' });
  config = withColumn(config, 'qty', { excludedFromPivot: true });
  const opened = () => openCube(saved({ snapshot: pivoted, configuration: config }), RELATION,
    now(...COLUMNS.map((c) => c.name)));

  it('the query the cube reopens with carries each', () => {
    const q = opened().snapshot;
    assert.deepEqual(q.pivotSort, { desk: 'desc' });
    assert.deepEqual(q.pivotTotal, { placement: 'left', functions: { notional: 'average' } });
    assert.equal(q.maxRows, 300);
    assert.equal(q.columns.find((c) => c.name === 'qty')?.excludedFromPivot, true, 'a column kept out of the pivot');
  });

  it('and the query alone gives each back, so the editor opens on what is running', () => {
    const back = fromSnapshot(opened().snapshot);
    assert.equal(back.maxRows, 300);
    assert.equal(back.pivotStatisticColumnPlacement, 'left');
    assert.equal(columnConfig(back, 'desk').pivotSortDirection, 'desc');
    assert.equal(columnConfig(back, 'notional').pivotStatisticColumnFunction, 'average');
    assert.equal(columnConfig(back, 'qty').excludedFromPivot, true);
  });
});

// ------------------------------------------------------------------------------------------------ writing

describe('a pivot total\'s own heatmap, saved and reopened (2026-10-18)', () => {
  it('is kept under the total\'s name, apart from its measure\'s', () => {
    const total = pivotTotalColumn('notional');
    const heat = { from: '#ffffff', to: '#ff0000' };
    const c = withColumn(DEFAULT_CONFIGURATION, total, { heatmap: heat });
    const back = reopened(saved({ snapshot: { ...SNAPSHOT, pivotOn: ['desk'] }, configuration: c }));
    assert.deepEqual(columnConfig(back, total).heatmap, heat);
    assert.equal(columnConfig(back, 'notional').heatmap, undefined);
  });
});

describe('what a cube writes besides its settings', () => {
  it('fields carried from the document it was opened from, and none when there were none', () => {
    const write = (unknown: Record<string, unknown>) => writeCube({
      name: 'Trades', source: sourceOf(COLUMNS), snapshot: SNAPSHOT, configuration: DEFAULT_CONFIGURATION,
      tree: TreeState.fromPaths([]), unknown,
    });
    const doc = write({ futureSetting: { a: 1 } });
    assert.deepEqual(doc.unknown, { futureSetting: { a: 1 } });
    assert.deepEqual(JSON.parse(cubeToJson(doc)).futureSetting, { a: 1 }, 'written back at the top, verbatim');
    assert.equal('unknown' in write({}), false);
  });

  it('a file that is not a sample records no sample', async () => {
    const src = await fileSource({ name: 'a.csv', size: 3, arrayBuffer: async () => new TextEncoder().encode('abc').buffer as ArrayBuffer },
      'csv', [{ name: 'a', type: 'String' }]);
    assert.equal('sample' in src, false);
    assert.deepEqual(src.columns, [{ name: 'a', type: 'String' }]);
  });

  it("a dataframe's source: its name on the engine and each column's name and type, nothing more, read back exactly", () => {
    // the columns as the compiler typed them, carrying what the cube set on each: only name and type are the source's
    const typed: ColumnSpec[] = [{ name: 'qty', type: 'Integer', kind: 'dimension', aggregate: 'max' }];
    const source = frameSource('trades', typed);
    assert.deepEqual(source, { _type: 'frame', name: 'trades', columns: [{ name: 'qty', type: 'Integer' }] });
    const back = readCube(cubeToJson(writeCube({
      name: 'Trades', source, snapshot: SNAPSHOT, configuration: DEFAULT_CONFIGURATION, tree: TreeState.fromPaths([]),
    })));
    assert.deepEqual(back.source, source);
  });

  it('the open rows and whether totals show, read back as the cube reopens them', () => {
    const doc = saved({ tree: TreeState.fromPaths([['EMEA'], ['EMEA', null]], true) });
    for (const tree of [treeOf(doc), openCube(doc, RELATION, now(...COLUMNS.map((c) => c.name))).tree]) {
      assert.equal(tree.isOpen(['EMEA']), true);
      assert.equal(tree.isOpen(['EMEA', null]), true, 'a group whose value is null is a path like any other');
      assert.equal(tree.isOpen(['APAC']), false);
      assert.equal(tree.showTotals, true);
    }
  });

  it('a sample, and a calculated column with no lambda (a window), read back exactly', () => {
    const windowed: CubeSnapshot = {
      ...SNAPSHOT,
      derived: [{ name: 'running', type: 'Float', window: { fn: 'sum', column: 'notional', partition: ['region'], order: [{ column: 'desk', direction: 'asc' }], frame: 'running' } }],
    };
    const source: FileSource = { ...sourceOf(COLUMNS), sample: { id: 'trades', rows: 500 } };
    const back = readCube(cubeToJson(writeCube({
      name: 'Trades', source, snapshot: windowed, configuration: DEFAULT_CONFIGURATION, tree: TreeState.fromPaths([]),
    })));
    assert.deepEqual(back.source, source);
    assert.deepEqual(back.query.derived, windowed.derived);
  });
});

// ------------------------------------------------------------------------------------------------ refusals

/** The saved cube as plain JSON, to break. */
const rawCube = (): Record<string, any> => JSON.parse(cubeToJson(saved()));

/** `breakIt` applied to a good cube is refused, saying exactly `detail`. */
function cubeRefused(breakIt: (raw: Record<string, any>) => unknown, detail: string): void {
  const raw = rawCube();
  breakIt(raw);
  assert.throws(() => readCube(raw), (e: unknown) => e instanceof CubeDocumentError
    && e.message === `cannot read saved cube: ${detail}`);
}

describe('a malformed cube is refused, saying what is wrong', () => {
  it('not an object', () => {
    assert.throws(() => readCube('[]'), (e: unknown) => e instanceof CubeDocumentError
      && e.message === 'cannot read saved cube: expected an object');
  });

  it('no kind', () => cubeRefused((r) => delete r['kind'], 'not a saved cube (kind null)'));

  it('no version, a version 0, or a version written as text', () => {
    cubeRefused((r) => delete r['version'], 'it has no version');
    cubeRefused((r) => { r['version'] = 0; }, 'it has no version');
    cubeRefused((r) => { r['version'] = '2'; }, 'it has no version');
  });

  it('no name', () => cubeRefused((r) => delete r['name'], "missing 'name'"));

  it('a configuration that is not an object', () => {
    cubeRefused((r) => { r['configuration'] = ['maxRows', 10]; }, "'configuration' is not an object");
  });

  it('a setting every grid has, unset (null): no writer writes one, a cleared one is its default again', () => {
    cubeRefused((r) => { r['configuration'] = { showTitleBar: null }; }, "'configuration.showTitleBar' is null: every grid has that setting");
    cubeRefused((r) => { r['configuration'] = { columns: null }; }, "'configuration.columns' is null: every grid has that setting");
  });

  it('no source, or a source of no kind', () => {
    cubeRefused((r) => delete r['source'], "missing 'source'");
    cubeRefused((r) => delete r['source']['_type'], 'a source of kind null is not supported yet');
  });

  it('a file source whose columns are not each a name and a type', () => {
    cubeRefused((r) => { r['source']['columns'] = [{ name: 'region' }]; },
      'the file source is incomplete (name, format, size, sha256, columns)');
  });

  it('a sample with no row count', () => {
    cubeRefused((r) => { r['source']['sample'] = { id: 'trades' }; }, 'the sample source is incomplete (id, rows)');
  });

  it('a file of a format DataCube does not open', () => {
    cubeRefused((r) => { r['source']['format'] = 'xlsx'; }, 'an unknown file format "xlsx"');
  });

  it('a warehouse table or a remote file with no columns', () => {
    cubeRefused((r) => {
      r['source'] = { _type: 'warehouseTable', name: 'sales.orders', warehouse: 'https://w.example.com', catalog: 'shop', schema: 'sales', table: 'orders' };
    }, 'the warehouse table source has no columns');
    cubeRefused((r) => {
      r['source'] = { _type: 'remoteFile', name: 't.parquet', url: 'https://d.example.com/t.parquet', columns: [{ name: 'qty' }] };
    }, 'the remote file source has no columns');
  });

  it('no query', () => cubeRefused((r) => delete r['query'], "missing 'query'"));

  it("a query whose columns, rows, pivot or sorts are not lists", () => {
    for (const k of ['columns', 'rows', 'pivotOn', 'sorts']) {
      cubeRefused((r) => { r['query'][k] = {}; }, `'query.${k}' is not a list`);
    }
  });

  it('calculated columns that are not a list of objects', () => {
    cubeRefused((r) => { r['query']['derived'] = { scaled: {} }; }, "'query.derived' is not a list");
    cubeRefused((r) => { r['query']['groupDerived'] = 'margin'; }, "'query.groupDerived' is not a list");
    cubeRefused((r) => { r['query']['derived'] = [42]; }, "an entry of 'query.derived' is not an object");
  });

  it('open rows that are not row paths', () => {
    cubeRefused((r) => { r['tree'] = { showTotals: true }; }, "'tree' has no open paths");
    cubeRefused((r) => { r['tree'] = { open: [[1]], showTotals: false }; }, 'not a row path: [1]');
  });
});

// ------------------------------------------------------------------------------------------------ drift

describe("opening a cube over its source as it is now: what drifted, said", () => {
  it('one new column, or several, hidden and named -- the cube held is still the one saved', () => {
    const one = openCube(saved({ snapshot: { ...SNAPSHOT, columns: now('region', 'notional', 'qty') } }), RELATION,
      now('region', 'notional', 'qty', 'desk'));
    assert.deepEqual(one.notes, ['1 new column since this cube was saved, hidden: desk']);
    const two = openCube(saved({ snapshot: { ...SNAPSHOT, columns: now('region', 'notional', 'qty') } }), RELATION,
      now('region', 'desk', 'book', 'notional', 'qty'));
    assert.deepEqual(two.notes, ['2 new columns since this cube was saved, hidden: desk, book']);
    assert.equal(two.configuration.columns['desk']?.hidden, true);
    assert.equal(two.configuration.columns['book']?.hidden, true);
    assert.equal(two.changed, false);
  });

  it('a retyped column: the compiler type wins, said -- nothing left out', () => {
    const opened = openCube(saved(), RELATION, [...now('region', 'desk', 'book', 'notional'), { name: 'qty', type: 'Float' }]);
    assert.deepEqual(opened.notes, ['qty is now Float (it was Integer)']);
    assert.equal(opened.changed, false);
  });

  it('one column gone, or several, counted and named', () => {
    assert.deepEqual(openCube(saved(), RELATION, now('region', 'desk', 'notional', 'qty')).notes,
      ['1 column no longer in the file: book']);
    assert.deepEqual(openCube(saved(), RELATION, now('region', 'notional', 'qty')).notes,
      ['2 columns no longer in the file: desk, book']);
  });

  it('the grouping, the pivot and a sort on a gone column, each left out and named; the open rows closed', () => {
    const cube = saved({
      snapshot: { ...SNAPSHOT, rows: ['region', 'desk'], pivotOn: ['book'], sorts: [{ column: 'desk', direction: 'desc' }, { column: 'region', direction: 'asc' }] },
      tree: TreeState.fromPaths([['EMEA']]),
    });
    const opened = openCube(cube, RELATION, now('region', 'notional', 'qty'));
    assert.deepEqual(opened.notes, [
      '2 columns no longer in the file: desk, book',
      'left out grouping by desk',
      'left out pivoting on book',
      'left out sorting by desk',
    ]);
    assert.deepEqual(opened.snapshot.rows, ['region']);
    assert.deepEqual(opened.snapshot.pivotOn, []);
    assert.deepEqual(opened.snapshot.sorts, [{ column: 'region', direction: 'asc' }]);
    assert.equal(opened.tree.isOpen(['EMEA']), false, 'the open rows were paths through the grouping it had');
    assert.equal(opened.changed, true);
  });

  it('pinned pivot values go with a pivot key that is gone, and stay with keys that are not', () => {
    const pinned = saved({ snapshot: { ...SNAPSHOT, pivotOn: ['desk', 'book'], pivotValues: ['Rates'] } });
    assert.deepEqual(openCube(pinned, RELATION, now(...COLUMNS.map((c) => c.name))).snapshot.pivotValues, ['Rates']);
    const lost = openCube(pinned, RELATION, now('region', 'desk', 'notional', 'qty'));
    assert.deepEqual(lost.snapshot.pivotOn, ['desk']);
    assert.equal('pivotValues' in lost.snapshot, false);
  });

  it('a filter whose every condition is on a gone column goes whole, each condition named', () => {
    const cube = saved({ snapshot: { ...SNAPSHOT, filter: { kind: 'and', children: [
      { kind: 'or', children: [
        { kind: 'condition', column: 'book', operator: 'equal', value: 'B1' },
        { kind: 'condition', column: 'region', operator: 'equalColumn', rightColumn: 'book' },
      ] },
      { kind: 'not', child: { kind: 'condition', column: 'book', operator: 'isEmpty' } },
    ] } } });
    const opened = openCube(cube, RELATION, now('region', 'desk', 'notional', 'qty'));
    assert.equal('filter' in opened.snapshot, false, 'no filter is left, not an empty one');
    assert.deepEqual(opened.notes.filter((n) => n.startsWith('left out the filter')),
      ['left out the filter on book', 'left out the filter on book', 'left out the filter on book']);
    assert.equal(opened.changed, true);
  });

  it('a calculated column on a gone column goes, and so does each one reading it, and a sort on it', () => {
    const cube = saved({ snapshot: {
      ...SNAPSHOT,
      derived: [
        { name: 'scaled', type: 'Float', lambda: lambda(['x'], times(col('x', 'qty'), lit.integer(2))) },
        { name: 'kept', type: 'Float', lambda: lambda(['x'], times(col('x', 'notional'), lit.integer(2))) },
      ],
      groupDerived: [
        { name: 'doubled', type: 'Float', lambda: lambda(['x'], times(col('x', 'scaled'), lit.integer(2))) },
        { name: 'kids', childAggregate: { fn: 'sum', of: 'qty' } },
      ],
      sorts: [{ column: 'scaled', direction: 'desc' }],
    } });
    const opened = openCube(cube, RELATION, now('region', 'desk', 'book', 'notional'));
    assert.deepEqual(opened.notes, [
      '1 column no longer in the file: qty',
      'left out calculated column scaled (it uses qty)',
      'left out calculated column doubled (it uses scaled)',
      'left out calculated column kids (it uses qty)',
      'left out sorting by scaled',
    ]);
    assert.deepEqual(opened.snapshot.derived.map((d) => d.name), ['kept']);
    assert.deepEqual(opened.snapshot.groupDerived, []);
  });

  it('a cube with no group-stage columns opens with none, not an empty list made up', () => {
    const opened = openCube(saved(), RELATION, now(...COLUMNS.map((c) => c.name)));
    assert.equal('groupDerived' in opened.snapshot, false);
    assert.deepEqual(opened.notes, []);
  });

  it("a weighted average whose weight column is gone: left out and named, so the query reads no gone column", () => {
    // openCube's rule: "a part that uses a column that is gone is left out and NAMED ... Nothing is refused for drift
    // alone." Kept, the reopened cube asks for `$x.notional->wavgRowMapper($x.qty)` over a source with no qty, and the
    // planner refuses the whole cube.
    const weighted = withColumn(DEFAULT_CONFIGURATION, 'notional', { aggregateFn: 'wavg', aggregationParameters: ['qty'] });
    const cube = saved({ configuration: weighted });
    const opened = openCube(cube, RELATION, now('region', 'desk', 'book', 'notional'));
    const running = applyToSnapshot(opened.snapshot, opened.configuration);
    assert.equal(running.columns.find((c) => c.name === 'notional')?.aggregateWeight, undefined,
      'the query it runs weights nothing by a column the source no longer has');
    assert.equal(opened.changed, true, 'and the cube held is not the one saved');
    assert.ok(opened.notes.includes("left out notional's weighted average (it is weighted by qty)"), opened.notes.join(' | '));
    assert.equal(columnConfig(opened.configuration, 'notional').aggregateFn, undefined, 'notional takes its kind\'s own again');
  });

  it('a saved cube rebuilt over a source is opened as openCube opens it; none, its rows as they are', () => {
    const cube = saved({ tree: TreeState.fromPaths([['EMEA']], true) });
    const made = rebuilt(cube, RELATION.query, now('region', 'desk', 'book', 'notional'), 'ignored', false);
    assert.deepEqual(made.notes, ['1 column no longer in the file: qty', 'left out calculated column scaled (it uses qty)']);
    assert.equal(made.changed, true);
    assert.equal(made.tree?.isOpen(['EMEA']), true);
    assert.equal(made.tree?.showTotals, true);
    assert.deepEqual(made.snapshot.source, RELATION);
    assert.equal(made.configuration.reportTitle, undefined, 'the label names only a fresh grid');
  });
});

// ------------------------------------------------------------------------------------------------ pages

const SPEC: ChartSpec = {
  version: 2, mark: 'bar', x: 'region', y: [{ column: 'notional', fn: 'sum' }],
  options: { orientation: 'vertical', stack: 'none', sort: { by: 'y', direction: 'desc' }, limit: 50, labels: false, legend: 'top' },
};

const VIEWS: PageViews = {
  views: [
    { id: 'grid', kind: 'grid', cube: 'cube' },
    { id: 'chart-1', kind: 'chart', cube: 'cube', title: 'Notional by region', spec: SPEC },
  ],
  sheets: oneSheet({ kind: 'bands', fit: false, bands: [
    { height: 1, node: { split: 'row', parts: [{ node: { tile: 'grid' }, size: 0.7 }, { node: { tile: 'chart-1' }, size: 0.3 }] } },
  ] }),
};

const MODEL = { project: 'datacube:trades', workspace: 'datacube', package: 'trades', tables: { cube: 'trades_2' } };

const page = () => writePage({ name: 'Q3 page', cube: saved(), views: VIEWS, model: MODEL });

/** The saved page as plain JSON, to break. */
const rawPage = (): Record<string, any> => JSON.parse(pageToJson(page()));

/** `breakIt` applied to a good page is refused, saying exactly `detail`. */
function pageRefused(breakIt: (raw: Record<string, any>) => unknown, detail: string): void {
  const raw = rawPage();
  breakIt(raw);
  assert.throws(() => readPage(JSON.stringify(raw)), (e: unknown) => e instanceof PageDocumentError
    && e.message === `cannot read saved page: ${detail}`);
}

describe('a malformed page is refused, saying what is wrong', () => {
  it('not an object, or another kind of document', () => {
    assert.throws(() => readPage('[]'), (e: unknown) => e instanceof PageDocumentError
      && e.message === 'cannot read saved page: expected an object');
    assert.throws(() => readPage(cubeToJson(saved())), (e: unknown) => e instanceof PageDocumentError
      && e.message === `cannot read saved page: not a saved page (kind "${CUBE_KIND}")`);
  });

  it('no version', () => pageRefused((r) => delete r['version'], 'it has no version'));

  it('a name that is not text', () => pageRefused((r) => { r['name'] = 42; }, "missing 'name'"));

  it('a cube entry that is not an id and a cube, or holds no saved cube', () => {
    pageRefused((r) => { r['cubes'] = [{ id: 'cube' }]; }, 'cube 1 is not an id and a cube');
    pageRefused((r) => { r['cubes'][0]['cube']['kind'] = 'datacube.chart'; }, 'cube cube is not a saved cube');
  });

  it('a cube inside it that no longer reads, refused in the cube\'s own words', () => {
    const raw = rawPage();
    raw['cubes'][0]['cube']['version'] = 1;
    assert.throws(() => readPage(JSON.stringify(raw)), (e: unknown) => e instanceof CubeDocumentError
      && e.message === "cannot read saved cube: saved by an older version (1 < 2), before a grid's aggregates were its "
        + "columns' Aggregation: it is no longer read");
  });

  it('views that are not a list, a view with no id, two views of one id', () => {
    pageRefused((r) => { r['views'] = { grid: {} }; }, "'views' is not a list");
    pageRefused((r) => { delete r['views'][0]['id']; }, 'a view has no id');
    pageRefused((r) => { r['views'][1]['id'] = 'grid'; }, 'two views share an id');
  });

  it('a chart with no title, or a selection that is not a list', () => {
    pageRefused((r) => { delete r['views'][1]['title']; }, 'chart chart-1 has no title');
    pageRefused((r) => { r['views'][1]['selection'] = 'EMEA'; }, "chart chart-1's selection is not a list");
  });

  it('a sheet with no id: none, not text, or blank', () => {
    const layout = rawPage()['sheets'][0]['layout'];
    for (const sheet of [42, { layout }, { id: 7, layout }, { id: '  ', layout }]) {
      pageRefused((r) => { r['sheets'] = [sheet]; }, 'sheet 1 has no id');
    }
  });

  it("a sheet's layout that is not one, a fit that is not true or false, bands that are not a list", () => {
    pageRefused((r) => { delete r['sheets'][0]['layout']; }, "sheet sheet-1's layout is not a layout");
    pageRefused((r) => { delete r['sheets'][0]['layout']['fit']; }, "sheet sheet-1's layout's fit is not true or false");
    pageRefused((r) => { r['sheets'][0]['layout']['bands'] = {}; }, "sheet sheet-1's layout's bands are not a list");
  });

  it('a band with no height, a part with no size', () => {
    pageRefused((r) => { delete r['sheets'][0]['layout']['bands'][0]['height']; }, 'band 1 is not a height and its tiles');
    pageRefused((r) => { delete r['sheets'][0]['layout']['bands'][0]['node']['parts'][1]['size']; }, 'band 1, part 2, has no size');
  });

  it('a model with no workspace, no package, no tables, or a table with no name', () => {
    pageRefused((r) => { r['model']['workspace'] = ''; }, "'model' has no workspace");
    pageRefused((r) => { delete r['model']['package']; }, "'model' has no package");
    pageRefused((r) => { delete r['model']['tables']; }, "'model' has no tables");
    pageRefused((r) => { r['model']['tables'] = { cube: '' }; }, "'model' keeps no table name for cube");
  });

  it("a model's classes: not an object, for no cube of the page, or with no name", () => {
    pageRefused((r) => { r['model']['classes'] = 'trades::Trade'; }, "'model' has classes that are not an object");
    pageRefused((r) => { r['model']['classes'] = { nope: 'trades::Trade' }; },
      "'model' keeps a class for nope, which is no cube of the page");
    pageRefused((r) => { r['model']['classes'] = { cube: '' }; }, "'model' keeps no class name for cube");
  });
});

describe('a page read back', () => {
  it("keeps a grid's title, and leaves out a chart's selection when there is none", () => {
    const raw = rawPage();
    raw['views'][0]['title'] = 'All trades';
    raw['views'][1]['selection'] = [];
    const back = readPage(JSON.stringify(raw));
    assert.deepEqual(back.views[0], { id: 'grid', kind: 'grid', cube: 'cube', title: 'All trades' });
    assert.equal('selection' in back.views[1]!, false);
  });

  it("keeps where each grid's class is", () => {
    const model = { ...MODEL, classes: { cube: 'trades::domain::Trade' } };
    const back = readPage(pageToJson(writePage({ name: 'Q3 page', cube: saved(), views: VIEWS, model })));
    assert.deepEqual(back.model, model);
  });

  it('is read by its kind from an object as from text', () => {
    assert.equal(readSaved(JSON.parse(pageToJson(page()))).kind, 'page');
    const cube = readSaved(JSON.parse(cubeToJson(saved())));
    assert.equal(cube.kind === 'cube' && cube.cube.kind, CUBE_KIND);
  });

  it('writes back the fields a newer writer added, and none when there were none', () => {
    const more = writePage({ name: 'Q3 page', cube: saved(), views: VIEWS, unknown: { variables: [{ name: 'asOf' }] } });
    assert.deepEqual(readPage(pageToJson(more)).unknown, { variables: [{ name: 'asOf' }] });
    assert.equal('unknown' in writePage({ name: 'Q3 page', cube: saved(), views: VIEWS, unknown: {} }), false);
  });
});

describe('a page of several cubes (writePageOf): one per grid, and one kept for a detached chart', () => {
  const views: PageViews = {
    views: [
      { id: 'grid-a', kind: 'grid', cube: 'a' },
      { id: 'grid-b', kind: 'grid', cube: 'b' },
      { id: 'chart', kind: 'chart', cube: 'c', title: 'Detached', spec: SPEC },
    ],
    sheets: oneSheet({ kind: 'bands', fit: true, bands: [{ height: 1, node: { split: 'row', parts: [
      { node: { tile: 'grid-a' }, size: 0.25 }, { node: { tile: 'grid-b' }, size: 0.25 }, { node: { tile: 'chart' }, size: 0.5 }] } }] }),
  };
  const written = () => writePageOf({
    name: 'Desk review',
    cubes: [{ id: 'a', cube: saved() }, { id: 'b', cube: saved() }, { id: 'c', cube: saved() }],
    views,
    // a grid removed since its class was made: its table and class go with it
    model: { ...MODEL, tables: { a: 't_a', gone: 't_gone' }, classes: { a: 'trades::A', gone: 'trades::Gone' } },
    unknown: { theme: 'dark' },
  });

  it('names every cube after the page, and keeps the model only for the cubes it holds', () => {
    const p = written();
    assert.equal(p.kind, PAGE_KIND);
    assert.deepEqual(p.cubes.map((c) => [c.id, c.cube.name]), [['a', 'Desk review'], ['b', 'Desk review'], ['c', 'Desk review']]);
    assert.deepEqual(p.model, { ...MODEL, tables: { a: 't_a' }, classes: { a: 'trades::A' } });
    assert.deepEqual(p.unknown, { theme: 'dark' });
  });

  it('reads back as it was written', () => {
    const back = readPage(pageToJson(written()));
    assert.deepEqual(back.cubes.map((c) => c.id), ['a', 'b', 'c']);
    assert.deepEqual(back.views, views.views);
    assert.deepEqual(back.sheets, views.sheets);
    assert.deepEqual(back.model, written().model);
    assert.deepEqual(back.unknown, { theme: 'dark' });
  });
});
