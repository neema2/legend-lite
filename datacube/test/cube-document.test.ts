// The saved cube (src/cube-document.ts): written whole, read whole, versioned, unknown
// fields kept, defaults not written down, and reconciled with its file on open.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
  CUBE_KIND,
  CUBE_VERSION,
  CubeDocumentError,
  cubeToJson,
  derivedReads,
  difference,
  fileSource,
  merge,
  openCube,
  readCube,
  sha256,
  writeCube,
  type FileSource,
  type QuerySource,
  type FrameSource,
  type RemoteSource,
  type WarehouseSource,
} from '../src/cube-document.ts';
import { DEFAULT_CONFIGURATION, type CubeConfiguration } from '../src/config.ts';
import type { ColumnSpec, CubeSnapshot } from '../src/snapshot.ts';
import { TreeState } from '../src/tree.ts';
import { accessor, col, lambda, lit, times } from '../../pure-protocol/src/index.ts';

const SOURCE: FileSource = {
  _type: 'file', name: 'trades.csv', format: 'csv', size: 120, sha256: 'ab'.repeat(32),
  columns: [
    { name: 'region', type: 'String' }, { name: 'desk', type: 'String' },
    { name: 'notional', type: 'Float' }, { name: 'qty', type: 'Integer' },
  ],
};
const RELATION = { query: accessor('local::DB', 'trades') };

/**
 * EVERY field a snapshot has, set: `Required<>` makes the compiler refuse this fixture the
 * day a field is added, so a new field cannot be forgotten by the reader (the old saved
 * view's allow-list dropped five, P2-71).
 */
const EVERY: Required<CubeSnapshot> = {
  source: RELATION,
  columns: [
    { name: 'region', type: 'String', kind: 'dimension' },
    { name: 'desk', type: 'String' },
    { name: 'notional', type: 'Float', aggregate: 'average' },
    { name: 'qty', type: 'Integer', excludedFromPivot: true },
  ],
  derived: [{
    name: 'scaled', type: 'Decimal',
    lambda: lambda(['x'], times(col('x', 'notional'), lit.decimal('12.30'))),
  }],
  groupDerived: [{ name: 'doubled', lambda: lambda(['x'], times(col('x', 'notional'), lit.integer(2))) }],
  filter: {
    kind: 'and',
    children: [
      { kind: 'condition', column: 'region', operator: 'equal', value: 'EMEA' },
      { kind: 'not', child: { kind: 'condition', column: 'qty', operator: 'greaterThan', value: 3 } },
    ],
  },
  rows: ['region'],
  pivotOn: ['desk'],
  pivotValues: ['Rates', 'Credit'],
  pivotSort: { desk: 'desc' },
  pivotTotal: { placement: 'left', functions: { notional: 'max' } },
  keepGroupedColumns: true,
  leafCount: true,
  childCount: true,
  measures: [{ name: 'notional', column: 'notional', fn: 'sum' }, { name: 'w', column: 'notional', fn: 'wavg', weight: 'qty' }],
  sorts: [{ column: 'region', direction: 'desc' }],
  window: { offset: 50, limit: 100 },
  treeColumnSort: 'desc',
  maxRows: 777,
  epoch: 42,
};

const { pivotStatisticColumnPlacement: _total, ...WITHOUT_TOTAL } = DEFAULT_CONFIGURATION;
const CONFIG: CubeConfiguration = {
  ...WITHOUT_TOTAL,
  reportTitle: 'Q3 review',
  maxRows: 777,
  columns: { notional: { format: { kind: 'number', decimals: 3 } }, desk: { hidden: true } },
};

const doc = () => writeCube({
  name: 'Q3 review', source: SOURCE, snapshot: EVERY, configuration: CONFIG,
  tree: TreeState.fromPaths([['EMEA'], [null]], true),
});

describe('writing and reading a cube', () => {
  it('reads back every field of the cube, exactly, through JSON text', () => {
    const back = readCube(cubeToJson(doc()));
    const { source: _s, epoch: _e, window: _w, ...query } = EVERY;
    assert.deepEqual(JSON.parse(cubeToJson(back)).query, JSON.parse(cubeToJson(doc())).query);
    assert.deepEqual(Object.keys(back.query).sort(), Object.keys(query).sort());
    // the decimal literal keeps its digits
    assert.match(cubeToJson(back), /"12\.30"|12\.30/);
    assert.equal(back.name, 'Q3 review');
    assert.deepEqual(back.source, SOURCE);
    assert.deepEqual(back.tree, { open: [['EMEA'], [null]], showTotals: true });
  });

  it('writes no data, no relation, and no runtime state', () => {
    const d = doc();
    assert.equal('source' in d.query, false);
    assert.equal('epoch' in d.query, false);
    assert.equal('window' in d.query, false, 'where the grid was scrolled is not the cube');
  });

  it('writes only what differs from the product defaults', () => {
    const c = doc().configuration;
    assert.equal(c['reportTitle'], 'Q3 review');
    assert.equal(c['maxRows'], 777);
    assert.equal(c['showTitleBar'], undefined, 'a default is not written down');
    assert.equal(c['pivotStatisticColumnPlacement'], null, 'an unset default is written as null');
    assert.deepEqual(merge(DEFAULT_CONFIGURATION, c), JSON.parse(JSON.stringify(CONFIG)));
  });

  it('keeps fields it does not know, and writes them back', () => {
    const text = cubeToJson(doc()).replace('{', '{"futureSetting":{"a":1},');
    const back = readCube(text);
    assert.deepEqual(back.unknown, { futureSetting: { a: 1 } });
    assert.match(cubeToJson(back), /"futureSetting":\{"a":1\}/);
  });

  it('refuses what it cannot read, saying why', () => {
    assert.throws(() => readCube('{ not json'), /not valid JSON/);
    assert.throws(() => readCube('{"kind":"other"}'), /not a saved cube/);
    assert.throws(() => readCube('{"query":"select(~[a])","source":{}}'), /legacy DataCube specification/);
    assert.throws(() => readCube(cubeToJson({ ...doc(), version: CUBE_VERSION + 1 })), /newer version/);
    const noSource = JSON.parse(cubeToJson(doc()));
    noSource.source = { _type: 'pointer' };
    assert.throws(() => readCube(noSource), CubeDocumentError);
    assert.equal(doc().kind, CUBE_KIND);
  });
});

describe('a cube over a saved query', () => {
  // the query itself, as a share link carries it (query-store sharedPart): no store identity
  const QUERY_SOURCE: QuerySource = {
    _type: 'savedQuery',
    name: 'Sells',
    query: {
      name: 'Sells', groupId: 'demo', artifactId: 'trading', versionId: '0.0.0',
      executionContext: { _type: 'dataSpaceExecutionContext', dataSpacePath: 'demo::trading::TradingDataSpace', executionKey: 'Production' },
      content: "|demo::trading::Trade.all()->project(~[side: x|$x.side])",
      defaultParameterValues: [],
    },
    columns: [{ name: 'side', type: 'String' }],
  };
  const over = () => writeCube({
    name: 'Sells by side', source: QUERY_SOURCE, snapshot: EVERY, configuration: CONFIG, tree: TreeState.fromPaths([]),
  });

  it('writes the query as its source, and reads it back exactly', () => {
    const back = readCube(cubeToJson(over()));
    assert.deepEqual(back.source, QUERY_SOURCE);
    assert.doesNotMatch(JSON.stringify(JSON.parse(cubeToJson(over())).source), /"id"|"owner"|"lastUpdatedAt"|"version"/, 'the store identity is not the query');
  });

  it('refuses an incomplete saved query source, naming what is missing', () => {
    const noContent = JSON.parse(cubeToJson(over()));
    delete noContent.source.query.content;
    assert.throws(() => readCube(noContent), /the saved query source has no content/);
    const noColumns = JSON.parse(cubeToJson(over()));
    delete noColumns.source.columns;
    assert.throws(() => readCube(noColumns), /saved query source is incomplete/);
  });
});

describe('a cube over a warehouse table, or a remote file: where it is, never how to get in', () => {
  const TABLE: WarehouseSource = {
    _type: 'warehouseTable', name: 'sales.orders', warehouse: 'https://warehouse.example.com', catalog: 'shop',
    schema: 'sales', table: 'orders', columns: [{ name: 'region', type: 'String' }],
  };
  const REMOTE: RemoteSource = {
    _type: 'remoteFile', name: 'trades.parquet', url: 'https://data.example.com/trades.parquet', columns: [{ name: 'qty', type: 'Integer' }],
  };
  const over = (source: WarehouseSource | RemoteSource) => writeCube({
    name: 'Orders', source, snapshot: EVERY, configuration: CONFIG, tree: TreeState.fromPaths([]),
  });

  it('writes each down and reads it back exactly', () => {
    assert.deepEqual(readCube(cubeToJson(over(TABLE))).source, TABLE);
    assert.deepEqual(readCube(cubeToJson(over(REMOTE))).source, REMOTE);
  });

  it('reads a warehouse table saved before catalogs were named as main, the only catalog then', () => {
    const before = JSON.parse(cubeToJson(over(TABLE)));
    delete before.source.catalog;
    assert.equal((readCube(before).source as WarehouseSource).catalog, 'main');
    const empty = JSON.parse(cubeToJson(over(TABLE)));
    empty.source.catalog = '';
    assert.throws(() => readCube(empty), /empty catalog/);
  });

  it('refuses one missing a part, or one carrying a credential', () => {
    const noTable = JSON.parse(cubeToJson(over(TABLE)));
    delete noTable.source.table;
    assert.throws(() => readCube(noTable), /the warehouse table source has no table/);
    const withToken = JSON.parse(cubeToJson(over(TABLE)));
    withToken.source.token = 'abc';
    assert.throws(() => readCube(withToken), /carries a credential/);
    const withSecret = JSON.parse(cubeToJson(over(REMOTE)));
    withSecret.source.secretAccessKey = 'shh';
    assert.throws(() => readCube(withSecret), /carries a credential/);
    const noUrl = JSON.parse(cubeToJson(over(REMOTE)));
    delete noUrl.source.url;
    assert.throws(() => readCube(noUrl), /the remote file source has no url/);
  });
});

describe('a cube over a dataframe: by its name on the engine that serves it, never its rows', () => {
  const FRAME: FrameSource = { _type: 'frame', name: 'trades', columns: [{ name: 'qty', type: 'Integer' }] };
  const over = (source: FrameSource) => writeCube({
    name: 'Trades', source, snapshot: EVERY, configuration: CONFIG, tree: TreeState.fromPaths([]),
  });

  it('writes it down and reads it back exactly', () => {
    assert.deepEqual(readCube(cubeToJson(over(FRAME))).source, FRAME);
  });

  it('refuses one whose name is not a frame\'s (a plain identifier), or with no columns', () => {
    const named = JSON.parse(cubeToJson(over(FRAME)));
    named.source.name = 'trades; drop';
    assert.throws(() => readCube(named), /the frame source has no name/);
    const noColumns = JSON.parse(cubeToJson(over(FRAME)));
    delete noColumns.source.columns;
    assert.throws(() => readCube(noColumns), /the frame source has no columns/);
  });
});

describe('opening a cube over its file as it is now', () => {
  const now = (cols: [string, string][]): ColumnSpec[] => cols.map(([name, type]) => ({ name, type }));

  it('opens unchanged when the file holds what it held', () => {
    const opened = openCube(readCube(cubeToJson(doc())), RELATION,
      now([['region', 'String'], ['desk', 'String'], ['notional', 'Float'], ['qty', 'Integer']]));
    assert.deepEqual(opened.notes, []);
    assert.equal(opened.changed, false);
    assert.deepEqual(opened.snapshot.rows, ['region']);
    assert.equal(opened.snapshot.columns.find((c) => c.name === 'notional')?.aggregate, 'average',
      'what the cube set on a column is kept');
    assert.equal(opened.configuration.maxRows, 777);
    assert.equal(opened.tree.isOpen(['EMEA']), true);
  });

  it('hides a new column and says so', () => {
    const opened = openCube(doc(), RELATION,
      now([['region', 'String'], ['desk', 'String'], ['notional', 'Float'], ['qty', 'Integer'], ['book', 'String']]));
    assert.equal(opened.configuration.columns['book']?.hidden, true);
    assert.match(opened.notes.join('\n'), /1 new column .*book/);
    assert.equal(opened.changed, false);
  });

  it('leaves out every part that uses a column that is gone, naming each', () => {
    const opened = openCube(doc(), RELATION,
      now([['region', 'String'], ['desk', 'String'], ['qty', 'Integer']]));
    const text = opened.notes.join('\n');
    assert.match(text, /no longer in the file: notional/);
    assert.match(text, /left out calculated column scaled \(it uses notional\)/);
    assert.match(text, /left out calculated column doubled/);
    assert.match(text, /left out measure notional/);
    assert.match(text, /left out measure w/);
    assert.equal(opened.changed, true);
    assert.deepEqual(opened.snapshot.derived, []);
    assert.deepEqual(opened.snapshot.measures, []);
    assert.deepEqual(opened.snapshot.rows, ['region'], 'what does not use it stays');
    assert.ok(opened.snapshot.filter, 'the filter on region and qty stays');
  });

  it('drops a filter condition on a gone column and keeps the rest', () => {
    const opened = openCube(doc(), RELATION,
      now([['region', 'String'], ['desk', 'String'], ['notional', 'Float']]));
    assert.deepEqual(opened.snapshot.filter,
      { kind: 'condition', column: 'region', operator: 'equal', value: 'EMEA' });
    assert.match(opened.notes.join('\n'), /left out the filter on qty/);
  });

  it('reports a changed type and takes the compiler\'s', () => {
    const opened = openCube(doc(), RELATION,
      now([['region', 'String'], ['desk', 'String'], ['notional', 'Decimal'], ['qty', 'Integer']]));
    assert.match(opened.notes.join('\n'), /notional is now Decimal \(it was Float\)/);
    assert.equal(opened.snapshot.columns.find((c) => c.name === 'notional')?.type, 'Decimal');
  });

  it('closes the open rows when the grouping changed', () => {
    const opened = openCube(doc(), RELATION, now([['desk', 'String'], ['notional', 'Float'], ['qty', 'Integer']]));
    assert.deepEqual(opened.snapshot.rows, []);
    assert.equal(opened.tree.isOpen(['EMEA']), false);
  });
});

describe('the pieces', () => {
  it('knows which columns a calculated column reads', () => {
    assert.deepEqual([...derivedReads(EVERY.derived[0]!)], ['notional']);
    assert.deepEqual([...derivedReads({ name: 'r', window: { fn: 'sum', column: 'qty', partition: ['region'], order: [{ column: 'desk', direction: 'asc' }] } })].sort(),
      ['desk', 'qty', 'region']);
  });

  it('fingerprints a file by its bytes', async () => {
    assert.equal(await sha256(new TextEncoder().encode('abc').buffer as ArrayBuffer),
      'ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad');
    const src = await fileSource({ name: 'a.csv', size: 3, arrayBuffer: async () => new TextEncoder().encode('abc').buffer as ArrayBuffer },
      'csv', [{ name: 'a', type: 'String' }], { id: 'trades', rows: 10 });
    assert.equal(src.sha256.slice(0, 8), 'ba7816bf');
    assert.deepEqual(src.sample, { id: 'trades', rows: 10 });
  });

  it('differences and merges are inverses', () => {
    const base = { a: 1, b: { c: 2, d: 3 }, e: [1, 2], f: 'x' };
    const value = { a: 1, b: { c: 5, d: 3 }, e: [1, 2, 3] };
    const d = difference(value, base);
    assert.deepEqual(d, { b: { c: 5 }, e: [1, 2, 3], f: null });
    assert.deepEqual(merge(base, d), value);
  });
});
