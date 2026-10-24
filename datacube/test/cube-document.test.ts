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
  gridCapped,
  merge,
  openCube,
  readCube,
  rebuilt,
  sha256,
  writeCube,
  type ClassSource,
  type FileSource,
  type QuerySource,
  type FrameSource,
  type RemoteSource,
  type WarehouseSource,
} from '../src/cube-document.ts';
import { DEFAULT_CONFIGURATION, DEFAULT_MAX_ROWS, type CubeConfiguration } from '../src/config.ts';
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
  topN: { by: 'notional', n: 5, bottom: true },
  leafCount: true,
  childCount: true,
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
  it("writes no group-level column's type: a run learns it (its level query's plan), so no side writes one (V4)", () => {
    // the screen's snapshot after a run carries the type its level query's plan gave; a host's, never run, none
    const ran = { ...EVERY, groupDerived: EVERY.groupDerived.map((d) => ({ ...d, type: 'Integer' })) };
    const written = (snapshot: CubeSnapshot) => cubeToJson(writeCube({
      name: 'Q3 review', source: SOURCE, snapshot, configuration: CONFIG, tree: TreeState.fromPaths([]),
    }));
    assert.equal(written(ran), written(EVERY), 'the same document, run or not');
    assert.equal(readCube(written(ran)).query.groupDerived?.[0]?.type, undefined);
  });

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

  it('refuses a warehouse table with no catalog: every cube of this version names it', () => {
    const none = JSON.parse(cubeToJson(over(TABLE)));
    delete none.source.catalog;
    assert.throws(() => readCube(none), /has no catalog/);
    const empty = JSON.parse(cubeToJson(over(TABLE)));
    empty.source.catalog = '';
    assert.throws(() => readCube(empty), /has no catalog/);
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
    // nor a literal's word: Python's frames refuse it as a name (frames._NAME), as the grammar reads it as a literal
    named.source.name = 'true';
    assert.throws(() => readCube(named), /the frame source has no name/);
    const noColumns = JSON.parse(cubeToJson(over(FRAME)));
    delete noColumns.source.columns;
    assert.throws(() => readCube(noColumns), /the frame source has no columns/);
  });
});

describe('a cube over a class: by what it is -- the class, its project and the revision it was read at', () => {
  const CLASS: ClassSource = {
    _type: 'class', name: 'Trade', class: 'trades::domain::Trade', project: 'datacube:trades', workspace: 'datacube',
    revision: '3', mapping: 'trades::mapping::Model', runtime: 'trades::runtime::Browser',
    columns: [{ name: 'region', type: 'String' }, { name: 'notional', type: 'Float' }],
  };
  const over = (source: ClassSource) => writeCube({
    name: 'Trades', source, snapshot: EVERY, configuration: CONFIG, tree: TreeState.fromPaths([]),
  });

  it('writes it down and reads it back exactly', () => {
    assert.deepEqual(readCube(cubeToJson(over(CLASS))).source, CLASS);
  });

  it('refuses one missing what reopens it, and a column that is no property\'s name', () => {
    for (const f of ['class', 'project', 'workspace', 'revision', 'mapping', 'runtime']) {
      const raw = JSON.parse(cubeToJson(over(CLASS)));
      delete raw.source[f];
      assert.throws(() => readCube(raw), new RegExp(`the class source has no ${f}`));
    }
    const noColumns = JSON.parse(cubeToJson(over(CLASS)));
    delete noColumns.source.columns;
    assert.throws(() => readCube(noColumns), /the class source has no columns/);
    // a column is read as `$x.p`: a name the grammar reads bare, so a pivot's name or a literal's word is none
    const quoted = JSON.parse(cubeToJson(over(CLASS)));
    quoted.source.columns[0].name = 'true';
    assert.throws(() => readCube(quoted), /the class source's column 'true' is no property's name/);
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
    assert.equal(opened.changed, true);
    assert.deepEqual(opened.snapshot.derived, []);
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

  it('keeps the expand level and the groups closed under it, the grouping as it was (V5)', () => {
    const levelled = writeCube({
      name: 'Q3 review', source: SOURCE, snapshot: EVERY, configuration: CONFIG,
      tree: TreeState.empty(true).withExpandTo(1).setOpen(['EMEA'], false),
    });
    const opened = openCube(readCube(cubeToJson(levelled)), RELATION,
      now([['region', 'String'], ['desk', 'String'], ['notional', 'Float'], ['qty', 'Integer']]));
    assert.equal(opened.tree.expandTo, 1);
    assert.equal(opened.tree.isOpen(['AMER'], 2), true, 'a group the level opens is open');
    assert.equal(opened.tree.isOpen(['EMEA'], 2), false, 'the one closed under it stays closed');
    // regrouped, the closed paths are of the old grouping: the level stays, they go
    const regrouped = openCube(readCube(cubeToJson(levelled)), RELATION, now([['desk', 'String'], ['notional', 'Float'], ['qty', 'Integer']]));
    assert.deepEqual([regrouped.tree.expandTo, regrouped.tree.closedPaths], [1, []]);
  });

  it('refuses a saved expand level or closed groups it cannot read, in words', () => {
    const text = (tree: unknown): string => JSON.stringify({ ...JSON.parse(cubeToJson(doc())), tree });
    assert.throws(() => readCube(text({ open: [], showTotals: false, expandTo: -1 })), /expand level is a whole number/);
    assert.throws(() => readCube(text({ open: [], showTotals: false, expandTo: 1, closed: 'EMEA' })), /closed groups are row paths/);
    // closed under a level it has not: refused, never dropped (the S2 audit's 10)
    assert.throws(() => readCube(text({ open: [], showTotals: false, closed: [['EMEA']] })), /closed under its expand level: it has none/);
    assert.throws(() => readCube(text({ open: [], showTotals: false, expandTo: 0, closed: [['EMEA']] })), /it has none/);
  });

  it('closes the open rows when the grouping changed', () => {
    const opened = openCube(doc(), RELATION, now([['desk', 'String'], ['notional', 'Float'], ['qty', 'Integer']]));
    assert.deepEqual(opened.snapshot.rows, []);
    assert.equal(opened.tree.isOpen(['EMEA']), false);
  });

  // NO OLDER CUBE IS READ (the user, 2026-10-17): a version 1 cube -- every cube saved before a grid's aggregates became
  // its columns' Aggregation (docs/DATACUBE_ONE_GRID_EDIT_PATH_2026_10_17.md) -- is refused by its version, and a
  // measures list in a cube of this one by name: never converted, never opened showing other numbers than it showed
  it('refuses a calculated column whose lambda the protocol cannot read, as the document it is in', () => {
    const broken = JSON.parse(cubeToJson(doc()));
    broken.query.derived[0].lambda.body = 'oops';
    assert.throws(() => readCube(broken), (e: unknown) => e instanceof CubeDocumentError
      && /calculated column "scaled"'s lambda: .*body/.test(e.message));
  });

  it('refuses a cube saved before a grid\'s aggregates were its columns\' Aggregation, and a measures list', () => {
    assert.throws(() => readCube(cubeToJson({ ...doc(), version: 1 })), /an older version \(1 < 2\).*no longer read/);
    const listed = JSON.parse(cubeToJson(doc()));
    listed.query.measures = [{ name: 'notional', column: 'notional', fn: 'sum' }];
    assert.throws(() => readCube(listed), /'query\.measures': a grid carries no measures list/);
    assert.equal('measures' in doc().query, false, 'a cube written carries none');
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

describe("a new grid is capped when it is not its page's only grid (gridCapped, counting it)", () => {
  const fresh = (gridsOnPage: number) =>
    rebuilt(undefined, accessor('db', 'T'), [{ name: 'a', type: 'Integer' }], 'T', gridCapped(gridsOnPage)).configuration;

  it("a page's only grid is not capped: a new page, a source opened in place, a reopened page of one", () => {
    assert.equal(gridCapped(1), false);
    assert.equal(fresh(1).maxRows, undefined);
  });

  it('the first grid added beside it is capped at the row limit, as is every grid of a reopened page of several', () => {
    assert.equal(gridCapped(2), true);
    assert.equal(gridCapped(3), true);
    assert.equal(fresh(2).maxRows, DEFAULT_MAX_ROWS);
  });
});

describe("a saved cube's format code is judged on open, as when it was set (format-code.ts)", () => {
  it('kept; refused naming the column and the part', () => {
    const coded = (code: string): string => cubeToJson(doc()).replace('"decimals":3', `"decimals":3,"code":${JSON.stringify(code)}`);
    assert.equal(readCube(coded('#,##0.000')).configuration['columns'] && (readCube(coded('#,##0.000')).configuration['columns'] as Record<string, { format?: { code?: string } }>)['notional']?.format?.code, '#,##0.000');
    assert.throws(() => readCube(coded('[>100]0')), /'configuration\.columns\.notional\.format\.code': the format code "\[>100\]0" has \[>100\]/);
  });
});
