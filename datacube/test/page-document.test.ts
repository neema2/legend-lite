// The saved page (src/page-document.ts): a cube's own document wrapped, with its charts
// and their sheets' layouts -- written whole, read whole, versioned, unknown fields kept, a
// bad page refused by name; a page of versions 1 and 2 read as one sheet; and a cube alone
// still read as a cube.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { cubeToJson, writeCube, type FileSource } from '../src/cube-document.ts';
import { DEFAULT_CONFIGURATION } from '../src/config.ts';
import {
  FIRST_SHEET,
  PAGE_KIND,
  PageDocumentError,
  oneSheet,
  pageDefinitionText,
  pageToJson,
  readPage,
  readSaved,
  writePage,
  type PageLayout,
  type PageViews,
} from '../src/page-document.ts';
import { givenName } from '../src/page-names.ts';
import type { ChartSpec } from '../src/chart-spec.ts';
import type { CubeSnapshot } from '../src/snapshot.ts';
import { TreeState } from '../src/tree.ts';
import { accessor, col, lambda, lit, times } from '../../pure-protocol/src/index.ts';

const SOURCE: FileSource = {
  _type: 'file', name: 'trades.csv', format: 'csv', size: 120, sha256: 'ab'.repeat(32),
  columns: [{ name: 'region', type: 'String' }, { name: 'desk', type: 'String' }, { name: 'notional', type: 'Float' }],
};

const SNAPSHOT: CubeSnapshot = {
  source: { query: accessor('local::DB', 'trades') },
  columns: [{ name: 'region', type: 'String' }, { name: 'desk', type: 'String' }, { name: 'notional', type: 'Float' }],
  // an exact decimal, which only the protocol's JSON writes right
  derived: [{ name: 'scaled', type: 'Decimal', lambda: lambda(['x'], times(col('x', 'notional'), lit.decimal('12.30'))) }],
  rows: ['region', 'desk'],
  pivotOn: [],
  measures: [],
  sorts: [],
  filter: {
    kind: 'and',
    children: [
      { kind: 'condition', column: 'desk', operator: 'equal', value: 'FX' },
      { kind: 'condition', column: 'region', operator: 'equal', value: 'EMEA' },
    ],
  },
  epoch: 3,
};

const CUBE = writeCube({
  name: 'draft', source: SOURCE, snapshot: SNAPSHOT,
  configuration: { ...DEFAULT_CONFIGURATION, reportTitle: 'Trades' },
  tree: TreeState.fromPaths([['EMEA']], false),
});

const SPEC: ChartSpec = {
  version: 2,
  mark: 'bar',
  x: 'region',
  y: [{ column: 'notional', fn: 'sum' }],
  split: 'desk',
  options: { orientation: 'vertical', stack: 'none', sort: { by: 'y', direction: 'desc' }, limit: 50, labels: false, legend: 'top' },
  frozen: true,
};

const { frozen: _frozen, ...LIVE } = SPEC;

const VIEWS: PageViews = {
  views: [
    { id: 'grid', kind: 'grid', cube: 'cube' },
    {
      id: 'chart-1', kind: 'chart', cube: 'cube', title: 'Notional by region', spec: SPEC,
      selection: [{ kind: 'condition', column: 'region', operator: 'equal', value: 'EMEA' }],
    },
    { id: 'chart-2', kind: 'chart', cube: 'cube', title: 'Chart 2', spec: { ...LIVE, mark: 'line' } },
  ],
  sheets: oneSheet({
    kind: 'bands',
    fit: false,
    bands: [
      { height: 0.6, node: { split: 'row', parts: [{ node: { tile: 'grid' }, size: 0.7 }, { node: { tile: 'chart-1' }, size: 0.3 }] } },
      { height: 0.4, node: { tile: 'chart-2' } },
    ],
  }),
};
const LAYOUT: PageLayout = VIEWS.sheets[0]!.layout;

const page = () => writePage({ name: 'Q3 page', cube: CUBE, views: VIEWS });

describe('a saved page', () => {
  it('reads back its cube, views and layout, exactly, through JSON text', () => {
    const back = readPage(pageToJson(page()));
    assert.equal(back.kind, PAGE_KIND);
    assert.equal(back.name, 'Q3 page');
    assert.equal(back.cubes.length, 1);
    // the cube inside is the cube's own document, and takes the page's name
    assert.equal(back.cubes[0]!.cube.name, 'Q3 page');
    assert.equal(cubeToJson(back.cubes[0]!.cube), cubeToJson({ ...CUBE, name: 'Q3 page' }));
    assert.match(pageToJson(back), /12\.30/, 'the decimal keeps its digits');
    assert.deepEqual(back.sheets, VIEWS.sheets);
    const chart = back.views.find((v) => v.id === 'chart-1');
    assert.ok(chart && chart.kind === 'chart');
    assert.equal(chart.title, 'Notional by region');
    assert.deepEqual(chart.spec, SPEC);
    assert.deepEqual(chart.selection, [{ kind: 'condition', column: 'region', operator: 'equal', value: 'EMEA' }]);
    assert.equal(pageDefinitionText(back), pageDefinitionText(page()));
  });

  it('reads a version 1 chart as version 2: every field 2 adds is optional, so only its number moves', () => {
    const raw = JSON.parse(pageToJson(page()));
    raw.views = raw.views.map((v: Record<string, unknown>) => (v['kind'] === 'chart' ? { ...v, spec: { ...(v['spec'] as object), version: 1 } } : v));
    const chart = readPage(JSON.stringify(raw)).views.find((v) => v.id === 'chart-1');
    assert.ok(chart && chart.kind === 'chart');
    assert.deepEqual(chart.spec, SPEC);
  });

  it('keeps every version 2 slot it reads', () => {
    const rich: ChartSpec = {
      ...SPEC,
      grain: 'month',
      y: [
        { column: 'notional', fn: 'sum', mark: 'bar' },
        { column: 'margin', fn: 'value', mark: 'line', axis: 'right', showAs: { kind: 'movingAverage', rows: 3 } },
      ],
      size: { column: 'notional', fn: 'count' },
      tooltip: [{ column: 'notional', fn: 'average' }],
      facet: { column: 'desk' },
      layers: [
        { kind: 'line', value: 100, label: 'Target' },
        { kind: 'stat', stat: 'average' },
        { kind: 'band', axis: 'x', from: '2024-01-01', to: '2024-03-31' },
        { kind: 'threshold', value: 0 },
        { kind: 'extremes', measure: 1 },
      ],
      options: { ...SPEC.options, smooth: true, zoom: true, decal: true },
    };
    const raw = JSON.parse(pageToJson(page()));
    raw.views = raw.views.map((v: Record<string, unknown>) => (v['id'] === 'chart-1' ? { ...v, spec: rich } : v));
    const chart = readPage(JSON.stringify(raw)).views.find((v) => v.id === 'chart-1');
    assert.ok(chart && chart.kind === 'chart');
    assert.deepEqual(chart.spec, rich);
  });

  it('is read by its kind; a cube alone is still a cube', () => {
    assert.equal(readSaved(pageToJson(page())).kind, 'page');
    const cube = readSaved(cubeToJson(CUBE));
    assert.equal(cube.kind, 'cube');
    assert.equal(cube.kind === 'cube' && cube.cube.name, 'draft');
  });

  it('keeps fields a newer writer added, and refuses a newer version by name', () => {
    const raw = JSON.parse(pageToJson(page()));
    const withMore = { ...raw, variables: [{ name: 'asOf' }] };
    const back = readPage(JSON.stringify(withMore));
    assert.deepEqual(back.unknown, { variables: [{ name: 'asOf' }] });
    assert.deepEqual(JSON.parse(pageToJson(back)).variables, [{ name: 'asOf' }]);
    assert.throws(() => readPage(JSON.stringify({ ...raw, version: 5 })), /newer version \(5 > 4\)/);
  });

  it('keeps where its classes are -- its project, its workspace, its package, each grid\'s table -- strictly', () => {
    // writePage's one cube is PAGE_CUBE ('cube')
    const model = { project: 'datacube:trades', workspace: 'datacube', package: 'trades', tables: { cube: 'trades_2' } };
    const base = page();
    const made = writePage({ name: 'Q3 page', cube: CUBE, views: VIEWS, model });
    const back = readPage(pageToJson(made));
    assert.deepEqual(back.model, model);
    assert.equal(base.model, undefined, 'a page with no class has no model');
    assert.notEqual(pageDefinitionText(back), pageDefinitionText(base), 'a class made: the page has changed');
    const raw = JSON.parse(pageToJson(made));
    assert.throws(() => readPage(JSON.stringify({ ...raw, model: { ...model, project: '' } })), /'model' has no project/);
    assert.throws(() => readPage(JSON.stringify({ ...raw, model: { ...model, tables: { nope: 't' } } })),
      /keeps a table for nope, which is no cube of the page/);
    assert.throws(() => readPage(JSON.stringify({ ...raw, model: 'datacube:trades' })), /'model' is not an object/);
  });

  it('writes a table kept only for a cube it holds: a grid removed since its class was made, the page still opens', () => {
    const model = { project: 'datacube:trades', workspace: 'datacube', package: 'trades', tables: { cube: 'trades_2', gone: 'customers' } };
    const written = writePage({ name: 'Q3 page', cube: CUBE, views: VIEWS, model });
    assert.deepEqual(written.model?.tables, { cube: 'trades_2' });
    assert.deepEqual(readPage(pageToJson(written)).model, { ...model, tables: { cube: 'trades_2' } });
  });

  it('writes a class kept only for a cube it holds: the class of every grid removed goes with its table', () => {
    const model = { project: 'datacube:trades', workspace: 'datacube', package: 'trades', tables: { gone: 'customers' },
      classes: { gone: 'trades::domain::Customer' } };
    const written = writePage({ name: 'Q3 page', cube: CUBE, views: VIEWS, model });
    assert.deepEqual(written.model, { project: 'datacube:trades', workspace: 'datacube', package: 'trades', tables: {} });
    assert.deepEqual(readPage(pageToJson(written)).model, written.model);
  });

  it('says what is wrong with a page it cannot read', () => {
    const raw = JSON.parse(pageToJson(page()));
    const bad = (patch: Record<string, unknown>) => () => readPage(JSON.stringify({ ...raw, ...patch }));
    assert.throws(bad({ cubes: [] }), PageDocumentError);
    assert.throws(bad({ cubes: [] }), /no cube/);
    // two cubes of one id: a view could not say which it shows
    const cubes = raw['cubes'] as unknown[];
    assert.throws(bad({ cubes: [...cubes, ...cubes] }), /two cubes share an id/);
    assert.throws(bad({ views: [{ id: 'x', kind: 'map', cube: 'cube' }] }), /unknown kind "map"/);
    assert.throws(bad({ views: [{ id: 'x', kind: 'grid', cube: 'elsewhere' }] }), /shows no cube of this page/);
    assert.throws(bad({ views: [{ id: 'c', kind: 'chart', cube: 'cube', title: 'c', spec: { version: 1, mark: 'violin', y: [], options: {} } }] }),
      /no chart it can draw/);
    // version 2's slots (chart-spec.ts §3.1): a word this DataCube does not know is refused by name, never drawn as
    // something else
    const chart = (spec: Record<string, unknown>) => bad({ views: [{ id: 'c', kind: 'chart', cube: 'cube', title: 'c',
      spec: { version: 2, mark: 'bar', x: 'region', y: [{ column: 'notional', fn: 'sum' }], options: {}, ...spec } }] });
    assert.throws(chart({ version: 3 }), /version 3 is not one this DataCube reads/);
    assert.throws(chart({ grain: 'decade' }), /grain "decade"/);
    assert.throws(chart({ y: [{ column: 'notional', fn: 'mode' }] }), /measure 1 is not a column and an aggregate/);
    assert.throws(chart({ y: [{ column: 'notional', fn: 'sum', mark: 'pie' }] }), /mark "pie" is not bar, line or area/);
    assert.throws(chart({ y: [{ column: 'notional', fn: 'sum', showAs: { kind: 'forecast' } }] }), /shown as something/);
    assert.throws(chart({ layers: [{ kind: 'sparkle' }] }), /layer 1 is of a kind this DataCube does not know \("sparkle"\)/);
    assert.throws(chart({ layers: [{ kind: 'stat', stat: 'mode' }] }), /statistic "mode"/);
    assert.throws(chart({ drill: { levels: ['region'], path: ['EMEA'] } }), /drill path/);
    const layout = (node: unknown, height = 1) => ({ kind: 'bands', fit: false, bands: [{ height, node }] });
    const band = (node: unknown, height = 1) => ({ sheets: [{ id: FIRST_SHEET, layout: layout(node, height) }] });
    assert.throws(bad(band({ tile: 'nowhere' })), /shows no view/);
    assert.throws(bad(band({ split: 'row', parts: [{ node: { tile: 'grid' }, size: 1 }] })),
      /sheet sheet-1's layout cannot be laid out: band 0: a split of 1 part/);
    assert.throws(bad(band({ tile: 'grid' }, -1)), /cannot be laid out: band 0: a height of -1/);
    assert.throws(bad(band({ split: 'diagonal', parts: [] })), /not a tile, a stack or a split of parts/);
    assert.throws(bad({ sheets: [{ id: FIRST_SHEET, layout: { kind: 'grid', cols: 12, tiles: [] } }] }), /not a layout of bands/);
    // a view with no place: it would be put somewhere on opening, and the page read as changed at once
    assert.throws(bad(band({ split: 'row', parts: [{ node: { tile: 'grid' }, size: 0.5 }, { node: { tile: 'chart-1' }, size: 0.5 }] })),
      /no sheet has a place for chart-2/);
    // the sheets: a list, each with an id of its own, a name that is a name, every view on one of them only
    assert.throws(bad({ sheets: [] }), /not a list of sheets/);
    const all = { split: 'column', parts: [{ node: { tile: 'grid' }, size: 0.5 }, { node: { split: 'row', parts: [
      { node: { tile: 'chart-1' }, size: 0.5 }, { node: { tile: 'chart-2' }, size: 0.5 }] }, size: 0.5 }] };
    assert.throws(bad({ sheets: [{ id: 'a', layout: layout(all) }, { id: 'a', layout: layout({ tile: 'chart-2' }) }] }), /two sheets are a/);
    assert.throws(bad({ sheets: [{ id: 'a', name: ' ', layout: layout(all) }] }), /sheet a's name is not a name/);
    // DataCube's one name rule (givenName): a page's name and a tile's title are not blank
    assert.throws(bad({ name: '  ' }), /the page's name is blank/);
    assert.throws(bad({ views: [{ id: 'c', kind: 'chart', cube: 'cube', title: ' ',
      spec: { version: 2, mark: 'bar', x: 'region', y: [{ column: 'notional', fn: 'sum' }], options: {} } }] }),
    /chart c's title is blank/);
    assert.throws(bad({ views: [{ id: 'g', kind: 'grid', cube: 'cube', title: '' }] }), /grid g's title is blank/);
    assert.throws(bad({ sheets: [{ id: 'a', layout: layout(all) }, { id: 'b', layout: layout({ tile: 'chart-2' }) }] }),
      /chart-2 on two sheets/);
    assert.throws(() => readPage('{'), /not valid JSON/);
  });

  it('is changed by a chart, a title, the layout or a sheet\'s name; not by its name', () => {
    const base = pageDefinitionText(page());
    const renamed = writePage({ name: 'other', cube: CUBE, views: VIEWS });
    assert.equal(pageDefinitionText(renamed), base);
    const moved = { ...VIEWS, sheets: oneSheet({ ...LAYOUT, bands: LAYOUT.bands.map((b, i) => (i === 1 ? { ...b, height: 0.5 } : b)) }) };
    assert.notEqual(pageDefinitionText(writePage({ name: 'Q3 page', cube: CUBE, views: moved })), base);
    const sheetNamed = { ...VIEWS, sheets: [{ ...VIEWS.sheets[0]!, name: 'Summary' }] };
    assert.notEqual(pageDefinitionText(writePage({ name: 'Q3 page', cube: CUBE, views: sheetNamed })), base);
    const retitled = { ...VIEWS, views: VIEWS.views.map((v) => (v.id === 'chart-2' ? { ...v, title: 'Lines' } : v)) };
    assert.notEqual(pageDefinitionText(writePage({ name: 'Q3 page', cube: CUBE, views: retitled })), base);
  });

  it('holds a cube with no charts: its grid alone, the whole board', () => {
    const alone: PageViews = {
      views: [{ id: 'grid', kind: 'grid', cube: 'cube' }],
      sheets: oneSheet({ kind: 'bands', fit: false, bands: [{ height: 1, node: { tile: 'grid' } }] }),
    };
    const back = readPage(pageToJson(writePage({ name: 'plain', cube: CUBE, views: alone })));
    assert.deepEqual(back.views, alone.views);
    assert.deepEqual(back.sheets, alone.sheets);
  });

  it('holds several sheets, in order, each its own layout and its name when it was given one', () => {
    const sheets: PageViews = {
      views: VIEWS.views,
      sheets: [
        { id: 'sheet-2', name: 'Charts', layout: { kind: 'bands', fit: true, bands: [{ height: 1, node: { split: 'row', parts: [
          { node: { tile: 'chart-1' }, size: 0.5 }, { node: { tile: 'chart-2' }, size: 0.5 }] } }] } },
        { id: FIRST_SHEET, layout: { kind: 'bands', fit: true, bands: [{ height: 1, node: { tile: 'grid' } }] } },
      ],
    };
    const back = readPage(pageToJson(writePage({ name: 'Q3 page', cube: CUBE, views: sheets })));
    assert.deepEqual(back.sheets, sheets.sheets);
    assert.equal(back.version, 4);
  });

  it('holds a stack of tiles in one place: its tiles in their tabs\' order, read back as they were', () => {
    const stacked: PageViews = {
      views: VIEWS.views,
      sheets: oneSheet({ kind: 'bands', fit: false, bands: [{ height: 1, node: { split: 'row', parts: [
        { node: { tile: 'grid' }, size: 0.5 }, { node: { stack: ['chart-2', 'chart-1'] }, size: 0.5 }] } }] }),
    };
    const back = readPage(pageToJson(writePage({ name: 'Q3 page', cube: CUBE, views: stacked })));
    assert.deepEqual(back.sheets, stacked.sheets);
    const raw = JSON.parse(pageToJson(page()));
    const sheetOf = (node: unknown) => ({ sheets: [{ id: FIRST_SHEET, layout: { kind: 'bands', fit: false, bands: [{ height: 1, node }] } }] });
    assert.throws(() => readPage(JSON.stringify({ ...raw, ...sheetOf({ stack: ['grid', 'nowhere'] }) })), /stack holds no view/);
    assert.throws(() => readPage(JSON.stringify({ ...raw, ...sheetOf({ split: 'row', parts: [
      { node: { stack: ['grid'] }, size: 0.5 }, { node: { split: 'column', parts: [{ node: { tile: 'chart-1' }, size: 0.5 }, { node: { tile: 'chart-2' }, size: 0.5 }] }, size: 0.5 }] }) })),
    /a stack of 1 tile/);
  });

  it('opens a page saved as version 2 (one layout) as one sheet, and writes it back as version 4', () => {
    const raw = JSON.parse(pageToJson(page()));
    const { sheets: _sheets, ...rest } = raw;
    const v2 = { ...rest, version: 2, layout: LAYOUT };
    const back = readPage(JSON.stringify(v2));
    assert.equal(back.version, 4);
    assert.deepEqual(back.sheets, [{ id: FIRST_SHEET, layout: LAYOUT }]);
    const written = JSON.parse(pageToJson(back));
    assert.equal(written.version, 4);
    assert.equal(written.layout, undefined, 'its layout is in its one sheet, not beside it');
  });

  it('opens a page saved as version 1 (tiles on a 12-column grid) as one sheet of bands, and writes it back as version 4', () => {
    const { sheets: _sheets, ...raw } = JSON.parse(pageToJson(page()));
    const v1 = { ...raw, version: 1, layout: { kind: 'grid', cols: 12, arranged: true, tiles: [
      { id: 'grid', x: 0, y: 0, w: 8, h: 14 },
      { id: 'chart-1', x: 8, y: 0, w: 4, h: 14 },
      { id: 'chart-2', x: 0, y: 14, w: 12, h: 10 },
    ] } };
    const back = readPage(JSON.stringify(v1));
    assert.equal(back.version, 4);
    assert.deepEqual(back.sheets, oneSheet({ kind: 'bands', fit: false, bands: [
      { height: 14 / 24, node: { split: 'row', parts: [{ node: { tile: 'grid' }, size: 8 / 12 }, { node: { tile: 'chart-1' }, size: 4 / 12 }] } },
      { height: 10 / 24, node: { tile: 'chart-2' } },
    ] }));
    assert.equal(JSON.parse(pageToJson(back)).version, 4);
    // a version 1 page's grid that is wrong is still refused by name
    assert.throws(() => readPage(JSON.stringify({ ...v1, layout: { ...v1.layout, tiles: [{ id: 'grid', x: -1, y: 0, w: 1, h: 1 }] } })),
      /not an id and a place/);
  });
});

describe("a name a person gives, by DataCube's one rule (givenName: the screen's renames and Python's ask it)", () => {
  it('a page and a tile: trimmed, a blank refused', () => {
    assert.deepEqual(givenName('page', '  Sales  '), { name: 'Sales' });
    assert.deepEqual(givenName('tile', 'Totals'), { name: 'Totals' });
    assert.deepEqual(givenName('page', '   '), { refused: "a page's name is not blank" });
    assert.deepEqual(givenName('tile', ''), { refused: "a tile's name is not blank" });
  });

  it("a sheet: trimmed, a blank none of its own (it is named after its first grid)", () => {
    assert.deepEqual(givenName('sheet', ' Summary '), { name: 'Summary' });
    assert.deepEqual(givenName('sheet', '  '), { name: undefined });
  });
});
