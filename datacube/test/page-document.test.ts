// The saved page (src/page-document.ts): a cube's own document wrapped, with its charts
// and their sheets' layouts -- written whole, read whole, versioned, unknown fields kept, a
// bad page refused by name; a page of versions 1 and 2 read as one sheet; and a cube alone
// still read as a cube.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { cubeToJson, frameSource, rawRows, writeCube, type FileSource } from '../src/cube-document.ts';
import { DEFAULT_CONFIGURATION } from '../src/config.ts';
import {
  FIRST_SHEET,
  NOTHING_TO_SAVE,
  modelPageName,
  newPage,
  pageContent,
  pageFile,
  pageSaving,
  savedRecord,
  type SavedRecord,
  PAGE_KIND,
  PageDocumentError,
  oneSheet,
  pageDefinitionText,
  pageToJson,
  readPage,
  readSaved,
  sheetLabel,
  startingViews,
  suggestedName,
  writePage,
  writePageOf,
  cubesRead,
  type PageDocument,
  type PageView,
  type PageLayout,
  type PageViews,
} from '../src/page-document.ts';
import { givenName } from '../src/page-names.ts';
import { pageApply, pageFile as pageFileJson, pageNew, pageRead, pageRecord as pageRecordJson, pageSaved } from '../src/headless/page.ts';
import type { ChartSpec } from '../src/chart-spec.ts';
import type { CubeSnapshot } from '../src/snapshot.ts';
import { TreeState } from '../src/tree.ts';
import { accessor, col, element, lambda, lit, times, toJson } from '../../pure-protocol/src/index.ts';
import { bandsOf } from './bands-of.ts';

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

  it('refuses a version 1 chart: no older chart is read (the user, 2026-10-17)', () => {
    const raw = JSON.parse(pageToJson(page()));
    raw.views = raw.views.map((v: Record<string, unknown>) => (v['kind'] === 'chart' ? { ...v, spec: { ...(v['spec'] as object), version: 1 } } : v));
    assert.throws(() => readPage(JSON.stringify(raw)), /its version 1 is not one this DataCube reads \(2\)/);
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
    assert.throws(() => readPage(JSON.stringify({ ...raw, version: 7 })), /newer version \(7 > 6\)/);
  });

  it('keeps where its classes are -- its project, its workspace, its package, each grid\'s tables -- strictly', () => {
    // writePage's one cube is PAGE_CUBE ('cube'); a join's grid keeps each of its leaves' tables, in order (§18.3)
    const model = { project: 'datacube:trades', workspace: 'datacube', package: 'trades', tables: { cube: ['trades_2', 'desks'] } };
    const base = page();
    const made = writePage({ name: 'Q3 page', cube: CUBE, views: VIEWS, model });
    const back = readPage(pageToJson(made));
    assert.deepEqual(back.model, model);
    assert.equal(base.model, undefined, 'a page with no class has no model');
    assert.notEqual(pageDefinitionText(back), pageDefinitionText(base), 'a class made: the page has changed');
    const raw = JSON.parse(pageToJson(made));
    assert.throws(() => readPage(JSON.stringify({ ...raw, model: { ...model, project: '' } })), /'model' has no project/);
    assert.throws(() => readPage(JSON.stringify({ ...raw, model: { ...model, tables: { nope: ['t'] } } })),
      /keeps tables for nope, which is no cube of the page/);
    for (const bad of ['trades_2', [], ['trades_2', ''], [1]]) {
      assert.throws(() => readPage(JSON.stringify({ ...raw, model: { ...model, tables: { cube: bad } } })),
        /'model' keeps no list of table names for cube/, JSON.stringify(bad));
    }
    assert.throws(() => readPage(JSON.stringify({ ...raw, model: 'datacube:trades' })), /'model' is not an object/);
  });

  it('keeps the revision the page is pinned to; one that is no revision\'s id refused', () => {
    const model = { project: 'datacube:trades', workspace: 'datacube', package: 'trades', tables: { cube: ['trades_2'] }, revision: '7' };
    const made = writePage({ name: 'Q3 page', cube: CUBE, views: VIEWS, model });
    assert.deepEqual(readPage(pageToJson(made)).model, model);
    const raw = JSON.parse(pageToJson(made));
    assert.throws(() => readPage(JSON.stringify({ ...raw, model: { ...model, revision: '' } })), /'model' has a revision that is no revision's id/);
    assert.throws(() => readPage(JSON.stringify({ ...raw, model: { ...model, revision: 7 } })), /'model' has a revision that is no revision's id/);
  });

  it('writes a table kept only for a cube it holds: a grid removed since its class was made, the page still opens', () => {
    const model = { project: 'datacube:trades', workspace: 'datacube', package: 'trades', tables: { cube: ['trades_2'], gone: ['customers'] } };
    const written = writePage({ name: 'Q3 page', cube: CUBE, views: VIEWS, model });
    assert.deepEqual(written.model?.tables, { cube: ['trades_2'] });
    assert.deepEqual(readPage(pageToJson(written)).model, { ...model, tables: { cube: ['trades_2'] } });
  });

  it('writes a class kept only for a cube it holds: the class of every grid removed goes with its table', () => {
    const model = { project: 'datacube:trades', workspace: 'datacube', package: 'trades', tables: { gone: ['customers'] },
      classes: { gone: 'trades::domain::Customer' } };
    const written = writePage({ name: 'Q3 page', cube: CUBE, views: VIEWS, model });
    assert.deepEqual(written.model, { project: 'datacube:trades', workspace: 'datacube', package: 'trades', tables: {} });
    assert.deepEqual(readPage(pageToJson(written)).model, written.model);
  });

  // the second audit's A-B1: a page's writers took its cubes from each view's one `cube`, and wrote a timeseries' kept
  // cube off the page -- a file its reader refused. One rule now, every tile kind answering it (cubesRead), and a page
  // written out (pageContent: a file, a record, its keeper, a link, Python's) refused without a cube a view reads
  it('holds every cube its views read, each tile kind answering which; a page written out without one is refused', () => {
    const ts: PageView = { id: 'timeseries-1', kind: 'timeseries', spec: { sources: [
      { alias: 'px', cube: 'cube', date: 'region' }, { alias: 'kept', cube: 'grid-2', date: 'region' }], series: [] } };
    assert.deepEqual([...VIEWS.views, ts, { id: 'text-1', kind: 'text', text: 'hi' } as PageView].map(cubesRead),
      [['cube'], ['cube'], ['cube'], ['cube', 'grid-2'], []]);
    const views: PageViews = {
      views: [VIEWS.views[0]!, ts],
      sheets: oneSheet({ kind: 'bands', fit: false, bands: [{ height: 1, node: { split: 'row', parts: [
        { node: { tile: 'grid' }, size: 0.5 }, { node: { tile: 'timeseries-1' }, size: 0.5 }] } }] }),
    };
    // as the screen holds it now, it may name a cube it cannot write yet (a grid that cannot say its source); written
    // out, never
    const partial = writePageOf({ name: 'p', cubes: [{ id: 'cube', cube: CUBE }], views });
    assert.throws(() => pageToJson(partial), /the page is written without cube grid-2, which timeseries timeseries-1 reads/);
    assert.throws(() => pageFile(writePage({ name: 'p', cube: CUBE, views })), /without cube grid-2/);
    const held = writePageOf({ name: 'p', cubes: [{ id: 'cube', cube: CUBE }, { id: 'grid-2', cube: CUBE }], views });
    assert.deepEqual(readPage(pageToJson(held)).cubes.map((c) => c.id), ['cube', 'grid-2'], 'read back, its kept cube with it');
  });

  it('says what is wrong with a page it cannot read', () => {
    const raw = JSON.parse(pageToJson(page()));
    const bad = (patch: Record<string, unknown>) => () => readPage(JSON.stringify({ ...raw, ...patch }));
    // a page with no cube: DataCube opens no saved page of nothing (an open page of none is read with `empty`)
    assert.throws(bad({ cubes: [] }), /^PageDocumentError: cannot read saved page: it has no cube$/);
    assert.throws(bad({ cubes: [] }), PageDocumentError);
    assert.throws(bad({ cubes: [] }), /no cube/);
    // two cubes of one id: a view could not say which it shows
    const cubes = raw['cubes'] as unknown[];
    assert.throws(bad({ cubes: [...cubes, ...cubes] }), /two cubes share an id/);
    assert.throws(bad({ views: [{ id: 'x', kind: 'map', cube: 'cube' }] }), /unknown kind "map"/);
    assert.throws(bad({ views: [{ id: 'x', kind: 'grid', cube: 'elsewhere' }] }), /shows no cube of this page/);
    assert.throws(bad({ views: [{ id: 'c', kind: 'chart', cube: 'cube', title: 'c', spec: { version: 2, mark: 'violin', y: [], options: {} } }] }),
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
    const moved = { ...VIEWS, sheets: oneSheet({ ...bandsOf(LAYOUT), bands: bandsOf(LAYOUT).bands.map((b, i) => (i === 1 ? { ...b, height: 0.5 } : b)) }) };
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
    assert.equal(back.version, 6);
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

  it('refuses a page saved by an older version, by its version: no older page is read (the user, 2026-10-17)', () => {
    const { sheets: _sheets, ...raw } = JSON.parse(pageToJson(page()));
    for (const version of [1, 2, 3, 4, 5]) {
      assert.throws(() => readPage(JSON.stringify({ ...raw, version, layout: LAYOUT })),
        new RegExp(`saved by an older version \\(${version} < 6\\): it is no longer read`));
    }
  });
});

describe('a page as it starts (startingViews: the cube app, a cube saved before pages, Python\'s new page)', () => {
  it('is one skeleton, unnamed unless named (newPage: Python\'s new page and the tab\'s New Page, D11, B9)', () => {
    assert.deepEqual(newPage(), { ...readPage(pageToJson(newPage()), { empty: true }), name: '' });
    assert.equal(newPage().name, '');
    assert.equal(newPage('Q3').name, 'Q3');
    assert.deepEqual({ views: newPage().views, sheets: newPage().sheets }, startingViews());
    assert.deepEqual(newPage().cubes, []);
    // headless pageNew: unnamed when Python gives no name, else named by the one rule
    assert.equal((JSON.parse(JSON.parse(pageNew(null)).page as string) as PageDocument).name, '');
    assert.equal((JSON.parse(JSON.parse(pageNew('Q3')).page as string) as PageDocument).name, 'Q3');
    assert.deepEqual(JSON.parse(pageNew(' ')).problems, [(givenName('page', ' ') as { refused: string }).refused]);
  });

  it("is called by its sheet's name, else its first grid's report title, else by its place (sheetLabel)", () => {
    const grids = new Map([['g1', { title: 'trades' }], ['g2', { title: undefined }], ['g3', { title: 'desks' }]]);
    const grid = (t: string): { title: string | undefined } | undefined => grids.get(t);
    assert.equal(sheetLabel({ name: 'Mine', tiles: ['g1'] }, 0, grid), 'Mine');
    assert.equal(sheetLabel({ tiles: ['chart', 'g1', 'g3'] }, 0, grid), 'trades');
    // the first grid's title only: one with none is the sheet's place, a later grid's title aside
    assert.equal(sheetLabel({ tiles: ['g2', 'g3'] }, 2, grid), 'Sheet 3');
    assert.equal(sheetLabel({ tiles: [] }, 0, grid), 'Sheet 1');
  });

  it("is saved, unnamed, under its first sheet's name, and its model named after it without a file's extension (D11)", () => {
    const unnamed = JSON.parse(JSON.parse(pageNew(null)).page as string) as PageDocument;
    assert.equal(suggestedName(unnamed), 'Sheet 1');
    const cube = writeCube({ name: 'q', source: frameSource('q', [{ name: 'a', type: 'String' }]),
      snapshot: rawRows(element('q'), [{ name: 'a', type: 'String' }]),
      configuration: { ...DEFAULT_CONFIGURATION, reportTitle: 'trades.csv' }, tree: TreeState.empty() });
    const added = JSON.parse(pageApply(JSON.stringify(unnamed), '["sheet-1"]', JSON.stringify([
      { kind: 'addGrid', document: cube, sheet: 'sheet-1', near: null, title: null, columns: null },
    ]))) as { page: string };
    const page = readPage(added.page);
    assert.equal(page.name, '');
    assert.equal(suggestedName(page), 'trades.csv');
    assert.equal(suggestedName({ ...page, name: 'Q3' }), 'Q3');
    // headless pageRecord: Python's save() naming nothing, saved as the Save window suggests
    assert.equal(JSON.parse(pageRecordJson('r-1', null, added.page, false)).name, 'trades.csv');
    assert.equal(JSON.parse(pageRecordJson('r-1', 'Q4', added.page, false)).name, 'Q4');
    assert.equal(modelPageName('', suggestedName(page)), 'trades');
    assert.equal(modelPageName('Q3.final', 'x.csv'), 'Q3.final');
    assert.equal(modelPageName('', ''), 'page');
  });

  it('is its grid on the whole board, or an empty board a grid is added to', () => {
    assert.deepEqual(startingViews({ id: 'grid', cube: 'cube' }), {
      views: [{ id: 'grid', kind: 'grid', cube: 'cube' }],
      sheets: [{ id: FIRST_SHEET, layout: { kind: 'bands', fit: true, bands: [{ height: 1, node: { tile: 'grid' } }] } }],
    });
    assert.deepEqual(startingViews(), { views: [], sheets: [{ id: FIRST_SHEET, layout: { kind: 'bands', fit: true, bands: [] } }] });
  });

  it("is the page Python starts and lays its first grid on, by the same add (headless pageNew, pageApply's addGrid)", () => {
    const started = JSON.parse(JSON.parse(pageNew('Q3 review')).page as string) as PageDocument;
    const cube = writeCube({ name: 'q', source: frameSource('q', [{ name: 'a', type: 'String' }]),
      snapshot: rawRows(element('q'), [{ name: 'a', type: 'String' }]), configuration: DEFAULT_CONFIGURATION, tree: TreeState.empty() });
    const applied = JSON.parse(pageApply(JSON.stringify(started), '["sheet-1"]', JSON.stringify([
      { kind: 'addGrid', document: cube, sheet: 'sheet-1', near: null, title: null, columns: null },
    ]))) as { page: string };
    const laid = (JSON.parse(applied.page) as PageDocument).sheets[0]!.layout;
    assert.deepEqual(laid, startingViews({ id: 'grid-1', cube: 'grid-1' }).sheets[0]!.layout);
  });

  it("an added grid's cube keeps its exact numbers, and a cube its reader refuses is refused in its words", () => {
    // (the phase 1 audit's 3) the commands are read from their exact numbers, as the page's cubes are: 12.30 stays 12.30
    const started = JSON.parse(pageNew('Q3 review')).page as string;
    const add = (document: string): { page?: string; problems?: string[] } => JSON.parse(pageApply(started, '["sheet-1"]',
      `[{"kind":"addGrid","document":${document},"sheet":"sheet-1","near":null,"title":null,"columns":null}]`));
    assert.match(cubeToJson(CUBE), /12\.30/);
    assert.match(add(cubeToJson(CUBE)).page!, /12\.30/);
    assert.match(add('{"bogus":1}').problems![0]!, /cannot read saved cube/);
  });

  it("saved or written as a file (headless pageSaved: Python's save and to_json) only with something on it, refused in the screen's words (B14)", () => {
    const started = JSON.parse(pageNew('Q3 review')).page as string;
    assert.deepEqual(JSON.parse(pageSaved(started)), { problems: [NOTHING_TO_SAVE] });
    assert.equal(NOTHING_TO_SAVE, 'There is nothing on this page to save: add a data source first.');
    const added = JSON.parse(pageApply(started, '["sheet-1"]',
      `[{"kind":"addGrid","document":${cubeToJson(CUBE)},"sheet":"sheet-1","near":null,"title":null,"columns":null}]`)) as { page: string };
    assert.equal(JSON.parse(pageSaved(added.page)).page, JSON.parse(pageRead(added.page)).page, 'written as DataCube writes it');
    assert.match(JSON.parse(pageSaved('{"bogus":1}')).problems[0], /^cannot read saved page/);
  });

  it("saved under a name as the page store keeps it (savedRecord; headless pageRecord: Python's save()), the page so named (Z18, B4)", () => {
    const started = JSON.parse(pageNew('Q3 review')).page as string;
    const added = (JSON.parse(pageApply(started, '["sheet-1"]',
      `[{"kind":"addGrid","document":${cubeToJson(CUBE)},"sheet":"sheet-1","near":null,"title":null,"columns":null}]`)) as { page: string }).page;
    const page = readPage(added);
    const record = savedRecord('  Q4 review ', page, 'r-1') as SavedRecord;
    assert.deepEqual(record, { id: 'r-1', name: 'Q4 review', content: pageContent({ ...page, name: 'Q4 review', record: 'r-1' }) });
    assert.deepEqual(savedRecord(' ', page, 'r-1'), { refused: "a page's name is not blank" });
    // headless: the record's text, its numbers exact, its id and the name it is saved under; nothing on the page, refused
    const said = JSON.parse(pageRecordJson('r-1', 'Q4 review', added, false)) as { record: string; id: string; name: string };
    assert.deepEqual([said.id, said.name], ['r-1', 'Q4 review']);
    assert.equal(said.record, toJson(record));
    assert.match(said.record, /12\.30/);
    assert.deepEqual(JSON.parse(pageRecordJson('r-1', 'Q4', started, false)), { problems: [NOTHING_TO_SAVE] });
  });

  it("the saved page it is, kept in the page: saved again, by either editor, over that record; never in a page file (B13)", () => {
    const started = JSON.parse(pageNew('Q3 review')).page as string;
    const added = (JSON.parse(pageApply(started, '["sheet-1"]',
      `[{"kind":"addGrid","document":${cubeToJson(CUBE)},"sheet":"sheet-1","near":null,"title":null,"columns":null}]`)) as { page: string }).page;
    // saved by one editor: the page says its record (the savedAs command), read and written by the page's own reader
    const saved = JSON.parse(pageApply(added, '["sheet-1","grid-1"]', '[{"kind":"savedAs","record":"r-1"}]')) as { page: string };
    const page = readPage(saved.page);
    assert.equal(page.record, 'r-1');
    assert.equal(readPage(pageToJson(page)).record, 'r-1');
    // saved again, by the other: over r-1, whatever new id it was handed
    assert.equal((savedRecord('Q3', page, 'r-other') as SavedRecord).id, 'r-1');
    assert.equal(JSON.parse(pageRecordJson('r-other', 'Q3', saved.page, false)).id, 'r-1');
    // Save As, by either editor (pageSaving; the tab's Save As, Python's save_as(): K13): a new record under the id it was
    // handed, the page it is saved from left saved as r-1 until it is told otherwise; Save, over r-1
    const copy = pageSaving('Q3 copy', page, 'r-new', true) as { record: SavedRecord; over: boolean };
    assert.deepEqual([copy.record.id, copy.record.name, copy.over], ['r-new', 'Q3 copy', false]);
    assert.equal(readPage(toJson(copy.record.content)).record, 'r-new', 'the copy is the saved page it is');
    assert.deepEqual(JSON.parse(pageRecordJson('r-new', 'Q3 copy', saved.page, true)).id, 'r-new');
    assert.equal(JSON.parse(pageRecordJson('r-new', 'Q3 copy', saved.page, true)).over, false);
    assert.equal(JSON.parse(pageRecordJson('r-new', 'Q3', saved.page, false)).over, true);
    assert.deepEqual(pageSaving(' ', page, 'r-new', true), { refused: "a page's name is not blank" });
    assert.throws(() => pageRecordJson('r-new', 'Q3', saved.page, 'yes' as unknown as boolean), /asNew is true or false/);
    // a page file carries none: opened elsewhere, its first save is a new record
    assert.equal(readPage((pageFile(page) as { file: string }).file).record, undefined);
    // its record deleted: the page no saved page any more
    const gone = JSON.parse(pageApply(saved.page, '["sheet-1","grid-1"]', '[{"kind":"savedAs","record":null}]')) as { page: string };
    assert.equal(readPage(gone.page).record, undefined);
    assert.throws(() => readPage({ ...JSON.parse(saved.page), record: '' }), /'record' is not a saved page's id/);
  });

  it("as a page file (Export ▸ Page File; headless pageFile: Python's save(path)): DataCube's text, or refused in one wording (B5)", () => {
    const started = JSON.parse(pageNew('Q3 review')).page as string;
    assert.deepEqual(JSON.parse(pageFileJson(started)), { problems: [NOTHING_TO_SAVE] });
    const added = (JSON.parse(pageApply(started, '["sheet-1"]',
      `[{"kind":"addGrid","document":${cubeToJson(CUBE)},"sheet":"sheet-1","near":null,"title":null,"columns":null}]`)) as { page: string }).page;
    const page = readPage(added);
    assert.deepEqual(pageFile(page), { file: pageToJson(page), name: 'Q3 review' });
    assert.deepEqual(JSON.parse(pageFileJson(added)), { file: pageToJson(page) });
    // an unnamed page: named as Save names it (its first sheet's), the file and its name alike (the S7 audit's Y-4: the
    // tab named it in its own code, Python's file said '')
    const unnamed = readPage((JSON.parse(pageApply(JSON.parse(pageNew(null)).page as string, '["sheet-1"]',
      `[{"kind":"addGrid","document":${cubeToJson(CUBE)},"sheet":"sheet-1","near":null,"title":null,"columns":null}]`)) as { page: string }).page);
    assert.equal(unnamed.name, '');
    const filed = pageFile(unnamed) as { file: string; name: string };
    assert.equal(filed.name, suggestedName(unnamed));
    assert.notEqual(filed.name, '');
    assert.equal(readPage(filed.file).name, filed.name);
    assert.equal(readPage(JSON.parse(pageFileJson(pageToJson(unnamed))).file as string).name, filed.name, "Python's the same");
    // a newer writer's field kept, as Save keeps it
    const newer = readPage({ ...JSON.parse(added), variables: [{ name: 'asOf' }] });
    assert.match((pageFile(newer) as { file: string }).file, /"variables":\[\{"name":"asOf"\}\]/);
    // classes kept in a project: a page file cannot carry them yet
    const modelled = readPage({ ...JSON.parse(added), model: { project: 'demo:trades', workspace: 'datacube', package: 'demo', tables: {} } });
    assert.deepEqual(pageFile(modelled), { refused: "This page's classes are in demo:trades: a page file cannot carry them yet -- "
      + 'save the page where its classes are kept.' });
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
