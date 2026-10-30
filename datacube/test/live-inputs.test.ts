// A grid over another grid's rows, and its live input (docs/DATACUBE_FLOW_MODE_2026_10_24.md §3.1, FL6): written and read
// whole, refused by name when malformed; what it reads asked only once its links are resolved against its page; a link
// naming no grid of the page, or going round, refused when the page is read; a page's grids opened after the grids they
// read; and the Join window's copy of such a grid holding no link.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
  CubeDocumentError, cubeToJson, leavesOf, rawRows, readCube, writeCube, type CubeDocument, type CubeSource, type FileSource,
  type RowsSource,
} from '../src/cube-document.ts';
import { DEFAULT_CONFIGURATION } from '../src/config.ts';
import { add, NEW_SHEET, type Bands } from '../src/layout/bands.ts';
import { afterWhatTheyRead, gridInput, liveGrids, liveProblem, resolvedSource } from '../src/live-inputs.ts';
import { openedLeaves } from '../src/join-open.ts';
import { joinInputOf } from '../src/page-commands.ts';
import {
  openingOrder, pageToJson, readPage, writePageOf, type PageDocument, type PageView,
} from '../src/page-document.ts';
import { pageReads } from '../src/page-rebase.ts';
import { CubeRefusal, ungroupedDimensions } from '../src/snapshot.ts';
import { TreeState } from '../src/tree.ts';
import { accessor, col, gt, lambda, lit } from '../../pure-protocol/src/index.ts';

const COLUMNS = [{ name: 'region', type: 'String' }, { name: 'notional', type: 'Float' }];

const FILE: FileSource = {
  _type: 'file', name: 'trades.csv', format: 'csv', size: 120, sha256: 'ab'.repeat(32), columns: COLUMNS,
};

/** A cube over `source`; `filtered`, its rows those over 100. */
function cube(source: CubeSource, filtered = false): CubeDocument {
  const snapshot = rawRows(accessor('local::DB', 'trades'), COLUMNS);
  return writeCube({
    name: 'flow', source,
    snapshot: filtered ? { ...snapshot, filter: { kind: 'condition', column: 'notional', operator: 'greaterThan', value: 100 } } : snapshot,
    configuration: DEFAULT_CONFIGURATION, tree: TreeState.fromPaths([], false),
  });
}

const rows = (grid: string, name = 'trades.csv'): RowsSource => ({ _type: 'rows', name, input: { grid }, columns: COLUMNS });

/** A page of grids, each a cube of its own under its id, stacked on one sheet; `titles`, the tiles a person named. */
function page(cubes: Readonly<Record<string, CubeDocument>>, titles: Readonly<Record<string, string>> = {}): PageDocument {
  const ids = Object.keys(cubes);
  const layout = ids.reduce<Bands>((l, id) => add(l, id), NEW_SHEET);
  const views: PageView[] = ids.map((id) => ({ id, kind: 'grid', cube: id, ...(titles[id] ? { title: titles[id] } : {}) }));
  return writePageOf({
    name: 'flow', cubes: ids.map((id) => ({ id, cube: cubes[id]! })),
    views: { views, sheets: [{ id: 'sheet-1', layout: { kind: 'bands', ...layout } }] },
  });
}

/** Input, then a Filter over it, then a third box over the Filter: a chain of three. */
const CHAIN = (): PageDocument => page({
  'grid-1': cube(FILE),
  'grid-2': cube(rows('grid-1'), true),
  'grid-3': cube(rows('grid-2', 'Big trades')),
}, { 'grid-2': 'Big trades' });

describe('a grid over another grid\'s rows, as its cube writes it', () => {
  it('reads back exactly, its input live', () => {
    const doc = cube(rows('grid-1'));
    const back = readCube(cubeToJson(doc));
    assert.deepEqual(back.source, rows('grid-1'));
    assert.equal(cubeToJson(back), cubeToJson(doc));
  });

  it('reads back exactly, its input frozen: what a grid was, its query exact', () => {
    const frozen: RowsSource = {
      _type: 'rows', name: 'trades.csv', columns: COLUMNS,
      input: {
        name: 'trades.csv', source: FILE,
        query: { ...cube(FILE).query, derived: [{ name: 'scaled', type: 'Decimal', lambda: lambda(['x'], gt(col('x', 'notional'), lit.decimal('12.30'))) }] },
      },
    };
    const text = cubeToJson(cube(frozen));
    assert.match(text, /12\.30/, 'the decimal keeps its digits');
    assert.equal(cubeToJson(readCube(text)), text);
  });

  it('is refused by name when malformed', () => {
    const raw = (source: unknown): string => JSON.stringify({ ...JSON.parse(cubeToJson(cube(FILE))), source });
    const read = (source: unknown): unknown => readCube(raw(source));
    assert.throws(() => read({ _type: 'rows', name: '', input: { grid: 'grid-1' }, columns: COLUMNS }),
      (e: unknown) => e instanceof CubeDocumentError && /another grid's rows has no name/.test(e.message));
    assert.throws(() => read({ _type: 'rows', name: 'a', input: { grid: 'grid-1' } }), /another grid's rows has no columns/);
    assert.throws(() => read({ _type: 'rows', name: 'a', columns: COLUMNS }), /another grid's rows has no input/);
    // a port is a Filter's False alone (FL7, docs/DATACUBE_FLOW_MODE_2026_10_24.md M2: read since 2026-10-25)
    assert.throws(() => read({ _type: 'rows', name: 'a', input: { grid: 'grid-1', port: 'true' }, columns: COLUMNS }),
      /a live input's port is a Filter's 'false' or a Join's 'left' or 'right', not "true"/);
    assert.throws(() => read({ _type: 'rows', name: 'a', input: { grid: 'grid-1', at: 1 }, columns: COLUMNS }),
      /a live input is its grid alone, and its port, not at/);
    assert.throws(() => read({ _type: 'rows', name: 'a', input: { grid: 'chart-1' }, columns: COLUMNS }),
      /a live input names a grid of its page \('grid-3'\), not "chart-1"/);
    assert.throws(() => read({ _type: 'rows', name: 'a', input: { name: '', source: FILE }, columns: COLUMNS }),
      /input has no name/);
  });

  it("reads a Union's inputs after its right (JoinSource.more) back exactly; refuses them on a join of two, or not a list", () => {
    const union: CubeSource = {
      _type: 'join', name: 'a ∪ b ∪ c', how: { form: 'append' }, columns: COLUMNS,
      left: { grid: 'grid-1' }, right: { grid: 'grid-2' }, more: [{ grid: 'grid-3' }, { name: 'd', source: FILE, query: cube(FILE).query }],
    };
    const doc = cube(union);
    assert.deepEqual(readCube(cubeToJson(doc)).source, union);
    assert.equal(cubeToJson(readCube(cubeToJson(doc))), cubeToJson(doc));
    assert.deepEqual(liveGrids(union), ['grid-1', 'grid-2', 'grid-3']);
    const raw = (source: unknown): string => JSON.stringify({ ...JSON.parse(cubeToJson(cube(FILE))), source });
    const plain = JSON.parse(cubeToJson(doc)).source;
    assert.throws(() => readCube(raw({ ...plain, how: { form: 'semi', on: { rows: [] } } })),
      (e: unknown) => e instanceof CubeDocumentError && /a join of two: only a Union takes more inputs/.test(e.message));
    assert.throws(() => readCube(raw({ ...plain, more: [] })), /inputs after its right are a list of inputs, not \[\]/);
    assert.throws(() => readCube(raw({ ...plain, more: { grid: 'grid-3' } })), /inputs after its right are a list of inputs/);
    assert.throws(() => readCube(raw({ ...plain, more: [{ grid: 'chart-1' }] })), /a live input names a grid of its page \('grid-3'\), not "chart-1"/);
  });

  it('says what it reads only once resolved: a live input is its page\'s to answer', () => {
    assert.throws(() => leavesOf(rows('grid-1')), /reads grid 'grid-1' of its page live: resolve it against the page first/);
    const p = CHAIN();
    assert.deepEqual(leavesOf(resolvedSource(p, p.cubes[2]!.cube.source, 'grid-3')), [FILE]);
  });
});

describe('a live input resolved against its page', () => {
  it('is the copy of its grid as it is now, through every grid it reads, its name the one the tile shows', () => {
    const p = CHAIN();
    const resolved = resolvedSource(p, p.cubes[2]!.cube.source, 'grid-3');
    assert.equal(resolved._type, 'rows');
    if (resolved._type !== 'rows' || 'grid' in resolved.input) throw new Error('not resolved');
    // grid-2 as it is now: its title, its filter, and its own source resolved in turn
    assert.equal(resolved.input.name, 'Big trades');
    assert.deepEqual(resolved.input.query.filter, { kind: 'condition', column: 'notional', operator: 'greaterThan', value: 100 });
    const inner = resolved.input.source;
    if (inner._type !== 'rows' || 'grid' in inner.input) throw new Error('grid-2 not resolved');
    assert.equal(inner.input.name, 'trades.csv', 'an untitled tile is called by its source');
    assert.deepEqual(inner.input.source, FILE);
    assert.deepEqual(liveGrids(resolved), [], 'no link is left');
  });

  it("to be opened, a snapped grid's input reads its copy (a cached box, FL10) -- through a chain too; asked otherwise, what it is a copy of", () => {
    const p = CHAIN();
    const snapped: PageDocument = { ...p, copies: { 'grid-2': { table: 'snap_7', label: 'Snap 14:02', takenAt: '2026-10-26T14:02:00.000Z', rowCount: 3 } } };
    // grid-3 over grid-2, which is snapped: its input reads the copy, its query over it
    const opened = resolvedSource(snapped, snapped.cubes[2]!.cube.source, 'grid-3', { copies: true });
    if (opened._type !== 'rows' || 'grid' in opened.input) throw new Error('not resolved');
    assert.deepEqual(opened.input.copy, { table: 'snap_7', label: 'Snap 14:02', takenAt: '2026-10-26T14:02:00.000Z' });
    assert.deepEqual(opened.input.query.filter, { kind: 'condition', column: 'notional', operator: 'greaterThan', value: 100 }, 'its query over its copy');
    assert.deepEqual(openedLeaves(opened), [{ _type: 'copy', table: 'snap_7', label: 'Snap 14:02', takenAt: '2026-10-26T14:02:00.000Z' }], 'one leaf: the copy');
    // not asked: what it is a copy of (a class maps those tables: §18.8), its leaves the file's
    const of = resolvedSource(snapped, snapped.cubes[2]!.cube.source, 'grid-3');
    if (of._type !== 'rows' || 'grid' in of.input) throw new Error('not resolved');
    assert.equal(of.input.copy, undefined);
    assert.deepEqual(leavesOf(of), [FILE]);
    // grid-1 snapped, grid-3 two boxes on: the chain reads grid-1's copy at its end
    const first: PageDocument = { ...p, copies: { 'grid-1': { table: 'snap_1', label: 'Snap', takenAt: '2026-10-26T14:00:00.000Z', rowCount: 9 } } };
    assert.deepEqual(openedLeaves(resolvedSource(first, first.cubes[2]!.cube.source, 'grid-3', { copies: true })).map((l) => l._type === 'copy' ? l.table : l._type), ['snap_1']);
    // a page keeps no copy in an input: one written is refused as it is read
    const kept = { name: 'x', source: FILE, query: cube(FILE).query, copy: { table: 'snap_7', label: 'Snap', takenAt: '2026-10-26T14:02:00.000Z' } };
    assert.throws(() => readCube(cubeToJson(cube({ _type: 'rows', name: 'x', input: kept, columns: COLUMNS }))), /keeps a copy: a copy lasts its session/);
  });

  it("a flow's box gives the columns it shows: the ones it hides left out (omit), kept in a copy; a board's grid all", () => {
    const hiding = (c: CubeDocument): CubeDocument => ({ ...c, configuration: { ...c.configuration, columns: { notional: { hidden: true } } } });
    const board = page({ 'grid-1': hiding(cube(FILE)), 'grid-2': cube(rows('grid-1')) });
    assert.equal(gridInput(board, 'grid-1').omit, undefined, "a board's grid: every column, as a Join of two grids has");
    const flow: PageDocument = { ...board, sheets: [{ id: 'sheet-1', layout: { kind: 'flow', boxes: [
      { tile: 'grid-1', x: 40, y: 40 }, { tile: 'grid-2', x: 288, y: 40 },
    ] } }] };
    assert.deepEqual(gridInput(flow, 'grid-1').omit, ['notional']);
    // written and read with its copy (a frozen input: FL6)
    const frozen = cube({ _type: 'rows', name: 'trades.csv', input: gridInput(flow, 'grid-1'), columns: COLUMNS });
    const again = readCube(JSON.parse(cubeToJson(frozen)));
    assert.deepEqual(again.source._type === 'rows' && !('grid' in again.source.input) ? again.source.input.omit : null, ['notional']);
    assert.throws(() => readCube(JSON.parse(cubeToJson(frozen).replace('"omit":["notional"]', '"omit":"notional"'))),
      (e: unknown) => e instanceof CubeDocumentError && /columns left out are a list of names/.test(e.message));
  });

  it("a flow's box hands on its columns by the names it gives them: renamed (rename), written and read with a copy", () => {
    const renaming = (c: CubeDocument): CubeDocument => ({ ...c, configuration: { ...c.configuration, columns: { region: { displayName: 'area' } } } });
    const board = page({ 'grid-1': renaming(cube(FILE)), 'grid-2': cube(rows('grid-1')) });
    assert.equal(gridInput(board, 'grid-1').rename, undefined, "a board's grid: its columns by their names");
    const flow: PageDocument = { ...board, sheets: [{ id: 'sheet-1', layout: { kind: 'flow', boxes: [
      { tile: 'grid-1', x: 40, y: 40 }, { tile: 'grid-2', x: 288, y: 40 },
    ] } }] };
    assert.deepEqual(gridInput(flow, 'grid-1').rename, [{ from: 'region', to: 'area' }]);
    const frozen = cube({ _type: 'rows', name: 'trades.csv', input: gridInput(flow, 'grid-1'), columns: COLUMNS });
    const again = readCube(JSON.parse(cubeToJson(frozen)));
    assert.deepEqual(again.source._type === 'rows' && !('grid' in again.source.input) ? again.source.input.rename : null, [{ from: 'region', to: 'area' }]);
    assert.throws(() => readCube(JSON.parse(cubeToJson(frozen).replace('"rename":[{"from":"region","to":"area"}]', '"rename":["region"]'))),
      (e: unknown) => e instanceof CubeDocumentError && /columns renamed are a list of \{from, to\}/.test(e.message));
  });

  it("a flow's box hands on its columns in its order (a Select's column order), written and read with a copy", () => {
    const ordering = (c: CubeDocument): CubeDocument => ({ ...c, configuration: { ...c.configuration, columnOrder: ['notional', 'region'] } });
    const board = page({ 'grid-1': ordering(cube(FILE)), 'grid-2': cube(rows('grid-1')) });
    assert.equal(gridInput(board, 'grid-1').order, undefined, "a board's grid: its columns as its query has them");
    const flow: PageDocument = { ...board, sheets: [{ id: 'sheet-1', layout: { kind: 'flow', boxes: [
      { tile: 'grid-1', x: 40, y: 40 }, { tile: 'grid-2', x: 288, y: 40 },
    ] } }] };
    assert.deepEqual(gridInput(flow, 'grid-1').order, ['notional', 'region']);
    const frozen = cube({ _type: 'rows', name: 'trades.csv', input: gridInput(flow, 'grid-1'), columns: COLUMNS });
    const again = readCube(JSON.parse(cubeToJson(frozen)));
    assert.deepEqual(again.source._type === 'rows' && !('grid' in again.source.input) ? again.source.input.order : null, ['notional', 'region']);
    assert.throws(() => readCube(JSON.parse(cubeToJson(frozen).replace('"order":["notional","region"]', '"order":"notional"'))),
      (e: unknown) => e instanceof CubeDocumentError && /columns' order is a list of names/.test(e.message));
    // an order its query has already: none written
    const same = page({ 'grid-1': { ...cube(FILE), configuration: { ...cube(FILE).configuration, columnOrder: ['region', 'notional'] } }, 'grid-2': cube(rows('grid-1')) });
    assert.equal(gridInput({ ...same, sheets: flow.sheets }, 'grid-1').order, undefined);
  });

  it("a flow's Summarize gives its keys and its aggregates: the dimensions it is not grouped by left out", () => {
    // region and desk dimensions, notional a measure; grouped by region: desk has no one value a group
    const wide = [{ name: 'region', type: 'String' }, { name: 'desk', type: 'String' }, { name: 'notional', type: 'Float', kind: 'measure' as const }];
    const grouped = (c: CubeDocument): CubeDocument => ({ ...c, query: { ...c.query, columns: wide, rows: ['region'] } });
    const flow: PageDocument = { ...page({ 'grid-1': grouped(cube(FILE)), 'grid-2': cube(rows('grid-1')) }),
      sheets: [{ id: 'sheet-1', layout: { kind: 'flow', boxes: [{ tile: 'grid-1', x: 40, y: 40 }, { tile: 'grid-2', x: 288, y: 40 }] } }] };
    assert.deepEqual(gridInput(flow, 'grid-1').omit, ['desk']);
    assert.deepEqual(ungroupedDimensions({ columns: wide, derived: [], rows: ['region'], pivotOn: ['desk'] }), [], 'a pivot\'s columns are its values');
    assert.deepEqual(ungroupedDimensions({ columns: wide, derived: [], rows: [], pivotOn: [] }), [], 'grouped by nothing: its rows');
  });

  it('follows its grid: an edit there is what it reads', () => {
    const p = CHAIN();
    const edited: PageDocument = {
      ...p, cubes: p.cubes.map((c) => (c.id === 'grid-2' ? { ...c, cube: cube(rows('grid-1')) } : c)),
    };
    const resolved = resolvedSource(edited, edited.cubes[2]!.cube.source, 'grid-3');
    assert.ok(resolved._type === 'rows' && !('grid' in resolved.input));
    assert.equal(resolved.input.query.filter, undefined, "grid-2's filter taken off reaches grid-3");
  });

  it('names the grids a source reads live, each once, through its frozen inputs too', () => {
    assert.deepEqual(liveGrids(FILE), []);
    assert.deepEqual(liveGrids(rows('grid-2')), ['grid-2']);
    const join: CubeSource = {
      _type: 'join', name: 'a ⋈ b', how: { form: 'append' }, columns: COLUMNS,
      left: { name: 'a', source: rows('grid-1'), query: cube(FILE).query },
      right: { name: 'b', source: rows('grid-1'), query: cube(FILE).query },
    };
    assert.deepEqual(liveGrids(join), ['grid-1']);
    assert.deepEqual(liveGrids({ ...join, right: { ...join.right, source: rows('grid-4') } }), ['grid-1', 'grid-4']);
  });

  it('is refused in words when its grid is not on the page, is no grid, or reads its own rows', () => {
    const p = CHAIN();
    const refused = (source: CubeSource, grid: string, words: RegExp): void =>
      assert.throws(() => resolvedSource(p, source, grid), (e: unknown) => e instanceof CubeRefusal && words.test(e.message));
    refused(rows('grid-9'), 'grid-3', /'grid-9' is no grid of this page/);
    refused(rows('grid-3'), 'grid-3', /'grid-3' reads its own rows \(grid-3 → grid-3\)/);
    const round = page({ 'grid-1': cube(rows('grid-2')), 'grid-2': cube(rows('grid-1')) });
    assert.throws(() => resolvedSource(round, round.cubes[0]!.cube.source, 'grid-1'),
      /'grid-1' reads its own rows \(grid-1 → grid-2 → grid-1\)/);
  });
});

describe('a page of live inputs', () => {
  it('reads back exactly', () => {
    const p = CHAIN();
    assert.equal(pageToJson(readPage(pageToJson(p))), pageToJson(p));
    assert.equal(liveProblem(p), undefined);
  });

  it('is refused when a link names no grid of it, or goes round: no editor writes such a page', () => {
    const missing = page({ 'grid-1': cube(FILE), 'grid-2': cube(rows('grid-7')) });
    assert.throws(() => readPage(pageToJson(missing)), /grid-2: 'grid-7' is no grid of this page/);
    const round = page({ 'grid-1': cube(rows('grid-2')), 'grid-2': cube(rows('grid-1')) });
    assert.throws(() => readPage(pageToJson(round)), /grid-1: 'grid-1' reads its own rows \(grid-1 → grid-2 → grid-1\)/);
  });

  it('opens each grid after the grids it reads, wherever its tile sits', () => {
    const p = CHAIN();
    const backwards = [...p.cubes].reverse();
    assert.deepEqual(openingOrder(backwards, p.views).map((c) => c.id), ['grid-1', 'grid-2', 'grid-3']);
  });

  it('keeps every other order: a cube in a cycle, or reading a grid not among them, keeps its place', () => {
    const items = [
      { id: 'a', cube: { source: rows('grid-b') } },
      { id: 'b', cube: { source: rows('grid-a') } },
      { id: 'c', cube: { source: rows('grid-z') } },
      { id: 'd', cube: { source: FILE as CubeSource } },
    ];
    const gridOf = (c: { readonly id: string }): string => `grid-${c.id}`;
    assert.deepEqual(afterWhatTheyRead(items, gridOf).map((c) => c.id), ['b', 'a', 'c', 'd']);
  });

  it("is read, by its keeper, as what its grids read: each file once", () => {
    assert.deepEqual(pageReads(CHAIN()).files, ['ab'.repeat(32)]);
  });

  it("gives the Join window a copy of a grid over another's rows that holds no link (FL6: a frozen input never changes)", () => {
    const p = CHAIN();
    const input = joinInputOf(p, 'grid-3');
    assert.deepEqual(liveGrids(input.source), []);
    assert.deepEqual(leavesOf(input.source), [FILE]);
    assert.deepEqual(gridInput(p, 'grid-3'), input, 'one rule: a grid as an input');
  });
});
