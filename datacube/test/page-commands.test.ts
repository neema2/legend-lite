// A PAGE EDIT AS A VALUE (src/page-commands.ts; docs/DATACUBE_COMMANDS_PAGE_2026_10_18.md): each command gives the
// page the screen's rules give it -- ids after every one held, a new chart titled by its number, a removed grid taking
// its following charts and leaving a frozen one detached, a chart's selection taken off its grid's filter, a move onto
// a tile's own sheet doing nothing -- and a command read from JSON is the same command, or refused in words.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { followCube } from '../src/chart-spec.ts';
import { cubeOpenedAgain, type CubeDocument, type CubeSource, type FrameSource } from '../src/cube-document.ts';
import { tiles } from '../src/layout/bands.ts';
import {
  PAGE_COMMAND_KINDS, applyPageCommand, applyPageCommands, chartTitle, gridTitle, idsOf, joinInputOf, nextPageId,
  readPageCommand, followedPage, opensAgain, rebuiltCommands,
  type PageCommand, type PageDraft,
} from '../src/page-commands.ts';
import { gridCopyOf, modelAcross, openedLive, pageToJson, readPage, unnamedLabel, type ChartView, type PageDocument } from '../src/page-document.ts';
import { cubeOpensAgain } from '../src/headless/page.ts';
import { CubeRefusal, withoutConditions, type FilterNode } from '../src/snapshot.ts';
import { PageModel } from '../src/page-model.ts';
import { rebaseEdits, withCube } from '../src/page-rebase.ts';
import { corpus } from './fixtures/pages/make.ts';

// the saved-page corpus's frame page: grids grid and grid-2, chart (following grid) stacked with chart-2 (frozen, of
// grid-2) on sheet-1 beside grid; grid-2 alone on sheet-2
const trades = (await corpus())['trades.page.json']!;
const draft = (page: PageDocument = trades): PageDraft => ({ page, held: idsOf(page) });
const apply = (command: PageCommand, d: PageDraft = draft()): PageDraft => applyPageCommand(d, command).draft;
const refused = (f: () => unknown, words: RegExp): void => {
  assert.throws(f, (e: unknown) => e instanceof CubeRefusal && words.test(e.message), String(words));
};
const sheet = (page: PageDocument, id: string) => page.sheets.find((s) => s.id === id)!;
const view = (page: PageDocument, id: string) => page.views.find((v) => v.id === id);
// a copy as a host's Snap writes it down (page-document.ts gridCopyOf)
const SNAP = gridCopyOf({ table: 'snap', rowCount: 3, takenAt: new Date('2026-10-20T14:02:00Z'), label: 'Snap 14:02' });

describe('ids and titles: one allocator, after every id the page has held', () => {
  it('a new id follows the largest of its kind held, and a removed one is not given out again', () => {
    assert.equal(nextPageId(idsOf(trades), 'chart'), 'chart-3');
    assert.equal(nextPageId(idsOf(trades), 'grid'), 'grid-3');
    assert.equal(nextPageId(idsOf(trades), 'sheet'), 'sheet-3');
    const gone = apply({ kind: 'removeTile', tile: 'chart-2' });
    assert.equal(nextPageId(gone.held, 'chart'), 'chart-3', 'chart-2 was held: never given out again');
    assert.equal(chartTitle('chart-12'), 'Chart 12');
  });

  it("a cube kept for a detached chart has no view, and its id is taken all the same (the phase 1 audit's 2)", () => {
    // grid-2 removed: chart-2, frozen, keeps its cube with no grid view
    const detached = apply({ kind: 'removeTile', tile: 'grid-2' }).page;
    assert.ok(idsOf(detached).includes('grid-2'), "the kept cube's id");
    const added = (d: PageDraft, id?: string): string => (applyPageCommand(d, {
      kind: 'addGrid', ...(id !== undefined ? { id } : {}), document: trades.cubes[0]!.cube, sheet: 'sheet-1', near: null, title: null, columns: null,
    }).command as { id: string }).id;
    assert.equal(added(draft(detached)), 'grid-3');
    // a host whose held left it out gets no second grid-2 either, nor may it name one
    const forgetful: PageDraft = { page: detached, held: detached.views.map((v) => v.id) };
    assert.equal(added(forgetful), 'grid-3');
    refused(() => added(forgetful, 'grid-2'), /'grid-2' is an id the page has held already/);
  });

  // 2026-10-24, found by the undo's random walks: a grid made 'Grid 3' was a page no rename put back
  it('a grid made named what it is numbered keeps no title of its own, as a rename to it leaves none', () => {
    const made = apply({ kind: 'addGrid', id: 'grid-3', document: trades.cubes[0]!.cube, sheet: 'sheet-1', near: null, title: 'Grid 3', columns: null }).page;
    assert.deepEqual(view(made, 'grid-3'), { id: 'grid-3', kind: 'grid', cube: 'grid-3' });
    const named = apply({ kind: 'addGrid', id: 'grid-3', document: trades.cubes[0]!.cube, sheet: 'sheet-1', near: null, title: 'Orders', columns: null }).page;
    assert.equal((view(named, 'grid-3') as { title?: string }).title, 'Orders');
  });

  it('a command that makes a tile carries the id it was given, as applied (what a live channel replays)', () => {
    const { draft: d, applied } = applyPageCommands(draft(), [
      { kind: 'addSheet', name: 'More' },
      { kind: 'addChart', near: 'grid-2', spec: (view(trades, 'chart') as ChartView).spec, title: null, columns: null },
    ]);
    assert.deepEqual(applied.map((c) => ('id' in c ? c.id : undefined)), ['sheet-3', 'chart-3']);
    // replayed, they give the same page; an id the page has held is refused
    assert.deepEqual(applyPageCommands(draft(), applied).draft, d);
    refused(() => apply({ kind: 'addSheet', id: 'sheet-2', name: null }), /'sheet-2' is an id the page has held already/);
  });
});

describe('each command gives the page the screen\'s rules give it', () => {
  it('a new chart goes beside its grid, titled by its number, of its grid\'s cube, following it from the start', () => {
    const spec = (view(trades, 'chart') as ChartView).spec;
    const page = apply({ kind: 'addChart', near: 'grid-2', spec, title: null, columns: null }).page;
    // a spec made for another grid (chart's, of grid) is regrouped as grid-2 is, as the next change of grid-2 would
    // regroup it (D19: a live chart is always where following leaves it)
    const followed = followCube(spec, { ...trades.cubes.find((c) => c.id === 'grid-2')!.cube.query, epoch: 0 });
    assert.notDeepEqual(followed, spec, "grid-2 aggregates qty otherwise: the chart's value follows it");
    assert.deepEqual(view(page, 'chart-3'), { id: 'chart-3', kind: 'chart', cube: 'grid-2', title: 'Chart 3', spec: followed });
    assert.deepEqual(tiles(sheet(page, 'sheet-2').layout), ['grid-2', 'chart-3']);
    refused(() => apply({ kind: 'addChart', near: 'chart', spec, title: null, columns: null }), /a chart is of a grid/);
  });

  it('a removed grid takes its following charts; a frozen one stays, detached, its cube kept for it', () => {
    const page = apply({ kind: 'removeTile', tile: 'grid' }).page;
    assert.deepEqual(page.views.map((v) => v.id), ['grid-2', 'chart-2'], 'chart followed grid: gone with it');
    assert.deepEqual(page.cubes.map((c) => c.id), ['grid-2']);
    assert.deepEqual(tiles(sheet(page, 'sheet-1').layout), ['chart-2']);
    const detached = apply({ kind: 'removeTile', tile: 'grid-2' }).page;
    assert.deepEqual(detached.views.map((v) => v.id), ['grid', 'chart', 'chart-2'], 'chart-2 frozen: it stays');
    assert.deepEqual(detached.cubes.map((c) => c.id), ['grid', 'grid-2'], 'its cube kept for the detached chart');
  });

  it('a chart removed takes its selection off its grid\'s filter (the screen\'s cleanup Python missed)', () => {
    const pick: FilterNode = { kind: 'condition', column: 'region', operator: 'equal', value: 'EMEA' };
    const cube = trades.cubes.find((c) => c.id === 'grid')!;
    const had = cube.cube.query.filter!;
    const selected: PageDocument = {
      ...trades,
      cubes: trades.cubes.map((c) => (c.id === 'grid' ? { ...c, cube: { ...c.cube, query: { ...c.cube.query, filter: { kind: 'and', children: [had, pick] } } } } : c)),
      views: trades.views.map((v) => (v.id === 'chart' ? { ...v, selection: [pick] } : v)),
    };
    const page = apply({ kind: 'removeTile', tile: 'chart' }, draft(selected)).page;
    assert.deepEqual(page.cubes.find((c) => c.id === 'grid')?.cube.query.filter, had);
  });

  it('a tile moved onto another sheet goes to its bottom; onto its own, nothing', () => {
    assert.deepEqual(apply({ kind: 'moveTile', tile: 'grid', sheet: 'sheet-1', columns: null }).page, trades);
    const page = apply({ kind: 'moveTile', tile: 'chart', sheet: 'sheet-2', columns: null }).page;
    assert.deepEqual(tiles(sheet(page, 'sheet-2').layout), ['grid-2', 'chart']);
    assert.deepEqual(tiles(sheet(page, 'sheet-1').layout), ['grid', 'chart-2']);
  });

  it('sheets: added, named, moved, removed with their tiles; the last one stays', () => {
    let d = apply({ kind: 'addSheet', name: '  Extra  ' });
    assert.deepEqual(sheet(d.page, 'sheet-3'), { id: 'sheet-3', name: 'Extra', layout: { kind: 'bands', fit: true, bands: [] } });
    d = apply({ kind: 'renameSheet', sheet: 'sheet-1', name: '' }, d);
    assert.equal('name' in sheet(d.page, 'sheet-1'), false, "a blank: named after its first grid");
    // named what it shows without a name: none of its own, as a tile's made title is none (B2; the S7 audit's P-7 --
    // the screen's tab told nothing, Python's pinned it)
    const shows = unnamedLabel(d.page, sheet(d.page, 'sheet-2'));
    assert.equal('name' in sheet(apply({ kind: 'renameSheet', sheet: 'sheet-2', name: shows }, d).page, 'sheet-2'), false);
    assert.equal(sheet(apply({ kind: 'renameSheet', sheet: 'sheet-2', name: `${shows}!` }, d).page, 'sheet-2').name, `${shows}!`);
    d = apply({ kind: 'moveSheet', sheet: 'sheet-3', to: 0 }, d);
    assert.deepEqual(d.page.sheets.map((s) => s.id), ['sheet-3', 'sheet-1', 'sheet-2']);
    const gone = apply({ kind: 'removeSheet', sheet: 'sheet-1' }, d).page;
    assert.deepEqual(gone.sheets.map((s) => s.id), ['sheet-3', 'sheet-2']);
    // grid went with sheet-1, and its following chart; chart-2, frozen, of grid-2, went with sheet-1 too
    assert.deepEqual(gone.views.map((v) => v.id), ['grid-2']);
    let one = draft();
    one = apply({ kind: 'removeSheet', sheet: 'sheet-2' }, one);
    refused(() => apply({ kind: 'removeSheet', sheet: 'sheet-1' }, one), /a page has one sheet at least/);
  });

  it('titles by the one rule (givenName): a chart keeps one, a grid\'s comes off, a blank is refused', () => {
    assert.equal(view(apply({ kind: 'renameTile', tile: 'chart', title: ' Qty ' }).page, 'chart')?.title, 'Qty');
    assert.equal('title' in view(apply({ kind: 'renameTile', tile: 'grid', title: null }).page, 'grid')!, false);
    refused(() => apply({ kind: 'renameTile', tile: 'chart', title: null }), /a chart has a title/);
    refused(() => apply({ kind: 'renameTile', tile: 'grid', title: '  ' }), /a tile's name is not blank/);
    // a grid given back the title it was made with keeps none of its own (B2): the screen sends what was typed
    const titled = apply({ kind: 'renameTile', tile: 'grid', title: 'Mine' }).page;
    assert.equal(view(titled, 'grid')?.title, 'Mine');
    const back = apply({ kind: 'renameTile', tile: 'grid', title: ` ${gridTitle('grid')} ` }, { page: titled, held: idsOf(titled) });
    assert.equal('title' in view(back.page, 'grid')!, false);
    // a chart's selection: set, and cleared by none
    const emea = { kind: 'condition', column: 'region', operator: 'equal', value: 'EMEA' } as const;
    const selected = apply({ kind: 'select', chart: 'chart', conditions: [emea] }).page;
    assert.deepEqual((view(selected, 'chart') as ChartView).selection, [emea]);
    const none = apply({ kind: 'select', chart: 'chart', conditions: [] }, { page: selected, held: idsOf(selected) });
    assert.equal('selection' in view(none.page, 'chart')!, false);
    refused(() => apply({ kind: 'select', chart: 'grid', conditions: [] }), /'grid' is a grid, not a chart/);
    // a chart onto another grid's cube (D7: Show Grid's followGrid): a chart onto a grid, never the one it reads
    refused(() => apply({ kind: 'followGrid', chart: 'chart', grid: 'grid' }), /'chart' reads grid already/);
    refused(() => apply({ kind: 'followGrid', chart: 'grid', grid: 'grid-2' }), /'grid' is a grid, not a chart/);
    refused(() => apply({ kind: 'followGrid', chart: 'chart', grid: 'chart-2' }), /'chart-2' is a chart, not a grid/);
  });

  it("the page's name is each cube's too (writePageOf's rule), written by no rule of its own", () => {
    const page = apply({ kind: 'renamePage', name: 'Desk view' }).page;
    assert.equal(page.name, 'Desk view');
    assert.deepEqual(page.cubes.map((c) => c.cube.name), ['Desk view', 'Desk view']);
  });

  it('stack, arrange and a whole layout, by bands.ts, each refused in its words', () => {
    const stacked = apply({ kind: 'stack', tiles: ['grid', 'chart'] }).page;
    assert.deepEqual(tiles(sheet(stacked, 'sheet-1').layout), ['grid', 'chart', 'chart-2']);
    refused(() => apply({ kind: 'stack', tiles: ['grid', 'grid-2'] }), /on one sheet/);
    const side = apply({ kind: 'arrange', sheet: 'sheet-1', preset: 'stacked', order: null }).page;
    assert.equal(sheet(side, 'sheet-1').layout.bands.length, 2);
    refused(() => apply({ kind: 'layout', sheet: 'sheet-2', layout: sheet(trades, 'sheet-1').layout }), /holds its tiles, each once/);
    // (the phase 1 audit's 1) read as a saved page's sheet is: its shape refused in the page reader's words, never kept
    const odd = (layout: unknown): PageCommand => ({ kind: 'layout', sheet: 'sheet-2', layout: layout as PageCommand & never });
    refused(() => apply(odd({ kind: 'bands', fit: 'yes', bands: [{ height: 1, node: { tile: 'grid-2' } }] })), /the layout's fit is not true or false/);
    refused(() => apply(odd({ kind: 'bands', fit: true })), /the layout's bands are not a list/);
    refused(() => apply(odd({ kind: 'bands', fit: true, bands: [{ height: '1', node: { tile: 'grid-2' } }] })), /band 1 is not a height and its tiles/);
    refused(() => apply(odd({ kind: 'bands', fit: true, bands: [{ height: 1, node: { tiles: 'grid-2' } }] })), /band 1 is not a tile, a stack or a split/);
    refused(() => apply(odd({ kind: 'bands', fit: true, bands: [{ height: 1, node: { tile: 'nope' } }] })), /a tile shows no view of this page \(nope\)/);
    // a node's stray keys are not kept: the page's reader rebuilds it
    const kept = apply(odd({ kind: 'bands', fit: true, bands: [{ height: 1, node: { tile: 'grid-2', extra: 1 } }] })).page;
    assert.deepEqual(sheet(kept, 'sheet-2').layout, sheet(trades, 'sheet-2').layout);
    const same = apply({ kind: 'layout', sheet: 'sheet-1', layout: sheet(side, 'sheet-1').layout }, draft()).page;
    assert.deepEqual(sheet(same, 'sheet-1').layout, sheet(side, 'sheet-1').layout);
  });
});

describe('a text tile: added, its words set, titled, moved and removed by the page commands (page-text.ts)', () => {
  it("added at its sheet's bottom, numbered; its words set whole; saved and read back as they are", () => {
    const made = applyPageCommand(draft(), { kind: 'addText', sheet: 'sheet-2', text: '# Q3\n\nNotes', title: null, columns: null });
    assert.equal((made.command as { id: string }).id, 'text-1');
    const page = made.draft.page;
    assert.deepEqual(view(page, 'text-1'), { id: 'text-1', kind: 'text', text: '# Q3\n\nNotes' });
    assert.equal(tiles(sheet(page, 'sheet-2').layout).at(-1), 'text-1', 'a band of its own at the bottom');
    assert.equal(page.views.at(-1)?.id, 'text-1', "after the grids and charts, as the screen writes them");
    const set = apply({ kind: 'setText', tile: 'text-1', text: 'Up **3%**' }, made.draft).page;
    assert.equal((view(set, 'text-1') as { text: string }).text, 'Up **3%**');
    // saved and read back: a text tile has no cube
    assert.deepEqual(readPage(JSON.parse(JSON.stringify(pageToJson(set)))).views.find((v) => v.id === 'text-1'), view(set, 'text-1'));
    // titled, its made title none of its own; removed alone, the page's cubes as they were
    const titled = apply({ kind: 'renameTile', tile: 'text-1', title: 'Notes' }, made.draft).page;
    assert.equal((view(titled, 'text-1') as { title?: string }).title, 'Notes');
    assert.equal((view(apply({ kind: 'renameTile', tile: 'text-1', title: 'Text 1' }, made.draft).page, 'text-1') as { title?: string }).title, undefined);
    const gone = apply({ kind: 'removeTile', tile: 'text-1' }, made.draft).page;
    assert.equal(view(gone, 'text-1'), undefined);
    assert.deepEqual(gone.cubes.map((c) => c.id), trades.cubes.map((c) => c.id));
  });

  it('refused in words: blank or too long words, a chart set as text, a sheet the page has not', () => {
    refused(() => apply({ kind: 'addText', sheet: 'sheet-1', text: '  ', title: null, columns: null }), /a text tile has words: its text is blank/);
    refused(() => apply({ kind: 'addText', sheet: 'sheet-9', text: 'x', title: null, columns: null }), /'sheet-9' is not a sheet of the page/);
    refused(() => apply({ kind: 'setText', tile: 'chart', text: 'x' }), /'chart' is a chart, not a text tile/);
    refused(() => apply({ kind: 'addText', sheet: 'sheet-1', text: 'x'.repeat(10_001), title: null, columns: null }), /at most 10,000 characters/);
    refused(() => readPageCommand({ kind: 'setText', tile: 'text-1', text: 3 }), /a text tile's text is text/);
  });
});

describe("a grid of the page as a join reads it, the tab's Join… and Python's alike (joinInputOf: Z9)", () => {
  it('its cube as the page has it, under the title a person gave its tile, else what it reads; a chart refused', () => {
    const grid = trades.cubes.find((c) => c.id === 'grid-2')!.cube;
    assert.deepEqual(joinInputOf(trades, 'grid-2'), { name: 'By desk', source: grid.source, query: grid.query });
    const untitled = apply({ kind: 'renameTile', tile: 'grid-2', title: null });
    assert.equal(joinInputOf(untitled.page, 'grid-2').name, 'trades', 'no title given: what it reads');
    refused(() => joinInputOf(trades, 'chart'), /'chart' is a chart: a join is of two grids/);
    refused(() => joinInputOf(trades, 'grid-9'), /'grid-9' is not a tile of the page/);
  });
});

describe('snapped or live is page state: a snap and a go-live are page commands (design §18.8)', () => {
  const cube = (id: string): string => view(trades, id)!.cube!;

  it('a snap says the copy its grid reads, by its cube; another snap replaces it; live takes it off', () => {
    const snapped = apply({ kind: 'snap', grid: 'grid', copy: SNAP });
    assert.deepEqual(snapped.page.copies, { [cube('grid')]: SNAP });
    const again = { ...SNAP, table: 'snap_2' };
    assert.deepEqual(apply({ kind: 'snap', grid: 'grid', copy: again }, snapped).page.copies, { [cube('grid')]: again },
      'one copy a grid: the one it read before is its host\'s to let go');
    const both = apply({ kind: 'snap', grid: 'grid-2', copy: again }, snapped);
    const live = apply({ kind: 'live', grid: 'grid' }, both);
    assert.deepEqual(live.page.copies, { [cube('grid-2')]: again });
    assert.equal(apply({ kind: 'live', grid: 'grid-2' }, live).page.copies, undefined, 'none, no field');
    // the page's views, sheets and cubes are as they were: where its rows are is no step of what it shows
    assert.deepEqual({ ...snapped.page, copies: undefined }, { ...trades, copies: undefined });
  });

  it('a chart snaps with its grid; a grid live already, or none, is refused in words', () => {
    refused(() => apply({ kind: 'snap', grid: 'chart', copy: SNAP }), /'chart' is a chart: its grid snaps/);
    refused(() => apply({ kind: 'live', grid: 'grid' }), /'grid' reads its live rows already/);
    refused(() => apply({ kind: 'snap', grid: 'grid-9', copy: SNAP }), /grid-9/);
  });

  it('a grid removed takes its copy off the page; a frozen chart kept for its cube keeps it', () => {
    const snapped = apply({ kind: 'snap', grid: 'grid-2', copy: SNAP });
    // grid-2's chart is frozen: its cube stays for it, and the copy it reads with it
    assert.deepEqual(apply({ kind: 'removeTile', tile: 'grid-2' }, snapped).page.copies, { [cube('grid-2')]: SNAP });
    const onGrid = apply({ kind: 'snap', grid: 'grid', copy: SNAP });
    assert.equal(apply({ kind: 'removeTile', tile: 'grid' }, onGrid).page.copies, undefined);
  });

  it('the page writes its copies and reads them strictly; a saved page opens live, each snapped grid said', () => {
    const page = apply({ kind: 'snap', grid: 'grid', copy: { ...SNAP, pulledBy: { plane: 'warehouse', where: 'acme', as: 'ana', serverRows: 3 } } }).page;
    assert.deepEqual(readPage(JSON.parse(JSON.stringify(page))), page);
    const raw = JSON.parse(JSON.stringify(page)) as Record<string, unknown>;
    const bad = (copies: unknown, words: RegExp) => assert.throws(() => readPage({ ...raw, copies }), words);
    bad({ nope: SNAP }, /holds one for nope, which is no cube of the page/);
    bad({}, /'copies' is empty/);
    bad({ [cube('grid')]: { ...SNAP, rows: 3 } }, /a copy has no rows/);
    bad({ [cube('grid')]: { ...SNAP, takenAt: 'yesterday' } }, /says when it was taken, as an instant/);
    bad({ [cube('grid')]: { ...SNAP, pulledBy: { plane: 'moon', where: '' } } }, /ran in the tab, a warehouse or an engine/);
    const opened = openedLive(page);
    assert.equal(opened.page.copies, undefined);
    assert.deepEqual(opened.notes, [`${trades.cubes.find((c) => c.id === cube('grid'))!.cube.source.name} was snapped `
      + '(Snap 14:02) when the page was saved: it opens live, as a snap lasts only as long as the session that made it']);
    assert.deepEqual(openedLive(trades), { page: trades, notes: [] });
  });
});

describe('a page command read from JSON is the same command, and what DataCube does not know is refused', () => {
  const spec = (view(trades, 'chart') as ChartView).spec;
  const every: PageCommand[] = [
    { kind: 'renamePage', name: 'P' },
    { kind: 'savedAs', record: 'r-1' },
    { kind: 'savedAs', record: null },
    { kind: 'addSheet', name: null },
    { kind: 'renameSheet', sheet: 'sheet-1', name: 'S' },
    { kind: 'moveSheet', sheet: 'sheet-1', to: 1 },
    { kind: 'removeSheet', sheet: 'sheet-2' },
    { kind: 'renameTile', tile: 'grid', title: null },
    { kind: 'addGrid', id: 'grid-3', document: trades.cubes[0]!.cube, sheet: 'sheet-1', near: 'grid', title: null, columns: 2 },
    { kind: 'addChart', near: 'grid', spec, title: 'T', columns: null },
    { kind: 'setChart', chart: 'chart', spec },
    { kind: 'addText', id: 'text-1', sheet: 'sheet-1', text: '# Sales', title: null, columns: null },
    { kind: 'setText', tile: 'text-1', text: 'Up **3%**' },
    { kind: 'followGrid', chart: 'chart', grid: 'grid-2' },
    { kind: 'detach', chart: 'chart', cube: 'grid-9', document: trades.cubes[0]!.cube },
    { kind: 'detach', chart: 'chart', cube: 'grid-9', document: trades.cubes[0]!.cube, copy: { table: 'snap_9', label: 'Snap 09:00', takenAt: '2026-10-24T09:00:00.000Z', rowCount: 3 } },
    { kind: 'select', chart: 'chart', conditions: [{ kind: 'condition', column: 'region', operator: 'equal', value: 'EMEA' }] },
    { kind: 'moveTile', tile: 'chart', sheet: 'sheet-2', columns: null },
    { kind: 'stack', tiles: ['grid', 'chart'] },
    { kind: 'removeTile', tile: 'chart' },
    { kind: 'arrange', sheet: 'sheet-1', preset: 'rows:1-1', order: ['grid', 'chart'] },
    { kind: 'layout', sheet: 'sheet-2', layout: sheet(trades, 'sheet-2').layout },
    // a page filter's two (applied over a page of class grids: test/page-filters.test.ts)
    { kind: 'setPageFilter', id: 'filter-1', class: 'trades::domain::Trade', condition: { kind: 'condition', column: 'region', operator: 'equal', value: 'EMEA' }, from: null },
    { kind: 'removePageFilter', filter: 'filter-1' },
    { kind: 'snap', grid: 'grid', copy: SNAP },
    { kind: 'live', grid: 'grid' },
    { kind: 'modelInto', project: 'datacube:q3', workspace: 'datacube', package: 'q3' },
    { kind: 'modelPin', revision: 'r2' },
    { kind: 'modelKeep', cube: 'grid', tables: ['t_trades'], class: null },
    { kind: 'rebuilt', cube: 'grid', source: trades.cubes[0]!.cube.source },
  ];

  it('every kind, through JSON and back', () => {
    assert.deepEqual([...new Set(every.map((c) => c.kind))].sort(), [...PAGE_COMMAND_KINDS].sort(), 'every kind is here');
    for (const c of every) assert.deepEqual(readPageCommand(JSON.parse(JSON.stringify(c))), c, c.kind);
  });

  it('an unknown kind, a misspelt field and a field of the wrong type, each in words', () => {
    refused(() => readPageCommand({ kind: 'addTab', name: null }), /'addTab' is not a page's command/);
    refused(() => readPageCommand({ kind: 'removeTile', tiles: 'x' }), /a 'removeTile' command takes tile, not tiles/);
    refused(() => readPageCommand({ kind: 'moveTile', tile: 'x', sheet: 'sheet-1', columns: 0 }), /columns is a whole number/);
    refused(() => readPageCommand({ kind: 'renameTile', tile: 'x', title: 3 }), /a tile's title is text/);
    // a copy, by the page's own reader
    refused(() => readPageCommand({ kind: 'snap', grid: 'grid', copy: { ...SNAP, rowCount: -1 } }), /a copy says its rows/);
    // a grid's cube, read by its own reader (the phase 1 audit's 3)
    refused(() => readPageCommand({ kind: 'addGrid', document: { bogus: 1 }, sheet: 'sheet-1', near: null, title: null, columns: null }),
      /cannot read saved cube/);
    // a grid whose source the page cannot write down is the screen's own: never one Python sends (the phase 2 audit's 9)
    refused(() => readPageCommand({ kind: 'addGrid', document: null, sheet: 'sheet-1', near: null, title: null, columns: null }),
      /a grid's cube is its document/);
  });
});

describe("the page's model, by its commands (page-model.ts: P4)", () => {
  // the corpus page with no model of its own yet
  const start: PageDocument = (({ model: _none, ...rest }) => rest)(trades);
  const draftOf = (p: PageDocument): PageDraft => ({ page: p, held: idsOf(p) });

  it('a model action records the commands of what it changed, in order: applied, they make the model it made (P15)', () => {
    const cube = start.cubes[0]!.id;
    const model = new PageModel();
    model.into('Q3', 'q3').made('datacube:q3');
    model.pin('r1');
    model.keep(cube, ['t_trades'], 'q3::domain::Trade');
    assert.deepEqual(model.commands.map((c) => c.kind), ['modelInto', 'modelPin', 'modelKeep']);
    const after = applyPageCommands(draftOf(start), model.commands).draft.page;
    assert.deepEqual(after.model, model.ref);
    assert.deepEqual(model.ref, { project: 'datacube:q3', workspace: 'datacube', package: 'q3', tables: { [cube]: ['t_trades'] },
      classes: { [cube]: 'q3::domain::Trade' }, revision: 'r1' });
    // the next action over the page's model: what it changed alone
    const next = new PageModel(model.ref);
    next.pin('r2');
    assert.deepEqual(next.commands, [{ kind: 'modelPin', revision: 'r2' }]);
    assert.deepEqual(new PageModel(model.ref).commands, []);
  });

  it('across an undo or a redo, what model actions wrote stays, and a grid brought back brings its own (D5: modelAcross)', () => {
    const [a, b] = [start.cubes[0]!.id, 'grid-b'];
    // the page put back held both grids, the model then knowing b's tables; the model now -- pinned since, a's class made
    const restored = { project: 'datacube:q3', workspace: 'datacube', package: 'q3', tables: { [b]: ['t_b'] }, revision: 'r1' };
    const now = { project: 'datacube:q3', workspace: 'datacube', package: 'q3', tables: { [a]: ['t_a'] },
      classes: { [a]: 'q3::domain::A' }, revision: 'r2' };
    assert.deepEqual(modelAcross(restored, now, [{ id: a }, { id: b }]), {
      project: 'datacube:q3', workspace: 'datacube', package: 'q3', tables: { [b]: ['t_b'], [a]: ['t_a'] },
      classes: { [a]: 'q3::domain::A' }, revision: 'r2' });
    // a grid the page put back does not hold keeps nothing; a page put back from before the model's first action, the
    // model as it is now
    assert.deepEqual(modelAcross(restored, now, [{ id: b }]), {
      project: 'datacube:q3', workspace: 'datacube', package: 'q3', tables: { [b]: ['t_b'] }, revision: 'r2' });
    assert.deepEqual(modelAcross(undefined, now, [{ id: a }])?.revision, 'r2');
    assert.equal(modelAcross(undefined, undefined, [{ id: a }]), undefined);
  });

  it('refused in words: a second model, a pin or a keep with none, a cube the page has not', () => {
    const with1 = applyPageCommand(draftOf(start), { kind: 'modelInto', project: 'p', workspace: 'w', package: 'k' }).draft;
    refused(() => applyPageCommand(with1, { kind: 'modelInto', project: 'q', workspace: 'w', package: 'k' }), /a page has one model/);
    refused(() => applyPageCommand(draftOf(start), { kind: 'modelPin', revision: 'r' }), /the page has no model yet/);
    refused(() => applyPageCommand(with1, { kind: 'modelKeep', cube: 'nope', tables: [], class: null }), /'nope' is no grid's cube/);
  });
});

describe("a page's charts following their cubes, by one rule wherever a cube changes (followedPage: S2)", () => {
  const draftOf = (p: PageDocument): PageDraft => ({ page: p, held: idsOf(p) });
  const chartOf = (p: PageDocument, id: string): ChartView => p.views.find((v) => v.id === id) as ChartView;
  const cube = trades.cubes[0]!;

  it('a live chart told a spec of another grouping is kept regrouped as its grid is, as the panel keeps it (P6)', () => {
    const live = chartOf(trades, 'chart');
    // told across desk (the panel tells before it follows): kept across region, the grid's first row group
    const told = { ...live.spec, x: 'desk' };
    const after = applyPageCommand(draftOf(trades), { kind: 'setChart', chart: 'chart', spec: told }).draft.page;
    assert.equal(chartOf(after, 'chart').spec.x, 'region');
    // a frozen chart keeps what it was told
    const frozen = applyPageCommand(draftOf(trades), { kind: 'setChart', chart: 'chart-2', spec: { ...chartOf(trades, 'chart-2').spec, x: 'desk' } }).draft.page;
    assert.equal(chartOf(frozen, 'chart-2').spec.x, 'desk');
  });

  it("a grid's edit regroups its live charts and takes off a selection its filter no longer holds (V1, V2)", () => {
    const mark: FilterNode = { kind: 'condition', column: 'region', operator: 'equal', value: 'EMEA' };
    const selected: PageDocument = {
      ...trades,
      views: trades.views.map((v) => (v.id === 'chart' ? { ...v, selection: [mark] } : v)),
      cubes: trades.cubes.map((c) => (c.id === cube.id ? { ...c, cube: { ...c.cube, query: { ...c.cube.query, filter: mark } } } : c)),
    };
    assert.deepEqual(followedPage(selected, cube.id, selected), selected, 'nothing moved: the same page');
    // regrouped by desk alone and the filter cleared, as a grid's edit leaves its cube
    const edited = withCube(selected, cube.id, { ...cube.cube, query: { ...cube.cube.query, rows: ['desk'] } });
    const after = followedPage(edited, cube.id, selected);
    assert.equal(chartOf(after, 'chart').spec.x, 'desk', 'the live chart regrouped');
    assert.equal(chartOf(after, 'chart').selection, undefined, 'its selection gone with the filter that held it');
    assert.equal(chartOf(after, 'chart-2').spec.x, 'region', 'the frozen one as it was');
  });
});

describe("a chart's selection judged only when its cube's filter moves (the S2 audit's 1 and 5: F1, F2)", () => {
  const draftOf = (p: PageDocument): PageDraft => ({ page: p, held: idsOf(p) });
  const chartOf = (p: PageDocument, id: string): ChartView => p.views.find((v) => v.id === id) as ChartView;
  const cube = trades.cubes[0]!;
  const mark: FilterNode = { kind: 'condition', column: 'region', operator: 'equal', value: 'EMEA' };
  const filtered = (p: PageDocument, filter: FilterNode | undefined): PageDocument => withCube(p, cube.id,
    { ...cube.cube, query: { ...(({ filter: _f, ...q }) => q)(cube.cube.query), ...(filter ? { filter } : {}) } });
  const selecting = (p: PageDocument, ...charts: string[]): PageDocument =>
    ({ ...p, views: p.views.map((v) => (charts.includes(v.id) ? { ...v, selection: [mark] } : v)) });

  it("a click's select told before its grid's refilter lands keeps its selection through edits that leave the filter", async () => {
    // the tab tells [select EMEA, addSort, refilter +EMEA]: the select at once, the grid's two when each commits
    const before = filtered(trades, undefined);
    const told = applyPageCommand(draftOf(before), { kind: 'select', chart: 'chart', conditions: [mark] }).draft.page;
    // made again on top of another writer's page (rebaseEdits), each grid edit following the page by the one rule
    const sorted = { ...cube.cube, query: { ...cube.cube.query, sorts: [{ column: 'desk', direction: 'asc' as const }] } };
    const done = await rebaseEdits(draftOf(before), [
      { page: { kind: 'select', chart: 'chart', conditions: [mark] } },
      { grid: 'grid', command: { kind: 'addSort', column: 'desk', direction: 'asc' } },
      { grid: 'grid', command: { kind: 'refilter', remove: [], add: [mark] } },
    ], async (_page, held, _id, commands) => (commands[0]!.kind === 'addSort'
      ? { ...held, query: { ...held.query, sorts: sorted.query.sorts } } : { ...held, query: { ...held.query, filter: mark } }));
    assert.deepEqual(chartOf(done.page, 'chart').selection, [mark], 'the selection kept: the filter caught up');
    // Python's options() on the chart while the refilter is in flight: setChart judges nothing of the filter
    const options = applyPageCommand(draftOf(told), { kind: 'setChart', chart: 'chart', spec: { ...chartOf(told, 'chart').spec, mark: 'line' } }).draft.page;
    assert.deepEqual(chartOf(options, 'chart').selection, [mark]);
  });

  it("a filter holds a selection's condition whatever order its writer put its keys in (F18, found by S4's soak)", () => {
    // the select's condition as its reader writes one; the grid's filter as a document read back from another writer
    const read: FilterNode = { kind: 'condition', column: 'region', operator: 'equal', value: 'EMEA' };
    const written = JSON.parse('{"column":"region","kind":"condition","operator":"equal","value":"EMEA"}') as FilterNode;
    const before = filtered(trades, undefined);
    const told = applyPageCommand(draftOf(before), { kind: 'select', chart: 'chart', conditions: [read] }).draft.page;
    const after = followedPage(filtered(told, written), cube.id, told);
    assert.deepEqual(chartOf(after, 'chart').selection, [read], 'the filter holds it: the selection stays');
    // and a refilter (or a chart's selection let go) takes it off the filter by its content too
    assert.equal(withoutConditions(written, [read]), undefined);
    assert.equal(withoutConditions({ kind: 'and', children: [written, read] }, [read, read], true), undefined);
  });

  it("a chart removed takes its selection off its cube's filter, and the cube's other charts are judged by it", () => {
    // two charts of one grid, both selecting EMEA, the filter holding it once (an edit in its Filters window)
    const two = applyPageCommand(draftOf(trades), { kind: 'addChart', near: 'grid', spec: chartOf(trades, 'chart').spec, title: null, columns: null });
    const both = selecting(filtered(two.draft.page, mark), 'chart', 'chart-3');
    const after = applyPageCommand(draftOf(both), { kind: 'removeTile', tile: 'chart' }).draft.page;
    assert.equal(after.cubes.find((c) => c.id === cube.id)!.cube.query.filter, undefined, "its selection off the grid's filter");
    assert.equal(chartOf(after, 'chart-3').selection, undefined, 'the other chart no longer holds a selection its filter lacks');
  });
});

describe("a cube changed by a page command, its live charts following it (followedPage: the S2 audit's 3)", () => {
  const draftOf = (p: PageDocument): PageDraft => ({ page: p, held: idsOf(p) });
  const chartOf = (p: PageDocument, id: string): ChartView => p.views.find((v) => v.id === id) as ChartView;
  const cube = trades.cubes[0]!;

  it("rebuilt over a frame that lost the live chart's column across: the chart regrouped as the cube opens", () => {
    assert.equal(chartOf(trades, 'chart').spec.x, 'region');
    const frame = cube.cube.source as FrameSource;
    const now: FrameSource = { ...frame, columns: frame.columns.filter((c) => c.name !== 'region') };
    const after = applyPageCommand(draftOf(trades), { kind: 'rebuilt', cube: cube.id, source: now }).draft.page;
    assert.deepEqual(after.cubes.find((c) => c.id === cube.id)!.cube.query.rows, ['desk']);
    assert.equal(chartOf(after, 'chart').spec.x, 'desk');
  });

  it("a detached chart's cube reads the copy its detach carries, or its live rows without one (the D19 audit's W2 limit, closed)", () => {
    const copy = { table: 'snap_9', label: 'Snap 09:00', takenAt: '2026-10-24T09:00:00.000Z', rowCount: 3 };
    const detached = apply({ kind: 'removeTile', tile: 'grid-2' }).page;
    const document = detached.cubes.find((c) => c.id === 'grid-2')!.cube;
    const snapped = applyPageCommands(draft(detached), [{ kind: 'detach', chart: 'chart-2', cube: 'grid-2', document, copy }]).draft.page;
    assert.deepEqual(snapped.copies?.['grid-2'], copy, 'the kept cube reads the copy');
    const same = applyPageCommands(draft(snapped), [{ kind: 'detach', chart: 'chart-2', cube: 'grid-2', document }]).draft.page;
    assert.deepEqual(same.copies?.['grid-2'], copy, 'the field absent: the copy as it is (a detach saying nothing of it un-snaps nothing)');
    const live = applyPageCommands(draft(snapped), [{ kind: 'detach', chart: 'chart-2', cube: 'grid-2', document, copy: null }]).draft.page;
    assert.equal(live.copies?.['grid-2'], undefined, 'null: its live rows, the copy let go');
    assert.deepEqual(readPageCommand({ kind: 'detach', chart: 'chart-2', cube: 'grid-2', document, copy: null }), { kind: 'detach', chart: 'chart-2', cube: 'grid-2', document, copy: null });
    assert.throws(() => readPageCommand({ kind: 'detach', chart: 'chart-2', cube: 'grid-2', document, copy: { table: 'x' } }), /copy|takenAt|label|rowCount/);
  });

  it("a detached chart has no grid to follow: made to follow, refused in DataCube's words, as the screen's pill is off (B8)", () => {
    // grid-2 removed: its frozen chart-2 stays, detached
    const grid2 = trades.views.find((v) => v.kind === 'grid' && v.id === 'grid-2')!;
    const d = applyPageCommand(draftOf(trades), { kind: 'removeTile', tile: grid2.id }).draft;
    const { frozen: _f, ...live } = chartOf(d.page, 'chart-2').spec;
    assert.throws(() => applyPageCommand(d, { kind: 'setChart', chart: 'chart-2', spec: live }),
      /^CubeRefusal: 'chart-2' is detached -- its grid was removed, and it keeps its own copy of that grid's query -- so it has no grid to follow$/);
    // frozen, it takes any other edit
    const told = applyPageCommand(d, { kind: 'setChart', chart: 'chart-2', spec: { ...chartOf(d.page, 'chart-2').spec, mark: 'line' } }).draft.page;
    assert.equal(chartOf(told, 'chart-2').spec.mark, 'line');
    // and a chart that follows its grid goes frozen and live again as its pill toggles it
    const { frozen: _g, ...following } = chartOf(trades, 'chart-2').spec;
    assert.equal(chartOf(applyPageCommand(draftOf(trades), { kind: 'setChart', chart: 'chart-2', spec: following }).draft.page, 'chart-2').spec.frozen, undefined);
  });

  it("followGrid has a chart read another grid's cube (D7: Show Grid): its selection off the cube it read, a detached one's cube let go", () => {
    // grid-2 removed: its frozen chart-2 stays, detached, reading grid-2's cube kept for it
    const grid2 = trades.views.find((v) => v.kind === 'grid' && v.id === 'grid-2')!;
    const d = applyPageCommand(draftOf(trades), { kind: 'removeTile', tile: grid2.id }).draft;
    assert.ok(d.page.cubes.some((c) => c.id === grid2.cube), "the detached chart's cube kept for it");
    const after = applyPageCommand(d, { kind: 'followGrid', chart: 'chart-2', grid: 'grid' }).draft.page;
    assert.equal(chartOf(after, 'chart-2').cube, 'grid');
    assert.equal(after.cubes.some((c) => c.id === grid2.cube), false, 'the cube no view reads let go');
    // a chart with a selection: taken off the filter of the cube it read, and off the chart
    const emea = { kind: 'condition', column: 'region', operator: 'equal', value: 'EMEA' } as const;
    const gridCube = trades.views.find((v) => v.id === 'grid')!.cube;
    const selected: PageDocument = {
      ...trades,
      cubes: trades.cubes.map((c) => (c.id === gridCube ? { ...c, cube: { ...c.cube, query: { ...c.cube.query, filter: emea } } } : c)),
      views: trades.views.map((v) => (v.id === 'chart' && v.kind === 'chart' ? { ...v, selection: [emea] } : v)),
    };
    const moved = applyPageCommand(draftOf(selected), { kind: 'followGrid', chart: 'chart', grid: 'grid-2' }).draft.page;
    assert.equal(chartOf(moved, 'chart').cube, trades.views.find((v) => v.id === 'grid-2')!.cube);
    assert.equal('selection' in chartOf(moved, 'chart'), false);
    assert.equal(moved.cubes.find((c) => c.id === gridCube)!.cube.query.filter, undefined, "off its old grid's filter");
    // its page filter (its click on a grid over a class) off too, as a click on nothing takes it; another's stays (the S7
    // audit's Y-1: Show Grid takes off what the chart selected, on either side)
    const TRADE = 'trades::domain::Trade';
    const filtered: PageDocument = { ...selected, filters: [{ id: 'filter-1', class: TRADE, condition: emea, from: 'chart' },
      { id: 'filter-2', class: TRADE, condition: emea }] };
    const unfiltered = applyPageCommand(draftOf(filtered), { kind: 'followGrid', chart: 'chart', grid: 'grid-2' }).draft.page;
    assert.deepEqual(unfiltered.filters?.map((f) => f.id), ['filter-2']);
  });
});

describe("a grid's cube opened again over its source as it is now, by its command (rebuilt: P11)", () => {
  const draftOf = (p: PageDocument): PageDraft => ({ page: p, held: idsOf(p) });
  const cube = trades.cubes[0]!;
  const view = trades.views.find((v) => v.kind === 'grid' && v.cube === cube.id)!;
  // its frame a column more, and one gone, as a host opens it again over the frame as it is now
  const frame = cube.cube.source as FrameSource;
  const now: FrameSource = { ...frame, columns: [...frame.columns.filter((c) => c.name !== 'qty'), { name: 'book', type: 'String' }] };
  const held = (p: PageDocument): CubeDocument => p.cubes.find((c) => c.id === cube.id)!.cube;

  it('the cube held opened over the source as it is now: a new column hidden, a gone one left out, nothing else moved', () => {
    const after = applyPageCommand(draftOf(trades), { kind: 'rebuilt', cube: cube.id, source: now }).draft.page;
    assert.deepEqual(held(after).source, now);
    assert.deepEqual(held(after).query.columns.map((c) => c.name), now.columns.map((c) => c.name));
    assert.equal((held(after).configuration as { columns?: Record<string, { hidden?: boolean }> }).columns?.['book']?.hidden, true);
    assert.deepEqual({ ...after, cubes: trades.cubes }, trades, 'the rest of the page as it was');
    // the rule is cube-document.ts cubeOpenedAgain's, whoever applies it
    assert.deepEqual(held(after), cubeOpenedAgain(cube.cube, now).document);
  });

  it("made again on top of another writer's edit of the grid, that edit kept (the S1 audit's 1)", async () => {
    // the other writer regrouped the grid meanwhile; the host's rebuilt, made against the page before, made again on it
    const regrouped = { ...trades, cubes: trades.cubes.map((c) => (c.id === cube.id ? { ...c, cube: { ...c.cube, query: { ...c.cube.query, rows: ['desk'] } } } : c)) };
    const done = await rebaseEdits(draftOf(regrouped), [{ page: { kind: 'rebuilt', cube: cube.id, source: now } }],
      async () => { throw new Error('no grid edit here'); });
    assert.deepEqual(held(done.page).query.rows, ['desk'], "the other writer's grouping kept");
    assert.deepEqual(held(done.page).source, now, 'and the cube opened over the frame as it is now');
  });

  it('a page opened again tells each cube it opened as another, but a snapped one (the S1 audit\'s 6)', () => {
    const opened = { ...trades, cubes: trades.cubes.map((c) => (c.id === cube.id ? { ...c, cube: { ...c.cube, source: now } } : c)) };
    assert.deepEqual(rebuiltCommands(trades, opened, [cube.id]), [{ kind: 'rebuilt', cube: cube.id, source: now }]);
    assert.deepEqual(rebuiltCommands(trades, opened, []), [], 'none opened as another: nothing told');
    // snapped, it reads its copy: no frame opens it again
    const snapped = applyPageCommand(draftOf(trades), { kind: 'snap', grid: view.id, copy: SNAP }).draft.page;
    assert.deepEqual(rebuiltCommands(snapped, opened, [cube.id]), []);
    // the one rule, which Python asks before opening a cube again (headless cubeOpensAgain: the S7 audit's Y-2)
    assert.equal(opensAgain(trades, cube.id), true);
    assert.equal(opensAgain(snapped, cube.id), false);
    assert.equal(cubeOpensAgain(pageToJson(snapped), cube.id), 'false');
    assert.equal(cubeOpensAgain(pageToJson(trades), cube.id), 'true');
  });

  it('refused in words: no such cube, a snapped grid (its copy does not move), another source', () => {
    refused(() => applyPageCommand(draftOf(trades), { kind: 'rebuilt', cube: 'nope', source: now }), /'nope' is no grid's cube/);
    const snapped = applyPageCommand(draftOf(trades), { kind: 'snap', grid: view.id, copy: SNAP }).draft;
    refused(() => applyPageCommand(snapped, { kind: 'rebuilt', cube: cube.id, source: now }), /reads its snap/);
    refused(() => applyPageCommand(draftOf(trades), { kind: 'rebuilt', cube: cube.id, source: { ...now, name: 'elsewhere' } }),
      /a cube rebuilt reads the same source/);
  });

  it('opens a cube again over any source, its columns as they are now: which sources a host reads is host-tables.ts onHost', () => {
    // (X1, X3) a file's cube opened again over the file read in again, a new column hidden
    const file: CubeSource = { _type: 'file', name: 'trades.csv', format: 'csv', size: 3, sha256: 'ab'.repeat(32),
      columns: [...now.columns, { name: 'extra', type: 'Integer' }] } as unknown as CubeSource;
    const again = cubeOpenedAgain(cube.cube, file);
    assert.equal(again.document.source._type, 'file');
    assert.match(again.notes.join('; '), /2 new columns since this cube was saved, hidden: book, extra/);
  });
});
