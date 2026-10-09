// A page of its own (page/page-app.ts; docs/DATACUBE_PAGES_DESIGN_2026_10_09.md §3.1, §6), with real grids (CubeApps
// over a fake engine), made the way DataCube's app makes them: the page's bar and menu, a lone grid sharing the bar's
// strip, every grid equal (the first removable), the empty page, the page saved with one cube per grid and reopened, a
// detached chart's grid kept off the board and saved, a grid's own changes and Settings reaching the page; every grid
// made by the host's maker (a copy too), the host's readout in the first grid's status bar, the bar's fold; and sheets
// (the design's §7): the name box, the tabs, a grid moved to another sheet with its chart following it, saved and
// reopened, a sheet renamed and deleted.

import assert from 'node:assert/strict';
import { afterEach, beforeEach, describe, it } from 'node:test';
import { JSDOM } from 'jsdom';

import { CubeApp } from '../src/app.ts';
import { DEFAULT_CONFIGURATION } from '../src/config.ts';
import { configurationOf, type CubeDocument, type FileSource } from '../src/cube-document.ts';
import { PageApp, type GridMaker, type PageAppOptions } from '../src/page/page-app.ts';
import { pageToJson, readPage } from '../src/page-document.ts';
import { tiles as tilesOf } from '../src/layout/bands.ts';
import { GateEngine, SNAPSHOT, StubPlanner, settle } from './cube-fixture.ts';

let dom: JSDOM;
let host: HTMLElement;
let engine: GateEngine;
let page: PageApp;
let changes: number;
let unhandled: unknown[];

beforeEach(() => {
  dom = new JSDOM('<!doctype html><div id="p"></div>');
  const g = globalThis as unknown as Record<string, unknown>;
  g['window'] = dom.window;
  g['document'] = dom.window.document;
  g['requestAnimationFrame'] = (cb: FrameRequestCallback) => {
    cb(0);
    return 1;
  };
  g['cancelAnimationFrame'] = () => {};
  host = dom.window.document.getElementById('p') as unknown as HTMLElement;
  // jsdom has no canvas: a 2D context that draws nothing, so ECharts can mount (as chart-tiles.test.ts does)
  const inert: ProxyHandler<object> = {
    get: (_t, key) => (key === 'canvas' ? dom.window.document.createElement('canvas')
      : key === 'measureText' ? () => ({ width: 0 }) : () => undefined),
    set: () => true,
  };
  (dom.window.HTMLCanvasElement.prototype as unknown as { getContext: () => unknown }).getContext = () => new Proxy({}, inert);
  engine = new GateEngine();
  changes = 0;
  made = [];
  unhandled = [];
  process.removeAllListeners('unhandledRejection');
  process.on('unhandledRejection', (e) => unhandled.push(e));
});

afterEach(async () => {
  await settle();
  page?.dispose();
  process.removeAllListeners('unhandledRejection');
  assert.deepEqual(unhandled, [], 'a promise the page started was rejected and nothing handled it');
});

/** A file a grid reads, as its saved cube names it. */
const file = (name: string): FileSource => ({
  _type: 'file', name, format: 'csv', size: 10, sha256: name.padEnd(64, '0').slice(0, 64),
  columns: SNAPSHOT.columns.map((c) => ({ name: c.name, type: c.type })),
});

/** Each grid the makers made, by its tile's id, and whether it was a copy (made with a start). */
let made: [string | undefined, boolean][] = [];

/**
 * A grid over `name`, made as DataCube's app makes one: compact, its source written down, wired to the page -- or, a
 * copy, starting where its grid is.
 */
const over = (name: string, saved?: CubeDocument): GridMaker => (gridHost, spawned, start) => {
  made.push([spawned.id, start !== undefined]);
  // named after its source, as DataCube's app names a grid it opens (its report title)
  const configuration = start?.configuration ?? (saved ? configurationOf(saved) : { ...DEFAULT_CONFIGURATION, reportTitle: name });
  return new CubeApp(gridHost, start?.snapshot ?? SNAPSHOT, {
    engine, planner: new StubPlanner(), compact: true, cubeSource: file(name), sourceLabel: name,
    ...(configuration ? { configuration } : {}),
    ...(page ? { windowHost: page.root } : {}), ...spawned,
  });
};

function newPage(extra: Partial<PageAppOptions> = {}): PageApp {
  page = new PageApp({
    host,
    title: 'Q3',
    empty: (slot) => { slot.textContent = 'Add a data source'; },
    onChange: () => { changes += 1; },
    ...extra,
  });
  return page;
}

const tile = (id: string): HTMLElement => host.querySelector(`[data-tile="${id}"]`) as HTMLElement;
/** The sheets' tabs, as they read. */
const tabs = (): string[] => [...host.querySelectorAll<HTMLElement>('.dc-sheet-tab')].map((t) => t.textContent ?? '');
const tabOf = (label: string): HTMLElement => [...host.querySelectorAll<HTMLElement>('.dc-sheet-tab')].find((t) => t.textContent === label)!;
/** A tab clicked, as a pointer does (pressed and let go where it was). */
function clickTab(label: string): void {
  const tab = tabOf(label);
  tab.dispatchEvent(new dom.window.PointerEvent('pointerdown', { bubbles: true, button: 0, clientX: 0 }));
  tab.dispatchEvent(new dom.window.PointerEvent('pointerup', { bubbles: true, button: 0, clientX: 0 }));
}
const bar = (): HTMLElement => host.querySelector('.dc-page-bar') as HTMLElement;
/** The page's menu, opened: its entries at the top level by what a person reads. */
function pageMenu(): HTMLElement[] {
  dom.window.document.dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: 'Escape' }));
  (bar().querySelector('.dc-titlebar-menu') as HTMLElement).click();
  return [...dom.window.document.querySelectorAll<HTMLElement>('.dc-menu [role^="menuitem"]')];
}
const label = (item: HTMLElement): string => item.querySelector(':scope > .dc-menu-label')?.textContent ?? '';
const entry = (items: HTMLElement[], text: string): HTMLElement | undefined => items.find((i) => label(i) === text);
/** An entry of a grid's own menu (its tile's, or the bar's while it is alone), by what it says. */
function gridMenuItem(gridTile: string, text: string): HTMLElement {
  dom.window.document.dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: 'Escape' }));
  const button = (tile(gridTile).querySelector('.dc-tile-menu') ?? bar().querySelector('.dc-page-alone .dc-tile-menu')) as HTMLElement;
  button.click();
  const item = [...dom.window.document.querySelectorAll<HTMLElement>('.dc-menu [role^="menuitem"]')].find((i) => label(i) === text);
  assert.ok(item, `${gridTile}'s menu has ${text}`);
  return item;
}
/** Insert > Visualization, from a grid's right-click menu. */
async function chartOf(gridTile: string): Promise<string> {
  const before = new Set(page.views().views.filter((v) => v.kind === 'chart').map((v) => v.id));
  (tile(gridTile).querySelector('.dc-row .dc-cell') as HTMLElement)
    .dispatchEvent(new dom.window.MouseEvent('contextmenu', { bubbles: true, cancelable: true }));
  await settle();
  const item = [...dom.window.document.querySelectorAll<HTMLElement>('.dc-menu .dc-menu-item')]
    .find((el) => label(el) === 'Visualization' && label(el.parentElement!.closest('.dc-menu-item') as HTMLElement) === 'Insert');
  assert.ok(item, 'Insert > Visualization');
  item.click();
  await settle();
  const chart = [...host.querySelectorAll<HTMLElement>('[data-tile^="chart-"]')].map((t) => t.dataset['tile']!).find((id) => !before.has(id));
  assert.ok(chart, 'a chart on the page');
  return chart;
}

describe('a page of its own', () => {
  it('starts empty: its bar and menu, and the host\'s choices of a source where the tiles would be', () => {
    newPage();
    assert.equal(bar().querySelector('.dc-page-name')!.textContent, 'Q3');
    // one sheet, its tab, and a + for more
    assert.deepEqual([...bar().querySelectorAll('.dc-sheet-tab')].map((t) => t.textContent), ['Sheet 1']);
    assert.ok(bar().querySelector('.dc-sheet-add'));
    assert.equal(host.querySelector<HTMLElement>('.dc-page-empty')!.hidden, false);
    assert.equal(host.querySelector('.dc-page-empty')!.textContent, 'Add a data source');
    assert.equal(host.querySelector<HTMLElement>('.dc-board-host')!.hidden, true);
    assert.equal(entry(pageMenu(), 'Arrange…')!.getAttribute('aria-disabled'), 'true');
    assert.match(page.saveRefusal() ?? '', /nothing on this page to save/);
    assert.equal(page.document('Q3'), undefined);
  });

  it('one grid alone shares the bar\'s strip: no frame, its source, pill and menu at the bar\'s right', async () => {
    newPage();
    const id = page.addGrid(over('trades.csv'));
    await settle();
    assert.equal(host.querySelector<HTMLElement>('.dc-page-empty')!.hidden, true);
    assert.ok(host.querySelector('.dc-bands')!.classList.contains('dc-bands-alone'));
    assert.ok(tile(id).classList.contains('dc-band-tile-alone'));
    const strip = bar().querySelector('.dc-page-alone')!;
    assert.match(strip.textContent ?? '', /trades\.csv/);
    assert.ok(strip.querySelector('.dc-titlebar-toggle'), 'its Live/Snapped pill');
    assert.ok(strip.querySelector('.dc-tile-menu'), 'its own menu');
    assert.equal(tile(id).querySelector('.dc-tile-menu'), null, 'not in the tile as well');
  });

  it('a second tile moves the lone grid\'s header back into its tile; every grid alike, the first one removable', async () => {
    newPage();
    const a = page.addGrid(over('trades.csv'));
    const b = page.addGrid(over('orders.csv'), { near: a });
    await settle();
    assert.equal(host.querySelector('.dc-bands')!.classList.contains('dc-bands-alone'), false);
    assert.equal(bar().querySelector('.dc-page-alone')!.children.length, 0);
    for (const id of [a, b]) {
      assert.ok(tile(id).querySelector('.dc-tile-menu'), `${id} has its own menu`);
      assert.equal(tile(id).querySelector<HTMLButtonElement>('.dc-tile-remove')!.hidden, false, `${id} can be removed`);
    }
    // the first one removed: the other alone in the bar
    tile(a).querySelector<HTMLButtonElement>('.dc-tile-remove')!.click();
    await settle();
    assert.deepEqual(page.grids, [b]);
    assert.match(bar().querySelector('.dc-page-alone')!.textContent ?? '', /orders\.csv/);
    // and the last, from its own menu (its tile has no remove button while alone): the empty page
    (bar().querySelector('.dc-tile-menu') as HTMLElement).click();
    const remove = [...dom.window.document.querySelectorAll<HTMLElement>('.dc-menu [role^="menuitem"]')].find((i) => label(i) === 'Remove from Page');
    assert.ok(remove, 'Remove from Page, in the grid\'s own menu');
    remove.click();
    await settle();
    assert.deepEqual(page.grids, []);
    assert.equal(host.querySelector<HTMLElement>('.dc-page-empty')!.hidden, false);
  });

  it('is saved as one cube per grid, each over its own source, and reopens as it was', async () => {
    newPage();
    const a = page.addGrid(over('trades.csv'));
    const b = page.addGrid(over('orders.csv'), { near: a });
    await settle();
    const chart = await chartOf(b);
    const doc = page.document('Q3')!;
    assert.deepEqual(doc.cubes.map((c) => [c.id, c.cube.source._type === 'file' && c.cube.source.name]), [[a, 'trades.csv'], [b, 'orders.csv']]);
    assert.deepEqual(doc.views.map((v) => [v.id, v.kind, v.cube]), [[a, 'grid', a], [b, 'grid', b], [chart, 'chart', b]]);
    const back = readPage(pageToJson(doc));
    // reopened in a page of its own, each cube's source opened by the host (here: the same files)
    page.dispose();
    newPage();
    page.restore(back, new Map(back.cubes.map((c) => [c.id, over(c.cube.source.name)])));
    await settle();
    assert.deepEqual(page.grids, [a, b]);
    assert.deepEqual(page.views(), { views: doc.views, sheets: doc.sheets });
  });

  it('a frozen chart whose grid is removed keeps reading that grid, kept off the board; saved, and reopened detached', async () => {
    newPage();
    const a = page.addGrid(over('trades.csv'));
    const b = page.addGrid(over('orders.csv'), { near: a });
    await settle();
    const chart = await chartOf(b);
    (tile(chart).querySelector('.dc-tile-actions .dc-titlebar-toggle') as HTMLElement).click();
    await settle();
    tile(b).querySelector<HTMLButtonElement>('.dc-tile-remove')!.click();
    await settle();
    assert.equal(tile(chart).querySelector('.dc-titlebar-toggle')!.textContent, 'Detached');
    assert.deepEqual(page.grids, [a]);
    const doc = page.document('Q3')!;
    assert.deepEqual(doc.cubes.map((c) => c.id), [a, b], 'the removed grid kept, for its chart');
    assert.deepEqual(doc.views.map((v) => [v.id, v.kind, v.cube]), [[a, 'grid', a], [chart, 'chart', b]], 'no grid view for it');
    page.dispose();
    newPage();
    const back = readPage(pageToJson(doc));
    page.restore(back, new Map(back.cubes.map((c) => [c.id, over(c.cube.source.name)])));
    await settle();
    assert.deepEqual(page.grids, [a]);
    assert.equal(tile(chart).querySelector('.dc-titlebar-toggle')!.textContent, 'Detached');
    // its last chart gone, the kept grid goes too
    tile(chart).querySelector<HTMLButtonElement>('.dc-tile-remove')!.click();
    await settle();
    assert.deepEqual(page.document('Q3')!.cubes.map((c) => c.id), [a]);
  });

  it('a grid\'s own change is the page\'s: "changed since saved" re-reads it', async () => {
    newPage();
    const a = page.addGrid(over('trades.csv'));
    await settle();
    const before = changes;
    (tile(a).querySelector('.dc-zone-rows .dc-chip[data-column="desk"] .dc-chip-remove') as HTMLElement).click();
    await settle();
    assert.ok(changes > before, 'told');
    assert.deepEqual(page.grid(a)!.snapshot.rows, ['region']);
  });

  it('Settings saved on one grid are in effect on every grid, and the host keeps them', async () => {
    const kept: unknown[] = [];
    newPage({ onSettingsChanged: (values) => kept.push(values) });
    const a = page.addGrid(over('trades.csv'));
    const b = page.addGrid(over('orders.csv'), { near: a });
    await settle();
    const used: string[] = [];
    for (const id of [a, b]) {
      const grid = page.grid(id)!;
      const original = grid.useSettings.bind(grid);
      grid.useSettings = (values) => { used.push(id); original(values); };
    }
    entry(pageMenu(), 'Settings...')!.click();
    await settle();
    const save = [...dom.window.document.querySelectorAll<HTMLButtonElement>('button')].find((x) => x.textContent === 'Save');
    assert.ok(save, 'the Settings window, from the page\'s menu');
    save.click();
    await settle();
    assert.equal(kept.length, 1);
    assert.deepEqual(used.sort(), [a, b].sort());
  });

  it('Copy of Grid is made by the host\'s maker, starting where its grid is; the page saves both', async () => {
    newPage();
    const a = page.addGrid(over('trades.csv'));
    await settle();
    (tile(a).querySelector('.dc-zone-rows .dc-chip[data-column="desk"] .dc-chip-remove') as HTMLElement).click();
    await settle();
    gridMenuItem(a, 'Copy of Grid').click();
    await settle();
    assert.equal(page.grids.length, 2);
    const copy = page.grids.find((id) => id !== a)!;
    assert.deepEqual(made, [[a, false], [copy, true]], 'the copy made by the same maker, from a start');
    assert.ok(page.grid(copy), 'the page knows the copy');
    assert.deepEqual(page.grid(copy)!.snapshot.rows, ['region'], 'it starts where its grid is');
    assert.equal(page.saveRefusal(), undefined);
    const doc = page.document('Q3');
    assert.ok(doc, 'the page can be saved');
    assert.deepEqual(doc.cubes.map((c) => c.id), [a, copy]);
    assert.deepEqual(doc.views.map((v) => [v.id, v.kind, v.cube]), [[a, 'grid', a], [copy, 'grid', copy]]);
  });

  it('a detached chart\'s Update keeps a grid of its own, made by the host\'s maker, and the page saves it', async () => {
    newPage();
    const a = page.addGrid(over('trades.csv'));
    const b = page.addGrid(over('orders.csv'), { near: a });
    await settle();
    const chart = await chartOf(b);
    (tile(chart).querySelector('.dc-tile-actions .dc-titlebar-toggle') as HTMLElement).click();
    await settle();
    tile(b).querySelector<HTMLButtonElement>('.dc-tile-remove')!.click();
    await settle();
    (tile(chart).querySelector('.dc-chart-tile') as HTMLElement)
      .dispatchEvent(new dom.window.MouseEvent('contextmenu', { bubbles: true, cancelable: true }));
    [...dom.window.document.querySelectorAll<HTMLElement>('.dc-menu .dc-menu-item')].find((i) => label(i) === 'Open in grid')!.click();
    await settle();
    const update = [...tile(`edit-${chart}`).querySelectorAll<HTMLElement>('.dc-tile-actions > button')]
      .find((x) => x.textContent?.startsWith('Update'));
    assert.ok(update, 'the editing grid\'s Update');
    update.click();
    await settle();
    const own = `${chart}-query`;
    assert.deepEqual(made.at(-1), [own, true], 'made by the maker of the grid it copies');
    assert.ok(page.grid(own), 'the page knows it');
    const doc = page.document('Q3');
    assert.ok(doc, 'the page can be saved');
    assert.deepEqual(doc.cubes.map((c) => c.id), [a, own], 'the grid it replaced is gone');
    assert.deepEqual(doc.views.find((v) => v.id === chart)?.cube, own);
  });

  it('the host\'s readout is in the first grid\'s status bar only, and moves when that grid goes', async () => {
    const readout = dom.window.document.createElement('span');
    readout.textContent = 'in this tab';
    newPage({ hostStatus: (slot) => slot.append(readout) });
    const a = page.addGrid(over('trades.csv'));
    const b = page.addGrid(over('orders.csv'), { near: a });
    await settle();
    assert.ok(tile(a).contains(readout), 'in the first grid\'s bar');
    assert.equal(tile(b).querySelector('.dc-status-host'), null, 'the second grid has no empty slot for it');
    tile(a).querySelector<HTMLButtonElement>('.dc-tile-remove')!.click();
    await settle();
    assert.ok(tile(b).contains(readout), 'moved to the grid that is first now');
    assert.equal(tile(b).querySelectorAll('.dc-status-host').length, 1);
  });

  it('the bar\'s fold is kept on every grid: the grid left alone says the same, and a fold by a setting takes no focus', async () => {
    newPage();
    const a = page.addGrid(over('trades.csv'));
    const b = page.addGrid(over('orders.csv'), { near: a });
    await settle();
    (bar().querySelector('.dc-titlebar-fold') as HTMLElement).click();
    await settle();
    assert.equal(page.barFolded, true);
    assert.deepEqual([a, b].map((id) => page.grid(id)!.configuration.showTitleBar), [false, false]);
    (bar().querySelector('.dc-titlebar-lip') as HTMLElement).click();
    await settle();
    assert.deepEqual([a, b].map((id) => page.grid(id)!.configuration.showTitleBar), [true, true]);
    tile(b).querySelector<HTMLButtonElement>('.dc-tile-remove')!.click();
    await settle();
    assert.equal(page.barFolded, false, 'the grid left alone says shown, as the bar was');
    // the lone grid's own setting folds the bar; focus elsewhere (the host's own control) stays where it was
    const elsewhere = dom.window.document.createElement('button');
    dom.window.document.body.append(elsewhere);
    elsewhere.focus();
    page.grid(a)!.setChrome({ showTitleBar: false });
    await settle();
    assert.equal(page.barFolded, true);
    assert.equal(dom.window.document.activeElement, elsewhere);
  });

  it('a grid added while the bar is folded starts folded: the bar stays folded once it is alone; reopened folded', async () => {
    newPage();
    const a = page.addGrid(over('trades.csv'));
    const b = page.addGrid(over('orders.csv'), { near: a });
    await settle();
    (bar().querySelector('.dc-titlebar-fold') as HTMLElement).click();
    await settle();
    const c = page.addGrid(over('fills.csv'), { near: b });
    await settle();
    assert.equal(page.grid(c)!.configuration.showTitleBar, false, 'made while folded');
    const doc = page.document('Q3')!;
    for (const id of [a, b]) {
      tile(id).querySelector<HTMLButtonElement>('.dc-tile-remove')!.click();
      await settle();
    }
    assert.deepEqual(page.grids, [c]);
    assert.equal(page.barFolded, true, 'the grid left says folded, as the bar was');
    // a page of three grids, folded, reopened (each grid as it was saved, as the host reopens one): folded, as its
    // grids say
    page.dispose();
    newPage();
    const back = readPage(pageToJson(doc));
    page.restore(back, new Map(back.cubes.map((x) => [x.id, over(x.cube.source.name, x.cube)])));
    await settle();
    assert.equal(page.barFolded, true);
  });

  it('names its one sheet after its grid\'s source; the name box says the page\'s name, or that it has none yet', async () => {
    const renamed: string[] = [];
    newPage({ title: '', onRename: (name) => { renamed.push(name); page.setTitle(name); } });
    const a = page.addGrid(over('trades.csv'));
    await settle();
    assert.deepEqual(tabs(), ['trades.csv']);
    const box = bar().querySelector<HTMLElement>('.dc-page-name')!;
    assert.equal(box.textContent, 'Untitled page');
    assert.equal(page.suggestedName, 'trades.csv', 'Save suggests the sheet\'s name');
    // renamed in place: the host keeps it
    box.click();
    const field = box.querySelector('input')!;
    field.value = 'Q3 review';
    field.dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: 'Enter' }));
    assert.deepEqual(renamed, ['Q3 review']);
    assert.equal(box.textContent, 'Q3 review');
    assert.deepEqual(tabs(), ['trades.csv'], 'the page\'s name is not its sheet\'s');
    page.setChanged(true);
    assert.ok(box.classList.contains('dc-page-name-changed'), 'the dot, once changed since saved');
    assert.ok(page.grid(a));
  });

  it('a second sheet: +, shown and empty; a grid moved onto it from its menu; its chart, on the first, follows it', async () => {
    newPage();
    const a = page.addGrid(over('trades.csv'));
    await settle();
    const chart = await chartOf(a);
    const first = page.shownSheet;
    (bar().querySelector('.dc-sheet-add') as HTMLElement).click();
    await settle();
    assert.equal(tabs().length, 2);
    assert.notEqual(page.shownSheet, first, 'the new sheet is shown');
    assert.equal(host.querySelector<HTMLElement>('.dc-sheet-empty')!.hidden, false, 'an empty sheet says so');
    // back to the first, and its grid onto the second from the grid's own menu
    clickTab(tabs()[0]!);
    await settle();
    assert.equal(page.shownSheet, first);
    const second = page.sheets[1]!;
    gridMenuItem(a, 'Sheet 2').click();
    await settle();
    assert.deepEqual(page.sheets.map((id) => page.views().sheets.find((s) => s.id === id)!.layout.bands.length), [1, 1]);
    assert.equal(tile(a).closest<HTMLElement>('.dc-sheet')!.dataset['sheet'], second, 'the grid is on the second sheet');
    assert.equal(tile(chart).closest<HTMLElement>('.dc-sheet')!.dataset['sheet'], first, 'its chart stays');
    assert.deepEqual(tabs(), ['Sheet 1', 'trades.csv'], 'each sheet named after its first grid, else by its place');
    // the chart still follows the grid it reads, on the other sheet
    const before = page.views().views.find((v) => v.id === chart);
    (tile(a).querySelector('.dc-zone-rows .dc-chip[data-column="desk"] .dc-chip-remove') as HTMLElement).click();
    await settle();
    const after = page.views().views.find((v) => v.id === chart);
    assert.ok(before?.kind === 'chart' && after?.kind === 'chart');
    assert.notDeepEqual(after.spec, before.spec, 'it followed its grid\'s regrouping');
  });

  it('is saved with its sheets, their order and names; reopens on its first sheet, each tile on its own', async () => {
    newPage();
    const a = page.addGrid(over('trades.csv'));
    await settle();
    const chart = await chartOf(a);
    (bar().querySelector('.dc-sheet-add') as HTMLElement).click();
    await settle();
    const [s1, s2] = page.sheets;
    clickTab(tabs()[0]!);
    await settle();
    // the chart onto the second sheet, by its own right-click menu
    (tile(chart).querySelector('.dc-chart-tile') as HTMLElement)
      .dispatchEvent(new dom.window.MouseEvent('contextmenu', { bubbles: true, cancelable: true }));
    [...dom.window.document.querySelectorAll<HTMLElement>('.dc-menu [role^="menuitem"]')].find((i) => label(i) === 'Sheet 2')!.click();
    await settle();
    // the second sheet renamed with a double click, then moved first
    tabOf('Sheet 2').dispatchEvent(new dom.window.MouseEvent('dblclick', { bubbles: true }));
    const field = tabOf('').querySelector('input')!;
    field.value = 'Charts';
    field.dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: 'Enter' }));
    await settle();
    assert.deepEqual(tabs(), ['trades.csv', 'Charts']);
    tabOf('Charts').dispatchEvent(new dom.window.MouseEvent('contextmenu', { bubbles: true, cancelable: true }));
    [...dom.window.document.querySelectorAll<HTMLElement>('.dc-menu [role^="menuitem"]')].find((i) => label(i) === 'Move Left')!.click();
    await settle();
    assert.deepEqual(tabs(), ['Charts', 'trades.csv']);
    const doc = page.document('Q3')!;
    assert.deepEqual(doc.sheets.map((s) => [s.id, s.name]), [[s2, 'Charts'], [s1, undefined]]);
    assert.deepEqual(doc.sheets.map((s) => tilesOf(s.layout)), [[chart], [a]]);
    const back = readPage(pageToJson(doc));
    page.dispose();
    newPage();
    page.restore(back, new Map(back.cubes.map((c) => [c.id, over(c.cube.source.name, c.cube)])));
    await settle();
    assert.deepEqual(tabs(), ['Charts', 'trades.csv']);
    assert.equal(page.shownSheet, s2, 'it reopens on its first sheet');
    assert.deepEqual(page.views(), { views: doc.views, sheets: doc.sheets });
  });

  it('deletes a sheet with its tiles once asked, never its last; the readout follows the sheet shown', async () => {
    const asked: string[] = [];
    let answer = false;
    const readout = dom.window.document.createElement('span');
    newPage({ confirm: (q) => { asked.push(q); return answer; }, hostStatus: (slot) => slot.append(readout) });
    const a = page.addGrid(over('trades.csv'));
    await settle();
    (bar().querySelector('.dc-sheet-add') as HTMLElement).click();
    const b = page.addGrid(over('orders.csv'));
    await settle();
    assert.ok(tile(b).contains(readout), 'in the first grid of the sheet shown');
    clickTab('trades.csv');
    await settle();
    assert.ok(tile(a).contains(readout), 'moved with the sheet shown');
    const del = (labelText: string): void => {
      tabOf(labelText).dispatchEvent(new dom.window.MouseEvent('contextmenu', { bubbles: true, cancelable: true }));
      [...dom.window.document.querySelectorAll<HTMLElement>('.dc-menu [role^="menuitem"]')].find((i) => label(i) === 'Delete')!.click();
    };
    del('orders.csv');
    assert.equal(asked.length, 1, 'asked, as it has a tile');
    assert.equal(page.sheets.length, 2, 'and kept on No');
    answer = true;
    del('orders.csv');
    await settle();
    assert.deepEqual(tabs(), ['trades.csv']);
    assert.deepEqual(page.grids, [a], 'its grid went with it');
    tabOf('trades.csv').dispatchEvent(new dom.window.MouseEvent('contextmenu', { bubbles: true, cancelable: true }));
    const last = [...dom.window.document.querySelectorAll<HTMLElement>('.dc-menu [role^="menuitem"]')].find((i) => label(i) === 'Delete')!;
    assert.equal(last.getAttribute('aria-disabled'), 'true', 'the last sheet stays');
  });

  it('its menu: New > Data Source adds a grid through the host\'s picker; the host\'s entries; Page File exports it', async () => {
    const files: [string, string][] = [];
    const picked: string[] = [];
    newPage({
      openSource: async () => over('picked.csv'),
      hostMenu: () => [{ id: 'host.save', label: 'Save', section: 'file' }],
      onHostMenu: (item) => picked.push(item.id ?? ''),
      download: (name, _mime, content) => files.push([name, content]),
    });
    const items = pageMenu();
    const newItem = entry(items, 'New')!;
    (newItem.querySelector('[role^="menuitem"]') as HTMLElement).click();
    await settle();
    assert.equal(page.grids.length, 1);
    entry(pageMenu(), 'Save')!.click();
    assert.deepEqual(picked, ['host.save']);
    const exportItem = entry(pageMenu(), 'Export')!;
    (exportItem.querySelector('[role^="menuitem"]') as HTMLElement).click();
    assert.equal(files.length, 1);
    assert.equal(files[0]![0], 'Q3.page.json');
    assert.equal(readPage(files[0]![1]).cubes[0]!.cube.source.name, 'picked.csv');
  });
});
