// A page of its own (page/page-app.ts; docs/DATACUBE_PAGES_DESIGN_2026_10_09.md §3.1, §6), with real grids (CubeApps
// over a fake engine), made the way DataCube's app makes them: the page's bar and menu, a lone grid sharing the bar's
// strip, every grid equal (the first removable), the empty page, the page saved with one cube per grid and reopened, a
// detached chart's grid kept off the board and saved, a grid's own changes and Settings reaching the page.

import assert from 'node:assert/strict';
import { afterEach, beforeEach, describe, it } from 'node:test';
import { JSDOM } from 'jsdom';

import { CubeApp } from '../src/app.ts';
import type { FileSource } from '../src/cube-document.ts';
import { PageApp, type GridMaker, type PageAppOptions } from '../src/page/page-app.ts';
import { pageToJson, readPage } from '../src/page-document.ts';
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

/** A grid over `name`, made as DataCube's app makes one: compact, its source written down, wired to the page. */
const over = (name: string): GridMaker => (gridHost, spawned) => new CubeApp(gridHost, SNAPSHOT, {
  engine, planner: new StubPlanner(), compact: true, cubeSource: file(name), sourceLabel: name,
  ...(page ? { windowHost: page.root } : {}), ...spawned,
});

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
const bar = (): HTMLElement => host.querySelector('.dc-page-bar') as HTMLElement;
/** The page's menu, opened: its entries at the top level by what a person reads. */
function pageMenu(): HTMLElement[] {
  dom.window.document.dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: 'Escape' }));
  (bar().querySelector('.dc-titlebar-menu') as HTMLElement).click();
  return [...dom.window.document.querySelectorAll<HTMLElement>('.dc-menu [role^="menuitem"]')];
}
const label = (item: HTMLElement): string => item.querySelector(':scope > .dc-menu-label')?.textContent ?? '';
const entry = (items: HTMLElement[], text: string): HTMLElement | undefined => items.find((i) => label(i) === text);
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
    assert.equal(bar().querySelector('.dc-titlebar-title')!.textContent, 'Q3');
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
    assert.deepEqual(page.views(), { views: doc.views, layout: doc.layout });
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
