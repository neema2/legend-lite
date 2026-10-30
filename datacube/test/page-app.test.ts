// A page of its own (page/page-app.ts; docs/DATACUBE_PAGES_DESIGN_2026_10_09.md §3.1, §6), with real grids (CubeApps
// over a fake engine), made the way DataCube's app makes them: the page's bar and menu, a lone grid sharing the bar's
// strip, every grid equal (the first removable), the empty page, the page saved with one cube per grid and reopened, a
// detached chart's grid kept off the board and saved, a grid's own changes and Settings reaching the page; every grid
// made by the host's maker (a copy too), the host's readout in the first grid's status bar, the bar's fold; and sheets
// (the design's §7): the name box, the tabs, a grid moved to another sheet with its chart following it, saved and
// reopened, a sheet renamed and deleted.

import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { afterEach, beforeEach, describe, it } from 'node:test';
import { JSDOM } from 'jsdom';

import { CubeApp } from '../src/app.ts';
import { DEFAULT_CONFIGURATION } from '../src/config.ts';
import { configurationOf, treeOf, type CubeDocument, type FileSource } from '../src/cube-document.ts';
import { applyGridCommands, type GridCommand } from '../src/grid-commands.ts';
import type { ExportPage } from '../src/export-model.ts';
import { PageApp, type GridMaker, type PageAppOptions } from '../src/page/page-app.ts';
import { HOLD_MS } from '../src/ui/long-press.ts';
import { gridCopyOf, newPage as freshPage, pageToJson, readPage, unnamedLabel, type PageDocument } from '../src/page-document.ts';
import { PlanThenRun, type QueryRunner } from '../src/runner.ts';
import { applyPageCommands, idsOf, type PageCommand, type PageDraft } from '../src/page-commands.ts';
import { CubeRefusal, type FilterNode } from '../src/snapshot.ts';
import type { TimeseriesSpec } from '../src/timeseries-spec.ts';
import { GateEngine, SNAPSHOT, StubPlanner, settle, plus } from './cube-fixture.ts';
import { theirs } from './theirs.ts';
import { convergeOn, type ConvergeGrid } from '../src/app/live-converge.ts';
import { classRelation } from '../src/class-source.ts';
import * as echarts from 'echarts/core';
// the page fetches its filter bar when it first needs one (a dynamic import): loaded here, so that import finds it
// loaded within one settle() on any machine -- CI's Windows runner took longer than that to fetch it (PR #64's CI,
// 2026-10-22: 'a page with a class grid has the bar' saw the slot still empty)
import '../src/ui/page-filter-bar.ts';
import { bandsOf } from './bands-of.ts';
import { layoutTiles } from '../src/page-document.ts';

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
/** A chart pinned by its state (its grouping its own): its state's menu opened, Pinned picked. */
async function pin(chart: string): Promise<void> {
  (tile(chart).querySelector('.dc-tile-state') as HTMLElement).click();
  const item = [...dom.window.document.querySelectorAll<HTMLElement>('.dc-menu .dc-menu-item')]
    .find((e) => e.querySelector(':scope > .dc-menu-label')?.textContent === 'Pinned');
  if (!item) throw new Error('the state offers no Pinned');
  item.click();
  await settle();
}
/** The sheets' tabs, as they read. */
const tabs = (): string[] => [...host.querySelectorAll<HTMLElement>('.dc-sheet-tab')].map((t) => t.querySelector('.dc-sheet-label')?.textContent ?? '');
const tabOf = (label: string): HTMLElement => [...host.querySelectorAll<HTMLElement>('.dc-sheet-tab')]
  .find((t) => (t.querySelector('.dc-sheet-label')?.textContent ?? '') === label)!;
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
/** A grid's plane moved from its state (left of its title, or a lone grid's in its sheet's tab): its menu opened,
 *  `pick` chosen -- Snap, or Go Live, Snap Again. */
async function movePlane(pick: string): Promise<void> {
  dom.window.document.dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: 'Escape' }));
  (host.querySelector('.dc-tile-state:is([data-state="live"], [data-state="snapped"])') as HTMLElement).click();
  const items = [...dom.window.document.querySelectorAll<HTMLElement>('.dc-menu [role^="menuitem"]')];
  const item = entry(items, pick);
  assert.ok(item, `its state offers ${pick}: ${items.map(label).join(' | ')}`);
  item.click();
  await settle();
}
/** An entry of a grid's right-click (a submenu's too: Create ▸, Tile ▸ Move to Sheet ▸), by what it says. */
function gridMenuItem(gridTile: string, text: string): HTMLElement {
  dom.window.document.dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: 'Escape' }));
  (tile(gridTile).querySelector('.dc-row .dc-cell') as HTMLElement)
    .dispatchEvent(new dom.window.MouseEvent('contextmenu', { bubbles: true, cancelable: true }));
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
    .find((el) => label(el) === 'Visualization' && label(el.parentElement!.closest('.dc-menu-item') as HTMLElement) === 'Create');
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
    assert.deepEqual([...bar().querySelectorAll('.dc-sheet-tab .dc-sheet-label')].map((t) => t.textContent), ['Sheet 1']);
    assert.ok(bar().querySelector('.dc-sheet-add'));
    assert.equal(host.querySelector<HTMLElement>('.dc-page-empty')!.hidden, false);
    assert.equal(host.querySelector('.dc-page-empty')!.textContent, 'Add a data source');
    assert.equal(host.querySelector<HTMLElement>('.dc-board-host')!.hidden, true);
    assert.equal(entry(pageMenu(), 'Arrange…')!.getAttribute('aria-disabled'), 'true');
    assert.match(page.saveRefusal() ?? '', /nothing on this page to save/);
    assert.equal(page.document('Q3'), undefined);
  });

  it('cleared (New Page), is the new page DataCube starts, unnamed (page-document.ts newPage: D11, B9)', async () => {
    newPage();
    page.addGrid(over('trades.csv'));
    await settle();
    page.setTitle('Q3 review');
    page.clear();
    assert.equal(page.title, '');
    assert.equal(bar().querySelector('.dc-page-name')!.textContent, 'Untitled page');
    assert.deepEqual(page.document(page.title, undefined, true), freshPage());
  });

  it('one grid alone: no frame, no header -- its state in its sheet\'s tab, its source in its status bar (the user, 2026-10-25)', async () => {
    newPage();
    const id = page.addGrid(over('trades.csv'));
    await settle();
    assert.equal(host.querySelector<HTMLElement>('.dc-page-empty')!.hidden, true);
    assert.ok(host.querySelector('.dc-bands')!.classList.contains('dc-bands-alone'));
    assert.ok(tile(id).classList.contains('dc-band-tile-alone'));
    const state = bar().querySelector<HTMLElement>('.dc-sheet-tab-shown .dc-sheet-state .dc-tile-state');
    assert.ok(state, 'its state in its sheet\'s tab, before the sheet\'s name');
    assert.deepEqual([state.dataset['state'], state.textContent], ['live', 'Live']);
    assert.equal(tile(id).querySelector('.dc-tile-head .dc-tile-state'), null, 'not in its hidden header too');
    assert.equal(tile(id).querySelector('.dc-status-source')?.textContent, 'trades.csv \u00b7 Live', 'its status bar says it in words');
    assert.doesNotMatch(bar().querySelector('.dc-page-alone')!.textContent ?? '', /trades\.csv/, 'no source in the bar');
    assert.equal(host.querySelector('.dc-tile-menu'), null, 'no menu button: its right-click is its menu (the user, 2026-10-23)');
  });

  it('a second tile moves the lone grid\'s header back into its tile; every grid alike, the first one removable', async () => {
    newPage();
    const a = page.addGrid(over('trades.csv'));
    const b = page.addGrid(over('orders.csv'), { near: a });
    await settle();
    assert.equal(host.querySelector('.dc-bands')!.classList.contains('dc-bands-alone'), false);
    assert.equal(bar().querySelector('.dc-page-alone')!.children.length, 0);
    assert.equal(bar().querySelector('.dc-sheet-state .dc-tile-state'), null, 'no lone grid\'s state in the tab');
    for (const id of [a, b]) {
      assert.equal(tile(id).querySelector<HTMLButtonElement>('.dc-tile-remove')!.hidden, false, `${id} can be removed`);
      // its state back in its own header, first, before its title
      assert.ok(tile(id).querySelector('.dc-tile-head')!.firstElementChild!.classList.contains('dc-tile-state'), `${id}: its state first`);
    }
    // the first one removed: the other alone -- its state in the sheet's tab
    tile(a).querySelector<HTMLButtonElement>('.dc-tile-remove')!.click();
    await settle();
    assert.deepEqual(page.grids, [b]);
    assert.ok(bar().querySelector('.dc-sheet-state .dc-tile-state'), 'the lone grid\'s state in its sheet\'s tab');
    assert.equal(tile(b).querySelector('.dc-status-source')?.textContent, 'orders.csv \u00b7 Live');
    // and the last, from its right-click's Tile ▸ (its tile has no remove button while alone): the empty page
    gridMenuItem(b, 'Remove from Page').click();
    await settle();
    assert.deepEqual(page.grids, []);
    assert.equal(host.querySelector<HTMLElement>('.dc-page-empty')!.hidden, false);
  });

  it("a text tile from a grid's Create ▸ Text Box: edited in place, told as setText; another editor's followed; saved and reopened", async () => {
    const told: PageCommand[] = [];
    // the text tile's drawing and its editor are a chunk of their own (page-text.ts): fetched, then shown
    const until = async (shown: () => unknown): Promise<void> => {
      for (let i = 0; i < 100 && !shown(); i++) await new Promise((r) => setTimeout(r, 5));
    };
    newPage({ confirm: () => true, onPageCommand: (c) => told.push(c) });
    const a = page.addGrid(over('trades.csv'));
    await settle();
    told.length = 0;
    gridMenuItem(a, 'Text Box').click();
    await settle();
    assert.deepEqual(told.map((c) => c.kind), ['addText']);
    const id = (told[0] as { id: string }).id;
    assert.equal(id, 'text-1');
    // its editor open on its starting words: typed, OK tells them; drawn as its Markdown says, never as HTML
    await until(() => tile(id).querySelector('textarea'));
    const area = tile(id).querySelector('textarea') as HTMLTextAreaElement;
    assert.ok(area, 'its editor open');
    area.value = '# Q3\n\nBy **desk** <b>x</b>';
    [...tile(id).querySelectorAll('button')].find((b) => b.textContent === 'OK')!.click();
    await settle();
    await until(() => tile(id).querySelector('.dc-text-tile'));
    assert.deepEqual(told.at(-1), { kind: 'setText', tile: id, text: '# Q3\n\nBy **desk** <b>x</b>' });
    assert.equal(tile(id).querySelector('.dc-text-tile h3')?.textContent, 'Q3');
    assert.equal(tile(id).querySelector('.dc-text-tile strong')?.textContent, 'desk');
    assert.equal(tile(id).querySelector('.dc-text-tile b'), null, 'typed markup is text');
    // blank refused in the editor, which stays; Escape leaves the words as they were
    (tile(id).querySelector('.dc-text-tile') as HTMLElement).dispatchEvent(new dom.window.MouseEvent('dblclick', { bubbles: true }));
    await until(() => tile(id).querySelector('textarea'));
    const again = tile(id).querySelector('textarea') as HTMLTextAreaElement;
    again.value = '  ';
    const before = told.length;
    [...tile(id).querySelectorAll('button')].find((b) => b.textContent === 'OK')!.click();
    assert.match(tile(id).querySelector('.dc-text-problem')?.textContent ?? '', /blank/);
    assert.equal(told.length, before, 'nothing told');
    again.dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: 'Escape', bubbles: true }));
    await until(() => tile(id).querySelector('.dc-text-tile'));
    assert.equal(tile(id).querySelector('textarea'), null);
    assert.equal(tile(id).querySelector('.dc-text-tile h3')?.textContent, 'Q3');
    // another editor's words followed
    await theirs(page, [{ kind: 'setText', tile: id, text: 'From **Python**' }]);
    await settle();
    await until(() => tile(id).querySelector('.dc-text-tile p')?.textContent === 'From Python');
    assert.equal(tile(id).querySelector('.dc-text-tile p')?.textContent, 'From Python');
    // saved with the page, reopened as it was
    const doc = page.document('Q3')!;
    assert.deepEqual(doc.views.find((v) => v.id === id), { id, kind: 'text', text: 'From **Python**' });
    const back = readPage(pageToJson(doc));
    page.dispose();
    newPage();
    page.restore(back, new Map(back.cubes.map((c) => [c.id, over(c.cube.source.name)])));
    await settle();
    await until(() => tile(id)?.querySelector('.dc-text-tile strong'));
    assert.deepEqual(page.views(), { views: doc.views, sheets: doc.sheets });
    assert.equal(tile(id).querySelector('.dc-text-tile strong')?.textContent, 'Python');
  });

  it("a text box's files (§8): opened, a tab each beside its words, drawn; taken off; reopened where the file is not, it is asked for", async () => {
    // docs/DATACUBE_UI_REVIEW_2026_10_24.md §8: a page points to a text box's files by name, size and fingerprint --
    // their bytes stay where they were picked
    const told: PageCommand[] = [];
    const until = async (shown: () => unknown): Promise<void> => {
      for (let i = 0; i < 200 && !shown(); i++) await new Promise((r) => setTimeout(r, 5));
    };
    const plan = new File(['# Plan\n\nBy **desk**'], 'plan.md');
    const sha = createHash('sha256').update('# Plan\n\nBy **desk**').digest('hex');
    /** The file input Open Files… puts in the page (no handles to give in this browser), given `files`. */
    const pick = async (...files: File[]): Promise<void> => {
      await until(() => dom.window.document.querySelector('input.dc-file-input'));
      const input = dom.window.document.querySelector<HTMLInputElement>('input.dc-file-input')!;
      Object.defineProperty(input, 'files', { value: files });
      input.dispatchEvent(new dom.window.Event('change'));
    };
    const tabs = (id: string): string[] => [...tile(id).querySelectorAll('.dc-text-file-name')].map((t) => t.textContent ?? '');
    const open = (id: string, entry: string): void => {
      dom.window.document.dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: 'Escape' }));
      tile(id).querySelector('.dc-text-host')!.dispatchEvent(new dom.window.MouseEvent('contextmenu', { bubbles: true, cancelable: true }));
      [...dom.window.document.querySelectorAll<HTMLElement>('.dc-menu .dc-menu-item')].find((i) => label(i) === entry)!.click();
    };
    newPage({ confirm: () => true, onPageCommand: (c) => told.push(c) });
    const a = page.addGrid(over('trades.csv'));
    await settle();
    gridMenuItem(a, 'Text Box').click();
    await settle();
    const id = 'text-1';
    await until(() => tile(id).querySelector('textarea'));
    tile(id).querySelector('textarea')!.dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: 'Escape', bubbles: true }));
    await until(() => tile(id).querySelector('.dc-text-tile'));
    // Open Files…: told as the page's command, pointed to; shown in its own tab, drawn as Markdown
    told.length = 0;
    open(id, 'Open Files\u2026');
    await pick(plan);
    await until(() => tile(id).querySelector('.dc-text-file h3'));
    assert.deepEqual(told, [{ kind: 'setTextFiles', tile: id, files: [{ name: 'plan.md', size: plan.size, sha256: sha }] }]);
    assert.deepEqual(tabs(id), ['Text', 'plan.md']);
    assert.equal(tile(id).querySelector('.dc-text-file h3')?.textContent, 'Plan');
    assert.equal(tile(id).querySelector('.dc-text-file-name[aria-pressed="true"]')?.textContent, 'plan.md', 'the file added shown');
    // its words a tab away
    [...tile(id).querySelectorAll<HTMLElement>('.dc-text-file-name')].find((t) => t.textContent === 'Text')!.click();
    await until(() => tile(id).querySelector('.dc-text-tile'));
    assert.equal(tile(id).querySelector('.dc-text-file'), null);
    // the same file again: nothing told
    told.length = 0;
    open(id, 'Open Files\u2026');
    await pick(plan);
    await settle();
    assert.deepEqual(told, [], 'a file it shows already');
    // saved with the page as where it is, never its bytes
    const doc = page.document('Q3')!;
    assert.deepEqual(doc.views.find((v) => v.id === id), { id, kind: 'text', text: 'Double-click to write here.', files: [{ name: 'plan.md', size: plan.size, sha256: sha }] });
    assert.equal(pageToJson(doc).includes('By **desk**'), false, "the file's words are not in the page");
    // its tab's x takes it off
    tile(id).querySelector<HTMLButtonElement>('.dc-text-file-close')!.click();
    await settle();
    assert.deepEqual(told.at(-1), { kind: 'setTextFiles', tile: id, files: [] });
    assert.equal(tile(id).querySelector('.dc-text-files'), null, 'no files: no tabs');
    // reopened in a browser that kept nothing of it: asked for again by its name; the same file picked is shown
    const back = readPage(pageToJson(doc));
    page.dispose();
    newPage();
    page.restore(back, new Map(back.cubes.map((c) => [c.id, over(c.cube.source.name)])));
    await settle();
    await until(() => tile(id)?.querySelector('.dc-text-files'));
    [...tile(id).querySelectorAll<HTMLElement>('.dc-text-file-name')].find((t) => t.textContent === 'plan.md')!.click();
    await until(() => tile(id).querySelector('.dc-text-file-missing'));
    const missing = tile(id).querySelector('.dc-text-file-missing')!;
    assert.match(missing.textContent ?? '', /^plan\.md is not here \(this browser keeps nothing of it\): a page points to its files, never copies them\./);
    missing.querySelector('button')!.click();
    await pick(plan);
    await until(() => tile(id).querySelector('.dc-text-file h3'));
    assert.equal(tile(id).querySelector('.dc-text-file h3')?.textContent, 'Plan');
    // where the host kept its handle: read from there -- the browser asking first, the person's click letting it
    page.dispose();
    const asked: boolean[] = [];
    newPage({ textFile: async (given, click) => {
      asked.push(click);
      return given === sha ? (click ? { state: 'file', file: plan } : { state: 'needs-click' }) : undefined;
    } });
    page.restore(back, new Map(back.cubes.map((c) => [c.id, over(c.cube.source.name)])));
    await settle();
    await until(() => tile(id)?.querySelector('.dc-text-files'));
    [...tile(id).querySelectorAll<HTMLElement>('.dc-text-file-name')].find((t) => t.textContent === 'plan.md')!.click();
    await until(() => tile(id).querySelector('.dc-text-file-missing button'));
    assert.equal(tile(id).querySelector('.dc-text-file-missing button')?.textContent, 'Allow reading plan.md');
    tile(id).querySelector<HTMLButtonElement>('.dc-text-file-missing button')!.click();
    await until(() => tile(id).querySelector('.dc-text-file h3'));
    assert.deepEqual(asked, [false, true], 'asked once without a click, then with the click');
  });

  it("a text box's files, as the 2026-10-25 audit found them: refused before they are read, a read that fails said, by fingerprint, a changed file said each time, its preview no editor, an export its words", async () => {
    const told: PageCommand[] = [];
    const said: string[] = [];
    const until = async (shown: () => unknown): Promise<void> => {
      for (let i = 0; i < 200 && !shown(); i++) await new Promise((r) => setTimeout(r, 5));
    };
    const pick = async (...files: File[]): Promise<void> => {
      await until(() => dom.window.document.querySelector('input.dc-file-input'));
      const input = dom.window.document.querySelector<HTMLInputElement>('input.dc-file-input')!;
      Object.defineProperty(input, 'files', { value: files });
      input.dispatchEvent(new dom.window.Event('change'));
    };
    const open = (id: string, entry: string): void => {
      dom.window.document.dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: 'Escape' }));
      tile(id).querySelector('.dc-text-host')!.dispatchEvent(new dom.window.MouseEvent('contextmenu', { bubbles: true, cancelable: true }));
      [...dom.window.document.querySelectorAll<HTMLElement>('.dc-menu .dc-menu-item')].find((i) => label(i) === entry)!.click();
    };
    const show = (id: string, name: string): void => {
      [...tile(id).querySelectorAll<HTMLElement>('.dc-text-file-name')].find((t) => t.textContent === name)!.click();
    };
    // a maker that keeps each grid's export, as its CubeApp would call it
    const exports = new Map<string, () => ExportPage | undefined>();
    const keep = (name: string): GridMaker => (gridHost, spawned, start) => {
      if (spawned.id !== undefined && spawned.exportPage) exports.set(spawned.id, spawned.exportPage);
      return over(name)(gridHost, spawned, start);
    };
    newPage({ confirm: () => true, onPageCommand: (c) => told.push(c), refuse: (t) => said.push(t) });
    const a = page.addGrid(keep('trades.csv'));
    await settle();
    await chartOf(a);
    gridMenuItem(a, 'Text Box').click();
    await settle();
    const id = page.views().views.find((v) => v.kind === 'text')!.id;
    await until(() => tile(id).querySelector('textarea'));
    tile(id).querySelector('textarea')!.dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: 'Escape', bubbles: true }));
    await until(() => tile(id).querySelector('.dc-text-tile'));
    told.length = 0;
    // a kind it does not show: refused by its name, never read (a gigabyte was read and hashed before its kind was seen)
    let read = false;
    class Watched extends File {
      override arrayBuffer(): Promise<ArrayBuffer> {
        read = true;
        return super.arrayBuffer();
      }
    }
    open(id, 'Open Files…');
    await pick(new Watched(['not text'], 'clip.mp4'));
    await until(() => said.length > 0);
    assert.match(said.at(-1) ?? '', /^clip\.mp4 is not a kind of file a text box shows: Markdown, text, CSV or JSON/);
    assert.equal(read, false, 'refused before it was read');
    // a file that cannot be read (a folder dropped, a file changed since it was picked): said, never a silent nothing
    class Unread extends File {
      override arrayBuffer(): Promise<ArrayBuffer> {
        return Promise.reject(new Error('The file could not be read'));
      }
    }
    open(id, 'Open Files…');
    await pick(new Unread(['x'], 'notes.md'));
    await until(() => said.length > 1);
    assert.match(said.at(-1) ?? '', /^notes\.md could not be read \(The file could not be read\)/);
    assert.deepEqual(told, [], 'nothing told for either');
    // added: its preview double-clicked opens no editor over it; an export writes the box's words, the file in front
    const plan = new File(['# Plan\n\nBy **desk**'], 'plan.md');
    open(id, 'Open Files…');
    await pick(plan);
    await until(() => tile(id).querySelector('.dc-text-file h3'));
    tile(id).querySelector('.dc-text-file h3')!.dispatchEvent(new dom.window.MouseEvent('dblclick', { bubbles: true }));
    await settle();
    assert.equal(tile(id).querySelector('textarea'), null, "its preview's words are not the box's");
    const exported = exports.get(a)?.()?.tiles.find((t) => t.id === id) as { readonly text?: { readonly html: string } } | undefined;
    assert.ok(exported?.text?.html.includes('dc-text-tile'), `its words drawn, not their Markdown: ${exported?.text?.html}`);
    const withPlan = page.document('Q3')!;
    // taken off by its fingerprint: a step that draws the same files again draws equal ones, not the same objects
    page.putBack(readPage(pageToJson(withPlan)), []);
    await settle();
    told.length = 0;
    tile(id).querySelector<HTMLButtonElement>('.dc-text-file-close')!.click();
    await settle();
    assert.deepEqual(told.at(-1), { kind: 'setTextFiles', tile: id, files: [] });
    // reopened where the file changed since: said each time it is shown, never kept as the one added
    const back = readPage(pageToJson(withPlan));
    page.dispose();
    newPage({ textFile: async () => ({ state: 'file', file: new File(['# Plan, again'], 'plan.md') }) });
    page.restore(back, new Map(back.cubes.map((c) => [c.id, over(c.cube.source.name)])));
    await settle();
    await until(() => tile(id)?.querySelector('.dc-text-files'));
    for (let time = 0; time < 2; time++) {
      show(id, 'plan.md');
      await until(() => tile(id).querySelector('.dc-text-file h3'));
      assert.equal(tile(id).querySelector('.dc-text-file-note')?.textContent, 'plan.md has changed since it was added to this text box.', `shown ${time + 1}`);
      show(id, 'Text');
      await until(() => tile(id).querySelector('.dc-text-tile'));
    }
  });

  it("a text tile's right-click, in the menus' one order: Create ▸, Edit Text…, Open Files…, then View ▸ -- Maximise / Restore, Move to Sheet, Remove from Page", async () => {
    // the menus (docs/DATACUBE_UI_REVIEW_2026_10_24.md): a text box had no right-click; its own window first, then how
    // it is shown and where, as a chart's and a grid's
    const told: PageCommand[] = [];
    const until = async (shown: () => unknown): Promise<void> => {
      for (let i = 0; i < 100 && !shown(); i++) await new Promise((r) => setTimeout(r, 5));
    };
    // a host with a picker: Create ▸ Data Source… on it (its grid beside the text box: the 2026-10-25 audit)
    newPage({ confirm: () => true, onPageCommand: (c) => told.push(c), openSource: async () => over('orders.csv') });
    const a = page.addGrid(over('trades.csv'));
    await settle();
    gridMenuItem(a, 'Text Box').click();
    await settle();
    const id = 'text-1';
    await until(() => tile(id).querySelector('textarea'));
    const menu = (): { top: string[]; view: string[]; create: string[]; entry: (text: string) => HTMLElement } => {
      dom.window.document.dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: 'Escape' }));
      const opened = tile(id).querySelector('.dc-text-host')!.dispatchEvent(
        new dom.window.MouseEvent('contextmenu', { bubbles: true, cancelable: true }));
      assert.equal(opened, false, 'the menu is the tile\'s, not the browser\'s');
      const root = dom.window.document.querySelector('.dc-menu')!;
      const under = (parent: string): string[] => {
        const item = [...root.querySelectorAll<HTMLElement>(':scope > .dc-menu-item')].find((i) => label(i) === parent)!;
        return [...item.querySelectorAll<HTMLElement>(':scope > .dc-submenu > .dc-menu-item')].map(label);
      };
      return {
        top: [...root.querySelectorAll<HTMLElement>(':scope > .dc-menu-item')].map(label),
        view: under('View'),
        create: under('Create'),
        entry: (text) => [...root.querySelectorAll<HTMLElement>('.dc-menu-item')].find((i) => label(i) === text)!,
      };
    };
    // while its words are edited, the browser's own menu (copy, paste) in its editor
    assert.equal(tile(id).querySelector('.dc-text-host')!.dispatchEvent(
      new dom.window.MouseEvent('contextmenu', { bubbles: true, cancelable: true })), true, 'the editor keeps the browser\'s menu');
    tile(id).querySelector('textarea')!.dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: 'Escape', bubbles: true }));
    await until(() => tile(id).querySelector('.dc-text-tile'));
    const first = menu();
    assert.deepEqual(first.top, ['Create', 'Edit Text\u2026', 'Open Files\u2026', 'View']);
    assert.deepEqual(first.view, ['Maximise', 'Move to Sheet', 'Remove from Page']);
    // Create ▸ in the grid's own words; what needs a grid is off, and says why
    assert.deepEqual(first.create, ['Data Source\u2026', 'Copy of This Grid', 'Visualization', 'Timeseries', 'Text Box']);
    for (const off of ['Copy of This Grid', 'Visualization', 'Timeseries']) {
      assert.equal(first.entry(off).getAttribute('aria-disabled'), 'true', off);
      assert.equal(first.entry(off).title, 'A text box reads no grid');
    }
    first.entry('Text Box').click();
    await settle();
    assert.deepEqual(told.at(-1)?.kind, 'addText', 'Create ▸ Text Box: another text box beside it');
    await until(() => tile('text-2')?.querySelector('textarea'));
    tile('text-2').querySelector('textarea')!.dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: 'Escape', bubbles: true }));
    await until(() => tile('text-2').querySelector('.dc-text-tile'));
    first.entry('Edit Text\u2026').click();
    await until(() => tile(id).querySelector('textarea'));
    assert.ok(tile(id).querySelector('textarea'), 'Edit Text\u2026 opens its editor');
    tile(id).querySelector('textarea')!.dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: 'Escape', bubbles: true }));
    await until(() => tile(id).querySelector('.dc-text-tile'));
    // Create ▸ Data Source…: the host's picker, its grid told beside the text box (the 2026-10-25 audit: placed as one
    // beside nothing)
    told.length = 0;
    menu().entry('Data Source\u2026').click();
    await settle();
    const added = told.find((c) => c.kind === 'addGrid');
    assert.equal(added?.kind === 'addGrid' ? added.near : undefined, id, 'beside the text box');
    // a locked page: its Remove from Page offered off, as its x is hidden (the 2026-10-25 audit)
    page.setLayoutEditing(false);
    const locked = menu().entry('Remove from Page');
    assert.deepEqual([locked.getAttribute('aria-disabled'), locked.title], ['true', 'Edit Layout first']);
    page.setLayoutEditing(true);
    // Maximise fills the page with it, and the same entry then says Restore
    menu().entry('Maximise').click();
    assert.ok(host.querySelector('.dc-bands-maximised'), 'the page filled with it');
    assert.deepEqual(menu().view, ['Restore', 'Move to Sheet', 'Remove from Page']);
    menu().entry('Restore').click();
    assert.equal(host.querySelector('.dc-bands-maximised'), null, 'back to the page');
    told.length = 0;
    menu().entry('Remove from Page').click();
    await settle();
    assert.deepEqual(told.map((c) => c.kind), ['removeTile']);
    assert.equal(tile(id), null, 'off the page');
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

  it("restored with the ids its keeper has held: none is given out again (B12, the S4 audit's 5)", async () => {
    newPage();
    page.addGrid(over('trades.csv'));
    await settle();
    const back = readPage(pageToJson(page.document('Q3')!));
    page.dispose();
    const told: PageCommand[] = [];
    newPage({ onPageCommand: (c) => told.push(c) });
    // the page as its keeper has it, sheet-9 among the ids it has held (made and taken off by its other editor)
    page.restore(back, new Map(back.cubes.map((c) => [c.id, over(c.cube.source.name)])), ['sheet-9']);
    await settle();
    plus(host);
    await settle();
    assert.deepEqual(told.flatMap((c) => (c.kind === 'addSheet' ? [c.id] : [])), ['sheet-10']);
  });

  it("held while its channel converges (D19, the audit's 5): a gesture does nothing, neither applied nor told; released, it works again", async () => {
    const told: PageCommand[] = [];
    newPage({ onPageCommand: (c) => told.push(c) });
    page.addGrid(over('trades.csv'));
    await settle();
    const sheets = (): number => page.document('Q3')!.sheets.length;
    page.hold();
    assert.equal(page.held, true);
    plus(host);
    await settle();
    assert.equal(sheets(), 1, 'nothing applied');
    assert.deepEqual(told.filter((c) => c.kind === 'addSheet'), [], 'nothing told');
    page.undo();
    await settle();
    assert.equal(page.document('Q3')!.views.length, 1, 'nothing moves');
    page.release();
    assert.equal(page.held, false);
    plus(host);
    await settle();
    assert.equal(sheets(), 2, 'released: applied');
    assert.equal(told.filter((c) => c.kind === 'addSheet').length, 1, 'and told');
  });

  it("the +'s Flow adds a flow sheet, shown, told as the menu's Create ▸ Flow tells it (2026-10-26)", async () => {
    const told: PageCommand[] = [];
    newPage({ onPageCommand: (c) => told.push(c) });
    page.addGrid(over('trades.csv'));
    await settle();
    plus(host, 'Flow');
    await settle();
    const doc = page.document('Q3')!;
    assert.deepEqual(doc.sheets.map((s) => s.layout.kind), ['bands', 'flow']);
    assert.deepEqual(told.map((c) => c.kind).filter((k) => k !== 'addGrid'), ['addFlow'], 'told as one addFlow');
    assert.equal(page.shownSheet, doc.sheets[1]!.id, 'the new flow is the sheet shown');
  });

  it('a frozen chart whose grid is removed keeps reading that grid, kept off the board; saved, and reopened detached', async () => {
    newPage();
    const a = page.addGrid(over('trades.csv'));
    const b = page.addGrid(over('orders.csv'), { near: a });
    await settle();
    const chart = await chartOf(b);
    await pin(chart);
    tile(b).querySelector<HTMLButtonElement>('.dc-tile-remove')!.click();
    await settle();
    assert.equal(tile(chart).querySelector('.dc-tile-state')!.textContent, 'Detached');
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
    assert.equal(tile(chart).querySelector('.dc-tile-state')!.textContent, 'Detached');
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
    entry(pageMenu(), 'Settings…')!.click();
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
    gridMenuItem(a, 'Copy of This Grid').click();
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

  it("a detached chart's Show Grid makes a grid of its own grouping, by the host's maker, which it follows and the page saves", async () => {
    newPage();
    const a = page.addGrid(over('trades.csv'));
    const b = page.addGrid(over('orders.csv'), { near: a });
    await settle();
    const chart = await chartOf(b);
    await pin(chart);
    tile(b).querySelector<HTMLButtonElement>('.dc-tile-remove')!.click();
    await settle();
    (tile(chart).querySelector('.dc-chart-tile') as HTMLElement)
      .dispatchEvent(new dom.window.MouseEvent('contextmenu', { bubbles: true, cancelable: true }));
    [...dom.window.document.querySelectorAll<HTMLElement>('.dc-menu .dc-menu-item')].find((i) => label(i) === 'Show Grid')!.click();
    await settle();
    const doc = page.document('Q3');
    assert.ok(doc, 'the page can be saved');
    const own = doc.views.find((v) => v.kind === 'grid' && v.id !== a)?.id;
    assert.ok(own, 'a grid of its own on the page');
    assert.deepEqual(made.at(-1), [own, true], 'made by the maker of the grid it copies');
    assert.ok(page.grid(own), 'the page knows it');
    assert.deepEqual(doc.cubes.map((c) => c.id), [a, own], 'the grid kept for the detached chart is gone');
    const view = doc.views.find((v) => v.id === chart);
    assert.equal(view?.cube, own, 'the chart reads it');
    assert.equal(view?.kind === 'chart' ? view.spec.frozen : 'not a chart', undefined, 'and follows it');
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

  // the user, 2026-10-23: the grid's menu button gone, its right-click is its menu -- and an iPad fires no right-click
  it('a long press on a touch screen opens the right-click menu: a grid cell\'s, a sheet tab\'s (ui/long-press.ts)', async () => {
    newPage();
    const id = page.addGrid(over('trades.csv'));
    await settle();
    const hold = async (el: Element): Promise<HTMLElement[]> => {
      dom.window.document.dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: 'Escape' }));
      el.dispatchEvent(new dom.window.PointerEvent('pointerdown', {
        bubbles: true, cancelable: true, pointerType: 'touch', pointerId: 7, isPrimary: true, clientX: 40, clientY: 60,
      }));
      await new Promise((r) => setTimeout(r, HOLD_MS + 50));
      return [...dom.window.document.querySelectorAll<HTMLElement>('.dc-menu [role^="menuitem"]')];
    };
    const onCell = await hold(tile(id).querySelector('.dc-row .dc-cell') as HTMLElement);
    assert.ok(['Edit', 'Create', 'View', 'Move to Sheet'].every((l) => onCell.some((i) => label(i) === l)), onCell.map(label).join(' | '));
    const onTab = await hold(bar().querySelector('.dc-sheet-tab') as HTMLElement);
    assert.ok(onTab.some((i) => /^Rename/.test(label(i))), `the tab's own menu: ${onTab.map(label).join(' | ')}`);
  });

  it('a second sheet: +, shown and empty; a grid moved onto it from its menu; its chart, on the first, follows it', async () => {
    newPage();
    const a = page.addGrid(over('trades.csv'));
    await settle();
    const chart = await chartOf(a);
    const first = page.shownSheet;
    plus(bar());
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
    assert.deepEqual(page.sheets.map((id) => bandsOf(page.views().sheets.find((s) => s.id === id)!.layout).bands.length), [1, 1]);
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
    plus(bar());
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
    assert.deepEqual(doc.sheets.map((s) => layoutTiles(s.layout)), [[chart], [a]]);
    const back = readPage(pageToJson(doc));
    page.dispose();
    newPage();
    page.restore(back, new Map(back.cubes.map((c) => [c.id, over(c.cube.source.name, c.cube)])));
    await settle();
    assert.deepEqual(tabs(), ['Charts', 'trades.csv']);
    assert.equal(page.shownSheet, s2, 'it reopens on its first sheet');
    assert.deepEqual(page.views(), { views: doc.views, sheets: doc.sheets });
  });

  it('on a locked page offers Move to Sheet and Remove from Page disabled, in a grid\'s menu and a chart\'s, and moves nothing', async () => {
    newPage();
    const a = page.addGrid(over('trades.csv'));
    await settle();
    const chart = await chartOf(a);
    plus(bar());
    clickTab(tabs()[0]!);
    page.setLayoutEditing(false);
    await settle();
    const move = gridMenuItem(a, 'Move to Sheet');
    assert.equal(move.getAttribute('aria-disabled'), 'true', 'the grid\'s');
    // Remove from Page too, as a tile's x is hidden on a locked page (the 2026-10-25 audit)
    const remove = gridMenuItem(a, 'Remove from Page');
    assert.deepEqual([remove.getAttribute('aria-disabled'), remove.title], ['true', 'Edit Layout first'], "the grid's Remove");
    (tile(chart).querySelector('.dc-chart-tile') as HTMLElement)
      .dispatchEvent(new dom.window.MouseEvent('contextmenu', { bubbles: true, cancelable: true }));
    const chartMove = [...dom.window.document.querySelectorAll<HTMLElement>('.dc-menu [role^="menuitem"]')].find((i) => label(i) === 'Move to Sheet');
    assert.equal(chartMove?.getAttribute('aria-disabled'), 'true', 'the chart\'s');
    const chartRemove = [...dom.window.document.querySelectorAll<HTMLElement>('.dc-menu [role^="menuitem"]')].find((i) => label(i) === 'Remove from Page');
    assert.equal(chartRemove?.getAttribute('aria-disabled'), 'true', "the chart's Remove");
    // and its entries, reached anyway, move nothing
    [...dom.window.document.querySelectorAll<HTMLElement>('.dc-menu [role^="menuitem"]')].find((i) => label(i) === 'New Sheet')?.click();
    await settle();
    assert.equal(page.sheets.length, 2);
    // the grid and its chart under it (two bands since 2026-10-23: a chart goes under its grid) stay where they were
    assert.deepEqual(page.views().sheets.map((s) => bandsOf(s.layout).bands.length), [2, 0]);
  });

  it('a grid behind a chart in a stack still exports its table, beside the charts of its sheet', async () => {
    // a maker that keeps each grid's export, as its CubeApp would call it
    const exports = new Map<string, () => ExportPage | undefined>();
    const keep = (name: string, saved?: CubeDocument): GridMaker => (gridHost, spawned, start) => {
      if (spawned.id !== undefined && spawned.exportPage) exports.set(spawned.id, spawned.exportPage);
      return over(name, saved)(gridHost, spawned, start);
    };
    newPage();
    const a = page.addGrid(keep('trades.csv'));
    await settle();
    const chart = await chartOf(a);
    const doc = page.document('Q3')!;
    // the chart in front of its grid, in one place
    const stacked = { ...doc, sheets: [{ ...doc.sheets[0]!, layout: { kind: 'bands' as const, fit: true, bands: [{ height: 1, node: { stack: [chart, a] } }] } }] };
    page.dispose();
    newPage();
    exports.clear();
    const back = readPage(pageToJson(stacked));
    page.restore(back, new Map(back.cubes.map((c) => [c.id, keep(c.cube.source.name, c.cube)])));
    await settle();
    const exported = exports.get(a)?.();
    assert.ok(exported, 'a page export, with its chart');
    assert.ok(exported.tiles.some((t) => t.id === a && t.kind === 'grid'), 'its table in it');
  });

  it('deletes a sheet with its tiles once asked, never its last; the readout follows the sheet shown', async () => {
    const asked: string[] = [];
    let answer = false;
    const readout = dom.window.document.createElement('span');
    newPage({ confirm: (q) => { asked.push(q); return answer; }, hostStatus: (slot) => slot.append(readout) });
    const a = page.addGrid(over('trades.csv'));
    await settle();
    plus(bar());
    const b = page.addGrid(over('orders.csv'));
    await settle();
    assert.ok(tile(b).contains(readout), 'in the first grid of the sheet shown');
    clickTab('trades.csv');
    await settle();
    assert.ok(tile(a).contains(readout), 'moved with the sheet shown');
    const del = (labelText: string): void => {
      tabOf(labelText).dispatchEvent(new dom.window.MouseEvent('contextmenu', { bubbles: true, cancelable: true }));
      [...dom.window.document.querySelectorAll<HTMLElement>('.dc-menu [role^="menuitem"]')].find((i) => label(i) === 'Delete Sheet')!.click();
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
    const last = [...dom.window.document.querySelectorAll<HTMLElement>('.dc-menu [role^="menuitem"]')].find((i) => label(i) === 'Delete Sheet')!;
    assert.equal(last.getAttribute('aria-disabled'), 'true', 'the last sheet stays');
  });

  it('started with a grid its host made (the app\'s start\'s sample), it has no step to undo; an edit after is one', async () => {
    newPage({ confirm: () => true });
    page.addGrid(over('trades.csv'));
    await settle();
    assert.equal(page.undoLabel, 'Add Grid', 'a grid a person adds: a step');
    page.started();
    assert.equal(page.undoLabel, undefined, 'a fresh page offered "Undo Add Grid", which left it blank (2026-10-23)');
    plus(host);
    await settle();
    assert.equal(page.undoLabel, 'Add Sheet');
  });

  it("renamed by its editor, it tells renamePage -- no step of its history; a name it has tells nothing (P1, P2)", async () => {
    const told: PageCommand[] = [];
    newPage({ confirm: () => true, onPageCommand: (c) => told.push(c) });
    page.addGrid(over('trades.csv'));
    await settle();
    told.length = 0;
    const step = page.undoLabel;
    page.rename('Q4 review');
    page.rename('Q4 review');
    assert.deepEqual(told, [{ kind: 'renamePage', name: 'Q4 review' }]);
    assert.equal(page.title, 'Q4 review');
    assert.equal(page.undoLabel, step, "the host's to name: no step of its own to undo");
    // a host setting the name it follows (a page opened, another editor's) tells nothing
    page.setTitle('Q5');
    assert.equal(told.length, 1);
  });

  it("a model action's record applied as its commands and told -- no step of its history, kept through an undo (P15, D5)", async () => {
    const told: PageCommand[] = [];
    newPage({ confirm: () => true, onPageCommand: (c) => told.push(c) });
    const a = page.addGrid(over('trades.csv'));
    await settle();
    plus(host);
    await settle();
    told.length = 0;
    const step = page.undoLabel;
    const record: PageCommand[] = [
      { kind: 'modelInto', project: 'datacube:q3', workspace: 'datacube', package: 'q3' },
      { kind: 'modelPin', revision: 'r1' },
      { kind: 'modelKeep', cube: a, tables: ['t_trades'], class: 'q3::domain::Trade' },
    ];
    page.applyModel(record);
    assert.deepEqual(told, record, 'told, as applied');
    assert.equal(page.undoLabel, step, 'no step of its own to undo');
    const made = { project: 'datacube:q3', workspace: 'datacube', package: 'q3', tables: { [a]: ['t_trades'] },
      classes: { [a]: 'q3::domain::Trade' }, revision: 'r1' };
    assert.deepEqual(page.model, made);
    assert.deepEqual(page.document('Q3')?.model, made, 'what the page saves and reports');
    assert.deepEqual(page.state.page.model, made, "and its state's");
    // undone past it: the sheet goes, what the model action wrote stays
    page.undo();
    await settle();
    assert.equal(page.sheets.length, 1);
    assert.deepEqual(page.model, made);
    assert.deepEqual(page.state.page.model, made);
    // a record that does not apply is refused in its words, nothing changed
    told.length = 0;
    page.applyModel([{ kind: 'modelInto', project: 'datacube:other', workspace: 'datacube', package: 'o' }]);
    assert.deepEqual([told, page.model], [[], made]);
  });

  it('tells each edit of its structure as its page command: applyPageCommand gives for them the page it saves', async () => {
    // (docs/DATACUBE_COMMANDS_PAGE_2026_10_18.md) the screen and the one function cannot drift before stage 2 makes the
    // function the screen's: what the screen told, applied to the page it started as, is the page it writes
    const told: PageCommand[] = [];
    newPage({ confirm: () => true, onPageCommand: (c) => told.push(c) });
    const a = page.addGrid(over('trades.csv'));
    await settle();
    const chart = await chartOf(a);
    const b = page.addGrid(over('orders.csv'));
    await settle();
    const start = page.document('Q3')!;
    told.length = 0;
    // a sheet added; the second grid moved onto it from its menu; the chart removed from its menu; the first sheet
    // deleted with its grid
    plus(bar());
    await settle();
    clickTab(tabs()[0]!);
    await settle();
    gridMenuItem(b, 'Sheet 2').click();
    await settle();
    const [first, second] = page.sheets;
    (tile(chart).querySelector('.dc-chart-tile') as HTMLElement)
      .dispatchEvent(new dom.window.MouseEvent('contextmenu', { bubbles: true, cancelable: true }));
    [...dom.window.document.querySelectorAll<HTMLElement>('.dc-menu [role^="menuitem"]')].find((i) => label(i) === 'Remove from Page')!.click();
    await settle();
    tabOf('trades.csv').dispatchEvent(new dom.window.MouseEvent('contextmenu', { bubbles: true, cancelable: true }));
    [...dom.window.document.querySelectorAll<HTMLElement>('.dc-menu [role^="menuitem"]')].find((i) => label(i) === 'Delete Sheet')!.click();
    await settle();
    assert.deepEqual(told.map((c) => c.kind), ['addSheet', 'moveTile', 'removeTile', 'removeSheet']);
    assert.deepEqual(told[0], { kind: 'addSheet', id: second, name: null });
    assert.deepEqual(told[3], { kind: 'removeSheet', sheet: first });
    const replayed = applyPageCommands({ page: start, held: idsOf(start) }, told).draft.page;
    assert.deepEqual({ views: replayed.views, sheets: replayed.sheets }, page.views());
    assert.deepEqual(replayed.cubes.map((c) => c.id), page.cubes, 'the grid that went took its cube');
  });

  it('follows another editor\'s page commands: applied to the page as it is, a grid added by its maker and none made again, its history kept (W13); refused, nothing changed', async () => {
    // (docs/DATACUBE_LIVE_COMMANDS_2026_10_18.md) Python's change, as the commands it was
    newPage({ confirm: () => true });
    const a = page.addGrid(over('trades.csv'));
    await settle();
    plus(bar());
    await settle();
    assert.equal(page.undoLabel, 'Add Sheet', "the screen's own edit: a step");
    const second = page.sheets[1]!;
    const commands: PageCommand[] = [
      { kind: 'renameSheet', sheet: second, name: 'From Python' },
      { kind: 'addGrid', id: 'grid-7', document: page.document('Q3')!.cubes[0]!.cube, sheet: second, near: null, title: 'Orders', columns: null },
      { kind: 'renameTile', tile: a, title: 'Trades' },
      // (the phase 3 audit's 1) its name too: the host reads it back
      { kind: 'renamePage', name: 'Q4' },
    ];
    const expected = applyPageCommands(page.state, commands).draft.page;
    const madeBefore = made.length;
    await theirs(page, commands, new Map([['grid-7', over('orders.csv')]]));
    await settle();
    assert.deepEqual(page.views(), { views: expected.views, sheets: expected.sheets });
    assert.deepEqual(made.slice(madeBefore), [['grid-7', false]], 'one grid made, the one added: none made again');
    assert.equal(tile('grid-7').querySelector('.dc-tile-title')?.textContent, 'Orders');
    // 2026-10-24 (D19 W13): a step is commands applied to the page as it is now (page-state.ts), so the history survives
    assert.equal(page.undoLabel, 'Add Sheet', "its history survives the other editor's change");
    assert.equal(page.title, 'Q4', 'renamed as Python renamed it');
    assert.deepEqual(page.state.page, page.document('Q4'), 'the state is the page on screen');
    // a command naming a tile closed here: refused in its words, nothing changed
    const before = page.views();
    await assert.rejects(theirs(page, [{ kind: 'removeTile', tile: 'grid-9' }], new Map()), CubeRefusal);
    assert.deepEqual(page.views(), before);
  });

  // the review's follow-up (A, suspected; docs/TIMESERIES_REVIEW_2026_10_25.md §9): another editor's change bringing a
  // cube this tab never held, read by a timeseries tile alone (its grid removed there) -- refused here, "maker is gone":
  // the converge opened another editor's GRIDS only
  it("another editor's timeseries over a cube this tab never held, its grid removed there: the cube opened from the page there", async () => {
    newPage();
    const a = page.addGrid(over('trades.csv'));
    await settle();
    const sheet = page.sheets[0]!;
    const spec: TimeseriesSpec = { sources: [{ alias: 'o', cube: 'grid-7', date: 'region' }], series: [{ id: 'A', expr: { kind: 'column', column: 'notional' } }] };
    const commands: PageCommand[] = [
      { kind: 'addGrid', id: 'grid-7', document: page.document('Q3')!.cubes[0]!.cube, sheet, near: a, title: 'Orders', columns: null },
      { kind: 'addTimeseries', sheet, near: 'grid-7', spec, title: 'Orders over time', columns: null },
      { kind: 'removeTile', tile: 'grid-7' },
    ];
    const expected = applyPageCommands(page.state, commands).draft.page;
    assert.ok(expected.cubes.some((c) => c.id === 'grid-7') && !expected.views.some((v) => v.id === 'grid-7'), 'there: kept for the tile, no grid');
    await theirs(page, commands, new Map([['grid-7', over('orders.csv')]]));
    await settle();
    assert.deepEqual(page.views(), { views: expected.views, sheets: expected.sheets });
    assert.ok(page.cubes.includes('grid-7'), 'kept off the board here too, to run the tile\'s queries');
    assert.deepEqual(page.document('Q3')!.cubes.map((c) => c.id), expected.cubes.map((c) => c.id), 'the page holds what the page there holds');
  });

  // the review's follow-up (A, suspected): a cube kept for a timeseries here, its document replaced by another editor's
  // setTimeseries (its `kept`) -- the converge put the board's grids at their cubes there, never one kept off it
  it("another editor's setTimeseries replacing a cube kept here for the tile: the kept grid put at its cube there", async () => {
    newPage();
    const a = page.addGrid(over('trades.csv'));
    await settle();
    const sheet = page.sheets[0]!;
    const spec: TimeseriesSpec = { sources: [{ alias: 'o', cube: a, date: 'region' }], series: [{ id: 'A', expr: { kind: 'column', column: 'notional' } }] };
    await theirs(page, [{ kind: 'addTimeseries', sheet, near: a, spec, title: 'Trades over time', columns: null }]);
    await settle();
    const ts = page.views().views.find((v) => v.kind === 'timeseries')!.id;
    // removed here while the tile reads it: kept off the board for it
    tile(a).querySelector<HTMLButtonElement>('.dc-tile-remove')!.click();
    await settle();
    assert.ok(page.cubes.includes(a) && !page.grids.includes(a), 'kept off the board');
    const was = page.document('Q3')!.cubes.find((c) => c.id === a)!.cube;
    const EMEA = { kind: 'condition', column: 'region', operator: 'equal', value: 'EMEA' } as const;
    const document: CubeDocument = { ...was, query: { ...was.query, filter: EMEA } };
    await theirs(page, [{ kind: 'setTimeseries', tile: ts, spec, kept: [{ cube: a, document, copy: null }] }]);
    await settle();
    assert.deepEqual(page.document('Q3')!.cubes.find((c) => c.id === a)!.cube.query.filter, EMEA, 'the page holds the cube there');
    assert.deepEqual(page.grid(a)?.snapshot.filter, EMEA, 'and the grid kept for the tile reads it');
  });

  it("snapped or live is the page's (§18.8): another editor's snap and live commands followed by its grid, the screen's own Snap in the page's document, nothing of the other editor's let go", async () => {
    // a grid read through its host's runner, its host keeping copies (Python's page): what it was asked to let go
    const released: string[] = [];
    const runner = new PlanThenRun(new StubPlanner(), engine) as unknown as QueryRunner;
    const hosted: GridMaker = (gridHost, spawned) => new CubeApp(gridHost, SNAPSHOT, {
      runner, cubeSource: file('trades.csv'), sourceLabel: 'trades.csv', compact: true,
      hostSnap: {
        store: 'Python',
        snap: async () => ({ rowCount: 3, table: 'snap_2', source: SNAPSHOT.source.query, runner }),
        release: async (table) => { released.push(table); },
        held: async (table) => ({ table, source: SNAPSHOT.source.query, runner }),
      },
      ...(page ? { windowHost: page.root } : {}), ...spawned,
    });
    newPage();
    const a = page.addGrid(hosted);
    await settle();
    // a grid alone on its sheet has its state in its sheet's tab
    const pill = (): string | null | undefined => host.querySelector('.dc-tile-state')?.textContent;
    const copy = gridCopyOf({ table: 'snap_1', rowCount: 9, takenAt: new Date('2026-10-20T14:02:00Z'), label: 'Snap 14:02' });
    await theirs(page, [{ kind: 'snap', grid: a, copy }], new Map());
    await settle();
    assert.deepEqual(page.grid(a)?.copy, copy, 'its grid reads the copy, as the page wrote it down');
    assert.deepEqual(page.state.page.copies, { [a]: copy });
    assert.deepEqual(page.document('Q3')?.copies, { [a]: copy }, 'what the page reports to its other editor');
    assert.equal(pill(), 'Snapped');
    await theirs(page, [{ kind: 'live', grid: a }], new Map());
    await settle();
    assert.equal(page.grid(a)?.copy, undefined);
    assert.equal(page.state.page.copies, undefined);
    assert.equal(pill(), 'Live');
    // the screen's own Snap: the page's document says the copy its host made
    await movePlane('Snap');
    const stateCopies = (): PageDocument['copies'] => page.state.page.copies;
    assert.equal(stateCopies()?.[a]?.table, 'snap_2', "the screen's Snap is in the page's state");
    // gone live by the other editor, which let the copy go (it read the page first): nothing let go here
    await theirs(page, [{ kind: 'live', grid: a }], new Map());
    await settle();
    assert.equal(pill(), 'Live');
    assert.deepEqual(released, [], "the other editor's live command lets nothing go here: the page no longer reads the copy, which its host lets go");
    // a snap command for a grid this page cannot read a host's copy for: refused, so the page opens again as it is there
    await assert.rejects(theirs(page, [{ kind: 'snap', grid: 'grid-9', copy }], new Map()));
  });

  /** A grid read through its host's runner, its host keeping copies -- `slow`: another editor's copy taken up only when
   *  `taken()` lets it -- over `file`, or (none) a source the page cannot write down. */
  const hostedGrid = (o: { slow?: { taken: () => Promise<void> }; file?: string }): GridMaker => {
    const runner = new PlanThenRun(new StubPlanner(), engine) as unknown as QueryRunner;
    return (gridHost, spawned) => new CubeApp(gridHost, SNAPSHOT, {
      runner, ...(o.file !== undefined ? { cubeSource: file(o.file) } : {}), sourceLabel: o.file ?? 'rows', compact: true,
      hostSnap: {
        store: 'Python',
        snap: async () => ({ rowCount: 3, table: 'snap_2', source: SNAPSHOT.source.query, runner }),
        release: async () => {},
        held: async (table) => {
          await o.slow?.taken();
          return { table, source: SNAPSHOT.source.query, runner };
        },
      },
      ...(page ? { windowHost: page.root } : {}), ...spawned,
    });
  };

  it("F10: an edit made while another editor's snap is taken up does nothing; the next is a step on top of the page the editor made", async () => {
    // (2026-10-24, the middle version: another editor's change reaches the page by the converge, which holds the page
    // -- the D19 audit's 5 -- so a gesture meanwhile lands on no board that is being set)
    let release: () => void = () => {};
    newPage();
    const a = page.addGrid(hostedGrid({ file: 'trades.csv', slow: { taken: () => new Promise<void>((r) => { release = r; }) } }));
    await settle();
    const copy = gridCopyOf({ table: 'snap_1', rowCount: 9, takenAt: new Date('2026-10-20T14:02:00Z'), label: 'Snap 14:02' });
    const following = theirs(page, [{ kind: 'snap', grid: a, copy }], new Map());
    await settle();
    // the person adds a sheet while the converge waits for the copy: held, nothing
    plus(host);
    await settle();
    release();
    await following;
    await settle();
    assert.equal(page.sheets.length, 1, 'nothing added while the page was held');
    assert.deepEqual(page.grid(a)?.copy, copy, "the editor's snap: its grid reads the copy");
    plus(host);
    await settle();
    assert.deepEqual(page.state.page.sheets.map((s) => s.id), page.sheets, 'the state holds the sheet added after');
    assert.ok(page.state.held.includes(page.sheets[1]!), 'and its id, held');
    assert.equal(page.undoLabel, 'Add Sheet', 'a step of its own, on top of the editor\'s page');
    // the next edit -- the first sheet renamed in its tab -- leaves it where it is
    host.querySelectorAll<HTMLElement>('.dc-sheet-tab')[0]!.dispatchEvent(new dom.window.MouseEvent('dblclick', { bubbles: true }));
    const field = host.querySelector<HTMLInputElement>('.dc-sheet-tab input')!;
    field.value = 'First';
    field.dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: 'Enter' }));
    await settle();
    assert.equal(page.sheets.length, 2, "the sheet added after is the page's");
  });

  it("a snap pending here when the page converges keeps its copy: the keeper's page, which has none yet, never takes it live (the middle version's audit's 5)", async () => {
    newPage();
    const a = page.addGrid(hostedGrid({ file: 'trades.csv' }));
    await settle();
    const keepers = page.state;
    // the person snaps the grid; the write carrying it is still to be answered when the converge comes
    const copy = gridCopyOf({ table: 'snap_7', rowCount: 3, takenAt: new Date('2026-10-20T14:02:00Z'), label: 'Snap 14:02' });
    await page.grid(a)!.followSnap!(copy);
    await settle();
    assert.deepEqual(page.grid(a)?.copy, copy);
    await convergeOn({ target: keepers.page, held: keepers.held, base: keepers.page, refused: [], pending: [{ page: { kind: 'snap', grid: a, copy } }] }, {
      page, grid: (id) => page.grid(id) as unknown as ConvergeGrid | undefined, pageNow: () => page.document(page.title, undefined, true),
      open: async () => undefined,
    });
    assert.deepEqual(page.grid(a)?.copy, copy, 'still reads the copy the pending snap made');
  });

  it("another editor's grid opened again over its source as it is now (rebuilt): no state a grid's commands reach, the converge refuses it in words (the audit's 6)", async () => {
    newPage();
    const a = page.addGrid(hostedGrid({ file: 'trades.csv' }));
    await settle();
    const source = page.state.page.cubes.find((c) => c.id === a)!.cube.source as FileSource & { readonly columns: readonly { readonly name: string; readonly type: string }[] };
    const now = { ...source, columns: [...source.columns, { name: 'extra', type: 'String' }] };
    await assert.rejects(theirs(page, [{ kind: 'rebuilt', cube: a, source: now }]), /was opened again over its source as it is now/);
  });

  it("F13: a grid whose cube the page cannot hold snaps and goes live with no command of the page's, nothing refused", async () => {
    const refused: string[] = [];
    const told: PageCommand[] = [];
    newPage({ refuse: (text) => refused.push(text), onPageCommand: (c) => told.push(c) });
    page.addGrid(hostedGrid({}));
    await settle();
    const pill = (): HTMLElement => host.querySelector('.dc-tile-state') as HTMLElement;
    await movePlane('Snap');
    assert.equal(pill().textContent, 'Snapped');
    await movePlane('Go Live');
    assert.equal(pill().textContent, 'Live');
    assert.deepEqual(refused, [], 'Go Live refused nothing');
    assert.deepEqual(told.filter((c) => c.kind === 'snap' || c.kind === 'live'), [], "no copy of the page's: it holds no cube for the grid");
  });

  it("another editor's grid commands: applied as one change, no step of the grid's own history, which starts again from them", async () => {
    // (the phase 3 audit's 7) an undo past Python's change would take it back unseen
    newPage();
    const a = page.addGrid(over('trades.csv'));
    await settle();
    const grid = page.grid(a) as unknown as CubeApp;
    // an edit of the grid's own (a change as its screen makes one): a step
    await grid.edit([{ kind: 'group', rows: ['desk'] }], 'group');
    await settle();
    assert.equal(grid.canUndo, true, "the grid's own edit: a step");
    assert.equal(await grid.applyGridCommands([{ kind: 'group', rows: ['region'] }], 'Python'), null);
    await settle();
    assert.deepEqual(grid.snapshot.rows, ['region']);
    assert.equal(grid.canUndo, false, "Python's change: none, and none before it");
    assert.match(String(await grid.applyGridCommands([{ kind: 'group', rows: ['nope'] }], 'Python')), /nope/, 'refused in words');
  });

  it('its menu: Create > Data Source adds a grid through the host\'s picker; the host\'s entries; Export Page File… exports it', async () => {
    const files: [string, string][] = [];
    const picked: string[] = [];
    newPage({
      openSource: async () => over('picked.csv'),
      hostMenu: () => [{ id: 'host.save', label: 'Save', section: 'file' }],
      onHostMenu: (item) => picked.push(item.id ?? ''),
      download: (name, _mime, content) => files.push([name, content]),
    });
    const items = pageMenu();
    const newItem = entry(items, 'Create')!;
    (newItem.querySelector('[role^="menuitem"]') as HTMLElement).click();
    await settle();
    assert.equal(page.grids.length, 1);
    entry(pageMenu(), 'Save')!.click();
    assert.deepEqual(picked, ['host.save']);
    entry(pageMenu(), 'Export Page File…')!.click();
    assert.equal(files.length, 1);
    assert.equal(files[0]![0], 'Q3.page.json');
    assert.equal(readPage(files[0]![1]).cubes[0]!.cube.source.name, 'picked.csv');
  });
});

describe("the page's ≡, grouped by purpose (docs/DATACUBE_UI_REVIEW_2026_10_24.md §3; the user, 2026-10-25)", () => {
  /** The menu's top level as a person reads it down: each entry, a line (─) between its groups. */
  const topLevel = (): string[] => {
    pageMenu();
    const menu = dom.window.document.querySelector('.dc-menu')!;
    return [...menu.children].map((c) => (c.classList.contains('dc-menu-sep') ? '─' : label(c as HTMLElement)));
  };
  /** A submenu's entries, by its parent's words. */
  const under = (parent: string): string[] =>
    [...entry(pageMenu(), parent)!.querySelectorAll<HTMLElement>(':scope > .dc-submenu > [role^="menuitem"]')].map(label);
  const sitePage = (o: Partial<PageAppOptions> = {}): PageApp => newPage({
    openSource: async () => over('picked.csv'),
    onBlankPage: () => {},
    download: () => {},
    hostMenu: () => [
      { id: 'host.open', label: 'Open…', section: 'file' },
      { id: 'host.save', label: 'Save', section: 'file' },
      { id: 'host.saveAs', label: 'Save As…', section: 'file' },
      { id: 'host.share', label: 'Share…', section: 'file' },
      { id: 'host.query', label: 'Generated Pure & SQL…', section: 'view' },
      { id: 'host.association', label: 'Association…', section: 'create' },
    ],
    ...o,
  });

  it('what leaves the app, the history, the layout, Create ▸, then View ▸ with the app\'s Settings…', async () => {
    sitePage();
    page.addGrid(over('trades.csv'));
    await settle();
    assert.deepEqual(topLevel(), [
      'New Page', 'Open…', 'Save', 'Save As…', 'Share…', 'Export Page File…', '─',
      'Undo Add Grid', 'Redo', '─',
      'Done Editing Layout', 'Arrange…', '─',
      'Create', '─',
      'View',
    ]);
    // a Parameter (the flow mode's M8, 2026-10-26): the page's parameters bar, its Add form open
    assert.deepEqual(under('Create'), ['Data Source…', 'Sheet', 'Flow', 'Text Box', 'Parameter', 'Association…']);
    // the app's Settings… under View, beside its Keyboard Shortcuts… (the user, 2026-10-25): this browser's, not a grid's
    assert.deepEqual(under('View'), ['Hide the Bar', 'Generated Pure & SQL…', 'Keyboard Shortcuts…', 'Settings…']);
  });

  it('New Page asks the host for a new page; Export Page File… writes the page, one entry', async () => {
    let asked = 0;
    const files: string[] = [];
    sitePage({ onBlankPage: () => { asked += 1; }, download: (name) => files.push(name) });
    page.addGrid(over('trades.csv'));
    await settle();
    entry(pageMenu(), 'New Page')!.click();
    assert.equal(asked, 1);
    entry(pageMenu(), 'Export Page File…')!.click();
    assert.deepEqual(files, ['Q3.page.json']);
  });

  it('while the host is starting, New Page and Data Source… are greyed, saying so; started, they act (2026-10-26)', async () => {
    let starting: string | undefined = 'The page is still starting';
    let asked = 0;
    sitePage({ onBlankPage: () => { asked += 1; }, openSource: () => Promise.resolve(undefined), starting: () => starting });
    page.addGrid(over('trades.csv'));
    await settle();
    const blank = entry(pageMenu(), 'New Page')!;
    assert.equal(blank.getAttribute('aria-disabled'), 'true');
    assert.equal(blank.title, 'The page is still starting');
    blank.click();
    assert.equal(asked, 0, 'greyed: nothing asked of the host');
    const source = [...entry(pageMenu(), 'Create')!.querySelectorAll<HTMLElement>(':scope > .dc-submenu > [role^="menuitem"]')]
      .find((i) => label(i) === 'Data Source…');
    assert.equal(source?.getAttribute('aria-disabled'), 'true');
    starting = undefined;
    assert.equal(entry(pageMenu(), 'New Page')!.getAttribute('aria-disabled'), null);
    entry(pageMenu(), 'New Page')!.click();
    assert.equal(asked, 1);
  });

  it('Edit Layout says what it will do and flips; off, Arrange… and Sheet are greyed, saying why', async () => {
    sitePage();
    page.addGrid(over('trades.csv'));
    await settle();
    entry(pageMenu(), 'Done Editing Layout')!.click();
    const items = pageMenu();
    assert.ok(entry(items, 'Edit Layout'), 'flipped');
    assert.equal(entry(items, 'Done Editing Layout'), undefined);
    for (const off of ['Arrange…', 'Sheet']) {
      assert.equal(entry(items, off)!.getAttribute('aria-disabled'), 'true', off);
      assert.equal(entry(items, off)!.title, 'Edit Layout first', off);
    }
    entry(items, 'Edit Layout')!.click();
    assert.equal(entry(pageMenu(), 'Arrange…')!.getAttribute('aria-disabled'), null);
  });

  it('Create ▸ Text Box puts a text tile on the page, an empty one too', async () => {
    sitePage();
    entry(pageMenu(), 'Text Box')!.click();
    await settle();
    assert.equal(page.views().views.filter((v) => v.kind === 'text').length, 1);
    assert.equal(host.querySelector<HTMLElement>('.dc-page-empty')!.hidden, true);
  });

  it('View ▸ Hide the Bar folds it; Keyboard Shortcuts… opens their window', async () => {
    sitePage();
    page.addGrid(over('trades.csv'));
    await settle();
    entry(pageMenu(), 'Keyboard Shortcuts…')!.click();
    await settle();
    assert.ok(dom.window.document.querySelector('[data-window="Keyboard Shortcuts"]:not([hidden])'), 'the shortcuts window');
    entry(pageMenu(), 'Hide the Bar')!.click();
    assert.equal(page.barFolded, true);
  });

  it('on an empty page, Settings… and Keyboard Shortcuts… are greyed, saying why', () => {
    sitePage();
    const items = pageMenu();
    for (const off of ['Settings…', 'Keyboard Shortcuts…', 'Arrange…']) {
      assert.equal(entry(items, off)!.getAttribute('aria-disabled'), 'true', off);
    }
    assert.equal(entry(items, 'Settings…')!.title, 'Open a data source first');
    assert.equal(entry(items, 'Arrange…')!.title, 'Nothing on this page to arrange');
  });
});

describe('one tile header for every kind: its state left of its title, the board\'s buttons at its right (the user, 2026-10-25)', () => {
  const until = async (shown: () => unknown): Promise<void> => {
    for (let i = 0; i < 100 && !shown(); i++) await new Promise((r) => setTimeout(r, 5));
  };
  /** A tile's header, child by child: its state's `data-state`, then the rest by class. */
  const headOf = (id: string): string[] => [...tile(id).querySelector('.dc-tile-head')!.children]
    .map((c) => (c.classList.contains('dc-tile-state') ? `state:${(c as HTMLElement).dataset['state']}` : c.className));
  /** What a grid's state offers, its menu opened and shut. */
  const offered = (): string[] => {
    dom.window.document.dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: 'Escape' }));
    (host.querySelector('.dc-tile-state:is([data-state="live"], [data-state="snapped"])') as HTMLElement).click();
    const labels = [...dom.window.document.querySelectorAll<HTMLElement>('.dc-menu [role^="menuitem"]')].map(label);
    dom.window.document.dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: 'Escape' }));
    return labels;
  };

  it('a grid, a chart and a text box: state, title, the board\'s buttons -- in that order', async () => {
    newPage({ confirm: () => true });
    const a = page.addGrid(over('trades.csv'));
    const b = page.addGrid(over('orders.csv'), { near: a });
    await settle();
    const chart = await chartOf(b);
    entry(pageMenu(), 'Text Box')!.click();
    await settle();
    const text = page.views().views.find((v) => v.kind === 'text')!.id;
    for (const [id, state] of [[a, 'live'], [chart, 'dynamic'], [text, 'editing']] as const) {
      const head = headOf(id);
      assert.equal(head[0], `state:${state}`, `${id}: its state first`);
      assert.equal(head[1], 'dc-tile-title', `${id}: then its title`);
      assert.equal(head.at(-1), 'dc-tile-actions', `${id}: the board's buttons last`);
    }
    // the buttons say no state: no pill among them
    assert.equal(host.querySelector('.dc-tile-actions .dc-tile-state, .dc-tile-actions .dc-titlebar-toggle'), null);
  });

  it('a chart\'s state: Dynamic quiet, a pick among three, its ✓ on the one it is in, what each does on hover', async () => {
    newPage();
    const a = page.addGrid(over('trades.csv'));
    const b = page.addGrid(over('orders.csv'), { near: a });
    await settle();
    const chart = await chartOf(b);
    const state = tile(chart).querySelector<HTMLElement>('.dc-tile-head > .dc-tile-state')!;
    assert.deepEqual([state.dataset['state'], state.textContent, state.getAttribute('aria-haspopup')], ['dynamic', 'Dynamic', 'menu']);
    state.click();
    const items = [...dom.window.document.querySelectorAll<HTMLElement>('.dc-menu [role^="menuitem"]')];
    assert.deepEqual(items.map(label), ['Dynamic', 'Pinned', 'Frozen']);
    assert.deepEqual(items.map((i) => i.getAttribute('aria-checked')), ['true', 'false', 'false']);
    assert.match(items[2]!.title, /^It keeps its grouping and the grid's filter/);
    dom.window.document.dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: 'Escape' }));
    await pin(chart);
    assert.deepEqual([state.dataset['state'], state.textContent], ['pinned', 'Pinned']);
  });

  it('a text box\'s pencil: accented while its words are edited, quiet once they are', async () => {
    newPage({ confirm: () => true });
    page.addGrid(over('trades.csv'));
    await settle();
    entry(pageMenu(), 'Text Box')!.click();
    await settle();
    const id = page.views().views.find((v) => v.kind === 'text')!.id;
    const pencil = tile(id).querySelector<HTMLElement>('.dc-tile-head > .dc-tile-state')!;
    await until(() => tile(id).querySelector('textarea'));
    assert.deepEqual([pencil.dataset['state'], pencil.textContent], ['editing', 'Editing'], 'its editor opens on a new one');
    [...tile(id).querySelectorAll('button')].find((b) => b.textContent === 'Cancel')!.click();
    await until(() => tile(id).querySelector('.dc-text-tile'));
    assert.deepEqual([pencil.dataset['state'], pencil.textContent], ['viewing', 'Edit']);
    // a click on it edits the words again
    pencil.click();
    await until(() => tile(id).querySelector('textarea'));
    assert.equal(pencil.dataset['state'], 'editing');
  });

  it('a grid\'s state: live, it offers Snap; snapped, Go Live and Snap Again -- a fresh copy, written down', async () => {
    let made = 0;
    const runner = new PlanThenRun(new StubPlanner(), engine) as unknown as QueryRunner;
    newPage();
    const a = page.addGrid((gridHost, spawned) => new CubeApp(gridHost, SNAPSHOT, {
      runner, cubeSource: file('trades.csv'), sourceLabel: 'trades.csv', compact: true,
      hostSnap: {
        store: 'Python',
        snap: async () => ({ rowCount: 3, table: `snap_${made += 1}`, source: SNAPSHOT.source.query, runner }),
        release: async () => {},
        held: async (table) => ({ table, source: SNAPSHOT.source.query, runner }),
      },
      ...(page ? { windowHost: page.root } : {}), ...spawned,
    }));
    await settle();
    assert.deepEqual(offered(), ['Snap']);
    await movePlane('Snap');
    assert.equal(page.state.page.copies?.[a]?.table, 'snap_1');
    assert.equal(host.querySelector<HTMLElement>('.dc-tile-state')!.dataset['state'], 'snapped');
    assert.deepEqual(offered(), ['Go Live', 'Snap Again']);
    await movePlane('Snap Again');
    assert.equal(page.state.page.copies?.[a]?.table, 'snap_2', 'a fresh copy, the page told');
    assert.equal(host.querySelector<HTMLElement>('.dc-tile-state')!.dataset['state'], 'snapped');
    await movePlane('Go Live');
    assert.equal(page.state.page.copies, undefined);
    assert.equal(host.querySelector<HTMLElement>('.dc-tile-state')!.dataset['state'], 'live');
    // a grid whose rows cannot move (no store to snap into): its state says Live, and opens nothing
    const b = page.addGrid(over('orders.csv'), { near: a });
    await settle();
    const fixed = tile(b).querySelector<HTMLButtonElement>('.dc-tile-head > .dc-tile-state')!;
    assert.equal(fixed.getAttribute('aria-disabled'), 'true');
    fixed.click();
    assert.equal(dom.window.document.querySelector('.dc-menu'), null, 'no menu');
  });
});

describe("another editor's commands followed: nothing told back that the page did not say (the S7 audit's P-1)", () => {
  /** Each grid command the page's grids told, by grid: what a host's channel would write. */
  let told: { grid: string; kind: string }[] = [];
  const telling = (name: string, filter?: FilterNode): GridMaker => (gridHost, spawned, start) => new CubeApp(gridHost,
    start?.snapshot ?? (filter ? { ...SNAPSHOT, filter } : SNAPSHOT), {
    engine, planner: new StubPlanner(), compact: true, cubeSource: file(name), sourceLabel: name,
    configuration: start?.configuration ?? { ...DEFAULT_CONFIGURATION, reportTitle: name },
    ...(page ? { windowHost: page.root } : {}), ...spawned,
    onCommand: (command) => { if (spawned.id !== undefined) told.push({ grid: spawned.id, kind: command.kind }); },
  });
  const filterOf = (doc: PageDocument, grid: string): unknown => doc.cubes.find((c) => c.id === grid)!.cube.query.filter;

  it("a selected chart removed by the other editor: its grid's filter as the page's rule leaves it, no refilter told", async () => {
    newPage();
    const a = page.addGrid(telling('trades.csv'));
    page.addGrid(telling('orders.csv'), { near: a });
    await settle();
    const chart = await chartOf(a);
    await settle();
    echarts.getInstanceByDom(tile(chart).querySelector<HTMLElement>('[_echarts_instance_]')!)!
      .trigger('click', { seriesIndex: 0, dataIndex: 0 } as never);
    await settle();
    const before = page.document('Q3')!;
    assert.ok(filterOf(before, a) !== undefined, 'the mark is on its grid\'s filter');
    // the other editor's removal (Python's page.remove(chart)), as its keeper applies it
    const kept = applyPageCommands({ page: before, held: idsOf(before) }, [{ kind: 'removeTile', tile: chart }]).draft.page;
    told = [];
    await theirs(page, [{ kind: 'removeTile', tile: chart }], new Map());
    await settle();
    assert.deepEqual(told, [], 'following tells nothing back: the removal took the selection off its grid already');
    assert.deepEqual(filterOf(page.document('Q3')!, a), filterOf(kept, a), 'the grid holds the filter its keeper holds');
    assert.deepEqual(filterOf(page.state.page, a), filterOf(kept, a));
  });

  it("the screen's own Remove of a selected chart: removeTile alone, its grid's refilter a step of its own history (the S7 confirmation audit's G2-2)", async () => {
    const pageTold: PageCommand[] = [];
    newPage({ onPageCommand: (c) => pageTold.push(c) });
    const a = page.addGrid(telling('trades.csv'));
    page.addGrid(telling('orders.csv'), { near: a });
    await settle();
    const chart = await chartOf(a);
    await settle();
    echarts.getInstanceByDom(tile(chart).querySelector<HTMLElement>('[_echarts_instance_]')!)!
      .trigger('click', { seriesIndex: 0, dataIndex: 0 } as never);
    await settle();
    // an edit of the grid's own after the click
    const grid = page.grid(a) as unknown as CubeApp;
    await grid.edit([{ kind: 'sorts', sorts: [{ column: 'notional', direction: 'desc' }] }], 'sort');
    await settle();
    const before = page.document('Q3')!;
    const kept = applyPageCommands({ page: before, held: idsOf(before) }, [{ kind: 'removeTile', tile: chart }]).draft.page;
    told = [];
    pageTold.length = 0;
    tile(chart).querySelector<HTMLButtonElement>('.dc-tile-remove')!.click();
    await settle();
    assert.deepEqual(pageTold.map((c) => c.kind), ['removeTile']);
    assert.deepEqual(told, [], 'no refilter beside it: the removal took the selection off (a second would take a condition put there by hand off too)');
    assert.deepEqual(filterOf(page.document('Q3')!, a), filterOf(kept, a));
    assert.deepEqual(filterOf(page.state.page, a), filterOf(kept, a));
    assert.equal(grid.canUndo, true, "the grid's own history kept (a removal followed as another editor's wiped it)");
    // a step of it, recorded: the grid's undo lands where it was before the removal, its sort kept (the third round's
    // 1: unrecorded, the undo took the sort away and put the selection back)
    await grid.undo();
    await settle();
    assert.deepEqual(grid.snapshot.sorts, [{ column: 'notional', direction: 'desc' }]);
    assert.deepEqual(grid.snapshot.filter, filterOf(before, a));
  });

  it("a selected chart removed while the grid runs another edit: its selection taken off once, the condition put there by hand too kept (the S7 confirmation audit's third round's 3)", async () => {
    const all: { readonly page?: PageCommand; readonly grid?: { readonly id: string; readonly command: GridCommand } }[] = [];
    const recording = (name: string): GridMaker => (gridHost, spawned, start) => new CubeApp(gridHost, start?.snapshot ?? SNAPSHOT, {
      engine, planner: new StubPlanner(), compact: true, cubeSource: file(name), sourceLabel: name,
      configuration: start?.configuration ?? { ...DEFAULT_CONFIGURATION, reportTitle: name },
      ...(page ? { windowHost: page.root } : {}), ...spawned,
      onCommand: (command) => { if (spawned.id !== undefined) all.push({ grid: { id: spawned.id, command } }); },
    });
    newPage({ onPageCommand: (c) => all.push({ page: c }) });
    const a = page.addGrid(recording('trades.csv'));
    page.addGrid(recording('orders.csv'), { near: a });
    await settle();
    const chart = await chartOf(a);
    await settle();
    echarts.getInstanceByDom(tile(chart).querySelector<HTMLElement>('[_echarts_instance_]')!)!
      .trigger('click', { seriesIndex: 0, dataIndex: 0 } as never);
    await settle();
    const grid = page.grid(a) as unknown as CubeApp;
    const view = page.document('Q3')!.views.find((v) => v.id === chart);
    const mark = view?.kind === 'chart' ? view.selection?.[0] : undefined;
    assert.ok(mark);
    // the same condition put on the grid by hand too
    await grid.edit([{ kind: 'addFilter', condition: mark }], 'filter');
    await settle();
    const before = page.document('Q3')!;
    all.length = 0;
    // a sort of the grid's own still running when the chart is removed
    engine.gate.hold = true;
    void grid.edit([{ kind: 'sorts', sorts: [{ column: 'notional', direction: 'desc' }] }], 'sort');
    await settle();
    tile(chart).querySelector<HTMLButtonElement>('.dc-tile-remove')!.click();
    await settle();
    engine.gate.hold = false;
    engine.gate.releaseAll();
    await settle();
    assert.deepEqual(all.flatMap((t) => (t.grid ? [t.grid.command.kind] : [])), ['sorts'], 'no refilter told: the removal said it');
    let host: PageDraft = { page: before, held: idsOf(before) };
    for (const t of all) {
      if (t.page) host = applyPageCommands(host, [t.page]).draft;
      else {
        const at = host.page.cubes.find((c) => c.id === t.grid!.id)!;
        const d = applyGridCommands({ snapshot: { ...SNAPSHOT, ...at.cube.query }, config: configurationOf(at.cube), tree: treeOf(at.cube) },
          [t.grid!.command]);
        const { filter: _was, ...query } = at.cube.query;
        const cube = { ...at.cube, query: d.snapshot.filter ? { ...query, filter: d.snapshot.filter } : query };
        host = { ...host, page: { ...host.page, cubes: host.page.cubes.map((c) => (c.id === at.id ? { ...c, cube } : c)) } };
      }
    }
    assert.deepEqual(filterOf(host.page, a), filterOf(page.document('Q3')!, a), "the host's grid filtered as the screen's");
    assert.deepEqual(filterOf(page.document('Q3')!, a), mark, 'the condition put there by hand kept');
  });

  it("a selected chart removed while its click still runs: the grid's edits told in the order made, the host's page as the screen's (the S7 confirmation audit's B2-3)", async () => {
    const all: { readonly page?: PageCommand; readonly grid?: { readonly id: string; readonly command: GridCommand } }[] = [];
    const recording = (name: string): GridMaker => (gridHost, spawned, start) => new CubeApp(gridHost, start?.snapshot ?? SNAPSHOT, {
      engine, planner: new StubPlanner(), compact: true, cubeSource: file(name), sourceLabel: name,
      configuration: start?.configuration ?? { ...DEFAULT_CONFIGURATION, reportTitle: name },
      ...(page ? { windowHost: page.root } : {}), ...spawned,
      onCommand: (command) => { if (spawned.id !== undefined) all.push({ grid: { id: spawned.id, command } }); },
    });
    newPage({ onPageCommand: (c) => all.push({ page: c }) });
    const a = page.addGrid(recording('trades.csv'));
    page.addGrid(recording('orders.csv'), { near: a });
    await settle();
    const chart = await chartOf(a);
    await settle();
    const before = page.document('Q3')!;
    all.length = 0;
    // the click's query held: its refilter not yet landed (nor told) when the chart is removed
    engine.gate.hold = true;
    echarts.getInstanceByDom(tile(chart).querySelector<HTMLElement>('[_echarts_instance_]')!)!
      .trigger('click', { seriesIndex: 0, dataIndex: 0 } as never);
    await settle();
    tile(chart).querySelector<HTMLButtonElement>('.dc-tile-remove')!.click();
    await settle();
    engine.gate.hold = false;
    engine.gate.releaseAll();
    await settle();
    // the host's replay: the page's commands and each grid's, in the order told, on the page before
    let host: PageDraft = { page: before, held: idsOf(before) };
    for (const t of all) {
      if (t.page) host = applyPageCommands(host, [t.page]).draft;
      else {
        const at = host.page.cubes.find((c) => c.id === t.grid!.id)!;
        // a refilter's change is the grid's filter: the cube's query as the fixture's snapshot over its source
        const d = applyGridCommands({ snapshot: { ...SNAPSHOT, ...at.cube.query }, config: configurationOf(at.cube), tree: treeOf(at.cube) },
          [t.grid!.command]);
        const { filter: _was, ...query } = at.cube.query;
        const cube = { ...at.cube, query: d.snapshot.filter ? { ...query, filter: d.snapshot.filter } : query };
        host = { ...host, page: { ...host.page, cubes: host.page.cubes.map((c) => (c.id === at.id ? { ...c, cube } : c)) } };
      }
    }
    assert.deepEqual(filterOf(host.page, a), filterOf(page.document('Q3')!, a), "the host's grid filtered as the screen's");
    assert.equal(filterOf(page.document('Q3')!, a), undefined, 'no selection left on the grid: its chart is gone');
  });

  it("a grid the other editor adds while the bar is folded: its title bar as its document says, the page's state too (the S7 audit's G-3)", async () => {
    newPage({ confirm: () => true });
    page.addGrid(over('trades.csv'));
    await settle();
    page.setBarFolded(true);
    await settle();
    // Python's grid, its title bar shown (the default it writes)
    const doc = page.document('Q3')!.cubes[0]!.cube;
    const shown: CubeDocument = { ...doc, configuration: { ...doc.configuration, showTitleBar: true } };
    await theirs(page, [{ kind: 'addGrid', id: 'grid-7', document: shown, sheet: page.sheets[0]!, near: null, title: null, columns: null }],
      new Map([['grid-7', over('orders.csv', shown)]]));
    await settle();
    const cubeOf = (p: PageDocument): CubeDocument => p.cubes.find((c) => c.id === 'grid-7')!.cube;
    assert.equal(configurationOf(cubeOf(page.state.page)).showTitleBar, true, 'the state holds what its keeper holds');
    assert.equal(configurationOf(cubeOf(page.document('Q3')!)).showTitleBar, true);
  });

  it("the saved page it is and its name are the page's state, by their commands: its own, another editor's, a page restored (the S7 audit's H-4, P-5)", async () => {
    const pageTold: PageCommand[] = [];
    newPage({ onPageCommand: (c) => pageTold.push(c) });
    page.addGrid(over('trades.csv'));
    await settle();
    pageTold.length = 0;
    const step = page.undoLabel;
    page.savedAs('r-1');
    page.rename('Trades');
    assert.deepEqual(pageTold.map((c) => c.kind), ['savedAs', 'renamePage']);
    assert.equal(page.record, 'r-1');
    assert.equal(page.state.page.record, 'r-1');
    assert.equal(page.document('Trades')!.record, 'r-1', 'the page written carries it');
    assert.equal(page.state.page.name, 'Trades');
    assert.equal(page.title, 'Trades');
    assert.equal(page.undoLabel, step, 'no step of its history');
    // the same again tells nothing
    page.savedAs('r-1');
    page.rename('Trades');
    assert.equal(pageTold.length, 2);
    // another editor's: followed by the same rule, no host keeping it beside the page
    await theirs(page, [{ kind: 'savedAs', record: 'r-2' }, { kind: 'renamePage', name: 'Q5' }], new Map());
    assert.equal(page.record, 'r-2');
    assert.equal(page.title, 'Q5');
    await theirs(page, [{ kind: 'savedAs', record: null }], new Map());
    assert.equal(page.record, undefined);
    // a page restored with its record holds it, and an edit keeps it
    const doc = { ...page.document('Q5')!, record: 'r-3' };
    page.restore(doc, new Map(doc.cubes.map((c) => [c.id, over(c.cube.source.name)])));
    await settle();
    assert.equal(page.record, 'r-3');
    plus(bar());
    await settle();
    assert.equal(page.state.page.record, 'r-3', 'kept through an edit');
    // an undo or a redo of an edit made before a Save and a rename: neither is a step, so both stay (the S7
    // confirmation audit's H2-2: an undo past a Save saved a second record, under the old name)
    page.savedAs('r-4');
    page.rename('Trades');
    page.undo();
    await settle();
    assert.equal(page.record, 'r-4', 'the record kept by an undo');
    assert.equal(page.state.page.record, 'r-4');
    assert.equal(page.title, 'Trades', 'the name kept by an undo');
    assert.equal(page.document(page.title)!.name, 'Trades');
    page.redo();
    await settle();
    assert.equal(page.record, 'r-4', 'and by a redo');
    assert.equal(page.title, 'Trades');
  });

  it("a sheet's label counts a grid the page could not open, as the rule does (the S7 confirmation audit's B2-4)", async () => {
    // unnamed: its title is its first sheet's label
    newPage({ title: '' });
    const a = page.addGrid(over('trades.csv'));
    page.addGrid(over('orders.csv'), { near: a });
    await settle();
    const doc = page.document('')!;
    // its first grid left out when it opens again (a class whose rows are not read here)
    page.restore(doc, new Map(doc.cubes.filter((c) => c.id !== a).map((c) => [c.id, over(c.cube.source.name)])));
    await settle();
    const sheet = page.state.page.sheets[0]!;
    assert.equal(page.suggestedName, unnamedLabel(page.state.page, sheet), "the screen's label is the rule's");
    assert.equal(page.suggestedName, 'trades.csv');
  });

  it("the same mark clicked again takes it off, whatever order its page wrote the selection's keys in (the S7 audit's Y-9)", async () => {
    newPage();
    const a = page.addGrid(telling('trades.csv'));
    await settle();
    const chart = await chartOf(a);
    await settle();
    const click = (): void => {
      echarts.getInstanceByDom(tile(chart).querySelector<HTMLElement>('[_echarts_instance_]')!)!
        .trigger('click', { seriesIndex: 0, dataIndex: 0 } as never);
    };
    click();
    await settle();
    const doc = page.document('Q3')!;
    // the page as another writer wrote it: each condition of the chart's selection with its keys in another order
    const reordered = (v: PageDocument['views'][number]): PageDocument['views'][number] => (v.kind === 'chart' && v.selection
      ? { ...v, selection: v.selection.map((c) => Object.fromEntries(Object.entries(c).reverse()) as typeof c) } : v);
    const written: PageDocument = { ...doc, views: doc.views.map(reordered) };
    assert.ok(written.views.some((v) => v.kind === 'chart' && v.selection !== undefined), 'the chart selects its mark');
    page.dispose();
    newPage();
    // its grid opened as it was saved: filtered to the mark
    page.restore(written, new Map([[a, telling('trades.csv', written.cubes[0]!.cube.query.filter)]]));
    await settle();
    await new Promise((r) => setTimeout(r, 200));
    await settle();
    click();
    await settle();
    const view = page.document('Q3')!.views.find((v) => v.id === chart);
    assert.equal(view?.kind === 'chart' ? view.selection : 'no chart', undefined, 'the same mark again: the selection taken off, not set again');
  });

});

describe('a dashboard\'s page filters (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §16.2): on a class, reaching its grids', () => {
  const TRADE = 'trades::domain::Trade';
  const COLUMNS = SNAPSHOT.columns.map((c) => ({ name: c.name, type: c.type }));
  const EMEA = { kind: 'condition', column: 'region', operator: 'equal', value: 'EMEA' } as const;

  /** A planner that keeps every query it plans: what the grid asked the database for. */
  class Recording extends StubPlanner {
    readonly plans: string[] = [];
    override async plan(query?: unknown): ReturnType<StubPlanner['plan']> {
      this.plans.push(JSON.stringify(query));
      return super.plan();
    }
  }

  /** A grid over the class Trade (its class source), its planner the one handed in. */
  const overClass = (planner: Recording): GridMaker => (gridHost, spawned, start) => new CubeApp(gridHost,
    start?.snapshot ?? { ...SNAPSHOT, source: { query: classRelation(TRADE, COLUMNS.map((c) => c.name)) } }, {
      engine, planner, compact: true, sourceLabel: 'Trade',
      cubeSource: { _type: 'class', name: 'Trade', class: TRADE, project: 'datacube:trades', workspace: 'datacube',
        revision: '3', mapping: 'trades::mapping::Model', runtime: 'trades::runtime::Browser', columns: COLUMNS },
      configuration: { ...DEFAULT_CONFIGURATION, reportTitle: 'Trade' }, ...(page ? { windowHost: page.root } : {}), ...spawned,
    });
  const filtered = (plans: readonly string[]): boolean => plans.some((p) => p.includes('"function":"filter"') && p.includes('"function":"getAll"'));
  const chips = (): string[] => [...host.querySelectorAll('.dc-page-filter')].map((c) => `${c.querySelector('.dc-page-filter-words')?.textContent} / ${c.querySelector('.dc-page-filter-reach')?.textContent}`);

  it("on a page open on its keeper, a filter set from its bar is made under the page's tag, and the page reads back (the middle version's audit's 1, 2)", async () => {
    // an untagged filter-N made here would, made again on top of another editor's filter-N, be set in that one's place --
    // and a page holding a tagged filter must be read again as the keeper serves it
    newPage();
    page.idTag = 'tabcde';
    page.addGrid(overClass(new Recording()));
    await settle();
    const id = await page.setFilter({ class: TRADE, condition: EMEA });
    assert.equal(id, 'filter-tabcde-1');
    const doc = page.document('Q3')!;
    assert.deepEqual(readPage(JSON.parse(pageToJson(doc))).filters?.map((f) => f.id), ['filter-tabcde-1'], 'read back as written');
  });

  it('set from the page: every grid over the class runs filtered, a grid over a file does not; undone, none is', async () => {
    newPage();
    const a = new Recording();
    const b = new Recording();
    page.addGrid(overClass(a));
    page.addGrid(overClass(b));
    page.addGrid(over('trades.csv'));
    await settle();
    assert.equal(filtered([...a.plans, ...b.plans]), false);
    assert.equal((host.querySelector('.dc-page-filters') as HTMLElement).hidden, false, 'a page with a class grid has the bar');
    const id = await page.setFilter({ class: TRADE, condition: EMEA });
    await settle();
    assert.equal(id, 'filter-1');
    assert.ok(filtered(a.plans) && filtered(b.plans), 'both class grids ran again, filtered');
    assert.deepEqual(chips(), ['Trade: region = EMEA / 2 grids']);
    // each tile says what filters it: the class grids, in their status bars; the file's grid, nothing
    assert.deepEqual([...host.querySelectorAll('.dc-status-page-filter')].map((s) => s.textContent),
      ['Page: region = EMEA', 'Page: region = EMEA']);
    assert.equal(page.undoLabel, 'Filter');
    // the grids' own state is theirs: the filter is the page's, saved with it, never in a grid's cube
    const doc = page.document('Q3')!;
    assert.deepEqual(doc.filters, [{ id: 'filter-1', class: TRADE, condition: EMEA }]);
    assert.ok(doc.cubes.every((c) => c.cube.query.filter === undefined));
    a.plans.length = 0;
    page.undo();
    await settle();
    assert.equal(page.filters.length, 0);
    assert.deepEqual(chips(), []);
    assert.ok(a.plans.length > 0 && !filtered(a.plans), 'undone: it ran again, unfiltered');
    page.redo();
    await settle();
    // the filter back is a new one (B12: an id the page has held is never given out again, a page filter's too)
    assert.deepEqual(page.filters.map((f) => f.id), ['filter-2']);
  });

  it('refused in words, nothing changed: a property the class has not', async () => {
    newPage();
    page.addGrid(overClass(new Recording()));
    await settle();
    const before = page.undoLabel;
    await assert.rejects(page.setFilter({ class: TRADE, condition: { ...EMEA, column: 'colour' } }), /Trade has no colour/);
    assert.equal(page.filters.length, 0);
    assert.equal(page.undoLabel, before, 'no step of it');
  });

  it('another editor\'s commands (the live channel): a grid they add with a filter opens filtered from its first query', async () => {
    newPage();
    page.addGrid(overClass(new Recording()));
    await settle();
    const doc = page.document('Q3')!;
    const added = new Recording();
    const grid = `grid-${page.cubes.length + 1}`;
    await theirs(page, [
      { kind: 'addGrid', id: grid, document: doc.cubes[0]!.cube, sheet: doc.sheets[0]!.id, near: null, title: null, columns: null },
      { kind: 'setPageFilter', class: TRADE, condition: EMEA, from: null },
    ], new Map([[grid, overClass(added)]]));
    await settle();
    assert.ok(added.plans.length > 0 && added.plans.every((p) => !p.includes('"function":"getAll"') || p.includes('"function":"filter"')),
      'no query of it read the class unfiltered');
    assert.deepEqual(page.filters.map((f) => f.id), ['filter-1']);
  });

  it('a saved page reopened: each class grid opens filtered from its first query', async () => {
    newPage();
    page.addGrid(overClass(new Recording()));
    await settle();
    await page.setFilter({ class: TRADE, condition: EMEA });
    const doc = readPage(pageToJson(page.document('Q3')!));
    page.dispose();
    newPage();
    const again = new Recording();
    page.restore(doc, new Map([[doc.cubes[0]!.id, overClass(again)]]));
    await settle();
    assert.ok(again.plans.length > 0 && again.plans.every((p) => !p.includes('"function":"getAll"') || p.includes('"function":"filter"')),
      'no query of it read the class unfiltered');
    assert.deepEqual(chips(), ['Trade: region = EMEA / 1 grid']);
  });
});

describe('a chart\'s click on a grid over a class (§16.2): a page filter from that chart, its own grid left whole', () => {
  const TRADE = 'trades::domain::Trade';
  const COLUMNS = SNAPSHOT.columns.map((c) => ({ name: c.name, type: c.type }));

  class Recording extends StubPlanner {
    readonly plans: string[] = [];
    override async plan(query?: unknown): ReturnType<StubPlanner['plan']> {
      this.plans.push(JSON.stringify(query));
      return super.plan();
    }
  }
  const overClass = (planner: Recording): GridMaker => (gridHost, spawned, start) => new CubeApp(gridHost,
    start?.snapshot ?? { ...SNAPSHOT, source: { query: classRelation(TRADE, COLUMNS.map((c) => c.name)) } }, {
      engine, planner, compact: true, sourceLabel: 'Trade',
      cubeSource: { _type: 'class', name: 'Trade', class: TRADE, project: 'datacube:trades', workspace: 'datacube',
        revision: '3', mapping: 'trades::mapping::Model', runtime: 'trades::runtime::Browser', columns: COLUMNS },
      configuration: { ...DEFAULT_CONFIGURATION, reportTitle: 'Trade' }, ...(page ? { windowHost: page.root } : {}), ...spawned,
    });
  const filtered = (plans: readonly string[]): boolean => plans.some((p) => p.includes('"function":"filter"') && p.includes('"function":"getAll"'));

  it('the click filters every other grid over the class; the same mark again takes it off', async () => {
    newPage();
    const a = new Recording();
    const b = new Recording();
    const gridA = page.addGrid(overClass(a));
    page.addGrid(overClass(b));
    await settle();
    const chart = await chartOf(gridA);
    await settle();
    // the mark clicked, as ECharts tells the chart (chart-render.ts: its click on a bar)
    const mounted = tile(chart).querySelector<HTMLElement>('[_echarts_instance_]');
    assert.ok(mounted, 'the chart is mounted');
    const instance = echarts.getInstanceByDom(mounted)!;
    a.plans.length = 0;
    b.plans.length = 0;
    instance.trigger('click', { seriesIndex: 0, dataIndex: 0 } as never);
    await settle();
    assert.equal(page.filters.length, 1);
    const f = page.filters[0]!;
    assert.equal(f.from, chart);
    assert.equal(f.class, TRADE);
    // the mark's conditions, as the chart said them (a bar of a region, of a desk), on the class's properties
    assert.match(JSON.stringify(f.condition), /"column":"region"/);
    assert.ok(filtered(b.plans), 'the other grid over the class ran filtered');
    // the clicked chart's own grid, run again: whole, no page filter on it
    await (page.grid(gridA) as CubeApp).open();
    await settle();
    assert.ok(a.plans.length > 0 && !filtered(a.plans), 'the clicked chart\'s own grid is left whole');
    assert.equal(page.document('Q3')!.cubes.find((c) => c.id === gridA)!.cube.query.filter, undefined,
      'nothing on the clicked grid\'s own filter');
    instance.trigger('click', { seriesIndex: 0, dataIndex: 0 } as never);
    await settle();
    assert.equal(page.filters.length, 0, 'the same mark again: taken off');
  });

  /** A grid over Trade whose class has only `columns` (its grid's other columns, as a calculated column is, not the class's). */
  const overNarrow = (planner: Recording, columns: typeof COLUMNS): GridMaker => (gridHost, spawned, start) => new CubeApp(gridHost,
    start?.snapshot ?? { ...SNAPSHOT, source: { query: classRelation(TRADE, COLUMNS.map((c) => c.name)) } }, {
      engine, planner, compact: true, sourceLabel: 'Trade',
      cubeSource: { _type: 'class', name: 'Trade', class: TRADE, project: 'datacube:trades', workspace: 'datacube',
        revision: '3', mapping: 'trades::mapping::Model', runtime: 'trades::runtime::Browser', columns },
      configuration: { ...DEFAULT_CONFIGURATION, reportTitle: 'Trade' }, ...(page ? { windowHost: page.root } : {}), ...spawned,
    });

  it('a mark naming a property and another column: the property\'s part the page\'s, the rest the grid\'s own (the audit\'s 4)', async () => {
    newPage();
    const gridA = page.addGrid(overNarrow(new Recording(), COLUMNS.filter((c) => c.name !== 'desk')));
    await settle();
    const chart = await chartOf(gridA);
    await settle();
    const instance = echarts.getInstanceByDom(tile(chart).querySelector<HTMLElement>('[_echarts_instance_]')!)!;
    instance.trigger('click', { seriesIndex: 0, dataIndex: 0 } as never);
    await settle();
    assert.equal(page.filters.length, 1);
    assert.doesNotMatch(JSON.stringify(page.filters[0]!.condition), /"column":"desk"/, 'the page\'s part names the class\'s properties only');
    assert.match(JSON.stringify(page.filters[0]!.condition), /"column":"region"/);
    const own = JSON.stringify(page.document('Q3')!.cubes.find((c) => c.id === gridA)!.cube.query.filter ?? null);
    assert.match(own, /"column":"desk"/, 'the rest, on the grid\'s own filter');
    assert.doesNotMatch(own, /"column":"region"/);
  });

  it('two quick clicks on two marks: one filter from the chart, the second\'s (the audit\'s 5)', async () => {
    newPage();
    const gridA = page.addGrid(overClass(new Recording()));
    page.addGrid(overClass(new Recording()));
    await settle();
    const chart = await chartOf(gridA);
    await settle();
    const instance = echarts.getInstanceByDom(tile(chart).querySelector<HTMLElement>('[_echarts_instance_]')!)!;
    instance.trigger('click', { seriesIndex: 0, dataIndex: 0 } as never);
    instance.trigger('click', { seriesIndex: 0, dataIndex: 1 } as never);
    await settle();
    assert.equal(page.filters.length, 1, 'never two from one chart (two clicks must not filter to nothing)');
  });

  it('a grid spawned by another (a copy on a cube alone\'s board) reads as its grid does, the page\'s filters changing with it (the audit\'s 6)', async () => {
    newPage();
    const a = new Recording();
    const gridA = page.addGrid(overClass(a));
    await settle();
    const host2 = dom.window.document.createElement('div') as unknown as HTMLElement;
    const editing = (page.grid(gridA) as CubeApp).chartSource().spawn(host2, (page.grid(gridA) as CubeApp).snapshot) as CubeApp;
    await page.setFilter({ class: TRADE, condition: { kind: 'condition', column: 'region', operator: 'equal', value: 'EMEA' } });
    await settle();
    a.plans.length = 0;
    await editing.open();
    await settle();
    assert.ok(filtered(a.plans), 'the spawned grid reads the class filtered too');
    await page.removeFilter(page.filters[0]!.id);
    await settle();
    a.plans.length = 0;
    await editing.open();
    await settle();
    assert.ok(a.plans.length > 0 && !filtered(a.plans), 'the filter taken off: the spawned grid reads it whole again');
    editing.dispose();
  });
});
