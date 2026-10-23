// The page shell (page/cube-page.ts) as a person meets it, beyond what chart-tiles.test.ts and page-app.test.ts hold:
// SHOW GRID (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md D7) -- the grid a following chart follows shown, the page as it
// is; a frozen or a DETACHED chart's grid of its own grouping made in one step, the chart following it from then on, its
// values the new grid's Column Properties (an Aggregation changed there reaching it), its selection taken off its old
// grid's filter, the cube a detached chart kept let go, undone and redone as one step -- then SHEETS (a tile dragged onto
// a tab, a sheet deleted with a grid whose charts are elsewhere, a page reopened without its deleted first sheet, each
// sheet's own layout undo), TITLES (a renamed tile moved and saved), a new grid's id beside a kept one, and a chart's
// click-to-filter SELECTION (its chip, cleared by it or by the grid's filter).
//
// Real grids (CubeApps over a fake engine: cube-fixture.ts), on a page of its own made the way DataCube's app makes
// one (page-app.test.ts), or on a cube alone (chart-tiles.test.ts). No test here needs the WebAssembly planner.

import assert from 'node:assert/strict';
import { afterEach, beforeEach, describe, it } from 'node:test';
import { JSDOM } from 'jsdom';

import { CubeApp } from '../src/app.ts';
import { DEFAULT_CONFIGURATION } from '../src/config.ts';
import { cubeToJson, openCube, writeCube, type CubeDocument, type FileSource } from '../src/cube-document.ts';
import { tiles as tilesOf } from '../src/layout/bands.ts';
import { PageApp, type GridMaker, type PageAppOptions } from '../src/page/page-app.ts';
import { CubePage, type SpawnedGrid } from '../src/page/cube-page.ts';
import { PAGE_CUBE, pageToJson, readPage, startingViews, writePageOf, type ChartView, type PageDocument } from '../src/page-document.ts';
import { pageSources } from '../src/app/sources.ts';
import { TreeState } from '../src/tree.ts';
import { applyPageCommands, idsOf, type PageCommand } from '../src/page-commands.ts';
import type { ChartMeasure } from '../src/chart-spec.ts';
import type { FilterNode } from '../src/snapshot.ts';
import { getInstanceByDom } from 'echarts/core';

import * as cube from './cube-fixture.ts';
import { GateEngine, SNAPSHOT, StubPlanner, settle } from './cube-fixture.ts';

/** jsdom has no canvas: a 2D context that draws nothing, so ECharts can mount (as chart-tiles.test.ts does). */
function inertCanvas(window: JSDOM['window']): void {
  const inert: ProxyHandler<object> = {
    get: (_t, key) => (key === 'canvas' ? window.document.createElement('canvas')
      : key === 'measureText' ? () => ({ width: 0 }) : () => undefined),
    set: () => true,
  };
  (window.HTMLCanvasElement.prototype as unknown as { getContext: () => unknown }).getContext = () => new Proxy({}, inert);
}

// -- a page of its own: DataCube's app ------------------------------------------------------------------------------

let dom: JSDOM;
let host: HTMLElement;
let engine: GateEngine;
let page: PageApp | undefined;
let unhandled: unknown[];
let savedRejectionListeners: NodeJS.UnhandledRejectionListener[] = [];

/** The page made by the test (each test makes one). */
const pg = (): PageApp => {
  assert.ok(page, 'the test made a page');
  return page;
};

function setUpPage(): void {
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
  inertCanvas(dom.window);
  engine = new GateEngine();
  page = undefined;
  unhandled = [];
  savedRejectionListeners = process.listeners('unhandledRejection');
  process.removeAllListeners('unhandledRejection');
  process.on('unhandledRejection', (e) => unhandled.push(e));
}

async function tearDownPage(): Promise<void> {
  await settle();
  page?.dispose();
  page = undefined;
  process.removeAllListeners('unhandledRejection');
  for (const listener of savedRejectionListeners) process.on('unhandledRejection', listener);
  assert.deepEqual(unhandled, [], 'a promise the page started was rejected and nothing handled it');
}

/** A file a grid reads, as its saved cube names it. */
const file = (name: string): FileSource => ({
  _type: 'file', name, format: 'csv', size: 10, sha256: name.padEnd(64, '0').slice(0, 64),
  columns: SNAPSHOT.columns.map((c) => ({ name: c.name, type: c.type })),
});

/**
 * A grid over `name`, made as DataCube's app makes one (page-app.test.ts `over`): a saved cube opened over its source
 * as it is now (openCube), or, a copy, starting where its grid is.
 */
const over = (name: string, saved?: CubeDocument): GridMaker => (gridHost, spawned, start) => {
  const opened = saved ? openCube(saved, SNAPSHOT.source, SNAPSHOT.columns) : undefined;
  return new CubeApp(gridHost, start?.snapshot ?? opened?.snapshot ?? SNAPSHOT, {
    engine, planner: new StubPlanner(), compact: true, cubeSource: file(name), sourceLabel: name,
    configuration: start?.configuration ?? opened?.configuration ?? { ...DEFAULT_CONFIGURATION, reportTitle: name },
    // the same grid made again by an undo starts with its own open groups, as DataCube's app makes it
    ...(start?.tree ? { tree: start.tree as TreeState } : {}),
    ...(page ? { windowHost: page.root } : {}), ...spawned,
  });
};

function newPage(extra: Partial<PageAppOptions> = {}): PageApp {
  page = new PageApp({ host, title: 'Q3', empty: (slot) => { slot.textContent = 'Add a data source'; }, ...extra });
  return page;
}

/** A saved page reopened in a page of its own, each cube's source opened again by the host. */
function reopen(doc: PageDocument, extra: Partial<PageAppOptions> = {}): void {
  const back = readPage(pageToJson(doc));
  pg().dispose();
  newPage(extra);
  pg().restore(back, new Map(back.cubes.map((c) => [c.id, over(c.cube.source._type === 'file' ? c.cube.source.name : c.id, c.cube)])));
}

const tile = (id: string): HTMLElement => host.querySelector(`[data-tile="${id}"]`) as HTMLElement;
const label = (item: Element): string => item.querySelector(':scope > .dc-menu-label')?.textContent ?? '';
const menuItems = (): HTMLElement[] => [...dom.window.document.querySelectorAll<HTMLElement>('.dc-menu [role^="menuitem"]')];
const chartView = (id: string): ChartView | undefined =>
  pg().views().views.find((v): v is ChartView => v.kind === 'chart' && v.id === id);
const valuesOf = (id: string): readonly ChartMeasure[] | undefined => chartView(id)?.spec.y;
/** A chart's Dynamic / Frozen / Detached pill: what it says. */
const pill = (id: string): string => tile(id).querySelector('.dc-tile-actions .dc-titlebar-toggle')?.textContent ?? '';
const tabs = (): string[] => [...host.querySelectorAll<HTMLElement>('.dc-sheet-tab')].map((t) => t.querySelector('.dc-sheet-label')?.textContent ?? '');
const tabOf = (text: string): HTMLElement => [...host.querySelectorAll<HTMLElement>('.dc-sheet-tab')]
  .find((t) => (t.querySelector('.dc-sheet-label')?.textContent ?? '') === text)!;
/** The sheet a tile is drawn on. */
const sheetOf = (id: string): string | undefined => tile(id)?.closest<HTMLElement>('.dc-sheet')?.dataset['sheet'];
/** A tab clicked, as a pointer does. */
function clickTab(text: string): void {
  const tab = tabOf(text);
  tab.dispatchEvent(new dom.window.PointerEvent('pointerdown', { bubbles: true, button: 0, clientX: 0 }));
  tab.dispatchEvent(new dom.window.PointerEvent('pointerup', { bubbles: true, button: 0, clientX: 0 }));
}
/** A tab's right-click menu entry, clicked. */
function tabMenu(text: string, entry: string): void {
  tabOf(text).dispatchEvent(new dom.window.MouseEvent('contextmenu', { bubbles: true, cancelable: true }));
  const item = menuItems().find((i) => label(i) === entry);
  assert.ok(item, `the ${text} tab's menu has ${entry}`);
  item.click();
}
/** The page's menu, opened: its entries by what a person reads. */
function pageMenu(): HTMLElement[] {
  dom.window.document.dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: 'Escape' }));
  (host.querySelector('.dc-page-bar .dc-titlebar-menu') as HTMLElement).click();
  return menuItems();
}
/** An entry of a grid's own menu (its tile's header), by what it says. */
function gridMenuItem(gridTile: string, text: string): HTMLElement {
  dom.window.document.dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: 'Escape' }));
  (tile(gridTile).querySelector('.dc-tile-menu') as HTMLElement).click();
  const item = menuItems().find((i) => label(i) === text);
  assert.ok(item, `${gridTile}'s menu has ${text}`);
  return item;
}
/** Insert > Visualization, from a grid's right-click menu: the new chart's id. */
async function chartOf(gridTile: string): Promise<string> {
  const charts = (): string[] => [...host.querySelectorAll<HTMLElement>('[data-tile^="chart-"]')].map((t) => t.dataset['tile']!);
  const before = new Set(charts());
  (tile(gridTile).querySelector('.dc-row .dc-cell') as HTMLElement)
    .dispatchEvent(new dom.window.MouseEvent('contextmenu', { bubbles: true, cancelable: true }));
  await settle();
  const item = [...dom.window.document.querySelectorAll<HTMLElement>('.dc-menu .dc-menu-item')]
    .find((el) => label(el) === 'Visualization' && label(el.parentElement!.closest('.dc-menu-item') as HTMLElement) === 'Insert');
  assert.ok(item, 'Insert > Visualization');
  item.click();
  await settle();
  const chart = charts().find((id) => !before.has(id));
  assert.ok(chart, 'a chart on the page');
  return chart;
}
/** A chart's pill clicked: Dynamic to Frozen, or back. */
async function togglePill(chart: string): Promise<void> {
  (tile(chart).querySelector('.dc-tile-actions .dc-titlebar-toggle') as HTMLElement).click();
  await settle();
}
/** A chart's right-click menu, opened on its body. */
function chartMenu(chart: string): HTMLElement[] {
  dom.window.document.dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: 'Escape' }));
  (tile(chart).querySelector('.dc-chart-tile') as HTMLElement)
    .dispatchEvent(new dom.window.MouseEvent('contextmenu', { bubbles: true, cancelable: true }));
  return menuItems();
}
/** Show Grid, from the chart's right-click menu (D7): the grid it follows shown, or one of its own grouping made. */
async function showGrid(chart: string): Promise<void> {
  const item = chartMenu(chart).find((i) => label(i) === 'Show Grid');
  assert.ok(item && item.getAttribute('aria-disabled') !== 'true', `${chart} offers Show Grid`);
  item.click();
  await settle();
}
/** Whether a chart's right-click menu offers Show Grid: the entry is always there, disabled where it shows nothing. */
function offersShowGrid(chart: string): boolean {
  const item = chartMenu(chart).find((i) => label(i) === 'Show Grid');
  assert.ok(item, `${chart}'s menu has Show Grid`);
  return item.getAttribute('aria-disabled') !== 'true';
}
/** A tile taken off by its own x. */
async function removeTile(id: string): Promise<void> {
  tile(id).querySelector<HTMLButtonElement>('.dc-tile-remove')!.click();
  await settle();
}
/** The row groups a grid's tile shows. */
const rowChips = (id: string): string[] => [...tile(id).querySelectorAll<HTMLElement>('.dc-zone-rows .dc-chip')].map((c) => c.dataset['column'] ?? '');
async function ungroup(id: string, column: string): Promise<void> {
  (tile(id).querySelector(`.dc-zone-rows .dc-chip[data-column="${column}"] .dc-chip-remove`) as HTMLElement).click();
  await settle();
}
/** A grid's Column Properties for `column`, opened from its header in tile `at`, as a person opens it. */
async function columnProperties(at: string, column: string): Promise<HTMLElement> {
  const th = tile(at).querySelector<HTMLElement>(`.dc-th[data-column="${column}"]`);
  assert.ok(th, `${at} shows a ${column} header`);
  th.dispatchEvent(new dom.window.MouseEvent('contextmenu', { bubbles: true, cancelable: true }));
  await settle();
  const item = menuItems().find((i) => label(i) === 'Properties...');
  assert.ok(item, 'the header offers Properties...');
  item.click();
  await settle();
  const win = dom.window.document.querySelector<HTMLElement>('[data-window="Properties"]');
  assert.ok(win, 'the Properties window');
  assert.equal(win.querySelector('.dc-editor-tab[aria-selected="true"]')?.textContent, 'Column Properties');
  assert.equal(win.querySelector<HTMLElement>('.dc-pe-col.dc-on')?.dataset['column'], column);
  return win;
}
const aggregationField = (win: HTMLElement): HTMLSelectElement =>
  [...win.querySelectorAll('.dc-field')].find((f) => f.querySelector('.dc-field-label')?.textContent === 'Aggregation:')!
    .querySelector('select') as HTMLSelectElement;
const footerButton = (win: HTMLElement, text: string): HTMLButtonElement =>
  [...win.querySelectorAll<HTMLButtonElement>('.dc-editor-footer button')].find((b) => b.textContent === text)!;
/** What a grid's Column Properties says a column's Aggregation is ('' for its kind's default); closed again by Cancel. */
async function aggregationShown(at: string, column: string): Promise<string> {
  const win = await columnProperties(at, column);
  const shown = aggregationField(win).value;
  footerButton(win, 'Cancel').click();
  await settle();
  return shown;
}
/** A column's Aggregation set in a grid's Column Properties, and OK. */
async function setAggregation(at: string, column: string, fn: string): Promise<void> {
  const win = await columnProperties(at, column);
  const select = aggregationField(win);
  select.value = fn;
  assert.equal(select.value, fn, `Column Properties offers ${fn}`);
  select.dispatchEvent(new dom.window.Event('change'));
  footerButton(win, 'OK').click();
  await settle();
  assert.equal(dom.window.document.querySelector('[data-window="Properties"]'), null, 'OK closed the window');
}
/** A page of two grids, `a` over trades and `b` over orders, beside it. */
async function twoGrids(extra: Partial<PageAppOptions> = {}): Promise<{ a: string; b: string }> {
  newPage(extra);
  const a = pg().addGrid(over('trades.csv'));
  const b = pg().addGrid(over('orders.csv'), { near: a });
  await settle();
  return { a, b };
}
/** Charts of grid `b`, frozen, then `b` removed: each chart DETACHED, its grid kept off the board for it. */
async function detached(b: string, n = 1): Promise<string[]> {
  const charts: string[] = [];
  for (let i = 0; i < n; i += 1) charts.push(await chartOf(b));
  for (const chart of charts) await togglePill(chart);
  await removeTile(b);
  for (const chart of charts) assert.equal(pill(chart), 'Detached');
  return charts;
}

/**
 * A chart's mark clicked, as ECharts tells the chart of a click (jsdom draws nothing to click on): series `series`, its
 * `index`th category. The fixture's rows draw EMEA and AMER across, desk A and B as the series.
 */
function clickMark(chartTile: HTMLElement, series: number, index: number): void {
  const drawn = getInstanceByDom(chartTile.querySelector('.dc-chartpanel-chart') as HTMLElement);
  assert.ok(drawn, 'the chart is drawn');
  (drawn as unknown as { trigger(name: string, params: unknown): void }).trigger('click', { seriesIndex: series, dataIndex: index });
}

/** A chart redraws a moment after what it reads changes (ui/chart-panel.ts debounces, 150 ms): that moment passed. */
async function drawn(): Promise<void> {
  await new Promise((r) => setTimeout(r, 200));
  await settle();
}

const SUM: ChartMeasure = { column: 'notional', fn: 'sum' };
const MAX: ChartMeasure = { column: 'notional', fn: 'max' };

describe('Show Grid on a DETACHED chart: a grid of its own grouping, which it follows from then on (D7)', () => {
  beforeEach(setUpPage);
  afterEach(tearDownPage);

  it('its values as they were -- its grid\'s Aggregation had moved on -- said in its own grid\'s Column Properties; the cube it kept let go', async () => {
    const { a, b } = await twoGrids();
    const chart = await chartOf(b);
    await togglePill(chart);
    // the grid's Aggregation of notional moves on after the freeze: the frozen chart keeps its sum, and the grid kept
    // for it once it is detached carries the average
    await setAggregation(b, 'notional', 'average');
    assert.equal(await aggregationShown(b, 'notional'), 'average');
    assert.deepEqual(valuesOf(chart), [SUM]);
    await removeTile(b);
    assert.equal(pill(chart), 'Detached');
    const { frozen: _frozen, ...before } = chartView(chart)!.spec;
    await showGrid(chart);
    const own = chartView(chart)!.cube;
    assert.notEqual(own, b);
    assert.deepEqual(pg().grids, [a, own], 'a grid of its own, on the page');
    assert.deepEqual(pg().cubes, [a, own], 'the cube it kept for the removed grid let go');
    assert.equal(sheetOf(own), sheetOf(chart), 'beside it');
    // its own grid's Column Properties say the chart's sum, not the kept grid's average; grouped as the chart is
    assert.equal(await aggregationShown(own, 'notional'), 'sum');
    assert.deepEqual(rowChips(own), ['region', 'desk']);
    assert.deepEqual(chartView(chart)!.spec, before, 'the chart as it was -- its values, its grouping -- following now');
    assert.equal(pill(chart), 'Dynamic');
  });

  it('two values of one column (a sum and a max) both come back, carried by its own grid\'s calculated copy, and stay as it follows', async () => {
    const { a, b } = await twoGrids();
    const chart = await chartOf(b);
    await togglePill(chart);
    // a chart of two values of one column, as a saved page has it (the panel's form makes one too)
    const doc = pg().document('Q3')!;
    reopen({ ...doc, views: doc.views.map((v) => (v.kind === 'chart' ? { ...v, spec: { ...v.spec, y: [SUM, MAX] } } : v)) });
    await settle();
    assert.deepEqual(valuesOf(chart), [SUM, MAX]);
    await removeTile(b);
    await showGrid(chart);
    const own = chartView(chart)!.cube;
    assert.deepEqual(valuesOf(chart), [SUM, MAX], 'both values as they were');
    // the max is carried by a calculated copy of notional, made once
    assert.deepEqual(pg().grid(own)!.snapshot.derived.map((d) => d.name), ['max_notional']);
    assert.deepEqual(pg().cubes, [a, own]);
    // its grid regrouped: the chart follows its grouping and keeps both values
    await ungroup(own, 'desk');
    assert.equal(chartView(chart)!.spec.split, undefined, 'regrouped as its grid is');
    assert.deepEqual(valuesOf(chart), [SUM, MAX], 'still both');
  });

  it('an Aggregation changed in its own grid\'s Column Properties reaches the chart, which follows it, saved and reopened', async () => {
    const { a, b } = await twoGrids();
    const [chart] = await detached(b);
    await showGrid(chart!);
    const own = chartView(chart!)!.cube;
    await setAggregation(own, 'notional', 'max');
    assert.deepEqual(valuesOf(chart!), [MAX], 'the chart takes the max');
    assert.equal(chartView(chart!)!.spec.frozen, undefined, 'following its grid');
    // saved and reopened: it follows its own grid, which says the max
    reopen(pg().document('Q3')!);
    await settle();
    assert.equal(pill(chart!), 'Dynamic');
    assert.deepEqual(valuesOf(chart!), [MAX]);
    assert.deepEqual(pg().cubes, [a, own]);
    assert.equal(await aggregationShown(own, 'notional'), 'max');
  });

  it('two charts detached from one grid: Show Grid gives one a grid of its own; the other keeps the removed grid\'s cube, until it goes', async () => {
    const { a, b } = await twoGrids();
    const [x, y] = await detached(b, 2);
    await showGrid(x!);
    const own = chartView(x!)!.cube;
    assert.equal(pill(x!), 'Dynamic');
    assert.equal(pill(y!), 'Detached', 'the other as it was');
    assert.equal(chartView(y!)!.cube, b, 'and still reads the removed grid\'s cube');
    assert.deepEqual([...pg().cubes].sort(), [a, b, own].sort());
    // the removed grid's last chart gone, the cube kept for it goes too; the new grid stays
    await removeTile(y!);
    assert.deepEqual([...pg().cubes].sort(), [a, own].sort());
  });
});

describe('Show Grid on a following chart and on a frozen one', () => {
  beforeEach(setUpPage);
  afterEach(tearDownPage);

  it('a following chart: the grid it follows shown, on its sheet -- nothing told, the page as it was', async () => {
    const told: PageCommand[] = [];
    const { a, b } = await twoGrids({ onPageCommand: (c) => told.push(c) });
    const chart = await chartOf(a);
    // the chart on a sheet of its own, shown: its grid is on the first
    const first = pg().shownSheet;
    chartMenu(chart).find((i) => label(i) === 'New Sheet')!.click();
    await settle();
    pg().showSheet(sheetOf(chart)!);
    await settle();
    assert.notEqual(pg().shownSheet, first);
    const before = pg().document('Q3')!;
    const labelBefore = pg().undoLabel;
    told.length = 0;
    await showGrid(chart);
    assert.equal(pg().shownSheet, first, 'its grid\'s sheet shown');
    assert.deepEqual(told, [], 'nothing told');
    assert.equal(pg().undoLabel, labelBefore, 'no step');
    assert.deepEqual(pg().document('Q3'), before, 'the page as it was');
    assert.deepEqual(pg().grids, [a, b]);
    assert.equal(pill(chart), 'Dynamic');
  });

  it('a frozen chart: one step makes a grid of its own grouping beside it, which it follows; the grid it was frozen on untouched', async () => {
    let changes = 0;
    const told: PageCommand[] = [];
    newPage({ onChange: () => { changes += 1; }, onPageCommand: (c) => told.push(c) });
    const a = pg().addGrid(over('trades.csv'));
    await settle();
    const chart = await chartOf(a);
    await togglePill(chart);
    // its grid moves on; the frozen chart keeps region by desk
    await ungroup(a, 'desk');
    assert.equal(chartView(chart)!.spec.split, 'desk');
    const n = told.length;
    const was = changes;
    await showGrid(chart);
    const own = chartView(chart)!.cube;
    assert.notEqual(own, a);
    assert.deepEqual(told.slice(n).map((c) => c.kind), ['addGrid', 'followGrid', 'setChart'],
      'one step: the grid added, the chart on its cube, following it');
    assert.equal(pg().undoLabel, 'Show Grid');
    assert.ok(changes > was, 'the page is told it changed');
    assert.deepEqual(rowChips(own), ['region', 'desk'], 'grouped as the chart is');
    assert.equal(sheetOf(own), sheetOf(chart), 'beside it');
    assert.equal(chartView(chart)!.spec.frozen, undefined, 'following it');
    assert.equal(chartView(chart)!.spec.split, 'desk', 'drawn as it was');
    assert.equal(pill(chart), 'Dynamic');
    assert.deepEqual(rowChips(a), ['region'], 'the grid it was frozen on never moved');
    // following its own grid: an Aggregation changed in its Column Properties reaches it; its old grid's no longer does
    await setAggregation(own, 'notional', 'max');
    assert.deepEqual(valuesOf(chart), [MAX]);
    await setAggregation(a, 'notional', 'average');
    assert.deepEqual(valuesOf(chart), [MAX], 'its old grid no longer moves it');
    assert.equal(await aggregationShown(a, 'notional'), 'average');
  });

  it('a frozen chart filtering its grid: its selection taken off that grid\'s filter, in the same step, and never on its own grid\'s', async () => {
    const told: PageCommand[] = [];
    newPage({ onPageCommand: (c) => told.push(c) });
    const a = pg().addGrid(over('trades.csv'));
    await settle();
    const chart = await chartOf(a);
    await drawn();
    await togglePill(chart);
    clickMark(tile(chart), 0, 0);
    await settle();
    assert.ok(pg().grid(a)!.snapshot.filter && chartView(chart)!.selection, 'the click filtered its grid');
    const n = told.length;
    await showGrid(chart);
    const own = chartView(chart)!.cube;
    // one step: its selection off its old grid by followGrid's own rule, as Python's chart.grid() takes it off (the S7
    // audit's Y-1), that grid following the step
    assert.deepEqual(told.slice(n).map((c) => c.kind), ['addGrid', 'followGrid', 'setChart']);
    assert.equal(pg().undoLabel, 'Show Grid');
    assert.equal(chartView(chart)!.selection, undefined, 'the chart selects nothing');
    assert.equal(pg().grid(a)!.snapshot.filter, undefined, 'off its old grid\'s filter');
    assert.equal(pg().grid(own)!.snapshot.filter, undefined, 'never on its own grid');
    assert.equal((tile(chart).querySelector('.dc-tile-chip') as HTMLElement).hidden, true, 'its chip gone');
  });

  it('shown again, a chart shows the grid it follows now, on that grid\'s sheet: never a second one', async () => {
    newPage();
    const a = pg().addGrid(over('trades.csv'));
    await settle();
    const chart = await chartOf(a);
    await togglePill(chart);
    await showGrid(chart);
    const own = chartView(chart)!.cube;
    const first = pg().shownSheet;
    chartMenu(chart).find((i) => label(i) === 'New Sheet')!.click();
    await settle();
    pg().showSheet(sheetOf(chart)!);
    await settle();
    assert.notEqual(pg().shownSheet, first);
    await showGrid(chart);
    assert.equal(pg().shownSheet, first, 'its grid\'s sheet shown');
    assert.deepEqual(pg().grids, [a, own], 'no second grid');
  });
});

describe('a chart\'s right-click menu: Options..., and Show Grid offered only where it shows a grid', () => {
  beforeEach(setUpPage);
  afterEach(tearDownPage);

  it('Options... shows the chart\'s form, and again hides it', async () => {
    newPage();
    const a = pg().addGrid(over('trades.csv'));
    await settle();
    const chart = await chartOf(a);
    const form = (): HTMLElement => tile(chart).querySelector('.dc-chartpanel-form') as HTMLElement;
    assert.equal(form().hidden, true, 'a chart on the page starts without its form');
    chartMenu(chart).find((i) => label(i) === 'Options...')!.click();
    assert.equal(form().hidden, false);
    chartMenu(chart).find((i) => label(i) === 'Options...')!.click();
    assert.equal(form().hidden, true);
  });

  it('a frozen KPI with no column across: Show Grid is not offered when it would show nothing', async () => {
    // A KPI may have no column across (its trend: chart-spec.ts chartProblems). Show Grid groups a frozen chart's own
    // grid by its column across (chart-spec.ts chartGrid, ownGridRefusal: none without one), so the menu should not
    // offer it -- as it does not for a gauge, which never has one.
    newPage();
    const a = pg().addGrid(over('trades.csv'));
    await settle();
    const chart = await chartOf(a);
    await togglePill(chart);
    const doc = pg().document('Q3')!;
    reopen({ ...doc, views: doc.views.map((v) => {
      if (v.kind !== 'chart') return v;
      const { x: _x, split: _split, ...kpi } = v.spec;
      return { ...v, spec: { ...kpi, mark: 'kpi' as const } };
    }) });
    await settle();
    assert.equal(chartView(chart)!.spec.mark, 'kpi');
    assert.equal(chartView(chart)!.spec.x, undefined);
    assert.equal(pill(chart), 'Frozen');
    const item = chartMenu(chart).find((i) => label(i) === 'Show Grid');
    assert.ok(item, 'the entry is in the menu');
    const offered = item.getAttribute('aria-disabled') !== 'true';
    const grids = pg().grids.length;
    item.click();
    await settle();
    const opened = pg().grids.length > grids;
    // offered exactly when it makes one (either way out of the mismatch: the entry disabled, or a grid of its own)
    assert.equal(offered, opened, `offered: ${String(offered)}, but clicked it made ${opened ? 'a' : 'no'} grid`);
    assert.equal(offered, false);
  });

  it('a map: frozen, Show Grid is not offered (its places are its own columns, no grouping a grid holds); following, it shows its grid; its pill says so', async () => {
    newPage();
    const a = pg().addGrid(over('trades.csv'));
    await settle();
    const chart = await chartOf(a);
    const doc = pg().document('Q3')!;
    reopen({ ...doc, views: doc.views.map((v) => (v.kind !== 'chart' ? v : { ...v, spec: { ...v.spec, mark: 'map' as const, frozen: true } })) });
    await settle();
    assert.equal(chartView(chart)!.spec.mark, 'map');
    assert.equal(offersShowGrid(chart), false);
    await togglePill(chart);
    assert.equal(offersShowGrid(chart), true, 'following its grid: that grid shown');
    const title = tile(chart).querySelector<HTMLElement>('.dc-tile-actions .dc-titlebar-toggle')!.title;
    assert.match(title, /a map is placed by its own columns, not the grid's grouping/);
  });

  it('a frozen scatter: Show Grid is not offered (its marks are rows, no grouping of its own); clicked anyway, nothing is made', async () => {
    newPage();
    const a = pg().addGrid(over('trades.csv'));
    await settle();
    const chart = await chartOf(a);
    const doc = pg().document('Q3')!;
    reopen({ ...doc, views: doc.views.map((v) => {
      if (v.kind !== 'chart') return v;
      const { split: _split, ...rest } = v.spec;
      return { ...v, spec: { ...rest, mark: 'scatter' as const, x: 'notional', frozen: true } };
    }) });
    await settle();
    assert.equal(chartView(chart)!.spec.mark, 'scatter');
    assert.equal(pill(chart), 'Frozen');
    assert.equal(offersShowGrid(chart), false);
    chartMenu(chart).find((i) => label(i) === 'Show Grid')!.click();
    await settle();
    assert.deepEqual(pg().grids, [a], 'no grid made');
    assert.equal(chartView(chart)!.spec.frozen, true, 'the chart as it was');
  });
});

describe('sheets: tiles dragged onto tabs, sheets deleted and reopened, one page undo', () => {
  beforeEach(setUpPage);
  afterEach(tearDownPage);

  /** Each tab at its own place along the bar (jsdom lays nothing out): tab i from x = 100 + 100i, 80 wide, 20 high. */
  function placeTabs(): void {
    [...host.querySelectorAll<HTMLElement>('.dc-sheet-tab')].forEach((tab, i) => {
      const left = 100 + 100 * i;
      tab.getBoundingClientRect = () => ({ left, right: left + 80, top: 0, bottom: 20, x: left, y: 0, width: 80, height: 20, toJSON: () => ({}) });
    });
  }
  const pointer = (el: Element, type: string, x: number, y: number): void => {
    el.dispatchEvent(new dom.window.PointerEvent(type, { pointerId: 1, clientX: x, clientY: y, button: 0, bubbles: true }));
  };
  const marked = (): string[] => [...host.querySelectorAll<HTMLElement>('.dc-sheet-tab-target')].map((t) => t.dataset['sheet'] ?? '');

  it('a tile dragged by its title bar onto another sheet\'s tab moves there; onto its own sheet\'s tab, nothing moves', async () => {
    newPage();
    const a = pg().addGrid(over('trades.csv'));
    await settle();
    const chart = await chartOf(a);
    (host.querySelector('.dc-sheet-add') as HTMLElement).click();
    await settle();
    const [first, second] = pg().sheets;
    clickTab('trades.csv');
    await settle();
    placeTabs();
    const head = tile(chart).querySelector<HTMLElement>('.dc-tile-head')!;
    pointer(head, 'pointerdown', 600, 300);
    pointer(head, 'pointermove', 240, 10);
    assert.deepEqual(marked(), [second], 'the tab under the pointer is marked while the tile is over it');
    pointer(head, 'pointerup', 240, 10);
    await settle();
    assert.deepEqual(marked(), [], 'and unmarked once it is let go');
    assert.equal(sheetOf(chart), second, 'the chart is on the second sheet');
    assert.equal(pg().shownSheet, first, 'the sheet shown stays shown');
    assert.equal(host.querySelector(`.dc-sheet[data-sheet="${first}"] .dc-board-live`)?.textContent,
      `${chartView(chart)!.title} moved to Sheet 2.`, 'the board says where it went');
    // the grid onto its own sheet's tab: no tab marked, nothing moved
    placeTabs();
    const gridHead = tile(a).querySelector<HTMLElement>('.dc-tile-head')!;
    pointer(gridHead, 'pointerdown', 600, 300);
    pointer(gridHead, 'pointermove', 140, 10);
    assert.deepEqual(marked(), []);
    pointer(gridHead, 'pointerup', 140, 10);
    await settle();
    assert.equal(sheetOf(a), first);
    assert.deepEqual(pg().views().sheets.map((s) => tilesOf(s.layout)), [[a], [chart]]);
  });

  it('a sheet deleted with its grid: that grid\'s following chart elsewhere goes, its frozen one stays, detached; the next sheet is shown', async () => {
    const { a, b } = await twoGrids({ confirm: () => true });
    const following = await chartOf(b);
    const frozen = await chartOf(b);
    await togglePill(frozen);
    (host.querySelector('.dc-sheet-add') as HTMLElement).click();
    await settle();
    clickTab('trades.csv');
    await settle();
    // grid b onto the second sheet, which then goes first: b's charts stay on the other sheet with grid a
    gridMenuItem(b, 'Sheet 2').click();
    await settle();
    tabMenu('orders.csv', 'Move Left');
    await settle();
    assert.deepEqual(tabs(), ['orders.csv', 'trades.csv']);
    clickTab('orders.csv');
    await settle();
    tabMenu('orders.csv', 'Delete');
    await settle();
    assert.deepEqual(tabs(), ['trades.csv'], 'the sheet is gone');
    assert.equal(pg().shownSheet, sheetOf(a), 'the sheet after it is shown');
    assert.deepEqual(pg().grids, [a], 'its grid went with it');
    assert.equal(tile(following), null, 'the chart following that grid went too, from the other sheet');
    assert.equal(pill(frozen), 'Detached', 'the frozen one stays, reading its own copy of the grid\'s query');
    assert.deepEqual(pg().cubes, [a, b], 'the grid kept off the board for it');
  });

  it('Move to Sheet > New Sheet puts the tile on a sheet of its own; a sheet\'s name cleared, it is named after what it shows again', async () => {
    newPage();
    const a = pg().addGrid(over('trades.csv'));
    await settle();
    const chart = await chartOf(a);
    const first = pg().shownSheet;
    const item = chartMenu(chart).find((i) => label(i) === 'New Sheet');
    assert.ok(item, 'the chart\'s Move to Sheet > New Sheet');
    item.click();
    await settle();
    assert.equal(pg().sheets.length, 2);
    assert.equal(sheetOf(chart), pg().sheets[1], 'the chart on the new sheet');
    assert.equal(pg().shownSheet, first, 'the sheet shown stays shown');
    assert.deepEqual(tabs(), ['trades.csv', 'Sheet 2']);
    // the new sheet named, then its name cleared: its tab says its place again
    const renameTab = (from: string, to: string): void => {
      tabOf(from).dispatchEvent(new dom.window.MouseEvent('dblclick', { bubbles: true }));
      const field = host.querySelector<HTMLInputElement>('.dc-sheet-tab input');
      assert.ok(field, 'the tab\'s name field');
      field.value = to;
      field.dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: 'Enter' }));
    };
    renameTab('Sheet 2', 'Charts');
    await settle();
    assert.deepEqual(tabs(), ['trades.csv', 'Charts']);
    assert.equal(pg().views().sheets[1]!.name, 'Charts');
    renameTab('Charts', '   ');
    await settle();
    assert.deepEqual(tabs(), ['trades.csv', 'Sheet 2']);
    assert.equal(pg().views().sheets[1]!.name, undefined, 'no name of its own saved');
  });

  it('ready waits for every grid\'s first view -- a grid kept off the board too -- and for a refused one as well', async () => {
    const { b } = await twoGrids();
    await detached(b);
    const doc = pg().document('Q3')!;
    engine.gate.hold = true;
    reopen(doc);
    let ready = false;
    void pg().ready().then(() => { ready = true; });
    await settle();
    assert.equal(ready, false, 'its grids\' first queries are held');
    assert.ok(engine.gate.held.length >= 2, 'the grid on the board and the one kept off it both asked');
    // the first query of each refused: the page has still opened
    engine.gate.fail = 'the file is gone';
    engine.gate.releaseAll();
    await settle();
    assert.equal(ready, true);
  });

  it('a page whose first sheet was deleted reopens with its own sheets alone, on its first', async () => {
    newPage({ confirm: () => true });
    pg().addGrid(over('trades.csv'));
    await settle();
    (host.querySelector('.dc-sheet-add') as HTMLElement).click();
    const b = pg().addGrid(over('orders.csv'));
    await settle();
    const second = pg().shownSheet;
    tabMenu('trades.csv', 'Delete');
    await settle();
    assert.deepEqual(pg().sheets, [second]);
    reopen(pg().document('Q3')!, { confirm: () => true });
    await settle();
    // the board a page starts with has a first sheet of its own: no such sheet is left behind, empty, in the tabs
    assert.deepEqual(pg().sheets, [second]);
    assert.deepEqual(tabs(), ['orders.csv']);
    assert.equal(pg().shownSheet, second);
    assert.deepEqual(pg().grids, [b]);
    assert.equal(sheetOf(b), second);
  });

  it('one page undo for every sheet, step by step: a sheet added is undone first, then the arrangement before it', async () => {
    newPage();
    const a = pg().addGrid(over('trades.csv'));
    await settle();
    const chart = await chartOf(a);
    const entry = (text: string): HTMLElement => {
      const item = pageMenu().find((i) => label(i) === text);
      assert.ok(item, `the page's menu offers ${text}`);
      return item;
    };
    const bands = (): string[][] => pg().views().sheets[0]!.layout.bands.map((band) => tilesOf({ fit: false, bands: [band] }));
    const sideBySide = bands();
    entry('Arrange…').click();
    await settle();
    const stacked = host.querySelector<HTMLButtonElement>('.dc-layout-picker [data-preset="stacked"]');
    assert.ok(stacked, 'the layouts');
    stacked.click();
    await settle();
    assert.deepEqual(bands(), [[a], [chart]]);
    (host.querySelector('.dc-sheet-add') as HTMLElement).click();
    await settle();
    assert.equal(pg().sheets.length, 2);
    // the page's Undo names its step, whichever sheet is shown
    entry('Undo Add Sheet').click();
    await settle();
    assert.equal(pg().sheets.length, 1, 'the sheet added is taken off');
    entry('Undo Layout Change').click();
    await settle();
    assert.deepEqual(bands(), sideBySide, 'then the arrangement before it');
    // before them, the chart and the grid added: each a step, back to the page as it began
    entry('Undo Add Chart').click();
    await settle();
    assert.equal(tile(chart), null, 'the chart taken off');
    entry('Undo Add Grid').click();
    await settle();
    assert.ok(pg().empty, 'the page as it began');
    assert.equal(entry('Undo').getAttribute('aria-disabled'), 'true', 'nothing before it');
    // and done again: the grid made again by its maker, under its id, its chart beside it, the arrangement and the sheet
    for (const step of ['Redo Add Grid', 'Redo Add Chart', 'Redo Layout Change', 'Redo Add Sheet']) {
      entry(step).click();
      await settle();
    }
    assert.deepEqual(pg().grids, [a]);
    assert.ok(tile(chart), 'the chart back');
    assert.deepEqual(bands(), [[a], [chart]]);
    assert.equal(pg().sheets.length, 2);
    assert.equal(entry('Redo').getAttribute('aria-disabled'), 'true');
  });
});

describe('the page\'s undo and redo: one page state (page-state.ts), each structural edit one step', () => {
  beforeEach(setUpPage);
  afterEach(tearDownPage);

  const viewsOf = (page: PageDocument) => ({ views: page.views, sheets: page.sheets });
  /** The page as it is saved: what the state must be, and what an undo must put back. */
  const saved = (): PageDocument => pg().document('Q3')!;

  /**
   * An edit as one step (docs/DATACUBE_PAGE_STATE_2026_10_18.md): made, the state is the page on screen and what the
   * screen told, applied to the page before it, is the page after it; undone, the page is as it was before it (its
   * views, its sheets, its cubes); redone, as it was after it -- the state the page on screen at each.
   */
  async function step(what: string, told: PageCommand[], edit: () => Promise<void> | void, label: string): Promise<void> {
    const before = saved();
    const n = told.length;
    const labelBefore = pg().undoLabel;
    await edit();
    await drawn();
    const after = saved();
    assert.deepEqual(pg().state.page, after, `${what}: the state is the page on screen`);
    assert.equal(pg().undoLabel, label, `${what}: one step, named`);
    const replayed = applyPageCommands({ page: before, held: idsOf(before) }, told.slice(n)).draft.page;
    assert.deepEqual(viewsOf(replayed), viewsOf(after), `${what}: what it told replays to the page it made`);
    assert.deepEqual(replayed.cubes.map((c) => c.id), after.cubes.map((c) => c.id), `${what}: and to its cubes (a grid added, its document)`);
    pg().undo();
    await drawn();
    assert.equal(pg().undoLabel, labelBefore, `${what}, undone: one step, no other told with it`);
    assert.deepEqual(viewsOf(saved()), viewsOf(before), `${what}, undone: the page as it was`);
    assert.deepEqual(saved().cubes, before.cubes, `${what}, undone: its cubes as they were`);
    assert.deepEqual(pg().state.page, saved(), `${what}, undone: the state is the page on screen`);
    pg().redo();
    await drawn();
    assert.deepEqual(viewsOf(saved()), viewsOf(after), `${what}, redone`);
    assert.deepEqual(saved().cubes, after.cubes, `${what}, redone: its cubes`);
    assert.deepEqual(pg().state.page, saved(), `${what}, redone: the state is the page on screen`);
  }

  /** A tile renamed in place: its title bar double-clicked, a name typed, Enter. */
  function rename(id: string, name: string): void {
    tile(id).querySelector('.dc-tile-head')!.dispatchEvent(new dom.window.MouseEvent('dblclick', { bubbles: true }));
    const field = tile(id).querySelector<HTMLInputElement>('.dc-tile-rename');
    assert.ok(field, `${id}'s rename field`);
    field.value = name;
    field.dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: 'Enter' }));
  }

  it('every edit of its structure undone and redone: the page as it was, then as it is, the state the page on screen', async () => {
    const told: PageCommand[] = [];
    const { a, b } = await twoGrids({ confirm: () => true, onPageCommand: (c) => told.push(c) });
    let chart = '';
    await step('a chart added', told, async () => { chart = await chartOf(a); }, 'Add Chart');
    let other = '';
    await step('another added', told, async () => { other = await chartOf(b); }, 'Add Chart');
    await step('a chart frozen', told, () => togglePill(other), 'Chart Change');
    await step('a tile renamed', told, () => rename(chart, 'Desk mix'), 'Rename Tile');
    await step('a grid renamed', told, () => rename(b, 'Orders'), 'Rename Tile');
    await step('a sheet added', told, () => (host.querySelector('.dc-sheet-add') as HTMLElement).click(), 'Add Sheet');
    clickTab('trades.csv');
    await settle();
    await step('a grid moved to the sheet', told, () => gridMenuItem(b, 'Sheet 2').click(), 'Move to Sheet');
    await step('a chart moved to a new sheet', told, () => chartMenu(chart).find((i) => label(i) === 'New Sheet')!.click(), 'Move to Sheet');
    await step('a sheet moved', told, () => tabMenu('Sheet 3', 'Move Left'), 'Move Sheet');
    await step('a sheet renamed', told, () => {
      tabOf('Sheet 2').dispatchEvent(new dom.window.MouseEvent('dblclick', { bubbles: true }));
      const field = host.querySelector<HTMLInputElement>('.dc-sheet-tab input')!;
      field.value = 'Charts';
      field.dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: 'Enter' }));
    }, 'Rename Sheet');
    await step('a sheet arranged', told, async () => {
      clickTab('trades.csv');
      await settle();
      pageMenu().find((i) => label(i) === 'Arrange…')!.click();
      await settle();
      host.querySelector<HTMLButtonElement>('.dc-layout-picker [data-preset="stacked"]')!.click();
    }, 'Layout Change');
    let copy = '';
    await step('a grid copied', told, async () => {
      const had = new Set(pg().grids);
      gridMenuItem(a, 'Copy of Grid').click();
      await settle();
      copy = pg().grids.find((g) => !had.has(g))!;
    }, 'Add Grid');
    // b, its frozen chart beside it: removed, the chart detached; undone, b made again and the chart following it
    await step('a grid removed, its frozen chart detached', told, () => removeTile(b), 'Remove Orders');
    assert.equal(pill(other), 'Detached', 'redone: its grid removed again, the chart detached');
    pg().undo();
    await drawn();
    assert.equal(pill(other), 'Frozen', 'undone: its grid made again, and the chart reads it, not a kept copy');
    assert.deepEqual([...pg().cubes], pg().grids, 'no grid kept off the board any more');
    pg().redo();
    await drawn();
    await step('a chart removed', told, () => removeTile(chart), 'Remove Desk mix');
    await step('the copy moved to a named sheet', told, () => gridMenuItem(copy, 'Charts').click(), 'Move to Sheet');
    await step('a sheet deleted with its tiles', told, () => tabMenu('Charts', 'Delete'), 'Delete Sheet');
  });

  it('a chart removed while it filters its grid: undone, its selection is back on the grid\'s filter', async () => {
    const { a } = await twoGrids();
    const chart = await chartOf(a);
    await drawn();
    clickMark(tile(chart), 0, 0);
    await settle();
    const filtered = pg().grid(a)!.snapshot.filter;
    const selection = chartView(chart)!.selection;
    assert.ok(filtered && selection, 'the click filtered the grid, the chart\'s selection');
    await removeTile(chart);
    assert.equal(pg().grid(a)!.snapshot.filter, undefined, 'removed, it took its selection off');
    pg().undo();
    await drawn();
    assert.deepEqual(chartView(chart)!.selection, selection, 'the chart back with its selection');
    assert.deepEqual(pg().grid(a)!.snapshot.filter, filtered, 'its selection back on the grid\'s filter, by the grid\'s own change');
    pg().redo();
    await drawn();
    assert.equal(pg().grid(a)!.snapshot.filter, undefined, 'redone: taken off again');
  });

  it('a locked page undoes nothing; Ctrl+Z on a tile\'s frame is the page\'s undo', async () => {
    const { b } = await twoGrids();
    await removeTile(b);
    pg().setLayoutEditing(false);
    assert.equal(pg().undoLabel, undefined, 'locked: nothing moves');
    pg().undo();
    await settle();
    assert.equal(pg().grids.length, 1);
    pg().setLayoutEditing(true);
    assert.equal(pg().undoLabel, 'Remove Grid 2');
    const frame = host.querySelector<HTMLElement>('.dc-band-tile')!;
    frame.dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: 'z', ctrlKey: true, bubbles: true }));
    await drawn();
    assert.equal(pg().grids.length, 2, 'the grid back');
  });

  // -- the phase 2 audit's findings, each pinned ------------------------------------------------------------------

  it("a grid's source is kept while an undo can bring the grid back, and let go once none can (the audit's 1)", async () => {
    const forgotten: string[] = [];
    const disposed: string[] = [];
    const services = { engine: { database: { forgetFile: async (f: string, t: string) => { forgotten.push(`${f}:${t}`); } } } } as never;
    let sources: ReturnType<typeof pageSources> | undefined;
    const planner = Object.assign(new StubPlanner(), { dispose: () => { disposed.push('orders'); } });
    const place = { planner, label: 'orders.csv', cubeSource: file('orders.csv'),
      file: { file: new dom.window.File(['x'], 'orders.csv') as unknown as File, table: 'orders' }, tables: ['orders'] };
    // as DataCube's app makes a grid over a file: its planner, its file's table, told to its sources
    const orders: GridMaker = (gridHost, spawned, start) => {
      const grid = new CubeApp(gridHost, start?.snapshot ?? SNAPSHOT, {
        engine, planner, compact: true, cubeSource: place.cubeSource, sourceLabel: 'orders.csv',
        configuration: start?.configuration ?? { ...DEFAULT_CONFIGURATION, reportTitle: 'orders.csv' }, ...spawned,
      });
      if (spawned.id !== undefined) sources?.madeGrid(spawned.id, place as never);
      return grid;
    };
    newPage({ onChange: () => sources?.dropUnread() });
    sources = pageSources({ services, page: pg(), gridMaker: (() => { throw new Error('unused'); }) as never, taken: [], ask: () => {} });
    const a = pg().addGrid(over('trades.csv'));
    const b = pg().addGrid(orders, { near: a });
    await settle();
    await removeTile(b);
    assert.deepEqual([forgotten, disposed], [[], []], 'removed, its table and planner kept: an undo can bring it back');
    pg().undo();
    await drawn();
    assert.deepEqual(pg().grids, [a, b]);
    pg().redo();
    await drawn();
    (host.querySelector('.dc-sheet-add') as HTMLElement).click();
    await settle();
    assert.deepEqual([forgotten, disposed], [[], []], 'removed again: still a step an undo takes back');
    // the page cleared: no step can bring it back, and what it read goes
    pg().clear();
    await settle();
    assert.deepEqual(forgotten, ['orders.csv:orders']);
    assert.deepEqual(disposed, ['orders']);
  });

  it("on a host's page, a saved grid over a file is opened by the host over the table it holds, what it changed said (the S7 audit's H-2, H-3)", async () => {
    // the host's own answer (headless gridOpened through Python's opened.json): the cube as it is now, taken
    const asked: string[] = [];
    const now = writeCube({ name: 'Q3', source: file('trades.csv'), snapshot: SNAPSHOT, configuration: DEFAULT_CONFIGURATION, tree: TreeState.empty() });
    const services = {
      engine: {
        rows: () => ({ planner: new StubPlanner(), engine }),
        database: {
          opened: async (doc: string) => {
            asked.push(doc);
            return { cube: cubeToJson(now), notes: ['qty: left out'], taken: true, model: 'm', runtime: 'r', how: {},
              relation: SNAPSHOT.source.query, frames: {} };
          },
        },
      },
    } as never;
    newPage();
    const sources = pageSources({ services, page: pg(), gridMaker: (() => () => undefined) as never, taken: [], ask: () => {} });
    // asked of the host, never of the person (a file it holds is no file to choose: a tab would wait on a hidden button)
    const opened = await Promise.race([
      sources.reopened(now, undefined, false, undefined, true),
      new Promise<'waiting'>((done) => { setTimeout(() => done('waiting'), 2000); }),
    ]);
    assert.notEqual(opened, 'waiting', 'opened by the host, not waiting on a file to be chosen');
    assert.equal(asked.length, 1);
    assert.equal(opened !== 'waiting' && opened?.taken, true, "what the host's opening changed: told as the page's rebuilt");
    assert.equal(opened !== 'waiting' ? opened?.notes[0] : '', 'qty: left out', "the host's notes said first");
  });

  it("a page of the tab's own on a host opens a file grid the tab's way: the file asked for, not the host (the S7 confirmation audit's H2-1)", async () => {
    // the host holds only what was read there this session: a file of a page saved earlier is refused there
    const asked: string[] = [];
    const now = writeCube({ name: 'Q3', source: file('trades.csv'), snapshot: SNAPSHOT, configuration: DEFAULT_CONFIGURATION, tree: TreeState.empty() });
    const services = {
      engine: {
        rows: () => ({ planner: new StubPlanner(), engine }),
        database: {
          opened: async (doc: string) => {
            asked.push(doc);
            throw new Error('the file trades.csv is not read here');
          },
          ingestFile: async () => { throw new Error('not reached'); },
        },
      },
      browser: { address: 'http://localhost/' },
    } as never;
    newPage();
    const sources = pageSources({ services, page: pg(), gridMaker: (() => () => undefined) as never, taken: [], ask: () => {} });
    const opened = await Promise.race([
      sources.reopened(now, undefined, false).catch((e: unknown) => (e instanceof Error ? e.message : String(e))),
      new Promise<'waiting'>((done) => { setTimeout(() => done('waiting'), 500); }),
    ]);
    assert.equal(opened, 'waiting', 'the person asked for the file');
    assert.deepEqual(asked, [], 'the host never asked');
    for (const d of document.querySelectorAll('dialog')) d.remove();
  });

  it("a frozen chart's selection, cleared when its grid went, comes back with it (the audit's 2)", async () => {
    const { b } = await twoGrids();
    const chart = await chartOf(b);
    await drawn();
    await togglePill(chart);
    clickMark(tile(chart), 0, 0);
    await settle();
    const before = pg().document('Q3')!;
    assert.ok(chartView(chart)!.selection, 'the chart filters its grid');
    await removeTile(b);
    assert.equal(pill(chart), 'Detached');
    pg().undo();
    await drawn();
    assert.equal(pill(chart), 'Frozen');
    assert.deepEqual(pg().document('Q3')!.views, before.views, 'its selection back, as before the removal');
    assert.equal((tile(chart).querySelector('.dc-tile-chip') as HTMLElement).hidden, false, 'its chip shown');
    assert.deepEqual(pg().grid(b)!.snapshot.filter, before.cubes.find((c) => c.id === b)!.cube.query.filter, 'its grid filtered by it once');
  });

  it("a chart following its grid again takes the grid's grouping in the step it tells (the audit's 3)", async () => {
    const told: PageCommand[] = [];
    newPage({ onPageCommand: (c) => told.push(c) });
    const a = pg().addGrid(over('trades.csv'));
    await settle();
    const chart = await chartOf(a);
    await drawn();
    await togglePill(chart);
    await ungroup(a, 'desk');
    await drawn();
    const n = told.length;
    await togglePill(chart);
    await drawn();
    assert.deepEqual(pg().state.page, pg().document('Q3'), 'the state is the page on screen');
    assert.equal((told.at(-1) as { spec: { split?: string } }).spec.split, chartView(chart)!.spec.split, 'told as it draws');
    assert.equal(told.length, n + 1, 'one command');
  });

  it("a grid made again by an undo has its own open groups and its own query (the audit's 4)", async () => {
    const { b } = await twoGrids();
    await ungroup(b, 'desk');
    const row = [...tile(b).querySelectorAll<HTMLElement>('.dc-row')].find((r) => (r.querySelector('.dc-tree-label')?.textContent ?? '').startsWith('EMEA'));
    if (row?.querySelector('.dc-chevron')) {
      (row.querySelector('.dc-chevron') as HTMLElement).click();
      await settle();
    }
    const before = pg().document('Q3')!;
    await removeTile(b);
    pg().undo();
    await drawn();
    assert.deepEqual(rowChips(b), ['region'], 'regrouped as it was');
    assert.deepEqual(pg().document('Q3')!.cubes, before.cubes, 'its cube as it was, its open groups among it');
  });

  it("a detached chart's Show Grid, then its grid's removal, undone: it is frozen on its grid again (the audit's 5)", async () => {
    const { b } = await twoGrids();
    const chart = await chartOf(b);
    await togglePill(chart);
    const frozen = pg().document('Q3')!;
    await removeTile(b);
    await showGrid(chart);
    await ungroup(chartView(chart)!.cube, 'desk');
    pg().undo();
    await drawn();
    assert.equal(pill(chart), 'Detached', 'Show Grid undone: detached on the cube it kept');
    pg().undo();
    await drawn();
    assert.equal(pill(chart), 'Frozen');
    assert.deepEqual(pg().views().views, frozen.views, 'the page as it was before its grid was removed');
    assert.deepEqual([...pg().cubes], pg().grids, 'the grid Show Grid made let go');
  });

  it('Show Grid on a frozen chart and on a detached one is one step each: undone, its grid gone and the chart as it was; redone, back (D7)', async () => {
    const told: PageCommand[] = [];
    const { b } = await twoGrids({ onPageCommand: (c) => told.push(c) });
    const chart = await chartOf(b);
    await togglePill(chart);
    await step('a frozen chart shown in a grid of its own', told, () => showGrid(chart), 'Show Grid');
    assert.equal(pill(chart), 'Dynamic', 'redone: following its own grid');
    const other = await chartOf(b);
    await togglePill(other);
    await removeTile(b);
    assert.equal(pill(other), 'Detached');
    assert.equal(pill(chart), 'Dynamic', 'its own grid stays when the one it was frozen on goes');
    await step('a detached chart shown in a grid of its own', told, () => showGrid(other), 'Show Grid');
    assert.equal(pill(other), 'Dynamic');
    pg().undo();
    await drawn();
    assert.equal(pill(other), 'Detached', 'undone: detached again, on the cube it kept');
    assert.ok(pg().cubes.includes(b), 'that cube back');
  });

  it("a chart changed in its options form is one step, told once (the test gap: both its callbacks fire)", async () => {
    const told: PageCommand[] = [];
    newPage({ onPageCommand: (c) => told.push(c) });
    const a = pg().addGrid(over('trades.csv'));
    await settle();
    const chart = await chartOf(a);
    await drawn();
    chartMenu(chart).find((i) => label(i) === 'Options...')!.click();
    await settle();
    const across = [...tile(chart).querySelectorAll<HTMLElement>('.dc-field')]
      .find((f) => f.querySelector('.dc-field-label')?.textContent === 'Across:')?.querySelector('select');
    assert.ok(across, "the form's Across");
    const labelBefore = pg().undoLabel;
    const n = told.length;
    across.value = 'desk';
    across.dispatchEvent(new dom.window.Event('change', { bubbles: true }));
    await drawn();
    assert.equal(chartView(chart)!.spec.x, 'desk');
    assert.equal(pill(chart), 'Frozen', 'a hand on its grouping froze it');
    assert.deepEqual(told.slice(n).map((c) => c.kind), ['setChart'], 'told once');
    assert.equal(pg().undoLabel, 'Chart Change');
    pg().undo();
    await drawn();
    assert.equal(pg().undoLabel, labelBefore, 'one step');
    assert.equal(pill(chart), 'Dynamic');
  });

  it('a renamed page keeps no step; reopened or cleared, its history starts again; a sheet an undo brings back is shown', async () => {
    const { a } = await twoGrids();
    pg().setTitle('Renamed');
    assert.equal(pg().undoLabel, 'Add Grid', 'the name is its host\'s: no step');
    assert.equal(pg().state.page.name, 'Renamed');
    (host.querySelector('.dc-sheet-add') as HTMLElement).click();
    await settle();
    clickTab('trades.csv');
    await settle();
    tabMenu('Sheet 2', 'Delete');
    await settle();
    pg().undo();
    await drawn();
    assert.equal(pg().shownSheet, pg().sheets[1], 'the sheet brought back is shown');
    reopen(pg().document('Q3')!);
    await settle();
    assert.equal(pg().undoLabel, undefined, 'reopened: no history');
    void a;
    pg().clear();
    assert.equal(pg().undoLabel, undefined, 'cleared: none');
  });
});

describe('titles and ids: renamed tiles moved and saved, a new grid beside a kept one', () => {
  beforeEach(setUpPage);
  afterEach(tearDownPage);

  /** A tile renamed in place: its title bar double-clicked, a name typed, Enter. */
  function rename(id: string, name: string): void {
    tile(id).querySelector('.dc-tile-head')!.dispatchEvent(new dom.window.MouseEvent('dblclick', { bubbles: true }));
    const field = tile(id).querySelector<HTMLInputElement>('.dc-tile-rename');
    assert.ok(field, `${id}'s rename field`);
    field.value = name;
    field.dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: 'Enter' }));
  }

  it('a renamed chart keeps its name on another sheet; a renamed grid\'s name is saved, an unrenamed one\'s is not', async () => {
    const { a, b } = await twoGrids();
    const chart = await chartOf(a);
    rename(chart, 'Desk mix');
    rename(a, 'Trades');
    await settle();
    (host.querySelector('.dc-sheet-add') as HTMLElement).click();
    await settle();
    // the sheet is named after its first grid's source, not its tile's title
    clickTab('trades.csv');
    await settle();
    const item = chartMenu(chart).find((i) => label(i) === 'Sheet 2');
    assert.ok(item, 'the chart\'s Move to Sheet > Sheet 2');
    item.click();
    await settle();
    assert.equal(pg().views().sheets.find((s) => tilesOf(s.layout).includes(chart))?.id, sheetOf(chart));
    assert.equal(chartView(chart)!.title, 'Desk mix', 'moved under the name it was given');
    // saved: the renamed grid's title written, the other's (as it was made) left out
    const doc = pg().document('Q3')!;
    const gridViews = doc.views.filter((v) => v.kind === 'grid');
    assert.deepEqual(gridViews.map((v) => [v.id, v.title]), [[a, 'Trades'], [b, undefined]]);
    reopen(doc);
    await settle();
    assert.equal(pg().views().views.find((v) => v.id === a)?.title, 'Trades', 'reopened under its name');
    assert.equal(tile(a).querySelector('.dc-tile-title')?.textContent, 'Trades');
    assert.equal(chartView(chart)!.title, 'Desk mix');
    assert.deepEqual(tabs(), ['trades.csv', 'Sheet 2']);
  });

  it('a rename tells its page command as the page saves it: a grid given its made title back none', async () => {
    // (the phase 1 audit's 4 and 5) what the screen tells replays to the page it writes
    const told: PageCommand[] = [];
    const { b } = await twoGrids({ onPageCommand: (c) => told.push(c) });
    (host.querySelector('.dc-sheet-add') as HTMLElement).click();
    await settle();
    clickTab('trades.csv');
    await settle();
    const start = pg().document('Q3')!;
    told.length = 0;
    rename(b, 'Sales');
    rename(b, 'Grid 2');
    await settle();
    // the titles as typed: the made one back keeps none, by renameTile's rule (the replay below)
    assert.deepEqual(told, [{ kind: 'renameTile', tile: b, title: 'Sales' }, { kind: 'renameTile', tile: b, title: 'Grid 2' }]);
    const replayed = applyPageCommands({ page: start, held: idsOf(start) }, told).draft.page;
    assert.deepEqual({ views: replayed.views, sheets: replayed.sheets }, pg().views());
  });

  it('a new grid never takes the id of a grid kept off the board for a detached chart', async () => {
    newPage();
    const g1 = pg().addGrid(over('trades.csv'));
    const g2 = pg().addGrid(over('trades.csv'), { near: g1 });
    const g3 = pg().addGrid(over('trades.csv'), { near: g2 });
    await settle();
    assert.deepEqual([g1, g2, g3], ['grid-1', 'grid-2', 'grid-3']);
    const [chart] = await detached(g3);
    await removeTile(g2);
    reopen(pg().document('Q3')!);
    await settle();
    assert.deepEqual(pg().grids, [g1]);
    assert.deepEqual(pg().cubes, [g1, g3], 'grid-3 kept for its chart');
    // Copy of Grid, twice: after every id the page holds (page-commands.ts nextPageId, the rule Python's ids are given
    // by too, 2026-10-18) -- grid-3, the kept grid's, never taken
    gridMenuItem(g1, 'Copy of Grid').click();
    await settle();
    gridMenuItem(g1, 'Copy of Grid').click();
    await settle();
    assert.deepEqual(pg().grids.slice().sort(), ['grid-1', 'grid-4', 'grid-5']);
    assert.equal(pill(chart!), 'Detached');
    assert.equal(chartView(chart!)!.cube, g3, 'the detached chart still reads its kept grid');
  });
});

// -- a cube alone: its own grid, and a chart's click-to-filter selection --------------------------------------------

/** A cube alone (cube-fixture.ts), its board the page the helpers above read. */
async function setUpCube(): Promise<void> {
  await cube.setUp();
  inertCanvas(cube.dom.window);
  dom = cube.dom;
  host = cube.root;
}

describe('Show Grid on a cube alone: a cube alone saves its own grid alone, so a frozen or detached chart gets no grid of its own', () => {
  beforeEach(setUpCube);
  afterEach(cube.tearDown);

  /** What the chart draws, as it says it to a screen reader: its values by name, its grouping. */
  const drawing = async (chart: string): Promise<string> => {
    await drawn();
    return tile(chart).querySelector('.dc-chartpanel-chart')?.getAttribute('aria-label') ?? '';
  };
  const added = (): string[] => [...host.querySelectorAll<HTMLElement>('[data-tile^="grid-"]')].map((t) => t.dataset['tile']!);

  it('a detached chart: Show Grid offered disabled; clicked anyway, nothing made, the chart drawn from its kept query as it was', async () => {
    // a cube alone saves its own grid's charts only, so the chart is read as a person reads it: by what it draws
    await cube.app.newGrid();
    await settle();
    const [was] = added();
    const chart = await chartOf(was!);
    await togglePill(chart);
    await setAggregation(was!, 'notional', 'average');
    await removeTile(was!);
    assert.equal(pill(chart), 'Detached');
    const drawnBefore = await drawing(chart);
    assert.match(drawnBefore, /^Bar chart of sum_notional by region, split by desk/);
    assert.equal(offersShowGrid(chart), false, 'not offered on a cube alone');
    chartMenu(chart).find((i) => label(i) === 'Show Grid')!.click();
    await settle();
    assert.deepEqual(added(), [], 'no grid made');
    assert.equal(pill(chart), 'Detached');
    assert.equal(await drawing(chart), drawnBefore, 'drawn as it was: its own sum, not its old grid\'s average');
  });
});

describe('what is looked at survives an edit: the board put at the page keeps it (S3)', () => {
  beforeEach(setUpPage);
  afterEach(tearDownPage);

  /** An edit made by hand: the first sheet renamed in its tab (the page's state applies it, the board is put at it). */
  async function anEdit(name: string): Promise<void> {
    host.querySelectorAll<HTMLElement>('.dc-sheet-tab')[0]!.dispatchEvent(new dom.window.MouseEvent('dblclick', { bubbles: true }));
    const field = host.querySelector<HTMLInputElement>('.dc-sheet-tab input')!;
    field.value = name;
    field.dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: 'Enter' }));
    await settle();
  }
  async function aPage(): Promise<{ a: string; chart: string }> {
    newPage();
    const a = pg().addGrid(over('trades.csv'));
    await settle();
    return { a, chart: await chartOf(a) };
  }

  it('the sheet shown: another renamed, or a tile moved onto a new one', async () => {
    const { chart } = await aPage();
    (host.querySelector('.dc-sheet-add') as HTMLElement).click();
    await settle();
    assert.equal(pg().shownSheet, 'sheet-2', 'a sheet added is shown');
    await anEdit('First');
    assert.equal(pg().shownSheet, 'sheet-2', 'the first renamed: the second still shown');
    clickTab('First');
    chartMenu(chart).find((i) => label(i) === 'New Sheet')!.click();
    await settle();
    assert.deepEqual([pg().shownSheet, sheetOf(chart)], ['sheet-1', 'sheet-3'], 'moved onto a new sheet: the sheet shown stays');
  });

  it("a chart's open options, through an edit and through its Show Grid", async () => {
    const { chart } = await aPage();
    chartMenu(chart).find((i) => label(i) === 'Options...')!.click();
    await settle();
    const form = (): HTMLElement => tile(chart).querySelector<HTMLElement>('.dc-chartpanel-form')!;
    assert.equal(form().hidden, false);
    await anEdit('Renamed');
    assert.equal(form().hidden, false, 'its options still open');
    await togglePill(chart);
    await showGrid(chart);
    assert.equal(form().hidden, false, 'still open after its grid of its own was made');
  });

  it("a maximised tile, the board's scroll and the tile with focus", async () => {
    const { a, chart } = await aPage();
    const board = host.querySelector<HTMLElement>('.dc-sheet')!;
    board.scrollTop = 120;
    await anEdit('Scrolled');
    assert.equal(board.scrollTop, 120, 'scrolled where it was');
    tile(chart).focus();
    await pg().follow([{ kind: 'renameSheet', sheet: pg().sheets[0]!, name: 'Followed' }], new Map());
    assert.equal(dom.window.document.activeElement, tile(chart), "another editor's edit: focus where it was");
    tile(a).querySelector<HTMLButtonElement>('.dc-tile-maximise')!.click();
    assert.ok(board.classList.contains('dc-bands-maximised'));
    await anEdit('Maximised');
    assert.ok(board.classList.contains('dc-bands-maximised'), 'still maximised');
  });

  it("the tile a stack shows in front: its tab's, never the saved page's first", async () => {
    const { a, chart } = await aPage();
    await pg().follow([{ kind: 'stack', tiles: [a, chart] }], new Map());
    const shows = (id: string): boolean => !tile(id).hidden;
    assert.ok(shows(a) && !shows(chart), 'stacked: the first in front');
    const tab = [...host.querySelectorAll<HTMLElement>('.dc-tile-tab')].find((t) => t.dataset['tab'] === chart && !(t.closest('.dc-tile-tabs') as HTMLElement).hidden)!;
    tab.dispatchEvent(new dom.window.PointerEvent('pointerdown', { pointerId: 1, button: 0, bubbles: true }));
    tab.dispatchEvent(new dom.window.PointerEvent('pointerup', { pointerId: 1, button: 0, bubbles: true }));
    assert.ok(shows(chart) && !shows(a), 'its tab pressed: the chart in front');
    await anEdit('Stacked');
    assert.ok(shows(chart) && !shows(a), 'still in front after an edit');
  });
});

describe("a cube alone's board: the same state and commands as a page of its own's (P13)", () => {
  beforeEach(setUpCube);
  afterEach(cube.tearDown);

  it('tells each edit of its board as page commands, applied first: they replay to the board it shows', async () => {
    const told: PageCommand[] = [];
    await cube.remount({ onPageCommand: (c) => told.push(c) });
    inertCanvas(cube.dom.window);
    // the board a cube alone grows: its own grid, the whole board
    const start = writePageOf({ name: '', cubes: [], views: startingViews({ id: 'grid', cube: PAGE_CUBE }) });
    const replayed = (): unknown => {
      const page = applyPageCommands({ page: start, held: idsOf(start) }, told).draft.page;
      return JSON.parse(JSON.stringify({ views: page.views, sheets: page.sheets }));
    };
    const shown = (): unknown => JSON.parse(JSON.stringify(cube.app.pageViews()));
    await cube.app.openChart();
    await settle();
    assert.deepEqual(told.map((c) => c.kind), ['addChart']);
    assert.deepEqual(replayed(), shown(), 'a chart added');
    const chart = cube.app.pageViews().views.find((v) => v.kind === 'chart')!.id;
    clickMark(tile(chart), 0, 0);
    await settle();
    assert.deepEqual(told.map((c) => c.kind), ['addChart', 'select']);
    assert.deepEqual(replayed(), shown(), "a chart's mark clicked");
    await cube.app.openChart();
    await settle();
    assert.deepEqual(replayed(), shown(), 'a second chart, beside the first');
    tile(chart).querySelector<HTMLButtonElement>('.dc-tile-remove')!.click();
    await settle();
    // its selection off by removeTile's own rule, as Python's removal takes it off (the S7 audit's P-1)
    assert.deepEqual(told.slice(-1).map((c) => c.kind), ['removeTile']);
    assert.deepEqual(replayed(), shown(), 'a chart removed');
  });
});

describe('a chart\'s selection, on a cube alone: its chip, cleared by it or by the grid\'s filter', () => {
  beforeEach(setUpCube);
  afterEach(cube.tearDown);

  const EMEA: FilterNode = { kind: 'condition', column: 'region', operator: 'equal', value: 'EMEA' };
  const LARGE: FilterNode = { kind: 'condition', column: 'notional', operator: 'greaterThanEqual', value: 500 };
  const NO_DESK: FilterNode = { kind: 'condition', column: 'desk', operator: 'isEmpty' };
  const chip = (id: string): HTMLButtonElement => tile(id).querySelector('.dc-tile-actions .dc-tile-chip') as HTMLButtonElement;
  const selection = (id: string): readonly FilterNode[] | undefined =>
    cube.app.pageViews().views.find((v): v is ChartView => v.kind === 'chart' && v.id === id)?.selection;
  /** The cube's filter set to `filter`, as one change. */
  async function filterTo(filter: FilterNode | undefined): Promise<void> {
    await cube.app.edit([{ kind: 'filter', filter: filter ?? null }], 'filter');
    await settle();
  }
  /** A cube whose filter holds `conditions`, and a saved chart that put them there, put back. */
  async function restoredWith(conditions: readonly FilterNode[]): Promise<string> {
    await cube.app.openChart();
    await settle();
    const views = cube.app.pageViews();
    const chart = views.views.find((v): v is ChartView => v.kind === 'chart')!;
    const config = cube.app.configuration;
    await cube.remount({ configuration: config });
    await filterTo(conditions.length === 1 ? conditions[0] : { kind: 'and', children: conditions });
    await cube.app.restoreViews({ ...views, views: views.views.map((v) => (v.id === chart.id ? { ...chart, selection: conditions } : v)) });
    await settle();
    return chart.id;
  }

  it('a click on a mark filters the grid to it; another mark replaces it; the same mark again takes it off', async () => {
    await cube.app.openChart();
    await settle();
    const chart = cube.app.pageViews().views.find((v) => v.kind === 'chart')!.id;
    const A_EMEA: FilterNode = { kind: 'and', children: [EMEA, { kind: 'condition', column: 'desk', operator: 'equal', value: 'A' }] };
    const B_AMER: FilterNode = { kind: 'and', children: [
      { kind: 'condition', column: 'region', operator: 'equal', value: 'AMER' },
      { kind: 'condition', column: 'desk', operator: 'equal', value: 'B' },
    ] };
    clickMark(tile(chart), 0, 0);
    await settle();
    assert.deepEqual(cube.app.snapshot.filter, A_EMEA, 'the mark\'s region and desk, ANDed on');
    assert.equal(chip(chart).textContent, 'region: EMEA, desk: A \u00d7');
    clickMark(tile(chart), 1, 1);
    await settle();
    assert.deepEqual(cube.app.snapshot.filter, B_AMER, 'replaced, not piled on: two clicks never filter to nothing');
    assert.equal(chip(chart).textContent, 'region: AMER, desk: B \u00d7');
    clickMark(tile(chart), 1, 1);
    await settle();
    assert.equal(cube.app.snapshot.filter, undefined, 'the same mark again: taken off');
    assert.equal(chip(chart).hidden, true);
    // each click one change: undone, the filter is as the click before left it -- and the chart claims no conditions
    // the filter does not hold
    await cube.app.undo();
    await settle();
    assert.deepEqual(cube.app.snapshot.filter, B_AMER);
    await cube.app.undo();
    await settle();
    assert.deepEqual(cube.app.snapshot.filter, A_EMEA);
    assert.equal(chip(chart).hidden, true, 'the undone selection is not the chart\'s to take off');
  });

  it('a chart removed while it filters its grid takes its conditions off the filter', async () => {
    const DESK_A: FilterNode = { kind: 'condition', column: 'desk', operator: 'equal', value: 'A' };
    const chart = await restoredWith([EMEA]);
    await filterTo({ kind: 'and', children: [EMEA, DESK_A] });
    tile(chart).querySelector<HTMLButtonElement>('.dc-tile-remove')!.click();
    await settle();
    assert.equal(tile(chart), null);
    assert.deepEqual(cube.app.snapshot.filter, DESK_A, 'its own condition gone; the other stays');
  });

  it('a restored selection is said in its chip, by the columns\' names; the chip takes it off the grid\'s filter', async () => {
    await cube.app.edit([{ kind: 'configure', patch: { columns: { region: { displayName: 'Region' } } } }], 'settings');
    const chart = await restoredWith([EMEA, LARGE, NO_DESK]);
    assert.equal(chip(chart).hidden, false);
    assert.equal(chip(chart).textContent, 'Region: EMEA, notional ≥ 500, desk: (empty) ×');
    assert.equal(chip(chart).getAttribute('aria-label'), 'Clear the filter to Region: EMEA, notional ≥ 500, desk: (empty)');
    assert.deepEqual(selection(chart), [EMEA, LARGE, NO_DESK]);
    chip(chart).click();
    await settle();
    assert.equal(cube.app.snapshot.filter, undefined, 'its conditions taken off the cube\'s filter');
    assert.equal(chip(chart).hidden, true);
    assert.equal(selection(chart), undefined, 'and no longer saved with the chart');
  });

  it('the chip takes off only its own conditions: another condition on the filter stays', async () => {
    const DESK_A: FilterNode = { kind: 'condition', column: 'desk', operator: 'equal', value: 'A' };
    const chart = await restoredWith([EMEA]);
    await filterTo({ kind: 'and', children: [EMEA, DESK_A] });
    assert.equal(chip(chart).hidden, false, 'its condition is still on the filter: still its selection');
    chip(chart).click();
    await settle();
    assert.deepEqual(cube.app.snapshot.filter, DESK_A);
  });

  it('a selection taken off the grid\'s filter some other way is no longer the chart\'s', async () => {
    const chart = await restoredWith([EMEA]);
    assert.deepEqual(selection(chart), [EMEA]);
    await filterTo(undefined);
    assert.equal(chip(chart).hidden, true);
    assert.equal(selection(chart), undefined);
    // the chip has nothing to take off now: clicking it changes nothing
    const filter: FilterNode = { kind: 'condition', column: 'desk', operator: 'equal', value: 'B' };
    await filterTo(filter);
    chip(chart).click();
    await settle();
    assert.deepEqual(cube.app.snapshot.filter, filter);
  });
});

describe('a frozen chart on a page of its own loses its selection with its grid', () => {
  beforeEach(setUpPage);
  afterEach(tearDownPage);

  it('its grid removed, a detached chart has no grid\'s filter to keep its selection on: the chip goes', async () => {
    const EMEA: FilterNode = { kind: 'condition', column: 'region', operator: 'equal', value: 'EMEA' };
    const { a, b } = await twoGrids();
    const chart = await chartOf(b);
    await togglePill(chart);
    const doc = pg().document('Q3')!;
    // saved with b filtered to EMEA by the chart's last click
    reopen({
      ...doc,
      cubes: doc.cubes.map((c) => (c.id === b ? { ...c, cube: { ...c.cube, query: { ...c.cube.query, filter: EMEA } } } : c)),
      views: doc.views.map((v) => (v.id === chart ? { ...v, selection: [EMEA] } : v)),
    });
    await settle();
    const chip = (): HTMLButtonElement => tile(chart).querySelector('.dc-tile-actions .dc-tile-chip') as HTMLButtonElement;
    assert.equal(chip().hidden, false);
    assert.equal(chip().textContent, 'region: EMEA ×');
    await removeTile(b);
    assert.equal(pill(chart), 'Detached');
    assert.equal(chip().hidden, true, 'no selection on a grid that is gone');
    assert.equal(chartView(chart)!.selection, undefined);
    assert.deepEqual(pg().grids, [a]);
    // a click on its marks has no grid's filter to go on: nothing is filtered, no grid on the page changes
    await drawn();
    assert.match(tile(chart).querySelector('.dc-chartpanel-chart')?.getAttribute('aria-label') ?? '', /^Bar chart .* by region, split by desk: 2 rows/,
      'drawn from its kept query: its marks are there to click');
    clickMark(tile(chart), 0, 0);
    await settle();
    assert.equal(chip().hidden, true);
    assert.equal(pg().grid(a)!.snapshot.filter, undefined);
    assert.deepEqual(pg().grid(b)!.snapshot.filter, EMEA, 'the kept grid\'s query as it was');
  });
});

// -- the S3 audit's findings (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md F6-F13) ---------------------------------------

/** Each error a listener threw (jsdom reports them to the window, never to the test). */
function thrown(window: JSDOM['window']): unknown[] {
  const errors: unknown[] = [];
  window.addEventListener('error', (event) => errors.push((event as ErrorEvent).error ?? (event as ErrorEvent).message));
  return errors;
}

/** A tile's head pressed, and let go at `where` on another tile (its middle, or just inside its left edge). */
function pressOn(id: string): void {
  const head = tile(id).querySelector<HTMLElement>('.dc-tile-head')!;
  const from = tile(id).style;
  head.dispatchEvent(new dom.window.PointerEvent('pointerdown', { pointerId: 1, clientX: parseFloat(from.left) + 10,
    clientY: parseFloat(from.top) + 10, button: 0, bubbles: true }));
}
function letGoOn(id: string, onto: string, where: 'middle' | 'left'): void {
  const head = tile(id).querySelector<HTMLElement>('.dc-tile-head')!;
  const at = tile(onto).style;
  head.dispatchEvent(new dom.window.PointerEvent('pointerup', { pointerId: 1,
    clientX: parseFloat(at.left) + (where === 'middle' ? parseFloat(at.width) / 2 : 4),
    clientY: parseFloat(at.top) + parseFloat(at.height) / 2, button: 0, bubbles: true }));
}
/** The sheets given a width and a height (jsdom lays nothing out), the page's layout editable. */
function sized(): void {
  for (const s of host.querySelectorAll<HTMLElement>('.dc-sheet')) {
    Object.defineProperty(s, 'clientWidth', { configurable: true, get: () => 1200 });
    Object.defineProperty(s, 'clientHeight', { configurable: true, get: () => 600 });
  }
  pg().setLayoutEditing(true);
}
/** Each saved tile's share of its band, by id, as the page has the sheet. */
function shares(): Record<string, number> {
  const out: Record<string, number> = {};
  const walk = (node: unknown, share: number): void => {
    const n = node as { tile?: string; parts?: { node: unknown; size: number }[] };
    if (n.tile !== undefined) out[n.tile] = Math.round(share * 1000) / 1000;
    else for (const p of n.parts ?? []) walk(p.node, share * p.size);
  };
  for (const band of (pg().views().sheets[0]!.layout as unknown as { bands: { node: unknown }[] }).bands) walk(band.node, 1);
  return out;
}

describe("the S3 audit's findings: a page of its own", () => {
  beforeEach(setUpPage);
  afterEach(tearDownPage);

  it('F9: a tile dragged onto another\'s left edge goes beside it, in the page\'s layout', async () => {
    // a and its chart in one band, 0.7 / 0.3, b in a band below; b dragged onto a's left edge
    newPage();
    const a = pg().addGrid(over('trades.csv'));
    await settle();
    const chart = await chartOf(a);
    await togglePill(chart);
    const b = pg().addGrid(over('orders.csv'), { near: a });
    await settle();
    await pg().follow([{ kind: 'layout', sheet: pg().sheets[0]!, layout: { kind: 'bands', fit: true, bands: [
      { height: 1, node: { split: 'row', parts: [{ node: { tile: a }, size: 0.7 }, { node: { tile: chart }, size: 0.3 }] } },
      { height: 1, node: { tile: b } },
    ] } }], new Map());
    sized();
    pressOn(b);
    letGoOn(b, a, 'left');
    await settle();
    const moved = shares();
    assert.deepEqual(Object.keys(moved).sort(), [a, b, chart].sort(), `the drag moved b beside a: ${JSON.stringify(moved)}`);
    assert.equal(pg().views().sheets[0]!.layout.bands.length, 1, 'one band: b beside a and its chart');
  });

  it('P14: an arrangement picked is told as the arrangement, the page\'s tiles arranged by it', async () => {
    const told: PageCommand[] = [];
    newPage({ onPageCommand: (c) => told.push(c) });
    const a = pg().addGrid(over('trades.csv'));
    await settle();
    const chart = await chartOf(a);
    await togglePill(chart);
    const b = pg().addGrid(over('orders.csv'), { near: a });
    await settle();
    sized();
    const sheet = pg().sheets[0]!;
    told.length = 0;
    // the board's own arrangement, as its picker picks one: the page bar's Arrange…
    dom.window.document.dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: 'Escape' }));
    (host.querySelector('.dc-page-bar .dc-titlebar-menu') as HTMLElement).click();
    menuItems().find((i) => label(i) === 'Arrange…')!.click();
    await settle();
    host.querySelector<HTMLButtonElement>('.dc-layout-picker [data-preset="focus-left"]')!.click();
    await settle();
    assert.deepEqual(told.map((c) => c.kind), ['arrange']);
    assert.deepEqual(new Set(tilesOf(pg().views().sheets.find((s) => s.id === sheet)!.layout)), new Set([a, chart, b]));
  });

  it('F11: a detached chart\'s Show Grid is told, and undone and redone the board is the state the step holds', async () => {
    const { a, b } = await twoGrids();
    const chart = await chartOf(b);
    await togglePill(chart);
    await removeTile(b);
    assert.equal(pill(chart), 'Detached');
    // the cubes by content (a document made again from another's text writes its keys in its own order)
    const seen = (): unknown => JSON.parse(JSON.stringify(pg().document('Q3')!.cubes));
    const held = (): unknown => JSON.parse(JSON.stringify(pg().state.page.cubes));
    await showGrid(chart);
    const own = chartView(chart)!.cube;
    assert.notEqual(own, b);
    assert.deepEqual(seen(), held(), 'the board is the state: the grid Show Grid made');
    pg().undo();
    await settle();
    assert.equal(chartView(chart)!.cube, b, 'undone: it reads the cube it kept again');
    assert.equal(pill(chart), 'Detached');
    assert.deepEqual(pg().cubes, [a, b]);
    assert.deepEqual(seen(), held(), 'undone: the board is the state');
    pg().redo();
    await settle();
    assert.equal(chartView(chart)!.cube, own, 'redone: its own grid again');
    assert.equal(pill(chart), 'Dynamic');
    assert.deepEqual(seen(), held(), 'redone: the board is the state');
  });
});

describe("the S3 audit's findings: a page of its own, again", () => {
  beforeEach(setUpPage);
  afterEach(tearDownPage);

  it("F16: a drag goes on through another editor's step that leaves its sheet as it is", async () => {
    const told: PageCommand[] = [];
    const { a, b } = await twoGrids({ onPageCommand: (c) => told.push(c) });
    sized();
    told.length = 0;
    pressOn(b);
    // the other editor renames the page while the tile is held
    await pg().follow([{ kind: 'renamePage', name: 'Q4' }], new Map());
    letGoOn(b, a, 'middle');
    await settle();
    assert.deepEqual(told.map((c) => c.kind), ['stack'], `the drop landed: told as its stack (${JSON.stringify(told)})`);
  });

  it('F16: a chart with no spec yet stays beside its grid through the next edit of the page', async () => {
    newPage();
    // a grid of figures alone: no column a chart can be grouped by, so a chart opened on it has no spec to start from
    const figures = SNAPSHOT.columns.filter((c) => c.name === 'notional');
    const numbers: GridMaker = (gridHost, spawned) => new CubeApp(gridHost, { ...SNAPSHOT, columns: figures, rows: [] }, {
      engine, planner: new StubPlanner(), compact: true, sourceLabel: 'figures.csv',
      cubeSource: { ...file('figures.csv'), columns: figures.map((c) => ({ name: c.name, type: c.type })) },
      configuration: { ...DEFAULT_CONFIGURATION, reportTitle: 'figures' }, windowHost: pg().root, ...spawned,
    });
    const a = pg().addGrid(numbers);
    await settle();
    sized();
    const before = new Set(host.querySelectorAll('[data-tile^="chart-"]'));
    await (pg().grid(a) as unknown as { openChart(): Promise<void> }).openChart();
    await settle();
    const chart = [...host.querySelectorAll<HTMLElement>('[data-tile^="chart-"]')].find((t) => !before.has(t))?.dataset['tile'];
    assert.ok(chart, 'a chart on the board');
    assert.equal(chartView(chart), undefined, 'no spec: no tile of the page\'s');
    const beside = (): boolean => tile(chart).style.top === tile(a).style.top;
    assert.ok(beside(), 'beside its grid');
    (host.querySelector('.dc-sheet-add') as HTMLElement).click();
    await settle();
    assert.ok(beside(), 'still beside its grid after the next edit');
  });

  it('F17: a grid whose addition is refused is let go, nothing thrown, no tile under its id', async () => {
    let disposed = 0;
    let tilesSaid = 0;
    const fake = (): SpawnedGrid => {
      const element = dom.window.document.createElement('div');
      return {
        snapshot: SNAPSHOT, open: async () => {}, dispose: () => { disposed += 1; },
        chartSource: () => ({}) as never, on: () => () => {}, tileHead: () => element,
      };
    };
    const board = new CubePage({
      host, onChange: () => {}, onEmpty: () => {}, onTiles: () => { tilesSaid += 1; },
      // its owner refuses every edit (as it refuses one whose commands do not apply)
      onPageEdit: () => false, taken: () => [],
    });
    const id = board.addGridOver(() => fake());
    assert.equal(disposed, 1, 'let go');
    assert.ok(tilesSaid > 0, 'its owner told the tiles changed (it forgets the grid)');
    assert.equal(board.grids.has(id), false);
    assert.equal(host.querySelector(`[data-tile="${id}"]`), null);
    board.dispose();
  });

  it("follow keeps the ids its commands held and the cubes they made: an id given and taken off there is never given here", async () => {
    newPage();
    pg().addGrid(over('trades.csv'));
    await settle();
    await pg().follow([{ kind: 'addSheet', id: 'sheet-5', name: 'Briefly' }, { kind: 'removeSheet', sheet: 'sheet-5' }], new Map());
    assert.ok(pg().state.held.includes('sheet-5'), 'held');
    (host.querySelector('.dc-sheet-add') as HTMLElement).click();
    await settle();
    assert.ok(!pg().sheets.includes('sheet-5') && pg().sheets.length === 2, `a sheet added here takes another id: ${pg().sheets.join(', ')}`);
    assert.deepEqual(pg().state.page.cubes.map((c) => c.id), pg().cubes);
  });
});

describe("the S3 audit's findings: a cube alone", () => {
  beforeEach(setUpCube);
  afterEach(cube.tearDown);

  const chip = (id: string): HTMLButtonElement => tile(id).querySelector('.dc-tile-actions .dc-tile-chip') as HTMLButtonElement;
  const selection = (id: string): readonly FilterNode[] | undefined =>
    cube.app.pageViews().views.find((v): v is ChartView => v.kind === 'chart' && v.id === id)?.selection;

  it('F6: a selection its grid\'s filter lost stays lost after the next edit of the board', async () => {
    await cube.app.openChart();
    await settle();
    const chart = cube.app.pageViews().views.find((v) => v.kind === 'chart')!.id;
    clickMark(tile(chart), 0, 0);
    await settle();
    assert.equal(chip(chart).hidden, false);
    await cube.app.edit([{ kind: 'filter', filter: null }], 'filter');
    await settle();
    assert.equal(chip(chart).hidden, true, 'the grid\'s filter lost it');
    await cube.app.openChart();
    await settle();
    await drawn();
    assert.equal(chip(chart).hidden, true, 'not brought back by the next edit');
    assert.equal(selection(chart), undefined);
    assert.equal(cube.app.snapshot.filter, undefined);
  });

  it('F7, F8: its last chart removed throws nothing, and a chart opened again takes an id never given', async () => {
    const errors = thrown(cube.dom.window);
    const told: PageCommand[] = [];
    await cube.remount({ onPageCommand: (c) => told.push(c) });
    inertCanvas(cube.dom.window);
    await cube.app.openChart();
    await settle();
    const first = cube.app.pageViews().views.find((v) => v.kind === 'chart')!.id;
    tile(first).querySelector<HTMLButtonElement>('.dc-tile-remove')!.click();
    await settle();
    assert.deepEqual(errors, [], 'nothing thrown');
    assert.equal(host.querySelector('.dc-board-host'), null, 'the board gone: the grid alone again');
    await cube.app.openChart();
    await settle();
    const second = cube.app.pageViews().views.find((v) => v.kind === 'chart')!.id;
    assert.notEqual(second, first, 'an id the board held is never given again');
    const start = writePageOf({ name: '', cubes: [], views: startingViews({ id: 'grid', cube: PAGE_CUBE }) });
    assert.doesNotThrow(() => applyPageCommands({ page: start, held: idsOf(start) }, told), 'its told commands replay');
    // an added grid, the board's last tile but the cube's own: removed, nothing thrown either
    await cube.app.newGrid();
    await settle();
    tile(second).querySelector<HTMLButtonElement>('.dc-tile-remove')!.click();
    await settle();
    const added = host.querySelector<HTMLElement>('[data-tile^="grid-"]')!.dataset['tile']!;
    tile(added).querySelector<HTMLButtonElement>('.dc-tile-remove')!.click();
    await settle();
    assert.deepEqual(errors, [], 'nothing thrown');
  });

  it('F12: its told commands replay to the board it shows; what it saves is that less what a cube alone cannot save', async () => {
    const told: PageCommand[] = [];
    await cube.remount({ onPageCommand: (c) => told.push(c) });
    inertCanvas(cube.dom.window);
    await cube.app.openChart();
    await settle();
    await cube.app.newGrid();
    await settle();
    const added = host.querySelector<HTMLElement>('[data-tile^="grid-"]')!.dataset['tile']!;
    await chartOf(added);
    const start = writePageOf({ name: '', cubes: [], views: startingViews({ id: 'grid', cube: PAGE_CUBE }) });
    const replayed = applyPageCommands({ page: start, held: idsOf(start) }, told).draft.page;
    const onBoard = [...host.querySelectorAll<HTMLElement>('[data-tile]')].map((t) => t.dataset['tile']!).sort();
    assert.deepEqual(replayed.views.map((v) => v.id).sort(), onBoard, 'the board it shows');
    // a cube alone saves one cube: an added grid, and the charts that read it, are the board's alone
    const theirs = new Set(replayed.views.filter((v) => v.cube === added).map((v) => v.id));
    assert.deepEqual(cube.app.pageViews().views.map((v) => v.id).sort(), replayed.views.map((v) => v.id).filter((id) => !theirs.has(id)).sort());
  });

  it("F14: an added grid is named by its id, as a page's: renamed to that name, it keeps it", async () => {
    await cube.app.openChart();
    await settle();
    await cube.app.newGrid();
    await settle();
    const added = host.querySelector<HTMLElement>('[data-tile^="grid-"]')!.dataset['tile']!;
    const title = (): string | null | undefined => tile(added).querySelector('.dc-tile-title')?.textContent;
    const named = `Grid ${added.slice('grid-'.length)}`;
    assert.equal(title(), named);
    for (const name of ['Sales', named]) {
      (tile(added).querySelector('.dc-tile-title') as HTMLElement).dispatchEvent(new dom.window.MouseEvent('dblclick', { bubbles: true }));
      const field = tile(added).querySelector<HTMLInputElement>('input')!;
      field.value = name;
      field.dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: 'Enter' }));
      await settle();
      assert.equal(title(), name);
    }
  });
});
