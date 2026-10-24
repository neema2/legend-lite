// THE ONE API LEDGERS, BEHAVIOURAL HALF (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md §3: L2, L3). The real page -- PageApp,
// its grids CubeApps, its charts -- wired to the tab's live channel (src/app/live-channel.ts) as DataCube's app wires a
// page another editor serves; each gesture made as a person makes it, on a page that is quiet first, and what the
// channel then writes held to the tenet:
//
//   told    the change was told as commands: the channel wrote no page whole -- an undo's or a redo's are the commands
//           between (docs/DATACUBE_LIVE_EDITING_2026_10_24.md W3, W4) -- and the screen shows the page its writes make (L2)
//   wire    the host reads them as Python's keeper reads a write (headless readEdits, pageRebase's), as sent (L3)
//   replays the commands as the host read them, applied to the page before by the shared functions (rebaseEdits:
//           applyPageCommand, and a grid's applyGridCommands), make the page the screen shows (L2)
//
// Each gesture's failing checks are listed by the plan's items, with what fails (the host's words, the parts of the page
// that differ), and only those: a gesture failing a check it is not listed for, or failing it in another way, is a new
// gap; one listed that now passes is a gap closed -- close its item, take it off. And every command kind is told by
// some gesture here, or listed as told by none: a new kind is driven here before it ships. The static half is
// one-api-ledger.test.ts; Python's is python/tests/test_one_api.py.

import assert from 'node:assert/strict';
import { after, afterEach, beforeEach, describe, it } from 'node:test';
import { JSDOM } from 'jsdom';
import { getInstanceByDom } from 'echarts/core';

import { CubeApp } from '../src/app.ts';
import { applyToSnapshot, DEFAULT_CONFIGURATION } from '../src/config.ts';
import { fold } from '../src/cube-state.ts';
import { configurationOf, cubeCopied, cubeToJson, treeOf, writeCube, type FileSource } from '../src/cube-document.ts';
import { applyGridCommands, GRID_COMMAND_KINDS } from '../src/grid-commands.ts';
import { LiveChannel, type LiveScreen } from '../src/app/live-channel.ts';
import { pageHash } from '../src/page-hash.ts';
import type { LivePage, LiveWrite } from '../src/app/services.ts';
import { applyPageCommand, idsOf, PAGE_COMMAND_KINDS } from '../src/page-commands.ts';
import { pageToJson, readPage, type ChartView, type PageDocument } from '../src/page-document.ts';
import { rebaseEdits, type GridApplier, type PageEdit } from '../src/page-rebase.ts';
import { PageApp, type GridMaker, type PageAppOptions } from '../src/page/page-app.ts';
import { chartPick, readEdits } from '../src/headless/page.ts';
import type { CubeSnapshot } from '../src/snapshot.ts';
import { toJson } from '../../pure-protocol/src/index.ts';
import { GateEngine, SNAPSHOT, StubPlanner, settle, shown } from './cube-fixture.ts';
import { sameButIds } from './page-same.ts';
import type { ResultTable } from '../../engine-client/src/result.ts';

// -- the page, as DataCube's app wires one another editor serves ----------------------------------------------------

let dom: JSDOM;
let host: HTMLElement;
let engine: GateEngine;
let page: PageApp | undefined;
let channel: LiveChannel | undefined;
const cubes = new Map<string, CubeApp>();

/** What the channel wrote, each write as the host was sent it -- and the page the screen showed as it was sent (the
 *  test's own look: no write carries the page). */
interface Write { readonly at: number; readonly write: LiveWrite; readonly screen: PageDocument }
const writes: Write[] = [];
/** Every command kind some gesture told, across the run. */
const told = new Set<string>();

/** The page's other editor (Python's host): each write recorded and taken. */
const editor: LivePage = {
  read: () => { throw new Error('not opened here'); },
  report: async (at, write) => {
    const screen = onScreen();
    writes.push({ at, write, screen });
    // at its version: the page as the screen sent it is the keeper's, and hashes so (W17)
    return { version: at + 1, hash: pageHash(screen) };
  },
};

/** The screen as the channel sees it here: the page, never busy (each gesture is settled before its write), converging
 *  and reopening never (a write at its version) -- said so when one would. */
const screenOf = (): LiveScreen => ({
  page: onScreen,
  busy: () => false,
  converge: async () => { throw new Error('not converged here: every write is at its version'); },
  reapply: async () => {},
  reopen: async (why) => { throw new Error(`not reopened here: ${why}`); },
  say: () => {},
});

const pg = (): PageApp => {
  assert.ok(page, 'the test made a page');
  return page;
};

beforeEach(() => {
  dom = new JSDOM('<!doctype html><div id="p"></div>');
  const g = globalThis as unknown as Record<string, unknown>;
  g['window'] = dom.window;
  g['document'] = dom.window.document;
  g['requestAnimationFrame'] = (cb: FrameRequestCallback) => { cb(0); return 1; };
  g['cancelAnimationFrame'] = () => {};
  host = dom.window.document.getElementById('p') as unknown as HTMLElement;
  const inert: ProxyHandler<object> = {
    get: (_t, key) => (key === 'canvas' ? dom.window.document.createElement('canvas') : key === 'measureText' ? () => ({ width: 0 }) : () => undefined),
    set: () => true,
  };
  (dom.window.HTMLCanvasElement.prototype as unknown as { getContext: () => unknown }).getContext = () => new Proxy({}, inert);
  engine = new GateEngine();
  writes.length = 0;
  cubes.clear();
  channel = undefined;
});

afterEach(async () => {
  channel?.stop();
  await settle();
  page?.dispose();
  page = undefined;
});

const file = (name: string): FileSource => ({
  _type: 'file', name, format: 'csv', size: 10, sha256: name.padEnd(64, '0').slice(0, 64),
  columns: SNAPSHOT.columns.map((c) => ({ name: c.name, type: c.type })),
});

/** A grid over `name`, as DataCube's app makes one on a page another editor serves: its commands told to the channel. */
const over = (name: string): GridMaker => (gridHost, spawned, start) => {
  const made = new CubeApp(gridHost, start?.snapshot ?? SNAPSHOT, {
    engine, planner: new StubPlanner(), compact: true, cubeSource: file(name), sourceLabel: name,
    configuration: start?.configuration ?? { ...DEFAULT_CONFIGURATION, reportTitle: name },
    ...(page ? { windowHost: page.root } : {}), ...spawned,
    onCommand: (command) => { if (spawned.id !== undefined) channel?.edit({ grid: spawned.id, command }); },
  });
  if (spawned.id !== undefined) cubes.set(spawned.id, made);
  return made;
};

/** The statement a snap counts its rows by, here: the fixture's engine answers it with the fixture's two rows. */
const COUNT = 'count the rows';
class CountingGate extends GateEngine {
  override async answer(sql: string, epoch: number): Promise<ResultTable> {
    if (sql === COUNT) return { columns: [{ name: 'n', type: 'Integer', values: [2] }], rowCount: 1, epoch, elapsedMs: 1 };
    return super.answer(sql, epoch);
  }
}
/** A grid over `name` that can snap its rows into the tab (its toggle a toggle): a copy whose statements run nowhere
 *  but its count. */
const snappable = (name: string): GridMaker => (gridHost, spawned, start) => {
  const made = new CubeApp(gridHost, start?.snapshot ?? SNAPSHOT, {
    engine: new CountingGate(), planner: new StubPlanner(), compact: true, cubeSource: file(name), sourceLabel: name,
    snapTarget: {
      host: { hostOperations: async (_type, operations) => operations.map((o) => (o.op === 'count' ? [COUNT] : [])) },
      copy: () => ({ table: `snap_${name}`, landed: async () => ({ source: SNAPSHOT.source.query, planner: new StubPlanner() }), dispose: () => {} }),
    },
    configuration: start?.configuration ?? { ...DEFAULT_CONFIGURATION, reportTitle: name },
    ...(page ? { windowHost: page.root } : {}), ...spawned,
    onCommand: (command) => { if (spawned.id !== undefined) channel?.edit({ grid: spawned.id, command }); },
  });
  if (spawned.id !== undefined) cubes.set(spawned.id, made);
  return made;
};
/** A page of a grid that can snap, `a`, beside one that cannot (so each pill is in its tile's header). */
const snapGrid = async (): Promise<Record<string, string>> => {
  newPage();
  const a = pg().addGrid(snappable('trades.csv'));
  const b = pg().addGrid(over('orders.csv'), { near: a });
  await settle();
  return { a, b };
};
const toggle = (id: string): void => tile(id).querySelector<HTMLButtonElement>('.dc-titlebar-toggle')!.click();

/** A grid over the class Trade (a page filter is on a class a grid reads): its source a class, its rows the fixture's. */
const TRADE = 'trades::domain::Trade';
const overClass = (): GridMaker => (gridHost, spawned, start) => {
  const made = new CubeApp(gridHost, start?.snapshot ?? SNAPSHOT, {
    engine, planner: new StubPlanner(), compact: true, sourceLabel: 'Trade',
    cubeSource: {
      _type: 'class', name: 'Trade', class: TRADE, project: 'datacube:trades', workspace: 'datacube', revision: '3',
      mapping: 'trades::mapping::Model', runtime: 'trades::runtime::Browser',
      columns: SNAPSHOT.columns.map((c) => ({ name: c.name, type: c.type })),
    },
    configuration: start?.configuration ?? { ...DEFAULT_CONFIGURATION, reportTitle: 'Trade' },
    ...(page ? { windowHost: page.root } : {}), ...spawned,
    onCommand: (command) => { if (spawned.id !== undefined) channel?.edit({ grid: spawned.id, command }); },
  });
  if (spawned.id !== undefined) cubes.set(spawned.id, made);
  return made;
};

/** The page as the tab writes it (datacube-app.ts pageNow: its name as it is now, a page of no grid too; the saved page
 *  it is as its state holds it, the savedAs command's). */
const onScreen = (): PageDocument => pg().document(pg().title || 'Q3', undefined, true)!;

/**
 * The page, wired as datacube-app.ts wires a page another editor serves. One part is the host's own and stood in for
 * here: renaming (its renamePage keeps the name, sets the title and says the page changed). P1's check on the host's
 * own code is the static half's (L1: renamePage tells no command); this gesture holds what the channel does with it.
 */
function newPage(extra: Partial<PageAppOptions> = {}): PageApp {
  page = new PageApp({
    host, title: 'Q3', confirm: () => true, empty: (slot) => { slot.textContent = 'empty'; },
    onChange: () => channel?.changed(),
    onPageCommand: (command) => channel?.edit({ page: command }),
    onRename: (name) => {
      pg().rename(name);
      channel?.changed();
    },
    ...extra,
  });
  return page;
}

/** A page of two grids, `a` over trades, `b` over orders beside it. */
async function twoGrids(): Promise<{ a: string; b: string }> {
  newPage();
  const a = pg().addGrid(over('trades.csv'));
  const b = pg().addGrid(over('orders.csv'), { near: a });
  await settle();
  return { a, b };
}

// -- the screen, as a person works it (cube-page-shell.test.ts's gestures) --------------------------------------------

const tile = (id: string): HTMLElement => host.querySelector(`[data-tile="${id}"]`) as HTMLElement;
const label = (item: Element): string => item.querySelector(':scope > .dc-menu-label')?.textContent ?? '';
const menuItems = (): HTMLElement[] => [...dom.window.document.querySelectorAll<HTMLElement>('.dc-menu [role^="menuitem"]')];
const escape = (): void => { dom.window.document.dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: 'Escape' })); };
const contextMenu = (el: Element): void => { el.dispatchEvent(new dom.window.MouseEvent('contextmenu', { bubbles: true, cancelable: true })); };

async function drawn(): Promise<void> {
  await new Promise((r) => setTimeout(r, 200));
  await settle();
}

/** An entry of the menu open now, by what it says. */
function entry(text: string): HTMLElement {
  const item = menuItems().find((i) => label(i) === text && i.getAttribute('aria-disabled') !== 'true');
  assert.ok(item, `the menu offers ${text} (it offers ${menuItems().filter((i) => i.getAttribute('aria-disabled') !== 'true').map(label).join(' | ')})`);
  return item;
}

/** A submenu's entry: `text` under `parent` in the menu open now (the first under it, given none). */
function nested(parent: string, text?: string): HTMLElement {
  const item = [...dom.window.document.querySelectorAll<HTMLElement>('.dc-menu .dc-menu-item')]
    .find((el) => {
      const under = el.parentElement?.closest('.dc-menu-item');
      return (text === undefined || label(el) === text) && under !== null && under !== undefined && label(under) === parent;
    });
  assert.ok(item, `${parent} > ${text ?? 'its first entry'}`);
  return item;
}

/** An entry of a grid's right-click (a submenu's too: Create ▸, Tile ▸ Move to Sheet ▸), by what it says. */
function gridMenuItem(gridTile: string, text: string): HTMLElement {
  escape();
  contextMenu(tile(gridTile).querySelector('.dc-row .dc-cell') as HTMLElement);
  return entry(text);
}

function chartMenu(chart: string): void {
  escape();
  contextMenu(tile(chart).querySelector('.dc-chart-tile') as HTMLElement);
}

function tabOf(text: string): HTMLElement {
  return [...host.querySelectorAll<HTMLElement>('.dc-sheet-tab')].find((t) => (t.querySelector('.dc-sheet-label')?.textContent ?? '') === text)!;
}

function tabMenu(text: string, item: string): void {
  escape();
  contextMenu(tabOf(text));
  entry(item).click();
}

async function chartOf(gridTile: string): Promise<string> {
  const charts = (): string[] => [...host.querySelectorAll<HTMLElement>('[data-tile^="chart-"]')].map((t) => t.dataset['tile']!);
  const before = new Set(charts());
  contextMenu(tile(gridTile).querySelector('.dc-row .dc-cell') as HTMLElement);
  await settle();
  nested('Create', 'Visualization').click();
  await settle();
  const chart = charts().find((id) => !before.has(id));
  assert.ok(chart, 'a chart on the page');
  return chart;
}

/** A chart's pill used as a hand does: Dynamic to Pinned (its grouping its own), or back -- picked from the menu it opens. */
async function togglePill(chart: string): Promise<void> {
  const pill = tile(chart).querySelector('.dc-tile-actions .dc-titlebar-toggle') as HTMLElement;
  const to = pill.textContent === 'Dynamic' ? 'Pinned' : 'Dynamic';
  pill.click();
  const item = [...dom.window.document.querySelectorAll<HTMLElement>('.dc-menu .dc-menu-item')]
    .find((e) => e.textContent?.startsWith(`${to} \u2014 `));
  if (!item) throw new Error(`the pill offers no ${to}`);
  item.click();
  await settle();
}

async function removeTile(id: string): Promise<void> {
  tile(id).querySelector<HTMLButtonElement>('.dc-tile-remove')!.click();
  await settle();
}

async function ungroup(id: string, column: string): Promise<void> {
  (tile(id).querySelector(`.dc-zone-rows .dc-chip[data-column="${column}"] .dc-chip-remove`) as HTMLElement).click();
  await settle();
}

function rename(id: string, name: string): void {
  tile(id).querySelector('.dc-tile-head')!.dispatchEvent(new dom.window.MouseEvent('dblclick', { bubbles: true }));
  const field = tile(id).querySelector<HTMLInputElement>('.dc-tile-rename');
  assert.ok(field, `${id}'s rename field`);
  field.value = name;
  field.dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: 'Enter' }));
}

/** Show Grid, from the chart's right-click menu (D7). */
async function showGrid(chart: string): Promise<void> {
  chartMenu(chart);
  entry('Show Grid').click();
  await settle();
}

function clickMark(chart: string, series: number, index: number): void {
  const drawnChart = getInstanceByDom(tile(chart).querySelector('.dc-chartpanel-chart') as HTMLElement);
  assert.ok(drawnChart, 'the chart is drawn');
  (drawnChart as unknown as { trigger(name: string, params: unknown): void }).trigger('click', { seriesIndex: series, dataIndex: index });
}

/** A header's right-click menu, opened on `column` in grid `id`. */
async function headerMenu(id: string, column: string): Promise<void> {
  escape();
  const th = tile(id).querySelector<HTMLElement>(`.dc-th[data-column="${column}"]`);
  assert.ok(th, `${id} shows a ${column} header (it shows ${[...tile(id).querySelectorAll<HTMLElement>('.dc-th')]
    .map((h) => h.dataset['column']).join(', ')})`);
  contextMenu(th);
  await settle();
}

/** A group's chevron clicked: the first group row that has one. */
function chevron(id: string): void {
  const el = tile(id).querySelector<HTMLElement>('.dc-row .dc-chevron:not(.dc-chevron-empty)');
  assert.ok(el, `${id} shows a group to open`);
  el.dispatchEvent(new dom.window.MouseEvent('click', { bubbles: true }));
}

/** The sheets' boards given a size (jsdom lays nothing out) and drawn at it, so a tile can be dragged on them. */
function sizedBoards(): void {
  for (const sheet of host.querySelectorAll<HTMLElement>('.dc-sheet')) {
    Object.defineProperty(sheet, 'clientWidth', { configurable: true, get: () => 1000 });
    Object.defineProperty(sheet, 'clientHeight', { configurable: true, get: () => 600 });
  }
  pg().setLayoutEditing(true);
}

/** Tile `id` dragged by its header and let go on tile `onto` -- its middle, or just inside its left edge (the board's
 *  canvas at the page's top left). */
function dragOnto(id: string, onto: string, where: 'middle' | 'left' = 'middle'): void {
  const head = tile(id).querySelector<HTMLElement>('.dc-tile-head')!;
  const at = tile(onto).style;
  const x = parseFloat(at.left) + (where === 'middle' ? parseFloat(at.width) / 2 : 4);
  const y = parseFloat(at.top) + parseFloat(at.height) / 2;
  const press = (type: string, clientX: number, clientY: number): void => {
    head.dispatchEvent(new dom.window.PointerEvent(type, { pointerId: 1, clientX, clientY, button: 0, bubbles: true }));
  };
  const from = tile(id).style;
  press('pointerdown', parseFloat(from.left) + 10, parseFloat(from.top) + 10);
  press('pointerup', x, y);
}

/** A grid's Column Properties for `column`, its Aggregation set to `fn`, and OK. */
async function setAggregation(at: string, column: string, fn: string): Promise<void> {
  await headerMenu(at, column);
  entry('Properties...').click();
  await settle();
  const win = dom.window.document.querySelector<HTMLElement>('[data-window="Properties"]')!;
  const select = [...win.querySelectorAll('.dc-field')].find((f) => f.querySelector('.dc-field-label')?.textContent === 'Aggregation:')!
    .querySelector('select') as HTMLSelectElement;
  select.value = fn;
  select.dispatchEvent(new dom.window.Event('change'));
  [...win.querySelectorAll<HTMLButtonElement>('.dc-editor-footer button')].find((b) => b.textContent === 'OK')!.click();
  await settle();
}

// -- the checks -----------------------------------------------------------------------------------------------------

/** A grid's commands applied to its saved cube as headless gridApply applies them (headless/grid.ts applied: the
 *  commands, the settings laid on the query, folded as the state lands, written as the screen writes it) -- no planner:
 *  what a run alone adds (column types) is no command's. */
const applyGrid: GridApplier = async (_page, cube, _id, commands) => {
  const snapshot = { ...cube.query, source: SNAPSHOT.source, epoch: 0 } as CubeSnapshot;
  const d = applyGridCommands({ snapshot, config: configurationOf(cube), tree: treeOf(cube) }, commands);
  const landed = fold({ snapshot: applyToSnapshot(d.snapshot, d.config), configuration: d.config, tree: d.tree });
  return writeCube({
    name: cube.name, source: cube.source, snapshot: landed.snapshot, configuration: landed.configuration, tree: landed.tree,
    ...(cube.unknown ? { unknown: cube.unknown } : {}),
  });
};

/** Where two JSON values differ: each path to a differing leaf (an array index as a number), at most `depth` deep. */
function differing(a: unknown, b: unknown, path = '', depth = 9): string[] {
  if (JSON.stringify(a) === JSON.stringify(b)) return [];
  const objects = typeof a === 'object' && a !== null && typeof b === 'object' && b !== null;
  if (!objects || depth === 0) return [path || '(the page)'];
  const keys = [...new Set([...Object.keys(a as object), ...Object.keys(b as object)])];
  return keys.flatMap((k) => differing((a as Record<string, unknown>)[k], (b as Record<string, unknown>)[k], path ? `${path}.${k}` : k, depth - 1));
}

type Check = 'told' | 'wire' | 'replays';
/** How a check failed: in words, and -- a replay -- the parts of the page that differ. */
interface Failed { readonly words: string; readonly paths?: readonly string[] }

/** What a gesture made the channel write, held to the three checks: each failing one, how. An undo or a redo is held
 *  as any gesture is: its write is the commands between (D19 W3, W4). */
async function judged(before: PageDocument, made: readonly Write[]): Promise<Partial<Record<Check, Failed>>> {
  assert.ok(made.length > 0, 'the gesture changed the page (the channel wrote)');
  const failed: Partial<Record<Check, Failed>> = {};
  let was = before;
  for (const w of made) {
    for (const e of w.write.edits) told.add('page' in e ? e.page.kind : e.command.kind);
    const sent = `[${w.write.edits.map((e) => toJson(e)).join(',')}]`;
    // the commands as the host reads them, applied as the host applies them
    let read: PageEdit[];
    try {
      read = readEdits(sent);
    } catch (e) {
      failed.wire = { words: e instanceof Error ? e.message : String(e) };
      was = w.screen;
      continue;
    }
    if (`[${read.map((e) => toJson(e)).join(',')}]` !== sent) failed.wire = { words: 'the host reads other commands than were sent' };
    try {
      const replayed = (await rebaseEdits({ page: was, held: idsOf(was) }, read, applyGrid)).page;
      const paths = differing(JSON.parse(pageToJson(replayed)), JSON.parse(pageToJson(w.screen)));
      if (paths.length > 0) failed.replays = { words: 'its commands, applied, make another page than the screen shows', paths };
    } catch (e) {
      failed.replays = { words: `its commands do not apply to the page before: ${e instanceof Error ? e.message : String(e)}` };
    }
    was = w.screen;
  }
  // nothing changed after the last write that no write says
  const after = differing(JSON.parse(pageToJson(was)), JSON.parse(pageToJson(onScreen())));
  if (after.length > 0) failed.told = { words: 'changed by no command: the screen shows another page than its writes make', paths: after };
  return failed;
}

/** A gesture: what it needs set up (on a page whose channel is not open yet), then made, as a person makes it. */
interface Gesture {
  readonly name: string;
  /** A change of the view alone (no part of the saved page): it must write nothing. */
  readonly quiet?: true;
  readonly setUp: () => Promise<Record<string, string>>;
  readonly make: (ids: Record<string, string>) => Promise<void> | void;
}

const withChart = async (): Promise<Record<string, string>> => { const g = await twoGrids(); return { ...g, chart: await chartOf(g.a) }; };
/** A page of a grid over the class Trade. */
const classGrid = async (): Promise<Record<string, string>> => {
  newPage();
  const a = pg().addGrid(overClass());
  await settle();
  return { a };
};
const EMEA = { kind: 'condition', column: 'region', operator: 'equal', value: 'EMEA' } as const;
/** Two grids, the first ungrouped to its rows (each cell a value of its column). */
const flat = async (): Promise<Record<string, string>> => {
  const g = await twoGrids();
  await ungroup(g.a, 'desk');
  await ungroup(g.a, 'region');
  await settle();
  return g;
};
/** Two grids, the first with a calculated column: notional shown as a running total (Show As). */
const shownAs = async (): Promise<Record<string, string>> => {
  const g = await twoGrids();
  await headerMenu(g.a, 'notional');
  nested('Show notional As', 'Running total').click();
  await drawn();
  return g;
};
/** The calculated column shownAs made. */
const RUNNING = 'notional_runningTotal';
/** A flat grid filtered from a cell: desk = its first value. */
const filtered = async (): Promise<Record<string, string>> => {
  const g = await flat();
  escape();
  contextMenu(tile(g.a!).querySelector('.dc-row .dc-cell[data-column="desk"]') as HTMLElement);
  await settle();
  nested('Filter').click();
  await settle();
  return g;
};
const withSheet = async (): Promise<Record<string, string>> => {
  const g = await twoGrids();
  (host.querySelector('.dc-sheet-add') as HTMLElement).click();
  await settle();
  return g;
};

const GESTURES: readonly Gesture[] = [
  // the page
  { name: 'a grid added', setUp: twoGrids, make: async ({ a }) => { pg().addGrid(over('desks.csv'), { near: a! }); } },
  { name: 'a sheet added', setUp: twoGrids, make: () => (host.querySelector('.dc-sheet-add') as HTMLElement).click() },
  {
    name: 'a sheet renamed',
    setUp: withSheet,
    make: () => {
      tabOf('Sheet 2').dispatchEvent(new dom.window.MouseEvent('dblclick', { bubbles: true }));
      const field = host.querySelector<HTMLInputElement>('.dc-sheet-tab input')!;
      field.value = 'Charts';
      field.dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: 'Enter' }));
    },
  },
  { name: 'a sheet moved', setUp: withSheet, make: () => tabMenu('Sheet 2', 'Move Left') },
  { name: 'a sheet deleted', setUp: withSheet, make: () => tabMenu('Sheet 2', 'Delete') },
  { name: 'a tile renamed', setUp: twoGrids, make: ({ b }) => rename(b!, 'Orders') },
  { name: 'a chart added', setUp: twoGrids, make: async ({ a }) => { await chartOf(a!); } },
  { name: 'a chart frozen', setUp: withChart, make: ({ chart }) => togglePill(chart!) },
  { name: 'a chart removed', setUp: withChart, make: ({ chart }) => removeTile(chart!) },
  { name: 'a grid removed', setUp: twoGrids, make: ({ b }) => removeTile(b!) },
  { name: 'a grid moved to another sheet', setUp: withSheet, make: ({ b }) => gridMenuItem(b!, 'Sheet 2').click() },
  { name: 'a grid copied', setUp: twoGrids, make: ({ a }) => gridMenuItem(a!, 'Copy of This Grid').click() },
  {
    name: 'a sheet arranged',
    setUp: twoGrids,
    make: async () => {
      escape();
      (host.querySelector('.dc-page-bar .dc-titlebar-menu') as HTMLElement).click();
      entry('Arrange…').click();
      await settle();
      (await shown<HTMLButtonElement>(host, '.dc-layout-picker [data-preset="stacked"]')).click();
    },
  },
  {
    // a lone tile let go on another's middle: told as stack (P14), not as the sheet's layout whole
    name: 'a chart stacked on its grid by dragging',
    setUp: async () => { const ids = await withChart(); sizedBoards(); return ids; },
    make: ({ a, chart }) => dragOnto(chart!, a!),
  },
  {
    // a drop beside a tile is no command of its own: the sheet's layout, whole
    name: 'a grid dragged to the left of another',
    setUp: async () => { const ids = await twoGrids(); sizedBoards(); return ids; },
    make: ({ a, b }) => dragOnto(b!, a!, 'left'),
  },
  { name: 'a page filter set', setUp: classGrid, make: async () => { await pg().setFilter({ class: TRADE, condition: EMEA }); } },
  {
    name: 'a page filter taken off',
    setUp: async () => {
      const g = await classGrid();
      await pg().setFilter({ class: TRADE, condition: EMEA });
      return { ...g, filter: onScreen().filters![0]!.id };
    },
    make: async ({ filter }) => { await pg().removeFilter(filter!); },
  },
  {
    name: "the page saved by its host's Save: the record it is, told (B13)",
    setUp: twoGrids,
    // the host's Save: the record the page is saved as, applied to the page's state by its command and told (the S7
    // audit's H-4: the host had kept it beside the page)
    make: () => { pg().savedAs('r-1'); },
  },
  {
    name: 'the page renamed in its name box',
    setUp: twoGrids,
    make: () => {
      const box = host.querySelector<HTMLElement>('.dc-page-name')!;
      box.click();
      const field = box.querySelector('input')!;
      field.value = 'Q4 review';
      field.dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: 'Enter' }));
    },
  },
  {
    name: 'a page edit undone',
    setUp: withSheet,
    make: () => pg().undo(),
  },
  {
    name: 'a page edit undone and redone',
    setUp: withSheet,
    make: () => { pg().undo(); pg().redo(); },
  },
  // 2026-10-24 (D19 W3): a grid's undo is the commands between the step and the one before it (grid-between.ts), told as
  // any edit is -- a move that queries, and a presentation change (no query) alike (the S7 audit's G-1 closed)
  {
    name: 'a grid edit undone',
    setUp: async () => {
      const g = await twoGrids();
      assert.equal(await cubes.get(g.a!)!.edit([{ kind: 'group', rows: ['desk'] }], 'Group'), null);
      await settle();
      return g;
    },
    make: async ({ a }) => { await cubes.get(a!)!.undo(); },
  },
  {
    name: "a grid's Collapse All undone (the tree put back whole: W1)",
    setUp: async () => {
      const g = await twoGrids();
      assert.equal(await cubes.get(g.a!)!.edit([{ kind: 'group', rows: ['desk'] }], 'Group'), null);
      await settle();
      chevron(g.a!);
      await drawn();
      escape();
      contextMenu(tile(g.a!).querySelector('.dc-row .dc-cell') as HTMLElement);
      entry('Collapse All').click();
      await drawn();
      return g;
    },
    make: async ({ a }) => { await cubes.get(a!)!.undo(); },
  },
  {
    name: "a grid's presentation edit undone",
    setUp: async () => {
      const g = await twoGrids();
      assert.equal(await cubes.get(g.a!)!.edit([{ kind: 'configure', patch: { columns: { desk: { pinned: 'left' } } } }], 'Pin'), null);
      await settle();
      return g;
    },
    make: async ({ a }) => { await cubes.get(a!)!.undo(); },
  },
  {
    name: "a grid's presentation edit undone and redone",
    setUp: async () => {
      const g = await twoGrids();
      assert.equal(await cubes.get(g.a!)!.edit([{ kind: 'configure', patch: { columns: { desk: { pinned: 'left' } } } }], 'Pin'), null);
      await settle();
      return g;
    },
    make: async ({ a }) => { const grid = cubes.get(a!)!; await grid.undo(); await grid.redo(); },
  },
  // charts
  {
    name: "a chart's mark clicked",
    setUp: async () => { const ids = await withChart(); await drawn(); return ids; },
    make: ({ chart }) => clickMark(chart!, 0, 0),
  },
  {
    // its selection on its grid's filter, the same condition put there by hand too: removeTile's rule takes the
    // selection off once, as the screen does -- the grid's change no second command (the S1 audit's 5)
    name: 'a chart with a mark removed, its condition also on its grid by hand',
    setUp: async () => {
      const ids = await withChart();
      await drawn();
      clickMark(ids['chart']!, 0, 0);
      await settle();
      const chart = onScreen().views.find((v) => v.id === ids['chart']);
      const mark = chart?.kind === 'chart' ? chart.selection?.[0] : undefined;
      assert.ok(mark, 'the mark is the chart\'s selection');
      await (pg().grid(ids['a']!) as unknown as CubeApp).edit([{ kind: 'addFilter', condition: mark }], 'filter');
      await settle();
      return ids;
    },
    make: ({ chart }) => removeTile(chart!),
  },
  // Show Grid (D7): a following chart's grid shown, the page as it is; a frozen or a detached chart's grid of its own
  // grouping made, one step -- addGrid, followGrid, setChart, its selection's select first -- that it follows
  { name: "a following chart's grid shown", quiet: true, setUp: withChart, make: ({ chart }) => showGrid(chart!) },
  {
    name: 'a frozen chart shown in a grid of its own',
    setUp: async () => { const ids = await withChart(); await togglePill(ids['chart']!); return ids; },
    make: ({ chart }) => showGrid(chart!),
  },
  {
    name: 'a frozen chart with a mark selected shown in a grid of its own',
    setUp: async () => {
      const ids = await withChart();
      await togglePill(ids['chart']!);
      await drawn();
      clickMark(ids['chart']!, 0, 0);
      await settle();
      return ids;
    },
    make: ({ chart }) => showGrid(chart!),
  },
  {
    name: 'a detached chart shown in a grid of its own',
    setUp: async () => {
      const g = await twoGrids();
      const chart = await chartOf(g.b);
      await togglePill(chart);
      await removeTile(g.b);
      return { ...g, chart };
    },
    make: ({ chart }) => showGrid(chart!),
  },
  {
    // 2026-10-24 (D19 W19): the chart back on the very cube it read, detached -- `detach`, the one command no gesture
    // but an undo tells
    name: 'a detached chart shown in a grid of its own, undone',
    setUp: async () => {
      const g = await twoGrids();
      const chart = await chartOf(g.b);
      await togglePill(chart);
      await removeTile(g.b);
      showGrid(chart);
      await drawn();
      return { ...g, chart };
    },
    make: () => pg().undo(),
  },
  { name: 'a grid with a following chart ungrouped', setUp: withChart, make: ({ a }) => ungroup(a!, 'desk') },
  // grids
  { name: 'a grid ungrouped', setUp: twoGrids, make: ({ a }) => ungroup(a!, 'desk') },
  { name: 'a column sorted from its header menu', setUp: twoGrids, make: async ({ a }) => { await headerMenu(a!, 'notional'); entry('Ascending').click(); } },
  {
    name: "a column's sort cleared from its header menu",
    setUp: async () => { const g = await twoGrids(); await headerMenu(g.a, 'notional'); entry('Ascending').click(); await settle(); return g; },
    make: async ({ a }) => { await headerMenu(a!, 'notional'); entry('Clear Sort').click(); },
  },
  {
    name: 'a sort added from its header menu',
    setUp: async () => { const g = await twoGrids(); await headerMenu(g.a, 'notional'); entry('Ascending').click(); await settle(); return g; },
    make: async ({ a }) => { await headerMenu(a!, 'notional'); entry('Add Descending').click(); },
  },
  {
    name: 'a header clicked to sort',
    setUp: twoGrids,
    make: ({ a }) => tile(a!).querySelector<HTMLElement>('.dc-th[data-column="notional"]')!.click(),
  },
  {
    name: 'the tree column clicked to sort the groups',
    setUp: twoGrids,
    make: ({ a }) => tile(a!).querySelector<HTMLElement>('.dc-th[data-column="__tree"]')!.click(),
  },
  { name: 'a column hidden', setUp: flat, make: async ({ a }) => { await headerMenu(a!, 'desk'); entry('Hide desk').click(); } },
  { name: "a column's Aggregation set in Column Properties", setUp: twoGrids, make: ({ a }) => setAggregation(a!, 'notional', 'max') },
  {
    name: 'a filter added from a cell',
    setUp: flat,
    make: async ({ a }) => {
      escape();
      const cell = tile(a!).querySelector('.dc-row .dc-cell[data-column="desk"]');
      assert.ok(cell, `a desk cell (rows: ${[...tile(a!).querySelectorAll('.dc-row')].map((r) => [...r.querySelectorAll('.dc-cell')].map((c) => (c as HTMLElement).dataset['column']).join(',')).join(' / ')})`);
      contextMenu(cell);
      await settle();
      nested('Filter').click();
    },
  },
  {
    name: 'a measure shown as a running total (Show As)',
    setUp: twoGrids,
    make: async ({ a }) => { await headerMenu(a!, 'notional'); nested('Show notional As', 'Running total').click(); },
  },
  {
    name: 'a calculated column renamed in its Edit Column window',
    setUp: shownAs,
    make: async ({ a }) => {
      // the menu's Edit Column (the fixture's engine answers its own columns only: no header shows the new one)
      cubes.get(a!)!.openColumnEditor({ edit: RUNNING });
      await settle();
      const name = host.ownerDocument.querySelector<HTMLInputElement>('.dc-calc-input-name');
      assert.ok(name, 'the Edit Column window');
      name.value = 'running';
      name.dispatchEvent(new dom.window.Event('input'));
      // the editor compiles what is typed after its pause (upstream's 500 ms), and offers OK once it has
      await new Promise((r) => setTimeout(r, 700));
      await settle();
      const ok = host.ownerDocument.querySelector<HTMLButtonElement>('.dc-calc-ok');
      assert.ok(ok && !ok.disabled, `OK offered (${host.ownerDocument.querySelector('.dc-calc-problem')?.textContent ?? ''})`);
      ok.click();
    },
  },
  {
    name: 'a calculated column deleted in its Edit Column window',
    setUp: shownAs,
    make: async ({ a }) => {
      cubes.get(a!)!.openColumnEditor({ edit: RUNNING });
      await settle();
      const remove = host.ownerDocument.querySelector<HTMLButtonElement>('.dc-calc-delete');
      assert.ok(remove, 'the Edit Column window, its Delete');
      remove.click();
    },
  },
  {
    name: 'a filter changed in the Filters window',
    setUp: filtered,
    make: async ({ a }) => {
      cubes.get(a!)!.openFilters();
      await settle();
      host.ownerDocument.querySelector<HTMLButtonElement>('.dc-filter-not')!.click();
      await settle();
      host.ownerDocument.querySelector<HTMLButtonElement>('.dc-filter-ok')!.click();
    },
  },
  { name: 'a grid snapped by its toggle', setUp: snapGrid, make: ({ a }) => toggle(a!) },
  {
    name: 'a snapped grid made live again by its toggle',
    setUp: async () => { const g = await snapGrid(); toggle(g.a!); await drawn(); return g; },
    make: ({ a }) => toggle(a!),
  },
  { name: 'a group opened', setUp: twoGrids, make: ({ a }) => chevron(a!) },
  {
    name: 'every group collapsed',
    setUp: async () => { const g = await twoGrids(); chevron(g.a); await settle(); return g; },
    make: ({ a }) => {
      escape();
      contextMenu(tile(a!).querySelector('.dc-row .dc-cell') as HTMLElement);
      entry('Collapse All').click();
    },
  },
  {
    // opened only by the level its settings open it to: Collapse All offered, as Python's collapse_all() clears it (the
    // S7 audit's G-5: the menu offered it only for a group opened by hand)
    name: 'every group collapsed, a grid opened to its level',
    setUp: async () => {
      const g = await twoGrids();
      // two row groups: the level opens the first's groups (it never opens the deepest: the S7 confirmation audit's G2-4)
      assert.equal(await cubes.get(g.a!)!.edit([{ kind: 'group', rows: ['region', 'desk'] }, { kind: 'configure', patch: { initialExpandToLevel: 1 } }], 'Level'), null);
      await settle();
      return g;
    },
    make: ({ a }) => {
      escape();
      contextMenu(tile(a!).querySelector('.dc-row .dc-cell') as HTMLElement);
      const item = entry('Collapse All');
      assert.ok(item, 'Collapse All offered');
      item.click();
    },
  },
  {
    name: 'a grid added while the page bar is folded',
    setUp: async () => { const g = await twoGrids(); pg().setBarFolded(true); await settle(); return g; },
    make: ({ a }) => { pg().addGrid(over('desks.csv'), { near: a! }); },
  },
  {
    name: "a grid's settings set by its host (edit: configure)",
    setUp: twoGrids,
    make: async ({ a }) => { await cubes.get(a!)!.edit([{ kind: 'configure', patch: { maxRows: 7 } }], 'settings'); },
  },
];

/** A gap a gesture shows: its items, and how it fails (the host's words; the parts of the page that differ). */
interface Gap { readonly items: readonly string[]; readonly words?: RegExp; readonly paths?: readonly string[] }
/** Each gesture's checks that fail today, by the plan's items: the only failures allowed, until each is closed. */
const L2_GAPS: Record<string, Partial<Record<Check, Gap>>> = {};

/** The command kinds no gesture here tells, each by its item or why: the screen makes none, or none is driven yet. */
const UNTOLD: Record<string, { readonly items: readonly string[]; readonly why?: string }> = {
  // the model home is not in this harness: page-commands.test.ts holds what the screen tells (modelCommands), and
  // python/tests/test_model.py the commands Python's model actions apply
  modelInto: { items: [], why: 'a model action needs a model home: page-commands.test.ts modelCommands' },
  modelPin: { items: [], why: 'a model action needs a model home: page-commands.test.ts modelCommands' },
  modelKeep: { items: [], why: 'a model action needs a model home: page-commands.test.ts modelCommands' },
  // a cube opened again over its frames as they are now: told as a host's page opens (test/python-engine), and by Python
  rebuilt: { items: [], why: "opening a host's page whose frame changed in place: test/python-engine, python/tests/test_page.py" },
};
/** No gesture leaves: the list only grows (a gesture taken off is a check taken off). 2026-10-22 (the S7 confirmation
 *  audit's G2-5): 40 -> 51, every gesture the catalogue holds, S7's among them -- the floor had not followed it. */
const GESTURES_AT_LEAST = 51;

describe('L2, L3: every gesture told as commands that replay to the page it made, and that the host reads', () => {
  it('the gestures and their gaps are one list', () => {
    const names = GESTURES.map((g) => g.name);
    assert.equal(new Set(names).size, names.length, 'each gesture named once');
    assert.ok(names.length >= GESTURES_AT_LEAST, `${names.length} gestures: none is taken off (at least ${GESTURES_AT_LEAST})`);
    assert.deepEqual(Object.keys(L2_GAPS).filter((n) => !names.includes(n)), [], 'gaps listed for no gesture');
  });

  it('a page left alone writes nothing (the control)', async () => {
    await twoGrids();
    await drawn();
    channel = new LiveChannel(editor, screenOf(), 0);
    channel.opened(1);
    await drawn();
    await channel.idle();
    assert.equal(writes.length, 0, 'nothing written: no gesture made');
  });

  for (const g of GESTURES) {
    it(g.name, async () => {
      const ids = await g.setUp();
      await drawn();
      channel = new LiveChannel(editor, screenOf(), 0);
      channel.opened(1);
      const before = onScreen();
      // quiet first: what the set-up started has settled, and nothing is written before the gesture
      await drawn();
      await channel.idle();
      assert.equal(writes.length, 0, `${g.name}: the page was not quiet before the gesture (a write came from its set-up)`);
      assert.equal(pageToJson(onScreen()), pageToJson(before), `${g.name}: the page changed before the gesture`);
      await g.make(ids);
      await drawn();
      await channel.idle();
      await drawn();
      // the board is the page's state (S3: the board produced by the commands): what it shows is what the state holds
      // (as content: a key's place in the text is the writer's)
      const state = pg().state.page;
      assert.deepEqual(JSON.parse(JSON.stringify({ views: onScreen().views, sheets: onScreen().sheets })),
        JSON.parse(JSON.stringify({ views: state.views, sheets: state.sheets })), `${g.name}: the board shows another page than its state holds`);
      if (g.quiet) {
        assert.equal(writes.length, 0, `${g.name}: a change of the view alone wrote the page`);
        return;
      }
      const failed = await judged(before, writes);
      const listed = L2_GAPS[g.name] ?? {};
      for (const check of ['told', 'wire', 'replays'] as const) {
        const f = failed[check];
        const gap = listed[check];
        if (f === undefined) {
          assert.ok(!gap, `${g.name}: ${check} passes now -- close ${gap?.items.join(', ')} and take it off`);
          continue;
        }
        assert.ok(gap, `${g.name}: ${check} fails -- ${f.words}${f.paths ? ` (${f.paths.join(', ')})` : ''} -- a new gap: fix it, or list it with its item`);
        if (gap.words) assert.match(f.words, gap.words, `${g.name}: ${check} fails another way than ${gap.items.join(', ')}: ${f.words}`);
        if (gap.paths) assert.deepEqual(f.paths, gap.paths, `${g.name}: ${check} differs in other parts of the page than ${gap.items.join(', ')}'s`);
      }
    });
  }

  after(() => {
    const kinds = [...GRID_COMMAND_KINDS, ...PAGE_COMMAND_KINDS];
    const problems = [
      ...kinds.filter((k) => !told.has(k) && UNTOLD[k] === undefined).map((k) => `${k}: no gesture tells it -- drive one here, or list it`),
      ...Object.keys(UNTOLD).filter((k) => told.has(k)).map((k) => `${k}: a gesture tells it now -- take it off the list`),
      ...Object.keys(UNTOLD).filter((k) => !(kinds as readonly string[]).includes(k)).map((k) => `${k}: no such kind`),
    ];
    assert.deepEqual(problems, [], 'every command kind is told by some gesture, or listed');
  });
});

describe("Copy of Grid adds the grid's copy by the one rule Python's grid.copy asks (K10: cube-document.ts cubeCopied)", () => {
  it('the addGrid it tells carries the copied grid\'s cube as cubeCopied makes it: its query and settings, no group open', async () => {
    const { a } = await twoGrids();
    // a group of the grid copied open first: the copy starts with none open, as a new grid does
    chevron(a);
    await drawn();
    channel = new LiveChannel(editor, screenOf(), 0);
    channel.opened(1);
    await drawn();
    await channel.idle();
    const was = onScreen();
    const original = was.cubes.find((c) => c.id === was.views.find((v) => v.id === a)?.cube)?.cube;
    assert.ok(original && original.tree.open.length > 0, 'the grid copied has a group open');
    gridMenuItem(a, 'Copy of This Grid').click();
    await drawn();
    await channel.idle();
    await drawn();
    const added = writes.flatMap((w) => w.write.edits)
      .flatMap((e) => ('page' in e && e.page.kind === 'addGrid' ? [e.page] : []));
    assert.equal(added.length, 1, 'one grid added');
    const copy = added[0]!.document;
    assert.ok(copy, 'its document written');
    assert.deepEqual(JSON.parse(cubeToJson(copy)), JSON.parse(cubeToJson(cubeCopied(original))));
    assert.deepEqual(copy.tree.open, [], 'no group open in the copy');
  });

  it("a grid opened to a level by its settings: the copy opened to it too, on both sides (the S7 audit's G-2)", async () => {
    const { a } = await twoGrids();
    assert.equal(await cubes.get(a)!.edit([{ kind: 'group', rows: ['desk'] }, { kind: 'configure', patch: { initialExpandToLevel: 1 } }], 'Level'), null);
    await drawn();
    channel = new LiveChannel(editor, screenOf(), 0);
    channel.opened(1);
    await drawn();
    await channel.idle();
    const was = onScreen();
    const original = was.cubes.find((c) => c.id === was.views.find((v) => v.id === a)?.cube)?.cube;
    assert.ok(original);
    gridMenuItem(a, 'Copy of This Grid').click();
    await drawn();
    await channel.idle();
    await drawn();
    const copy = writes.flatMap((w) => w.write.edits)
      .flatMap((e) => ('page' in e && e.page.kind === 'addGrid' ? [e.page] : []))[0]?.document;
    assert.ok(copy, 'its document written');
    assert.equal(copy.tree.expandTo, 1, 'the screen\'s copy opened to its level');
    assert.deepEqual(JSON.parse(cubeToJson(copy)), JSON.parse(cubeToJson(cubeCopied(original))), "Python's copy the same");
  });
});

describe("A chart's mark clicked selects it by the one rule Python's chart.select asks (K7, D8: headless chartPick)", () => {
  it('the refilter and select the click tells are what chartPick makes of the mark named by its value', async () => {
    const ids = await withChart();
    await drawn();
    channel = new LiveChannel(editor, screenOf(), 0);
    channel.opened(1);
    await drawn();
    await channel.idle();
    const was = onScreen();
    clickMark(ids['chart']!, 0, 0);
    await drawn();
    await channel.idle();
    await drawn();
    const edits = writes.flatMap((w) => w.write.edits);
    const chart = was.views.find((v): v is ChartView => v.id === ids['chart'] && v.kind === 'chart')!;
    const selected = onScreen().views.find((v): v is ChartView => v.id === ids['chart'] && v.kind === 'chart')?.selection ?? [];
    // the mark as Python names it: its value across (and its series', on a split chart), as the click's selection says
    const valueOf = (column: string | undefined): unknown => {
      const c = selected.find((n) => n.kind === 'condition' && n.column === column);
      return c?.kind === 'condition' ? c.value : undefined;
    };
    assert.ok(valueOf(chart.spec.x) !== undefined, 'the click selected a mark across');
    const asked = { value: valueOf(chart.spec.x), ...(chart.spec.split !== undefined ? { split: valueOf(chart.spec.split) } : {}) };
    const said = JSON.parse(chartPick(pageToJson(was), chart.id, JSON.stringify(asked))) as {
      grid: string; commands: unknown[]; page: unknown[]; problems?: string[];
    };
    assert.equal(said.problems, undefined);
    assert.deepEqual(edits.flatMap((e) => ('grid' in e ? [{ grid: e.grid, command: e.command }] : [])),
      said.commands.map((command) => ({ grid: said.grid, command })), "the grid's refilter");
    assert.deepEqual(edits.flatMap((e) => ('page' in e ? [e.page] : [])), said.page, "the page's select");
  });
});

describe('P9: a saved page opened keeps what it cannot open', () => {
  /** The page of two grids and a chart of the second, saved, opened again with only the first grid's source. */
  const opened = async (): Promise<{ saved: PageDocument; a: string; b: string; chart: string }> => {
    const { a, b } = await twoGrids();
    const chart = await chartOf(b);
    const saved = readPage(pageToJson(onScreen()));
    page!.dispose();
    newPage();
    const kept = saved.views.find((v) => v.id === a) as { readonly cube: string };
    pg().restore(saved, new Map([[kept.cube, over('trades.csv')]]));
    await settle();
    return { saved, a, b, chart };
  };

  it('a view whose grid cannot be opened is kept in its place, saying why, and written back as saved', async () => {
    const { saved, b } = await opened();
    assert.match(tile(b).querySelector('.dc-tile-unavailable')?.textContent ?? '', /orders\.csv could not be opened/);
    const now = onScreen();
    assert.deepEqual([now.views, now.cubes, now.sheets], [saved.views, saved.cubes, saved.sheets],
      'the page written is the page saved: nothing it could not open dropped');
  });

  it("a kept grid's snap is written back with it, as saved (the S1 audit's 11)", async () => {
    const { a, b } = await twoGrids();
    const doc = readPage(pageToJson(onScreen()));
    const cubeOf = (grid: string): string => (doc.views.find((v) => v.id === grid) as { readonly cube: string }).cube;
    const copy = { table: 'snap_orders', rowCount: 2, takenAt: '2026-10-21T10:00:00.000Z', label: 'Snap 10:00' };
    const saved = readPage(pageToJson({ ...doc, copies: { [cubeOf(b)]: copy } }));
    page!.dispose();
    newPage();
    pg().restore(saved, new Map([[cubeOf(a), over('trades.csv')]]));
    await settle();
    assert.deepEqual(onScreen().copies, saved.copies, 'the copy the kept grid read, kept with it');
  });

  it('a kept tile removed takes its view, its cube and its charts, as removeTile does; undone, it is back', async () => {
    const { saved, b, chart } = await opened();
    await removeTile(b);
    const now = onScreen();
    const replayed = applyPageCommand({ page: saved, held: idsOf(saved) }, { kind: 'removeTile', tile: b }).draft.page;
    assert.deepEqual([now.views, now.cubes, now.sheets], [replayed.views, replayed.cubes, replayed.sheets],
      'the screen and removeTile agree');
    assert.equal(now.views.some((v) => v.id === chart), false, 'its following chart went with it');
    pg().undo();
    await settle();
    // kept again, as saved -- a new tile (B12), under the id its cube was given
    const back = sameButIds(saved, onScreen());
    assert.ok(back.same, `undone: kept again, as saved: ${JSON.stringify(back.a)} vs ${JSON.stringify(back.b)}`);
    assert.deepEqual(Object.keys(back.renamed), [b, chart], 'the kept tile and its chart, brought back as new tiles');
  });
});
