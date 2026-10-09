// The board's charts as a person meets them (plan B1, revised 2026-09-30): any number follow the
// grid, each badged so; Freeze keeps one's grouping and Follow the grid takes it back; Open in
// grid edits a frozen chart's grouping in a GRID OF ITS OWN beside it -- the cube's grid is not
// touched -- and Update writes it back.

import assert from 'node:assert/strict';
import { afterEach, beforeEach, describe, it } from 'node:test';

import { tiles } from '../src/layout/bands.ts';
import type { ChartView } from '../src/page-document.ts';
import { app, dom, GateEngine, remount, root, settle, setUp, SNAPSHOT, StubPlanner, tearDown } from './cube-fixture.ts';

// The fixture's cube is grouped by region, then desk: a following chart is region across, split
// by desk; taking desk out of the row groups is a pivot it follows.
beforeEach(async () => {
  await setUp();
  // jsdom has no canvas: a 2D context that draws nothing, so ECharts can mount and these tests
  // read the tiles' state (drawing is chart-echarts.test.ts's, and the browser harness's)
  const inert: ProxyHandler<object> = {
    get: (_t, key) => (key === 'canvas' ? dom.window.document.createElement('canvas')
      : key === 'measureText' ? () => ({ width: 0 }) : () => undefined),
    set: () => true,
  };
  (dom.window.HTMLCanvasElement.prototype as unknown as { getContext: () => unknown }).getContext =
    () => new Proxy({}, inert);
});
afterEach(tearDown);


const charts = (): ChartView[] => app.pageViews().views.filter((v): v is ChartView => v.kind === 'chart');
const chart = (id: string): ChartView => charts().find((c) => c.id === id)!;
const tile = (id: string): HTMLElement => root.querySelector(`[data-tile="${id}"]`) as HTMLElement;
const shown = (id: string): string[] => [...tile(id).querySelectorAll<HTMLElement>('.dc-tile-actions > *')]
  .filter((el) => !el.hidden).map((el) => el.textContent ?? '');
const click = async (id: string, text: string): Promise<void> => {
  const el = [...tile(id).querySelectorAll<HTMLElement>('.dc-tile-actions > button')].find((b) => b.textContent === text);
  assert.ok(el && !el.hidden, `${id} has a "${text}" button: ${shown(id).join(', ')}`);
  el.click();
  await settle();
};
/** A chart's Dynamic / Frozen / Detached pill: what it says. */
const pill = (id: string): string => tile(id).querySelector('.dc-tile-actions .dc-titlebar-toggle')?.textContent ?? '';
/** Toggle a chart between Dynamic and Frozen, by its pill. */
const togglePill = async (id: string): Promise<void> => {
  (tile(id).querySelector('.dc-tile-actions .dc-titlebar-toggle') as HTMLElement).click();
  await settle();
};
/** The entries of a tile's right-click menu, opened on its body. */
const chartMenu = (id: string): HTMLElement[] => {
  (tile(id).querySelector('.dc-chart-tile') as HTMLElement)
    .dispatchEvent(new dom.window.MouseEvent('contextmenu', { bubbles: true, cancelable: true }));
  return [...dom.window.document.querySelectorAll<HTMLElement>('.dc-menu .dc-menu-item')];
};
const menuEntry = (items: HTMLElement[], label: string): HTMLElement | undefined =>
  items.find((el) => el.querySelector(':scope > .dc-menu-label')?.textContent === label);
/** Open in grid, from the chart's right-click menu. */
const openInGrid = async (id: string): Promise<void> => {
  const item = menuEntry(chartMenu(id), 'Open in grid');
  assert.ok(item && item.getAttribute('aria-disabled') !== 'true', `${id} offers Open in grid`);
  item.click();
  await settle();
};
/** Insert > Chart, from a grid's right-click menu (a grid in a tile). */
const chartOf = async (gridTile: string): Promise<void> => {
  (tile(gridTile).querySelector('.dc-row .dc-cell') as HTMLElement)
    .dispatchEvent(new dom.window.MouseEvent('contextmenu', { bubbles: true, cancelable: true }));
  await settle();
  const item = [...dom.window.document.querySelectorAll<HTMLElement>('.dc-menu .dc-menu-item')]
    .find((el) => el.querySelector(':scope > .dc-menu-label')?.textContent === 'Visualization'
      && el.parentElement?.closest('.dc-menu-item')?.querySelector(':scope > .dc-menu-label')?.textContent === 'Insert');
  assert.ok(item, 'Insert > Chart');
  item.click();
  await settle();
};
/** The main grid's row-group zone: the cube's own, not an editing grid's. */
const mainZone = (): HTMLElement => [...root.querySelectorAll<HTMLElement>('.dc-zone-rows')]
  .find((z) => !z.closest('[data-tile^="edit-"]'))!;
/** Take a column out of a row-group zone, by its chip's x. */
async function ungroup(zone: HTMLElement, column: string): Promise<void> {
  (zone.querySelector(`.dc-chip[data-column="${column}"] .dc-chip-remove`) as HTMLElement).click();
  await settle();
}
const chips = (zone: HTMLElement): string[] => [...zone.querySelectorAll<HTMLElement>('.dc-chip')].map((c) => c.dataset['column'] ?? '');

async function twoCharts(): Promise<[string, string]> {
  app.openChart();
  app.openChart();
  await settle();
  const [a, b] = charts();
  return [a!.id, b!.id];
}

describe('charts follow the grid until frozen', () => {
  it('any number follow, each badged, and a pivot changes every one', async () => {
    const [a, b] = await twoCharts();
    for (const id of [a, b]) {
      assert.equal(pill(id), 'Dynamic');
      assert.equal(menuEntry(chartMenu(id), 'Open in grid')?.getAttribute('aria-disabled'), 'true',
        'a dynamic chart has nothing of its own to open');
    }
    await ungroup(mainZone(), 'desk');
    assert.deepEqual([chart(a).spec.split, chart(b).spec.split], [undefined, undefined]);
  });

  it('a frozen chart keeps its grouping; Follow the grid takes the grid\'s again', async () => {
    const [a, b] = await twoCharts();
    await togglePill(a);
    assert.equal(pill(a), 'Frozen');
    assert.notEqual(menuEntry(chartMenu(a), 'Open in grid')?.getAttribute('aria-disabled'), 'true');
    await ungroup(mainZone(), 'desk');
    assert.equal(chart(a).spec.split, 'desk', 'frozen: the pivot did not change it');
    assert.equal(chart(b).spec.split, undefined, 'the other still follows');
    await togglePill(a);
    assert.equal(chart(a).spec.split, undefined, 'following again, it took the grid\'s grouping');
    assert.equal(pill(a), 'Dynamic');
  });
});

describe('Open in grid: a grid of its own, beside the chart', () => {
  it('opens a second cube with the chart\'s grouping and leaves the cube\'s grid alone; Update writes it back', async () => {
    const [a] = await twoCharts();
    await togglePill(a);
    await ungroup(mainZone(), 'desk');
    assert.deepEqual(app.snapshot.rows, ['region']);
    await openInGrid(a);
    const editing = tile(`edit-${a}`);
    assert.ok(editing, 'an editing tile on the board');
    const editZone = editing.querySelector('.dc-zone-rows') as HTMLElement;
    assert.deepEqual(chips(editZone), ['region', 'desk'], 'grouped as the chart is');
    assert.notEqual(editing.querySelector('.dc-app')?.getAttribute('data-dc-cube'), root.getAttribute('data-dc-cube'),
      'a cube of its own');
    assert.deepEqual(app.snapshot.rows, ['region'], 'the cube\'s grid is untouched');
    // not part of the page while open
    assert.ok(!tiles(app.pageViews().layout).includes(`edit-${a}`));
    // re-group there, and update the chart
    await ungroup(editZone, 'desk');
    const title = app.pageViews().views.find((v) => v.id === a)!.title;
    await click(`edit-${a}`, `Update ${title}`);
    assert.equal(chart(a).spec.split, undefined, 'the chart took the editing grid\'s grouping');
    assert.equal(chart(a).spec.frozen, true, 'and is still frozen');
    assert.equal(tile(`edit-${a}`), null, 'the editing grid is closed');
    assert.deepEqual(app.snapshot.rows, ['region'], 'the cube\'s grid never moved');
  });

  it('removing the editing grid leaves the chart as it was', async () => {
    const [a] = await twoCharts();
    await togglePill(a);
    const before = JSON.stringify(chart(a).spec);
    await openInGrid(a);
    (tile(`edit-${a}`).querySelector('.dc-tile-remove') as HTMLElement).click();
    await settle();
    assert.equal(tile(`edit-${a}`), null);
    assert.equal(JSON.stringify(chart(a).spec), before);
    assert.equal(charts().length, 2);
  });

  it('Ctrl-Z inside the editing grid undoes it, not the cube', async () => {
    const [a] = await twoCharts();
    await togglePill(a);
    await openInGrid(a);
    // looked up each time: the zone is drawn again as the editing cube changes
    const editZone = (): HTMLElement => tile(`edit-${a}`).querySelector('.dc-zone-rows') as HTMLElement;
    await ungroup(editZone(), 'desk');
    assert.deepEqual(chips(editZone()), ['region']);
    editZone().dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: 'z', ctrlKey: true, bubbles: true }));
    await settle();
    assert.deepEqual(chips(editZone()), ['region', 'desk'], 'the editing grid undid');
    assert.deepEqual(app.snapshot.rows, ['region', 'desk'], 'the cube did not');
  });
});

describe('the page\'s layout: placed beside, arranged, undone, locked', () => {
  /** The title bar's menu, opened: its entries by what a person reads. */
  const titleMenu = (): HTMLElement[] => {
    // a menu left open is shut first: the button toggles it
    dom.window.document.dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: 'Escape' }));
    (root.querySelector('.dc-titlebar-menu') as HTMLElement).click();
    return [...dom.window.document.querySelectorAll<HTMLElement>('.dc-menu [role="menuitem"], .dc-menu [role="menuitemcheckbox"]')];
  };
  const pick = (label: string): HTMLElement => {
    const item = menuEntry(titleMenu(), label);
    assert.ok(item, `the title bar's menu offers ${label}`);
    return item;
  };
  const bands = (): string[][] => app.pageViews().layout.bands.map((b) => tiles({ fit: false, bands: [b] }));

  it('a new chart goes beside its grid; Arrange lays the page out; Undo Layout and Redo Layout step through it', async () => {
    const [a, b] = await twoCharts();
    assert.deepEqual(bands(), [['grid', a, b]], 'beside the grid, in its band');
    assert.equal(pick('Undo Layout').getAttribute('aria-disabled'), 'true', 'nothing arranged yet');
    pick('Arrange\u2026').click();
    await settle();
    const stacked = root.querySelector<HTMLButtonElement>('.dc-layout-picker [data-preset="stacked"]');
    assert.ok(stacked, 'the layouts, by the menu');
    stacked.click();
    await settle();
    assert.deepEqual(bands(), [['grid'], [a], [b]]);
    pick('Undo Layout').click();
    await settle();
    assert.deepEqual(bands(), [['grid', a, b]], 'back as it was');
    pick('Redo Layout').click();
    await settle();
    assert.deepEqual(bands(), [['grid'], [a], [b]]);
    // and from a tile's own frame: the arrangement, not the cube's last change
    await ungroup(mainZone(), 'desk');
    const rows = app.snapshot.rows;
    assert.deepEqual(rows, ['region'], 'the cube changed after the arrangement');
    tile(a).dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: 'z', ctrlKey: true, bubbles: true }));
    await settle();
    assert.deepEqual(bands(), [['grid', a, b]]);
    assert.equal(app.snapshot.rows, rows, 'the cube\'s own undo was not taken');
  });

  it('a tile\'s own layouts put that tile first', async () => {
    const [a] = await twoCharts();
    (tile(a).querySelector('.dc-tile-layout') as HTMLElement).click();
    root.querySelector<HTMLButtonElement>('.dc-layout-picker [data-preset="left-and-column"]')!.click();
    await settle();
    assert.equal(tiles(app.pageViews().layout)[0], a);
  });

  it('Edit Layout unticked locks the page: no handles, nothing moves', async () => {
    await twoCharts();
    const board = root.querySelector<HTMLElement>('.dc-bands')!;
    assert.equal(pick('Edit Layout').getAttribute('aria-checked'), 'true');
    pick('Edit Layout').click();
    assert.ok(board.classList.contains('dc-bands-view'));
    assert.equal(board.querySelectorAll('.dc-band-divider').length, 0);
    assert.equal(pick('Edit Layout').getAttribute('aria-checked'), 'false');
  });
});

describe('grids are tiles like charts: + Grid, their own charts, removing one', () => {
  const added = (): HTMLElement[] => [...root.querySelectorAll<HTMLElement>('[data-tile^="grid-"]')];
  const zoneOf = (tileId: string): HTMLElement => tile(tileId).querySelector('.dc-zone-rows') as HTMLElement;

  it('New grid adds a grid of its own on the board, starting as the cube\'s grid is', async () => {
    app.newGrid();
    await settle();
    const [g] = added();
    assert.ok(g, 'a grid tile');
    const id = g.dataset['tile']!;
    assert.notEqual(g.querySelector('.dc-app')?.getAttribute('data-dc-cube'), root.getAttribute('data-dc-cube'));
    assert.deepEqual(chips(zoneOf(id)), ['region', 'desk'], 'grouped as the cube\'s grid was');
    // its own grouping, the cube's untouched
    await ungroup(zoneOf(id), 'desk');
    assert.deepEqual(chips(zoneOf(id)), ['region']);
    assert.deepEqual(app.snapshot.rows, ['region', 'desk']);
  });

  it('a chart of an added grid follows that grid, not the cube\'s', async () => {
    app.openChart();
    app.newGrid();
    await settle();
    const id = added()[0]!.dataset['tile']!;
    const mine = charts()[0]!.id;
    await chartOf(id);
    const theirs = [...root.querySelectorAll<HTMLElement>('[data-tile^="chart-"]')].map((t) => t.dataset['tile']!)
      .find((t) => t !== mine)!;
    assert.ok(theirs, 'a chart of the added grid');
    await ungroup(zoneOf(id), 'desk');
    assert.equal(chart(mine).spec.split, 'desk', 'the cube\'s chart did not move');
    // the added grid's chart is not saved yet (v1); its pill says it follows its grid
    assert.equal(pill(theirs), 'Dynamic');
  });

  it('removing a grid removes its following charts and detaches its frozen ones', async () => {
    app.newGrid();
    await settle();
    const id = added()[0]!.dataset['tile']!;
    await chartOf(id);
    await chartOf(id);
    const ofGrid = [...root.querySelectorAll<HTMLElement>('[data-tile^="chart-"]')].map((t) => t.dataset['tile']!);
    assert.equal(ofGrid.length, 2);
    await togglePill(ofGrid[0]!);
    (tile(id).querySelector('.dc-tile-remove') as HTMLElement).click();
    await settle();
    assert.equal(tile(id), null, 'the grid is gone');
    assert.equal(tile(ofGrid[1]!), null, 'its following chart went with it');
    assert.ok(tile(ofGrid[0]!), 'its frozen chart stayed');
    assert.equal(pill(ofGrid[0]!), 'Detached');
    assert.equal((tile(ofGrid[0]!).querySelector('.dc-tile-actions .dc-titlebar-toggle') as HTMLButtonElement).disabled, true,
      'a detached chart has no grid to follow');
    assert.notEqual(menuEntry(chartMenu(ofGrid[0]!), 'Open in grid')?.getAttribute('aria-disabled'), 'true',
      'it can still be changed');
  });

  it('Insert > Grid from an added grid\'s own menu adds to the page, not inside the grid', async () => {
    app.newGrid();
    await settle();
    const first = added()[0]!;
    const cell = first.querySelector('.dc-row .dc-cell') as HTMLElement;
    cell.dispatchEvent(new dom.window.MouseEvent('contextmenu', { bubbles: true, cancelable: true }));
    await settle();
    // Insert > Grid, by its own label (Insert's entry holds its submenu's words too)
    const item = [...dom.window.document.querySelectorAll<HTMLElement>('.dc-menu .dc-menu-item')]
      .find((el) => el.querySelector(':scope > .dc-menu-label')?.textContent === 'Copy of Grid'
        && el.parentElement?.closest('.dc-menu-item')?.querySelector(':scope > .dc-menu-label')?.textContent === 'Insert');
    assert.ok(item, 'the added grid\'s menu offers Insert > Grid');
    item.click();
    await settle();
    assert.equal(added().length, 2, 'a second added grid, on the page');
    assert.equal(first.querySelector('.dc-board-host'), null, 'no board inside the added grid');
  });
});

describe('each grid\'s header says its source and its plane', () => {
  it('alone, the title bar says them; on a board, each grid\'s own tile header does', async () => {
    await remount({ heldCopy: { label: 'trades (test)', takenAt: new Date(), rowCount: 3 } });
    const bar = (): Element => root.querySelector(':scope > .dc-titlebar')!;
    assert.equal(bar().querySelector('.dc-source-tag')?.textContent, 'trades (test)');
    assert.equal(bar().querySelector('.dc-titlebar-toggle')?.textContent, 'Snapped');
    app.newGrid();
    await settle();
    // the page's bar is the page's now
    assert.equal(bar().querySelector('.dc-source-tag'), null);
    assert.equal(bar().querySelector('.dc-titlebar-toggle'), null);
    const head = (id: string): Element => tile(id).querySelector('.dc-tile-actions')!;
    assert.equal(head('grid').querySelector('.dc-source-tag')?.textContent, 'trades (test)');
    assert.equal(head('grid').querySelector('.dc-titlebar-toggle')?.textContent, 'Snapped');
    const added = root.querySelector<HTMLElement>('[data-tile^="grid-"]')!.dataset['tile']!;
    assert.equal(head(added).querySelector('.dc-source-tag')?.textContent, 'trades (test)', 'the same source');
    assert.equal(head(added).querySelector('.dc-titlebar-toggle')?.textContent, 'Snapped', 'the same copy in the tab');
  });
});


describe('New > Source: a grid over another source, with its own planner and engine', () => {
  /** The other source's planner: counts what it was asked, so a query's planner can be told apart. */
  class Counting extends StubPlanner {
    plans = 0;
    override async plan(): ReturnType<StubPlanner['plan']> {
      this.plans += 1;
      return super.plan();
    }
  }
  let other: { planner: Counting; engine: GateEngine };
  const ORDERS = { ...SNAPSHOT, rows: ['region'] };
  beforeEach(async () => {
    other = { planner: new Counting(), engine: new GateEngine() };
    await remount({ showColumnZone: true, openSource: async () => ({ snapshot: ORDERS, place: other, label: 'orders.csv' }) });
  });

  it('a new grid has the same drop zones as this one: Column Labels too, by either way in', async () => {
    await app.newSource();
    await settle();
    app.newGrid();
    await settle();
    const tiles = added();
    assert.equal(tiles.length, 2, 'one over the other source, one over this one');
    for (const t of tiles) {
      assert.ok(t.querySelector('.dc-zone-rows'), `${t.dataset['tile']} has Row Groups`);
      assert.ok(t.querySelector('.dc-zone-columns'), `${t.dataset['tile']} has Column Labels`);
    }
  });
  const added = (): HTMLElement[] => [...root.querySelectorAll<HTMLElement>('[data-tile^="grid-"]')];

  it('joins the page as a grid tile, says its source, and runs on its own planner and engine', async () => {
    await app.newSource();
    await settle();
    const [g] = added();
    assert.ok(g, 'a grid tile over the other source');
    assert.match(g.querySelector('.dc-tile-cube')?.textContent ?? '', /orders\.csv/);
    assert.ok(other.planner.plans > 0, 'planned by its own planner');
    assert.ok(other.engine.queries > 0, 'run on its own engine');
  });

  /** A menu entry by its label, under a parent entry's label. */
  const entry = (label: string, under: string): HTMLElement | undefined =>
    [...dom.window.document.querySelectorAll<HTMLElement>('.dc-menu .dc-menu-item')]
      .find((el) => el.querySelector(':scope > .dc-menu-label')?.textContent === label
        && el.parentElement?.closest('.dc-menu-item')?.querySelector(':scope > .dc-menu-label')?.textContent === under);

  it('its own Insert > Grid is over ITS source, on the page', async () => {
    await app.newSource();
    await settle();
    const plansBefore = other.planner.plans;
    const first = added()[0]!;
    (first.querySelector('.dc-row .dc-cell') as HTMLElement)
      .dispatchEvent(new dom.window.MouseEvent('contextmenu', { bubbles: true, cancelable: true }));
    await settle();
    const item = entry('Copy of Grid', 'Insert');
    assert.ok(item, 'the added grid offers Insert > Grid');
    item.click();
    await settle();
    assert.equal(added().length, 2, 'a further grid, on the page');
    assert.ok(other.planner.plans > plansBefore, 'over the other source, planned by its planner');
  });

  it('the hamburger offers New > Source… only when the host can open one', async () => {
    (root.querySelector('.dc-titlebar-menu') as HTMLButtonElement).click();
    await settle();
    assert.ok(entry('Data Source\u2026', 'New'), 'New > Data Source…');
    assert.ok(entry('Visualization', 'New') && entry('Copy of Grid', 'New'), 'New > Visualization and Copy of Grid');
    await remount({});
    (root.querySelector('.dc-titlebar-menu') as HTMLButtonElement).click();
    await settle();
    assert.equal(entry('Data Source\u2026', 'New'), undefined);
  });
});
