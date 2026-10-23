// The board's charts as a person meets them (plan B1, revised 2026-09-30): any number follow the
// grid, each badged so; Freeze keeps one's grouping and Follow the grid takes it back; Show Grid
// (D7) shows the grid a chart follows. A cube alone saves its own grid alone, so a frozen or a
// detached chart gets no grid of its own here (that is a page of its own's: cube-page-shell.test.ts).

import assert from 'node:assert/strict';
import { afterEach, beforeEach, describe, it } from 'node:test';

import { tiles } from '../src/layout/bands.ts';
import type { PageCommand } from '../src/page-commands.ts';
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
/** Whether a chart's right-click menu offers Show Grid (the entry is always there, disabled where it shows nothing). */
const offersShowGrid = (id: string): boolean => {
  const item = menuEntry(chartMenu(id), 'Show Grid');
  assert.ok(item, `${id}'s menu has Show Grid`);
  return item.getAttribute('aria-disabled') !== 'true';
};
/** Show Grid, from the chart's right-click menu. */
const showGrid = async (id: string): Promise<void> => {
  const item = menuEntry(chartMenu(id), 'Show Grid');
  assert.ok(item && item.getAttribute('aria-disabled') !== 'true', `${id} offers Show Grid`);
  item.click();
  await settle();
};
/** The grids added to the board (the cube's own is 'grid'), by tile id. */
const addedGrids = (): string[] => [...root.querySelectorAll<HTMLElement>('[data-tile^="grid-"]')].map((t) => t.dataset['tile']!);
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
/** The main grid's row-group zone: the cube's own, not an added grid's. */
const mainZone = (): HTMLElement => [...root.querySelectorAll<HTMLElement>('.dc-zone-rows')]
  .find((z) => !z.closest('[data-tile^="grid-"]'))!;
/** Take a column out of a row-group zone, by its chip's x. */
async function ungroup(zone: HTMLElement, column: string): Promise<void> {
  (zone.querySelector(`.dc-chip[data-column="${column}"] .dc-chip-remove`) as HTMLElement).click();
  await settle();
}
const chips = (zone: HTMLElement): string[] => [...zone.querySelectorAll<HTMLElement>('.dc-chip')].map((c) => c.dataset['column'] ?? '');

async function twoCharts(): Promise<[string, string]> {
  // awaited: the first opens the board, whose code loads lazily -- longer, on a slow machine, than settle waits
  await app.openChart();
  await app.openChart();
  await settle();
  const [a, b] = charts();
  return [a!.id, b!.id];
}

describe('charts follow the grid until frozen', () => {
  it('any number follow, each badged, and a pivot changes every one', async () => {
    const [a, b] = await twoCharts();
    for (const id of [a, b]) {
      assert.equal(pill(id), 'Dynamic');
      assert.ok(offersShowGrid(id), 'a dynamic chart shows the grid it follows');
    }
    await ungroup(mainZone(), 'desk');
    assert.deepEqual([chart(a).spec.split, chart(b).spec.split], [undefined, undefined]);
  });

  it('a frozen chart keeps its grouping; Follow the grid takes the grid\'s again', async () => {
    const [a, b] = await twoCharts();
    await togglePill(a);
    assert.equal(pill(a), 'Frozen');
    assert.equal(offersShowGrid(a), false, 'a cube alone saves its own grid alone: no grid of its own for a frozen chart');
    await ungroup(mainZone(), 'desk');
    assert.equal(chart(a).spec.split, 'desk', 'frozen: the pivot did not change it');
    assert.equal(chart(b).spec.split, undefined, 'the other still follows');
    await togglePill(a);
    assert.equal(chart(a).spec.split, undefined, 'following again, it took the grid\'s grouping');
    assert.equal(pill(a), 'Dynamic');
  });
});

describe('Show Grid: the grid a chart follows, or a grid of its own grouping, which it follows from then on (D7)', () => {
  it('a following chart: the grid it follows shown, nothing added, the page as it was', async () => {
    const [a] = await twoCharts();
    const before = JSON.stringify(app.pageViews());
    await showGrid(a);
    assert.deepEqual(addedGrids(), [], 'no grid added');
    assert.equal(JSON.stringify(app.pageViews()), before, 'the page as it was');
    assert.equal(pill(a), 'Dynamic');
  });

  it('a frozen chart on a cube alone: offered disabled; clicked anyway, no grid made, nothing told, the chart frozen and saved', async () => {
    const told: PageCommand[] = [];
    await remount({ onPageCommand: (c) => told.push(c) });
    const [a] = await twoCharts();
    await togglePill(a);
    await ungroup(mainZone(), 'desk');
    assert.equal(chart(a).spec.split, 'desk', 'frozen: its own grouping');
    assert.equal(offersShowGrid(a), false);
    const before = JSON.stringify(app.pageViews());
    told.length = 0;
    menuEntry(chartMenu(a), 'Show Grid')!.click();
    await settle();
    assert.deepEqual(addedGrids(), [], 'no grid made');
    assert.deepEqual(told, [], 'nothing told');
    assert.equal(JSON.stringify(app.pageViews()), before, 'the page as it was');
    assert.equal(chart(a).spec.frozen, true, 'still frozen, and saved with the cube');
    assert.equal(pill(a), 'Frozen');
    assert.deepEqual(app.snapshot.rows, ['region'], 'the cube\'s grid never moved');
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
  const bands = (): string[][] => app.pageViews().sheets[0]!.layout.bands.map((b) => tiles({ fit: false, bands: [b] }));
  /** The bands as the board shows them, every tile in (an added grid too, which the saved page leaves out until v2). */
  const boardBands = (): string[][] => {
    const tops = new Map<string, string[]>();
    for (const el of root.querySelectorAll<HTMLElement>('.dc-band-tile')) {
      const top = el.style.top;
      tops.set(top, [...(tops.get(top) ?? []), el.dataset['tile']!]);
    }
    return [...tops.entries()].sort((x, y) => parseFloat(x[0]) - parseFloat(y[0]))
      .map(([, ids]) => ids.sort((x, y) => parseFloat(tile(x).style.left) - parseFloat(tile(y).style.left)));
  };

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

  it('a tile\'s own layouts arrange the whole page, that tile first: four tiles in 2 x 2 are two rows of two', async () => {
    const [a, b] = await twoCharts();
    await app.newGrid();
    await settle();
    const copy = [...root.querySelectorAll<HTMLElement>('[data-tile^="grid-"]')][0]!.dataset['tile']!;
    pick('Arrange\u2026').click();
    await settle();
    root.querySelector<HTMLButtonElement>('.dc-layout-picker [data-preset="stacked"]')!.click();
    await settle();
    // the user, 2026-10-09: from a tile of four, the layouts showed two tiles, and 2 x 2 came out 2, 1, 1
    (tile(b).querySelector('.dc-tile-layout') as HTMLElement).click();
    await settle();
    const option = root.querySelector<HTMLElement>('.dc-layout-picker [data-preset="rows:2-2"]')!;
    assert.equal(option.querySelectorAll('.dc-layout-cell').length, 4, 'the thumbnails show every tile on the page');
    assert.equal(option.querySelectorAll('.dc-layout-cell-mark').length, 1, 'the tile it was opened from marked');
    option.click();
    await settle();
    assert.deepEqual(boardBands(), [[b, 'grid'], [a, copy]]);
  });

  it('Edit Layout unticked locks the page: no handles, no layouts, no layout undo -- nothing moves', async () => {
    await twoCharts();
    pick('Arrange\u2026').click();
    await settle();
    root.querySelector<HTMLButtonElement>('.dc-layout-picker [data-preset="stacked"]')!.click();
    await settle();
    const arranged = bands();
    const board = root.querySelector<HTMLElement>('.dc-bands')!;
    assert.equal(pick('Edit Layout').getAttribute('aria-checked'), 'true');
    pick('Edit Layout').click();
    assert.ok(board.classList.contains('dc-bands-view'));
    assert.equal(board.querySelectorAll('.dc-band-divider').length, 0);
    assert.equal(pick('Edit Layout').getAttribute('aria-checked'), 'false');
    assert.equal(pick('Arrange\u2026').getAttribute('aria-disabled'), 'true');
    assert.equal(pick('Undo Layout').getAttribute('aria-disabled'), 'true');
    // nor from a tile's frame
    tile('grid').dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: 'z', ctrlKey: true, bubbles: true }));
    await settle();
    assert.deepEqual(bands(), arranged);
  });

  it('a grid\'s menu button opens its menu even while the title bar\'s is open, and says when it is open', async () => {
    await twoCharts();
    titleMenu();
    const button = [...tile('grid').querySelectorAll<HTMLElement>('.dc-tile-menu')][0]!;
    button.click();
    const labels = [...dom.window.document.querySelectorAll<HTMLElement>('.dc-menu > [role^="menuitem"]')]
      .map((i) => i.querySelector(':scope > .dc-menu-label')?.textContent);
    assert.deepEqual(labels, ['New', 'Undo', 'Redo', 'Export', 'Email', 'Properties...', 'Ad Hoc Analysis'], 'the grid\'s, not shut');
    assert.equal(button.getAttribute('aria-expanded'), 'true');
    button.click();
    assert.equal(dom.window.document.querySelector('.dc-menu'), null, 'its own button again shuts it');
    assert.equal(button.getAttribute('aria-expanded'), 'false');
  });
});

describe('grids are tiles like charts: + Grid, their own charts, removing one', () => {
  const added = (): HTMLElement[] => [...root.querySelectorAll<HTMLElement>('[data-tile^="grid-"]')];
  const zoneOf = (tileId: string): HTMLElement => tile(tileId).querySelector('.dc-zone-rows') as HTMLElement;

  it('New grid adds a grid of its own on the board, starting as the cube\'s grid is', async () => {
    await app.newGrid();
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

  it('every grid has its own menu in its tile\'s header, the same for the cube\'s grid and an added one', async () => {
    await app.newGrid();
    await settle();
    const id = added()[0]!.dataset['tile']!;
    /** A grid's own menu, opened from its tile: its entries at the top level, by what a person reads. */
    const gridMenu = (tileId: string): HTMLElement[] => {
      dom.window.document.dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: 'Escape' }));
      const buttons = [...tile(tileId).querySelectorAll<HTMLElement>('.dc-tile-menu')]
        .filter((b) => b.closest('[data-tile]') === tile(tileId));
      assert.equal(buttons.length, 1, `${tileId} has one menu button of its own`);
      buttons[0]!.click();
      return [...dom.window.document.querySelectorAll<HTMLElement>('.dc-menu > [role^="menuitem"]')];
    };
    const labels = (items: HTMLElement[]): string[] => items.map((i) => i.querySelector(':scope > .dc-menu-label')?.textContent ?? '');
    const theirs = labels(gridMenu(id));
    assert.deepEqual(theirs, ['New', 'Undo', 'Redo', 'Export', 'Email', 'Properties...', 'Ad Hoc Analysis', 'Remove from Page']);
    // the cube's own grid is its host's: the same menu, but it cannot be taken off the page
    assert.deepEqual(labels(gridMenu('grid')), theirs.filter((l) => l !== 'Remove from Page'), 'the cube\'s own grid has the same menu');
    // its Undo is its own: the added grid's change undone, the cube's grid untouched
    await ungroup(zoneOf(id), 'desk');
    menuEntry(gridMenu(id), 'Undo')!.click();
    await settle();
    assert.deepEqual(chips(zoneOf(id)), ['region', 'desk']);
    assert.deepEqual(app.snapshot.rows, ['region', 'desk']);
  });

  it('a chart of an added grid follows that grid, not the cube\'s', async () => {
    await app.openChart();
    await app.newGrid();
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
    await app.newGrid();
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
    // a cube alone saves its own grid alone: no grid of its own for it here, offered disabled, and clicked anyway nothing
    assert.equal(offersShowGrid(ofGrid[0]!), false, 'a detached chart gets no grid of its own on a cube alone');
    menuEntry(chartMenu(ofGrid[0]!), 'Show Grid')!.click();
    await settle();
    assert.deepEqual(added(), [], 'no grid made');
    assert.equal(pill(ofGrid[0]!), 'Detached');
  });

  it('Insert > Grid from an added grid\'s own menu adds to the page, not inside the grid', async () => {
    await app.newGrid();
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
    await app.newGrid();
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
    await app.newGrid();
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

describe('a KPI tile (docs/DATACUBE_CHARTS_DESIGN_2026_10_09.md, batch 1)', () => {
  it('says its number, against its target, with its trend\'s line under it; a chart again, the number gives way', async () => {
    const { ChartPanel } = await import('../src/ui/chart-panel.ts');
    const host = dom.window.document.createElement('div');
    root.append(host);
    const asked: string[][] = [];
    const panel = new ChartPanel(host, {
      snapshot: () => SNAPSHOT,
      // its value from one query (no grouping), its trend from another (by the column across)
      run: async (_query, s) => {
        asked.push([...s.rows]);
        return s.rows.length === 0
          ? { columns: [{ name: 'sum_notional', type: 'Float', values: [1500] }], rowCount: 1, epoch: 1, elapsedMs: 0 }
          : { columns: [{ name: 'region', type: 'String', values: ['AMER', 'EMEA'] }, { name: 'sum_notional', type: 'Float', values: [600, 900] }],
            rowCount: 2, epoch: 1, elapsedMs: 0 };
      },
      label: (v) => `$${String(v)}`,
      onPick: () => {},
      initial: {
        version: 2, mark: 'kpi', x: 'region', y: [{ column: 'notional', fn: 'sum' }],
        options: { orientation: 'vertical', stack: 'none', sort: { by: 'y', direction: 'desc' }, limit: 50, labels: false,
          legend: 'top', target: 1000 },
      },
      debounceMs: 0,
      formOpen: true,
    });
    // the form's module, which the panel loads on demand: the first load in this file, slower than settle's turns on
    // a slow runner (PR #62's Windows runs read an empty form)
    await import('../src/ui/chart-form.ts');
    await settle();
    assert.equal(host.querySelector('.dc-kpi-value')?.textContent, '$1500');
    const against = host.querySelector('.dc-kpi-target')!;
    assert.equal(against.textContent, '▲ 50% above target ($1000)');
    assert.ok(against.classList.contains('dc-kpi-good'));
    assert.ok(host.querySelector('.dc-kpi-trend canvas, .dc-kpi-trend div'), 'its trend drawn under it');
    assert.deepEqual(asked, [[], ['region']]);
    assert.match(host.querySelector('.dc-chartpanel-chart')!.getAttribute('aria-label') ?? '', /^KPI: sum of notional is \$1500/);
    // its form: what it measures, its trend, its target, which way is good -- and no order, no legend
    const fields = [...host.querySelectorAll('.dc-chartpanel-form label, .dc-chartpanel-form .dc-field-label')].map((l) => l.textContent);
    assert.ok(fields.some((f) => f?.startsWith('Target')), `its form: ${fields.join(', ')}`);
    assert.ok(!fields.some((f) => f?.startsWith('Legend')));
    // another kind again: the number gives way to the chart
    panel.setSpec({ ...panel.spec!, mark: 'bar' });
    await settle();
    assert.equal(host.querySelector('.dc-kpi'), null);
    panel.dispose();
  });
});
