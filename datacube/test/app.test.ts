import assert from 'node:assert/strict';
import { beforeEach, describe, it } from 'node:test';
import { JSDOM } from 'jsdom';

import { CubeApp } from '../src/app.ts';
import type { Planner } from '../src/cube.ts';
import type { QueryEngine } from '../src/engine.ts';
import type { ResultTable } from '../src/result.ts';
import { DEFAULT_CONFIGURATION } from '../src/config.ts';
import type { CubeSnapshot } from '../src/snapshot.ts';
import { setHeaderDrag } from '../src/ui/pivot-panel.ts';

const SNAPSHOT: CubeSnapshot = {
  source: { expression: 'trades' },
  columns: [
    { name: 'region', type: 'String' },
    { name: 'desk', type: 'String' },
    { name: 'notional', type: 'Float' },
  ],
  derived: [],
  rows: ['region'],
  pivotOn: [],
  measures: [{ name: 'total', column: 'notional', fn: 'sum' }],
  sorts: [],
  epoch: 1,
};

/** A grand total and two regions, enough for the grid to render. */
function result(epoch: number): ResultTable {
  return {
    columns: [
      { name: 'region', type: 'String', values: ['EMEA', 'AMER'] },
      { name: 'total', type: 'Float', values: [600, 400] },
    ],
    rowCount: 2,
    epoch,
    elapsedMs: 1,
  };
}

class StubEngine implements QueryEngine {
  readonly name = 'stub';
  readonly sql: string[] = [];
  async execute(sql: string, epoch: number): Promise<ResultTable> {
    this.sql.push(sql);
    return result(epoch);
  }
  async close(): Promise<void> {}
}

/**
 * A stub that answers the snap's preflight COUNT with a real number.
 *
 * `StubEngine` returns the grid fixture for every query, so a
 * preflight reads 'EMEA' as its row count and the snapshot ends up
 * NaN rows big -- which would let a tooltip that dropped the row
 * count pass.
 */
class CountingEngine implements QueryEngine {
  readonly name = 'counting';
  readonly sql: string[] = [];
  async execute(sql: string, epoch: number): Promise<ResultTable> {
    this.sql.push(sql);
    if (/count\(\*\)/i.test(sql)) {
      return {
        columns: [{ name: 'n', type: 'Integer', values: [29] }],
        rowCount: 1,
        epoch,
        elapsedMs: 0,
      };
    }
    return result(epoch);
  }
  async close(): Promise<void> {}
}

class StubPlanner implements Planner {
  readonly pure: string[] = [];
  async plan(pureGrammar: string): Promise<string> {
    this.pure.push(pureGrammar);
    return 'SELECT 1';
  }
}

class MemoryStorage {
  readonly map = new Map<string, string>();
  getItem(k: string): string | null {
    return this.map.get(k) ?? null;
  }
  setItem(k: string, v: string): void {
    this.map.set(k, v);
  }
  removeItem(k: string): void {
    this.map.delete(k);
  }
}

describe('the app', () => {
  let dom: JSDOM;
  let root: HTMLElement;
  let app: CubeApp;
  let engine: StubEngine;
  let planner: StubPlanner;
  let storage: MemoryStorage;
  let downloads: [string, string, string][];
  let clipboard: string[];
  let statuses: [string, string][];

  beforeEach(async () => {
    dom = new JSDOM('<!doctype html><body><div id="r"></div></body>');
    (globalThis as { requestAnimationFrame?: unknown }).requestAnimationFrame =
      (fn: () => void) => {
        fn();
        return 0;
      };
    root = dom.window.document.getElementById('r') as HTMLElement;
    engine = new StubEngine();
    planner = new StubPlanner();
    storage = new MemoryStorage();
    downloads = [];
    clipboard = [];
    statuses = [];
    app = new CubeApp(root, SNAPSHOT, {
      engine,
      planner,
      storage,
      dimensions: [{ name: 'Geography', columns: ['region', 'desk'] }],
      showColumnZone: true,
      onStatus: (text, kind) => statuses.push([text, kind]),
      writeClipboard: (t) => {
        clipboard.push(t);
      },
      download: (n, m, t) => downloads.push([n, m, t]),
    });
    await app.open();
  });

  /** Open the grid's right-click menu, optionally over a column. */
  const rightClick = (selector = '.dc-app-grid'): void => {
    (root.querySelector(selector) as HTMLElement).dispatchEvent(
      new dom.window.MouseEvent('contextmenu', { bubbles: true }),
    );
  };
  /** Open the title bar's hamburger. */
  const hamburger = (): void => {
    (root.querySelector('.dc-titlebar-menu') as HTMLButtonElement).click();
  };
  const menuItems = (): HTMLElement[] =>
    [
      ...dom.window.document.querySelectorAll('.dc-menu [role="menuitem"]'),
    ] as HTMLElement[];
  /** Click a menu entry by the words a user reads. */
  const pick = (label: string): void => {
    const item = menuItems().find((i) =>
      (i.querySelector('.dc-menu-label')?.textContent ?? i.textContent) ===
      label,
    );
    if (!item) {
      throw new Error(
        `no menu entry "${label}"; saw: ${menuItems()
          .map((i) => i.textContent)
          .join(', ')}`,
      );
    }
    item.click();
  };

  it('renders the title bar, the drag zones, the grid and the status line', () => {
    // A TITLE BAR, not a toolbar. DataCube has no row of buttons
    // over the grid: everything lives in the right-click menu.
    assert.notEqual(root.querySelector('.dc-titlebar'), null);
    assert.equal(root.querySelector('.dc-app-toolbar'), null);
    // NO BRAND. An unnamed cube gets no title element at all --
    // "DataCube" over the grid tells a person nothing they did not
    // know from opening it, and cost a third of a 28px bar.
    assert.equal(root.querySelector('.dc-titlebar-title'), null);
    assert.equal(
      /DataCube/.test(root.querySelector('.dc-titlebar')?.textContent ?? ''),
      false,
      root.querySelector('.dc-titlebar')?.textContent ?? '',
    );
    assert.notEqual(root.querySelector('.dc-zone-rows'), null);
    assert.notEqual(root.querySelector('.dc-grid'), null);
    assert.notEqual(root.querySelector('.dc-app-stats'), null);
  });

  // -- folding the chrome away ---------------------------------------
  //
  // Both bars fold, the way the columns panel does, because the
  // grid is what the page is for. The rule they all follow: a bar
  // that vanishes leaves something to click, and the grid's own
  // right-click menu can restore either one -- which matters most
  // for the title bar, because the hamburger is IN it.

  const zoneBar = (): HTMLElement =>
    root.querySelector('.dc-zone-bar') as HTMLElement;
  const press = (selector: string): void => {
    const el = root.querySelector(selector);
    if (!el) throw new Error(`no ${selector} to press`);
    (el as HTMLButtonElement).click();
  };

  it('starts with both bars on screen', () => {
    // A drop target you cannot see is a feature you cannot find, so
    // the default is what upstream shows: everything visible.
    assert.equal(zoneBar().hidden, false);
    assert.equal(
      root.querySelector('.dc-titlebar')?.classList.contains('dc-collapsed'),
      false,
    );
    assert.equal(app.configuration.showDragZones, true);
    assert.equal(app.configuration.showTitleBar, true);
  });

  it('folds the drag zones, leaving the way back in the title bar', () => {
    press('.dc-zone-fold');
    assert.equal(zoneBar().hidden, true);
    // NEVER NOTHING TO CLICK. The bar is gone, so the bar that is
    // still there carries the twin that brings it back.
    assert.notEqual(root.querySelector('.dc-titlebar-zones'), null);
    press('.dc-titlebar-zones');
    assert.equal(zoneBar().hidden, false);
    // And the control goes away again, rather than sitting there
    // doing nothing.
    assert.equal(root.querySelector('.dc-titlebar-zones'), null);
  });

  it('brings the folded zones back FOR THE LENGTH OF A DRAG', () => {
    // Folding them must not take anything away: a person who folds
    // the zones and then drags a column header has nowhere to drop
    // it, and a drag that can never land is worse than no drag.
    press('.dc-zone-fold');
    assert.equal(zoneBar().hidden, true);
    root.dispatchEvent(new dom.window.Event('dragstart', { bubbles: true }));
    assert.equal(zoneBar().hidden, false, 'nowhere to drop the column');
    assert.equal(zoneBar().classList.contains('dc-peeking'), true);
    root.dispatchEvent(new dom.window.Event('dragend', { bubbles: true }));
    assert.equal(zoneBar().hidden, true, 'the peek did not fold itself back');
    // The fold is still what the configuration says, so the peek
    // did not quietly become the setting.
    assert.equal(app.configuration.showDragZones, false);
  });

  it('folds the title bar to a LIP, which is the way back', () => {
    press('.dc-titlebar-fold');
    const bar = root.querySelector('.dc-titlebar') as HTMLElement;
    assert.equal(bar.classList.contains('dc-collapsed'), true);
    // The hamburger went with it. That is precisely why there is a
    // lip: without one, hiding this bar would be a one-way door.
    assert.equal(root.querySelector('.dc-titlebar-menu'), null);
    press('.dc-titlebar-lip');
    assert.equal(root.querySelector('.dc-titlebar-lip'), null);
    assert.notEqual(root.querySelector('.dc-titlebar-menu'), null);
  });

  it("restores either bar from the GRID's menu, with both folded", () => {
    press('.dc-zone-fold');
    press('.dc-titlebar-fold');
    // No title bar, therefore no hamburger. The grid's own menu is
    // the second way back, and the entries say what they will do
    // rather than what state they are in.
    rightClick();
    pick('Show Drag Zones');
    assert.equal(zoneBar().hidden, false);
    rightClick();
    pick('Show Title Bar');
    assert.notEqual(root.querySelector('.dc-titlebar-menu'), null);
    assert.equal(
      root.querySelector('.dc-titlebar')?.classList.contains('dc-collapsed'),
      false,
    );
  });

  it('folds from the PROPERTIES editor, not only from the bars', () => {
    // The setting lives in General Properties, beside the rest of
    // "what is on screen" -- the chevrons are the in-passing way to
    // reach it. Applying the editor replaces the whole
    // configuration, so this is also the check that the flags and
    // the DOM cannot drift apart: a bar left on screen while the
    // configuration says it is folded gives a toggle that folds when
    // it should unfold.
    hamburger();
    pick('Properties...');
    const overlay = root.querySelector('.dc-app-overlay') as HTMLElement;
    [...overlay.querySelectorAll('.dc-editor-tab')]
      .find((b) => b.textContent === 'General Properties')
      ?.dispatchEvent(new dom.window.MouseEvent('click', { bubbles: true }));
    // BY ITS OWN LABEL. A `.dc-field` holds several inputs, so
    // taking the first one in the row has twice now toggled a
    // different setting than the one under test.
    const box = [...overlay.querySelectorAll('.dc-check')]
      .find((l) => l.querySelector('.dc-check-label')?.textContent
        === 'Show drag zones')
      ?.querySelector('input') as HTMLInputElement;
    assert.equal(box.checked, true);
    box.checked = false;
    box.dispatchEvent(new dom.window.Event('change'));
    (
      [...overlay.querySelectorAll('.dc-editor-footer button')].find(
        (b) => b.textContent === 'Apply',
      ) as HTMLButtonElement
    ).click();
    assert.equal(app.configuration.showDragZones, false);
    assert.equal(zoneBar().hidden, true, 'the DOM and the flag disagree');
    // And the way back is on screen, as it is for every other fold.
    assert.notEqual(root.querySelector('.dc-titlebar-zones'), null);
  });

  it('offers the folds in the hamburger as well', () => {
    hamburger();
    const labels = menuItems().map((i) =>
      i.querySelector('.dc-menu-label')?.textContent ?? '');
    assert.ok(labels.includes('Hide Drag Zones'), labels.join(', '));
    assert.ok(labels.includes('Hide Title Bar'), labels.join(', '));
  });

  it('shows the row grouping already in force as chips', () => {
    const chips = [...root.querySelectorAll('.dc-zone-rows .dc-chip')].map(
      (c) => (c as HTMLElement).dataset['column'],
    );
    assert.deepEqual(chips, ['region']);
  });

  it('a column dropped in the row zone regroups the cube', async () => {
    setHeaderDrag({ column: 'desk' });
    (
      root.querySelector('.dc-zone-rows') as HTMLElement
    ).dispatchEvent(new dom.window.Event('drop', { bubbles: true }));
    assert.deepEqual(app.snapshot.rows, ['region', 'desk']);
  });

  it('a measure dropped in the row zone is REFUSED', () => {
    setHeaderDrag({ column: 'notional' });
    (
      root.querySelector('.dc-zone-rows') as HTMLElement
    ).dispatchEvent(new dom.window.Event('drop', { bubbles: true }));
    assert.deepEqual(app.snapshot.rows, ['region']);
  });

  it('opens the context menu on right-click — the audit found it unreachable', () => {
    const grid = root.querySelector('.dc-app-grid') as HTMLElement;
    grid.dispatchEvent(
      new dom.window.MouseEvent('contextmenu', { bubbles: true }),
    );
    assert.notEqual(
      dom.window.document.querySelector('.dc-menu'),
      null,
      'the menu is in the document',
    );
  });

  it('exports every format, from the right-click menu', () => {
    for (const label of [
      'HTML',
      'Excel (Grid)',
      'CSV (Grid)',
      'DataCube Specification',
    ]) {
      rightClick();
      pick(label);
    }
    assert.deepEqual(
      downloads.map((d) => d[1]),
      [
        'text/html',
        'application/vnd.ms-excel',
        'text/csv',
        'application/json',
      ],
    );
    // SpreadsheetML rather than CSV, so numbers arrive as numbers.
    assert.ok(downloads[1]?.[2].includes('<Workbook'));
  });

  it('opens the editor from the right-click menu, as DataCube does', () => {
    rightClick();
    pick('Properties...');
    assert.equal(
      (root.querySelector('.dc-app-overlay') as HTMLElement).hidden,
      false,
    );
  });

  it('adds and removes a heatmap from the menu', () => {
    rightClick('.dc-cell:not(.dc-dim)');
    pick('Add Heatmap to total');
    const column = Object.entries(app.configuration.columns).find(
      ([, c]) => c.heatmap,
    );
    assert.notEqual(column, undefined);
    rightClick('.dc-cell:not(.dc-dim)');
    pick('Remove Heatmap');
    assert.equal(
      Object.values(app.configuration.columns).some((c) => c.heatmap),
      false,
    );
  });

  it('a right-click on a TREE cell resolves the dimension at that level', () => {
    // Their menu does the same, from the node's level: right-click
    // EMEA under region and the filter entries are about region.
    // Row 1, not row 0: the grand total's path is empty, so it
    // names no dimension -- and every column-specific entry
    // correctly greys out there.
    const treeCell = root
      .querySelectorAll('.dc-row')[1]
      ?.querySelector('.dc-cell.dc-tree') as HTMLElement;
    treeCell.dispatchEvent(
      new dom.window.MouseEvent('contextmenu', { bubbles: true }),
    );
    assert.ok(
      menuItems().some((i) =>
        (i.querySelector('.dc-menu-label')?.textContent ?? '').startsWith(
          'Add Filter: region =',
        ),
      ),
      menuItems()
        .map((i) => i.querySelector('.dc-menu-label')?.textContent)
        .join(' | '),
    );
  });

  it('the grand total row names no dimension, and greys what needs one', () => {
    const totalCell = root
      .querySelector('.dc-row .dc-cell.dc-tree') as HTMLElement;
    totalCell.dispatchEvent(
      new dom.window.MouseEvent('contextmenu', { bubbles: true }),
    );
    const live = menuItems().filter(
      (i) => i.getAttribute('aria-disabled') !== 'true',
    );
    assert.equal(
      live.some(
        (i) =>
          (i.querySelector('.dc-menu-label')?.textContent ?? '') === 'Hide',
      ),
      false,
      'Hide with nothing to hide must not be actionable',
    );
  });

  it('a right-click on a CELL still knows its column', () => {
    // Without data-column on body cells the menu lost every
    // column-specific entry, which is most of the menu.
    rightClick('.dc-cell');
    assert.ok(
      menuItems().some((i) => i.textContent === 'Ascending'),
      'no sort entries',
    );
  });

  it('saves a view and loads it back, from the title bar menu', () => {
    hamburger();
    pick('Save View');
    assert.equal(storage.map.size, 1);
    hamburger();
    pick('Load View');
  });

  it('LOADING A VIEW RESTORES THE QUERY, not just the status line', async () => {
    // The test above asserted that something was stored and then
    // clicked Load with no assertion at all, so it passed while
    // loading restored NOTHING. The status line said `loaded "..."`
    // either way, which is the only thing anyone checked.
    //
    // The cause: `loadView` set the app's snapshot and then called
    // `setTree`, which refreshes -- and that refresh ran the
    // CONTROLLER's snapshot, the one being replaced, then handed the
    // resulting view back through `onView`, which assigns
    // `this.#snapshot = view.snapshot`. The freshly loaded snapshot
    // was overwritten by the stale one before the real refresh ran.
    await app.applyConfiguration({ maxRows: 123 });
    const saved = { ...app.controller.snapshot };
    app.saveView('a view worth keeping');

    // Move AWAY from the saved shape, so restoring has work to do.
    const now = app.controller.snapshot;
    assert.ok(now);
    await app.controller.update({ ...now, rows: [], epoch: now.epoch + 1 });
    assert.deepEqual(app.controller.snapshot?.rows, [],
      'could not set up: the cube is still grouped');

    await app.loadView();

    assert.equal(statuses.at(-1)?.[1], 'ok', 'it reported success');
    assert.deepEqual(
      app.controller.snapshot?.rows,
      saved.rows,
      'the row dimensions came back',
    );
    assert.equal(app.controller.snapshot?.maxRows, 123,
      'and the configuration the view was saved with');
  });

  it('the columns PANEL follows the grid order, not the declared one', async () => {
    // The panel listed the cube's declared columns, so reordering a
    // header moved the column on screen and left the panel beside it
    // saying something else -- and the panel is the list people read
    // to find a column. Tested here rather than through a browser
    // drag: the drag is covered by the grid's own DOM tests, and what
    // broke was this hand-off.
    const named = () => [...root.querySelectorAll('.dc-tool-panel-row')]
      .map((e) => (e as HTMLElement).dataset['column']);
    const before = named();
    assert.ok(before.length >= 3, `only ${before.length} columns listed`);

    const moved = [before[before.length - 1], ...before.slice(0, -1)]
      .filter((n): n is string => n !== undefined);
    await app.applyConfiguration({ columnOrder: moved });

    assert.deepEqual(named(), moved,
      'the panel must read in the order the grid does');
  });

  it('reports a bad saved view rather than throwing past the user', async () => {
    storage.setItem('datacube.savedView', '{ not json');
    await app.loadView();
    assert.equal(statuses.at(-1)?.[1], 'error');
  });

  it('offers each named dimension in the title bar menu', () => {
    hamburger();
    pick('Geography');
    // Starts at ONE level: opening it fully would fetch desk-level
    // groups for every region before anyone asked.
    assert.deepEqual(app.snapshot.rows, ['region']);
  });

  it('opens the editor, and Cancel closes it', () => {
    rightClick();
    pick('Properties...');
    const overlay = root.querySelector('.dc-app-overlay') as HTMLElement;
    assert.equal(overlay.hidden, false);
    assert.notEqual(overlay.querySelector('.dc-editor'), null);
    (
      [...overlay.querySelectorAll('.dc-editor-footer button')].find(
        (b) => b.textContent === 'Cancel',
      ) as HTMLButtonElement
    ).click();
    assert.equal(overlay.hidden, true);
  });

  it('opens the filter editor showing the filter ALREADY in force', () => {
    // Without this the editor opens empty on a filtered cube, and the
    // user's first change writes that emptiness back -- silently
    // dropping a filter visible on the grid behind the dialog.
    const host = dom.window.document.createElement('div');
    dom.window.document.body.append(host);
    const filtered = new CubeApp(
      host,
      {
        ...SNAPSHOT,
        filter: {
          kind: 'condition',
          column: 'region',
          operator: 'equal',
          value: 'EMEA',
        },
      },
      { engine, planner },
    );
    filtered.openFilters();
    const editor = host.querySelector('.dc-filters') as HTMLElement;
    assert.notEqual(editor, null);
    const values = [...editor.querySelectorAll('select')].map((s) => s.value);
    assert.ok(values.includes('region'), `columns offered: ${values}`);
    assert.ok(values.includes('equal'), `operators offered: ${values}`);
    const text = editor.querySelector(
      'input[type="text"]',
    ) as HTMLInputElement;
    assert.equal(text.value, 'EMEA');
  });

  it('a heatmap set on a measure reaches its PIVOTED leaves', async () => {
    // The bug this replaced looked the spec up by leaf name, so a
    // heatmap on `notional` never matched `2021__|__notional` and a
    // pivoted cube showed nothing -- the same mistake the formats
    // had. And it painted the cells that existed at the time, which
    // a virtualised grid throws away on the next scroll.
    const host = dom.window.document.createElement('div');
    dom.window.document.body.append(host);
    const pivoted = new CubeApp(
      host,
      { ...SNAPSHOT, pivotOn: ['desk'] },
      {
        engine,
        planner,
        configuration: {
          ...DEFAULT_CONFIGURATION,
          columns: { total: { heatmap: { from: '#ffffff', to: '#ff0000' } } },
        },
      },
    );
    await pivoted.open();
    const painted = [...host.querySelectorAll('.dc-cell')].filter(
      (c) => (c as HTMLElement).style.backgroundColor !== '',
    );
    assert.ok(painted.length > 0, 'no cell was painted');
  });

  it('the grid header carries the column name, so the menu knows what was clicked', () => {
    const header = root.querySelector('.dc-th[data-column]') as HTMLElement;
    assert.notEqual(header, null);
    assert.ok(typeof header.dataset['column'] === 'string');
  });

  it('Ctrl+E opens the editor', () => {
    dom.window.document.dispatchEvent(
      new dom.window.KeyboardEvent('keydown', {
        key: 'e',
        ctrlKey: true,
        bubbles: true,
      }),
    );
    assert.equal(
      (root.querySelector('.dc-app-overlay') as HTMLElement).hidden,
      false,
    );
  });

  it('folds the configuration into the query on every refresh', async () => {
    // Once, here, so a setting that shapes the query cannot reach the
    // engine through one path and not another.
    rightClick();
    pick('Properties...');
    const overlay = root.querySelector('.dc-app-overlay') as HTMLElement;
    [...overlay.querySelectorAll('.dc-editor-tab')]
      .find((b) => b.textContent === 'General Properties')
      ?.dispatchEvent(new dom.window.MouseEvent('click', { bubbles: true }));
    const limit = [...overlay.querySelectorAll('.dc-field')]
      .find(
        (f) => f.querySelector('.dc-field-label')?.textContent === 'Row Limit:',
      )
      ?.querySelector('input') as HTMLInputElement;
    limit.value = '42';
    limit.dispatchEvent(new dom.window.Event('change'));
    (
      [...overlay.querySelectorAll('.dc-editor-footer button')].find(
        (b) => b.textContent === 'Apply',
      ) as HTMLButtonElement
    ).click();
    assert.equal(app.snapshot.maxRows, 42);
  });
});

describe('the bar says what you are looking at, and nothing else', () => {
  let dom: JSDOM;
  let root: HTMLElement;

  beforeEach(() => {
    dom = new JSDOM('<!doctype html><body><div id="r"></div></body>');
    (globalThis as { requestAnimationFrame?: unknown }).requestAnimationFrame =
      (fn: () => void) => {
        fn();
        return 0;
      };
    root = dom.window.document.getElementById('r') as HTMLElement;
  });

  /** Let the toggle's async click handler finish. */
  const flush = async (): Promise<void> => {
    for (let i = 0; i < 5; i += 1) {
      await new Promise((done) => setTimeout(done, 0));
    }
  };

  it('names the REPORT in the title bar', async () => {
    const app = new CubeApp(root, SNAPSHOT, {
      engine: new StubEngine(),
      planner: new StubPlanner(),
      configuration: { ...DEFAULT_CONFIGURATION, reportTitle: 'Trades' },
    });
    await app.open();
    assert.equal(
      root.querySelector('.dc-titlebar-title')?.textContent,
      'Trades',
    );
  });

  it('states the result and its cost in the STATUS bar', async () => {
    // The row count, the column count and the elapsed time went to
    // the host through `onStatus` and were rendered above the grid,
    // while the status bar said "Rows: 2" -- two readouts of one
    // fact, the fuller one in the wrong place.
    const statuses: string[] = [];
    const app = new CubeApp(root, SNAPSHOT, {
      engine: new StubEngine(),
      planner: new StubPlanner(),
      onStatus: (text) => statuses.push(text),
    });
    await app.open();
    const timing = root.querySelector('.dc-status-timing')?.textContent ?? '';
    assert.match(timing, /^2 rows × \d+ cols in \d+ms$/);
    // The same line, so a host's figure and the screen's cannot
    // drift apart.
    assert.equal(statuses.at(-1), timing);
    assert.equal(
      root.querySelector('.dc-app-stats')?.textContent?.includes('Rows:'),
      false,
    );
  });

  it("MOVES the host's readout into the status bar, once", async () => {
    const marker = dom.window.document.createElement('span');
    marker.id = 'hoststatus';
    marker.textContent = 'planning…';
    const app = new CubeApp(root, SNAPSHOT, {
      engine: new StubEngine(),
      planner: new StubPlanner(),
      hostStatus: (slot) => slot.append(marker),
    });
    // BEFORE the first render: a cube whose first query fails never
    // renders a status bar, and that is exactly when the host has
    // something to say.
    assert.equal(
      root.querySelector('.dc-app-stats #hoststatus'),
      marker,
      'the host slot was not filled at build time',
    );
    await app.open();
    // Still the SAME node, and only one of it: the host keeps
    // writing to whichever node is on screen.
    assert.equal(root.querySelector('.dc-app-stats #hoststatus'), marker);
    assert.equal(root.querySelectorAll('#hoststatus').length, 1);
    assert.equal(marker.textContent, 'planning…');
  });

  it('puts the row and column zones in ONE bar, side by side', async () => {
    const app = new CubeApp(root, SNAPSHOT, {
      engine: new StubEngine(),
      planner: new StubPlanner(),
      showColumnZone: true,
    });
    await app.open();
    // Both halves of one strip rather than two stacked strips: the
    // zones must stay visible drop targets (you cannot drag a column
    // into a menu), and two bars cost 66px of the viewport.
    const panel = root.querySelector('.dc-pivot-panel');
    assert.notEqual(panel, null);
    const zones = [...(panel?.children ?? [])].filter((c) =>
      c.classList.contains('dc-zone'),
    );
    assert.deepEqual(
      zones.map((z) => (z as HTMLElement).dataset['zone']),
      ['rows', 'columns'],
    );
  });

  it('says WHEN the snapshot was taken and how big it is', async () => {
    // The banner that said "trades — frozen at 14:02:11 — 29 rows"
    // is gone, and the toggle beside it said only "Snapped". Rule 1
    // of snap mode is that what you are looking at is never
    // inferable, so the detail moved into the toggle's tooltip.
    const app = new CubeApp(root, SNAPSHOT, {
      engine: new CountingEngine(),
      planner: new StubPlanner(),
    });
    await app.open();
    const toggle = root.querySelector('.dc-titlebar-toggle') as HTMLButtonElement;
    assert.equal(toggle.textContent, 'Live');
    assert.match(toggle.title, /Live data/);

    toggle.click();
    await flush();

    assert.equal(toggle.textContent, 'Snapped');
    assert.match(toggle.title, /frozen at \d/);
    assert.match(toggle.title, /29 rows/);
    assert.match(toggle.title, /Click to go live/);
  });
});
