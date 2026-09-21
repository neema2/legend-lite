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
    assert.equal(
      root.querySelector('.dc-titlebar-title')?.textContent,
      'DataCube',
    );
    assert.notEqual(root.querySelector('.dc-zone-rows'), null);
    assert.notEqual(root.querySelector('.dc-grid'), null);
    assert.notEqual(root.querySelector('.dc-app-stats'), null);
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
