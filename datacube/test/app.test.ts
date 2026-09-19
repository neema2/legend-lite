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

  const tool = (label: string): HTMLButtonElement =>
    [...root.querySelectorAll('.dc-tool')].find(
      (b) => b.textContent === label,
    ) as HTMLButtonElement;

  it('renders the toolbar, the drag zones, the grid and the status line', () => {
    assert.notEqual(root.querySelector('.dc-app-toolbar'), null);
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

  it('exports every format the menu offers', () => {
    tool('CSV').click();
    tool('Excel').click();
    tool('HTML').click();
    tool('Spec').click();
    assert.deepEqual(
      downloads.map((d) => d[1]),
      [
        'text/csv',
        'application/vnd.ms-excel',
        'text/html',
        'application/json',
      ],
    );
    // SpreadsheetML rather than CSV, so numbers arrive as numbers.
    assert.ok(downloads[1]?.[2].includes('<Workbook'));
  });

  it('saves a view and loads it back', async () => {
    tool('Save view').click();
    assert.equal(storage.map.size, 1);
    await app.loadView();
    assert.ok(statuses.some(([t]) => t.startsWith('loaded')));
  });

  it('reports a bad saved view rather than throwing past the user', async () => {
    storage.setItem('datacube.savedView', '{ not json');
    await app.loadView();
    assert.equal(statuses.at(-1)?.[1], 'error');
  });

  it('offers each named dimension as a drill', () => {
    assert.notEqual(tool('Geography'), undefined);
    tool('Geography').click();
    // Starts at ONE level: opening it fully would fetch desk-level
    // groups for every region before anyone asked.
    assert.deepEqual(app.snapshot.rows, ['region']);
  });

  it('opens the editor, and Cancel closes it', () => {
    tool('Properties…').click();
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

  it('puts a floating filter box under the header', () => {
    assert.notEqual(root.querySelector('.dc-floating-row'), null);
    assert.ok(root.querySelectorAll('.dc-floating-input').length > 0);
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
    tool('Properties…').click();
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
