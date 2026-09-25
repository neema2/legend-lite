// Properties > Apply with a draft the planner refuses must leave the
// cube where it was. It did not: the refused draft stayed as the
// cube's snapshot and configuration, so EVERY later query repeated the
// refusal -- one Apply wedged the cube until a reload (census headline
// 7; upstream compiles the whole query before publishing it).

import assert from 'node:assert/strict';
import { beforeEach, describe, it } from 'node:test';
import { JSDOM } from 'jsdom';

import { CubeApp } from '../src/app.ts';
import type { Planner } from '../src/cube.ts';
import type { QueryEngine } from '../src/engine.ts';
import type { ResultTable } from '../src/result.ts';
import type { CubeSnapshot } from '../src/snapshot.ts';

const SNAPSHOT: CubeSnapshot = {
  source: { expression: 'trades' },
  columns: [
    { name: 'region', type: 'String' },
    { name: 'notional', type: 'Float' },
  ],
  derived: [],
  rows: ['region'],
  pivotOn: [],
  measures: [{ name: 'total', column: 'notional', fn: 'sum' }],
  sorts: [],
  epoch: 1,
};

class Engine implements QueryEngine {
  readonly name = 'stub';
  async execute(_sql: string, epoch: number): Promise<ResultTable> {
    return {
      columns: [
        { name: 'region', type: 'String', values: ['EMEA'] },
        { name: 'total', type: 'Float', values: [1] },
      ],
      rowCount: 1,
      epoch,
      elapsedMs: 1,
    };
  }
  async close(): Promise<void> {}
}

/** Refuses any query capped at 43 rows -- a Row Limit of 42. */
class RefusingPlanner implements Planner {
  refusals = 0;
  planned = 0;
  async plan(pure: string): Promise<string> {
    this.planned += 1;
    if (/limit\(43\)/.test(pure)) {
      this.refusals += 1;
      throw new Error('refused: no limit of 42 here');
    }
    return 'SELECT 1';
  }
}

const flush = async (): Promise<void> => {
  for (let i = 0; i < 10; i += 1) await new Promise((r) => setTimeout(r, 0));
};

describe('Properties > Apply with a refused draft', () => {
  let dom: JSDOM;
  let root: HTMLElement;
  let app: CubeApp;
  let planner: RefusingPlanner;
  let statuses: [string, string][];

  beforeEach(async () => {
    dom = new JSDOM('<!doctype html><body><div id="r"></div></body>');
    (globalThis as { requestAnimationFrame?: unknown }).requestAnimationFrame =
      (fn: () => void) => { fn(); return 0; };
    root = dom.window.document.getElementById('r') as HTMLElement;
    planner = new RefusingPlanner();
    statuses = [];
    app = new CubeApp(root, SNAPSHOT, {
      engine: new Engine(),
      planner,
      onStatus: (text, kind) => statuses.push([text, kind]),
    });
    await app.open();
  });

  const applyRowLimit = async (value: string): Promise<void> => {
    app.openEditor();
    const overlay = root.querySelector('.dc-app-overlay') as HTMLElement;
    [...overlay.querySelectorAll('.dc-editor-tab')]
      .find((b) => b.textContent === 'General Properties')
      ?.dispatchEvent(new dom.window.MouseEvent('click', { bubbles: true }));
    const limit = [...overlay.querySelectorAll('.dc-field')]
      .find((f) => f.querySelector('.dc-field-label')?.textContent === 'Row Limit:')
      ?.querySelector('input') as HTMLInputElement;
    limit.value = value;
    limit.dispatchEvent(new dom.window.Event('change'));
    ([...overlay.querySelectorAll('.dc-editor-footer button')]
      .find((b) => b.textContent === 'Apply') as HTMLButtonElement).click();
    await flush();
  };

  it('puts the cube back, and says why', async () => {
    await applyRowLimit('42');
    assert.equal(planner.refusals, 1);
    assert.notEqual(app.snapshot.maxRows, 42, 'the refused snapshot stayed');
    assert.notEqual(app.configuration.maxRows, 42, 'the refused setting stayed');
    assert.ok(statuses.some(([t, k]) => k === 'error' && /refused/.test(t)));
  });

  it('is not wedged: the next action queries the cube as it was', async () => {
    await applyRowLimit('42');
    const refusals = planner.refusals;
    await applyRowLimit('7');
    assert.equal(planner.refusals, refusals, 'a later query repeated the refusal');
    assert.equal(app.snapshot.maxRows, 7);
  });
});
