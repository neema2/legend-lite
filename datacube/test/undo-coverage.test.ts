// Does undo cover everything a person can change?
//
// The controller's history records the SNAPSHOT and the tree, which is
// what decides the query. But a cube has a second half -- the
// CONFIGURATION: pinned columns, widths, colours, number formats,
// hidden columns, the row cap. Those live on CubeApp, not on the
// snapshot, and a person changing one has no idea they have crossed an
// internal boundary.
//
// So this suite asks the awkward question directly: pin a column, press
// undo, is it unpinned? It was not, in two different ways:
//
//   - a purely cosmetic change (pin, width, colour) recorded a step
//     whose snapshot was identical, so undo appeared to do nothing
//   - a query-shaping setting (the row cap, the grand total) undid the
//     SNAPSHOT while the config kept its new value, and the next
//     refresh folded that config back in -- an undo that silently
//     reverted itself
//
// Both are worse than a missing feature, because the button is there
// and appears to work.

import assert from 'node:assert/strict';
import { beforeEach, describe, it } from 'node:test';
import { JSDOM } from 'jsdom';

import { CubeApp } from '../src/app.ts';
import type { Planner } from '../src/cube.ts';
import type { QueryEngine } from '../src/engine.ts';
import type { ResultTable } from '../src/result.ts';
import type { CubeSnapshot } from '../src/snapshot.ts';

const SNAPSHOT: CubeSnapshot = {
  source: { expression: '#>{db.T}#' },
  columns: [
    { name: 'region', type: 'String' },
    { name: 'total', type: 'Float' },
  ],
  derived: [],
  rows: [],
  pivotOn: [],
  measures: [{ name: 'total', column: 'total', fn: 'sum' }],
  sorts: [],
  epoch: 1,
};

class StubEngine implements QueryEngine {
  readonly name = 'stub';
  async execute(_sql: string, epoch: number): Promise<ResultTable> {
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
  async close(): Promise<void> {}
}

class StubPlanner implements Planner {
  async plan(): Promise<string> {
    return 'SELECT 1';
  }
}

let dom: JSDOM;
let app: CubeApp;

beforeEach(async () => {
  dom = new JSDOM('<!doctype html><div id="r"></div>');
  const g = globalThis as unknown as Record<string, unknown>;
  g['window'] = dom.window;
  g['document'] = dom.window.document;
  g['requestAnimationFrame'] = (cb: FrameRequestCallback) => {
    cb(0);
    return 1;
  };
  g['cancelAnimationFrame'] = () => {};
  const root = dom.window.document.getElementById('r');
  assert.ok(root);
  app = new CubeApp(root as unknown as HTMLElement, SNAPSHOT, {
    engine: new StubEngine(),
    planner: new StubPlanner(),
  });
  await app.open();
});

describe('undo over the configuration, not just the query', () => {
  it('undoes a pin', async () => {
    assert.equal(app.configuration.columns['region']?.pinned, undefined);

    await app.applyConfiguration({
      columns: { region: { pinned: 'left' } },
    });

    assert.equal(app.configuration.columns['region']?.pinned, 'left');

    await app.controller.undo();
    assert.equal(
      app.configuration.columns['region']?.pinned,
      undefined,
      'the pin came back off',
    );
  });

  it('undoes a change that also reshapes the query', async () => {
    // maxRows is folded into the SNAPSHOT by applyToSnapshot, so this
    // is the case where undoing half the state left the other half to
    // put it straight back on the next refresh.
    const original = app.configuration.maxRows;
    await app.applyConfiguration({ maxRows: 17 });

    assert.equal(app.configuration.maxRows, 17);

    await app.controller.undo();
    assert.equal(app.configuration.maxRows, original, 'config reverted');
    assert.equal(
      app.controller.snapshot?.maxRows,
      original,
      'and the snapshot agrees with it',
    );
  });

  it('does not put the change back on the next refresh', async () => {
    // The self-reverting undo: the snapshot rolled back, the config
    // did not, and the next refresh folded the stale config in again.
    await app.applyConfiguration({ maxRows: 17 });

    await app.controller.undo();
    await app.controller.refresh();
    assert.notEqual(
      app.controller.snapshot?.maxRows,
      17,
      'the undone setting stayed undone',
    );
  });

  it('redoes a configuration change', async () => {
    await app.applyConfiguration({ columns: { region: { pinned: 'left' } } });

    await app.controller.undo();
    assert.equal(app.configuration.columns['region']?.pinned, undefined);

    await app.controller.redo();
    assert.equal(app.configuration.columns['region']?.pinned, 'left');
  });
});
