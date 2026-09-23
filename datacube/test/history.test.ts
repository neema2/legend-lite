// Undo and redo.
//
// The interesting tests here are not "does undo go back" -- that is
// one line and it works. They are the four ways an undo stack feels
// broken while technically functioning:
//
//   - undo that TOGGLES, because applying an undone state recorded
//     itself as a new step, so the stack never gets past two entries
//   - undo that does NOTHING, because every refresh pushed a step and
//     the first several undos land on states that look identical
//   - redo surviving a new edit, so going forward arrives somewhere
//     that no longer follows from where you are
//   - a stack that grows without limit in a long session
//
// Each of those has a test below, because each one is what a user
// means when they say undo is unreliable.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { CubeController, type Planner } from '../src/cube.ts';
import type { QueryEngine } from '../src/engine.ts';
import { History, stateKey } from '../src/history.ts';
import type { ResultTable } from '../src/result.ts';
import type { CubeSnapshot } from '../src/snapshot.ts';
import { TreeState } from '../src/tree.ts';

const BASE: CubeSnapshot = {
  source: { expression: '#>{db.T}#' },
  columns: [
    { name: 'region', type: 'String' },
    { name: 'notional', type: 'Float' },
  ],
  derived: [],
  rows: [],
  pivotOn: [],
  measures: [{ name: 'm', column: 'notional', fn: 'sum' }],
  sorts: [],
  epoch: 1,
};

class StubPlanner implements Planner {
  async plan(): Promise<string> {
    return 'SELECT 1';
  }
}

class StubEngine implements QueryEngine {
  readonly name = 'stub';
  async execute(_sql: string, epoch: number): Promise<ResultTable> {
    return {
      columns: [
        { name: 'region', type: 'String', values: ['EMEA'] },
        { name: 'm', type: 'Float', values: [1] },
      ],
      rowCount: 1,
      epoch,
      elapsedMs: 0,
    };
  }
  async close(): Promise<void> {}
}

const controller = () => new CubeController(new StubEngine(), new StubPlanner());

describe('the history stack itself', () => {
  const s = (rows: string[]) => ({
    snapshot: { ...BASE, rows },
    tree: TreeState.empty(),
  });

  it('ignores a step that changed nothing', () => {
    const h = new History();
    h.record(s(['region']));
    h.record(s(['region']));
    assert.equal(h.depth.past, 1);
  });

  it('does not count the epoch as a change', () => {
    // The epoch advances on every refresh. Counting it would make
    // every state unique and fill the stack with entries that all
    // undo to the same screen.
    const a = { snapshot: { ...BASE, epoch: 1 }, tree: TreeState.empty() };
    const b = { snapshot: { ...BASE, epoch: 99 }, tree: TreeState.empty() };
    assert.equal(stateKey(a), stateKey(b));
  });

  it('counts expansion as a change', () => {
    const a = { snapshot: BASE, tree: TreeState.empty() };
    const b = { snapshot: BASE, tree: TreeState.fromPaths([['EMEA']]) };
    assert.notEqual(stateKey(a), stateKey(b));
  });

  it('drops the oldest step past the limit', () => {
    const h = new History({ limit: 3 });
    for (let i = 0; i < 10; i++) h.record(s([`c${i}`]));
    assert.equal(h.depth.past, 3);
  });

  it('clears the redo branch when a new step is recorded', () => {
    const h = new History();
    h.record(s(['a']));
    const back = h.undo(s(['b']));
    assert.ok(back);
    assert.equal(h.depth.future, 1);
    h.record(s(['c']));
    assert.equal(h.depth.future, 0, 'a new edit invalidates the future');
  });
});

describe('undo through the controller', () => {
  it('goes back to the previous cube', async () => {
    const c = controller();
    await c.update({ ...BASE, rows: [] });
    await c.update({ ...BASE, rows: ['region'] });
    assert.deepEqual(c.snapshot?.rows, ['region']);

    await c.undo();
    assert.deepEqual(c.snapshot?.rows, [], 'back to the flat cube');
  });

  it('walks back more than one step rather than toggling', async () => {
    // The bug this guards: applying an undone state through update()
    // records it as a NEW step, so the second undo returns to where
    // the first came from and the stack oscillates between two
    // states forever.
    const c = controller();
    await c.update({ ...BASE, rows: [] });
    await c.update({ ...BASE, rows: ['region'] });
    await c.update({ ...BASE, rows: ['region', 'country'] });

    await c.undo();
    assert.deepEqual(c.snapshot?.rows, ['region']);
    await c.undo();
    assert.deepEqual(c.snapshot?.rows, [], 'three states, two undos');
  });

  it('redoes forward again', async () => {
    const c = controller();
    await c.update({ ...BASE, rows: [] });
    await c.update({ ...BASE, rows: ['region'] });
    await c.undo();
    assert.deepEqual(c.snapshot?.rows, []);

    await c.redo();
    assert.deepEqual(c.snapshot?.rows, ['region']);
  });

  it('does not record a plain refresh as an undo step', async () => {
    // refresh() re-runs the CURRENT cube -- after a snap, on a retry.
    // Recording it would mean several undos that all land on the
    // same screen, which reads as "undo is broken".
    const c = controller();
    await c.update({ ...BASE, rows: ['region'] });
    const before = c.historyDepth.past;
    await c.refresh();
    await c.refresh();
    assert.equal(c.historyDepth.past, before);
  });

  it('undoes an expand', async () => {
    const c = controller();
    await c.update({ ...BASE, rows: ['region'] });
    await c.toggle(['EMEA']);
    assert.equal(c.tree.isOpen(['EMEA']), true);

    await c.undo();
    assert.equal(c.tree.isOpen(['EMEA']), false, 'collapsed again');
  });

  it('reports nothing to undo rather than throwing', async () => {
    const c = controller();
    await c.update(BASE);
    await c.undo();
    assert.equal(await c.undo(), null, 'an empty stack is not an error');
    assert.equal(c.canUndo, false);
  });

  it('tells the UI when availability changes', async () => {
    const seen: { canUndo: boolean; canRedo: boolean }[] = [];
    const c = new CubeController(new StubEngine(), new StubPlanner(), {
      onHistory: (s) => seen.push(s),
    });
    await c.update({ ...BASE, rows: [] });
    await c.update({ ...BASE, rows: ['region'] });
    assert.ok(seen.some((s) => s.canUndo), 'undo became available');

    await c.undo();
    assert.ok(seen.some((s) => s.canRedo), 'and redo did too');
  });

  it('honours a history limit', async () => {
    const c = new CubeController(new StubEngine(), new StubPlanner(), {
      historyLimit: 2,
    });
    for (let i = 0; i < 6; i++) {
      await c.update({ ...BASE, rows: [`c${i}`] });
    }
    assert.equal(c.historyDepth.past, 2);
  });
});
