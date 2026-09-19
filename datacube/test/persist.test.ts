import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
  CURRENT_VERSION,
  SavedViewError,
  load,
  save,
  toJson,
  treeOf,
} from '../src/persist.ts';
import type { CubeSnapshot } from '../src/snapshot.ts';
import { TreeState } from '../src/tree.ts';

const SNAPSHOT: CubeSnapshot = {
  source: { expression: 'trades' },
  columns: [{ name: 'region', type: 'String' }],
  derived: [],
  rows: ['region', 'desk'],
  pivotOn: ['year'],
  measures: [{ name: 'total', column: 'notional', fn: 'sum' }],
  sorts: [{ column: 'total', direction: 'desc' }],
  epoch: 42,
};

const TREE = TreeState.empty().expand(['EMEA', 'Rates']);
const NOW = new Date('2026-03-01T09:00:00Z');

describe('save', () => {
  it('captures the cube, the tree and the time', () => {
    const v = save({ name: 'Morning P&L', snapshot: SNAPSHOT, tree: TREE, now: NOW });
    assert.equal(v.version, CURRENT_VERSION);
    assert.equal(v.name, 'Morning P&L');
    assert.equal(v.savedAt, '2026-03-01T09:00:00.000Z');
    assert.deepEqual(v.snapshot.rows, ['region', 'desk']);
    assert.equal(v.showTotals, true);
    assert.equal(v.expanded.length, 2, 'EMEA and EMEA/Rates');
  });

  it('does not persist the runtime epoch', () => {
    // The epoch exists to discard stale queries within a session; the
    // number a session happened to reach means nothing tomorrow.
    const v = save({ name: 'x', snapshot: SNAPSHOT, tree: TREE, now: NOW });
    assert.equal(v.snapshot.epoch, 0);
  });
});

describe('round trip', () => {
  it('restores the cube and the expansion exactly', () => {
    const v = save({ name: 'x', snapshot: SNAPSHOT, tree: TREE, now: NOW });
    const back = load(toJson(v));

    assert.deepEqual(back.snapshot, { ...SNAPSHOT, epoch: 0 });
    const tree = treeOf(back);
    assert.equal(tree.isOpen(['EMEA']), true);
    assert.equal(tree.isOpen(['EMEA', 'Rates']), true);
    assert.equal(tree.isOpen(['AMER']), false);
  });

  it('survives a group label containing a comma or a space', () => {
    // The path separator is why this works; a comma-joined key would
    // split 'Rates, Credit' into two phantom groups.
    const tricky = TreeState.empty().expand(['A B', 'Rates, Credit']);
    const back = load(
      toJson(save({ name: 'x', snapshot: SNAPSHOT, tree: tricky, now: NOW })),
    );
    assert.equal(treeOf(back).isOpen(['A B', 'Rates, Credit']), true);
  });

  it('keeps column settings', () => {
    const v = save({
      name: 'x',
      snapshot: SNAPSHOT,
      tree: TREE,
      now: NOW,
      columns: {
        hidden: ['qty'],
        widths: { region: 200 },
        formats: { total: { kind: 'currency', currency: 'GBP' } },
      },
    });
    const back = load(toJson(v));
    assert.deepEqual(back.columns?.hidden, ['qty']);
    assert.equal(back.columns?.widths?.['region'], 200);
    assert.equal(back.columns?.formats?.['total']?.currency, 'GBP');
  });
});

describe('forward and backward compatibility', () => {
  it('preserves fields a newer writer added', () => {
    // An older client that opens and re-saves must not silently
    // delete a colleague's settings.
    const fromFuture = {
      version: CURRENT_VERSION,
      name: 'x',
      savedAt: NOW.toISOString(),
      snapshot: { ...SNAPSHOT, epoch: 0 },
      expanded: [],
      showTotals: true,
      conditionalFormats: [{ column: 'total', rule: 'negative-red' }],
    };
    const back = load(JSON.stringify(fromFuture));
    assert.deepEqual(back.unknown?.['conditionalFormats'], [
      { column: 'total', rule: 'negative-red' },
    ]);
    // And they survive being written back out.
    const again = JSON.parse(toJson(back)) as Record<string, unknown>;
    assert.deepEqual(again['conditionalFormats'], [
      { column: 'total', rule: 'negative-red' },
    ]);
  });

  it('reads a version-0 view that predates the version field', () => {
    const v0 = {
      name: 'legacy',
      snapshot: {
        source: 'trades', // was a bare string before it became an object
        rows: ['region'],
        pivotOn: [],
        measures: [{ name: 'total', column: 'notional', fn: 'sum' }],
      },
    };
    const back = load(JSON.stringify(v0));
    assert.equal(back.version, CURRENT_VERSION);
    assert.equal(back.snapshot.source.expression, 'trades');
    assert.deepEqual(back.snapshot.rows, ['region']);
    // Absent collections become empty rather than undefined, so every
    // consumer can assume the current shape.
    assert.deepEqual(back.snapshot.derived, []);
    assert.deepEqual(back.snapshot.sorts, []);
    assert.equal(back.showTotals, true);
  });

  it('refuses a view from a newer version, and says so', () => {
    assert.throws(
      () => load(JSON.stringify({ version: 99, snapshot: SNAPSHOT })),
      (e: unknown) => {
        assert.ok(e instanceof SavedViewError);
        assert.match((e as Error).message, /newer version \(99 > 1\)/);
        assert.match((e as Error).message, /upgrade to open it/);
        return true;
      },
    );
  });
});

describe('failures are loud', () => {
  it('reports invalid JSON rather than returning a default cube', () => {
    // A silently-default cube reads as data loss to the person whose
    // work it was.
    assert.throws(() => load('{not json'), /not valid JSON/);
  });

  it('reports a missing snapshot', () => {
    assert.throws(() => load(JSON.stringify({ version: 1 })), /missing 'snapshot'/);
  });

  it('reports a snapshot with no source', () => {
    assert.throws(
      () => load(JSON.stringify({ version: 1, snapshot: { rows: [] } })),
      /missing 'snapshot.source'/,
    );
  });

  it('rejects a non-object', () => {
    assert.throws(() => load('42'), /expected an object/);
  });
});
