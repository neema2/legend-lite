import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
  TreeState,
  flattenTree,
  pathKey,
  parsePathKey,
  requiredLevels,
  requestKey,
  rowLabel,
} from '../src/tree.ts';
import type { RowPath } from '../src/tree.ts';

/** region -> desk, a two-level tree. */
const CHILDREN = new Map<string, RowPath[]>([
  [pathKey([]), [['EMEA'], ['AMER']]],
  [pathKey(['EMEA']), [['EMEA', 'Rates'], ['EMEA', 'Credit']]],
  [pathKey(['AMER']), [['AMER', 'FX']]],
]);

const childrenOf = (p: RowPath) => CHILDREN.get(pathKey(p));

describe('TreeState', () => {
  it('starts closed', () => {
    const s = TreeState.empty(true);
    assert.equal(s.isOpen(['EMEA']), false);
    assert.deepEqual(s.openPaths, []);
  });

  it('toggles open and closed', () => {
    let s = TreeState.empty(true);
    s = s.toggle(['EMEA']);
    assert.equal(s.isOpen(['EMEA']), true);
    s = s.toggle(['EMEA']);
    assert.equal(s.isOpen(['EMEA']), false);
  });

  it('is immutable, so it can live in a snapshot', () => {
    const a = TreeState.empty(true);
    const b = a.expand(['EMEA']);
    assert.equal(a.isOpen(['EMEA']), false, 'the original is untouched');
    assert.equal(b.isOpen(['EMEA']), true);
  });

  it('opening a deep path opens its ancestors', () => {
    // Otherwise a restored saved view points at a row nothing can reach.
    const s = TreeState.empty(true).expand(['EMEA', 'Rates']);
    assert.equal(s.isOpen(['EMEA']), true);
    assert.equal(s.isOpen(['EMEA', 'Rates']), true);
  });

  it('collapsing closes everything beneath', () => {
    const s = TreeState.empty(true)
      .expand(['EMEA', 'Rates'])
      .collapse(['EMEA']);
    assert.equal(s.isOpen(['EMEA']), false);
    assert.equal(
      s.isOpen(['EMEA', 'Rates']),
      false,
      'reopening must not restore a tree left open three levels down',
    );
  });

  it('does not collapse a sibling with a shared prefix', () => {
    // 'EMEA' must not close 'EMEA2'. A prefix test without the
    // separator would.
    const s = TreeState.fromPaths([['EMEA'], ['EMEA2']], true).collapse(['EMEA']);
    assert.equal(s.isOpen(['EMEA']), false);
    assert.equal(s.isOpen(['EMEA2']), true);
  });

  it('keys by identity, so paths survive reordering', () => {
    const s = TreeState.fromPaths([['AMER'], ['EMEA']], true);
    // Nothing about the state references a row index, so a re-sort
    // that renumbers every row cannot move the open groups.
    assert.equal(s.isOpen(['EMEA']), true);
    assert.equal(s.isOpen(['AMER']), true);
  });

  it('round-trips through openPaths for a saved view', () => {
    const a = TreeState.empty(true).expand(['EMEA', 'Rates']);
    // parsePathKey is the inverse, so no caller -- and no saved view
    // -- has to know the separator, or carry an invisible control
    // character around in its source.
    const restored = TreeState.fromPaths(a.openPaths.map(parsePathKey));
    assert.equal(restored.isOpen(['EMEA', 'Rates']), true);
    assert.equal(restored.isOpen(['EMEA']), true);
    assert.deepEqual(
      restored.openPaths.slice().sort(),
      a.openPaths.slice().sort(),
      'the restored state is the same state, not a lookalike',
    );
  });

  it('parsePathKey inverts pathKey, root included', () => {
    const cases: RowPath[] = [[], ['EMEA'], ['EMEA', 'Rates'], ['a b', 'c,d']];
    for (const path of cases) {
      assert.deepEqual(parsePathKey(pathKey(path)), path);
    }
  });
});

describe('requiredLevels', () => {
  it('asks only for the top level when everything is closed', () => {
    const reqs = requiredLevels(TreeState.empty(true), 2, childrenOf);
    assert.deepEqual(reqs.map(requestKey), ['0:', '1:']);
  });

  it('omits the grand total when totals are off', () => {
    const s = TreeState.empty(true).withTotals(false);
    assert.deepEqual(requiredLevels(s, 2, childrenOf).map(requestKey), ['1:']);
  });

  it('asks for children only of open groups', () => {
    const s = TreeState.empty(true).expand(['EMEA']);
    const keys = requiredLevels(s, 2, childrenOf).map(requestKey);
    assert.deepEqual(keys, ['0:', '1:', '2:EMEA']);
    // AMER is closed, so its children are never fetched -- a collapsed
    // cube costs one query, not one per group.
    assert.equal(keys.some((k) => k.includes('AMER')), false);
  });

  it('never asks past the last dimension', () => {
    const s = TreeState.empty(true).expand(['EMEA']).expand(['EMEA', 'Rates']);
    const keys = requiredLevels(s, 2, childrenOf).map(requestKey);
    assert.equal(
      keys.some((k) => k.startsWith('3:')),
      false,
      'depth 2 means level 3 does not exist',
    );
  });

  it('asks for nothing when there are no row dimensions', () => {
    assert.deepEqual(requiredLevels(TreeState.empty(true), 0, childrenOf), []);
  });
});

describe('flattenTree', () => {
  it('lists the grand total then the top level', () => {
    const rows = flattenTree(TreeState.empty(true), 2, childrenOf);
    assert.deepEqual(
      rows.map((r) => [r.level, rowLabel(r), r.isGroup, r.expanded]),
      [
        [0, 'Total', false, false],
        [1, 'EMEA', true, false],
        [1, 'AMER', true, false],
      ],
    );
  });

  it('inlines the children of an open group, in engine order', () => {
    const s = TreeState.empty(true).expand(['EMEA']);
    const rows = flattenTree(s, 2, childrenOf);
    assert.deepEqual(rows.map((r) => rowLabel(r)), [
      'Total',
      'EMEA',
      'Rates',
      'Credit', // engine order, not alphabetical: never re-sorted here
      'AMER',
    ]);
  });

  it('makes the grand total the ROOT, shifting everything below it', () => {
    // aria-level cannot be 0, so clamping the total to 1 would put it
    // at the same level as its own children and a screen reader would
    // announce them as siblings. DataCube shifts rowGroupIndex by one
    // for the same reason.
    const rows = flattenTree(
      TreeState.empty(true).expand(['EMEA']),
      2,
      childrenOf,
    );
    assert.deepEqual(
      rows.map((r) => [rowLabel(r), r.level, r.depth]),
      [
        ['Total', 0, 1],
        ['EMEA', 1, 2],
        ['Rates', 2, 3],
        ['Credit', 2, 3],
        ['AMER', 1, 2],
      ],
    );
  });

  it('starts at depth 1 when there is no total to be the root', () => {
    const rows = flattenTree(
      TreeState.empty(true).withTotals(false).expand(['EMEA']),
      2,
      childrenOf,
    );
    assert.deepEqual(
      rows.map((r) => [rowLabel(r), r.depth]),
      [
        ['EMEA', 1],
        ['Rates', 2],
        ['Credit', 2],
        ['AMER', 1],
      ],
    );
  });

  it('marks an open group as a subtotal and a leaf as neither', () => {
    const rows = flattenTree(TreeState.empty(true).expand(['EMEA']), 2, childrenOf);
    const emea = rows.find((r) => rowLabel(r) === 'EMEA');
    const rates = rows.find((r) => rowLabel(r) === 'Rates');
    assert.equal(emea?.isTotal, true, 'an open group shows a subtotal');
    assert.equal(rates?.isGroup, false, 'a leaf cannot expand');
    assert.equal(rates?.isTotal, false);
  });

  it('omits the grand total when totals are off', () => {
    const s = TreeState.empty(true).withTotals(false);
    assert.equal(flattenTree(s, 2, childrenOf)[0]?.level, 1);
  });

  it('stops at a group whose children have not arrived yet', () => {
    const s = TreeState.empty(true).expand(['AMER']).expand(['AMER', 'FX']);
    // 'AMER/FX' is a leaf at depth 2, so nothing is fetched beneath it
    // and flattening must not invent rows.
    const rows = flattenTree(s, 2, childrenOf);
    assert.deepEqual(rows.map((r) => rowLabel(r)), ['Total', 'EMEA', 'AMER', 'FX']);
  });
});
