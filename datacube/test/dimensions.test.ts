import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
  availableDimensions,
  drillDown,
  drillPath,
  drillTo,
  drillUp,
  drilledDepth,
  isDrilling,
  useDimension,
  type Dimension,
} from '../src/dimensions.ts';
import type { CubeSnapshot } from '../src/snapshot.ts';

const GEO: Dimension = {
  name: 'Geography',
  columns: ['region', 'country', 'city'],
};

const CUBE: CubeSnapshot = {
  source: { expression: 't' },
  columns: [
    { name: 'region', type: 'String' },
    { name: 'country', type: 'String' },
    { name: 'city', type: 'String' },
    { name: 'desk', type: 'String' },
  ],
  derived: [],
  rows: [],
  pivotOn: [],
  measures: [{ name: 'total', column: 'amt', fn: 'sum' }],
  sorts: [],
  epoch: 1,
};

describe('drilling a dimension', () => {
  it('starts at one level, not all of them', () => {
    // Opening Geography fully would fetch city-level groups for
    // every country before anyone asked for one.
    assert.deepEqual(useDimension(CUBE, GEO).rows, ['region']);
  });

  it('drills down and back up through the hierarchy', () => {
    let s = useDimension(CUBE, GEO);
    s = drillDown(s, GEO);
    assert.deepEqual(s.rows, ['region', 'country']);
    s = drillDown(s, GEO);
    assert.deepEqual(s.rows, ['region', 'country', 'city']);
    s = drillUp(s, GEO);
    assert.deepEqual(s.rows, ['region', 'country']);
  });

  it('clamps rather than throwing at either end', () => {
    // A UI offering "drill down" on the finest level should get a
    // no-op, so the caller's guard is optional.
    const deepest = drillTo(CUBE, GEO, 3);
    assert.equal(drillDown(deepest, GEO), deepest, 'same object at the bottom');
    const top = drillTo(CUBE, GEO, 0);
    assert.equal(drillUp(top, GEO), top, 'same object at the top');
  });

  it('returns the SAME snapshot when nothing changes', () => {
    // Identity lets a caller skip a refresh rather than re-running an
    // identical query.
    const s = useDimension(CUBE, GEO);
    assert.equal(drillTo(s, GEO, 1), s);
  });

  it('measures how far it is drilled', () => {
    assert.equal(drilledDepth(CUBE, GEO), 0);
    assert.equal(drilledDepth(drillTo(CUBE, GEO, 2), GEO), 2);
  });

  it('knows when the rows are NOT this dimension', () => {
    // Rows set by hand to something else must not read as a partial
    // drill, or the UI offers to drill deeper into a hierarchy the
    // user is not in.
    const byDesk: CubeSnapshot = { ...CUBE, rows: ['desk'] };
    assert.equal(drilledDepth(byDesk, GEO), 0);
    assert.equal(isDrilling(byDesk, GEO), false);

    const mixed: CubeSnapshot = { ...CUBE, rows: ['region', 'desk'] };
    assert.equal(drilledDepth(mixed, GEO), 1, 'a prefix matches');
    assert.equal(
      isDrilling(mixed, GEO),
      false,
      'but the extra column means it is not purely this dimension',
    );
  });

  it('reports a breadcrumb of where the user is', () => {
    assert.deepEqual(drillPath(drillTo(CUBE, GEO, 2), GEO), [
      { label: 'Geography', depth: 0 },
      { label: 'region', depth: 1 },
      { label: 'country', depth: 2 },
    ]);
  });
});

describe('availableDimensions', () => {
  it('skips a dimension naming a column the source lacks', () => {
    // Model configuration and source schema drift apart, and a grid
    // is the wrong place to discover that with an error.
    const stale: Dimension = { name: 'Legal', columns: ['entity', 'region'] };
    assert.deepEqual(
      availableDimensions(CUBE, [GEO, stale]).map((d) => d.name),
      ['Geography'],
    );
  });

  it('skips an empty dimension', () => {
    assert.deepEqual(availableDimensions(CUBE, [{ name: 'X', columns: [] }]), []);
  });
});
