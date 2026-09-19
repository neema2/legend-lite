import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
  DEFAULT_DRILL_LIMIT,
  drillConditions,
  drillQuery,
  pivotPathOf,
} from '../src/drill.ts';
import { NULL_GROUP } from '../src/serialize.ts';
import type { CubeSnapshot } from '../src/snapshot.ts';

const SNAPSHOT: CubeSnapshot = {
  source: { expression: 'trades' },
  columns: [],
  derived: [],
  rows: ['region', 'desk'],
  pivotOn: ['year'],
  measures: [{ name: 'total', column: 'notional', fn: 'sum' }],
  sorts: [],
  epoch: 1,
};

describe('drillQuery', () => {
  it('pins the row path and the pivot value, and does not aggregate', () => {
    const sql = drillQuery(SNAPSHOT, {
      path: ['EMEA', 'Rates'],
      pivotPath: ['2023'],
    });
    assert.equal(
      sql,
      'trades->filter(x|($x.region == \'EMEA\' && $x.desk == \'Rates\' ' +
        "&& $x.year == '2023'))" +
        `->limit(${DEFAULT_DRILL_LIMIT})`,
    );
    // The population, not its aggregate: drilling into a number must
    // show the rows it was computed from.
    assert.equal(sql.includes('groupBy('), false);
    assert.equal(sql.includes('pivot('), false);
  });

  it('keeps the cube filter, so the drill sees the same population', () => {
    const s: CubeSnapshot = {
      ...SNAPSHOT,
      filter: {
        kind: 'condition',
        column: 'notional',
        operator: 'greaterThan',
        value: 0,
      },
    };
    assert.match(
      drillQuery(s, { path: ['EMEA'] }),
      /\$x\.notional > 0 && \$x\.region == 'EMEA'/,
    );
  });

  it('drills the grand total to the whole filtered population', () => {
    assert.equal(
      drillQuery(SNAPSHOT, { path: [] }),
      `trades->limit(${DEFAULT_DRILL_LIMIT})`,
    );
  });

  it('matches a NULL group with isEmpty', () => {
    assert.match(
      drillQuery(SNAPSHOT, { path: [NULL_GROUP] }),
      /\$x\.region->isEmpty\(\)/,
    );
  });

  it('applies derived columns before filtering on them', () => {
    const s: CubeSnapshot = {
      ...SNAPSHOT,
      derived: [{ name: 'net', expression: '$x.notional * 0.98' }],
    };
    assert.match(
      drillQuery(s, { path: ['EMEA'] }),
      /^trades->extend\(~\[net: x\|\$x\.notional \* 0\.98\]\)->filter\(/,
    );
  });

  it('caps the rows, because a drill is a peek not an export', () => {
    assert.match(drillQuery(SNAPSHOT, { path: [], limit: 25 }), /->limit\(25\)$/);
  });
});

describe('drillConditions', () => {
  it('exposes the conditions so a reviewer can read them', () => {
    const cs = drillConditions(SNAPSHOT, {
      path: ['EMEA'],
      pivotPath: ['2024'],
    });
    assert.deepEqual(cs, [
      { kind: 'condition', column: 'region', operator: 'equal', value: 'EMEA' },
      { kind: 'condition', column: 'year', operator: 'equal', value: '2024' },
    ]);
  });

  it('ignores a path deeper than the cube has dimensions', () => {
    const cs = drillConditions(SNAPSHOT, { path: ['A', 'B', 'C'] });
    assert.equal(cs.length, 2, 'only region and desk exist');
  });
});

describe('pivotPathOf', () => {
  it('strips the measure, leaving the pivot values', () => {
    assert.deepEqual(pivotPathOf(['2023', 'total'], ['total']), ['2023']);
    assert.deepEqual(
      pivotPathOf(['USA', 'NYC', 'total'], ['total']),
      ['USA', 'NYC'],
    );
  });

  it('leaves a path that does not end in a measure alone', () => {
    assert.deepEqual(pivotPathOf(['2023'], ['total']), ['2023']);
  });
});
