import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
  EDITOR_TABS,
  TAB_LABELS,
  draftFor,
  sortLabel,
} from '../src/ui/editor.ts';
import { DEFAULT_CONFIGURATION } from '../src/config.ts';
import { freshName } from '../src/ui/panel-dimensions.ts';
import { groupableColumns } from '../src/grid-edits.ts';
import { AGGREGATES } from '../src/ui/panel-column.ts';
import type { CubeSnapshot } from '../src/snapshot.ts';
import { element } from '../../pure-protocol/src/index.ts';
import { stand } from './fake-planner.ts';

const CUBE: CubeSnapshot = {
  source: { query: element('t') },
  columns: [
    { name: 'region', type: 'String' },
    { name: 'desk', type: 'String' },
    { name: 'year', type: 'Integer', kind: 'dimension' },
    { name: 'notional', type: 'Float' },
  ],
  // A ratio, so a measure -- declared, as the calculated-column
  // editor always declares a row-stage column's kind now.
  derived: [{ name: 'margin', lambda: stand('$x.a / $x.b'), kind: 'measure' }],
  rows: ['region'],
  pivotOn: [],
  sorts: [{ column: 'notional', direction: 'desc' }],
  epoch: 3,
};

describe('the rail', () => {
  it("is DataCube's seven sections: the layout, named as the drop zones name it, then the properties", () => {
    assert.deepEqual(EDITOR_TABS, [
      'Columns',
      'Vertical Pivots',
      'Horizontal Pivots',
      'Sorts',
      'Dimensions',
      'Column Properties',
      'General Properties',
    ]);
    assert.equal(TAB_LABELS['Vertical Pivots'], 'Row Groups');
    assert.equal(TAB_LABELS['Horizontal Pivots'], 'Column Labels');
  });
});


describe('sortLabel', () => {
  it('reads a pivoted column back as its dimension values', () => {
    assert.equal(sortLabel('2023__|__EMEA'), '2023 / EMEA');
    assert.equal(sortLabel('total'), 'total');
  });
});

describe('the aggregate list', () => {
  it('offers every aggregate the snapshot can express', () => {
    // A measure the query supports and the UI cannot reach is the
    // failure worth catching; the compile-time check in the panel
    // catches the other direction.
    // 13 since `unique` joined them: DataCube's default aggregate
    // for every non-numeric column, and what lets grouping keep the
    // text columns instead of dropping them; 15 since the quartiles
    // did (chart batch 4, a box plot's box); 21 since the aggregates
    // of two columns and the distinct count (BI parity P2: corr, the
    // covariances, max by, min by, distinctCount).
    assert.equal(AGGREGATES.length, 21);
    assert.equal(new Set(AGGREGATES.map((a) => a.value)).size, 21);
  });
});

describe('groupableColumns', () => {
  it('offers a calculated DIMENSION, and never a calculated measure', () => {
    // Reading the source columns alone, a calculated column could
    // never be grouped on, whatever kind it declared.
    const names = groupableColumns(draftFor({
      ...CUBE,
      derived: [
        { name: 'margin', lambda: stand('$x.a / $x.b'), kind: 'measure' },
        { name: 'big', lambda: stand('$x.notional > 1'), kind: 'dimension' },
      ],
      groupDerived: [{ name: 'share', lambda: stand('$x.notional / 2') }],
    }));
    assert.deepEqual(names, ['region', 'desk', 'year', 'big']);
  });
});

describe('freshName', () => {
  it('skips names already taken', () => {
    assert.equal(freshName([]), 'Dimension 1');
    assert.equal(
      freshName([
        { name: 'Dimension 1', columns: [] },
        { name: 'Dimension 2', columns: [] },
      ]),
      'Dimension 3',
    );
  });
});

describe('the default configuration', () => {
  it('records nothing per column until a user says something', () => {
    assert.deepEqual(DEFAULT_CONFIGURATION.columns, {});
  });
});
