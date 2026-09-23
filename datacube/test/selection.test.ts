import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { toClipboard } from '../src/export.ts';
import type { ResultTable } from '../src/result.ts';
import {
  bounds,
  cellCount,
  contains,
  extend,
  selectionStats,
  selectionTable,
  single,
} from '../src/selection.ts';

const TABLE: ResultTable = {
  columns: [
    { name: 'region', type: 'String', values: ['EMEA', 'AMER', 'APAC'] },
    { name: 'q1', type: 'Float', values: [10, -5, null] },
    { name: 'q2', type: 'Float', values: [20, 0, 30] },
  ],
  rowCount: 3,
  epoch: 1,
  elapsedMs: 0,
};

describe('ranges', () => {
  it('normalises bounds however the drag went', () => {
    // Dragging up-and-left is as ordinary as down-and-right.
    const down = { anchor: { row: 0, col: 0 }, focus: { row: 2, col: 2 } };
    const up = { anchor: { row: 2, col: 2 }, focus: { row: 0, col: 0 } };
    assert.deepEqual(bounds(down), bounds(up));
  });

  it('keeps the anchor when extended, so growth is from the start', () => {
    // Extending from wherever the user last was, rather than where
    // they started, is the classic shift-click bug.
    const r = extend(single(1, 1), { row: 3, col: 4 });
    assert.deepEqual(r.anchor, { row: 1, col: 1 });
    assert.deepEqual(bounds(r), { top: 1, left: 1, bottom: 3, right: 4 });

    const back = extend(r, { row: 0, col: 0 });
    assert.deepEqual(back.anchor, { row: 1, col: 1 }, 'still the anchor');
    assert.deepEqual(bounds(back), { top: 0, left: 0, bottom: 1, right: 1 });
  });

  it('counts cells and tests membership', () => {
    const r = { anchor: { row: 0, col: 0 }, focus: { row: 1, col: 2 } };
    assert.equal(cellCount(r), 6);
    assert.equal(contains(r, 1, 2), true);
    assert.equal(contains(r, 2, 0), false);
  });

  it('treats a single cell as a range of one', () => {
    assert.equal(cellCount(single(3, 4)), 1);
  });
});

describe('selectionStats', () => {
  it('excludes blanks from the arithmetic but counts them', () => {
    // An average that treated an empty pivot combination as zero
    // would be wrong in the direction of looking plausible.
    const s = selectionStats(TABLE, {
      anchor: { row: 0, col: 1 },
      focus: { row: 2, col: 1 },
    });
    assert.equal(s.cells, 3);
    assert.equal(s.blank, 1);
    assert.equal(s.numeric, 2);
    assert.equal(s.sum, 5);
    assert.equal(s.average, 2.5, 'divided by 2, not by 3');
  });

  it('ignores non-numeric cells rather than coercing them', () => {
    // Selecting a label column alongside a measure must not make the
    // sum NaN.
    const s = selectionStats(TABLE, {
      anchor: { row: 0, col: 0 },
      focus: { row: 2, col: 2 },
    });
    assert.equal(s.numeric, 5, 'three labels excluded');
    assert.equal(Number.isNaN(s.sum), false);
    assert.equal(s.sum, 55);
  });

  it('counts a real zero as a value, not a blank', () => {
    const s = selectionStats(TABLE, {
      anchor: { row: 1, col: 2 },
      focus: { row: 1, col: 2 },
    });
    assert.equal(s.numeric, 1);
    assert.equal(s.blank, 0);
    assert.equal(s.min, 0);
  });

  it('reports min and max across the rectangle', () => {
    const s = selectionStats(TABLE, {
      anchor: { row: 0, col: 1 },
      focus: { row: 2, col: 2 },
    });
    assert.equal(s.min, -5);
    assert.equal(s.max, 30);
  });

  it('returns zeros rather than infinities for an empty selection', () => {
    const s = selectionStats(TABLE, {
      anchor: { row: 0, col: 0 },
      focus: { row: 2, col: 0 },
    });
    assert.equal(s.numeric, 0);
    assert.equal(s.min, 0);
    assert.equal(s.max, 0);
    assert.equal(s.average, 0);
  });
});

describe('selectionTable', () => {
  it('cuts out the rectangle, columns and rows', () => {
    const t = selectionTable(TABLE, {
      anchor: { row: 1, col: 1 },
      focus: { row: 2, col: 2 },
    });
    assert.deepEqual(t.columns.map((c) => c.name), ['q1', 'q2']);
    assert.deepEqual(t.columns[0]?.values, [-5, null]);
    assert.equal(t.rowCount, 2);
  });

  it('feeds the existing exporter, so escaping cannot drift', () => {
    // A clipboard payload and a CSV file differ in their delimiter,
    // not in how they escape.
    const t = selectionTable(TABLE, {
      anchor: { row: 0, col: 0 },
      focus: { row: 1, col: 1 },
    });
    assert.equal(toClipboard(t), 'region\tq1\nEMEA\t10\nAMER\t-5\n');
  });
});
