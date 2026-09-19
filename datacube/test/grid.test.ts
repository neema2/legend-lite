import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
  TREE_COLUMN,
  buildColumnModel,
  splitPath,
  valueColumns,
} from '../src/grid/columns.ts';
import {
  computeRowWindow,
  isCovered,
  sliceFor,
} from '../src/grid/viewport.ts';
import type { ResultTable } from '../src/result.ts';

function table(names: string[]): ResultTable {
  return {
    columns: names.map((name) => ({ name, type: 'Float', values: [] })),
    rowCount: 0,
    epoch: 1,
    elapsedMs: 0,
  };
}

describe('computeRowWindow', () => {
  const base = {
    scrollTop: 0,
    viewportHeight: 400,
    rowHeight: 20,
    totalRows: 1000,
    overscan: 5,
  };

  it('renders only the visible band plus overscan', () => {
    const w = computeRowWindow(base);
    assert.equal(w.start, 0);
    // 400/20 = 20 visible, +1 partial, +5 overscan
    assert.equal(w.end, 26);
    assert.equal(w.totalHeight, 20000);
    assert.equal(w.offsetTop, 0);
  });

  it('offsets the block to match the scroll position', () => {
    const w = computeRowWindow({ ...base, scrollTop: 2000 });
    assert.equal(w.start, 95); // row 100 minus 5 overscan
    assert.equal(w.offsetTop, 1900);
  });

  it('clamps at the end rather than running past the last row', () => {
    const w = computeRowWindow({ ...base, scrollTop: 19_990 });
    assert.equal(w.end, 1000);
    assert.ok(w.start < w.end);
  });

  it('survives elastic overscroll reporting a negative scrollTop', () => {
    // macOS rubber-banding really does report this; a negative start
    // index would index off the front of the data.
    const w = computeRowWindow({ ...base, scrollTop: -120 });
    assert.equal(w.start, 0);
    assert.equal(w.offsetTop, 0);
  });

  it('returns an empty but valid window with no rows', () => {
    const w = computeRowWindow({ ...base, totalRows: 0 });
    assert.deepEqual(w, { start: 0, end: 0, offsetTop: 0, totalHeight: 0 });
  });

  it('returns an empty window before layout, not NaN', () => {
    const w = computeRowWindow({ ...base, viewportHeight: 0 });
    assert.equal(Number.isNaN(w.end), false);
    assert.equal(w.start, 0);
  });

  it('never divides by a zero row height', () => {
    const w = computeRowWindow({ ...base, rowHeight: 0 });
    assert.equal(Number.isFinite(w.totalHeight), true);
    assert.equal(Number.isFinite(w.end), true);
  });
});

describe('isCovered and sliceFor', () => {
  it('ignores a scroll that stays inside the rendered band', () => {
    const rendered = { start: 0, end: 50, offsetTop: 0, totalHeight: 1000 };
    const wanted = { start: 5, end: 40, offsetTop: 100, totalHeight: 1000 };
    assert.equal(isCovered(rendered, wanted), true);
    assert.equal(
      isCovered(rendered, { ...wanted, end: 60 }),
      false,
    );
  });

  it('maps a window onto a fetched block, or reports a miss', () => {
    const block = { offset: 100, rowCount: 200 };
    assert.deepEqual(
      sliceFor(block, { start: 120, end: 150, offsetTop: 0, totalHeight: 0 }),
      { from: 20, to: 50 },
    );
    assert.equal(
      sliceFor(block, { start: 90, end: 150, offsetTop: 0, totalHeight: 0 }),
      null,
    );
    assert.equal(
      sliceFor(block, { start: 250, end: 400, offsetTop: 0, totalHeight: 0 }),
      null,
    );
  });
});

describe('buildColumnModel', () => {
  it('splits on the separator when no measures are known', () => {
    assert.deepEqual(splitPath('2023__|__total'), ['2023', 'total']);
    assert.deepEqual(splitPath('region'), ['region']);
  });

  it("splits on the engine's own naming, given the measure names", () => {
    // DuckDB joins value to measure with '_', not '__|__'. Assuming
    // the separator produced a flat header and unformatted measures;
    // the measure name is the reliable anchor.
    assert.deepEqual(splitPath('2021_notional', ['notional']), [
      '2021',
      'notional',
    ]);
    assert.deepEqual(splitPath('USA__|__NYC_total', ['total']), [
      'USA',
      'NYC',
      'total',
    ]);
  });

  it('prefers the longest measure so one cannot shadow another', () => {
    assert.deepEqual(splitPath('2021_pnl_net', ['pnl', 'pnl_net']), [
      '2021',
      'pnl_net',
    ]);
  });

  it('treats a bare measure column as depth 1', () => {
    assert.deepEqual(splitPath('notional', ['notional']), ['notional']);
  });

  it('builds a nested header from engine-style names', () => {
    const m = buildColumnModel(
      table(['region', '2021_notional', '2022_notional']),
      ['region'],
      ['notional'],
    );
    assert.equal(m.depth, 2);
    assert.deepEqual(
      m.headerRows[0]?.map((h) => [h.label, h.rowSpan]),
      [
        ['region', 2],
        ['2021', 1],
        ['2022', 1],
      ],
    );
  });

  it('never splits a row dimension, even if it ends in a measure name', () => {
    // A dimension called 'desk' with a measure called 'k' would
    // otherwise be torn into ['des', 'k'].
    const m = buildColumnModel(table(['desk', '2021_k']), ['desk'], ['k']);
    assert.deepEqual(m.leaves[0]?.path, ['desk']);
  });

  it('builds a flat single-level header when there is no pivot', () => {
    const m = buildColumnModel(table(['region', 'total']), ['region']);
    assert.equal(m.depth, 1);
    assert.equal(m.headerRows.length, 1);
    assert.deepEqual(
      m.headerRows[0]?.map((h) => h.label),
      ['region', 'total'],
    );
  });

  it('nests pivot value over measure', () => {
    const m = buildColumnModel(
      table(['region', '2023__|__total', '2023__|__count', '2024__|__total']),
      ['region'],
    );
    assert.equal(m.depth, 2);
    assert.deepEqual(
      m.headerRows[0]?.map((h) => [h.label, h.colSpan, h.rowSpan]),
      [
        // The row dimension is ragged: depth 1 in a 2-deep header, so
        // it spans both levels rather than leaving a hole beneath.
        ['region', 1, 2],
        ['2023', 2, 1],
        ['2024', 1, 1],
      ],
    );
    assert.deepEqual(
      m.headerRows[1]?.map((h) => h.label),
      ['total', 'count', 'total'],
    );
  });

  it('does not merge equal labels under different parents', () => {
    // Both years have a 'total'; merging on the segment alone would
    // fuse two unrelated columns into one header cell.
    const m = buildColumnModel(
      table(['2023__|__total', '2024__|__total']),
      [],
    );
    assert.equal(m.headerRows[0]?.length, 2);
    assert.deepEqual(
      m.headerRows[1]?.map((h) => h.colSpan),
      [1, 1],
    );
  });

  it('handles a three-level multi-column pivot', () => {
    const m = buildColumnModel(
      table([
        'book',
        'USA__|__NYC__|__total',
        'USA__|__LA__|__total',
        'UK__|__LDN__|__total',
      ]),
      ['book'],
    );
    assert.equal(m.depth, 3);
    assert.deepEqual(
      m.headerRows[0]?.map((h) => [h.label, h.colSpan, h.rowSpan]),
      [
        ['book', 1, 3],
        ['USA', 2, 1],
        ['UK', 1, 1],
      ],
    );
    assert.deepEqual(
      m.headerRows[1]?.map((h) => [h.label, h.colSpan]),
      [
        ['NYC', 1],
        ['LA', 1],
        ['LDN', 1],
      ],
    );
  });

  it('gives every leaf a header cell exactly once per level', () => {
    const m = buildColumnModel(
      table(['region', 'country', '2023__|__total', '2024__|__total']),
      ['region', 'country'],
    );
    // Total horizontal coverage at each level must equal the leaf
    // count, once rowSpan-covered shallow columns are accounted for.
    for (let level = 0; level < m.depth; level++) {
      const covered = m.headerRows[level]!.reduce(
        (sum, h) => sum + h.colSpan,
        0,
      );
      const shallow = m.leaves.filter((l) => l.path.length <= level).length;
      assert.equal(
        covered + shallow,
        m.leaves.length,
        `level ${level} must cover every column exactly once`,
      );
    }
  });

  it('hides columns before building the header', () => {
    // Hiding after the header is built leaves a phantom colSpan that
    // pushes every neighbour sideways.
    const m = buildColumnModel(
      table(['region', 'a', 'b']),
      ['region'],
      [],
      { hidden: ['a'] },
    );
    assert.deepEqual(m.leaves.map((l) => l.name), ['region', 'b']);
    assert.deepEqual(
      m.headerRows[0]?.map((h) => [h.label, h.colStart]),
      [
        ['region', 0],
        ['b', 1],
      ],
    );
  });

  it('never hides the tree column', () => {
    // Without it a grouped cube has no row labels at all.
    const m = buildColumnModel(
      table([TREE_COLUMN, 'a']),
      [],
      [],
      { hidden: [TREE_COLUMN, 'a'] },
    );
    assert.deepEqual(m.leaves.map((l) => l.name), [TREE_COLUMN]);
  });

  it('reorders columns, keeping unlisted ones behind in engine order', () => {
    // A measure added after a view was saved must not vanish.
    const m = buildColumnModel(
      table(['a', 'b', 'c', 'd']),
      [],
      [],
      { order: ['c', 'a'] },
    );
    assert.deepEqual(m.leaves.map((l) => l.name), ['c', 'a', 'b', 'd']);
  });

  it('keeps the source index across reordering, so cells follow', () => {
    const m = buildColumnModel(table(['a', 'b']), [], [], { order: ['b', 'a'] });
    assert.deepEqual(
      m.leaves.map((l) => [l.name, l.index]),
      [
        ['b', 1],
        ['a', 0],
      ],
    );
  });

  it('carries an explicit width', () => {
    const m = buildColumnModel(
      table(['a', 'b']),
      [],
      [],
      { widths: { a: 320 } },
    );
    assert.equal(m.leaves[0]?.width, 320);
    assert.equal(m.leaves[1]?.width, undefined);
  });

  it('separates dimensions from value columns', () => {
    const m = buildColumnModel(
      table(['region', '2023__|__total']),
      ['region'],
    );
    assert.deepEqual(
      valueColumns(m).map((l) => l.name),
      ['2023__|__total'],
    );
  });
});
