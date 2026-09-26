// Ad Hoc Analysis mode's pure state: the ad hoc operations, one by one.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
  DEFAULT_OPTIONS,
  MEASURES,
  initialGrid,
  keepOnly,
  pivot,
  pivotToPov,
  povToAxis,
  removeOnly,
  selectMembers,
  setPov,
  tuples,
  withOptions,
  zoomIn,
  zoomOut,
  type AdHocGrid,
  type Outline,
} from '../src/adhoc/state.ts';

const OUTLINE: Outline = {
  dimensions: [
    { name: 'Time', generations: ['year', 'quarter'] },
    { name: 'Geography', generations: ['region', 'desk'] },
    { name: 'Book', generations: ['book'] },
  ],
  measures: ['notional', 'pnl'],
};

const rowsOf = (g: AdHocGrid, dim = 'Time'): string[] =>
  (g.rows.find((a) => a.dimension === dim)?.members ?? []).map((m) => m.join('/') || '(top)');

describe('the opening grid', () => {
  it("is the classic ad hoc grid: first dimension down, measures across, the rest on the POV at the top", () => {
    const g = initialGrid(OUTLINE);
    assert.deepEqual(g.rows, [{ dimension: 'Time', members: [[]] }]);
    assert.deepEqual(g.columns, [{ dimension: MEASURES, members: [['notional'], ['pnl']] }]);
    assert.deepEqual(g.pov, { Geography: [], Book: [] });
    assert.equal(g.options.suppressMissingRows, true, 'the user decision: suppress missing on');
    assert.equal(g.options.ancestorPosition, 'top', 'the user decision: top by default');
  });

  it('opens on the dimension asked for, the rest on the POV', () => {
    const g = initialGrid(OUTLINE, undefined, 'Geography');
    assert.deepEqual(g.rows.map((a) => a.dimension), ['Geography']);
    assert.ok('Time' in g.pov && !('Geography' in g.pov));
  });
});

describe('Zoom In / Zoom Out', () => {
  const g0 = initialGrid(OUTLINE);

  it('Next Level puts the children after the member (ancestor position top)', () => {
    const g = zoomIn(g0, 'Time', [], [['2021'], ['2022']]);
    assert.deepEqual(rowsOf(g), ['(top)', '2021', '2022']);
  });

  it('ancestor position BOTTOM puts them before it', () => {
    const g = zoomIn(withOptions(g0, { ancestorPosition: 'bottom' }), 'Time', [], [['2021'], ['2022']]);
    assert.deepEqual(rowsOf(g), ['2021', '2022', '(top)']);
  });

  it('zooming a member twice replaces its descendants rather than repeating them', () => {
    let g = zoomIn(g0, 'Time', [], [['2021'], ['2022']]);
    g = zoomIn(g, 'Time', ['2021'], [['2021', 'Q1'], ['2021', 'Q2']]);
    assert.deepEqual(rowsOf(g), ['(top)', '2021', '2021/Q1', '2021/Q2', '2022']);
    g = zoomIn(g, 'Time', [], [['2021'], ['2021', 'Q1'], ['2022']]);
    assert.deepEqual(rowsOf(g), ['(top)', '2021', '2021/Q1', '2022']);
  });

  it('Zoom Out collapses a member and its siblings into the parent', () => {
    let g = zoomIn(g0, 'Time', [], [['2021'], ['2022']]);
    g = zoomIn(g, 'Time', ['2021'], [['2021', 'Q1'], ['2021', 'Q2']]);
    g = zoomOut(g, 'Time', ['2021', 'Q2']);
    assert.deepEqual(rowsOf(g), ['(top)', '2021', '2022']);
    g = zoomOut(g, 'Time', ['2022']);
    assert.deepEqual(rowsOf(g), ['(top)']);
    assert.equal(zoomOut(g, 'Time', []), g, 'the top has nowhere to go');
  });

  it('Zoom Out where the parent is not shown brings it back in their place', () => {
    const g = selectMembers(g0, 'Time', [['2020'], ['2021', 'Q1'], ['2021', 'Q2'], ['2022']]);
    assert.deepEqual(rowsOf(zoomOut(g, 'Time', ['2021', 'Q1'])), ['2020', '2021', '2022']);
  });
});

describe('Keep Only / Remove Only', () => {
  const g = zoomIn(initialGrid(OUTLINE), 'Time', [], [['2021'], ['2022'], ['2023']]);

  it('keeps only the selected members', () => {
    assert.deepEqual(rowsOf(keepOnly(g, 'Time', [['2021'], ['2023']])), ['2021', '2023']);
  });

  it('removes only the selected members, never the last', () => {
    assert.deepEqual(rowsOf(removeOnly(g, 'Time', [['2022']])), ['(top)', '2021', '2023']);
    const one = keepOnly(g, 'Time', [['2021']]);
    assert.equal(removeOnly(one, 'Time', [['2021']]), one);
  });

  it('works on Measures like any other dimension (the user decision)', () => {
    const k = keepOnly(g, MEASURES, [['pnl']]);
    assert.deepEqual(k.columns[0]?.members, [['pnl']]);
  });
});

describe('Pivot / POV', () => {
  const g0 = initialGrid(OUTLINE);

  it('never empties an axis', () => {
    assert.equal(pivot(g0, 'Time'), g0);
    assert.equal(pivotToPov(g0, MEASURES), g0);
  });

  it('a POV dimension comes onto an axis at its member, and pivots between axes', () => {
    let g = povToAxis(setPov(g0, 'Geography', ['EMEA']), 'Geography', 'rows');
    assert.deepEqual(g.rows.map((a) => a.dimension), ['Time', 'Geography']);
    assert.deepEqual(g.rows[1]?.members, [['EMEA']]);
    assert.equal('Geography' in g.pov, false);
    g = pivot(g, 'Geography');
    assert.deepEqual(g.columns.map((a) => a.dimension), [MEASURES, 'Geography']);
    g = pivotToPov(g, 'Geography');
    assert.deepEqual(g.pov['Geography'], ['EMEA'], 'it keeps the member it showed');
  });

  it('nested axes give the tuples outer-first', () => {
    const g = povToAxis(zoomIn(g0, 'Time', [], [['2021']]), 'Book', 'rows');
    assert.deepEqual(tuples(g.rows).map((t) => t.map((m) => m.join('/') || '*').join(' | ')),
      ['* | *', '2021 | *']);
  });

  it('setPov with the same member changes nothing', () => {
    assert.equal(setPov(g0, 'Book', []), g0);
    assert.equal(setPov(g0, 'Nope', ['x']), g0);
  });

  it('options default as decided', () => {
    assert.equal(DEFAULT_OPTIONS.zoomLevel, 'next');
    assert.equal(DEFAULT_OPTIONS.suppressZeroRows, false);
  });
});
