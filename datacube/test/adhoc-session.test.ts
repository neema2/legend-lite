// Ad Hoc Analysis mode, running: an in-memory source answers the very
// snapshots the planner would be handed (the filter, the grouping, the
// aggregates), so the whole flow -- member lookups, zoom levels, one
// query per shape, suppression, undo -- runs without a browser. The
// browser harness proves the same against the real planner and engine.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { buildCube } from '../src/adhoc/outline.ts';
import type { AdHocSession } from '../src/adhoc/session.ts';
import { SNAPSHOT, session } from './adhoc-fixture.ts';

const labels = (s: AdHocSession) =>
  s.view?.table.columns[0]?.values.map((v) => String(v).replace(/ /g, '.'));
const values = (s: AdHocSession, measure = 1) => s.view?.table.columns[measure]?.values;

describe('the outline', () => {
  it('named dimensions are hierarchies; every other dimension column stands alone; measures sum', () => {
    const cube = buildCube(SNAPSHOT, [{ name: 'Time', columns: ['year', 'quarter'] }]);
    assert.deepEqual(cube.outline.dimensions.map((d) => [d.name, d.generations.join('>')]),
      [['Time', 'year>quarter'], ['region', 'region']]);
    assert.deepEqual(cube.outline.measures, ['notional', 'pnl']);
  });
});

describe('an ad hoc session', () => {
  it('opens on the top member with the grand totals', async () => {
    const s = session();
    await s.refresh();
    assert.deepEqual(labels(s), ['Time']);
    assert.deepEqual(values(s), [75]);
  });

  it('Zoom In (next level) finds the children in the source, and totals them', async () => {
    const s = session();
    await s.refresh();
    await s.zoomIn('Time', []);
    assert.deepEqual(labels(s), ['Time', '.2021', '.2022']);
    assert.deepEqual(values(s), [75, 35, 40]);
  });

  it('Zoom In, all levels: every descendant in hierarchy order', async () => {
    const s = session();
    await s.zoomIn('Time', [], 'all');
    assert.deepEqual(labels(s), ['Time', '.2021', '..Q1', '..Q2', '.2022', '..Q1']);
    assert.deepEqual(values(s), [75, 35, 10, 25, 40, 40]);
  });

  it('Zoom In, bottom level: only the leaves', async () => {
    const s = session();
    await s.zoomIn('Time', [], 'bottom');
    assert.deepEqual(labels(s), ['Time', '..Q1', '..Q2', '..Q1']);
  });

  it('the POV filters every cell', async () => {
    const s = session();
    await s.zoomIn('Time', []);
    await s.setPov('region', ['EMEA']);
    assert.deepEqual(labels(s), ['Time', '.2021'], '2022 has no EMEA rows: suppressed');
    assert.deepEqual(values(s), [30, 30]);
  });

  it('suppression and indentation re-place the answers without asking again', async () => {
    const calls: string[] = [];
    const s = session(calls);
    await s.zoomIn('Time', []);
    await s.setPov('region', ['EMEA']);
    const asked = calls.length;
    await s.setOptions({ suppressMissingRows: false, indentation: 'none' });
    assert.equal(calls.length, asked, 'a display option ran a query');
    assert.deepEqual(labels(s), ['Time', '2021', '2022']);
    assert.deepEqual(values(s), [30, 30, null]);
  });

  it('undo and redo walk the grid back and forth', async () => {
    const s = session();
    await s.refresh();
    await s.zoomIn('Time', []);
    await s.undo();
    assert.deepEqual(labels(s), ['Time']);
    await s.redo();
    assert.deepEqual(labels(s), ['Time', '.2021', '.2022']);
  });
});
