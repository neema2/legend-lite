// Ad Hoc Analysis mode, running: an in-memory source answers the very
// snapshots the planner would be handed (the filter, the grouping, the
// aggregates), so the whole flow -- member lookups, zoom levels, one
// query per shape, suppression, undo -- runs without a browser. The
// browser harness proves the same against the real planner and engine.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { buildCube, carryOver } from '../src/adhoc/outline.ts';
import { AdHocSession } from '../src/adhoc/session.ts';
import type { FilterNode } from '../src/snapshot.ts';
import { fakeRun, SNAPSHOT, session } from './adhoc-fixture.ts';

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

describe('opening on the cube as it stands', () => {
  const TIME = [{ name: 'Time', columns: ['year', 'quarter'] }];

  it('row groups go down the rows, column pivots across after the measures', async () => {
    const { cube, grid } = carryOver({ ...SNAPSHOT, rows: ['region'], pivotOn: ['year'] }, TIME);
    assert.deepEqual(grid.rows.map((a) => a.dimension), ['region']);
    assert.deepEqual(grid.columns.map((a) => a.dimension), ['Measures', 'Time']);
    assert.deepEqual(grid.pov, {});
    const s = new AdHocSession(cube, fakeRun([]), grid);
    await s.refresh();
    assert.deepEqual(labels(s), ['region']);
    assert.deepEqual(values(s), [75]);
  });

  it('a filter pinning a member down a hierarchy becomes that member, and leaves the filter', async () => {
    const filter: FilterNode = { kind: 'and', children: [
      { kind: 'condition', column: 'year', operator: 'equal', value: '2021' },
      { kind: 'condition', column: 'quarter', operator: 'equal', value: 'Q2' },
    ] };
    const { cube, grid } = carryOver({ ...SNAPSHOT, filter }, TIME);
    assert.deepEqual(grid.rows.map((a) => a.dimension), ['Time']);
    assert.deepEqual(grid.rows[0]?.members, [['2021', 'Q2']], 'on the rows: the member shown');
    assert.equal(cube.snapshot.filter, undefined, 'the conditions left the cube filter');
    const s = new AdHocSession(cube, fakeRun([]), grid);
    await s.refresh();
    assert.deepEqual(values(s), [25]);
  });

  it('on the POV when the dimension is off the grid; other conditions stay on the cube', async () => {
    const keep: FilterNode = { kind: 'condition', column: 'notional', operator: 'greaterThan', value: 5 };
    const filter: FilterNode = { kind: 'and', children: [
      { kind: 'condition', column: 'region', operator: 'equal', value: 'EMEA' },
      keep,
    ] };
    const { cube, grid } = carryOver({ ...SNAPSHOT, rows: ['year'], filter }, TIME);
    assert.deepEqual(grid.pov, { region: ['EMEA'] });
    assert.deepEqual(cube.snapshot.filter, keep);
  });

  it('an OR filter is not a member: it stays whole', () => {
    const filter: FilterNode = { kind: 'or', children: [
      { kind: 'condition', column: 'region', operator: 'equal', value: 'EMEA' },
      { kind: 'condition', column: 'region', operator: 'equal', value: 'AMER' },
    ] };
    const { cube, grid } = carryOver({ ...SNAPSHOT, filter }, TIME);
    assert.deepEqual(grid.pov, { region: [] });
    assert.deepEqual(cube.snapshot.filter, filter);
  });
});
