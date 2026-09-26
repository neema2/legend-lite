// Ad Hoc Analysis mode, running: an in-memory source answers the very
// snapshots the planner would be handed (the filter, the grouping, the
// aggregates), so the whole flow -- member lookups, zoom levels, one
// query per shape, suppression, undo -- runs without a browser. The
// browser harness proves the same against the real planner and engine.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { buildCube } from '../src/adhoc/outline.ts';
import { AdHocSession, type Run } from '../src/adhoc/session.ts';
import type { ResultTable, Scalar } from '../src/result.ts';
import type { CubeSnapshot, FilterNode } from '../src/snapshot.ts';

type Row = Record<string, string | number | null>;
const DATA: Row[] = [
  { year: '2021', quarter: 'Q1', region: 'EMEA', notional: 10, pnl: 1 },
  { year: '2021', quarter: 'Q2', region: 'EMEA', notional: 20, pnl: 2 },
  { year: '2021', quarter: 'Q2', region: 'AMER', notional: 5, pnl: 0 },
  { year: '2022', quarter: 'Q1', region: 'AMER', notional: 40, pnl: 4 },
];

const SNAPSHOT: CubeSnapshot = {
  source: { expression: 't' },
  columns: [
    { name: 'year', type: 'String', kind: 'dimension' },
    { name: 'quarter', type: 'String', kind: 'dimension' },
    { name: 'region', type: 'String', kind: 'dimension' },
    { name: 'notional', type: 'Float' },
    { name: 'pnl', type: 'Float' },
  ],
  derived: [],
  rows: [],
  pivotOn: [],
  measures: [],
  sorts: [],
  epoch: 1,
};

function matches(row: Row, f: FilterNode | undefined): boolean {
  if (!f) return true;
  switch (f.kind) {
    case 'and': return f.children.every((c) => matches(row, c));
    case 'or': return f.children.some((c) => matches(row, c));
    case 'not': return !matches(row, f.child);
    case 'condition':
      if (f.operator === 'isEmpty') return row[f.column] === null;
      if (f.operator === 'equal') return String(row[f.column]) === String(f.value);
      throw new Error(`the fake database has no ${f.operator}`);
  }
}

/** Answers a snapshot as the planner + engine would: filter, group, aggregate, sort. */
function fakeRun(calls: string[]): Run {
  return async (pure, snapshot) => {
    calls.push(pure);
    const rows = DATA.filter((r) => matches(r, snapshot.filter));
    const keys = snapshot.rows;
    const groups = new Map<string, Row[]>();
    for (const r of rows) {
      const k = keys.map((c) => String(r[c])).join('|');
      groups.set(k, [...(groups.get(k) ?? []), r]);
    }
    const ordered = [...groups.entries()].sort(([a], [b]) => a.localeCompare(b));
    const columns = [
      ...keys.map((c) => ({ name: c, type: 'String',
        values: ordered.map(([, rs]) => rs[0]?.[c] ?? null) as Scalar[] })),
      ...snapshot.measures.map((m) => ({ name: m.name, type: 'Float',
        values: ordered.map(([, rs]) => (m.fn === 'count' ? rs.length
          : rs.reduce((n, r) => n + Number(r[m.column] ?? 0), 0))) as Scalar[] })),
    ];
    const t: ResultTable = { columns, rowCount: ordered.length, epoch: 1, elapsedMs: 1 };
    return t;
  };
}

const session = (calls: string[] = []) => new AdHocSession(
  buildCube(SNAPSHOT, [{ name: 'Time', columns: ['year', 'quarter'] }]), fakeRun(calls));

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
