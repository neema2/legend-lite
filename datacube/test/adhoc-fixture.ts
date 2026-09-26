// The in-memory source Ad Hoc Analysis mode's tests query: it answers
// the very snapshots the planner would be handed (the filter, the
// grouping, the aggregates), as the planner and engine would.

import { buildCube } from '../src/adhoc/outline.ts';
import { AdHocSession, type Run } from '../src/adhoc/session.ts';
import type { ResultTable, Scalar } from '../src/result.ts';
import type { CubeSnapshot, FilterNode } from '../src/snapshot.ts';

type Row = Record<string, string | number | null>;
export const DATA: Row[] = [
  { year: '2021', quarter: 'Q1', region: 'EMEA', notional: 10, pnl: 1 },
  { year: '2021', quarter: 'Q2', region: 'EMEA', notional: 20, pnl: 2 },
  { year: '2021', quarter: 'Q2', region: 'AMER', notional: 5, pnl: 0 },
  { year: '2022', quarter: 'Q1', region: 'AMER', notional: 40, pnl: 4 },
];

export const SNAPSHOT: CubeSnapshot = {
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
export function fakeRun(calls: string[]): Run {
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

/** A session over the fixture: Time = year > quarter, region on its own. */
export const session = (calls: string[] = []): AdHocSession => new AdHocSession(
  buildCube(SNAPSHOT, [{ name: 'Time', columns: ['year', 'quarter'] }]), fakeRun(calls));
