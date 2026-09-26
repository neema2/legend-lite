// The cube shapes DataCube's WASM differential plans, and the model they
// plan against. Shared by emit.ts (which serialises them, for the JVM's
// answers) and compare.ts (which plans them in the module).
//
// The grammar is not written by hand anywhere: it comes out of the
// product's own serialiser, which is the only way to catch a construct
// DataCube emits that the hand-written corpus (wasm/corpus) never
// thought to.

import { detailSnapshot, serialize } from '../../src/serialize.ts';
import { planQueries, type AdHocCube } from '../../src/adhoc/query.ts';
import { initialGrid, setPov, zoomIn } from '../../src/adhoc/state.ts';
import type { LevelScope } from '../../src/serialize.ts';
import type { CubeSnapshot, WindowSpec } from '../../src/snapshot.ts';

export const MODEL = `###Relational
Database trades::DB
(
    Table TRADES
    (
        region VARCHAR(32), desk VARCHAR(32), book VARCHAR(32),
        year INTEGER, qtr VARCHAR(8),
        notional DOUBLE, pnl DOUBLE, qty INTEGER
    )
)

###Connection
RelationalDatabaseConnection trades::Conn
{
    type: DuckDB;
    specification: DuckDB { };
    auth: Test;
}

###Runtime
Runtime trades::RT
{
    mappings: [];
    connections:
    [
        trades::DB: [ c1: trades::Conn ]
    ];
}
`;
export const RUNTIME = 'trades::RT';

const COLUMNS: CubeSnapshot['columns'] = [
  { name: 'region', type: 'String' },
  { name: 'desk', type: 'String' },
  { name: 'book', type: 'String' },
  { name: 'year', type: 'Integer', kind: 'dimension' },
  { name: 'qtr', type: 'String' },
  { name: 'notional', type: 'Float' },
  { name: 'pnl', type: 'Float' },
  { name: 'qty', type: 'Integer' },
];

function snap(over: Partial<CubeSnapshot>): CubeSnapshot {
  return {
    source: { expression: '#>{trades::DB.TRADES}#' },
    columns: COLUMNS,
    derived: [],
    rows: [],
    pivotOn: [],
    measures: [],
    sorts: [],
    epoch: 1,
    ...over,
  };
}

const SUM_NOTIONAL: CubeSnapshot['measures'] = [
  { name: 'notional', column: 'notional', fn: 'sum' },
];

/**
 * Cube shapes a user actually reaches.
 *
 * No `as` casts anywhere below: every case is checked against the
 * real snapshot types, which is the only reason this file catches a
 * shape DataCube would reject.
 */
export const CASES: { name: string; snapshot: CubeSnapshot; scope?: LevelScope }[] = [
  { name: 'flat', snapshot: snap({}) },
  { name: 'measures-only', snapshot: snap({ measures: SUM_NOTIONAL }) },
  {
    name: 'one-dimension',
    snapshot: snap({ rows: ['region'], measures: SUM_NOTIONAL }),
  },
  {
    name: 'three-dimensions',
    snapshot: snap({
      rows: ['region', 'desk', 'book'],
      measures: SUM_NOTIONAL,
    }),
  },
  {
    name: 'pivot',
    snapshot: snap({
      rows: ['region', 'desk'],
      pivotOn: ['year'],
      measures: SUM_NOTIONAL,
    }),
  },
  {
    name: 'pivot-two-measures',
    snapshot: snap({
      rows: ['region'],
      pivotOn: ['year', 'qtr'],
      measures: [
        { name: 'notional', column: 'notional', fn: 'sum' },
        { name: 'qty', column: 'qty', fn: 'count' },
      ],
    }),
  },
  {
    name: 'filtered',
    snapshot: snap({
      rows: ['region'],
      measures: SUM_NOTIONAL,
      filter: { kind: 'condition', column: 'region', operator: 'equal',
                value: 'EMEA' },
    }),
  },
  {
    name: 'filter-injection',
    snapshot: snap({
      rows: ['desk'],
      measures: SUM_NOTIONAL,
      // A quote and a backslash: the pair that turns a literal into
      // grammar if escapePure gets the order wrong.
      filter: { kind: 'condition', column: 'book', operator: 'equal',
                value: "O'Brien\\Co" },
    }),
  },
  {
    name: 'filter-in',
    snapshot: snap({
      rows: ['region'],
      measures: SUM_NOTIONAL,
      filter: { kind: 'condition', column: 'region', operator: 'in',
                value: ['EMEA', 'APAC'] },
    }),
  },
  {
    name: 'filter-and-or',
    snapshot: snap({
      rows: ['region'],
      measures: SUM_NOTIONAL,
      filter: {
        kind: 'and',
        children: [
          { kind: 'condition', column: 'qty', operator: 'greaterThan',
            value: 10 },
          { kind: 'or', children: [
            { kind: 'condition', column: 'region', operator: 'equal',
              value: 'EMEA' },
            { kind: 'condition', column: 'desk', operator: 'notEqual',
              value: 'FX' },
          ] },
        ],
      },
    }),
  },
  {
    name: 'filter-not-empty',
    snapshot: snap({
      rows: ['region'],
      measures: SUM_NOTIONAL,
      filter: { kind: 'not', child: {
        kind: 'condition', column: 'book', operator: 'isEmpty' } },
    }),
  },
  {
    name: 'sorted',
    snapshot: snap({
      rows: ['region'],
      measures: SUM_NOTIONAL,
      sorts: [{ column: 'region', direction: 'desc' }],
    }),
  },
  {
    name: 'derived',
    snapshot: snap({
      rows: ['region'],
      derived: [{ name: 'double_qty', expression: '$x.qty * 2' }],
      measures: SUM_NOTIONAL,
    }),
  },
  // Tree levels: what an expanded row actually asks for. A RowPath is
  // the parent's dimension VALUES, in row order.
  {
    name: 'tree-level-1',
    snapshot: snap({
      rows: ['region', 'desk', 'book'],
      measures: SUM_NOTIONAL,
    }),
    scope: { level: 1, parent: [] },
  },
  {
    name: 'tree-level-2',
    snapshot: snap({
      rows: ['region', 'desk', 'book'],
      measures: SUM_NOTIONAL,
    }),
    scope: { level: 2, parent: ['EMEA'] },
  },
  {
    name: 'tree-level-3-capped',
    snapshot: snap({
      rows: ['region', 'desk', 'book'],
      measures: SUM_NOTIONAL,
    }),
    scope: { level: 3, parent: ['EMEA', 'Rates'], limit: 501 },
  },
  // THE DETAIL ROWS under a deepest group, as fetchTree asks for them:
  // the group's keys as a filter, no groupBy, the cube's sort on a
  // source column kept and the one on an aggregate-only name dropped.
  detailCase('tree-detail', snap({
    rows: ['region', 'desk'],
    measures: SUM_NOTIONAL,
    sorts: [{ column: 'pnl', direction: 'desc' }],
  }), ['EMEA', 'Rates']),
  // ...and a PIVOTED cube's, which keep the pivot, grouped by every
  // dimension.
  detailCase('tree-detail-pivot', snap({
    rows: ['region', 'desk'],
    pivotOn: ['year'],
    measures: SUM_NOTIONAL,
  }), ['EMEA', 'Rates']),
];

// AD HOC ANALYSIS mode's queries, exactly as the mode plans them: a Time
// dimension zoomed to mixed generations (the top, years, one year's
// quarters) with Geography pinned on the POV -- one query per shape.
const AD_HOC: AdHocCube = {
  snapshot: snap({}),
  outline: {
    dimensions: [
      { name: 'Time', generations: ['year', 'qtr'] },
      { name: 'Geography', generations: ['region', 'desk'] },
    ],
    measures: ['notional', 'qty'],
  },
  measures: [
    { name: 'notional', column: 'notional', fn: 'sum' },
    { name: 'qty', column: 'qty', fn: 'count' },
  ],
};
{
  let g = initialGrid(AD_HOC.outline);
  g = zoomIn(g, 'Time', [], [['2021'], ['2022']]);
  g = zoomIn(g, 'Time', ['2021'], [['2021', 'Q1'], ['2021', 'Q2']]);
  g = setPov(g, 'Geography', ['EMEA', 'Rates']);
  for (const q of planQueries(AD_HOC, g)) {
    CASES.push({ name: `adhoc-shape-${q.key}`, snapshot: q.snapshot,
      ...(q.scope ? { scope: q.scope } : {}) });
  }
}

// WINDOW COLUMNS, at both stages, as the serialiser writes them: every
// function, every over() form (partition + order + frame, order only,
// partition only, neither), at each tree level including the grand
// total, and under a pivot.
{
  const row = (name: string, window: WindowSpec, kind: 'measure' | 'dimension' = 'measure') =>
    ({ name, expression: '', kind, window });
  const grp = (name: string, window: WindowSpec) => ({ name, expression: '', window });
  const WIN_ROW = snap({
    rows: ['region', 'desk'],
    measures: SUM_NOTIONAL,
    derived: [
      row('cum_notional', { fn: 'sum', column: 'notional', partition: ['region'],
        order: [{ column: 'year', direction: 'asc' }, { column: 'qtr', direction: 'asc' }], frame: 'running' }),
      row('moving_avg', { fn: 'average', column: 'pnl', partition: ['desk'],
        order: [{ column: 'year', direction: 'asc' }], frame: { lastRows: 3 } }),
      row('prev_pnl', { fn: 'lag', column: 'pnl', partition: ['book'],
        order: [{ column: 'year', direction: 'asc' }], offset: 2 }),
      row('rank_in_desk', { fn: 'rank', partition: ['region', 'desk'],
        order: [{ column: 'notional', direction: 'desc' }] }, 'dimension'),
      row('global_running', { fn: 'sum', column: 'qty',
        partition: [], order: [{ column: 'year', direction: 'asc' }], frame: 'running' }),
      row('region_total', { fn: 'sum', column: 'notional', partition: ['region'], order: [] }),
      row('table_max', { fn: 'max', column: 'pnl', partition: [], order: [] }),
      row('bucket', { fn: 'ntile', partition: [], order: [{ column: 'pnl', direction: 'asc' }], buckets: 4 }, 'dimension'),
      row('last_notional', { fn: 'last', column: 'notional', partition: ['region'],
        order: [{ column: 'year', direction: 'asc' }] }),
    ],
  });
  for (const level of [0, 1, 2]) {
    CASES.push({ name: `window-row-level-${level}`, snapshot: WIN_ROW,
      scope: { level, parent: level === 2 ? ['EMEA'] : [] } });
  }
  const WIN_GROUP = snap({
    rows: ['region', 'desk'],
    measures: SUM_NOTIONAL,
    groupDerived: [
      grp('running_notional', { fn: 'sum', column: 'notional', partition: [], order: [], frame: 'running' }),
      grp('share_rank', { fn: 'rank', partition: [], order: [{ column: 'notional', direction: 'desc' }] }),
      grp('prev_group', { fn: 'lag', column: 'notional', partition: [], order: [] }),
      grp('desk_running', { fn: 'sum', column: 'notional', partition: ['region'],
        order: [{ column: 'desk', direction: 'asc' }], frame: 'running' }),
      grp('cume', { fn: 'cumeDist', partition: [], order: [{ column: 'notional', direction: 'asc' }] }),
    ],
  });
  for (const level of [0, 1, 2]) {
    CASES.push({ name: `window-group-level-${level}`, snapshot: WIN_GROUP,
      scope: { level, parent: level === 2 ? ['EMEA'] : [] } });
  }
  CASES.push({ name: 'window-row-under-pivot', snapshot: { ...WIN_ROW, pivotOn: ['year'] },
    scope: { level: 1, parent: [] } });
}

/** A detail level, exactly as `fetchTree` builds its query. */
function detailCase(
  name: string,
  cube: CubeSnapshot,
  parent: readonly string[],
): { name: string; snapshot: CubeSnapshot; scope: LevelScope } {
  const snapshot = detailSnapshot(cube, parent);
  return { name, snapshot, scope: { level: snapshot.rows.length, parent: [], limit: 501 } };
}

/** Each case's Pure, as DataCube emits it. */
export function grammars(): { name: string; grammar: string }[] {
  return CASES.map((c) => ({ name: c.name, grammar: serialize(c.snapshot, c.scope) }));
}
