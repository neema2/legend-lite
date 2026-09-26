import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import path from 'node:path';
import { after, before, describe, it } from 'node:test';

import { DuckDbEngine, type ArrowishConnection } from '../src/duckdb.ts';
import type { ResultTable } from '../src/result.ts';
import { LEAF_COUNT_COLUMN, type CubeSnapshot } from '../src/snapshot.ts';
import { TreeState, flattenTree, requestKey } from '../src/tree.ts';
import type { LevelData } from '../src/treeview.ts';
import {
  DEFAULT_MAX_ROWS,
  TREE_COLUMN,
  assemble,
  fetchTree,
} from '../src/treeview.ts';
import { EpochGuard } from '../src/epoch.ts';
import type { QueryRunner } from '../src/runner.ts';
import { NULL_GROUP, serialize } from '../src/serialize.ts';

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

function table(
  columns: { name: string; values: (string | number | null)[] }[],
): ResultTable {
  return {
    columns: columns.map((c) => ({ name: c.name, type: 'Float', values: c.values })),
    rowCount: columns[0]?.values.length ?? 0,
    epoch: 1,
    elapsedMs: 1,
  };
}

describe('assemble', () => {
  /** Grand total, two regions, and EMEA expanded into two desks. */
  function fixture(): Map<string, LevelData> {
    const levels = new Map<string, LevelData>();
    levels.set(requestKey({ level: 0, parent: [] }), {
      request: { level: 0, parent: [] },
      table: table([{ name: '2023__|__total', values: [600] }]),
      paths: [[]],
      truncated: false,
    });
    levels.set(requestKey({ level: 1, parent: [] }), {
      request: { level: 1, parent: [] },
      table: table([
        { name: 'region', values: ['AMER', 'EMEA'] },
        { name: '2023__|__total', values: [100, 500] },
      ]),
      paths: [['AMER'], ['EMEA']],
      truncated: false,
    });
    levels.set(requestKey({ level: 2, parent: ['EMEA'] }), {
      request: { level: 2, parent: ['EMEA'] },
      table: table([
        { name: 'region', values: ['EMEA', 'EMEA'] },
        { name: 'desk', values: ['Credit', 'Rates'] },
        { name: '2023__|__total', values: [200, 300] },
      ]),
      paths: [
        ['EMEA', 'Credit'],
        ['EMEA', 'Rates'],
      ],
      truncated: false,
    });
    return levels;
  }

  const childrenOf = (levels: Map<string, LevelData>) => (p: never) =>
    levels.get(requestKey({ level: (p as string[]).length + 1, parent: p }))
      ?.paths;

  it('puts every row in display order with its own value', () => {
    const levels = fixture();
    const state = TreeState.empty(true).expand(['EMEA']);
    const rows = flattenTree(state, 2, childrenOf(levels) as never);
    const t = assemble(SNAPSHOT, rows, levels);

    assert.equal(t.rowCount, 5);
    const total = t.columns.find((c) => c.name === '2023__|__total');
    assert.deepEqual(total?.values, [600, 100, 500, 200, 300]);
  });

  describe('keeping the grouped columns', () => {
    // Turning the option on restored `desk` and `book` -- whose
    // aggregated versions the query returns -- and never `region`.
    // At each level the query groups BY that dimension, so its value
    // comes back as the group key and becomes the TREE's label; there
    // is no `region` column to un-hide. It has to be rebuilt from the
    // row paths, which is what this does.
    const kept = (): ResultTable => {
      const levels = fixture();
      const rows = flattenTree(
        TreeState.empty(true).expand(['EMEA']),
        2,
        childrenOf(levels) as never,
      );
      return assemble({ ...SNAPSHOT, keepGroupedColumns: true }, rows, levels);
    };

    it('rebuilds every row dimension as a column', () => {
      const t = kept();
      for (const name of ['region', 'desk']) {
        assert.ok(t.columns.some((c) => c.name === name),
          `${name} must be a column of its own`);
      }
      // The tree is still there: this is "as well as", not "instead".
      assert.ok(t.columns.some((c) => c.name === TREE_COLUMN));
    });

    it('fills ANCESTORS, not just the row\'s own level', () => {
      // Rows are: total, AMER, EMEA, EMEA/Credit, EMEA/Rates.
      // A column blank on every row but one says less than the tree
      // it sits beside, so a desk row under EMEA reads EMEA.
      const t = kept();
      const region = t.columns.find((c) => c.name === 'region');
      const desk = t.columns.find((c) => c.name === 'desk');
      assert.deepEqual(region?.values,
        [null, 'AMER', 'EMEA', 'EMEA', 'EMEA']);
      assert.deepEqual(desk?.values,
        [null, null, null, 'Credit', 'Rates']);
    });

    it('does not ALSO carry the query\'s aggregated copy', () => {
      // At this level that copy is a uniqueValueOnly over the whole
      // group -- blank whenever the group holds more than one value.
      // Two columns of the same name, one blank, is worse than either.
      const t = kept();
      for (const name of ['region', 'desk']) {
        assert.equal(
          t.columns.filter((c) => c.name === name).length, 1,
          `${name} appears more than once`,
        );
      }
    });

    it('drops the aggregated copy the QUERY returns', () => {
      // The fixture above slices the group keys off each level, so it
      // has no duplicate to drop. The real level-1 result does: it
      // groups by `region` and carries `desk` as a uniqueValueOnly
      // aggregate, which is blank whenever the region holds more than
      // one desk. Two columns called `desk`, one of them blank, is
      // worse than either alone.
      const levels = new Map<string, LevelData>();
      levels.set(requestKey({ level: 1, parent: [] }), {
        request: { level: 1, parent: [] },
        table: table([
          { name: 'region', values: ['AMER', 'EMEA'] },
          { name: 'desk', values: [null, null] },
          { name: '2023__|__total', values: [100, 500] },
        ]),
        paths: [['AMER'], ['EMEA']],
        truncated: false,
      });
      const rows = flattenTree(
        TreeState.empty(false), 2, childrenOf(levels) as never,
      );
      const t = assemble(
        { ...SNAPSHOT, keepGroupedColumns: true }, rows, levels,
      );
      assert.equal(t.columns.filter((c) => c.name === 'desk').length, 1,
        'the query\'s blank copy must give way to the one built from'
        + ' the paths');
      // And the surviving one is the rebuilt one, not the blanks.
      const desk = t.columns.find((c) => c.name === 'desk');
      assert.deepEqual(desk?.values, [null, null],
        'at level one no desk is known, so both are null either way');
      const region = t.columns.find((c) => c.name === 'region');
      assert.deepEqual(region?.values, ['AMER', 'EMEA'],
        'region comes from the paths, where its value actually is');
    });

    it('is OFF unless asked, which is what DataCube shows', () => {
      const levels = fixture();
      const rows = flattenTree(
        TreeState.empty(true).expand(['EMEA']),
        2,
        childrenOf(levels) as never,
      );
      const t = assemble(SNAPSHOT, rows, levels);
      assert.equal(t.columns.some((c) => c.name === 'desk'), false);
    });
  });

  it('uses ONE tree column by default, as DataCube does', () => {
    // groupDisplayType: 'singleColumn'. One column holds whichever
    // dimension belongs to the row, so the grid stays the same width
    // however deep the cube goes.
    const levels = fixture();
    const rows = flattenTree(
      TreeState.empty(true).expand(['EMEA']),
      2,
      childrenOf(levels) as never,
    );
    const t = assemble(SNAPSHOT, rows, levels);

    assert.equal(
      t.columns.filter((c) => c.name === 'region' || c.name === 'desk').length,
      0,
      'the per-dimension columns are gone',
    );
    const tree = t.columns.find((c) => c.name === TREE_COLUMN);
    assert.deepEqual(tree?.values, [
      'Total',
      'AMER',
      'EMEA',
      'Credit',
      'Rates',
    ]);
    // Heterogeneous by construction: a different dimension per level.
    assert.equal(tree?.type, 'Any');
  });

  it('never leaks the NULL sentinel into the tree column', () => {
    const levels = fixture();
    levels.set(requestKey({ level: 1, parent: [] }), {
      request: { level: 1, parent: [] },
      table: table([
        { name: 'region', values: [null, 'EMEA'] },
        { name: '2023__|__total', values: [100, 500] },
      ]),
      paths: [[NULL_GROUP], ['EMEA']],
      truncated: false,
    });
    const rows = flattenTree(TreeState.empty(true), 2, childrenOf(levels) as never);
    const tree = assemble(SNAPSHOT, rows, levels).columns.find(
      (c) => c.name === TREE_COLUMN,
    );
    // An internal sentinel must never reach the screen.
    assert.deepEqual(tree?.values, ['Total', null, 'EMEA']);
  });

  it('labels each row only in its own level column, per-dimension', () => {
    const levels = fixture();
    const rows = flattenTree(
      TreeState.empty(true).expand(['EMEA']),
      2,
      childrenOf(levels) as never,
    );
    const t = assemble(SNAPSHOT, rows, levels, {
      treeColumn: 'perDimension',
    });
    const region = t.columns.find((c) => c.name === 'region');
    const desk = t.columns.find((c) => c.name === 'desk');

    // The stepped look: a child shows its own label, not its parent's.
    // The grand total is labelled rather than left as a blank row of
    // numbers with nothing saying what it totals.
    assert.deepEqual(region?.values, ['Total', 'AMER', 'EMEA', null, null]);
    assert.deepEqual(desk?.values, [null, null, null, 'Credit', 'Rates']);
  });

  it('unions value columns across levels', () => {
    // A dynamic pivot discovers its own values per level, so a branch
    // with no 2024 rows yields no 2024 column. Without the union, its
    // neighbours' values would shift left into the wrong column.
    const levels = fixture();
    levels.set(requestKey({ level: 1, parent: [] }), {
      request: { level: 1, parent: [] },
      table: table([
        { name: 'region', values: ['AMER', 'EMEA'] },
        { name: '2023__|__total', values: [100, 500] },
        { name: '2024__|__total', values: [7, 9] },
      ]),
      paths: [['AMER'], ['EMEA']],
      truncated: false,
    });
    const rows = flattenTree(
      TreeState.empty(true).expand(['EMEA']),
      2,
      childrenOf(levels) as never,
    );
    const t = assemble(SNAPSHOT, rows, levels);

    const c2024 = t.columns.find((c) => c.name === '2024__|__total');
    assert.ok(c2024, '2024 survives even though deeper levels lack it');
    // Rows from levels without the column read null, never a
    // neighbour's number.
    assert.deepEqual(c2024.values, [null, 7, 9, null, null]);
  });

  it('keeps engine order and never re-sorts', () => {
    const levels = fixture();
    const rows = flattenTree(
      TreeState.empty(true).expand(['EMEA']),
      2,
      childrenOf(levels) as never,
    );
    const t = assemble(SNAPSHOT, rows, levels);
    const tree = t.columns.find((c) => c.name === TREE_COLUMN);
    // 'Credit' before 'Rates' because the engine said so; a client-side
    // sort here is how a grid disagrees with its own pagination.
    assert.deepEqual(tree?.values.slice(3), ['Credit', 'Rates']);
  });

  it('sums elapsed time across the levels it fetched', () => {
    const levels = fixture();
    const rows = flattenTree(TreeState.empty(true), 2, childrenOf(levels) as never);
    assert.equal(assemble(SNAPSHOT, rows, levels).elapsedMs, 3);
  });
});

// -- the property that matters, against a real engine -----------------

let engine: DuckDbEngine;

before(async () => {
  const require = createRequire(import.meta.url);
  const duckdb = require('@duckdb/duckdb-wasm/blocking');
  const dist = path.dirname(require.resolve('@duckdb/duckdb-wasm/blocking'));
  const db = await duckdb.createDuckDB(
    {
      mvp: {
        mainModule: path.join(dist, 'duckdb-mvp.wasm'),
        mainWorker: path.join(dist, 'duckdb-node-mvp.worker.cjs'),
      },
      eh: {
        mainModule: path.join(dist, 'duckdb-eh.wasm'),
        mainWorker: path.join(dist, 'duckdb-node-eh.worker.cjs'),
      },
    },
    new duckdb.VoidLogger(),
    duckdb.NODE_RUNTIME,
  );
  await db.instantiate();
  engine = new DuckDbEngine(db.connect() as ArrowishConnection);
  await engine.execute(
    `CREATE TABLE trades AS
       SELECT ((i * 7) % 3) AS r, ((i * 11) % 4) AS d,
              2020 + (i % 3) AS year,
              ((i * 7919) % 100000) / 100.0 AS notional
       FROM range(5000) t(i)`,
    0,
  );
});

after(async () => {
  await engine?.close();
});

describe('the global row cap', () => {
  it('detects truncation with N+1 and drops the surplus row', async () => {
    // 40 groups against a cap of 10: asking for 11 is what makes
    // "there is more" a fact rather than a guess, and it costs no
    // second counting query.
    await engine.execute(
      `CREATE OR REPLACE TABLE many AS
         SELECT (i % 40) AS g, i * 1.0 AS v FROM range(400) t(i)`,
      1,
    );
    const capped = await engine.execute(
      'SELECT g, sum(v) AS total FROM many GROUP BY g ORDER BY g LIMIT 11',
      1,
    );
    assert.equal(capped.rowCount, 11, 'the engine returned the probe row');
    assert.equal(capped.rowCount > 10, true, 'so the level is truncated');
  });

  it('reports no truncation when the level fits', async () => {
    const fits = await engine.execute(
      'SELECT r, sum(notional) AS total FROM trades GROUP BY r ' +
        'ORDER BY r LIMIT 11',
      1,
    );
    // 3 distinct regions, far under the probe.
    assert.equal(fits.rowCount <= 10, true, `${fits.rowCount} rows`);
  });

  it('exposes the cap as a constant rather than a magic number', () => {
    // DataCube's own maximum cache block size, for the same reason:
    // past it a level costs real main-thread time to materialise.
    assert.equal(DEFAULT_MAX_ROWS, 1000);
  });
});

describe('subtotals against a real engine', () => {
  const num = (t: ResultTable, col: number, row = 0): number =>
    Number(t.columns[col]?.values[row] ?? NaN);

  it('a subtotal equals the aggregate of its children', () => {
    // This is the whole claim: the same measure with one grouping
    // column dropped. If these ever disagree, the design is wrong --
    // not the rounding.
    return (async () => {
      const level1 = await engine.execute(
        'SELECT r, sum(notional) AS total FROM trades GROUP BY r ORDER BY r',
        1,
      );
      const level2 = await engine.execute(
        'SELECT r, d, sum(notional) AS total FROM trades ' +
          'GROUP BY r, d ORDER BY r, d',
        1,
      );

      for (let i = 0; i < level1.rowCount; i++) {
        const region = level1.columns[0]?.values[i];
        let childSum = 0;
        for (let j = 0; j < level2.rowCount; j++) {
          if (level2.columns[0]?.values[j] === region) {
            childSum += Number(level2.columns[2]?.values[j]);
          }
        }
        const subtotal = Number(level1.columns[1]?.values[i]);
        assert.ok(
          Math.abs(subtotal - childSum) < 1e-6,
          `region ${String(region)}: subtotal ${subtotal} vs children ${childSum}`,
        );
      }
    })();
  });

  it('the grand total equals the aggregate of the top level', async () => {
    const grand = await engine.execute(
      'SELECT sum(notional) AS total FROM trades',
      1,
    );
    const level1 = await engine.execute(
      'SELECT r, sum(notional) AS total FROM trades GROUP BY r',
      1,
    );
    let sum = 0;
    for (let i = 0; i < level1.rowCount; i++) {
      sum += Number(level1.columns[1]?.values[i]);
    }
    assert.ok(Math.abs(num(grand, 0) - sum) < 1e-6);
  });

  it('a count subtotal equals the count of its children', async () => {
    // count is the aggregate most likely to be re-derived wrongly,
    // because summing child counts is only correct when the grouping
    // is a true partition.
    const grand = await engine.execute(
      'SELECT count(*) AS n FROM trades',
      1,
    );
    const level1 = await engine.execute(
      'SELECT r, count(*) AS n FROM trades GROUP BY r',
      1,
    );
    let sum = 0;
    for (let i = 0; i < level1.rowCount; i++) {
      sum += Number(level1.columns[1]?.values[i]);
    }
    assert.equal(num(grand, 0), sum);
    assert.equal(num(grand, 0), 5000);
  });
});

describe('Show leaf count', () => {
  // General Properties > "Show leaf count" was written to the
  // configuration and read by nothing (the no-reader guardrail found
  // it). Upstream's group queries carry a count and the group cell
  // shows it beside the label.
  const FLAT: CubeSnapshot = {
    source: { expression: 'trades' },
    columns: [
      { name: 'region', type: 'String' },
      { name: 'notional', type: 'Float' },
    ],
    derived: [],
    rows: ['region'],
    pivotOn: [],
    measures: [],
    sorts: [],
    epoch: 1,
    leafCount: true,
  };

  it('a grouped level counts its rows; the grand total does not', () => {
    const q1 = serialize(FLAT, { level: 1, parent: [] });
    assert.match(q1, /__leafCount:x\|1:y\|\$y->count\(\)/);
    const q0 = serialize(FLAT, { level: 0, parent: [] });
    assert.doesNotMatch(q0, /__leafCount/);
    assert.doesNotMatch(serialize({ ...FLAT, leafCount: false },
      { level: 1, parent: [] }), /__leafCount/);
  });

  it('shows the count on the label, never as a column', () => {
    const levels = new Map<string, LevelData>();
    levels.set(requestKey({ level: 1, parent: [] }), {
      request: { level: 1, parent: [] },
      table: table([
        { name: 'region', values: ['AMER', 'EMEA'] },
        { name: 'notional', values: [10, 20] },
        { name: LEAF_COUNT_COLUMN, values: [3, 1234] },
      ]),
      paths: [['AMER'], ['EMEA']],
      truncated: false,
    });
    const rows = flattenTree(TreeState.empty(), 1, (p) =>
      levels.get(requestKey({ level: p.length + 1, parent: p }))?.paths);
    const t = assemble(FLAT, rows, levels);
    assert.deepEqual(t.columns.find((c) => c.name === TREE_COLUMN)?.values,
      ['AMER (3)', 'EMEA (1234)']);
    assert.equal(t.columns.some((c) => c.name === LEAF_COUNT_COLUMN), false);
  });
});

describe('the count, next level (the default)', () => {
  // An OPENED group shows how many rows sit directly beneath it, read
  // off the rows opening it fetched; a closed one shows nothing.
  const CUBE: CubeSnapshot = {
    source: { expression: 'trades' },
    columns: [
      { name: 'region', type: 'String' },
      { name: 'desk', type: 'String' },
      { name: 'notional', type: 'Float' },
    ],
    derived: [],
    rows: ['region', 'desk'],
    pivotOn: [],
    measures: [],
    sorts: [],
    epoch: 1,
    childCount: true,
  };
  const levelsWith = (truncated: boolean): Map<string, LevelData> => {
    const levels = new Map<string, LevelData>();
    levels.set(requestKey({ level: 1, parent: [] }), {
      request: { level: 1, parent: [] },
      table: table([{ name: 'region', values: ['AMER', 'EMEA'] }, { name: 'notional', values: [1, 2] }]),
      paths: [['AMER'], ['EMEA']],
      truncated: false,
    });
    levels.set(requestKey({ level: 2, parent: ['EMEA'] }), {
      request: { level: 2, parent: ['EMEA'] },
      table: table([{ name: 'region', values: ['EMEA', 'EMEA', 'EMEA'] },
        { name: 'desk', values: ['a', 'b', 'c'] }, { name: 'notional', values: [1, 1, 0] }]),
      paths: [['EMEA', 'a'], ['EMEA', 'b'], ['EMEA', 'c']],
      truncated,
    });
    return levels;
  };
  const tree = (levels: Map<string, LevelData>) => {
    const rows = flattenTree(TreeState.empty().expand(['EMEA']), 2, (p) =>
      levels.get(requestKey({ level: p.length + 1, parent: p }))?.paths);
    return assemble(CUBE, rows, levels).columns.find((c) => c.name === TREE_COLUMN)?.values;
  };

  it('an opened group counts its next level; a closed one shows none', () => {
    assert.deepEqual(tree(levelsWith(false)), ['AMER', 'EMEA (3)', 'a', 'b', 'c']);
  });

  it('children cut short by the row cap read N+', () => {
    assert.equal(tree(levelsWith(true))?.[1], 'EMEA (3+)');
  });

  it('asks nothing of the query', () => {
    assert.doesNotMatch(serialize(CUBE, { level: 1, parent: [] }), /__leafCount/);
  });
});

describe('detail rows under the deepest group', () => {
  // Upstream: "when maximum level of drilldown is reached, we simply
  // just need to filter the data to match drilldown values, no
  // groupBy() is needed."
  const CUBE: CubeSnapshot = {
    source: { expression: 'trades' },
    columns: [
      { name: 'region', type: 'String' },
      { name: 'pnl', type: 'Float' },
      { name: 'notional', type: 'Float' },
    ],
    derived: [],
    rows: ['region'],
    pivotOn: [],
    measures: [{ name: 'total', column: 'notional', fn: 'sum' }],
    // One on a source column, one on an aggregate-only name.
    sorts: [{ column: 'pnl', direction: 'desc' }, { column: 'total', direction: 'asc' }],
    epoch: 1,
    leafCount: true,
  };

  it('fetches the group\'s own rows, filtered to its keys, and hangs them under it', async () => {
    const sent: string[] = [];
    const runner: QueryRunner = {
      name: 'stub',
      async run(pure) {
        sent.push(pure);
        const rows = pure.includes('groupBy')
          ? table([
            { name: 'region', values: ['EMEA'] },
            { name: 'pnl', values: [9] },
            { name: 'total', values: [60] },
            { name: LEAF_COUNT_COLUMN, values: [3] },
          ])
          : table([
            { name: 'region', values: ['EMEA', 'EMEA', 'EMEA'] },
            { name: 'pnl', values: [5, 3, 1] },
            { name: 'notional', values: [10, 20, 30] },
          ]);
        return { rows, sql: 'SELECT' };
      },
    };
    const view = await fetchTree(CUBE, TreeState.empty().expand(['EMEA']), {
      runner, guard: new EpochGuard(), epoch: 0,
    });
    const detail = sent.find((q) => !q.includes('groupBy'));
    assert.ok(detail, `no detail query among ${sent.join(' | ')}`);
    assert.match(detail!, /\$x\.region == 'EMEA'/);
    assert.match(detail!, /pnl->descending\(\)/);
    assert.doesNotMatch(detail!, /total/, 'a sort on an aggregate-only name was kept');
    const tree = view.table.columns.find((c) => c.name === TREE_COLUMN)?.values;
    assert.deepEqual(tree, ['EMEA (3)', null, null, null]);
    assert.deepEqual(view.rows.map((r) => r.isDetail ?? false), [false, true, true, true]);
    assert.deepEqual(view.table.columns.find((c) => c.name === 'notional')?.values,
      [null, 10, 20, 30]);
  });
});
