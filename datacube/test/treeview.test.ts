import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import path from 'node:path';
import { after, before, describe, it } from 'node:test';

import { DuckDbEngine, type ArrowishConnection } from '../src/duckdb.ts';
import type { ResultTable } from '../src/result.ts';
import type { CubeSnapshot } from '../src/snapshot.ts';
import { TreeState, flattenTree, requestKey } from '../src/tree.ts';
import type { LevelData } from '../src/treeview.ts';
import { TREE_COLUMN, assemble } from '../src/treeview.ts';
import { NULL_GROUP } from '../src/serialize.ts';

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
    });
    levels.set(requestKey({ level: 1, parent: [] }), {
      request: { level: 1, parent: [] },
      table: table([
        { name: 'region', values: ['AMER', 'EMEA'] },
        { name: '2023__|__total', values: [100, 500] },
      ]),
      paths: [['AMER'], ['EMEA']],
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
    });
    return levels;
  }

  const childrenOf = (levels: Map<string, LevelData>) => (p: never) =>
    levels.get(requestKey({ level: (p as string[]).length + 1, parent: p }))
      ?.paths;

  it('puts every row in display order with its own value', () => {
    const levels = fixture();
    const state = TreeState.empty().expand(['EMEA']);
    const rows = flattenTree(state, 2, childrenOf(levels) as never);
    const t = assemble(SNAPSHOT, rows, levels);

    assert.equal(t.rowCount, 5);
    const total = t.columns.find((c) => c.name === '2023__|__total');
    assert.deepEqual(total?.values, [600, 100, 500, 200, 300]);
  });

  it('uses ONE tree column by default, as DataCube does', () => {
    // groupDisplayType: 'singleColumn'. One column holds whichever
    // dimension belongs to the row, so the grid stays the same width
    // however deep the cube goes.
    const levels = fixture();
    const rows = flattenTree(
      TreeState.empty().expand(['EMEA']),
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
    });
    const rows = flattenTree(TreeState.empty(), 2, childrenOf(levels) as never);
    const tree = assemble(SNAPSHOT, rows, levels).columns.find(
      (c) => c.name === TREE_COLUMN,
    );
    // An internal sentinel must never reach the screen.
    assert.deepEqual(tree?.values, ['Total', null, 'EMEA']);
  });

  it('labels each row only in its own level column, per-dimension', () => {
    const levels = fixture();
    const rows = flattenTree(
      TreeState.empty().expand(['EMEA']),
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
    });
    const rows = flattenTree(
      TreeState.empty().expand(['EMEA']),
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
      TreeState.empty().expand(['EMEA']),
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
    const rows = flattenTree(TreeState.empty(), 2, childrenOf(levels) as never);
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
