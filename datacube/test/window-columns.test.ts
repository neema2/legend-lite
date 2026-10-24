// Window columns, as the compiler prints them: every function in the `over()` form the
// planner takes (the WASM differential plans these same shapes on both
// planners), and how a group-level window follows the tree's levels.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { extendWindow, levelLambda } from '../src/query.ts';
import { showAsColumns, SHOW_AS_LABELS } from '../src/calc-presets.ts';
import { candleMeasures, chartQuery, DEFAULT_OPTIONS, type ShowAsKind } from '../src/chart-spec.ts';
import {
  renameColumnReferences, WINDOW_FUNCTIONS, type QuerySnapshot, type WindowFrame, type WindowSpec,
} from '../src/snapshot.ts';
import { element, from } from '../../pure-protocol/src/index.ts';
import { plannerFor } from './catalog-builder.ts';
import { print, printLevel, source } from './lite-compiler.ts';

/** A window column over `t`, as the compiler prints the query that extends it. */
function windowExtend(name: string, w: WindowSpec): Promise<string> {
  return print(extendWindow(from(element('t')), name, w).lambda());
}

const base = { partition: [] as string[], order: [] as { column: string; direction: 'asc' | 'desc' }[] };

describe('window columns as Pure', () => {
  it('a running sum: partition, order, frame', async () => {
    assert.equal((await windowExtend('cum', { ...base, fn: 'sum', column: 'notional',
      partition: ['region', 'desk'], order: [{ column: 'year', direction: 'asc' }], frame: 'running' })),
    '|t->extend(over(~[region, desk], [~year->ascending()], unbounded()->rows(0)), ~[cum:{p, w, r|$r.notional}:y|$y->plus()])');
  });

  it('with no partition, the order alone -- or, to carry a frame, one constant partition (engine has no unpartitioned frame, S24)', async () => {
    assert.equal((await windowExtend('rn', { ...base, fn: 'rowNumber', order: [{ column: 'pnl', direction: 'desc' }] })),
      '|t->extend(over([~pnl->descending()]), ~[rn:{p, w, r|$p->rowNumber($r)}])');
    assert.equal((await windowExtend('m', { ...base, fn: 'average', column: 'pnl',
      order: [{ column: 'year', direction: 'asc' }], frame: { lastRows: 3 } })),
    '|t->extend(~[__all__m:x|\'all\'])->extend(over(~[__all__m], [~year->ascending()], (-2)->rows(0)), ~[m:{p, w, r|$r.pnl}:y|$y->average()])');
  });

  it('a whole-table aggregate: every row, whatever the order', async () => {
    assert.equal((await windowExtend('t', { ...base, fn: 'max', column: 'pnl' })),
      '|t->extend(~[__all__t:x|\'all\'])->extend(over(~[__all__t], [~pnl->ascending()], unbounded()->rows(unbounded())), ~[t:{p, w, r|$r.pnl}:y|$y->max()])');
  });

  it('ranking takes no frame; lag reads its column n rows back; last reads the whole partition', async () => {
    const o = [{ column: 'year', direction: 'asc' as const }];
    assert.equal((await windowExtend('r', { ...base, fn: 'rank', order: o, frame: 'running' })),
      '|t->extend(over([~year->ascending()]), ~[r:{p, w, r|$p->rank($w, $r)}])');
    assert.equal((await windowExtend('p', { ...base, fn: 'lag', column: 'pnl', partition: ['book'], order: o, offset: 2 })),
      '|t->extend(over(~[book], [~year->ascending()]), ~[p:{p, w, r|$p->lag($r, 2).pnl}])');
    assert.equal((await windowExtend('n', { ...base, fn: 'lead', column: 'pnl', order: o })),
      '|t->extend(over([~year->ascending()]), ~[n:{p, w, r|$p->lead($r).pnl}])');
    assert.equal((await windowExtend('l', { ...base, fn: 'last', column: 'pnl', partition: ['book'], order: o })),
      '|t->extend(over(~[book], [~year->ascending()], unbounded()->rows(unbounded())), ~[l:{p, w, r|$p->last($w, $r).pnl}])');
    assert.equal((await windowExtend('b', { ...base, fn: 'ntile', order: o, buckets: 10 })),
      '|t->extend(over([~year->ascending()]), ~[b:{p, w, r|$p->ntile($r, 10)}])');
  });

  it('refuses what cannot mean anything', () => {
    assert.throws(() => windowExtend('r', { ...base, fn: 'rank' }), /needs an order/);
    assert.throws(() => windowExtend('s', { ...base, fn: 'sum', order: [{ column: 'year', direction: 'asc' }] }),
      /needs a column/);
  });
});

describe('a group-level window follows the tree', () => {
  const CUBE: QuerySnapshot = {
    source: { query: element('t') },
    columns: [
      { name: 'region', type: 'String' }, { name: 'desk', type: 'String' },
      { name: 'notional', type: 'Float' },
    ],
    derived: [],
    rows: ['region', 'desk'],
    pivotOn: [],
    measures: [{ name: 'notional', column: 'notional', fn: 'sum' }],
    sorts: [],
    epoch: 1,
    groupDerived: [{ name: 'running', window: {
      fn: 'sum', column: 'notional', partition: ['region'], order: [], frame: 'running' } as WindowSpec }],
  };

  it('at a level, an empty order is the level\'s own; a dimension it lacks leaves the partition', async () => {
    // Level 1 has region: each region is its own partition there.
    const q1 = (await printLevel(CUBE, { level: 1, parent: [] }));
    assert.match(q1, /extend\(over\(~\[region\], \[~region->ascending\(\)\], unbounded\(\)->rows\(0\)\), ~\[running:/);
    // A partition on a dimension the level lacks drops out.
    const byDesk = { ...CUBE, groupDerived: [{ ...CUBE.groupDerived![0]!, window: {
      ...CUBE.groupDerived![0]!.window!, partition: ['desk'] } }] };
    assert.match((await printLevel(byDesk, { level: 1, parent: [] })),
      /extend\(~\[__all__running:x\|'all'\]\)->extend\(over\(~\[__all__running\], \[~region->ascending\(\)\], unbounded\(\)->rows\(0\)\)/);
    const q2 = (await printLevel(CUBE, { level: 2, parent: ['EMEA'] }));
    assert.match(q2, /extend\(over\(~\[region\], \[~region->ascending\(\), ~desk->ascending\(\)\], unbounded\(\)->rows\(0\)\)/);
  });

  it('follows the grid\'s own sort when it has one', async () => {
    const sorted = { ...CUBE, sorts: [{ column: 'notional', direction: 'desc' as const }] };
    assert.match((await printLevel(sorted, { level: 1, parent: [] })),
      /over\(~\[region\], \[~notional->descending\(\), ~region->ascending\(\)\], unbounded\(\)->rows\(0\)\)/);
  });

  it('the grand total orders by its one group', async () => {
    assert.match((await printLevel(CUBE, { level: 0, parent: [] })), /over\(~\[__all__running\], \[~__root__->ascending\(\)\], unbounded\(\)->rows\(0\)\)/);
  });

  it('a row-level window is written as its extend, before the filter', async () => {
    const row: QuerySnapshot = { ...CUBE, groupDerived: [], derived: [{ name: 'rk', window: {
      fn: 'rank', partition: ['region'], order: [{ column: 'notional', direction: 'desc' }] } }],
      filter: { kind: 'condition', column: 'desk', operator: 'equal', value: 'Rates' } };
    const q = (await printLevel(row, { level: 1, parent: [] }));
    assert.ok(q.indexOf('rank(') < q.indexOf('filter('), q);
    assert.equal((await windowExtend('rk', row.derived[0]!.window!)),
      '|t->extend(over(~[region], [~notional->descending()]), ~[rk:{p, w, r|$p->rank($w, $r)}])');
  });

  it('a rename reaches the columns a window names', () => {
    const renamed = renameColumnReferences(CUBE, 'notional', 'amount');
    assert.equal(renamed.groupDerived?.[0]?.window?.column, 'amount');
  });
});

// ON DEEPHAVEN (docs/DEEPHAVEN_TRADER_DEMOS_2026_10_24.md K6): every window DataCube writes -- each function with each
// frame it takes, each show-as on a cube's level -- plans for a Deephaven runtime as the server's table operations,
// refused by none; their rows are DuckDB's on the pinned server (test/deephaven/page.ts).
const DEEPHAVEN = `###Relational
Database risk::Ref
(
  Table trades (id INTEGER PRIMARY KEY, region VARCHAR(16), desk VARCHAR(16), year INTEGER, notional DOUBLE, pnl DOUBLE)
  Table prints (seq BIGINT PRIMARY KEY, sym VARCHAR(8) NOT NULL, ts TIMESTAMP NOT NULL, px DOUBLE NOT NULL, sz DOUBLE NOT NULL)
)
###Connection
RelationalDatabaseConnection risk::Conn
{
  store: risk::Ref;
  type: Deephaven;
  specification: Deephaven { host: '127.0.0.1'; port: 8443; useTls: true; };
  auth: PreSharedKey { keyVaultReference: 'deephaven.psk'; };
}
###Runtime
Runtime risk::RT
{
  mappings: [];
  connections: [ risk::Ref: [ c: risk::Conn ] ];
}`;

describe('every window DataCube writes plans on Deephaven', () => {
  const planner = plannerFor(DEEPHAVEN, 'risk::RT');
  const planned = async (query: Parameters<typeof planner.plan>[0], what: string, refused: string[]): Promise<void> => {
    try {
      const plan = await planner.plan(query);
      if (!plan.sql.startsWith('{"ops": [')) refused.push(`${what}: not Deephaven's operations: ${plan.sql}`);
    } catch (e) {
      refused.push(`${what}: ${String(e)}`);
    }
  };

  it('each window function, with each frame it takes', async () => {
    const trades = from(await source('#>{risk::Ref.trades}#'));
    const refused: string[] = [];
    for (const meta of WINDOW_FUNCTIONS) {
      const frames: readonly (WindowFrame | undefined)[] = meta.framed
        ? [undefined, 'running', 'partition', { lastRows: 3 }] : [undefined];
      for (const frame of frames) {
        const w: WindowSpec = {
          fn: meta.fn, partition: ['desk'], order: [{ column: 'year', direction: 'asc' }, { column: 'id', direction: 'asc' }],
          ...(meta.column ? { column: 'pnl' } : {}), ...(frame ? { frame } : {}),
          ...(meta.fn === 'lag' ? { offset: 2 } : {}), ...(meta.fn === 'ntile' ? { buckets: 3 } : {}),
        };
        await planned(extendWindow(trades, 'w', w).lambda(), `${meta.fn} ${JSON.stringify(frame)}`, refused);
      }
    }
    assert.equal(WINDOW_FUNCTIONS.length, 15);
    assert.deepEqual(refused, []);
  });

  it('each show-as, on a cube\'s levels', async () => {
    const refused: string[] = [];
    const kinds = Object.keys(SHOW_AS_LABELS) as ShowAsKind[];
    for (const kind of kinds) {
      const cube: QuerySnapshot = {
        source: { query: await source('#>{risk::Ref.trades}#') },
        columns: [
          { name: 'id', type: 'Integer' }, { name: 'region', type: 'String' }, { name: 'desk', type: 'String' },
          { name: 'year', type: 'Integer' }, { name: 'notional', type: 'Float' }, { name: 'pnl', type: 'Float' },
        ],
        derived: [],
        rows: ['region', 'desk'],
        pivotOn: [],
        measures: [{ name: 'notional', column: 'notional', fn: 'sum' }],
        sorts: [],
        epoch: 1,
        groupDerived: showAsColumns({ kind }, 'notional', ['desk'], ['region']),
      };
      for (const level of [1, 2]) {
        await planned(levelLambda(cube, { level, parent: level === 2 ? ['EMEA'] : [] }), `${kind} at level ${level}`,
          refused);
      }
    }
    assert.equal(kinds.length, 6);
    assert.deepEqual(refused, []);
  });
});

describe('a candlestick plans on Deephaven (K3)', () => {
  it('a name\'s candles by 5 minutes: its open and close by the print\'s number, its high, low and volume', async () => {
    const planner = plannerFor(DEEPHAVEN, 'risk::RT');
    const tape: QuerySnapshot = {
      source: { query: await source('#>{risk::Ref.prints}#') },
      columns: [{ name: 'seq', type: 'Integer', kind: 'dimension' }, { name: 'sym', type: 'String' },
        { name: 'ts', type: 'DateTime' }, { name: 'px', type: 'Float' }, { name: 'sz', type: 'Float' }],
      derived: [], rows: ['sym'], pivotOn: [], sorts: [], epoch: 1,
      filter: { kind: 'condition', column: 'sym', operator: 'equal', value: 'AAPL' },
    };
    const { query } = chartQuery(tape, { version: 2, mark: 'candlestick', x: 'ts', grain: 'minute5',
      y: candleMeasures('px', 'seq', 'sz'), options: { ...DEFAULT_OPTIONS, sort: { by: 'x', direction: 'asc' } } });
    const plan = await planner.plan(query);
    // the server's own: the bucket its lowerBin, the open and close its sortedFirst and sortedLast by the number
    assert.ok(plan.sql.startsWith('{"ops": ['), plan.sql);
    assert.match(plan.sql, /lowerBin\(/);
    assert.match(plan.sql, /"sortedFirst": \{"columns": \[\{"columnName": "[^"]*seq"\}\]\}/);
    assert.match(plan.sql, /"sortedLast": \{"columns": \[\{"columnName": "[^"]*seq"\}\]\}/);
  });
});

