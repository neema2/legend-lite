// A grid's right-click Join ▸ (src/app/join-menu.ts; the user, 2026-10-23): as its Filter ▸ is the value clicked, its
// quick joins are the column clicked -- one to each grid with that column, the other ways of each, the window on it --
// and on no column, the window alone.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { JOIN_SOURCE, JOIN_WINDOW, joinMenu } from '../src/app/join-menu.ts';
import type { MenuItem } from '../src/ui/menu.ts';

const cols = (...c: [string, string][]) => c.map(([name, type]) => ({ name, type }));
const MINE = { name: 'Orders', columns: cols(['country', 'String'], ['customer_id', 'Integer'], ['units', 'Integer']) };
const OTHERS = [
  { id: 'grid-2', name: 'Countries', columns: cols(['code', 'String'], ['country', 'String']) },
  { id: 'grid-3', name: 'customers', columns: cols(['id', 'Integer'], ['name', 'String']) },
];
const labels = (items: readonly MenuItem[] | undefined) =>
  (items ?? []).map((i) => `${i.separated ? '| ' : ''}${i.label}${i.disabled ? ' (off)' : ''}`);
const COUNTRY = { left: 'country', op: '==', right: 'country' } as const;

describe("a grid's right-click Join ▸, as its Filter ▸", () => {
  it('on a column: a quick join to each grid with it, its other ways, the window on it, a data source', () => {
    const { items, asks } = joinMenu({ column: 'country', mine: MINE, others: OTHERS, source: true });
    assert.deepEqual(labels(items), ['With Countries on country', 'Other Ways', '| Join…', 'Join a Data Source…']);
    assert.deepEqual(labels(items[1]!.submenu), [
      'Matching only, with Countries on country', 'Mine with a match in Countries on country', 'Mine with none in Countries on country',
    ]);
    assert.deepEqual(asks.get('host.join.with.0'), { their: 'grid-2', row: COUNTRY, quick: 'left' });
    assert.deepEqual(asks.get('host.join.way.0.semi'), { their: 'grid-2', row: COUNTRY, quick: 'semi' });
    assert.deepEqual(asks.get(JOIN_WINDOW), { their: 'grid-2', row: COUNTRY }, 'the window at that grid, on that key');
    assert.equal(asks.has(JOIN_SOURCE), false, 'a data source asks nothing: its grid is picked first');
  });

  it("on a column their id under their name keys: customers' id", () => {
    const { items, asks } = joinMenu({ column: 'customer_id', mine: MINE, others: OTHERS, source: false });
    assert.deepEqual(labels(items).slice(0, 1), ['With customers on customer_id = id']);
    assert.deepEqual(asks.get('host.join.with.0'), { their: 'grid-3', row: { left: 'customer_id', op: '==', right: 'id' }, quick: 'left' });
  });

  it('on a column no other grid has: said so, and the window', () => {
    const { items, asks } = joinMenu({ column: 'units', mine: MINE, others: OTHERS, source: true });
    assert.deepEqual(labels(items), ['No other grid has a column units (off)', '| Join…', 'Join a Data Source…']);
    assert.deepEqual(asks.get(JOIN_WINDOW), {});
  });

  it('on no column: the window alone -- no quick join, as Filter ▸ offers no value', () => {
    const { items, asks } = joinMenu({ column: undefined, mine: MINE, others: OTHERS, source: true });
    assert.deepEqual(labels(items), ['Join…', 'Join a Data Source…']);
    assert.deepEqual([...asks.keys()], [JOIN_WINDOW]);
  });

  it('no other grid: the window off; no source picker: no Join a Data Source…', () => {
    assert.deepEqual(labels(joinMenu({ column: 'country', mine: MINE, others: [], source: false }).items),
      ['No other grid on the page to join (off)', '| Join… (off)']);
    assert.deepEqual(labels(joinMenu({ column: undefined, mine: MINE, others: [], source: false }).items), ['Join… (off)']);
    // saying why, as the status bar's Join says it greyed
    assert.equal(joinMenu({ column: undefined, mine: MINE, others: [], source: false }).items[0]?.hint,
      'There is no other grid on this page to join');
  });
});
