// Run on (src/run-on.ts; docs/ONE_CONNECTION_DESK_2026_10_28.md D2): a page's saved queries over a data space run on
// the execution context named -- legend's executionKey on each one -- and only those; what a page runs on, read back.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { runOnEntries, runningOnChoice } from '../src/app/run-on-menu.ts';
import type { PageDocument } from '../src/page-document.ts';
import { runOn, runningOn } from '../src/run-on.ts';
import type { DataSpaceRead } from '../src/saved-queries.ts';

const query = (name: string, executionContext: unknown): unknown => ({
  id: name,
  cube: {
    source: {
      _type: 'savedQuery', name, columns: [],
      query: { name, groupId: 'g', artifactId: 'a', versionId: 'v', content: '|1', executionContext },
    },
  },
});

/** A page of four grids: two over the desk's data space (one on its default), one over another, one explicit. */
const page = {
  cubes: [
    query('watchlist', { _type: 'dataSpaceExecutionContext', dataSpacePath: 'market::TradingDesk' }),
    query('tape', { _type: 'dataSpaceExecutionContext', dataSpacePath: 'market::TradingDesk', executionKey: 'In this tab' }),
    query('other', { _type: 'dataSpaceExecutionContext', dataSpacePath: 'risk::Book', executionKey: 'Prod' }),
    query('pinned', { _type: 'explicitExecutionContext', mapping: 'm::M', runtime: 'r::R' }),
  ],
} as unknown as PageDocument;

const contextOf = (p: PageDocument, i: number): unknown => {
  const source = p.cubes[i]!.cube.source;
  assert.equal(source._type, 'savedQuery');
  return source._type === 'savedQuery' ? source.query.executionContext : undefined;
};

describe('run on', () => {
  it("writes the context on every saved query over the data space, and on nothing else", () => {
    const on = runOn(page, 'market::TradingDesk', 'Deephaven');
    assert.deepEqual(contextOf(on, 0), { _type: 'dataSpaceExecutionContext', dataSpacePath: 'market::TradingDesk', executionKey: 'Deephaven' });
    assert.deepEqual(contextOf(on, 1), { _type: 'dataSpaceExecutionContext', dataSpacePath: 'market::TradingDesk', executionKey: 'Deephaven' });
    assert.deepEqual(contextOf(on, 2), contextOf(page, 2));
    assert.deepEqual(contextOf(on, 3), contextOf(page, 3));
    // the page given is not changed
    assert.deepEqual(contextOf(page, 0), { _type: 'dataSpaceExecutionContext', dataSpacePath: 'market::TradingDesk' });
  });

  it('says what each data space runs on: its default as null, two contexts as both', () => {
    assert.deepEqual(runningOn(page), new Map([
      ['market::TradingDesk', new Set([null, 'In this tab'])],
      ['risk::Book', new Set(['Prod'])],
    ]));
    assert.deepEqual(runningOn(runOn(page, 'market::TradingDesk', 'Deephaven')).get('market::TradingDesk'), new Set(['Deephaven']));
  });

  it('refuses a context with no name', () => {
    assert.throws(() => runOn(page, 'market::TradingDesk', ''), /an execution context of market::TradingDesk is named/);
  });
});

/** The desk's data space as a saved query's read gives it (saved-queries.ts DataSpaceRead). */
const DESK: DataSpaceRead = {
  path: 'market::TradingDesk', title: 'Trading desk', context: 'In this tab', defaultContext: 'In this tab',
  contexts: [{ name: 'In this tab', title: 'In this tab (DuckDB)' }, { name: 'Deephaven', title: 'Deephaven (localhost:10000)' }],
};

describe('Run On ▸', () => {
  it("offers the data space's contexts, the one its queries run on checked; another chosen, the page runs on it", () => {
    const desk = runOn(page, 'market::TradingDesk', 'In this tab');
    const { items, choices } = runOnEntries(desk, new Map([['market::TradingDesk', DESK]]));
    // one data space read here (risk::Book's queries were never read: nothing offered for it)
    assert.equal(items.length, 1);
    assert.equal(items[0]!.label, 'Run On');
    assert.deepEqual(items[0]!.submenu?.map((i) => [i.id, i.label, i.checked ?? false]), [
      ['host.runOn.0', 'In this tab (DuckDB)', true],
      ['host.runOn.1', 'Deephaven (localhost:10000)', false],
    ]);
    const moved = runningOnChoice(desk, choices[1]!);
    assert.deepEqual(runningOn(moved).get('market::TradingDesk'), new Set(['Deephaven']));
  });

  it("a query naming no context runs on the data space's default, and is checked so", () => {
    const { items } = runOnEntries(page, new Map([['market::TradingDesk', DESK]]));
    // the page's two desk queries: one on the default, one on 'In this tab' -- both the same context
    assert.deepEqual(items[0]!.submenu?.map((i) => i.checked ?? false), [true, false]);
  });
});
