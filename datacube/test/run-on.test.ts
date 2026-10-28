// A page on a context (src/run-on.ts; docs/ONE_CONNECTION_DESK_2026_10_28.md D2): a page's saved queries over a data
// space written on the execution context named -- legend's executionKey on each one -- and only those; and Open With ▸
// (src/app/open-with-menu.ts): a saved page opened again, in a tab of its own, on another context.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { openWithAddress, openWithEntries } from '../src/app/open-with-menu.ts';
import type { PageDocument } from '../src/page-document.ts';
import { runOn } from '../src/run-on.ts';
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

  it('refuses a context with no name', () => {
    assert.throws(() => runOn(page, 'market::TradingDesk', ''), /an execution context of market::TradingDesk is named/);
  });
});

/** The desk's data space as a saved query's read gives it (saved-queries.ts DataSpaceRead). */
const DESK: DataSpaceRead = {
  path: 'market::TradingDesk', title: 'Trading desk', context: 'In this tab', defaultContext: 'In this tab',
  contexts: [{ name: 'In this tab', title: 'In this tab (DuckDB)' }, { name: 'Deephaven', title: 'Deephaven (localhost:10000)' }],
};

describe('Open With ▸', () => {
  it("offers the data space's contexts, the one this tab reads it on checked; each opens the saved page on it", () => {
    const { items, choices } = openWithEntries(new Map([['market::TradingDesk', DESK]]));
    assert.equal(items.length, 1);
    assert.equal(items[0]!.label, 'Open With');
    assert.deepEqual(items[0]!.submenu?.map((i) => [i.id, i.label, i.checked ?? false]), [
      ['host.openWith.0', 'In this tab (DuckDB)', true],
      ['host.openWith.1', 'Deephaven (localhost:10000)', false],
    ]);
    // the saved page's own address, its fragment the opening's: the page's id and the context, by name
    assert.equal(openWithAddress('https://site.example/datacube/demo/index.html?planner=local#p1.old', 'page 7', choices[1]!),
      'https://site.example/datacube/demo/index.html?planner=local#open=page%207&space=market%3A%3ATradingDesk&context=Deephaven');
  });

  it('names each data space where the page read several', () => {
    const book: DataSpaceRead = { path: 'risk::Book', title: 'Risk book', context: 'Prod', defaultContext: 'Prod', contexts: [{ name: 'Prod', title: 'Prod' }] };
    const { items, choices } = openWithEntries(new Map([['market::TradingDesk', DESK], ['risk::Book', book]]));
    assert.deepEqual(items.map((i) => i.label), ['Open Trading desk With', 'Open Risk book With']);
    assert.deepEqual(choices.map((c) => `${c.dataSpace} ${c.context}`), ['market::TradingDesk In this tab', 'market::TradingDesk Deephaven', 'risk::Book Prod']);
  });
});
