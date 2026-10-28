// Run on (src/run-on.ts; docs/ONE_CONNECTION_DESK_2026_10_28.md D2): a page's saved queries over a data space run on
// the execution context named -- legend's executionKey on each one -- and only those; what a page runs on, read back.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import type { PageDocument } from '../src/page-document.ts';
import { runOn, runningOn } from '../src/run-on.ts';

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
