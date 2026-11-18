// cancel.test.ts's test of the planner client over a stubbed fetch (its HTTP). The others are in cancel.test.ts,
// which runs in the headless lane too (docs/DATACUBE_HEADLESS_PYTHON_DESIGN_2026_10_11.md §6).

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { UpstreamPlanner } from '../src/edge/upstream-planner.ts';
import { someQuery } from './fake-planner.ts';

describe('the planner client', () => {
  it('passes the signal to fetch', async () => {
    let sawSignal: AbortSignal | undefined;
    const planner = new UpstreamPlanner({
      baseUrl: 'http://example',
      model: 'model',
      runtime: 'rt',
      cache: false,
      fetch: (async (_url: string, init?: RequestInit) => {
        sawSignal = init?.signal ?? undefined;
        const plan = {
          rootExecutionNode: {
            _type: 'relationalTdsInstantiation',
            executionNodes: [{ _type: 'sql', sqlQuery: 'SELECT 1', connection: { type: 'DuckDB' }, executionNodes: [] }],
            resultType: { _type: 'tds', tdsColumns: [{ name: 'one', type: 'Integer' }] },
          },
        };
        return new Response(JSON.stringify(plan), { status: 200 });
      }) as unknown as typeof fetch,
    });
    await planner.plan(someQuery('grammar'));
    assert.ok(sawSignal === undefined, 'no signal passed, none forwarded');

    const ac = new AbortController();
    await planner.plan(someQuery('grammar2'), ac.signal);
    assert.equal(sawSignal, ac.signal, 'the caller signal reaches fetch');
  });
});
