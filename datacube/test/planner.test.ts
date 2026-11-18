import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { UpstreamPlanner } from '../src/edge/upstream-planner.ts';
import { fromElement } from '../../pure-protocol/src/index.ts';

/** A cube query, as DataCube builds one. */
const Q = fromElement('demo::T').select(['a']).lambda();

describe('UpstreamPlanner: legend-engine\'s own API, the same client for lite and the engine', () => {
  it('reports an unreachable planner distinctly from a rejected plan', async () => {
    const p = new UpstreamPlanner({
      baseUrl: 'http://x',
      model: 'm',
      runtime: 'r',
      fetch: (() => Promise.reject(new Error('ECONNREFUSED'))) as never,
    });
    await assert.rejects(() => p.plan(Q), /could not reach the server at http:\/\/x/);
  });
});

