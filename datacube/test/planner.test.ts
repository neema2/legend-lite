import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
  LegendLitePlanner,
  PlanError,
} from '../src/planner.ts';
import type { CubeSnapshot } from '../src/snapshot.ts';

const SNAPSHOT = {
  source: { expression: '$trades' },
  columns: [],
  derived: [],
  rows: [],
  pivotOn: [],
  measures: [],
  sorts: [],
  epoch: 1,
} satisfies CubeSnapshot;

function fakeFetch(
  handler: (body: unknown) => { status?: number; json: unknown },
): typeof fetch {
  return (async (_url: string, init?: RequestInit) => {
    const body = JSON.parse(String(init?.body ?? '{}'));
    const { status = 200, json } = handler(body);
    return {
      ok: status >= 200 && status < 300,
      status,
      json: async () => json,
    } as Response;
  }) as unknown as typeof fetch;
}

describe('LegendLitePlanner', () => {
  it('posts model plus grammar and returns the SQL', async () => {
    let seen: { code?: string; runtime?: string } = {};
    const p = new LegendLitePlanner({
      baseUrl: 'http://localhost:9999/',
      model: 'Class demo::Person {}',
      runtime: 'demo::RT',
      fetch: fakeFetch((body) => {
        seen = body as typeof seen;
        return { json: { success: true, sql: 'SELECT 1' } };
      }),
    });

    const sql = await p.plan('$trades->select(~[a])', SNAPSHOT);
    assert.equal(sql, 'SELECT 1');
    assert.match(seen.code ?? '', /Class demo::Person/);
    assert.match(seen.code ?? '', /\$trades->select/);
    assert.equal(seen.runtime, 'demo::RT');
  });

  it('caches, because planning the same grammar is pure', async () => {
    let calls = 0;
    const p = new LegendLitePlanner({
      baseUrl: 'http://x',
      model: 'm',
      runtime: 'r',
      fetch: fakeFetch(() => {
        calls += 1;
        return { json: { sql: 'SELECT 1' } };
      }),
    });
    await p.plan('g', SNAPSHOT);
    await p.plan('g', SNAPSHOT);
    await p.plan('other', SNAPSHOT);
    assert.equal(calls, 2, 'the repeat was served from cache');
    assert.equal(p.cacheSize, 2);
  });

  it('surfaces the compiler message verbatim', async () => {
    const p = new LegendLitePlanner({
      baseUrl: 'http://x',
      model: 'm',
      runtime: 'r',
      fetch: fakeFetch(() => ({
        status: 500,
        json: { error: "Column 'nope' not found" },
      })),
    });
    await assert.rejects(
      () => p.plan('bad grammar', SNAPSHOT),
      (e: unknown) => {
        assert.ok(e instanceof PlanError);
        // The compiler's own words, not a friendlier vaguer wrapper.
        assert.match((e as Error).message, /Column 'nope' not found/);
        assert.equal((e as PlanError).grammar, 'bad grammar');
        return true;
      },
    );
  });

  it('reports an unreachable planner distinctly from a rejected plan', async () => {
    const p = new LegendLitePlanner({
      baseUrl: 'http://x',
      model: 'm',
      runtime: 'r',
      fetch: (() => Promise.reject(new Error('ECONNREFUSED'))) as never,
    });
    await assert.rejects(() => p.plan('g', SNAPSHOT), /could not reach the planner/);
  });
});

