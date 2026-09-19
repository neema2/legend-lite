import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
  LegendLitePlanner,
  PlanError,
  SnapRewritingPlanner,
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

describe('SnapRewritingPlanner', () => {
  const inner = {
    plan: async (_g: string) =>
      'SELECT t0.FIRST_NAME AS name FROM PERSON AS t0 ORDER BY t0.FIRST_NAME NULLS LAST',
  };

  it('leaves SQL alone when live', async () => {
    const p = new SnapRewritingPlanner(inner, 'PERSON', () => null);
    assert.match(await p.plan('g', SNAPSHOT), /FROM PERSON AS t0/);
  });

  it('swaps only the base table when snapped', async () => {
    const p = new SnapRewritingPlanner(inner, 'PERSON', () => 'dc_snap_1');
    const sql = await p.plan('g', SNAPSHOT);
    assert.match(sql, /FROM "dc_snap_1" AS t0/);
    // The same query otherwise: snapped and live differ only in source.
    assert.match(sql, /ORDER BY t0\.FIRST_NAME NULLS LAST/);
  });

  it('does not rewrite the name inside a column or literal', async () => {
    const chatty = {
      plan: async () =>
        "SELECT PERSON_ID, 'PERSON' AS label FROM PERSON WHERE x = 1",
    };
    const p = new SnapRewritingPlanner(chatty, 'PERSON', () => 'snap1');
    const sql = await p.plan('g', SNAPSHOT);
    assert.match(sql, /SELECT PERSON_ID/, 'column name untouched');
    assert.match(sql, /'PERSON' AS label/, 'string literal untouched');
    assert.match(sql, /FROM "snap1" WHERE/, 'only the FROM target swapped');
  });

  it('rewrites a JOIN target too', async () => {
    const joined = {
      plan: async () => 'SELECT * FROM A JOIN PERSON ON A.id = PERSON.id',
    };
    const p = new SnapRewritingPlanner(joined, 'PERSON', () => 'snap2');
    const sql = await p.plan('g', SNAPSHOT);
    assert.match(sql, /JOIN "snap2" ON/);
  });
});
