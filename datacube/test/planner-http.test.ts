// planner.test.ts's tests of UpstreamPlanner over a stubbed fetch (its HTTP). The others are in planner.test.ts,
// which runs in the headless lane too (docs/DATACUBE_HEADLESS_PYTHON_DESIGN_2026_10_11.md §6).

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
  PlanError,
  UpstreamPlanner,
} from '../src/planner.ts';

import { element, fn, fromElement, lambda, toJson } from '../../pure-protocol/src/index.ts';
import { inRun } from '../../engine-client/src/run-frame.ts';

/** This test reads a JSON text loosely, its fields probed as it goes: since the C final audit's H5 (2026-11-12)
 *  JSON.parse is `unknown` everywhere (engine-client/src/json-unknown.ts), so the product reads none unchecked. */
const looseJson = (text: string): any => JSON.parse(text);

/** A plan as legend-engine's generatePlan shapes it: the SQL in a nested sql node. */
const PLAN = {
  _type: 'simple',
  rootExecutionNode: {
    _type: 'relationalTdsInstantiation',
    executionNodes: [{ _type: 'sql', sqlQuery: 'SELECT 1', connection: { type: 'DuckDB' }, executionNodes: [] }],
    resultType: {
      _type: 'tds',
      tdsColumns: [
        { name: 'region', type: 'meta::pure::precisePrimitives::Varchar' },
        { name: 'total', type: 'Float' },
      ],
    },
  },
};

interface Seen { readonly url: string; readonly body: string; readonly type: string }

/** Answers each upstream call; records what was sent. */
function fakeServer(seen: Seen[], answer: (path: string) => { status?: number; json: unknown }): typeof fetch {
  return (async (url: string, init?: RequestInit) => {
    const headers = (init?.headers ?? {}) as Record<string, string>;
    seen.push({ url, body: String(init?.body ?? ''), type: headers['Content-Type'] ?? '' });
    const { status = 200, json } = answer(new URL(url).pathname);
    return new Response(JSON.stringify(json), { status });
  }) as unknown as typeof fetch;
}

const ok = (path: string) => ({ json: path.endsWith('/lambda') ? looseJson(toJson(Q)) : PLAN });

/** A cube query, as DataCube builds one. */
const Q = fromElement('demo::T').select(['a']).lambda();

const OTHER = fromElement('demo::T').select(['b']).lambda();

describe('UpstreamPlanner inside a run (the final audit of 9af16fbe58\'s T2)', () => {
  it('refuses plan and relationType inside a run of the caller\'s flow, before anything is sent', async () => {
    const seen: Seen[] = [];
    const p = new UpstreamPlanner({ baseUrl: 'http://localhost:9999/', model: 'Class demo::Person {}',
      runtime: 'demo::RT', fetch: fakeServer(seen, ok) });
    await assert.rejects(inRun("a test's run", () => p.plan(Q)), /the planner was asked 'plan' inside a run/);
    await assert.rejects(inRun("a test's run", () => p.relationType(Q)),
      /the planner was asked 'relationType' inside a run/);
    assert.equal(seen.length, 0, 'nothing sent inside the run');
  });
});

describe('UpstreamPlanner: legend-engine\'s own API, the same client for lite and the engine', () => {
  it('sends the query to generatePlan with its runtime, and returns the plan\'s SQL', async () => {
    const seen: Seen[] = [];
    const p = new UpstreamPlanner({
      baseUrl: 'http://localhost:9999/',
      model: 'Class demo::Person {}',
      runtime: 'demo::RT',
      fetch: fakeServer(seen, ok),
    });
    assert.deepEqual(await p.plan(Q), {
      sql: 'SELECT 1',
      // the plan's own tdsColumns, both vocabularies read as one
      columns: [{ name: 'region', type: 'String' }, { name: 'total', type: 'Float' }],
    });
    // The query is already protocol: one call, nothing parsed first.
    assert.equal(seen.length, 1);
    assert.equal(seen[0]!.url, 'http://localhost:9999/api/pure/v1/execution/generatePlan');
    assert.equal(seen[0]!.type, 'application/json');
    const input = looseJson(seen[0]!.body);
    assert.deepEqual(input.model, { _type: 'text', code: 'Class demo::Person {}' });
    assert.deepEqual(input.function, looseJson(toJson(lambda([], fn('from', Q.body[0]!, element('demo::RT'))))));
  });

  it('caches, because planning the same query is pure', async () => {
    const seen: Seen[] = [];
    const p = new UpstreamPlanner({ baseUrl: 'http://x', model: 'm', runtime: 'r', fetch: fakeServer(seen, ok) });
    await p.plan(Q);
    await p.plan(fromElement('demo::T').select(['a']).lambda());
    await p.plan(OTHER);
    assert.equal(seen.length, 2, 'one call per new query; the equal repeat was served from cache');
    assert.equal(p.cacheSize, 2);
  });

  it('surfaces the compiler message verbatim, from the engine\'s error JSON', async () => {
    const p = new UpstreamPlanner({
      baseUrl: 'http://x',
      model: 'm',
      runtime: 'r',
      fetch: fakeServer([], (path) => (path.endsWith('/generatePlan')
        ? { status: 400, json: { code: -1, errorType: 'COMPILATION', message: "Column 'nope' not found", status: 'error' } }
        : { json: {} })),
    });
    await assert.rejects(
      () => p.plan(Q),
      (e: unknown) => {
        assert.ok(e instanceof PlanError);
        assert.match((e as Error).message, /Column 'nope' not found/);
        assert.equal((e as PlanError).subject, Q);
        return true;
      },
    );
  });

  it('types a query compile-only through lambdaRelationType, the engine\'s own call', async () => {
    const seen: Seen[] = [];
    const p = new UpstreamPlanner({
      baseUrl: 'http://x', model: 'm', runtime: 'r',
      fetch: fakeServer(seen, () => ({
        json: {
          _type: 'relationType',
          columns: [{ name: 'amount', genericType: { rawType: { _type: 'packageableType',
            fullPath: 'meta::pure::precisePrimitives::Numeric' } }, multiplicity: { lowerBound: 0, upperBound: 1 } }],
        },
      })),
    });
    assert.deepEqual(await p.relationType(Q), [{ name: 'amount', type: 'Decimal' }]);
    assert.equal(new URL(seen[0]!.url).pathname, '/api/pure/v1/compilation/lambdaRelationType');
    assert.deepEqual(looseJson(seen[0]!.body).model, { _type: 'text', code: 'm' });
  });

  it('parses what a person typed through grammarToJson (E1), and prints a query through jsonToGrammar (E4)', async () => {
    const seen: Seen[] = [];
    const p = new UpstreamPlanner({
      baseUrl: 'http://x', model: 'm', runtime: 'r',
      fetch: (async (url: string, init?: RequestInit) => {
        const headers = (init?.headers ?? {}) as Record<string, string>;
        seen.push({ url, body: String(init?.body ?? ''), type: headers['Content-Type'] ?? '' });
        return new Response(new URL(url).pathname.includes('jsonToGrammar')
          ? 'demo::T->select(~[a])' : toJson(Q), { status: 200 });
      }) as unknown as typeof fetch,
    });
    assert.deepEqual(await p.parse('|demo::T->select(~[a])'), Q);
    assert.equal(new URL(seen[0]!.url).pathname, '/api/pure/v1/grammar/grammarToJson/lambda');
    assert.equal(seen[0]!.type, 'text/plain');
    assert.equal(seen[0]!.body, '|demo::T->select(~[a])');
    assert.equal(await p.print(Q), 'demo::T->select(~[a])');
    const printed = new URL(seen[1]!.url);
    assert.equal(printed.pathname, '/api/pure/v1/grammar/jsonToGrammar/lambda');
    assert.equal(printed.searchParams.get('renderStyle'), 'PRETTY');
    assert.deepEqual(looseJson(seen[1]!.body), looseJson(toJson(Q)));
  });
});

describe('UpstreamPlanner.withModel: another source on the page, the same server', () => {
  it('plans over its own model and runtime; the first planner keeps its own', async () => {
    const seen: Seen[] = [];
    const first = new UpstreamPlanner({ baseUrl: 'http://x', model: 'model A', runtime: 'a::RT', fetch: fakeServer(seen, ok) });
    const second = first.withModel('model B', 'b::RT');
    await second.plan(Q);
    await first.plan(Q);
    const bodies = seen.filter((s) => s.url.endsWith('/generatePlan')).map((s) => looseJson(s.body) as { model: { code: string } });
    assert.deepEqual(bodies.map((b) => b.model.code), ['model B', 'model A']);
  });
});
