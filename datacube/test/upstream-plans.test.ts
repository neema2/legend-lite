// A SERVER PLANNING FOR A PAGE (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md G1): the tab's planner on a server writes each
// request of test/fixtures/upstream-plans.json exactly, and reads each plan there as its SQL and columns. (Python's
// twin went with the remote planner on Python, 2026-10-15: a host plans in process, the program's §1.1.)

import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { describe, it } from 'node:test';
import { PureV1Client } from '../../engine-client/src/pure-v1.ts';
import { pureType } from '../../engine-client/src/relation-type.ts';
import { fromJson, readLambda, toJson } from '../../pure-protocol/src/index.ts';
import { PlanError, serverPlan } from '../src/planner.ts';
import { runfileFromEnv } from '../../tools/js/runfiles.mts';

interface Corpus {
  readonly requests: readonly { name: string; query: unknown; model: string; runtime: string; text: string }[];
  readonly refused: readonly { name: string; query: unknown }[];
  readonly plans: readonly { name: string; answer: unknown; database: string; sql: string; columns: readonly [string, string][] }[];
  readonly refusals: readonly { name: string; answer: unknown; message: string }[];
}

// read with the protocol's exact JSON: a decimal keeps its digits, as each request must
const CORPUS = fromJson(readFileSync(runfileFromEnv('UPSTREAM_PLANS'), 'utf8')) as Corpus;

/** The client, its one call's body caught and answered with `answer` (an empty plan unless told), `status` (200). */
function caught(model: string, runtime: string, answer = '{}', status = 200): { client: PureV1Client; bodies: string[] } {
  const bodies: string[] = [];
  const fetch = (async (_url: string, init: RequestInit) => {
    bodies.push(String(init.body));
    return new Response(answer, { status, headers: { 'Content-Type': 'application/json' } });
  }) as typeof globalThis.fetch;
  return {
    client: new PureV1Client({ baseUrl: 'http://server.invalid', model, runtime, fetch }, (m, subject) => new PlanError(m, subject)),
    bodies,
  };
}

describe('a server planning for a page: the requests', () => {
  for (const c of CORPUS.requests) {
    it(`${c.name}: generatePlan's body is the case's, exactly`, async () => {
      const { client, bodies } = caught(c.model, c.runtime);
      await client.generatePlan(readLambda(c.query));
      assert.equal(bodies.length, 1);
      // the bytes the tab's planner on a server sends, exactly
      assert.equal(bodies[0], c.text);
    });
  }
  for (const c of CORPUS.refused) {
    it(`${c.name}: refused before anything is sent`, () => {
      const { client, bodies } = caught('', 'x::R');
      assert.throws(() => client.fromRuntime(readLambda(c.query)), /one relation expression with no parameters/);
      assert.equal(bodies.length, 0);
    });
  }
});

describe('a server planning for a page: the plans', () => {
  const query = readLambda(CORPUS.requests[0]!.query);
  for (const c of CORPUS.plans) {
    it(`${c.name}: read as its SQL and its columns`, () => {
      const plan = serverPlan(c.answer, query, new Set(), c.database);
      assert.equal(plan.sql, c.sql);
      // the page reads each type by legend-lite's lattice (relation-type.ts); the fixture carries each column's SQL
      // type too (sqlTypes), which the tab's columns do not yet (G9)
      assert.deepEqual(plan.columns.map((col) => [col.name, col.type]), c.columns.map(([n, t]) => [n, pureType(t)]));
    });
  }
  for (const c of CORPUS.refusals) {
    it(`${c.name}: refused in the server's words`, async () => {
      const refused = (e: unknown): boolean => e instanceof PlanError && e.message === c.message;
      const isPlan = c.answer !== null && typeof c.answer === 'object' && 'rootExecutionNode' in c.answer;
      if (isPlan) {
        // a plan the tab does not run: refused by its one reader
        assert.throws(() => serverPlan(c.answer, query), refused);
      } else {
        // legend-engine's error shape, as it answers a refusal (not 200): the client says the server's own words
        const first = CORPUS.requests[0]!;
        const { client } = caught(first.model, first.runtime, toJson(c.answer), 500);
        await assert.rejects(client.generatePlan(query), refused);
      }
    });
  }
});
