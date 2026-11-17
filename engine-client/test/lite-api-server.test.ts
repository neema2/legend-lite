// LITE API V1'S CONFORMANCE SUITE, THE SERVER BINDING (docs/records/lite-api-v1-2026-11-17.md §5, the user's go of
// 2026-11-17): each case of test/fixtures/lite-api-cases.json -- DataCube's suite, the one file every binding reads --
// asked of legend-lite's server (`//core:server`, started for this file) as one POST to `/api/lite/v1/<op>`, and its
// answer held to the case's: an exact answer equal, an included one held, a refusal its kind, in lite's one shape,
// answered with its kind's status (§3). The browser's operations (core's LiteApiConformanceTest) and Python's native
// entries are held to the same file by their own readers, so the three bindings cannot drift.

import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { describe, it } from 'node:test';

import { runfileFromEnv } from '../../tools/js/runfiles.mts';
import { liteServer } from '../../query-store/test/lite-server.ts';
import { parseExact } from '../src/values.ts';
import { liteError, type LiteErrorKind } from '../src/lite-error.ts';

const server = liteServer();

interface Case {
  readonly name: string;
  readonly op: string;
  readonly request: unknown;
  readonly answer?: unknown;
  readonly answerIncludes?: unknown;
  readonly refusal?: { readonly kind: LiteErrorKind };
}

/** The status a refusal of each kind is answered with over HTTP (spec §3). */
const STATUS: Readonly<Record<LiteErrorKind, number>> = {
  parse: 400, compile: 400, unsupported: 400, request: 400, parameters: 400, data: 422, internal: 500,
};

/** Whether `actual` holds `expected`, as the Java reader asks it: each key it names equal, recursively; each array item
 *  it names held by some item of the actual array; any other value equal. */
function includes(expected: unknown, actual: unknown): boolean {
  if (Array.isArray(expected)) {
    return Array.isArray(actual) && expected.every((e) => actual.some((a) => includes(e, a)));
  }
  if (expected !== null && typeof expected === 'object') {
    if (actual === null || typeof actual !== 'object' || Array.isArray(actual)) return false;
    const a = actual as Record<string, unknown>;
    return Object.entries(expected as Record<string, unknown>).every(([k, v]) => k in a && includes(v, a[k]));
  }
  return expected === actual;
}

// the cases, their numbers exact (2^63-1 a bigint, as the answers are read)
const fixture = parseExact(readFileSync(runfileFromEnv('LITE_API_CASES'), 'utf8')) as { readonly cases: readonly Case[] };

describe("lite API v1's conformance suite over legend-lite's server", () => {
  it('holds cases', () => {
    assert.ok(fixture.cases.length > 0, 'the suite holds cases');
  });

  for (const c of fixture.cases) {
    it(c.name, async () => {
      // the request as written in the file, its numbers exact
      const body = JSON.stringify(c.request, (_k, v: unknown) => (typeof v === 'bigint' ? `__big__${v}` : v))
        .replace(/"__big__(-?\d+)"/g, '$1');
      const res = await fetch(`${server.api()}/lite/v1/${c.op}`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body,
      });
      const text = await res.text();
      if (c.refusal !== undefined) {
        // a refusal case names its kind alone (§3: the kind is the contract, the words the product's), answered with
        // its kind's status and in lite's one shape
        const refusal = liteError(JSON.parse(text));
        assert.equal(refusal.kind, c.refusal.kind, text);
        assert.equal(res.status, STATUS[c.refusal.kind], text);
        assert.ok(includes(c.refusal, refusal), text);
        return;
      }
      assert.equal(res.status, 200, text);
      const answer = parseExact(text);
      if (c.answer !== undefined) assert.deepEqual(answer, c.answer, text);
      else assert.ok(includes(c.answerIncludes, answer), text);
    });
  }
});
