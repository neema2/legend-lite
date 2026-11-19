// The tab's warehouse driver held to the one corpus (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md C3): each recorded
// scenario's action run against the recorded answers, the driver making exactly the recorded calls -- the ones the
// binding names (warehouse/src/test/resources/sqlapi-corpus.json; SqlApiCallsTest holds the binding to them, and
// python/tests/test_warehouse.py holds Python's driver to the same).

import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { after, describe, it } from 'node:test';
import { fileURLToPath } from 'node:url';

import { runfileDirUrl, runfileFromEnv } from '../../tools/js/runfiles.mts';
import {
  BINDING_OPTIONS,
  SessionExpired,
  WarehouseBinding,
  WarehouseEngine,
  listObjects,
  signIn,
  signInWithKey,
  type Clock,
  type SqlApiModule,
  type WarehouseSession,
} from '../src/warehouse.ts';
import * as driver from '../src/warehouse.ts';
import { loadWarehouseBinding } from '../src/doors/warehouse.ts';
import { unplanned } from './unplanned.ts';
import { inRun } from '../src/run-frame.ts';

interface Exchange {
  readonly op: string;
  readonly call: { readonly method: string; readonly path: string; readonly headers: Record<string, string>; readonly body: string | null };
  readonly answer: { readonly status: number; readonly body?: string; readonly bodyBase64?: string };
}
interface Scenario {
  readonly name: string;
  readonly action: Record<string, string | number> & {
    readonly kind: string;
    /** A run's values for its `?`s, in order (since 2026-11-17: the API's parameters). */
    readonly parameters?: readonly (string | number | bigint | boolean | null)[];
  };
  readonly outcome: Record<string, unknown>;
  readonly exchanges: readonly Exchange[];
}

const BASE = 'https://wh.example';
// a recorded parameter's whole number is an Integer's, handed to the driver as a bigint (plan-runner.ts checkParameters:
// a number is a Float's alone), read exactly from its source text -- which JSON.parse alone loses (3 and 3.0 parse alike):
// an array's whole-number elements are seen first (their source kept by index), its key after them
const wholeElements = new WeakMap<object, Map<string, string>>();
const corpus = JSON.parse(readFileSync(runfileFromEnv('SQLAPI_CORPUS'), 'utf8'),
  function (this: unknown, key: string, v: unknown, context?: { readonly source?: string }): unknown {
    if (typeof v === 'number' && Array.isArray(this) && context?.source !== undefined && /^-?\d+$/.test(context.source)) {
      const at = wholeElements.get(this) ?? new Map<string, string>();
      at.set(key, context.source);
      wholeElements.set(this, at);
    }
    if (key === 'parameters' && Array.isArray(v)) {
      const at = wholeElements.get(v);
      return at === undefined ? v : v.map((p: unknown, i) => (at.has(String(i)) ? BigInt(at.get(String(i))!) : p));
    }
    return v;
  }) as { scenarios: readonly Scenario[] };
// Node loads the runtime by file URL and the module by path
const binding = await (async (): Promise<WarehouseBinding> => {
  const dir = runfileDirUrl('SQLAPI_PAGE');
  const runtime = await import(new URL('wasm-gc-module-runtime.js', dir).href) as {
    load(src: string, options?: unknown): Promise<SqlApiModule>;
  };
  return new WarehouseBinding(await runtime.load(fileURLToPath(new URL('classes.wasm', dir)), BINDING_OPTIONS));
})();
// the binding is loaded by its door, from its directory alone: no caller hands it the module it runs (the C5-T last
// round's T3, 2026-11-15), and the run side loads no module at all -- it takes the loaded binding (2026-11-15, the C5-T
// fold-in: the run layers hold no computed specifier). Node's tests, here, load the module themselves and make the
// binding of it
describe('the binding, loaded by its door', () => {
  it('from its directory alone: it takes no loader', () => {
    assert.equal(loadWarehouseBinding.length, 1);
  });
  it('the run side has no load of its own', () => {
    assert.equal('loadWarehouseBinding' in driver, false);
  });
});

// a clock whose refresh never comes: these scenarios name their calls
const still: Clock = { now: () => 0, setTimeout: () => undefined, clearTimeout: () => undefined };
const session = (token: string): WarehouseSession => ({ baseUrl: BASE, token, principal: 'rita', expiresAt: '2099-01-01T00:00:00Z' });

const realFetch = globalThis.fetch;
after(() => { globalThis.fetch = realFetch; });

/** The corpus's answers, in order; each request held to its recorded call. */
function replay(s: Scenario): { readonly done: () => void } {
  let next = 0;
  globalThis.fetch = (async (input: string | URL | Request, init?: RequestInit) => {
    const e = s.exchanges[next++];
    assert.ok(e, `${s.name}: a call past the recording: ${String(input)}`);
    assert.deepEqual({
      method: init?.method, url: String(input), headers: init?.headers, body: init?.body ?? null,
    }, { method: e.call.method, url: BASE + e.call.path, headers: e.call.headers, body: e.call.body }, `${s.name}: ${e.op}`);
    const body = e.answer.bodyBase64 !== undefined ? Buffer.from(e.answer.bodyBase64, 'base64') : e.answer.body ?? '';
    return new Response(body, { status: e.answer.status });
  }) as typeof fetch;
  return { done: () => assert.equal(next, s.exchanges.length, `${s.name}: every recorded call made`) };
}

async function act(s: Scenario): Promise<Record<string, unknown>> {
  const a = s.action;
  // the catalog's type reaches no recorded call (it says what a host's own statement is written for: Snap's count)
  const engine = (): WarehouseEngine =>
    new WarehouseEngine(binding, session(String(a.token)), String(a.catalog ?? 'main'), 'DuckDB', still);
  try {
    switch (a.kind) {
      case 'signIn': {
        const t = await signIn(binding, BASE, String(a.user), String(a.password));
        return { principal: t.principal, token: t.token };
      }
      case 'signInWithKey': {
        const t = await signInWithKey(binding, BASE, String(a.key));
        return { principal: t.principal, token: t.token };
      }
      case 'objects':
        return { objects: await listObjects(binding, session(String(a.token))) };
      // the test's own statements, each sent in its own run (run-frame.ts: on Node a statement is sent inside one)
      case 'run': return await inRun("a test's own statement", async () => {
        const chunks: string[] = [];
        let receipt: { statementId?: string; serverRows?: number } = {};
        // a statement's values, bound at run by the server (the API's parameters, the orchestrator's ruling (a))
        const params = a.parameters ?? [];
        for await (const bytes of engine().arrowChunks(unplanned(String(a.sql)), undefined, (r) => { receipt = r; }, params)) {
          chunks.push(Buffer.from(bytes).toString('base64'));
        }
        return { chunks, statementId: receipt.statementId, rowCount: receipt.serverRows };
      });
      case 'firstChunk': return await inRun("a test's own statement", async () => {
        const chunks: string[] = [];
        // the read stops after its first chunk: the driver closes what it leaves
        for await (const bytes of engine().arrowChunks(unplanned(String(a.sql)))) {
          chunks.push(Buffer.from(bytes).toString('base64'));
          break;
        }
        return { chunks };
      });
      case 'refreshThenCheck': {
        // the new token is what the next call carries: the replay holds that call to the recorded one
        const e = engine();
        await e.refreshToken();
        return { principal: e.principal, said: await e.check('s1') };
      }
      case 'history':
        return { said: await engine().check('s1') };
      default:
        throw new Error(`no such action: ${a.kind}`);
    }
  } catch (error: unknown) {
    if (error instanceof SessionExpired) return { expired: true };
    return { failed: error instanceof Error ? error.message : String(error) };
  }
}

describe('the tab\'s warehouse driver makes the corpus\'s calls', () => {
  for (const s of corpus.scenarios) {
    it(s.name, async () => {
      const replayed = replay(s);
      const got = await act(s);
      replayed.done();
      const want = s.outcome;
      if ('failed' in want) {
        assert.ok(typeof got.failed === 'string' && got.failed.endsWith(String(want.failed)), `${String(got.failed)}`);
      } else if (s.action.kind === 'refreshThenCheck') {
        assert.deepEqual(got, { principal: want.principal, said: want.said });
      } else if (s.action.kind === 'history') {
        assert.match(String(got.said), /^On the warehouse's record for rita: statement s1, succeeded, 3 rows/);
      } else {
        assert.deepEqual(got, want);
      }
    });
  }
});
