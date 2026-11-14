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
  SessionExpired,
  WarehouseEngine,
  listObjects,
  loadWarehouseBinding,
  signIn,
  signInWithKey,
  type Clock,
  type WarehouseSession,
} from '../src/warehouse.ts';
import { unplanned } from './unplanned.ts';

interface Exchange {
  readonly op: string;
  readonly call: { readonly method: string; readonly path: string; readonly headers: Record<string, string>; readonly body: string | null };
  readonly answer: { readonly status: number; readonly body?: string; readonly bodyBase64?: string };
}
interface Scenario {
  readonly name: string;
  readonly action: Record<string, string | number> & { readonly kind: string };
  readonly outcome: Record<string, unknown>;
  readonly exchanges: readonly Exchange[];
}

const BASE = 'https://wh.example';
const corpus = JSON.parse(readFileSync(runfileFromEnv('SQLAPI_CORPUS'), 'utf8')) as { scenarios: readonly Scenario[] };
// Node loads the runtime by file URL and the module by path
const binding = await loadWarehouseBinding(runfileDirUrl('SQLAPI_PAGE'), async (runtimeUrl) => {
  const runtime = await import(runtimeUrl) as { load(src: string, options?: unknown): Promise<unknown> };
  return { load: (src: string, options?: unknown) => runtime.load(fileURLToPath(src), options) };
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
      case 'run': {
        const chunks: string[] = [];
        let receipt: { statementId?: string; serverRows?: number } = {};
        for await (const bytes of engine().arrowChunks(unplanned(String(a.sql)), undefined, (r) => { receipt = r; })) {
          chunks.push(Buffer.from(bytes).toString('base64'));
        }
        return { chunks, statementId: receipt.statementId, rowCount: receipt.serverRows };
      }
      case 'firstChunk': {
        const chunks: string[] = [];
        // the read stops after its first chunk: the driver closes what it leaves
        for await (const bytes of engine().arrowChunks(unplanned(String(a.sql)))) {
          chunks.push(Buffer.from(bytes).toString('base64'));
          break;
        }
        return { chunks };
      }
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
