// warehouse-session.test.ts's tests of the warehouse sign-in over a stubbed fetch (its HTTP). The others are in
// warehouse-session.test.ts, which runs in the headless lane too (docs/DATACUBE_HEADLESS_PYTHON_DESIGN_2026_10_11.md §6).

import assert from 'node:assert/strict';
import { afterEach, beforeEach, describe, it } from 'node:test';

import { fileURLToPath } from 'node:url';

import { runfileDirUrl } from '../../tools/js/runfiles.mts';
import { WarehouseEngine, connect, loadWarehouseBinding, type WarehouseSession } from '../../engine-client/src/warehouse.ts';
import { unplanned } from '../../engine-client/test/unplanned.ts';

// the one binding's module (//warehouse:sqlapi_page): Node loads the runtime by file URL and the module by path
const binding = await loadWarehouseBinding(runfileDirUrl('SQLAPI_PAGE'), async (runtimeUrl) => {
  const runtime = await import(runtimeUrl) as { load(src: string, options?: unknown): Promise<unknown> };
  return { load: (src: string, options?: unknown) => runtime.load(fileURLToPath(src), options) };
});

const session = (token: string, principal = 'alice'): WarehouseSession =>
  ({ baseUrl: 'https://wh.example', token, principal, expiresAt: '2099-01-01T00:00:00Z' });

let sent: { url: string; auth: string | null }[];

let reply: (url: string) => Response;

const realFetch = globalThis.fetch;

beforeEach(() => {
  sent = [];
  // every statement fails at once: one request each, whose token is what these tests read
  reply = () => new Response(JSON.stringify({ statementId: 's', state: 'failed',
    error: { code: 'TEST', message: 'answered by the test' } }), { status: 200 });
  globalThis.fetch = (async (input: string | URL | Request, init?: RequestInit) => {
    const url = String(input);
    const headers = new Headers(init?.headers);
    sent.push({ url, auth: headers.get('Authorization') });
    return reply(url);
  }) as typeof fetch;
});

afterEach(() => {
  globalThis.fetch = realFetch;
});

describe('a renewed sign-in reaches the open cube (P2-297)', () => {
  it('after renew, the engine sends the NEW token', async () => {
    const engine = new WarehouseEngine(binding, session('tok1'), 'main', 'DuckDB');
    engine.renew(session('tok2'));
    await engine.run(unplanned('SELECT 1'), 1).catch(() => {});
    assert.ok(sent.length > 0, 'a request was sent');
    assert.ok(sent.every((r) => r.auth === 'Bearer tok2'), sent.map((r) => r.auth).join(', '));
  });
});

describe('signing in is one step with its table list (P2-334)', () => {
  it('a listing that fails after a good sign-in gives no session at all', async () => {
    reply = (url) => (url.endsWith('/login')
      ? new Response(JSON.stringify({ token: 't', expiresAt: 'x', principal: 'bob' }), { status: 200 })
      : new Response('upstream down', { status: 502 }));
    await assert.rejects(() => connect(binding, 'https://wh.example', 'bob', 'pw'), /could not list the tables/);
  });

  it('a good sign-in and listing give the session WITH its own tables', async () => {
    reply = (url) => (url.endsWith('/login')
      ? new Response(JSON.stringify({ token: 't', expiresAt: 'x', principal: 'bob' }), { status: 200 })
      : url.endsWith('/sql/v1/objects')
        ? new Response(JSON.stringify([{ catalog: 'main', databaseType: 'DuckDB', schema: 's', name: 'bobs', kind: 'table', columns: [] }]), { status: 200 })
        : new Response('not this route', { status: 404 }));
    const got = await connect(binding, 'https://wh.example', 'bob', 'pw');
    assert.equal(got.session.principal, 'bob');
    // one listing call: every catalog's objects, each with its catalog and database type
    assert.deepEqual(got.objects.map((o) => [o.catalog, o.databaseType, o.name]), [['main', 'DuckDB', 'bobs']]);
  });
});
