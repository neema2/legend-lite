// Leg B / B6: the warehouse session on the host page (docs/DATACUBE_LEG_B_STATE_OWNER_2026_09_28.md).
// A signed-in session renewed must reach the cube already open (P2-297); signing in is one step
// with its table list, so a failed listing never leaves one user's tables beside another's
// session (P2-334).

import assert from 'node:assert/strict';
import { afterEach, beforeEach, describe, it } from 'node:test';

import { WarehouseEngine, connect, type WarehouseSession } from '../../engine-client/src/warehouse.ts';

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
    const engine = new WarehouseEngine(session('tok1'));
    engine.renew(session('tok2'));
    await engine.run('SELECT 1', 1).catch(() => {});
    assert.ok(sent.length > 0, 'a request was sent');
    assert.ok(sent.every((r) => r.auth === 'Bearer tok2'), sent.map((r) => r.auth).join(', '));
  });

  it('a sign-in as SOMEONE ELSE never becomes the open cube\'s: it is refused, said', () => {
    const engine = new WarehouseEngine(session('tok1', 'alice'));
    assert.throws(() => engine.renew(session('tok9', 'bob')), /alice/);
    assert.equal(engine.principal, 'alice');
  });
});

describe('signing in is one step with its table list (P2-334)', () => {
  it('a listing that fails after a good sign-in gives no session at all', async () => {
    reply = (url) => (url.endsWith('/login')
      ? new Response(JSON.stringify({ token: 't', expiresAt: 'x', principal: 'bob' }), { status: 200 })
      : new Response('upstream down', { status: 502 }));
    await assert.rejects(() => connect('https://wh.example', 'bob', 'pw'), /could not list the tables/);
  });

  it('a good sign-in and listing give the session WITH its own tables', async () => {
    reply = (url) => (url.endsWith('/login')
      ? new Response(JSON.stringify({ token: 't', expiresAt: 'x', principal: 'bob' }), { status: 200 })
      : url.endsWith('/sql/v1/objects')
        ? new Response(JSON.stringify([{ catalog: 'main', databaseType: 'DuckDB', schema: 's', name: 'bobs', kind: 'table', columns: [] }]), { status: 200 })
        : new Response('not this route', { status: 404 }));
    const got = await connect('https://wh.example', 'bob', 'pw');
    assert.equal(got.session.principal, 'bob');
    // one listing call: every catalog's objects, each with its catalog and database type
    assert.deepEqual(got.objects.map((o) => [o.catalog, o.databaseType, o.name]), [['main', 'DuckDB', 'bobs']]);
  });
});
