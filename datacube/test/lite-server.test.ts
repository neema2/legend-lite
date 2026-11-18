// THE PLANNER'S WORKER ON LEGEND-LITE'S SERVER (src/services/lite-server.ts; lite API v1, the edge audit's H7,
// 2026-11-17): each operation on the server's own route, a held model added to its question, a refusal folded as the
// module folds one, and legend-engine's pure/v1 never asked.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { liteServerWorker } from '../src/services/lite-server.ts';
import type { Request, RequestBody, Response } from '../../engine-client/src/legend/planner-protocol.ts';

/** A server answering each route with `answer`, and what it was asked. */
function server(answer: (path: string, body: string) => { status: number; body: string }) {
  const asked: { path: string; body: string }[] = [];
  const fetcher = (async (url: string, init: RequestInit) => {
    const path = new URL(url).pathname;
    const body = String(init.body);
    asked.push({ path, body });
    const a = answer(path, body);
    return new Response(a.body, { status: a.status });
  }) as unknown as typeof fetch;
  return { fetcher, asked };
}

/** `request` posted to `worker`, its answer. */
function ask(worker: ReturnType<typeof liteServerWorker>, request: RequestBody): Promise<Response> {
  return new Promise((done) => {
    worker.onmessage = (e) => done(e.data);
    worker.postMessage({ id: 1, ...request } as Request);
  });
}

describe("the planner's worker on legend-lite's server", () => {
  it('asks each operation on its own route, its request the route\'s body, its answer folded', async () => {
    const s = server(() => ({ status: 200, body: '{"query":{"_type":"lambda"}}' }));
    const w = liteServerWorker('http://lite.example/', s.fetcher);
    const answered = await ask(w, { kind: 'queryParse', request: '{"text":"|1"}' });
    assert.deepEqual(answered, { id: 1, ok: true, answer: 'OK\n{"query":{"_type":"lambda"}}' });
    assert.deepEqual(s.asked, [{ path: '/api/lite/v1/query/parse', body: '{"text":"|1"}' }]);
  });

  it('holds a model here, and adds it to each question about it, its query\'s digits whole', async () => {
    const s = server(() => ({ status: 200, body: '{"plan":{}}' }));
    const w = liteServerWorker('http://lite.example', s.fetcher);
    const held = await ask(w, { kind: 'hold', model: 'M' });
    assert.deepEqual(held, { id: 1, ok: true, answer: 'OK\n1' });
    await ask(w, { kind: 'queryPlanHeld', handle: 1, request: '{"query":{"value":9223372036854775807},"runtime":"r"}' });
    await ask(w, { kind: 'queryTypeHeld', handle: 1, request: '{"query":{}}' });
    assert.deepEqual(s.asked, [
      { path: '/api/lite/v1/query/plan', body: '{"query":{"value":9223372036854775807},"runtime":"r","model":"M"}' },
      { path: '/api/lite/v1/query/type', body: '{"query":{},"model":"M"}' },
    ]);
  });

  it('holds a model over a library as the one text the server compiles', async () => {
    const s = server(() => ({ status: 200, body: '{"columns":[]}' }));
    const w = liteServerWorker('http://lite.example', s.fetcher);
    await ask(w, { kind: 'hold', model: 'M', library: 'L' });
    await ask(w, { kind: 'queryTypeHeld', handle: 1, request: '{"query":{}}' });
    assert.equal(s.asked[0]!.body, '{"query":{},"model":"M\\nL"}');
  });

  it("passes the route's refusal on, lite's one error shape; a reply of no such shape is the server's failure", async () => {
    const refusal = '{"kind":"parse","message":"[1:3] expected identifier"}';
    const w = liteServerWorker('http://lite.example', server(() => ({ status: 400, body: refusal })).fetcher);
    assert.deepEqual(await ask(w, { kind: 'queryParse', request: '{"text":"|$"}' }), { id: 1, ok: true, answer: `ERR\n${refusal}` });
    const broken = liteServerWorker('http://lite.example', server(() => ({ status: 502, body: 'bad gateway' })).fetcher);
    const answered = await ask(broken, { kind: 'queryParse', request: '{"text":"|1"}' });
    assert.equal(answered.ok, false);
  });

  // (legend-engine's pure/v1 is no planner kind since the user's K2 ruling, 2026-11-18: the engine module's, the edge's)
  it("refuses by name what it does not ask the server: the tab's own, a model not held", async () => {
    const s = server(() => ({ status: 200, body: '{}' }));
    const w = liteServerWorker('http://lite.example', s.fetcher);
    const own = await ask(w, { kind: 'tableModel', table: '{}' });
    assert.ok(!own.ok && /tableModel is not asked of legend-lite's server/.test(own.error));
    const unheld = await ask(w, { kind: 'queryTypeHeld', handle: 7, request: '{"query":{}}' });
    assert.ok(unheld.ok && unheld.answer.startsWith('ERR\n') && /no model held as 7/.test(unheld.answer));
    assert.deepEqual(s.asked, [], 'nothing asked of the server');
  });
});
