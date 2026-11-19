// The session's engine (plan A1): Studio's Compiler asks whichever engine the config names the same two questions --
// the in-tab one (legend-lite's planner, WebAssembly) and a legend-lite server over its /api/lite/v1 (engine-client's
// LiteServer; lite's API v1, 2026-11-17: model/elements and model/check). The server is a stand-in here that answers
// lite's routes and its refusal shape ({kind, message, where?}, LiteRefusal) -- or none of them, as legend-engine;
// lite's own server is held to the same answers by its conformance reader (engine-client/test/lite-api-server.test.ts).

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import type { QueryEngine } from '../../engine-client/src/engine.ts';
import { statementOf } from '../../engine-client/src/legend/browser-engine.ts';
import { BrowserEngine } from '../../engine-client/src/legend/edge-engine.ts';
import { EngineError } from '../../engine-client/src/legend/engine.ts';
import { LiteServer } from '../../engine-client/src/doors/lite-server.ts';
import { LiteRefusal } from '../../engine-client/src/lite-error.ts';
import { WasmGrammar } from '../../engine-client/src/legend/wasm-grammar.ts';
import { Compiler } from '../src/backend/planner.ts';
import { compiler as inTab, edge, grammar } from './modules.ts';

const GOOD = 'Class demo::Desk\n{\n  name: String[1];\n}\n';
const BAD = 'Class demo::Desk\n{\n  name: Strin[1];\n}\n';
/** Two bodies that do not compile: every error is both, the first error the first function's. */
const TWO = 'function demo::f():String[1]\n{\n  $nope\n}\n\nfunction demo::g():String[1]\n{\n  $nope\n}\n';

/**
 * A legend server's answers to lite's routes, and what it was asked: legend-lite's (`lite`: model/elements and
 * model/check, every error in lite's shape) or legend-engine's (none of lite's: its own 404 text).
 */
function server(lite: boolean): { fetch: typeof fetch; asked: string[] } {
  const asked: string[] = [];
  const fetcher = async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
    const url = new URL(String(input));
    const body = String(init?.body ?? '');
    asked.push(`${init?.method} ${url.pathname}`);
    if (lite && url.pathname.endsWith('/lite/v1/model/elements')) {
      assert.deepEqual(JSON.parse(body), { model: GOOD });
      return Response.json({ elements: [{ _type: 'class', package: 'demo', name: 'Desk' }, { _type: 'sectionIndex', package: '__internal__', name: 'SectionIndex' }] });
    }
    if (lite && url.pathname.endsWith('/lite/v1/model/check')) {
      const { model } = JSON.parse(body) as { model: string };
      const errors = model.includes('Strin[') ? ["[3:9] Can't find type 'Strin'", 'and another'] : [];
      return Response.json({ errors: errors.map((message) => ({ kind: 'compile', message })) });
    }
    return new Response('not found', { status: 404 });
  };
  return { fetch: fetcher as typeof fetch, asked };
}

describe("Studio's compiler over the session's engine", () => {
  it('in the tab: the elements a text declares, [] for a model that compiles, every error for one that does not', async () => {
    assert.deepEqual(await inTab.elements(GOOD), [{ path: 'demo::Desk', type: 'class' }]);
    assert.deepEqual(await inTab.compile(GOOD), []);
    assert.equal((await inTab.compile(BAD)).length, 1);
    const two = await inTab.compile(TWO);
    assert.equal(two.length, 2, JSON.stringify(two));
    assert.match(two[0]!, /demo::f/);
    assert.match(two[1]!, /demo::g/);
  });

  it("on legend-lite's server: the same answers, each through lite's own operation -- pure/v1 never asked", async () => {
    const lite = server(true);
    const compiler = new Compiler(new LiteServer('http://lite.example/api', lite.fetch));
    assert.deepEqual(await compiler.elements(GOOD), [{ path: 'demo::Desk', type: 'class' }]);
    assert.deepEqual(await compiler.compile(GOOD), []);
    assert.deepEqual(await compiler.compile(BAD), ["[3:9] Can't find type 'Strin'", 'and another']);
    assert.deepEqual(lite.asked, [
      'POST /api/lite/v1/model/elements',
      'POST /api/lite/v1/model/check',
      'POST /api/lite/v1/model/check',
    ]);
  });

  it("on a server with no lite route (legend-engine), Studio's compiler is refused by its 404 and asks nothing else", async () => {
    const engine = server(false);
    const compiler = new Compiler(new LiteServer('http://engine.example/api', engine.fetch));
    await assert.rejects(compiler.elements(GOOD), (e: unknown) => e instanceof LiteRefusal && e.status === 404 && e.kind === 'internal');
    await assert.rejects(compiler.compile(GOOD), (e: unknown) => e instanceof LiteRefusal && e.status === 404);
    assert.deepEqual(engine.asked, ['POST /api/lite/v1/model/elements', 'POST /api/lite/v1/model/check']);
  });

  it("on legend-lite's server: query/plan and query/run, each a closed request to its own operation, its rows exact", async () => {
    const asked: { path: string; body: unknown }[] = [];
    const fetcher = (async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
      const path = new URL(String(input)).pathname;
      asked.push({ path, body: JSON.parse(String(init?.body)) });
      if (path === '/api/lite/v1/query/plan') {
        return Response.json({ plan: { format: 'legend-lite-plan', root: { _type: 'tdsResult', shape: 'RELATION', sql: { statement: 'SELECT "id" FROM "T" WHERE "id" > ?' } } } });
      }
      if (path === '/api/lite/v1/query/run') {
        // an Integer is an exact JSON number (spec §4): 2^63-1 read exactly
        return new Response('{"shape":"RELATION","columns":[{"name":"id","type":"Integer"}],"rows":[[2],[9223372036854775807]]}',
          { headers: { 'Content-Type': 'application/json' } });
      }
      return Response.json({ kind: 'request', message: `no such legend-lite API: ${path}` }, { status: 400 });
    }) as typeof fetch;
    const lite = new LiteServer('http://lite.example/api/', fetcher, 'json');
    const query = { _type: 'lambda', parameters: [], body: [] } as never;
    assert.equal(statementOf(await lite.queryPlan(GOOD, query, 'demo::RT')), 'SELECT "id" FROM "T" WHERE "id" > ?');
    assert.deepEqual(await lite.queryRun(GOOD, query, 'demo::RT', { n: 1 }),
      { shape: 'RELATION', columns: [{ name: 'id', type: 'Integer' }], rows: [[2], [9223372036854775807n]] });
    assert.deepEqual(asked, [
      { path: '/api/lite/v1/query/plan', body: { model: GOOD, query: { _type: 'lambda', body: [], parameters: [] }, runtime: 'demo::RT' } },
      { path: '/api/lite/v1/query/run', body: { format: 'json', model: GOOD, params: { n: 1 }, query: { _type: 'lambda', body: [], parameters: [] }, runtime: 'demo::RT' } },
    ]);
    // a refusal is lite's one shape, its kind, its where and its status kept (spec §3)
    const refusing = new LiteServer('http://lite.example/api', (async () => Response.json(
      { kind: 'compile', message: "[3:9] Can't find type 'Strin'", where: '[3:9]' }, { status: 400 })) as typeof fetch);
    await assert.rejects(refusing.queryRun(GOOD, query, 'demo::RT'), (e: unknown) => e instanceof LiteRefusal && e.status === 400
      && e.kind === 'compile' && e.where === '[3:9]' && e.message === "[3:9] Can't find type 'Strin'");
    // a body in no refusal's shape (legend-engine's envelope) is no refusal of lite's: said as a defect, never guessed
    const engineShaped = new LiteServer('http://lite.example/api', (async () => Response.json(
      { code: -1, errorType: 'COMPILATION', message: 'x', status: 'error' }, { status: 400 })) as typeof fetch);
    await assert.rejects(engineShaped.queryType(GOOD, query), (e: unknown) => e instanceof LiteRefusal && e.kind === 'internal'
      && /in no refusal's shape/.test(e.message));
  });

  it("the in-tab engine's compilation/compile answers as the server's: OK, or the first failure, 400 COMPILATION", async () => {
    const noSql = {} as QueryEngine;     // compiling runs no SQL
    const engine = new BrowserEngine(edge, noSql, 'local');
    assert.deepEqual(await engine.compile({ _type: 'text', code: GOOD }), { message: 'OK', defects: [] });
    await assert.rejects(engine.compile({ _type: 'text', code: BAD }),
      (e: unknown) => e instanceof EngineError && e.status === 400 && e.errorType === 'COMPILATION' && /Strin/.test(e.message));
    await assert.rejects(engine.compile({ _type: 'text', code: TWO }),
      (e: unknown) => e instanceof EngineError && e.status === 400 && /demo::f/.test(e.message) && !/demo::g/.test(e.message));
  });

  it('in the tab, a model read (model/elements) and printed back by pure/v1, in either style', async () => {
    const model = await grammar.modelElements(GOOD);
    for (const style of ['STANDARD', 'PRETTY'] as const) {
      const text = await edge.modelText(model, style);
      assert.deepEqual(await grammar.modelElements(text), model, style);
    }
  });

  it("in the tab, a parse error is lite's refusal: kind parse, its position, its message", async () => {
    await assert.rejects(grammar.modelElements('Class demo::Desk\n{\n  name String[1];\n}\n'),
      (e: unknown) => e instanceof LiteRefusal && e.kind === 'parse' && e.where !== undefined && e.message.length > 0);
  });

  it("in the tab, the planner's fold read as lite's: OK and its answer, or ERR and lite's refusal; any other form a defect", async () => {
    const answering = (answer: string) => new WasmGrammar({ ask: async () => answer });
    // model/check's answer (Boundary.modelCheck): every error, folded OK
    assert.deepEqual(await answering('OK\n{"errors":[{"kind":"compile","message":"no such type","where":"[1:1]"}]}').modelCheck('x'),
      [{ kind: 'compile', message: 'no such type', where: '[1:1]' }]);
    await assert.rejects(answering('ERR\n{"kind":"internal","message":"IllegalStateException: the planner broke"}').modelCheck('x'),
      (e: unknown) => e instanceof LiteRefusal && e.kind === 'internal' && e.message === 'IllegalStateException: the planner broke');
    await assert.rejects(answering('ERR\n{"kind":"compile","message":"no such type"}').modelElements('x'),
      (e: unknown) => e instanceof LiteRefusal && e.kind === 'compile');
    // the fold before lite's API v1 (ERR, an errorType, a message) is no form the planner answers in now
    await assert.rejects(answering('ERR\nCOMPILATION\nno such type').modelElements('x'),
      (e: unknown) => e instanceof LiteRefusal && e.kind === 'internal' && /in no form it has/.test(e.message));
  });
});
