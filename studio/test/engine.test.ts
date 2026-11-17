// The session's engine (plan A1): Studio's Compiler asks whichever engine the config names the same two questions --
// the in-tab one (legend-lite's planner, WebAssembly) and a legend-lite server over its /api/lite/v1 (engine-client's
// LiteServer; the edge program's H7, 2026-11-17). The server is a stand-in here that answers the routes and refusal
// shape as legend-lite's server does (EngineError from {message, errorType}) -- with lite's own routes, or without
// them, as legend-engine; lite's own server joins when //core:server is visible to Studio.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import type { QueryEngine } from '../../engine-client/src/engine.ts';
import { BrowserEngine } from '../../engine-client/src/legend/browser-engine.ts';
import { EngineError, HttpEngine } from '../../engine-client/src/legend/engine.ts';
import { LiteServer } from '../../engine-client/src/doors/lite-server.ts';
import { WasmGrammar } from '../../engine-client/src/legend/wasm-grammar.ts';
import { Compiler } from '../src/backend/planner.ts';
import { compiler as inTab, grammar } from './modules.ts';

const GOOD = 'Class demo::Desk\n{\n  name: String[1];\n}\n';
const BAD = 'Class demo::Desk\n{\n  name: Strin[1];\n}\n';
/** Two bodies that do not compile: every error is both, the first error the first function's. */
const TWO = 'function demo::f():String[1]\n{\n  $nope\n}\n\nfunction demo::g():String[1]\n{\n  $nope\n}\n';

/**
 * A legend server's answers to these routes, and what it was asked: legend-lite's (`lite`: with its own compile
 * route, every error) or legend-engine's (without it: 404).
 */
function server(lite: boolean): { fetch: typeof fetch; asked: string[] } {
  const asked: string[] = [];
  const fetcher = async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
    const url = new URL(String(input));
    const body = String(init?.body ?? '');
    asked.push(`${init?.method} ${url.pathname}`);
    if (url.pathname.endsWith('/pure/v1/grammar/grammarToJson/model') || (lite && url.pathname.endsWith('/lite/v1/grammar/modelElements'))) {
      if (url.pathname.includes('/lite/')) assert.deepEqual(JSON.parse(body), { text: GOOD, sourceInformation: false });
      return Response.json({ _type: 'data', elements: [{ _type: 'class', package: 'demo', name: 'Desk' }, { _type: 'sectionIndex', package: '__internal__', name: 'SectionIndex' }] });
    }
    if (url.pathname.endsWith('/pure/v1/compilation/compile') || (lite && url.pathname.endsWith('/lite/v1/compilation/compile'))) {
      const model = JSON.parse(body) as { _type: string; code: string };
      assert.equal(model._type, 'text');
      const errors = model.code.includes('Strin[') ? ["Can't find type 'Strin'", 'and another'] : [];
      if (url.pathname.includes('/lite/')) return Response.json({ errors: errors.map((message) => ({ message })) });
      return errors.length > 0
        ? Response.json({ message: errors[0], errorType: 'COMPILATION' }, { status: 400 })
        : Response.json({ message: 'OK', defects: [] });
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

  it("on legend-lite's server: the same answers, each through lite's own route -- pure/v1 never asked", async () => {
    const lite = server(true);
    const compiler = new Compiler(new LiteServer('http://lite.example/api', lite.fetch));
    assert.deepEqual(await compiler.elements(GOOD), [{ path: 'demo::Desk', type: 'class' }]);
    assert.deepEqual(await compiler.compile(GOOD), []);
    assert.deepEqual(await compiler.compile(BAD), ["Can't find type 'Strin'", 'and another']);
    assert.deepEqual(lite.asked, [
      'POST /api/lite/v1/grammar/modelElements',
      'POST /api/lite/v1/compilation/compile',
      'POST /api/lite/v1/compilation/compile',
    ]);
  });

  it("on a server with no lite route (legend-engine), Studio's compiler is refused by its 404 and asks nothing else", async () => {
    const engine = server(false);
    const compiler = new Compiler(new LiteServer('http://engine.example/api', engine.fetch));
    await assert.rejects(compiler.elements(GOOD), (e: unknown) => e instanceof EngineError && e.status === 404);
    await assert.rejects(compiler.compile(GOOD), (e: unknown) => e instanceof EngineError && e.status === 404);
    assert.deepEqual(engine.asked, ['POST /api/lite/v1/grammar/modelElements', 'POST /api/lite/v1/compilation/compile']);
  });

  it("on legend-lite's server: a query's statement and its run, each a closed request to its own route, its answer lite's typed rows", async () => {
    const asked: { path: string; body: unknown }[] = [];
    const fetcher = (async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
      const path = new URL(String(input)).pathname;
      asked.push({ path, body: JSON.parse(String(init?.body)) });
      if (path === '/api/lite/v1/execution/plan') {
        return Response.json({ format: 'legend-lite-plan', root: { _type: 'tdsResult', shape: 'RELATION', sql: { statement: 'SELECT "id" FROM "T" WHERE "id" > ?' } } });
      }
      if (path === '/api/lite/v1/execution/execute') {
        return Response.json({ sql: 'SELECT "id" FROM "T" WHERE "id" > ?', shape: 'RELATION', columns: [{ name: 'id', type: 'Integer' }], rows: [[2]] });
      }
      return Response.json({ message: `no such legend-lite API: ${path}` }, { status: 404 });
    }) as typeof fetch;
    const lite = new LiteServer('http://lite.example/api/', fetcher);
    const model = { _type: 'text', code: GOOD } as const;
    const lambda = { _type: 'lambda', parameters: [], body: [] } as never;
    assert.equal(await lite.statement(model, lambda), 'SELECT "id" FROM "T" WHERE "id" > ?');
    assert.deepEqual(await lite.run(model, lambda, { n: 1 }),
      { sql: 'SELECT "id" FROM "T" WHERE "id" > ?', shape: 'RELATION', columns: [{ name: 'id', type: 'Integer' }], rows: [[2]] });
    assert.deepEqual(asked, [
      { path: '/api/lite/v1/execution/plan', body: { model, lambda } },
      { path: '/api/lite/v1/execution/execute', body: { model, lambda, parameters: { n: 1 } } },
    ]);
    // a refusal is lite's error shape, its kind kept
    const refusing = new LiteServer('http://lite.example/api', (async () => Response.json(
      { code: -1, errorType: 'COMPILATION', message: "Can't find type 'Strin'", status: 'error' }, { status: 400 })) as typeof fetch);
    await assert.rejects(refusing.run(model, lambda), (e: unknown) => e instanceof EngineError && e.status === 400
      && e.errorType === 'COMPILATION' && e.message === "Can't find type 'Strin'");
  });

  // the edge audit's H8 (2026-11-17, G7): a lite client asks legend-lite's route alone -- a server without it refuses
  // with its own 404, and nothing else is asked (legend-engine's compile is the edge's client's)
  it('on a server with no lite route, compileErrors is refused by its 404, asked once, no other route asked', async () => {
    const engine = server(false);
    const http = new HttpEngine('http://engine.example/api', engine.fetch);
    await assert.rejects(http.compileErrors(GOOD), (e: unknown) => e instanceof EngineError && e.status === 404);
    assert.deepEqual(engine.asked, ['POST /api/lite/v1/compilation/compile']);
  });

  it("the in-tab engine's compilation/compile answers as the server's: OK, or the first failure, 400 COMPILATION", async () => {
    const noSql = {} as QueryEngine;     // compiling runs no SQL
    const engine = new BrowserEngine(grammar, noSql, 'local');
    assert.deepEqual(await engine.compile({ _type: 'text', code: GOOD }), { message: 'OK', defects: [] });
    await assert.rejects(engine.compile({ _type: 'text', code: BAD }),
      (e: unknown) => e instanceof EngineError && e.status === 400 && e.errorType === 'COMPILATION' && /Strin/.test(e.message));
    await assert.rejects(engine.compile({ _type: 'text', code: TWO }),
      (e: unknown) => e instanceof EngineError && e.status === 400 && /demo::f/.test(e.message) && !/demo::g/.test(e.message));
  });

  it('in the tab, the grammar is pure/v1 answered by the planner: a model read and printed back, in either style', async () => {
    const model = await grammar.modelJson(GOOD);
    for (const style of ['STANDARD', 'PRETTY'] as const) {
      const text = await grammar.modelText(model, style);
      assert.deepEqual(await grammar.modelJson(text), model, style);
    }
  });

  it("in the tab, a parse error is the server's refusal: 400 PARSER, its message", async () => {
    await assert.rejects(grammar.modelJson('Class demo::Desk\n{\n  name String[1];\n}\n'),
      (e: unknown) => e instanceof EngineError && e.status === 400 && e.errorType === 'PARSER' && e.message.length > 0);
  });

  it("in the tab, a compile refusal is a problem listed; the planner failing is thrown, as a server's 500 is", async () => {
    const answering = (answer: string) => new WasmGrammar({ ask: async () => answer });
    // the compileErrors kind's answer (Boundary.compileErrors): every error, folded OK
    assert.deepEqual(await answering('OK\n{"errors":[{"message":"no such type"}]}').compileErrors('x'),
      ['no such type']);
    await assert.rejects(answering('ERR\n500\nIllegalStateException: the planner broke').compileErrors('x'),
      (e: unknown) => e instanceof EngineError && e.status === 500 && e.message === 'IllegalStateException: the planner broke');
    await assert.rejects(answering('ERR\nCOMPILATION\nno such type').modelJson('x'),
      (e: unknown) => e instanceof EngineError && e.status === 400 && e.errorType === 'COMPILATION');
  });
});
