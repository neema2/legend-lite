// The session's engine (plan A1): Studio's Compiler asks whichever engine the config names the same two questions --
// the in-tab one (legend-lite's planner, WebAssembly) and a legend server over pure/v1 (engine-client's HttpEngine).
// The server is a stand-in here that answers the routes and refusal shape as legend-lite's server does (EngineError
// from {message, errorType}) -- with lite's own compile route, or without it, as legend-engine; lite's own server
// joins when //core:server is visible to Studio.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import type { QueryEngine } from '../../engine-client/src/engine.ts';
import { BrowserEngine } from '../../engine-client/src/legend/browser-engine.ts';
import { EngineError, HttpEngine } from '../../engine-client/src/legend/engine.ts';
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
    if (url.pathname.endsWith('/pure/v1/grammar/grammarToJson/model')) {
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

  it("on legend-lite's server: the same answers, every error through its own route", async () => {
    const lite = server(true);
    const compiler = new Compiler(new HttpEngine('http://lite.example/api', lite.fetch));
    assert.deepEqual(await compiler.elements(GOOD), [{ path: 'demo::Desk', type: 'class' }]);
    assert.deepEqual(await compiler.compile(GOOD), []);
    assert.deepEqual(await compiler.compile(BAD), ["Can't find type 'Strin'", 'and another']);
    assert.deepEqual(lite.asked, [
      'POST /api/pure/v1/grammar/grammarToJson/model',
      'POST /api/lite/v1/compilation/compile',
      'POST /api/lite/v1/compilation/compile',
    ]);
  });

  it("on legend-engine: its compilation/compile, the first error, once it has answered that it has no lite route", async () => {
    const engine = server(false);
    const compiler = new Compiler(new HttpEngine('http://engine.example/api', engine.fetch));
    assert.deepEqual(await compiler.compile(GOOD), []);
    assert.deepEqual(await compiler.compile(BAD), ["Can't find type 'Strin'"]);
    assert.deepEqual(engine.asked, [
      'POST /api/lite/v1/compilation/compile',
      'POST /api/pure/v1/compilation/compile',
      'POST /api/pure/v1/compilation/compile',
    ]);
  });

  it('on legend-engine, two compiles asked at once both get its answer, whichever 404 comes back first', async () => {
    const http = new HttpEngine('http://engine.example/api', server(false).fetch);
    assert.deepEqual(await Promise.all([http.compileErrors(GOOD), http.compileErrors(BAD)]), [[], ["Can't find type 'Strin'"]]);
  });

  it("the in-tab engine's compilation/compile answers as the server's: OK, or the first failure, 400 COMPILATION", async () => {
    const noSql = {} as QueryEngine;     // compiling runs no SQL
    const engine = new BrowserEngine(grammar, noSql, () => false, 'local');
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
    assert.deepEqual(await answering('OK\n200\napplication/json\n{"errors":[{"message":"no such type"}]}').compileErrors('x'),
      ['no such type']);
    await assert.rejects(answering('ERR\n500\nIllegalStateException: the planner broke').compileErrors('x'),
      (e: unknown) => e instanceof EngineError && e.status === 500 && e.message === 'IllegalStateException: the planner broke');
    await assert.rejects(answering('ERR\nCOMPILATION\nno such type').modelJson('x'),
      (e: unknown) => e instanceof EngineError && e.status === 400 && e.errorType === 'COMPILATION');
  });
});
