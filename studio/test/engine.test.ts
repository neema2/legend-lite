// The session's engine (plan A1): Studio's Compiler asks whichever engine the config names the same two questions --
// the in-tab one (legend-lite's planner, WebAssembly) and a legend server over pure/v1 (engine-client's HttpEngine).
// The server is a stand-in here that answers pure/v1's routes and refusal shape as legend-lite's server does
// (EngineError from {message, errorType}); lite's own server joins when //core:server is visible to Studio.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { HttpEngine } from '../../engine-client/src/legend/engine.ts';
import { Compiler } from '../src/backend/planner.ts';
import { compiler as inTab } from './modules.ts';

const GOOD = 'Class demo::Desk\n{\n  name: String[1];\n}\n';
const BAD = 'Class demo::Desk\n{\n  name: Strin[1];\n}\n';

/** pure/v1 as legend-lite's server answers these two routes, and what it was asked. */
function pureV1(): { fetch: typeof fetch; asked: string[] } {
  const asked: string[] = [];
  const fetcher = async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
    const url = new URL(String(input));
    const body = String(init?.body ?? '');
    asked.push(`${init?.method} ${url.pathname}`);
    if (url.pathname.endsWith('/pure/v1/grammar/grammarToJson/model')) {
      return Response.json({ _type: 'data', elements: [{ _type: 'class', package: 'demo', name: 'Desk' }, { _type: 'sectionIndex', package: '__internal__', name: 'SectionIndex' }] });
    }
    if (url.pathname.endsWith('/pure/v1/compilation/compile')) {
      const model = JSON.parse(body) as { _type: string; code: string };
      assert.equal(model._type, 'text');
      return model.code.includes('Strin[')
        ? Response.json({ message: "Can't find type 'Strin'", errorType: 'COMPILATION' }, { status: 400 })
        : Response.json({ message: 'OK', defects: [] });
    }
    return new Response('not found', { status: 404 });
  };
  return { fetch: fetcher as typeof fetch, asked };
}

describe("Studio's compiler over the session's engine", () => {
  it('in the tab: the elements a text declares, [] for a model that compiles, the errors for one that does not', async () => {
    assert.deepEqual(await inTab.elements(GOOD), [{ path: 'demo::Desk', type: 'class' }]);
    assert.deepEqual(await inTab.compile(GOOD), []);
    assert.ok((await inTab.compile(BAD)).length > 0);
  });

  it('on a legend server: the same answers, through pure/v1 (grammarToJson/model, compilation/compile)', async () => {
    const server = pureV1();
    const compiler = new Compiler(new HttpEngine('http://lite.example/api', server.fetch));
    assert.deepEqual(await compiler.elements(GOOD), [{ path: 'demo::Desk', type: 'class' }]);
    assert.deepEqual(await compiler.compile(GOOD), []);
    assert.deepEqual(await compiler.compile(BAD), ["Can't find type 'Strin'"]);
    assert.deepEqual(server.asked, [
      'POST /api/pure/v1/grammar/grammarToJson/model',
      'POST /api/pure/v1/compilation/compile',
      'POST /api/pure/v1/compilation/compile',
    ]);
  });
});
