// A PAGE ON PYTHON, PLANNED BY PYTHON (docs/DATACUBE_ONE_APP_DESIGN_2026_10_11.md §15): the page's runner asks Python
// alone, with the token -- its plan-and-run, its dry run (plan.json), and its questions about the Pure query on
// legend-engine's own routes (pure/v1), never the tab's worker messages carried over HTTP (§15.2, as amended
// 2026-10-15) -- and reads what Python answers as the tab reads its own planner's. A Python host plans in process, on
// its one plane (the program's §1.1, the user's ruling of 2026-10-15): one that says another is refused, never followed.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { col, element, fn, lambda, toJson, type Lambda } from '../../pure-protocol/src/index.ts';
import { DEFAULT_CONFIGURATION } from '../src/config.ts';
import { cubeToJson, writeCube } from '../src/cube-document.ts';
import type { JoinHow } from '../src/join-how.ts';
import type { LitePlanner } from '../src/lite-planner.ts';
import { askedOffers, pythonDatabase, pythonRunner, type PythonLink, type PythonOffers } from '../src/services/python-host.ts';
import type { CubeSnapshot } from '../src/snapshot.ts';
import { TreeState } from '../src/tree.ts';

const MODEL = '###Pure\nClass x::T {}';
const RUNTIME = 'x::R';
const QUERY: Lambda = lambda([], fn('select', fn('from', element('x::DB')), col('x', 'desk')));

/** Python, as the page sees it: each call caught, and answered as `answer` says. */
function python(answer: (path: string, body: string) => Response): { link: PythonLink; calls: { path: string; body: string; auth: string | null }[] } {
  const calls: { path: string; body: string; auth: string | null }[] = [];
  const fetch = (async (url: string, init?: RequestInit) => {
    const path = new URL(url).pathname;
    const body = String(init?.body ?? '');
    calls.push({ path, body, auth: (init?.headers as Record<string, string> | undefined)?.['Authorization'] ?? null });
    return answer(path, body);
  }) as typeof globalThis.fetch;
  return { link: { baseUrl: 'http://127.0.0.1:9', fetch, authorization: 'Bearer t0ken' }, calls };
}

/** A JSON answer, as much of a Response as the page reads (V8 inside Python, the headless lane, has no Response). */
const json = (body: unknown, status = 200): Response => {
  const text = JSON.stringify(body);
  return {
    ok: status >= 200 && status < 300,
    status,
    headers: { get: (name: string) => (name.toLowerCase() === 'content-type' ? 'application/json' : null) },
    text: async () => text,
    json: async () => JSON.parse(text) as unknown,
  } as unknown as Response;
};

/** What a Python host says it offers (services.json), planning on `word`. */
const offers = (word: string) => {
  const plane = { word, label: `Plan ${word}` };
  return { files: true, remote: false, pages: { where: '/home/me/pages', user: 'local' }, plane, planes: [plane] };
};

describe('a page on Python, planned by Python', () => {
  it("sends Python the query in one plan-and-run, with the token, and reads Python's own planner's refusal", async () => {
    const { link, calls } = python(() => json({ plan: 'ERR\nCOMPILATION\nno such class' }));
    await assert.rejects(pythonRunner(link, MODEL, RUNTIME).run(QUERY, { epoch: 1 } as CubeSnapshot), /no such class/);
    assert.equal(calls.length, 1);
    assert.equal(calls[0]!.path, '/plan-and-run.json');
    assert.equal(calls[0]!.auth, 'Bearer t0ken');
  });

  it("asks a question about the Pure query on legend-engine's own route, with the token", async () => {
    const type = { columns: [{ name: 'desk', genericType: { rawType: { _type: 'packageableType', fullPath: 'String' } } }] };
    const { link, calls } = python(() => json(type));
    assert.deepEqual(await pythonRunner(link, MODEL, RUNTIME).relationType(QUERY), [{ name: 'desk', type: 'String' }]);
    assert.equal(calls.length, 1);
    assert.equal(calls[0]!.path, '/api/pure/v1/compilation/lambdaRelationType');
    assert.equal(calls[0]!.auth, 'Bearer t0ken');
  });

  it("dry-runs on the host's compute, plan-and-run's planning half, the model by its text and the token", async () => {
    const { link, calls } = python(() => json({ plan: 'OK\n{"sql":"SELECT 1","type":{"columns":[]}}' }));
    const runner = pythonRunner(link, MODEL, RUNTIME);
    await runner.compile(QUERY, { epoch: 1 } as CubeSnapshot);
    assert.equal(calls.length, 1, 'the model is held in the page: only the dry run is sent');
    assert.equal(calls[0]!.path, '/plan.json');
    assert.equal(calls[0]!.auth, 'Bearer t0ken');
    // the request plan-and-run sends for the same query (planRequest), so the dry run plans what the run would
    const sent = JSON.parse(calls[0]!.body) as { lambda: string; model: string; runtime: string };
    assert.deepEqual(sent, (runner.host.planner as LitePlanner).planRequest(QUERY));
  });

  it("refuses an answer of Python's it cannot read, never leaving the planner's request unanswered", async () => {
    const unread = { ok: true, status: 200, headers: { get: () => 'application/json' }, text: async () => 'not json' };
    const { link } = python(() => unread as unknown as Response);
    await assert.rejects(pythonRunner(link, MODEL, RUNTIME).compile(QUERY, { epoch: 1 } as CubeSnapshot),
      /the engine's answer to \/plan\.json could not be read/);
  });

  it("refuses in the page, by name, what the tab's worker answers for its own database: nothing is sent", async () => {
    const { link, calls } = python(() => json({}));
    const planner = pythonRunner(link, MODEL, RUNTIME).host.planner as LitePlanner;
    await assert.rejects(planner.sessionSetup('DuckDB'), /sessionSetup is not asked of a Python host: a session's setup/);
    assert.equal(calls.length, 0);
  });

  it('takes the one plane a Python host plans on, its own', async () => {
    const { link } = python(() => json(offers('local')));
    assert.equal((await askedOffers(link)).plane.word, 'local');
  });

  it('refuses a Python host that says it plans elsewhere: no plane but its own is followed', async () => {
    for (const word of ['remote', 'engine']) {
      const { link } = python(() => json(offers(word)));
      await assert.rejects(askedOffers(link), new RegExp(`plans on its own planner, in process: it named "${word}"`), word);
    }
  });
});

describe("where Python reads a model's rows: a saved query's are its project's own, said so (F28)", () => {
  it('asks plan-and-run and the dry run for a project\'s rows only when the base is a saved query\'s', async () => {
    const { link, calls } = python(() => json({ plan: 'ERR\nCOMPILATION\nno such class' }));
    await assert.rejects(pythonRunner(link, MODEL, RUNTIME, { project: true }).run(QUERY, { epoch: 1 } as CubeSnapshot));
    await assert.rejects(pythonRunner(link, MODEL, RUNTIME).run(QUERY, { epoch: 1 } as CubeSnapshot));
    assert.deepEqual(calls.map((c) => (JSON.parse(c.body) as { project?: boolean }).project), [true, undefined]);
  });
});

describe("a join Python makes for a tab on its page: the wire (B11, the review's F37)", () => {
  const HOW: JoinHow = { form: 'join', kind: 'left', on: { rows: [{ left: 'desk', op: '==', right: 'desk' }] }, prefix: 'd_', keep: null };
  const RELATION = fn('from', element('x::DB'));
  const CUBE = writeCube({
    name: 'trades \u22c8 desks', source: { _type: 'frame', name: 't', columns: [{ name: 'desk', type: 'String' }] },
    snapshot: { source: { query: { _type: 'var', name: 'x' } }, columns: [{ name: 'desk', type: 'String' }], derived: [],
      rows: [], pivotOn: [], sorts: [], epoch: 0 } as unknown as CubeSnapshot,
    configuration: DEFAULT_CONFIGURATION, tree: TreeState.empty(),
  });
  const side = (name: string) => ({ name, relation: JSON.parse(toJson(RELATION)) as unknown, columns: [{ name: 'desk', type: 'String' }] });
  const ANSWERS: Record<string, unknown> = {
    sides: { left: side('trades'), right: side('desks'), notes: ['desks: a column gone'] },
    questions: { counts: { mine: 5, matched: 3 }, words: ['3 rows of my 5 rows find a match, 2 rows do not.'] },
    cube: { command: { kind: 'addGrid', document: JSON.parse(cubeToJson(CUBE)) as unknown }, notes: [] },
  };

  it("asks join.json each of the window's answers -- the page as its text, how as the protocol writes it -- and reads them", async () => {
    const { link, calls } = python((_path, body) => json(ANSWERS[(JSON.parse(body) as { op: string }).op]));
    const join = pythonDatabase(link, offers('local') as unknown as PythonOffers).join!;
    const sides = await join.sides('PAGE TEXT', 'grid', 'grid-2');
    assert.deepEqual([sides.left.name, sides.right.name, sides.notes], ['trades', 'desks', ['desks: a column gone']]);
    assert.equal(sides.left.relation._type, 'func', 'read as the protocol writes a relation');
    assert.deepEqual(await join.questions('PAGE TEXT', 'grid', 'grid-2', HOW), { mine: 5, matched: 3 });
    assert.equal((await join.cube('PAGE TEXT', 'grid', 'grid-2', HOW, 'sheet-1')).name, CUBE.name, 'its cube document, read');
    assert.deepEqual(calls.map((c) => [c.path, c.auth]), Array(3).fill(['/join.json', 'Bearer t0ken']));
    const [sent, asked, made] = calls.map((c) => JSON.parse(c.body) as Record<string, unknown>);
    assert.deepEqual(sent, { op: 'sides', page: 'PAGE TEXT', mine: 'grid', theirs: 'grid-2' });
    assert.deepEqual([asked!['op'], asked!['how']], ['questions', JSON.parse(toJson(HOW))]);
    assert.deepEqual([made!['op'], made!['sheet'], made!['how']], ['cube', 'sheet-1', JSON.parse(toJson(HOW))]);
  });

  it("says Python's refusal in its words", async () => {
    const { link } = python(() => ({ ...json({}, 422), text: async () => "ValueError: 'grid-9' is not a tile of the page" }) as unknown as Response);
    await assert.rejects(pythonDatabase(link, offers('local') as unknown as PythonOffers).join!.sides('PAGE', 'grid', 'grid-9'), /'grid-9' is not a tile of the page/);
  });
});

describe("a tab's grid over a class, on a page Python serves (K16, K17)", () => {
  it('asks source.json for the cube page.grid makes -- the page\'s model and the class as their text -- and reads it; refused in its words', async () => {
    const { link, calls } = python(() => json({ cube: 'CUBE TEXT' }));
    const made = pythonDatabase(link, offers('local') as unknown as PythonOffers).made!;
    assert.equal(await made('{"project":"p:q"}', '{"class":"Trade"}', 'Q3', 2), 'CUBE TEXT');
    assert.deepEqual(calls.map((c) => [c.path, c.auth]), [['/source.json', 'Bearer t0ken']]);
    assert.deepEqual(JSON.parse(calls[0]!.body), { model: '{"project":"p:q"}', given: '{"class":"Trade"}', page: 'Q3', gridsOnPage: 2 });
    const refused = python(() => ({ ...json({}, 422), text: async () => 'the project has no class Nope' }) as unknown as Response);
    await assert.rejects(pythonDatabase(refused.link, offers('local') as unknown as PythonOffers).made!(null, '{"class":"Nope"}', 'Q3', 1),
      /the project has no class Nope/);
  });
});
