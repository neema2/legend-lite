// INTO A PROJECT (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md B4): each case of test/cases -- every call one create makes,
// to the SDLC and to the writer, its exact body and the answer it was given, recorded against the native library
// (python/tests/test_model.py: this library run headless by DataCube's model actions, each call through Python's
// host; docs/DATACUBE_ONE_API_PLAN_2026_10_21.md D4) -- replayed here: this library makes the same calls, in order,
// with the same bodies, byte for byte, and answers the same.

import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { describe, it } from 'node:test';
import { readLambda } from '../../pure-protocol/src/index.ts';
import { SdlcClient } from '../../sdlc-client/src/client.ts';
import { runfilesFromEnv } from '../../tools/js/runfiles.mts';
import {
  createAssociation, createClass, createDataSpace, ensureWorkspace, serverWriter, writerBodies, type ClassAction,
  type Created, type ModelAnswer, type ModelWriter, type NewProject,
} from '../src/index.ts';

type Call =
  | { readonly to: 'sdlc'; readonly method: string; readonly path: string; readonly body: string | null;
      readonly status: number; readonly answer: string }
  | { readonly to: 'writer'; readonly route: string; readonly body: string; readonly answer: string };

interface Case {
  readonly name: string;
  readonly function: 'createClass' | 'createAssociation' | 'createDataSpace';
  readonly project: string | NewProject;
  readonly workspace: string;
  readonly package: string;
  readonly class?: ClassAction;
  readonly classes?: readonly ClassAction[];
  readonly association?: Parameters<typeof createAssociation>[0]['association'];
  readonly associations?: Parameters<typeof createDataSpace>[0]['associations'];
  readonly dataSpace?: Parameters<typeof createDataSpace>[0]['dataSpace'];
  readonly calls: readonly Call[];
  readonly result: { readonly project: string; readonly workspace: string; readonly revision: string };
}

/** A class action as the case holds it: its relation read as the protocol's lambda. */
const action = (c: ClassAction): ClassAction => ({ ...c, query: readLambda(c.query) });

/** The case's calls, answered in order: each one checked against the next, its answer the recorded one. */
function replay(calls: readonly Call[]): { sdlc: SdlcClient; writer: ModelWriter; done: () => void } {
  let next = 0;
  const expected = (to: Call['to']): Call => {
    const call = calls[next];
    assert.ok(call, `a call more than the case has: the ${to}`);
    next += 1;
    assert.equal(call.to, to, `call ${next}: the case has a call to the ${call.to}`);
    return call;
  };
  const fetcher = (async (url: string, init?: RequestInit) => {
    const call = expected('sdlc');
    if (call.to !== 'sdlc') throw new Error('unreachable');
    const body = init?.body === undefined ? null : String(init.body);
    assert.deepEqual({ method: init?.method, path: url, body }, { method: call.method, path: call.path, body: call.body },
      `call ${next}`);
    return new Response(call.answer === '' ? null : call.answer, { status: call.status });
  }) as typeof fetch;
  const write = (route: string, body: string): ModelAnswer => {
    const call = expected('writer');
    if (call.to !== 'writer') throw new Error('unreachable');
    assert.equal(route, call.route, `call ${next}`);
    assert.equal(body, call.body, `call ${next}: the ${route} body, byte for byte`);
    return JSON.parse(call.answer) as ModelAnswer;
  };
  const writer: ModelWriter = {
    createClass: async (project, pkg, cls) => write('createClass', writerBodies.createClass(project, pkg, cls)),
    createAssociation: async (project, pkg, association, classes) =>
      write('createAssociation', writerBodies.createAssociation(project, pkg, association, classes)),
    createDataSpace: async (project, pkg, dataSpace, classes, associations) =>
      write('createDataSpace', writerBodies.createDataSpace(project, pkg, dataSpace, classes, associations)),
  };
  return { sdlc: new SdlcClient('', fetcher), writer, done: () => assert.equal(next, calls.length, 'every call of the case made') };
}

function run(c: Case, sdlc: SdlcClient, writer: ModelWriter): Promise<Created> {
  const into = { sdlc, writer, project: c.project, workspace: c.workspace, package: c.package };
  switch (c.function) {
    case 'createClass':
      return createClass({ ...into, class: action(c.class!) });
    case 'createAssociation':
      return createAssociation({ ...into, association: c.association!, ...(c.classes ? { classes: c.classes.map(action) } : {}) });
    case 'createDataSpace':
      return createDataSpace({
        ...into, dataSpace: c.dataSpace!, ...(c.classes ? { classes: c.classes.map(action) } : {}),
        ...(c.associations ? { associations: c.associations } : {}),
      });
  }
}

const CASES = runfilesFromEnv('MODEL_CHANGES_CASES').map((f) => JSON.parse(readFileSync(f, 'utf8')) as Case);

describe("into a project: the cases recorded against the native library", () => {
  it('there are cases', () => assert.ok(CASES.length >= 5));
  for (const c of CASES) {
    it(c.name, async () => {
      const { sdlc, writer, done } = replay(c.calls);
      const made = await run(c, sdlc, writer);
      done();
      assert.deepEqual({ project: made.project, workspace: made.workspace, revision: made.revision }, c.result);
    });
  }
});

/** An SDLC answering each call in turn from `answers`: its status and its body. */
function answering(answers: readonly [number, string][]): SdlcClient {
  let next = 0;
  return new SdlcClient('', (async () => {
    const [status, body] = answers[next++]!;
    // a 204 carries no body
    return new Response(status === 204 ? null : body, { status });
  }) as unknown as typeof fetch);
}

describe('into a project: what the cases cannot record', () => {
  it('a workspace made by another ask meanwhile is there (lite answers its making 500 once it has commits)', async () => {
    await ensureWorkspace(answering([[404, '{"message":"no such workspace"}'], [500, '{"message":"Branch already exists"}'],
      [200, '{"workspaceId":"w"}']]), { project: 'g:a', workspace: 'w' });
  });

  it('a workspace refused and still not there is said', async () => {
    await assert.rejects(ensureWorkspace(answering([[404, '{}'], [409, '{"message":"a workspace W differs only in case"}'],
      [404, '{}']]), { project: 'g:a', workspace: 'w' }), /differs only in case/);
  });

  it('a save that changes nothing answers the revision read (the SDLC\'s 204, no body)', async () => {
    const writer = { createClass: async () => ({ changes: [{ type: 'MODIFY', path: 'a::A', pureCode: 'Class a::A {}' }] }) } as
      unknown as ModelWriter;
    const made = await createClass({ sdlc: answering([[200, '{}'], [200, '{"id":"r1"}'], [200, '[]'], [204, '']]), writer,
      project: 'g:a', workspace: 'w', package: 'a', class: {} as ClassAction });
    assert.equal(made.revision, 'r1');
  });

  it('a project made stays the caller\'s when a later step fails', async () => {
    const seen: string[] = [];
    const sdlc = answering([[200, '[]'], [200, '{"projectId":"datacube:trades"}'], [500, '{"message":"busy"}']]);
    await assert.rejects(createClass({ sdlc, writer: {} as ModelWriter, project: { name: 'Trades', group: 'datacube' },
      workspace: 'datacube', package: 'trades', class: {} as ClassAction, made: (p) => seen.push(p) }), /busy/);
    assert.deepEqual(seen, ['datacube:trades']);
  });
});

describe("legend-lite's model writer, over a fetch", () => {
  const refusing = (async () => new Response('{"message":"the column \'desk\' is asked to be \'primary\'"}', { status: 400 })) as
    unknown as typeof fetch;

  it("a server's refusal says where it is and the writer's own words", async () => {
    await assert.rejects(serverWriter('http://lite:8080/', refusing).createAssociation('', 'a', undefined),
      { message: "legend-lite's createAssociation at http://lite:8080: the column 'desk' is asked to be 'primary'" });
  });

  it("the host's own writer (a path of its own: a root of '') is said by its route alone", async () => {
    await assert.rejects(serverWriter('', refusing).createAssociation('', 'a', undefined),
      { message: "legend-lite's createAssociation: the column 'desk' is asked to be 'primary'" });
  });
});
