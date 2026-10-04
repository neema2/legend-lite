// ONE SUITE, every SDLC here: upstream legend-sdlc's routes and lite's text routes asked over raw HTTP --
// the paths, the JSON and its field order, the statuses, the refusals word for word -- and through the
// client. It runs against the page's SDLC (wasm.test.ts: sdlc-server's rules in WebAssembly) and, when
// it lands, against sdlc-server over HTTP, so the two cannot answer differently without one of them
// failing here. The rules:
// studio/docs/SDLC_CONTRACT_SLICE1.md; lite's departures: README.md.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { SdlcClient, SdlcError } from '../src/client.ts';
import type { PureChange, Revision } from '../src/wire.ts';

export interface Target {
  /** The SDLC's API root, `http://host:port/sdlc/api`. */
  readonly api: string;
  readonly fetch: typeof fetch;
  /** Who the SDLC says the caller is. */
  readonly user: string;
}

const REVISION_FIELDS = ['id', 'authorName', 'authoredTimestamp', 'committerName', 'committedTimestamp', 'message'];
const CONFIG_FIELDS = ['projectId', 'projectType', 'projectStructureVersion', 'platformConfigurations', 'groupId',
  'artifactId', 'projectDependencies', 'metamodelDependencies', 'artifactGenerations', 'runDependencyTests',
  'produceShadedServiceJar'];
/** `Instant.toString()`: whole seconds, or milliseconds when there are any. */
const INSTANT = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{3})?Z$/;

const PERSON = `// a person, as the demo writes one
Class demo::party::Person
{
  name: String[1];
  age: Integer[0..1];
}
`;
const ADDRESS = `Class demo::party::Address
{
  street: String[1];
}
`;
const COUNTRY = `Enum demo::types::Country
{
  GB, US
}
`;

export function conformance(name: string, target: () => Target): void {
  describe(`the SDLC: ${name}`, () => {
    // each run its own project: a server's store may hold an earlier run's
    const run = `c${Date.now().toString(36)}${Math.floor(Math.random() * 1e6).toString(36)}`;
    const groupId = 'org.finos.lite.conformance';
    const p = `${groupId}:${run}`;
    const P = encodeURIComponent(p);
    const raw = async (method: string, path: string, body?: unknown): Promise<{ status: number; json: unknown; text: string }> => {
      const t = target();
      const r = await t.fetch(`${t.api}${path}`, {
        method,
        ...(body === undefined ? {} : { headers: { 'Content-Type': 'application/json' }, body: typeof body === 'string' ? body : JSON.stringify(body) }),
      });
      const text = await r.text();
      let json: unknown = undefined;
      try {
        json = text ? JSON.parse(text) : undefined;
      } catch {
        // not JSON: compared as text
      }
      return { status: r.status, json, text };
    };
    const refused = async (method: string, path: string, body: unknown, status: number, message: string): Promise<Record<string, unknown>> => {
      const r = await raw(method, path, body);
      assert.equal(r.status, status, r.text);
      const e = r.json as Record<string, unknown>;
      assert.equal(e['message'], message);
      return e;
    };
    const client = (): SdlcClient => new SdlcClient(target().api, target().fetch);
    const create = (path: string, pureCode: string): PureChange => ({ type: 'CREATE', path, pureCode });

    it('answers who the caller is, and that it is authorized', async () => {
      const me = await client().currentUser();
      assert.deepEqual(Object.keys(me), ['userId', 'name']);
      assert.equal(me.userId, target().user);
      assert.equal(await client().authorized(), true);
    });

    it('refuses a project without what upstream requires, in its words and order', async () => {
      await refused('POST', '/projects', '', 400, 'Input required to create project');
      const ok = { name: 'Conformance', description: '', groupId, artifactId: run };
      await refused('POST', '/projects', { ...ok, name: '' }, 400, 'name may not be null or empty');
      await refused('POST', '/projects', { ...ok, description: null }, 400, 'description may not be null');
      await refused('POST', '/projects', { ...ok, groupId: 'org.finos.class' }, 400, 'Invalid groupId: org.finos.class');
      await refused('POST', '/projects', { ...ok, artifactId: 'Bad-Id' }, 400,
        'Invalid artifactId: Bad-Id. ArtifactId must follow pattern that starts with a lowercase letter and can include lowercase letters, digits, underscores, and hyphens between segments.');
      await refused('POST', '/projects', { ...ok, type: 'PROTOTYPE' }, 400, 'Invalid type: PROTOTYPE');
    });

    it('creates a project: named by its coordinates, one revision "Build project structure"', async () => {
      const r = await raw('POST', '/projects', { name: 'Conformance', description: 'the suite', groupId, artifactId: run, tags: ['suite'] });
      assert.equal(r.status, 200, r.text);
      assert.deepEqual(r.json, { projectId: p, name: 'Conformance', description: 'the suite', tags: ['suite'], webUrl: null });
      assert.deepEqual(await client().project(p), r.json);
      const head = await client().revision({ project: p });
      assert.deepEqual(Object.keys(head), REVISION_FIELDS);
      assert.equal(head.message, 'Build project structure');
      assert.match(head.committedTimestamp, INSTANT);
      assert.deepEqual(await client().revision({ project: p, revision: 'base' }), head);
      assert.deepEqual(await client().revision({ project: p, revision: head.id }), head);
    });

    it('serves its configuration in upstream\'s order, nulls written', async () => {
      const r = await raw('GET', `/projects/${P}/configuration`);
      assert.equal(r.status, 200, r.text);
      const c = r.json as Record<string, unknown>;
      assert.deepEqual(Object.keys(c), CONFIG_FIELDS);
      assert.equal(c['projectId'], p);
      assert.equal(c['projectType'], 'MANAGED');
      assert.deepEqual(c['projectStructureVersion'], { version: 13, extensionVersion: null });
      assert.equal(c['groupId'], groupId);
      assert.equal(c['artifactId'], run);
      assert.deepEqual(c['projectDependencies'], []);
    });

    it('lists projects by search, and refuses a negative limit', async () => {
      assert.ok((await client().projects({ search: 'conformance' })).some((x) => x.projectId === p));
      assert.deepEqual(await client().projects({ limit: 0 }), []);
      await refused('GET', '/projects?limit=-1', undefined, 400, 'Invalid limit: -1');
    });

    it('answers a missing project 404, in the error envelope', async () => {
      const e = await refused('GET', `/projects/${encodeURIComponent(`${groupId}:nope`)}`, undefined, 404, `Unknown project: ${groupId}:nope`);
      assert.deepEqual(Object.keys(e), ['code', 'message', 'timestamp']);
      assert.equal(e['code'], 404);
      assert.match(String(e['timestamp']), INSTANT);
    });

    it('creates a workspace from the project line: idempotent, listed, 404 when missing', async () => {
      await refused('POST', `/projects/${P}/workspaces/..bad`, undefined, 400,
        'Invalid workspace id: "..bad". A workspace id must be a non-empty string consisting of characters from the following set: {a-z, A-Z, 0-9, _, ., -}. The id may not contain ".." and may not start or end with \'.\' or \'-\'.');
      const w = await client().createWorkspace(p, 'w1');
      assert.deepEqual(w, { projectId: p, userId: target().user, workspaceId: 'w1' });
      assert.deepEqual(await client().createWorkspace(p, 'w1'), w);
      assert.deepEqual(await client().workspace(p, 'w1'), w);
      assert.ok((await client().workspaces(p)).some((x) => x.workspaceId === 'w1'));
      await refused('GET', `/projects/${P}/workspaces/nope`, undefined, 404, `Unknown: user workspace nope of project ${p}`);
      assert.equal(await client().outdated(p, 'w1'), false);
      assert.equal(await client().inConflictResolutionMode(p, 'w1'), false);
      const line = await client().revision({ project: p });
      assert.deepEqual(await client().revision({ project: p, workspace: 'w1', revision: 'BASE' }), line);
      assert.deepEqual(await client().revision({ project: p, workspace: 'w1' }), line);
    });

    let saved: Revision;

    it('saves text: one element per file, comments kept, the new revision answered', async () => {
      const base = await client().revision({ project: p, workspace: 'w1' });
      saved = await client().performPureChanges(p, 'w1', {
        message: 'add the party', revisionId: base.id,
        changes: [create('demo::party::Person', PERSON), create('demo::party::Address', ADDRESS), create('demo::types::Country', COUNTRY)],
      });
      assert.deepEqual(Object.keys(saved), REVISION_FIELDS);
      assert.equal(saved.message, 'add the party');
      assert.equal(saved.authorName, target().user);
      assert.notEqual(saved.id, base.id);
      assert.deepEqual(await client().revision({ project: p, workspace: 'w1' }), saved);
      assert.deepEqual(await client().revision({ project: p, workspace: 'w1', revision: 'BASE' }), base);
      // the text comes back exactly as written, comment and all, by path
      assert.deepEqual(await client().pure({ project: p, workspace: 'w1' }), [
        { path: 'demo::party::Address', pureCode: ADDRESS },
        { path: 'demo::party::Person', pureCode: PERSON },
        { path: 'demo::types::Country', pureCode: COUNTRY },
      ]);
      assert.deepEqual(await client().pureFile({ project: p, workspace: 'w1' }, 'demo::party::Person'), { path: 'demo::party::Person', pureCode: PERSON });
      // the project line has not moved (no review lands it)
      assert.deepEqual(await client().pure({ project: p }), []);
    });

    it('derives upstream\'s entities from the text', async () => {
      const all = await client().entities({ project: p, workspace: 'w1' });
      assert.deepEqual(all.map((e) => [e.path, e.classifierPath]), [
        ['demo::party::Address', 'meta::pure::metamodel::type::Class'],
        ['demo::party::Person', 'meta::pure::metamodel::type::Class'],
        ['demo::types::Country', 'meta::pure::metamodel::type::Enumeration'],
      ]);
      const person = await client().entity({ project: p, workspace: 'w1' }, 'demo::party::Person');
      assert.deepEqual(Object.keys(person), ['path', 'classifierPath', 'content']);
      assert.equal(person.content['_type'], 'class');
      assert.equal(person.content['package'], 'demo::party');
      assert.equal(person.content['name'], 'Person');
      // at a revision, by id and by alias
      assert.deepEqual((await client().entities({ project: p, workspace: 'w1', revision: saved.id })).length, 3);
      assert.deepEqual(await client().entities({ project: p, workspace: 'w1', revision: 'BASE' }), []);
    });

    it('filters entities as upstream does', async () => {
      const where = { project: p, workspace: 'w1' };
      const paths = async (f: Parameters<SdlcClient['entities']>[1]): Promise<string[]> => (await client().entities(where, f)).map((e) => e.path);
      assert.deepEqual(await paths({ package: ['demo::party'] }), ['demo::party::Address', 'demo::party::Person']);
      assert.deepEqual(await paths({ package: ['demo'] }), ['demo::party::Address', 'demo::party::Person', 'demo::types::Country']);
      assert.deepEqual(await paths({ package: ['demo'], includeSubPackages: false }), []);
      assert.deepEqual(await paths({ name: 'pers' }), ['demo::party::Person']);
      assert.deepEqual(await paths({ classifierPath: ['meta::pure::metamodel::type::Enumeration'] }), ['demo::types::Country']);
    });

    it('answers a missing entity 404 in upstream\'s words', async () => {
      await refused('GET', `/projects/${P}/workspaces/w1/entities/demo::party::Nope`, undefined, 404,
        `Unknown entity demo::party::Nope for user workspace w1 of project ${p}`);
      await refused('GET', `/projects/${P}/workspaces/w1/revisions/BASE/pure/demo::party::Person`, undefined, 404,
        `Unknown entity demo::party::Person for revision BASE of user workspace w1 of project ${p}`);
      await refused('GET', `/projects/${P}/revisions/0000000000000000000000000000000000000000`, undefined, 404,
        `Revision 0000000000000000000000000000000000000000 is unknown for project ${p}`);
    });

    it('refuses bad changes all at once, in upstream\'s layout', async () => {
      await refused('POST', `/projects/${P}/workspaces/w1/pureChanges`, '', 400, 'Input required to perform entity changes');
      await refused('POST', `/projects/${P}/workspaces/w1/pureChanges`, { changes: [] }, 400, 'message may not be null');
      await refused('POST', `/projects/${P}/workspaces/w1/pureChanges`, {
        message: 'bad',
        changes: [
          { path: 'demo::x::A', pureCode: 'Class demo::x::A {}' },
          { type: 'CREATE', path: 'demo::x::B' },
          { type: 'CREATE', path: 'meta::x::C', pureCode: 'Class meta::x::C {}' },
          create('demo::x::D', 'Class demo::x::Other {}'),
          create('demo::x::E', 'import demo::types::*;\nClass demo::x::E {}'),
          create('demo::x::F', 'Class demo::x::F {}\nClass demo::x::G {}'),
          { type: 'DELETE', path: 'demo::party::Person', pureCode: 'x' },
        ],
      }, 400, [
        'There are entity change errors:',
        '\tEntity change #1 (<PureChange type=null path=demo::x::A>):',
        '\t\tMissing entity change type',
        '\tEntity change #2 (<PureChange type=CREATE path=demo::x::B>):',
        '\t\tMissing Pure code',
        '\tEntity change #3 (<PureChange type=CREATE path=meta::x::C>):',
        '\t\tInvalid entity path: meta::x::C',
        '\tEntity change #4 (<PureChange type=CREATE path=demo::x::D>):',
        '\t\tMismatch between entity path ("demo::x::D") and the element\'s path ("demo::x::Other")',
        '\tEntity change #5 (<PureChange type=CREATE path=demo::x::E>):',
        '\t\tImports in Pure files are not currently supported',
        '\tEntity change #6 (<PureChange type=CREATE path=demo::x::F>):',
        '\t\tExpected one element, found 2',
        '\tEntity change #7 (<PureChange type=DELETE path=demo::party::Person>):',
        '\t\tUnexpected Pure code',
      ].join('\n'));
    });

    it('refuses text the grammar cannot read, naming the change', async () => {
      const r = await raw('POST', `/projects/${P}/workspaces/w1/pureChanges`, { message: 'bad', changes: [create('demo::x::A', 'Class demo::x::A {')] });
      assert.equal(r.status, 400, r.text);
      assert.match(String((r.json as { message: string }).message),
        /^There are entity change errors:\n\tEntity change #1 \(<PureChange type=CREATE path=demo::x::A>\):\n\t\t\S/);
    });

    it('holds the revision lock: a stale save is 409, in upstream\'s words', async () => {
      const base = await client().revision({ project: p, workspace: 'w1', revision: 'BASE' });
      await refused('POST', `/projects/${P}/workspaces/w1/pureChanges`, { message: 'stale', revisionId: base.id, changes: [create('demo::x::A', 'Class demo::x::A {}')] },
        409, `Expected revision ${base.id} of user workspace w1 of project ${p} to be at revision ${base.id}; instead it was at revision ${saved.id}`);
    });

    it('applies upstream\'s operation rules: no duplicate, nothing missing, the same text a no-op', async () => {
      await refused('POST', `/projects/${P}/workspaces/w1/pureChanges`, { message: 'again', changes: [create('demo::party::Person', PERSON)] },
        500, 'Unable to handle operation <PureChange type=CREATE path=demo::party::Person>: entity "demo::party::Person" already exists');
      await refused('POST', `/projects/${P}/workspaces/w1/pureChanges`, { message: 'gone', changes: [{ type: 'DELETE', path: 'demo::party::Nope' }] },
        500, 'Unable to handle operation <PureChange type=DELETE path=demo::party::Nope>: could not find entity "demo::party::Nope"');
      const same = await raw('POST', `/projects/${P}/workspaces/w1/pureChanges`, { message: 'same', changes: [{ type: 'MODIFY', path: 'demo::party::Person', pureCode: PERSON }] });
      assert.equal(same.status, 204, same.text);
      const none = await raw('POST', `/projects/${P}/workspaces/w1/pureChanges`, { message: 'none', changes: [] });
      assert.equal(none.status, 204, none.text);
      assert.deepEqual(await client().revision({ project: p, workspace: 'w1' }), saved);
    });

    it('modifies and deletes, each a revision on the last', async () => {
      const edited = PERSON.replace('age: Integer[0..1];', 'age: Integer[0..1];\n  email: String[*];');
      const modified = await client().performPureChanges(p, 'w1', { message: 'email', revisionId: saved.id, changes: [{ type: 'MODIFY', path: 'demo::party::Person', pureCode: edited }] });
      assert.equal((await client().pureFile({ project: p, workspace: 'w1' }, 'demo::party::Person')).pureCode, edited);
      const props = (await client().entity({ project: p, workspace: 'w1' }, 'demo::party::Person')).content['properties'] as { name: string }[];
      assert.deepEqual(props.map((x) => x.name), ['name', 'age', 'email']);
      await client().performPureChanges(p, 'w1', { message: 'drop', revisionId: modified.id, changes: [{ type: 'DELETE', path: 'demo::party::Address' }] });
      assert.deepEqual((await client().pure({ project: p, workspace: 'w1' })).map((f) => f.path), ['demo::party::Person', 'demo::types::Country']);
      // the history keeps what was
      assert.equal((await client().pure({ project: p, workspace: 'w1', revision: saved.id })).length, 3);
    });

    it('refuses what the client cannot ask for in upstream\'s words', async () => {
      await refused('GET', '/nowhere', undefined, 404, 'HTTP 404 Not Found');
      const bad = await refused('POST', `/projects/${P}/workspaces/w1/pureChanges`, '{not json', 400, 'Unable to process JSON');
      assert.equal(typeof bad['details'], 'string');
      await assert.rejects(client().workspace(p, 'nope'), (e: unknown) => e instanceof SdlcError && e.status === 404
        && e.message === `Unknown: user workspace nope of project ${p}`);
    });

    it('deletes a workspace, and deleting one that is not there succeeds', async () => {
      await client().createWorkspace(p, 'w2');
      await client().deleteWorkspace(p, 'w2');
      await refused('GET', `/projects/${P}/workspaces/w2`, undefined, 404, `Unknown: user workspace w2 of project ${p}`);
      const again = await raw('DELETE', `/projects/${P}/workspaces/w2`);
      assert.equal(again.status, 204, again.text);
    });
  });
}
