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

    it('keeps ids differing only in case as one: a repository on macOS or Windows would (re-review D)', async () => {
      await refused('POST', `/projects/${P}/workspaces/W1`, undefined, 409,
        `Error creating user workspace W1 of project ${p}: workspace w1 already exists, and ids differing only in case are one`);
      const upper = groupId.replace(/^org/, 'Org');
      await refused('POST', '/projects', { name: 'Shout', description: '', groupId: upper, artifactId: run }, 409,
        `Failed to create project: Shout: a project with coordinates ${p} already exists`);
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

    // ---- the loop: review, commit, version (slice 2) ----

    const P2 = `${groupId}:${run}-loop`;
    const E2 = encodeURIComponent(P2);
    const loopProject = async (): Promise<void> => {
      await client().createProject({ name: 'Loop', description: '', groupId, artifactId: `${run}-loop` });
    };

    it('reviews a workspace and commits it onto the project line, as a merge, deleting the workspace', async () => {
      await loopProject();
      await client().createWorkspace(P2, 'feature');
      const saved = await client().performPureChanges(P2, 'feature', { message: 'add types', changes: [create('demo::types::Country', COUNTRY)] });
      await refused('POST', `/projects/${E2}/reviews`, '', 400, 'Input required to create review');
      await refused('POST', `/projects/${E2}/reviews`, { workspaceId: 'feature', workspaceType: 'USER', description: '' }, 400, 'title may not be null');
      await refused('POST', `/projects/${E2}/reviews`, { workspaceId: 'nope', workspaceType: 'USER', title: 't', description: '' }, 404,
        `Unknown: user workspace nope of project ${P2}`);
      const r = await raw('POST', `/projects/${E2}/reviews`, { workspaceId: 'feature', workspaceType: 'USER', title: 'Add types', description: 'd' });
      assert.equal(r.status, 200, r.text);
      const review = r.json as Record<string, unknown>;
      assert.deepEqual(Object.keys(review), ['id', 'projectId', 'workspaceId', 'workspaceType', 'title', 'description', 'createdAt',
        'lastUpdatedAt', 'closedAt', 'committedAt', 'state', 'author', 'commitRevisionId', 'webURL', 'labels']);
      assert.equal(review['state'], 'OPEN');
      assert.equal(review['workspaceId'], 'feature');
      assert.match(String(review['createdAt']), INSTANT);
      const id = String(review['id']);
      // a second open review of the same workspace
      await refused('POST', `/projects/${E2}/reviews`, { workspaceId: 'feature', title: 'again', description: '' }, 409,
        `Error submitting changes from user workspace feature of project ${P2} for review: an open review already exists for it: ${id}`);
      // Studio's question: the open review holding the workspace's head
      const found = await raw('GET', `/projects/${E2}/reviews?state=OPEN&revisionIds=${saved.id}&revisionIds=${saved.id}&limit=1`);
      assert.deepEqual((found.json as { id: string }[]).map((x) => x.id), [id]);
      await refused('GET', `/projects/${E2}/reviews/x1`, undefined, 400, 'Invalid id: x1');
      await refused('GET', `/projects/${E2}/reviews/999`, undefined, 404, `Unknown review in project ${P2}: 999`);
      await refused('POST', `/projects/${E2}/reviews/${id}/reopen`, undefined, 409, 'Review is not closed (state: open)');
      const approved = await raw('POST', `/projects/${E2}/reviews/${id}/approve`);
      assert.equal(approved.status, 200, approved.text);
      assert.deepEqual((await raw('GET', `/projects/${E2}/reviews/${id}/approval`)).json, { approvedBy: [{ name: 'Local User', userId: target().user }] });
      assert.deepEqual(await client().approval(P2, id), { approvedBy: [{ name: 'Local User', userId: target().user }] });
      await refused('POST', `/projects/${E2}/reviews/${id}/commit`, {}, 400, 'message may not be null');
      const committed = await raw('POST', `/projects/${E2}/reviews/${id}/commit`, { message: 'Add types [review]' });
      assert.equal(committed.status, 200, committed.text);
      const c = committed.json as Record<string, unknown>;
      assert.equal(c['state'], 'COMMITTED');
      assert.match(String(c['committedAt']), INSTANT);
      // the line now holds the file, as one merge commit with the review's message; the workspace is gone
      const line = await client().revision({ project: P2 });
      assert.equal(line.id, c['commitRevisionId']);
      assert.equal(line.message, 'Add types [review]');
      assert.deepEqual(await client().pure({ project: P2 }), [{ path: 'demo::types::Country', pureCode: COUNTRY }]);
      await refused('GET', `/projects/${E2}/workspaces/feature`, undefined, 404, `Unknown: user workspace feature of project ${P2}`);
      await refused('POST', `/projects/${E2}/reviews/${id}/commit`, { message: 'again' }, 409, 'Review is not open (state: committed)');
      // listed by state, and the committed review still answers for the commits it brought
      const committedList = await raw('GET', `/projects/${E2}/reviews?state=COMMITTED&revisionIds=${saved.id}&limit=1`);
      assert.deepEqual((committedList.json as { id: string }[]).map((x) => x.id), [id]);
    });

    it('closes and reopens a review', async () => {
      await client().createWorkspace(P2, 'side');
      await client().performPureChanges(P2, 'side', { message: 'add address', changes: [create('demo::party::Address', ADDRESS)] });
      const r = await raw('POST', `/projects/${E2}/reviews`, { workspaceId: 'side', title: 'Address', description: '' });
      const id = String((r.json as { id: string }).id);
      assert.equal(((await raw('POST', `/projects/${E2}/reviews/${id}/reject`)).json as { state: string }).state, 'CLOSED');
      await refused('POST', `/projects/${E2}/reviews/${id}/close`, undefined, 409, 'Review is not open (state: closed)');
      assert.equal(((await raw('POST', `/projects/${E2}/reviews/${id}/reopen`)).json as { state: string }).state, 'OPEN');
      await raw('POST', `/projects/${E2}/reviews/${id}/close`);
    });

    it('will not commit a review that would leave the project line not compiling, or that conflicts', async () => {
      await client().createWorkspace(P2, 'broken');
      await client().performPureChanges(P2, 'broken', { message: 'bad', changes: [create('demo::party::Order', 'Class demo::party::Order\n{\n  country: demo::types::Nope[1];\n}\n')] });
      const r = await raw('POST', `/projects/${E2}/reviews`, { workspaceId: 'broken', title: 'Bad', description: '' });
      const id = String((r.json as { id: string }).id);
      const refusal = await raw('POST', `/projects/${E2}/reviews/${id}/commit`, { message: 'bad' });
      assert.equal(refusal.status, 409, refusal.text);
      assert.match(String((refusal.json as { message: string }).message), new RegExp(`^Review ${id} in project ${P2.replace(/\./g, '\\.')} is not in a committable state: the project would not compile: .*demo::types::Nope`));
      await raw('POST', `/projects/${E2}/reviews/${id}/close`);
      // two workspaces change the same file: the first lands, the second conflicts
      for (const w of ['one', 'two']) await client().createWorkspace(P2, w);
      for (const [w, value] of [['one', 'GB, US, FR'], ['two', 'GB, US, DE']]) {
        await client().performPureChanges(P2, w!, { message: w!, changes: [{ type: 'MODIFY', path: 'demo::types::Country', pureCode: COUNTRY.replace('GB, US', value!) }] });
      }
      const one = String(((await raw('POST', `/projects/${E2}/reviews`, { workspaceId: 'one', title: 'one', description: '' })).json as { id: string }).id);
      const two = String(((await raw('POST', `/projects/${E2}/reviews`, { workspaceId: 'two', title: 'two', description: '' })).json as { id: string }).id);
      assert.equal((await raw('POST', `/projects/${E2}/reviews/${one}/commit`, { message: 'one' })).status, 200);
      await refused('POST', `/projects/${E2}/reviews/${two}/commit`, { message: 'two' }, 409,
        `Could not commit review ${two} in project ${P2} because of a conflict: the project line changed the same files since the workspace was made: demo/types/Country.pure`);
    });

    it("updates a workspace onto the line's head: UPDATED (its commits replayed), NO_OP, or CONFLICT left as it was", async () => {
      const line = await client().revision({ project: P2 });
      // 'side' was made before 'one' landed: rebased, its own commit replayed on the line's head, author and message kept
      const before = await client().revision({ project: P2, workspace: 'side' });
      assert.equal(await client().outdated(P2, 'side'), true);
      const r = await raw('POST', `/projects/${E2}/workspaces/side/update`);
      assert.equal(r.status, 200, r.text);
      const report = r.json as Record<string, unknown>;
      assert.deepEqual(Object.keys(report), ['status', 'workspaceMergeBaseRevisionId', 'workspaceRevisionId']);
      assert.equal(report['status'], 'UPDATED');
      assert.equal(report['workspaceMergeBaseRevisionId'], line.id);
      const after = await client().revision({ project: P2, workspace: 'side' });
      assert.equal(after.id, report['workspaceRevisionId']);
      assert.equal(after.message, before.message);
      assert.equal(after.authorName, before.authorName);
      assert.equal(await client().outdated(P2, 'side'), false);
      assert.deepEqual(await client().revision({ project: P2, workspace: 'side', revision: 'BASE' }), line);
      assert.deepEqual((await client().pure({ project: P2, workspace: 'side' })).map((f) => f.path).sort(), ['demo::party::Address', 'demo::types::Country']);
      // again: nothing to do
      assert.deepEqual(await client().updateWorkspace(P2, 'side'), { status: 'NO_OP', workspaceMergeBaseRevisionId: line.id, workspaceRevisionId: after.id });
      // 'two' changed the file 'one' changed, differently: a conflict, named, and the workspace as it was
      const two = await client().revision({ project: P2, workspace: 'two' });
      const conflict = await client().updateWorkspace(P2, 'two');
      assert.equal(conflict.status, 'CONFLICT');
      assert.deepEqual(conflict.conflicts, ['demo/types/Country.pure']);
      assert.equal(conflict.workspaceRevisionId, two.id);
      assert.equal((await client().revision({ project: P2, workspace: 'two' })).id, two.id);
      await refused('POST', `/projects/${E2}/workspaces/nope/update`, undefined, 404, `Unknown: user workspace nope of project ${P2}`);
    });

    it('cuts versions on the project line, numbered from the latest, and reads a version\'s files', async () => {
      const none = await raw('GET', `/projects/${E2}/versions/latest`);
      assert.equal(none.status, 204, none.text);
      await refused('POST', `/projects/${E2}/versions`, '', 400, 'Input required to create version');
      const line = await client().revision({ project: P2 });
      const v1 = await raw('POST', `/projects/${E2}/versions`, { versionType: 'MINOR', notes: 'first' });
      assert.equal(v1.status, 200, v1.text);
      assert.deepEqual(v1.json, { id: { majorVersion: 0, minorVersion: 1, patchVersion: 0 }, projectId: P2, revisionId: line.id, notes: 'first' });
      const v2 = await raw('POST', `/projects/${E2}/versions`, { versionType: 'PATCH', revisionId: line.id });
      assert.deepEqual((v2.json as { id: unknown }).id, { majorVersion: 0, minorVersion: 1, patchVersion: 1 });
      await refused('POST', `/projects/${E2}/versions`, { versionType: 'MAJOR', revisionId: 'abc' }, 400, `Revision abc is unknown in project ${P2}`);
      const list = (await raw('GET', `/projects/${E2}/versions`)).json as { id: { patchVersion: number } }[];
      assert.deepEqual(list.map((v) => v.id.patchVersion), [1, 0]);
      assert.deepEqual(((await raw('GET', `/projects/${E2}/versions/latest`)).json as { id: unknown }).id, { majorVersion: 0, minorVersion: 1, patchVersion: 1 });
      await refused('GET', `/projects/${E2}/versions/1.0`, undefined, 400, 'Invalid version string: "1.0"');
      await refused('GET', `/projects/${E2}/versions/9.9.9`, undefined, 404, `Version 9.9.9 is unknown for project ${P2}`);
      assert.equal(((await raw('GET', `/projects/${E2}/versions/0.1.0/pure`)).json as unknown[]).length, 1);
      assert.deepEqual(((await raw('GET', `/projects/${E2}/versions/0.1.0/entities`)).json as { path: string }[]).map((e) => e.path), ['demo::types::Country']);
      await refused('GET', `/projects/${E2}/versions/0.1.0/entities/demo::x::Y`, undefined, 404, `Unknown entity demo::x::Y for version 0.1.0 of project ${P2}`);
    });

    it('a project depends on another\'s version: its configuration names it, and the gate compiles with it', async () => {
      const P3 = `${groupId}:${run}-app`;
      await client().createProject({ name: 'App', description: '', groupId, artifactId: `${run}-app` });
      await client().createWorkspace(P3, 'w');
      await refused('POST', `/projects/${encodeURIComponent(P3)}/workspaces/w/configuration`, {}, 400, 'message may not be null');
      const changed = await raw('POST', `/projects/${encodeURIComponent(P3)}/workspaces/w/configuration`, {
        message: 'depend on the loop', projectDependenciesToAdd: [{ projectId: P2, versionId: '0.1.1' }],
      });
      assert.equal(changed.status, 200, changed.text);
      assert.deepEqual((await client().configuration({ project: P3, workspace: 'w' })).projectDependencies, [{ projectId: P2, versionId: '0.1.1' }]);
      await client().performPureChanges(P3, 'w', { message: 'use it', changes: [create('demo::app::Customer', 'Class demo::app::Customer\n{\n  country: demo::types::Country[1];\n}\n')] });
      const id = String(((await raw('POST', `/projects/${encodeURIComponent(P3)}/reviews`, { workspaceId: 'w', title: 'use', description: '' })).json as { id: string }).id);
      const committed = await raw('POST', `/projects/${encodeURIComponent(P3)}/reviews/${id}/commit`, { message: 'use' });
      assert.equal(committed.status, 200, committed.text);
      const v = await raw('POST', `/projects/${encodeURIComponent(P3)}/versions`, { versionType: 'MAJOR' });
      assert.deepEqual((v.json as { id: unknown }).id, { majorVersion: 1, minorVersion: 0, patchVersion: 0 });
    });

    it('commits a review against the MERGED project.json: the line\'s new dependency counts (review finding 5)', async () => {
      const P4 = `${groupId}:${run}-merge`;
      const E4 = encodeURIComponent(P4);
      await client().createProject({ name: 'Merge', description: '', groupId, artifactId: `${run}-merge` });
      // B is made first, from the line before A's dependency
      await client().createWorkspace(P4, 'b');
      await client().createWorkspace(P4, 'a');
      await raw('POST', `/projects/${E4}/workspaces/a/configuration`, { message: 'dep', projectDependenciesToAdd: [{ projectId: P2, versionId: '0.1.1' }] });
      await client().performPureChanges(P4, 'a', { message: 'use', changes: [create('demo::m::UsesCountry', 'Class demo::m::UsesCountry\n{\n  c: demo::types::Country[1];\n}\n')] });
      const a = String(((await raw('POST', `/projects/${E4}/reviews`, { workspaceId: 'a', title: 'a', description: '' })).json as { id: string }).id);
      assert.equal((await raw('POST', `/projects/${E4}/reviews/${a}/commit`, { message: 'a' })).status, 200);
      await client().performPureChanges(P4, 'b', { message: 'other', changes: [create('demo::m::Other', 'Class demo::m::Other\n{\n  n: String[1];\n}\n')] });
      const b = String(((await raw('POST', `/projects/${E4}/reviews`, { workspaceId: 'b', title: 'b', description: '' })).json as { id: string }).id);
      const committed = await raw('POST', `/projects/${E4}/reviews/${b}/commit`, { message: 'b' });
      assert.equal(committed.status, 200, committed.text);
      assert.deepEqual((await client().pure({ project: P4 })).map((f) => f.path), ['demo::m::Other', 'demo::m::UsesCountry']);
    });

    it('refuses ids that are not ids before they reach storage (review finding 1)', async () => {
      // a project id must be groupId:artifactId; anything else -- separators, `..` -- is refused, never looked up
      // (a bare `..` segment, even escaped, is resolved by the URL itself before it is sent)
      for (const bad of ['..%2F..%2Fx:y', 'a%2Fb:c', 'x:..%2F..', '..%5C..%5Cx:y']) {
        const r = await raw('GET', `/projects/${bad}`);
        assert.equal(r.status, 400, `${bad}: ${r.text}`);
        assert.match(String((r.json as { message: string }).message), /^Invalid project id: "/);
        assert.equal((await raw('DELETE', `/projects/${bad}/workspaces/w`)).status, 400, bad);
      }
      // a workspace id is checked on every route, the delete included; git's `.lock` and staging's `.tmp` are refused
      for (const bad of ['..%2F..%2Fx', 'a%2Fb', 'x.lock', 'x.tmp']) {
        const r = await raw('DELETE', `/projects/${E2}/workspaces/${bad}`);
        assert.equal(r.status, 400, `${bad}: ${r.text}`);
        assert.match(String((r.json as { message: string }).message), /^Invalid workspace id: "/);
      }
      await refused('POST', `/projects/${E2}/workspaces/x.lock`, undefined, 400,
        'Invalid workspace id: "x.lock". A workspace id must be a non-empty string consisting of characters from the following set: {a-z, A-Z, 0-9, _, ., -}. The id may not contain ".." and may not start or end with \'.\' or \'-\'.');
      // a revision is an alias or a commit id
      await refused('GET', `/projects/${E2}/revisions/..%2F..%2Fx`, undefined, 404, `Revision ../../x is unknown for project ${P2}`);
      await refused('GET', `/projects/${E2}/revisions/a`, undefined, 404, `Revision a is unknown for project ${P2}`);
      // a malformed escape is the client's error
      assert.equal((await raw('GET', `/projects/${E2}%zz`)).status, 400);
    });

    it('lists a history newest first, with upstream\'s limit rules', async () => {
      const all = (await raw('GET', `/projects/${E2}/revisions`)).json as { id: string; message: string }[];
      assert.equal(all[all.length - 1]!.message, 'Build project structure');
      assert.equal(all[0]!.id, (await client().revision({ project: P2 })).id);
      assert.deepEqual((await raw('GET', `/projects/${E2}/revisions?limit=0`)).json, []);
      assert.equal(((await raw('GET', `/projects/${E2}/revisions?limit=1`)).json as unknown[]).length, 1);
      await refused('GET', `/projects/${E2}/revisions?limit=-1`, undefined, 400, 'Invalid limit: -1');
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
