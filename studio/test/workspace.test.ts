// Studio's workspace model over the real in-page SDLC and the real compiler: what a save sends (text
// read for its element), renames as delete + create, the lock, and compile errors attached to the file
// and line they come from.

import assert from 'node:assert/strict';
import { before, describe, it } from 'node:test';

import type { SdlcClient } from '../../sdlc-client/src/client.ts';
import { Workspace } from '../src/model/workspace.ts';
import { compiler, pageSdlc } from './modules.ts';

const P = 'org.finos.lite.studio:test';
let client: SdlcClient;

before(async () => {
  client = await pageSdlc();
  await client.createProject({ name: 'Studio test', description: '', groupId: 'org.finos.lite.studio', artifactId: 'test' });
  await client.createWorkspace(P, 'w');
});

describe('a workspace in Studio', () => {
  it('saves a new file as the element its text declares', async () => {
    const ws = new Workspace(client, compiler, P, 'w');
    await ws.load();
    assert.deepEqual(ws.files(), []);
    const key = ws.add('// people\nClass demo::Person\n{\n  name: String[1];\n}\n');
    assert.equal(ws.hasChanges(), true);
    assert.deepEqual((await ws.pending()).changes.map((c) => [c.type, c.path]), [['CREATE', 'demo::Person']]);
    assert.deepEqual(await ws.save('add Person'), []);
    assert.equal(ws.hasChanges(), false);
    assert.equal(ws.revision?.message, 'add Person');
    assert.deepEqual(ws.files().map((f) => f.key), ['demo::Person']);
    void key;
  });

  it('a renamed element is a delete and a create; an edit is a modify', async () => {
    const ws = new Workspace(client, compiler, P, 'w');
    await ws.load();
    ws.add('Enum demo::Kind\n{\n  A, B\n}\n');
    await ws.save('add Kind');
    ws.edit('demo::Person', ws.file('demo::Person')!.text.replace('demo::Person', 'demo::Party'));
    ws.edit('demo::Kind', 'Enum demo::Kind\n{\n  A, B, C\n}\n');
    assert.deepEqual((await ws.pending()).changes.map((c) => [c.type, c.path]),
      [['MODIFY', 'demo::Kind'], ['DELETE', 'demo::Person'], ['CREATE', 'demo::Party']]);
    await ws.save('rename');
    assert.deepEqual(ws.files().map((f) => f.key), ['demo::Kind', 'demo::Party']);
    // a delete undone before the save: the element back as saved, nothing to save
    const saved = ws.file('demo::Kind')!.text;
    ws.remove('demo::Kind');
    assert.deepEqual(ws.removed(), ['demo::Kind']);
    assert.equal(ws.restore('demo::Kind'), 'demo::Kind');
    assert.equal(ws.file('demo::Kind')!.text, saved);
    assert.equal(ws.hasChanges(), false);
    assert.throws(() => ws.restore('demo::Kind'), /is not removed/);
    ws.remove('demo::Kind');
    await ws.save('drop Kind');
    assert.deepEqual(ws.files().map((f) => f.key), ['demo::Party']);
  });

  it('will not save text it cannot read, naming the file', async () => {
    const ws = new Workspace(client, compiler, P, 'w');
    await ws.load();
    const key = ws.add('Class demo::Broken {');
    const problems = await ws.save('broken');
    assert.equal(problems.length, 1);
    assert.equal(problems[0]!.key, key);
    const two = ws.add('Class demo::A {}\nClass demo::B {}');
    ws.remove(key);
    assert.deepEqual(await ws.save('two'), [{ key: two, message: 'Expected one element, found 2' }]);
  });

  it('holds the lock: a save from a stale revision is refused', async () => {
    const stale = new Workspace(client, compiler, P, 'w');
    await stale.load();
    const fresh = new Workspace(client, compiler, P, 'w');
    await fresh.load();
    fresh.add('Class demo::Fresh {}');
    await fresh.save('fresh');
    stale.add('Class demo::Stale {}');
    await assert.rejects(stale.save('stale'), (e: Error) => /to be at revision .*; instead it was at revision/.test(e.message));
  });

  it('compiles the whole workspace, each error on its file and line', async () => {
    const ws = new Workspace(client, compiler, P, 'w');
    await ws.load();
    assert.deepEqual(await ws.compile(), []);
    const key = ws.add('Class demo::Order\n{\n  party: demo::Nope[1];\n}\n');
    const [problem] = await ws.compile();
    assert.equal(problem?.key, key);
    assert.equal(problem?.line, 1);
    assert.match(problem!.message, /demo::Nope/);
    ws.edit(key, 'Class demo::Order\n{\n  id: Integer[1];\n}\nfunction demo::total(): Integer[1]\n{\n  1 + \'x\'\n}\n');
    const body = await ws.compile();
    assert.equal(body.length, 1);
    assert.equal(body[0]!.key, key);
    assert.match(body[0]!.message, /in function 'demo::total'/);
  });
});
