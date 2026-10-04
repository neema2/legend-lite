// The dogfood model (design S18) published through the page's SDLC: every release passes the compile
// gate with its dependencies, the diamond resolves nearest-wins, and a workspace on trading compiles in
// the tab with its dependencies' files from Depot.

import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { describe, it } from 'node:test';

import { loadDemoProjects, type Manifest } from '../src/app/demo-projects.ts';
import { Workspace } from '../src/model/workspace.ts';
import { compiler, pageSdlcAndDepot } from './modules.ts';

const dir = new URL('../demo/projects/', import.meta.url);
const manifest = JSON.parse(await readFile(new URL('manifest.json', dir), 'utf8')) as Manifest;
const read = (file: string): Promise<string> => readFile(new URL(file, dir), 'utf8');

describe('the demo projects', () => {
  it('publish, every release through the compile gate, and a trading workspace compiles with its dependencies', async () => {
    const { client, depot } = await pageSdlcAndDepot();
    const steps: string[] = [];
    await loadDemoProjects(client, compiler, manifest, read, (m) => steps.push(m));
    assert.equal(steps.length, manifest.steps.length);
    const g = manifest.groupId;
    assert.deepEqual((await client.versions(`${g}:types`)).map((v) => `${v.id.majorVersion}.${v.id.minorVersion}.${v.id.patchVersion}`), ['1.1.0', '1.0.0']);
    // the diamond: types is two deep under both; instruments sorts first, so its 1.1.0 wins
    const closure = await depot.dependencyEntities([{ groupId: g, artifactId: 'trading', versionId: '1.0.0' }]);
    assert.deepEqual(Object.fromEntries(closure.map((v) => [v.artifactId, v.versionId])),
      { trading: '1.0.0', instruments: '1.0.0', party: '1.0.0', types: '1.1.0' });
    // loading again changes nothing
    await loadDemoProjects(client, compiler, manifest, read, (m) => steps.push(m));
    assert.equal(steps.length, manifest.steps.length);
    // a workspace on trading: its files and its dependencies' compile together in the tab
    await client.createWorkspace(`${g}:trading`, 'dev');
    const ws = new Workspace(client, compiler, `${g}:trading`, 'dev', depot);
    await ws.load();
    assert.deepEqual(await ws.compile(), []);
    const key = ws.add('Class demo::trading::Desk\n{\n  base: demo::types::Currency[1];\n  bad: demo::party::Nope[1];\n}\n');
    const problems = await ws.compile();
    assert.equal(problems.length, 1);
    assert.equal(problems[0]!.key, key);
    assert.match(problems[0]!.message, /demo::party::Nope/);
  });
});
