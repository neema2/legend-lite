// FOLLOWING PYTHON'S PAGE BY ITS COMMANDS (src/app/live-follow.ts, src/services/python-host.ts livePage;
// docs/DATACUBE_LIVE_COMMANDS_2026_10_18.md): a page move is followed by its commands, or by opening the page again when
// a frame moved with it; each run of page commands goes to the page, each run of a grid's to that grid -- one closed
// here, or one a frame moved under while it was added, refused so the page opens again -- and the commands are read with
// their exact numbers.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { applyLive, followBy, type LiveTarget } from '../src/app/live-follow.ts';
import type { LiveCommand } from '../src/app/services.ts';
import { leavesOf, type CubeDocument, type FrameSource } from '../src/cube-document.ts';
import type { GridCommand } from '../src/grid-commands.ts';
import type { PageCommand } from '../src/page-commands.ts';
import { pageToJson } from '../src/page-document.ts';
import { askedVersions, livePage } from '../src/services/python-host.ts';
import { toJson } from '../../pure-protocol/src/index.ts';
import { corpus } from './fixtures/pages/make.ts';

const trades = (await corpus())['trades.page.json']!;
const frameCube = (name: string): CubeDocument => ({ ...trades.cubes[0]!.cube, source: { _type: 'frame', name, columns: [] } } as unknown as CubeDocument);
/** A join of two frames' grids (cube-document.ts JoinSource): what it reads is both frames. */
const joinCube = (left: string, right: string): CubeDocument => {
  const input = (name: string) => ({ name, source: { _type: 'frame', name, columns: [] } as FrameSource, query: trades.cubes[0]!.cube.query });
  return { ...trades.cubes[0]!.cube, source: { _type: 'join', name: `${left} ⋈ ${right}`, left: input(left), right: input(right), how: { form: 'append' }, columns: [] } } as unknown as CubeDocument;
};

/** A page that records what it was asked, over `frames` (each frame's read version, which `bump` moves on open). */
function target(o: { grids?: string[]; frames?: Record<string, number>; bump?: string; refuse?: string } = {}) {
  const asked: string[] = [];
  const frames = { ...(o.frames ?? {}) };
  const t: LiveTarget = {
    grids: o.grids ?? ['grid-1'],
    cubes: 1,
    follow: async (commands, makers) => { asked.push(`page ${commands.map((c) => c.kind).join(',')} makers ${[...makers.keys()].join(',')}`); },
    applyGrid: async (grid, commands) => { asked.push(`grid ${grid} ${commands.map((c) => c.kind).join(',')}`); return o.refuse ?? null; },
    open: async (document, grid) => {
      asked.push(`open ${grid}`);
      if (o.bump !== undefined && leavesOf(document.source).some((l) => l._type === 'frame' && l.name === o.bump)) frames[o.bump] = (frames[o.bump] ?? 0) + 1;
      return { make: () => { throw new Error('not made here'); }, discard: async () => { asked.push(`discard ${grid}`); } };
    },
    frameVersion: (frame) => frames[frame],
  };
  return { t, asked };
}
const page = (c: PageCommand): LiveCommand => ({ page: c });
const grid = (id: string, c: GridCommand): LiveCommand => ({ grid: id, command: c });

describe('how a move of the page is followed', () => {
  it('by its commands; opened again when a frame moved with it; by its frames when the page did not move', () => {
    const read = (f: string): number | undefined => ({ trades: 3 } as Record<string, number>)[f];
    assert.equal(followBy({ version: 5, frames: { trades: 3 } }, 4, read), 'commands');
    assert.equal(followBy({ version: 5, frames: { trades: 4 } }, 4, read), 'open', 'a frame moved with the page');
    assert.equal(followBy({ version: 5, frames: { desks: 9 } }, 4, read), 'commands', 'a frame the page never read');
    assert.equal(followBy({ version: 4, frames: { trades: 4 } }, 4, read), 'frames');
  });
});

describe('the commands applied, or refused so the page opens again', () => {
  it('each run of page commands to the page, each run of one grid\'s to that grid, in order', async () => {
    const { t, asked } = target({ grids: ['grid-1', 'grid-2'] });
    await applyLive([
      page({ kind: 'addSheet', id: 'sheet-3', name: null }), page({ kind: 'renamePage', name: 'Q4' }),
      grid('grid-1', { kind: 'group', rows: ['desk'] }), grid('grid-1', { kind: 'pivot', columns: [] }),
      grid('grid-2', { kind: 'group', rows: [] }), page({ kind: 'removeSheet', sheet: 'sheet-3' }),
    ], t);
    assert.deepEqual(asked, ['page addSheet,renamePage makers ', 'grid grid-1 group,pivot', 'grid grid-2 group', 'page removeSheet makers ']);
  });

  it('a grid closed here -- kept off the board for a detached chart -- or a refused command: refused', async () => {
    // (the phase 3 audit's 3) the cube of a grid kept for a detached chart is no grid Python's command is for
    await assert.rejects(applyLive([grid('grid-2', { kind: 'group', rows: ['desk'] })], target({ grids: ['grid-1'] }).t), /grid-2 was closed/);
    await assert.rejects(applyLive([grid('grid-1', { kind: 'group', rows: ['nope'] })], target({ refuse: 'no column nope' }).t), /no column nope/);
  });

  it('a grid added is made by its maker; a frame that moved while it was opened refuses it, and what was opened is let go', async () => {
    const add = (id: string, frame: string): LiveCommand => page({
      kind: 'addGrid', id, document: frameCube(frame), sheet: 'sheet-1', near: null, title: null, columns: null,
    });
    const ok = target({ frames: { trades: 2 } });
    await applyLive([add('grid-5', 'orders')], ok.t);
    assert.deepEqual(ok.asked, ['open grid-5', 'page addGrid makers grid-5']);
    // (the phase 3 audit's 4) the page's other grids over trades read it as it was: opened again, all read it now
    const moved = target({ frames: { trades: 2 }, bump: 'trades' });
    await assert.rejects(applyLive([add('grid-5', 'orders'), add('grid-6', 'trades')], moved.t), /the frame trades moved/);
    assert.deepEqual(moved.asked, ['open grid-5', 'open grid-6', 'discard grid-5', 'discard grid-6'], 'nothing followed; both let go');
  });

  it("a cube its host opened again over its frames (rebuilt): refused before anything is applied, so the page opens again", async () => {
    const { t, asked } = target({ grids: ['grid-1'] });
    await assert.rejects(applyLive([
      page({ kind: 'rebuilt', cube: 'cube-1', source: frameCube('trades').source }), grid('grid-1', { kind: 'group', rows: ['desk'] }),
    ], t), /grid cube-1 was opened again over its source/);
    assert.deepEqual(asked, [], 'nothing followed');
  });

  it('a join of frames reads each: one of them moving while it was opened refuses it (§18.3, leavesOf)', async () => {
    const add = (id: string, document: CubeDocument): LiveCommand => page({
      kind: 'addGrid', id, document, sheet: 'sheet-1', near: null, title: null, columns: null,
    });
    const moved = target({ frames: { trades: 2, desks: 1 }, bump: 'desks' });
    await assert.rejects(applyLive([add('grid-7', joinCube('trades', 'desks'))], moved.t), /the frame desks moved/);
    assert.deepEqual(moved.asked, ['open grid-7', 'discard grid-7']);
  });
});

describe("Python's commands read as the page reads them", () => {
  /** A link answering page.json with `text`. */
  const link = (text: string) => ({
    baseUrl: 'http://engine',
    fetch: (async () => new Response(text, { status: 200 })) as unknown as typeof fetch,
  });

  it("a calculated column's lambda keeps its exact numbers; an added grid's cube too", async () => {
    // (the phase 3 audit's 2) 12.30 stays 12.30, an integer past 2^53 every digit
    const text = `{"version": 3, "commands": [{"grid": "grid-1", "command": {"kind": "calculate", "level": "row", "column":
      {"name": "scaled", "lambda": {"_type": "lambda", "parameters": [], "body": [{"_type": "decimal", "value": 12.30},
      {"_type": "integer", "value": 1180591620717411303424}]}}}}]}`;
    const moved = await livePage(link(text), 'q3').changes(2);
    assert.ok('commands' in moved);
    assert.equal(moved.version, 3);
    const command = (moved.commands[0] as { command: GridCommand }).command;
    assert.equal(command.kind, 'calculate');
    const written = toJson(command);
    assert.match(written, /"value":12\.30/);
    assert.match(written, /"value":1180591620717411303424/);
  });

  it('the page as it is now when the engine says the page, with the ids it has held; a command of no page and no grid refused', async () => {
    const said = await livePage(link(`{"version": 7, "page": ${pageToJson(trades)}, "held": ["grid", "grid-9"]}`), 'q3').changes(2);
    assert.ok('page' in said && said.version === 7 && said.page.name === trades.name);
    assert.deepEqual(said.held, ['grid', 'grid-9'], 'the ids the keeper holds, read with the page (B12)');
    // a page said with no ids held is no page the keeper serves: refused, never taken as holding none
    await assert.rejects(livePage(link(`{"version": 7, "page": ${pageToJson(trades)}}`), 'q3').changes(2), /no ids the page has held/);
    await assert.rejects(livePage(link('{"version": 3, "commands": [{"nope": 1}]}'), 'q3').changes(2), /a command of no page and no grid/);
  });
});

describe('a page open on Python\'s says who it is and the version it shows (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md Z17)', () => {
  // Python keeps the copies the version it shows reads, and every later version's, until it says it moved past them:
  // the keeper's known limit 6, closed
  const asking = () => {
    const urls: string[] = [];
    const link = {
      baseUrl: 'http://engine',
      fetch: (async (url: string) => {
        urls.push(url.replace('http://engine/', ''));
        return new Response(url.includes('version.json') ? '{"version": 4, "frames": {}}' : '{"version": 4, "commands": [], "combined": false}');
      }) as unknown as typeof fetch,
    };
    return { link, urls };
  };

  it('as it asks the versions, follows and writes; asking with no version on screen, it says none', async () => {
    const { link, urls } = asking();
    let shown: number | undefined = 3;
    const follower = { id: 'f00d', shown: () => shown };
    await askedVersions(link, { page: 'q 3' }, follower);
    shown = undefined;
    await askedVersions(link, { page: 'q 3' }, follower);
    const page = livePage(link, 'q 3', 'f00d');
    await page.changes(3);
    await page.report(3, { edits: [] });
    assert.deepEqual(urls, [
      'version.json?page=q%203&follower=f00d&at=3',
      'version.json?page=q%203&follower=f00d',
      'page.json?page=q%203&since=3&follower=f00d',
      'page.json?page=q%203&version=3&follower=f00d',
    ]);
  });
});
