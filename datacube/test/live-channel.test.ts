// THE TAB'S SIDE OF THE PAGE KEEPER (src/app/live-channel.ts; docs/DATACUBE_PAGE_KEEPER_2026_10_20.md): the edits
// made on screen written as commands, as applied there, never the page (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md S4);
// an undo or a redo written whole, a restore (N2), and nothing else; what the screen does while following the other
// editor never taken for its own; writes and follows one at a time, in order -- a follow told while a write is on its
// way waits for it, and sees the version it made; a write the keeper made another page of opening the page as the
// keeper made it; a refused one opening it too, its words said. And a real page (PageApp, its grids CubeApps over a
// gated engine) writing to a keeper that applies its edits by DataCube's one rule (page-rebase.ts), on top of the other
// editor's: both editors' changes on the page it ends with; its undo, a restore, kept only at the version it was made
// at.

import assert from 'node:assert/strict';
import { afterEach, beforeEach, describe, it } from 'node:test';
import { JSDOM } from 'jsdom';

import { CubeApp } from '../src/app.ts';
import { DEFAULT_CONFIGURATION } from '../src/config.ts';
import type { CubeDocument, FileSource } from '../src/cube-document.ts';
import { LiveChannel, type LiveScreen } from '../src/app/live-channel.ts';
import type { LiveCommand, LivePage, LiveWrite } from '../src/app/services.ts';
import { applyPageCommands, idsOf, type PageCommand } from '../src/page-commands.ts';
import { PageApp, type GridMaker } from '../src/page/page-app.ts';
import type { PageDocument } from '../src/page-document.ts';
import { rebaseEdits, type GridApplier } from '../src/page-rebase.ts';
import { CubeRefusal } from '../src/snapshot.ts';
import { GateEngine, SNAPSHOT, StubPlanner, settle } from './cube-fixture.ts';
import { corpus } from './fixtures/pages/make.ts';

const trades = (await corpus())['trades.page.json']!;
const renamed = (page: PageDocument, name: string): PageDocument => ({ ...page, name });

/** A promise held until the test settles it. */
function held<T>(): { promise: Promise<T>; resolve: (v: T) => void; reject: (e: unknown) => void } {
  let resolve!: (v: T) => void;
  let reject!: (e: unknown) => void;
  const promise = new Promise<T>((res, rej) => { resolve = res; reject = rej; });
  return { promise, resolve, reject };
}

type Answer = { readonly version: number; readonly combined: boolean };

/** An editor (the keeper's host) that records each write and answers it as the test says. */
class Editor implements LivePage {
  readonly writes: { at: number; edits?: readonly LiveCommand[]; restore?: PageDocument }[] = [];
  answers: (() => Promise<Answer>)[] = [];
  read(): never { throw new Error('not read here'); }
  changes(): never { throw new Error('not followed here'); }
  report(at: number, write: LiveWrite): Promise<Answer> {
    this.writes.push({ at, ...write });
    const answer = this.answers.shift();
    return answer ? answer() : Promise.resolve({ version: at + 1, combined: false });
  }
}

/** A screen showing `page`, which the test changes; what reopening it did, and each refusal it was told. */
function screen(start: PageDocument): LiveScreen & { now: PageDocument | undefined; reopened: number; refused: string[] } {
  const s = {
    now: start as PageDocument | undefined,
    reopened: 0,
    refused: [] as string[],
    page: () => s.now,
    reopen: async (refused?: string) => {
      s.reopened += 1;
      if (refused !== undefined) s.refused.push(refused);
    },
  };
  return s;
}

/** Every timer due by now fired, and the channel's queue drained. */
async function written(channel: LiveChannel): Promise<void> {
  await new Promise((r) => setTimeout(r, 5));
  await channel.idle();
}

describe('the channel', () => {
  it('writes the edits made on screen, as applied there -- never the page -- and then has none pending', async () => {
    const editor = new Editor();
    const s = screen(trades);
    const channel = new LiveChannel(editor, s, 0);
    channel.opened(1);
    channel.edit({ page: { kind: 'renamePage', name: 'Here' } });
    s.now = renamed(trades, 'Here');
    channel.changed();
    await written(channel);
    assert.deepEqual(editor.writes, [{ at: 1, edits: [{ page: { kind: 'renamePage', name: 'Here' } }] }]);
    assert.equal(channel.shown, 2);
    channel.edit({ grid: 'grid', command: { kind: 'group', rows: ['desk'] } });
    s.now = renamed(trades, 'Here, grouped');
    channel.changed();
    await written(channel);
    assert.deepEqual(editor.writes[1]!.edits, [{ grid: 'grid', command: { kind: 'group', rows: ['desk'] } }], 'only the new edit');
  });

  it('writes an undo or a redo whole, a restore (N2) -- the edits before it in it -- and a change no command made, never', async () => {
    const editor = new Editor();
    const s = screen(trades);
    const channel = new LiveChannel(editor, s, 0);
    channel.opened(1);
    // a change no edit was told for: nothing written (the page is never sent but by an undo or a redo)
    s.now = renamed(trades, 'No command');
    channel.changed();
    await written(channel);
    assert.equal(editor.writes.length, 0, 'no page written whole for a change no command made');
    channel.edit({ page: { kind: 'renamePage', name: 'Here' } });
    channel.restored();
    s.now = renamed(trades, 'Undone');
    channel.changed();
    await written(channel);
    assert.deepEqual(editor.writes, [{ at: 1, restore: renamed(trades, 'Undone') }], 'the page whole, its edits in it');
    // and the next edit is an edit again
    channel.edit({ page: { kind: 'renamePage', name: 'After' } });
    s.now = renamed(trades, 'After');
    channel.changed();
    await written(channel);
    assert.deepEqual(editor.writes[1], { at: 2, edits: [{ page: { kind: 'renamePage', name: 'After' } }] });
    // an undo while following the other editor is that editor's: no restore of the screen's
    await channel.following(async () => { channel.restored(); });
    channel.changed();
    await written(channel);
    assert.equal(editor.writes.length, 2);
  });

  it('takes nothing the screen does while following the other editor for its own', async () => {
    const editor = new Editor();
    const s = screen(trades);
    const channel = new LiveChannel(editor, s, 0);
    channel.opened(1);
    await channel.following(async () => {
      channel.shown = undefined;
      // the other editor's command, applied: the screen tells it, and changes
      channel.edit({ page: { kind: 'renamePage', name: 'Python' } });
      s.now = renamed(trades, 'Python');
      channel.changed();
    });
    channel.shown = 2;
    channel.changed();
    await written(channel);
    assert.equal(editor.writes.length, 0, 'nothing of its own to write -- Python\'s change never written back as the screen\'s');
    // and an edit of its own after: written alone
    channel.edit({ page: { kind: 'renamePage', name: 'Mine' } });
    s.now = renamed(trades, 'Mine');
    channel.changed();
    await written(channel);
    assert.deepEqual(editor.writes.map((w) => w.edits), [[{ page: { kind: 'renamePage', name: 'Mine' } }]]);
  });

  it("keeps the person's own grid edit told while following the other editor, and writes it (the S1 audit's 2)", async () => {
    const editor = new Editor();
    const s = screen(trades);
    const channel = new LiveChannel(editor, s, 0);
    channel.opened(1);
    const grid = trades.views.find((v) => v.kind === 'grid')!.id;
    await channel.following(async () => {
      channel.shown = undefined;
      // the person's own change, landing while the other editor's commands are followed (a grid tells none of the
      // other editor's: CubeApp.applyGridCommands)
      channel.edit({ grid, command: { kind: 'group', rows: ['desk'] } });
      s.now = renamed(trades, 'regrouped on screen');
      channel.changed();
    });
    channel.shown = 2;
    channel.changed();
    await written(channel);
    assert.deepEqual(editor.writes.map((w) => w.edits), [[{ grid, command: { kind: 'group', rows: ['desk'] } }]]);
  });

  // (the browser harness: such a write, made behind a page Python had moved, had the page opened again in place of
  // following Python's command)
  it('writes nothing when nothing was made on screen since it opened', async () => {
    const editor = new Editor();
    const s = screen(trades);
    const channel = new LiveChannel(editor, s, 0);
    channel.opened(1);
    // the page as it opened, changed again by nothing (a view landing, a restore settling)
    channel.changed();
    channel.changed();
    await written(channel);
    assert.equal(editor.writes.length, 0);
  });

  it('runs a follow told while a write is on its way after it, at the version the write made', async () => {
    const editor = new Editor();
    const answer = held<Answer>();
    editor.answers.push(() => answer.promise);
    const s = screen(trades);
    const channel = new LiveChannel(editor, s, 0);
    channel.opened(1);
    channel.edit({ page: { kind: 'renamePage', name: 'Here' } });
    s.now = renamed(trades, 'Here');
    channel.changed();
    await new Promise((r) => setTimeout(r, 5));
    assert.equal(editor.writes.length, 1, 'the write is on its way');
    const started: (number | undefined)[] = [];
    // the version notice of that very write arrives first
    const follow = channel.enqueue(async () => { started.push(channel.shown); });
    await new Promise((r) => setTimeout(r, 5));
    assert.deepEqual(started, [], 'not while the write is on its way');
    answer.resolve({ version: 2, combined: false });
    await follow;
    assert.deepEqual(started, [2], 'after it, at the version it made: its own write is never taken for the other editor\'s');
  });

  it('opens the page as the keeper made it when its edits were made again on top', async () => {
    const editor = new Editor();
    editor.answers.push(async () => ({ version: 3, combined: true }));
    const s = screen(trades);
    const channel = new LiveChannel(editor, s, 0);
    channel.opened(1);
    channel.edit({ page: { kind: 'renamePage', name: 'Here' } });
    s.now = renamed(trades, 'Here');
    channel.changed();
    await written(channel);
    assert.equal(s.reopened, 1);
    assert.equal(channel.shown, undefined, 'at no version until the page is opened again');
  });

  it("says what it drops: a change made while a write the keeper made again on top was on its way (the S4 audit's 7)", async () => {
    const editor = new Editor();
    const answer = held<Answer>();
    editor.answers.push(() => answer.promise);
    const s = screen(trades);
    const channel = new LiveChannel(editor, s, 0);
    channel.opened(1);
    channel.edit({ page: { kind: 'renamePage', name: 'Here' } });
    s.now = renamed(trades, 'Here');
    channel.changed();
    await new Promise((r) => setTimeout(r, 5));
    // made while the write is on its way
    channel.edit({ page: { kind: 'renamePage', name: 'Meanwhile' } });
    answer.resolve({ version: 3, combined: true });
    await channel.idle();
    assert.equal(s.reopened, 1);
    assert.deepEqual(s.refused, ['a change made while the page was written was made on a page its keeper no longer has']);
  });

  it('opens the page as the keeper has it after a refused write, its words said -- never writing the screen\'s whole', async () => {
    const editor = new Editor();
    editor.answers.push(async () => { throw new Error('409: chart-9 is no longer on the page'); });
    const s = screen(trades);
    const channel = new LiveChannel(editor, s, 0);
    channel.opened(1);
    channel.edit({ page: { kind: 'renamePage', name: 'Here' } });
    s.now = renamed(trades, 'Here');
    channel.changed();
    await written(channel);
    assert.deepEqual([s.reopened, s.refused], [1, ['409: chart-9 is no longer on the page']]);
    assert.equal(channel.shown, undefined, 'at no version until the page is opened again');
    channel.opened(2);
    channel.changed();
    await written(channel);
    assert.equal(editor.writes.length, 1, 'nothing of the refused edit written again');
  });

  it('writes nothing once stopped, nor while at no version', async () => {
    const editor = new Editor();
    const s = screen(trades);
    const channel = new LiveChannel(editor, s, 0);
    channel.changed();
    await written(channel);
    channel.opened(1);
    channel.stop();
    s.now = renamed(trades, 'Too late');
    channel.changed();
    await written(channel);
    assert.equal(editor.writes.length, 0);
  });
});

// -- a real page and its keeper ----------------------------------------------------------------------------------------

let dom: JSDOM;
let host: HTMLElement;
let engine: GateEngine;

beforeEach(() => {
  dom = new JSDOM('<!doctype html><div id="p"></div>');
  const g = globalThis as unknown as Record<string, unknown>;
  g['window'] = dom.window;
  g['document'] = dom.window.document;
  g['requestAnimationFrame'] = (cb: FrameRequestCallback) => { cb(0); return 1; };
  g['cancelAnimationFrame'] = () => {};
  host = dom.window.document.getElementById('p') as unknown as HTMLElement;
  const inert: ProxyHandler<object> = {
    get: (_t, key) => (key === 'canvas' ? dom.window.document.createElement('canvas') : key === 'measureText' ? () => ({ width: 0 }) : () => undefined),
    set: () => true,
  };
  (dom.window.HTMLCanvasElement.prototype as unknown as { getContext: () => unknown }).getContext = () => new Proxy({}, inert);
  engine = new GateEngine();
});

let page: PageApp | undefined;
afterEach(async () => {
  await settle();
  page?.dispose();
  page = undefined;
});

const file = (name: string): FileSource => ({
  _type: 'file', name, format: 'csv', size: 10, sha256: name.padEnd(64, '0').slice(0, 64),
  columns: SNAPSHOT.columns.map((c) => ({ name: c.name, type: c.type })),
});

/** A grid's command as the keeper's host applies one -- here a group, setting its rows (the host's grid base is no
 *  part of what this test holds to). */
const applyGrid: GridApplier = async (_page, cube, _id, commands) => {
  let next: CubeDocument = cube;
  for (const c of commands) {
    if (c.kind !== 'group') throw new CubeRefusal(`this keeper makes groups only again, not ${c.kind}`);
    next = { ...next, query: { ...next.query, rows: [...c.rows] } };
  }
  return next;
};

/** A keeper as Python's is (keeper.py settle, restore): a write's edits applied by DataCube's rule to the page as it
 *  is -- made again on top when it moved on since -- and what they make kept; a restore kept whole only at the version
 *  it was made at. */
class Keeper implements LivePage {
  version = 1;
  held: string[];
  /** Each write. */
  readonly written: LiveWrite[] = [];
  page: PageDocument;
  constructor(page: PageDocument) {
    this.page = page;
    this.held = idsOf(page);
  }
  read(): never { throw new Error('opened by the test'); }
  changes(): never { throw new Error('followed by the test'); }
  /** Another editor (Python) writes commands it made at the version now. */
  pythonWrites(commands: readonly PageCommand[]): void {
    const done = applyPageCommands({ page: this.page, held: this.held }, commands);
    this.page = done.draft.page;
    this.held = [...done.draft.held];
    this.version += 1;
  }
  async report(at: number, write: LiveWrite): Promise<Answer> {
    this.written.push(write);
    if ('restore' in write) {
      if (at !== this.version) throw new Error('409: the page moved on: an undo is taken only at the version it was made at');
      this.page = write.restore;
      this.held = [...new Set([...this.held, ...idsOf(write.restore)])];
      this.version += 1;
      return { version: this.version, combined: false };
    }
    const done = await rebaseEdits({ page: this.page, held: this.held }, write.edits, applyGrid);
    const combined = at !== this.version || Object.keys(done.renamed).length > 0;
    this.page = done.page;
    this.held = [...done.held];
    this.version += 1;
    return { version: this.version, combined };
  }
}

describe('a real page and its keeper', () => {
  it('writes its grids\' and its own edits as commands, which the keeper makes again on top of the other editor\'s', async () => {
    const cubes = new Map<string, CubeApp>();
    let channel: LiveChannel | undefined;
    const over = (name: string): GridMaker => (gridHost, spawned, start) => {
      const made = new CubeApp(gridHost, start?.snapshot ?? SNAPSHOT, {
        engine, planner: new StubPlanner(), compact: true, cubeSource: file(name), sourceLabel: name,
        configuration: start?.configuration ?? { ...DEFAULT_CONFIGURATION, reportTitle: name },
        ...spawned,
        // as DataCube's app wires a grid on a page another editor serves
        onCommand: (command) => { if (spawned.id !== undefined) channel?.edit({ grid: spawned.id, command }); },
      });
      if (spawned.id !== undefined) cubes.set(spawned.id, made);
      return made;
    };
    page = new PageApp({
      host, title: 'Q3', empty: (slot) => { slot.textContent = 'empty'; },
      onChange: () => channel?.changed(),
      onPageCommand: (command) => channel?.edit({ page: command }),
    });
    const a = page.addGrid(over('trades.csv'));
    page.addGrid(over('desks.csv'));
    await settle();
    const keeper = new Keeper(page.document('Q3')!);
    const makers = new Map<string, GridMaker>();
    channel = new LiveChannel(keeper, {
      page: () => page!.document('Q3', undefined, true),
      reopen: async () => {
        // the page as the keeper made it, opened again whole (datacube-app.ts openLive)
        for (const v of keeper.page.views) if (v.kind === 'grid' && !makers.has(v.id)) makers.set(v.id, over(`${v.id}.csv`));
        await channel!.following(async () => {
          page!.restore(keeper.page, makers);
          await page!.ready();
          await settle();
        });
        channel!.opened(keeper.version);
      },
    }, 0);
    channel.opened(keeper.version);
    // the other editor writes first: a sheet of its own, so the edits made here next are made against a version the
    // page has moved on from
    keeper.pythonWrites([{ kind: 'addSheet', name: 'From Python' }]);

    // edits on screen: a grid regrouped (as its own edits are applied), a grid added to the page
    assert.equal(await cubes.get(a)!.edit([{ kind: 'group', rows: ['desk'] }], 'Group'), null);
    const added = page.addGrid(over('more.csv'));
    await settle();
    await written(channel);
    await settle();

    // its edits were commands, not the page whole -- the grid's group as its grid made it, the grid added as the page
    // made it -- and the keeper made them again on top
    const told = keeper.written[0]!;
    assert.ok('edits' in told, 'written as commands');
    assert.deepEqual(told.edits.map((e) => ('grid' in e ? `${e.grid}:${e.command.kind}` : e.page.kind)), [`${a}:group`, 'addGrid']);
    assert.equal(keeper.version, 3, 'kept, made again on top of the other editor\'s');
    const kinds = (keeper.page.views.filter((v) => v.kind === 'grid').length);
    assert.equal(kinds, 3, 'the grid added here is on the page');
    assert.ok(keeper.page.sheets.some((s) => s.name === 'From Python'), "the other editor's sheet is on the page");
    const grouped = keeper.page.cubes.find((c) => c.id === a)!.cube.query.rows;
    assert.deepEqual(grouped, ['desk'], 'the grid regrouped here is regrouped on the page');
    // and the page on screen is the keeper's
    const now = page.document('Q3', undefined, true)!;
    assert.deepEqual(now.sheets.map((s) => s.name ?? null), keeper.page.sheets.map((s) => s.name ?? null));
    assert.deepEqual(now.views.map((v) => v.id).sort(), keeper.page.views.map((v) => v.id).sort());
    assert.ok(now.views.some((v) => v.id === added), 'the grid added here, under its id');
  });

  it('writes its undo whole, a restore the keeper keeps at its version -- behind another editor\'s write, refused', async () => {
    const cubes = new Map<string, CubeApp>();
    let channel: LiveChannel | undefined;
    const over = (name: string): GridMaker => (gridHost, spawned, start) => {
      const made = new CubeApp(gridHost, start?.snapshot ?? SNAPSHOT, {
        engine, planner: new StubPlanner(), compact: true, cubeSource: file(name), sourceLabel: name,
        configuration: start?.configuration ?? { ...DEFAULT_CONFIGURATION, reportTitle: name },
        ...spawned,
        onCommand: (command) => { if (spawned.id !== undefined) channel?.edit({ grid: spawned.id, command }); },
        onRestore: () => channel?.restored(),
      });
      if (spawned.id !== undefined) cubes.set(spawned.id, made);
      return made;
    };
    page = new PageApp({
      host, title: 'Q3', empty: (slot) => { slot.textContent = 'empty'; },
      onChange: () => channel?.changed(),
      onPageCommand: (command) => channel?.edit({ page: command }),
      onRestore: () => channel?.restored(),
    });
    const a = page.addGrid(over('trades.csv'));
    await settle();
    const keeper = new Keeper(page.document('Q3')!);
    const refused: string[] = [];
    channel = new LiveChannel(keeper, {
      page: () => page!.document('Q3', undefined, true),
      reopen: async (why) => {
        if (why !== undefined) refused.push(why);
        await channel!.following(async () => {
          page!.restore(keeper.page, new Map([[a, over('trades.csv')]]), keeper.held);
          await page!.ready();
          await settle();
        });
        channel!.opened(keeper.version);
      },
    }, 0);
    channel.opened(keeper.version);
    const sheets = (): number => page!.document('Q3', undefined, true)!.sheets.length;

    // a sheet added, written as its command; undone, written whole and kept at the version it was made at
    (host.querySelector('.dc-sheet-add') as HTMLElement).click();
    await settle();
    await written(channel);
    assert.equal(sheets(), 2);
    page.undo();
    await settle();
    await written(channel);
    assert.deepEqual(keeper.written.map((w) => ('restore' in w ? 'restore' : 'edits')), ['edits', 'restore']);
    assert.equal(keeper.page.sheets.length, 1, 'the keeper keeps the page as the undo left it');
    // a grid's undo too (G12): its group undone, written whole
    const rows = (): readonly string[] => keeper.page.cubes.find((c) => c.id === a)!.cube.query.rows;
    const grouped = rows();
    assert.equal(await cubes.get(a)!.edit([{ kind: 'group', rows: ['desk'] }], 'Group'), null);
    await settle();
    await written(channel);
    await cubes.get(a)!.undo();
    await settle();
    await written(channel);
    assert.deepEqual(keeper.written.slice(2).map((w) => ('restore' in w ? 'restore' : 'edits')), ['edits', 'restore']);
    assert.deepEqual(rows(), grouped, 'the group undone on the page');

    // the other editor writes; the undo here, made behind it, is refused: the page opened as the keeper has it, said
    (host.querySelector('.dc-sheet-add') as HTMLElement).click();
    await settle();
    await written(channel);
    keeper.pythonWrites([{ kind: 'renamePage', name: 'From Python' }]);
    page.undo();
    await settle();
    await written(channel);
    await settle();
    assert.equal(refused.length, 1, 'the stale restore refused');
    assert.match(refused[0]!, /an undo is taken only at the version it was made at/);
    assert.equal(keeper.page.name, 'From Python', "the other editor's write stands");
    assert.equal(keeper.page.sheets.length, 2, 'nothing of the refused undo kept');
    assert.equal(sheets(), 2, 'the screen is the keeper\'s page again');
  });
});
