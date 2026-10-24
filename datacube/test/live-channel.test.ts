// THE TAB'S SIDE OF THE PAGE KEEPER (src/app/live-channel.ts; docs/DATACUBE_PAGE_KEEPER_2026_10_20.md;
// docs/DATACUBE_LIVE_EDITING_2026_10_24.md §2): the edits made on screen written as commands, as applied there, never the
// page (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md S4) -- an undo or a redo the commands between (W3, W4), and nothing
// else; what the screen does while following the other editor never taken for its own; writes and follows one at a
// time, in order -- a follow told while a write is on its way waits for it, and sees the version it made. A write the
// keeper made another page of has the screen CONVERGE, without a read and without a reopen (W7): back at the page as
// the keeper had it, the other editor's steps since, its own as the keeper applied them, its pending edits made again,
// renamed; a refused write converges the same way over what moved the page since (W8), the refusal said; the keeper's
// hash checked at every answer (W17), the one message that must never show said if it ever differs; a flush writes
// what is pending once the screen is quiet (F90, F97, H-5). And a real page (PageApp, its grids CubeApps over a gated
// engine) on a keeper that applies its edits by DataCube's one rule (test/fake-keeper.ts), converging on top of the
// other editor's write: both editors' changes on the page it ends with, its undo made again on top as any edit is.

import assert from 'node:assert/strict';
import { afterEach, beforeEach, describe, it } from 'node:test';
import { JSDOM } from 'jsdom';

import { CubeApp } from '../src/app.ts';
import { DEFAULT_CONFIGURATION } from '../src/config.ts';
import type { FileSource } from '../src/cube-document.ts';
import { HASH_MISMATCH, LiveChannel, type Converge, type LiveScreen } from '../src/app/live-channel.ts';
import { convergeOn } from '../src/app/live-converge.ts';
import type { LiveCommand, LivePage, LiveSettled, LiveWrite } from '../src/app/services.ts';
import { idsOf } from '../src/page-commands.ts';
import { PageApp, type GridMaker } from '../src/page/page-app.ts';
import type { PageDocument } from '../src/page-document.ts';
import { PlanThenRun, type QueryRunner } from '../src/runner.ts';
import { pageHash } from '../src/page-hash.ts';
import { rebaseEdits } from '../src/page-rebase.ts';
import { GateEngine, SNAPSHOT, StubPlanner, settle, shown } from './cube-fixture.ts';
import { openCube, type CubeDocument } from '../src/cube-document.ts';
import { applyGridToCube, FakeKeeper } from './fake-keeper.ts';
import { corpus } from './fixtures/pages/make.ts';
import * as echarts from 'echarts/core';
import '../src/ui/page-filter-bar.ts';
import type { FilterNode } from '../src/snapshot.ts';

const trades = (await corpus())['trades.page.json']!;
/** `edits` made on screen `s`: applied to its page by the keeper's own rule (page-rebase.ts), and told to the channel. */
async function made(s: Screen, channel: LiveChannel, ...edits: LiveCommand[]): Promise<void> {
  const page = s.now!;
  s.now = (await rebaseEdits({ page, held: idsOf(page) }, edits, applyGridToCube)).page;
  for (const e of edits) channel.edit(e);
  channel.changed();
}
const kinds = (edits: readonly LiveCommand[]): string[] => edits.map((e) => ('grid' in e ? `${e.grid}:${e.command.kind}` : e.page.kind));

/** A screen showing `start`, which the test changes: a perfect one, whose converge makes exactly the page its keeper's
 *  steps and its own make (the keeper's rule, page-rebase.ts), and which records what it was told. */
interface Screen extends LiveScreen {
  now: PageDocument | undefined;
  busyNow: boolean;
  failConverge?: boolean;
  channel?: LiveChannel;
  readonly reopened: string[];
  readonly said: string[];
  readonly converged: Converge[];
  readonly reapplied: LiveCommand[][];
}
function screen(start: PageDocument): Screen {
  const s: Screen = {
    now: start as PageDocument | undefined,
    busyNow: false,
    reopened: [],
    said: [],
    converged: [],
    reapplied: [],
    page: () => s.now,
    busy: () => s.busyNow,
    converge: async (on) => {
      s.converged.push(on);
      if (s.failConverge) throw new Error('the screen could not be put at its keeper\'s page');
      // put at the keeper's page; a grid whose cube there is not the one the screen last knew started again
      const cube = (p: PageDocument, id: string): unknown => p.cubes.find((c) => c.id === p.views.find((v) => v.id === id)?.cube)?.cube.query;
      const restarted = new Set([
        ...on.target.views.filter((v) => v.kind === 'grid' && JSON.stringify(cube(on.base, v.id)) !== JSON.stringify(cube(on.target, v.id))).map((v) => v.id),
        ...on.refused.flatMap((e) => ('grid' in e ? [e.grid] : [])),
      ]);
      s.now = on.target;
      return { page: on.target, restarted };
    },
    // the pending edits made again: on this screen each is applied to its page (a grid the steps left alone shows it
    // still: here, the page is the keeper's, so it is applied too) and told again
    reapply: async (pending) => {
      s.reapplied.push([...pending]);
      s.now = (await rebaseEdits({ page: s.now!, held: idsOf(s.now!) }, pending, applyGridToCube)).page;
      for (const e of pending) s.channel?.edit(e);
    },
    reopen: async (why) => { s.reopened.push(why); },
    say: (text) => { s.said.push(text); },
  };
  return s;
}

/** Every timer due by now fired, and the channel's queue drained -- twice, for a write that a write's own answer schedules
 *  (a converge's pending edits, told again and written next). */
async function written(channel: LiveChannel): Promise<void> {
  for (let round = 0; round < 2; round += 1) {
    await new Promise((r) => setTimeout(r, 5));
    await channel.idle();
  }
}

/** A channel over `keeper`, opened at the keeper's page. */
function opened(keeper: FakeKeeper): { channel: LiveChannel; s: Screen } {
  const s = screen(keeper.page);
  const channel = new LiveChannel(keeper, s, 0);
  s.channel = channel;
  channel.opened(keeper.version, keeper.page, keeper.held);
  return { channel, s };
}

describe('the channel', () => {
  it('writes the edits made on screen, as applied there -- never the page -- and then has none pending; its confirmed page moves with each', async () => {
    const keeper = new FakeKeeper(trades);
    const { channel, s } = opened(keeper);
    await made(s, channel, { page: { kind: 'renamePage', name: 'Here' } });
    await written(channel);
    assert.deepEqual(keeper.written.map((w) => w.edits), [[{ page: { kind: 'renamePage', name: 'Here' } }]]);
    assert.equal(channel.shown, 2);
    assert.equal(channel.confirmed?.name, 'Here', 'confirmed: the page as it was sent');
    assert.deepEqual(channel.pending, []);
    await made(s, channel, { grid: 'grid', command: { kind: 'group', rows: ['desk'] } });
    await written(channel);
    assert.deepEqual(keeper.written[1]!.edits, [{ grid: 'grid', command: { kind: 'group', rows: ['desk'] } }], 'only the new edit');
    assert.deepEqual([s.reopened, s.converged.length], [[], 0], 'at its version each time: nothing to converge on');
  });

  it('writes an undo or a redo as the commands between, an edit like any (D19 W3, W4), and a change no command made, never', async () => {
    const keeper = new FakeKeeper(trades);
    const { channel, s } = opened(keeper);
    // a change no edit was told for: nothing written (the page is never sent)
    s.now = { ...trades, name: 'No command' };
    channel.changed();
    await written(channel);
    assert.equal(keeper.written.length, 0, 'no page written whole for a change no command made');
    s.now = trades;
    await made(s, channel, { page: { kind: 'renameSheet', sheet: 'sheet-1', name: 'Here' } });
    await written(channel);
    // undone: the page tells the commands that take the step back (page-state.ts), which are its edit as any other
    const before = trades.sheets.find((x) => x.id === 'sheet-1')!.name ?? null;
    await made(s, channel, { page: { kind: 'renameSheet', sheet: 'sheet-1', name: before } });
    await written(channel);
    assert.deepEqual(keeper.written.map((w) => w.edits), [
      [{ page: { kind: 'renameSheet', sheet: 'sheet-1', name: 'Here' } }],
      [{ page: { kind: 'renameSheet', sheet: 'sheet-1', name: before } }],
    ], 'the undo written as its commands, never the page whole');
    assert.deepEqual(keeper.written.map((w) => w.seq), [1, 2], 'each write numbered, by one writer');
    assert.deepEqual(keeper.page, trades);
  });

  it('takes nothing the screen does while following the other editor for its own', async () => {
    const keeper = new FakeKeeper(trades);
    const { channel, s } = opened(keeper);
    await channel.following(async () => {
      channel.shown = undefined;
      // the other editor's command, applied: the screen tells it, and changes
      channel.edit({ page: { kind: 'renamePage', name: 'Python' } });
      s.now = { ...trades, name: 'Python' };
      channel.changed();
    });
    keeper.pythonWrites([{ kind: 'renamePage', name: 'Python' }]);
    s.now = keeper.page;
    channel.opened(keeper.version, keeper.page, keeper.held);
    channel.changed();
    await written(channel);
    assert.equal(keeper.written.length, 0, 'nothing of its own to write -- Python\'s change never written back as the screen\'s');
    // and an edit of its own after: written alone
    await made(s, channel, { page: { kind: 'renamePage', name: 'Mine' } });
    await written(channel);
    assert.deepEqual(keeper.written.map((w) => w.edits), [[{ page: { kind: 'renamePage', name: 'Mine' } }]]);
  });

  it("keeps the person's own grid edit told while following the other editor, and writes it (the S1 audit's 2)", async () => {
    const keeper = new FakeKeeper(trades);
    const { channel, s } = opened(keeper);
    await channel.following(async () => {
      channel.shown = undefined;
      // the person's own change, landing while the other editor's commands are followed (a grid tells none of the
      // other editor's: CubeApp.applyGridCommands)
      await made(s, channel, { grid: 'grid', command: { kind: 'group', rows: ['desk'] } });
    });
    channel.shown = 1;
    channel.changed();
    await written(channel);
    assert.deepEqual(keeper.written.map((w) => w.edits), [[{ grid: 'grid', command: { kind: 'group', rows: ['desk'] } }]]);
  });

  it('writes nothing when nothing was made on screen since it opened', async () => {
    const keeper = new FakeKeeper(trades);
    const { channel } = opened(keeper);
    // the page as it opened, changed again by nothing (a view landing, a saved page put back settling)
    channel.changed();
    channel.changed();
    await written(channel);
    assert.equal(keeper.written.length, 0);
  });

  it('runs a follow told while a write is on its way after it, at the version the write made', async () => {
    const keeper = new FakeKeeper(trades);
    keeper.hold = true;
    const { channel, s } = opened(keeper);
    await made(s, channel, { page: { kind: 'renamePage', name: 'Here' } });
    await new Promise((r) => setTimeout(r, 5));
    assert.equal(keeper.written.length, 1, 'the write is on its way');
    const started: (number | undefined)[] = [];
    // the version notice of that very write arrives first
    const follow = channel.enqueue(async () => { started.push(channel.shown); });
    await new Promise((r) => setTimeout(r, 5));
    assert.deepEqual(started, [], 'not while the write is on its way');
    keeper.release();
    await follow;
    assert.deepEqual(started, [2], 'after it, at the version it made: its own write is never taken for the other editor\'s');
  });

  it('converges on a write the keeper made again on top of the other editor\'s, without a reopen (W7)', async () => {
    const keeper = new FakeKeeper(trades);
    const { channel, s } = opened(keeper);
    // the other editor writes first (a sheet of its own): the edit made here next is against a version the page has
    // moved on from
    keeper.pythonWrites([{ kind: 'addSheet', name: 'From Python' }]);
    await made(s, channel, { page: { kind: 'renameSheet', sheet: 'sheet-1', name: 'Mine' } });
    await written(channel);
    assert.deepEqual(s.reopened, [], 'no reopen');
    assert.equal(s.converged.length, 1);
    const on = s.converged[0]!;
    assert.deepEqual(on.target, keeper.page, "put at the keeper's page, which the answer carries: both editors' changes");
    assert.equal(on.base.sheets.find((x) => x.id === 'sheet-1')?.name, 'Mine', 'from the page as it was sent');
    assert.deepEqual([on.refused, on.pending], [[], []]);
    assert.equal(keeper.reads, 0, 'no read: the answer carried the page');
    assert.equal(channel.shown, 3, 'at the version the write made');
    assert.deepEqual(channel.confirmed, keeper.page, 'confirmed: the page as the keeper has it now');
    assert.deepEqual(s.now, keeper.page, 'the screen shows what the keeper has: both editors\' changes');
    assert.deepEqual(s.said, []);
  });

  it('an id the page holds already is refused in words, never given out afresh: nothing is renamed (the middle version)', async () => {
    const keeper = new FakeKeeper(trades);
    const { channel, s } = opened(keeper);
    keeper.pythonWrites([{ kind: 'addSheet', id: 'sheet-3', name: 'Theirs' }]);
    await made(s, channel, { page: { kind: 'addSheet', id: 'sheet-3', name: 'Mine' } });
    await written(channel);
    assert.deepEqual(s.reopened, []);
    assert.match(s.said[0]!, /not kept: .*'sheet-3' is an id the page has held already/);
    assert.deepEqual(kinds(s.converged[0]!.refused), ['addSheet']);
    assert.deepEqual(s.now, keeper.page, "the keeper's page, its sheet-3 the other editor's");
    assert.equal(keeper.page.sheets.find((x) => x.id === 'sheet-3')?.name, 'Theirs');
  });

  it('two editors making sheets at once under their own tags: both kept, under the ids each gave (the middle version)', async () => {
    const keeper = new FakeKeeper(trades);
    const { channel, s } = opened(keeper);
    keeper.pythonWrites([{ kind: 'addSheet', name: 'Theirs' }]);
    const theirs = keeper.page.sheets.at(-1)!.id;
    keeper.hold = true;
    await made(s, channel, { page: { kind: 'addSheet', id: 'sheet-tab0-3', name: 'Mine' } });
    await new Promise((r) => setTimeout(r, 5));
    // made while the write is on its way: on the sheet made here, under the id it was given here, and on a grid
    await made(s, channel, { page: { kind: 'renameSheet', sheet: 'sheet-tab0-3', name: 'Mine, renamed' } }, { grid: 'grid', command: { kind: 'group', rows: ['desk'] } });
    keeper.release();
    await written(channel);
    assert.deepEqual(s.reopened, []);
    assert.equal(keeper.combinedCount, 1);
    // the pending edits made again, as they were, and told again: written next (the queue drained, they are)
    assert.deepEqual(s.reapplied, [[{ page: { kind: 'renameSheet', sheet: 'sheet-tab0-3', name: 'Mine, renamed' } }, { grid: 'grid', command: { kind: 'group', rows: ['desk'] } }]]);
    assert.deepEqual(kinds(keeper.written.at(-1)!.edits), ['renameSheet', 'grid:group'], 'told again, written next');
    assert.deepEqual(channel.pending, []);
    assert.deepEqual(keeper.page.sheets.map((x) => x.name ?? null).slice(-2), ['Theirs', 'Mine, renamed']);
    assert.equal(keeper.page.sheets.find((x) => x.id === theirs)?.name, 'Theirs', "the other editor's sheet under its own id");
    assert.deepEqual(s.now, keeper.page);
  });

  it('a pending edit on a grid the keeper\'s steps touched is made again through the ordinary path', async () => {
    const keeper = new FakeKeeper(trades);
    const { channel, s } = opened(keeper);
    // Python regrouped the grid: a grid edit of its own, a step the keeper kept
    keeper.pythonWrites([{ kind: 'renameTile', tile: 'grid', title: 'Theirs' }]);
    keeper.steps.set(keeper.version, [...keeper.steps.get(keeper.version)!, { grid: 'grid', command: { kind: 'group', rows: ['region'] } }]);
    keeper.page = (await rebaseEdits({ page: keeper.page, held: keeper.held }, [{ grid: 'grid', command: { kind: 'group', rows: ['region'] } }], applyGridToCube)).page;
    keeper.hold = true;
    await made(s, channel, { page: { kind: 'renamePage', name: 'Mine' } });
    await new Promise((r) => setTimeout(r, 5));
    await made(s, channel, { grid: 'grid', command: { kind: 'sorts', sorts: [{ column: 'desk', direction: 'asc' }] } });
    keeper.release();
    await written(channel);
    assert.deepEqual(s.reopened, []);
    assert.deepEqual(s.reapplied, [[{ grid: 'grid', command: { kind: 'sorts', sorts: [{ column: 'desk', direction: 'asc' }] } }]], 'made again: the grid was put back and moved');
    assert.deepEqual(kinds(keeper.written.at(-1)!.edits), ['grid:sorts'], 'told again, written next');
    assert.deepEqual(s.now, keeper.page);
  });

  it('converges after a refused write, over what moved the page since -- its words said, nothing of the refused edit written again (W8)', async () => {
    const keeper = new FakeKeeper(trades);
    const { channel, s } = opened(keeper);
    // the other editor took the grid off: an edit of it made here no longer applies there
    keeper.pythonWrites([{ kind: 'removeTile', tile: 'grid-2' }]);
    await made(s, channel, { grid: 'grid-2', command: { kind: 'group', rows: ['desk'] } });
    await written(channel);
    assert.deepEqual(s.reopened, []);
    assert.equal(s.said.length, 1);
    assert.match(s.said[0]!, /not kept: .*grid-2 is no longer on the page/);
    assert.equal(keeper.reads, 1, "the keeper's page read");
    assert.deepEqual([s.converged[0]!.target, kinds(s.converged[0]!.refused)], [keeper.page, ['grid-2:group']], "put at the keeper's page, the refused batch named");
    assert.equal(channel.shown, 2);
    assert.deepEqual(s.now, keeper.page);
    channel.changed();
    await written(channel);
    assert.equal(keeper.written.length, 1, 'nothing of the refused edit written again');
  });

  it('a page the other editor loaded whole (N1): the write made again on it, and the screen put at it -- no reopen', async () => {
    const keeper = new FakeKeeper(trades);
    const { channel, s } = opened(keeper);
    keeper.pythonLoads({ ...trades, name: 'Loaded' });
    await made(s, channel, { page: { kind: 'renameSheet', sheet: 'sheet-1', name: 'Mine' } });
    await written(channel);
    assert.deepEqual(s.reopened, []);
    assert.equal(keeper.page.name, 'Loaded');
    assert.equal(keeper.page.sheets.find((x) => x.id === 'sheet-1')?.name, 'Mine');
    assert.deepEqual(s.now, keeper.page);
    assert.equal(channel.shown, keeper.version);
  });

  it('says what it drops with a reopen: a change made meanwhile (H-11)', async () => {
    const keeper = new FakeKeeper(trades);
    keeper.hold = true;
    const { channel, s } = opened(keeper);
    keeper.pythonWrites([{ kind: 'addSheet', name: 'From Python' }]);
    // a screen that cannot be put at the keeper's page: opened again instead
    s.failConverge = true;
    await made(s, channel, { page: { kind: 'renamePage', name: 'Here' } });
    await new Promise((r) => setTimeout(r, 5));
    await made(s, channel, { page: { kind: 'renamePage', name: 'Meanwhile' } });
    keeper.release();
    await channel.idle();
    assert.equal(s.reopened.length, 1);
    assert.match(s.reopened[0]!, /1 change made here meanwhile was not kept/);
  });

  // The keeper out of reach (the notebook's kernel restarting, the network gone for a moment), or a write's answer lost:
  // every write has an id, so the tab asks the keeper whether it took it (live-channel.ts #unanswered) -- never takes
  // the failure for a refusal (the middle version).
  it("a write the keeper never saw (unreachable): asked, not taken -- its edit pending again and written, nothing said", async () => {
    const keeper = new FakeKeeper(trades);
    const { channel, s } = opened(keeper);
    keeper.unreachable = 1;
    await made(s, channel, { page: { kind: 'renamePage', name: 'Here' } });
    await written(channel);
    assert.deepEqual(s.reopened, []);
    assert.deepEqual(s.said, [], 'nothing said: nothing was lost');
    assert.deepEqual(kinds(s.converged[0]!.pending), ['renamePage'], 'its edit pending again, made again on the keeper\'s page');
    assert.deepEqual(keeper.written.map((w) => kinds(w.edits)), [['renamePage']], 'written once the keeper answered');
    assert.equal(keeper.page.name, 'Here');
    assert.deepEqual([channel.shown, channel.pending], [keeper.version, []]);
  });

  it('the keeper unreachable for the write and for the ask after it: opened again, the failure said; opened, it writes as before', async () => {
    const keeper = new FakeKeeper(trades);
    const { channel, s } = opened(keeper);
    keeper.unreachable = 1;
    keeper.readsFail = 1;
    await made(s, channel, { page: { kind: 'renamePage', name: 'Here' } });
    await written(channel);
    assert.equal(s.reopened.length, 1);
    assert.match(s.reopened[0]!, /did not answer: fetch failed/);
    assert.equal(channel.shown, undefined, 'at no version until the page is opened again');
    assert.deepEqual(s.converged, []);
    s.now = keeper.page;
    channel.opened(keeper.version, keeper.page, keeper.held);
    await made(s, channel, { page: { kind: 'renamePage', name: 'Back' } });
    await written(channel);
    assert.deepEqual([keeper.written.length, keeper.page.name, channel.shown], [1, 'Back', keeper.version]);
    assert.equal(s.reopened.length, 1, 'no second reopen');
  });

  it("the answer lost: the keeper kept the write but the tab heard a failure -- asked, taken: the screen at the keeper's page, nothing said, never written twice", async () => {
    const keeper = new FakeKeeper(trades);
    const { channel, s } = opened(keeper);
    keeper.loseAnswer = true;
    await made(s, channel, { page: { kind: 'renamePage', name: 'Here' } });
    await written(channel);
    assert.deepEqual([keeper.written.length, keeper.page.name], [1, 'Here'], 'the keeper saw it once and kept it');
    assert.deepEqual([s.reopened, s.said], [[], []], 'nothing said: it was kept');
    assert.deepEqual(s.converged[0]!.pending, [], 'nothing of it pending again');
    assert.equal(channel.shown, keeper.version);
    assert.deepEqual(s.now, keeper.page);
    channel.changed();
    await written(channel);
    assert.equal(keeper.written.length, 1, 'nothing written again');
    await made(s, channel, { page: { kind: 'renamePage', name: 'Next' } });
    await written(channel);
    assert.deepEqual([keeper.written.length, keeper.page.name, channel.shown], [2, 'Next', keeper.version]);
  });

  it('a write sent again after it was taken is applied once: the keeper answers its page as it is', async () => {
    const keeper = new FakeKeeper(trades);
    const write = { edits: [{ page: { kind: 'renameSheet', sheet: 'sheet-1', name: 'Once' } as const }], writer: 'w1', seq: 1 };
    await keeper.report(1, write);
    const again = await keeper.report(1, write);
    assert.deepEqual([keeper.version, again.version, again.page], [2, 2, keeper.page]);
  });

  it('checks the keeper\'s hash at every answer: a page that differs is the one message that must never show, and opens the page again once (W17)', async () => {
    const keeper = new FakeKeeper(trades);
    const { channel, s } = opened(keeper);
    channel.edit({ page: { kind: 'renamePage', name: 'Here' } });
    // the screen holds another page than it sent (a bug elsewhere: the hash says so)
    s.now = { ...trades, name: 'Elsewhere' };
    channel.changed();
    await written(channel);
    assert.deepEqual(s.reopened, [HASH_MISMATCH]);
    assert.equal(channel.shown, undefined);
    // opened again, and still differing: said, no second reopen (no loop)
    channel.opened(keeper.version, keeper.page, keeper.held);
    channel.edit({ page: { kind: 'renamePage', name: 'Again' } });
    s.now = { ...trades, name: 'Elsewhere again' };
    channel.changed();
    await written(channel);
    assert.equal(s.reopened.length, 1, 'no second reopen');
    assert.deepEqual(s.said, [HASH_MISMATCH]);
    // a write that hashes alike clears it
    s.now = keeper.page;
    await made(s, channel, { page: { kind: 'renamePage', name: 'Right' } });
    await written(channel);
    assert.equal(channel.shown, keeper.version);
    assert.equal(pageHash(s.now), pageHash(keeper.page));
  });

  it('waits for the screen to be quiet before a write (F97), and a flush writes what is pending once it is (F90, H-5)', async () => {
    const keeper = new FakeKeeper(trades);
    const { channel, s } = opened(keeper);
    s.busyNow = true;
    await made(s, channel, { page: { kind: 'renamePage', name: 'Here' } });
    await written(channel);
    assert.equal(keeper.written.length, 0, 'a gesture in flight: its commands are written together once it has landed');
    const flushed = channel.flush();
    await new Promise((r) => setTimeout(r, 40));
    assert.equal(keeper.written.length, 0, 'the flush waits for it too');
    s.busyNow = false;
    await flushed;
    assert.equal(keeper.written.length, 1);
    assert.equal(channel.shown, 2);
    assert.deepEqual(channel.pending, []);
  });

  it('writes nothing once stopped, nor while at no version', async () => {
    const keeper = new FakeKeeper(trades);
    const s = screen(trades);
    const channel = new LiveChannel(keeper, s, 0);
    channel.changed();
    await written(channel);
    channel.opened(1, trades, idsOf(trades));
    channel.stop();
    s.now = { ...trades, name: 'Too late' };
    channel.changed();
    await written(channel);
    assert.equal(keeper.written.length, 0);
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
/** Every channel a test made: stopped with it, so no write of its own fires into the next test's page. */
const channels: LiveChannel[] = [];
afterEach(async () => {
  for (const c of channels.splice(0)) c.stop();
  await settle();
  page?.dispose();
  page = undefined;
});

const file = (name: string): FileSource => ({
  _type: 'file', name, format: 'csv', size: 10, sha256: name.padEnd(64, '0').slice(0, 64),
  columns: SNAPSHOT.columns.map((c) => ({ name: c.name, type: c.type })),
});

/** A real page on a keeper: its grids CubeApps told to the channel, the page's commands too, converging by the one
 *  rule (live-converge.ts) as DataCube's app does. */
/** A test's own doing inside the converge: as it asks for a grid, and as it opens another editor's. */
interface Hooks { readonly grid?: (id: string) => void; opened?: ((grid: string, several: boolean) => void) | undefined }

function realPage(hooks: Hooks = {}): { cubes: Map<string, CubeApp>; over: (name: string) => GridMaker; channelOf: (keeper: FakeKeeper) => { channel: LiveChannel; reopened: string[]; said: string[]; atReopen: () => PageDocument | undefined } } {
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
  /** Another editor's grid opened from its document, as the app opens one (datacube-app.ts): over the document's source,
   *  starting at the cube it says. */
  const overDocument = (document: CubeDocument): GridMaker => (gridHost, spawned, start) => {
    const opened = openCube(document, document.source as never, SNAPSHOT.columns);
    return over((document.source as { readonly name?: string }).name ?? 'opened.csv')(gridHost, spawned, start ?? { snapshot: opened.snapshot, configuration: opened.configuration, tree: opened.tree });
  };
  // this test's own page, held by the closures below (never the module's `page`, which the next test's replaces)
  const app = new PageApp({
    host, title: 'Q3', empty: (slot) => { slot.textContent = 'empty'; },
    onChange: () => channel?.changed(),
    onPageCommand: (command) => channel?.edit({ page: command }),
  });
  page = app;
  const channelOf = (keeper: FakeKeeper): { channel: LiveChannel; reopened: string[]; said: string[]; atReopen: () => PageDocument | undefined } => {
    const reopened: string[] = [];
    const said: string[] = [];
    /** The page on screen as a reopen found it (the page the keeper's hash was compared with). */
    let atReopen: PageDocument | undefined;
    const now = (): PageDocument | undefined => app.document(app.title || 'Q3', undefined, true);
    channel = new LiveChannel(keeper, {
      page: now,
      busy: () => [...cubes.values()].some((c) => c.busy),
      converge: (on) => convergeOn(on, {
        page: app,
        // `hooks.grid`: a test's own doing while the converge asks for a grid (inside the page's hold)
        grid: (id) => { hooks.grid?.(id); return cubes.get(id); },
        pageNow: now,
        // a grid the other editor added: made as a saved page's are, over the source its document says, at the cube it
        // says
        open: async (document, grid, several) => {
          hooks.opened?.(grid, several);
          return { make: overDocument(document) };
        },
      }),
      reapply: async (pending, restarted) => {
        for (const edit of pending) {
          if ('page' in edit) app.reapply([edit.page], 'made again');
          else await cubes.get(edit.grid)?.edit([edit.command], 'made again', { again: !restarted.has(edit.grid) });
        }
      },
      reopen: async (why) => {
        reopened.push(why);
        atReopen = now();
        // the page as the keeper has it, opened again whole (datacube-app.ts openLive)
        const makers = new Map<string, GridMaker>();
        for (const c of keeper.page.cubes) makers.set(c.id, over(c.cube.source.name));
        await channel!.following(async () => {
          app.restore(keeper.page, makers, keeper.held);
          await app.ready();
          await settle();
        });
        channel!.opened(keeper.version, keeper.page, keeper.held);
      },
      say: (text) => { said.push(text); },
    }, 0);
    channels.push(channel);
    channel.opened(keeper.version, keeper.page, keeper.held);
    return { channel, reopened, said, atReopen: () => atReopen };
  };
  return { cubes, over, channelOf };
}

const tileEl = (id: string): HTMLElement => host.querySelector(`[data-tile="${id}"]`) as HTMLElement;
/** A mark of `chart` clicked: its condition on the chart's selection and on its grid's filter -- once the chart has
 *  drawn (its drawing is fetched when first wanted, and that outlasted settle() on macOS' CI, 2026-10-24). */
async function markOf(chart: string): Promise<void> {
  const el = await shown<HTMLElement>(tileEl(chart), '[_echarts_instance_]');
  echarts.getInstanceByDom(el)!.trigger('click', { seriesIndex: 0, dataIndex: 0 } as never);
}
const labelOf = (el: HTMLElement | null): string => el?.querySelector('.dc-menu-label')?.textContent ?? el?.textContent ?? '';
/** A chart made over a grid's tile, as a person makes one: a cell's menu, Create > Visualization. */
async function chartOver(gridTile: string): Promise<string> {
  const before = new Set(page!.views().views.filter((v) => v.kind === 'chart').map((v) => v.id));
  (tileEl(gridTile).querySelector('.dc-row .dc-cell') as HTMLElement)
    .dispatchEvent(new dom.window.MouseEvent('contextmenu', { bubbles: true, cancelable: true }));
  await settle();
  const item = [...dom.window.document.querySelectorAll<HTMLElement>('.dc-menu .dc-menu-item')]
    .find((el) => labelOf(el) === 'Visualization' && labelOf(el.parentElement!.closest('.dc-menu-item') as HTMLElement) === 'Create');
  assert.ok(item, 'Create > Visualization');
  item.click();
  await settle();
  const chart = [...host.querySelectorAll<HTMLElement>('[data-tile^="chart-"]')].map((t) => t.dataset['tile']!).find((id) => !before.has(id));
  assert.ok(chart, 'a chart on the page');
  return chart;
}

describe("a real page and its keeper: the D19 audit's second round", () => {
  it("a page step of the other editor's that changes a grid's cube restarts that grid: a selected chart removed while the grid's whole filter is set here (1)", async () => {
    const { cubes, over, channelOf } = realPage();
    const a = page!.addGrid(over('trades.csv'));
    await settle();
    const chart = await chartOver(a);
    await settle();
    // a mark clicked: its condition on the chart's selection, and on a's filter
    await markOf(chart);
    await settle();
    const before = page!.document('Q3', undefined, true)!;
    const sel = before.cubes.find((c) => c.id === a)!.cube.query.filter as FilterNode | undefined;
    assert.ok(sel !== undefined, "the mark is on the grid's filter");
    const keeper = new FakeKeeper(before);
    const { channel, reopened, said } = channelOf(keeper);
    // the other editor removes the chart (its selection comes off a's filter: a page step that changes a's cube); here,
    // still at the version before, a is grouped -- an edit of another field than the one the step changes, so a grid
    // left at its sent cube would keep the selection the keeper took off (the mutation M1, 2026-10-24) -- and written
    // on top
    keeper.pythonWrites([{ kind: 'removeTile', tile: chart }]);
    assert.equal(await cubes.get(a)!.edit([{ kind: 'group', rows: ['desk'] }], 'Group'), null);
    await settle();
    await written(channel);
    await settle();
    // the keeper: the selection off, then the group; the screen the same, to the hash, with the grid put back at the
    // confirmed cube and the steps replayed (not left at its sent cube, where the selection would stay)
    assert.equal(keeper.combinedCount, 1);
    assert.deepEqual([reopened, said], [[], []]);
    const now = page!.document('Q3', undefined, true)!;
    const theirs = keeper.page.cubes.find((c) => c.id === a)!.cube.query;
    assert.notDeepEqual(theirs.filter, sel, "the keeper took the selection off a's filter");
    assert.deepEqual(now.cubes.find((c) => c.id === a)!.cube.query.filter, theirs.filter, "a's filter is the keeper's");
    assert.deepEqual(now.cubes.find((c) => c.id === a)!.cube.query.rows, theirs.rows, "a's rows are the keeper's");
    assert.equal(pageHash(now), pageHash(keeper.page), 'one page (W17)');
    // another editor changed this grid while its group was on the way: its history starts again at the page the keeper
    // made of both (W13, the middle version: the screen is put at the keeper's page, never replayed step by step)
    assert.equal(cubes.get(a)!.canUndo, false, "the grid's history started again, at the keeper's page");
  });

  it("two edits pending at a follow: made again as no new step, so the grid's history has each once and an undo never goes forward (3)", async () => {
    const { cubes, over, channelOf } = realPage();
    const a = page!.addGrid(over('trades.csv'));
    await settle();
    const keeper = new FakeKeeper(page!.document('Q3')!);
    const { channel, reopened, said } = channelOf(keeper);
    const g = cubes.get(a)!;
    const state = (): string => `rows=${JSON.stringify(g.snapshot.rows)} sorts=${JSON.stringify(g.snapshot.sorts.map((x) => x.column))}`;
    const initial = state();
    keeper.pythonWrites([{ kind: 'addSheet', name: 'From Python' }]);
    const moved = await keeper.read();
    // the follow queued behind a gate, so both edits are pending when it runs (as edits made while the keeper is asked)
    let release!: () => void;
    const gate = new Promise<void>((r) => { release = r; });
    let followed = false;
    const run = channel.enqueue(async () => { await gate; followed = await channel.follow(moved); });
    assert.equal(await g.edit([{ kind: 'group', rows: ['desk'] }], 'Group'), null);
    await settle();
    assert.equal(await g.edit([{ kind: 'sorts', sorts: [{ column: 'desk', direction: 'asc' }] }], 'Sort'), null);
    await settle();
    assert.equal(channel.pending.length, 2, 'two pending');
    const edited = state();
    release();
    await run;
    assert.equal(followed, true);
    await written(channel);
    await settle();
    assert.deepEqual([reopened, said], [[], []]);
    assert.equal(pageHash(page!.document('Q3', undefined, true)!), pageHash(keeper.page), 'one page (W17)');
    assert.equal(state(), edited, 'the pending edits are on the grid');
    // its history: the sort, then the group, then nothing -- each state once, never a step forward
    const trace: string[] = [state()];
    for (let i = 0; i < 6 && g.canUndo; i += 1) {
      await g.undo();
      await settle();
      trace.push(state());
    }
    assert.deepEqual(trace, [edited, `rows=["desk"] sorts=[]`, initial], `the undo trace: ${trace.join(' -> ')}`);
  });

  it('a grid added here, grouped in a write the keeper holds and sorted meanwhile, converges on the keeper\'s page: no reopen (4)', async () => {
    const { cubes, over, channelOf } = realPage();
    page!.addGrid(over('trades.csv'));
    await settle();
    const keeper = new FakeKeeper(page!.document('Q3')!);
    const { channel, reopened, said } = channelOf(keeper);
    keeper.pythonWrites([{ kind: 'addSheet', name: 'From Python' }]);
    keeper.hold = true;
    const added = page!.addGrid(over('more.csv'));
    assert.equal(await cubes.get(added)!.edit([{ kind: 'group', rows: ['desk'] }], 'Group'), null);
    await settle();
    assert.equal(keeper.written.length, 1, 'the write is on its way, held');
    assert.equal(await cubes.get(added)!.edit([{ kind: 'sorts', sorts: [{ column: 'desk', direction: 'asc' }] }], 'Sort'), null);
    await settle();
    assert.equal(channel.pending.length, 1, 'the sort pending');
    keeper.release();
    await written(channel);
    await settle();
    await written(channel);
    await settle();
    assert.deepEqual([reopened, said], [[], []], 'converged: the grid made again from where it was, then put back at its sent cube, the sort made again');
    const now = page!.document('Q3', undefined, true)!;
    assert.equal(pageHash(now), pageHash(keeper.page), 'one page (W17)');
    assert.deepEqual(keeper.page.cubes.find((c) => c.id === added)!.cube.query.sorts.map((x) => x.column), ['desk'], 'the sort written');
    assert.equal(cubes.get(added)!.canUndo, true, "the sort a step of the remade grid's history (the third round, 3)");
  });

  it("a grid gesture while the page converges is refused, said: the grid is being put back and replayed (the third round, 1)", async () => {
    let calls = 0;
    let refusedDuring: string | null | undefined;
    const { cubes, over, channelOf } = realPage({ grid: (id) => {
      // the converge asks for the grid a second time (held by then): a person sorts it meanwhile
      calls += 1;
      if (calls === 2) void cubes.get(id)!.edit([{ kind: 'sorts', sorts: [{ column: 'desk', direction: 'asc' }] }], 'Sort').then((r) => { refusedDuring = r; });
    } });
    const a = page!.addGrid(over('trades.csv'));
    await settle();
    const keeper = new FakeKeeper(page!.document('Q3')!);
    const { channel, reopened, said } = channelOf(keeper);
    keeper.pythonWrites([{ kind: 'renamePage', name: 'Theirs' }]);
    assert.equal(await cubes.get(a)!.edit([{ kind: 'group', rows: ['desk'] }], 'Group'), null);
    await written(channel);
    await settle();
    await written(channel);
    await settle();
    assert.ok(calls >= 2, 'the gesture was made while the grid was held');
    assert.equal(refusedDuring, 'the page is catching up with its keeper: a moment');
    assert.deepEqual([reopened, said], [[], []], 'no reopen: nothing of the gesture landed or was written');
    const now = page!.document(page!.title, undefined, true)!;
    assert.deepEqual(now.cubes.find((c) => c.id === a)!.cube.query.sorts, [], 'no sort');
    assert.equal(pageHash(now), pageHash(keeper.page), 'one page (W17)');
    assert.equal(cubes.get(a)!.held, false, 'released after the converge');
  });

  it("a chart added here with a mark clicked, dropped by the put-back: its grid's history kept, the mark made again (the third round, 2)", async () => {
    const { cubes, over, channelOf } = realPage();
    const a = page!.addGrid(over('trades.csv'));
    await settle();
    const keeper = new FakeKeeper(page!.document('Q3')!);
    const { channel, reopened, said } = channelOf(keeper);
    const g = cubes.get(a)!;
    keeper.pythonWrites([{ kind: 'addSheet', name: 'From Python' }]);
    const moved = await keeper.read();
    // held back, the follow runs with the chart, its selection and the grid's refilter pending
    let release!: () => void;
    const gate = new Promise<void>((r) => { release = r; });
    let followed = false;
    const run = channel.enqueue(async () => { await gate; followed = await channel.follow(moved); });
    const chart = await chartOver(a);
    await settle();
    await markOf(chart);
    await settle();
    assert.ok(channel.pending.some((e) => 'grid' in e && e.command.kind === 'refilter'), "the mark on the grid's filter, pending");
    assert.equal(g.canUndo, true, 'the refilter a step of the grid');
    release();
    await run;
    assert.equal(followed, true);
    await written(channel);
    await settle();
    assert.deepEqual([reopened, said], [[], []]);
    assert.equal(pageHash(page!.document('Q3', undefined, true)!), pageHash(keeper.page), 'one page (W17)');
    assert.ok(keeper.page.views.some((v) => v.kind === 'chart' && (v.selection ?? []).length > 0), 'the chart and its selection written');
    assert.equal(g.canUndo, true, "the grid's history kept through the put-back's drop of the chart");
  });
});

describe('a real page and its keeper', () => {
  it("a refused write's grid edits are put back too: the grid at the keeper's cube, one page to the hash, no reopen (the D19 audit's 1)", async () => {
    const { cubes, over, channelOf } = realPage();
    const a = page!.addGrid(over('trades.csv'));
    const b = page!.addGrid(over('desks.csv'));
    await settle();
    const keeper = new FakeKeeper(page!.document('Q3')!);
    const { channel, reopened, said } = channelOf(keeper);
    const rowsBefore = page!.document('Q3', undefined, true)!.cubes.find((c) => c.id === a)!.cube.query.rows;
    // the other editor takes grid b off; here, still at the version before, a is grouped and b taken off too, in one
    // write (the tile's Remove told before the write's timer fires)
    keeper.pythonWrites([{ kind: 'removeTile', tile: b }]);
    assert.equal(await cubes.get(a)!.edit([{ kind: 'group', rows: ['desk'] }], 'Group'), null);
    (host.querySelector(`[data-tile="${b}"] .dc-tile-remove`) as HTMLElement).click();
    await written(channel);
    await settle();
    // refused whole (b is no longer on the page), said; the screen converged on the keeper's page: a's group -- refused
    // with the batch -- put back, b gone, nothing of the batch made again
    assert.deepEqual(keeper.written.map((w) => kinds(w.edits)), [[`${a}:group`, 'removeTile']], 'one write, both edits');
    assert.equal(keeper.refusedCount, 1);
    assert.ok(said.some((s) => s.startsWith('not kept:')), `the refusal said: ${said.join('; ')}`);
    assert.deepEqual(reopened, [], 'no reopen: the refused grid edit is put back, not left on screen to differ');
    const now = page!.document('Q3', undefined, true)!;
    assert.equal(pageHash(now), pageHash(keeper.page), 'one page (W17)');
    assert.deepEqual(now.cubes.find((c) => c.id === a)!.cube.query.rows, rowsBefore, "a's group is off the screen");
    assert.deepEqual([...page!.grids], [a], 'b is gone, as the other editor took it off');
    assert.equal(channel.shown, keeper.version);
    assert.deepEqual(channel.pending, [], 'nothing of the refused batch pending');
  });

  it("a selected chart's removal whose write never reached the keeper: written again once it answers, the grid's filter as the keeper then makes it (the faulted soak, seed 10; the middle version)", async () => {
    const { over, channelOf } = realPage();
    const a = page!.addGrid(over('trades.csv'));
    await settle();
    const chart = await chartOver(a);
    await settle();
    // a mark clicked: its condition on the chart's selection, and on a's filter
    await markOf(chart);
    await settle();
    const before = page!.document('Q3', undefined, true)!;
    assert.ok(before.cubes.find((c) => c.id === a)!.cube.query.filter !== undefined, "the mark is on the grid's filter");
    const keeper = new FakeKeeper(before);
    const { channel, reopened, said } = channelOf(keeper);
    // the keeper out of reach for the one write: the chart's removal -- which took its selection off a's filter here --
    // never reaches it; asked, the keeper has not taken it: the screen at the keeper's page, the removal made again there
    keeper.unreachable = 1;
    (host.querySelector(`[data-tile="${chart}"] .dc-tile-remove`) as HTMLElement).click();
    await written(channel);
    await settle();
    await written(channel);
    await settle();
    assert.deepEqual([reopened, said], [[], []], 'nothing said: nothing was lost');
    assert.equal(keeper.page.views.some((v) => v.id === chart), false, 'the removal written once the keeper answered');
    const now = page!.document('Q3', undefined, true)!;
    assert.equal(now.views.some((v) => v.id === chart), false);
    assert.deepEqual(now.cubes.find((c) => c.id === a)!.cube.query.filter, keeper.page.cubes.find((c) => c.id === a)!.cube.query.filter, "a's filter is the keeper's");
    assert.equal(pageHash(now), pageHash(keeper.page), 'one page (W17)');
    assert.deepEqual(channel.pending, []);
  });

  it("a grid only this screen's edits changed is put back at its sent cube with its charts following it: the state agrees with the screen (the three-tab soak, seed 17)", async () => {
    const { cubes, over, channelOf } = realPage();
    const a = page!.addGrid(over('trades.csv'));
    await settle();
    const chart = await chartOver(a);
    await settle();
    // a mark clicked: the chart selected, its condition on a's filter
    await markOf(chart);
    await settle();
    const before = page!.document('Q3', undefined, true)!;
    assert.ok(before.views.some((v) => v.kind === 'chart' && v.id === chart && (v.selection ?? []).length > 0), 'the chart is selected');
    const keeper = new FakeKeeper(before);
    const { channel, reopened, said } = channelOf(keeper);
    const xOf = (p: PageDocument): unknown[] => p.views.filter((v) => v.kind === 'chart').map((v) => (v.spec as { readonly x?: unknown } | undefined)?.x);
    // the other editor renames a sheet (a step that changes no grid's cube); here a is grouped by desk -- its chart
    // follows it -- and written on top: a grid only this screen's edits changed, put back at its sent cube
    keeper.pythonWrites([{ kind: 'renameSheet', sheet: before.sheets[0]!.id, name: 'Theirs' }]);
    assert.equal(await cubes.get(a)!.edit([{ kind: 'group', rows: ['desk'] }], 'Group'), null);
    await written(channel);
    await settle();
    assert.equal(keeper.combinedCount, 1);
    assert.deepEqual([reopened, said], [[], []]);
    const now = page!.document(page!.title, undefined, true)!;
    assert.equal(pageHash(now), pageHash(keeper.page), 'one page (W17)');
    assert.notDeepEqual(xOf(now), xOf(before), "the chart follows its grid's grouping");
    assert.deepEqual(xOf(page!.state.page), xOf(now), "and the page's state says the same of it as the screen");
    assert.deepEqual(JSON.parse(JSON.stringify(page!.state.page)), JSON.parse(JSON.stringify(now)), 'the state is the page on screen');
  });

  it("a page renamed by the other editor changes no grid's cube: a grid edited here meanwhile keeps its history (W13), its name the page's", async () => {
    const { cubes, over, channelOf } = realPage();
    const a = page!.addGrid(over('trades.csv'));
    await settle();
    // one step of a's history, confirmed
    assert.equal(await cubes.get(a)!.edit([{ kind: 'group', rows: ['desk'] }], 'Group'), null);
    await settle();
    const keeper = new FakeKeeper(page!.document('Q3', undefined, true)!);
    const { channel, reopened, said } = channelOf(keeper);
    // the other editor renames the page (every cube's name is the page's: page-document.ts writePageOf); here a is
    // sorted and written on top
    keeper.pythonWrites([{ kind: 'renamePage', name: 'Theirs' }]);
    assert.equal(await cubes.get(a)!.edit([{ kind: 'sorts', sorts: [{ column: 'desk', direction: 'asc' }] }], 'Sort'), null);
    await written(channel);
    await settle();
    assert.equal(keeper.combinedCount, 1);
    assert.deepEqual([reopened, said], [[], []]);
    const now = page!.document(page!.title, undefined, true)!;
    assert.equal(pageHash(now), pageHash(keeper.page), 'one page (W17), under the name the other editor gave it');
    assert.equal(now.cubes.find((c) => c.id === a)!.cube.name, 'Theirs', "a's cube is named as the page is");
    // the sort undone, then the group: two steps, as a grid only this screen's edits changed keeps them
    const g = cubes.get(a)!;
    assert.equal(g.canUndo, true);
    await g.undo();
    await settle();
    assert.equal(g.canUndo, true, 'the group is still a step of its history: the rename restarted nothing');
  });

  it("a tile taken off here and the removal undone before its write lands: the converge puts the board back, the grid's maker kept (the three-tab soak, seed 25)", async () => {
    const { over, channelOf } = realPage();
    const a = page!.addGrid(over('trades.csv'));
    const b = page!.addGrid(over('desks.csv'));
    await settle();
    const keeper = new FakeKeeper(page!.document('Q3')!);
    keeper.hold = true;
    const { channel, reopened, said } = channelOf(keeper);
    // b taken off, its write held at the keeper; the removal undone here meanwhile (b made again by its maker, told as
    // the commands between, pending); the other editor adds a sheet before the write lands
    (host.querySelector(`[data-tile="${b}"] .dc-tile-remove`) as HTMLElement).click();
    await settle();
    await new Promise((r) => setTimeout(r, 5));
    assert.deepEqual(keeper.written.map((w) => kinds(w.edits)), [['removeTile']], 'the removal sent, held');
    assert.deepEqual([...page!.grids], [a]);
    page!.undo();
    await settle();
    // back under a new id (b's is held: never given out again, H3), made by b's maker, where b was
    assert.equal(page!.grids.length, 2, 'the grid is back on the board');
    const again = [...page!.grids].find((id) => id !== a)!;
    assert.notEqual(again, b);
    assert.ok(channel.pending.some((e) => 'page' in e && e.page.kind === 'addGrid'), 'the undo told as the commands between, pending');
    keeper.pythonWrites([{ kind: 'addSheet', name: 'Theirs' }]);
    keeper.release();
    await written(channel);
    await settle();
    assert.deepEqual([reopened, said], [[], []]);
    assert.equal(keeper.combinedCount, 1);
    const now = page!.document('Q3', undefined, true)!;
    assert.equal(pageHash(now), pageHash(keeper.page), 'one page (W17)');
    assert.deepEqual([...page!.grids].sort(), [a, again].sort(), 'the grid brought back is on the board, its removal written and its return made again');
    assert.ok(keeper.page.views.some((v) => v.id === again) && !keeper.page.views.some((v) => v.id === b), "and on the keeper's page, b's removal kept");
  });

  it("a gesture while the page converges does nothing: the board is its keeper's to put back and replay (the D19 audit's 5)", async () => {
    let during = 0;
    const { cubes, over, channelOf } = realPage({ grid: () => {
      // the converge asks for a touched grid (step 1, the page held): a person clicks New Sheet meanwhile
      (host.querySelector('.dc-sheet-add') as HTMLElement).click();
      during += 1;
    } });
    const a = page!.addGrid(over('trades.csv'));
    await settle();
    const keeper = new FakeKeeper(page!.document('Q3')!);
    const { channel, reopened, said } = channelOf(keeper);
    const sheets = (): number => page!.document('Q3', undefined, true)!.sheets.length;
    // the other editor renames the page; the grid grouped here meanwhile is written on top (combined), the converge
    // putting the grid back -- the gesture lands inside it
    keeper.pythonWrites([{ kind: 'renamePage', name: 'Theirs' }]);
    assert.equal(await cubes.get(a)!.edit([{ kind: 'group', rows: ['desk'] }], 'Group'), null);
    await written(channel);
    await settle();
    assert.ok(during >= 1, 'the gesture was made while the page was held');
    assert.deepEqual([reopened, said], [[], []]);
    assert.equal(sheets(), 1, 'the gesture did nothing: no sheet');
    assert.deepEqual(channel.pending.filter((e) => 'page' in e), [], 'nothing of it told');
    assert.equal(pageHash(page!.document(page!.title, undefined, true)!), pageHash(keeper.page), 'one page (W17), under the name the other editor gave it');
    assert.equal(page!.held, false, 'released after the converge');
    // and the same click once the page is released: applied, told, written
    (host.querySelector('.dc-sheet-add') as HTMLElement).click();
    await settle();
    await written(channel);
    await settle();
    assert.equal(sheets(), 2);
    assert.equal(keeper.page.sheets.length, 2, 'written');
  });

  it("a follow is a converge: an edit made while the keeper was asked is kept, made again after the steps and written next (the D19 audit's 2)", async () => {
    const { cubes, over, channelOf } = realPage();
    const a = page!.addGrid(over('trades.csv'));
    await settle();
    const keeper = new FakeKeeper(page!.document('Q3')!);
    const { channel, reopened, said } = channelOf(keeper);
    keeper.pythonWrites([{ kind: 'addSheet', name: 'From Python' }]);
    // the keeper asked what moved the page (datacube-app.ts followLive, after its flush); before its answer is applied,
    // a grid is grouped here -- pending, its write not yet made
    const moved = await keeper.read();
    assert.equal(await cubes.get(a)!.edit([{ kind: 'group', rows: ['desk'] }], 'Group'), null);
    assert.equal(channel.pending.length, 1, 'the group pending, told');
    assert.equal(keeper.written.length, 0, 'not written yet');
    let followed = false;
    await channel.enqueue(async () => { followed = await channel.follow(moved); });
    assert.equal(followed, true, "followed: the screen holds the keeper's page");
    assert.deepEqual([reopened, said], [[], []], 'no reopen, nothing said: the edit meanwhile is no mismatch');
    // the edit made meanwhile was taken off for the hash check and made again: pending still, and written next, on top
    // of the other editor's sheet
    await written(channel);
    await settle();
    assert.equal(keeper.version, 3);
    assert.deepEqual(kinds(keeper.written.at(-1)!.edits), [`${a}:group`]);
    assert.deepEqual(keeper.page.cubes.find((c) => c.id === a)!.cube.query.rows, ['desk']);
    assert.ok(keeper.page.sheets.some((s) => s.name === 'From Python'));
    assert.equal(pageHash(page!.document('Q3', undefined, true)!), pageHash(keeper.page), 'one page (W17)');
    assert.equal(channel.shown, 3);
    assert.equal(cubes.get(a)!.canUndo, true, "the grid's own history kept: no other editor touched it (W13, the audit's 4)");
  });

  it('writes its grids\' and its own edits as commands, which the keeper makes again on top of the other editor\'s, and converges: no reopen', async () => {
    const { cubes, over, channelOf } = realPage();
    const a = page!.addGrid(over('trades.csv'));
    page!.addGrid(over('desks.csv'));
    await settle();
    const keeper = new FakeKeeper(page!.document('Q3')!);
    const { channel, reopened, said } = channelOf(keeper);
    // the other editor writes first: a sheet of its own, so the edits made here next are made against a version the
    // page has moved on from
    keeper.pythonWrites([{ kind: 'addSheet', name: 'From Python' }]);

    // edits on screen: a grid regrouped (as its own edits are applied), a grid added to the page
    assert.equal(await cubes.get(a)!.edit([{ kind: 'group', rows: ['desk'] }], 'Group'), null);
    const added = page!.addGrid(over('more.csv'));
    await settle();
    await written(channel);
    await settle();

    // its edits were commands, not the page whole -- the grid's group as its grid made it, the grid added as the page
    // made it -- and the keeper made them again on top; the screen converged on what it made
    const told = keeper.written[0]!;
    assert.deepEqual(kinds(told.edits), [`${a}:group`, 'addGrid']);
    assert.equal(keeper.version, 3, 'kept, made again on top of the other editor\'s');
    assert.deepEqual([reopened, said], [[], []], 'converged: no reopen, nothing refused');
    assert.equal(channel.shown, 3);
    assert.equal(keeper.page.views.filter((v) => v.kind === 'grid').length, 3, 'the grid added here is on the page');
    assert.ok(keeper.page.sheets.some((s) => s.name === 'From Python'), "the other editor's sheet is on the page");
    assert.deepEqual(keeper.page.cubes.find((c) => c.id === a)!.cube.query.rows, ['desk'], 'the grid regrouped here is regrouped on the page');
    // and the page on screen is the keeper's, to the hash
    const now = page!.document('Q3', undefined, true)!;
    assert.deepEqual(now.sheets.map((s) => s.name ?? null), keeper.page.sheets.map((s) => s.name ?? null));
    assert.deepEqual(now.views.map((v) => v.id).sort(), keeper.page.views.map((v) => v.id).sort());
    assert.ok(now.views.some((v) => v.id === added), 'the grid added here, under its id');
    assert.equal(pageHash(now), pageHash(keeper.page), 'one page (W17)');
    assert.equal(page!.undoLabel, 'Add Grid', 'its history kept through the converge');
  });

  it("a grid added here at once with the other editor's, under this page's tag: both kept under their own ids, the one here its own, with its history (the middle version)", async () => {
    const { over, channelOf } = realPage();
    page!.addGrid(over('trades.csv'));
    await settle();
    page!.idTag = 'tab0';
    const keeper = new FakeKeeper(page!.document('Q3')!);
    const { channel, reopened, said } = channelOf(keeper);
    keeper.pythonWrites([{ kind: 'addGrid', document: keeper.page.cubes[0]!.cube, sheet: 'sheet-1', near: null, title: 'Theirs', columns: null }]);
    const theirs = keeper.page.views.filter((v) => v.kind === 'grid').at(-1)!.id;
    const mine = page!.addGrid(over('mine.csv'));
    assert.match(mine, /^grid-tab0-\d+$/, "the tile's id under this page's tag");
    assert.notEqual(mine, theirs);
    await settle();
    await written(channel);
    await settle();
    assert.deepEqual([reopened, said], [[], []]);
    assert.equal(keeper.page.views.filter((v) => v.kind === 'grid').length, 3);
    assert.ok(keeper.page.views.some((v) => v.id === mine), 'kept under the id it was given here');
    assert.deepEqual([...page!.grids].sort(), keeper.page.views.filter((v) => v.kind === 'grid').map((v) => v.id).sort(), "the screen has the keeper's tiles");
    assert.equal(page!.document('Q3')!.views.find((v) => v.id === theirs)?.title, 'Theirs');
    assert.equal(pageHash(page!.document('Q3', undefined, true)!), pageHash(keeper.page), 'one page (W17)');
    // the step that added the grid here kept, and undone: the grid made here goes, the other editor's stays
    assert.equal(page!.undoLabel, 'Add Grid', 'its history kept');
    page!.undo();
    await settle();
    await written(channel);
    await settle();
    assert.deepEqual(said, [], 'nothing said: the step applied');
    assert.ok(!keeper.page.views.some((v) => v.id === mine) && keeper.page.views.some((v) => v.id === theirs), "undone: the grid made here gone, the other editor's kept");
  });

  it('writes its undo as the commands between, which the keeper makes again on top of another editor\'s write (D19 W3, W4)', async () => {
    const { cubes, over, channelOf } = realPage();
    const a = page!.addGrid(over('trades.csv'));
    await settle();
    const keeper = new FakeKeeper(page!.document('Q3')!);
    const { channel, reopened, said } = channelOf(keeper);
    const sheets = (): number => page!.document(page!.title || 'Q3', undefined, true)!.sheets.length;

    // a sheet added, written as its command; undone, written as the command that takes the sheet off, and kept
    (host.querySelector('.dc-sheet-add') as HTMLElement).click();
    await settle();
    await written(channel);
    assert.equal(sheets(), 2);
    page!.undo();
    await settle();
    await written(channel);
    assert.deepEqual(keeper.written.map((w) => kinds(w.edits)), [['addSheet'], ['removeSheet']], 'the undo is its commands, never the page whole');
    assert.equal(keeper.page.sheets.length, 1, 'the keeper keeps the page as the undo left it');
    // a grid's undo too (G12): its group undone, told as the grouping before it (grid-between.ts)
    const rows = (): readonly string[] => keeper.page.cubes.find((c) => c.id === a)!.cube.query.rows;
    const grouped = rows();
    assert.equal(await cubes.get(a)!.edit([{ kind: 'group', rows: ['desk'] }], 'Group'), null);
    await settle();
    await written(channel);
    await cubes.get(a)!.undo();
    await settle();
    await written(channel);
    assert.deepEqual(keeper.written.slice(2).map((w) => kinds(w.edits)[0]), [`${a}:group`, `${a}:group`]);
    assert.deepEqual(rows(), grouped, 'the group undone on the page');

    // the other editor writes; the undo here, made behind it, is made again on top as any edit is: both stand, the screen
    // converged on both
    (host.querySelector('.dc-sheet-add') as HTMLElement).click();
    await settle();
    await written(channel);
    assert.equal(keeper.page.sheets.length, 2);
    keeper.pythonWrites([{ kind: 'renamePage', name: 'From Python' }]);
    page!.undo();
    await settle();
    await written(channel);
    await settle();
    assert.deepEqual([reopened, said], [[], []], 'nothing refused, nothing reopened: an undo behind another editor\'s write is an edit behind it');
    assert.deepEqual(kinds(keeper.written.at(-1)!.edits), ['removeSheet']);
    assert.equal(keeper.page.name, 'From Python', "the other editor's write stands");
    assert.equal(keeper.page.sheets.length, 1, 'and the undo, made again on top of it');
    assert.equal(sheets(), 1, 'the screen is the keeper\'s page');
    assert.equal(page!.title, 'From Python', 'renamed as the other editor renamed it');
    assert.equal(pageHash(page!.document(page!.title, undefined, true)!), pageHash(keeper.page), 'one page (W17)');
  });
});

describe("a real page and its keeper: the D19 audit's fourth round", () => {
  /** A mark of `chart` clicked: its condition on the chart's selection and on its grid's filter. */
  const clickMark = (chart: string): Promise<void> => markOf(chart);
  const chartsOf = (p: PageDocument): string[] => p.views.filter((v) => v.kind === 'chart').map((v) => v.id);
  const gridsOf = (p: PageDocument): string[] => p.views.filter((v) => v.kind === 'grid').map((v) => v.id);

  it("1: a selected chart removed while a write is on its way (a PENDING page step that changes its grid's cube), the write combined: one page to the hash, no reopen, the removal kept", async () => {
    const { cubes, over, channelOf } = realPage();
    const a = page!.addGrid(over('trades.csv'));
    const b = page!.addGrid(over('desks.csv'));
    await settle();
    const chart = await chartOver(a);
    await settle();
    await clickMark(chart);
    await settle();
    const before = page!.document('Q3', undefined, true)!;
    assert.ok(before.cubes.find((c) => c.id === a)!.cube.query.filter !== undefined, "the mark is on a's filter");
    const keeper = new FakeKeeper(before);
    const { channel, reopened, said } = channelOf(keeper);
    keeper.pythonWrites([{ kind: 'addSheet', name: 'From Python' }]);
    keeper.hold = true;
    assert.equal(await cubes.get(b)!.edit([{ kind: 'sorts', sorts: [{ column: 'desk', direction: 'asc' }] }], 'Sort'), null);
    await settle();
    assert.equal(keeper.written.length, 1, 'the write on its way, held');
    (tileEl(chart).querySelector('.dc-tile-remove') as HTMLElement).click();
    await settle();
    assert.deepEqual(kinds(channel.pending), ['removeTile'], 'only the page command is pending');
    keeper.release();
    await written(channel);
    await settle();
    await written(channel);
    await settle();
    assert.deepEqual(reopened, [], 'no reopen');
    assert.deepEqual(said.filter((t) => t.includes(HASH_MISMATCH)), []);
    assert.equal(pageHash(page!.document('Q3', undefined, true)!), pageHash(keeper.page), 'one page');
    assert.equal(keeper.page.views.some((v) => v.id === chart), false, 'the removal reached the keeper');
  });

  it("2: a chart added here while a write is on its way, the other editor's added meanwhile: each under its own editor's id, both kept; Undo here takes off this chart, never theirs (the middle version)", async () => {
    const { cubes, over, channelOf } = realPage();
    const a = page!.addGrid(over('trades.csv'));
    await settle();
    const first = await chartOver(a);
    await settle();
    page!.idTag = 'tab0';
    const spec = (page!.document('Q3', undefined, true)!.views.find((v) => v.id === first) as { readonly spec: unknown }).spec;
    const keeper = new FakeKeeper(page!.document('Q3', undefined, true)!);
    const { channel, reopened } = channelOf(keeper);
    keeper.hold = true;
    assert.equal(await cubes.get(a)!.edit([{ kind: 'sorts', sorts: [{ column: 'desk', direction: 'asc' }] }], 'Sort'), null);
    await settle();
    assert.equal(keeper.written.length, 1, 'the write on its way, held');
    const mine = await chartOver(a);
    await settle();
    assert.match(mine, /^chart-tab0-\d+$/);
    keeper.pythonWrites([{ kind: 'addChart', near: a, spec: spec as never, title: 'Python chart', columns: null }]);
    const theirs = chartsOf(keeper.page).find((id) => id !== first)!;
    assert.notEqual(theirs, mine);
    keeper.release();
    await written(channel);
    await settle();
    await written(channel);
    await settle();
    await written(channel);
    await settle();
    assert.deepEqual(reopened, []);
    assert.deepEqual(chartsOf(keeper.page).sort(), [first, theirs, mine].sort(), "the first chart, the other editor's and this one");
    assert.equal(pageHash(page!.document('Q3', undefined, true)!), pageHash(keeper.page), 'one page');
    // Undo here: the chart made here comes off; the other editor's stays
    page!.undo();
    await settle();
    await written(channel);
    await settle();
    assert.deepEqual(chartsOf(keeper.page).sort(), [first, theirs].sort());
    assert.equal(pageHash(page!.document('Q3', undefined, true)!), pageHash(keeper.page), 'one page after the undo');
  });

  it("3: a grid added and header-sorted in ONE write, the other editor's grid added first: kept under its own id, the sort applied once (the middle version)", async () => {
    const { cubes, over, channelOf } = realPage();
    page!.addGrid(over('trades.csv'));
    await settle();
    page!.idTag = 'tab0';
    const keeper = new FakeKeeper(page!.document('Q3', undefined, true)!);
    const { channel, reopened } = channelOf(keeper);
    keeper.pythonWrites([{ kind: 'addGrid', document: keeper.page.cubes[0]!.cube, sheet: keeper.page.sheets[0]!.id, near: null, title: 'Python grid', columns: null }]);
    keeper.hold = true;
    const mine = page!.addGrid(over('more.csv'));
    // in flight at once: the write waits for the grid, so the addGrid and the sort go together
    assert.equal(await cubes.get(mine)!.edit([{ kind: 'clickSort', column: 'desk', first: 'asc', chained: false }], 'Sort'), null);
    await settle();
    await new Promise((r) => setTimeout(r, 10));
    keeper.release();
    await written(channel);
    await settle();
    await written(channel);
    await settle();
    assert.deepEqual(reopened, []);
    assert.equal(gridsOf(keeper.page).length, 3);
    assert.deepEqual(keeper.page.cubes.find((c) => c.id === mine)!.cube.query.sorts.map((x) => x.column), ['desk'], 'sorted once, under its own id');
    assert.deepEqual(cubes.get(mine)!.snapshot.sorts.map((x) => x.column), ['desk'], 'and here');
    assert.equal(pageHash(page!.document('Q3', undefined, true)!), pageHash(keeper.page), 'one page');
  });

  it("4: a grid's own Undo pressed while the page converges (the grid held): nothing moves, and the page hashes alike", async () => {
    let bId = '';
    let pressed = 0;
    let asks = 0;
    let cubesRef: Map<string, CubeApp> | undefined;
    const { cubes, over, channelOf } = realPage({ grid: (id) => {
      if (pressed > 0 || bId === '' || id !== 'grid-1') return;
      asks += 1;
      // the fourth ask for a: inside the converge, every grid held
      if (asks < 4) return;
      pressed += 1;
      void cubesRef!.get(bId)!.undo();
    } });
    cubesRef = cubes;
    const a = page!.addGrid(over('trades.csv'));
    const b = page!.addGrid(over('desks.csv'));
    bId = b;
    await settle();
    const keeper = new FakeKeeper(page!.document('Q3', undefined, true)!);
    const { channel, reopened } = channelOf(keeper);
    assert.equal(await cubes.get(b)!.edit([{ kind: 'group', rows: ['desk'] }], 'Group'), null);
    await settle();
    await written(channel);
    assert.equal(keeper.combinedCount, 0);
    const rows = cubes.get(b)!.snapshot.rows;
    // the other editor moves the page; an edit on a is written on top: combined, the page converges, and the hook
    // presses Undo on b meanwhile
    keeper.pythonWrites([{ kind: 'addSheet', name: 'From Python' }]);
    assert.equal(await cubes.get(a)!.edit([{ kind: 'sorts', sorts: [{ column: 'desk', direction: 'asc' }] }], 'Sort'), null);
    await settle();
    await written(channel);
    await settle();
    await written(channel);
    await settle();
    assert.equal(pressed, 1, 'Undo was pressed while the page converged');
    assert.deepEqual(reopened, [], 'no reopen');
    assert.deepEqual(cubes.get(b)!.snapshot.rows, rows, "b's group stands: the undo during the converge did nothing");
    assert.equal(pageHash(page!.document('Q3', undefined, true)!), pageHash(keeper.page), 'one page');
  });

  it('5: a refused batch [removeTile b, sorts a]: b made again by the put-back, and a gesture on it before the steps are replayed is refused -- held as it appears', async () => {
    let bId = '';
    let asks = 0;
    let fired = false;
    let refused: string | null | undefined;
    let cubesRef: Map<string, CubeApp> | undefined;
    const { cubes, over, channelOf } = realPage({ grid: (id) => {
      if (fired || bId === '' || id !== bId) return;
      asks += 1;
      // the second ask for b after the board was put at the keeper's page made it again (the first holds it): the
      // window the fourth round found b unheld in
      if (asks < 2) return;
      const b = cubesRef!.get(bId);
      if (b === undefined) return;
      fired = true;
      void b.edit([{ kind: 'group', rows: ['desk'] }], 'Group during converge').then((r) => { refused = r; });
    } });
    cubesRef = cubes;
    const a = page!.addGrid(over('trades.csv'));
    const b = page!.addGrid(over('desks.csv'));
    bId = b;
    await settle();
    const keeper = new FakeKeeper(page!.document('Q3', undefined, true)!);
    const { channel, reopened } = channelOf(keeper);
    keeper.hold = true;
    (tileEl(b).querySelector('.dc-tile-remove') as HTMLElement).click();
    assert.equal(await cubes.get(a)!.edit([{ kind: 'sorts', sorts: [{ column: 'desk', direction: 'asc' }] }], 'Sort'), null);
    await settle();
    await new Promise((r) => setTimeout(r, 10));
    // the other editor takes a off: the batch is refused whole, b comes back by the put-back
    keeper.pythonWrites([{ kind: 'removeTile', tile: a }]);
    keeper.release();
    await written(channel);
    await settle();
    await written(channel);
    await settle();
    await written(channel);
    await settle();
    assert.equal(fired, true, 'the hook ran');
    assert.equal(refused, 'the page is catching up with its keeper: a moment', 'the gesture on the grid made again was refused');
    assert.deepEqual(reopened, []);
    assert.equal(pageHash(page!.document('Q3', undefined, true)!), pageHash(keeper.page), 'one page');
  });

  it("6: this screen's own removal of a selected chart, made again as its sent edit: its grid, which no other editor touched, keeps its history (W13)", async () => {
    const { cubes, over, channelOf } = realPage();
    const a = page!.addGrid(over('trades.csv'));
    await settle();
    const chart = await chartOver(a);
    await settle();
    await clickMark(chart);
    await settle();
    const keeper = new FakeKeeper(page!.document('Q3', undefined, true)!);
    const { channel, reopened } = channelOf(keeper);
    assert.equal(await cubes.get(a)!.edit([{ kind: 'group', rows: ['desk'] }], 'Group'), null);
    await settle();
    await written(channel);
    assert.equal(keeper.combinedCount, 0);
    const depth = cubes.get(a)!.state.historyDepth.past;
    assert.ok(depth >= 1);
    keeper.pythonWrites([{ kind: 'addSheet', name: 'From Python' }]);
    (tileEl(chart).querySelector('.dc-tile-remove') as HTMLElement).click();
    await settle();
    await written(channel);
    await settle();
    await written(channel);
    await settle();
    assert.equal(keeper.combinedCount, 1);
    assert.deepEqual(reopened, []);
    assert.equal(pageHash(page!.document('Q3', undefined, true)!), pageHash(keeper.page), 'one page');
    assert.ok(cubes.get(a)!.state.historyDepth.past >= depth, "a's history kept: no other editor edited a");
  });

  it("7: a grid added here while a write is on its way, the other editor's added meanwhile: theirs opened as theirs, this one kept under its own id (the middle version)", async () => {
    const hooks: Hooks = {};
    const { cubes, over, channelOf } = realPage(hooks);
    const a = page!.addGrid(over('trades.csv'));
    await settle();
    page!.idTag = 'tab0';
    const keeper = new FakeKeeper(page!.document('Q3', undefined, true)!);
    const { channel, reopened } = channelOf(keeper);
    keeper.hold = true;
    assert.equal(await cubes.get(a)!.edit([{ kind: 'sorts', sorts: [{ column: 'desk', direction: 'asc' }] }], 'Sort'), null);
    await settle();
    assert.equal(keeper.written.length, 1);
    const mine = page!.addGrid(over('more.csv'));
    await settle();
    const doc = { ...keeper.page.cubes[0]!.cube, query: { ...keeper.page.cubes[0]!.cube.query, rows: ['desk'] } };
    keeper.pythonWrites([{ kind: 'addGrid', document: doc as never, sheet: keeper.page.sheets[0]!.id, near: null, title: 'Python grid', columns: null }]);
    const theirs = gridsOf(keeper.page).find((id) => id !== a)!;
    assert.notEqual(theirs, mine);
    const opened: [string, boolean][] = [];
    hooks.opened = (grid, several) => { opened.push([grid, several]); };
    keeper.release();
    await written(channel);
    await settle();
    await written(channel);
    await settle();
    await written(channel);
    await settle();
    hooks.opened = undefined;
    assert.deepEqual(reopened, []);
    // opened capped, as a grid sharing its page is (the middle version's audit's 8)
    assert.deepEqual(opened, [[theirs, true]]);
    assert.deepEqual(gridsOf(keeper.page).sort(), [a, theirs, mine].sort(), "a, the other editor's and this one");
    assert.deepEqual(cubes.get(theirs)!.snapshot.rows, ['desk'], "the other editor's grid shows their cube");
    assert.equal((cubes.get(theirs)!.snapshot.source as { readonly name?: string }).name, 'trades.csv', 'over their source');
    assert.deepEqual([...page!.grids].sort(), gridsOf(keeper.page).sort());
    assert.equal(pageHash(page!.document('Q3', undefined, true)!), pageHash(keeper.page), 'one page');
  });

  it("9: a grid added here in a write the keeper never saw: written again once it answers, and the other editor's grid added meanwhile kept as theirs -- Undo here takes off only this one (the faulted soak, seed 27; the middle version)", async () => {
    const { cubes, over, channelOf } = realPage();
    const a = page!.addGrid(over('trades.csv'));
    await settle();
    page!.idTag = 'tab0';
    const keeper = new FakeKeeper(page!.document('Q3', undefined, true)!);
    const { channel, reopened, said } = channelOf(keeper);
    // the other editor adds a grid; the keeper out of reach for the one write that adds one here
    const doc = { ...keeper.page.cubes[0]!.cube, query: { ...keeper.page.cubes[0]!.cube.query, rows: ['desk'] } };
    keeper.pythonWrites([{ kind: 'addGrid', document: doc as never, sheet: keeper.page.sheets[0]!.id, near: null, title: 'Python grid', columns: null }]);
    const theirs = gridsOf(keeper.page).find((id) => id !== a)!;
    keeper.unreachable = 1;
    const mine = page!.addGrid(over('more.csv'));
    await settle();
    await written(channel);
    await settle();
    await written(channel);
    await settle();
    assert.deepEqual([reopened, said], [[], []], 'nothing said: nothing was lost');
    assert.deepEqual(gridsOf(keeper.page).sort(), [a, theirs, mine].sort(), 'written again, and kept beside theirs');
    assert.equal((cubes.get(theirs)!.snapshot.source as { readonly name?: string }).name, 'trades.csv', "the other editor's grid reads their source");
    assert.deepEqual(cubes.get(theirs)!.snapshot.rows, ['desk'], 'and shows their cube');
    assert.equal(pageHash(page!.document('Q3', undefined, true)!), pageHash(keeper.page), 'one page');
    // Undo here: the grid made here comes off; the other editor's stays
    assert.equal(page!.undoLabel, 'Add Grid');
    page!.undo();
    await settle();
    await written(channel);
    await settle();
    assert.deepEqual(gridsOf(keeper.page).sort(), [a, theirs].sort(), "the other editor's grid is still on the page");
    assert.equal(pageHash(page!.document('Q3', undefined, true)!), pageHash(keeper.page), 'one page still');
  });

  for (const variant of ['combined', 'follow', 'lost'] as const) {
    it(`11 (${variant}): a snap made here, pending when the page converges, keeps its copy, hashes as the keeper's and is written (the confirmation audit's 1)`, async () => {
      // the write it rides answered combined, a follow first, or its write lost and fenced off: in each the page
      // converges with the snap pending, its grid kept at the person's copy
      const { cubes, channelOf } = realPage();
      const runner = new PlanThenRun(new StubPlanner(), engine) as unknown as QueryRunner;
      const hosted: GridMaker = (gridHost, spawned) => {
        const made = new CubeApp(gridHost, SNAPSHOT, {
          runner, cubeSource: file('trades.csv'), sourceLabel: 'trades.csv', compact: true, ...(page ? { windowHost: page.root } : {}),
          hostSnap: {
            store: 'Python',
            snap: async () => ({ rowCount: 3, table: 'snap_2', source: SNAPSHOT.source.query, runner }),
            release: async () => {},
            held: async (table) => ({ table, source: SNAPSHOT.source.query, runner }),
          },
          ...spawned,
        });
        if (spawned.id !== undefined) cubes.set(spawned.id, made);
        return made;
      };
      const a = page!.addGrid(hosted);
      await settle();
      page!.idTag = 'tab0';
      const keeper = new FakeKeeper(page!.document('Q3', undefined, true)!);
      const { channel, reopened } = channelOf(keeper);
      if (variant === 'lost') keeper.unreachable = 1;
      if (variant === 'combined') {
        keeper.hold = true;
        channel.edit({ page: { kind: 'renamePage', name: 'Here' } });
        channel.changed();
        await new Promise((r) => setTimeout(r, 20));
      }
      // the person's Snap
      (host.querySelector('.dc-titlebar-toggle') as HTMLButtonElement).click();
      await settle();
      await settle();
      assert.equal(cubes.get(a)!.copy?.table, 'snap_2', 'snapped here');
      if (variant !== 'lost') keeper.pythonWrites([{ kind: 'addSheet', name: 'From Python' }]);
      if (variant === 'combined') keeper.release();
      if (variant === 'follow') {
        const moved = await keeper.read();
        await channel.enqueue(async () => { await channel.follow(moved); });
      }
      await written(channel);
      await settle();
      await written(channel);
      await settle();
      assert.deepEqual(reopened, [], 'no reopen, no mismatch');
      assert.equal(cubes.get(a)!.copy?.table, 'snap_2', 'still snapped');
      assert.equal(keeper.page.copies?.[a]?.table, 'snap_2', 'the snap written to the keeper');
      assert.equal(pageHash(page!.document(page!.title || 'Q3', undefined, true)!), pageHash(keeper.page), 'one page');
    });
  }

  it("10: another editor's mark on a chart and the grid's undo of its refilter, followed in one move: the selection the keeper took off is off here too (the long soak, seed 3)", async () => {
    const { cubes, over, channelOf } = realPage();
    const a = page!.addGrid(over('trades.csv'));
    await settle();
    const before = page!.document('Q3', undefined, true)!;
    const keeper = new FakeKeeper(before);
    const { channel, reopened } = channelOf(keeper);
    // the other editor (this page, writing as it goes): a chart, a mark on it, then the grid's own Undo of the mark's
    // refilter -- the keeper judges the chart's selection where the filter moves, and takes it off at the undo
    const chart = await chartOver(a);
    await settle();
    await clickMark(chart);
    await settle();
    await written(channel);
    await cubes.get(a)!.undo();
    await settle();
    await written(channel);
    await settle();
    assert.ok(!keeper.page.views.some((v) => v.kind === 'chart' && (v.selection ?? []).length > 0), "the keeper took the selection off with the undo of the mark's refilter");
    // a tab behind: the page as it was, following all of it in one move
    const makers = new Map<string, GridMaker>();
    for (const c of before.cubes) makers.set(c.id, over(c.cube.source.name));
    await channel.following(async () => { page!.restore(before, makers, idsOf(before)); await page!.ready(); await settle(); });
    channel.opened(1, before, idsOf(before));
    const moves = [...keeper.steps.values()].flatMap((step) => kinds(step));
    assert.ok(moves.includes(`${a}:refilter`) && moves.includes(`${a}:filter`), `the mark's refilter and the undo's filter are among the steps: ${moves.join(', ')}`);
    const moved = await keeper.read();
    await channel.enqueue(async () => { await channel.follow(moved); });
    await settle();
    assert.deepEqual(reopened, []);
    assert.equal(pageHash(page!.document('Q3', undefined, true)!), pageHash(keeper.page), 'one page: the selection off here too');
  });

  it('8: a follow with nothing moved since the version shown (a poll stale by the time its work ran) converges on nothing', async () => {
    const keeper = new FakeKeeper(trades);
    const { channel, s } = opened(keeper);
    assert.equal(await channel.follow(await keeper.read()), true);
    assert.deepEqual(s.converged, []);
    assert.equal(channel.shown, keeper.version);
  });
});

describe("a write's answer lost, or never coming: the middle version's audit", () => {
  const condition = { kind: 'condition', column: 'region', operator: 'equal', value: 'EMEA' } as FilterNode;
  const filterOf = (keeper: FakeKeeper): unknown =>
    keeper.page.cubes.find((c) => c.id === keeper.page.views.find((v) => v.id === 'grid')?.cube)?.cube.query.filter;

  it("4: a write whose answer is lost while the keeper still works on it (a proxy's 504): the ask fences it, so it is kept once", async () => {
    const keeper = new FakeKeeper(trades);
    let finish: (() => void) | undefined;
    let original: Promise<unknown> | undefined;
    let first = true;
    const wire: LivePage = {
      read: async (writer?: string, fence?: number) => {
        const read = await keeper.read(writer, fence);
        // the keeper finishes the original just after it answered the ask
        if (finish) { finish(); await original; }
        return read;
      },
      report: async (at: number, write: LiveWrite): Promise<LiveSettled> => {
        if (first) {
          first = false;
          // the write reaches the keeper, which is slow; the proxy answers the tab first
          const gate = new Promise<void>((r) => { finish = r; });
          original = gate.then(() => keeper.report(at, write)).catch(() => undefined);
          throw new Error('the engine answered 504: gateway timeout');
        }
        return keeper.report(at, write);
      },
    };
    const s = screen(keeper.page);
    const channel = new LiveChannel(wire, s, 0);
    s.channel = channel;
    channel.opened(keeper.version, keeper.page, keeper.held);
    await made(s, channel, { grid: 'grid', command: { kind: 'addFilter', condition } } as LiveCommand);
    await written(channel);
    await written(channel);
    const steps = [...keeper.steps.values()].flatMap((e) => kinds(e)).filter((k) => k === 'grid:addFilter');
    assert.equal(steps.length, 1, `kept once: ${JSON.stringify(steps)}`);
    assert.equal(JSON.stringify(filterOf(keeper)).split('EMEA').length - 1, 1, 'the grid filtered by EMEA once');
    assert.deepEqual(s.reopened, []);
    channel.stop();
  });

  it('3: a write the keeper never answers, its reads answered: tried UNANSWERED_AT_MOST times, then the page opened again, said', async () => {
    const keeper = new FakeKeeper(trades);
    let reports = 0;
    const wire: LivePage = {
      read: (writer?: string, fence?: number) => keeper.read(writer, fence),
      report: async (): Promise<LiveSettled> => { reports += 1; throw new TypeError('fetch failed'); },
    };
    const s = screen(keeper.page);
    const channel = new LiveChannel(wire, s, 0);
    s.channel = channel;
    channel.opened(keeper.version, keeper.page, keeper.held);
    await made(s, channel, { page: { kind: 'renamePage', name: 'Here' } });
    for (let i = 0; i < 10; i += 1) await written(channel);
    assert.equal(reports, 3, 'three tries');
    assert.equal(s.reopened.length, 1);
    assert.match(s.reopened[0]!, /took none of 3 writes in a row: fetch failed; 1 change made here meanwhile was not kept/);
    channel.stop();
  });

  it("3: a page opened afresh counts no unanswered write from before it (the confirmation audit's 3)", async () => {
    // two writes lost and not taken; the third lost and its ask too: the page opened again, not known -- then, on the
    // page opened afresh, one write lost is one, never the third in a row
    const keeper = new FakeKeeper(trades);
    let failing = 3;
    let reads = 0;
    const wire: LivePage = {
      read: async (writer?: string, fence?: number) => {
        reads += 1;
        if (reads === 3) throw new TypeError('fetch failed');
        return keeper.read(writer, fence);
      },
      report: async (at: number, write: LiveWrite): Promise<LiveSettled> => {
        if (failing > 0) {
          failing -= 1;
          throw new TypeError('fetch failed');
        }
        return keeper.report(at, write);
      },
    };
    const s = screen(keeper.page);
    const channel = new LiveChannel(wire, s, 0);
    s.channel = channel;
    channel.opened(keeper.version, keeper.page, keeper.held);
    await made(s, channel, { page: { kind: 'renamePage', name: 'Here' } });
    for (let i = 0; i < 4; i += 1) await written(channel);
    assert.equal(s.reopened.length, 1);
    assert.match(s.reopened[0]!, /whether the last change sent was kept is not known$/);
    // the app's reopen: the page as its keeper has it, opened afresh
    channel.opened(keeper.version, keeper.page, keeper.held);
    s.now = keeper.page;
    failing = 1;
    await made(s, channel, { page: { kind: 'renamePage', name: 'There' } });
    for (let i = 0; i < 4; i += 1) await written(channel);
    assert.equal(s.reopened.length, 1, `one unanswered write on the page opened afresh: ${JSON.stringify(s.reopened)}`);
    assert.equal(keeper.page.name, 'There', 'made again and written');
    channel.stop();
  });

  it("7: the keeper out of reach for the write and the ask: the page opened again, the last change never said lost (it may be kept)", async () => {
    const keeper = new FakeKeeper(trades);
    const { channel, s } = opened(keeper);
    keeper.unreachable = 1;
    keeper.readsFail = 1;
    await made(s, channel, { page: { kind: 'renamePage', name: 'Here' } });
    await written(channel);
    assert.equal(s.reopened.length, 1);
    assert.match(s.reopened[0]!, /whether the last change sent was kept is not known$/);
    channel.stop();
  });
});
