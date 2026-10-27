// THE TWO-EDITOR SOAK (docs/DATACUBE_LIVE_EDITING_2026_10_24.md §6, W15): TWO real pages (PageApp, their grids CubeApps
// over the fixture's gated engine) open on ONE keeper (test/fake-keeper.ts: a keeper as Python's, applying every write by
// DataCube's one rule), with Python's stand-in writing between them, interleaved at random -- grid edits, sheets and grids
// added, tiles taken off, undo and redo on each tab -- each tab's writes held at random so writes combine, get refused,
// and give out ids afresh. After every quiet point (every write landed, each tab followed to the keeper's version with
// nothing pending) the three hold ONE page, to the hash (W17), and neither tab ever opened the page again (no reopen
// cause arises here) nor said the message that must never show. The seeds run here; a failure names its seed and step:
// LEGEND_LITE_SOAK_SEED=n runs that seed alone, each step and each write's answer logged; LEGEND_LITE_SOAK_SEEDS=n runs
// more (60 run green, 2026-10-24).

import assert from 'node:assert/strict';
import { afterEach, beforeEach, describe, it } from 'node:test';
import { JSDOM } from 'jsdom';

import { CubeApp } from '../src/app.ts';
import { DEFAULT_CONFIGURATION } from '../src/config.ts';
import type { FileSource } from '../src/cube-document.ts';
import { HASH_MISMATCH, LiveChannel } from '../src/app/live-channel.ts';
import { convergeOn } from '../src/app/live-converge.ts';
import type { PageCommand } from '../src/page-commands.ts';
import { PageApp, type GridMaker } from '../src/page/page-app.ts';
import type { PageDocument } from '../src/page-document.ts';
import { pageHash } from '../src/page-hash.ts';
import { GateEngine, SNAPSHOT, StubPlanner, settle, plus } from './cube-fixture.ts';
import { FakeKeeper } from './fake-keeper.ts';
import type { LiveCommand } from '../src/app/services.ts';
import { openCube } from '../src/cube-document.ts';
import { differing } from './page-same.ts';
import * as echarts from 'echarts/core';
import '../src/ui/page-filter-bar.ts';

/** Seeds per run: 24 in CI (2026-10-24, the gaps after the third audit: 6 before; three tabs and the keeper's faults
 *  since, about 45 s on a desk, the test `medium`), more by hand (the forty-seed runs that found seeds 10, 17, 25 and
 *  27 of this file's history). */
const SEEDS = Number(process.env['LEGEND_LITE_SOAK_SEEDS'] ?? '24');
/** One seed run alone (LEGEND_LITE_SOAK_SEED=4), its steps logged. */
const ONLY = process.env['LEGEND_LITE_SOAK_SEED'] === undefined ? undefined : Number(process.env['LEGEND_LITE_SOAK_SEED']);
/** Steps per seed; LEGEND_LITE_SOAK_STEPS=300 is a long session's worth, run alone. */
const STEPS = Number(process.env['LEGEND_LITE_SOAK_STEPS'] ?? '14');
/** The keeper's faults (a write it never sees, an answer lost) in the mix; LEGEND_LITE_SOAK_FAULTS=0 leaves them out. */
const FAULTS = process.env['LEGEND_LITE_SOAK_FAULTS'] !== '0';

/** How two pages differ, for a failure's message: each path that differs, with both sides. */
function unlike(ours: PageDocument, theirs: PageDocument): string {
  return differing(JSON.parse(JSON.stringify(ours)), JSON.parse(JSON.stringify(theirs))).join('\n  ');
}

/** A seeded random number generator (mulberry32): a failure says its seed, run alone again. */
function random(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

let dom: JSDOM;
let engine: GateEngine;
const tabs: Tab[] = [];

beforeEach(() => {
  dom = new JSDOM('<!doctype html><div id="a"></div><div id="b"></div><div id="c"></div>');
  const g = globalThis as unknown as Record<string, unknown>;
  g['window'] = dom.window;
  g['document'] = dom.window.document;
  g['requestAnimationFrame'] = (cb: FrameRequestCallback) => { cb(0); return 1; };
  g['cancelAnimationFrame'] = () => {};
  const inert: ProxyHandler<object> = {
    get: (_t, key) => (key === 'canvas' ? dom.window.document.createElement('canvas') : key === 'measureText' ? () => ({ width: 0 }) : () => undefined),
    set: () => true,
  };
  (dom.window.HTMLCanvasElement.prototype as unknown as { getContext: () => unknown }).getContext = () => new Proxy({}, inert);
  engine = new GateEngine();
});

afterEach(async () => {
  for (const tab of tabs.splice(0)) tab.dispose();
  await settle();
});

const file = (name: string): FileSource => ({
  _type: 'file', name, format: 'csv', size: 10, sha256: name.padEnd(64, '0').slice(0, 64),
  columns: SNAPSHOT.columns.map((c) => ({ name: c.name, type: c.type })),
});

/**
 * A TAB: a real page on the keeper, wired as DataCube's app wires one (datacube-app.ts): its grids' commands and its own
 * told to its channel, converging by the one rule (live-converge.ts), following the keeper's moves by their commands
 * (live-follow.ts applyLive, after a flush, as the app follows), opening the page again only when told to -- which this
 * soak counts as a failure, since no reopen cause arises in it.
 */
class Tab {
  readonly name: string;
  readonly keeper: FakeKeeper;
  readonly page: PageApp;
  readonly cubes = new Map<string, CubeApp>();
  readonly channel: LiveChannel;
  readonly reopened: string[] = [];
  readonly said: string[] = [];
  readonly host: HTMLElement;

  constructor(name: string, host: HTMLElement, keeper: FakeKeeper) {
    this.name = name;
    this.keeper = keeper;
    this.host = host;
    const over = (source: string): GridMaker => (gridHost, spawned, start) => {
      const made = new CubeApp(gridHost, start?.snapshot ?? SNAPSHOT, {
        engine, planner: new StubPlanner(), compact: true, cubeSource: file(source), sourceLabel: source,
        configuration: start?.configuration ?? { ...DEFAULT_CONFIGURATION, reportTitle: source },
        ...spawned,
        onCommand: (command) => { if (spawned.id !== undefined) this.channel.edit({ grid: spawned.id, command }); },
      });
      if (spawned.id !== undefined) this.cubes.set(spawned.id, made);
      return made;
    };
    this.over = over;
    this.page = new PageApp({
      host, title: keeper.page.name, empty: (slot) => { slot.textContent = 'empty'; },
      onChange: () => this.channel.changed(),
      onPageCommand: (command) => this.channel.edit({ page: command }),
    });
    // each tab's ids its own (page-ids.ts nextPageId), as the app tags a page open on a keeper
    this.page.idTag = `t${name}`;
    const now = (): PageDocument | undefined => this.page.document(this.page.title || keeper.page.name, undefined, true);
    this.channel = new LiveChannel(keeper, {
      page: now,
      busy: () => [...this.cubes.values()].some((c) => c.busy),
      converge: async (on) => {
        // the seed run alone: each converge's inputs, and the state against the screen before and after
        const k = (es: readonly LiveCommand[]): string[] => es.map((e) => ('page' in e ? `page:${e.page.kind}` : `${e.grid}:${e.command.kind}`));
        if (ONLY !== undefined) console.log(`# ${this.name} converges: to views ${on.target.views.map((v) => v.id).join(',')} refused ${JSON.stringify(k(on.refused))} pending ${JSON.stringify(k(on.pending))}; state vs screen: ${unlike(this.page.state.page, this.shown()) || 'same'}`);
        const out = await convergeOn(on, {
          page: this.page,
          grid: (id) => this.cubes.get(id),
          pageNow: now,
          // another editor's grid: over the source its document says, at the cube it says (as the app opens one)
          open: async (document) => ({ make: (gridHost, spawned, start) => {
            const opened = openCube(document, document.source as never, SNAPSHOT.columns);
            return over(document.source.name)(gridHost, spawned, start ?? { snapshot: opened.snapshot, configuration: opened.configuration, tree: opened.tree });
          } }),
        });
        if (ONLY !== undefined) console.log(`# ${this.name} converged; restarted ${JSON.stringify([...out.restarted])}; state vs screen: ${unlike(this.page.state.page, this.shown()) || 'same'}`);
        return out;
      },
      reapply: async (pending, restarted) => {
        for (const edit of pending) {
          if ('page' in edit) this.page.reapply([edit.page], 'made again');
          else await this.cubes.get(edit.grid)?.edit([edit.command], 'made again', { again: !restarted.has(edit.grid) });
        }
      },
      reopen: async (why) => {
        if (ONLY !== undefined) console.log(`# ${this.name} reopens at v${keeper.version}: ${why}; here views ${this.shown().views.map((v) => v.id).join(',')} sheets ${this.shown().sheets.map((v) => v.id).join(',')}`);
        this.reopened.push(`${why}\n  ${unlike(this.shown(), keeper.page)}`);
        await this.restore();
      },
      say: (text) => { this.said.push(text); },
    }, 0);
  }

  readonly over: (source: string) => GridMaker;

  /** The page opened as the keeper has it. */
  open(): Promise<void> {
    return this.restore();
  }

  /** The page put at the keeper's, whole: how it is opened, and opened again. */
  async restore(): Promise<void> {
    const makers = new Map<string, GridMaker>();
    for (const c of this.keeper.page.cubes) makers.set(c.id, this.over(c.cube.source.name));
    await this.channel.following(async () => {
      this.page.restore(this.keeper.page, makers, this.keeper.held);
      await this.page.ready();
      await settle();
    });
    this.channel.opened(this.keeper.version, this.keeper.page, this.keeper.held);
  }

  /** The keeper's page followed, as the app follows it (datacube-app.ts followLive): what is pending written first, the
   *  screen quiet, then the screen put at the keeper's page, read whole (live-channel.ts follow): one that cannot be
   *  set, or a hash that differs, opens the page again -- a reopen, counted. */
  follow(): Promise<void> {
    return this.channel.enqueue(async () => {
      await this.channel.flush(true);
      const at = this.channel.shown;
      if (at === undefined || at === this.keeper.version) return;
      await this.channel.follow(await this.keeper.read());
    });
  }

  /** The page as this tab shows it now. */
  shown(): PageDocument {
    return this.page.document(this.page.title || this.keeper.page.name, undefined, true)!;
  }

  dispose(): void {
    this.channel.stop();
    this.page.dispose();
  }
}

/** A quiet point: every write let go and landed, every tab followed to the keeper's version with nothing pending --
 *  passes of draining and following until no tab is behind (a tab's write, landed late, moves the keeper past the
 *  other's follow). */
async function quiet(all: readonly Tab[], keeper: FakeKeeper): Promise<void> {
  keeper.release();
  for (let pass = 0; pass < 20; pass += 1) {
    for (let round = 0; round < 3; round += 1) {
      await settle();
      await new Promise((r) => setTimeout(r, 5));
      for (const t of all) await t.channel.idle();
    }
    for (const t of all) await t.follow();
    for (const t of all) await t.channel.idle();
    await settle();
    if (all.every((t) => t.channel.shown === keeper.version && t.channel.pending.length === 0)) return;
  }
  throw new Error(`never quiet: ${all.map((t) => `${t.name} at v${t.channel.shown ?? '?'} with ${t.channel.pending.length} pending`).join(', ')}, the keeper at v${keeper.version}`);
}

const tileOf = (tab: Tab, id: string): HTMLElement | null => tab.host.querySelector(`[data-tile="${id}"]`);
const labelOf = (el: HTMLElement | null): string => el?.querySelector('.dc-menu-label')?.textContent ?? el?.textContent ?? '';
/** The charts on a tab's board. */
const chartsOf = (tab: Tab): string[] => tab.page.views().views.filter((v) => v.kind === 'chart').map((v) => v.id);

/** A chart made over a grid's tile as a person makes one (a cell's menu, Create > Visualization): its id, or undefined. */
async function chartOver(tab: Tab, grid: string): Promise<string | undefined> {
  const before = new Set(chartsOf(tab));
  const cell = tileOf(tab, grid)?.querySelector('.dc-row .dc-cell') as HTMLElement | null;
  if (!cell) return undefined;
  cell.dispatchEvent(new dom.window.MouseEvent('contextmenu', { bubbles: true, cancelable: true }));
  await settle();
  const item = [...dom.window.document.querySelectorAll<HTMLElement>('.dc-menu .dc-menu-item')]
    .find((el) => labelOf(el) === 'Visualization' && labelOf(el.parentElement!.closest('.dc-menu-item') as HTMLElement) === 'Create');
  if (!item) return undefined;
  item.click();
  await settle();
  return chartsOf(tab).find((id) => !before.has(id));
}

/** A mark of a chart clicked (its condition on the chart's selection, and on its grid's filter): false when it has
 *  not drawn. */
function clickMark(tab: Tab, chart: string): boolean {
  const el = tileOf(tab, chart)?.querySelector<HTMLElement>('[_echarts_instance_]');
  const instance = el ? echarts.getInstanceByDom(el) : undefined;
  if (!instance) return false;
  instance.trigger('click', { seriesIndex: 0, dataIndex: 0 } as never);
  return true;
}

/** One edit on a tab, as a person makes one: its kind, or undefined when there was none to make. */
async function gesture(tab: Tab, rand: () => number, n: number): Promise<string | undefined> {
  const grids = tab.page.grids;
  const sheets = tab.page.sheets;
  const pick = <T>(xs: readonly T[]): T => xs[Math.floor(rand() * xs.length)]!;
  const choice = Math.floor(rand() * 12);
  switch (choice) {
    case 9: {
      // a chart over a grid, as its menu makes one (its selection, when a mark is clicked, is on the grid's filter: a
      // page step of the other editor's that takes it off changes the grid's cube -- the audit's second round, 1)
      if (grids.length === 0) return undefined;
      const chart = await chartOver(tab, pick(grids));
      if (chart === undefined) return undefined;
      // and, mostly, a mark clicked right away: its selection on the grid's filter
      if (rand() < 0.7 && clickMark(tab, chart)) {
        await settle();
        return 'addChart+mark';
      }
      return 'addChart';
    }
    case 10: {
      const charts = chartsOf(tab);
      if (charts.length === 0) return undefined;
      if (!clickMark(tab, pick(charts))) return undefined;
      await settle();
      return 'clickMark';
    }
    case 11: {
      const charts = chartsOf(tab);
      if (charts.length === 0) return undefined;
      const remove = tileOf(tab, pick(charts))?.querySelector('.dc-tile-remove') as HTMLElement | null;
      if (!remove) return undefined;
      remove.click();
      await settle();
      return 'removeChart';
    }
    case 0:
    case 1: {
      if (grids.length === 0) return undefined;
      const grid = tab.cubes.get(pick(grids));
      if (!grid) return undefined;
      await grid.edit([{ kind: 'group', rows: pick([['desk'], ['region'], ['region', 'desk'], []]) }], 'Group');
      return 'group';
    }
    case 2: {
      if (grids.length === 0) return undefined;
      const grid = tab.cubes.get(pick(grids));
      if (!grid) return undefined;
      await grid.edit([{ kind: 'sorts', sorts: rand() < 0.5 ? [] : [{ column: pick(['desk', 'region']), direction: pick(['asc', 'desc']) }] }], 'Sort');
      return 'sorts';
    }
    case 3: {
      tab.page.addGrid(tab.over(`more-${tab.name}-${n}.csv`));
      await settle();
      return 'addGrid';
    }
    case 4: {
      plus(tab.host);
      await settle();
      return 'addSheet';
    }
    case 5: {
      if (grids.length < 2) return undefined;
      // a tile taken off, as its title bar's Remove takes it
      const tile = tab.host.querySelector(`[data-tile="${pick(grids)}"] .dc-tile-remove`) as HTMLElement | null;
      if (!tile) return undefined;
      tile.click();
      await settle();
      return 'removeTile';
    }
    case 6: {
      if (sheets.length < 2) return undefined;
      // a sheet renamed, as its tab is: double-clicked, a name typed, Enter
      const tabs = [...tab.host.querySelectorAll<HTMLElement>('.dc-sheet-tab')];
      const which = tabs[Math.floor(rand() * tabs.length)];
      if (!which) return undefined;
      which.dispatchEvent(new dom.window.MouseEvent('dblclick', { bubbles: true }));
      const field = tab.host.querySelector<HTMLInputElement>('.dc-sheet-tab input');
      if (!field) return undefined;
      field.value = `Sheet ${tab.name} ${n}`;
      field.dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: 'Enter' }));
      await settle();
      return 'renameSheet';
    }
    case 7: {
      if (tab.page.undoLabel === undefined) return undefined;
      tab.page.undo();
      await settle();
      return 'undo';
    }
    default: {
      if (tab.page.redoLabel === undefined) {
        const grid = grids.length > 0 ? tab.cubes.get(pick(grids)) : undefined;
        if (grid && grid.canUndo) {
          await grid.undo();
          return 'grid undo';
        }
        return undefined;
      }
      tab.page.redo();
      await settle();
      return 'redo';
    }
  }
}

/** Python's stand-in: a page command written at the keeper's version now. */
function pythonEdit(keeper: FakeKeeper, rand: () => number, n: number): string {
  const pick = <T>(xs: readonly T[]): T => xs[Math.floor(rand() * xs.length)]!;
  const grids = keeper.page.views.filter((v) => v.kind === 'grid');
  const charts = keeper.page.views.filter((v): v is Extract<typeof v, { kind: 'chart' }> => v.kind === 'chart');
  const choice = Math.floor(rand() * 6);
  let command: PageCommand;
  if (choice === 0) command = { kind: 'addSheet', name: `Python ${n}` };
  else if (choice === 1) command = { kind: 'renamePage', name: `Named in Python ${n}` };
  else if (choice === 2 && grids.length > 0) command = { kind: 'renameTile', tile: pick(grids).id, title: `Titled in Python ${n}` };
  // a chart taken off by the other editor: a page step that changes its grid's cube when a mark was clicked on it (a
  // selected one first)
  else if (choice === 3 && charts.length > 0) command = { kind: 'removeTile', tile: pick(charts.filter((c) => (c.selection ?? []).length > 0).length > 0 ? charts.filter((c) => (c.selection ?? []).length > 0) : charts).id };
  // a chart made by the other editor, as one of the page's charts is (page.chart in Python)
  else if (choice === 4 && charts.length > 0 && grids.length > 0) command = { kind: 'addChart', near: pick(grids).id, spec: pick(charts).spec, title: null, columns: null };
  else if (grids.length > 1) command = { kind: 'removeTile', tile: pick(grids).id };
  else command = { kind: 'addSheet', name: `Python ${n}` };
  keeper.pythonWrites([command]);
  return command.kind;
}

describe('two tabs and Python on one page, interleaved at random, hold one page (W15)', () => {
  it(`${SEEDS} seeds of ${STEPS} steps: after every quiet point one page to the hash, no reopen, never the message that must never show`, async () => {
    const made: Record<string, number> = {};
    let combined = 0;
    let refused = 0;
    for (let seed = ONLY ?? 1; seed <= (ONLY ?? SEEDS); seed += 1) {
      const rand = random(seed);
      // the page as Python builds it: two grids
      const a = new Tab('a', dom.window.document.getElementById('a') as unknown as HTMLElement, new FakeKeeper({
        kind: 'datacube.page', version: 6, name: 'Soak', cubes: [], views: [],
        sheets: [{ id: 'sheet-1', layout: { kind: 'bands', fit: true, bands: [] } }],
      } as PageDocument));
      tabs.push(a);
      a.page.addGrid(a.over('trades.csv'));
      a.page.addGrid(a.over('desks.csv'));
      await settle();
      const keeper = new FakeKeeper(a.shown());
      if (ONLY !== undefined) {
        // the seed run alone: every write and its answer logged
        const report = keeper.report.bind(keeper);
        keeper.report = async (at, write) => {
          const what = write.edits.map((e) => ('page' in e ? `page:${e.page.kind}` : `${e.grid}:${e.command.kind}`)).join(' ');
          try {
            const said = await report(at, write);
            console.log(`# write at v${at} [${what}] #${write.seq} -> v${said.version}${said.page ? ' combined' : ''}; keeper views ${keeper.page.views.map((v) => v.id).join(',')} sheets ${keeper.page.sheets.map((v) => v.id).join(',')}`);
            return said;
          } catch (e) {
            console.log(`# write at v${at} [${what}] -> refused: ${e instanceof Error ? e.message : String(e)}`);
            throw e;
          }
        };
      }
      // the keeper's page is what both tabs open: a's page is replaced by the keeper's, as a tab opens one
      const first = new Tab('a', dom.window.document.getElementById('a') as unknown as HTMLElement, keeper);
      a.dispose();
      tabs.pop();
      tabs.push(first);
      const second = new Tab('b', dom.window.document.getElementById('b') as unknown as HTMLElement, keeper);
      tabs.push(second);
      // a third tab (2026-10-24, the gaps after the third audit): three editors and Python, so two tabs converge on the
      // same move while the third writes
      const third = new Tab('c', dom.window.document.getElementById('c') as unknown as HTMLElement, keeper);
      tabs.push(third);
      await first.open();
      await second.open();
      await third.open();
      const all = [first, second, third];
      let where = `seed ${seed}`;
      try {
      for (let step = 0; step < STEPS; step += 1) {
        where = `seed ${seed}, step ${step}`;
        // the keeper slow to answer, at random: writes made meanwhile combine, or are made again on top
        keeper.hold = rand() < 0.4;
        // the keeper out of reach for one write (the tab converges on what moved, nothing of the write kept), or a
        // write kept whose answer is lost (the tab converges, its edit coming back as the keeper's step)
        const fault = rand();
        if (FAULTS && fault < 0.05) keeper.unreachable = 1;
        keeper.loseAnswer = FAULTS && fault >= 0.05 && fault < 0.15;
        const who = Math.floor(rand() * 7);
        let kind: string | undefined;
        if (who === 0) kind = `python:${pythonEdit(keeper, rand, step)}`;
        else {
          const tab = all[(who - 1) % all.length]!;
          kind = await gesture(tab, rand, step);
          if (kind !== undefined) kind = `${tab.name}:${kind}`;
        }
        if (kind !== undefined) made[kind] = (made[kind] ?? 0) + 1;
        if (ONLY !== undefined) console.log(`# ${where}: ${kind ?? '(nothing)'}${keeper.hold ? ', keeper held' : ''}, keeper at v${keeper.version}; ${all.map((t) => `${t.name} at v${t.channel.shown ?? '?'} pending ${t.channel.pending.length}`).join(', ')}`);
        if (rand() < 0.5) {
          // a quiet point: everything written, followed, and compared
          await quiet(all, keeper);
          for (const t of all) {
            assert.deepEqual(t.said.filter((s) => s === HASH_MISMATCH), [], `${where}: ${t.name} said the message that must never show`);
            assert.deepEqual(t.reopened, [], `${where}: ${t.name} opened the page again`);
            assert.equal(pageHash(t.shown()), pageHash(keeper.page), `${where}: ${t.name} holds another page than the keeper (${t.said.join('; ')})\n  ${unlike(t.shown(), keeper.page)}`);
            assert.deepEqual(t.page.state.page, t.shown(), `${where}: ${t.name}'s state is the page on screen\n  ${unlike(t.page.state.page, t.shown())}`);
          }
        }
      }
      await quiet(all, keeper);
      for (const t of all) {
        assert.deepEqual(t.reopened, [], `seed ${seed}: ${t.name} opened the page again`);
        assert.equal(pageHash(t.shown()), pageHash(keeper.page), `seed ${seed}: ${t.name} holds another page than the keeper at the end\n  ${unlike(t.shown(), keeper.page)}`);
      }
      } catch (e) {
        // whatever fails names its seed and step, to run alone (LEGEND_LITE_SOAK_SEED)
        if (e instanceof assert.AssertionError) throw e;
        throw new Error(`${where}: ${e instanceof Error ? e.message : String(e)}`, { cause: e });
      }
      combined += keeper.combinedCount;
      refused += keeper.refusedCount;
      first.dispose();
      second.dispose();
      third.dispose();
      tabs.splice(0);
    }
    // (one seed run alone may have no combined write: the claim is the run's)
    if (ONLY === undefined) assert.ok(combined > 0, 'some write was made again on top of another editor\'s (else this proved nothing about converging)');
    assert.ok(Object.keys(made).length >= 6, `every kind of edit was tried somewhere: ${JSON.stringify(made)}`);
    console.log(`# soak: ${SEEDS} seeds, ${combined} writes combined, ${refused} refused, edits ${JSON.stringify(made)}`);
  });
});
