// THE SCREEN PUT AT ITS KEEPER'S PAGE (docs/DATACUBE_LIVE_EDITING_REDESIGN_2026_10_24.md, the middle version): a write
// the keeper made another page of, one it refused, one whose answer never came, and a follow all end here, with the
// keeper's page whole. The screen does not replay anyone's steps: the page it must show is the keeper's, and the screen
// is set to it in one pass, inside the channel's `following` (live-channel.ts):
//   1. each grid on the board and the keeper's page at its cube there, by its own commands (grid-between.ts) -- its
//      history kept when no other editor changed it (its cube there is the one this screen last knew the keeper to hold,
//      with what it sent), started again when one did, or the refused batch touched it (W13) -- before the board is
//      set, so a chart placed over it follows the grid as the keeper has it, never as it was here (chart-spec.ts
//      followCube: a chart regroups as its grid is; seed 31 of the three-tab soak);
//   2. the board at the keeper's page (PageApp.putBack): its tiles, sheets, layouts and charts, no widget's own side
//      effect firing (a chart dropped takes nothing off its grid: the keeper's cube says the grid's filter); a grid of it
//      this page has never had (another editor's) opened from its cube, as a saved page's are; one this page let go,
//      made again by its own maker from its cube there -- and any grid still not at its cube there set so, as in 1 --
//      and each grid's rows where the keeper's page says (CubeApp.followSnap, followLive: another editor's snap read
//      from its host, its going live the live rows again; the page writes a grid's copy from the grid, §18.8);
//   3. every grid's change in flight landed, and the page on screen read: the keeper's, which the channel checks against
//      the keeper's hash (W17) before the pending edits are made again (a restarted grid's as new steps, the others' as
//      no step).
// The page and its grids are held throughout (PageApp.hold, CubeApp.hold): a gesture meanwhile does nothing on the page
// and is refused on a grid, said, rather than land on a board that is being set (the D19 audit's 5; its third round's 1).
// Throws when the screen cannot be set: the page is then opened again, said. No DOM of its own: the app hands it the page
// and its grids, so the rule is tested with real pages alone (test/live-channel.test.ts).

import { configurationOf, gridCapped, treeOf, type CubeDocument } from '../cube-document.ts';
import { gridBetween } from '../grid-between.ts';
import type { GridCommand } from '../grid-commands.ts';
import type { PageCommand } from '../page-commands.ts';
import type { GridCopy, GridView, PageDocument } from '../page-document.ts';
import type { GridMaker, PageApp } from '../page/page-app.ts';
import { sameValue, type CubeSnapshot } from '../snapshot.ts';
import type { Converge, Converged } from './live-channel.ts';

/** A grid on the page, as the converge needs it: its state, and its commands applied as another editor's or its own. */
export interface ConvergeGrid {
  /** Held through the converge: a person's edit refused meanwhile (CubeApp.hold). */
  hold(): void;
  release(): void;
  /** A change of the grid in flight (its query not answered yet). */
  readonly busy: boolean;
  readonly snapshot: CubeSnapshot;
  readonly configuration: ReturnType<typeof configurationOf>;
  readonly tree: ReturnType<typeof treeOf>;
  /** The copy it reads (undefined: its live rows), and how another editor's snap or going live is followed. */
  readonly copy: GridCopy | undefined;
  followSnap?(copy: GridCopy): Promise<void>;
  followLive?(): Promise<void>;
  /** `own`: a step of its history, told nothing; `keepHistory`: its keeper's put-back of a grid no other editor touched. */
  applyGridCommands(commands: readonly GridCommand[], label: string, own?: boolean, keepHistory?: boolean): Promise<string | null>;
}

/** What the converge works on: the page, its grids by tile, how a grid another editor added is opened. */
export interface ConvergeDeps {
  readonly page: PageApp;
  grid(id: string): ConvergeGrid | undefined;
  /** The page as the screen writes it now. */
  pageNow(): PageDocument | undefined;
  /** A grid another editor added, opened from its cube as a saved page's are: its maker, or undefined (not opened). */
  open(document: CubeDocument, grid: string, several: boolean): Promise<{ readonly make: GridMaker } | undefined>;
}

/** How long a grid's change in flight is waited for before the page is read (the channel's QUIET_AT_MOST). */
const QUIET_AT_MOST = 10_000;

export async function convergeOn(on: Converge, deps: ConvergeDeps): Promise<Converged> {
  const { page } = deps;
  page.hold();
  const held = new Set<ConvergeGrid>();
  // every grid on the board held, each made as the screen is set held as it appears (the fourth round's 5)
  const holdGrids = (): void => {
    for (const id of page.grids) {
      const grid = deps.grid(id);
      if (grid !== undefined && !held.has(grid)) {
        grid.hold();
        held.add(grid);
      }
    }
  };
  holdGrids();
  try {
    return await converging(on, deps, holdGrids);
  } finally {
    for (const grid of held) grid.release();
    page.release();
  }
}

async function converging(on: Converge, deps: ConvergeDeps, holdGrids: () => void): Promise<Converged> {
  const { page } = deps;
  const was = new Map(page.grids.map((id) => [id, deps.grid(id)] as const));
  // the grids whose history starts again: another editor changed them (their cube on the keeper's page is not the one
  // this screen last knew there, with what it sent -- a cube's name is the page's, and is no change of the grid's), or
  // the refused batch edited them (their history holds steps the keeper never took)
  const theirs = new Set<string>();
  for (const view of on.target.views) {
    if (view.kind === 'grid' && !sameCube(cubeIn(on.base, view.id), cubeIn(on.target, view.id))) theirs.add(view.id);
  }
  for (const e of on.refused) if ('grid' in e) theirs.add(e.grid);
  // a grid another editor opened again over its source as it is now (`rebuilt`: a frame's columns moved) is no state a
  // grid's commands reach -- the page is opened again, said, as before the middle version (the audit's 6)
  for (const view of on.target.views) {
    if (view.kind !== 'grid') continue;
    const was = cubeIn(on.base, view.id);
    const now = cubeIn(on.target, view.id);
    if (was !== undefined && now !== undefined && !sameValue(was.source, now.source)) {
      throw new Error(`${view.id} was opened again over its source as it is now`);
    }
  }
  // each grid at its cube there, by its own commands
  const gridsAtTarget = async (): Promise<void> => {
    for (const id of page.grids) {
      const grid = deps.grid(id);
      const cube = cubeIn(on.target, id);
      if (grid === undefined || cube === undefined) continue;
      const back = gridBetween({ snapshot: grid.snapshot, config: grid.configuration, tree: grid.tree }, draftOf(cube, grid.snapshot));
      if (back.length === 0) continue;
      const refused = await grid.applyGridCommands(back, 'its keeper', false, !theirs.has(id));
      if (refused !== null) throw new Error(`${id} could not be put at its keeper's: ${refused}`);
    }
  };
  // 1. the grids the board and the keeper's page both have, at their cubes there
  await gridsAtTarget();
  // 2. the board at the keeper's page; a grid of it this page has never had opened from its cube there
  const makers = new Map<string, GridMaker>();
  for (const view of on.target.views) {
    if (view.kind !== 'grid' || page.grids.includes(view.id) || page.knows(view.id)) continue;
    const cube = cubeIn(on.target, view.id);
    if (cube === undefined) continue;
    // capped as a grid sharing its page is (the page it joins: the keeper's)
    const opened = await deps.open(cube, view.id, gridCapped(on.target.views.filter((v) => v.kind === 'grid').length));
    if (opened === undefined) throw new Error(`${cube.source.name} was not opened`);
    makers.set(view.id, opened.make);
  }
  const refusedSteps = on.refused.flatMap((e): PageCommand[] => ('page' in e ? [e.page] : []));
  page.putBack(on.target, on.held, makers, refusedSteps);
  holdGrids();
  await gridsAtTarget();
  // each grid's rows where the keeper's page says: its copy, or its live rows -- but a grid a pending edit snaps or makes
  // live, which keeps where the person put it: the edit is made again on the keeper's page after, and written (the
  // audit's 5)
  const ownCopy = new Set(on.pending.flatMap((e) => ('page' in e && (e.page.kind === 'snap' || e.page.kind === 'live') ? [e.page.grid] : [])));
  for (const id of page.grids) {
    if (ownCopy.has(id)) continue;
    const grid = deps.grid(id);
    const view = on.target.views.find((v): v is GridView => v.kind === 'grid' && v.id === id);
    if (grid === undefined || view === undefined) continue;
    const copy = (on.target.copies ?? {})[view.cube];
    if (sameValue(grid.copy, copy)) continue;
    if (!grid.followSnap || !grid.followLive) throw new Error(`${id} cannot read its other editor's copy here`);
    if (copy === undefined) await grid.followLive();
    else await grid.followSnap(copy);
  }
  holdGrids();
  await page.ready();
  // 3. every grid's change in flight landed (a chart's removal refilters its grid, asynchronously) before the page is
  //    read: the hash is of the page as it will be, never of a state passing (the D19 audit's second round, 2)
  const until = Date.now() + QUIET_AT_MOST;
  while (page.grids.some((id) => deps.grid(id)?.busy === true) && Date.now() < until) await new Promise((r) => setTimeout(r, 10));
  const now = deps.pageNow();
  if (now === undefined) throw new Error('the page cannot say what it is');
  // a grid a pending snap or live names kept its own rows (above), its copy the pending edit's: the page is checked
  // against the keeper's as it is before the pending edits are made again, so that grid's copy is said as the keeper's
  // there -- the edit, made again and written next, then makes the keeper's page this one (the confirmation audit's 1)
  const checked = ownCopy.size === 0 ? now : withKeepersCopies(now, on.target, ownCopy);
  // a grid made as the screen was set (opened, or made again) starts its history here; one a pending edit makes is made
  // again with it, from the cube that edit gave it
  const remade = page.grids.filter((id) => deps.grid(id) !== was.get(id));
  const made = on.pending.flatMap((e) => ('page' in e && e.page.kind === 'addGrid' && e.page.id !== undefined ? [e.page.id] : []));
  return { page: checked, restarted: new Set([...theirs, ...remade, ...made]) };
}

/** `page` with the copy of each of `grids`' cubes as `target` (the keeper's page) has it -- none where it has none. */
function withKeepersCopies(page: PageDocument, target: PageDocument, grids: ReadonlySet<string>): PageDocument {
  const copies = { ...(page.copies ?? {}) };
  for (const id of grids) {
    const cube = page.views.find((v) => v.id === id)?.cube;
    if (cube === undefined) continue;
    const theirs = (target.copies ?? {})[cube];
    if (theirs === undefined) delete copies[cube];
    else copies[cube] = theirs;
  }
  const { copies: _all, ...rest } = page;
  // as the page writes it (page-document.ts writePageOf): no field when it has none
  return Object.keys(copies).length > 0 ? { ...rest, copies } : rest;
}

/** The cube a grid tile of `page` shows, or undefined. */
function cubeIn(page: PageDocument, id: string): CubeDocument | undefined {
  const view = page.views.find((v) => v.id === id);
  return view === undefined ? undefined : page.cubes.find((c) => c.id === view.cube)?.cube;
}

/** Two cubes alike as their grid holds them: less their name, which is the page's (page-document.ts writePageOf). */
function sameCube(a: CubeDocument | undefined, b: CubeDocument | undefined): boolean {
  const own = (c: CubeDocument | undefined): Omit<CubeDocument, 'name'> | undefined => {
    if (c === undefined) return undefined;
    const { name: _page, ...rest } = c;
    return rest;
  };
  return sameValue(own(a), own(b));
}

/** A cube document as a grid's draft, over the grid's source (what a run alone adds, the column types, is no part). */
function draftOf(cube: CubeDocument, like: CubeSnapshot): { snapshot: CubeSnapshot; config: ReturnType<typeof configurationOf>; tree: ReturnType<typeof treeOf> } {
  return { snapshot: { ...cube.query, source: like.source, epoch: 0 } as CubeSnapshot, config: configurationOf(cube), tree: treeOf(cube) };
}
