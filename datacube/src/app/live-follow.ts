// FOLLOWING ANOTHER EDITOR'S PAGE BY ITS COMMANDS (docs/DATACUBE_LIVE_COMMANDS_2026_10_18.md): Python's change applied
// to the page as it is open -- each run of page commands to the page, each run of one grid's commands to that grid --
// or refused, so the page is opened again as Python has it. No DOM: the app hands it what it needs (a `LiveTarget`), so
// every rule here is tested alone (test/live-follow.test.ts).

import { gridCapped, leavesOf, type CubeDocument } from '../cube-document.ts';
import type { GridCommand } from '../grid-commands.ts';
import type { PageCommand } from '../page-commands.ts';
import type { GridMaker } from '../page/page-app.ts';
import type { LiveCommand, Versions } from './services.ts';

/** What following needs of the page on screen. */
export interface LiveTarget {
  /** The grids on the board, by their tiles' ids: a grid kept off it for a detached chart is closed, as far as Python is. */
  readonly grids: readonly string[];
  /** How many cubes the page holds now (a grid added beside others is capped). */
  readonly cubes: number;
  /** Page commands applied to the page and the screen put at them (PageApp.follow); refused in their words. */
  follow(commands: readonly PageCommand[], makers: ReadonlyMap<string, GridMaker>): Promise<void>;
  /** A grid's commands applied to it as one change: the refusal, or null. */
  applyGrid(grid: string, commands: readonly GridCommand[]): Promise<string | null>;
  /** An added grid's cube opened, as a saved page's are: its maker, and how to let it go unused; undefined, not opened. */
  open(document: CubeDocument, grid: string, several: boolean): Promise<{ readonly make: GridMaker; readonly discard?: () => Promise<void> } | undefined>;
  /** The version of a frame the page has read, or undefined. */
  frameVersion(frame: string): number | undefined;
}

/**
 * How a move of the editor's page is followed: by its commands; by opening it again when a frame moved with it (a grid
 * the commands add would read the frame as it is now while the others read it as it was: all read it again together);
 * or, the page not moved, by its frames alone.
 */
export function followBy(now: Versions, shown: number, frameVersion: (frame: string) => number | undefined): 'commands' | 'open' | 'frames' {
  if (now.version === undefined || now.version === shown) return 'frames';
  const framesMoved = Object.entries(now.frames).some(([frame, version]) => {
    const read = frameVersion(frame);
    return read !== undefined && read !== version;
  });
  return framesMoved ? 'open' : 'commands';
}

/** The editor's commands, in order: refused (thrown) when one does not apply here, the page then to be opened again. */
export async function applyLive(commands: readonly LiveCommand[], target: LiveTarget): Promise<void> {
  for (let i = 0; i < commands.length;) {
    const first = commands[i]!;
    if ('page' in first) {
      const run: PageCommand[] = [];
      for (let c = commands[i]; c && 'page' in c; c = commands[++i]) run.push(c.page);
      await followPage(run, target);
      continue;
    }
    const grid = first.grid;
    const run: GridCommand[] = [];
    for (let c = commands[i]; c && 'grid' in c && c.grid === grid; c = commands[++i]) run.push(c.command);
    // a grid closed here -- kept off the board for a detached chart, or gone -- is no grid Python's command is for
    if (!target.grids.includes(grid)) throw new Error(`grid ${grid} was closed on this page`);
    const refused = await target.applyGrid(grid, run);
    if (refused !== null) throw new Error(refused);
  }
}

/** A run of page commands: each grid they add made by the host's maker for its cube, then the page put at them. */
async function followPage(run: readonly PageCommand[], target: LiveTarget): Promise<void> {
  // a cube its host opened again over its source as it is now: the page reads that source again, opened again whole
  const rebuilt = run.find((c) => c.kind === 'rebuilt');
  if (rebuilt) throw new Error(`its grid ${rebuilt.cube} was opened again over its source as it is now`);
  const makers = new Map<string, GridMaker>();
  const discards: (() => Promise<void>)[] = [];
  try {
    for (const command of run) {
      if (command.kind !== 'addGrid' || command.id === undefined || command.document === null) continue;
      const source = command.document.source;
      // the frames it reads, its leaves' (a join's of frames, each), as the page read each before it was opened
      const frames = leavesOf(source).flatMap((l) => (l._type === 'frame' ? [l.name] : []));
      const read = new Map(frames.map((f) => [f, target.frameVersion(f)]));
      const o = await target.open(command.document, command.id, gridCapped(target.cubes + makers.size + 1));
      if (!o) throw new Error(`${source.name} was not opened`);
      if (o.discard) discards.push(o.discard);
      makers.set(command.id, o.make);
      // a frame moved while it was opened: the page's other grids over it read it as it was -- opened again, all
      // read it as it is now
      for (const [frame, version] of read) {
        if (version !== undefined && target.frameVersion(frame) !== version) {
          throw new Error(`the frame ${frame} moved while a grid over it was added`);
        }
      }
    }
    await target.follow(run, makers);
  } catch (e) {
    for (const discard of discards) await discard();
    throw e;
  }
}
