// ONE WRITER'S EDITS MADE AGAIN ON TOP OF A PAGE THAT MOVED ON (docs/DATACUBE_PAGE_KEEPER_2026_10_20.md): the one rule
// by which a page two editors write -- Python's page and a tab open on it -- takes an edit made against a version the
// page has moved on from. Whoever keeps the page asks it (headless pageRebase, for Python's keeper); no editor keeps a
// rule of its own for it. Pure: the page, the ids it has held, the edits; a grid's edit applied by the function its
// host gives (a grid's commands need its base, which only the host has).

import type { CubeDocument } from './cube-document.ts';
import { leavesOf } from './cube-document.ts';
import type { GridCommand } from './grid-commands.ts';
import { add, asSaved, remove, tiles, type Bands } from './layout/bands.ts';
import { applyPageCommand, followedPage, idsOf, type PageCommand, type PageDraft } from './page-commands.ts';
import type { PageDocument, PageLayout } from './page-document.ts';
import { CubeRefusal } from './snapshot.ts';

/** One edit of a page, as its writer made it: a page command, or a grid's command by its tile's id. */
export type PageEdit =
  | { readonly page: PageCommand }
  | { readonly grid: string; readonly command: GridCommand };

/**
 * A grid's commands applied to its cube (`id`: the cube's id on `page`, the page as the edits before left it -- whether
 * the grid is snapped is the page's), as the screen applies them: the cube they make, or a CubeRefusal in DataCube's
 * words. Its host's: only the host has a grid's base (its frame, its copy).
 */
export type GridApplier = (page: PageDocument, cube: CubeDocument, id: string, commands: readonly GridCommand[]) =>
  Promise<CubeDocument>;

/** Edits made again on top: the page they make, the ids it has held, and the edits as applied there. */
export interface Rebased {
  readonly page: PageDocument;
  readonly held: readonly string[];
  readonly edits: readonly PageEdit[];
}

/** `id` as the edits made again call it: its new name when an edit before gave it out afresh. */
const named = (id: string, renamed: ReadonlyMap<string, string>): string => renamed.get(id) ?? id;

/** A layout with each tile renamed: its bands hold tiles' ids and nothing else that is text. */
function renamedLayout(node: unknown, renamed: ReadonlyMap<string, string>): unknown {
  if (typeof node === 'string') return named(node, renamed);
  if (Array.isArray(node)) return node.map((n) => renamedLayout(n, renamed));
  if (typeof node === 'object' && node !== null) {
    return Object.fromEntries(Object.entries(node).map(([k, v]) => [k, renamedLayout(v, renamed)]));
  }
  return node;
}

/** A page command with every id it names renamed (the one it makes is its own, never renamed here; a page filter's is
 *  the one it sets, which an edit before may have made afresh). */
export function renamedCommand(command: PageCommand, renamed: ReadonlyMap<string, string>): PageCommand {
  if (renamed.size === 0) return command;
  const n = (id: string): string => named(id, renamed);
  switch (command.kind) {
    case 'renamePage':
    case 'savedAs':
    case 'modelInto':
    case 'modelPin':
      return command;
    // a maker's planned id follows too (page-state.ts rename: a tile brought back under a new id is that tile in every
    // later step; the keeper gives a taken one out afresh either way)
    case 'addSheet':
      return { ...command, ...(command.id !== undefined ? { id: n(command.id) } : {}) };
    case 'modelKeep':
    case 'rebuilt':
      return { ...command, cube: n(command.cube) };
    case 'renameSheet':
    case 'moveSheet':
    case 'removeSheet':
      return { ...command, sheet: n(command.sheet) };
    case 'renameTile':
    case 'removeTile':
      return { ...command, tile: n(command.tile) };
    case 'addGrid':
      return { ...command, ...(command.id !== undefined ? { id: n(command.id) } : {}), sheet: n(command.sheet), near: command.near === null ? null : n(command.near) };
    case 'addChart':
      return { ...command, ...(command.id !== undefined ? { id: n(command.id) } : {}), near: n(command.near) };
    case 'setChart':
    case 'select':
      return { ...command, chart: n(command.chart) };
    case 'followGrid':
      return { ...command, chart: n(command.chart), grid: n(command.grid) };
    case 'detach':
      return { ...command, chart: n(command.chart), cube: n(command.cube) };
    case 'moveTile':
      return { ...command, tile: n(command.tile), sheet: n(command.sheet) };
    case 'stack':
      return { ...command, tiles: command.tiles.map(n) };
    case 'arrange':
      return { ...command, sheet: n(command.sheet), order: command.order === null ? null : command.order.map(n) };
    case 'layout':
      return { ...command, sheet: n(command.sheet), layout: renamedLayout(command.layout, renamed) as typeof command.layout };
    case 'setPageFilter':
      return { ...command, ...(command.id !== undefined ? { id: n(command.id) } : {}), from: command.from === null ? null : n(command.from) };
    case 'removePageFilter':
      return { ...command, filter: n(command.filter) };
    case 'snap':
    case 'live':
      return { ...command, grid: n(command.grid) };
  }
}

/**
 * A WRITER'S ARRANGEMENT OF A SHEET MADE AGAIN ON TOP (a layout whole -- a drag, a resize -- or an arrangement in an
 * order): the places of the tiles it knew, as it arranged them; a tile on that sheet now that it did not know (added or
 * moved there by the other writer) placed as adding places one; one it knew that is no longer there (taken off or moved
 * away) left out -- so the other writer's tiles keep their sheet, and neither writer's arrangement is refused for them.
 */
export function rearranged(command: PageCommand, page: PageDocument): PageCommand {
  if (command.kind !== 'layout' && command.kind !== 'arrange') return command;
  const sheet = page.sheets.find((s) => s.id === command.sheet);
  if (sheet === undefined) return command;
  const now = tiles(sheet.layout);
  if (command.kind === 'arrange') {
    if (command.order === null) return command;
    const knew = command.order;
    return { ...command, order: [...knew.filter((t) => now.includes(t)), ...now.filter((t) => !knew.includes(t))] };
  }
  const { kind: _kind, ...given } = command.layout as PageLayout & Bands;
  let bands = given as Bands;
  try {
    for (const t of tiles(bands)) if (!now.includes(t)) bands = remove(bands, t);
    for (const t of now) if (!tiles(bands).includes(t)) bands = add(bands, t);
  } catch {
    // not a layout the page reads: refused by the command's own rule, as written
    return command;
  }
  return { ...command, layout: { kind: 'bands', ...asSaved(bands) } as PageLayout };
}

/** `page` with its cube `id` replaced by `cube` (as a grid's edit leaves it: the page otherwise as it was). */
export function withCube(page: PageDocument, id: string, cube: CubeDocument): PageDocument {
  return { ...page, cubes: page.cubes.map((c) => (c.id === id ? { id, cube } : c)) };
}

/**
 * EDITS MADE AGAIN ON TOP: `edits`, made by one writer against a version of the page it has moved on from, applied in
 * order to `draft` (the page as it is now, and every id it has held) -- each page command by the page commands' own
 * function, each grid's by `applyGrid`. Every id a command makes is its editor's own (page-ids.ts nextPageId: an editor's
 * tag), so none is given out afresh and nothing is renamed (docs/DATACUBE_LIVE_EDITING_REDESIGN_2026_10_24.md, the
 * middle version): one the page holds already is refused, by the command's own rule. One that no longer applies -- its
 * tile or sheet taken off there, a column gone -- refuses them all, in DataCube's words (CubeRefusal): a writer's edits
 * are one change, applied whole or not at all. A sheet's arrangement keeps the other writer's tiles on it (rearranged).
 */
export async function rebaseEdits(draft: PageDraft, edits: readonly PageEdit[], applyGrid: GridApplier): Promise<Rebased> {
  let d = draft;
  const applied: PageEdit[] = [];
  for (let i = 0; i < edits.length;) {
    const edit = edits[i]!;
    if ('grid' in edit) {
      const grid = edit.grid;
      const view = d.page.views.find((v) => v.id === grid);
      if (view === undefined || view.kind !== 'grid') throw new CubeRefusal(`${grid} is no longer on the page`);
      const held = d.page.cubes.find((c) => c.id === view.cube);
      if (held === undefined) throw new CubeRefusal(`${grid} shows no cube the page holds`);
      // the grid's run of commands (an undo's few, a gesture's), applied in one call -- one open of its cube, one compile
      // (docs/DATACUBE_LIVE_EDITING_2026_10_24.md W14) -- the same reduce, refused whole, each listed as applied
      const run: GridCommand[] = [];
      for (let c = edits[i]; c !== undefined && 'grid' in c && c.grid === grid; c = edits[++i]) run.push(c.command);
      // the cube as the grid's edits leave it, its charts following it (page-commands.ts followedPage: V1, V2)
      d = { ...d, page: followedPage(withCube(d.page, view.cube, await applyGrid(d.page, held.cube, view.cube, run)), view.cube, d.page) };
      for (const command of run) applied.push({ grid, command });
      continue;
    }
    i += 1;
    const one = applyPageCommand(d, rearranged(edit.page, d.page));
    d = one.draft;
    applied.push({ page: one.command });
  }
  return { page: d.page, held: d.held, edits: applied };
}

/**
 * WHAT A PAGE READS, as its keeper needs it and never reads it itself: the frames its cubes read (their leaves', each
 * once, in order), the copies its snapped grids read (their tables), and every id it holds -- its tiles', cubes' and
 * sheets', and its page filters' (a writer's edit of a filter the page held then is told from one it made: H3, the S4
 * audit's 3).
 */
export function pageReads(page: PageDocument): PageReads {
  const leaves = page.cubes.flatMap((c) => leavesOf(c.cube.source));
  const sorted = (keys: readonly string[]): string[] => [...new Set(keys)].sort();
  return {
    frames: sorted(leaves.flatMap((leaf) => (leaf._type === 'frame' ? [leaf.name] : []))),
    copies: sorted(Object.values(page.copies ?? {}).map((c) => c.table)),
    // the files it reads by their bytes' hash, the remote files by URL: what a host holds a table for while a page reads
    // it, as it holds a copy (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md X3, D14)
    files: sorted(leaves.flatMap((leaf) => (leaf._type === 'file' ? [leaf.sha256] : []))),
    remotes: sorted(leaves.flatMap((leaf) => (leaf._type === 'remoteFile' ? [leaf.url] : []))),
    ids: [...idsOf(page), ...(page.filters ?? []).map((f) => f.id)],
  };
}

/** What a page reads, as its keeper takes it (pageReads). */
export interface PageReads {
  /** The frames its cubes read, by name. */
  readonly frames: string[];
  /** The copies its snapped grids read, by table. */
  readonly copies: string[];
  /** The files its cubes read, by their bytes' SHA-256. */
  readonly files: string[];
  /** The remote files its cubes read, by URL. */
  readonly remotes: string[];
  /** Every id it holds. */
  readonly ids: string[];
}
