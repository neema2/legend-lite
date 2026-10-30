// A PAGE'S GESTURES AS THEIR COMMANDS (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md S8; the user, 2026-10-22: "find all
// [places the UI is coupled to the implementation] and guard against this"). What a gesture on a page means, written
// outside the screen: a layout the board drew told as the page saves it, a hand move told as what the hand did, a sheet
// renamed by the names' one rule, Show Grid's steps. The board keeps what is its alone -- where a drag let go, how many
// tiles fit side by side, which of its tiles the page saves (an editing grid is the board's moment, never the page's) --
// and passes it. A gesture whose every field is the person's input (a tile renamed as typed, a sheet removed) is built
// by pageCommand, as Python's generated builder builds it (_commands.py, from the same kinds and fields).

import { ExactNumber } from '../../pure-protocol/src/index.ts';
import type { ChartSpec } from './chart-spec.ts';
import { asSaved, remove, tiles, type Bands, type Preset } from './layout/bands.ts';
import { fieldNames } from './command-fields.ts';
import { idsOf, nextPageId, PAGE_COMMAND_FIELDS, takenIds, type PageCommand } from './page-commands.ts';
import { PageDocumentError, readPage, type PageDocument, type PageLayout } from './page-document.ts';
import { FlowRefusal, arranged, containerMoves, type FlowBox, type Frame } from './layout/flow.ts';
import { liveGrids } from './live-inputs.ts';
import { pickCommands, type InputPick } from './dynamic-input.ts';
import { givenName } from './page-names.ts';
import { CubeRefusal, sameValue } from './snapshot.ts';
import { inputOn, inputSides, joinInputs, moreIndex, type CubeDocument, type CubeSource, type InputSide } from './cube-document.ts';

/** A page command of `kind` whose every field is the person's input -- what they typed, picked or pointed at -- built
 *  as Python's generated builder builds it (_commands.py, from the same list): its kind, then its fields in the order
 *  PAGE_COMMAND_FIELDS lists them, an optional one not given left out; nothing decided. A field the list does not have
 *  is a programming error, thrown. */
export function pageCommand<K extends PageCommand['kind']>(kind: K, fields: Omit<Extract<PageCommand, { readonly kind: K }>, 'kind'>):
  Extract<PageCommand, { readonly kind: K }> {
  const listed: readonly string[] = PAGE_COMMAND_FIELDS[kind];
  const given = fields as Readonly<Record<string, unknown>>;
  const names = fieldNames(listed);
  const odd = Object.keys(given).filter((k) => !names.includes(k));
  if (odd.length > 0) throw new Error(`a '${kind}' command has no field ${odd.join(', ')}`);
  const written: Record<string, unknown> = { kind };
  for (const field of listed) {
    const name = field.replace(/\?$/, '');
    if (!field.endsWith('?') || given[name] !== undefined) written[name] = given[name];
  }
  return written as unknown as Extract<PageCommand, { readonly kind: K }>;
}

/** A sheet's layout as the page saves it: its saved tiles alone, each other one's place closed by its neighbours as a
 *  removal closes it, a stack reopening on its first tab (the design's §5, 7). */
export function savedLayout(layout: Bands, saved: ReadonlySet<string>): PageLayout {
  const kept = tiles(layout).filter((id) => !saved.has(id)).reduce((rest, id) => remove(rest, id), layout);
  return { kind: 'bands', ...asSaved(kept) };
}

/** A sheet's layout as the board drew it, told whole, as the page saves it (savedLayout). */
export function layoutCommand(sheet: string, layout: Bands, saved: ReadonlySet<string>): PageCommand {
  return { kind: 'layout', sheet, layout: savedLayout(layout, saved) };
}

/** What the hand did on the board (band-board.ts LayoutMove): a lone tile stacked on another, a preset picked. */
export type HandMove =
  | { readonly kind: 'stack'; readonly tiles: readonly [string, string] }
  | { readonly kind: 'arrange'; readonly preset: Preset; readonly first?: string };

/**
 * A sheet's layout changed by hand, told as what the hand did (P14): a lone tile stacked on another, when both are tiles
 * the page saves (an editing grid's place is no command's); a preset picked, the page's tiles arranged as the preset
 * arranges them -- in the board's order: `first`, then the rest in reading order (`before`, the layout the hand changed);
 * none given, the page's own -- an editing grid open among them the board's, drawn after (P7; the S3 audit's 4). Any
 * other change is the sheet's layout whole (layoutCommand of `layout`, the board's now).
 */
export function movedCommand(sheet: string, before: Bands, move: HandMove | undefined, layout: Bands,
  saved: ReadonlySet<string>): PageCommand {
  if (move?.kind === 'stack' && move.tiles.every((t) => saved.has(t))) return { kind: 'stack', tiles: [...move.tiles] };
  if (move?.kind === 'arrange') {
    const was = tiles(before).filter((t) => saved.has(t));
    const first = move.first !== undefined && saved.has(move.first) ? move.first : undefined;
    return { kind: 'arrange', sheet, preset: move.preset, order: first !== undefined ? [first, ...was.filter((t) => t !== first)] : null };
  }
  return layoutCommand(sheet, layout, saved);
}

/** A sheet's name given as typed (`name`; undefined, none), by the names' one rule (page-names.ts givenName: trimmed, a
 *  blank none of its own -- named after its first grid): its renameSheet, or null when it changes nothing -- the name
 *  it has (`was`), or, with none of its own, the label it shows (`shown`), which renameSheet takes as none. */
export function renameSheetCommand(sheet: string, name: string | undefined, was: string | undefined,
  shown: string | undefined): PageCommand | null {
  const given = givenName('sheet', name ?? '');
  if ('refused' in given) throw new CubeRefusal(given.refused);
  if (given.name === was || (was === undefined && given.name === shown)) return null;
  return { kind: 'renameSheet', sheet, name: given.name ?? null };
}

/** Sheet `sheet`, at `at` of `count`, moved to `to` in the order (counted with it taken out), held to the order's ends;
 *  null where it is already. */
export function moveSheetCommand(sheet: string, at: number, to: number, count: number): PageCommand | null {
  const place = Math.max(0, Math.min(count - 1, to));
  return at === place ? null : { kind: 'moveSheet', sheet, to: place };
}

/** MOVE TO SHEET: tile `tile` onto sheet `sheet`, at its bottom -- a new one (`isNew`), made first, after the last, in
 *  the same step -- `columns` fitting side by side there (the board's measure; null, as many as a sheet holds). */
export function moveToSheetCommands(tile: string, sheet: string, isNew: boolean, columns: number | null, flow = false): PageCommand[] {
  // a flow's box onto a new sheet: a new flow (a box goes from flow to flow: page-commands.ts moveTile)
  const added = { kind: flow ? 'addFlow' : 'addSheet', id: sheet, name: null } as const;
  return [...(isNew ? [added] : []), { kind: 'moveTile', tile, sheet, columns }];
}

/**
 * SHOW GRID's steps after its grid is added (D7; the screen's Show Grid and Python's chart.grid(), headless pageGesture):
 * the chart on the new grid's cube (`followGrid`: its selection off its old grid's filter and its page filter off, as a
 * click on nothing takes them), following it as it was drawn (`setChart`).
 */
export function showGridCommands(chart: string, grid: string, spec: ChartSpec): PageCommand[] {
  return [{ kind: 'followGrid', chart, grid }, { kind: 'setChart', chart, spec }];
}

/**
 * AN OUTPUT MADE ON A FLOW (the palette's Output, docs/DATACUBE_FLOW_MODE_2026_10_24.md M6): box `grid` set to write a CSV
 * named after what it reads (`reads`, its input's name): that name without its own ending, the characters a file's name
 * cannot hold made _, then " out.csv". Python names the file it gives grid.output itself.
 */
export function outputCommand(grid: string, reads: string): PageCommand {
  const base = reads.replace(/\.[A-Za-z0-9]+$/, '').replace(/[^A-Za-z0-9 _.-]+/g, '_').trim();
  return pageCommand('setOutput', { grid, output: { format: 'csv', name: `${base === '' ? 'output' : base} out.csv` } });
}

/**
 * TIDY UP (docs/DATACUBE_FLOW_MODE_2026_10_24.md M7; the canvas's button and Python's flow.tidy(), headless pageGesture):
 * flow sheet `sheet`'s boxes placed by the flow's own rule (layout/flow.ts arranged) -- each a column after the boxes it
 * reads -- told as its layout whole, or nothing when they are there already. Refused in words: no flow sheet of the page.
 */
export function tidyCommands(page: PageDocument, sheet: string): PageCommand[] {
  const s = page.sheets.find((x) => x.id === sheet);
  if (s === undefined) throw new CubeRefusal(`'${sheet}' is no sheet of the page`);
  const layout = s.layout;
  if (layout.kind !== 'flow') throw new CubeRefusal(`'${sheet}' is a board: its tiles are laid out in bands, not tidied`);
  const reads = (tile: string): string[] => {
    const view = page.views.find((v) => v.id === tile);
    const cube = page.cubes.find((c) => c.id === view?.cube)?.cube;
    return cube === undefined ? [] : liveGrids(cube.source);
  };
  // its boxes tidied, its comments where they are: a comment is put beside what it says by hand
  const isBox = (tile: string): boolean => page.views.find((v) => v.id === tile)?.kind === 'grid';
  const placed = new Map(arranged({ boxes: layout.boxes.filter((b) => isBox(b.tile)) }, reads).boxes.map((b) => [b.tile, b]));
  const boxes = layout.boxes.map((b) => placed.get(b.tile) ?? b);
  return JSON.stringify(boxes) === JSON.stringify(layout.boxes) ? [] : [{ kind: 'layout', sheet, layout: { kind: 'flow', boxes } }];
}

/**
 * A UNION'S INPUT TAKEN OFF (docs/DATACUBE_FLOW_MODE_2026_10_24.md M2: a line into it deleted, its settings' ×): grid
 * `grid`'s input on `side` gone, those after it each moved up one -- told as a setInput of its left and of its right when
 * either changes, then its more whole (setJoin). Refused in words: a join of two, whose line taken off freezes it.
 */
export function takeOffCommands(grid: string, source: CubeSource, side: InputSide): PageCommand[] {
  inputOn(source, side);
  if (source._type !== 'join' || source.more === undefined) {
    throw new CubeRefusal(`'${grid}' reads two inputs: one taken off is frozen (a copy of its rows), not removed`);
  }
  const at = inputSides(source).indexOf(side);
  const [left, right, ...more] = joinInputs(source).filter((_, i) => i !== at);
  return [
    ...(sameValue(left, source.left) ? [] : [pageCommand('setInput', { grid, side: 'left', input: left! })]),
    ...(sameValue(right, source.right) ? [] : [pageCommand('setInput', { grid, side: 'right', input: right! })]),
    pageCommand('setJoin', { grid, how: source.how, more }),
  ];
}

/** A BOX DUPLICATED (Ctrl+D, its menu: docs/DATACUBE_FLOW_MODE_2026_10_24.md M7; Python's grid.copy of a flow's box):
 *  `document`, its cube as the page has it -- reading what it reads -- added on `sheet` after the box it reads first,
 *  titled by its number unless `title` is given: the page command every grid is added by. One rule for both (the
 *  review of 2026-10-26: Python's copy went after the box copied, as a board's does). */
export function duplicateCommands(document: CubeDocument, tile: string, sheet: string, title: string | null = null,
  copied?: { readonly id: string; readonly box: FlowBox }): PageCommand[] {
  const near = liveGrids(document.source)[0] ?? tile;
  return [
    { kind: 'addGrid', ...(copied !== undefined ? { id: copied.id } : {}), document, sheet, near, title, columns: null },
    ...(copied !== undefined ? settingsCommands(copied.id, copied.box) : []),
  ];
}

/**
 * IMPORT PURE'S BOXES DONE (FL12: pure-import.ts): box `from` taken apart into its first step and boxes after it, `made`,
 * the last of them its rows as `from` handed them on -- so what read `from` reads the last from then on, each box of a
 * flow reading it live (not one of `made`), and `from`'s settings (an Output, its cache, its blocking, its message) go
 * with its rows to the last (the roadmap audit of 2026-10-26: the boxes after it read its first step's rows, its Output
 * wrote them). A Test's stopping stays: `from` is a Pure box, no Test.
 */
export function importedCommands(page: PageDocument, from: string, made: readonly string[]): PageCommand[] {
  const last = made.at(-1);
  if (last === undefined) return [];
  const commands: PageCommand[] = [];
  for (const v of page.views) {
    if (v.kind !== 'grid' || v.id === from || made.includes(v.id)) continue;
    const source = page.cubes.find((c) => c.id === v.cube)?.cube.source;
    if (source === undefined || (source._type !== 'rows' && source._type !== 'join')) continue;
    for (const side of inputSides(source)) {
      const input = inputOn(source, side);
      if (input !== undefined && 'grid' in input && input.grid === from && input.port === undefined) {
        commands.push({ kind: 'setInput', grid: v.id, side, input: { grid: last } });
      }
    }
  }
  const sheet = page.sheets.find((s) => s.layout.kind === 'flow' && s.layout.boxes.some((b) => b.tile === from));
  const box = sheet?.layout.kind === 'flow' ? sheet.layout.boxes.find((b) => b.tile === from) : undefined;
  if (box?.output !== undefined) commands.push({ kind: 'setOutput', grid: from, output: null }, { kind: 'setOutput', grid: last, output: box.output });
  if (box?.cache === true) commands.push({ kind: 'setCache', grid: from, cache: false }, { kind: 'setCache', grid: last, cache: true });
  if (box?.blocks === true) commands.push({ kind: 'setBlocks', grid: from, blocks: false }, { kind: 'setBlocks', grid: last, blocks: true });
  if (box?.message !== undefined) commands.push({ kind: 'setMessage', grid: from, message: null }, { kind: 'setMessage', grid: last, message: box.message });
  return commands;
}

/** A copied box's modes, set on its copy `grid` (a paste, a duplicate): its stopping, its cache, its blocking and its
 *  message -- not its Output, which two boxes would write one file by (the roadmap audit of 2026-10-26: each was
 *  dropped, a Message's copy a plain box). */
export function settingsCommands(grid: string, box: FlowBox): PageCommand[] {
  return [
    ...(box.stops === true ? [{ kind: 'setStops' as const, grid, stops: true }] : []),
    ...(box.cache === true ? [{ kind: 'setCache' as const, grid, cache: true }] : []),
    ...(box.blocks === true ? [{ kind: 'setBlocks' as const, grid, blocks: true }] : []),
    ...(box.message !== undefined ? [{ kind: 'setMessage' as const, grid, message: box.message }] : []),
  ];
}

/** The page a gesture is asked over (`a.page`), read: one its reader refuses refused in its words; any other error is a
 *  bug, thrown as it is (the review of 2026-10-26: every error was made a refusal). */
function pageOf(a: Readonly<Record<string, unknown>>): PageDocument {
  try {
    return readPage(a['page'], { empty: true });
  } catch (e) {
    if (e instanceof PageDocumentError) throw new CubeRefusal(e.message);
    throw e;
  }
}

/** The page's gestures Python asks by name (headless pageGesture), with their arguments read from JSON: those whose
 *  meaning a Python call shares with the screen's; the board's own (a hand's move, a layout drawn) are the board's. */
export const PAGE_GESTURES = {
  'chart.showGrid': (a: Readonly<Record<string, unknown>>): PageCommand[] => {
    const text = (k: string): string => {
      if (typeof a[k] !== 'string') throw new CubeRefusal(`'chart.showGrid' needs ${k}, the ${k}'s id`);
      return a[k] as string;
    };
    if (typeof a['spec'] !== 'object' || a['spec'] === null) throw new CubeRefusal("'chart.showGrid' needs spec, the chart's spec");
    return showGridCommands(text('chart'), text('grid'), a['spec'] as ChartSpec);
  },
  // a flow's boxes tidied, over the page as it is (its boxes' links)
  'flow.tidy': (a: Readonly<Record<string, unknown>>): PageCommand[] => {
    if (typeof a['sheet'] !== 'string') throw new CubeRefusal("'flow.tidy' needs sheet, the flow sheet's id");
    const page = pageOf(a);
    return tidyCommands(page, a['sheet']);
  },
  // a box placed -- a container, its boxes with it (flow.ts containerMoves), its frame set or kept -- over the page as it
  // is: one step, as the canvas's drag tells it (the review of 2026-10-26's B6)
  'flow.place': (a: Readonly<Record<string, unknown>>): PageCommand[] => {
    if (typeof a['tile'] !== 'string') throw new CubeRefusal("'flow.place' needs tile, the box's id");
    // a place as JSON carries it exactly (pure-protocol's reader: an ExactNumber), or as a number
    const at = (v: unknown): number => (v instanceof ExactNumber ? Number(v.text) : typeof v === 'number' ? v : NaN);
    const x = at(a['x']);
    const y = at(a['y']);
    if (!Number.isFinite(x) || !Number.isFinite(y)) throw new CubeRefusal("'flow.place' needs x and y, its place");
    const page = pageOf(a);
    const frame = a['frame'] as Frame | null | undefined;
    const sheet = page.sheets.find((s) => s.layout.kind === 'flow' && s.layout.boxes.some((b) => b.tile === a['tile']));
    // a tile of a board: the command alone, refused in its own words
    if (sheet === undefined || sheet.layout.kind !== 'flow') {
      return [{ kind: 'placeBox', tile: a['tile'], x, y, ...(frame !== undefined ? { frame } : {}) }];
    }
    let moves: ReturnType<typeof containerMoves>;
    try {
      moves = containerMoves(sheet.layout, a['tile'], x, y);
    } catch (e) {
      throw e instanceof FlowRefusal ? new CubeRefusal(e.message) : e;
    }
    return moves.map((m, i): PageCommand => ({ kind: 'placeBox', tile: m.tile, x: m.x, y: m.y, ...(i === 0 && frame !== undefined ? { frame } : {}) }));
  },
  // a flow's box duplicated, over the page as it is (duplicateCommands)
  'flow.duplicate': (a: Readonly<Record<string, unknown>>): PageCommand[] => {
    if (typeof a['tile'] !== 'string') throw new CubeRefusal("'flow.duplicate' needs tile, the box's id");
    if (a['title'] !== undefined && a['title'] !== null && typeof a['title'] !== 'string') throw new CubeRefusal("a duplicate's title is text, or null");
    const page = pageOf(a);
    const sheet = page.sheets.find((s) => s.layout.kind === 'flow' && s.layout.boxes.some((b) => b.tile === a['tile']));
    const view = page.views.find((v) => v.id === a['tile']);
    const cube = view?.kind === 'grid' ? page.cubes.find((c) => c.id === view.cube) : undefined;
    if (sheet === undefined || cube === undefined || sheet.layout.kind !== 'flow') throw new CubeRefusal(`'${a['tile']}' is no grid's box of a flow`);
    const box = sheet.layout.boxes.find((b) => b.tile === a['tile'])!;
    return duplicateCommands(cube.cube, a['tile'], sheet.id, (a['title'] as string | null | undefined) ?? null,
      { id: nextPageId(takenIds({ page, held: idsOf(page) }), 'grid'), box });
  },
  // a Union's input taken off, over the page as it is (its grid's source)
  'flow.takeOff': (a: Readonly<Record<string, unknown>>): PageCommand[] => {
    if (typeof a['grid'] !== 'string') throw new CubeRefusal("'flow.takeOff' needs grid, the Union's id");
    if (typeof a['side'] !== 'string' || (moreIndex(a['side']) === undefined && a['side'] !== 'left' && a['side'] !== 'right')) {
      throw new CubeRefusal(`'flow.takeOff' needs side, a Union's left, right or more1, more2, ..., not ${JSON.stringify(a['side'])}`);
    }
    const page = pageOf(a);
    const view = page.views.find((v) => v.id === a['grid']);
    const cube = view?.kind === 'grid' ? page.cubes.find((c) => c.id === view.cube) : undefined;
    if (cube === undefined) throw new CubeRefusal(`'${a['grid']}' is no grid of the page`);
    return takeOffCommands(a['grid'], cube.cube.source, a['side'] as InputSide);
  },
  // Import Pure's boxes done (importedCommands): what read the box taken apart reads its last box
  'flow.imported': (a: Readonly<Record<string, unknown>>): PageCommand[] => {
    if (typeof a['grid'] !== 'string') throw new CubeRefusal("'flow.imported' needs grid, the box taken apart");
    const made = a['made'];
    if (!Array.isArray(made) || !made.every((m): m is string => typeof m === 'string')) throw new CubeRefusal("'flow.imported' needs made, the boxes made after it");
    return importedCommands(pageOf(a), a['grid'], made);
  },
  // a Union's pick -- a Dynamic Input -- over the page as it is: a parameter's name, `made` (a new parameter of its
  // inputs' names), or null (every input's rows: dynamic-input.ts pickCommands)
  'flow.pick': (a: Readonly<Record<string, unknown>>): PageCommand[] => {
    if (typeof a['grid'] !== 'string') throw new CubeRefusal("'flow.pick' needs grid, the Union's id");
    const by = a['parameter'];
    if (by !== null && by !== true && typeof by !== 'string') {
      throw new CubeRefusal(`'flow.pick' needs parameter: a parameter's name, true for a new one, or null, not ${JSON.stringify(by ?? null)}`);
    }
    const pick: InputPick = by === null ? null : by === true ? { made: true } : { parameter: by };
    return pickCommands(pageOf(a), a['grid'], pick);
  },
} as const;

export function isPageGesture(name: unknown): name is keyof typeof PAGE_GESTURES {
  return typeof name === 'string' && Object.hasOwn(PAGE_GESTURES, name);
}
