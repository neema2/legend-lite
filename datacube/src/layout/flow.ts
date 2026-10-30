// A FLOW SHEET'S LAYOUT (docs/DATACUBE_FLOW_MODE_2026_10_24.md FL4): boxes on a canvas, each a tile of the page -- a grid
// that does one thing (FL5) -- at a place, in canvas units (CSS pixels at 100%), its top-left corner. The lines between
// boxes are no part of it: each is a box's input, which its grid's source says (live-inputs.ts).
//
// PURE, as bands.ts is: no DOM, no state. Every function takes a flow and returns a new one, so where a box goes when it
// is added, moved or taken off is the page commands' (page-commands.ts) and testable in node, and the canvas only draws
// a flow and turns the pointer into these calls.
//
// The rules, enforced by construction and checked by `flowProblems`:
//   - every box is in the flow once;
//   - a place is two whole numbers, neither below zero;
//   - a frame (a container's) is two whole numbers, no smaller than FRAME_MIN.

/** A box on the canvas: its tile, and where its top-left corner is -- and, a container's (a comment with a frame:
 *  Alteryx's Tool Container, docs/DATACUBE_FLOW_MODE_2026_10_24.md M7), its size, and whether it is folded to its head. */
export interface FlowBox {
  readonly tile: string;
  readonly x: number;
  readonly y: number;
  readonly frame?: Frame;
  /** A box that writes its rows to a file (an Output: Alteryx's Output Data, docs/DATACUBE_FLOW_MODE_2026_10_24.md M6),
   *  when it is told to and when the flow runs. A grid's only. */
  readonly output?: Output;
  /** A Test box (checks.ts) that stops a run when one of its checks fails, before the run writes any Output (FL8's
   *  refuse); absent, it warns -- the run writes on, its failure said. A grid's only. */
  readonly stops?: true;
  /** A box whose rows are cached (FL10, Alteryx's Cache): copied into its host's own database when it is shown or run --
   *  its grid's Snap -- the boxes after it, and a join of it, reading the copy for as long as the session lasts (FL11).
   *  A grid's only. */
  readonly cache?: true;
  /** A box after which nothing runs until every Output before it is written (Alteryx's Block Until Done; M8): the boxes
   *  after it shown, and their Outputs written, once each Output of the boxes not after it is (runStages). A grid's only. */
  readonly blocks?: true;
  /** What a run's log says of a box (Alteryx's Message; M8), when its rows are as it asks: an error stops the run as a
   *  failing Test does, no Output written (FL8). A grid's only. */
  readonly message?: FlowMessage;
}

/** A box's message (Alteryx's Message tool): its words -- `{rows}` its rows counted -- said in a run's log `when` its
 *  rows are as it asks (always, when it has some, when it has none), as information, a warning, or an error that stops
 *  the run. */
export interface FlowMessage {
  readonly text: string;
  readonly when: MessageWhen;
  readonly level: MessageLevel;
}
export type MessageWhen = 'always' | 'rows' | 'noRows';
export type MessageLevel = 'info' | 'warning' | 'error';
export const MESSAGE_WHEN: readonly MessageWhen[] = ['always', 'rows', 'noRows'];
export const MESSAGE_LEVELS: readonly MessageLevel[] = ['info', 'warning', 'error'];

/** What is wrong with a message, in words, if anything. */
export function messageProblem(m: FlowMessage): string | undefined {
  if (typeof m.text !== 'string' || m.text.trim() === '') return 'a message says something: its words are none';
  if (m.text.length > 500) return `a message is 500 letters at most, not ${m.text.length}`;
  if (!MESSAGE_WHEN.includes(m.when)) return `a message is said ${MESSAGE_WHEN.join(', ')}, not ${JSON.stringify(m.when)}`;
  if (!MESSAGE_LEVELS.includes(m.level)) return `a message is ${MESSAGE_LEVELS.join(', ')}, not ${JSON.stringify(m.level)}`;
  return undefined;
}

/** A message read from JSON (the page's, a command's): `{text, when, level}`, or what is wrong with it. */
export function readMessage(raw: unknown): FlowMessage | string {
  const o = raw !== null && typeof raw === 'object' && !Array.isArray(raw) ? raw as Record<string, unknown> : undefined;
  if (o === undefined) return `a message is {text, when, level}, not ${JSON.stringify(raw)}`;
  const odd = Object.keys(o).filter((k) => k !== 'text' && k !== 'when' && k !== 'level');
  if (odd.length > 0) return `a message takes no ${odd.join(', ')}`;
  const m = { text: o['text'], when: o['when'], level: o['level'] } as FlowMessage;
  return messageProblem(m) ?? m;
}

/** What a box's message says of a run whose box gave `rows` rows -- its `{rows}` that count -- or undefined: its rows
 *  are not as it asks, or were not counted. */
export function messageSaid(m: FlowMessage, rows: number | undefined): string | undefined {
  if (rows === undefined && m.when !== 'always') return undefined;
  if (m.when === 'rows' && rows === 0) return undefined;
  if (m.when === 'noRows' && rows !== 0) return undefined;
  return m.text.replaceAll('{rows}', rows === undefined ? '?' : rows.toLocaleString('en-US'));
}

/**
 * A RUN'S STAGES (Block Until Done, M8): the boxes of `flow` (its comments left out by `isBox`) in the order a run runs
 * them, stage by stage -- a box's stage the most of its inputs' (`reads`: the boxes it reads live), one more past an
 * input that blocks -- each stage in the canvas's order (left to right, then top to bottom). A run shows each stage's
 * boxes and writes its Outputs before the next's.
 */
export function runStages(flow: Flow, reads: (tile: string) => readonly string[], isBox: (tile: string) => boolean = () => true): string[][] {
  const boxes = flow.boxes.filter((b) => isBox(b.tile));
  const at = new Map(boxes.map((b) => [b.tile, b] as const));
  const stage = new Map<string, number>();
  const stageOf = (tile: string, seen: readonly string[] = []): number => {
    const known = stage.get(tile);
    if (known !== undefined) return known;
    // a box read twice on the way to itself is no flow a page holds (live-inputs.ts liveProblem): its stage its own
    const s = Math.max(0, ...reads(tile).filter((i) => at.has(i) && !seen.includes(i))
      .map((i) => stageOf(i, [...seen, tile]) + (at.get(i)!.blocks === true ? 1 : 0)));
    stage.set(tile, s);
    return s;
  };
  const order = [...boxes].sort((a, b) => a.x - b.x || a.y - b.y).map((b) => b.tile);
  const stages: string[][] = [];
  for (const t of order) (stages[stageOf(t)] ??= []).push(t);
  return stages.filter((s) => s !== undefined);
}

/**
 * Where an Output box writes its rows: a file of a format, by its name (no folder: where it lands is its host's -- the
 * browser's downloads, Python's folder) -- or a TABLE of its host's own database, by its name (M6's Output Data to a
 * table, FL13): made for them (`create`; `overwrite`, made again), its rows added (`append`; `deleteAppend`, its rows
 * deleted first) or matched by `keys` (`merge`: each row of the same key updated, each new one added) -- each a write the
 * planner plans and the database runs in one transaction (table-output.ts).
 */
export type Output =
  | { readonly format: 'csv'; readonly name: string }
  | { readonly format: 'table'; readonly name: string; readonly mode: TableMode; readonly keys?: readonly string[] };

/** A table Output. */
export type TableOutput = Extract<Output, { readonly format: 'table' }>;

/** What an Output writes, in a few words (a run's log): its file's name, or its table's. */
export function outputWords(o: Output): string {
  return o.format === 'csv' ? o.name : `table ${o.name}`;
}

/** How an Output writes its table (Alteryx's Output Data options). */
export type TableMode = 'create' | 'overwrite' | 'append' | 'deleteAppend' | 'merge';
export const TABLE_MODES: readonly TableMode[] = ['create', 'overwrite', 'append', 'deleteAppend', 'merge'];

/** Each way in words, as the Output's settings offer it. */
export const TABLE_MODE_WORDS: Readonly<Record<TableMode, string>> = {
  create: 'Create a new table', overwrite: 'Overwrite the table (made again)', append: 'Append to the table',
  deleteAppend: 'Delete its rows, then append', merge: 'Update by key, insert the new (merge)',
};

/** A table's or a key's name as a table Output takes one: letters, digits and underscores, not a digit first. */
const PLAIN_NAME = /^[A-Za-z_][A-Za-z0-9_]{0,62}$/;

/** What is wrong with an Output, if anything: a file's name with no folder in it, of its format's own ending; a table's
 *  plain name, none of legend-lite's own, its mode one of TABLE_MODES, keys a merge's alone (one at least, each once). */
export function outputProblem(o: Output): string | undefined {
  if (o.format === 'table') {
    if (!PLAIN_NAME.test(o.name)) return `a table output is named by letters, digits and underscores, not a digit first: not ${JSON.stringify(o.name)}`;
    if (/^__legend_lite/i.test(o.name)) return `'${o.name}' is a name of legend-lite's own: name the table another way`;
    if (!TABLE_MODES.includes(o.mode)) return `a table output is written by ${TABLE_MODES.join(', ')}, not ${JSON.stringify(o.mode)}`;
    const keys = o.keys ?? [];
    if (o.mode !== 'merge') return keys.length > 0 ? `only a merge is matched by keys, not a ${o.mode}` : undefined;
    if (keys.length === 0) return 'a merge matches its rows by a key: name its columns';
    const odd = keys.find((k) => !PLAIN_NAME.test(k));
    if (odd !== undefined) return `a key is a column's plain name, not ${JSON.stringify(odd)}`;
    return new Set(keys).size === keys.length ? undefined : 'a merge names each key once';
  }
  if (o.format !== 'csv') return `an output is a CSV file or a table, not ${JSON.stringify((o as { format: unknown }).format)}`;
  if (o.name.trim() === '' || /[\\/]/.test(o.name)) return `an output is a file's name with no folder in it, not ${JSON.stringify(o.name)}`;
  if (!o.name.toLowerCase().endsWith('.csv')) return `a CSV output's name ends .csv, not ${JSON.stringify(o.name)}`;
  return undefined;
}

/** An Output read field by field from JSON (the page's reader, a command's): `{format: 'csv', name}` or `{format:
 *  'table', name, mode, keys?}` -- or what is wrong, in words (its rule is outputProblem's, asked after). */
export function readOutput(raw: unknown): Output | string {
  const o = raw !== null && typeof raw === 'object' && !Array.isArray(raw) ? raw as Record<string, unknown> : undefined;
  const odd = (keys: readonly string[]): string[] => (o === undefined ? [] : Object.keys(o).filter((k) => !keys.includes(k)));
  if (o?.['format'] === 'csv' && typeof o['name'] === 'string' && odd(['format', 'name']).length === 0) return { format: 'csv', name: o['name'] };
  if (o?.['format'] === 'table' && typeof o['name'] === 'string' && typeof o['mode'] === 'string' && odd(['format', 'name', 'mode', 'keys']).length === 0) {
    const keys = o['keys'];
    if (keys !== undefined && (!Array.isArray(keys) || !keys.every((k): k is string => typeof k === 'string'))) return `a table output's keys are a list of names, not ${JSON.stringify(keys)}`;
    // no keys is no keys, as setOutput keeps it: `keys: []` read as none (the roadmap audit of 2026-10-26)
    return { format: 'table', name: o['name'], mode: o['mode'] as TableMode, ...(keys !== undefined && keys.length > 0 ? { keys } : {}) };
  }
  return `an output is {format: 'csv', name} or {format: 'table', name, mode, keys?}, not ${JSON.stringify(raw)}`;
}

/** A container's size on the canvas; folded (`collapsed`), its head alone is drawn and the boxes in it are not. The boxes
 *  in it are never written: they are the boxes whose place is inside it (`framedBy`). */
export interface Frame {
  readonly w: number;
  readonly h: number;
  readonly collapsed?: true;
}

/** The smallest a frame is: room for its head and a box. */
export const FRAME_MIN = { w: 200, h: 100 } as const;
/** A frame's head: its title, its fold, its ×. What a folded container is. */
export const FRAME_HEAD = 30;
/** A container's size as the palette's Container makes one: room for two boxes side by side. */
export const CONTAINER_FRAME: Frame = { w: 480, h: 180 };

/** A flow: its boxes, in the order they were placed. */
export interface Flow {
  readonly boxes: readonly FlowBox[];
}

/** A new flow sheet's: no box yet. */
export const EMPTY_FLOW: Flow = { boxes: [] };

/** A box's size on the canvas, and the room kept between boxes: what a box added beside another is placed by. */
export const BOX_WIDTH = 184;
export const BOX_HEIGHT = 60;
export const GAP_X = 64;
export const GAP_Y = 32;
/** Where the first box goes: the canvas's margin. */
export const MARGIN = 40;
/** A box's size, as a container's frame takes it. */
const BOX_SIZE = { w: BOX_WIDTH, h: BOX_HEIGHT } as const;

/** The tiles of a flow, in the order they were placed. */
export function flowTiles(flow: Flow): string[] {
  return flow.boxes.map((b) => b.tile);
}

/** Whether two boxes' places overlap. */
const overlaps = (a: { readonly x: number; readonly y: number }, b: { readonly x: number; readonly y: number }): boolean =>
  Math.abs(a.x - b.x) < BOX_WIDTH && Math.abs(a.y - b.y) < BOX_HEIGHT;

/**
 * A BOX ADDED: after `near` when it is a box of the flow -- the next column, on its row, else the first free row below
 * it there, as a tool dropped on a box's output goes in Alteryx -- else on a row of its own below every box. Refused when
 * it is in the flow already.
 */
export function placeAfter(flow: Flow, tile: string, near?: string): Flow {
  if (flow.boxes.some((b) => b.tile === tile)) throw new FlowRefusal(`'${tile}' is on the canvas already`);
  const after = near === undefined ? undefined : flow.boxes.find((b) => b.tile === near);
  let place: { x: number; y: number };
  if (after !== undefined) {
    place = { x: after.x + BOX_WIDTH + GAP_X, y: after.y };
    while (flow.boxes.some((b) => overlaps(b, place))) place = { ...place, y: place.y + BOX_HEIGHT + GAP_Y };
  } else {
    const bottom = Math.max(MARGIN - BOX_HEIGHT - GAP_Y, ...flow.boxes.map((b) => b.y));
    place = { x: MARGIN, y: bottom + BOX_HEIGHT + GAP_Y };
  }
  return { boxes: [...flow.boxes, { tile, ...place }] };
}

/** A box moved to (`x`, `y`), rounded to whole units; refused when it is no box of the flow, or the place is off the canvas. */
export function moveBox(flow: Flow, tile: string, x: number, y: number, frame?: Frame | null): Flow {
  if (!flow.boxes.some((b) => b.tile === tile)) throw new FlowRefusal(`'${tile}' is no box of this flow`);
  if (!Number.isFinite(x) || !Number.isFinite(y) || x < 0 || y < 0) {
    throw new FlowRefusal(`a box's place is two numbers, neither below zero, not (${x}, ${y})`);
  }
  const problem = frame === undefined || frame === null ? undefined : frameProblem(frame);
  if (problem !== undefined) throw new FlowRefusal(`'${tile}'s frame: ${problem}`);
  return {
    boxes: flow.boxes.map((b) => {
      if (b.tile !== tile) return b;
      // its frame given, else the one it has (null: none)
      const kept = frame === undefined ? b.frame : frame === null ? undefined : framed(frame);
      // its settings (an Output, its stopping, its cache, its blocking, its message) as they are: its place alone moved
      const { tile: _t, x: _x, y: _y, frame: _f, ...settings } = b;
      return { tile, x: Math.round(x), y: Math.round(y), ...(kept !== undefined ? { frame: kept } : {}), ...settings };
    }),
  };
}

/** What is wrong with a frame, if anything: its size whole numbers no smaller than FRAME_MIN. */
export function frameProblem(f: Frame): string | undefined {
  if (!Number.isFinite(f.w) || !Number.isFinite(f.h) || Math.round(f.w) < FRAME_MIN.w || Math.round(f.h) < FRAME_MIN.h) {
    return `a frame is at least ${FRAME_MIN.w} by ${FRAME_MIN.h}, not ${f.w} by ${f.h}`;
  }
  return undefined;
}

const framed = (f: Frame): Frame => ({ w: Math.round(f.w), h: Math.round(f.h), ...(f.collapsed === true ? { collapsed: true } : {}) });

/** THE BOXES IN A CONTAINER (`frame`'s box): those whose whole place is inside its frame, below its head -- never a frame
 *  itself. What moves with it, and what it hides folded. */
export function framedBy(flow: Flow, container: string, size: { readonly w: number; readonly h: number } = BOX_SIZE): string[] {
  const c = flow.boxes.find((b) => b.tile === container);
  const f = c?.frame;
  if (c === undefined || f === undefined) return [];
  return flow.boxes.filter((b) => b.tile !== container && b.frame === undefined
    && b.x >= c.x && b.y >= c.y + FRAME_HEAD && b.x + size.w <= c.x + f.w && b.y + size.h <= c.y + f.h).map((b) => b.tile);
}

/**
 * A CONTAINER MOVED (M7, Alteryx's Tool Container), its boxes with it: `tile` to (`x`, `y`), each box inside it (framedBy)
 * by as much -- the places the canvas's drag tells and Python's text.place sets alike (the review of 2026-10-26's B6).
 * Any other box is itself alone. Refused in words when a box would go past the canvas's edge.
 */
export function containerMoves(flow: Flow, tile: string, x: number, y: number): { readonly tile: string; readonly x: number; readonly y: number }[] {
  const at = flow.boxes.find((b) => b.tile === tile);
  if (at === undefined) throw new FlowRefusal(`'${tile}' is no box of this flow`);
  const dx = Math.round(x) - at.x;
  const dy = Math.round(y) - at.y;
  const inside = flow.boxes.filter((b) => framedBy(flow, tile).includes(b.tile));
  const past = inside.find((b) => b.x + dx < 0 || b.y + dy < 0);
  if (past !== undefined) throw new FlowRefusal(`'${past.tile}' in it would go past the canvas's edge, at (${past.x + dx}, ${past.y + dy})`);
  return [{ tile, x: Math.round(x), y: Math.round(y) }, ...inside.map((b) => ({ tile: b.tile, x: b.x + dx, y: b.y + dy }))];
}

/** A box set to write its rows (`output`), or no longer (null). Refused when it is no box of the flow, or the output is
 *  not one (outputProblem). */
export function setOutput(flow: Flow, tile: string, output: Output | null): Flow {
  if (!flow.boxes.some((b) => b.tile === tile)) throw new FlowRefusal(`'${tile}' is no box of this flow`);
  const problem = output === null ? undefined : outputProblem(output);
  if (problem !== undefined) throw new FlowRefusal(problem);
  return {
    boxes: flow.boxes.map((b) => {
      if (b.tile !== tile) return b;
      const { output: _was, ...rest } = b;
      if (output === null) return rest;
      const kept: Output = output.format === 'csv' ? { format: 'csv', name: output.name.trim() }
        : { format: 'table', name: output.name, mode: output.mode, ...(output.keys !== undefined && output.keys.length > 0 ? { keys: [...output.keys] } : {}) };
      return { ...rest, output: kept };
    }),
  };
}

/** A box set to stop a run when its checks fail (`stops`: a Test's refuse, FL8), or to warn. Refused when it is no box of
 *  the flow. */
export function setStops(flow: Flow, tile: string, stops: boolean): Flow {
  if (!flow.boxes.some((b) => b.tile === tile)) throw new FlowRefusal(`'${tile}' is no box of this flow`);
  return {
    boxes: flow.boxes.map((b) => {
      if (b.tile !== tile) return b;
      const { stops: _was, ...rest } = b;
      return stops ? { ...rest, stops: true } : rest;
    }),
  };
}

/** A box set to cache its rows (`cache`: FL10), or no longer. Refused when it is no box of the flow. */
export function setCache(flow: Flow, tile: string, cache: boolean): Flow {
  if (!flow.boxes.some((b) => b.tile === tile)) throw new FlowRefusal(`'${tile}' is no box of this flow`);
  return {
    boxes: flow.boxes.map((b) => {
      if (b.tile !== tile) return b;
      const { cache: _was, ...rest } = b;
      return cache ? { ...rest, cache: true } : rest;
    }),
  };
}

/** A box set to block (`blocks`: Block Until Done), or no longer. Refused when it is no box of the flow. */
export function setBlocks(flow: Flow, tile: string, blocks: boolean): Flow {
  if (!flow.boxes.some((b) => b.tile === tile)) throw new FlowRefusal(`'${tile}' is no box of this flow`);
  return {
    boxes: flow.boxes.map((b) => {
      if (b.tile !== tile) return b;
      const { blocks: _was, ...rest } = b;
      return blocks ? { ...rest, blocks: true } : rest;
    }),
  };
}

/** A box set to say `message` in a run's log, or nothing (null). Refused when it is no box of the flow, or the message
 *  is none (messageProblem). */
export function setMessage(flow: Flow, tile: string, message: FlowMessage | null): Flow {
  if (!flow.boxes.some((b) => b.tile === tile)) throw new FlowRefusal(`'${tile}' is no box of this flow`);
  const problem = message === null ? undefined : messageProblem(message);
  if (problem !== undefined) throw new FlowRefusal(problem);
  return {
    boxes: flow.boxes.map((b) => {
      if (b.tile !== tile) return b;
      const { message: _was, ...rest } = b;
      return message === null ? rest : { ...rest, message: { text: message.text, when: message.when, level: message.level } };
    }),
  };
}

/** A box taken off the canvas; a tile the flow does not have leaves it as it is. */
export function removeBox(flow: Flow, tile: string): Flow {
  return { boxes: flow.boxes.filter((b) => b.tile !== tile) };
}

/**
 * A FLOW TIDIED (Tidy Up, Alteryx's Arrange: docs/DATACUBE_FLOW_MODE_2026_10_24.md M7): each box in the column of how far
 * it is from the flow's start -- a box that reads none of the flow's first, a box one column after the furthest box it
 * reads -- and, down a column, in the order of the boxes it reads (as level with them as the column has room), else as it
 * was; every place on the grid of the flow's gaps. `reads`: the boxes a box reads live. The boxes in the order they came.
 */
export function arranged(flow: Flow, reads: (tile: string) => readonly string[]): Flow {
  const on = new Set(flow.boxes.map((b) => b.tile));
  const inputs = (tile: string): string[] => reads(tile).filter((r) => r !== tile && on.has(r));
  const depth = new Map<string, number>();
  const visiting = new Set<string>();
  const depthOf = (tile: string): number => {
    const known = depth.get(tile);
    if (known !== undefined) return known;
    // a cycle the page would refuse: its box where it starts
    if (visiting.has(tile)) return 0;
    visiting.add(tile);
    const read = inputs(tile);
    const d = read.length === 0 ? 0 : 1 + Math.max(...read.map(depthOf));
    visiting.delete(tile);
    depth.set(tile, d);
    return d;
  };
  for (const b of flow.boxes) depthOf(b.tile);
  const was = new Map(flow.boxes.map((b) => [b.tile, b] as const));
  const row = new Map<string, number>();
  const deepest = Math.max(0, ...depth.values());
  for (let d = 0; d <= deepest; d += 1) {
    // where each would like to be: level with the boxes it reads, else where it was
    const wanted = (tile: string): number => {
      const read = inputs(tile).filter((r) => row.has(r));
      return read.length === 0 ? Number.POSITIVE_INFINITY : read.reduce((sum, r) => sum + row.get(r)!, 0) / read.length;
    };
    const these = flow.boxes.map((b) => b.tile).filter((t) => depth.get(t) === d)
      .sort((a, b) => wanted(a) - wanted(b) || was.get(a)!.y - was.get(b)!.y || was.get(a)!.x - was.get(b)!.x);
    let next = 0;
    for (const tile of these) {
      const w = wanted(tile);
      const at = Number.isFinite(w) ? Math.max(next, Math.round(w)) : next;
      row.set(tile, at);
      next = at + 1;
    }
  }
  // each box as it was but for its place: an Output's file and a Test's stopping kept
  return {
    boxes: flow.boxes.map((b) => ({
      ...b, x: MARGIN + depth.get(b.tile)! * (BOX_WIDTH + GAP_X), y: MARGIN + row.get(b.tile)! * (BOX_HEIGHT + GAP_Y),
    })),
  };
}

/** What is wrong with a flow, in words (none: it is one). */
export function flowProblems(flow: Flow): string[] {
  const problems: string[] = [];
  const seen = new Set<string>();
  for (const b of flow.boxes) {
    if (seen.has(b.tile)) problems.push(`'${b.tile}' is on the canvas twice`);
    seen.add(b.tile);
    if (!Number.isInteger(b.x) || !Number.isInteger(b.y) || b.x < 0 || b.y < 0) {
      problems.push(`'${b.tile}' is at (${b.x}, ${b.y}): a box's place is two whole numbers, neither below zero`);
    }
    const out = b.output === undefined ? undefined : outputProblem(b.output);
    if (out !== undefined) problems.push(`'${b.tile}': ${out}`);
    const f = b.frame;
    if (f !== undefined && (frameProblem(f) !== undefined || !Number.isInteger(f.w) || !Number.isInteger(f.h))) {
      problems.push(`'${b.tile}'s frame is ${f.w} by ${f.h}: a frame is two whole numbers, at least ${FRAME_MIN.w} by ${FRAME_MIN.h}`);
    }
  }
  return problems;
}

/** A flow's rule broken: said in words (a page command refuses it as a CubeRefusal, page-commands.ts `laid`). */
export class FlowRefusal extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'FlowRefusal';
  }
}
