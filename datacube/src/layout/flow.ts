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
}

/** Where an Output box writes its rows: a file of a format, by its name (no folder: where it lands is its host's -- the
 *  browser's downloads, Python's folder). */
export interface Output {
  readonly format: 'csv';
  readonly name: string;
}

/** What is wrong with an Output, if anything: a name with no folder in it, of its format's own ending. */
export function outputProblem(o: Output): string | undefined {
  if (o.format !== 'csv') return `an output is a CSV file, not ${JSON.stringify(o.format)}`;
  if (o.name.trim() === '' || /[\\/]/.test(o.name)) return `an output is a file's name with no folder in it, not ${JSON.stringify(o.name)}`;
  if (!o.name.toLowerCase().endsWith('.csv')) return `a CSV output's name ends .csv, not ${JSON.stringify(o.name)}`;
  return undefined;
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
      return { tile, x: Math.round(x), y: Math.round(y), ...(kept !== undefined ? { frame: kept } : {}), ...(b.output !== undefined ? { output: b.output } : {}),
        ...(b.stops === true ? { stops: true as const } : {}) };
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
      return output === null ? rest : { ...rest, output: { format: output.format, name: output.name.trim() } };
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
