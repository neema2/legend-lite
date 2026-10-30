// A FLOW SHEET'S BOARD (docs/DATACUBE_FLOW_MODE_2026_10_24.md FL4, §3.4): the boxes of a flow on a canvas, and below it
// the results pane, which holds the grid of the box shown -- one at a time, as Alteryx's Results window shows the tool
// selected. Every other box is a document, not a running grid (M1's design, 1 and 3): drawn from its cube document as the
// page has it, with no grid made for it until it is shown.
//
// This is the board's state, kept as a band board keeps its (layout/band-board.ts): which box is where (layout/flow.ts),
// what each is called, which is shown and what its grid's element is. The drawing -- the canvas, its lines, the palette,
// dragging a box -- is fetched the first time a flow sheet is made (layout/flow-canvas.ts), so a page with no flow never
// downloads it; until it comes, nothing is drawn, and once it has, it draws the board as it is then.
//
// What a box DOES is its document's (flow-boxes.ts boxSays); what a gesture DOES is its page command's: the board only
// says what the hand did (`FlowBoardOptions`), and is put at the page the command makes (page/cube-page.ts sync).

import type { BandTile } from './band-board.ts';
import { EMPTY_FLOW, placeAfter, removeBox, type Flow, type FlowMessage, type Frame, type MessageLevel, type Output } from './flow.ts';
import { isLive, joinInputs, type CubeDocument, type CubeInput, type InputPort, type InputSide } from '../cube-document.ts';
import { toJson } from '../../../pure-protocol/src/index.ts';
import type { FlowTool, StepEditor } from '../flow-boxes.ts';
import type { InputPick } from '../dynamic-input.ts';
import type { PageParameter } from '../page-parameters.ts';
import { boxOmitted, boxOrder, boxRenamed } from '../box-columns.ts';
import type { Profile, TopValue } from '../profile.ts';
import { lazily } from '../lazily.ts';
import type { FilterValue } from '../snapshot.ts';

/** A box's chart as its results pane holds it: its options shown or hidden, and taken down. */
export interface FlowChart {
  toggleForm(): void;
  dispose(): void;
}

export interface FlowBoardOptions {
  /** A box let go at (`x`, `y`) by hand -- its drag, or its arrow keys: the caller tells its place (placeBox). */
  readonly onMove?: (id: string, x: number, y: number) => void;
  /** Boxes picked together (Shift+click, a band dragged round them) let go each at its place, by one drag or one arrow
   *  key: the caller tells their places as one step. */
  readonly onMoveMany?: (moves: readonly { readonly id: string; readonly x: number; readonly y: number }[], label?: string) => void;
  /** A container's frame changed by hand -- sized by its corner, folded or unfolded by its head -- at (`x`, `y`): the
   *  caller tells it (placeBox with its frame). */
  readonly onFrame?: (id: string, x: number, y: number, frame: Frame) => void;
  /** A container made at (`x`, `y`), `frame` its size (the palette's Container let go there, or Ctrl+G round the boxes
   *  picked): the caller adds a comment framed so. */
  readonly onContainer?: (at: FlowPoint, frame: Frame) => void;
  /** A comment's words to be edited (a container's head double-clicked): the caller opens their editor. */
  readonly onEditText?: (id: string) => void;
  /** A box set to write its rows to a file (an Output), or no longer (null): the caller tells it (setOutput). */
  readonly onOutput?: (id: string, output: Output | null) => void;
  /** A Test box set to stop a run when its checks fail (its settings' checkbox), or to warn: the caller tells it
   *  (setStops). */
  readonly onStops?: (id: string, stops: boolean) => void;
  /** A box set to cache its rows (FL10: copied into this page's database when it is shown or run, the boxes after it
   *  reading the copy), or no longer. */
  readonly onCache?: (id: string, cache: boolean) => void;
  /** The copy a cached box's rows are in now (its grid's, or the page's while it is not shown), if one is -- or 'here',
   *  its rows this page's database's own from the start (a file read into the tab): nothing to copy. */
  readonly cachedCopy?: (id: string) => { readonly label: string; readonly rowCount: number } | 'here' | undefined;
  /** A Cross Tab box's Load values (`pin`: its values as its rows hold them now pinned, its columns fixed -- FL2) or
   *  Unfix: the refusal in words, or null. */
  readonly onPinValues?: (id: string, pin: boolean) => Promise<string | null>;
  /** A Cross Tab's values its pin lacks (FL3), each given a column of its own after its columns (FL8's fix, "Add to the
   *  cast"); why not when it could not. */
  readonly onAddPinned?: (id: string, values: readonly (FilterValue | null)[]) => Promise<string | null>;
  /** An Output's Write now: the caller writes its rows to its file, and says why not when it could not. */
  readonly onWrite?: (id: string) => Promise<string | null>;
  /** A box clicked, or Enter on it: the caller shows its rows (its grid made, `add`) -- and its settings, `editor` (the
   *  window of its grid's that sets what it does: flow-boxes.ts TOOL_EDITORS), docked in the board's settings pane. */
  readonly onSelect?: (id: string, editor: StepEditor | undefined) => void;
  /** A box's × or Delete on it: the caller takes it off the page. */
  readonly onRemove?: (id: string) => void;
  /** Delete on boxes picked together: the caller takes them all off the page, as one step. */
  readonly onRemoveMany?: (ids: readonly string[]) => void;
  /** A box renamed in place. */
  readonly onRename?: (id: string, title: string) => void;
  /** A tool of the palette picked -- clicked, after the box shown, or dropped on a box, after it; dropped on the canvas,
   *  after none; dropped on a line, after the box it comes out of and `into` the box it goes into, on that side, which
   *  reads the new box from then on: the caller adds a box of it. */
  readonly onTool?: (tool: FlowTool, after: string | undefined, editor: StepEditor | undefined, into?: LineEnd, port?: InputPort,
    at?: FlowPoint) => void;
  /** A line drawn by hand from box `from`'s output into box `tile`'s input on `side` (docs/DATACUBE_FLOW_MODE_2026_10_24.md
   *  M2): the caller links it (setInput, live). */
  readonly onConnect?: (tile: string, side: InputSide, from: string, port?: InputPort) => void;
  /** A line into box `tile` on `side` taken off, or its settings' Freeze: the caller freezes it -- it reads a copy of what
   *  it read (setInput, a copy: FL6). */
  readonly onFreeze?: (tile: string, side: InputSide) => void;
  /** A Union's input taken off (one of more than two: its line deleted, its settings' ×, its menu's Take off): the
   *  caller takes it off, those after it moved up (page-gestures.ts takeOffCommands). */
  readonly onTakeOff?: (tile: string, side: InputSide) => void;
  /** Ctrl+Z (Shift for redo) on the canvas, or its Undo and Redo: the page's own undo. */
  readonly onUndo?: (redo: boolean) => void;
  /** What the page's Undo and Redo would undo and redo now, by name (undefined: nothing): the canvas's Undo and Redo. */
  readonly history?: () => { readonly undo?: string | undefined; readonly redo?: string | undefined };
  /** A file dropped onto the canvas (from the desktop): the caller adds an Input box over it. */
  readonly onFile?: (file: File) => void;
  /** Tidy Up (the canvas's button): the caller places every box by the flow's own rule (page-gestures.ts tidyCommands). */
  readonly onTidy?: () => void;
  /** Run (the canvas's button): the caller shows each box in turn, so each is counted (`counted`). */
  /** Run (`dry`: a Dry Run -- every box counted and judged, no Output written). */
  readonly onRun?: (dry?: boolean) => void;
  /** Duplicate (Ctrl+D on a box, or its menu): the caller adds a copy of it, reading what it reads. */
  readonly onDuplicate?: (id: string) => void;
  /** Ctrl+C on a box or a note, or on the boxes picked together (Ctrl+X: `cut`, and taken off the page): the caller
   *  keeps a copy of them to paste. */
  readonly onCopy?: (ids: readonly string[], cut: boolean) => void;
  /** The shown box's Profile (its results pane's): its columns summarised, asked of the database (profile.ts). */
  readonly profileOf?: (id: string) => Promise<Profile>;
  /** A column of the shown box's Profile opened: its most frequent values, asked of the database (`column`: its name
   *  among the box's rows). */
  readonly topValuesOf?: (id: string, column: string) => Promise<readonly TopValue[]>;
  /** The first row box `id`'s grid shows now, a column's value at a time (each undefined when it has no row) --
   *  undefined while it has no grid (not shown) or no view yet: a Test box's counts, read as its rows land (checks.ts). */
  readonly firstRowOf?: (id: string) => ((column: string) => unknown) | undefined;
  /** The shown box's Code (its results pane's): the query its grid ran last, as the compiler prints it, and its SQL. */
  readonly codeOf?: (id: string) => Promise<{ readonly pure: string; readonly sql: string }>;
  /** Text onto the clipboard, by box `id`'s grid's host (its Code copied). */
  readonly copyText?: (id: string, text: string) => Promise<void>;
  /** The shown box's Chart (its results pane's): its rows drawn by its grid's chart panel, mounted into `host` (the
   *  caller's, which holds the panel: the canvas fetches no chart code) -- its options shown and hidden, and taken down. */
  readonly chartOf?: (id: string, host: HTMLElement) => FlowChart;
  /** Ctrl+V on the canvas: the caller adds what it copied last, at `at` (where the pointer is), else after the box it
   *  reads. */
  readonly onPaste?: (at: FlowPoint | undefined) => void;
  /** Each box's cube document as the page has it: what it does, and the boxes it reads (the lines into it). */
  readonly documentOf?: (id: string) => CubeDocument | undefined;
  /** A box set to block a run (Block Until Done, M8), or no longer: the caller tells it (setBlocks). */
  readonly onBlocks?: (id: string, blocks: boolean) => void;
  /** A box set to say a message in a run's log (M8), or nothing (null): the refusal in words, or null (setMessage). */
  readonly onMessage?: (id: string, message: FlowMessage | null) => string | null;
  /** The page's parameters (M8, page-parameters.ts), by name and type: what a Dynamic Input is picked by. */
  readonly parameters?: () => readonly PageParameter[];
  /** A Union set to keep the rows of the one input a parameter names, a new one of its inputs' names, or every input's
   *  (null: dynamic-input.ts InputPick) -- `called`, each box's name on the canvas, what a box no one named is named
   *  where two would share one: the refusal in words, or null. */
  readonly onPick?: (id: string, pick: InputPick, called: (grid: string) => string) => string | null;
  /** THE PAGE'S MACROS (M8, macros.ts): each by its name, in words (its steps, the parameters it reads). */
  readonly macros?: () => readonly { readonly name: string; readonly words: string }[];
  /** Macro `name` used after box `after`: the caller adds a Pure box of its function, named after it. */
  readonly onMacro?: (name: string, after: string) => void;
  /** Save as Macro on box `id`: its own steps and those of the boxes after box `from` up to it, kept as macro `name` --
   *  the refusal in words, or null. */
  readonly onSaveMacro?: (id: string, from: string, name: string) => Promise<string | null>;
}

/** A RUN'S LOG (Run: docs/DATACUBE_FLOW_MODE_2026_10_24.md M6): when it ran, how long it took, and each box -- its rows,
 *  how long its grid took to open and answer, or why it was refused -- in the order it ran them. The screen's. */
export interface RunLog {
  readonly at: Date;
  readonly ms: number;
  readonly boxes: readonly {
    readonly id: string; readonly ms: number; readonly rows?: number; readonly refused?: string;
    /** An Output's file, written this run. */
    readonly wrote?: string;
    /** An Output's file, a Dry Run's: it would be written. */
    readonly wouldWrite?: string;
    /** A Message's words (M8, layout/flow.ts messageSaid), and how it said them. */
    readonly said?: string;
    readonly level?: MessageLevel;
  }[];
  /** The boxes that stopped it (a stopping Test's checks failing: FL8's refuse; a Message's error, M8), by box: no
   *  Output written from their stage on. */
  readonly stopped?: readonly string[];
  /** Its stages, when it had more than one (Block Until Done, M8): how many. */
  readonly stages?: number;
  /** A Dry Run's (M6): every box counted and judged, nothing written. */
  readonly dry?: true;
}

/** How many runs a flow keeps the logs of, this session (its run history). */
export const RUN_HISTORY = 20;

/** A point of a flow's canvas, in its own units: where a tool or a line was let go, the box made there placed at it. */
export interface FlowPoint {
  readonly x: number;
  readonly y: number;
}

/** Where a line goes in: the box, and the side of it (a join's left or right, any other box's one input). */
export interface LineEnd {
  readonly tile: string;
  readonly side: InputSide;
}

/** A box as the board holds it: what it is called, and -- shown -- its grid's tile. */
interface Held {
  title: string;
  tile?: BandTile;
}

/** What the drawing reads of the board, and tells it (layout/flow-canvas.ts). */
export interface FlowBoardState {
  readonly layout: Flow;
  readonly shown: string | undefined;
  /** What the results pane says while no box is shown, when it says more than how to show one (a box not opened). */
  readonly noted: string | undefined;
  readonly editing: boolean;
  readonly options: FlowBoardOptions;
  /** The settings pane's body, where the shown box's grid docks the window that sets it. */
  readonly dock: HTMLElement;
  title(id: string): string | undefined;
  /** The tool a box was added as, while its grid does nothing yet (`addedAs`). */
  addedAs(id: string): FlowTool | undefined;
  /** Why a box's grid could not be made the last time it was shown (`refuse`), if it could not. */
  refusal(id: string): string | undefined;
  /** How many rows a box gave the last time its grid ran, while its chain is as it was then (`counted`). */
  count(id: string): number | undefined;
  /** How many views box `id`'s grid has landed: its Code is asked again with each. */
  viewed(id: string): number;
  /** A Test box found failing (true) or passing, the last time its rows landed, while its chain is as it was then
   *  (`tested`, the canvas's: it reads its row); undefined, not known. */
  testFails(id: string): boolean | undefined;
  /** A Cross Tab's values its pinned columns lack (FL3), the last time the database was asked, while its chain is as it
   *  was then (`setLacking`, the page's); undefined, not known. */
  lacking(id: string): readonly (FilterValue | null)[] | undefined;
  /** A Test box's result, as the canvas read it off its row: kept for its chain as it is now. */
  tested(id: string, fails: boolean): void;
  /** A run in progress (Run): how many boxes are done, of how many. */
  readonly running: { readonly done: number; readonly of: number } | undefined;
  /** The last run's log, if one has run. */
  readonly log: RunLog | undefined;
  /** Every run's log this session, the last last (the run history). */
  readonly logs: readonly RunLog[];
  tile(id: string): BandTile | undefined;
  /** A comment of the flow (M7): its text tile, drawn as a note on the canvas. */
  noteOf(id: string): BandTile | undefined;
  /** Said to a screen reader. */
  say(text: string): void;
  /** What a box's rows are made of now -- its document and those of the boxes it reads -- to know an answer about them
   *  (its count, its profile) is still about them. */
  chain(id: string): string;
}

/** The drawing, once fetched. */
interface Canvas {
  paint(): void;
  reveal(id: string): void;
  pick(ids: Iterable<string>): void;
  dispose(): void;
}

/**
 * THE BOXES THAT STOP A RUN (docs/DATACUBE_FLOW_MODE_2026_10_24.md FL8's refuse) of `order`, each with why: a box set to
 * stop it that fails -- or that was never judged, its rows refused or its question unanswered (`unanswered`: why, where
 * the page knows): a stop that cannot be judged stops (the review of 2026-10-26's B7). None, every Output is written.
 */
export function runStoppers(board: Pick<FlowBoardState, 'layout' | 'testFails' | 'title' | 'refusal'>, order: readonly string[],
  unanswered: (id: string) => string | undefined = () => undefined): { readonly id: string; readonly why: string }[] {
  return order.filter((id) => board.layout.boxes.find((b) => b.tile === id)?.stops === true && board.testFails(id) !== false).map((id) => {
    const name = board.title(id) ?? id;
    if (board.testFails(id) === true) return { id, why: `${name} failed` };
    const why = board.refusal(id) ?? unanswered(id);
    return { id, why: `${name} could not be judged${why !== undefined ? ` (${why})` : ''}` };
  });
}

export class FlowBoard implements FlowBoardState {
  readonly #host: HTMLElement;
  /** What it says to a screen reader: its own, so it is said before the drawing has come. */
  readonly #live: HTMLElement;
  /** THE SETTINGS PANE's body (§3.4): where the shown box's grid docks the window that sets it (CubeAppOptions.dock) --
   *  made with the board, so a grid made before the drawing has come docks there all the same. */
  readonly dock: HTMLElement;
  readonly options: FlowBoardOptions;
  readonly #boxes = new Map<string, Held>();
  /** Each comment of the flow (docs/DATACUBE_FLOW_MODE_2026_10_24.md M7): a text tile, its words' element its owner's,
   *  drawn as a note where the flow places it. */
  readonly #notes = new Map<string, BandTile>();
  /** Each box added from the palette, as the tool it was added as: drawn so while its grid does nothing yet (a Filter
   *  with no condition is a Filter to set, not a Browse) -- the screen's, never the page's. */
  readonly #addedAs = new Map<string, FlowTool>();
  /** Why each box shown last could not be: its source not opened, refused in words (drawn red until it is shown). */
  readonly #refused = new Map<string, string>();
  /** Each box's rows the last time its grid ran (docs/DATACUBE_FLOW_MODE_2026_10_24.md M5, FL7: counted after a run, not
   *  kept live), and its chain then: a count of a chain that has changed since is no count of it. The screen's. */
  readonly #counts = new Map<string, { readonly n: number; readonly chain: string }>();
  /** How many views each box's grid has landed: what its Code is of changes with each. */
  readonly #views = new Map<string, number>();
  /** Each Test box's result, for the chain it was read for. */
  readonly #tests = new Map<string, { readonly fails: boolean; readonly chain: string }>();
  readonly #lacking = new Map<string, { readonly values: readonly (FilterValue | null)[]; readonly chain: string }>();
  #running: { readonly done: number; readonly of: number } | undefined;
  #logs: readonly RunLog[] = [];
  #layout: Flow = EMPTY_FLOW;
  #shown: string | undefined;
  #noted: string | undefined;
  #editing = true;
  #canvas: Canvas | undefined;
  #disposed = false;

  constructor(host: HTMLElement, options: FlowBoardOptions = {}) {
    this.#host = host;
    this.options = options;
    host.classList.add('dc-flow');
    this.#live = host.ownerDocument.createElement('div');
    this.#live.className = 'dc-board-live';
    this.#live.setAttribute('aria-live', 'polite');
    this.#live.setAttribute('role', 'status');
    this.dock = host.ownerDocument.createElement('div');
    this.dock.className = 'dc-flow-dock';
    host.append(this.#live);
    void lazily(() => import('./flow-canvas.ts')).then(({ FlowCanvas }) => {
      if (this.#disposed) return;
      this.#canvas = new FlowCanvas(host, this);
      this.#canvas.paint();
    });
  }

  /** Once the drawing has come and drawn the board (a test's, and a harness's, wait). */
  async drawn(): Promise<void> {
    await lazily(() => import('./flow-canvas.ts'));
  }

  /** Where each box is, as the page has it -- with any box a gesture made that its command has not placed yet. */
  get layout(): Flow {
    return this.#layout;
  }

  setLayout(flow: Flow): void {
    this.#layout = flow;
    this.#paint();
  }

  /** How many tiles are on it: its boxes and its comments. */
  get size(): number {
    return this.#boxes.size + this.#notes.size;
  }

  /** A flow is no board of bands: a tile added has no columns to take. */
  get columns(): undefined {
    return undefined;
  }

  /** The box whose grid is shown (in the results pane), if one is. */
  get shown(): string | undefined {
    return this.#shown;
  }

  get editing(): boolean {
    return this.#editing;
  }

  get noted(): string | undefined {
    return this.#noted;
  }

  /** What the results pane says until a box is shown (a box whose source was not opened, and why), said as well. */
  note(text: string): void {
    this.#noted = text;
    this.say(text);
    this.#paint();
  }

  title(id: string): string | undefined {
    return this.#boxes.get(id)?.title ?? this.#notes.get(id)?.title;
  }

  addedAs(id: string): FlowTool | undefined {
    return this.#addedAs.get(id);
  }

  refusal(id: string): string | undefined {
    return this.#refused.get(id);
  }

  /** A box whose grid could not be made, and why -- or, `why` undefined, one that was made since. */
  refuse(id: string, why: string | undefined): void {
    if (why === undefined) this.#refused.delete(id);
    else this.#refused.set(id, why);
    this.#paint();
  }

  count(id: string): number | undefined {
    const c = this.#counts.get(id);
    return c !== undefined && c.chain === this.#chain(id) ? c.n : undefined;
  }

  testFails(id: string): boolean | undefined {
    const t = this.#tests.get(id);
    return t !== undefined && t.chain === this.#chain(id) ? t.fails : undefined;
  }

  /** A Cross Tab's values its pinned columns lack (FL3), as the database last said them for what it is now (its chain):
   *  none, every value has a column; undefined, not asked since it changed. */
  lacking(id: string): readonly (FilterValue | null)[] | undefined {
    const l = this.#lacking.get(id);
    return l !== undefined && l.chain === this.#chain(id) ? l.values : undefined;
  }

  /** A Cross Tab's lacking values, as the database said them (the page asks as its rows land): its result a Test's for a
   *  run (`tested`: a stopping one with some stops it), drawn. */
  setLacking(id: string, values: readonly (FilterValue | null)[]): void {
    this.#lacking.set(id, { values, chain: this.#chain(id) });
    this.tested(id, values.length > 0);
    this.#paint();
  }

  tested(id: string, fails: boolean): void {
    this.#tests.set(id, { fails, chain: this.#chain(id) });
  }

  /** How many views box `id`'s grid has landed (its Code asked again with each). */
  viewed(id: string): number {
    return this.#views.get(id) ?? 0;
  }

  /** A box's grid ran and gave `n` rows: its count, for its chain as it is now. */
  counted(id: string, n: number): void {
    this.#views.set(id, this.viewed(id) + 1);
    const was = this.#counts.get(id);
    const chain = this.#chain(id);
    // the same count: drawn again only where its Code may be shown
    if (was?.n === n && was.chain === chain) {
      if (this.shown === id) this.#paint();
      return;
    }
    this.#counts.set(id, { n, chain });
    this.#paint();
  }

  get running(): { readonly done: number; readonly of: number } | undefined {
    return this.#running;
  }

  setRunning(running: { readonly done: number; readonly of: number } | undefined): void {
    this.#running = running;
    this.#paint();
  }

  get log(): RunLog | undefined {
    return this.#logs.at(-1);
  }

  /** THE RUN HISTORY (F3's, this session's: M8): each run's log, the last last -- RUN_HISTORY at most. */
  get logs(): readonly RunLog[] {
    return this.#logs;
  }

  /** A run done: its log kept, the last of the history, drawn (layout/flow-canvas.ts). */
  setLog(log: RunLog): void {
    this.#logs = [...this.#logs, log].slice(-RUN_HISTORY);
    this.#paint();
  }

  /** A box's chain as its documents say it: its source and query, each live input in it the chain of the box it reads --
   *  what its count is a count of. */
  #chain(id: string, seen: ReadonlySet<string> = new Set()): string {
    const doc = this.options.documentOf?.(id);
    if (doc === undefined || seen.has(id)) return '';
    const through = new Set([...seen, id]);
    // a live input by the port it reads too (a Filter's True or False), and each box by what it hands on besides its
    // query -- the columns its configuration leaves out, renames and orders (box-columns.ts): what a box after it reads
    // (the roadmap audit of 2026-10-26: a column hidden upstream left a kept copy as it was)
    const input = (i: CubeInput): string => (isLive(i) ? `<${i.port ?? ''}:${this.#chain(i.grid, through)}>` : toJson(i));
    const s = doc.source;
    const source = s._type === 'rows' ? `rows ${input(s.input)}${s.pure !== undefined ? ` ${toJson(s.pure)}` : ''}` : s._type === 'join'
      ? `join ${joinInputs(s).map(input).join(' ')} ${toJson(s.how)}` : toJson(s);
    const c = doc.configuration;
    const handed = JSON.stringify([boxOmitted(doc.query, c), boxRenamed(doc.query, c), boxOrder(doc.query, c) ?? null]);
    return `${source}|${toJson(doc.query)}|${handed}`;
  }

  /** A box's chain as it is now (#chain), with the page's parameters' values its Pure may read: a value changed, the
   *  rows changed (the roadmap audit of 2026-10-26: a cached copy outlived its parameter). */
  chain(id: string): string {
    return `${this.#chain(id)}|${JSON.stringify((this.options.parameters?.() ?? []).map((p) => [p.name, p.type, p.value]))}`;
  }

  /** A box added from the palette as `tool`: drawn as one until its grid does something. */
  addAs(id: string, tool: FlowTool): void {
    this.#addedAs.set(id, tool);
    this.#paint();
  }

  /** The grid's tile of a box shown. */
  tile(id: string): BandTile | undefined {
    return this.#boxes.get(id)?.tile;
  }

  /** A BOX SHOWN: its grid's tile in the results pane -- the box it is, or a new one after `near`. One is shown at a
   *  time: the box shown before is closed by its owner first (its grid is the owner's to let go). */
  add(tile: BandTile, near?: string): void {
    if (this.#shown !== undefined && this.#shown !== tile.id) throw new Error(`box ${this.#shown} is shown: it is closed before ${tile.id} is`);
    this.#hold(tile.id, tile.title, near).tile = tile;
    this.#shown = tile.id;
    this.#noted = undefined;
    this.#refused.delete(tile.id);
    this.#paint();
  }

  /** A comment on the flow: its text tile, drawn as a note -- placed after `near`, or where the flow has it. */
  addNote(tile: BandTile, near?: string): void {
    this.#notes.set(tile.id, tile);
    if (!this.#layout.boxes.some((b) => b.tile === tile.id)) this.#layout = placeAfter(this.#layout, tile.id, near);
    this.#paint();
  }

  noteOf(id: string): BandTile | undefined {
    return this.#notes.get(id);
  }

  /** A box with no grid made for it: drawn from its document. */
  addBox(id: string, title: string, near?: string): void {
    this.#hold(id, title, near);
    this.#paint();
  }

  #hold(id: string, title: string, near?: string): Held {
    let held = this.#boxes.get(id);
    if (held === undefined) {
      held = { title };
      this.#boxes.set(id, held);
    }
    held.title = title;
    if (!this.#layout.boxes.some((b) => b.tile === id)) this.#layout = placeAfter(this.#layout, id, near);
    return held;
  }

  /** A box shown, closed: its grid's element taken out of the results pane, the box kept. */
  close(id: string): void {
    const held = this.#boxes.get(id);
    if (held === undefined) return;
    held.tile?.element.remove();
    delete held.tile;
    if (this.#shown === id) this.#shown = undefined;
    this.#paint();
  }

  /** A box off the canvas (its grid's element taken out, not destroyed). */
  remove(id: string): void {
    this.close(id);
    this.#notes.get(id)?.element.remove();
    this.#notes.delete(id);
    this.#boxes.delete(id);
    this.#addedAs.delete(id);
    this.#refused.delete(id);
    this.#counts.delete(id);
    this.#views.delete(id);
    this.#tests.delete(id);
    this.#lacking.delete(id);
    this.#layout = removeBox(this.#layout, id);
    this.#paint();
  }

  rename(id: string, title: string): void {
    const note = this.#notes.get(id);
    if (note !== undefined) {
      this.#notes.set(id, { ...note, title });
      return;
    }
    const held = this.#boxes.get(id);
    if (held === undefined) return;
    held.title = title;
    this.#paint();
  }

  setEditing(editing: boolean): void {
    this.#editing = editing;
    this.#paint();
  }

  /** What changed that the board draws from its documents (a box's settings, its inputs): drawn again. */
  repaint(): void {
    this.#paint();
  }

  /** Said to a screen reader. */
  say(text: string): void {
    this.#live.textContent = text;
  }

  /** A box scrolled into view. */
  reveal(id: string): void {
    this.#canvas?.reveal(id);
  }

  /** These boxes picked together (boxes just pasted), the others let go. */
  pick(ids: Iterable<string>): void {
    this.#canvas?.pick(ids);
  }

  dispose(): void {
    this.#disposed = true;
    for (const held of this.#boxes.values()) held.tile?.element.remove();
    this.#boxes.clear();
    for (const note of this.#notes.values()) note.element.remove();
    this.#notes.clear();
    this.#canvas?.dispose();
    this.#canvas = undefined;
    this.#host.replaceChildren();
    this.#host.classList.remove('dc-flow');
  }

  #paint(): void {
    this.#canvas?.paint();
  }
}
