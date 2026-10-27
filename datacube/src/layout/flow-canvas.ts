// A FLOW SHEET DRAWN (docs/DATACUBE_FLOW_MODE_2026_10_24.md §3.4; layout/flow-board.ts, which fetches this the first
// time a flow sheet is made): the palette on the left, the canvas of boxes and the lines between them, and below it the
// results pane -- the grid of the box shown, a full DataCube.
//
// What it draws is the board's (FlowBoardState): where each box is, what each is called, which is shown; what each box
// does and what it reads, from its document (flow-boxes.ts boxSays, live-inputs.ts liveGrids). What the hand does is
// told, never applied here: a box let go somewhere else is its place told (onMove), a click shows it (onSelect), a tool
// picked or dropped adds a box of it (onTool) -- each the caller's page command, and the board put at what it makes.
//
// SMOOTH, as the band board is: a dragged box follows the pointer (a transform), its lines with it, and its place is
// told once, on drop. A gesture owns its pointer to the end: let go (applied), Escape, a lost capture (undone).
//
// THE KEYBOARD: Tab moves among the palette's tools and the boxes; Enter shows a box, the arrows move it (Shift: further),
// Delete takes it off, F2 renames it, Ctrl+D duplicates it; Enter on a tool adds a box of it after the box shown, and the
// palette's find field narrows its tools (Enter there adds the first). A box right-clicked offers all of it; Tidy Up, at
// the canvas's corner, places every box a column after the boxes it reads. A box's inputs are set in its
// settings too (Input, a join's Left and Right): a box of the flow, or a frozen copy.
//
// LINES (docs/DATACUBE_FLOW_MODE_2026_10_24.md M2): drawn from a box's output port onto another box, a line links it (a
// join's left or right by where it is let go: its top half, its bottom); a line clicked is selected, and Delete (or its
// ×) takes it off -- the box reading it then reads a copy of what it read (frozen); a tool dropped on a line goes between
// the two boxes.

import { BOX_HEIGHT, BOX_WIDTH, CONTAINER_FRAME, FRAME_HEAD, FRAME_MIN, MARGIN, containerMoves, framedBy, type Flow, type FlowBox, type Frame } from './flow.ts';
import type { FlowBoardState, FlowChart } from './flow-board.ts';
import type { FlowPoint, LineEnd, RunLog } from './flow-board.ts';
import { moreIndex, type InputPort, type InputSide } from '../cube-document.ts';
import {
  boxInputs, boxNamed, boxSays, FLOW_TOOLS, PALETTE, readersOf, TOOL_EDITORS, type BoxInput, type BoxSays, type FlowGroup, type FlowTool,
} from '../flow-boxes.ts';
import { rowSampleOf } from '../row-sample.ts';
import { checksOf, failingChecks, failingWords } from '../checks.ts';
import { lackingWords } from '../cross-tab.ts';
import { UI_LOCALE } from '../../../engine-client/src/locale.ts';
import type { Profile } from '../profile.ts';
import { handedOn } from '../box-columns.ts';
import type { MenuItem } from '../ui/menu.ts';
import { MenuView } from '../ui/menu-view.ts';
import { chromeIcon, closeMark } from '../ui/tile-icons.ts';

/** How far a pressed box or tool moves before it is dragged rather than clicked, in pixels. */
const DRAG_PX = 4;
/** An arrow key's step for a box, and Shift's. */
const STEP = 8;
const BIG_STEP = 32;
/** The results pane's least height, and the canvas's, in pixels. */
const LEAST_PANE = 120;
/** The settings pane's least width, and the canvas's beside it, in pixels. */
const LEAST_SETTINGS = 320;
const LEAST_CANVAS = 240;

const SVG = 'http://www.w3.org/2000/svg';
/** The minimap's room, at most: the plane drawn within it. */
const MINIMAP = { w: 168, h: 108 } as const;

/** Where the `i`th of `n` input ports is on a box's left edge, a share of its height from its top: one in the middle, a
 *  join's left above its right (as Alteryx's Join has its L and R anchors), more -- a Union's, and its + -- spread out. */
const portShare = (i: number, n: number): number => (n <= 1 ? 0.5 : n === 2 ? (i === 0 ? 0.3 : 0.7) : 0.18 + (0.64 * i) / (n - 1));

/** The zoom levels the canvas steps through (−, +, Ctrl+wheel), 1 its own size. */
const ZOOMS = [0.5, 0.67, 0.8, 0.9, 1, 1.1, 1.25, 1.5] as const;

/** Each palette group's heading. */
const GROUP_WORDS: Readonly<Record<FlowGroup, string>> = {
  in: 'In and Out', prepare: 'Prepare', join: 'Join', parse: 'Parse', transform: 'Transform', docs: 'Notes',
};

/** How a box's outputs are drawn on its right edge: one; a Filter's True and False (a Unique's U and D); a Join's L, J
 *  and R (Alteryx's anchors). */
type Outs = 'one' | 'two' | 'three';

/** Where a box's output is on its right edge, from its top: a Filter's True above its False, a Join's L above its J
 *  above its R, any other box's in the middle. */
const outY = (outs: Outs, port: InputPort | undefined): number => (outs === 'two' ? (port === 'false' ? BOX_HEIGHT * 0.7 : BOX_HEIGHT * 0.3)
  : outs === 'three' && port === 'left' ? BOX_HEIGHT * 0.22 : outs === 'three' && port === 'right' ? BOX_HEIGHT * 0.78 : BOX_HEIGHT / 2);

/** The canvas's keys and gestures, as its ? says them. */
const KEYS: readonly (readonly [string, string])[] = [
  ['Drag a tool', 'onto the canvas, a box (after it) or a line (between)'],
  ['Drag an output', 'onto a box: a line into it; let go on the canvas: a Browse'],
  ['Click a box', 'its rows below, its settings beside'],
  ['Shift+click, a band', 'pick several boxes'],
  ['Drag, arrow keys', 'move what is picked (Shift: further)'],
  ['Delete', 'remove a box, the boxes picked, or the line clicked'],
  ['Ctrl+C, X, V', 'copy, cut, paste where the pointer is'],
  ['Ctrl+D, F2', 'duplicate a box, rename it'],
  ['Ctrl+Z, Ctrl+Shift+Z', 'undo, redo'],
  ['Ctrl+Enter', 'run the flow'],
  ['Ctrl+Shift+Enter', 'a dry run: every box counted, no Output written'],
  ['Ctrl+K', 'a command: any tool, box or action of the canvas, by name'],
  ['Ctrl+A, Escape', 'pick every box, none'],
  ['Ctrl+G', 'a container round the boxes picked'],
  ['Ctrl+wheel', 'zoom about the pointer'],
  ['Map', 'the whole flow small: press or drag on it to move'],
  ['Right-click a box', 'its menu'],
];

/** An output's name: a Filter's True and False, a Unique's Unique and Duplicates, a Sample's Sampled and the rest, a
 *  Join's Joined and its Left and Right unmatched. */
function portWord(tool: FlowTool | undefined, port: InputPort | undefined): string {
  if (tool === 'join' || tool === 'fuzzyMatch') return port === 'left' ? 'Left unmatched' : port === 'right' ? 'Right unmatched' : 'Joined';
  if (tool === 'unique') return port === 'false' ? 'Duplicates' : 'Unique';
  if (tool === 'sample') return port === 'false' ? 'The rest' : 'Sampled';
  return port === 'false' ? 'False' : 'True';
}

/** A side's name, as a port and the settings say it: a join's Left and Right, a Union's inputs numbered. */
function sideWords(side: InputSide, union: boolean): string {
  const k = moreIndex(side);
  if (k !== undefined) return `Input ${k + 2}`;
  if (side === 'input') return 'Input';
  return union ? (side === 'left' ? 'Input 1' : 'Input 2') : side === 'left' ? 'Left' : 'Right';
}

/** A command of the palette (Ctrl+K): what it is called, the words it is found by besides, its kind (the order it is
 *  listed in among those found alike) and what it does -- the gesture of a button, the palette or a click. */
interface Command { readonly label: string; readonly words: string; readonly kind: 0 | 1 | 2; readonly run: () => void }

/** A box's Code as asked (its key: the box, and how many views its grid had landed), answered or refused. */
type Coded = { readonly key: string; readonly answer?: { readonly pure: string; readonly sql: string }; readonly refused?: string };

/** A line's key: the box it goes into, and its side. */
const lineKey = (end: LineEnd): string => `${end.tile}:${end.side}`;

/** Each tool's icon, as strokes in a 16-unit square. */
const ICONS: Readonly<Record<FlowTool, string>> = {
  input: 'M3 4c0-1.1 2.2-2 5-2s5 .9 5 2v8c0 1.1-2.2 2-5 2s-5-.9-5-2zM3 4c0 1.1 2.2 2 5 2s5-.9 5-2M3 8c0 1.1 2.2 2 5 2s5-.9 5-2',
  textInput: 'M2 2.5h12v11H2zM2 6h12M6.5 2.5v11M8.5 9.5h4M8.5 11.5h2.5',
  output: 'M3 4c0-1.1 2.2-2 5-2s5 .9 5 2v5M3 4c0 1.1 2.2 2 5 2s5-.9 5-2M3 4v8c0 1.1 2.2 2 5 2M10 11.5h5M13 9.5l2 2-2 2',
  cleanse: 'M10 2l4 4-6.5 6.5H3.5V8.5zM8 4l4 4M2 14.5h12',
  impute: 'M2 3h12v10H2zM2 6.5h12M2 10h12M6 3v10M10 6.5v3.5M10.5 8.2h3',
  test: 'M8 1.8l5.5 2v4c0 3.3-2.4 5.6-5.5 6.4C4.9 13.4 2.5 11.1 2.5 7.8v-4zM5.3 8l1.9 1.9 3.6-3.8',
  jsonParse: 'M5 2.5c-1.5 0-2 .7-2 2v1.8c0 .8-.4 1.2-1.2 1.2.8 0 1.2.4 1.2 1.2V10.5c0 1.3.5 2 2 2M11 2.5c1.5 0 2 .7 2 2v1.8c0 .8.4 1.2 1.2 1.2-.8 0-1.2.4-1.2 1.2V10.5c0 1.3-.5 2-2 2M6.5 7.5h3',
  dateFilter: 'M2 3.5h9v8H2zM2 6h9M4.5 2v3M8.5 2v3M10 9.5h5l-2 2.5v2.5l-1-1v-1.5z',
  // a row, and the rows made of it beneath
  generateRows: 'M2 2.5h7v3H2zM5.5 5.5v8M5.5 8.5h3M5.5 13h3M9 7.5h5v2H9zM9 12h5v2H9z',
  // bins of one width, side by side
  binning: 'M1.5 13.5h13M2.5 13V9h3v4M6.5 13V4.5h3V13M10.5 13V7.5h3V13',
  // a lambda, Pure's own sign for a function
  pure: 'M3.5 2.5h2.5l6.5 11h1.5M8.2 6.4L3.5 13.5',
  // a funnel, and a condition written out beside it
  customFilter: 'M1.5 3h9L7 7.5V11l-2 1V7.5zM12 8.5l-1.5 2 1.5 2M14 8.5l1.5 2-1.5 2',
  textToColumns: 'M2 4.5h12M2 8h4M10 8h4M2 11.5h4M10 11.5h4M8 6v8',
  regex: 'M4 2.5v6M1.5 4l5 3M6.5 4l-5 3M10 2.5l-2 11M14 2.5l-2 11M8.5 9.5H14',
  join: 'M6 3.5a4.5 4.5 0 1 0 0 9a4.5 4.5 0 1 0 0-9M10 3.5a4.5 4.5 0 1 0 0 9a4.5 4.5 0 1 0 0-9',
  union: 'M2.5 3h11v4h-11zM2.5 9h11v4h-11z',
  appendFields: 'M2 3h5v10H2zM9 3h5v10H9zM2 8h12',
  fuzzyMatch: 'M5.5 4a4 4 0 1 0 0 8a4 4 0 1 0 0-8M10.5 4a4 4 0 1 0 0 8a4 4 0 1 0 0-8M6 14.5c1-.8 2-.8 3 0s2 .8 3 0',
  findReplace: 'M6.5 2.5a4 4 0 1 0 0 8a4 4 0 1 0 0-8M9.5 9.5l4 4M10 3.5h4M12 1.5l2 2-2 2',
  countRecords: 'M2.5 3h11v10h-11zM5 6.5h2M5 9.5h2M9.5 6l1 -1v5M9 10h2.5',
  recordId: 'M6 2.5L4.5 13.5M11.5 2.5L10 13.5M2.5 6h11M2 10.5h11',
  unique: 'M3 3.5h4v4H3zM9 3.5h4v4H9zM3 10h4M9 10h4M3 12.5h4M9 12.5h4',
  sample: 'M2.5 3h11M2.5 6.5h11M2.5 10h4M2.5 13h4M10 10.5l1.5 1.5 2.5-3',
  multiRow: 'M2 3.5h8M2 8h8M2 12.5h8M12.5 5.5l2 2.5-2 2.5',
  tile: 'M2.5 2.5h4.5v4.5H2.5zM9 2.5h4.5v4.5H9zM2.5 9h4.5v4.5H2.5zM9 9h4.5v4.5H9z',
  datetime: 'M2.5 3.5h11v10h-11zM2.5 6.5h11M5.5 2v3M10.5 2v3M5 9h1.5M7.5 9H9M10 9h1.5M5 11h1.5M7.5 11H9',
  runningTotal: 'M2 13l4-4.5 3 2.5 5-6.5M10.5 4.5H14V8',
  crosstab: 'M2 2.5h12v11H2zM2 6.5h12M6 2.5v11',
  transpose: 'M2.5 3h4v4h-4zM9.5 9h4v4h-4zM8.5 4h3.5v3.5M11 6.5l1-1M7.5 12H4V8.5M5 9.5l-1-1',
  comment: 'M2.5 2.5h11v8h-6l-3 3v-3h-2zM5 5.5h6M5 8h4',
  container: 'M2 3.5h12v10H2zM2 6.5h12M4 5h3M5 9h2.5v2.5H5zM9 9h2.5v2.5H9z',
  filter: 'M2 3h12l-4.5 5.5V13l-3 1.5V8.5z',
  formula: 'M10.5 2.5c-2 0-2.5 1-2.8 3L6.8 11c-.3 2-.8 3-2.8 3M5 6.5h5.5M10 9l3.5 4M13.5 9L10 13',
  multiField: 'M7 2.5c-1.5 0-2 .8-2.2 2.3L4.2 9.2c-.2 1.5-.7 2.3-2.2 2.3M3 5.5h4M10 4.5h4M10 8h4M10 11.5h4',
  select: 'M2 3h3v3H2zM7.5 4.5H14M2 10h3v3H2zM7.5 11.5H14M2.8 4.5l.7.7 1-1.4',
  sort: 'M4 2v12M1.5 11.5L4 14l2.5-2.5M9 4h6M9 8h4M9 12h2',
  summarize: 'M12.5 3H3.5l4.5 5-4.5 5h9',
  browse: 'M1 8s2.5-5 7-5 7 5 7 5-2.5 5-7 5-7-5-7-5zM8 6a2 2 0 1 0 0 4a2 2 0 1 0 0-4',
};

/** A tool's icon, drawn. */
function icon(doc: Document, tool: FlowTool): SVGSVGElement {
  const svg = doc.createElementNS(SVG, 'svg');
  svg.setAttribute('viewBox', '0 0 16 16');
  svg.setAttribute('aria-hidden', 'true');
  const path = doc.createElementNS(SVG, 'path');
  path.setAttribute('d', ICONS[tool]);
  svg.append(path);
  return svg;
}

/** A box as drawn: its element and its parts, kept from paint to paint (a box focused stays focused). */
interface Drawn {
  readonly root: HTMLElement;
  readonly icon: HTMLElement;
  readonly name: HTMLElement;
  readonly says: HTMLElement;
  /** How many rows it gave when its grid ran last, while its chain is as it was. */
  readonly count: HTMLElement;
  readonly remove: HTMLButtonElement;
  /** What it was drawn as, to draw it again only when that changes. */
  key: string;
  renaming: boolean;
}

export class FlowCanvas {
  readonly #doc: Document;
  readonly #host: HTMLElement;
  readonly #board: FlowBoardState;
  readonly #palette: HTMLElement;
  readonly #canvas: HTMLElement;
  readonly #plane: HTMLElement;
  readonly #lines: SVGSVGElement;
  readonly #hint: HTMLElement;
  /** THE MINIMAP (M7): the whole flow small, the part in view framed -- shown when asked (its toolbar button), while the
   *  flow does not fit; never unasked, as it covers its corner of the canvas. */
  readonly #minimap: SVGSVGElement;
  #mapOn = false;
  #mapButton!: HTMLButtonElement;
  readonly #split: HTMLElement;
  readonly #settings: HTMLElement;
  readonly #settingsTitle: HTMLElement;
  readonly #settingsEmpty: HTMLElement;
  readonly #side: HTMLElement;
  readonly #results: HTMLElement;
  readonly #head: HTMLElement;
  readonly #headTitle: HTMLElement;
  readonly #headActions: HTMLElement;
  /** The results pane's Rows and Profile, and which it shows (the screen's): the box's grid, or its columns summarised. */
  readonly #modes: HTMLElement;
  #mode: 'rows' | 'profile' | 'chart' | 'code' = 'rows';
  /** The shown box's Code as last asked (its key: the box and how many views its grid has landed), and its view. */
  #coded: Coded | undefined;
  readonly #codeView: HTMLElement;
  /** The shown box's chart, while the results pane draws it: for which box and chain, and the panel mounted. */
  #charted: { readonly key: string; readonly chart: FlowChart } | undefined;
  readonly #chartView: HTMLElement;
  /** The profile asked for, by the box and what its rows are made of (its chain): its answer, or why there is none. */
  #profiled: { readonly key: string; readonly answer?: Profile; readonly refused?: string } | undefined;
  readonly #profileView: HTMLElement;
  readonly #body: HTMLElement;
  readonly #empty: HTMLElement;
  readonly #boxes = new Map<string, Drawn>();
  /** The boxes being dragged -- one, or the ones picked together: where each is drawn while it is. */
  #dragged: ReadonlyMap<string, { readonly x: number; readonly y: number }> | undefined;
  /** The boxes picked together (Shift+click, a band dragged round them): moved, removed and copied as one. The screen's,
   *  never the page's. */
  readonly #picked = new Set<string>();
  /** The band a press on the empty canvas drags out, while it does. */
  #band: HTMLElement | undefined;
  /** The gesture in flight: how to end it. */
  #gesture: ((apply: boolean) => void) | undefined;
  /** The box a dragged tool is over, marked. */
  #target: string | undefined;
  /** The results pane's height, as the divider left it (the screen's, not the page's). */
  #paneHeight: number | undefined;
  /** What the settings pane's placeholder was drawn as. */
  #settingsKey = '';
  /** The box clicked last: the settings pane's while no box is shown (one whose source was not opened). */
  #clicked: string | undefined;
  /** The line selected (clicked), which Delete takes off; and the one a dragged tool is over. */
  #line: LineEnd | undefined;
  #lineTarget: LineEnd | undefined;
  /** The line being drawn from a box's output (its port), as the pointer has it (the plane's units), and the side it
   *  would go into. */
  #wire: { readonly from: string; readonly port?: InputPort; x: number; y: number; to?: LineEnd } | undefined;
  /** A selected line's ×, at its middle. */
  readonly #unlink: HTMLButtonElement;
  /** The settings' inputs: a box's Input (a join's Left and Right), each a box of the flow or a frozen copy. */
  readonly #inputs: HTMLElement;
  #inputsKey = '';
  /** The settings' Output: the file a box writes, Write now, and Write no file -- an Output box's. */
  readonly #outputStrip: HTMLElement;
  #outputKey = '';
  /** A Test's mode, in the settings (#paintStops), and what it was drawn for. */
  readonly #stopsStrip: HTMLElement;
  #stopsKey = '';
  /** A Cross Tab's columns, in the settings (#paintPin): found as it runs, or fixed by Load values; what it was drawn for,
   *  and what its last Load values or Unfix said, by box. */
  readonly #pinStrip: HTMLElement;
  #pinKey = '';
  readonly #pinSaid = new Map<string, string>();
  /** Each comment drawn, as a note: its card, by its tile's id. */
  readonly #notes = new Map<string, HTMLElement>();
  /** A box's menu (a right-click), made the first time, and what its entry picked does. */
  #menu: MenuView | undefined;
  #menuAct: ((item: MenuItem) => void) | undefined;
  /** The canvas's own tools, at its corner: Run, Tidy Up. */
  readonly #toolbar: HTMLElement;
  readonly #run: HTMLButtonElement;
  /** A Dry Run (M6): every box counted and judged, no Output written. */
  readonly #dry: HTMLButtonElement;
  readonly #tidy: HTMLButtonElement;
  readonly #undo: HTMLButtonElement;
  readonly #redo: HTMLButtonElement;
  /** Where the pointer is over the canvas, in the window's units: where a paste goes. */
  #pointer: { x: number; y: number } | undefined;
  /** The canvas's zoom (the screen's, never the page's): its boxes drawn at this size, and the level said between − and +. */
  #zoom = 1;
  readonly #zoomLevel: HTMLButtonElement;
  /** What the canvas's keys and gestures do: a panel the toolbar's ? opens. */
  readonly #keysPanel: HTMLElement;
  /** THE COMMAND PALETTE (Ctrl+K, M7): a tool, a box or an action of the canvas found by name -- each the gesture of
   *  its button, its palette entry or a click -- its field, its list, what it lists now and the one Enter runs. */
  readonly #commands: HTMLElement;
  readonly #commandFind: HTMLInputElement;
  readonly #commandList: HTMLElement;
  #commandsFound: readonly Command[] = [];
  #commandAt = 0;
  /** Each Test box's checks found failing (their words) the last time its rows landed, for what its rows were made of
   *  then (its chain) and how many views its grid had landed: kept while it is not shown (#testOf). */
  readonly #tested = new Map<string, { readonly chain: string; readonly viewed: number; readonly of: number; readonly failing: readonly string[] }>();
  /** What the live region was told of each box last (#announce): its name and what it reads. */
  #told: ReadonlyMap<string, { readonly name: string; readonly reads: string }> | undefined;
  /** The toolbar's buttons a command presses, by what it is called. */
  #toolbarCommands: readonly (readonly [string, HTMLButtonElement])[] = [];
  /** The last run's log, at the canvas's corner: drawn for the log it was drawn for, closed until the next run. */
  readonly #logPanel: HTMLElement;
  #logDrawn: RunLog | undefined;
  #logClosed: RunLog | undefined;
  /** The palette's find field, and its tools by name. */
  readonly #find: HTMLInputElement;
  readonly #tools = new Map<FlowTool, HTMLButtonElement>();

  constructor(host: HTMLElement, board: FlowBoardState) {
    this.#host = host;
    this.#doc = host.ownerDocument;
    this.#board = board;
    const doc = this.#doc;
    const el = (tag: string, cls: string): HTMLElement => {
      const e = doc.createElement(tag);
      e.className = cls;
      return e;
    };
    this.#palette = el('div', 'dc-flow-palette');
    this.#palette.setAttribute('role', 'toolbar');
    this.#palette.setAttribute('aria-label', 'Tools');
    this.#palette.setAttribute('aria-orientation', 'vertical');
    // FIND: its tools narrowed to those whose name or words hold what is typed; Enter adds the first after the box shown
    this.#find = doc.createElement('input');
    this.#find.type = 'search';
    this.#find.className = 'dc-flow-find';
    this.#find.placeholder = 'Find';
    this.#find.setAttribute('aria-label', 'Find a tool');
    this.#find.addEventListener('input', () => this.#narrow());
    this.#find.addEventListener('keydown', (e) => {
      if (e.key === 'Escape' && this.#find.value !== '') {
        e.stopPropagation();
        this.#find.value = '';
        this.#narrow();
        return;
      }
      if (e.key !== 'Enter') return;
      e.preventDefault();
      const first = PALETTE.find((t) => !this.#tools.get(t)!.hidden);
      if (first !== undefined) this.#board.options.onTool?.(first, this.#board.shown, TOOL_EDITORS[first]);
    });
    this.#palette.append(this.#find);
    // its tools by Alteryx's groups, each under its heading
    let group: string | undefined;
    for (const tool of PALETTE) {
      const g = FLOW_TOOLS[tool].group;
      if (g !== group) {
        group = g;
        const heading = el('div', 'dc-flow-group');
        heading.textContent = GROUP_WORDS[g];
        heading.dataset['group'] = g;
        heading.setAttribute('aria-hidden', 'true');
        this.#palette.append(heading);
      }
      const button = this.#tool(tool);
      this.#tools.set(tool, button);
      this.#palette.append(button);
    }
    const main = el('div', 'dc-flow-main');
    this.#canvas = el('div', 'dc-flow-canvas');
    this.#canvas.tabIndex = -1;
    this.#plane = el('div', 'dc-flow-plane');
    this.#lines = doc.createElementNS(SVG, 'svg');
    this.#lines.setAttribute('class', 'dc-flow-lines');
    this.#lines.setAttribute('aria-hidden', 'true');
    this.#hint = el('div', 'dc-flow-hint');
    this.#hint.textContent = 'Drag an Input onto the canvas to start, then add tools after it: each box is one step, and its rows show below.';
    this.#unlink = doc.createElement('button');
    this.#unlink.type = 'button';
    this.#unlink.className = 'dc-flow-unlink';
    closeMark(this.#unlink, 'Disconnect');
    this.#unlink.title = 'Disconnect: the box it goes into keeps the rows it has now, frozen';
    this.#unlink.setAttribute('aria-label', 'Disconnect this line');
    this.#unlink.hidden = true;
    this.#unlink.addEventListener('pointerdown', (e) => e.stopPropagation());
    this.#unlink.addEventListener('click', () => this.#takeOffLine());
    this.#plane.append(this.#lines, this.#unlink);
    this.#canvas.append(this.#plane, this.#hint);
    // THE SETTINGS PANE (§3.4, Alteryx's Configuration window): the shown box's grid docks the window that sets it here;
    // with none docked, what the box does and how to set it
    this.#side = el('div', 'dc-flow-side');
    this.#side.setAttribute('role', 'separator');
    this.#side.setAttribute('aria-orientation', 'vertical');
    this.#side.setAttribute('aria-label', 'Resize the settings');
    this.#side.tabIndex = 0;
    this.#settings = el('section', 'dc-flow-settings');
    this.#settings.setAttribute('aria-label', 'Settings');
    const settingsHead = el('div', 'dc-flow-settings-head');
    this.#settingsTitle = el('div', 'dc-flow-settings-title');
    settingsHead.append(this.#settingsTitle);
    this.#settingsEmpty = el('div', 'dc-flow-settings-empty');
    this.#inputs = el('div', 'dc-flow-inputs');
    this.#outputStrip = el('div', 'dc-flow-output');
    this.#stopsStrip = el('div', 'dc-flow-output dc-flow-stops');
    this.#pinStrip = el('div', 'dc-flow-output dc-flow-pin');
    this.#settings.append(settingsHead, this.#inputs, this.#outputStrip, this.#stopsStrip, this.#pinStrip, board.dock, this.#settingsEmpty);
    // the canvas on its stage, its own tools at its corner, over it as it scrolls
    const stage = el('div', 'dc-flow-stage');
    this.#toolbar = el('div', 'dc-flow-toolbar');
    const tool = (cls: string, text: string, title: string, act: () => void): HTMLButtonElement => {
      const b = doc.createElement('button');
      b.type = 'button';
      b.className = `dc-flow-act ${cls}`;
      b.textContent = text;
      b.title = title;
      b.addEventListener('click', act);
      return b;
    };
    this.#run = tool('dc-flow-run', '▶ Run', 'Run every box, left to right, and count the rows each gives (Ctrl+Enter)', () => this.#board.options.onRun?.());
    this.#dry = tool('dc-flow-dry', 'Dry Run', 'Run every box and count its rows, its Tests judged: no Output written (Ctrl+Shift+Enter)',
      () => this.#board.options.onRun?.(true));
    this.#tidy = tool('dc-flow-tidy', 'Tidy Up', 'Place every box a column after the boxes it reads, level with them', () => this.#board.options.onTidy?.());
    // the page's own Undo and Redo, each saying what it would undo or redo
    this.#undo = tool('dc-flow-undo', '↶', 'Undo (Ctrl+Z)', () => this.#board.options.onUndo?.(false));
    this.#redo = tool('dc-flow-redo', '↷', 'Redo (Ctrl+Shift+Z)', () => this.#board.options.onUndo?.(true));
    this.#undo.setAttribute('aria-label', 'Undo');
    this.#redo.setAttribute('aria-label', 'Redo');
    // ZOOM: − and + a level each, the level itself back to its own size, Fit all of it in view; Ctrl+wheel too
    const zoomOut = tool('dc-flow-zoom', '−', 'Zoom out (Ctrl+wheel)', () => this.#zoomBy(-1));
    this.#zoomLevel = tool('dc-flow-zoom dc-flow-zoom-level', '100%', 'Its own size', () => this.#setZoom(1));
    const zoomIn = tool('dc-flow-zoom', '+', 'Zoom in (Ctrl+wheel)', () => this.#zoomBy(1));
    const fit = tool('dc-flow-zoom', 'Fit', 'Every box in view', () => this.#fit());
    // what the canvas's keys and gestures do, said on asking
    this.#mapButton = tool('dc-flow-zoom dc-flow-map', 'Map', 'The whole flow, small: click or drag on it to move there', () => {
      this.#mapOn = !this.#mapOn;
      this.#paintMinimap();
    });
    this.#mapButton.setAttribute('aria-pressed', 'false');
    const keys = tool('dc-flow-zoom dc-flow-keys', '?', 'Keys and gestures', () => {
      this.#keysPanel.hidden = !this.#keysPanel.hidden;
    });
    keys.setAttribute('aria-label', 'Keys and gestures');
    zoomOut.setAttribute('aria-label', 'Zoom out');
    zoomIn.setAttribute('aria-label', 'Zoom in');
    this.#toolbar.append(this.#run, this.#dry, this.#tidy, this.#undo, this.#redo, zoomOut, this.#zoomLevel, zoomIn, fit, this.#mapButton, keys);
    // what Undo and Redo would do read again as the pointer comes to them
    this.#toolbar.addEventListener('pointerenter', () => this.#paintHistory());
    this.#canvas.addEventListener('wheel', (e) => {
      if (!(e.ctrlKey || e.metaKey)) return;
      e.preventDefault();
      this.#zoomBy(e.deltaY < 0 ? 1 : -1, { x: e.clientX, y: e.clientY });
    }, { passive: false });
    this.#logPanel = el('section', 'dc-flow-log');
    this.#logPanel.setAttribute('aria-label', 'Run log');
    this.#logPanel.hidden = true;
    this.#keysPanel = el('section', 'dc-flow-keys-panel');
    this.#keysPanel.setAttribute('aria-label', 'Keys and gestures');
    this.#keysPanel.hidden = true;
    const keysHead = el('div', 'dc-flow-keys-head');
    keysHead.append(Object.assign(doc.createElement('span'), { className: 'dc-flow-keys-title', textContent: 'Keys and gestures' }));
    const keysClose = doc.createElement('button');
    keysClose.type = 'button';
    keysClose.className = 'dc-flow-keys-close';
    closeMark(keysClose);
    keysClose.setAttribute('aria-label', 'Close');
    keysClose.addEventListener('click', () => { this.#keysPanel.hidden = true; });
    keysHead.append(keysClose);
    const keysList = doc.createElement('dl');
    keysList.className = 'dc-flow-keys-list';
    for (const [k, does] of KEYS) {
      keysList.append(Object.assign(doc.createElement('dt'), { textContent: k }), Object.assign(doc.createElement('dd'), { textContent: does }));
    }
    this.#keysPanel.append(keysHead, keysList);
    this.#minimap = doc.createElementNS(SVG, 'svg');
    this.#minimap.setAttribute('class', 'dc-flow-minimap');
    this.#minimap.setAttribute('role', 'img');
    this.#minimap.setAttribute('aria-label', 'The whole flow: click or drag to move there');
    // a press or a drag on it: the view centred there
    this.#minimap.addEventListener('pointerdown', (e) => {
      e.preventDefault();
      this.#minimap.setPointerCapture(e.pointerId);
      const go = (ev: PointerEvent): void => this.#minimapTo(ev);
      go(e);
      this.#minimap.addEventListener('pointermove', go);
      this.#minimap.addEventListener('pointerup', () => this.#minimap.removeEventListener('pointermove', go), { once: true });
    });
    this.#canvas.addEventListener('scroll', () => this.#paintMinimap());
    this.#commands = el('div', 'dc-flow-commands');
    this.#commands.hidden = true;
    this.#commands.setAttribute('role', 'dialog');
    this.#commands.setAttribute('aria-label', 'Commands');
    this.#commandFind = doc.createElement('input');
    this.#commandFind.type = 'search';
    this.#commandFind.className = 'dc-flow-commands-find';
    this.#commandFind.placeholder = 'A tool, a box or an action…';
    this.#commandFind.setAttribute('aria-label', 'Find a command');
    this.#commandList = el('div', 'dc-flow-commands-list');
    this.#commandList.setAttribute('role', 'listbox');
    this.#commands.append(this.#commandFind, this.#commandList);
    this.#commandFind.addEventListener('input', () => {
      this.#commandAt = 0;
      this.#paintCommands();
    });
    this.#commandFind.addEventListener('keydown', (e) => this.#onCommandKey(e));
    this.#commands.addEventListener('focusout', (e) => {
      // where the focus went (a node of the same page, or none): outside it, closed
      if (!this.#commands.contains(e.relatedTarget as Node | null)) this.#closeCommands(false);
    });
    this.#toolbarCommands = [
      ['Run the flow', this.#run], ['Dry run the flow', this.#dry], ['Tidy Up', this.#tidy], ['Undo', this.#undo], ['Redo', this.#redo], ['Zoom in', zoomIn],
      ['Zoom out', zoomOut], ['Zoom to its own size', this.#zoomLevel], ['Fit every box in view', fit], ['Map', this.#mapButton],
      ['Keys and gestures', keys],
    ];
    stage.append(this.#canvas, this.#toolbar, this.#logPanel, this.#keysPanel, this.#commands, this.#minimap);
    const top = el('div', 'dc-flow-top');
    top.append(stage, this.#side, this.#settings);
    this.#split = el('div', 'dc-flow-split');
    this.#split.setAttribute('role', 'separator');
    this.#split.setAttribute('aria-orientation', 'horizontal');
    this.#split.setAttribute('aria-label', 'Resize the results');
    this.#split.tabIndex = 0;
    this.#results = el('section', 'dc-flow-results');
    this.#results.setAttribute('aria-label', 'Results');
    this.#head = el('div', 'dc-flow-results-head');
    this.#headTitle = el('div', 'dc-flow-results-title');
    this.#headActions = el('div', 'dc-flow-results-actions');
    this.#modes = el('div', 'dc-flow-modes');
    this.#modes.setAttribute('role', 'tablist');
    this.#modes.setAttribute('aria-label', 'Rows, profile, chart or code');
    for (const [mode, label, title] of [
      ['rows', 'Rows', 'Its rows, as its grid shows them'],
      ['profile', 'Profile', 'Each column summarised: its values, its different values, its smallest and largest'],
      ['chart', 'Chart', 'Its rows drawn: a chart of what it groups by, its options a click away'],
      ['code', 'Code', 'Its query, as Pure, and the SQL the database ran: to read, copy or keep'],
    ] as const) {
      const b = doc.createElement('button');
      b.type = 'button';
      b.className = 'dc-flow-mode';
      b.dataset['mode'] = mode;
      b.textContent = label;
      b.title = title;
      b.setAttribute('role', 'tab');
      b.addEventListener('click', () => {
        this.#mode = mode;
        this.#paintResults();
      });
      this.#modes.append(b);
    }
    this.#profileView = el('div', 'dc-flow-profile');
    this.#chartView = el('div', 'dc-flow-chart');
    this.#codeView = el('div', 'dc-flow-profile dc-flow-code');
    this.#head.append(this.#headTitle, this.#modes, this.#headActions);
    this.#body = el('div', 'dc-flow-results-body');
    this.#empty = el('div', 'dc-flow-results-empty');
    this.#results.append(this.#head, this.#body);
    main.append(top, this.#split, this.#results);
    host.prepend(this.#palette, main);
    this.#canvas.addEventListener('keydown', (e) => this.#onCanvasKey(e));
    // where the pointer is over the canvas: where Ctrl+V pastes
    this.#canvas.addEventListener('pointermove', (e) => { this.#pointer = { x: e.clientX, y: e.clientY }; });
    this.#canvas.addEventListener('pointerleave', () => { this.#pointer = undefined; });
    // A FILE DROPPED from the desktop: an Input over it, as the picker opens one
    const files = (e: DragEvent): boolean => this.#board.editing && (e.dataTransfer?.types ?? []).includes('Files');
    this.#canvas.addEventListener('dragover', (e) => {
      if (!files(e)) return;
      e.preventDefault();
      this.#canvas.classList.add('dc-flow-dropping');
    });
    this.#canvas.addEventListener('dragleave', (e) => {
      if (e.target === this.#canvas) this.#canvas.classList.remove('dc-flow-dropping');
    });
    this.#canvas.addEventListener('drop', (e) => {
      this.#canvas.classList.remove('dc-flow-dropping');
      if (!files(e)) return;
      e.preventDefault();
      const file = e.dataTransfer?.files[0];
      if (file !== undefined) this.#board.options.onFile?.(file);
    });
    this.#canvas.addEventListener('pointerdown', (e) => {
      // a press on the canvas itself, not a box: focus there, so Ctrl+Z reaches the page -- and any line let go
      if (e.target === this.#canvas || e.target === this.#plane || e.target === this.#lines) {
        this.#canvas.focus({ preventScroll: true });
        this.#selectLine(undefined);
        // a band dragged out picks the boxes it touches; a click, none
        this.#startBand(e);
      }
    });
    this.#split.addEventListener('pointerdown', (e) => this.#startSplit(e));
    this.#side.addEventListener('pointerdown', (e) => this.#startSide(e));
    this.#side.addEventListener('keydown', (e) => {
      if (e.key !== 'ArrowLeft' && e.key !== 'ArrowRight') return;
      e.preventDefault();
      this.#setSide(this.#settings.offsetWidth + (e.key === 'ArrowLeft' ? BIG_STEP : -BIG_STEP));
    });
    this.#split.addEventListener('keydown', (e) => {
      if (e.key !== 'ArrowUp' && e.key !== 'ArrowDown') return;
      e.preventDefault();
      this.#setPane((this.#paneHeight ?? this.#results.offsetHeight) + (e.key === 'ArrowUp' ? BIG_STEP : -BIG_STEP));
    });
  }

  /** The board drawn as it is now. */
  paint(): void {
    const board = this.#board;
    const layout = this.#placed(board.layout);
    const editing = board.editing;
    this.#host.classList.toggle('dc-flow-view', !editing);
    this.#palette.hidden = !editing;
    // the boxes: each one's element kept, drawn again where it changed -- and its comments, each a note (M7)
    const folded = this.#foldedBoxes(layout);
    const ids = new Set(layout.boxes.filter((b) => board.noteOf(b.tile) === undefined).map((b) => b.tile));
    const notes = new Set(layout.boxes.filter((b) => board.noteOf(b.tile) !== undefined).map((b) => b.tile));
    for (const [id, drawn] of [...this.#boxes]) {
      if (ids.has(id)) continue;
      drawn.root.remove();
      this.#boxes.delete(id);
    }
    for (const [id, note] of [...this.#notes]) {
      if (notes.has(id)) continue;
      note.remove();
      this.#notes.delete(id);
    }
    for (const box of layout.boxes) {
      const tile = board.noteOf(box.tile);
      if (tile !== undefined && box.frame !== undefined) {
        this.#paintContainer(box, box.frame, tile.element.textContent ?? '', layout, editing);
        continue;
      }
      if (tile !== undefined) {
        const note = this.#notes.get(box.tile) ?? this.#makeNote(box.tile);
        note.classList.remove('dc-flow-container', 'dc-flow-folded');
        note.style.width = '';
        note.style.height = '';
        // its words' element, its owner's, in its body: placed once
        const body = note.querySelector<HTMLElement>('.dc-flow-note-body')!;
        if (body.firstChild !== tile.element) body.replaceChildren(tile.element);
        note.style.transform = `translate(${box.x}px, ${box.y}px)`;
        note.querySelector<HTMLElement>('.dc-flow-note-x')!.hidden = !editing;
        continue;
      }
      const drawn = this.#boxes.get(box.tile) ?? this.#make(box.tile);
      this.#paintBox(box.tile, drawn);
      drawn.root.style.transform = `translate(${box.x}px, ${box.y}px)`;
      // a box in a folded container is not drawn: its container's head stands for it
      drawn.root.hidden = folded.has(box.tile);
    }
    this.#paintPicked();
    this.#paintLines(layout);
    // the plane as large as its boxes, and the canvas's room past them
    const right = Math.max(0, ...layout.boxes.map((b) => b.x + BOX_WIDTH)) + MARGIN;
    const bottom = Math.max(0, ...layout.boxes.map((b) => b.y + BOX_HEIGHT)) + MARGIN;
    this.#plane.style.width = `${right}px`;
    this.#plane.style.height = `${bottom}px`;
    this.#paintMinimap();
    this.#hint.hidden = layout.boxes.length > 0 || !editing;
    // Run on a locked page too: it changes nothing of the page's
    const running = board.running;
    const history = this.#paintHistory();
    // an empty canvas has no tools but Undo and Redo, while there is something to undo or redo
    this.#toolbar.hidden = layout.boxes.length === 0 && history.undo === undefined && history.redo === undefined;
    this.#tidy.hidden = !editing || layout.boxes.length < 2;
    this.#run.disabled = running !== undefined;
    this.#run.textContent = running === undefined ? '▶ Run' : `Running ${Math.min(running.done + 1, running.of)} of ${running.of}…`;
    this.#dry.disabled = running !== undefined;
    this.#dry.hidden = !layout.boxes.some((b) => b.output !== undefined);
    this.#paintLog();
    this.#paintResults();
    this.#paintSettings();
    this.#announce([...this.#boxes.keys()]);
  }

  /**
   * WHAT CHANGED, SAID (M7, a screen reader's): each box added (and what it reads), taken off, renamed, or reading other
   * boxes now -- whatever made the change, a gesture, an undo or Python -- told in the board's live region in a sentence
   * or two; nothing the first time it is drawn, nor when only how it looks changed.
   */
  #announce(ids: readonly string[]): void {
    const nameOf = (id: string): string => this.#nameOf(id, this.#said(id)?.tool ?? 'browse');
    const now = new Map(ids.map((id) => [id, {
      name: nameOf(id),
      reads: this.#inputsOf(id).map((i) => ('grid' in i
        ? `${nameOf(i.grid)}${i.port !== undefined ? ` (${portWord(this.#said(i.grid)?.tool, i.port)})` : ''}` : `a copy of ${i.frozen}`)).join(' and '),
    }] as const));
    const was = this.#told;
    this.#told = now;
    if (was === undefined) return;
    const said: string[] = [];
    for (const [id, n] of now) {
      const w = was.get(id);
      if (w === undefined) said.push(n.reads === '' ? `Added ${n.name}` : `Added ${n.name}, reading ${n.reads}`);
      else {
        if (w.name !== n.name) said.push(`${w.name} renamed ${n.name}`);
        if (w.reads !== n.reads) said.push(n.reads === '' ? `${n.name} reads no box now` : `${n.name} reads ${n.reads} now`);
      }
    }
    for (const [id, w] of was) if (!now.has(id)) said.push(`Removed ${w.name}`);
    if (said.length === 0) return;
    this.#board.say(`${said.slice(0, 3).join('. ')}${said.length > 3 ? `. And ${said.length - 3} more` : ''}.`);
  }

  /** Undo and Redo as the page has them now: each on while it has a step, its title the step's name; neither on a
   *  locked page. */
  #paintHistory(): { readonly undo?: string | undefined; readonly redo?: string | undefined } {
    const editing = this.#board.editing;
    const history = editing ? this.#board.options.history?.() ?? {} : {};
    for (const [button, label, word, keys] of [
      [this.#undo, history.undo, 'Undo', 'Ctrl+Z'], [this.#redo, history.redo, 'Redo', 'Ctrl+Shift+Z'],
    ] as const) {
      button.hidden = !editing;
      button.disabled = label === undefined;
      button.title = label === undefined ? `Nothing to ${word.toLowerCase()}` : `${word} ${label} (${keys})`;
    }
    return history;
  }

  /** A box scrolled into view (the canvas scrolls in its zoomed size). */
  reveal(id: string): void {
    const box = this.#placed(this.#board.layout).boxes.find((b) => b.tile === id);
    if (!box) return;
    const c = this.#canvas;
    const z = this.#zoom;
    const [x, y, w, h, m] = [box.x * z, box.y * z, BOX_WIDTH * z, BOX_HEIGHT * z, MARGIN * z];
    if (x < c.scrollLeft) c.scrollLeft = Math.max(0, x - m);
    else if (x + w > c.scrollLeft + c.clientWidth) c.scrollLeft = x + w + m - c.clientWidth;
    if (y < c.scrollTop) c.scrollTop = Math.max(0, y - m);
    else if (y + h > c.scrollTop + c.clientHeight) c.scrollTop = y + h + m - c.clientHeight;
  }

  /** The zoom a level up or down (`by`), kept at the point `at` of the window when given (a wheel's pointer). */
  #zoomBy(by: 1 | -1, at?: { readonly x: number; readonly y: number }): void {
    const i = ZOOMS.findIndex((z) => z >= this.#zoom - 1e-9);
    const next = ZOOMS[Math.max(0, Math.min(ZOOMS.length - 1, (i < 0 ? ZOOMS.length - 1 : i) + by))]!;
    this.#setZoom(next, at);
  }

  /** The canvas drawn at `zoom`, the point `at` of the window (else its top left) where it was. */
  #setZoom(zoom: number, at?: { readonly x: number; readonly y: number }): void {
    const was = this.#zoom;
    if (Math.abs(zoom - was) < 1e-9) return;
    const c = this.#canvas;
    const r = c.getBoundingClientRect();
    const px = at === undefined ? 0 : at.x - r.left;
    const py = at === undefined ? 0 : at.y - r.top;
    const planeX = (c.scrollLeft + px) / was;
    const planeY = (c.scrollTop + py) / was;
    this.#zoom = zoom;
    this.#plane.style.setProperty('zoom', String(zoom));
    this.#zoomLevel.textContent = `${Math.round(zoom * 100)}%`;
    c.scrollLeft = Math.max(0, planeX * zoom - px);
    c.scrollTop = Math.max(0, planeY * zoom - py);
    this.#paintMinimap();
  }

  /** How the minimap draws the plane: its scale, and the plane's size (its boxes' room). */
  #minimapScale(): { readonly k: number; readonly w: number; readonly h: number } {
    const w = parseFloat(this.#plane.style.width) || 1;
    const h = parseFloat(this.#plane.style.height) || 1;
    return { k: Math.min(MINIMAP.w / w, MINIMAP.h / h), w, h };
  }

  /**
   * THE MINIMAP drawn, when asked: each box a mark (the one shown in its colour, a container its frame), and the part of
   * the plane in view framed. Its button off while every box is in view -- there is nowhere to move to.
   */
  #paintMinimap(): void {
    const map = this.#minimap;
    const c = this.#canvas;
    const z = this.#zoom;
    const { k, w, h } = this.#minimapScale();
    const fits = w * z <= c.clientWidth + 1 && h * z <= c.clientHeight + 1;
    const button = this.#mapButton;
    if (button !== undefined) {
      button.disabled = fits;
      button.setAttribute('aria-pressed', String(this.#mapOn && !fits));
      button.classList.toggle('dc-chosen', this.#mapOn && !fits);
    }
    map.style.display = !this.#mapOn || fits || this.#board.layout.boxes.length === 0 ? 'none' : '';
    if (map.style.display === 'none') return;
    map.setAttribute('width', String(Math.ceil(w * k)));
    map.setAttribute('height', String(Math.ceil(h * k)));
    const doc = this.#doc;
    const rect = (cls: string, x: number, y: number, rw: number, rh: number): SVGRectElement => {
      const r = doc.createElementNS(SVG, 'rect');
      r.setAttribute('class', cls);
      r.setAttribute('x', String(x * k));
      r.setAttribute('y', String(y * k));
      r.setAttribute('width', String(Math.max(1, rw * k)));
      r.setAttribute('height', String(Math.max(1, rh * k)));
      return r;
    };
    const layout = this.#placed(this.#board.layout);
    const folded = this.#foldedBoxes(layout);
    const marks: SVGRectElement[] = [];
    for (const box of layout.boxes) {
      const note = this.#board.noteOf(box.tile);
      if (note !== undefined && box.frame !== undefined) marks.push(rect('dc-flow-minimap-frame', box.x, box.y, box.frame.w, box.frame.h));
      else if (note === undefined && !folded.has(box.tile)) {
        marks.push(rect(`dc-flow-minimap-box${this.#board.shown === box.tile ? ' dc-flow-minimap-shown' : ''}`, box.x, box.y, BOX_WIDTH, BOX_HEIGHT));
      }
    }
    marks.push(rect('dc-flow-minimap-view', c.scrollLeft / z, c.scrollTop / z, c.clientWidth / z, c.clientHeight / z));
    map.replaceChildren(...marks);
  }

  /** The view centred where the minimap is pressed. */
  #minimapTo(e: PointerEvent): void {
    const r = this.#minimap.getBoundingClientRect();
    const { k } = this.#minimapScale();
    const c = this.#canvas;
    const z = this.#zoom;
    c.scrollLeft = Math.max(0, ((e.clientX - r.left) / k) * z - c.clientWidth / 2);
    c.scrollTop = Math.max(0, ((e.clientY - r.top) / k) * z - c.clientHeight / 2);
  }

  /** Every box in view: the largest zoom, its own size at most, that shows them all. */
  #fit(): void {
    const boxes = this.#board.layout.boxes;
    if (boxes.length === 0) return;
    const w = Math.max(...boxes.map((b) => b.x + BOX_WIDTH)) + MARGIN;
    const h = Math.max(...boxes.map((b) => b.y + BOX_HEIGHT)) + MARGIN;
    const c = this.#canvas;
    const fits = Math.min(1.5, c.clientWidth / w, c.clientHeight / h);
    this.#setZoom(Math.max(0.25, Math.min(1, fits)));
    c.scrollLeft = 0;
    c.scrollTop = 0;
  }

  dispose(): void {
    this.#gesture?.(false);
    this.#menu?.close();
    this.#palette.remove();
    this.#canvas.parentElement?.remove();
  }

  /** The flow as drawn: a box being dragged where the pointer has it. */
  #placed(flow: Flow): Flow {
    const d = this.#dragged;
    return d === undefined ? flow : { boxes: flow.boxes.map((b) => {
      const at = d.get(b.tile);
      return at === undefined ? b : { tile: b.tile, x: at.x, y: at.y };
    }) };
  }

  /** The boxes a gesture on `id` acts on: the ones picked together, when it is one of them; else it alone. */
  #group(id: string): string[] {
    return this.#picked.has(id) && this.#picked.size > 1 ? [...this.#picked] : [id];
  }

  /** The picked boxes drawn so -- and any no longer on the canvas let go. */
  #paintPicked(): void {
    for (const id of [...this.#picked]) if (!this.#boxes.has(id)) this.#picked.delete(id);
    for (const [id, drawn] of this.#boxes) drawn.root.classList.toggle('dc-flow-picked', this.#picked.has(id));
  }

  /** The boxes picked together, now these: the board's (boxes just pasted). */
  pick(ids: Iterable<string>): void {
    this.#pick(ids);
  }

  /** The boxes picked together, now these (none: none). */
  #pick(ids: Iterable<string>): void {
    this.#picked.clear();
    for (const id of ids) this.#picked.add(id);
    this.#paintPicked();
    const n = this.#picked.size;
    if (n > 1) this.#board.say(`${n} boxes picked: drag one to move them, Delete to remove them, Ctrl+C to copy them.`);
  }

  /** A BAND dragged out from a press on the empty canvas: the boxes it touches picked when it is let go. A press that does
   *  not move lets the picked ones go. */
  #startBand(e: PointerEvent): void {
    if (e.button !== 0) return;
    const r = this.#plane.getBoundingClientRect();
    const from = { x: (e.clientX - r.left) / this.#zoom, y: (e.clientY - r.top) / this.#zoom };
    let to = from;
    let moved = false;
    const adding = e.shiftKey || e.ctrlKey || e.metaKey;
    const touched = (): string[] => {
      const x0 = Math.min(from.x, to.x);
      const x1 = Math.max(from.x, to.x);
      const y0 = Math.min(from.y, to.y);
      const y1 = Math.max(from.y, to.y);
      return this.#board.layout.boxes
        .filter((b) => this.#boxes.has(b.tile) && b.x < x1 && b.x + BOX_WIDTH > x0 && b.y < y1 && b.y + BOX_HEIGHT > y0)
        .map((b) => b.tile);
    };
    this.#own(this.#canvas, e, (ev) => {
      const rect = this.#plane.getBoundingClientRect();
      to = { x: (ev.clientX - rect.left) / this.#zoom, y: (ev.clientY - rect.top) / this.#zoom };
      if (!moved && Math.hypot(to.x - from.x, to.y - from.y) * this.#zoom < DRAG_PX) return;
      moved = true;
      const band = this.#band ?? this.#doc.createElement('div');
      if (this.#band === undefined) {
        band.className = 'dc-flow-band';
        this.#plane.append(band);
        this.#band = band;
      }
      band.style.transform = `translate(${Math.min(from.x, to.x)}px, ${Math.min(from.y, to.y)}px)`;
      band.style.width = `${Math.abs(to.x - from.x)}px`;
      band.style.height = `${Math.abs(to.y - from.y)}px`;
      for (const [id, drawn] of this.#boxes) drawn.root.classList.toggle('dc-flow-picked', this.#picked.has(id) || touched().includes(id));
    }, (apply) => {
      this.#band?.remove();
      this.#band = undefined;
      if (!moved) {
        if (apply && !adding) this.#pick([]);
        return;
      }
      this.#pick(apply ? [...(adding ? this.#picked : []), ...touched()] : this.#picked);
    });
  }

  // -- the palette --------------------------------------------------------------------------------------------------

  #tool(tool: FlowTool): HTMLButtonElement {
    const doc = this.#doc;
    const button = doc.createElement('button');
    button.type = 'button';
    button.className = 'dc-flow-tool';
    button.dataset['tool'] = tool;
    button.dataset['group'] = FLOW_TOOLS[tool].group;
    const glyph = doc.createElement('span');
    glyph.className = 'dc-flow-icon';
    glyph.append(icon(doc, tool));
    const name = doc.createElement('span');
    name.className = 'dc-flow-tool-name';
    name.textContent = FLOW_TOOLS[tool].name;
    button.append(glyph, name);
    button.title = `${FLOW_TOOLS[tool].name}: ${FLOW_TOOLS[tool].does}. Click to add one after the box shown, or drag it onto a box.`;
    // a press: a click adds one after the box shown; dragged, after the box it is let go on
    button.addEventListener('pointerdown', (e) => this.#startTool(tool, button, e));
    button.addEventListener('keydown', (e) => {
      if (e.key !== 'Enter' && e.key !== ' ') return;
      e.preventDefault();
      this.#board.options.onTool?.(tool, this.#board.shown, TOOL_EDITORS[tool]);
    });
    return button;
  }

  #startTool(tool: FlowTool, button: HTMLElement, e: PointerEvent): void {
    if (e.button !== 0 || !this.#board.editing) return;
    e.preventDefault();
    const start = { x: e.clientX, y: e.clientY };
    let ghost: HTMLElement | undefined;
    this.#own(button, e, (ev) => {
      if (ghost === undefined && Math.hypot(ev.clientX - start.x, ev.clientY - start.y) < DRAG_PX) return;
      if (ghost === undefined) {
        ghost = button.cloneNode(true) as HTMLElement;
        ghost.classList.add('dc-flow-ghost');
        this.#doc.body.append(ghost);
      }
      ghost.style.transform = `translate(${ev.clientX + 8}px, ${ev.clientY + 8}px)`;
      const box = this.#boxAt(ev.clientX, ev.clientY);
      this.#mark(box);
      // over a line, not a box: the tool goes between the boxes it links (an Input reads none: never between)
      this.#markLine(box === undefined && tool !== 'input' ? this.#lineAt(ev.clientX, ev.clientY) : undefined);
    }, (apply, ev) => {
      ghost?.remove();
      const over = ev ? this.#boxAt(ev.clientX, ev.clientY) : undefined;
      const line = this.#lineTarget;
      this.#mark(undefined);
      this.#markLine(undefined);
      if (!apply) return;
      const editor = TOOL_EDITORS[tool];
      if (ghost === undefined) {
        button.focus({ preventScroll: true });
        this.#board.options.onTool?.(tool, this.#board.shown, editor);
        return;
      }
      // let go on a box: after it; on a line: between its boxes; on the canvas: after none; anywhere else: nothing
      if (over !== undefined) this.#board.options.onTool?.(tool, over, editor);
      else if (line !== undefined) {
        const from = this.#fromOf(line);
        this.#board.options.onTool?.(tool, from?.grid, editor, line, from?.port);
      }
      else if (ev && this.#onCanvas(ev.clientX, ev.clientY) && tool === 'container') {
        // its head's middle where it was let go
        const p = this.#pointAt(ev);
        this.#board.options.onContainer?.({ x: Math.max(0, p.x + BOX_WIDTH / 2 - CONTAINER_FRAME.w / 2), y: Math.max(0, p.y + BOX_HEIGHT / 2 - FRAME_HEAD / 2) }, CONTAINER_FRAME);
      } else if (ev && this.#onCanvas(ev.clientX, ev.clientY)) this.#board.options.onTool?.(tool, undefined, editor, undefined, undefined, this.#pointAt(ev));
    });
  }

  /** The palette narrowed to the tools whose name or words hold what the find field holds (a heading kept with its own). */
  #narrow(): void {
    const q = this.#find.value.trim().toLowerCase();
    for (const [tool, button] of this.#tools) {
      const t = FLOW_TOOLS[tool];
      button.hidden = q !== '' && !`${t.name} ${t.does}`.toLowerCase().includes(q);
    }
    for (const heading of this.#palette.querySelectorAll<HTMLElement>('.dc-flow-group')) {
      heading.hidden = PALETTE.every((t) => FLOW_TOOLS[t].group !== heading.dataset['group'] || this.#tools.get(t)!.hidden);
    }
  }

  /**
   * THE RUN'S LOG (M6), at the canvas's bottom corner: when it ran and how long it took, then each box -- its name, its
   * rows, its time, or why it was refused, red -- each a button that shows it. Closed by its ×, until the next run.
   */
  #paintLog(): void {
    const log = this.#board.log;
    const shown = log !== undefined && log !== this.#logClosed && this.#board.running === undefined;
    this.#logPanel.hidden = !shown;
    if (!shown || log === this.#logDrawn) return;
    this.#logDrawn = log;
    const d = this.#doc;
    const el = (tag: string, cls: string, text?: string): HTMLElement => {
      const e = d.createElement(tag);
      e.className = cls;
      if (text !== undefined) e.textContent = text;
      return e;
    };
    const head = el('div', 'dc-flow-log-head');
    const refused = log.boxes.filter((b) => b.refused !== undefined).length;
    // a Test's result, as its rows landed in the run
    const tests = new Map(log.boxes.flatMap((b) => {
      const t = this.#board.layout.boxes.some((x) => x.tile === b.id) ? this.#testOf(b.id) ?? this.#lackingOf(b.id) : undefined;
      return t === undefined ? [] : [[b.id, t] as const];
    }));
    // a Test failing, and a Cross Tab whose values lack a column (its result says its own words), each counted
    const failing = [...tests.values()].filter((t) => t.failing.length > 0 && !('says' in t)).length;
    const lacking = [...tests.values()].filter((t) => t.failing.length > 0 && 'says' in t).length;
    const seconds = (ms: number): string => (ms < 1000 ? `${ms} ms` : `${(ms / 1000).toLocaleString(UI_LOCALE, { maximumFractionDigits: 1 })} s`);
    const stoppers = (log.stopped ?? []).map((id) => (this.#board.layout.boxes.some((x) => x.tile === id) ? this.#nameOf(id, 'test') : id));
    // a Dry Run says so, and that it wrote nothing (M6)
    const ran = `${log.dry === true ? 'Dry run: ran' : 'Ran'} ${log.boxes.length} ${log.boxes.length === 1 ? 'box' : 'boxes'} in ${seconds(log.ms)}`;
    head.append(el('span', 'dc-flow-log-title', ran
      + (refused > 0 ? `, ${refused} refused` : '') + (failing > 0 ? `, ${failing} ${failing === 1 ? 'test' : 'tests'} failing` : '')
      + (lacking > 0 ? `, ${lacking} ${lacking === 1 ? 'Cross Tab' : 'Cross Tabs'} missing values` : '')
      + (stoppers.length > 0 ? `; stopped by ${stoppers.join(', ')}: nothing ${log.dry === true ? 'would be ' : ''}written`
        : log.dry === true ? '; nothing written' : '')));
    const close = closeMark(el('button', 'dc-flow-log-close') as HTMLButtonElement);
    close.type = 'button';
    close.setAttribute('aria-label', 'Close the run log');
    close.addEventListener('click', () => {
      this.#logClosed = log;
      this.#paintLog();
    });
    head.append(close);
    const list = el('ol', 'dc-flow-log-list');
    for (const b of log.boxes) {
      const tested = tests.get(b.id);
      const item = el('li', `dc-flow-log-item${b.refused !== undefined ? ' dc-flow-log-refused' : ''}${tested !== undefined && tested.failing.length > 0 ? ' dc-flow-log-failing' : ''}`);
      const go = el('button', 'dc-flow-log-box') as HTMLButtonElement;
      go.type = 'button';
      const name = this.#board.layout.boxes.some((x) => x.tile === b.id) ? this.#nameOf(b.id, this.#said(b.id)?.tool ?? 'browse') : b.id;
      go.append(el('span', 'dc-flow-log-name', name),
        el('span', 'dc-flow-log-rows', b.refused ?? (tested !== undefined ? FlowCanvas.#testWords(tested)
          : `${b.rows === undefined ? '' : `${b.rows.toLocaleString(UI_LOCALE)} ${b.rows === 1 ? 'row' : 'rows'}`}${b.wrote !== undefined ? `, wrote ${b.wrote}` : ''}${b.wouldWrite !== undefined ? `, would write ${b.wouldWrite}` : ''}`)),
        el('span', 'dc-flow-log-ms', seconds(b.ms)));
      go.title = b.refused ?? (tested !== undefined && tested.failing.length > 0 ? tested.failing.join('\n') : `Show ${name}`);
      go.addEventListener('click', () => {
        if (this.#board.layout.boxes.some((x) => x.tile === b.id)) this.#select(b.id);
      });
      item.append(go);
      list.append(item);
    }
    this.#logPanel.replaceChildren(head, list);
  }

  /** A box's place to be made at a point of the window: its middle there, in the canvas's own units. */
  #pointAt(ev: { readonly clientX: number; readonly clientY: number }): FlowPoint {
    const r = this.#plane.getBoundingClientRect();
    const x = (ev.clientX - r.left) / this.#zoom - BOX_WIDTH / 2;
    const y = (ev.clientY - r.top) / this.#zoom - BOX_HEIGHT / 2;
    return { x: Math.max(0, Math.round(x)), y: Math.max(0, Math.round(y)) };
  }

  /** The box under a point of the window, if one is. */
  #boxAt(x: number, y: number): string | undefined {
    for (const [id, drawn] of this.#boxes) {
      const r = drawn.root.getBoundingClientRect();
      if (x >= r.left && x <= r.right && y >= r.top && y <= r.bottom) return id;
    }
    return undefined;
  }

  #onCanvas(x: number, y: number): boolean {
    const r = this.#canvas.getBoundingClientRect();
    return x >= r.left && x <= r.right && y >= r.top && y <= r.bottom;
  }

  #mark(id: string | undefined): void {
    if (this.#target === id) return;
    if (this.#target !== undefined) this.#boxes.get(this.#target)?.root.classList.remove('dc-flow-target');
    this.#target = id;
    if (id !== undefined) this.#boxes.get(id)?.root.classList.add('dc-flow-target');
  }

  // -- the boxes ----------------------------------------------------------------------------------------------------

  #make(id: string): Drawn {
    const doc = this.#doc;
    const root = doc.createElement('div');
    root.className = 'dc-flow-box';
    root.dataset['tile'] = id;
    root.tabIndex = 0;
    root.setAttribute('role', 'button');
    const port = (cls: string, label?: string): HTMLElement => {
      const p = doc.createElement('span');
      p.className = `dc-flow-port ${cls}`;
      if (label !== undefined) p.textContent = label;
      return p;
    };
    // its inputs: one, or a join's left and right (CSS shows the ones its tool has)
    const into = port('dc-flow-port-in');
    const left = port('dc-flow-port-in dc-flow-port-left', 'L');
    const right = port('dc-flow-port-in dc-flow-port-right', 'R');
    // its output: pressed and dragged onto another box, a line linking them -- a Filter's True, and its False below it
    const out = port('dc-flow-port-out', 'T');
    out.title = 'Drag onto another box to link it here';
    out.addEventListener('pointerdown', (e) => this.#startWire(id, out, e));
    const outFalse = port('dc-flow-port-out dc-flow-port-false', 'F');
    outFalse.title = 'Its False: the rows its condition leaves out. Drag onto another box to link it here';
    outFalse.addEventListener('pointerdown', (e) => this.#startWire(id, outFalse, e, 'false'));
    // a Join's Left and Right unmatched, above and below its J (Alteryx's L, J and R)
    const outLeft = port('dc-flow-port-out dc-flow-port-unmatched dc-flow-port-l', 'L');
    outLeft.title = 'Its Left unmatched: the rows of its Left no row of its Right pairs with. Drag onto another box to link it here';
    outLeft.addEventListener('pointerdown', (e) => this.#startWire(id, outLeft, e, 'left'));
    const outRight = port('dc-flow-port-out dc-flow-port-unmatched dc-flow-port-r', 'R');
    outRight.title = 'Its Right unmatched: the rows of its Right no row of its Left pairs with. Drag onto another box to link it here';
    outRight.addEventListener('pointerdown', (e) => this.#startWire(id, outRight, e, 'right'));
    const glyph = doc.createElement('span');
    glyph.className = 'dc-flow-icon';
    const text = doc.createElement('div');
    text.className = 'dc-flow-text';
    const name = doc.createElement('div');
    name.className = 'dc-flow-name';
    const says = doc.createElement('div');
    says.className = 'dc-flow-says';
    text.append(name, says);
    const count = doc.createElement('span');
    count.className = 'dc-flow-count';
    const remove = doc.createElement('button');
    remove.type = 'button';
    remove.className = 'dc-flow-x';
    closeMark(remove, 'Remove the box');
    remove.addEventListener('pointerdown', (e) => e.stopPropagation());
    remove.addEventListener('click', (e) => {
      e.stopPropagation();
      this.#board.options.onRemove?.(id);
    });
    root.append(into, left, right, glyph, text, count, remove, out, outFalse, outLeft, outRight);
    const drawn: Drawn = { root, icon: glyph, name, says, count, remove, key: '', renaming: false };
    root.addEventListener('pointerdown', (e) => this.#startBox(id, drawn, e));
    root.addEventListener('keydown', (e) => this.#onBoxKey(id, drawn, e));
    // right-clicked: what can be done to it, each the gesture it is besides (a click, F2, Ctrl+D, Delete)
    root.addEventListener('contextmenu', (e) => {
      if (drawn.renaming) return;
      e.preventDefault();
      this.#boxMenu(id, drawn, e.clientX, e.clientY);
    });
    name.addEventListener('dblclick', (e) => {
      e.stopPropagation();
      this.#rename(id, drawn);
    });
    this.#boxes.set(id, drawn);
    this.#plane.append(root);
    return drawn;
  }

  /**
   * A COMMENT AS A NOTE (M7): a card on the canvas, its words in its body (its text tile's own element: a double-click
   * edits them in place, as on a board), its head what it is moved by, its × what takes it off; Delete on it too.
   */
  #makeNote(id: string): HTMLElement {
    const doc = this.#doc;
    const root = doc.createElement('div');
    root.className = 'dc-flow-note';
    root.dataset['tile'] = id;
    root.tabIndex = 0;
    root.setAttribute('role', 'note');
    const head = doc.createElement('div');
    head.className = 'dc-flow-note-head';
    head.title = 'Drag to move the comment; double-click its words to edit them';
    const remove = doc.createElement('button');
    remove.type = 'button';
    remove.className = 'dc-flow-note-x';
    closeMark(remove, 'Remove the comment');
    remove.setAttribute('aria-label', 'Remove the comment');
    remove.addEventListener('pointerdown', (e) => e.stopPropagation());
    remove.addEventListener('click', () => this.#board.options.onRemove?.(id));
    head.append(remove);
    const body = doc.createElement('div');
    body.className = 'dc-flow-note-body';
    root.append(head, body);
    head.addEventListener('pointerdown', (e) => (root.classList.contains('dc-flow-container') ? this.#startContainer(id, root, e) : this.#startNote(id, root, e)));
    root.addEventListener('keydown', (e) => {
      if (e.target !== root || !this.#board.editing) return;
      if (this.#copied(id, e)) return;
      if (e.key === 'Delete' || e.key === 'Backspace') {
        e.preventDefault();
        this.#board.options.onRemove?.(id);
      }
    });
    this.#notes.set(id, root);
    this.#plane.append(root);
    return root;
  }

  /** The boxes folded containers hide, each by the container's box (its place and frame) that stands for it. */
  #foldedBoxes(flow: Flow): Map<string, FlowBox> {
    const hidden = new Map<string, FlowBox>();
    for (const c of flow.boxes) {
      if (c.frame?.collapsed !== true) continue;
      for (const t of framedBy(flow, c.tile)) if (!hidden.has(t)) hidden.set(t, c);
    }
    return hidden;
  }

  /**
   * A CONTAINER DRAWN (Alteryx's Tool Container, docs/DATACUBE_FLOW_MODE_2026_10_24.md M7): a comment with a frame -- a
   * frame behind the boxes, its head its words' first line, how many boxes it holds, its fold and its ×; dragged by its
   * head with the boxes in it, sized by its corner, folded to its head. Its element is a note's, kept by its tile.
   */
  #paintContainer(box: FlowBox, frame: Frame, words: string, flow: Flow, editing: boolean): void {
    const root = this.#notes.get(box.tile) ?? this.#makeNote(box.tile);
    root.classList.add('dc-flow-container');
    root.classList.toggle('dc-flow-folded', frame.collapsed === true);
    root.style.transform = `translate(${box.x}px, ${box.y}px)`;
    root.style.width = `${frame.w}px`;
    root.style.height = `${frame.collapsed === true ? FRAME_HEAD : frame.h}px`;
    const head = root.querySelector<HTMLElement>('.dc-flow-note-head')!;
    let title = head.querySelector<HTMLElement>('.dc-flow-frame-title');
    if (title === null) {
      const fold = this.#doc.createElement('button');
      fold.type = 'button';
      fold.className = 'dc-flow-frame-fold';
      fold.addEventListener('pointerdown', (e) => e.stopPropagation());
      fold.addEventListener('click', () => {
        const now = this.#board.layout.boxes.find((b) => b.tile === box.tile);
        if (now?.frame === undefined) return;
        const { collapsed: _c, ...open } = now.frame;
        this.#board.options.onFrame?.(box.tile, now.x, now.y, now.frame.collapsed === true ? open : { ...open, collapsed: true });
      });
      title = this.#doc.createElement('span');
      title.className = 'dc-flow-frame-title';
      const count = this.#doc.createElement('span');
      count.className = 'dc-flow-frame-count';
      head.prepend(fold, title, count);
      const size = this.#doc.createElement('span');
      size.className = 'dc-flow-frame-size';
      size.title = 'Drag to size the container';
      size.addEventListener('pointerdown', (e) => this.#startFrameSize(box.tile, root, e));
      root.append(size);
      head.addEventListener('dblclick', (e) => {
        if (e.target === head || e.target === title) this.#board.options.onEditText?.(box.tile);
      });
    }
    const fold = head.querySelector<HTMLButtonElement>('.dc-flow-frame-fold')!;
    fold.replaceChildren(chromeIcon(fold.ownerDocument, frame.collapsed === true ? 'chevronRight' : 'chevronDown'));
    fold.title = frame.collapsed === true ? 'Unfold: its boxes drawn again' : 'Fold to its head: its boxes out of sight';
    fold.setAttribute('aria-label', frame.collapsed === true ? 'Unfold the container' : 'Fold the container');
    fold.disabled = !editing;
    const name = words.trim().split('\n')[0]!.trim() || 'Container';
    title.textContent = name;
    const held = framedBy(flow, box.tile).length;
    head.querySelector<HTMLElement>('.dc-flow-frame-count')!.textContent = `${held} ${held === 1 ? 'box' : 'boxes'}`;
    head.title = editing ? 'Drag to move the container and the boxes in it; double-click to name it' : name;
    root.querySelector<HTMLElement>('.dc-flow-note-x')!.hidden = !editing;
    root.querySelector<HTMLElement>('.dc-flow-frame-size')!.hidden = !editing || frame.collapsed === true;
    root.setAttribute('aria-label', `Container ${name}: ${held} ${held === 1 ? 'box' : 'boxes'}${frame.collapsed === true ? ', folded' : ''}`);
  }

  /** A container dragged by its head: it and the boxes in it follow the pointer, their places told as one step, let go. */
  #startContainer(id: string, root: HTMLElement, e: PointerEvent): void {
    if (e.button !== 0 || !this.#board.editing) return;
    const flow = this.#board.layout;
    const at = flow.boxes.find((b) => b.tile === id);
    if (at === undefined) return;
    e.preventDefault();
    const inside = new Map(flow.boxes.filter((b) => framedBy(flow, id).includes(b.tile)).map((b) => [b.tile, b] as const));
    const start = { x: e.clientX, y: e.clientY };
    let by = { x: 0, y: 0 };
    this.#own(root, e, (ev) => {
      const dx = (ev.clientX - start.x) / this.#zoom;
      const dy = (ev.clientY - start.y) / this.#zoom;
      // the whole as one: none of it past the canvas's edge
      by = { x: Math.max(dx, -at.x, ...[...inside.values()].map((b) => -b.x)), y: Math.max(dy, -at.y, ...[...inside.values()].map((b) => -b.y)) };
      root.classList.add('dc-flow-dragging');
      root.style.transform = `translate(${at.x + by.x}px, ${at.y + by.y}px)`;
      this.#dragged = new Map([...inside].map(([t, b]) => [t, { x: b.x + by.x, y: b.y + by.y }]));
      for (const [t, p] of this.#dragged) {
        const d = this.#boxes.get(t);
        if (d) d.root.style.transform = `translate(${p.x}px, ${p.y}px)`;
      }
      this.#paintLines(this.#placed(this.#board.layout));
    }, (apply) => {
      this.#dragged = undefined;
      root.classList.remove('dc-flow-dragging');
      if (apply && (by.x !== 0 || by.y !== 0)) {
        // the one rule Python's text.place moves one by (flow.ts containerMoves)
        const moves = containerMoves(flow, id, at.x + by.x, at.y + by.y).map((m) => ({ id: m.tile, x: m.x, y: m.y }));
        if (moves.length === 1) this.#board.options.onMove?.(id, moves[0]!.x, moves[0]!.y);
        else this.#board.options.onMoveMany?.(moves, 'Move Container');
      }
      this.paint();
    });
  }

  /** A container sized by its corner: no smaller than FRAME_MIN, told once, let go. */
  #startFrameSize(id: string, root: HTMLElement, e: PointerEvent): void {
    if (e.button !== 0 || !this.#board.editing) return;
    const at = this.#board.layout.boxes.find((b) => b.tile === id);
    const frame = at?.frame;
    if (at === undefined || frame === undefined) return;
    e.preventDefault();
    e.stopPropagation();
    const start = { x: e.clientX, y: e.clientY };
    let size = { w: frame.w, h: frame.h };
    this.#own(root, e, (ev) => {
      size = {
        w: Math.max(FRAME_MIN.w, Math.round(frame.w + (ev.clientX - start.x) / this.#zoom)),
        h: Math.max(FRAME_MIN.h, Math.round(frame.h + (ev.clientY - start.y) / this.#zoom)),
      };
      root.style.width = `${size.w}px`;
      root.style.height = `${size.h}px`;
    }, (apply) => {
      if (apply && (size.w !== frame.w || size.h !== frame.h)) this.#board.options.onFrame?.(id, at.x, at.y, { ...frame, ...size });
      this.paint();
    });
  }

  /** Ctrl+G: a container round the boxes picked, a margin about them and room for its head. */
  #wrapPicked(): void {
    const boxes = this.#board.layout.boxes.filter((b) => this.#picked.has(b.tile));
    if (boxes.length === 0) return;
    const pad = 20;
    const x = Math.max(0, Math.min(...boxes.map((b) => b.x)) - pad);
    const y = Math.max(0, Math.min(...boxes.map((b) => b.y)) - pad - FRAME_HEAD);
    const w = Math.max(FRAME_MIN.w, Math.max(...boxes.map((b) => b.x + BOX_WIDTH)) + pad - x);
    const h = Math.max(FRAME_MIN.h, Math.max(...boxes.map((b) => b.y + BOX_HEIGHT)) + pad - y);
    this.#board.options.onContainer?.({ x, y }, { w, h });
  }

  /** A note dragged by its head: it follows the pointer, and its place is told once, let go (onMove). */
  #startNote(id: string, root: HTMLElement, e: PointerEvent): void {
    if (e.button !== 0 || !this.#board.editing) return;
    const at = this.#board.layout.boxes.find((b) => b.tile === id);
    if (at === undefined) return;
    e.preventDefault();
    const start = { x: e.clientX, y: e.clientY };
    let to = { x: at.x, y: at.y };
    this.#own(root, e, (ev) => {
      to = { x: Math.max(0, at.x + (ev.clientX - start.x) / this.#zoom), y: Math.max(0, at.y + (ev.clientY - start.y) / this.#zoom) };
      root.classList.add('dc-flow-dragging');
      root.style.transform = `translate(${to.x}px, ${to.y}px)`;
    }, (apply) => {
      root.classList.remove('dc-flow-dragging');
      if (apply && (to.x !== at.x || to.y !== at.y)) this.#board.options.onMove?.(id, Math.round(to.x), Math.round(to.y));
      this.paint();
    });
  }

  /** A BOX'S MENU (a right-click): shown, its settings, renamed, duplicated, an input frozen, taken off -- each the same
   *  as its gesture elsewhere on the canvas; a locked page's, shown alone. */
  #boxMenu(id: string, drawn: Drawn, x: number, y: number): void {
    const o = this.#board.options;
    const said = this.#said(id);
    const editor = said ? TOOL_EDITORS[said.tool] : undefined;
    const editing = this.#board.editing;
    const inputs = this.#inputsOf(id);
    const live = inputs.filter((i) => 'grid' in i);
    const union = this.#isUnion(id, inputs);
    const off = this.#takesOff(id) ? inputs : [];
    const items: MenuItem[] = [
      { id: 'tile.box.show', label: 'Show its rows' },
      ...(editing ? [
        { id: 'tile.box.settings', label: 'Open its settings', ...(editor === undefined ? { disabled: true } : {}) } as const,
        { id: 'tile.box.rename', label: 'Rename (F2)' } as const,
        { id: 'tile.box.duplicate', label: 'Duplicate (Ctrl+D)' } as const,
        ...live.map((i) => ({ id: `tile.box.freeze.${i.side}` as const, label: `Freeze its ${sideWords(i.side, union)}: a copy of its rows now` })),
        ...off.map((i) => ({ id: `tile.box.takeOff.${i.side}` as const, label: `Take off its ${sideWords(i.side, union)}` })),
        { id: 'tile.box.remove', label: 'Remove (Delete)' } as const,
      ] : []),
    ];
    this.#menu ??= new MenuView(this.#doc, { onSelect: (item) => this.#menuAct?.(item) });
    this.#menuAct = (item) => {
      const act = item.id ?? '';
      if (act === 'tile.box.show') this.#select(id);
      else if (act === 'tile.box.settings' && editor !== undefined) o.onSelect?.(id, editor);
      else if (act === 'tile.box.rename') this.#rename(id, drawn);
      else if (act === 'tile.box.duplicate') o.onDuplicate?.(id);
      else if (act === 'tile.box.remove') o.onRemove?.(id);
      else if (act.startsWith('tile.box.freeze.')) o.onFreeze?.(id, act.slice('tile.box.freeze.'.length) as InputSide);
      else if (act.startsWith('tile.box.takeOff.')) o.onTakeOff?.(id, act.slice('tile.box.takeOff.'.length) as InputSide);
    };
    this.#menu.show([{ label: '', items }], x, y);
  }

  /** The columns a box hands on, as its document says them (box-columns.ts handedOn: its query's columns and calculated
   *  columns, but those it leaves out, each under the name it gives it) -- read, never run for. */
  #columnsOf(id: string): string[] {
    const doc = this.#board.options.documentOf?.(id);
    if (doc === undefined) return [];
    return handedOn([...doc.query.columns.map((c) => c.name), ...doc.query.derived.map((d) => d.name)], doc.query, doc.configuration)
      .map((c) => c.as);
  }

  /** A box shown, and its settings with it: the window of its grid's that sets what it does (TOOL_EDITORS) -- but the
   *  Join window, a window over the page, opened only when asked (its settings' button). */
  #select(id: string): void {
    this.#clicked = id;
    const said = this.#said(id);
    const editor = said ? TOOL_EDITORS[said.tool] : undefined;
    this.#board.options.onSelect?.(id, editor?.window === 'join' ? undefined : editor);
  }

  /** What a box does, as it is drawn: its document's (boxSays) -- or, added as a tool and doing nothing yet, that tool,
   *  not set yet. */
  #said(id: string): BoxSays | undefined {
    const out = this.#board.layout.boxes.find((b) => b.tile === id)?.output;
    const own = this.#ownSaid(id);
    // an Output: the file it writes, what its grid does besides after it
    if (out !== undefined && own !== undefined) {
      return { tool: 'output', also: own.tool === 'browse' || own.tool === 'output' ? own.also : [own.tool, ...own.also], says: `→ ${out.name}` };
    }
    return own;
  }

  #ownSaid(id: string): BoxSays | undefined {
    const doc = this.#board.options.documentOf?.(id);
    if (doc === undefined) return undefined;
    const said = boxSays(doc);
    const added = this.#board.addedAs(id);
    return said.tool === 'browse' && added !== undefined && added !== 'browse' ? { tool: added, also: [], says: 'not set yet' } : said;
  }

  /** What a box is called on the canvas: the name a person gave it, else its tool's (a flow never says "grid"). */
  #nameOf(id: string, tool: FlowTool): string {
    return boxNamed(id, this.#board.title(id), tool);
  }

  /**
   * A TEST BOX'S RESULT (FL8, checks.ts) for what its rows are made of now: how many checks it has and the words of those
   * its row says fail -- read off its grid's first row once it lands for that, kept after; undefined before (a Test not
   * shown since it changed, nor run).
   */
  #testOf(id: string): { readonly of: number; readonly failing: readonly string[] } | undefined {
    const doc = this.#board.options.documentOf?.(id);
    const checks = doc === undefined ? undefined : checksOf(doc.query);
    if (checks === undefined) return undefined;
    const chain = this.#board.chain(id);
    const viewed = this.#board.viewed(id);
    const had = this.#tested.get(id);
    if (had?.chain === chain && had.viewed === viewed) return had;
    // its rows landed for what it is now (its count is for its chain): read, while its grid is there
    const row = this.#board.count(id) !== undefined ? this.#board.options.firstRowOf?.(id) : undefined;
    if (row !== undefined) {
      const failing = failingChecks(checks, row).map((f) => failingWords(f, (n) => n.toLocaleString(UI_LOCALE)));
      const now = { chain, viewed, of: checks.length, failing };
      this.#tested.set(id, now);
      // the board told too: a stopping Test failing stops a run's writes (page/cube-page.ts #runFlow)
      this.#board.tested(id, failing.length > 0);
      return now;
    }
    return had?.chain === chain ? had : undefined;
  }

  /**
   * A CROSS TAB'S CHECK (FL3, cross-tab.ts): the values its rows hold that its pinned columns lack, as the database said
   * them for what it is now (the page asks as its rows land), a Test's result in shape -- its failing in words, and its
   * own few words -- undefined for a box that pins nothing, or before it was asked.
   */
  #lackingOf(id: string): { readonly of: number; readonly failing: readonly string[]; readonly says: string } | undefined {
    const q = this.#board.options.documentOf?.(id)?.query;
    const on = q?.pivotOn[0];
    if (q === undefined || on === undefined || (q.pivotValues ?? []).length === 0) return undefined;
    const values = this.#board.lacking(id);
    if (values === undefined) return undefined;
    const n = values.length;
    return { of: 1, failing: n > 0 ? [lackingWords(on, values)] : [],
      says: n === 0 ? 'every value has a column' : `${n.toLocaleString(UI_LOCALE)} ${n === 1 ? 'value has' : 'values have'} no column` };
  }

  /** A Test's result in a few words: how many of its checks fail, or that every one passes (a Cross Tab's, its own). */
  static #testWords(t: { readonly of: number; readonly failing: readonly string[]; readonly says?: string }): string {
    if (t.says !== undefined) return t.says;
    return t.failing.length > 0 ? `${t.failing.length} of ${t.of} ${t.of === 1 ? 'check fails' : 'checks fail'}`
      : `${t.of === 1 ? 'its check passes' : `all ${t.of} checks pass`}`;
  }

  #paintBox(id: string, drawn: Drawn): void {
    const said = this.#said(id);
    const tool = said?.tool ?? 'browse';
    const name = this.#nameOf(id, tool);
    const says = said === undefined ? '' : said.also.length > 0
      ? `${said.says} · ${said.also.map((t) => FLOW_TOOLS[t].name).join(' · ')}` : said.says;
    const shown = this.#board.shown === id;
    const refused = this.#board.refusal(id);
    const columns = this.#columnsOf(id);
    // a side read from a copy (FL6, a line taken off): frozen, said so
    const inputs = this.#inputsOf(id);
    const frozen = inputs.flatMap((i) => ('frozen' in i ? [i.frozen] : []));
    const counted = this.#board.count(id);
    const outs = this.#outsOf(id);
    // a Test's result, or a Cross Tab's values its columns lack (FL3): said and marked alike
    const tested = tool === 'test' ? this.#testOf(id) : this.#lackingOf(id);
    const stops = this.#board.layout.boxes.find((b) => b.tile === id)?.stops === true;
    const key = JSON.stringify([tool, name, says, shown, this.#board.editing, refused, columns, frozen, counted, inputs.length, outs, tested, stops]);
    if (key === drawn.key) return;
    drawn.key = key;
    drawn.root.dataset['tool'] = tool;
    drawn.root.dataset['group'] = FLOW_TOOLS[tool].group;
    this.#paintIns(drawn.root, tool === 'union' && inputs.length >= 2 ? inputs.map((i) => i.side) : undefined);
    // its outputs, where it has several: a Filter's T and F, a Unique's U and D, a Sample's S and R, a Join's J between
    // its L and R
    drawn.root.classList.toggle('dc-flow-two', outs === 'two');
    const [outTrue, outFalse] = drawn.root.querySelectorAll<HTMLElement>(':scope > .dc-flow-port-out');
    if (outTrue !== undefined && outFalse !== undefined) {
      const unique = tool === 'unique';
      const sample = tool === 'sample';
      const joined = outs === 'three';
      outTrue.textContent = joined ? 'J' : unique ? 'U' : sample ? 'S' : 'T';
      outTrue.title = joined ? 'Its Joined rows. Drag onto another box to link it here'
        : sample && outs === 'two' ? 'Its sampled rows. Drag onto another box to link it here' : 'Drag onto another box to link it here';
      outFalse.textContent = unique ? 'D' : sample ? 'R' : 'F';
      outFalse.title = unique ? 'Its Duplicates: every row after its key\'s first. Drag onto another box to link it here'
        : sample ? 'The rest: every row its sample leaves out (a holdout, by the same seed). Drag onto another box to link it here'
          : 'Its False: the rows its condition leaves out. Drag onto another box to link it here';
    }
    drawn.root.classList.toggle('dc-flow-shown', shown);
    drawn.root.classList.toggle('dc-flow-refused', refused !== undefined);
    drawn.root.classList.toggle('dc-flow-frozen', frozen.length > 0);
    // a Test's result, once its rows have landed: its failing checks said, the box marked
    drawn.root.classList.toggle('dc-flow-failing', tested !== undefined && tested.failing.length > 0);
    drawn.root.classList.toggle('dc-flow-passing', tested !== undefined && tested.failing.length === 0);
    // a Test that stops a run: red when it fails (FL8's refuse), amber when it only warns
    drawn.root.classList.toggle('dc-flow-stops', stops);
    drawn.root.setAttribute('aria-pressed', String(shown));
    const frozenWords = frozen.length > 0 ? `Frozen: reads a copy of what ${frozen.join(' and ')} was` : undefined;
    drawn.root.setAttribute('aria-label', `${name}: ${refused ?? says}${frozenWords !== undefined ? `. ${frozenWords}` : ''}`);
    // what it hands on: its columns, as its document says them (no query run for it)
    drawn.root.title = refused ?? [frozenWords, tested !== undefined ? [FlowCanvas.#testWords(tested), ...tested.failing].join('\n') : undefined,
      columns.length > 0 ? `${columns.length} columns out: ${columns.join(', ')}` : undefined]
      .filter((t) => t !== undefined).join('\n');
    drawn.icon.replaceChildren(icon(this.#doc, tool));
    if (!drawn.renaming) drawn.name.textContent = name;
    // a Test says its result, once known; its checks beside it
    drawn.says.textContent = tested !== undefined ? FlowCanvas.#testWords(tested) : says;
    drawn.says.title = tested !== undefined ? [says, ...tested.failing].join('\n') : says;
    // its rows when its grid ran last (Run, or shown), while its chain is as it was then
    drawn.count.hidden = counted === undefined;
    drawn.count.textContent = counted === undefined ? '' : counted.toLocaleString(UI_LOCALE);
    drawn.count.title = counted === undefined ? '' : `${counted.toLocaleString(UI_LOCALE)} ${counted === 1 ? 'row' : 'rows'} when it ran last`;
    drawn.remove.hidden = !this.#board.editing;
    drawn.remove.title = `Remove ${name}`;
    drawn.remove.setAttribute('aria-label', `Remove ${name}`);
  }

  #startBox(id: string, drawn: Drawn, e: PointerEvent): void {
    if (e.button !== 0 || drawn.renaming) return;
    const box = this.#board.layout.boxes.find((b) => b.tile === id);
    if (!box) return;
    const start = { x: e.clientX, y: e.clientY };
    let moved = false;
    // Shift (or Ctrl) and a click: the box picked with the others, or let go of -- never shown
    const adding = e.shiftKey || e.ctrlKey || e.metaKey;
    // dragged, the boxes picked with it go with it; any other box goes alone
    const group = this.#group(id);
    const from = new Map(this.#board.layout.boxes.filter((b) => group.includes(b.tile)).map((b) => [b.tile, b] as const));
    // a locked page moves nothing: a press only shows the box
    const editing = this.#board.editing;
    this.#own(drawn.root, e, (ev) => {
      // the pointer's way across the window, in the canvas's own units at its zoom
      const dx = (ev.clientX - start.x) / this.#zoom;
      const dy = (ev.clientY - start.y) / this.#zoom;
      if (!editing || (!moved && Math.hypot(dx, dy) * this.#zoom < DRAG_PX)) return;
      moved = true;
      // the group moves as one: none past the canvas's edge, so their places keep their shape
      const sx = Math.max(dx, ...[...from.values()].map((b) => -b.x));
      const sy = Math.max(dy, ...[...from.values()].map((b) => -b.y));
      this.#dragged = new Map([...from].map(([t, b]) => [t, { x: b.x + sx, y: b.y + sy }]));
      for (const [t, at] of this.#dragged) {
        const d = this.#boxes.get(t);
        d?.root.classList.add('dc-flow-dragging');
        if (d) d.root.style.transform = `translate(${at.x}px, ${at.y}px)`;
      }
      this.#paintLines(this.#placed(this.#board.layout));
    }, (apply) => {
      const at = this.#dragged;
      this.#dragged = undefined;
      for (const t of from.keys()) this.#boxes.get(t)?.root.classList.remove('dc-flow-dragging');
      if (!moved) {
        if (!apply) return;
        if (adding) {
          if (this.#picked.has(id)) this.#picked.delete(id);
          else this.#picked.add(id);
          this.#pick([...this.#picked]);
          return;
        }
        this.#pick([id]);
        this.#select(id);
        return;
      }
      if (apply && at !== undefined) {
        const moves = [...at].map(([t, p]) => ({ id: t, x: Math.round(p.x), y: Math.round(p.y) }));
        if (moves.length === 1) this.#board.options.onMove?.(moves[0]!.id, moves[0]!.x, moves[0]!.y);
        else this.#board.options.onMoveMany?.(moves);
      }
      // drawn where the page has it: moved, once its command places it; else back where it was
      this.paint();
    });
  }

  #onBoxKey(id: string, drawn: Drawn, e: KeyboardEvent): void {
    if (drawn.renaming || e.target !== drawn.root) return;
    const o = this.#board.options;
    // Ctrl+Enter is the canvas's: Run (#onCanvasKey)
    if ((e.key === 'Enter' || e.key === ' ') && !(e.ctrlKey || e.metaKey)) {
      e.preventDefault();
      // as a click: picked, and shown
      this.#pick([id]);
      this.#select(id);
      return;
    }
    if (!this.#board.editing) return;
    if (e.key === 'Delete' || e.key === 'Backspace') {
      e.preventDefault();
      const group = this.#group(id);
      if (group.length > 1) o.onRemoveMany?.(group);
      else o.onRemove?.(id);
      return;
    }
    if (e.key === 'F2') {
      e.preventDefault();
      this.#rename(id, drawn);
      return;
    }
    if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 'd') {
      e.preventDefault();
      o.onDuplicate?.(id);
      return;
    }
    if (this.#copied(id, e)) return;
    const step = e.shiftKey ? BIG_STEP : STEP;
    const by: Readonly<Record<string, readonly [number, number]>> = {
      ArrowLeft: [-step, 0], ArrowRight: [step, 0], ArrowUp: [0, -step], ArrowDown: [0, step],
    };
    const move = by[e.key];
    const group = this.#group(id);
    const boxes = this.#board.layout.boxes.filter((b) => group.includes(b.tile));
    if (move === undefined || boxes.length === 0) return;
    e.preventDefault();
    // the group as one: none past the canvas's edge
    const mx = Math.max(move[0], ...boxes.map((b) => -b.x));
    const my = Math.max(move[1], ...boxes.map((b) => -b.y));
    if (boxes.length === 1) o.onMove?.(id, boxes[0]!.x + mx, boxes[0]!.y + my);
    else o.onMoveMany?.(boxes.map((b) => ({ id: b.tile, x: b.x + mx, y: b.y + my })));
  }

  #onCanvasKey(e: KeyboardEvent): void {
    // a line selected: Delete takes it off, Escape lets it go
    if (this.#line !== undefined && (e.key === 'Delete' || e.key === 'Backspace')) {
      e.preventDefault();
      this.#takeOffLine();
      return;
    }
    if (this.#line !== undefined && e.key === 'Escape') {
      this.#selectLine(undefined);
      return;
    }
    const target = e.target as HTMLElement | null;
    // a field (a box being renamed, a note's words) keeps its own keys, its own undo and its own clipboard
    if (target?.tagName === 'INPUT' || target?.tagName === 'TEXTAREA') return;
    // the boxes picked together, from the canvas itself (a band dragged round them leaves its focus there)
    if (target === this.#canvas && this.#picked.size > 0) {
      if (e.key === 'Escape') {
        this.#pick([]);
        return;
      }
      if ((e.key === 'Delete' || e.key === 'Backspace') && this.#board.editing) {
        e.preventDefault();
        const picked = [...this.#picked];
        if (picked.length === 1) this.#board.options.onRemove?.(picked[0]!);
        else this.#board.options.onRemoveMany?.(picked);
        return;
      }
      if (this.#copied([...this.#picked][0]!, e)) return;
    }
    if (!(e.ctrlKey || e.metaKey)) return;
    // the command palette
    if (e.key.toLowerCase() === 'k') {
      e.preventDefault();
      this.#openCommands();
      return;
    }
    // Run, from the keys: as its button, while no run is going; with Shift, a Dry Run
    if (e.key === 'Enter') {
      e.preventDefault();
      if (!this.#run.disabled) this.#board.options.onRun?.(e.shiftKey);
      return;
    }
    const key = e.key.toLowerCase();
    if (key === 'a' && target === this.#canvas) {
      e.preventDefault();
      this.#pick([...this.#boxes].filter(([, d]) => !d.root.hidden).map(([id]) => id));
      return;
    }
    if (key === 'g' && this.#board.editing && this.#picked.size > 0) {
      e.preventDefault();
      this.#wrapPicked();
      return;
    }
    if (key === 'v' && this.#board.editing && this.#board.options.onPaste) {
      e.preventDefault();
      this.#board.options.onPaste(this.#pointer !== undefined ? this.#pointAt({ clientX: this.#pointer.x, clientY: this.#pointer.y }) : undefined);
      return;
    }
    if (key !== 'z') return;
    e.preventDefault();
    this.#board.options.onUndo?.(e.shiftKey);
  }

  /** The command palette opened (Ctrl+K): its field emptied, every command listed, the field to type in. */
  #openCommands(): void {
    this.#paintHistory();
    this.#commands.hidden = false;
    this.#commandFind.value = '';
    this.#commandAt = 0;
    this.#paintCommands();
    this.#commandFind.focus();
  }

  /** The command palette closed -- the canvas given its keys back, unless the focus went elsewhere. */
  #closeCommands(focusCanvas = true): void {
    if (this.#commands.hidden) return;
    this.#commands.hidden = true;
    if (focusCanvas) this.#canvas.focus({ preventScroll: true });
  }

  /**
   * EVERY COMMAND THERE IS NOW: each toolbar button a person could press (Run, Tidy Up, Undo and Redo saying what they
   * would do, the zoom, the Map, the keys), each tool of the palette (added as its click adds it, after the box shown:
   * on a page that is edited), and each box (shown, and scrolled into view).
   */
  #allCommands(): Command[] {
    const o = this.#board.options;
    const history = this.#paintHistory();
    const buttons = this.#toolbarCommands.filter(([, b]) => !b.hidden && !b.disabled).map(([label, b]): Command => ({
      label: label === 'Undo' ? `Undo ${history.undo ?? ''}`.trim() : label === 'Redo' ? `Redo ${history.redo ?? ''}`.trim()
        : label === 'Map' ? (this.#mapOn ? 'Hide the map' : 'Show the map') : label,
      words: b.title, kind: 0, run: () => b.click(),
    }));
    const tools = this.#board.editing ? PALETTE.map((t): Command => ({
      label: `Add ${FLOW_TOOLS[t].name}`, words: FLOW_TOOLS[t].does, kind: 1, run: () => o.onTool?.(t, this.#board.shown, TOOL_EDITORS[t]),
    })) : [];
    const boxes = [...this.#boxes.keys()].map((id): Command => {
      const said = this.#said(id);
      const tool = said?.tool ?? 'browse';
      return {
        label: `Show ${this.#nameOf(id, tool)}`, words: `${FLOW_TOOLS[tool].name} ${said?.says ?? ''}`, kind: 2,
        run: () => {
          this.#select(id);
          this.reveal(id);
        },
      };
    });
    return [...buttons, ...tools, ...boxes];
  }

  /** The commands its field finds -- each word typed in its name or its words, those whose name starts with what is
   *  typed first, then those with a word that does, then the rest; among each, actions, tools, boxes -- drawn, the one
   *  Enter runs marked. */
  #paintCommands(): void {
    const typed = this.#commandFind.value.trim().toLowerCase();
    const words = typed.split(/\s+/).filter((w) => w !== '');
    const rank = (c: Command): number => {
      const label = c.label.toLowerCase();
      return label.startsWith(typed) ? 0 : label.split(/\s+/).some((w) => w.startsWith(words[0] ?? '')) ? 1 : 2;
    };
    this.#commandsFound = this.#allCommands()
      .filter((c) => words.every((w) => `${c.label} ${c.words}`.toLowerCase().includes(w)))
      .map((c, i) => ({ c, i, r: typed === '' ? 0 : rank(c) }))
      .sort((a, b) => a.r - b.r || a.c.kind - b.c.kind || a.i - b.i)
      .map(({ c }) => c);
    this.#commandAt = Math.min(this.#commandAt, Math.max(0, this.#commandsFound.length - 1));
    const doc = this.#doc;
    if (this.#commandsFound.length === 0) {
      this.#commandList.replaceChildren(Object.assign(doc.createElement('div'), { className: 'dc-flow-commands-none', textContent: 'No command has those words' }));
      return;
    }
    this.#commandList.replaceChildren(...this.#commandsFound.map((c, i) => {
      const item = doc.createElement('div');
      item.className = 'dc-flow-command';
      item.setAttribute('role', 'option');
      item.setAttribute('aria-selected', String(i === this.#commandAt));
      item.classList.toggle('dc-active', i === this.#commandAt);
      item.append(Object.assign(doc.createElement('span'), { className: 'dc-flow-command-label', textContent: c.label }),
        Object.assign(doc.createElement('span'), { className: 'dc-flow-command-words', textContent: c.words }));
      // pressed: run, before the field's focus goes (which would close it first)
      item.addEventListener('pointerdown', (e) => {
        e.preventDefault();
        this.#runCommand(c);
      });
      return item;
    }));
    this.#commandList.children[this.#commandAt]?.scrollIntoView({ block: 'nearest' });
  }

  #onCommandKey(e: KeyboardEvent): void {
    if (e.key === 'Escape') {
      e.preventDefault();
      e.stopPropagation();
      this.#closeCommands();
      return;
    }
    if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
      e.preventDefault();
      const n = this.#commandsFound.length;
      if (n === 0) return;
      this.#commandAt = (this.#commandAt + (e.key === 'ArrowDown' ? 1 : n - 1)) % n;
      this.#paintCommands();
      return;
    }
    if (e.key !== 'Enter') return;
    e.preventDefault();
    const c = this.#commandsFound[this.#commandAt];
    if (c !== undefined) this.#runCommand(c);
  }

  /** A command run: the palette closed first (its gesture may open a window of its own), then its gesture. */
  #runCommand(c: Command): void {
    this.#closeCommands();
    c.run();
  }

  /** Ctrl+C on a box or a note, or Ctrl+X (and it taken off): told, and true. */
  #copied(id: string, e: KeyboardEvent): boolean {
    const key = e.key.toLowerCase();
    if (!(e.ctrlKey || e.metaKey) || (key !== 'c' && key !== 'x')) return false;
    if (key === 'x' && !this.#board.editing) return false;
    e.preventDefault();
    // its own: the canvas's Ctrl+V is the canvas's
    e.stopPropagation();
    this.#board.options.onCopy?.(this.#group(id), key === 'x');
    return true;
  }

  /** A box renamed in place: its name a field until Enter (or leaving it) says it, or Escape keeps the old one. */
  #rename(id: string, drawn: Drawn): void {
    if (!this.#board.editing || drawn.renaming) return;
    drawn.renaming = true;
    const field = this.#doc.createElement('input');
    field.className = 'dc-flow-rename';
    const was = drawn.name.textContent ?? '';
    field.value = was;
    field.setAttribute('aria-label', 'Box name');
    drawn.name.replaceChildren(field);
    field.focus();
    field.select();
    let done = false;
    const finish = (apply: boolean): void => {
      if (done) return;
      done = true;
      drawn.renaming = false;
      drawn.key = '';
      // the name as typed, trimmed: a blank one, or the one it had, renames nothing
      const name = field.value.trim();
      this.paint();
      drawn.root.focus({ preventScroll: true });
      if (apply && name !== '' && name !== was) this.#board.options.onRename?.(id, name);
    };
    field.addEventListener('keydown', (e) => {
      e.stopPropagation();
      if (e.key === 'Enter') finish(true);
      else if (e.key === 'Escape') finish(false);
    });
    field.addEventListener('blur', () => finish(true));
    field.addEventListener('pointerdown', (e) => e.stopPropagation());
  }

  // -- the lines ----------------------------------------------------------------------------------------------------

  /** Each box's inputs, from its document (flow-boxes.ts boxInputs): none for one with no document. */
  #inputsOf(id: string): BoxInput[] {
    const doc = this.#board.options.documentOf?.(id);
    return doc === undefined ? [] : boxInputs(doc);
  }

  /** The box a line comes out of, and its port. */
  #fromOf(line: LineEnd): { readonly grid: string; readonly port?: InputPort } | undefined {
    const i = this.#inputsOf(line.tile).find((x) => x.side === line.side);
    return i !== undefined && 'grid' in i ? { grid: i.grid, ...(i.port !== undefined ? { port: i.port } : {}) } : undefined;
  }

  /** Whether a box is a Union (`inputs`, its own): an append, of two inputs or more. */
  #isUnion(id: string, inputs: readonly BoxInput[] = this.#inputsOf(id)): boolean {
    return inputs.length >= 2 && this.#said(id)?.tool === 'union';
  }

  /** Whether a line into box `id` taken off takes its input off too: a Union's of more than two (any other's freezes). */
  #takesOff(id: string): boolean {
    const inputs = this.#inputsOf(id);
    return inputs.length > 2 && this.#isUnion(id, inputs);
  }

  /** Where box `id`'s input on `side` is on its left edge, from its top (portShare): a Union's, each in turn above its +,
   *  where a side it has not yet goes in. */
  #inY(id: string, side: InputSide): number {
    const inputs = this.#inputsOf(id);
    const i = inputs.findIndex((x) => x.side === side);
    if (this.#isUnion(id, inputs)) return BOX_HEIGHT * portShare(i < 0 ? inputs.length : i, inputs.length + 1);
    return BOX_HEIGHT * portShare(Math.max(0, i), inputs.length);
  }

  /** A box's input ports drawn: a Union's numbered (`union`, its sides), one a side, and its + below them, where a line
   *  let go adds one; any other's as made -- one, or a join's L and R. */
  #paintIns(root: HTMLElement, union: readonly InputSide[] | undefined): void {
    for (const p of root.querySelectorAll(':scope > .dc-flow-port-more, :scope > .dc-flow-port-add')) p.remove();
    const left = root.querySelector<HTMLElement>(':scope > .dc-flow-port-left')!;
    const right = root.querySelector<HTMLElement>(':scope > .dc-flow-port-right')!;
    root.classList.toggle('dc-flow-many', union !== undefined && union.length >= 3);
    if (union === undefined) {
      left.textContent = 'L';
      right.textContent = 'R';
      left.style.top = right.style.top = '';
      return;
    }
    const at = (i: number): string => `${portShare(i, union.length + 1) * 100}%`;
    const port = (cls: string, label: string, i: number): HTMLElement => {
      const p = this.#doc.createElement('span');
      p.className = `dc-flow-port dc-flow-port-in ${cls}`;
      p.textContent = label;
      p.style.top = at(i);
      return p;
    };
    left.textContent = '1';
    left.style.top = at(0);
    right.textContent = '2';
    right.style.top = at(1);
    const add = port('dc-flow-port-add', '+', union.length);
    add.title = 'Let a line go on this Union to add an input';
    right.after(...union.slice(2).map((_, i) => port('dc-flow-port-more', String(i + 3), i + 2)), add);
  }

  /** How a box's outputs are drawn: a Filter's True and False (a Date Filter's, a Custom Filter's), a Unique's U and D
   *  (its filter's, turned round); a Join's
   *  (a Fuzzy Match's too) L, J and R; any other box's one. */
  #outsOf(id: string): Outs {
    const said = this.#said(id);
    const tool = said?.tool;
    if (tool === 'filter' || tool === 'dateFilter' || tool === 'customFilter' || tool === 'unique') return 'two';
    // a Sample a filter makes (at random, one of every N, at some places, the first N by row): its rows, and the rest --
    // its False, the filter turned round (Alteryx's Create Samples); a Sample of the first N by an order (its topN), one
    if (tool === 'sample' && said?.also.length === 0) {
      const doc = this.#board.options.documentOf?.(id);
      if (doc !== undefined && rowSampleOf(doc.query) !== undefined) return 'two';
    }
    return tool === 'join' || tool === 'fuzzyMatch' ? 'three' : 'one';
  }

  /** The columns a Join's L or R hands on: those of the box it reads on that side, live; none said, a copy. */
  #sideColumns(id: string, side: 'left' | 'right'): string[] {
    const input = this.#inputsOf(id).find((i) => i.side === side);
    return input !== undefined && 'grid' in input ? this.#columnsOf(input.grid) : [];
  }

  /**
   * A line into each box from each box it reads live: out of its right side, into the left of the box reading it (a
   * join's left above its right) -- each with a wider stroke beneath it that a pointer finds, and the line being drawn
   * from a box's output.
   */
  #paintLines(flow: Flow): void {
    const at = new Map(flow.boxes.map((b) => [b.tile, b]));
    const curve = (x1: number, y1: number, x2: number, y2: number): string => {
      const bend = Math.max(32, Math.abs(x2 - x1) / 2);
      return `M${x1} ${y1}C${x1 + bend} ${y1} ${x2 - bend} ${y2} ${x2} ${y2}`;
    };
    const lines: { readonly end: LineEnd; readonly d: string; readonly mid: { x: number; y: number }; readonly says: string }[] = [];
    // a box in a folded container: its lines to its container's head, its right edge out and its left in; a line inside
    // one container, folded, not drawn
    const foldedIn = this.#foldedBoxes(flow);
    for (const box of flow.boxes) {
      for (const input of this.#inputsOf(box.tile)) {
        if (!('grid' in input)) continue;
        const a = at.get(input.grid);
        if (!a) continue;
        const from = foldedIn.get(input.grid);
        const into = foldedIn.get(box.tile);
        if (from !== undefined && from === into) continue;
        const x1 = from !== undefined ? from.x + from.frame!.w : a.x + BOX_WIDTH;
        const y1 = from !== undefined ? from.y + FRAME_HEAD / 2 : a.y + outY(this.#outsOf(input.grid), input.port);
        const x2 = into !== undefined ? into.x : box.x;
        const y2 = into !== undefined ? into.y + FRAME_HEAD / 2 : box.y + this.#inY(box.tile, input.side);
        lines.push({
          end: { tile: box.tile, side: input.side }, d: curve(x1, y1, x2, y2), mid: { x: (x1 + x2) / 2, y: (y1 + y2) / 2 },
          says: this.#lineWords(input.grid, input.port, box.tile),
        });
      }
    }
    const w = this.#wire;
    const wire = w === undefined ? undefined : (() => {
      const a = at.get(w.from);
      return a === undefined ? undefined : curve(a.x + BOX_WIDTH, a.y + outY(this.#outsOf(w.from), w.port), w.x, w.y);
    })();
    // a line selected or marked that is gone (its box taken off, linked elsewhere) is let go
    const keys = new Set(lines.map((l) => lineKey(l.end)));
    if (this.#line !== undefined && !keys.has(lineKey(this.#line))) this.#line = undefined;
    const selected = this.#line === undefined ? undefined : lineKey(this.#line);
    const target = this.#lineTarget === undefined ? undefined : lineKey(this.#lineTarget);
    const key = JSON.stringify([lines.map((l) => [lineKey(l.end), l.d, l.says]), wire, selected, target, this.#board.editing]);
    if (this.#lines.dataset['key'] !== key) {
      this.#lines.dataset['key'] = key;
      const path = (d: string, cls: string): SVGPathElement => {
        const p = this.#doc.createElementNS(SVG, 'path');
        p.setAttribute('d', d);
        p.setAttribute('class', cls);
        return p;
      };
      this.#lines.replaceChildren(...lines.flatMap((l) => {
        const k = lineKey(l.end);
        const state = k === selected ? ' dc-flow-line-selected' : k === target ? ' dc-flow-line-target' : '';
        const hit = path(l.d, 'dc-flow-hit');
        hit.dataset['tile'] = l.end.tile;
        hit.dataset['side'] = l.end.side;
        // what goes along it, said where the pointer rests on it (Alteryx's connection's metadata)
        const title = this.#doc.createElementNS(SVG, 'title');
        title.textContent = l.says;
        hit.append(title);
        if (this.#board.editing) hit.addEventListener('pointerdown', (e) => this.#pressLine(l.end, e));
        return [hit, path(l.d, `dc-flow-line${state}`)];
      }), ...(wire === undefined ? [] : [path(wire, 'dc-flow-line dc-flow-wire')]));
    }
    // the selected line's ×, at its middle
    const mid = selected === undefined ? undefined : lines.find((l) => lineKey(l.end) === selected)?.mid;
    this.#unlink.hidden = mid === undefined || !this.#board.editing;
    if (mid !== undefined) this.#unlink.style.transform = `translate(${mid.x - 9}px, ${mid.y - 9}px)`;
  }

  /** What goes along a line from `from` (its port: a Filter's False, a Unique's Duplicates, a Join's L or R) into `into`:
   *  its rows when they were counted, and its columns, as `from`'s document says them (a Join's L or R, its side's). */
  #lineWords(from: string, port: InputPort | undefined, into: string): string {
    const tool = this.#said(from)?.tool ?? 'browse';
    const out = port === undefined ? '' : ` (${portWord(tool, port)})`;
    const columns = port === 'left' || port === 'right' ? this.#sideColumns(from, port) : this.#columnsOf(from);
    const rows = port === undefined ? this.#board.count(from) : undefined;
    return [
      `${this.#nameOf(from, tool)}${out} → ${this.#nameOf(into, this.#said(into)?.tool ?? 'browse')}`,
      ...(rows !== undefined ? [`${rows.toLocaleString(UI_LOCALE)} ${rows === 1 ? 'row' : 'rows'}`] : []),
      ...(columns.length > 0 ? [`${columns.length} ${columns.length === 1 ? 'column' : 'columns'}: ${columns.join(', ')}`] : []),
    ].join('\n');
  }

  /** A line pressed: selected (its × shown, Delete takes it off). */
  #pressLine(end: LineEnd, e: PointerEvent): void {
    if (e.button !== 0) return;
    e.preventDefault();
    e.stopPropagation();
    this.#canvas.focus({ preventScroll: true });
    this.#selectLine(end);
  }

  #selectLine(end: LineEnd | undefined): void {
    if ((end === undefined ? undefined : lineKey(end)) === (this.#line === undefined ? undefined : lineKey(this.#line))) return;
    this.#line = end;
    this.#paintLines(this.#placed(this.#board.layout));
  }

  /** The selected line taken off: the box it goes into frozen on that side (onFreeze). */
  #takeOffLine(): void {
    const line = this.#line;
    if (line === undefined || !this.#board.editing) return;
    this.#line = undefined;
    this.#paintLines(this.#placed(this.#board.layout));
    // a Union of more than two: that input taken off, those after it moved up; any other's, frozen
    if (this.#takesOff(line.tile)) this.#board.options.onTakeOff?.(line.tile, line.side);
    else this.#board.options.onFreeze?.(line.tile, line.side);
  }

  /** The line under a point of the window, if one is (its wider stroke found). */
  #lineAt(x: number, y: number): LineEnd | undefined {
    for (const hit of this.#doc.elementsFromPoint(x, y)) {
      if (!(hit instanceof SVGPathElement) || !hit.classList.contains('dc-flow-hit')) continue;
      const tile = hit.dataset['tile'];
      const side = hit.dataset['side'] as InputSide | undefined;
      if (tile !== undefined && side !== undefined) return { tile, side };
    }
    return undefined;
  }

  #markLine(end: LineEnd | undefined): void {
    if ((end === undefined ? undefined : lineKey(end)) === (this.#lineTarget === undefined ? undefined : lineKey(this.#lineTarget))) return;
    this.#lineTarget = end;
    this.#paintLines(this.#placed(this.#board.layout));
  }

  /** Where a line let go at a point of the window goes in: the box under it that reads another's rows -- a join's left
   *  or right by the half it is let go on; a Union's first side read from a copy, else one input more (Alteryx's Union
   *  takes any number) -- never `from` itself, nor a box that reads `from` already on that side. */
  #endAt(from: string, x: number, y: number, port?: InputPort): LineEnd | undefined {
    const tile = this.#boxAt(x, y);
    if (tile === undefined || tile === from) return undefined;
    const inputs = this.#inputsOf(tile);
    if (inputs.length === 0) return undefined;
    if (this.#isUnion(tile, inputs)) {
      const frozen = inputs.find((i) => 'frozen' in i);
      return { tile, side: frozen?.side ?? (`more${inputs.length - 1}` as InputSide) };
    }
    const r = this.#boxes.get(tile)!.root.getBoundingClientRect();
    const side: InputSide = inputs.length === 1 ? inputs[0]!.side : (y - r.top < r.height / 2 ? 'left' : 'right');
    const now = inputs.find((i) => i.side === side);
    if (now !== undefined && 'grid' in now && now.grid === from && now.port === port) return undefined;
    return { tile, side };
  }

  /** A line drawn from box `from`'s output (`which`: its False): it follows the pointer, and let go on a box that reads
   *  another's rows, links it (onConnect); let go on the canvas, a box of its rows there (a Browse). */
  #startWire(from: string, port: HTMLElement, e: PointerEvent, which?: InputPort): void {
    if (e.button !== 0 || !this.#board.editing) return;
    e.preventDefault();
    e.stopPropagation();
    this.#selectLine(undefined);
    const plane = (ev: PointerEvent): { x: number; y: number } => {
      const r = this.#plane.getBoundingClientRect();
      return { x: (ev.clientX - r.left) / this.#zoom, y: (ev.clientY - r.top) / this.#zoom };
    };
    this.#wire = { from, ...(which !== undefined ? { port: which } : {}), ...plane(e) };
    this.#host.classList.add('dc-flow-wiring');
    this.#own(port, e, (ev) => {
      const w = this.#wire;
      if (w === undefined) return;
      const to = this.#endAt(from, ev.clientX, ev.clientY, which);
      this.#mark(to?.tile);
      // drawn to the port it would go into, else to the pointer
      const box = to === undefined ? undefined : this.#placed(this.#board.layout).boxes.find((b) => b.tile === to.tile);
      Object.assign(w, box !== undefined && to !== undefined ? { x: box.x, y: box.y + this.#inY(to.tile, to.side), to } : { ...plane(ev), to: undefined });
      this.#paintLines(this.#placed(this.#board.layout));
    }, (apply, ev) => {
      const to = this.#wire?.to;
      this.#wire = undefined;
      this.#host.classList.remove('dc-flow-wiring');
      this.#mark(undefined);
      this.#paintLines(this.#placed(this.#board.layout));
      if (!apply) return;
      if (to !== undefined) this.#board.options.onConnect?.(to.tile, to.side, from, which);
      // let go on the canvas, away from any box: a box of the rows it came out with
      else if (ev !== undefined && this.#boxAt(ev.clientX, ev.clientY) === undefined && this.#onCanvas(ev.clientX, ev.clientY)) {
        this.#board.options.onTool?.('browse', from, undefined, undefined, which, this.#pointAt(ev));
      }
    });
  }

  // -- the results pane ---------------------------------------------------------------------------------------------

  #paintResults(): void {
    const id = this.#board.shown;
    const tile = id === undefined ? undefined : this.#board.tile(id);
    if (tile === undefined) {
      const boxes = this.#board.layout.boxes.length;
      this.#empty.textContent = this.#board.noted
        ?? (boxes === 0 ? 'No rows yet: a flow starts with an Input.' : 'Click a box to see its rows here.');
      if (this.#body.firstChild !== this.#empty || this.#body.childNodes.length !== 1) this.#body.replaceChildren(this.#empty);
      this.#headTitle.textContent = 'Results';
      this.#headActions.replaceChildren();
      this.#modes.hidden = true;
      this.#body.classList.remove('dc-flow-profiling');
      this.#profileView.remove();
      this.#codeView.remove();
      this.#unchart();
      return;
    }
    this.#headTitle.textContent = this.#nameOf(id!, this.#said(id!)?.tool ?? 'browse');
    const { profileOf, chartOf, codeOf } = this.#board.options;
    this.#modes.hidden = profileOf === undefined && chartOf === undefined && codeOf === undefined;
    for (const b of this.#modes.querySelectorAll<HTMLElement>('.dc-flow-mode')) {
      const on = b.dataset['mode'] === this.#mode;
      b.classList.toggle('dc-chosen', on);
      b.setAttribute('aria-selected', String(on));
      b.hidden = (b.dataset['mode'] === 'profile' && profileOf === undefined) || (b.dataset['mode'] === 'chart' && chartOf === undefined)
        || (b.dataset['mode'] === 'code' && codeOf === undefined);
    }
    if (this.#mode !== 'code') this.#codeView.remove();
    if (this.#mode === 'code' && codeOf !== undefined) {
      // its grid kept where it is, out of sight, as the Profile keeps it; its query beside it
      this.#body.classList.add('dc-flow-profiling');
      this.#profileView.remove();
      this.#unchart();
      if (!this.#body.contains(tile.element)) this.#body.replaceChildren(tile.element);
      if (!this.#body.contains(this.#codeView)) this.#body.append(this.#codeView);
      this.#paintCode(id!, codeOf);
      this.#headActions.replaceChildren();
      return;
    }
    if (this.#mode === 'chart' && chartOf !== undefined) {
      // its grid kept where it is, out of sight, as the Profile keeps it; its chart beside it
      this.#body.classList.add('dc-flow-profiling');
      this.#profileView.remove();
      if (!this.#body.contains(tile.element)) this.#body.replaceChildren(tile.element);
      if (!this.#body.contains(this.#chartView)) this.#body.append(this.#chartView);
      this.#paintChart(id!, chartOf);
      return;
    }
    this.#unchart();
    if (this.#mode === 'profile' && this.#board.options.profileOf !== undefined) {
      // its grid kept where it is, out of sight, so its rows are not asked again when it comes back
      this.#body.classList.add('dc-flow-profiling');
      if (!this.#body.contains(tile.element)) this.#body.replaceChildren(tile.element);
      if (!this.#body.contains(this.#profileView)) this.#body.append(this.#profileView);
      this.#paintProfile(id!);
      this.#headActions.replaceChildren();
      return;
    }
    this.#body.classList.remove('dc-flow-profiling');
    this.#profileView.remove();
    // the box's grid placed once, as the band board places a tile: never moved while it is shown
    if (this.#body.firstChild !== tile.element || this.#body.childNodes.length !== 1) this.#body.replaceChildren(tile.element);
    const actions = tile.actions ?? [];
    if (actions.some((a, i) => this.#headActions.children[i] !== a) || this.#headActions.children.length !== actions.length) {
      this.#headActions.replaceChildren(...actions);
    }
  }

  /**
   * THE SHOWN BOX'S CHART (M5): its rows drawn by its grid's chart panel (the caller's: the canvas fetches no chart code),
   * a chart of what it groups by as DataCube starts one -- mounted once for what its rows are made of now, and again when
   * that changes; its Options beside the pane's title.
   */
  #paintChart(id: string, chartOf: NonNullable<FlowBoardState['options']['chartOf']>): void {
    const key = `${id}|${this.#board.chain(id)}`;
    if (this.#charted?.key !== key) {
      // another box's, or its rows made otherwise since: taken down, the view kept
      this.#charted?.chart.dispose();
      this.#charted = undefined;
      const host = this.#doc.createElement('div');
      host.className = 'dc-flow-chart-host';
      this.#chartView.replaceChildren(host);
      this.#charted = { key, chart: chartOf(id, host) };
    }
    const options = this.#doc.createElement('button');
    options.type = 'button';
    options.className = 'dc-picker-button dc-quiet dc-flow-chart-options';
    options.textContent = 'Options';
    options.title = 'Its kind, its columns and its look';
    options.addEventListener('click', () => this.#charted?.chart.toggleForm());
    this.#headActions.replaceChildren(options);
  }

  /**
   * THE SHOWN BOX'S CODE (M5; FL12, each box's Pure shown): the query its grid ran last, as the compiler prints it, and the
   * SQL the database ran -- each with its Copy -- asked again each time its grid lands a view (its query moves with it).
   */
  #paintCode(id: string, codeOf: NonNullable<FlowBoardState['options']['codeOf']>): void {
    const key = `${id}|${this.#board.viewed(id)}`;
    const view = this.#codeView;
    if (this.#coded?.key !== key) {
      this.#coded = { key };
      if (view.childElementCount === 0 || view.dataset['box'] !== id) {
        view.replaceChildren(Object.assign(this.#doc.createElement('div'), { className: 'dc-flow-profile-wait', textContent: 'Reading its query…' }));
      }
      void codeOf(id).then(
        (answer) => { if (this.#coded?.key === key) this.#coded = { key, answer }; },
        (e: unknown) => { if (this.#coded?.key === key) this.#coded = { key, refused: e instanceof Error ? e.message : String(e) }; },
      ).then(() => {
        if (this.#coded?.key === key && this.#mode === 'code' && this.#board.shown === id) this.#drawCode(id, view, this.#coded);
      });
      return;
    }
    const c = this.#coded;
    if (view.dataset['drawn'] !== key && (c.answer !== undefined || c.refused !== undefined)) this.#drawCode(id, view, c);
  }

  #drawCode(id: string, view: HTMLElement, c: Coded): void {
    const doc = this.#doc;
    view.dataset['drawn'] = c.key;
    view.dataset['box'] = id;
    if (c.answer === undefined) {
      view.replaceChildren(Object.assign(doc.createElement('div'), { className: 'dc-flow-profile-refused', textContent: c.refused ?? '' }));
      return;
    }
    const part = (title: string, text: string, what: string): HTMLElement => {
      const section = doc.createElement('section');
      section.className = 'dc-flow-code-part';
      const head = doc.createElement('div');
      head.className = 'dc-flow-code-head';
      const h = doc.createElement('h3');
      h.textContent = title;
      const copy = doc.createElement('button');
      copy.type = 'button';
      copy.className = 'dc-picker-button dc-quiet dc-flow-code-copy';
      copy.textContent = 'Copy';
      copy.title = `Its ${what} onto the clipboard`;
      const said = doc.createElement('span');
      said.className = 'dc-flow-code-said';
      said.setAttribute('role', 'status');
      copy.addEventListener('click', () => {
        void (this.#board.options.copyText?.(id, text) ?? Promise.reject(new Error('this page has no clipboard to copy to'))).then(
          () => { said.textContent = 'Copied'; },
          (e: unknown) => { said.textContent = e instanceof Error ? e.message : String(e); },
        );
      });
      head.append(h, copy, said);
      const pre = doc.createElement('pre');
      pre.className = 'dc-flow-code-text';
      pre.textContent = text;
      section.append(head, pre);
      return section;
    };
    view.replaceChildren(
      part('Pure', c.answer.pure, 'Pure'),
      part('SQL', c.answer.sql === '' ? '(no SQL: its database is not asked in SQL)' : c.answer.sql, 'SQL'),
    );
  }

  /** The chart taken down, out of the pane. */
  #unchart(): void {
    this.#charted?.chart.dispose();
    this.#charted = undefined;
    this.#chartView.replaceChildren();
    this.#chartView.remove();
  }

  /**
   * THE SHOWN BOX'S PROFILE (M5, profile.ts): each column of its rows -- its type, how many rows have a value (a bar of
   * it), its different values, its smallest and largest -- asked of the database once for what its rows are made of
   * now, and asked again when that changes.
   */
  #paintProfile(id: string): void {
    const key = `${id}|${this.#board.chain(id)}`;
    const view = this.#profileView;
    const doc = this.#doc;
    if (this.#profiled?.key !== key) {
      this.#profiled = { key };
      view.replaceChildren(Object.assign(doc.createElement('div'), { className: 'dc-flow-profile-wait', textContent: 'Profiling its columns…' }));
      void this.#board.options.profileOf?.(id).then(
        (answer) => { if (this.#profiled?.key === key) this.#profiled = { key, answer }; },
        (e: unknown) => { if (this.#profiled?.key === key) this.#profiled = { key, refused: e instanceof Error ? e.message : String(e) }; },
      ).then(() => {
        if (this.#profiled?.key === key && this.#mode === 'profile' && this.#board.shown === id) this.#drawProfile(view, this.#profiled);
      });
      return;
    }
    // drawn once answered: one still being asked keeps saying so
    const p = this.#profiled;
    if (view.dataset['drawn'] !== key && (p.answer !== undefined || p.refused !== undefined)) this.#drawProfile(view, p);
  }

  #drawProfile(view: HTMLElement, p: { readonly key: string; readonly answer?: Profile; readonly refused?: string }): void {
    const doc = this.#doc;
    const el = <K extends keyof HTMLElementTagNameMap>(tag: K, cls: string, parent: Element, text?: string): HTMLElementTagNameMap[K] => {
      const e = doc.createElement(tag);
      if (cls) e.className = cls;
      if (text !== undefined) e.textContent = text;
      parent.append(e);
      return e;
    };
    view.replaceChildren();
    view.dataset['drawn'] = p.key;
    if (p.answer === undefined) {
      if (p.refused !== undefined) el('div', 'dc-flow-profile-wait dc-flow-profile-refused', view, `No profile: ${p.refused}`);
      return;
    }
    const { rows, columns } = p.answer;
    const n = (v: number): string => v.toLocaleString(UI_LOCALE);
    const said = (v: unknown): string => (v === null || v === undefined ? '—' : typeof v === 'number' ? n(v) : String(v));
    el('div', 'dc-flow-profile-sum', view, `${n(rows)} ${rows === 1 ? 'row' : 'rows'}, ${columns.length} ${columns.length === 1 ? 'column' : 'columns'}`);
    const table = el('table', 'dc-flow-profile-table', view);
    const head = el('tr', '', el('thead', '', table));
    for (const h of ['Column', 'Type', 'Filled', 'Distinct', 'Smallest', 'Largest']) el('th', '', head, h);
    const body = el('tbody', '', table);
    for (const c of columns) {
      const tr = el('tr', '', body);
      tr.dataset['column'] = c.name;
      el('td', 'dc-flow-profile-name', tr, c.name);
      el('td', 'dc-flow-profile-type', tr, c.type);
      // its share filled and its empties the database's (profile.ts), a percent as its reader writes one
      const filled = el('td', 'dc-flow-profile-filled', tr);
      const bar = el('span', 'dc-flow-profile-bar', filled);
      el('span', 'dc-flow-profile-fill', bar).style.width = `${Math.round(c.share * 100)}%`;
      el('span', 'dc-flow-profile-count', filled, `${n(c.filled)} (${Math.round(c.share * 100)}%)`);
      filled.title = `${n(c.empty)} empty`;
      el('td', 'dc-flow-profile-num', tr, n(c.distinct));
      el('td', 'dc-flow-profile-value', tr, 'smallest' in c ? said(c.smallest) : '');
      el('td', 'dc-flow-profile-value', tr, 'largest' in c ? said(c.largest) : '');
    }
    // how its number columns move together (Alteryx's Pearson Correlation): each pair's r, tinted by its sign and strength
    const corr = p.answer.correlations;
    if (corr === undefined) return;
    const section = el('section', 'dc-flow-corr', view);
    el('div', 'dc-flow-corr-sum', section, 'How its number columns move together: each pair\'s correlation, from -1 to 1');
    const matrix = el('table', 'dc-flow-corr-table', section);
    const top = el('tr', '', el('thead', '', matrix));
    el('th', '', top);
    for (const c of corr.columns) el('th', 'dc-flow-corr-head', top, c).title = c;
    const rowsOf = el('tbody', '', matrix);
    corr.r.forEach((line, i) => {
      const tr = el('tr', '', rowsOf);
      tr.dataset['column'] = corr.columns[i]!;
      el('th', 'dc-flow-corr-name', tr, corr.columns[i]!);
      line.forEach((r, j) => {
        // to two places, a negative zero none
        const shown = r === null ? undefined : Math.round(r * 100) / 100 || 0;
        const td = el('td', 'dc-flow-corr-cell', tr, shown === undefined ? '—' : shown.toLocaleString(UI_LOCALE, { minimumFractionDigits: 2, maximumFractionDigits: 2 }));
        td.dataset['r'] = r === null ? '' : String(r);
        if (r !== null && i !== j) {
          td.style.setProperty('--dc-corr', String(Math.min(1, Math.abs(r))));
          td.classList.add(r < 0 ? 'dc-flow-corr-negative' : 'dc-flow-corr-positive');
          if (Math.abs(r) >= 0.6) td.classList.add('dc-flow-corr-strong');
        }
        if (i === j) td.classList.add('dc-flow-corr-self');
        td.title = `${corr.columns[i]} and ${corr.columns[j]}: ${r === null ? 'no correlation (one does not vary)' : r.toFixed(3)}`;
      });
    });
  }

  /**
   * THE SETTINGS' INPUTS, docked window or none (M2): what the box reads on each side -- a box of the flow (none that
   * reads it: a box reading its own rows), or the copy it reads, frozen -- each changed there as a line drawn to it would.
   */
  #paintInputs(id: string | undefined): void {
    const inputs = id === undefined ? [] : this.#inputsOf(id);
    const boxes = this.#board.layout.boxes.map((b) => b.tile);
    const readers = id === undefined || inputs.length === 0 ? new Set<string>()
      : readersOf(id, boxes, (b) => this.#board.options.documentOf?.(b));
    // each box of the flow, and its other outputs besides: a Filter's False, a Join's L and R
    const offered = boxes.filter((b) => b !== id && !readers.has(b)).flatMap((b) => {
      const tool = this.#said(b)?.tool;
      const name = this.#nameOf(b, tool ?? 'browse');
      const outs = this.#outsOf(b);
      if (outs === 'one') return [{ id: b, name }];
      const ports: readonly (InputPort | undefined)[] = outs === 'two' ? [undefined, 'false'] : [undefined, 'left', 'right'];
      return ports.map((port) => ({ id: b, name: `${name} (${portWord(tool, port)})`, ...(port !== undefined ? { port } : {}) }));
    });
    const union = id !== undefined && this.#isUnion(id, inputs);
    const takesOff = id !== undefined && this.#takesOff(id);
    const key = JSON.stringify([id, inputs, offered, this.#board.editing, union]);
    if (key === this.#inputsKey) return;
    this.#inputsKey = key;
    this.#inputs.hidden = inputs.length === 0;
    const d = this.#doc;
    const option = (label: string, value: string, on: boolean): HTMLOptionElement => {
      const o = d.createElement('option');
      o.textContent = label;
      o.value = value;
      o.selected = on;
      return o;
    };
    const boxOptions = (pick: HTMLSelectElement, on: (grid: string, port: InputPort | undefined) => boolean): void => {
      for (const o of offered) {
        const port = 'port' in o ? o.port : undefined;
        pick.append(option(o.name, `grid:${o.id}${port !== undefined ? `:${port}` : ''}`, on(o.id, port)));
      }
    };
    // a box picked: linked on `side` as a line drawn to it would be
    const link = (side: InputSide, v: string): void => {
      const [grid, port] = v.slice('grid:'.length).split(':');
      this.#board.options.onConnect?.(id!, side, grid!, port === 'false' || port === 'left' || port === 'right' ? port : undefined);
    };
    const row = (side: string, pick: HTMLSelectElement, ...after: HTMLElement[]): HTMLElement => {
      const r = d.createElement('label');
      r.className = 'dc-flow-input';
      const words = d.createElement('span');
      words.className = 'dc-flow-input-side';
      words.textContent = side;
      pick.className = 'dc-select';
      pick.disabled = !this.#board.editing;
      r.append(words, pick, ...after);
      return r;
    };
    this.#inputs.replaceChildren(...inputs.map((input) => {
      const pick = d.createElement('select');
      boxOptions(pick, (grid, port) => 'grid' in input && input.grid === grid && input.port === port);
      // a copy: what it reads when frozen -- or, live, Freeze: a copy of what it reads now
      pick.append(option('frozen' in input ? `A frozen copy of ${input.frozen}` : 'Freeze: a copy of its rows now', 'frozen', 'frozen' in input));
      pick.addEventListener('change', () => {
        if (pick.value === 'frozen') this.#board.options.onFreeze?.(id!, input.side);
        else link(input.side, pick.value);
      });
      // a Union's of more than two: taken off, as its line deleted
      const off = d.createElement('button');
      off.type = 'button';
      off.className = 'dc-flow-input-off';
      closeMark(off, 'Take it off');
      off.title = `Take off its ${sideWords(input.side, union)}: the inputs after it move up`;
      off.setAttribute('aria-label', `Take off its ${sideWords(input.side, union)}`);
      off.addEventListener('click', (e) => {
        e.preventDefault();
        this.#board.options.onTakeOff?.(id!, input.side);
      });
      return row(sideWords(input.side, union), pick, ...(takesOff && this.#board.editing ? [off] : []));
    }), ...(union && this.#board.editing ? [(() => {
      // a Union's next input: a box picked, appended after its last (Alteryx's Union takes any number)
      const pick = d.createElement('select');
      pick.append(option('Add an input…', '', true));
      boxOptions(pick, () => false);
      pick.addEventListener('change', () => {
        if (pick.value !== '') link(`more${inputs.length - 1}` as InputSide, pick.value);
      });
      const r = row(`Input ${inputs.length + 1}`, pick);
      r.classList.add('dc-flow-input-add');
      return r;
    })()] : []));
  }

  /** The settings pane with nothing docked: the shown box's tool and what it is set to, and how to set it. */
  #paintSettings(): void {
    const clicked = this.#clicked !== undefined && this.#board.layout.boxes.some((b) => b.tile === this.#clicked) ? this.#clicked : undefined;
    const id = this.#board.shown ?? clicked;
    this.#paintInputs(id);
    this.#paintOutput(id);
    this.#paintStops(id);
    this.#paintPin(id);
    const said = id === undefined ? undefined : this.#said(id);
    const name = id === undefined ? undefined : this.#nameOf(id, said?.tool ?? 'browse');
    const editor = said ? TOOL_EDITORS[said.tool] : undefined;
    const columns = id === undefined ? [] : this.#columnsOf(id);
    const refused = id === undefined ? undefined : this.#board.refusal(id);
    const key = JSON.stringify([id, name, said, editor !== undefined, columns, refused, this.#board.noted]);
    if (key === this.#settingsKey) return;
    this.#settingsKey = key;
    this.#settingsTitle.textContent = name === undefined ? 'Settings' : `Settings · ${name}`;
    const d = this.#doc;
    const parts: (string | HTMLElement)[] = [];
    if (refused !== undefined) {
      const why = d.createElement('p');
      why.className = 'dc-flow-settings-refused';
      why.textContent = refused;
      parts.push(why);
    }
    if (id === undefined || said === undefined) parts.push('Click a box to set what it does.');
    else {
      const what = d.createElement('p');
      what.textContent = `${FLOW_TOOLS[said.tool].name}: ${FLOW_TOOLS[said.tool].does}.`;
      const now = d.createElement('p');
      now.className = 'dc-flow-settings-now';
      now.textContent = said.says;
      parts.push(what, now);
      if (columns.length > 0) {
        const out = d.createElement('p');
        out.className = 'dc-flow-settings-columns';
        out.textContent = `${columns.length} columns out: ${columns.join(', ')}`;
        parts.push(out);
      }
      if (editor !== undefined) {
        const open = d.createElement('button');
        open.type = 'button';
        open.className = 'dc-picker-button dc-primary';
        open.textContent = 'Open its settings';
        open.addEventListener('click', () => this.#board.options.onSelect?.(id, editor));
        parts.push(open);
      } else if (said.tool === 'browse') {
        const tip = d.createElement('p');
        tip.textContent = 'It passes its input\'s rows on: drop a tool on it to add a step after it.';
        parts.push(tip);
      }
    }
    this.#settingsEmpty.replaceChildren(...parts);
  }

  /** THE SETTINGS' MODE OF A TEST (FL8), above its window when docked: Stop a run when a check fails -- no Output
   *  written -- or (off) warn. */
  #paintStops(id: string | undefined): void {
    // a Test's, and a Cross Tab's whose columns are fixed (FL3: a value with no column of its own)
    const query = id === undefined ? undefined : this.#board.options.documentOf?.(id)?.query;
    const crossTab = query !== undefined && query.pivotOn.length > 0 && (query.pivotValues ?? []).length > 0;
    const test = id !== undefined && (this.#said(id)?.tool === 'test' || crossTab);
    const stops = test && this.#board.layout.boxes.find((b) => b.tile === id)?.stops === true;
    const key = JSON.stringify([id, test, crossTab, stops, this.#board.editing]);
    if (key === this.#stopsKey) return;
    this.#stopsKey = key;
    this.#stopsStrip.hidden = !test;
    if (!test) {
      this.#stopsStrip.replaceChildren();
      return;
    }
    const d = this.#doc;
    const label = d.createElement('label');
    label.className = 'dc-text-tool-check dc-flow-stops-check';
    const box = d.createElement('input');
    box.type = 'checkbox';
    box.checked = stops;
    box.disabled = !this.#board.editing;
    box.addEventListener('change', () => this.#board.options.onStops?.(id!, box.checked));
    label.append(box, Object.assign(d.createElement('span'), {
      textContent: crossTab ? 'Stop a run when a value has no column: no Output is written' : 'Stop a run when a check fails: no Output is written',
    }));
    this.#stopsStrip.replaceChildren(label);
  }

  /**
   * A CROSS TAB'S COLUMNS (docs/DATACUBE_FLOW_MODE_2026_10_24.md FL2): a pivoted box's columns are its values -- found
   * as it runs, so a box after it cannot read them -- or fixed by **Load values** (its values as its rows hold them now,
   * pinned: query.ts crossTabRows), named here; Load values again, or Unfix. What the last one said beside them.
   */
  #paintPin(id: string | undefined): void {
    const q = id === undefined ? undefined : this.#board.options.documentOf?.(id)?.query;
    const pivoted = q !== undefined && q.pivotOn.length > 0;
    const pinned = pivoted && (q.pivotValues ?? []).length > 0 ? q.pivotValues! : undefined;
    const said = id === undefined ? undefined : this.#pinSaid.get(id);
    // the values its rows hold that it has no column for (FL3), once the database has said them
    const lacking = pinned === undefined || id === undefined ? undefined : this.#board.lacking(id);
    const key = JSON.stringify([id, pivoted, pinned, lacking, said, this.#board.editing, this.#board.options.onPinValues !== undefined]);
    if (key === this.#pinKey) return;
    this.#pinKey = key;
    this.#pinStrip.hidden = !pivoted;
    if (!pivoted || id === undefined) {
      this.#pinStrip.replaceChildren();
      return;
    }
    const d = this.#doc;
    const words = d.createElement('span');
    words.className = 'dc-flow-pin-words';
    const shown = (pinned ?? []).slice(0, 8).map(String);
    words.textContent = pinned === undefined
      ? 'Its columns are its values, found as it runs: a box after it cannot read them until they are fixed.'
      : `Its columns: ${shown.join(', ')}${pinned.length > shown.length ? ` and ${pinned.length - shown.length} more` : ''}.`;
    const button = (text: string, cls: string, pin: boolean): HTMLButtonElement => {
      const b = d.createElement('button');
      b.type = 'button';
      b.className = `dc-picker-button dc-quiet ${cls}`;
      b.textContent = text;
      b.disabled = !this.#board.editing || this.#board.options.onPinValues === undefined;
      b.title = pin ? 'Its values as its rows hold them now, each a column of its own from now on' : 'Its values found as it runs again';
      b.addEventListener('click', () => void (async () => {
        b.disabled = true;
        const refused = await this.#board.options.onPinValues?.(id, pin);
        this.#pinSaid.set(id, refused ?? (pin ? 'Its columns are fixed: a value its rows gain has no column of its own' : 'Its columns are its values again'));
        this.#paintPin(id);
      })());
      return b;
    };
    const missing = d.createElement('span');
    missing.className = 'dc-flow-pin-lacking';
    missing.textContent = lacking !== undefined && lacking.length > 0 ? lackingWords(q.pivotOn[0]!, lacking) : '';
    const status = d.createElement('span');
    status.className = 'dc-flow-pin-said';
    status.setAttribute('role', 'status');
    status.textContent = said ?? '';
    this.#pinStrip.replaceChildren(words, button(pinned === undefined ? 'Load values' : 'Load values again', 'dc-flow-pin-load', true),
      ...(pinned !== undefined ? [button('Unfix', 'dc-flow-pin-unfix', false)] : []), missing, status);
  }

  /** THE SETTINGS' OUTPUT (M6): the CSV file an Output box writes, named here (Enter, or leaving the field); Write now
   *  writes it (its rows streamed from the database); Write no file makes it a box that writes none. */
  #paintOutput(id: string | undefined): void {
    const out = id === undefined ? undefined : this.#board.layout.boxes.find((b) => b.tile === id)?.output;
    const key = JSON.stringify([id, out, this.#board.editing]);
    if (key === this.#outputKey) return;
    this.#outputKey = key;
    this.#outputStrip.hidden = out === undefined;
    if (id === undefined || out === undefined) {
      this.#outputStrip.replaceChildren();
      return;
    }
    const d = this.#doc;
    const label = d.createElement('label');
    label.className = 'dc-flow-input';
    const words = d.createElement('span');
    words.className = 'dc-flow-input-side';
    words.textContent = 'Writes';
    const name = d.createElement('input');
    name.className = 'dc-picker-input dc-flow-output-name';
    name.value = out.name;
    name.disabled = !this.#board.editing;
    name.setAttribute('aria-label', 'The CSV file it writes');
    const said = d.createElement('span');
    said.className = 'dc-flow-output-said';
    said.setAttribute('role', 'status');
    const tell = (): void => {
      const now = name.value.trim();
      if (now === out.name) return;
      if (!now.toLowerCase().endsWith('.csv')) {
        said.textContent = 'A CSV file\'s name ends .csv';
        return;
      }
      this.#board.options.onOutput?.(id, { format: 'csv', name: now });
    };
    name.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') tell();
      if (e.key === 'Escape') name.value = out.name;
    });
    name.addEventListener('change', tell);
    label.append(words, name);
    const write = d.createElement('button');
    write.type = 'button';
    write.className = 'dc-picker-button dc-primary dc-flow-output-write';
    write.textContent = 'Write now';
    write.title = 'Its rows written to the file now -- every row, from the database; Run writes it too';
    write.addEventListener('click', () => void (async () => {
      write.disabled = true;
      said.textContent = 'Writing…';
      const refused = await (this.#board.options.onWrite?.(id) ?? Promise.resolve('this page writes no file'));
      said.textContent = refused ?? `Wrote ${out.name}`;
      write.disabled = false;
    })());
    const stop = d.createElement('button');
    stop.type = 'button';
    stop.className = 'dc-picker-button dc-quiet';
    stop.textContent = 'Write no file';
    stop.hidden = !this.#board.editing;
    stop.addEventListener('click', () => this.#board.options.onOutput?.(id, null));
    this.#outputStrip.replaceChildren(label, write, stop, said);
  }

  #startSide(e: PointerEvent): void {
    if (e.button !== 0) return;
    e.preventDefault();
    const start = e.clientX;
    const from = this.#settings.offsetWidth;
    this.#own(this.#side, e, (ev) => this.#setSide(from - (ev.clientX - start)), (apply) => {
      if (!apply) this.#setSide(from);
    });
  }

  /** The settings pane's width, kept between its least and the canvas's least beside it. */
  #setSide(width: number): void {
    const room = this.#settings.parentElement?.clientWidth ?? 0;
    const most = room > 0 ? Math.max(LEAST_SETTINGS, room - LEAST_CANVAS) : width;
    this.#host.style.setProperty('--dc-flow-settings', `${Math.round(Math.min(most, Math.max(LEAST_SETTINGS, width)))}px`);
  }

  #startSplit(e: PointerEvent): void {
    if (e.button !== 0) return;
    e.preventDefault();
    const start = e.clientY;
    const from = this.#results.offsetHeight;
    this.#own(this.#split, e, (ev) => this.#setPane(from - (ev.clientY - start)), (apply) => {
      if (!apply) this.#setPane(from);
    });
  }

  /** The results pane's height, kept between the canvas's least and the pane's. */
  #setPane(height: number): void {
    const room = this.#host.clientHeight;
    const most = room > 0 ? Math.max(LEAST_PANE, room - LEAST_PANE) : height;
    this.#paneHeight = Math.round(Math.min(most, Math.max(LEAST_PANE, height)));
    this.#host.style.setProperty('--dc-flow-results', `${this.#paneHeight}px`);
  }

  /**
   * A gesture owning its pointer to its end: `move` at each move, `finish` once -- applied when let go, undone on
   * Escape, a cancelled pointer or a lost capture.
   */
  #own(el: HTMLElement, e: PointerEvent, move: (ev: PointerEvent) => void, finish: (apply: boolean, ev?: PointerEvent) => void): void {
    this.#gesture?.(false);
    try {
      el.setPointerCapture(e.pointerId);
    } catch {
      // a pointer the element cannot capture (a test's): followed all the same
    }
    const view = this.#doc.defaultView;
    const onMove = (ev: PointerEvent): void => move(ev);
    let ended = false;
    const end = (apply: boolean, ev?: PointerEvent): void => {
      if (ended) return;
      ended = true;
      this.#gesture = undefined;
      el.removeEventListener('pointermove', onMove);
      el.removeEventListener('pointerup', onUp);
      el.removeEventListener('pointercancel', onCancel);
      el.removeEventListener('lostpointercapture', onCancel);
      view?.removeEventListener('keydown', onKey, true);
      finish(apply, ev);
    };
    const onUp = (ev: PointerEvent): void => end(true, ev);
    const onCancel = (): void => end(false);
    const onKey = (ev: KeyboardEvent): void => {
      if (ev.key !== 'Escape') return;
      ev.stopPropagation();
      end(false);
    };
    el.addEventListener('pointermove', onMove);
    el.addEventListener('pointerup', onUp);
    el.addEventListener('pointercancel', onCancel);
    el.addEventListener('lostpointercapture', onCancel);
    view?.addEventListener('keydown', onKey, true);
    this.#gesture = (apply) => end(apply);
  }
}
