// THE PAGE'S BOARD, AS BANDS (docs/DATACUBE_PAGES_DESIGN_2026_10_09.md, §3): tiles laid out by bands.ts, drawn, and
// arranged by hand.
//
// What a tile HOLDS is the caller's (a DataCube grid, a chart); where it SITS is bands.ts, a pure model this file only
// drives. Every tile is drawn at its box in one container, by position, and its element is never moved in the DOM, so
// a grid inside keeps its scroll position and a chart its canvas.
//
// SMOOTH BY CONSTRUCTION (§3.4): while a tile is dragged nothing inside any tile re-lays out -- the tile follows the
// pointer (a transform), the zone it would land in is outlined, and the layout changes once, on drop. A divider or a
// band's edge moves only the boxes it bounds, at most once a frame. A gesture owns its pointer to the end, whatever
// ends it: let go (applied), Escape, a lost capture or a cancelled pointer (undone).
//
// THE KEYBOARD: a focused tile swaps with its neighbour with the arrow keys, and grows or shrinks with Shift+arrows
// (Right wider, Left narrower, Down taller, Up shorter), each step announced.
//
// Edit and view (§3.3): in view mode, and on a narrow board (stacked, §3.2), there are no handles, dividers, edges or
// remove buttons, and nothing moves.

import {
  type Bands,
  type Box,
  type Drawn,
  type Drop,
  type Preset,
  EMPTY,
  type Toward,
  add,
  arrange,
  bandOf,
  draw,
  drop,
  dividerBeside,
  dropAt,
  evenAll,
  evenOut,
  fitted,
  neighbour,
  remove,
  resize,
  resizeBand,
  shareText,
  sharesBeside,
  snapped,
  stacked,
  tiles,
  tradeBands,
} from './bands.ts';

export interface BandTile {
  readonly id: string;
  readonly title: string;
  /** What the tile shows. Placed in the tile once, never moved in the DOM. */
  readonly element: HTMLElement;
  /** Extra buttons for the title bar (a chart's pill, a grid's source). */
  readonly actions?: readonly HTMLElement[];
  /** Whether the tile offers to be removed. */
  readonly removable?: boolean;
}

export interface BandBoardOptions {
  /** Space between tiles, in pixels. */
  readonly gap?: number;
  /** A band's least height, in pixels, on a page that scrolls. */
  readonly leastBand?: number;
  /** Below this width the page is stacked, one tile under another, and cannot be rearranged. */
  readonly narrowBelow?: number;
  /** The layout changed by the user's hand (not by the screen's width), from `before`: one step, to undo. */
  readonly onChange?: (layout: Bands, before: Bands) => void;
  /** Ctrl+Z (Shift for redo) on a focused tile's frame: the caller undoes the last arrangement. */
  readonly onUndo?: (redo: boolean) => void;
  /** The user asked to remove a tile. The caller removes it. */
  readonly onRemove?: (id: string) => void;
  /** The user renamed a tile. */
  readonly onRename?: (id: string, title: string) => void;
  /** A tile's layout button was pressed: the caller shows the layouts (ui/layout-picker.ts) by `anchor`. */
  readonly onLayout?: (id: string, anchor: HTMLElement) => void;
  /**
   * A place OUTSIDE the board a dragged tile can be let go on (a page's sheet tab, page/cube-page.ts): asked at each
   * move with the pointer's position in the window -- true, and the board outlines nothing, the caller marks it --
   * then `drop` when let go there; `leave` when the drag ends anyhow.
   */
  readonly outside?: {
    readonly over: (x: number, y: number) => boolean;
    readonly drop: (id: string, x: number, y: number) => void;
    readonly leave: () => void;
  };
}

/** A band's least height, on average, on a page that fits its window: past it the page scrolls (bands.ts draw). */
const FIT_LEAST = 160;

/** How near a quarter, a third or the half a dragged divider snaps to it, in pixels (Alt held: no snapping). */
const SNAP_PX = 8;

/** A tile's least readable width (a grid's few columns, a chart's axes): smart placement puts no more side by side. */
const READABLE_WIDTH = 360;

/** A Shift+arrow's step: a twentieth of the split (or of a screen, for a band's height). */
const KEY_STEP = 0.05;

const ARROWS: Readonly<Record<string, Toward>> = {
  ArrowLeft: 'left', ArrowRight: 'right', ArrowUp: 'up', ArrowDown: 'down',
};

interface Placed {
  readonly spec: BandTile;
  readonly root: HTMLElement;
  readonly title: HTMLElement;
  /** The header's buttons: the tile's own (`spec.actions`), then the board's (layouts, maximise, remove). */
  readonly actions: HTMLElement;
  readonly remove: HTMLButtonElement;
  name: string;
}

/** A gesture in flight: how to end it. */
interface Gesture {
  readonly end: (apply: boolean) => void;
}

export class BandBoard {
  readonly #host: HTMLElement;
  readonly #doc: Document;
  readonly #canvas: HTMLElement;
  readonly #zone: HTMLElement;
  /** A divider's or an edge's sizes while it is dragged ("⅔ · ⅓", "62% · 38%"), by the pointer. */
  readonly #readout: HTMLElement;
  readonly #live: HTMLElement;
  readonly #gap: number;
  readonly #least: number;
  readonly #narrowBelow: number;
  readonly #options: BandBoardOptions;
  readonly #tiles = new Map<string, Placed>();
  readonly #observer: ResizeObserver | undefined;
  /** The layout as saved. */
  #layout: Bands = EMPTY;
  /** What a gesture or a preview proposes, drawn instead of the saved layout while it lasts. */
  #shown: Bands | null = null;
  #drawn: Drawn = draw(EMPTY, 0, 0);
  #narrow = false;
  /** The page as drawn fits its window: it is set to, and its bands were few enough to (bands.ts draw's floor). */
  #fits = false;
  #editing = true;
  #maximised: string | null = null;
  /** A lone tile drawn without its frame, its own header buttons in another place (`setAlone`). */
  #alone: string | null = null;
  /** Where the page was scrolled when a tile was maximised: back there after. */
  #scrolled = 0;
  #gesture: Gesture | null = null;
  /** A preset previewed (a hovered thumbnail): drawn, but not arranged by hand, so no handles. */
  #previewing = false;
  /** Where the page was scrolled when a preview began: a shorter preview must not leave it scrolled elsewhere. */
  #previewScroll = 0;
  #frame = 0;
  /** The dividers and band edges drawn, by what each is: kept, and only moved, while the page's shape is the same. */
  #handles: { readonly key: string; readonly el: HTMLElement }[] = [];

  constructor(host: HTMLElement, options: BandBoardOptions = {}) {
    this.#host = host;
    this.#doc = host.ownerDocument;
    this.#options = options;
    this.#gap = options.gap ?? 8;
    this.#least = options.leastBand ?? 120;
    this.#narrowBelow = options.narrowBelow ?? 640;
    host.classList.add('dc-bands');
    this.#canvas = this.#doc.createElement('div');
    this.#canvas.className = 'dc-bands-canvas';
    this.#zone = this.#doc.createElement('div');
    this.#zone.className = 'dc-bands-zone';
    this.#zone.hidden = true;
    this.#readout = this.#doc.createElement('div');
    this.#readout.className = 'dc-bands-readout';
    this.#readout.setAttribute('aria-hidden', 'true');
    this.#readout.hidden = true;
    this.#live = this.#doc.createElement('div');
    this.#live.className = 'dc-board-live';
    this.#live.setAttribute('aria-live', 'polite');
    this.#live.setAttribute('role', 'status');
    this.#canvas.append(this.#zone, this.#readout);
    host.append(this.#canvas, this.#live);
    const Observer = this.#doc.defaultView?.ResizeObserver;
    // the board's size, re-drawn at most once a frame
    this.#observer = Observer ? new Observer(() => this.#schedule()) : undefined;
    this.#observer?.observe(host);
    this.#paint();
  }

  /** The layout as saved. */
  get layout(): Bands {
    return this.#layout;
  }

  /** How many tiles are on the board. */
  get size(): number {
    return this.#tiles.size;
  }

  /** Whether the page can be arranged now: edit mode, on a board wide enough not to be stacked. */
  get arrangeable(): boolean {
    return this.#editing && !this.#narrow;
  }

  /** A tile's title as shown (renamed or not). */
  title(id: string): string | undefined {
    return this.#tiles.get(id)?.name;
  }

  /**
   * Add a tile, placed where it is wanted (bands.ts `add`): beside `near` while its band has room for one more at a
   * readable width, else below it; with no `near`, at the bottom.
   */
  add(tile: BandTile, near?: string): void {
    if (this.#tiles.has(tile.id)) throw new Error(`a tile ${tile.id} is already on the board`);
    // a divider's or an edge's drag ends first (undone): it would end on a layout from before this tile
    this.#gesture?.end(false);
    this.#tiles.set(tile.id, this.#make(tile));
    const { width } = this.#size();
    const columns = width > 0 ? Math.max(1, Math.floor((width + this.#gap) / (READABLE_WIDTH + this.#gap))) : undefined;
    this.#commit(add(this.#layout, tile.id, near, columns));
  }

  /**
   * A LONE TILE (a page of its own with one grid; the design's §3.1): drawn without its frame or header, its own header
   * buttons (`actions`: a grid's source, pill and menu) moved into `slot`, the page's bar. `null` puts every tile back
   * in its own frame, its buttons in its header.
   */
  setAlone(id: string | null, slot: HTMLElement | null): void {
    const next = id !== null && slot !== null && this.#tiles.has(id) ? id : null;
    const was = this.#alone !== null ? this.#tiles.get(this.#alone) : undefined;
    if (was && this.#alone !== next) was.actions.prepend(...(was.spec.actions ?? []));
    this.#alone = next;
    const now = next !== null ? this.#tiles.get(next) : undefined;
    if (now && slot) slot.replaceChildren(...(now.spec.actions ?? []));
    else slot?.replaceChildren();
    this.#host.classList.toggle('dc-bands-alone', next !== null);
    for (const [tile, p] of this.#tiles) p.root.classList.toggle('dc-band-tile-alone', tile === next);
    this.#paint();
  }

  /** The tile drawn alone, if any. */
  get alone(): string | null {
    return this.#alone;
  }

  /** Take a tile off the board, its neighbours closing over its place. Its element is detached, not destroyed. */
  remove(id: string): void {
    const placed = this.#tiles.get(id);
    if (!placed) return;
    if (this.#alone === id) {
      // its buttons go back to it, and go with it
      placed.actions.prepend(...(placed.spec.actions ?? []));
      this.#alone = null;
      this.#host.classList.remove('dc-bands-alone');
    }
    this.#gesture?.end(false);
    placed.root.remove();
    this.#tiles.delete(id);
    if (this.#maximised === id) this.#maximised = null;
    this.#commit(remove(this.#layout, id));
  }

  /** Put a whole layout (a saved page): tiles not on the board are left out, tiles not in it go below. */
  setLayout(layout: Bands): void {
    this.#gesture?.end(false);
    let next: Bands = layout;
    for (const id of tiles(layout)) if (!this.#tiles.has(id)) next = remove(next, id);
    for (const id of this.#tiles.keys()) if (!tiles(next).includes(id)) next = add(next, id);
    this.#commit(next);
  }

  /** Arrange every tile on the page as a preset, `first` in its first slot; the others in reading order. */
  arrange(preset: Preset, first?: string): void {
    this.#gesture?.end(false);
    this.#change(arrange(this.#layout, preset, this.#order(first)), 'Arranged.');
  }

  /** Even out the whole page: every split's parts alike, every band as tall as the rest. */
  evenOut(): void {
    this.#gesture?.end(false);
    this.#change(evenAll(this.#layout), 'Evened out.');
  }

  /** Show what a preset would look like (a hovered thumbnail), or the layout again (null); as `arrange` would do it. */
  preview(preset: Preset | null, first?: string): void {
    if (this.#gesture) return;
    const was = this.#previewing;
    if (preset !== null && !was) this.#previewScroll = this.#host.scrollTop;
    this.#shown = preset === null ? null : arrange(this.#layout, preset, this.#order(first));
    this.#previewing = preset !== null;
    this.#paint();
    if (preset === null && was) this.#host.scrollTop = this.#previewScroll;
  }

  /** The page fitting its window, or scrolling past it. */
  setFit(fit: boolean): void {
    if (fit === this.#layout.fit) return;
    this.#gesture?.end(false);
    this.#change(fitted(this.#layout, fit), fit ? 'The page fits the window.' : 'The page scrolls.');
  }

  /** One tile filling the board (the rest still there, hidden), or the page again (null), scrolled back where it was. */
  maximise(id: string | null): void {
    const next = id !== null && this.#tiles.has(id) ? id : null;
    if (next === this.#maximised) return;
    this.#gesture?.end(false);
    if (this.#maximised === null) this.#scrolled = this.#host.scrollTop;
    this.#maximised = next;
    this.#host.classList.toggle('dc-bands-maximised', next !== null);
    this.#paint();
    this.#host.scrollTop = next === null ? this.#scrolled : 0;
    if (next !== null) this.#tiles.get(next)?.root.focus();
  }

  get maximised(): string | null {
    return this.#maximised;
  }

  /** Edit mode (handles, dividers, remove) or view mode (nothing moves). */
  setEditing(editing: boolean): void {
    this.#editing = editing;
    if (!editing) this.#gesture?.end(false);
    this.#host.classList.toggle('dc-bands-view', !editing);
    this.#paint();
  }

  get editing(): boolean {
    return this.#editing;
  }

  /** Say something to a screen reader, as the board's own steps do (an undo, by its caller). */
  say(text: string): void {
    this.#live.textContent = text;
  }

  /** Scroll a tile into view (a new one below the screen's fold). */
  reveal(id: string): void {
    const box = this.#drawn.tiles.get(id);
    if (!box) return;
    const top = this.#host.scrollTop;
    const bottom = top + this.#host.clientHeight;
    if (box.y + box.h > bottom) this.#host.scrollTop = Math.min(box.y, box.y + box.h - this.#host.clientHeight + this.#gap);
    else if (box.y < top) this.#host.scrollTop = Math.max(0, box.y - this.#gap);
  }

  /** Rename a tile, as a double click on its title does. */
  rename(id: string, title: string): void {
    const p = this.#tiles.get(id);
    if (!p) return;
    p.name = title;
    p.title.textContent = title;
    p.root.setAttribute('aria-label', title);
    p.remove.title = `Remove ${title}`;
    p.remove.setAttribute('aria-label', `Remove ${title}`);
  }

  dispose(): void {
    this.#gesture?.end(false);
    this.#observer?.disconnect();
    const view = this.#doc.defaultView;
    if (this.#frame && view) view.cancelAnimationFrame(this.#frame);
    this.#host.replaceChildren();
    this.#host.classList.remove('dc-bands', 'dc-bands-narrow', 'dc-bands-view', 'dc-bands-maximised', 'dc-bands-fit');
  }

  /** The tiles in reading order, `first` first. */
  #order(first?: string): string[] {
    const order = tiles(this.#layout);
    return first !== undefined && order.includes(first) ? [first, ...order.filter((id) => id !== first)] : order;
  }

  // -- drawing ----------------------------------------------------------------

  #make(spec: BandTile): Placed {
    const doc = this.#doc;
    const root = doc.createElement('section');
    root.className = 'dc-tile dc-band-tile';
    root.dataset['tile'] = spec.id;
    root.tabIndex = 0;
    root.setAttribute('aria-label', spec.title);
    root.setAttribute('aria-roledescription', 'tile');
    const head = doc.createElement('div');
    head.className = 'dc-tile-head';
    const title = doc.createElement('span');
    title.className = 'dc-tile-title';
    title.textContent = spec.title;
    title.title = 'Drag to move; double-click to rename';
    const actions = doc.createElement('span');
    actions.className = 'dc-tile-actions';
    actions.append(...(spec.actions ?? []));
    const layout = doc.createElement('button');
    layout.type = 'button';
    layout.className = 'dc-tile-tool dc-tile-layout';
    layout.textContent = '⊞';
    layout.title = 'Layouts';
    layout.setAttribute('aria-label', `Layouts for ${spec.title}`);
    layout.hidden = this.#options.onLayout === undefined;
    layout.addEventListener('click', () => this.#options.onLayout?.(spec.id, layout));
    const maximise = doc.createElement('button');
    maximise.type = 'button';
    maximise.className = 'dc-tile-tool dc-tile-maximise';
    maximise.textContent = '⤢';
    maximise.title = 'Fill the page with this tile (and back)';
    maximise.setAttribute('aria-label', `Maximise ${spec.title}`);
    maximise.addEventListener('click', () => this.maximise(this.#maximised === spec.id ? null : spec.id));
    const remove = doc.createElement('button');
    remove.type = 'button';
    remove.className = 'dc-tile-remove';
    remove.textContent = '×';
    remove.title = `Remove ${spec.title}`;
    remove.setAttribute('aria-label', `Remove ${spec.title}`);
    remove.hidden = spec.removable === false;
    remove.addEventListener('click', () => this.#options.onRemove?.(spec.id));
    actions.append(layout, maximise, remove);
    head.append(title, actions);
    const body = doc.createElement('div');
    body.className = 'dc-tile-body';
    body.append(spec.element);
    root.append(head, body);
    this.#canvas.append(root);
    const placed: Placed = { spec, root, title, actions, remove, name: spec.title };
    head.addEventListener('pointerdown', (e) => this.#startMove(spec.id, e));
    root.addEventListener('keydown', (e) => this.#onKey(spec.id, e));
    head.addEventListener('dblclick', (e) => {
      if (!(e.target as Element | null)?.closest('button, input, select')) this.#startRename(placed);
    });
    return placed;
  }

  #schedule(): void {
    const view = this.#doc.defaultView;
    if (!view?.requestAnimationFrame) {
      this.#paint();
      return;
    }
    if (this.#frame) return;
    this.#frame = view.requestAnimationFrame(() => {
      this.#frame = 0;
      this.#paint();
    });
  }

  /** The board's own size: its width, and a screenful (its visible height). */
  #size(): { width: number; screen: number } {
    return { width: this.#host.clientWidth, screen: this.#host.clientHeight };
  }

  #paint(): void {
    const { width, screen } = this.#size();
    const narrow = width > 0 && width < this.#narrowBelow;
    if (narrow !== this.#narrow) {
      this.#narrow = narrow;
      this.#host.classList.toggle('dc-bands-narrow', narrow);
      if (narrow) this.#gesture?.end(false);
    }
    const view = this.#narrow ? stacked(this.#layout) : this.#shown ?? this.#layout;
    this.#drawn = draw(view, width, screen, this.#gap, this.#least, FIT_LEAST);
    // nothing scrolls on a page that fits -- unless its bands were too many to fit, and it scrolls after all
    this.#fits = view.fit && this.#drawn.height <= screen;
    this.#host.classList.toggle('dc-bands-fit', this.#fits && this.#maximised === null);
    const maximised = this.#maximised;
    for (const [id, p] of this.#tiles) {
      const box = maximised === null ? this.#drawn.tiles.get(id)
        : id === maximised ? { x: 0, y: 0, w: width, h: screen } : undefined;
      p.root.hidden = box === undefined;
      if (box) place(p.root, box);
    }
    this.#canvas.style.height = `${maximised === null ? this.#drawn.height : screen}px`;
    // a lone tile has nothing beside it to divide from, and no edge to drag
    this.#paintHandles(maximised === null && this.#alone === null && this.arrangeable && !this.#previewing);
  }

  /**
   * The dividers and band edges, while the page can be arranged. Kept and only moved while the page's shape is the same
   * -- as it is through a divider's or an edge's drag, whose element holds the pointer and must not be replaced -- and
   * made again when it changes.
   */
  #paintHandles(shown: boolean): void {
    const wanted: { key: string; box: Box; make: () => HTMLElement }[] = [];
    if (shown) {
      for (const divider of this.#drawn.dividers) {
        wanted.push({
          // its direction too: a row turned column at the same place is a different divider (its cursor, its drag)
          key: `divider ${divider.path.join('.')} ${divider.after} ${divider.split}`,
          box: divider.box,
          make: () => {
            const el = this.#doc.createElement('div');
            el.className = `dc-band-divider dc-band-divider-${divider.split}`;
            el.title = 'Drag to resize (it snaps at quarters, thirds and the half; hold Alt not to); double-click to even out';
            el.addEventListener('pointerdown', (e) => this.#startDivider(divider.path, divider.after, divider.split, e));
            el.addEventListener('dblclick', () => {
              this.#change(evenOut(this.#layout, divider.path));
            });
            return el;
          },
        });
      }
      const bands = this.#drawn.bands;
      const fit = this.#fits;
      bands.forEach((band, i) => {
        // a page that fits its window has edges between its bands only; one that scrolls, under each
        if (fit && i === bands.length - 1) return;
        wanted.push({
          key: `edge ${i}`,
          box: { x: band.x, y: band.y + band.h, w: band.w, h: this.#gap },
          make: () => {
            const el = this.#doc.createElement('div');
            el.className = 'dc-band-edge';
            el.title = 'Drag to change the band\'s height';
            el.addEventListener('pointerdown', (e) => this.#startEdge(i, e));
            return el;
          },
        });
      });
    }
    const same = wanted.length === this.#handles.length && wanted.every((w, i) => w.key === this.#handles[i]!.key);
    if (!same) {
      for (const handle of this.#handles) handle.el.remove();
      this.#handles = wanted.map((w) => {
        const el = w.make();
        this.#canvas.append(el);
        return { key: w.key, el };
      });
    }
    wanted.forEach((w, i) => place(this.#handles[i]!.el, w.box));
  }

  /** A change by the user's hand: committed, and told with the layout it came from. */
  #change(next: Bands, announce?: string): void {
    const before = this.#layout;
    this.#commit(next, announce);
    this.#options.onChange?.(next, before);
  }

  #commit(next: Bands, announce?: string): void {
    this.#layout = next;
    this.#shown = null;
    this.#previewing = false;
    this.#paint();
    if (announce) this.#live.textContent = announce;
  }

  // -- renaming ---------------------------------------------------------------

  #startRename(p: Placed): void {
    if (!this.#editing) return;
    const input = this.#doc.createElement('input');
    input.className = 'dc-tile-rename';
    input.value = p.name;
    input.setAttribute('aria-label', 'Tile title');
    let done = false;
    const finish = (keep: boolean): void => {
      if (done) return;
      done = true;
      const next = input.value.trim();
      input.replaceWith(p.title);
      if (keep && next !== '' && next !== p.name) {
        this.rename(p.spec.id, next);
        this.#options.onRename?.(p.spec.id, next);
      }
      p.root.focus();
    };
    input.addEventListener('keydown', (e) => {
      e.stopPropagation();
      if (e.key === 'Enter') finish(true);
      else if (e.key === 'Escape') finish(false);
    });
    input.addEventListener('blur', () => finish(true));
    input.addEventListener('pointerdown', (e) => e.stopPropagation());
    p.title.replaceWith(input);
    input.focus();
    input.select();
  }

  // -- gestures ---------------------------------------------------------------

  /** The pointer, in the canvas's own pixels (the board scrolled, the canvas moving with it). */
  #at(e: PointerEvent): { x: number; y: number } {
    const box = this.#canvas.getBoundingClientRect();
    return { x: e.clientX - box.left, y: e.clientY - box.top };
  }

  /**
   * One gesture on `el`, owning its pointer to the end: `move` for each pointer move (at most once a frame), `finish`
   * once -- with true when let go, false when undone (Escape, a lost capture, a cancelled pointer, the page going).
   */
  #own(el: HTMLElement, e: PointerEvent, move: (ev: PointerEvent) => void, finish: (apply: boolean, ev?: PointerEvent) => void): void {
    e.preventDefault();
    el.setPointerCapture?.(e.pointerId);
    const view = this.#doc.defaultView;
    let pending: PointerEvent | null = null;
    let frame = 0;
    const onMove = (ev: PointerEvent): void => {
      if (ev.pointerId !== e.pointerId) return;
      pending = ev;
      if (!view?.requestAnimationFrame) {
        move(ev);
        return;
      }
      if (frame) return;
      frame = view.requestAnimationFrame(() => {
        frame = 0;
        if (pending) move(pending);
      });
    };
    let over = false;
    const end = (apply: boolean, ev?: PointerEvent): void => {
      if (over) return;
      over = true;
      if (frame && view) view.cancelAnimationFrame(frame);
      el.removeEventListener('pointermove', onMove);
      el.removeEventListener('pointerup', up);
      el.removeEventListener('pointercancel', cancel);
      el.removeEventListener('lostpointercapture', lost);
      this.#doc.removeEventListener('keydown', escape, true);
      if (el.hasPointerCapture?.(e.pointerId)) el.releasePointerCapture(e.pointerId);
      this.#gesture = null;
      finish(apply, ev);
    };
    const up = (ev: PointerEvent): void => { if (ev.pointerId === e.pointerId) end(true, ev); };
    const cancel = (ev: PointerEvent): void => { if (ev.pointerId === e.pointerId) end(false); };
    // a capture lost without a pointerup (another element took it, the window lost focus): undone, never stuck
    const lost = (ev: PointerEvent): void => { if (ev.pointerId === e.pointerId) end(false); };
    const escape = (ev: KeyboardEvent): void => {
      if (ev.key !== 'Escape') return;
      ev.preventDefault();
      ev.stopPropagation();
      end(false);
    };
    el.addEventListener('pointermove', onMove);
    el.addEventListener('pointerup', up);
    el.addEventListener('pointercancel', cancel);
    el.addEventListener('lostpointercapture', lost);
    this.#doc.addEventListener('keydown', escape, true);
    this.#gesture = { end: (apply) => end(apply) };
  }

  /** A tile dragged by its title bar: it follows the pointer, the zone it would land in outlined; applied on drop. */
  #startMove(id: string, e: PointerEvent): void {
    if (!this.arrangeable || this.#maximised !== null || e.button !== 0 || this.#gesture) return;
    if ((e.target as Element | null)?.closest('button, input, select')) return;
    const tile = this.#tiles.get(id);
    if (!tile) return;
    const head = e.currentTarget as HTMLElement;
    const start = this.#at(e);
    const outside = this.#options.outside;
    let target: Drop | undefined;
    /** Over the place outside the board (a sheet's tab): the board proposes nothing. */
    let away = false;
    tile.root.classList.add('dc-tile-dragging');
    this.#own(head, e, (ev) => {
      const at = this.#at(ev);
      tile.root.style.transform = `translate(${at.x - start.x}px, ${at.y - start.y}px)`;
      away = outside?.over(ev.clientX, ev.clientY) ?? false;
      target = away ? undefined : dropAt(this.#drawn, id, at.x, at.y);
      this.#outline(target);
    }, (apply, ev) => {
      tile.root.classList.remove('dc-tile-dragging');
      tile.root.style.transform = '';
      this.#outline(undefined);
      if (apply && ev) {
        away = outside?.over(ev.clientX, ev.clientY) ?? false;
        if (away) {
          outside?.leave();
          outside?.drop(id, ev.clientX, ev.clientY);
          return;
        }
        const at = this.#at(ev);
        target = dropAt(this.#drawn, id, at.x, at.y);
      }
      outside?.leave();
      if (!apply || target === undefined) return;
      const next = drop(this.#layout, id, target);
      if (next === this.#layout) return;
      this.#change(next, `${tile.name} moved.`);
    });
  }

  /** Where a drop would land, outlined: the half of a tile it divides, the tile it swaps with, the line of a new band. */
  #outline(target: Drop | undefined): void {
    if (target === undefined) {
      this.#zone.hidden = true;
      return;
    }
    let box: Box | undefined;
    if ('band' in target) {
      const bands = this.#drawn.bands;
      const above = bands[target.band - 1];
      const y = above === undefined ? 0 : above.y + above.h + this.#gap / 2;
      box = { x: 0, y: Math.max(0, y - 3), w: this.#size().width, h: 6 };
    } else {
      const of = this.#drawn.tiles.get('swap' in target ? target.swap : target.onto);
      if (of && 'swap' in target) box = of;
      else if (of && 'edge' in target) {
        const half = { w: Math.round(of.w / 2), h: Math.round(of.h / 2) };
        box = target.edge === 'left' ? { ...of, w: half.w }
          : target.edge === 'right' ? { ...of, x: of.x + of.w - half.w, w: half.w }
            : target.edge === 'top' ? { ...of, h: half.h }
              : { ...of, y: of.y + of.h - half.h, h: half.h };
      }
    }
    this.#zone.hidden = box === undefined;
    this.#zone.classList.toggle('dc-bands-zone-line', 'band' in target);
    if (box) place(this.#zone, box);
  }

  /**
   * A divider dragged: its two parts trade share, the boxes re-drawn at most once a frame; applied on let go, at where
   * it was let go (a last move still waiting for its frame is not lost). Not moved at all (a click), nothing changes.
   */
  #startDivider(path: readonly number[], after: number, split: 'row' | 'column', e: PointerEvent): void {
    if (!this.arrangeable || e.button !== 0 || this.#gesture) return;
    const key = path.join('.');
    const length = this.#drawn.dividers.find((d) => d.path.join('.') === key && d.after === after)?.length ?? 0;
    const el = e.currentTarget as HTMLElement;
    const start = this.#at(e);
    const base = this.#layout;
    const follow = (ev: PointerEvent): void => {
      const at = this.#at(ev);
      const moved = split === 'row' ? at.x - start.x : at.y - start.y;
      if (moved === 0 || length <= 0) {
        this.#shown = null;
        return;
      }
      // onto a quarter, a third or the half when it comes near one; Alt held, exactly where the pointer is
      const delta = ev.altKey ? moved / length : snapped(base, path, after, moved / length, SNAP_PX / length);
      this.#shown = resize(base, path, after, delta);
    };
    /** The two parts' sizes, as each is a share of the split: shown by the pointer, said when let go. */
    const sizes = (): string => {
      const shares = sharesBeside(this.#shown ?? base, path, after);
      return shares ? `${shareText(shares[0])} \u00b7 ${shareText(shares[1])}` : '';
    };
    this.#own(el, e, (ev) => {
      follow(ev);
      this.#paint();
      this.#showReadout(sizes(), this.#at(ev));
    }, (apply, ev) => {
      if (apply && ev) follow(ev);
      const said = sizes();
      this.#hideReadout();
      const next = this.#shown ?? base;
      if (apply && next !== base) {
        this.#change(next, `Sizes ${said.replace(' \u00b7 ', ' and ')}.`);
      } else {
        this.#commit(base);
      }
    });
  }

  /** The readout by the pointer (`at`, in the canvas's pixels), kept inside the board. */
  #showReadout(text: string, at: { x: number; y: number }): void {
    if (text === '') return;
    this.#readout.textContent = text;
    this.#readout.hidden = false;
    const { width } = this.#size();
    this.#readout.style.left = `${Math.max(0, Math.min(at.x + 14, width - 120))}px`;
    this.#readout.style.top = `${Math.max(0, at.y + 14)}px`;
  }

  #hideReadout(): void {
    this.#readout.hidden = true;
  }

  /** A band's edge dragged: its height (a page that scrolls) or its share with the band below (one that fits), as a divider is. */
  #startEdge(band: number, e: PointerEvent): void {
    if (!this.arrangeable || e.button !== 0 || this.#gesture) return;
    const el = e.currentTarget as HTMLElement;
    const start = this.#at(e);
    const base = this.#layout;
    // as the page is drawn: its bands sharing the window, or keeping their heights (too many to fit, it scrolls)
    const fits = this.#fits;
    const { screen } = this.#size();
    const total = base.bands.reduce((sum, b) => sum + b.height, 0);
    const free = Math.max(1, screen - this.#gap * (base.bands.length - 1));
    const follow = (ev: PointerEvent): void => {
      const moved = this.#at(ev).y - start.y;
      this.#shown = moved === 0 ? null : fits
        ? tradeBands(base, band, (moved / free) * total)
        : resizeBand(base, band, base.bands[band]!.height + moved / Math.max(1, screen));
    };
    /** The band's height: on a page that fits, its share and the next band's; on one that scrolls, of the window. */
    const sizes = (): string => {
      const bands = (this.#shown ?? base).bands;
      const own = bands[band]?.height ?? 0;
      if (!fits) return `${shareText(own)} of the window`;
      const sum = bands.reduce((s, b) => s + b.height, 0);
      return `${shareText(own / sum)} \u00b7 ${shareText((bands[band + 1]?.height ?? 0) / sum)}`;
    };
    this.#own(el, e, (ev) => {
      follow(ev);
      this.#paint();
      this.#showReadout(sizes(), this.#at(ev));
    }, (apply, ev) => {
      if (apply && ev) follow(ev);
      const said = sizes();
      this.#hideReadout();
      const next = this.#shown ?? base;
      if (apply && next !== base) {
        this.#change(next, `Height ${said.replace(' \u00b7 ', ' and ')}.`);
      } else {
        this.#commit(base);
      }
    });
  }

  // -- the keyboard -------------------------------------------------------------

  #onKey(id: string, e: KeyboardEvent): void {
    const p = this.#tiles.get(id);
    if (e.key === 'Escape' && this.#maximised === id && e.target === p?.root) {
      e.preventDefault();
      this.maximise(null);
      p.root.focus();
      return;
    }
    // undo and redo of the page's arrangement, from a tile's own frame (inside a tile, its content's): Ctrl+Z, and
    // Ctrl+Shift+Z or -- Windows' spelling -- Ctrl+Y to redo; never on a locked or stacked page, where nothing moves
    const undoKey = e.key === 'z' || e.key === 'Z';
    const redoKey = e.key === 'y' || e.key === 'Y';
    if ((undoKey || redoKey) && (e.ctrlKey || e.metaKey) && e.target === p?.root && this.#options.onUndo) {
      e.preventDefault();
      e.stopPropagation();
      if (this.arrangeable && this.#gesture === null) this.#options.onUndo(redoKey || e.shiftKey);
      return;
    }
    const toward = ARROWS[e.key];
    if (!toward || !p || e.target !== p.root || !this.arrangeable || this.#maximised !== null || this.#gesture) return;
    e.preventDefault();
    const name = p.name;
    if (!e.shiftKey) {
      const other = neighbour(this.#drawn, id, toward);
      if (other === undefined) {
        this.#live.textContent = `${name} is already at the ${toward === 'up' ? 'top' : toward === 'down' ? 'bottom' : toward}.`;
        return;
      }
      const next = drop(this.#layout, id, { swap: other });
      this.#change(next, `${name} swapped with ${this.#tiles.get(other)?.name ?? other}.`);
      p.root.focus();
      return;
    }
    const grow = toward === 'right' || toward === 'down';
    const next = this.#stepSize(id, toward === 'left' || toward === 'right', grow);
    if (next === undefined || JSON.stringify(next) === JSON.stringify(this.#layout)) {
      this.#live.textContent = `${name} cannot grow or shrink that way.`;
      return;
    }
    const said = toward === 'right' ? 'wider' : toward === 'left' ? 'narrower' : toward === 'down' ? 'taller' : 'shorter';
    this.#change(next, `${name} ${said}.`);
    p.root.focus();
  }

  /**
   * A tile a step wider or narrower (`across`), or taller or shorter: the divider after it moved, or else the one
   * before it, the other way; a tile as tall as its band, the band's own height (on a page that fits its window, traded
   * with the band below, or above for the last).
   */
  #stepSize(id: string, across: boolean, grow: boolean): Bands | undefined {
    const layout = this.#layout;
    const step = grow ? KEY_STEP : -KEY_STEP;
    const after = dividerBeside(this.#drawn, id, across ? 'right' : 'bottom');
    if (after) return resize(layout, after.path, after.after, step);
    const before = dividerBeside(this.#drawn, id, across ? 'left' : 'top');
    if (before) return resize(layout, before.path, before.after, -step);
    if (across) return undefined;
    const band = bandOf(layout, id);
    if (band < 0) return undefined;
    if (!layout.fit) return resizeBand(layout, band, layout.bands[band]!.height + step);
    const total = layout.bands.reduce((sum, b) => sum + b.height, 0);
    if (band + 1 < layout.bands.length) return tradeBands(layout, band, step * total);
    return band > 0 ? tradeBands(layout, band - 1, -step * total) : undefined;
  }
}

function place(el: HTMLElement, box: Box): void {
  el.style.left = `${box.x}px`;
  el.style.top = `${box.y}px`;
  el.style.width = `${box.w}px`;
  el.style.height = `${box.h}px`;
}
