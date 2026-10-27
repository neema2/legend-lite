// A floating window: dragged by its title bar, resized from any edge.
//
// The dialogs were not dialogs. They were a block appended after the
// grid, so opening one pushed the page down and showed it at the
// bottom -- readable, but not a window, and impossible to consult
// beside the data it is about.
//
// DataCube's are real windows (DataCubeLayout / DataCubeLayoutService):
// absolutely positioned, dragged by their header, resizable from all
// eight edges and corners, 800x600 by default with a 300x300 floor,
// centred in the container with a 50px margin, and each remembering
// where it was put. Those are the numbers below.
//
// Pointer events rather than mouse events: one code path covers mouse,
// pen and touch, and `setPointerCapture` keeps the drag alive when the
// pointer outruns the window -- which it does, every time, at the
// moment the user is trying to make it smaller.

/** Where a window is and how big. */
export interface WindowSpec {
  readonly x: number;
  readonly y: number;
  readonly width: number;
  readonly height: number;
}

export interface WindowOptions {
  /** Starting size, before the container's own limits apply. */
  readonly width?: number;
  readonly height?: number;
  readonly minWidth?: number;
  readonly minHeight?: number;
  /** Where to put it, if not centred. Negative counts from the far edge. */
  readonly x?: number;
  readonly y?: number;
  readonly center?: boolean;
  /** A spec to restore instead of computing one. */
  readonly spec?: WindowSpec | undefined;
  /**
   * Called whenever a drag or resize finishes, or a window on the screen is moved back onto it -- saying which: a size a
   * person gave it is only one `resized` (a press on its title bar settles `moved`, the browser shrinking `screen`).
   */
  readonly onChange?: (spec: WindowSpec, by: WindowChange) => void;
  /**
   * Kept on the screen rather than in its container: fixed to the browser's window, its spec in the window's own
   * coordinates, and wholly inside it however it is dragged, resized, or the browser's window shrinks -- a window placed
   * beside something on the page (a chart's options: `besideWindow`), never half off the screen.
   */
  readonly viewport?: boolean;
  /** Its listeners let go (a window on the screen listens to the browser's window resizing): when it closes. */
  readonly signal?: AbortSignal;
}

/** What changed a window: its title bar dragged (or pressed), a grip dragged to a new size, the screen it is kept on. */
export type WindowChange = 'moved' | 'resized' | 'screen';

/**
 * The browser window's room for a window kept on the screen: its viewport without the page's scrollbars (`innerWidth`
 * counts them, and a window at the edge sat under one -- the 2026-10-25 review); the inner size where nothing is laid
 * out (jsdom).
 */
export function screenOf(view: Window): { width: number; height: number } {
  const root = view.document.documentElement;
  return {
    width: root.clientWidth > 0 ? root.clientWidth : view.innerWidth,
    height: root.clientHeight > 0 ? root.clientHeight : view.innerHeight,
  };
}

/** DataCube's own figures (DataCubeLayoutService). */
export const WINDOW_OFFSET = 50;
export const WINDOW_WIDTH = 800;
export const WINDOW_HEIGHT = 600;
export const WINDOW_MIN_WIDTH = 300;
export const WINDOW_MIN_HEIGHT = 300;

/** The eight directions a window can be resized from. */
const EDGES = [
  'n', 's', 'e', 'w', 'ne', 'nw', 'se', 'sw',
] as const;
type Edge = (typeof EDGES)[number];

const clamp = (v: number, lo: number, hi: number): number =>
  Math.min(Math.max(v, lo), hi);

/** Each window `makeWindow` made: where it is now, and how to put it elsewhere as a drag would. */
const made = new WeakMap<HTMLElement, { readonly spec: () => WindowSpec; readonly place: (next: WindowSpec) => void }>();

/** Where a window `makeWindow` made is now; undefined for any other element. */
export function windowSpecOf(el: HTMLElement): WindowSpec | undefined {
  return made.get(el)?.spec();
}

/**
 * Put a window `makeWindow` made at `next`, as a drag would -- its next drag or resize starts from there -- without
 * telling its `onChange` (a person did not move it: a window fitted to what it shows).
 */
export function moveWindow(el: HTMLElement, next: WindowSpec): void {
  made.get(el)?.place(next);
}

/** The z-index windows start from, above a cube's own layers. */
export const WINDOW_Z_BASE = 20;
/** The top of each window host's stack of windows (`raiseWindow`). */
const windowStack = new WeakMap<HTMLElement, number>();

/**
 * Put a window above every other floating in `host`. One stack per place windows float: several cubes sharing a host
 * share it, and so does a chart's options window, so the window touched last is on top whoever opened it.
 */
export function raiseWindow(win: HTMLElement, host: HTMLElement): void {
  // already on top: left there -- every press in a window raised it again, and after a thousand or so the page's windows
  // rose over its menus (z-index 1000; the 2026-10-25 review)
  if (win.style.zIndex !== '' && Number(win.style.zIndex) === windowStack.get(host)) return;
  const top = (windowStack.get(host) ?? WINDOW_Z_BASE) + 1;
  windowStack.set(host, top);
  win.style.zIndex = String(top);
}

/** A box on the screen, in the browser window's coordinates (a DOMRect's four sides). */
export interface ScreenBox {
  readonly left: number;
  readonly top: number;
  readonly right: number;
  readonly bottom: number;
}

/** The gap between a window and the box it is beside, and between it and the screen's edge. */
export const BESIDE_GAP = 8;

/**
 * Where a window goes beside `box` (a chart), on a `screen` of that size: NEXT TO IT, NOT OVER IT, AND WHOLLY ON THE
 * SCREEN (the user, 2026-10-25). To its right where the window fits, else its left, else below it, else above it, at
 * the size wanted; where none fits, on the side with the most room, made smaller to fit it (never under its minimum);
 * where even that cannot be -- the box fills the screen -- over the box on the side with the most room. Always inside
 * the screen: the window no bigger than the screen, its edges `BESIDE_GAP` in from it.
 */
export function besideWindow(
  box: ScreenBox,
  screen: { readonly width: number; readonly height: number },
  wanted: { readonly width: number; readonly height: number; readonly minWidth: number; readonly minHeight: number },
): WindowSpec {
  const gap = BESIDE_GAP;
  const width = Math.max(0, Math.min(wanted.width, screen.width - 2 * gap));
  const height = Math.max(0, Math.min(wanted.height, screen.height - 2 * gap));
  const xIn = (x: number, w = width): number => clamp(x, gap, Math.max(gap, screen.width - gap - w));
  const yIn = (y: number, h = height): number => clamp(y, gap, Math.max(gap, screen.height - gap - h));
  // the room on each side of the box, between it and the screen's edge, gaps taken
  const room = {
    right: screen.width - gap - (box.right + gap),
    left: box.left - gap - gap,
    below: screen.height - gap - (box.bottom + gap),
    above: box.top - gap - gap,
  };
  type Side = keyof typeof room;
  const across = (side: Side): boolean => side === 'right' || side === 'left';
  // on the screen whatever the box's place: one scrolled half off it still has its window wholly on it
  const at = (side: Side, w: number, h: number): WindowSpec => {
    const x = side === 'right' ? box.right + gap : side === 'left' ? box.left - gap - w : box.left;
    const y = side === 'below' ? box.bottom + gap : side === 'above' ? box.top - gap - h : box.top;
    return { x: xIn(x, w), y: yIn(y, h), width: w, height: h };
  };
  const sides: readonly Side[] = ['right', 'left', 'below', 'above'];
  const fits = sides.find((side) => room[side] >= (across(side) ? width : height));
  if (fits !== undefined) return at(fits, width, height);
  // the side with the most room, the window made smaller to it -- where it can be, still at its minimum
  const roomiest = [...sides].sort((a, b) => room[b] - room[a]);
  const smaller = roomiest.find((side) => room[side] >= (across(side) ? wanted.minWidth : wanted.minHeight));
  if (smaller !== undefined) {
    return across(smaller) ? at(smaller, room[smaller], height) : at(smaller, width, room[smaller]);
  }
  // nowhere beside it: over the box, on the side with the most room
  return at(roomiest[0]!, width, height);
}

/**
 * `spec` kept wholly inside a screen of that size: no bigger than it, moved back onto it (a window on the screen,
 * after the browser's window shrank).
 */
export function onScreen(spec: WindowSpec, screen: { readonly width: number; readonly height: number }): WindowSpec {
  const width = Math.min(spec.width, screen.width);
  const height = Math.min(spec.height, screen.height);
  return {
    x: clamp(spec.x, 0, Math.max(0, screen.width - width)),
    y: clamp(spec.y, 0, Math.max(0, screen.height - height)),
    width,
    height,
  };
}

/** A resized `spec` cut at the screen's edges: the edge dragged stops there, the opposite one stays put. */
function cutToScreen(spec: WindowSpec, screen: { readonly width: number; readonly height: number }): WindowSpec {
  const x = Math.max(0, spec.x);
  const y = Math.max(0, spec.y);
  return {
    x,
    y,
    width: Math.min(spec.x + spec.width, screen.width) - x,
    height: Math.min(spec.y + spec.height, screen.height) - y,
  };
}

/**
 * Fit a window to its container.
 *
 * Shrinks rather than overflows, and reads a negative coordinate as a
 * distance from the far edge -- both of which upstream does, and the
 * second is how a window asks to sit bottom-right without knowing how
 * big the container is.
 */
export function fitWindow(
  container: { width: number; height: number },
  options: WindowOptions = {},
): WindowSpec {
  const minWidth = options.minWidth ?? WINDOW_MIN_WIDTH;
  const minHeight = options.minHeight ?? WINDOW_MIN_HEIGHT;
  const wanted = options.width ?? WINDOW_WIDTH;
  const tall = options.height ?? WINDOW_HEIGHT;

  if (options.center !== false) {
    const width = Math.max(minWidth, wanted + WINDOW_OFFSET * 2
      > container.width ? container.width - WINDOW_OFFSET * 2 : wanted);
    const height = Math.max(minHeight, tall + WINDOW_OFFSET * 2
      > container.height ? container.height - WINDOW_OFFSET * 2 : tall);
    return {
      x: Math.max(0, (container.width - width) / 2),
      y: Math.max(0, (container.height - height) / 2),
      width,
      height,
    };
  }

  const x = options.x ?? WINDOW_OFFSET;
  const y = options.y ?? WINDOW_OFFSET;
  const width = Math.max(minWidth,
    wanted + Math.abs(x) + WINDOW_OFFSET > container.width
      ? container.width - Math.abs(x) - WINDOW_OFFSET
      : wanted);
  const height = Math.max(minHeight,
    tall + Math.abs(y) + WINDOW_OFFSET > container.height
      ? container.height - Math.abs(y) - WINDOW_OFFSET
      : tall);
  return {
    x: x < 0 ? container.width - Math.abs(x) - width : x,
    y: y < 0 ? container.height - Math.abs(y) - height : y,
    width,
    height,
  };
}

/**
 * Make `el` a window inside `container`, dragged by `handle`.
 *
 * Returns the spec it settled on, so a caller can remember it.
 */
export function makeWindow(
  el: HTMLElement,
  handle: HTMLElement,
  container: HTMLElement,
  options: WindowOptions = {},
): WindowSpec {
  const doc = el.ownerDocument;
  const minWidth = options.minWidth ?? WINDOW_MIN_WIDTH;
  const minHeight = options.minHeight ?? WINDOW_MIN_HEIGHT;

  // FLOAT IT FIRST, THEN MEASURE. While the element is still in
  // flow it makes its own container taller, so measuring first gave
  // a container half again too tall: a 600px window was centred at
  // y=394 inside a container that collapsed to 545px the moment it
  // went absolute. The window then hung below the fold with its
  // bottom grips off-screen and unreachable, and the page grew a
  // scrollbar to accommodate a window that was supposed to float
  // over it.
  el.classList.add('dc-window');
  const view = doc.defaultView!;
  const screen = (): { width: number; height: number } => screenOf(view);
  // ON THE SCREEN: fixed, its spec the browser window's coordinates. Where an ancestor makes itself the fixed element's
  // frame instead (a transform, a filter), the frame's corner is measured and taken off, so the window still lands where
  // its spec says on the screen.
  let origin = { x: 0, y: 0 };
  if (options.viewport) {
    el.classList.add('dc-window-fixed');
    el.style.left = '0px';
    el.style.top = '0px';
    const corner = el.getBoundingClientRect();
    origin = { x: corner.left, y: corner.top };
  }
  const box = container.getBoundingClientRect();
  // A container with no layout yet -- jsdom, or a hidden parent --
  // would otherwise produce a zero-sized window that nothing can
  // grab. Fall back to the wanted size and let the first real layout
  // sort it out.
  const bounds = options.viewport ? screen() : {
    width: box.width || (options.width ?? WINDOW_WIDTH) + WINDOW_OFFSET * 2,
    height: box.height || (options.height ?? WINDOW_HEIGHT) + WINDOW_OFFSET * 2,
  };

  let spec = options.spec ?? fitWindow(bounds, options);
  if (options.viewport) spec = onScreen(spec, bounds);

  const place = (next: WindowSpec): void => {
    spec = next;
    el.style.left = `${next.x - origin.x}px`;
    el.style.top = `${next.y - origin.y}px`;
    el.style.width = `${next.width}px`;
    el.style.height = `${next.height}px`;
  };
  place(spec);
  made.set(el, { spec: () => spec, place: (next) => place(options.viewport ? onScreen(next, screen()) : next) });

  const settle = (by: WindowChange): void => options.onChange?.(spec, by);

  // ---- dragging ----
  handle.classList.add('dc-window-handle');
  handle.addEventListener('pointerdown', (event) => {
    // Only the primary button, and never from a control inside the
    // bar -- the close button is in there, and a window that moves
    // when you try to shut it is worse than one that cannot move.
    if (event.button !== 0) return;
    const target = event.target;
    if (target instanceof doc.defaultView!.HTMLElement
      && target.closest('button')) return;
    event.preventDefault();
    const startX = event.clientX;
    const startY = event.clientY;
    const from = spec;
    const live = container.getBoundingClientRect();

    const move = (e: PointerEvent): void => {
      const x = from.x + (e.clientX - startX);
      const y = from.y + (e.clientY - startY);
      // on the screen, all of it stays on the screen
      if (options.viewport) {
        place(onScreen({ ...from, x, y }, screen()));
        return;
      }
      // At least a title bar's worth stays reachable: a window
      // dragged fully outside cannot be dragged back.
      const maxX = Math.max(0, live.width - 40);
      const maxY = Math.max(0, live.height - 24);
      place({
        ...from,
        x: clamp(x, -(from.width - 40), maxX),
        y: clamp(y, 0, maxY),
      });
    };
    const up = (): void => {
      handle.removeEventListener('pointermove', move);
      handle.removeEventListener('pointerup', up);
      handle.removeEventListener('pointercancel', up);
      settle('moved');
    };
    handle.setPointerCapture?.(event.pointerId);
    handle.addEventListener('pointermove', move);
    handle.addEventListener('pointerup', up);
    handle.addEventListener('pointercancel', up);
  });

  // ---- resizing ----
  for (const edge of EDGES) {
    const grip = doc.createElement('div');
    grip.className = `dc-window-grip dc-window-${edge}`;
    grip.setAttribute('aria-hidden', 'true');
    grip.addEventListener('pointerdown', (event) => {
      if (event.button !== 0) return;
      event.preventDefault();
      event.stopPropagation();
      const startX = event.clientX;
      const startY = event.clientY;
      const from = spec;

      const move = (e: PointerEvent): void => {
        const dx = e.clientX - startX;
        const dy = e.clientY - startY;
        const next = resize(from, edge, dx, dy, minWidth, minHeight);
        place(options.viewport ? cutToScreen(next, screen()) : next);
      };
      const up = (): void => {
        grip.removeEventListener('pointermove', move);
        grip.removeEventListener('pointerup', up);
        grip.removeEventListener('pointercancel', up);
        // a press on a grip that changed nothing is no size given
        settle(spec.width !== from.width || spec.height !== from.height ? 'resized' : 'moved');
      };
      grip.setPointerCapture?.(event.pointerId);
      grip.addEventListener('pointermove', move);
      grip.addEventListener('pointerup', up);
      grip.addEventListener('pointercancel', up);
    });
    el.appendChild(grip);
  }

  // the browser's window shrank: a window on the screen moved back onto it, made smaller if it must be
  if (options.viewport) {
    const keep = (): void => {
      place(onScreen(spec, screen()));
      settle('screen');
    };
    view.addEventListener('resize', keep);
    // let go by hand, not by `addEventListener`'s own signal: that takes only its window's AbortSignal, and the caller's
    // may be another realm's (a test's page)
    options.signal?.addEventListener('abort', () => view.removeEventListener('resize', keep), { once: true });
  }

  return spec;
}

/**
 * The window a drag of `dx, dy` on `edge` produces.
 *
 * Separate and pure because the arithmetic is where this goes wrong:
 * dragging a WEST edge moves the window and shrinks it at once, and
 * the minimum has to stop the edge rather than let the opposite one
 * run away.
 */
export function resize(
  from: WindowSpec,
  edge: Edge,
  dx: number,
  dy: number,
  minWidth = WINDOW_MIN_WIDTH,
  minHeight = WINDOW_MIN_HEIGHT,
): WindowSpec {
  let { x, y, width, height } = from;
  if (edge.includes('e')) {
    width = Math.max(minWidth, from.width + dx);
  }
  if (edge.includes('s')) {
    height = Math.max(minHeight, from.height + dy);
  }
  if (edge.includes('w')) {
    width = Math.max(minWidth, from.width - dx);
    // The right edge stays put, so x follows the width it did not get.
    x = from.x + (from.width - width);
  }
  if (edge.includes('n')) {
    height = Math.max(minHeight, from.height - dy);
    y = from.y + (from.height - height);
  }
  return { x, y, width, height };
}
