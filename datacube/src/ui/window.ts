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
  /** Called whenever a drag or resize finishes. */
  readonly onChange?: (spec: WindowSpec) => void;
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
  const box = container.getBoundingClientRect();
  // A container with no layout yet -- jsdom, or a hidden parent --
  // would otherwise produce a zero-sized window that nothing can
  // grab. Fall back to the wanted size and let the first real layout
  // sort it out.
  const bounds = {
    width: box.width || (options.width ?? WINDOW_WIDTH) + WINDOW_OFFSET * 2,
    height: box.height || (options.height ?? WINDOW_HEIGHT) + WINDOW_OFFSET * 2,
  };

  let spec = options.spec ?? fitWindow(bounds, options);

  const place = (next: WindowSpec): void => {
    spec = next;
    el.style.left = `${next.x}px`;
    el.style.top = `${next.y}px`;
    el.style.width = `${next.width}px`;
    el.style.height = `${next.height}px`;
  };
  place(spec);

  const settle = (): void => options.onChange?.(spec);

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
      // At least a title bar's worth stays reachable: a window
      // dragged fully outside cannot be dragged back.
      const maxX = Math.max(0, live.width - 40);
      const maxY = Math.max(0, live.height - 24);
      place({
        ...from,
        x: clamp(from.x + (e.clientX - startX), -(from.width - 40), maxX),
        y: clamp(from.y + (e.clientY - startY), 0, maxY),
      });
    };
    const up = (): void => {
      handle.removeEventListener('pointermove', move);
      handle.removeEventListener('pointerup', up);
      handle.removeEventListener('pointercancel', up);
      settle();
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
        place(resize(from, edge, dx, dy, minWidth, minHeight));
      };
      const up = (): void => {
        grip.removeEventListener('pointermove', move);
        grip.removeEventListener('pointerup', up);
        grip.removeEventListener('pointercancel', up);
        settle();
      };
      grip.setPointerCapture?.(event.pointerId);
      grip.addEventListener('pointermove', move);
      grip.addEventListener('pointerup', up);
      grip.addEventListener('pointercancel', up);
    });
    el.appendChild(grip);
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
