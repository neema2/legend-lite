// A LONG PRESS OPENS THE RIGHT-CLICK MENU on a touch screen (the user, 2026-10-23: the grid's menu button went, its
// right-click is its menu -- and an iPad's Safari fires no right-click at all). Shared by the document's active roots: a finger
// held still for HOLD_MS fires the right-click event (`contextmenu`) on what it pressed, at the finger, so every
// right-click menu -- a grid's, a chart's, a sheet tab's, Ad Hoc's -- opens there, none of them changed for it. Where
// the browser fires its own on a long press (Android), this stands down, and the browser's own is not let through
// after this one's; the click the finger's release makes is swallowed, or it would pick the menu entry under it.
//
// The user, 2026-10-24, on an iPad: the menu's first submenu flew open by itself, and columns could no longer be dragged
// or resized. The menu opens under the finger, and the release leaves the entry there hovered (a touch screen's sticky
// hover), which opened its submenu: from a long press's menu until the next touch, the owned roots say so (PRESSED_CLASS)
// and the hover opens none (grid.css). A press-and-hold on what the browser drags itself (a column's header, a chip:
// `draggable`) is how an iPad lifts it to drag: there the menu waits for the finger to lift where it pressed -- held
// still, the menu; moved, a drag, no menu. A column's resize grip and a band's divider take their drags themselves: a
// press there is never a menu.

/** How long a finger is held still before the menu opens, ms: the platforms' own long press. */
export const HOLD_MS = 500;
/** How far a held finger may drift, px: further is a scroll or a drag, not a press. */
const SLOP_PX = 10;
/** How soon after the release its click comes, at most, ms: one later is a click of its own. */
const CLICK_MS = 1000;
/** Where a press starts a drag the element takes itself: a column's resize grip, a band's divider or edge. */
const DRAG_SURFACE = '.dc-col-resize, .dc-band-divider, .dc-band-edge';
/** What the browser drags itself: a press-and-hold there lifts it on an iPad, so its menu waits for the release. */
const DRAGGABLE = '[draggable="true"]';
/** On DataCube's owned roots from a long press's menu until the next touch: the entry left hovered under the released
 *  finger opens no submenu (grid.css). */
export const PRESSED_CLASS = 'dc-long-pressed';

interface LongPressRoots { add(root: HTMLElement): () => void }
const installed = new WeakMap<Document, LongPressRoots>();

/**
 * Long presses inside an owned root open its right-click menus. Nested roots share one gesture handler; each owner
 * releases its registration on disposal. Floating menus register their own roots while open. Host UI is never a target.
 */
export function longPressMenus(root: HTMLElement): () => void {
  const doc = root.ownerDocument;
  let roots = installed.get(doc);
  if (roots === undefined) {
    roots = inDocument(doc);
    installed.set(doc, roots);
  }
  return roots.add(root);
}

function inDocument(doc: Document): LongPressRoots {
  const roots = new Map<HTMLElement, number>();
  const owns = (target: Element): boolean => [...roots.keys()].some((root) => root.contains(target));
  const targetOf = (event: Event): Element | undefined => {
    const target = event.composedPath()[0] ?? event.target;
    return target !== null && typeof (target as Element).closest === 'function' ? target as Element : undefined;
  };
  let pressed = false;
  const mark = (value: boolean): void => {
    pressed = value;
    for (const root of roots.keys()) root.classList.toggle(PRESSED_CLASS, value);
  };
  const listeners: (() => void)[] = [];
  const listen = <K extends keyof DocumentEventMap>(type: K, handler: (event: DocumentEventMap[K]) => void): void => {
    doc.addEventListener(type, handler, true);
    listeners.push(() => doc.removeEventListener(type, handler, true));
  };
  /** A finger down: its timer running, or -- on what the browser drags -- the time it went down, judged at its release. */
  let press: {
    readonly id: number; readonly x: number; readonly y: number; readonly target: Element; readonly at: number;
    readonly timer: ReturnType<typeof setTimeout> | null;
  } | null = null;
  /** A press the browser lifted into a drag (an iPad's): a menu at its end, if the finger never went far. */
  let lift: { readonly x: number; readonly y: number; readonly target: Element; readonly at: number; far: boolean } | null = null;
  /** The finger whose press opened the menu, still down. */
  let held: { readonly id: number; readonly target: Element } | null = null;
  /** Until when the click of that finger's release is swallowed (0: none to swallow). */
  let release = 0;
  let releaseTarget: Element | null = null;
  /** The right-click being dispatched is this one's, not the browser's. */
  let firing = false;
  const cancel = (): void => {
    if (press?.timer) clearTimeout(press.timer);
    press = null;
  };
  /** The right-click on what was pressed, at the finger; owned roots saying a long press's menu is open. */
  const open = (p: { readonly x: number; readonly y: number; readonly target: Element }): void => {
    if (!p.target.isConnected || !owns(p.target)) return;
    const MouseEventOf = doc.defaultView?.MouseEvent ?? MouseEvent;
    firing = true;
    try {
      p.target.dispatchEvent(new MouseEventOf('contextmenu', { bubbles: true, cancelable: true, clientX: p.x, clientY: p.y, button: 2 }));
    } finally {
      firing = false;
    }
    mark(true);
  };
  const fire = (): void => {
    const p = press;
    press = null;
    if (!p) return;
    held = { id: p.id, target: p.target };
    open(p);
  };
  /** A menu opened as the finger lifts (on what the browser drags): the click of that release is swallowed. */
  const openAtRelease = (p: { readonly x: number; readonly y: number; readonly target: Element }): void => {
    open(p);
    release = Date.now() + CLICK_MS;
    releaseTarget = p.target;
  };
  const ripe = (at: number): boolean => Date.now() - at >= HOLD_MS;
  // Capture across the document tracks a finger that leaves the root; only an owned target can start a gesture.
  listen('pointerdown', (e) => {
    cancel();
    held = null;
    release = 0;
    releaseTarget = null;
    lift = null;
    mark(false);
    const target = targetOf(e);
    // one finger, on an element: a second finger is a pinch; a pen or a mouse has a right-click of its own
    if (e.pointerType !== 'touch' || !e.isPrimary || !target || !owns(target)) return;
    if (target.closest(DRAG_SURFACE)) return;
    const draggable = target.closest(DRAGGABLE) !== null;
    press = { id: e.pointerId, x: e.clientX, y: e.clientY, target, at: Date.now(), timer: draggable ? null : setTimeout(fire, HOLD_MS) };
  });
  listen('pointermove', (e) => {
    if (press && e.pointerId === press.id && Math.hypot(e.clientX - press.x, e.clientY - press.y) > SLOP_PX) cancel();
  });
  listen('pointerup', (e) => {
    if (press && e.pointerId === press.id) {
      const p = press;
      cancel();
      // on what the browser drags: held long enough and never far, the menu as the finger lifts
      if (p.timer === null && ripe(p.at)) openAtRelease(p);
    }
    if (held?.id === e.pointerId) {
      releaseTarget = held.target;
      held = null;
      release = Date.now() + CLICK_MS;
    }
  });
  listen('pointercancel', (e) => {
    // the browser lifting what it drags (an iPad's drag starting) cancels the finger: the press is kept for the drag's end
    if (press && e.pointerId === press.id) {
      if (press.timer === null) lift = { x: press.x, y: press.y, target: press.target, at: press.at, far: false };
      cancel();
    }
    if (held?.id === e.pointerId) held = null;
  });
  listen('dragstart', () => {
    if (press && press.timer === null) lift = { x: press.x, y: press.y, target: press.target, at: press.at, far: false };
    cancel();
  });
  listen('dragover', (e) => {
    // a drag's position (one at 0,0 is a browser's "no position", not a place)
    if (lift && (e.clientX !== 0 || e.clientY !== 0) && Math.hypot(e.clientX - lift.x, e.clientY - lift.y) > SLOP_PX) lift.far = true;
  });
  listen('dragend', () => {
    const l = lift;
    lift = null;
    // lifted and let go where it was pressed, held long enough: that was the long press, not a drag
    if (l && !l.far && ripe(l.at)) openAtRelease(l);
  });
  listen('contextmenu', (e) => {
    if (firing) return;
    const target = targetOf(e);
    if (!target || !owns(target)) return;
    // the browser's own long press (Android): it opens the menu, this does not as well -- nor again after this one's;
    // a finger's menu all the same, its release leaving the entry under it hovered
    if (press !== null) mark(true);
    cancel();
    lift = null;
    if (held !== null) {
      e.preventDefault();
      e.stopPropagation();
    }
  });
  listen('click', (e) => {
    if (Date.now() >= release) return;
    const target = targetOf(e);
    if (!target || !owns(target)) return;
    release = 0;
    releaseTarget = null;
    e.preventDefault();
    e.stopPropagation();
  });

  return { add: (root) => {
    roots.set(root, (roots.get(root) ?? 0) + 1);
    root.classList.toggle(PRESSED_CLASS, pressed);
    let released = false;
    return () => {
      if (released) return;
      released = true;
      const count = roots.get(root)!;
      if (count > 1) { roots.set(root, count - 1); return; }
      roots.delete(root);
      root.classList.remove(PRESSED_CLASS);
      if (press && !owns(press.target)) cancel();
      if (lift && !owns(lift.target)) lift = null;
      if (held && !owns(held.target)) held = null;
      if (releaseTarget && !owns(releaseTarget)) { release = 0; releaseTarget = null; }
      if (roots.size === 0) {
        cancel();
        for (const remove of listeners) remove();
        installed.delete(doc);
      }
    };
  } };
}
