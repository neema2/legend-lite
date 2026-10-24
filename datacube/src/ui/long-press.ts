// A LONG PRESS OPENS THE RIGHT-CLICK MENU on a touch screen (the user, 2026-10-23: the grid's menu button went, its
// right-click is its menu -- and an iPad's Safari fires no right-click at all). Installed once per document: a finger
// held still for HOLD_MS fires the right-click event (`contextmenu`) on what it pressed, at the finger, so every
// right-click menu -- a grid's, a chart's, a sheet tab's, Ad Hoc's -- opens there, none of them changed for it. Where
// the browser fires its own on a long press (Android), this stands down, and the browser's own is not let through
// after this one's; the click the finger's release makes is swallowed, or it would pick the menu entry under it.
//
// The user, 2026-10-24, on an iPad: the menu's first submenu flew open by itself, and columns could no longer be dragged
// or resized. The menu opens under the finger, and the release leaves the entry there hovered (a touch screen's sticky
// hover), which opened its submenu: from a long press's menu until the next touch, the document says so (PRESSED_CLASS)
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
/** On the document element from a long press's menu until the next touch: the entry left hovered under the released
 *  finger opens no submenu (grid.css). */
export const PRESSED_CLASS = 'dc-long-pressed';

const installed = new WeakSet<Document>();

/** Long presses on `doc` open its right-click menus; a second call on the same document adds nothing. */
export function longPressMenus(doc: Document): void {
  if (installed.has(doc)) return;
  installed.add(doc);
  /** A finger down: its timer running, or -- on what the browser drags -- the time it went down, judged at its release. */
  let press: {
    readonly id: number; readonly x: number; readonly y: number; readonly target: Element; readonly at: number;
    readonly timer: ReturnType<typeof setTimeout> | null;
  } | null = null;
  /** A press the browser lifted into a drag (an iPad's): a menu at its end, if the finger never went far. */
  let lift: { readonly x: number; readonly y: number; readonly target: Element; readonly at: number; far: boolean } | null = null;
  /** The finger whose press opened the menu, still down. */
  let held: number | null = null;
  /** Until when the click of that finger's release is swallowed (0: none to swallow). */
  let release = 0;
  /** The right-click being dispatched is this one's, not the browser's. */
  let firing = false;
  const cancel = (): void => {
    if (press?.timer) clearTimeout(press.timer);
    press = null;
  };
  /** The right-click on what was pressed, at the finger; the document saying a long press's menu is open. */
  const open = (p: { readonly x: number; readonly y: number; readonly target: Element }): void => {
    if (!p.target.isConnected) return;
    const MouseEventOf = doc.defaultView?.MouseEvent ?? MouseEvent;
    firing = true;
    try {
      p.target.dispatchEvent(new MouseEventOf('contextmenu', { bubbles: true, cancelable: true, clientX: p.x, clientY: p.y, button: 2 }));
    } finally {
      firing = false;
    }
    doc.documentElement.classList.add(PRESSED_CLASS);
  };
  const fire = (): void => {
    const p = press;
    press = null;
    if (!p) return;
    held = p.id;
    open(p);
  };
  /** A menu opened as the finger lifts (on what the browser drags): the click of that release is swallowed. */
  const openAtRelease = (p: { readonly x: number; readonly y: number; readonly target: Element }): void => {
    open(p);
    release = Date.now() + CLICK_MS;
  };
  const ripe = (at: number): boolean => Date.now() - at >= HOLD_MS;
  // capture, on the document: seen before anything below can stop it
  doc.addEventListener('pointerdown', (e) => {
    cancel();
    held = null;
    release = 0;
    lift = null;
    doc.documentElement.classList.remove(PRESSED_CLASS);
    const target = e.target as Element | null;
    // one finger, on an element: a second finger is a pinch; a pen or a mouse has a right-click of its own
    if (e.pointerType !== 'touch' || !e.isPrimary || !target || typeof target.closest !== 'function') return;
    if (target.closest(DRAG_SURFACE)) return;
    const draggable = target.closest(DRAGGABLE) !== null;
    press = { id: e.pointerId, x: e.clientX, y: e.clientY, target, at: Date.now(), timer: draggable ? null : setTimeout(fire, HOLD_MS) };
  }, true);
  doc.addEventListener('pointermove', (e) => {
    if (press && e.pointerId === press.id && Math.hypot(e.clientX - press.x, e.clientY - press.y) > SLOP_PX) cancel();
  }, true);
  doc.addEventListener('pointerup', (e) => {
    if (press && e.pointerId === press.id) {
      const p = press;
      cancel();
      // on what the browser drags: held long enough and never far, the menu as the finger lifts
      if (p.timer === null && ripe(p.at)) openAtRelease(p);
    }
    if (held === e.pointerId) {
      held = null;
      release = Date.now() + CLICK_MS;
    }
  }, true);
  doc.addEventListener('pointercancel', (e) => {
    // the browser lifting what it drags (an iPad's drag starting) cancels the finger: the press is kept for the drag's end
    if (press && e.pointerId === press.id) {
      if (press.timer === null) lift = { x: press.x, y: press.y, target: press.target, at: press.at, far: false };
      cancel();
    }
    if (held === e.pointerId) held = null;
  }, true);
  doc.addEventListener('dragstart', () => {
    if (press && press.timer === null) lift = { x: press.x, y: press.y, target: press.target, at: press.at, far: false };
    cancel();
  }, true);
  doc.addEventListener('dragover', (e) => {
    // a drag's position (one at 0,0 is a browser's "no position", not a place)
    if (lift && (e.clientX !== 0 || e.clientY !== 0) && Math.hypot(e.clientX - lift.x, e.clientY - lift.y) > SLOP_PX) lift.far = true;
  }, true);
  doc.addEventListener('dragend', () => {
    const l = lift;
    lift = null;
    // lifted and let go where it was pressed, held long enough: that was the long press, not a drag
    if (l && !l.far && ripe(l.at)) openAtRelease(l);
  }, true);
  doc.addEventListener('contextmenu', (e) => {
    if (firing) return;
    // the browser's own long press (Android): it opens the menu, this does not as well -- nor again after this one's;
    // a finger's menu all the same, its release leaving the entry under it hovered
    if (press !== null) doc.documentElement.classList.add(PRESSED_CLASS);
    cancel();
    lift = null;
    if (held !== null) {
      e.preventDefault();
      e.stopPropagation();
    }
  }, true);
  doc.addEventListener('click', (e) => {
    if (Date.now() >= release) return;
    release = 0;
    e.preventDefault();
    e.stopPropagation();
  }, true);
}
