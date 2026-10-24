// A LONG PRESS OPENS THE RIGHT-CLICK MENU on a touch screen (the user, 2026-10-23: the grid's menu button went, its
// right-click is its menu -- and an iPad's Safari fires no right-click at all). Installed once per document: a finger
// held still for HOLD_MS fires the right-click event (`contextmenu`) on what it pressed, at the finger, so every
// right-click menu -- a grid's, a chart's, a sheet tab's, Ad Hoc's -- opens there, none of them changed for it. Where
// the browser fires its own on a long press (Android), this stands down, and the browser's own is not let through
// after this one's; the click the finger's release makes is swallowed, or it would pick the menu entry under it.

/** How long a finger is held still before the menu opens, ms: the platforms' own long press. */
export const HOLD_MS = 500;
/** How far a held finger may drift, px: further is a scroll or a drag, not a press. */
const SLOP_PX = 10;
/** How soon after the release its click comes, at most, ms: one later is a click of its own. */
const CLICK_MS = 1000;

const installed = new WeakSet<Document>();

/** Long presses on `doc` open its right-click menus; a second call on the same document adds nothing. */
export function longPressMenus(doc: Document): void {
  if (installed.has(doc)) return;
  installed.add(doc);
  /** A finger down, its timer running. */
  let press: { readonly id: number; readonly x: number; readonly y: number; readonly target: Element; readonly timer: ReturnType<typeof setTimeout> } | null = null;
  /** The finger whose press opened the menu, still down. */
  let held: number | null = null;
  /** Until when the click of that finger's release is swallowed (0: none to swallow). */
  let release = 0;
  /** The right-click being dispatched is this one's, not the browser's. */
  let firing = false;
  const cancel = (): void => {
    if (press) clearTimeout(press.timer);
    press = null;
  };
  const fire = (): void => {
    const p = press;
    press = null;
    if (!p || !p.target.isConnected) return;
    const MouseEventOf = doc.defaultView?.MouseEvent ?? MouseEvent;
    held = p.id;
    firing = true;
    try {
      p.target.dispatchEvent(new MouseEventOf('contextmenu', { bubbles: true, cancelable: true, clientX: p.x, clientY: p.y, button: 2 }));
    } finally {
      firing = false;
    }
  };
  // capture, on the document: seen before anything below can stop it
  doc.addEventListener('pointerdown', (e) => {
    cancel();
    held = null;
    release = 0;
    const target = e.target as Element | null;
    // one finger, on an element: a second finger is a pinch; a pen or a mouse has a right-click of its own
    if (e.pointerType !== 'touch' || !e.isPrimary || !target || typeof target.closest !== 'function') return;
    press = { id: e.pointerId, x: e.clientX, y: e.clientY, target, timer: setTimeout(fire, HOLD_MS) };
  }, true);
  doc.addEventListener('pointermove', (e) => {
    if (press && e.pointerId === press.id && Math.hypot(e.clientX - press.x, e.clientY - press.y) > SLOP_PX) cancel();
  }, true);
  doc.addEventListener('pointerup', (e) => {
    if (press && e.pointerId === press.id) cancel();
    if (held === e.pointerId) {
      held = null;
      release = Date.now() + CLICK_MS;
    }
  }, true);
  doc.addEventListener('pointercancel', (e) => {
    if (press && e.pointerId === press.id) cancel();
    if (held === e.pointerId) held = null;
  }, true);
  doc.addEventListener('contextmenu', (e) => {
    if (firing) return;
    // the browser's own long press (Android): it opens the menu, this does not as well -- nor again after this one's
    cancel();
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
