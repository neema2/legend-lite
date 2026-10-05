// A small DOM builder: `h('div', { class: 'x', onclick }, child, 'text')`.

import { icon } from '../../../legend-art/src/icon.ts';
import type { IconName } from '../../../legend-art/src/icons.ts';

type Child = Node | string | null | undefined | false;
type Attrs = Record<string, string | number | boolean | EventListener | undefined | null>;

export function h<K extends keyof HTMLElementTagNameMap>(tag: K, attrs: Attrs = {}, ...children: Child[]): HTMLElementTagNameMap[K] {
  const el = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (v === undefined || v === null || v === false) continue;
    if (k.startsWith('on') && typeof v === 'function') el.addEventListener(k.slice(2), v as EventListener);
    else if (v === true) el.setAttribute(k, '');
    else el.setAttribute(k, String(v));
  }
  for (const c of children) if (c !== null && c !== undefined && c !== false) el.append(c);
  return el;
}

export function clear(el: Element): void {
  while (el.firstChild) el.removeChild(el.firstChild);
}

/**
 * A modal dialog: resolves with what `read` answers as `{ ok }` when OK is pressed, undefined when
 * cancelled. `read` answers a string to refuse, shown in the dialog.
 */
export function dialog<T>(title: string, body: HTMLElement, read: () => { ok: T } | string, ok = 'OK'): Promise<T | undefined> {
  return new Promise((resolve) => {
    const error = h('div', { class: 'dialog-error' });
    const close = (v: T | undefined): void => {
      overlay.remove();
      resolve(v);
    };
    const submit = (): void => {
      const v = read();
      if (typeof v === 'string') {
        error.textContent = v;
        return;
      }
      close(v.ok);
    };
    const overlay = h('div', { class: 'overlay', onkeydown: ((e: KeyboardEvent) => {
      if (e.key === 'Escape') close(undefined);
      if (e.key === 'Enter' && !(e.target instanceof HTMLTextAreaElement)) submit();
    }) as EventListener },
    h('div', { class: 'dialog', role: 'dialog' },
      h('div', { class: 'dialog-title' }, title),
      body,
      error,
      h('div', { class: 'dialog-actions' },
        h('button', { class: 'btn', onclick: () => close(undefined) }, 'Cancel'),
        h('button', { class: 'btn btn-primary', onclick: submit }, ok))));
    document.body.append(overlay);
    (body.querySelector('input, select, textarea') as HTMLElement | null)?.focus();
  });
}

/** The toast now showing, and its timer: upstream shows one at a time, a new one replacing it. */
let shown: { readonly el: HTMLElement; readonly timer: ReturnType<typeof setTimeout> | undefined } | undefined;

const SEVERITY: Record<'info' | 'success' | 'error', [IconName, string]> = {
  info: ['info', 'Info'],
  success: ['checkCircle', 'Success'],
  error: ['timesCircle', 'Error'],
};

/**
 * A notification, as upstream's (census 10): one at a time at the bottom right, clear of the status bar -- the
 * severity's icon, the message (one line; a click copies it) and Dismiss. Info and success hide after 6 s; an
 * error stays until dismissed (upstream's notifyError passes no duration).
 */
export function toast(message: string, kind: 'info' | 'success' | 'error' = 'info'): void {
  const dismiss = (): void => {
    if (shown?.timer !== undefined) clearTimeout(shown.timer);
    shown?.el.remove();
    shown = undefined;
  };
  dismiss();
  const [glyph, label] = SEVERITY[kind];
  const el = h('div', { class: `toast toast-${kind}`, role: kind === 'error' ? 'alert' : 'status' },
    h('div', { class: 'toast__icon', title: label }, icon(glyph, '16px')),
    h('div', { class: 'toast__message', title: 'Click to Copy', onclick: () => void navigator.clipboard?.writeText(message).catch(() => undefined) }, message),
    h('button', { class: 'toast__action', title: 'Dismiss', onclick: dismiss }, icon('times')));
  document.body.append(el);
  shown = { el, timer: kind === 'error' ? undefined : setTimeout(dismiss, 6000) };
}

/** One entry of a {@link menu}. */
export interface MenuItem {
  readonly label: string;
  readonly run: () => void;
  readonly testId?: string;
}

/**
 * A dropdown beside `anchor` (upstream's menus: elevated, no animation): it opens to the anchor's right, top
 * edges aligned, and closes on a choice, a click elsewhere or Escape.
 */
export function menu(anchor: HTMLElement, items: readonly MenuItem[]): void {
  document.querySelector('.menu')?.remove();
  const box = anchor.getBoundingClientRect();
  const close = (): void => {
    list.remove();
    document.removeEventListener('mousedown', outside, true);
    document.removeEventListener('keydown', escape, true);
  };
  const outside = (e: MouseEvent): void => {
    if (!list.contains(e.target as Node)) close();
  };
  const escape = (e: KeyboardEvent): void => {
    if (e.key === 'Escape') close();
  };
  const list = h('div', { class: 'menu', role: 'menu' },
    ...items.map((i) => h('button', { class: 'menu__item', role: 'menuitem', 'data-testid': i.testId, onclick: () => { close(); i.run(); } }, i.label)));
  list.style.left = `${box.right}px`;
  list.style.top = `${box.top}px`;
  document.body.append(list);
  document.addEventListener('mousedown', outside, true);
  document.addEventListener('keydown', escape, true);
}

/** A side-bar header (census 2.2): the view's title, upper-case, then its actions (28px icon buttons). */
export function sideHead(title: string, ...actions: HTMLElement[]): HTMLElement {
  return h('div', { class: 'side-head' }, h('span', { class: 'side-head__title' }, title), h('div', { class: 'panel__header__actions' }, ...actions));
}

/** A side-bar header action: an icon button with its tooltip. */
export function headerAction(glyph: IconName, title: string, onclick: (() => void) | undefined, attrs: Attrs = {}): HTMLButtonElement {
  return h('button', { class: 'panel__header__action', title, onclick, disabled: onclick === undefined, ...attrs }, icon(glyph));
}

/**
 * A sub-panel inside a side-bar view (census 2.2, `side-bar__panel`): its 28px header on the header grey -- the
 * title in bold, an info icon whose tooltip says what it lists, a count pill -- then its content.
 */
export function subPanel(title: string, opts: { readonly info?: string; readonly count?: number; readonly testId?: string }, ...content: Child[]): HTMLElement {
  return h('div', { class: 'side-bar__panel' },
    h('div', { class: 'side-bar__panel__header' },
      h('div', { class: 'side-bar__panel__title' }, title),
      opts.info === undefined ? null : h('div', { class: 'side-bar__panel__info', title: opts.info }, icon('info')),
      opts.count === undefined ? null : h('div', { class: 'side-bar__panel__count' }, String(opts.count))),
    h('div', { class: 'side-bar__panel__content', 'data-testid': opts.testId }, ...content));
}

/** How long ago `iso` was, in date-fns formatDistanceToNow's words (upstream's review status: "created {N} ago"). */
export function ago(iso: string, now = Date.now()): string {
  const s = Math.max(0, (now - Date.parse(iso)) / 1000);
  const m = Math.round(s / 60);
  const hrs = Math.round(s / 3600);
  const d = Math.round(s / 86400);
  if (s < 30) return 'less than a minute';
  if (s < 90) return '1 minute';
  if (m < 45) return `${m} minutes`;
  if (m < 90) return 'about 1 hour';
  if (hrs < 24) return `about ${hrs} hours`;
  if (hrs < 42) return '1 day';
  if (d < 30) return `${d} days`;
  const months = Math.round(d / 30);
  return months < 2 ? 'about 1 month' : `${months} months`;
}
