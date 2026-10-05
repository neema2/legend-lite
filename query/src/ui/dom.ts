// DOM helpers: an element builder, dialogs, menus, toasts and tooltips. Plain DOM, no framework
// (design D6): a panel is a function that renders into its container from the app's state and
// renders again when that state changes.

import { ICONS, type IconName } from '../../../legend-art/src/icons.ts';

export type Child = Node | string | number | false | null | undefined | readonly Child[];

export type Attrs = {
  class?: string;
  style?: string;
  title?: string;
  [key: `on${string}`]: ((e: never) => void) | undefined;
  [key: string]: unknown;
};

/** `h('div', {class: 'x', onclick}, child, ...)`: an element with attributes, handlers and children. */
export function h<K extends keyof HTMLElementTagNameMap>(tag: K, attrs: Attrs | null = null, ...children: Child[]): HTMLElementTagNameMap[K] {
  const el = document.createElement(tag);
  if (attrs) {
    for (const [k, v] of Object.entries(attrs)) {
      if (v === undefined || v === null || v === false) continue;
      if (k.startsWith('on') && typeof v === 'function') {
        el.addEventListener(k.slice(2), v as EventListener);
      } else if (k === 'value' && 'value' in el) {
        (el as unknown as { value: unknown }).value = v;
      } else if (k === 'checked' && 'checked' in el) {
        (el as unknown as { checked: boolean }).checked = v === true;
      } else if (k === 'dataset' && typeof v === 'object') {
        Object.assign(el.dataset, v);
      } else {
        el.setAttribute(k, v === true ? '' : String(v));
      }
    }
  }
  append(el, children);
  return el;
}

function append(el: Node, children: readonly Child[]): void {
  for (const c of children) {
    if (c === null || c === undefined || c === false) continue;
    if (Array.isArray(c)) append(el, c);
    else el.appendChild(typeof c === 'string' || typeof c === 'number' ? document.createTextNode(String(c)) : c as Node);
  }
}

/** Replace a container's content (a tooltip over what is replaced goes with it). */
export function mount(container: Element, ...children: Child[]): void {
  if (tip && tipTarget && container.contains(tipTarget)) hideTip();
  container.replaceChildren();
  append(container, children);
}

// ---------------------------------------------------------------- dialogs

export interface DialogHandle {
  readonly body: HTMLElement;
  close(): void;
}

/** A modal dialog; Escape and the backdrop close it. */
export function dialog(title: string, build: (d: DialogHandle) => { body: Child; foot?: Child }, options: { wide?: boolean; onClose?: () => void } = {}): DialogHandle {
  const backdrop = h('div', { class: 'q-backdrop' });
  const body = h('div', { class: 'q-dialog-body' });
  const handle: DialogHandle = {
    body,
    close() {
      document.removeEventListener('keydown', onKey, true);
      backdrop.remove();
      options.onClose?.();
    },
  };
  const onKey = (e: KeyboardEvent): void => {
    if (e.key === 'Escape') { e.stopPropagation(); handle.close(); }
  };
  const parts = build(handle);
  append(body, [parts.body]);
  const box = h('div', { class: `q-dialog${options.wide ? ' wide' : ''}`, role: 'dialog', 'aria-label': title },
    h('div', { class: 'q-dialog-head' }, title, h('span', { class: 'q-spacer' }),
      h('button', { class: 'q-icon-btn', title: 'Close', onclick: () => handle.close() }, '✕')),
    body,
    parts.foot === undefined ? null : h('div', { class: 'q-dialog-foot' }, parts.foot));
  backdrop.addEventListener('mousedown', (e) => { if (e.target === backdrop) handle.close(); });
  backdrop.appendChild(box);
  document.body.appendChild(backdrop);
  document.addEventListener('keydown', onKey, true);
  queueMicrotask(() => (box.querySelector('input, select, textarea, button.primary') as HTMLElement | null)?.focus());
  return handle;
}

/** Ask a yes/no question. */
export function confirmDialog(title: string, message: string, confirm = 'Proceed'): Promise<boolean> {
  return new Promise((resolve) => {
    let answered = false;
    dialog(title, (d) => ({
      body: h('div', null, message),
      foot: [
        h('button', { class: 'q-btn', onclick: () => d.close() }, 'Cancel'),
        h('button', { class: 'q-btn primary', onclick: () => { answered = true; d.close(); resolve(true); } }, confirm),
      ],
    }), { onClose: () => { if (!answered) resolve(false); } });
  });
}

// ---------------------------------------------------------------- menus

export type MenuItem = { label: string; action: () => void; disabled?: boolean } | 'separator';

let openMenu: HTMLElement | undefined;

/** A context menu at a point; any click elsewhere closes it. */
export function showMenu(x: number, y: number, items: readonly MenuItem[]): void {
  openMenu?.remove();
  const menu = h('div', { class: 'q-menu', role: 'menu' },
    items.map((it) => (it === 'separator' ? h('hr') : h('button', {
      disabled: it.disabled === true,
      onclick: () => { menu.remove(); it.action(); },
    }, it.label))));
  document.body.appendChild(menu);
  const r = menu.getBoundingClientRect();
  menu.style.left = `${Math.min(x, window.innerWidth - r.width - 8)}px`;
  menu.style.top = `${Math.min(y, window.innerHeight - r.height - 8)}px`;
  openMenu = menu;
  setTimeout(() => {
    const close = (e: Event): void => {
      if (!menu.contains(e.target as Node)) { menu.remove(); document.removeEventListener('mousedown', close, true); }
    };
    document.addEventListener('mousedown', close, true);
  });
}

/** A menu under a button. */
export function menuButton(label: Child, items: () => readonly MenuItem[], attrs: Attrs = {}): HTMLButtonElement {
  const b = h('button', { class: 'q-btn', ...attrs }, label);
  b.addEventListener('click', () => {
    const r = b.getBoundingClientRect();
    showMenu(r.left, r.bottom + 4, items());
  });
  return b;
}

// ---------------------------------------------------------------- toasts and tooltips

export function toast(message: string, ms = 2600): void {
  const t = h('div', { class: 'q-toast', role: 'status' }, message);
  document.body.appendChild(t);
  setTimeout(() => t.remove(), ms);
}

let tip: HTMLElement | undefined;
let tipTarget: HTMLElement | undefined;

function hideTip(): void {
  tip?.remove();
  tip = undefined;
  tipTarget = undefined;
}

/** Show `content` near an element while the pointer rests on it. */
export function tooltip(target: HTMLElement, content: () => Child): void {
  let timer: ReturnType<typeof setTimeout> | undefined;
  target.addEventListener('mouseenter', () => {
    timer = setTimeout(() => {
      hideTip();
      if (!target.isConnected) return;
      tipTarget = target;
      tip = h('div', { class: 'q-tooltip' }, content());
      document.body.appendChild(tip);
      const r = target.getBoundingClientRect();
      const t = tip.getBoundingClientRect();
      tip.style.left = `${Math.min(r.right + 8, window.innerWidth - t.width - 8)}px`;
      tip.style.top = `${Math.min(r.top, window.innerHeight - t.height - 8)}px`;
    }, 450);
  });
  const leave = (): void => {
    if (timer) clearTimeout(timer);
    if (tipTarget === target) hideTip();
  };
  target.addEventListener('mouseleave', leave);
  target.addEventListener('mousedown', leave);
}

/** A labelled field row. */
export function field(label: string, control: Child): HTMLElement {
  return h('div', { class: 'q-field' }, h('label', null, label), control);
}

/** A searchable select: a `<select>` with every option (short lists), or with a filter box (long ones). */
export function select<T extends string>(value: T | undefined, options: readonly { value: T; label: string }[], onChange: (v: T) => void, attrs: Attrs = {}): HTMLSelectElement {
  const s = h('select', { class: 'q-select', ...attrs, onchange: () => onChange(s.value as T) },
    value === undefined ? h('option', { value: '', disabled: true, selected: true }, 'Choose…') : null,
    options.map((o) => h('option', { value: o.value, selected: o.value === value }, o.label)));
  return s;
}

/** Minimal, safe Markdown: headings, emphasis, code, links, paragraphs and lists -- as DOM, never HTML. */
export function markdown(text: string): HTMLElement {
  const root = h('div', { class: 'q-md' });
  const inline = (s: string): Child[] => {
    const out: Child[] = [];
    const re = /(\*\*[^*]+\*\*|\*[^*]+\*|`[^`]+`|\[[^\]]+\]\([^)]+\))/g;
    let last = 0;
    for (const m of s.matchAll(re)) {
      out.push(s.slice(last, m.index));
      const t = m[0];
      if (t.startsWith('**')) out.push(h('strong', null, t.slice(2, -2)));
      else if (t.startsWith('*')) out.push(h('em', null, t.slice(1, -1)));
      else if (t.startsWith('`')) out.push(h('code', null, t.slice(1, -1)));
      else {
        const [, label = '', href = ''] = /\[([^\]]+)\]\(([^)]+)\)/.exec(t) ?? [];
        out.push(/^https?:\/\//.test(href) ? h('a', { href, target: '_blank', rel: 'noopener' }, label) : label);
      }
      last = (m.index ?? 0) + t.length;
    }
    out.push(s.slice(last));
    return out;
  };
  let list: HTMLElement | undefined;
  for (const block of text.split(/\n\s*\n/)) {
    for (const line of block.split('\n')) {
      const heading = /^(#{1,3})\s+(.*)$/.exec(line);
      const item = /^\s*[-*]\s+(.*)$/.exec(line);
      if (heading) { list = undefined; root.appendChild(h(`h${heading[1]!.length}` as 'h1', null, inline(heading[2]!))); }
      else if (item) {
        if (!list) { list = h('ul'); root.appendChild(list); }
        list.appendChild(h('li', null, inline(item[1]!)));
      } else if (line.trim()) { list = undefined; root.appendChild(h('p', null, inline(line))); }
    }
    list = undefined;
  }
  return root;
}

/** One of upstream's icons (icons.ts), 1em square in the text colour; `title` names it for a reader. */
export function icon(name: IconName, title?: string): HTMLElement {
  const span = h('span', { class: `q-icon q-icon--${name}`, ...(title ? { title, role: 'img', 'aria-label': title } : { 'aria-hidden': 'true' }) });
  span.innerHTML = ICONS[name];
  return span;
}

/**
 * A panel's header, as upstream's query builder draws one (legend-art Panel.tsx, PanelHeader):
 * its title as a lowercase chip, then the panel's own controls, then its actions at the right.
 */
export function panelHeader(title: string, lead: readonly Child[] = [], actions: readonly Child[] = []): HTMLElement {
  return h('div', { class: 'q-panel__header' },
    h('span', { class: 'q-panel__title' }, title.toLowerCase()),
    lead,
    h('span', { class: 'q-spacer' }),
    actions.length > 0 ? h('span', { class: 'q-panel__actions' }, actions) : null);
}

/** A header action: one of upstream's icons in a 28px button, named for a reader. */
export function panelAction(name: IconName, title: string, onclick: () => void, disabled = false): HTMLElement {
  return h('button', { class: 'q-panel__action', type: 'button', title, 'aria-label': title, disabled, onclick }, icon(name));
}

/**
 * An empty panel's placeholder, as upstream's (legend-art BlankPanelPlaceholder): what to do, in
 * bold, over a dashed drop box with the "drop here" icon.
 */
export function blankPlaceholder(text: string, tooltip = 'Drag and drop properties here'): HTMLElement {
  return h('div', { class: 'q-blank', title: tooltip },
    h('div', { class: 'q-blank__text' }, text),
    h('div', { class: 'q-blank__box' }, icon('dropHere')));
}
