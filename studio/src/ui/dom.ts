// A small DOM builder: `h('div', { class: 'x', onclick }, child, 'text')`.

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

/** A short-lived message at the bottom of the window. */
export function toast(message: string, kind: 'info' | 'error' = 'info'): void {
  const t = h('div', { class: `toast toast-${kind}` }, message);
  document.body.append(t);
  setTimeout(() => t.remove(), kind === 'error' ? 8000 : 3000);
}
