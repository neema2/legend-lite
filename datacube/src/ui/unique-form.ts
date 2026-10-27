// UNIQUE, SET (docs/DATACUBE_FLOW_MODE_2026_10_24.md M3: a flow's Unique box): the columns a key is, and the order a
// key's first row is first in -- unique.ts's rule, said as Alteryx's Unique says it -- in a window its grid docks where
// its options dock it. Fetched with the first Unique.

import type { SortSpec } from '../snapshot.ts';
import type { Unique } from '../unique.ts';

export interface UniqueFormOptions {
  /** The columns a key can be made of, by name. */
  readonly columns: readonly string[];
  /** Its rows made unique: null when they were, else why not (the form stays, saying it). */
  apply(unique: Unique): Promise<string | null>;
  close(): void;
}

/** The form, drawn into `host`. */
export function uniqueForm(host: HTMLElement, o: UniqueFormOptions): void {
  const doc = host.ownerDocument;
  const el = <K extends keyof HTMLElementTagNameMap>(tag: K, cls: string, parent: Element, text?: string): HTMLElementTagNameMap[K] => {
    const e = doc.createElement(tag);
    e.className = cls;
    if (text !== undefined) e.textContent = text;
    parent.append(e);
    return e;
  };
  const body = el('div', 'dc-sample dc-unique', host);
  el('span', 'dc-picker-field-label', body, 'Unique by');
  const list = el('div', 'dc-text-tool-columns', body);
  const picks = o.columns.map((c) => {
    const l = el('label', 'dc-text-tool-check', list);
    const box = el('input', '', l);
    box.type = 'checkbox';
    box.value = c;
    el('span', '', l, c);
    return box;
  });
  const line = el('div', 'dc-sample-line', body);
  el('span', 'dc-sample-word', line, 'Keep the first row of each, by');
  const by = el('select', 'dc-select dc-unique-by', line);
  by.setAttribute('aria-label', 'The order a key\'s first row is first in');
  const first = el('option', '', by, 'its first column');
  first.value = '';
  for (const c of o.columns) {
    const opt = el('option', '', by, c);
    opt.value = c;
  }
  const direction = el('select', 'dc-select dc-unique-direction', line);
  direction.setAttribute('aria-label', 'Rising or falling');
  for (const [value, label] of [['asc', '↑ smallest first'], ['desc', '↓ largest first']] as const) {
    const opt = el('option', '', direction, label);
    opt.value = value;
  }
  el('p', 'dc-sample-hint', body, 'Its U output is a row of each key; its D output, every row after a key\'s first -- its duplicates.');
  const status = el('p', 'dc-sample-status', body);
  status.setAttribute('role', 'status');
  const foot = el('div', 'dc-sample-foot', body);
  el('span', 'dc-save-spacer', foot);
  const cancel = el('button', 'dc-picker-button dc-quiet', foot, 'Cancel');
  cancel.type = 'button';
  cancel.addEventListener('click', () => o.close());
  const ok = el('button', 'dc-picker-button dc-primary', foot, 'OK');
  ok.type = 'button';
  const keys = (): string[] => picks.filter((p) => p.checked).map((p) => p.value);
  const check = (): void => {
    ok.disabled = keys().length === 0;
    ok.title = ok.disabled ? 'Not yet: pick a column of its key' : '';
  };
  for (const p of picks) p.addEventListener('change', check);
  check();
  ok.addEventListener('click', () => void (async () => {
    const chosen = keys();
    const order: SortSpec | undefined = by.value === '' && direction.value === 'asc' ? undefined
      : { column: by.value === '' ? chosen[0]! : by.value, direction: direction.value === 'desc' ? 'desc' : 'asc' };
    status.textContent = '';
    const refused = await o.apply({ keys: chosen, ...(order !== undefined ? { by: order } : {}) });
    if (refused === null) o.close();
    else status.textContent = refused;
  })());
}
