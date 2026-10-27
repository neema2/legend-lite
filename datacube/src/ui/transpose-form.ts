// TRANSPOSE, SET (docs/DATACUBE_FLOW_MODE_2026_10_24.md M4: a flow's Transpose box, Alteryx's): its key columns, kept
// beside the rows, and its data columns, made rows -- a Name and a Value for each -- transpose.ts's rule, said as
// Alteryx's Transpose says it, in a window its grid docks where its options dock it. Fetched with the first Transpose.

import { TRANSPOSE_NAME, TRANSPOSE_VALUE, type Transpose } from '../transpose.ts';

export interface TransposeFormOptions {
  /** The grid's columns, by name, and whether each holds numbers (its data columns to start with). */
  readonly columns: readonly { readonly name: string; readonly numeric: boolean }[];
  /** Its columns made rows: null when they were, else why not (the form stays, saying it). */
  apply(transpose: Transpose): Promise<string | null>;
  close(): void;
}

/** The form, drawn into `host`. */
export function transposeForm(host: HTMLElement, o: TransposeFormOptions): void {
  const doc = host.ownerDocument;
  const el = <K extends keyof HTMLElementTagNameMap>(tag: K, cls: string, parent: Element, text?: string): HTMLElementTagNameMap[K] => {
    const e = doc.createElement(tag);
    e.className = cls;
    if (text !== undefined) e.textContent = text;
    parent.append(e);
    return e;
  };
  const body = el('div', 'dc-sample dc-transpose', host);
  // each column one of three: a key kept beside the rows, a column made rows, or left out
  const head = el('div', 'dc-transpose-row dc-transpose-head', body);
  for (const h of ['Column', 'Key', 'Made rows']) el('span', '', head, h);
  const list = el('div', 'dc-transpose-list', body);
  const rows = o.columns.map((c, i) => {
    const row = el('div', 'dc-transpose-row', list);
    row.dataset['column'] = c.name;
    el('span', 'dc-transpose-name', row, c.name);
    const key = el('input', 'dc-transpose-key', row);
    key.type = 'checkbox';
    key.setAttribute('aria-label', `${c.name} a key`);
    const data = el('input', 'dc-transpose-data', row);
    data.type = 'checkbox';
    data.setAttribute('aria-label', `${c.name} made rows`);
    // to start: the number columns made rows, the first other column a key
    data.checked = c.numeric;
    key.checked = !c.numeric && o.columns.findIndex((x) => !x.numeric) === i;
    // a column is a key or made rows, never both
    key.addEventListener('change', () => { if (key.checked) data.checked = false; check(); });
    data.addEventListener('change', () => { if (data.checked) key.checked = false; check(); });
    return { name: c.name, key, data };
  });
  const names = el('div', 'dc-sample-line', body);
  el('span', 'dc-sample-word', names, 'Named');
  const name = el('input', 'dc-picker-input dc-transpose-name-input', names);
  name.value = TRANSPOSE_NAME;
  name.setAttribute('aria-label', 'The name column\'s name');
  el('span', 'dc-sample-word', names, 'and');
  const value = el('input', 'dc-picker-input dc-transpose-value-input', names);
  value.value = TRANSPOSE_VALUE;
  value.setAttribute('aria-label', 'The value column\'s name');
  const hint = el('p', 'dc-sample-hint', body);
  const status = el('p', 'dc-sample-status', body);
  status.setAttribute('role', 'status');
  const foot = el('div', 'dc-sample-foot', body);
  el('span', 'dc-save-spacer', foot);
  const cancel = el('button', 'dc-picker-button dc-quiet', foot, 'Cancel');
  cancel.type = 'button';
  cancel.addEventListener('click', () => o.close());
  const ok = el('button', 'dc-picker-button dc-primary', foot, 'OK');
  ok.type = 'button';
  const keys = (): string[] => rows.filter((r) => r.key.checked).map((r) => r.name);
  const data = (): string[] => rows.filter((r) => r.data.checked).map((r) => r.name);
  function check(): void {
    const made = data();
    hint.textContent = made.length === 0 ? 'Pick the columns made rows: each row is repeated once for each, its name and its value side by side.'
      : `Each row ${made.length === 1 ? 'once' : `${made.length} times`}: ${keys().join(', ') || 'no key'}, then ${name.value || 'Name'} and ${value.value || 'Value'}. The other columns are left out.`;
    const bad = made.length === 0 ? 'pick a column to make rows of' : name.value.trim() === '' || value.value.trim() === '' ? 'name its two columns' : undefined;
    ok.disabled = bad !== undefined;
    ok.title = bad === undefined ? '' : `Not yet: ${bad}`;
  }
  name.addEventListener('input', check);
  value.addEventListener('input', check);
  check();
  ok.addEventListener('click', () => void (async () => {
    status.textContent = '';
    const refused = await o.apply({ columns: data(), keys: keys(), name: name.value, value: value.value });
    if (refused === null) o.close();
    else status.textContent = refused;
  })());
}
