// A FIND REPLACE, SET (docs/DATACUBE_FLOW_MODE_2026_10_24.md M2, Alteryx's Find Replace): the column of mine whose
// values its Join found among theirs, and the column of theirs each is replaced with -- find-replace.ts's rule -- in a
// window its grid docks where its options dock it. Fetched with the first.

import type { FindReplace } from '../find-replace.ts';

export interface FindReplaceFormOptions {
  /** Its grid's columns, in order: mine, then the columns of theirs its Join brings. */
  readonly columns: readonly string[];
  /** Where it starts: its own when it has one, else its Join's key and the last column of theirs. */
  readonly start?: Partial<FindReplace>;
  /** Its column replaced: null when it was, else why not (the form stays, saying it). */
  apply(t: FindReplace): Promise<string | null>;
  close(): void;
}

/** The form, drawn into `host`. */
export function findReplaceForm(host: HTMLElement, o: FindReplaceFormOptions): void {
  const doc = host.ownerDocument;
  const el = <K extends keyof HTMLElementTagNameMap>(tag: K, cls: string, parent: Element, text?: string): HTMLElementTagNameMap[K] => {
    const e = doc.createElement(tag);
    e.className = cls;
    if (text !== undefined) e.textContent = text;
    parent.append(e);
    return e;
  };
  const body = el('div', 'dc-sample dc-find-replace', host);
  const line = el('div', 'dc-sample-line', body);
  const pick = (word: string, cls: string, label: string, value: string | undefined): HTMLSelectElement => {
    el('span', 'dc-sample-word', line, word);
    const s = el('select', `dc-select ${cls}`, line);
    s.setAttribute('aria-label', label);
    for (const c of o.columns) el('option', '', s, c).value = c;
    if (value !== undefined && o.columns.includes(value)) s.value = value;
    return s;
  };
  const column = pick('Replace each', 'dc-find-column', 'The column replaced', o.start?.column);
  const by = pick('found with its', 'dc-find-with', 'The column of theirs it is replaced with', o.start?.with ?? o.columns.at(-1));
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
  const check = (): void => {
    const same = column.value === by.value;
    hint.textContent = same ? 'Replace a column with another: one of theirs its Join found.'
      : `Each ${column.value} its Join found among theirs becomes its ${by.value}; one it did not find stays as it is. Handed on as ${column.value}, ${by.value} left out.`;
    ok.disabled = same;
    ok.title = same ? 'Not yet: pick a column of theirs to replace it with' : '';
  };
  column.addEventListener('change', check);
  by.addEventListener('change', check);
  check();
  ok.addEventListener('click', () => void (async () => {
    status.textContent = '';
    const refused = await o.apply({ column: column.value, with: by.value });
    if (refused === null) o.close();
    else status.textContent = refused;
  })());
}
