// IMPUTATION, SET (docs/DATACUBE_FLOW_MODE_2026_10_24.md M3: a flow's Imputation box): the number columns whose empties
// are filled, and what with -- impute.ts's rule, said as Alteryx's Imputation says it -- in a window its grid docks
// where its options dock it. Fetched with the first Imputation.

import type { Filler, Imputation } from '../impute.ts';

export interface ImputeFormOptions {
  /** The number columns it can fill, by name. */
  readonly columns: readonly string[];
  /** Their empties filled: null when they were, else why not (the form stays, saying it). */
  apply(imputation: Imputation): Promise<string | null>;
  close(): void;
}

const FILLERS: readonly { readonly kind: Filler['kind']; readonly label: string }[] = [
  { kind: 'average', label: 'its average' },
  { kind: 'smallest', label: 'its smallest' },
  { kind: 'largest', label: 'its largest' },
  { kind: 'value', label: 'a value' },
];

/** The form, drawn into `host`. */
export function imputeForm(host: HTMLElement, o: ImputeFormOptions): void {
  const doc = host.ownerDocument;
  const el = <K extends keyof HTMLElementTagNameMap>(tag: K, cls: string, parent: Element, text?: string): HTMLElementTagNameMap[K] => {
    const e = doc.createElement(tag);
    e.className = cls;
    if (text !== undefined) e.textContent = text;
    parent.append(e);
    return e;
  };
  const body = el('div', 'dc-sample dc-impute', host);
  if (o.columns.length === 0) {
    el('p', 'dc-sample-hint', body, 'Its input has no number column to fill.');
    const f = el('div', 'dc-sample-foot', body);
    el('span', 'dc-save-spacer', f);
    const close = el('button', 'dc-picker-button', f, 'Close');
    close.type = 'button';
    close.addEventListener('click', () => o.close());
    return;
  }
  el('span', 'dc-picker-field-label', body, 'Fill the empty values of');
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
  el('span', 'dc-sample-word', line, 'with');
  const filler = el('select', 'dc-select dc-impute-with', line);
  filler.setAttribute('aria-label', 'What fills them');
  for (const f of FILLERS) {
    const opt = el('option', '', filler, f.label);
    opt.value = f.kind;
  }
  const value = el('input', 'dc-picker-input dc-sample-n dc-impute-value', line);
  value.type = 'number';
  value.value = '0';
  value.setAttribute('aria-label', 'The value');
  // in place, as Alteryx's Imputation is: each filled column handed on by its own name
  const placed = el('label', 'dc-text-tool-check dc-in-place', body);
  const inPlace = el('input', '', placed);
  inPlace.type = 'checkbox';
  inPlace.checked = true;
  el('span', '', placed, 'In place: each handed on by its own name');
  el('p', 'dc-sample-hint', body, 'Its empties filled -- with the column\'s average, smallest or largest across every row, or a value -- in place, or a new column of each: notional_filled.');
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
    value.hidden = filler.value !== 'value';
    const bad = keys().length === 0 ? 'pick a column to fill'
      : filler.value === 'value' && (value.value === '' || !Number.isFinite(Number(value.value))) ? 'type the value' : undefined;
    ok.disabled = bad !== undefined;
    ok.title = bad === undefined ? '' : `Not yet: ${bad}`;
  };
  for (const p of picks) p.addEventListener('change', check);
  filler.addEventListener('change', check);
  value.addEventListener('input', check);
  check();
  ok.addEventListener('click', () => void (async () => {
    const kind = filler.value as Filler['kind'];
    const fill: Filler = kind === 'value' ? { kind, value: Number(value.value) } : { kind };
    status.textContent = '';
    const refused = await o.apply({ columns: keys(), with: fill, ...(inPlace.checked ? { inPlace: true } : {}) });
    if (refused === null) o.close();
    else status.textContent = refused;
  })());
}
