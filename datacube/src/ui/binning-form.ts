// A BINNING, SET (docs/DATACUBE_FLOW_MODE_2026_10_24.md M3: a flow's Binning box, Alteryx's Multi-Field Binning): the
// number columns put into bins, and how many bins of equal width each one's range is cut into -- binning.ts's rule -- in
// a window its grid docks where its options dock it. Fetched with the first Binning.

import type { Binning } from '../binning.ts';

export interface BinningFormOptions {
  /** The number columns it can put into bins, by name. */
  readonly columns: readonly string[];
  /** Its columns put into bins: null when they were, else why not (the form stays, saying it). */
  apply(binning: Binning): Promise<string | null>;
  close(): void;
}

/** The form, drawn into `host`. */
export function binningForm(host: HTMLElement, o: BinningFormOptions): void {
  const doc = host.ownerDocument;
  const el = <K extends keyof HTMLElementTagNameMap>(tag: K, cls: string, parent: Element, text?: string): HTMLElementTagNameMap[K] => {
    const e = doc.createElement(tag);
    e.className = cls;
    if (text !== undefined) e.textContent = text;
    parent.append(e);
    return e;
  };
  const body = el('div', 'dc-sample dc-binning', host);
  const foot = (): HTMLElement => el('div', 'dc-sample-foot', body);
  if (o.columns.length === 0) {
    el('p', 'dc-sample-hint', body, 'Its input has no number column to put into bins.');
    const f = foot();
    el('span', 'dc-save-spacer', f);
    const close = el('button', 'dc-picker-button', f, 'Close');
    close.type = 'button';
    close.addEventListener('click', () => o.close());
    return;
  }
  el('span', 'dc-picker-field-label', body, 'Put into bins');
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
  el('span', 'dc-sample-word', line, 'each into');
  const bins = el('input', 'dc-picker-input dc-sample-n dc-binning-bins', line);
  bins.type = 'number';
  bins.min = '1';
  bins.step = '1';
  bins.value = '10';
  bins.setAttribute('aria-label', 'How many bins');
  el('span', 'dc-sample-word', line, 'bins of equal width');
  el('p', 'dc-sample-hint', body, "Each column's range, its smallest to its largest across every row, cut into bins of one width: a "
    + 'column of each value\'s bin beside it, its lower edge (notional_bins), the database placing each value. The largest is in the last bin.');
  const status = el('p', 'dc-sample-status', body);
  status.setAttribute('role', 'status');
  const f = foot();
  el('span', 'dc-save-spacer', f);
  const cancel = el('button', 'dc-picker-button dc-quiet', f, 'Cancel');
  cancel.type = 'button';
  cancel.addEventListener('click', () => o.close());
  const ok = el('button', 'dc-picker-button dc-primary', f, 'OK');
  ok.type = 'button';
  const chosen = (): string[] => picks.filter((p) => p.checked).map((p) => p.value);
  const check = (): void => {
    const n = Number(bins.value);
    const bad = chosen().length === 0 ? 'pick a column' : !Number.isInteger(n) || n < 1 ? 'a count of bins is a whole number from 1' : undefined;
    ok.disabled = bad !== undefined;
    ok.title = bad === undefined ? '' : `Not yet: ${bad}`;
  };
  for (const p of picks) p.addEventListener('change', check);
  bins.addEventListener('input', check);
  check();
  ok.addEventListener('click', () => void (async () => {
    status.textContent = '';
    const refused = await o.apply({ columns: chosen(), bins: Number(bins.value) });
    if (refused === null) o.close();
    else status.textContent = refused;
  })());
}
