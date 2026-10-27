// A GENERATE ROWS, SET (docs/DATACUBE_FLOW_MODE_2026_10_24.md M3: a flow's Generate Rows box, Alteryx's): its new
// column's name, and what each row is repeated for -- each whole number from one to another, each a number given or one
// of its whole-number columns, by a step; or each day between two of its date columns -- generate-rows.ts's rule, in a
// window its grid docks where its options dock it. Fetched with the first Generate Rows.

import type { Bound, GenerateRows } from '../generate-rows.ts';

export interface GenerateRowsFormOptions {
  /** Its input's whole-number columns, by name: a bound. */
  readonly wholes: readonly string[];
  /** Its input's date columns, by name: a days' bounds. */
  readonly dates: readonly string[];
  /** Its rows repeated: null when they were, else why not (the form stays, saying it). */
  apply(g: GenerateRows): Promise<string | null>;
  close(): void;
}

/** A bound's choice: a number typed, or a column picked. */
const TYPED = '';

/** The form, drawn into `host`. */
export function generateRowsForm(host: HTMLElement, o: GenerateRowsFormOptions): void {
  const doc = host.ownerDocument;
  const el = <K extends keyof HTMLElementTagNameMap>(tag: K, cls: string, parent: Element, text?: string): HTMLElementTagNameMap[K] => {
    const e = doc.createElement(tag);
    e.className = cls;
    if (text !== undefined) e.textContent = text;
    parent.append(e);
    return e;
  };
  const body = el('div', 'dc-sample dc-generate-rows', host);
  const nameLine = el('label', 'dc-text-tool-field', body);
  el('span', 'dc-picker-field-label', nameLine, 'Its new column');
  const name = el('input', 'dc-picker-input dc-generate-name', nameLine);
  name.value = 'N';
  name.setAttribute('aria-label', 'Its new column');
  const wayLine = el('div', 'dc-sample-line', body);
  el('span', 'dc-sample-word', wayLine, 'Each row repeated for');
  const way = el('select', 'dc-select dc-generate-way', wayLine);
  way.setAttribute('aria-label', 'What each row is repeated for');
  el('option', '', way, 'each number').value = 'numbers';
  if (o.dates.length > 0) el('option', '', way, 'each day between two dates').value = 'days';
  // numbers: from, to, each a number typed or a column picked; a step
  const numbers = el('div', 'dc-sample-line dc-generate-numbers', body);
  const bound = (word: string, start: string, cls: string): { readonly pick: HTMLSelectElement; readonly typed: HTMLInputElement } => {
    el('span', 'dc-sample-word', numbers, word);
    const pick = el('select', `dc-select ${cls}-column`, numbers);
    pick.setAttribute('aria-label', `Its ${word} column, or a number`);
    el('option', '', pick, 'a number').value = TYPED;
    for (const c of o.wholes) el('option', '', pick, c).value = c;
    const typed = el('input', `dc-picker-input dc-sample-n ${cls}`, numbers);
    typed.type = 'number';
    typed.step = '1';
    typed.value = start;
    typed.setAttribute('aria-label', `Its ${word} number`);
    return { pick, typed };
  };
  const from = bound('from', '1', 'dc-generate-from');
  const to = bound('to', '10', 'dc-generate-to');
  el('span', 'dc-sample-word', numbers, 'by');
  const step = el('input', 'dc-picker-input dc-sample-n dc-generate-step', numbers);
  step.type = 'number';
  step.min = '1';
  step.value = '1';
  step.setAttribute('aria-label', 'Its step');
  // days: from one date column to another
  const days = el('div', 'dc-sample-line dc-generate-days', body);
  const dateOf = (word: string, cls: string): HTMLSelectElement => {
    el('span', 'dc-sample-word', days, word);
    const pick = el('select', `dc-select ${cls}`, days);
    pick.setAttribute('aria-label', `Its ${word} date`);
    for (const c of o.dates) el('option', '', pick, c).value = c;
    return pick;
  };
  const fromDay = dateOf('from', 'dc-generate-from-day');
  const toDay = dateOf('to', 'dc-generate-to-day');
  if (o.dates.length > 1) toDay.selectedIndex = 1;
  el('p', 'dc-sample-hint', body, 'Each row once for each, its new column the number or the day, both kept: the database makes the rows. '
    + 'A row whose bound is empty is repeated no time.');
  const status = el('p', 'dc-sample-status', body);
  status.setAttribute('role', 'status');
  const foot = el('div', 'dc-sample-foot', body);
  el('span', 'dc-save-spacer', foot);
  const cancel = el('button', 'dc-picker-button dc-quiet', foot, 'Cancel');
  cancel.type = 'button';
  cancel.addEventListener('click', () => o.close());
  const ok = el('button', 'dc-picker-button dc-primary', foot, 'OK');
  ok.type = 'button';
  const boundOf = (b: { readonly pick: HTMLSelectElement; readonly typed: HTMLInputElement }): Bound =>
    (b.pick.value === TYPED ? Number(b.typed.value) : { column: b.pick.value });
  const check = (): void => {
    numbers.hidden = way.value !== 'numbers';
    days.hidden = way.value !== 'days';
    for (const b of [from, to]) b.typed.hidden = b.pick.value !== TYPED;
    const whole = (v: string): boolean => v.trim() !== '' && Number.isInteger(Number(v));
    const bad = name.value.trim() === '' ? 'name its new column'
      : way.value === 'numbers' && [from, to].some((b) => b.pick.value === TYPED && !whole(b.typed.value)) ? 'a bound is a whole number'
        : way.value === 'numbers' && !(whole(step.value) && Number(step.value) >= 1) ? 'a step is a whole number from 1' : undefined;
    ok.disabled = bad !== undefined;
    ok.title = bad === undefined ? '' : `Not yet: ${bad}`;
  };
  for (const e of [way, from.pick, to.pick]) e.addEventListener('change', check);
  for (const e of [name, from.typed, to.typed, step]) e.addEventListener('input', check);
  check();
  ok.addEventListener('click', () => void (async () => {
    status.textContent = '';
    const g: GenerateRows = way.value === 'days' ? { kind: 'days', name: name.value, from: fromDay.value, to: toDay.value }
      : { kind: 'numbers', name: name.value, from: boundOf(from), to: boundOf(to), ...(Number(step.value) !== 1 ? { step: Number(step.value) } : {}) };
    const refused = await o.apply(g);
    if (refused === null) o.close();
    else status.textContent = refused;
  })());
}
