// A MULTI-FIELD FORMULA, SET (docs/DATACUBE_FLOW_MODE_2026_10_24.md M3: a flow's Multi-Field Formula box, Alteryx's): the
// columns it is over -- shown by kind, text, numbers, dates or all, as Alteryx's field types are -- one expression over
// each one's value, `$v`, and the suffix its new columns are named with: multi-field.ts's rule, in a window its grid
// docks where its options dock it. The compiler parses and types the expression when OK is pressed, in its words.
// Fetched with the first Multi-Field Formula.

import { DEFAULT_SUFFIX, VALUE } from '../multi-field.ts';

/** A column it can be over: its name, and the kind of value it holds. */
export interface MultiFieldColumnChoice {
  readonly name: string;
  readonly family: 'text' | 'numeric' | 'temporal' | 'other';
}

export interface MultiFieldFormOptions {
  readonly columns: readonly MultiFieldColumnChoice[];
  /** Its columns added: null when they were, else why not (the form stays, saying it). */
  apply(columns: readonly string[], expression: string, suffix: string, inPlace: boolean): Promise<string | null>;
  close(): void;
}

const KINDS: readonly (readonly [MultiFieldColumnChoice['family'] | 'all', string])[] = [
  ['text', 'Text'], ['numeric', 'Numbers'], ['temporal', 'Dates'], ['all', 'All'],
];

/** Examples of an expression over each kind, the first offered when the kind is chosen (a value that can be empty,
 *  as a file's can, `toOne` first for what takes one value). */
const EXAMPLES: Readonly<Record<MultiFieldColumnChoice['family'] | 'all', string>> = {
  text: `$${VALUE}->toOne()->trim()->toUpper()`,
  numeric: `$${VALUE}->toOne() * 1.1`,
  temporal: `$${VALUE}->monthNumber()`,
  other: `$${VALUE}`,
  all: `$${VALUE}->isEmpty()`,
};

/** The form, drawn into `host`. */
export function multiFieldForm(host: HTMLElement, o: MultiFieldFormOptions): void {
  const doc = host.ownerDocument;
  const el = <K extends keyof HTMLElementTagNameMap>(tag: K, cls: string, parent: Element, text?: string): HTMLElementTagNameMap[K] => {
    const e = doc.createElement(tag);
    e.className = cls;
    if (text !== undefined) e.textContent = text;
    parent.append(e);
    return e;
  };
  const body = el('div', 'dc-sample dc-multi-field', host);
  if (o.columns.length === 0) {
    el('p', 'dc-sample-hint', body, 'Its input has no column yet.');
    const f = el('div', 'dc-sample-foot', body);
    el('span', 'dc-save-spacer', f);
    const close = el('button', 'dc-picker-button', f, 'Close');
    close.type = 'button';
    close.addEventListener('click', () => o.close());
    return;
  }
  const line = el('div', 'dc-sample-line', body);
  el('span', 'dc-sample-word', line, 'Over the');
  const kinds = KINDS.filter(([k]) => k === 'all' || o.columns.some((c) => c.family === k));
  const kind = el('select', 'dc-select dc-multi-field-kind', line);
  kind.setAttribute('aria-label', 'The kind of column');
  for (const [k, label] of kinds) el('option', '', kind, label).value = k;
  el('span', 'dc-sample-word', line, 'columns');
  const list = el('div', 'dc-text-tool-columns', body);
  const picks = o.columns.map((c) => {
    const l = el('label', 'dc-text-tool-check', list);
    l.dataset['family'] = c.family;
    const box = el('input', '', l);
    box.type = 'checkbox';
    box.value = c.name;
    el('span', '', l, c.name);
    return { box, label: l, family: c.family };
  });
  const exprLine = el('label', 'dc-text-tool-field', body);
  el('span', 'dc-picker-field-label', exprLine, 'Each one\'s value, $v, as');
  const expression = el('textarea', 'dc-picker-input dc-multi-field-expression', exprLine);
  expression.rows = 2;
  expression.spellcheck = false;
  expression.setAttribute('aria-label', 'Its expression');
  let typed = false;
  expression.addEventListener('input', () => { typed = true; });
  const suffixLine = el('label', 'dc-text-tool-field', body);
  el('span', 'dc-picker-field-label', suffixLine, 'Named with');
  const suffix = el('input', 'dc-picker-input dc-multi-field-suffix', suffixLine);
  suffix.value = DEFAULT_SUFFIX;
  suffix.setAttribute('aria-label', 'The suffix of its columns\' names');
  // in place (Alteryx's "change existing fields"): each new column handed on by its column's name
  const placed = el('label', 'dc-text-tool-check dc-in-place', body);
  const inPlace = el('input', '', placed);
  inPlace.type = 'checkbox';
  el('span', '', placed, 'In place: each handed on by its own name');
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
  const chosen = (): string[] => picks.filter((p) => p.box.checked && !p.label.hidden).map((p) => p.box.value);
  const check = (): void => {
    const k = kind.value as MultiFieldColumnChoice['family'] | 'all';
    for (const p of picks) p.label.hidden = k !== 'all' && p.family !== k;
    if (!typed) expression.value = EXAMPLES[k];
    const names = chosen();
    hint.textContent = names.length === 0 ? 'A new column of each column picked, $v its value: $x is the row, for its other columns.'
      : inPlace.checked ? `Each changed, handed on by its own name: ${names.slice(0, 3).join(', ')}${names.length > 3 ? ', …' : ''}`
        : `New columns: ${names.slice(0, 3).map((n) => `${n}${suffix.value.trim()}`).join(', ')}${names.length > 3 ? ', …' : ''}`;
    const bad = names.length === 0 ? 'pick a column' : expression.value.trim() === '' ? 'type its expression'
      : suffix.value.trim() === '' ? 'type the suffix of its names' : undefined;
    ok.disabled = bad !== undefined;
    ok.title = bad === undefined ? '' : `Not yet: ${bad}`;
  };
  for (const p of picks) p.box.addEventListener('change', check);
  kind.addEventListener('change', check);
  expression.addEventListener('input', check);
  suffix.addEventListener('input', check);
  inPlace.addEventListener('change', check);
  check();
  ok.addEventListener('click', () => void (async () => {
    status.textContent = '';
    ok.disabled = true;
    const refused = await o.apply(chosen(), expression.value, suffix.value, inPlace.checked);
    ok.disabled = false;
    if (refused === null) o.close();
    else status.textContent = refused;
  })());
}
