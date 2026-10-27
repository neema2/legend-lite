// A TEST, SET (docs/DATACUBE_FLOW_MODE_2026_10_24.md FL8: a flow's Test box, Alteryx's Test tool): its checks, a row
// each -- what it checks (no empty value, no value repeated, a number within bounds, its count of rows within bounds,
// its values one of a list, a text matching a pattern) and of which column (a count of rows of none), a between's
// smallest and largest, a list's values or a pattern -- added and taken off, in a window its grid docks where its
// options dock it. Fetched with the first Test.

import type { Check } from '../checks.ts';
import { closeMark } from './tile-icons.ts';

export interface TestFormOptions {
  /** The columns it can check, each by name and whether it is a number (only a number is checked within bounds). */
  readonly columns: readonly { readonly name: string; readonly number: boolean }[];
  /** Its checks now (none: a new Test). */
  readonly checks: readonly Check[];
  /** Its checks set: null when they are, else why not (the form stays, saying it). */
  apply(checks: readonly Check[]): Promise<string | null>;
  close(): void;
}

const KINDS: readonly { readonly kind: Check['kind']; readonly label: string }[] = [
  { kind: 'notEmpty', label: 'has no empty value' },
  { kind: 'unique', label: 'repeats no value' },
  { kind: 'between', label: 'is within bounds' },
  { kind: 'rows', label: 'rows: a count within bounds' },
  { kind: 'oneOf', label: 'is one of' },
  { kind: 'matches', label: 'matches a pattern' },
];

/** A check given as text: a list of values, a pattern. */
const texted = (kind: string): boolean => kind === 'oneOf' || kind === 'matches';

/** A check with bounds: a between, a count of rows. */
const bounded = (kind: string): boolean => kind === 'between' || kind === 'rows';

/** The form, drawn into `host`. */
export function testForm(host: HTMLElement, o: TestFormOptions): void {
  const doc = host.ownerDocument;
  const el = <K extends keyof HTMLElementTagNameMap>(tag: K, cls: string, parent: Element, text?: string): HTMLElementTagNameMap[K] => {
    const e = doc.createElement(tag);
    e.className = cls;
    if (text !== undefined) e.textContent = text;
    parent.append(e);
    return e;
  };
  const body = el('div', 'dc-sample dc-test', host);
  el('span', 'dc-picker-field-label', body, 'Each row of its input passes every check');
  const list = el('div', 'dc-test-checks', body);
  const rows: {
    readonly kind: HTMLSelectElement; readonly column: HTMLSelectElement; readonly min: HTMLInputElement; readonly max: HTMLInputElement;
    readonly text: HTMLInputElement;
  }[] = [];
  const status = el('p', 'dc-sample-status', body);
  status.setAttribute('role', 'status');
  const ok = doc.createElement('button');
  const check = (): void => {
    for (const r of rows) {
      const between = r.kind.value === 'between';
      r.min.hidden = !bounded(r.kind.value);
      r.max.hidden = !bounded(r.kind.value);
      // a count of rows is of no column
      r.column.hidden = r.kind.value === 'rows';
      // a list of values, or a pattern: as text
      r.text.hidden = !texted(r.kind.value);
      r.text.placeholder = r.kind.value === 'oneOf' ? 'its values, a comma between' : 'its pattern: ^[A-Z]{3}$';
      r.text.setAttribute('aria-label', r.kind.value === 'oneOf' ? 'Its values' : 'Its pattern');
      // a bound is a number's: a between offers the number columns, the others every column
      for (const opt of r.column.options) opt.hidden = between && !o.columns.find((c) => c.name === opt.value)?.number;
    }
    const bad = rows.length === 0 ? 'add a check'
      : rows.some((r) => bounded(r.kind.value) && r.min.value === '' && r.max.value === '') ? 'give a bound its smallest, its largest, or both'
        : rows.some((r) => r.kind.value === 'between' && !o.columns.find((c) => c.name === r.column.value)?.number) ? 'bound a number column'
          : rows.filter((r) => r.kind.value === 'rows').length > 1 ? 'check its count of rows once'
            : rows.some((r) => texted(r.kind.value) && r.text.value.trim() === '') ? 'give its values or its pattern'
              : rows.some((r) => r.kind.value === 'matches' && o.columns.find((c) => c.name === r.column.value)?.number) ? 'a pattern is a text\'s'
                : undefined;
    ok.disabled = bad !== undefined;
    ok.title = bad === undefined ? '' : `Not yet: ${bad}`;
  };
  const add = (c: Check | undefined): void => {
    const line = el('div', 'dc-sample-line dc-test-check', list);
    const column = el('select', 'dc-select dc-test-column', line);
    column.setAttribute('aria-label', 'The column it checks');
    for (const c2 of o.columns) {
      const opt = el('option', '', column, c2.name);
      opt.value = c2.name;
    }
    const kind = el('select', 'dc-select dc-test-kind', line);
    kind.setAttribute('aria-label', 'What it checks');
    for (const k of KINDS) {
      const opt = el('option', '', kind, k.label);
      opt.value = k.kind;
    }
    const bound = (what: string, cls: string): HTMLInputElement => {
      const b = el('input', `dc-picker-input dc-sample-n ${cls}`, line);
      b.type = 'number';
      b.placeholder = what;
      b.setAttribute('aria-label', `Its ${what}`);
      return b;
    };
    const min = bound('smallest', 'dc-test-min');
    const max = bound('largest', 'dc-test-max');
    const text = el('input', 'dc-picker-input dc-test-text', line);
    text.type = 'text';
    text.spellcheck = false;
    const off = closeMark(el('button', 'dc-flow-input-off dc-test-off', line), 'Take this check off');
    off.type = 'button';
    off.title = 'Take this check off';
    off.setAttribute('aria-label', 'Take this check off');
    const row = { kind, column, min, max, text };
    off.addEventListener('click', () => {
      rows.splice(rows.indexOf(row), 1);
      line.remove();
      check();
    });
    if (c !== undefined) {
      if (c.kind !== 'rows') column.value = c.column;
      kind.value = c.kind;
      if (c.kind === 'between' || c.kind === 'rows') {
        if (c.min !== undefined) min.value = String(c.min);
        if (c.max !== undefined) max.value = String(c.max);
      }
      if (c.kind === 'oneOf') text.value = c.values.join(', ');
      if (c.kind === 'matches') text.value = c.pattern;
    }
    for (const e of [kind, column]) e.addEventListener('change', check);
    for (const e of [min, max, text]) e.addEventListener('input', check);
    rows.push(row);
    check();
  };
  const more = el('button', 'dc-picker-button dc-quiet dc-test-add', body, 'Add a check');
  more.type = 'button';
  more.addEventListener('click', () => add(undefined));
  el('p', 'dc-sample-hint', body, 'Its row counts the rows that fail each check, the database counting them; its box says which fail, '
    + 'and so does a run of the flow. A count of rows is of its whole input: an input of no rows fails a smallest.');
  body.append(status);
  const foot = el('div', 'dc-sample-foot', body);
  el('span', 'dc-save-spacer', foot);
  const cancel = el('button', 'dc-picker-button dc-quiet', foot, 'Cancel');
  cancel.type = 'button';
  cancel.addEventListener('click', () => o.close());
  ok.className = 'dc-picker-button dc-primary';
  ok.type = 'button';
  ok.textContent = 'OK';
  foot.append(ok);
  for (const c of o.checks.length > 0 ? o.checks : [undefined]) add(c);
  ok.addEventListener('click', () => void (async () => {
    const number = (v: string): number | undefined => (v === '' ? undefined : Number(v));
    const checks = rows.map((r): Check => {
      const kind = r.kind.value as Check['kind'];
      if (kind === 'notEmpty' || kind === 'unique') return { kind, column: r.column.value };
      if (kind === 'matches') return { kind, column: r.column.value, pattern: r.text.value };
      if (kind === 'oneOf') {
        // a number column's values as numbers (the rule refuses one that is none, in words)
        const numbers = o.columns.find((c) => c.name === r.column.value)?.number === true;
        const values = r.text.value.split(',').map((v) => v.trim()).filter((v) => v !== '');
        return { kind, column: r.column.value, values: numbers ? values.map((v) => (Number.isFinite(Number(v)) ? Number(v) : v)) : values };
      }
      const min = number(r.min.value);
      const max = number(r.max.value);
      const bounds = { ...(min !== undefined ? { min } : {}), ...(max !== undefined ? { max } : {}) };
      return kind === 'rows' ? { kind, ...bounds } : { kind: 'between', column: r.column.value, ...bounds };
    });
    status.textContent = '';
    const refused = await o.apply(checks);
    if (refused === null) o.close();
    else status.textContent = refused;
  })());
}
