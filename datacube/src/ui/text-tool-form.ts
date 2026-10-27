// A TEXT TOOL, SET (docs/DATACUBE_FLOW_MODE_2026_10_24.md M3: a flow's Text To Columns, RegEx and Data Cleansing): the
// calculated columns text-tools.ts makes, said as each tool is -- a column, and what is done to it -- in a window its
// grid docks where its options dock it. Fetched with the first of them.

import { regexName, textToolProblem } from '../text-tool-columns.ts';
import type { Cleansing, RegexMode, TextTool } from '../text-tools.ts';

export interface TextToolFormOptions {
  /** Which tool. */
  readonly kind: TextTool['kind'];
  /** The text columns it can work on, by name. */
  readonly columns: readonly string[];
  /** Its columns added: null when they were, else why not (the form stays, saying it). */
  apply(tool: TextTool): Promise<string | null>;
  close(): void;
}

const MODES: readonly { readonly mode: RegexMode; readonly label: string; readonly says: string }[] = [
  { mode: 'match', label: 'Match', says: 'A true or false column: whether its value matches the pattern.' },
  { mode: 'replace', label: 'Replace', says: 'Its value with each match of the pattern replaced.' },
  { mode: 'extract', label: 'Extract', says: 'The first part of its value the pattern matches.' },
];

/** The form, drawn into `host`. */
export function textToolForm(host: HTMLElement, o: TextToolFormOptions): void {
  const doc = host.ownerDocument;
  const el = <K extends keyof HTMLElementTagNameMap>(tag: K, cls: string, parent: Element, text?: string): HTMLElementTagNameMap[K] => {
    const e = doc.createElement(tag);
    e.className = cls;
    if (text !== undefined) e.textContent = text;
    parent.append(e);
    return e;
  };
  const body = el('div', `dc-sample dc-text-tool-form dc-text-tool-${o.kind}`, host);
  const foot = (): { ok: HTMLButtonElement; status: HTMLParagraphElement } => {
    const status = el('p', 'dc-sample-status', body);
    status.setAttribute('role', 'status');
    const f = el('div', 'dc-sample-foot', body);
    el('span', 'dc-save-spacer', f);
    const cancel = el('button', 'dc-picker-button dc-quiet', f, 'Cancel');
    cancel.type = 'button';
    cancel.addEventListener('click', () => o.close());
    const ok = el('button', 'dc-picker-button dc-primary', f, 'OK');
    ok.type = 'button';
    return { ok, status };
  };
  if (o.columns.length === 0) {
    el('p', 'dc-sample-hint', body, 'Its input has no text column.');
    const f = el('div', 'dc-sample-foot', body);
    el('span', 'dc-save-spacer', f);
    const close = el('button', 'dc-picker-button', f, 'Close');
    close.type = 'button';
    close.addEventListener('click', () => o.close());
    return;
  }
  const field = (label: string, input: HTMLElement): HTMLLabelElement => {
    const l = el('label', 'dc-text-tool-field', body);
    el('span', 'dc-picker-field-label', l, label);
    l.append(input);
    return l;
  };
  const select = (cls: string, aria: string, options: readonly { value: string; label: string }[]): HTMLSelectElement => {
    const s = doc.createElement('select');
    s.className = `dc-select ${cls}`;
    s.setAttribute('aria-label', aria);
    for (const opt of options) {
      const e = el('option', '', s, opt.label);
      e.value = opt.value;
    }
    return s;
  };
  const input = (cls: string, aria: string, value: string): HTMLInputElement => {
    const i = doc.createElement('input');
    i.className = `dc-picker-input ${cls}`;
    i.setAttribute('aria-label', aria);
    i.value = value;
    return i;
  };
  const columnOptions = o.columns.map((c) => ({ value: c, label: c }));
  let read: () => TextTool;

  switch (o.kind) {
    case 'split': {
      const column = select('dc-text-tool-column', 'The column to split', columnOptions);
      field('Column', column);
      const row = el('div', 'dc-sample-line', body);
      el('span', 'dc-sample-word', row, 'Split on');
      const delimiter = input('dc-text-tool-delimiter', 'The delimiter', ',');
      row.append(delimiter);
      el('span', 'dc-sample-word', row, 'into');
      const parts = input('dc-sample-n dc-text-tool-parts', 'How many columns', '2');
      parts.type = 'number';
      parts.min = '1';
      parts.max = '50';
      row.append(parts);
      // into columns, a part each, or into rows, a row a part (Alteryx's Split to rows)
      const into = doc.createElement('select');
      into.className = 'dc-select dc-text-tool-into';
      into.setAttribute('aria-label', 'Columns or rows');
      for (const [value, label] of [['columns', 'columns'], ['rows', 'rows: a row a part']] as const) {
        const opt = doc.createElement('option');
        opt.value = value;
        opt.textContent = label;
        into.append(opt);
      }
      row.append(into);
      const name = input('dc-text-tool-name', 'Their name', column.value);
      field('Named', name);
      const hint = el('p', 'dc-sample-hint', body);
      const shape = (): void => {
        const rows = into.value === 'rows';
        parts.hidden = rows;
        hint.textContent = rows ? 'One column, the name: each row once for each of its parts -- a, b, c three rows.'
          : 'Its columns are the name and each part\'s number: region_1, region_2… A value with fewer parts has its last columns empty.';
      };
      into.addEventListener('change', () => { shape(); check(); });
      shape();
      // the name follows the column until it is typed
      let named = false;
      name.addEventListener('input', () => { named = true; });
      column.addEventListener('change', () => { if (!named) name.value = column.value; });
      read = () => ({ kind: 'split', column: column.value, delimiter: delimiter.value, parts: Number(parts.value), name: name.value,
        ...(into.value === 'rows' ? { rows: true } : {}) });
      for (const i of [delimiter, parts, name]) i.addEventListener('input', () => check());
      break;
    }
    case 'regex': {
      const column = select('dc-text-tool-column', 'The column it reads', columnOptions);
      field('Column', column);
      const pattern = input('dc-text-tool-pattern', 'The pattern', '');
      pattern.placeholder = 'a regular expression, as [0-9]+';
      field('Pattern', pattern);
      const mode = select('dc-text-tool-mode', 'What it does', MODES.map((m) => ({ value: m.mode, label: m.label })));
      field('Output', mode);
      const says = el('p', 'dc-sample-hint', body);
      const replacement = input('dc-text-tool-replacement', 'Replaced by', '');
      const replacing = field('Replaced by', replacement);
      const name = input('dc-text-tool-name', 'Its name', '');
      field('Named', name);
      let named = false;
      const suggest = (): void => {
        if (!named) name.value = regexName(column.value, mode.value as RegexMode);
      };
      const shown = (): void => {
        replacing.hidden = mode.value !== 'replace';
        says.textContent = MODES.find((m) => m.mode === mode.value)!.says;
        suggest();
        check();
      };
      name.addEventListener('input', () => { named = true; check(); });
      column.addEventListener('change', shown);
      mode.addEventListener('change', shown);
      for (const i of [pattern, replacement]) i.addEventListener('input', () => check());
      read = () => ({
        kind: 'regex', column: column.value, pattern: pattern.value, mode: mode.value as RegexMode, name: name.value,
        ...(mode.value === 'replace' ? { replacement: replacement.value } : {}),
      });
      queueMicrotask(shown);
      break;
    }
    case 'cleanse': {
      el('span', 'dc-picker-field-label', body, 'Columns');
      const list = el('div', 'dc-text-tool-columns', body);
      const picks = o.columns.map((c) => {
        const l = el('label', 'dc-text-tool-check', list);
        const box = el('input', '', l);
        box.type = 'checkbox';
        box.value = c;
        el('span', '', l, c);
        box.addEventListener('change', () => check());
        return box;
      });
      el('span', 'dc-picker-field-label', body, 'What it does to them');
      const options = el('div', 'dc-text-tool-columns', body);
      const option = (label: string): HTMLInputElement => {
        const l = el('label', 'dc-text-tool-check', options);
        const box = el('input', '', l);
        box.type = 'checkbox';
        el('span', '', l, label);
        box.addEventListener('change', () => check());
        return box;
      };
      const blanks = option('Empty values made blank');
      const punctuation = option('Punctuation taken out');
      const spaces = option('Repeated spaces made one');
      const trim = option('Spaces at the ends taken off');
      trim.checked = true;
      const letterCase = select('dc-text-tool-case', 'Its letters', [
        { value: '', label: 'as they are' }, { value: 'upper', label: 'UPPER CASE' }, { value: 'lower', label: 'lower case' },
      ]);
      field('Letters', letterCase);
      letterCase.addEventListener('change', () => check());
      // in place (Alteryx's "change existing fields"): each handed on by its own name
      const placed = el('label', 'dc-text-tool-check dc-in-place', body);
      const inPlace = el('input', '', placed);
      inPlace.type = 'checkbox';
      inPlace.checked = true;
      el('span', '', placed, 'In place: each handed on by its own name');
      const hint = el('p', 'dc-sample-hint', body);
      const said = (): void => {
        hint.textContent = inPlace.checked ? 'Each cleansed, the box after it reading it by its name: region, cleansed.'
          : 'A new column of each, named after it: region_clean.';
      };
      inPlace.addEventListener('change', said);
      said();
      read = () => {
        const cleansing: Cleansing = {
          ...(blanks.checked ? { blanks: true } : {}), ...(punctuation.checked ? { punctuation: true } : {}),
          ...(spaces.checked ? { spaces: true } : {}), ...(trim.checked ? { trim: true } : {}),
          ...(letterCase.value === 'upper' || letterCase.value === 'lower' ? { letterCase: letterCase.value } : {}),
        };
        return { kind: 'cleanse', columns: picks.filter((p) => p.checked).map((p) => p.value), cleansing, ...(inPlace.checked ? { inPlace: true } : {}) };
      };
      break;
    }
  }
  const { ok, status } = foot();
  const check = (): void => {
    const problem = textToolProblem(read());
    ok.disabled = problem !== undefined;
    ok.title = problem === undefined ? '' : `Not yet: ${problem}`;
  };
  check();
  ok.addEventListener('click', () => void (async () => {
    status.textContent = '';
    const refused = await o.apply(read());
    if (refused === null) o.close();
    else status.textContent = refused;
  })());
}
