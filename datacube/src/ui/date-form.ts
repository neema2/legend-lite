// A DATETIME, SET (docs/DATACUBE_FLOW_MODE_2026_10_24.md M3: a flow's DateTime box, Alteryx's DateTime): one of four
// ways, each a calculated column -- a date taken to its PERIOD (the grid's own period preset, grid-presets.ts `grain`:
// its day, week, month, quarter or year, the period's first day), a date's PART (date-tools.ts: its year, its month's
// number, its day, its week, its hour), TEXT READ AS A DATE by a format, or a DATE WRITTEN AS TEXT by one (date-tools.ts)
// -- in a window its grid docks where its options dock it. Fetched with the first DateTime.

import { GRAIN_LABELS } from '../calc-presets.ts';
import type { TimeGrain } from '../chart-spec.ts';
import { DATE_FORMATS, PART_WORDS, TEXT_FORMATS, dateToolName, formatHasTime, type DatePart, type DateTool } from '../date-tools.ts';

export interface DateFormOptions {
  /** The date columns it can take to a period or a part, by name. */
  readonly columns: readonly string[];
  /** The text columns it can read as dates, by name. */
  readonly texts: readonly string[];
  /** A column of `column` taken to `grain`, added. */
  apply(column: string, grain: TimeGrain): Promise<string | null>;
  /** A date tool's column (a part, text read), added. */
  applyTool(tool: DateTool): Promise<string | null>;
  close(): void;
}

type Mode = 'period' | 'part' | 'parse' | 'write';

/** The form, drawn into `host`. */
export function dateForm(host: HTMLElement, o: DateFormOptions): void {
  const doc = host.ownerDocument;
  const el = <K extends keyof HTMLElementTagNameMap>(tag: K, cls: string, parent: Element, text?: string): HTMLElementTagNameMap[K] => {
    const e = doc.createElement(tag);
    e.className = cls;
    if (text !== undefined) e.textContent = text;
    parent.append(e);
    return e;
  };
  const select = (cls: string, aria: string, parent: Element, options: readonly (readonly [string, string])[]): HTMLSelectElement => {
    const s = el('select', `dc-select ${cls}`, parent);
    s.setAttribute('aria-label', aria);
    for (const [value, label] of options) el('option', '', s, label).value = value;
    return s;
  };
  const body = el('div', 'dc-sample dc-date', host);
  if (o.columns.length === 0 && o.texts.length === 0) {
    el('p', 'dc-sample-hint', body, 'Its input has no date column, and no text to read as one.');
    const foot = el('div', 'dc-sample-foot', body);
    el('span', 'dc-save-spacer', foot);
    const close = el('button', 'dc-picker-button', foot, 'Close');
    close.type = 'button';
    close.addEventListener('click', () => o.close());
    return;
  }
  const modes: (readonly [Mode, string])[] = [
    ...(o.columns.length > 0 ? [['period', 'Take a date to its period'] as const, ['part', 'Take a part of a date'] as const] : []),
    ...(o.texts.length > 0 ? [['parse', 'Read text as a date'] as const] : []),
    ...(o.columns.length > 0 ? [['write', 'Write a date as text'] as const] : []),
  ];
  const mode = select('dc-date-mode', 'What it does', body, modes);
  // a date to its period
  const period = el('div', 'dc-sample-line', body);
  el('span', 'dc-sample-word', period, 'Take');
  const column = select('dc-date-column', 'The date column', period, o.columns.map((c) => [c, c]));
  el('span', 'dc-sample-word', period, 'to its');
  const grain = select('dc-date-grain', 'The period', period, Object.entries(GRAIN_LABELS).map(([g, l]) => [g, l.toLowerCase()]));
  grain.value = 'month';
  // a date's part
  const part = el('div', 'dc-sample-line', body);
  el('span', 'dc-sample-word', part, 'Its');
  const which = select('dc-date-part', 'The part', part, Object.entries(PART_WORDS));
  el('span', 'dc-sample-word', part, 'of');
  const partOf = select('dc-date-part-column', 'The date column', part, o.columns.map((c) => [c, c]));
  // text read as a date
  const parse = el('div', 'dc-sample-line', body);
  el('span', 'dc-sample-word', parse, 'Read');
  const text = select('dc-date-text', 'The text column', parse, o.texts.map((c) => [c, c]));
  el('span', 'dc-sample-word', parse, 'as');
  const format = el('input', 'dc-picker-input dc-date-format', parse);
  format.value = DATE_FORMATS[0]!;
  format.setAttribute('aria-label', 'Its format');
  format.setAttribute('list', 'dc-date-formats');
  const formats = el('datalist', '', parse);
  formats.id = 'dc-date-formats';
  for (const f of DATE_FORMATS) el('option', '', formats).value = f;
  // a date written as text
  const write = el('div', 'dc-sample-line', body);
  el('span', 'dc-sample-word', write, 'Write');
  const written = select('dc-date-written', 'The date column', write, o.columns.map((c) => [c, c]));
  el('span', 'dc-sample-word', write, 'as');
  const textFormat = el('input', 'dc-picker-input dc-date-text-format', write);
  textFormat.value = TEXT_FORMATS[0]!;
  textFormat.setAttribute('aria-label', 'Its format');
  textFormat.setAttribute('list', 'dc-date-text-formats');
  const textFormats = el('datalist', '', write);
  textFormats.id = 'dc-date-text-formats';
  for (const f of TEXT_FORMATS) el('option', '', textFormats).value = f;
  const hint = el('p', 'dc-sample-hint', body);
  const nameLine = el('label', 'dc-text-tool-field', body);
  el('span', 'dc-picker-field-label', nameLine, 'Named');
  const name = el('input', 'dc-picker-input dc-date-name', nameLine);
  name.setAttribute('aria-label', 'Its name');
  let named = false;
  name.addEventListener('input', () => { named = true; });
  const status = el('p', 'dc-sample-status', body);
  status.setAttribute('role', 'status');
  const foot = el('div', 'dc-sample-foot', body);
  el('span', 'dc-save-spacer', foot);
  const cancel = el('button', 'dc-picker-button dc-quiet', foot, 'Cancel');
  cancel.type = 'button';
  const ok = el('button', 'dc-picker-button dc-primary', foot, 'OK');
  ok.type = 'button';
  const shown = (): void => {
    const m = mode.value as Mode;
    period.hidden = m !== 'period';
    part.hidden = m !== 'part';
    parse.hidden = m !== 'parse';
    write.hidden = m !== 'write';
    // a period's name is its preset's (a name the grid has gets a number); the others are named here
    nameLine.hidden = m === 'period';
    hint.textContent = m === 'period' ? 'A new column: each date taken to its period\'s first day (a day, its date).'
      : m === 'part' ? 'A new column of whole numbers: the part of each date.'
        : m === 'parse' ? 'A new column of dates: the text read by its format, as yyyy-MM-dd, dd/MM/yyyy or yyyy-MM-dd HH:mm:ss. A value it cannot read stops its rows, said by the database: filter such text out before it.'
          : 'A new column of text: each date written by its format, as dd/MM/yyyy, yyyyMMdd or yyyy-MM-dd HH:mm:ss (an empty date, an empty text).';
    if (!named) {
      name.value = m === 'part' ? dateToolName('datePart', partOf.value, which.value as DatePart)
        : m === 'write' ? dateToolName('dateFormat', written.value) : dateToolName('dateParse', text.value);
    }
    ok.disabled = (m === 'parse' && format.value.trim() === '') || (m === 'write' && textFormat.value.trim() === '');
  };
  for (const s of [mode, which, partOf, text, written]) s.addEventListener('change', shown);
  format.addEventListener('input', shown);
  textFormat.addEventListener('input', shown);
  shown();
  cancel.addEventListener('click', () => o.close());
  ok.addEventListener('click', () => void (async () => {
    const m = mode.value as Mode;
    const asked = m === 'period' ? o.apply(column.value, grain.value as TimeGrain)
      : o.applyTool(m === 'part'
        ? { kind: 'datePart', column: partOf.value, part: which.value as DatePart, name: name.value }
        : m === 'write' ? { kind: 'dateFormat', column: written.value, format: textFormat.value.trim(), name: name.value }
          : { kind: 'dateParse', column: text.value, format: format.value.trim(), time: formatHasTime(format.value), name: name.value });
    status.textContent = '';
    const refused = await asked;
    if (refused === null) o.close();
    else status.textContent = refused;
  })());
}
