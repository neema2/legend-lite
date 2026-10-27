// A DATE FILTER, SET (docs/DATACUBE_FLOW_MODE_2026_10_24.md M3: a flow's Date Filter box, Alteryx's): a date column, and
// the range of its dates kept -- one relative to when the query runs (date-ranges.ts DATE_RANGES, the header menu's Date
// Range), or the days between two -- in a window its grid docks where its options dock it. Fetched with the first.

import type { DateRange } from '../date-ranges.ts';
import type { DatesKept } from '../page-filters.ts';

export interface DateFilterFormOptions {
  /** The date columns it can filter, by name. */
  readonly columns: readonly string[];
  /** The ranges relative to today it offers, each in words (date-ranges.ts DATE_RANGES, given: none of its code here). */
  readonly ranges: readonly { readonly range: DateRange; readonly label: string }[];
  /** What it keeps now, when it is a Date Filter already. */
  readonly kept?: DatesKept;
  /** Its dates kept: null when they are, else why not (the form stays, saying it). */
  apply(kept: DatesKept): Promise<string | null>;
  close(): void;
}

/** The value of the way "between two days" in its list of ways. */
const BETWEEN = 'between';

/** The form, drawn into `host`. */
export function dateFilterForm(host: HTMLElement, o: DateFilterFormOptions): void {
  const doc = host.ownerDocument;
  const el = <K extends keyof HTMLElementTagNameMap>(tag: K, cls: string, parent: Element, text?: string): HTMLElementTagNameMap[K] => {
    const e = doc.createElement(tag);
    e.className = cls;
    if (text !== undefined) e.textContent = text;
    parent.append(e);
    return e;
  };
  const body = el('div', 'dc-sample dc-date-filter', host);
  const foot = (): HTMLElement => el('div', 'dc-sample-foot', body);
  if (o.columns.length === 0) {
    el('p', 'dc-sample-hint', body, 'Its input has no date column to filter.');
    const f = foot();
    el('span', 'dc-save-spacer', f);
    const close = el('button', 'dc-picker-button', f, 'Close');
    close.type = 'button';
    close.addEventListener('click', () => o.close());
    return;
  }
  const line = el('div', 'dc-sample-line', body);
  el('span', 'dc-sample-word', line, 'Keep the rows whose');
  const column = el('select', 'dc-select dc-date-filter-column', line);
  column.setAttribute('aria-label', 'The date column');
  for (const c of o.columns) {
    const opt = el('option', '', column, c);
    opt.value = c;
  }
  if (o.kept !== undefined && o.columns.includes(o.kept.column)) column.value = o.kept.column;
  el('span', 'dc-sample-word', line, 'is');
  const way = el('select', 'dc-select dc-date-filter-way', line);
  way.setAttribute('aria-label', 'The range of its dates');
  for (const r of o.ranges) {
    const opt = el('option', '', way, r.label);
    opt.value = r.range;
  }
  const between = el('option', '', way, 'Between two days');
  between.value = BETWEEN;
  way.value = o.kept === undefined ? 'last30Days' : 'range' in o.kept ? o.kept.range : BETWEEN;
  // the days between: both kept
  const days = el('div', 'dc-sample-line dc-date-filter-days', body);
  el('span', 'dc-sample-word', days, 'from');
  const from = el('input', 'dc-picker-input dc-date-filter-from', days);
  from.type = 'date';
  from.setAttribute('aria-label', 'Its first day');
  el('span', 'dc-sample-word', days, 'to');
  const to = el('input', 'dc-picker-input dc-date-filter-to', days);
  to.type = 'date';
  to.setAttribute('aria-label', 'Its last day');
  if (o.kept !== undefined && 'from' in o.kept) {
    from.value = o.kept.from;
    to.value = o.kept.to;
  }
  el('p', 'dc-sample-hint', body, 'A range is from when the query runs: a page saved with "Last 30 Days" keeps the last 30 days '
    + 'whenever it is opened, the database counting them. Between two days keeps both. The rows it leaves out are its F.');
  const status = el('p', 'dc-sample-status', body);
  status.setAttribute('role', 'status');
  const f = foot();
  el('span', 'dc-save-spacer', f);
  const cancel = el('button', 'dc-picker-button dc-quiet', f, 'Cancel');
  cancel.type = 'button';
  cancel.addEventListener('click', () => o.close());
  const ok = el('button', 'dc-picker-button dc-primary', f, 'OK');
  ok.type = 'button';
  const check = (): void => {
    days.hidden = way.value !== BETWEEN;
    const bad = way.value !== BETWEEN ? undefined : from.value === '' || to.value === '' ? 'pick its first and last day'
      : from.value > to.value ? 'its first day is after its last' : undefined;
    ok.disabled = bad !== undefined;
    ok.title = bad === undefined ? '' : `Not yet: ${bad}`;
  };
  for (const e of [way, from, to]) e.addEventListener(e === way ? 'change' : 'input', check);
  check();
  ok.addEventListener('click', () => void (async () => {
    const kept: DatesKept = way.value === BETWEEN ? { column: column.value, from: from.value, to: to.value }
      : { column: column.value, range: way.value as DateRange };
    status.textContent = '';
    const refused = await o.apply(kept);
    if (refused === null) o.close();
    else status.textContent = refused;
  })());
}
