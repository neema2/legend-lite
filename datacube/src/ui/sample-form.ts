// A SAMPLE, SET (docs/DATACUBE_FLOW_MODE_2026_10_24.md M3: a flow's Sample box, Alteryx's Sample): its first or last N
// rows, all but its first N, or one row of every N -- in the order of a column, of all its rows or of each group of some
// columns -- a random percent, or a value oversampled (Alteryx's Oversample Field) -- row-sample.ts's rule (sampleCommands: the first or last of all of them the grid's own Top N, any other a
// row number and a filter), said as Alteryx's Sample says it, in a window its grid docks where its options dock it.
// Fetched with the first Sample.

import { rangesWords, readRanges, type RecordRange, type RowSample, type RowSampleKind } from '../row-sample.ts';
import { CubeRefusal } from '../snapshot.ts';

export interface SampleFormOptions {
  /** The columns it can be ordered or grouped by. */
  readonly columns: readonly string[];
  /** The sample the grid has now, if any: where the form starts. */
  readonly now: RowSample | undefined;
  /** Its rows sampled: null when they were, else why not (the form stays, saying it). */
  apply(sample: RowSample): Promise<string | null>;
  /** Every row kept again (its sample taken off). */
  clear(): Promise<string | null>;
  close(): void;
}

const MODES: readonly (readonly [RowSampleKind, string])[] = [
  ['first', 'first'], ['last', 'last'], ['skip', 'all but the first'], ['every', 'one of every'], ['records', 'the rows at'],
  ['random', 'a random'], ['oversample', 'every row of a value, and others to make it'],
];

/** What each way keeps, under the form. */
const HINTS: Readonly<Record<RowSampleKind, string>> = {
  first: 'Its smallest by that column -- of all the rows (rows that tie are kept), or of each group.',
  last: 'Its largest by that column -- of all the rows (rows that tie are kept), or of each group.',
  skip: 'Every row after the first ones by that column -- of all the rows, or of each group.',
  every: 'The first row by that column, and every one that many rows after it -- of all the rows, or of each group.',
  records: 'The rows at those places by that column: 1-10, 20, 30+ (from the 30th on), -5 (the first 5) -- of all the rows, or of each group.',
  random: 'About that percent of the rows, each kept by the database\'s hash of it and the seed: the same rows each time it runs, others for another seed.',
  oversample: 'Every row of that value, and as many of the others -- chosen by the database\'s hash and the seed -- as make it that percent of the rows: the same rows each time it runs.',
};

/** The form, drawn into `host`. */
export function sampleForm(host: HTMLElement, o: SampleFormOptions): void {
  const doc = host.ownerDocument;
  const el = <K extends keyof HTMLElementTagNameMap>(tag: K, cls: string, parent: Element, text?: string): HTMLElementTagNameMap[K] => {
    const e = doc.createElement(tag);
    e.className = cls;
    if (text !== undefined) e.textContent = text;
    parent.append(e);
    return e;
  };
  const body = el('div', 'dc-sample', host);
  const line = el('div', 'dc-sample-line', body);
  el('span', 'dc-sample-word', line, 'Keep');
  const which = el('select', 'dc-select dc-sample-which', line);
  which.setAttribute('aria-label', 'Which rows');
  for (const [value, label] of MODES) el('option', '', which, label).value = value;
  const n = el('input', 'dc-picker-input dc-sample-n', line);
  n.type = 'number';
  n.min = '1';
  n.step = '1';
  n.setAttribute('aria-label', 'How many rows');
  // a Select Records' places, written as Alteryx writes them
  const places = el('input', 'dc-picker-input dc-sample-ranges', line);
  places.placeholder = '1-10, 20, 30+';
  places.setAttribute('aria-label', 'The rows it keeps');
  // a random sample's percent's word, and its seed
  const percent = el('span', 'dc-sample-word dc-sample-percent', line, '% of the rows, seed');
  const seed = el('input', 'dc-picker-input dc-sample-seed', line);
  seed.type = 'number';
  seed.min = '0';
  seed.step = '1';
  seed.setAttribute('aria-label', 'Its seed: the same seed, the same rows');
  const orderWords = el('span', 'dc-sample-word', line, 'rows, in the order of');
  const by = el('select', 'dc-select dc-sample-by', line);
  by.setAttribute('aria-label', 'The column it is ordered by');
  for (const c of o.columns) el('option', '', by, c).value = c;
  // an oversample's: the share its value is, the value, its seed
  const shareWords = el('span', 'dc-sample-word dc-sample-share', line, '% of the rows: those whose');
  line.insertBefore(shareWords, by);
  const eqWords = el('span', 'dc-sample-word', line, 'is');
  const value = el('input', 'dc-picker-input dc-sample-value', line);
  value.setAttribute('aria-label', 'The value it keeps every row of');
  const seedWords = el('span', 'dc-sample-word', line, ', seed');
  const overSeed = el('input', 'dc-picker-input dc-sample-over-seed', line);
  overSeed.type = 'number';
  overSeed.min = '0';
  overSeed.step = '1';
  overSeed.setAttribute('aria-label', 'Its seed: the same seed, the same rows');
  // of each group: the columns a group is, none for all the rows
  const grouped = el('label', 'dc-text-tool-check dc-sample-grouped', body);
  const groupOn = el('input', '', grouped);
  groupOn.type = 'checkbox';
  el('span', '', grouped, 'Of each group of');
  const list = el('div', 'dc-text-tool-columns dc-sample-groups', body);
  const picks = o.columns.map((c) => {
    const l = el('label', 'dc-text-tool-check', list);
    const box = el('input', '', l);
    box.type = 'checkbox';
    box.value = c;
    el('span', '', l, c);
    return box;
  });
  const hint = el('p', 'dc-sample-hint', body);
  const status = el('p', 'dc-sample-status', body);
  status.setAttribute('role', 'status');
  const now = o.now;
  n.value = String(now?.kind === 'records' ? 10 : now?.n ?? 10);
  places.value = now?.ranges !== undefined ? rangesWords(now.ranges) : '';
  seed.value = String(now?.seed ?? 1);
  overSeed.value = String(now?.seed ?? 1);
  value.value = now?.value !== undefined ? String(now.value) : '';
  which.value = now?.kind ?? 'first';
  if (now !== undefined && o.columns.includes(now.by)) by.value = now.by;
  groupOn.checked = (now?.groups.length ?? 0) > 0;
  for (const p of picks) p.checked = now?.groups.includes(p.value) ?? false;

  const foot = el('div', 'dc-sample-foot', body);
  const clear = el('button', 'dc-picker-button dc-quiet', foot, 'Keep every row');
  clear.type = 'button';
  clear.disabled = now === undefined;
  el('span', 'dc-save-spacer', foot);
  const cancel = el('button', 'dc-picker-button dc-quiet', foot, 'Cancel');
  cancel.type = 'button';
  const ok = el('button', 'dc-picker-button dc-primary', foot, 'OK');
  ok.type = 'button';
  const done = async (made: Promise<string | null>): Promise<void> => {
    status.textContent = '';
    const refused = await made;
    if (refused === null) o.close();
    else status.textContent = refused;
  };
  const groups = (): string[] => (groupOn.checked ? picks.filter((p) => p.checked).map((p) => p.value) : []);
  const ranges = (): RecordRange[] | undefined => {
    try {
      return readRanges(places.value);
    } catch (e) {
      if (e instanceof CubeRefusal) return undefined;
      throw e;
    }
  };
  const check = (): void => {
    const mode = which.value as RowSampleKind;
    list.hidden = !groupOn.checked;
    hint.textContent = HINTS[mode];
    n.hidden = mode === 'records';
    places.hidden = mode !== 'records';
    // a random sample is of all the rows, ordered by none of its columns
    const random = mode === 'random';
    percent.hidden = !random;
    seed.hidden = !random;
    for (const e of [orderWords, by, grouped]) e.hidden = random;
    if (random) list.hidden = true;
    // an oversample is of all the rows, its column's value of interest its own
    const over = mode === 'oversample';
    for (const e of [shareWords, eqWords, value, seedWords, overSeed]) e.hidden = !over;
    if (over) {
      orderWords.hidden = true;
      grouped.hidden = true;
      list.hidden = true;
    }
    const count = Number(n.value);
    const pct = Number(n.value);
    const bad = random ? (!Number.isInteger(pct) || pct < 1 || pct > 99 ? 'a whole percent, from 1 to 99'
      : !Number.isInteger(Number(seed.value)) || Number(seed.value) < 0 || seed.value === '' ? 'a seed: a whole number from 0' : undefined)
      : over ? (!Number.isInteger(pct) || pct < 1 || pct > 99 ? 'a whole percent, from 1 to 99' : by.value === '' ? 'the column its value is of'
        : value.value === '' ? 'the value it keeps every row of'
          : !Number.isInteger(Number(overSeed.value)) || Number(overSeed.value) < 0 || overSeed.value === '' ? 'a seed: a whole number from 0' : undefined)
      : mode === 'records' ? (ranges() === undefined ? 'its rows written as 1-10, 20, 30+' : by.value === '' ? 'a column to order them by'
      : groupOn.checked && groups().length === 0 ? 'a column of each group' : undefined)
      : !Number.isInteger(count) || count < 1 ? 'a whole number of rows, from 1'
      : mode === 'every' && count < 2 ? 'one of every 2 rows or more'
        : by.value === '' ? 'a column to order them by'
          : groupOn.checked && groups().length === 0 ? 'a column of each group' : undefined;
    ok.disabled = bad !== undefined;
    ok.title = bad === undefined ? '' : `Not yet: ${bad}`;
  };
  for (const e of [which, by, groupOn, ...picks]) e.addEventListener('change', check);
  n.addEventListener('input', check);
  places.addEventListener('input', check);
  seed.addEventListener('input', check);
  overSeed.addEventListener('input', check);
  value.addEventListener('input', check);
  check();
  cancel.addEventListener('click', () => o.close());
  clear.addEventListener('click', () => void done(o.clear()));
  ok.addEventListener('click', () => {
    const kind = which.value as RowSampleKind;
    const kept = kind === 'records' ? ranges() ?? [] : undefined;
    if (kind === 'random') {
      void done(o.apply({ kind, n: Number(n.value), by: '', groups: [], seed: Number(seed.value) }));
      return;
    }
    // its value as typed: the column's type reads it (query.ts literalNode)
    if (kind === 'oversample') {
      void done(o.apply({ kind, n: Number(n.value), by: by.value, groups: [], seed: Number(overSeed.value), value: value.value }));
      return;
    }
    void done(o.apply({ kind, n: kept?.length ?? Number(n.value), by: by.value, groups: groups(), ...(kept !== undefined ? { ranges: kept } : {}) }));
  });
  n.addEventListener('keydown', (e) => {
    if (e.key === 'Enter' && !ok.disabled) ok.click();
  });
}
