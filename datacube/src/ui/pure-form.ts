// A PURE BOX, SET (docs/DATACUBE_FLOW_MODE_2026_10_24.md FL12; pure-box.ts): its rows a function of its input's, written
// in Pure over `$input` -- its steps (a filter, a column, a grouping, ...) and its input's columns each a click away, put
// in where the cursor is -- in a window its grid docks where its options dock it. The compiler parses it and types its
// rows when OK is pressed, in its words. Fetched with the first Pure box.

export interface PureFormOptions {
  /** Its input's columns, by name: each put in as Pure writes a column's name. */
  readonly columns: readonly string[];
  /** Its Pure now, as the compiler prints it ('' for a new one). */
  readonly pure: string;
  /** Said as it opens (why its input's columns are not a click away), in the words of what refused them. */
  readonly note?: string;
  /** Its Pure set: null when it is, else why not (the form stays, saying it). */
  apply(pure: string): Promise<string | null>;
  /** Its Pure taken off, its rows its input's as they are: null when it is, else why not. Absent, it has none. */
  remove?(): Promise<string | null>;
  /** Its Pure taken apart into boxes, a step each (Import Pure, FL12: pure-import.ts) -- this box its first, a box after it
   *  each other: null when they are, else why not. Absent where boxes cannot be made after it. */
  importBoxes?(pure: string): Promise<string | null>;
  close(): void;
}

/** A step of a relation's, put in for the person to finish: its text, and where the cursor goes in it. */
interface Step {
  readonly label: string;
  readonly text: string;
  readonly at: number;
}

/** The steps a box's rows most often take, each a function of the relation API, the cursor left where it is to be
 *  finished. */
const STEPS: readonly Step[] = [
  { label: 'filter', text: '->filter(x|$x.)', at: 14 },
  { label: 'extend', text: '->extend(~new:x|$x.)', at: 19 },
  { label: 'groupBy', text: '->groupBy(~[], ~[total:x|$x.:y|$y->sum()])', at: 12 },
  { label: 'select', text: '->select(~[])', at: 11 },
  { label: 'sort', text: '->sort(~->descending())', at: 8 },
  { label: 'distinct', text: '->distinct()', at: 12 },
  { label: 'limit', text: '->limit(10)', at: 10 },
  { label: 'rename', text: '->rename(~, ~)', at: 10 },
];

/** A column's name as Pure writes it: quoted when it is no plain name. */
const named = (column: string): string => (/^[A-Za-z_]\w*$/.test(column) ? column : `'${column.replace(/'/g, "\\'")}'`);

/** The form, drawn into `host`. */
export function pureForm(host: HTMLElement, o: PureFormOptions): void {
  const doc = host.ownerDocument;
  const el = <K extends keyof HTMLElementTagNameMap>(tag: K, cls: string, parent: Element, text?: string): HTMLElementTagNameMap[K] => {
    const e = doc.createElement(tag);
    e.className = cls;
    if (text !== undefined) e.textContent = text;
    parent.append(e);
    return e;
  };
  const body = el('div', 'dc-sample dc-pure-box', host);
  const field = el('label', 'dc-text-tool-field', body);
  el('span', 'dc-picker-field-label', field, 'Its rows, $input its input\'s:');
  const pure = el('textarea', 'dc-picker-input dc-pure-text', field);
  pure.rows = 6;
  pure.spellcheck = false;
  pure.placeholder = '$input->filter(x|$x.notional > 1000000)->extend(~fee:x|$x.notional * 0.001)';
  pure.value = o.pure === '' ? '$input' : o.pure;
  pure.setAttribute('aria-label', 'Its Pure');
  const put = (text: string, at: number): void => {
    const from = pure.selectionStart ?? pure.value.length;
    const to = pure.selectionEnd ?? from;
    pure.setRangeText(text, from, to, 'end');
    pure.setSelectionRange(from + at, from + at);
    pure.focus();
    check();
  };
  // its steps, each put in where the cursor is, to be finished there
  const steps = el('div', 'dc-pure-steps', body);
  el('span', 'dc-pure-steps-label', steps, 'A step:');
  for (const s of STEPS) {
    const b = el('button', 'dc-pure-step', steps, s.label);
    b.type = 'button';
    b.dataset['step'] = s.label;
    b.title = `Put in ${s.text}`;
    b.addEventListener('click', () => put(s.text, s.at));
  }
  // each column a click away, put in where the cursor is
  const chips = el('div', 'dc-custom-filter-columns dc-pure-columns', body);
  for (const c of o.columns) {
    const chip = el('button', 'dc-custom-filter-column', chips, c);
    chip.type = 'button';
    chip.title = `Put in ${named(c)}`;
    chip.addEventListener('click', () => put(named(c), named(c).length));
  }
  el('p', 'dc-sample-hint', body, 'Its rows in Pure, a relation of its input\'s: the compiler checks it and the database computes '
    + 'it. What no other tool says -- a box after it reads its rows, its columns as the compiler types them.');
  const status = el('p', 'dc-sample-status', body);
  status.setAttribute('role', 'status');
  if (o.note !== undefined) status.textContent = o.note;
  const foot = el('div', 'dc-sample-foot', body);
  const remove = o.remove;
  if (remove !== undefined) {
    const off = el('button', 'dc-picker-button dc-quiet dc-pure-remove', foot, 'Take Its Pure Off');
    off.type = 'button';
    off.addEventListener('click', () => void (async () => {
      status.textContent = '';
      const refused = await remove();
      if (refused === null) o.close();
      else status.textContent = refused;
    })());
  }
  el('span', 'dc-save-spacer', foot);
  const cancel = el('button', 'dc-picker-button dc-quiet', foot, 'Cancel');
  cancel.type = 'button';
  cancel.addEventListener('click', () => o.close());
  // Import as Boxes: each step a box of its own, the flow then shows what it does
  const importBoxes = o.importBoxes;
  const split = importBoxes === undefined ? undefined : el('button', 'dc-picker-button dc-pure-import', foot, 'Import as Boxes');
  if (split !== undefined) {
    split.type = 'button';
    split.title = 'Each step of its Pure a box of its own -- this one its first, each after it over the one before';
    split.addEventListener('click', () => void (async () => {
      status.textContent = '';
      split.disabled = true;
      const refused = await importBoxes!(pure.value);
      split.disabled = false;
      if (refused === null) o.close();
      else status.textContent = refused;
    })());
  }
  const ok = el('button', 'dc-picker-button dc-primary', foot, 'OK');
  ok.type = 'button';
  const check = (): void => {
    const bad = pure.value.trim() === '' ? 'write its Pure' : undefined;
    ok.disabled = bad !== undefined;
    if (split !== undefined) split.disabled = bad !== undefined;
    ok.title = bad === undefined ? '' : `Not yet: ${bad}`;
  };
  pure.addEventListener('input', check);
  check();
  ok.addEventListener('click', () => void (async () => {
    status.textContent = '';
    ok.disabled = true;
    const refused = await o.apply(pure.value);
    ok.disabled = false;
    if (refused === null) o.close();
    else status.textContent = refused;
  })());
}
