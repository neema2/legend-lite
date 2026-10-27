// A CUSTOM FILTER, SET (docs/DATACUBE_FLOW_MODE_2026_10_24.md FL12: a flow's Custom Filter box, Alteryx's Filter, its
// Custom filter): its condition, written in Pure over `$x`, the row -- each of its input's columns a click away -- in a
// window its grid docks where its options dock it. The compiler parses and types it when OK is pressed, in its words.
// Fetched with the first Custom Filter.

export interface CustomFilterFormOptions {
  /** Its input's columns, by name: each inserted as `$x.<name>`. */
  readonly columns: readonly string[];
  /** Its condition now, as the compiler prints it ('' for a new one). */
  readonly condition: string;
  /** Its condition set: null when it is, else why not (the form stays, saying it). */
  apply(condition: string): Promise<string | null>;
  close(): void;
}

/** A column read off the row, as Pure writes it: quoted when it is no plain name. */
const read = (column: string): string => (/^[A-Za-z_]\w*$/.test(column) ? `$x.${column}` : `$x.'${column.replace(/'/g, "\\'")}'`);

/** The form, drawn into `host`. */
export function customFilterForm(host: HTMLElement, o: CustomFilterFormOptions): void {
  const doc = host.ownerDocument;
  const el = <K extends keyof HTMLElementTagNameMap>(tag: K, cls: string, parent: Element, text?: string): HTMLElementTagNameMap[K] => {
    const e = doc.createElement(tag);
    e.className = cls;
    if (text !== undefined) e.textContent = text;
    parent.append(e);
    return e;
  };
  const body = el('div', 'dc-sample dc-custom-filter', host);
  const field = el('label', 'dc-text-tool-field', body);
  el('span', 'dc-picker-field-label', field, 'Keep the rows for which, $x the row,');
  const condition = el('textarea', 'dc-picker-input dc-custom-filter-condition', field);
  condition.rows = 3;
  condition.spellcheck = false;
  condition.placeholder = "$x.notional > 1000 && $x.region == 'EMEA'";
  condition.value = o.condition;
  condition.setAttribute('aria-label', 'Its condition');
  // each column a click away, put in where the cursor is
  const chips = el('div', 'dc-custom-filter-columns', body);
  for (const c of o.columns) {
    const chip = el('button', 'dc-custom-filter-column', chips, c);
    chip.type = 'button';
    chip.title = `Put in ${read(c)}`;
    chip.addEventListener('click', () => {
      const at = condition.selectionStart ?? condition.value.length;
      const to = condition.selectionEnd ?? at;
      condition.setRangeText(read(c), at, to, 'end');
      condition.focus();
      check();
    });
  }
  el('p', 'dc-sample-hint', body, 'A condition true or false for each row, in Pure, $x the row. The database judges it; the rows '
    + 'it is not true for are its F.');
  const status = el('p', 'dc-sample-status', body);
  status.setAttribute('role', 'status');
  const foot = el('div', 'dc-sample-foot', body);
  el('span', 'dc-save-spacer', foot);
  const cancel = el('button', 'dc-picker-button dc-quiet', foot, 'Cancel');
  cancel.type = 'button';
  cancel.addEventListener('click', () => o.close());
  const ok = el('button', 'dc-picker-button dc-primary', foot, 'OK');
  ok.type = 'button';
  const check = (): void => {
    const bad = condition.value.trim() === '' ? 'write its condition' : undefined;
    ok.disabled = bad !== undefined;
    ok.title = bad === undefined ? '' : `Not yet: ${bad}`;
  };
  condition.addEventListener('input', check);
  check();
  ok.addEventListener('click', () => void (async () => {
    status.textContent = '';
    ok.disabled = true;
    const refused = await o.apply(condition.value);
    ok.disabled = false;
    if (refused === null) o.close();
    else status.textContent = refused;
  })());
}
