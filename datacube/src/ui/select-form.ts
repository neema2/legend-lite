// A SELECT, SET (docs/DATACUBE_FLOW_MODE_2026_10_24.md M1, Alteryx's Select): each column of the box's input kept or
// left out, renamed or not, retyped or not, moved up or down -- select-columns.ts's rule, said as Alteryx's Select lays
// it out (a row a column: kept, its name, its type, its new name; the rows in the order it hands them on) -- in a window
// its grid docks where its options dock it. Fetched with the first.

import type { Retype, SelectedColumn, Selection } from '../select-columns.ts';

export interface SelectFormOptions {
  /** Its columns, in order, as its Select has them now (select-columns.ts selectionOf). */
  readonly columns: readonly SelectedColumn[];
  /** Its columns set: null when they were, else why not (the form stays, saying it). */
  apply(selection: Selection): Promise<string | null>;
  close(): void;
}

/** The form, drawn into `host`. */
export function selectForm(host: HTMLElement, o: SelectFormOptions): void {
  const doc = host.ownerDocument;
  const el = <K extends keyof HTMLElementTagNameMap>(tag: K, cls: string, parent: Element, text?: string): HTMLElementTagNameMap[K] => {
    const e = doc.createElement(tag);
    e.className = cls;
    if (text !== undefined) e.textContent = text;
    parent.append(e);
    return e;
  };
  const body = el('div', 'dc-sample dc-select-form', host);
  const tools = el('div', 'dc-sample-line', body);
  const find = el('input', 'dc-picker-input dc-select-find', tools);
  find.type = 'search';
  find.placeholder = 'Find a column';
  find.setAttribute('aria-label', 'Find a column');
  const all = el('button', 'dc-picker-button dc-quiet dc-select-all', tools, 'All');
  all.type = 'button';
  const none = el('button', 'dc-picker-button dc-quiet dc-select-none', tools, 'None');
  none.type = 'button';
  // many renamed at once (Alteryx's Dynamic Rename): a prefix and a suffix given to each kept column in sight
  const names = el('div', 'dc-sample-line dc-select-names', body);
  el('span', 'dc-sample-word', names, 'Rename the kept ones');
  const prefix = el('input', 'dc-picker-input dc-select-prefix', names);
  prefix.placeholder = 'prefix_';
  prefix.setAttribute('aria-label', 'A prefix for each kept column');
  el('span', 'dc-sample-word', names, 'name');
  const suffix = el('input', 'dc-picker-input dc-select-suffix', names);
  suffix.placeholder = '_suffix';
  suffix.setAttribute('aria-label', 'A suffix for each kept column');
  const affix = el('button', 'dc-picker-button dc-quiet dc-select-affix', names, 'Rename');
  affix.type = 'button';
  const unname = el('button', 'dc-picker-button dc-quiet dc-select-unname', names, 'Own names');
  unname.type = 'button';
  unname.title = 'Each kept column in sight handed on by its own name again';
  const head = el('div', 'dc-select-row dc-select-head', body);
  for (const h of ['', 'Column', 'Type', 'Renamed', '']) el('span', '', head, h);
  const list = el('div', 'dc-select-list', body);
  const rows = o.columns.map((c) => {
    const row = el('label', 'dc-select-row', list);
    row.dataset['column'] = c.name;
    const keep = el('input', 'dc-select-keep', row);
    keep.type = 'checkbox';
    keep.checked = c.kept;
    keep.setAttribute('aria-label', `Keep ${c.name}`);
    el('span', 'dc-select-name', row, c.name);
    // its type: what it can be made, to choose among, its own first
    let retype: HTMLSelectElement | undefined;
    if (c.retypes.length === 0) el('span', 'dc-select-type', row, c.type);
    else {
      retype = el('select', 'dc-select dc-select-type dc-select-retype', row);
      retype.setAttribute('aria-label', `${c.name}'s type`);
      el('option', '', retype, c.type).value = '';
      for (const t of c.retypes) el('option', '', retype, `→ ${t}`).value = t;
      retype.value = c.retype ?? '';
    }
    const rename = el('input', 'dc-picker-input dc-select-rename', row);
    rename.value = c.as === c.name ? '' : c.as;
    rename.placeholder = c.name;
    rename.setAttribute('aria-label', `${c.name}'s new name`);
    // its place: moved up or down, as Alteryx's fields are (Alt+Up, Alt+Down from anywhere on its row)
    const move = el('span', 'dc-select-move', row);
    const up = el('button', 'dc-select-up', move, '↑');
    up.type = 'button';
    up.setAttribute('aria-label', `Move ${c.name} up`);
    const down = el('button', 'dc-select-down', move, '↓');
    down.type = 'button';
    down.setAttribute('aria-label', `Move ${c.name} down`);
    return { name: c.name, row, keep, rename, retype, up, down };
  });
  /** A row moved by `by` places (−1 up, +1 down), in the list and on screen. */
  const moveRow = (name: string, by: -1 | 1): void => {
    const i = rows.findIndex((r) => r.name === name);
    const j = i + by;
    if (i < 0 || j < 0 || j >= rows.length) return;
    const [r] = rows.splice(i, 1);
    rows.splice(j, 0, r!);
    list.insertBefore(r!.row, rows[j + 1]?.row ?? null);
    check();
  };
  const summary = el('p', 'dc-sample-hint', body);
  const status = el('p', 'dc-sample-status', body);
  status.setAttribute('role', 'status');
  const foot = el('div', 'dc-sample-foot', body);
  el('span', 'dc-save-spacer', foot);
  const cancel = el('button', 'dc-picker-button dc-quiet', foot, 'Cancel');
  cancel.type = 'button';
  cancel.addEventListener('click', () => o.close());
  const ok = el('button', 'dc-picker-button dc-primary', foot, 'OK');
  ok.type = 'button';
  const selection = (): Selection => ({
    keep: rows.filter((r) => r.keep.checked).map((r) => r.name),
    rename: Object.fromEntries(rows.filter((r) => r.rename.value.trim() !== '' && r.rename.value.trim() !== r.name)
      .map((r) => [r.name, r.rename.value.trim()])),
    retype: Object.fromEntries(rows.filter((r) => r.retype !== undefined && r.retype.value !== '').map((r) => [r.name, r.retype!.value as Retype])),
    order: rows.map((r) => r.name),
  });
  const check = (): void => {
    const t = selection();
    const renamed = Object.keys(t.rename).filter((c) => t.keep.includes(c)).length;
    const retyped = Object.keys(t.retype ?? {}).filter((c) => t.keep.includes(c)).length;
    summary.textContent = `${t.keep.length} of ${rows.length} columns kept${renamed > 0 ? `, ${renamed} renamed` : ''}${retyped > 0 ? `, ${retyped} retyped` : ''}: what the boxes after it read, by these names and types.`;
    for (const [i, r] of rows.entries()) {
      r.row.classList.toggle('dc-select-out', !r.keep.checked);
      r.up.disabled = i === 0;
      r.down.disabled = i === rows.length - 1;
    }
    ok.disabled = t.keep.length === 0;
    ok.title = t.keep.length === 0 ? 'Not yet: keep a column' : '';
  };
  for (const r of rows) {
    r.keep.addEventListener('change', check);
    r.rename.addEventListener('input', check);
    r.retype?.addEventListener('change', check);
    r.up.addEventListener('click', () => moveRow(r.name, -1));
    r.down.addEventListener('click', () => moveRow(r.name, 1));
    r.row.addEventListener('keydown', (e) => {
      if (!e.altKey || (e.key !== 'ArrowUp' && e.key !== 'ArrowDown')) return;
      e.preventDefault();
      moveRow(r.name, e.key === 'ArrowUp' ? -1 : 1);
    });
  }
  find.addEventListener('input', () => {
    const q = find.value.trim().toLowerCase();
    for (const r of rows) r.row.hidden = q !== '' && !r.name.toLowerCase().includes(q);
  });
  const every = (on: boolean): void => {
    for (const r of rows) if (!r.row.hidden) r.keep.checked = on;
    check();
  };
  all.addEventListener('click', () => every(true));
  none.addEventListener('click', () => every(false));
  const renameEach = (to: (name: string) => string): void => {
    for (const r of rows) if (!r.row.hidden && r.keep.checked) r.rename.value = to(r.name) === r.name ? '' : to(r.name);
    check();
  };
  affix.addEventListener('click', () => renameEach((n) => `${prefix.value.trim()}${n}${suffix.value.trim()}`));
  unname.addEventListener('click', () => renameEach((n) => n));
  check();
  ok.addEventListener('click', () => void (async () => {
    status.textContent = '';
    const refused = await o.apply(selection());
    if (refused === null) o.close();
    else status.textContent = refused;
  })());
}
