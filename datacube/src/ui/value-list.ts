// THE FILTER VALUES WINDOW (Excel's autofilter checklist): a column's values with how many rows each has -- the
// database's answer to value-filter.ts's question -- a search, (Select All), a tick box per value; OK keeps the ticked
// ones (the grid's 'filter.values' gesture, value-filter.ts's rule). The window decides nothing: it shows and it ticks.

import { UI_LOCALE } from '../../../engine-client/src/locale.ts';
import type { FilterValue } from '../snapshot.ts';

/** One value offered: what it filters by (null: the blanks), the words it shows, how many rows have it. */
export interface OfferedValue {
  readonly value: FilterValue | null;
  readonly label: string;
  readonly rows: string;
}

export interface ValueListOptions {
  readonly values: readonly OfferedValue[];
  /** Ticked when the window opens: the values the column's own filter keeps; every value when it has none. */
  readonly ticked: (value: FilterValue | null) => boolean;
  /** More values than the list shows (cut at its cap): said, so a person knows the list is not all of them. */
  readonly cut: boolean;
  /** OK: the values kept, or null for every one (the column's own filter taken off). */
  readonly onOk: (kept: readonly (FilterValue | null)[] | null) => void;
  readonly onCancel: () => void;
}

/** The window's body: the search, the list, OK and Cancel. */
export function valueList(doc: Document, o: ValueListOptions): HTMLElement {
  const body = doc.createElement('div');
  body.className = 'dc-value-list';
  const search = doc.createElement('input');
  search.type = 'search';
  search.className = 'dc-value-search';
  search.placeholder = 'Search';
  search.setAttribute('aria-label', 'Search the values');

  const list = doc.createElement('div');
  list.className = 'dc-value-items';
  list.setAttribute('role', 'group');

  const row = (label: string, rows: string | null, checked: boolean): { el: HTMLLabelElement; box: HTMLInputElement } => {
    const el = doc.createElement('label');
    el.className = 'dc-value-item';
    const box = doc.createElement('input');
    box.type = 'checkbox';
    box.checked = checked;
    const text = doc.createElement('span');
    text.className = 'dc-value-label';
    text.textContent = label;
    el.append(box, text);
    if (rows !== null) {
      const count = doc.createElement('span');
      count.className = 'dc-value-count';
      count.textContent = rows;
      el.append(count);
    }
    return { el, box };
  };

  const all = row('(Select All)', null, false);
  all.el.classList.add('dc-value-all');
  const items = o.values.map((v) => ({ v, ...row(v.label, v.rows, o.ticked(v.value)) }));
  const shown = (): typeof items => items.filter((i) => !i.el.hidden);
  const sync = (): void => {
    const visible = shown();
    const on = visible.filter((i) => i.box.checked).length;
    all.box.checked = visible.length > 0 && on === visible.length;
    all.box.indeterminate = on > 0 && on < visible.length;
    ok.disabled = items.every((i) => !i.box.checked);
  };
  all.box.addEventListener('change', () => {
    for (const i of shown()) i.box.checked = all.box.checked;
    sync();
  });
  for (const i of items) i.box.addEventListener('change', sync);
  // a search shows the matching values; (Select All) then ticks the ones shown, as Excel's does
  search.addEventListener('input', () => {
    const q = search.value.trim().toLowerCase();
    for (const i of items) i.el.hidden = q !== '' && !i.v.label.toLowerCase().includes(q);
    sync();
  });
  list.append(all.el, ...items.map((i) => i.el));

  const foot = doc.createElement('div');
  foot.className = 'dc-value-foot';
  if (o.cut) {
    const note = doc.createElement('span');
    note.className = 'dc-value-cut';
    note.textContent = `The first ${o.values.length.toLocaleString(UI_LOCALE)} values`;
    foot.append(note);
  }
  const ok = doc.createElement('button');
  ok.type = 'button';
  ok.className = 'dc-button dc-button-primary';
  ok.textContent = 'OK';
  const cancel = doc.createElement('button');
  cancel.type = 'button';
  cancel.className = 'dc-button';
  cancel.textContent = 'Cancel';
  ok.addEventListener('click', () => {
    const kept = items.filter((i) => i.box.checked).map((i) => i.v.value);
    // every value kept, and the list not cut: no filter on the column at all
    o.onOk(kept.length === items.length && !o.cut ? null : kept);
  });
  cancel.addEventListener('click', () => o.onCancel());
  foot.append(cancel, ok);
  body.append(search, list, foot);
  sync();
  return body;
}
