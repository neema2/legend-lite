// THE GROUP VALUES WINDOW (Tableau's Create Group, Power BI's Groups): a column's values -- the database's, as the Filter
// Values window lists them -- ticked and named into groups, the groups listed with a Remove, a name for the values in
// no group (blank: each its own, on a text column), and the column's name. OK makes the column (snapshot.ts ValueGroups)
// by the grid's 'column.calculate' gesture, or remakes it ('column.editCalculated'); the rules are the command's
// (grid-commands.ts groupsFrom, groupsProblem), said here as it refuses. The window decides nothing: it ticks and names.

import type { FilterValue, ValueGroups } from '../snapshot.ts';
import type { OfferedValue } from './value-list.ts';

export interface GroupValuesOptions {
  readonly column: string;
  readonly values: readonly OfferedValue[];
  /** The groups the column has now (Edit Groups...), else none. */
  readonly groups?: ValueGroups;
  /** The column's name: its own, editing; else the one suggested. */
  readonly name: string;
  /** OK: the column's name and its groups; a refusal in words to say, or null when it is made. */
  readonly onOk: (name: string, groups: ValueGroups) => Promise<string | null>;
  readonly onCancel: () => void;
}

const keyOf = (v: FilterValue | null): string => JSON.stringify(v);

/** The window's body. */
export function groupValues(doc: Document, o: GroupValuesOptions): HTMLElement {
  const body = doc.createElement('div');
  body.className = 'dc-group-values';
  const labelOf = new Map(o.values.map((v) => [keyOf(v.value), v.label]));
  // the groups as they are being made: their names, their values in order
  const groups: { name: string; values: (FilterValue | null)[] }[] = (o.groups?.groups ?? []).map((g) => ({ name: g.name, values: [...g.values] }));
  const ticked = new Set<string>();

  const list = doc.createElement('div');
  list.className = 'dc-value-items';
  list.setAttribute('role', 'group');
  const shown = doc.createElement('div');
  shown.className = 'dc-groups';
  const said = doc.createElement('div');
  said.className = 'dc-group-problem';
  said.setAttribute('role', 'alert');

  const groupOf = (v: FilterValue | null): string | undefined => groups.find((g) => g.values.some((x) => keyOf(x) === keyOf(v)))?.name;

  const groupName = input(doc, 'dc-group-name', 'Group name', `Group ${groups.length + 1}`);
  const otherName = input(doc, 'dc-group-other', 'Values in no group', o.groups?.other ?? '');
  otherName.placeholder = '(each its own)';
  const columnName = input(doc, 'dc-group-column', 'Column name', o.name);

  const draw = (): void => {
    list.replaceChildren(...o.values.map((v) => {
      const row = doc.createElement('label');
      row.className = 'dc-value-item';
      const box = doc.createElement('input');
      box.type = 'checkbox';
      const key = keyOf(v.value);
      const inGroup = groupOf(v.value);
      box.checked = ticked.has(key);
      // a value in a group is its group's: taken out by removing the group
      box.disabled = inGroup !== undefined;
      box.addEventListener('change', () => {
        if (box.checked) ticked.add(key);
        else ticked.delete(key);
      });
      const text = doc.createElement('span');
      text.className = 'dc-value-label';
      text.textContent = inGroup !== undefined ? `${v.label} (${inGroup})` : v.label;
      row.append(box, text);
      return row;
    }));
    shown.replaceChildren(...groups.map((g, i) => {
      const row = doc.createElement('div');
      row.className = 'dc-group';
      const words = doc.createElement('span');
      words.className = 'dc-group-words';
      words.textContent = `${g.name}: ${g.values.map((v) => labelOf.get(keyOf(v)) ?? (v === null ? '(Blank)' : String(v))).join(', ')}`;
      const remove = doc.createElement('button');
      remove.type = 'button';
      remove.className = 'dc-button';
      remove.textContent = 'Remove';
      remove.title = `Remove the group ${g.name}`;
      remove.addEventListener('click', () => {
        groups.splice(i, 1);
        draw();
      });
      row.append(words, remove);
      return row;
    }));
  };

  const group = doc.createElement('button');
  group.type = 'button';
  group.className = 'dc-button';
  group.textContent = 'Group Selected';
  group.addEventListener('click', () => {
    const name = groupName.value.trim();
    const values = o.values.map((v) => v.value).filter((v) => ticked.has(keyOf(v)) && groupOf(v) === undefined);
    if (name === '') { said.textContent = 'Name the group'; return; }
    if (values.length === 0) { said.textContent = 'Tick the values to group'; return; }
    if (groups.some((g) => g.name === name)) { said.textContent = `There is a group named ${name} already`; return; }
    groups.push({ name, values });
    ticked.clear();
    said.textContent = '';
    groupName.value = `Group ${groups.length + 1}`;
    draw();
  });

  const ok = doc.createElement('button');
  ok.type = 'button';
  ok.className = 'dc-button dc-primary';
  ok.textContent = 'OK';
  ok.addEventListener('click', () => {
    if (groups.length === 0) { said.textContent = 'Group some values first'; return; }
    const other = otherName.value.trim();
    const made = groups.map((g) => ({ name: g.name, values: [...g.values] }));
    void o.onOk(columnName.value.trim(), { of: o.column, groups: made, ...(other !== '' ? { other } : {}) }).then((refused) => {
      said.textContent = refused ?? '';
    });
  });
  const cancel = doc.createElement('button');
  cancel.type = 'button';
  cancel.className = 'dc-button';
  cancel.textContent = 'Cancel';
  cancel.addEventListener('click', () => o.onCancel());

  const pick = doc.createElement('div');
  pick.className = 'dc-group-pick';
  pick.append(groupName, group);
  const footer = doc.createElement('div');
  footer.className = 'dc-value-footer';
  footer.append(cancel, ok);
  body.append(list, pick, shown, labelled(doc, 'Other values:', otherName), labelled(doc, 'Column name:', columnName), said, footer);
  draw();
  return body;
}

function input(doc: Document, className: string, label: string, value: string): HTMLInputElement {
  const el = doc.createElement('input');
  el.type = 'text';
  el.className = `dc-input ${className}`;
  el.value = value;
  el.setAttribute('aria-label', label);
  return el;
}

function labelled(doc: Document, words: string, control: HTMLElement): HTMLElement {
  const row = doc.createElement('label');
  row.className = 'dc-group-field';
  const text = doc.createElement('span');
  text.textContent = words;
  row.append(text, control);
  return row;
}
