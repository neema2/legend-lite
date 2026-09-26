// The fields of a JSON column, for the Add Column screen.
//
// Samples the column through the cube's own query path, infers the
// shape of its documents (`json-shape.ts`), and lists every field with
// what can be made of it -- the value itself; a nested object or array
// as a JSON column of its own; for an array its count, its values as
// text, whether it contains a value; for an array of objects each
// field's values, first value and total. Picking one hands the column
// editor a name, a kind and the Pure, which it compiles like anything
// typed there: this writes Pure for you, it is not a second language.

import {
  fieldsOf,
  inferShape,
  type Extraction,
  type Field,
  type Sample,
} from '../json-shape.ts';

export interface JsonFieldsOptions {
  readonly column: string;
  /** `$x.<column>`, as a calculated column reaches it. */
  readonly columnRef: string;
  /** The column's cells from a sample of rows. */
  readonly sample: () => Promise<readonly unknown[]>;
  /** A field's extraction was chosen. */
  readonly onPick: (extraction: Extraction) => void;
}

/** A free name: `base`, else `base_2`, `base_3`... */
export function freeName(base: string, taken: ReadonlySet<string>): string {
  if (!taken.has(base)) return base;
  for (let n = 2; ; n += 1) {
    if (!taken.has(`${base}_${n}`)) return `${base}_${n}`;
  }
}

export function buildJsonFields(host: HTMLElement, options: JsonFieldsOptions): void {
  const doc = host.ownerDocument;
  host.classList.add('dc-jsonfields');
  const note = doc.createElement('div');
  note.className = 'dc-jsonfields-note';
  note.setAttribute('role', 'status');
  note.textContent = `Sampling ${options.column}…`;
  const tree = doc.createElement('div');
  tree.className = 'dc-jsonfields-tree';
  tree.setAttribute('role', 'tree');
  host.append(note, tree);

  const say = (text: string, bad = false): void => {
    note.textContent = text;
    note.classList.toggle('dc-jsonfields-bad', bad);
  };

  void (async () => {
    let sample: Sample;
    try {
      sample = inferShape(await options.sample());
    } catch (e) {
      say(`Could not sample ${options.column}: `
        + (e instanceof Error ? e.message : String(e)), true);
      return;
    }
    const read = sample.rows - sample.unreadable;
    say(`${sample.rows.toLocaleString()} rows sampled`
      + (sample.unreadable > 0 ? ` (${sample.unreadable} not JSON)` : '')
      + '. Pick a field: types are a guess from the sample, and the '
      + 'expression below is compiled as usual.');
    if (read === 0 || sample.shape.present === 0) {
      say(`No JSON values in the ${sample.rows.toLocaleString()} sampled rows.`, true);
      return;
    }
    for (const f of fieldsOf(options.column, options.columnRef, sample)) {
      tree.append(fieldRow(f));
    }
  })();

  function fieldRow(field: Field): HTMLElement {
    const row = doc.createElement('div');
    row.className = 'dc-jsonfields-field';
    row.setAttribute('role', 'treeitem');

    const head = doc.createElement('div');
    head.className = 'dc-jsonfields-head';
    const name = doc.createElement('span');
    name.className = 'dc-jsonfields-name';
    name.textContent = field.path.length === 0
      ? options.column : field.path[field.path.length - 1]!;
    const what = doc.createElement('span');
    what.className = 'dc-jsonfields-what';
    const pct = Math.round(field.presence * 100);
    what.textContent = field.description + (pct < 100 ? ` · in ${pct}%` : '');
    head.append(name, what);
    row.append(head);

    if (field.extractions.length > 0) {
      const list = doc.createElement('div');
      list.className = 'dc-jsonfields-actions';
      for (const e of field.extractions) list.append(actionButton(e));
      row.append(list);
    }
    if (field.children.length > 0) {
      const kids = doc.createElement('div');
      kids.setAttribute('role', 'group');
      for (const c of field.children) kids.append(fieldRow(c));
      row.append(kids);
    }
    return row;
  }

  function actionButton(e: Extraction): HTMLElement {
    const b = doc.createElement('button');
    b.type = 'button';
    b.className = 'dc-jsonfields-add';
    b.textContent = e.label;
    // The Pure it writes, on hover: nothing hidden about what it does.
    b.title = `${e.name}: ${e.type}\n${e.expression}`;
    b.addEventListener('click', () => {
      for (const other of tree.querySelectorAll('.dc-jsonfields-picked')) {
        other.classList.remove('dc-jsonfields-picked');
      }
      b.classList.add('dc-jsonfields-picked');
      options.onPick(e);
    });
    return b;
  }
}
