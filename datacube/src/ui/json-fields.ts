// The fields of a JSON column, for the Add Column screen.
//
// Reads the column through the cube's own query path -- a sample of its
// first rows, or on request EVERY row in one streamed pass (each chunk
// observed and let go) -- infers the shape of its documents
// (`json-shape.ts`), and lists every field with
// what can be made of it -- the value itself; a nested object or array
// as a JSON column of its own; for an array its count, its values as
// text, whether it contains a value; for an array of objects each
// field's values, first value and total. Picking one hands the column
// editor a name, a kind and the Pure, which it compiles like anything
// typed there: this writes Pure for you, it is not a second language.

import { UI_LOCALE } from '../../../engine-client/src/locale.ts';
import {
  fieldsOf,
  ShapeReader,
  type Extraction,
  type Field,
  type Sample,
} from '../json-shape.ts';

/** A JSON column's cells, read through the cube's own queries (so every plane). */
export interface JsonColumnReader {
  /** The first rows' cells, and how many rows the column has. */
  sample(signal?: AbortSignal): Promise<{ readonly cells: readonly unknown[]; readonly total: number }>;
  /**
   * Every row, in one streamed pass: each chunk's cells as they arrive. Resolves after the
   * last chunk; rejects when `signal` aborts.
   */
  all(onChunk: (cells: readonly unknown[]) => void, signal?: AbortSignal): Promise<void>;
}

export interface JsonFieldsOptions {
  readonly column: string;
  /** How the column's cells are read. */
  readonly reader: JsonColumnReader;
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
  // WHAT WAS READ, on one line: a sample or every row, and the way to read every row
  const note = doc.createElement('div');
  note.className = 'dc-jsonfields-note';
  note.setAttribute('role', 'status');
  const text = doc.createElement('span');
  text.className = 'dc-jsonfields-said';
  text.textContent = `Sampling ${options.column}…`;
  // Read every row: the shape of the whole column, not a sample -- Cancel while reading.
  const whole = doc.createElement('button');
  whole.type = 'button';
  whole.className = 'dc-jsonfields-all';
  whole.hidden = true;
  note.append(text, whole);
  // THE FIELDS, as a tree on the left; what can be taken of the chosen one, on the right
  const panes = doc.createElement('div');
  panes.className = 'dc-jsonfields-panes';
  const left = doc.createElement('div');
  left.className = 'dc-jsonfields-left';
  const search = doc.createElement('input');
  search.type = 'search';
  search.className = 'dc-jsonfields-search';
  search.placeholder = 'Find a field';
  search.setAttribute('aria-label', 'Find a field');
  const tree = doc.createElement('div');
  tree.className = 'dc-jsonfields-tree';
  tree.setAttribute('role', 'tree');
  left.append(search, tree);
  const detail = doc.createElement('div');
  detail.className = 'dc-jsonfields-detail';
  panes.append(left, detail);
  host.append(note, panes);

  const say = (message: string, bad = false): void => {
    text.textContent = message;
    note.classList.toggle('dc-jsonfields-bad', bad);
  };
  const fmt = (n: number): string => n.toLocaleString(UI_LOCALE);

  /** Each field's node in the tree and its options on the right, by path. */
  const nodes = new Map<string, { readonly node: HTMLElement; readonly options: HTMLElement; readonly field: Field }>();
  let chosen: string | undefined;
  const keyOf = (f: Field): string => JSON.stringify(f.path);
  const choose = (key: string): void => {
    chosen = key;
    for (const [k, n] of nodes) {
      const on = k === key;
      n.node.classList.toggle('dc-on', on);
      n.node.setAttribute('aria-selected', String(on));
      n.options.hidden = !on;
    }
  };

  /** The fields of what was read, and what the reading covers. */
  const show = (sample: Sample, total: number): void => {
    tree.replaceChildren();
    detail.replaceChildren();
    nodes.clear();
    const read = sample.rows - sample.unreadable;
    const unread = sample.unreadable > 0 ? ` (${fmt(sample.unreadable)} not JSON)` : '';
    whole.hidden = sample.complete;
    whole.textContent = 'Read every row';
    panes.hidden = false;
    if (read === 0 || sample.shape.present === sample.shape.nulls) {
      say(`No JSON values in the ${fmt(sample.rows)} ${sample.complete ? '' : 'sampled '}rows.`, true);
      panes.hidden = true;
      return;
    }
    say(sample.complete
      ? `All ${fmt(sample.rows)} rows read${unread}: every field, type and share covers the whole column.`
      : `${fmt(sample.rows)} of ${fmt(total)} rows sampled${unread}: types and shares are from the sample.`);
    const fields = fieldsOf(options.column, sample);
    for (const f of fields) tree.append(fieldNode(f, 0));
    // the first field with something to take, chosen to start (or the one chosen before)
    const first = [...nodes.values()].find((n) => n.field.extractions.length > 0);
    const again = chosen !== undefined && nodes.has(chosen) ? chosen : first ? keyOf(first.field) : undefined;
    if (again !== undefined) choose(again);
    filter();
  };

  /** Only the fields whose path matches, with the fields they are in. */
  const filter = (): void => {
    const q = search.value.trim().toLowerCase();
    for (const { node, field } of nodes.values()) {
      const item = node.parentElement as HTMLElement;
      const hit = q === '' || [options.column, ...field.path].join('.').toLowerCase().includes(q);
      item.dataset['hit'] = String(hit);
    }
    // a matching field keeps every field it is in shown
    for (const item of [...tree.querySelectorAll<HTMLElement>('.dc-jsonfields-item')].reverse()) {
      const shown = item.dataset['hit'] === 'true'
        || item.querySelector(':scope > .dc-jsonfields-kids > .dc-jsonfields-item:not([hidden])') !== null;
      item.hidden = !shown;
    }
  };
  search.addEventListener('input', filter);

  let shown: { readonly sample: Sample; readonly total: number } | undefined;
  let reading: AbortController | undefined;

  whole.addEventListener('click', () => {
    if (reading) {
      reading.abort();
      return;
    }
    const total = shown?.total ?? 0;
    const controller = new AbortController();
    reading = controller;
    whole.textContent = 'Cancel';
    const reader = new ShapeReader();
    let rows = 0;
    say(`Reading every row of ${options.column}…`);
    void options.reader.all((cells) => {
      for (const cell of cells) reader.add(cell);
      rows += cells.length;
      say(`Reading every row of ${options.column}… ${fmt(rows)} of ${fmt(total)}`);
    }, controller.signal).then(() => {
      reading = undefined;
      shown = { sample: reader.result(true), total: rows };
      show(shown.sample, shown.total);
    }, (e: unknown) => {
      reading = undefined;
      if (shown) show(shown.sample, shown.total);
      if (controller.signal.aborted) {
        say(`Stopped reading every row; showing the sample. ${text.textContent ?? ''}`);
      } else {
        say(`Could not read every row of ${options.column}: `
          + (e instanceof Error ? e.message : String(e)), true);
      }
    });
  });

  void (async () => {
    try {
      const { cells, total } = await options.reader.sample();
      const reader = new ShapeReader();
      for (const cell of cells) reader.add(cell);
      // a sample holding every row IS the whole column
      shown = { sample: reader.result(cells.length >= total), total };
      show(shown.sample, shown.total);
    } catch (e) {
      say(`Could not sample ${options.column}: `
        + (e instanceof Error ? e.message : String(e)), true);
    }
  })();

  /** A field in the tree -- its name, what it is, how often it is there -- and its children. */
  function fieldNode(field: Field, depth: number): HTMLElement {
    const item = doc.createElement('div');
    item.className = 'dc-jsonfields-item';
    const node = doc.createElement('div');
    node.className = 'dc-jsonfields-node';
    node.setAttribute('role', 'treeitem');
    node.tabIndex = 0;
    node.style.paddingLeft = `${6 + depth * 14}px`;
    const caret = doc.createElement('span');
    caret.className = 'dc-jsonfields-caret';
    const name = doc.createElement('span');
    name.className = 'dc-jsonfields-name';
    name.textContent = field.path.length === 0 ? options.column : field.path[field.path.length - 1]!;
    const what = doc.createElement('span');
    what.className = 'dc-jsonfields-what';
    what.textContent = field.description;
    node.append(caret, name, what);
    const pct = Math.round(field.presence * 100);
    if (pct < 100) {
      const share = doc.createElement('span');
      share.className = 'dc-jsonfields-share';
      share.textContent = `${pct}%`;
      share.title = `In ${pct}% of the rows read`;
      node.append(share);
    }
    item.append(node);
    let kids: HTMLElement | undefined;
    if (field.children.length > 0) {
      kids = doc.createElement('div');
      kids.className = 'dc-jsonfields-kids';
      kids.setAttribute('role', 'group');
      for (const c of field.children) kids.append(fieldNode(c, depth + 1));
      item.append(kids);
      // open two levels to start; deeper ones a click away
      const open = depth < 1;
      kids.hidden = !open;
      caret.textContent = open ? '▾' : '▸';
      node.setAttribute('aria-expanded', String(open));
      caret.addEventListener('click', (e) => {
        e.stopPropagation();
        kids!.hidden = !kids!.hidden;
        caret.textContent = kids!.hidden ? '▸' : '▾';
        node.setAttribute('aria-expanded', String(!kids!.hidden));
      });
    }
    nodes.set(keyOf(field), { node, options: optionsOf(field), field });
    node.addEventListener('click', () => choose(keyOf(field)));
    node.addEventListener('keydown', (e) => {
      if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); choose(keyOf(field)); }
    });
    return item;
  }

  /** What can be taken of a field: a short list, each its label and the type it gives. */
  function optionsOf(field: Field): HTMLElement {
    const box = doc.createElement('div');
    box.className = 'dc-jsonfields-options';
    box.hidden = true;
    const crumb = doc.createElement('div');
    crumb.className = 'dc-jsonfields-crumb';
    crumb.textContent = [options.column, ...field.path].join(' › ');
    const sub = doc.createElement('div');
    sub.className = 'dc-jsonfields-sub';
    const pct = Math.round(field.presence * 100);
    sub.textContent = field.description + (pct < 100 ? ` · in ${pct}% of rows` : '');
    box.append(crumb, sub);
    if (field.extractions.length === 0) {
      const none = doc.createElement('p');
      none.className = 'dc-jsonfields-none';
      none.textContent = field.children.length > 0
        ? 'Choose one of its fields on the left.'
        : 'Nothing to take from it on its own.';
      box.append(none);
    }
    const list = doc.createElement('div');
    list.className = 'dc-jsonfields-actions';
    list.setAttribute('role', 'list');
    for (const e of field.extractions) list.append(option(e));
    box.append(list);
    detail.append(box);
    return box;
  }

  function option(e: Extraction): HTMLElement {
    const row = doc.createElement('div');
    row.className = 'dc-jsonfields-option';
    row.setAttribute('role', 'listitem');
    const b = doc.createElement('button');
    b.type = 'button';
    b.className = 'dc-jsonfields-add';
    b.textContent = e.label;
    // What it makes, on hover: nothing hidden about what it does.
    b.title = `${e.name}: ${e.type}` + (e.unnest
      ? ' -- one row per element: a figure of the row itself then counts once per element' : '');
    const type = doc.createElement('span');
    type.className = 'dc-jsonfields-type';
    type.textContent = e.unnest ? `${e.type} · explode` : e.type;
    b.addEventListener('click', () => {
      for (const other of detail.querySelectorAll('.dc-jsonfields-picked')) {
        other.classList.remove('dc-jsonfields-picked');
      }
      b.classList.add('dc-jsonfields-picked');
      row.classList.add('dc-jsonfields-picked');
      options.onPick(e);
    });
    row.addEventListener('click', (ev) => { if (ev.target === row || ev.target === type) b.click(); });
    row.append(b, type);
    return row;
  }
}
