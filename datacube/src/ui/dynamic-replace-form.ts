// DYNAMIC REPLACE, SET (docs/DATACUBE_FLOW_MODE_2026_10_24.md §4: a flow's Dynamic Replace box, Alteryx's): the box of
// rules it loads, which of that box's columns holds each rule's column, condition and value, and whether its values are
// expressions -- dynamic-replace.ts's rule, said as Alteryx's Dynamic Replace says it -- in a window its grid docks where
// its options dock it. A box picked, its page reads its rows (the database reading them: a flow shows one box at a time,
// so it shows that box, then this one again) and the window opens again on them, the rules they hold counted before OK.
// Fetched with the first Dynamic Replace.

import type { ResultTable } from '../../../engine-client/src/result.ts';
import type { DynamicReplace, ReplaceRule, RuleColumns } from '../dynamic-replace.ts';
import { replacedColumns, rulesOf } from '../dynamic-replace-rules.ts';
import { CubeRefusal } from '../snapshot.ts';

export interface DynamicReplaceFormOptions {
  /** The page's other boxes it can load rules from, by id, as their tabs say them. */
  readonly boxes: readonly { readonly id: string; readonly label: string }[];
  /** The box it was opened on, its rows as its page read them -- or why they could not be read. */
  readonly start?: { readonly box: string; readonly rows?: ResultTable; readonly problem?: string; readonly expressions?: boolean };
  /** Box `box`'s rows read, as it hands them on, by its page: the window opened again on them (`start`), its values
   *  expressions still when they were (`expressions`). */
  read(box: string, expressions: boolean): void;
  /** Its values replaced: null when they were, else why not (the form stays, saying it). */
  apply(replace: DynamicReplace): Promise<string | null>;
  close(): void;
}

/** The form, drawn into `host`. */
export function dynamicReplaceForm(host: HTMLElement, o: DynamicReplaceFormOptions): void {
  const doc = host.ownerDocument;
  const el = <K extends keyof HTMLElementTagNameMap>(tag: K, cls: string, parent: Element, text?: string): HTMLElementTagNameMap[K] => {
    const e = doc.createElement(tag);
    e.className = cls;
    if (text !== undefined) e.textContent = text;
    parent.append(e);
    return e;
  };
  const body = el('div', 'dc-sample dc-dynamic-replace', host);
  const close = (): void => o.close();
  if (o.boxes.length === 0) {
    el('p', 'dc-sample-hint', body, 'Its rules are rows of another box -- a column, a condition and a value each: this page has none to load them from.');
    const f = el('div', 'dc-sample-foot', body);
    el('span', 'dc-save-spacer', f);
    const b = el('button', 'dc-picker-button', f, 'Close');
    b.type = 'button';
    b.addEventListener('click', close);
    return;
  }
  const line = el('div', 'dc-sample-line', body);
  el('span', 'dc-sample-word', line, 'Rules from');
  const from = el('select', 'dc-select dc-replace-from', line);
  from.setAttribute('aria-label', 'The box its rules are read from');
  el('option', '', from, 'a box…').value = '';
  for (const b of o.boxes) el('option', '', from, b.label).value = b.id;
  const parts = el('div', 'dc-replace-parts', body);
  const part = (label: string, cls: string): HTMLSelectElement => {
    const l = el('label', 'dc-sample-line', parts);
    el('span', 'dc-sample-word', l, label);
    const s = el('select', `dc-select ${cls}`, l);
    s.setAttribute('aria-label', label);
    return s;
  };
  const columnOf = part('Column it replaces', 'dc-replace-column');
  const conditionOf = part('Condition', 'dc-replace-condition');
  const valueOf = part('Value', 'dc-replace-value');
  const asExpressions = el('label', 'dc-text-tool-check', body);
  const expressions = el('input', 'dc-replace-expressions', asExpressions);
  expressions.type = 'checkbox';
  // as it was ticked before its rules box was read (the roadmap audit of 2026-10-26: the tick was lost)
  expressions.checked = o.start?.expressions === true;
  el('span', '', asExpressions, 'Values are expressions (Pure over $x)');
  el('p', 'dc-sample-hint', body, 'Each row of the rules box is a rule: the column it replaces, a condition in Pure over $x, the row ($x.region == \'EMEA\'), '
    + 'and the value put there. Of a column\'s rules the first whose condition holds; its own value where none does. Loaded now and kept: load them again to read the box again.');
  const read = el('p', 'dc-sample-status dc-replace-read', body);
  read.setAttribute('role', 'status');
  const status = el('p', 'dc-sample-status', body);
  status.setAttribute('role', 'status');
  const foot = el('div', 'dc-sample-foot', body);
  el('span', 'dc-save-spacer', foot);
  const cancel = el('button', 'dc-picker-button dc-quiet', foot, 'Cancel');
  cancel.type = 'button';
  cancel.addEventListener('click', close);
  const ok = el('button', 'dc-picker-button dc-primary', foot, 'Load Rules');
  ok.type = 'button';
  let rows: ResultTable | undefined;
  let rules: ReplaceRule[] | undefined;
  const judge = (): void => {
    rules = undefined;
    let bad: string | undefined = from.value === '' ? 'pick the box its rules are read from'
      : rows === undefined ? o.start?.problem ?? 'its rows are being read' : undefined;
    if (bad === undefined && rows !== undefined) {
      try {
        rules = rulesOf(rows, { column: columnOf.value, condition: conditionOf.value, value: valueOf.value } satisfies RuleColumns);
        const columns = replacedColumns({ rules, expressions: expressions.checked });
        read.textContent = `${rules.length} rule${rules.length === 1 ? '' : 's'}, replacing ${columns.join(', ')}`;
      } catch (e) {
        if (!(e instanceof CubeRefusal)) throw e;
        bad = e.message;
        read.textContent = '';
      }
    } else read.textContent = '';
    ok.disabled = bad !== undefined;
    ok.title = bad === undefined ? '' : `Not yet: ${bad}`;
  };
  const fill = (s: HTMLSelectElement, names: readonly string[], at: number): void => {
    s.replaceChildren();
    for (const n of names) el('option', '', s, n).value = n;
    s.value = names[Math.min(at, names.length - 1)] ?? '';
  };
  // opened on a box's rows: its parts in its columns' order, to be picked again where they are not
  if (o.start !== undefined && o.boxes.some((b) => b.id === o.start!.box)) {
    from.value = o.start.box;
    rows = o.start.rows;
    if (o.start.problem !== undefined) status.textContent = o.start.problem;
    const names = rows?.columns.map((c) => c.name) ?? [];
    fill(columnOf, names, 0);
    fill(conditionOf, names, 1);
    fill(valueOf, names, 2);
  }
  from.addEventListener('change', () => {
    rows = undefined;
    status.textContent = '';
    judge();
    if (from.value !== '') o.read(from.value, expressions.checked);
  });
  for (const s of [columnOf, conditionOf, valueOf]) s.addEventListener('change', judge);
  expressions.addEventListener('change', judge);
  judge();
  ok.addEventListener('click', () => void (async () => {
    if (rules === undefined) return;
    status.textContent = '';
    const refused = await o.apply({ rules, expressions: expressions.checked });
    if (refused === null) o.close();
    else status.textContent = refused;
  })());
}
