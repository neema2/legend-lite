// JOIN… (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §17.5), a window of its own in the Create a Class window's style: their
// grid (another of the page's), how the two combine (four kinds, as of, keep the rows with a match or with none,
// append), the condition -- rows of my column, a comparison, their column, a key suggested when one is plain, or the
// condition written in Pure -- their columns brought over and the prefix for names both have, and, before it is applied,
// what it will do: how many of my rows find a match, the rows after it, their keys that repeat. It builds a join's `how`
// (join.ts) and shows; the HOST opens the inputs, answers the questions and makes the grid. What is wrong with a name or
// a type is the compiler's to say, shown as it says it.

import type { Lambda } from '../../../pure-protocol/src/index.ts';
import { focusedElement } from '../focus.ts';
import {
  AS_OF_OPS, DEFAULT_PREFIX, JOIN_OPS, suggestedKeys, summaryWords, theirNames, type AsOfOp, type JoinCondition, type JoinHow,
  type JoinKindWord, type JoinOp, type JoinRow, type JoinSide, type JoinSummaryCounts,
} from '../join.ts';

/** Another grid of the page, as the window offers it. */
export interface JoinCandidate {
  readonly id: string;
  readonly name: string;
}

export interface JoinDialogOptions {
  /** My grid's name. */
  readonly mine: string;
  /** The page's other grids a join can read (tables of this tab). */
  readonly others: readonly JoinCandidate[];
  /** My grid's and `their` grid's results over one model: their columns, as the compiler types them. */
  inputs(their: string): Promise<{ readonly left: JoinSide; readonly right: JoinSide }>;
  /** A condition written in Pure, `{a, b | ...}`, read by the compiler. */
  parse(text: string): Promise<Lambda>;
  /** What a join will do: its questions planned over the inputs' model and run. */
  summary(their: string, how: JoinHow): Promise<JoinSummaryCounts>;
  /** The join made a grid of the page. */
  apply(their: string, how: JoinHow): Promise<void>;
}

/** What the window's form offers, each a form of join.ts's `how`. */
type Choice = JoinKindWord | 'asOf' | 'semi' | 'anti' | 'append';
const CHOICES: readonly { readonly value: Choice; readonly label: string }[] = [
  { value: 'left', label: 'All of mine, theirs where they match' },
  { value: 'inner', label: 'Matching rows only' },
  { value: 'right', label: 'All of theirs, mine where they match' },
  { value: 'full', label: 'Everything, matched where they match' },
  { value: 'asOf', label: 'The nearest of theirs (as of)' },
  { value: 'semi', label: 'My rows that have a match' },
  { value: 'anti', label: 'My rows that have none' },
  { value: 'append', label: 'Their rows appended to mine' },
];
const OP_WORDS: Readonly<Record<JoinOp, string>> = { '==': '=', '!=': '≠', '<': '<', '<=': '≤', '>': '>', '>=': '≥' };

function el<K extends keyof HTMLElementTagNameMap>(doc: Document, tag: K, cls: string, parent?: Element, text?: string): HTMLElementTagNameMap[K] {
  const e = doc.createElement(tag);
  if (cls) e.className = cls;
  if (text !== undefined) e.textContent = text;
  parent?.append(e);
  return e;
}

function select(doc: Document, cls: string, parent: Element, aria: string, options: readonly { value: string; label: string }[]): HTMLSelectElement {
  const s = el(doc, 'select', `dc-picker-input ${cls}`, parent);
  s.setAttribute('aria-label', aria);
  for (const o of options) (el(doc, 'option', '', s, o.label) as HTMLOptionElement).value = o.value;
  return s;
}

/** Open the window. Resolves true when a join was made, false when it was closed without one. */
export function joinDialog(doc: Document, o: JoinDialogOptions): Promise<boolean> {
  return new Promise((resolve) => {
    const backdrop = el(doc, 'div', 'dc-picker-backdrop', doc.body);
    const win = el(doc, 'div', 'dc-picker dc-save dc-join dc-app-floating', backdrop);
    win.setAttribute('role', 'dialog');
    win.setAttribute('aria-modal', 'true');
    win.setAttribute('aria-labelledby', 'dc-join-title');
    const before = focusedElement(doc) as HTMLElement | null;
    let busy = false;
    let done = false;
    let made = false;
    const finish = (): void => {
      if (done) return;
      done = true;
      doc.removeEventListener('keydown', onKey, true);
      backdrop.remove();
      before?.focus?.();
      resolve(made);
    };
    const onKey = (e: KeyboardEvent): void => {
      if (e.key === 'Escape' && !busy) { e.preventDefault(); finish(); }
    };
    doc.addEventListener('keydown', onKey, true);
    backdrop.addEventListener('mousedown', (e) => { if (e.target === backdrop && !busy) finish(); });

    const head = el(doc, 'div', 'dc-picker-head', win);
    const titles = el(doc, 'div', 'dc-picker-titles', head);
    el(doc, 'h2', 'dc-picker-title', titles, 'Join').id = 'dc-join-title';
    el(doc, 'p', 'dc-picker-subtitle', titles, `${o.mine} with another grid of the page: a new grid of the rows they make together.`);
    const close = el(doc, 'button', 'dc-picker-close', head, '×');
    close.type = 'button';
    close.setAttribute('aria-label', 'Close');
    close.addEventListener('click', () => { if (!busy) finish(); });

    const body = el(doc, 'div', 'dc-save-body', win);
    const top = el(doc, 'div', 'dc-save-card dc-join-what', body);
    el(doc, 'span', 'dc-picker-field-label', top, 'Their grid');
    const their = select(doc, 'dc-join-their', top, 'Their grid', o.others.map((c) => ({ value: c.id, label: c.name })));
    el(doc, 'span', 'dc-picker-field-label', top, 'Keep');
    const choice = select(doc, 'dc-join-form', top, 'How they combine', CHOICES);

    // THE CONDITION: rows, or Pure
    const condition = el(doc, 'div', 'dc-save-card dc-join-condition', body);
    el(doc, 'div', 'dc-save-card-title', condition, 'Where');
    const suggested = el(doc, 'div', 'dc-join-suggested', condition);
    const matchLine = el(doc, 'div', 'dc-join-row dc-join-match', condition);
    const rowsBox = el(doc, 'div', 'dc-join-rows', condition);
    const addRow = el(doc, 'button', 'dc-picker-button dc-quiet dc-join-add', condition, '+ Condition');
    addRow.type = 'button';
    const pureLabel = el(doc, 'label', 'dc-join-pure-label', condition);
    const pureOn = el(doc, 'input', 'dc-join-pure-on', pureLabel);
    pureOn.type = 'checkbox';
    pureLabel.append(' Write it in Pure');
    const pure = el(doc, 'textarea', 'dc-picker-input dc-join-pure', condition);
    pure.setAttribute('aria-label', 'The condition in Pure');
    pure.spellcheck = false;
    pure.hidden = true;
    const pureHint = el(doc, 'p', 'dc-join-hint', condition);
    pureHint.hidden = true;

    // THEIR COLUMNS: brought over, and the prefix for names both have
    const columnsCard = el(doc, 'div', 'dc-save-card dc-join-columns', body);
    el(doc, 'div', 'dc-save-card-title', columnsCard, 'Their columns brought over');
    const columnList = el(doc, 'div', 'dc-join-column-list', columnsCard);
    const prefixLine = el(doc, 'label', 'dc-join-prefix-line', columnsCard);
    prefixLine.append('Before their names that mine has: ');
    const prefix = el(doc, 'input', 'dc-picker-input dc-join-prefix', prefixLine);
    prefix.type = 'text';
    prefix.value = DEFAULT_PREFIX;
    prefix.spellcheck = false;

    const will = el(doc, 'div', 'dc-save-card dc-join-will', body);
    el(doc, 'div', 'dc-save-card-title', will, 'What it will do');
    const willList = el(doc, 'ul', 'dc-join-will-list', will);

    const status = el(doc, 'div', 'dc-picker-status', win);
    status.setAttribute('role', 'status');
    const foot = el(doc, 'div', 'dc-save-foot', win);
    const cancel = el(doc, 'button', 'dc-picker-button dc-quiet', foot, 'Cancel');
    cancel.type = 'button';
    cancel.addEventListener('click', () => { if (!busy) finish(); });
    el(doc, 'span', 'dc-save-spacer', foot).setAttribute('aria-hidden', 'true');
    const primary = el(doc, 'button', 'dc-picker-button dc-primary', foot, 'Join');
    primary.type = 'button';
    primary.disabled = true;
    const say = (text: string, kind: 'working' | 'failed' | 'ok' | ''): void => {
      status.className = `dc-picker-status${kind ? ` dc-${kind}` : ''}`;
      status.textContent = text;
    };

    let inputs: { left: JoinSide; right: JoinSide } | undefined;
    const mineOptions = (): { value: string; label: string }[] => (inputs?.left.columns ?? []).map((c) => ({ value: c.name, label: c.name }));
    const theirOptions = (): { value: string; label: string }[] => (inputs?.right.columns ?? []).map((c) => ({ value: c.name, label: c.name }));

    /** A row's three selects: my column, the comparison, their column (by their own name). */
    const rowLine = (parent: HTMLElement, ops: readonly string[], row: Partial<JoinRow>, removable: boolean): void => {
      const line = parent === matchLine ? parent : el(doc, 'div', 'dc-join-row', parent);
      line.replaceChildren();
      const left = select(doc, 'dc-join-left', line, 'My column', [{ value: '', label: 'my column…' }, ...mineOptions()]);
      const op = select(doc, 'dc-join-op', line, 'Comparison', ops.map((x) => ({ value: x, label: OP_WORDS[x as JoinOp] })));
      const right = select(doc, 'dc-join-right', line, 'Their column', [{ value: '', label: 'their column…' }, ...theirOptions()]);
      if (row.left !== undefined) left.value = row.left;
      if (row.op !== undefined) op.value = row.op;
      if (row.right !== undefined) right.value = row.right;
      for (const s of [left, op, right]) s.addEventListener('change', changed);
      if (removable) {
        const remove = el(doc, 'button', 'dc-picker-button dc-quiet dc-join-remove', line, '×');
        remove.type = 'button';
        remove.setAttribute('aria-label', 'Take this condition off');
        remove.addEventListener('click', () => { line.remove(); changed(); });
      }
    };
    const readRow = (line: Element): Partial<JoinRow> => ({
      left: (line.querySelector('.dc-join-left') as HTMLSelectElement).value,
      op: (line.querySelector('.dc-join-op') as HTMLSelectElement).value as JoinOp,
      right: (line.querySelector('.dc-join-right') as HTMLSelectElement).value,
    });

    /** The window's parts shown as the form chosen asks. */
    const shape = (): void => {
      const c = choice.value as Choice;
      condition.hidden = c === 'append';
      matchLine.hidden = c !== 'asOf';
      columnsCard.hidden = c === 'semi' || c === 'anti' || c === 'append';
      pure.hidden = !pureOn.checked;
      pureHint.hidden = !pureOn.checked;
      rowsBox.hidden = pureOn.checked;
      addRow.hidden = pureOn.checked;
      const renamed = c === 'semi' || c === 'anti' ? 'by their own names' : 'as they come out (a name mine has, prefixed)';
      pureHint.textContent = `Two rows, mine and theirs, their columns named ${renamed}: {a, b | $a.cust == $b.id}`;
      // the prefix said on their names that mine has
      if (inputs) {
        const names = theirNames(inputs.left.columns.map((x) => x.name), inputs.right.columns.map((x) => x.name), prefix.value);
        for (const box of columnList.querySelectorAll<HTMLInputElement>('input')) {
          const out = names.get(box.value) ?? box.value;
          (box.nextSibling as Text).textContent = out === box.value ? ` ${box.value}` : ` ${box.value} → ${out}`;
        }
      }
    };

    /** Their grid read with mine: the column choices filled, a plain key suggested. The last grid chosen wins. */
    let loads = 0;
    const load = async (): Promise<void> => {
      const mine = (loads += 1);
      // nothing is asked of the grid chosen before while this one is read
      inputs = undefined;
      asked += 1;
      willList.replaceChildren();
      primary.disabled = true;
      say('Reading the two grids…', 'working');
      let got: { left: JoinSide; right: JoinSide };
      try {
        got = await o.inputs(their.value);
      } catch (e) {
        if (done || mine !== loads) return;
        say(e instanceof Error ? e.message : String(e), 'failed');
        return;
      }
      if (done || mine !== loads) return;
      inputs = got;
      suggested.replaceChildren();
      const keys = suggestedKeys(inputs.left, inputs.right);
      if (keys.length > 0) el(doc, 'span', 'dc-picker-field-label', suggested, 'Suggested');
      for (const k of keys) {
        const chip = el(doc, 'button', 'dc-picker-button dc-join-chip', suggested, `${k.row.left} = ${k.row.right}`);
        chip.type = 'button';
        chip.title = k.why;
        chip.addEventListener('click', () => {
          rowsBox.replaceChildren();
          rowLine(rowsBox, JOIN_OPS, k.row, true);
          changed();
        });
      }
      rowsBox.replaceChildren();
      // one plain key: the condition it starts with; else a row to fill
      rowLine(rowsBox, JOIN_OPS, keys.length === 1 ? keys[0]!.row : { op: '==' }, true);
      rowLine(matchLine, AS_OF_OPS, { op: '>=' }, false);
      columnList.replaceChildren();
      for (const c of inputs.right.columns) {
        const label = el(doc, 'label', 'dc-join-column', columnList);
        const box = el(doc, 'input', '', label);
        box.type = 'checkbox';
        box.value = c.name;
        box.checked = true;
        label.append(` ${c.name}`);
        box.addEventListener('change', changed);
      }
      shape();
      changed();
    };

    /** The `how` the window says now, or what it still needs. */
    const how = async (): Promise<JoinHow | string> => {
      const c = choice.value as Choice;
      if (c === 'append') return { form: 'append' };
      let on: JoinCondition;
      if (pureOn.checked) {
        if (pure.value.trim() === '') return 'Write the condition, or take Pure off.';
        on = { lambda: await o.parse(pure.value) };
      } else {
        const rows = [...rowsBox.children].map(readRow);
        if (rows.some((r) => !r.left || !r.right)) return 'Choose both columns of each condition (or take it off).';
        on = { rows: rows as JoinRow[] };
      }
      if (c === 'semi' || c === 'anti') return { form: c, on };
      const boxes = [...columnList.querySelectorAll<HTMLInputElement>('input')];
      const keep = boxes.every((b) => b.checked) ? null : boxes.filter((b) => b.checked).map((b) => b.value);
      if (prefix.value === '') return 'Give their names that mine has a prefix.';
      const pre = prefix.value;
      if (c === 'asOf') {
        const m = readRow(matchLine);
        if (!m.left || !m.right) return 'Choose the two columns the nearest row is found by.';
        return { form: 'asOf', match: { left: m.left, op: m.op as AsOfOp, right: m.right }, on, prefix: pre, keep };
      }
      return { form: 'join', kind: c, on, prefix: pre, keep };
    };

    let asked = 0;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const ask = async (): Promise<void> => {
      if (busy || done || !inputs) return;
      const mine = (asked += 1);
      willList.replaceChildren();
      primary.disabled = true;
      try {
        const h = await how();
        if (mine !== asked || done) return;
        if (typeof h === 'string') {
          say(h, '');
          return;
        }
        say('Asking what it will do…', 'working');
        const counts = await o.summary(their.value, h);
        if (mine !== asked || done) return;
        for (const line of summaryWords(h, counts)) el(doc, 'li', 'dc-join-will-line', willList, line);
        primary.disabled = false;
        say('', '');
      } catch (e) {
        if (mine !== asked || done) return;
        // the compiler's words: a column one side lacks, a name taken twice, an append of two types
        say(e instanceof Error ? e.message : String(e), 'failed');
      }
    };
    function changed(): void {
      shape();
      clearTimeout(timer);
      timer = setTimeout(() => void ask(), 250);
    }
    their.addEventListener('change', () => void load());
    choice.addEventListener('change', changed);
    pureOn.addEventListener('change', changed);
    pure.addEventListener('input', changed);
    prefix.addEventListener('input', changed);
    addRow.addEventListener('click', () => { rowLine(rowsBox, JOIN_OPS, { op: '==' }, true); changed(); });

    primary.addEventListener('click', () => void (async () => {
      if (busy || done) return;
      clearTimeout(timer);
      // busy from the first: the window is not closed while what it says is read (a condition in Pure, parsed)
      busy = true;
      win.classList.add('dc-busy');
      const h = await how().catch((e: unknown) => (e instanceof Error ? e.message : String(e)));
      if (typeof h === 'string') {
        busy = false;
        win.classList.remove('dc-busy');
        say(h, 'failed');
        return;
      }
      say('Joining…', 'working');
      try {
        await o.apply(their.value, h);
        made = true;
        busy = false;
        finish();
      } catch (e) {
        busy = false;
        win.classList.remove('dc-busy');
        say(e instanceof Error ? e.message : String(e), 'failed');
      }
    })());

    shape();
    void load();
  });
}
