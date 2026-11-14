// JOIN… (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §17.5), a window of its own in the Create a Class window's style: mine
// and their grid (another of the page's), how the two combine -- a tile each, its picture and what it does (four kinds,
// as of, keep the rows with a match or with none, append) --, the condition -- rows of my column, a comparison, their column, a key suggested when one is plain, or the
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
import { isEveryPair, SIMILAR_WITHIN } from '../join-how.ts';
import { closeMark } from './tile-icons.ts';

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
  /**
   * Where the window starts (a right-click's Join ▸: the column clicked, or a quick join that would repeat rows of mine):
   * their grid, the key it starts on in place of a suggestion, and the form chosen.
   */
  readonly start?: { readonly their?: string; readonly row?: JoinRow; readonly form?: Choice; readonly how?: JoinHow;
    /** The first row's comparison, in place of a suggestion's (a flow's Fuzzy Match: similar). */
    readonly op?: JoinOp;
    /** Each of mine with one of theirs at most, ticked from the start (a flow's Find Replace: a lookup, C13). */
    readonly once?: true };
  /** What the window is for, under its title (none: a new grid of the page's two); its button's word (none: Join). A
   *  flow's Join box set again says so (docs/DATACUBE_FLOW_MODE_2026_10_24.md M2). */
  readonly lead?: string;
  readonly action?: string;
}

/** What the window's form offers, each a form of join.ts's `how`. */
export type Choice = JoinKindWord | 'asOf' | 'semi' | 'anti' | 'append' | 'cross';
/**
 * Each form as its tile says it (the user, 2026-10-23: the window was messy beside Create a Class's): a short name, a
 * picture of what it keeps of the two (`picture`, below), and what it does, in a line.
 */
const CHOICES: readonly { readonly value: Choice; readonly label: string; readonly says: string }[] = [
  { value: 'left', label: 'All of mine', says: 'Every row of mine, with theirs where the condition matches -- theirs left empty where none does.' },
  { value: 'inner', label: 'Matching only', says: 'Only the rows of mine that match one of theirs, side by side.' },
  { value: 'right', label: 'All of theirs', says: 'Every row of theirs, with mine where the condition matches.' },
  { value: 'full', label: 'Everything', says: 'Every row of both, side by side where they match.' },
  { value: 'asOf', label: 'Nearest (as of)', says: 'Each row of mine with the nearest of theirs by a date or a number -- a price as of a trade\'s time.' },
  { value: 'semi', label: 'Mine with a match', says: 'The rows of mine that have a match: mine alone, each once.' },
  { value: 'anti', label: 'Mine with none', says: 'The rows of mine that have no match: mine alone, each once.' },
  { value: 'append', label: 'Append theirs', says: 'Their rows under mine, column by column: no condition.' },
  { value: 'cross', label: 'Every pair', says: 'Each row of mine beside every row of theirs: no condition -- a small table\'s values on each of my rows (Alteryx\'s Append Fields).' },
];
const OP_WORDS: Readonly<Record<JoinOp, string>> = { '==': '=', '!=': '≠', '<': '<', '<=': '≤', '>': '>', '>=': '≥', '~': '≈ similar' };

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

// A FORM'S PICTURE: mine the left circle, theirs the right, what it keeps filled -- or, appended, theirs under mine
const SVG = 'http://www.w3.org/2000/svg';
const MINE = 'M11 2a7 7 0 1 0 0 14a7 7 0 1 0 0-14Z';
const THEIRS = 'M19 2a7 7 0 1 0 0 14a7 7 0 1 0 0-14Z';
const BOTH = 'M15 3.26A7 7 0 0 1 15 14.74A7 7 0 0 1 15 3.26Z';
const MINE_ALONE = 'M15 3.26A7 7 0 1 0 15 14.74A7 7 0 0 1 15 3.26Z';
const PICTURES: Readonly<Record<Choice, readonly { readonly d: string; readonly filled: boolean; readonly dashed?: boolean }[]>> = {
  left: [{ d: MINE, filled: true }, { d: THEIRS, filled: false }],
  inner: [{ d: BOTH, filled: true }, { d: MINE, filled: false }, { d: THEIRS, filled: false }],
  right: [{ d: THEIRS, filled: true }, { d: MINE, filled: false }],
  full: [{ d: MINE, filled: true }, { d: THEIRS, filled: true }],
  asOf: [{ d: MINE, filled: true }, { d: THEIRS, filled: false, dashed: true }],
  semi: [{ d: BOTH, filled: true }, { d: MINE, filled: false }],
  anti: [{ d: MINE_ALONE, filled: true }, { d: MINE, filled: false }],
  append: [{ d: 'M8 2h14v6H8Z', filled: true }, { d: 'M8 10h14v6H8Z', filled: false }],
  cross: [{ d: 'M3 3h11v12H3Z', filled: true }, { d: 'M16 3h11v12H16Z', filled: true }, { d: 'M3 9h24', filled: false }],
};

function picture(doc: Document, choice: Choice): SVGSVGElement {
  const svg = doc.createElementNS(SVG, 'svg');
  svg.setAttribute('viewBox', '0 0 30 18');
  svg.setAttribute('class', 'dc-join-picture');
  svg.setAttribute('aria-hidden', 'true');
  for (const part of PICTURES[choice]) {
    const path = doc.createElementNS(SVG, 'path');
    path.setAttribute('d', part.d);
    path.setAttribute('class', `${part.filled ? 'dc-join-fill' : 'dc-join-line'}${part.dashed ? ' dc-join-dashed' : ''}`);
    svg.append(path);
  }
  return svg;
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
    el(doc, 'p', 'dc-picker-subtitle', titles, o.lead ?? `Combine ${o.mine} with another grid of the page: a new grid of the rows they make together.`);
    const close = closeMark(el(doc, 'button', 'dc-picker-close', head, '×'));
    close.type = 'button';
    close.setAttribute('aria-label', 'Close');
    close.addEventListener('click', () => { if (!busy) finish(); });

    const body = el(doc, 'div', 'dc-save-body', win);
    // THE TWO GRIDS: mine, and theirs chosen among the page's
    const grids = el(doc, 'div', 'dc-join-grids', body);
    const mineSide = el(doc, 'div', 'dc-join-side', grids);
    el(doc, 'span', 'dc-picker-field-label', mineSide, 'Mine');
    el(doc, 'div', 'dc-join-name', mineSide, o.mine);
    const mineCount = el(doc, 'div', 'dc-join-count', mineSide);
    el(doc, 'div', 'dc-join-between', grids, '⋈').setAttribute('aria-hidden', 'true');
    const theirSide = el(doc, 'label', 'dc-join-side', grids);
    el(doc, 'span', 'dc-picker-field-label', theirSide, 'Theirs');
    const their = select(doc, 'dc-join-their', theirSide, 'Their grid', o.others.map((c) => ({ value: c.id, label: c.name })));
    if (o.start?.their !== undefined && o.others.some((c) => c.id === o.start!.their)) their.value = o.start.their;
    const theirCount = el(doc, 'div', 'dc-join-count', theirSide);

    // HOW THEY COMBINE: a tile a form, each a picture and a name, what the chosen one does said under them
    const kindsCard = el(doc, 'div', 'dc-join-how', body);
    el(doc, 'div', 'dc-save-card-title', kindsCard, 'How they combine');
    const kinds = el(doc, 'div', 'dc-join-kinds', kindsCard);
    kinds.setAttribute('role', 'radiogroup');
    kinds.setAttribute('aria-label', 'How they combine');
    let chosen: Choice = 'left';
    const tiles = CHOICES.map((c) => {
      const tile = el(doc, 'button', 'dc-join-kind', kinds);
      tile.type = 'button';
      tile.dataset['kind'] = c.value;
      tile.setAttribute('role', 'radio');
      tile.title = c.says;
      tile.append(picture(doc, c.value));
      el(doc, 'span', 'dc-join-kind-label', tile, c.label);
      tile.addEventListener('click', () => choose(c.value));
      return tile;
    });
    const kindSays = el(doc, 'p', 'dc-join-kind-says', kindsCard);
    // the arrow keys move along the tiles, as a group of radio buttons does
    kinds.addEventListener('keydown', (e) => {
      const step = e.key === 'ArrowRight' || e.key === 'ArrowDown' ? 1 : e.key === 'ArrowLeft' || e.key === 'ArrowUp' ? -1 : 0;
      if (step === 0) return;
      e.preventDefault();
      const at = CHOICES.findIndex((c) => c.value === chosen);
      const next = CHOICES[(at + step + CHOICES.length) % CHOICES.length]!.value;
      choose(next);
      tiles.find((t) => t.dataset['kind'] === next)?.focus();
    });

    // THE CONDITION: rows of my column, a comparison, their column -- or Pure
    const condition = el(doc, 'div', 'dc-save-card dc-join-condition', body);
    const conditionHead = el(doc, 'div', 'dc-join-card-head', condition);
    el(doc, 'div', 'dc-save-card-title', conditionHead, 'Match rows where');
    const pureLabel = el(doc, 'label', 'dc-join-pure-label', conditionHead);
    const pureOn = el(doc, 'input', 'dc-join-pure-on', pureLabel);
    pureOn.type = 'checkbox';
    pureLabel.append(' Write it in Pure');
    const suggested = el(doc, 'div', 'dc-join-suggested', condition);
    const rowsHead = el(doc, 'div', 'dc-join-row dc-join-row-head', condition);
    for (const h of ['My column', '', 'Their column']) el(doc, 'span', 'dc-picker-field-label', rowsHead, h);
    const rowsBox = el(doc, 'div', 'dc-join-rows', condition);
    const addRow = el(doc, 'button', 'dc-picker-button dc-quiet dc-join-add', condition, '+ Add a condition');
    addRow.type = 'button';
    // each of mine with one of theirs at most: their first row of each key (join.ts once), offered on equal keys alone
    const onceLabel = el(doc, 'label', 'dc-join-once', condition);
    const once = el(doc, 'input', 'dc-join-once-on', onceLabel);
    once.type = 'checkbox';
    once.checked = o.start?.how?.form === 'join' ? o.start.how.once === true : o.start?.once === true;
    onceLabel.append(' Each of mine once: where a key of theirs repeats, the first of theirs');
    onceLabel.title = 'A key theirs has on several rows matches its first (in an order of their other columns, rising), so a row of mine is never repeated: a lookup';
    once.addEventListener('change', () => changed());
    // each pair's likeness on its similar row (join.ts score: a Fuzzy Match's), offered with a similar row on matching rows
    const scoreLabel = el(doc, 'label', 'dc-join-once dc-join-score', condition);
    const score = el(doc, 'input', 'dc-join-score-on', scoreLabel);
    score.type = 'checkbox';
    const started = o.start?.how?.form === 'join' ? o.start.how.score : o.start?.op === '~' ? 'score' : undefined;
    score.checked = started !== undefined;
    scoreLabel.append(' Score each pair: its likeness, 0 to 100, as ');
    const scoreName = el(doc, 'input', 'dc-picker-input dc-join-score-name', scoreLabel);
    scoreName.value = started ?? 'score';
    scoreName.setAttribute('aria-label', 'The score column\'s name');
    scoreLabel.title = '100 less its edits apart for each 100 letters of the longer text, rounded: the database measures it';
    score.addEventListener('change', () => changed());
    scoreName.addEventListener('input', () => changed());
    const pure = el(doc, 'textarea', 'dc-picker-input dc-join-pure', condition);
    pure.setAttribute('aria-label', 'The condition in Pure');
    pure.spellcheck = false;
    pure.hidden = true;
    const pureHint = el(doc, 'p', 'dc-join-hint', condition);
    pureHint.hidden = true;
    // as of: the column the nearest of theirs is found by, besides the condition
    const matchBlock = el(doc, 'div', 'dc-join-match-block', condition);
    el(doc, 'span', 'dc-picker-field-label', matchBlock, 'Nearest by');
    const matchLine = el(doc, 'div', 'dc-join-row dc-join-match', matchBlock);

    // THEIR COLUMNS: brought over, and the prefix for names both have
    const columnsCard = el(doc, 'div', 'dc-save-card dc-join-columns', body);
    const columnsHead = el(doc, 'div', 'dc-join-card-head', columnsCard);
    el(doc, 'div', 'dc-save-card-title', columnsHead, 'Their columns brought over');
    const columnCount = el(doc, 'span', 'dc-join-column-count', columnsHead);
    const columnList = el(doc, 'div', 'dc-join-column-list', columnsCard);
    const prefixLine = el(doc, 'label', 'dc-join-prefix-line', columnsCard);
    el(doc, 'span', 'dc-picker-field-label', prefixLine, 'Before their names that mine has');
    const prefix = el(doc, 'input', 'dc-picker-input dc-join-prefix', prefixLine);
    prefix.type = 'text';
    prefix.value = DEFAULT_PREFIX;
    prefix.spellcheck = false;

    // WHAT IT WILL DO: how many of my rows match, as a bar, and in words
    const will = el(doc, 'div', 'dc-save-card dc-join-will', body);
    el(doc, 'div', 'dc-save-card-title', will, 'What it will do');
    const meter = el(doc, 'div', 'dc-join-meter', will);
    meter.setAttribute('aria-hidden', 'true');
    meter.hidden = true;
    const meterFill = el(doc, 'div', 'dc-join-meter-fill', meter);
    const willList = el(doc, 'ul', 'dc-join-will-list', will);

    const status = el(doc, 'div', 'dc-picker-status', win);
    status.setAttribute('role', 'status');
    const foot = el(doc, 'div', 'dc-save-foot', win);
    const cancel = el(doc, 'button', 'dc-picker-button dc-quiet', foot, 'Cancel');
    cancel.type = 'button';
    cancel.addEventListener('click', () => { if (!busy) finish(); });
    el(doc, 'span', 'dc-save-spacer', foot).setAttribute('aria-hidden', 'true');
    const primary = el(doc, 'button', 'dc-picker-button dc-primary', foot, o.action ?? 'Join');
    primary.type = 'button';
    primary.disabled = true;
    const say = (text: string, kind: 'working' | 'failed' | 'ok' | ''): void => {
      status.className = `dc-picker-status${kind ? ` dc-${kind}` : ''}`;
      status.textContent = text;
    };
    /** The form chosen: its tile checked, what it does said. */
    function choose(c: Choice): void {
      chosen = c;
      for (const t of tiles) {
        const on = t.dataset['kind'] === c;
        t.setAttribute('aria-checked', String(on));
        t.tabIndex = on ? 0 : -1;
        t.classList.toggle('dc-chosen', on);
      }
      kindSays.textContent = CHOICES.find((x) => x.value === c)!.says;
      changed();
    }

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
      // a similar row's edits apart: its own field, beside its comparison while it is similar
      const within = el(doc, 'input', 'dc-picker-input dc-join-within', line);
      within.type = 'number';
      within.min = '0';
      within.step = '1';
      within.value = String(row.within ?? SIMILAR_WITHIN);
      within.title = 'How many edits apart two texts may be: a letter added, taken away or changed is one';
      within.setAttribute('aria-label', 'Edits apart at most');
      const shown = (): void => { within.hidden = op.value !== '~'; };
      shown();
      op.addEventListener('change', shown);
      within.addEventListener('input', changed);
      for (const s of [left, op, right]) s.addEventListener('change', changed);
      if (removable) {
        const remove = closeMark(el(doc, 'button', 'dc-picker-button dc-quiet dc-join-remove', line, '×'));
        remove.type = 'button';
        remove.setAttribute('aria-label', 'Take this condition off');
        remove.addEventListener('click', () => { line.remove(); changed(); });
      }
    };
    const readRow = (line: Element): Partial<JoinRow> => {
      const op = (line.querySelector('.dc-join-op') as HTMLSelectElement).value as JoinOp;
      const within = Number((line.querySelector('.dc-join-within') as HTMLInputElement | null)?.value ?? SIMILAR_WITHIN);
      return {
        left: (line.querySelector('.dc-join-left') as HTMLSelectElement).value, op,
        right: (line.querySelector('.dc-join-right') as HTMLSelectElement).value,
        ...(op === '~' ? { within: Number.isInteger(within) && within >= 0 ? within : SIMILAR_WITHIN } : {}),
      };
    };

    /** The window's parts shown as the form chosen asks. */
    const shape = (): void => {
      const c = chosen;
      condition.hidden = c === 'append' || c === 'cross';
      matchBlock.hidden = c !== 'asOf';
      columnsCard.hidden = c === 'semi' || c === 'anti' || c === 'append';
      pure.hidden = !pureOn.checked;
      pureHint.hidden = !pureOn.checked;
      rowsBox.hidden = pureOn.checked;
      rowsHead.hidden = pureOn.checked || rowsBox.children.length === 0;
      suggested.hidden = pureOn.checked || suggested.children.length === 0;
      addRow.hidden = pureOn.checked;
      onceLabel.hidden = !onceOffered();
      scoreLabel.hidden = !scoreOffered();
      const renamed = c === 'semi' || c === 'anti' ? 'by their own names' : 'as they come out (a name mine has, prefixed)';
      pureHint.textContent = `Two rows, mine and theirs, their columns named ${renamed}: {a, b | $a.cust == $b.id}`;
      // the prefix said on their names that mine has, and how many of theirs come
      if (inputs) {
        const names = theirNames(inputs.left.columns.map((x) => x.name), inputs.right.columns.map((x) => x.name), prefix.value);
        const boxes = [...columnList.querySelectorAll<HTMLInputElement>('input')];
        for (const box of boxes) {
          const out = names.get(box.value) ?? box.value;
          const as = box.parentElement!.querySelector('.dc-join-column-out')!;
          as.textContent = out === box.value ? '' : ` → ${out}`;
          box.parentElement!.classList.toggle('dc-join-column-off', !box.checked);
        }
        columnCount.textContent = `${boxes.filter((b) => b.checked).length} of ${boxes.length}`;
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
      const columns = (n: number): string => `${n} column${n === 1 ? '' : 's'}`;
      mineCount.textContent = columns(inputs.left.columns.length);
      theirCount.textContent = columns(inputs.right.columns.length);
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
      // a join set again (a flow's Join box): its own condition, match, prefix and columns, while their grid is its own
      const was = o.start?.how !== undefined && their.value === o.start.their ? o.start.how : undefined;
      const wasRows = was !== undefined && was.form !== 'append' && 'rows' in was.on && was.on.rows.length > 0 ? was.on.rows : undefined;
      // the key it was opened on, while their grid is the one it was opened at; else one plain key; else a row to fill
      const given = o.start?.row !== undefined && their.value === o.start.their ? o.start.row : undefined;
      if (wasRows !== undefined) for (const r of wasRows) rowLine(rowsBox, JOIN_OPS, r, true);
      else {
        const first: Partial<JoinRow> = given ?? (keys.length === 1 ? keys[0]!.row : { op: '==' });
        rowLine(rowsBox, JOIN_OPS, o.start?.op !== undefined ? { ...first, op: o.start.op } : first, true);
      }
      rowLine(matchLine, AS_OF_OPS, was?.form === 'asOf' ? was.match : { op: '>=' }, false);
      if (was !== undefined && (was.form === 'join' || was.form === 'asOf')) prefix.value = was.prefix;
      const kept = was !== undefined && (was.form === 'join' || was.form === 'asOf') ? was.keep : null;
      columnList.replaceChildren();
      for (const c of inputs.right.columns) {
        const label = el(doc, 'label', 'dc-join-column', columnList);
        const box = el(doc, 'input', '', label);
        box.type = 'checkbox';
        box.value = c.name;
        box.checked = kept === null || kept.includes(c.name);
        el(doc, 'span', 'dc-join-column-name', label, c.name);
        el(doc, 'span', 'dc-join-column-out', label);
        el(doc, 'span', 'dc-join-column-type', label, c.type);
        box.addEventListener('change', changed);
      }
      shape();
      changed();
    };

    /** Whether one of theirs for each of mine is offered: a join on rows of equal keys, at least one (join-how.ts onceRefusal). */
    function onceOffered(): boolean {
      // a lookup of mine: a matching or a left join's (join-how.ts onceRefusal)
      if (chosen !== 'inner' && chosen !== 'left') return false;
      if (pureOn.checked) return false;
      const rows = [...rowsBox.children];
      return rows.length > 0 && rows.every((r) => (r.querySelector('.dc-join-op') as HTMLSelectElement | null)?.value === '==');
    }

    /** Whether a score is offered: matching rows only, on rows with a similar one (join-how.ts scoreRefusal). */
    function scoreOffered(): boolean {
      if (chosen !== 'inner' || pureOn.checked) return false;
      return [...rowsBox.children].some((r) => (r.querySelector('.dc-join-op') as HTMLSelectElement | null)?.value === '~');
    }

    /** The `how` the window says now, or what it still needs. */
    const how = async (): Promise<JoinHow | string> => {
      const c = chosen;
      if (c === 'append') return { form: 'append' };
      let on: JoinCondition;
      // every pair: no condition, their columns kept and prefixed as a join's are
      if (c === 'cross') on = { rows: [] };
      else if (pureOn.checked) {
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
      const scored = score.checked && scoreOffered();
      if (scored && scoreName.value.trim() === '') return 'Name the score\'s column, or take the score off.';
      return {
        form: 'join', kind: c === 'cross' ? 'inner' : c, on, prefix: pre, keep, ...(once.checked && onceOffered() ? { once: true as const } : {}),
        ...(scored ? { score: scoreName.value.trim() } : {}),
      };
    };

    let asked = 0;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const ask = async (): Promise<void> => {
      if (busy || done || !inputs) return;
      const mine = (asked += 1);
      willList.replaceChildren();
      meter.hidden = true;
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
        // how many of my rows find a match (or a nearest row): the bar's filled part
        if (h.form !== 'append' && counts.matched !== undefined && counts.mine > 0) {
          meter.hidden = false;
          meterFill.style.width = `${Math.round((100 * counts.matched) / counts.mine)}%`;
        }
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

    const startHow = o.start?.how;
    choose(o.start?.form ?? (startHow === undefined ? 'left' : isEveryPair(startHow) ? 'cross'
      : startHow.form === 'join' ? startHow.kind : startHow.form));
    void load();
  });
}
