// A calculated column's editor: ONE COLUMN PER WINDOW, as upstream's
// DataCubeColumnEditor -- "Add New Column" windows, any number open,
// and one "Edit Column" window per existing column.
//
// Upstream's form, top to bottom: the name with a live ✓/✗, the kind
// (Leaf Level Measure / Leaf Level Dimension / Group Level), the
// expression, then Cancel / Delete / Reset / OK. The expression is
// COMPILED as it is typed (500 ms after the last key, as upstream) --
// the cube with this column in it, planned and never run -- and OK
// waits for a clean compile. Upstream's Value Type check is not here:
// it needs the compiler's type of the expression (engine
// `lambdaRelationType`), and a type is never found by running a query.
// On a plane that cannot compile without running, the check happens
// on OK, as it did before, and the form says so.
//
// Kept from ours: the completion list of everything in scope at the
// column's level, inserted at the caret.

import { columnsInScope, completionsFor, nameProblem, type CalcStage, type Completion } from '../calc.ts';
import { ident } from '../serialize.ts';
import { docHint } from './docs.ts';
import {
  WINDOW_FUNCTIONS,
  isNumericType,
  renameColumnReferences,
  rowColumns,
  type CubeSnapshot,
  type DerivedColumn,
  type WindowFrame,
  type WindowFunction,
  type WindowSpec,
} from '../snapshot.ts';

/** Upstream's DataCubeExtendedColumnKind. */
export type ColumnLevel = 'measure' | 'dimension' | 'group';

export const LEVELS: readonly { value: ColumnLevel; label: string }[] = [
  { value: 'measure', label: 'Leaf Level Measure' },
  { value: 'dimension', label: 'Leaf Level Dimension' },
  { value: 'group', label: 'Group Level' },
];

/** A new column (seeded, as "Extend Column X..." does), or one to edit. */
export type ColumnEditorStart =
  | { readonly expression?: string; readonly level?: ColumnLevel }
  | { readonly edit: string };

export interface CompileOutcome {
  /** The Pure that was compiled, to place a position the refusal names. */
  readonly pure: string;
  /** The compiler's refusal; null when it compiles. */
  readonly refusal: string | null;
}

export interface ColumnEditorOptions {
  /** The cube as it is NOW: other windows change it under this one. */
  readonly snapshot: () => CubeSnapshot;
  readonly start: ColumnEditorStart;
  /** Compile without running; undefined where the plane cannot. */
  readonly compile: (
    candidate: CubeSnapshot,
    signal: AbortSignal,
  ) => Promise<CompileOutcome | undefined>;
  /** Offer both lists to the cube: its refusal, or null when it took them. */
  readonly apply: (
    row: readonly DerivedColumn[],
    group: readonly DerivedColumn[],
    rename?: { readonly from: string; readonly to: string },
  ) => Promise<string | null>;
  /** The window goes: after OK or Delete succeeds, or on Cancel. */
  readonly onClose: () => void;
  /** Upstream's 500 ms; a test passes 0. */
  readonly debounceMs?: number;
}

interface Draft {
  name: string;
  level: ColumnLevel;
  /** An expression over one row, or a window over the rows around it. */
  mode: 'expression' | 'window';
  expression: string;
  window: WindowSpec;
}

/** A window to start from: a running sum, order to be chosen. */
const NEW_WINDOW: WindowSpec = { fn: 'sum', partition: [], order: [], frame: 'running' };

type Check =
  | { readonly state: 'idle' }
  | { readonly state: 'compiling' }
  | { readonly state: 'ok' }
  | { readonly state: 'unavailable' }
  | { readonly state: 'refused'; readonly message: string; readonly caret?: string };

export class ColumnEditor {
  readonly #root: HTMLElement;
  readonly #doc: Document;
  readonly #options: ColumnEditorOptions;
  /** The column's name when editing began; absent when adding. */
  readonly #original: string | undefined;
  readonly #initial: Draft;
  #draft: Draft;
  #check: Check = { state: 'idle' };
  /** A refusal from the cube on OK or Delete, shown until the next edit. */
  #refusal: string | null = null;
  #busy = false;
  #timer: ReturnType<typeof setTimeout> | undefined;
  #inflight: AbortController | null = null;
  #els!: {
    nameMark: HTMLElement;
    check: HTMLElement;
    problem: HTMLElement;
    ok: HTMLButtonElement;
  };

  constructor(container: HTMLElement, options: ColumnEditorOptions) {
    this.#root = container;
    this.#doc = container.ownerDocument;
    this.#options = options;
    const start = options.start;
    if ('edit' in start) {
      const found = findColumn(options.snapshot(), start.edit);
      this.#original = start.edit;
      this.#initial = found ?? { name: start.edit, level: 'measure', mode: 'expression', expression: '', window: NEW_WINDOW };
    } else {
      this.#original = undefined;
      this.#initial = {
        name: freshName(options.snapshot()),
        // MEASURE by default, as upstream (DataCubeNewColumnState).
        level: start.level ?? 'measure',
        mode: 'expression',
        expression: start.expression ?? '',
        window: NEW_WINDOW,
      };
    }
    this.#draft = { ...this.#initial, window: { ...this.#initial.window } };
    this.#render();
    this.#schedule(0);
  }

  /** The column this window edits, if it edits one. */
  get editing(): string | undefined {
    return this.#original;
  }

  /** The cube changed under this window: compile again. */
  recheck(): void {
    this.#schedule(0);
  }

  /** The window is going: nothing may land in it afterwards. */
  dispose(): void {
    clearTimeout(this.#timer);
    this.#inflight?.abort();
    this.#inflight = null;
  }

  // -- the draft as the cube would take it --------------------------------

  #stage(): CalcStage {
    return this.#draft.level === 'group' ? 'group' : 'row';
  }

  /** The cube's two lists with this draft in place of the original. */
  #lists(): { row: DerivedColumn[]; group: DerivedColumn[] } {
    const s = this.#options.snapshot();
    const without = (list: readonly DerivedColumn[]): DerivedColumn[] =>
      list.filter((d) => d.name !== this.#original);
    const row = without(s.derived);
    const group = without(s.groupDerived ?? []);
    const d = this.#draft;
    const next: DerivedColumn = {
      name: d.name.trim(),
      expression: d.mode === 'window' ? '' : d.expression.trim(),
      ...(d.mode === 'window' ? { window: this.#window() } : {}),
      // A group-level column is already past the aggregation, so it has
      // no measure-or-dimension to decide; upstream draws the same line.
      ...(d.level === 'group' ? {} : { kind: d.level }),
    };
    // In its old place when it stays at its stage, so an edit does not
    // reorder the cube's columns.
    const target = d.level === 'group' ? group : row;
    const was = (d.level === 'group' ? s.groupDerived ?? [] : s.derived)
      .findIndex((x) => x.name === this.#original);
    if (was >= 0) target.splice(was, 0, next);
    else target.push(next);
    return { row, group };
  }

  #rename(): { from: string; to: string } | undefined {
    const to = this.#draft.name.trim();
    return this.#original !== undefined && this.#original !== to
      ? { from: this.#original, to }
      : undefined;
  }

  #candidate(): CubeSnapshot {
    const { row, group } = this.#lists();
    const next: CubeSnapshot = { ...this.#options.snapshot(), derived: row, groupDerived: group };
    const rename = this.#rename();
    return rename ? renameColumnReferences(next, rename.from, rename.to) : next;
  }

  /** What the calculation still lacks, or null when it is complete. */
  #bodyProblem(): string | null {
    const d = this.#draft;
    if (d.mode === 'expression') {
      return d.expression.trim().length === 0
        ? 'An expression is required — e.g. $x.notional * 1.05' : null;
    }
    const w = d.window;
    const meta = WINDOW_FUNCTIONS.find((f) => f.fn === w.fn);
    if (meta?.column && !w.column) return 'Choose the column the window reads';
    // At the group level an empty order is the grid's own order.
    if (meta?.ordered && w.order.length === 0 && this.#stage() === 'row') {
      return 'Choose an order: this function means nothing without one';
    }
    if (w.partition.length === 0 && w.order.length === 0 && this.#stage() === 'row' && !w.column) {
      return 'Choose a partition or an order';
    }
    return null;
  }

  /** The draft window, tidied: only what its function takes. */
  #window(): WindowSpec {
    const w = this.#draft.window;
    const meta = WINDOW_FUNCTIONS.find((f) => f.fn === w.fn);
    return {
      fn: w.fn,
      ...(meta?.column && w.column ? { column: w.column } : {}),
      partition: [...w.partition],
      order: w.order.map((o) => ({ ...o })),
      ...(meta?.framed && w.frame !== undefined ? { frame: w.frame } : {}),
      ...((w.fn === 'lag' || w.fn === 'lead') && w.offset !== undefined ? { offset: w.offset } : {}),
      ...(w.fn === 'ntile' ? { buckets: w.buckets ?? 4 } : {}),
    };
  }

  #nameProblem(): string | null {
    const s = this.#options.snapshot();
    return nameProblem(s, this.#stage(), this.#draft.name,
      ...(this.#original === undefined ? [] : [this.#original]) as [string?]);
  }

  // -- the live compile ----------------------------------------------------

  #schedule(ms = this.#options.debounceMs ?? 500): void {
    clearTimeout(this.#timer);
    this.#inflight?.abort();
    this.#inflight = null;
    if (this.#bodyProblem() !== null || this.#nameProblem() !== null) {
      this.#check = { state: 'idle' };
      this.#paint();
      return;
    }
    this.#check = { state: 'compiling' };
    this.#paint();
    this.#timer = setTimeout(() => void this.#compile(), ms);
  }

  async #compile(): Promise<void> {
    const abort = new AbortController();
    this.#inflight = abort;
    const expression = this.#draft.expression.trim();
    let outcome: CompileOutcome | undefined;
    try {
      outcome = await this.#options.compile(this.#candidate(), abort.signal);
    } catch {
      return; // aborted: a newer compile owns the form
    }
    if (abort.signal.aborted || this.#inflight !== abort) return;
    this.#inflight = null;
    if (outcome === undefined) {
      this.#check = { state: 'unavailable' };
    } else if (outcome.refusal === null) {
      this.#check = { state: 'ok' };
    } else {
      const caret = caretFor(outcome.pure, outcome.refusal, this.#draft.name.trim(), expression);
      this.#check = {
        state: 'refused',
        message: outcome.refusal,
        ...(caret === undefined ? {} : { caret }),
      };
    }
    this.#paint();
  }

  // -- actions ---------------------------------------------------------------

  async #ok(): Promise<void> {
    if (this.#busy || !this.#canApply()) return;
    this.#busy = true;
    this.#paint();
    try {
      const { row, group } = this.#lists();
      this.#refusal = await this.#options.apply(row, group, this.#rename());
    } finally {
      this.#busy = false;
    }
    if (this.#refusal === null) {
      this.#options.onClose();
      return;
    }
    // THE FORM STAYS, with what the user typed and the cube's reason.
    this.#paint();
  }

  async #delete(): Promise<void> {
    if (this.#busy || this.#original === undefined) return;
    const s = this.#options.snapshot();
    const row = s.derived.filter((d) => d.name !== this.#original);
    const group = (s.groupDerived ?? []).filter((d) => d.name !== this.#original);
    this.#busy = true;
    this.#paint();
    try {
      // Refused too, when another column refers to this one.
      this.#refusal = await this.#options.apply(row, group);
    } finally {
      this.#busy = false;
    }
    if (this.#refusal === null) this.#options.onClose();
    else this.#paint();
  }

  #reset(): void {
    this.#draft = { ...this.#initial, window: { ...this.#initial.window } };
    this.#refusal = null;
    this.#render();
    this.#schedule(0);
  }

  #canApply(): boolean {
    return this.#nameProblem() === null
      && this.#bodyProblem() === null
      && (this.#check.state === 'ok' || this.#check.state === 'unavailable')
      && this.#refusal === null;
  }

  #edited(): void {
    this.#refusal = null;
    this.#schedule();
  }

  // -- rendering -------------------------------------------------------------

  #render(): void {
    const doc = this.#doc;
    this.#root.replaceChildren();
    this.#root.classList.add('dc-coleditor');
    const form = el(doc, 'div', 'dc-coleditor-form', this.#root);

    const nameRow = field(doc, form, 'Column Name:');
    const name = el(doc, 'input', 'dc-calc-input-name', nameRow) as HTMLInputElement;
    name.type = 'text';
    name.value = this.#draft.name;
    name.spellcheck = false;
    const nameMark = el(doc, 'span', 'dc-calc-namemark', nameRow);
    name.addEventListener('input', () => {
      this.#draft.name = name.value;
      this.#edited();
    });

    const levelRow = field(doc, form, 'Column Kind:');
    const level = el(doc, 'select', 'dc-calc-level', levelRow) as HTMLSelectElement;
    for (const l of LEVELS) {
      const o = doc.createElement('option');
      o.value = l.value;
      o.textContent = l.label;
      level.append(o);
    }
    level.value = this.#draft.level;
    levelRow.append(docHint(doc, 'data-cube.extended-column.levels'));
    level.addEventListener('change', () => {
      this.#draft.level = level.value as ColumnLevel;
      this.#edited();
      // The scope changes with the level, so the completion list does.
      this.#paintPicker(picker, expr);
      this.#paintWindow(winBox);
    });

    // EXPRESSION OR WINDOW: one row in, one value out -- or a value from
    // the rows around it (a running total, a rank, the previous period).
    const modeRow = field(doc, form, 'Calculation:');
    const mode = el(doc, 'select', 'dc-calc-mode', modeRow) as HTMLSelectElement;
    for (const [value, label] of [['expression', 'Expression'], ['window', 'Window (running, rank, previous…)']] as const) {
      const o = doc.createElement('option');
      o.value = value;
      o.textContent = label;
      mode.append(o);
    }
    mode.value = this.#draft.mode;
    const exprBox = el(doc, 'div', 'dc-calc-exprbox', form);
    const winBox = el(doc, 'div', 'dc-calc-window', form);
    const showMode = (): void => {
      exprBox.hidden = this.#draft.mode !== 'expression';
      winBox.hidden = this.#draft.mode !== 'window';
    };
    mode.addEventListener('change', () => {
      this.#draft.mode = mode.value as Draft['mode'];
      showMode();
      this.#paintWindow(winBox);
      this.#edited();
    });

    const exprRow = el(doc, 'label', 'dc-coleditor-code', exprBox);
    const expr = el(doc, 'textarea', 'dc-calc-input-expr', exprRow) as HTMLTextAreaElement;
    expr.value = this.#draft.expression;
    expr.rows = 4;
    expr.spellcheck = false;
    expr.setAttribute('aria-label', 'Expression');
    expr.placeholder = '$x.notional * 1.05';
    expr.addEventListener('input', () => {
      this.#draft.expression = expr.value;
      this.#edited();
    });

    const check = el(doc, 'div', 'dc-calc-check', form);
    check.setAttribute('role', 'status');
    const problem = el(doc, 'p', 'dc-calc-problem', form);
    problem.setAttribute('role', 'alert');

    const picker = el(doc, 'div', 'dc-calc-picker', exprBox);
    this.#paintPicker(picker, expr);
    this.#paintWindow(winBox);
    showMode();

    const footer = el(doc, 'div', 'dc-calc-footer', this.#root);
    const button = (label: string, cls: string, onClick: () => void): HTMLButtonElement => {
      const b = el(doc, 'button', `dc-button ${cls}`, footer) as HTMLButtonElement;
      b.type = 'button';
      b.textContent = label;
      b.addEventListener('click', onClick);
      return b;
    };
    button('Cancel', 'dc-calc-cancel', () => this.#options.onClose());
    if (this.#original !== undefined) {
      button('Delete', 'dc-calc-delete', () => void this.#delete());
      button('Reset', 'dc-calc-reset', () => this.#reset());
    }
    const ok = button('OK', 'dc-calc-ok', () => void this.#ok());

    this.#els = { nameMark, check, problem, ok };
    this.#paint();
  }

  /** Everything that follows the draft, without disturbing the inputs. */
  #paint(): void {
    const { nameMark, check, problem, ok } = this.#els;
    const nameProblemText = this.#nameProblem();
    nameMark.textContent = nameProblemText === null ? '✓' : '✗';
    nameMark.classList.toggle('dc-bad', nameProblemText !== null);
    nameMark.title = nameProblemText ?? 'The name is free';

    check.replaceChildren();
    check.dataset['state'] = this.#check.state;
    const c = this.#check;
    switch (c.state) {
      case 'idle':
        check.textContent = this.#bodyProblem() ?? '';
        break;
      case 'compiling':
        check.textContent = 'Compiling…';
        break;
      case 'ok':
        check.textContent = '✓ Compiles';
        break;
      case 'unavailable':
        check.textContent = 'This engine cannot compile without running: checked on OK';
        break;
      case 'refused': {
        const text = el(this.#doc, 'div', 'dc-calc-refusal', check);
        text.textContent = c.message;
        if (c.caret) {
          const pre = el(this.#doc, 'pre', 'dc-calc-caret', check);
          pre.textContent = c.caret;
        }
        break;
      }
    }

    const message = nameProblemText ?? this.#refusal;
    problem.textContent = message ?? '';
    problem.hidden = message === null;
    ok.disabled = this.#busy || !this.#canApply();
  }

  /** The window form: function, column, partition, order, frame. */
  #paintWindow(box: HTMLElement): void {
    const doc = this.#doc;
    box.replaceChildren();
    const w = this.#draft.window;
    const stage = this.#stage();
    const columns = columnsInScope(this.#options.snapshot(), stage, this.#original).map((c) => c.label);
    const meta = WINDOW_FUNCTIONS.find((f) => f.fn === w.fn) ?? WINDOW_FUNCTIONS[0]!;
    const changed = (repaint = false): void => {
      if (repaint) this.#paintWindow(box);
      this.#edited();
    };
    const select = (parent: HTMLElement, cls: string, options: readonly (readonly [string, string])[],
      value: string, onChange: (v: string) => void): HTMLSelectElement => {
      const sel = el(doc, 'select', cls, parent) as HTMLSelectElement;
      for (const [v, label] of options) {
        const o = doc.createElement('option');
        o.value = v;
        o.textContent = label;
        sel.append(o);
      }
      sel.value = value;
      sel.addEventListener('change', () => onChange(sel.value));
      return sel;
    };
    const number = (parent: HTMLElement, cls: string, value: number, onChange: (n: number) => void): void => {
      const input = el(doc, 'input', cls, parent) as HTMLInputElement;
      input.type = 'number';
      input.min = '1';
      input.value = String(value);
      input.style.width = '64px';
      input.addEventListener('input', () => {
        const n = Math.floor(Number(input.value));
        if (Number.isFinite(n) && n >= 1) onChange(n);
      });
    };
    const colOptions = columns.map((c) => [c, c] as const);

    select(field(doc, box, 'Function:'), 'dc-win-fn',
      WINDOW_FUNCTIONS.map((f) => [f.fn, f.label] as const), w.fn, (v) => {
        const next = WINDOW_FUNCTIONS.find((f) => f.fn === v)!;
        this.#draft.window = {
          ...w,
          fn: v as WindowFunction,
          ...(next.framed ? { frame: w.frame ?? 'running' } : {}),
        };
        changed(true);
      });
    if (meta.column) {
      select(field(doc, box, 'Of:'), 'dc-win-column',
        [['', 'Choose a column…'], ...colOptions], w.column ?? '', (v) => {
          const { column: _c, ...rest } = this.#draft.window;
          void _c;
          this.#draft.window = v === '' ? rest : { ...rest, column: v };
          changed();
        });
    }

    // PARTITION: restart for each value of these -- the grouping
    // columns: the row groups at the group level, the dimension columns
    // at the row level. Partitioning by a figure means nothing.
    const snapshot = this.#options.snapshot();
    const partitionable = stage === 'group'
      ? snapshot.rows.filter((r) => columns.includes(r))
      : rowColumns(snapshot).filter((c) => c.kind === 'dimension' && columns.includes(c.name))
        .map((c) => c.name);
    const part = field(doc, box, 'Partition by:');
    const partList = el(doc, 'div', 'dc-win-partition', part);
    for (const c of partitionable) {
      const label = el(doc, 'label', 'dc-win-part', partList);
      const box2 = el(doc, 'input', 'dc-win-part-check', label) as HTMLInputElement;
      box2.type = 'checkbox';
      box2.value = c;
      box2.checked = w.partition.includes(c);
      label.append(doc.createTextNode(` ${c}`));
      box2.addEventListener('change', () => {
        const now = this.#draft.window.partition;
        this.#draft.window = {
          ...this.#draft.window,
          partition: box2.checked ? [...now, c] : now.filter((x) => x !== c),
        };
        changed();
      });
    }
    if (partitionable.length === 0) {
      partList.textContent = stage === 'group' ? 'No row groups: one partition' : 'No dimension columns';
    }

    // ORDER: the rows' order within each partition.
    const orderRow = field(doc, box, 'Order by:');
    const orderList = el(doc, 'div', 'dc-win-order', orderRow);
    w.order.forEach((o, i) => {
      const line = el(doc, 'div', 'dc-win-order-line', orderList);
      const setAt = (next: { column?: string; direction?: 'asc' | 'desc' }): void => {
        const order = this.#draft.window.order.map((x, j) => (j === i ? { ...x, ...next } : x));
        this.#draft.window = { ...this.#draft.window, order };
        changed();
      };
      select(line, 'dc-win-order-column', colOptions, o.column, (v) => setAt({ column: v }));
      select(line, 'dc-win-order-direction', [['asc', 'Ascending'], ['desc', 'Descending']],
        o.direction, (v) => setAt({ direction: v as 'asc' | 'desc' }));
      const remove = el(doc, 'button', 'dc-button dc-win-order-remove', line) as HTMLButtonElement;
      remove.type = 'button';
      remove.textContent = '×';
      remove.setAttribute('aria-label', `Remove ${o.column} from the order`);
      remove.addEventListener('click', () => {
        this.#draft.window = {
          ...this.#draft.window,
          order: this.#draft.window.order.filter((_x, j) => j !== i),
        };
        changed(true);
      });
    });
    const add = el(doc, 'button', 'dc-button dc-win-order-add', orderList) as HTMLButtonElement;
    add.type = 'button';
    add.textContent = '+ Add';
    add.disabled = columns.length === 0;
    add.addEventListener('click', () => {
      const used = new Set(this.#draft.window.order.map((o) => o.column));
      const column = columns.find((c) => !used.has(c)) ?? columns[0];
      if (column === undefined) return;
      this.#draft.window = {
        ...this.#draft.window,
        order: [...this.#draft.window.order, { column, direction: 'asc' }],
      };
      changed(true);
    });
    if (stage === 'group' && w.order.length === 0) {
      el(doc, 'span', 'dc-win-hint', orderList).textContent = 'none: the order the grid shows';
    }

    if (meta.framed) {
      const frameRow = field(doc, box, 'Frame:');
      const f = w.frame ?? (w.fn === 'last' ? 'partition' : 'running');
      const kind = typeof f === 'object' ? 'last' : f;
      select(frameRow, 'dc-win-frame', [
        ['running', 'Running (from the start to this row)'],
        ['partition', 'Whole partition'],
        ['last', 'Moving (the last N rows)'],
      ], kind, (v) => {
        const frame: WindowFrame = v === 'last' ? { lastRows: 3 } : v as 'running' | 'partition';
        this.#draft.window = { ...this.#draft.window, frame };
        changed(true);
      });
      if (typeof f === 'object') {
        number(frameRow, 'dc-win-rows', f.lastRows, (n) => {
          this.#draft.window = { ...this.#draft.window, frame: { lastRows: n } };
          changed();
        });
      }
    }
    if (w.fn === 'lag' || w.fn === 'lead') {
      number(field(doc, box, 'Rows back / ahead:'), 'dc-win-offset', w.offset ?? 1, (n) => {
        this.#draft.window = { ...this.#draft.window, offset: n };
        changed();
      });
    }
    if (w.fn === 'ntile') {
      number(field(doc, box, 'Buckets:'), 'dc-win-buckets', w.buckets ?? 4, (n) => {
        this.#draft.window = { ...this.#draft.window, buckets: n };
        changed();
      });
    }
  }

  #paintPicker(picker: HTMLElement, expr: HTMLTextAreaElement): void {
    const doc = this.#doc;
    picker.replaceChildren();
    // Scope is the cube WITHOUT this column: a column cannot see itself.
    const s = this.#options.snapshot();
    const offered = completionsFor(s, this.#stage(), this.#original);
    const search = el(doc, 'input', 'dc-calc-search', picker) as HTMLInputElement;
    search.type = 'search';
    search.placeholder = `Insert — ${offered.length} in scope`;
    const items = el(doc, 'ul', 'dc-calc-items', picker);
    const paint = (filter: string): void => {
      items.replaceChildren();
      const q = filter.trim().toLowerCase();
      const shown = q.length === 0
        ? offered
        : offered.filter((c) => c.label.toLowerCase().includes(q)
          || c.detail.toLowerCase().includes(q));
      for (const c of shown.slice(0, 60)) this.#item(items, c, expr);
      if (shown.length === 0) {
        el(doc, 'li', 'dc-calc-noitem', items).textContent = 'Nothing in scope matches.';
      }
    };
    search.addEventListener('input', () => paint(search.value));
    paint('');
  }

  #item(into: HTMLElement, c: Completion, expr: HTMLTextAreaElement): void {
    const doc = this.#doc;
    const li = el(doc, 'li', `dc-calc-item-${c.kind}`, into);
    const b = el(doc, 'button', 'dc-calc-insert', li) as HTMLButtonElement;
    b.type = 'button';
    b.dataset['insert'] = c.insert;
    el(doc, 'span', 'dc-calc-item-label', b).textContent = c.label;
    el(doc, 'span', 'dc-calc-item-detail', b).textContent = c.detail;
    b.addEventListener('click', () => {
      // At the CARET, not appended: half of using this is fixing the
      // middle of an expression already written.
      const at = expr.selectionStart ?? expr.value.length;
      const to = expr.selectionEnd ?? at;
      expr.value = expr.value.slice(0, at) + c.insert + expr.value.slice(to);
      const after = at + c.insert.length;
      expr.setSelectionRange(after, after);
      expr.focus();
      this.#draft.expression = expr.value;
      this.#edited();
    });
  }
}

// -- helpers ---------------------------------------------------------------

function el(doc: Document, tag: string, className: string, parent: HTMLElement): HTMLElement {
  const e = doc.createElement(tag);
  e.className = className;
  parent.append(e);
  return e;
}

function field(doc: Document, form: HTMLElement, label: string): HTMLElement {
  const row = el(doc, 'div', 'dc-field', form);
  el(doc, 'span', 'dc-field-label', row).textContent = label;
  return row;
}

/** Upstream's `col_{N+1}`, unique in the cube. */
function freshName(s: CubeSnapshot): string {
  let n = s.columns.length + s.derived.length + (s.groupDerived ?? []).length + 1;
  while (nameProblem(s, 'row', `col_${n}`) !== null) n += 1;
  return `col_${n}`;
}

/** An existing calculated column as a draft, or undefined if it has gone. */
function findColumn(s: CubeSnapshot, name: string): Draft | undefined {
  const row = s.derived.find((d) => d.name === name);
  if (row) {
    return {
      name,
      // A column saved before kinds were declared behaves as its type.
      level: row.kind ?? (isNumericType(row.type) ? 'measure' : 'dimension'),
      mode: row.window ? 'window' : 'expression',
      expression: row.expression,
      window: row.window ?? NEW_WINDOW,
    };
  }
  const group = (s.groupDerived ?? []).find((d) => d.name === name);
  return group
    ? {
      name, level: 'group', mode: group.window ? 'window' : 'expression',
      expression: group.expression, window: group.window ?? NEW_WINDOW,
    }
    : undefined;
}

/**
 * Where the refusal points INSIDE the expression, as the line with a
 * caret under it -- when the compiler named a `[line:col]` and it falls
 * in the expression. The compiled text is the cube's whole query; the
 * expression sits in it after `~[<name>: x|`, so its offset is found
 * there, never guessed.
 */
export function caretFor(
  pure: string,
  refusal: string,
  name: string,
  expression: string,
): string | undefined {
  const at = /\[(\d+):(\d+)\]/.exec(refusal);
  if (!at) return undefined;
  const head = `~[${ident(name)}: x|`;
  const start = pure.indexOf(head);
  if (start < 0) return undefined;
  const exprStart = start + head.length;
  if (pure.slice(exprStart, exprStart + expression.length) !== expression) return undefined;
  const offset = offsetOf(pure, Number(at[1]), Number(at[2]));
  const rel = offset - exprStart;
  if (rel < 0 || rel > expression.length) return undefined;
  const lineStart = expression.lastIndexOf('\n', rel - 1) + 1;
  const lineEnd = expression.indexOf('\n', rel);
  const line = expression.slice(lineStart, lineEnd < 0 ? undefined : lineEnd);
  return `${line}\n${' '.repeat(rel - lineStart)}^`;
}

/** A 1-based `[line:col]` as a character offset. */
function offsetOf(text: string, line: number, col: number): number {
  let offset = 0;
  for (let l = 1; l < line; l += 1) {
    const nl = text.indexOf('\n', offset);
    if (nl < 0) return text.length;
    offset = nl + 1;
  }
  return offset + col - 1;
}
