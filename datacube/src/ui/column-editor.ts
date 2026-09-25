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

import { completionsFor, nameProblem, type CalcStage, type Completion } from '../calc.ts';
import { ident } from '../serialize.ts';
import {
  isNumericType,
  renameColumnReferences,
  type CubeSnapshot,
  type DerivedColumn,
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
  expression: string;
}

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
      this.#initial = found ?? { name: start.edit, level: 'measure', expression: '' };
    } else {
      this.#original = undefined;
      this.#initial = {
        name: freshName(options.snapshot()),
        // MEASURE by default, as upstream (DataCubeNewColumnState).
        level: start.level ?? 'measure',
        expression: start.expression ?? '',
      };
    }
    this.#draft = { ...this.#initial };
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
      expression: d.expression.trim(),
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
    if (this.#draft.expression.trim().length === 0 || this.#nameProblem() !== null) {
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
    this.#draft = { ...this.#initial };
    this.#refusal = null;
    this.#render();
    this.#schedule(0);
  }

  #canApply(): boolean {
    return this.#nameProblem() === null
      && this.#draft.expression.trim().length > 0
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
    level.addEventListener('change', () => {
      this.#draft.level = level.value as ColumnLevel;
      this.#edited();
      // The scope changes with the level, so the completion list does.
      this.#paintPicker(picker, expr);
    });

    const exprRow = el(doc, 'label', 'dc-coleditor-code', form);
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

    const picker = el(doc, 'div', 'dc-calc-picker', form);
    this.#paintPicker(picker, expr);

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
        check.textContent = this.#draft.expression.trim().length === 0
          ? 'An expression is required — e.g. $x.notional * 1.05' : '';
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
      expression: row.expression,
    };
  }
  const group = (s.groupDerived ?? []).find((d) => d.name === name);
  return group ? { name, level: 'group', expression: group.expression } : undefined;
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
