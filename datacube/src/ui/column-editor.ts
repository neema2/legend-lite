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

import {
  columnsInScope,
  completionsFor,
  nameProblem,
  type CalcStage,
  type Completion,
} from '../calc.ts';
import { explodeLambda, explodeName, explodeType, type Extraction } from '../json-shape.ts';
import { buildJsonFields, freeName, type JsonColumnReader } from './json-fields.ts';
import type { CompileOutcome } from '../cube.ts';
import type { Lambda } from '../../../pure-protocol/src/index.ts';
import type { PivotColumn } from '../query.ts';
import { docHint } from './docs.ts';
import { emptyOperands, isEmptyOperandRefusal, sayEmpty, type EmptyAs } from '../calc-fix.ts';
import { columnType } from '../snapshot.ts';
import {
  WINDOW_FUNCTIONS,
  renameColumnReferences,
  rowColumns,
  type CubeSnapshot,
  type DerivedColumn,
  type ChildAggregate,
  type ChildAggregateFn,
  type WindowFrame,
  type WindowFunction,
  type WindowSpec,
} from '../snapshot.ts';
import { defaultKind, isVariant } from '../../../engine-client/src/types.ts';

/** Upstream's DataCubeExtendedColumnKind. */
export type ColumnLevel = 'measure' | 'dimension' | 'group';

export const LEVELS: readonly { value: ColumnLevel; label: string }[] = [
  { value: 'measure', label: 'Leaf Level Measure' },
  { value: 'dimension', label: 'Leaf Level Dimension' },
  { value: 'group', label: 'Group Level' },
];

/** A new column (seeded, as "Extend Column X..." does), or one to edit. */
export type ColumnEditorStart =
  | { readonly expression?: string; readonly level?: ColumnLevel;
    /** A JSON column whose fields to offer first. */
    readonly json?: string }
  | { readonly edit: string };

export interface ColumnEditorOptions {
  /** The cube as it is NOW: other windows change it under this one. */
  readonly snapshot: () => CubeSnapshot;
  /** The pivot's columns as the current view's plan made them. */
  readonly pivotColumns?: () => readonly PivotColumn[];
  readonly start: ColumnEditorStart;
  /** What the person typed, `x|...`, as its lambda: the compiler's parse (E1). */
  readonly parse: (text: string) => Promise<Lambda>;
  /** A column's lambda as text to edit: the compiler's print (E4). */
  readonly print: (query: Lambda) => Promise<string>;
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
  /**
   * A sample of a JSON column's cells, for picking a field of it
   * (`ui/json-fields.ts`). Absent: the editor offers no JSON fields.
   */
  /** A JSON column's cells: a sample, or every row (`JsonColumnReader`). */
  readonly readJson?: (column: string) => JsonColumnReader;
  /** Another calculated column (or a new one) in this window's place: the rail's choice. */
  readonly openOther?: (start: ColumnEditorStart) => void;
  /**
   * The first rows of the cube with the draft in it: the new column beside what it reads. Absent:
   * no preview.
   */
  readonly preview?: (candidate: CubeSnapshot, column: string, signal: AbortSignal) => Promise<ColumnPreview>;
}

/** A preview's table, its cells as the grid writes them. */
export interface ColumnPreview {
  readonly columns: readonly { readonly name: string; readonly type?: string }[];
  readonly rows: readonly (readonly string[])[];
}

/** What a calculated column computes: each kind its own builder, on one page. */
type Kind = 'formula' | 'window' | 'ratio' | 'children' | 'json';

interface KindMeta {
  readonly id: Kind;
  readonly label: string;
  readonly hint: string;
  /** The builder's heading. */
  readonly title: string;
}

const KINDS: readonly KindMeta[] = [
  { id: 'formula', label: 'Formula', hint: 'A value from each row', title: 'Formula on each row' },
  { id: 'window', label: 'Running, rank, previous', hint: 'From the rows around each one', title: 'From the rows around each one' },
  { id: 'ratio', label: 'Ratio of totals', hint: 'After grouping, from each group\u2019s figures', title: 'From each group\u2019s figures, after grouping' },
  { id: 'children', label: 'From child groups', hint: 'Smallest, largest, average\u2026 of those beneath', title: 'From the groups beneath each group' },
  { id: 'json', label: 'Field from JSON', hint: 'A field of a JSON column', title: 'A field of a JSON column' },
];

/**
 * A lambda's head -- its parameter and bar, `x|` -- apart from its body, so the editor can show the
 * head fixed and the person write the body. Only a leading `name|` is a head: the compiler parses
 * the whole lambda, this only decides what the box shows.
 */
function splitHead(text: string): { readonly head: string; readonly body: string } {
  const m = /^\s*([A-Za-z_][A-Za-z0-9_]*)\s*\|/.exec(text);
  return m ? { head: `${m[1]}|`, body: text.slice(m[0].length).replace(/^ /, '') } : { head: 'x|', body: text };
}

interface Draft {
  name: string;
  level: ColumnLevel;
  /**
   * An expression over one row, a window over the rows around it, or
   * (group level) an aggregate of the child groups' figures.
   */
  mode: 'expression' | 'window' | 'children';
  /** The whole lambda as the person edits it, `x|$x.a * 2` (upstream's editor shows the same). */
  expression: string;
  /** EXPLODE: the expression is a collection, and each row repeats once per element (`DerivedColumn.unnest`). */
  unnest: boolean;
  window: WindowSpec;
  child: ChildAggregate;
}

const CHILD_FUNCTIONS: readonly (readonly [ChildAggregateFn, string])[] = [
  ['min', 'Minimum'], ['max', 'Maximum'], ['average', 'Average'],
  ['median', 'Median'], ['sum', 'Sum'], ['count', 'Count'],
];

/** A window to start from: a running sum, order to be chosen. */
const NEW_WINDOW: WindowSpec = { fn: 'sum', partition: [], order: [], frame: 'running' };
/** A child-group aggregate to start from: the smallest child. */
const NEW_CHILD: ChildAggregate = { fn: 'min', of: '' };

type Check =
  | { readonly state: 'idle' }
  | { readonly state: 'compiling' }
  | { readonly state: 'ok' }
  | { readonly state: 'unavailable' }
  | {
    readonly state: 'refused'; readonly message: string; readonly caret?: string;
    /** What was compiled, when the refusal is the compiler's: a quick fix rewrites it (calc-fix.ts). */
    readonly lambda?: Lambda;
  };

export class ColumnEditor {
  readonly #root: HTMLElement;
  readonly #doc: Document;
  readonly #options: ColumnEditorOptions;
  /** The column's name when editing began; absent when adding. */
  readonly #original: string | undefined;
  #initial: Draft;
  #draft: Draft;
  #check: Check = { state: 'idle' };
  /** A refusal from the cube on OK or Delete, shown until the next edit. */
  #refusal: string | null = null;
  #busy = false;
  /**
   * The name is still the one the editor chose, so picking a JSON field
   * may replace it with the field's own. Typing a name ends that.
   */
  #autoName: boolean;
  #timer: ReturnType<typeof setTimeout> | undefined;
  #inflight: AbortController | null = null;
  #els!: {
    expr?: HTMLTextAreaElement;
    nameMark: HTMLElement;
    check: HTMLElement;
    problem: HTMLElement;
    ok: HTMLButtonElement;
    preview: HTMLElement;
    type: HTMLElement;
  };
  /** The draft is a JSON field's extraction (a kind of its own, not inferable from the draft). */
  #json = false;
  /** The lambda's head the formula box shows fixed, `x|`. */
  #head = 'x|';
  /** The preview: the last one asked for, the last table, what it says. */
  #previewAbort: AbortController | null = null;
  #previewTable: ColumnPreview | undefined;
  #previewNote = '';

  constructor(container: HTMLElement, options: ColumnEditorOptions) {
    this.#root = container;
    this.#doc = container.ownerDocument;
    this.#options = options;
    const start = options.start;
    let printing: Lambda | undefined;
    if ('edit' in start) {
      const found = findColumn(options.snapshot(), start.edit);
      this.#original = start.edit;
      printing = found?.lambda;
      this.#initial = found?.draft ?? {
        name: start.edit, level: 'measure', mode: 'expression', expression: '', unnest: false,
        window: NEW_WINDOW, child: NEW_CHILD,
      };
    } else {
      this.#original = undefined;
      this.#initial = {
        name: freshName(options.snapshot()),
        // MEASURE by default, as upstream (DataCubeNewColumnState).
        level: start.level ?? 'measure',
        mode: 'expression',
        expression: start.expression ?? 'x|',
        unnest: false,
        window: NEW_WINDOW,
        child: NEW_CHILD,
      };
    }
    this.#autoName = !('edit' in start);
    this.#json = !('edit' in start) && start.json !== undefined;
    this.#draft = { ...this.#initial, window: { ...this.#initial.window }, child: { ...this.#initial.child } };
    this.#render();
    if (printing) void this.#open(printing);
    else this.#schedule(0);
  }

  /** An existing column's lambda, printed by the compiler for the person to edit. */
  async #open(lambda: Lambda): Promise<void> {
    let text: string;
    try {
      text = await this.#options.print(lambda);
    } catch (error: unknown) {
      this.#refusal = error instanceof Error ? error.message : String(error);
      this.#paint();
      return;
    }
    this.#initial = { ...this.#initial, expression: text };
    this.#draft.expression = text;
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
    this.#previewAbort?.abort();
  }

  // -- the draft as the cube would take it --------------------------------

  #stage(): CalcStage {
    return this.#draft.level === 'group' ? 'group' : 'row';
  }

  /**
   * The draft's lambda, as the compiler parses what the person typed; a
   * refusal is the parser's message (a position it names is in `text`).
   */
  async #parsed(): Promise<{ readonly lambda?: Lambda } | { readonly refusal: string }> {
    if (this.#draft.mode !== 'expression') return {};
    try {
      return { lambda: await this.#options.parse(this.#draft.expression.trim()) };
    } catch (error: unknown) {
      return { refusal: error instanceof Error ? error.message : String(error) };
    }
  }

  /** The cube's two lists with this draft (its lambda parsed) in place of the original. */
  #lists(lambda?: Lambda): { row: DerivedColumn[]; group: DerivedColumn[] } {
    const s = this.#options.snapshot();
    const without = (list: readonly DerivedColumn[]): DerivedColumn[] =>
      list.filter((d) => d.name !== this.#original);
    const row = without(s.derived);
    const group = without(s.groupDerived ?? []);
    const d = this.#draft;
    const next: DerivedColumn = {
      name: d.name.trim(),
      ...(d.mode === 'expression' && lambda ? { lambda } : {}),
      // explode: row stage only (after grouping there are no source rows to repeat)
      ...(d.mode === 'expression' && d.unnest && d.level !== 'group' ? { unnest: true } : {}),
      ...(d.mode === 'window' ? { window: this.#window() } : {}),
      ...(d.mode === 'children' ? { childAggregate: { ...d.child } } : {}),
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

  #candidate(lambda?: Lambda): CubeSnapshot {
    const { row, group } = this.#lists(lambda);
    const next: CubeSnapshot = { ...this.#options.snapshot(), derived: row, groupDerived: group };
    const rename = this.#rename();
    return rename ? renameColumnReferences(next, rename.from, rename.to) : next;
  }

  /** What the calculation still lacks, or null when it is complete. */
  #bodyProblem(): string | null {
    const d = this.#draft;
    // An expression is judged by the compiler: its parse refusal names what is missing -- once
    // there is one. An empty one is not refused, it is not written yet.
    if (d.mode === 'expression') {
      if (splitHead(d.expression).body.trim() !== '') return null;
      return this.#json ? 'Pick a field of the JSON column above, or write a formula' : 'Write the formula';
    }
    if (d.mode === 'children') {
      const s = this.#options.snapshot();
      if (d.level !== 'group') return 'Child groups are a Group Level calculation';
      if (s.pivotOn.length > 0) return 'Not on a cube with column pivots';
      return d.child.of === '' ? 'Choose the measure whose child figures are aggregated' : null;
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

  #pivot(): readonly PivotColumn[] {
    return this.#options.pivotColumns?.() ?? [];
  }

  #nameProblem(): string | null {
    const s = this.#options.snapshot();
    return nameProblem(s, this.#stage(), this.#draft.name, this.#original, this.#pivot());
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
    const text = this.#draft.expression.trim();
    const parsed = await this.#parsed();
    if (abort.signal.aborted || this.#inflight !== abort) return;
    if ('refusal' in parsed) {
      this.#inflight = null;
      const caret = caretFor(text, parsed.refusal);
      this.#check = { state: 'refused', message: parsed.refusal, ...(caret === undefined ? {} : { caret }) };
      this.#paint();
      return;
    }
    let outcome: CompileOutcome | undefined;
    try {
      outcome = await this.#options.compile(this.#candidate(parsed.lambda), abort.signal);
    } catch (error: unknown) {
      // Aborted: a newer compile owns the form. Anything else is an answer
      // the person must see -- treating every failure as an abort left the
      // form on "Compiling..." with OK disabled for good (P2-152).
      if (abort.signal.aborted || this.#inflight !== abort) return;
      this.#inflight = null;
      this.#check = { state: 'refused', message: error instanceof Error ? error.message : String(error), ...this.#compiled(parsed) };
      this.#paint();
      return;
    }
    if (abort.signal.aborted || this.#inflight !== abort) return;
    this.#inflight = null;
    if (outcome === undefined) {
      this.#check = { state: 'unavailable' };
    } else if (outcome.refusal === null) {
      this.#check = { state: 'ok' };
      void this.#refreshPreview(parsed.lambda);
    } else {
      this.#check = { state: 'refused', message: outcome.refusal, ...this.#compiled(parsed) };
    }
    this.#paint();
  }

  #compiled(parsed: { readonly lambda?: Lambda }): { readonly lambda?: Lambda } {
    return parsed.lambda === undefined ? {} : { lambda: parsed.lambda };
  }

  /**
   * The quick fix: the person's expression with each bare column operand said (calc-fix.ts),
   * printed by the compiler into the box, and compiled again -- the compiler judges it.
   */
  async #sayEmpty(lambda: Lambda, as: EmptyAs): Promise<void> {
    const snapshot = this.#options.snapshot();
    const fixed = sayEmpty(lambda, as, (c) => columnType(snapshot, c));
    let text: string;
    try {
      text = await this.#options.print(fixed);
    } catch (error: unknown) {
      this.#refusal = error instanceof Error ? error.message : String(error);
      this.#paint();
      return;
    }
    this.#draft.expression = text;
    this.#render();
    this.#edited();
  }

  // -- actions ---------------------------------------------------------------

  async #ok(): Promise<void> {
    if (this.#busy || !this.#canApply()) return;
    this.#busy = true;
    this.#paint();
    try {
      const parsed = await this.#parsed();
      if ('refusal' in parsed) {
        this.#refusal = parsed.refusal;
      } else {
        const { row, group } = this.#lists(parsed.lambda);
        this.#refusal = await this.#options.apply(row, group, this.#rename());
      }
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
    this.#draft = { ...this.#initial, window: { ...this.#initial.window }, child: { ...this.#initial.child } };
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

  // -- the kind: what the person wants to compute ---------------------------

  /** The kind the draft is: from its mode and level (a JSON pick is remembered, not inferred). */
  #kind(): Kind {
    const d = this.#draft;
    if (d.mode === 'children') return 'children';
    if (d.mode === 'window') return 'window';
    if (d.level === 'group') return 'ratio';
    return this.#json ? 'json' : 'formula';
  }

  /** Choose a kind: it settles the mode and, where it decides it, the level. */
  #setKind(k: Kind): void {
    const d = this.#draft;
    const rowLevel = (): ColumnLevel => (d.level === 'group' ? 'measure' : d.level);
    this.#json = k === 'json';
    if (k === 'formula' || k === 'json') { d.mode = 'expression'; d.level = rowLevel(); }
    if (k === 'ratio') { d.mode = 'expression'; d.level = 'group'; }
    if (k === 'children') { d.mode = 'children'; d.level = 'group'; }
    if (k === 'window') d.mode = 'window';
    if (k !== 'json') d.unnest = false;
    // another kind: the last preview was of something else
    this.#previewTable = undefined;
    this.#previewNote = '';
    this.#render();
    this.#edited();
  }

  /**
   * Every kind, always -- the rail keeps its shape -- each with why this cube cannot take it, when
   * it cannot: a JSON field needs a JSON column; child groups, a cube that does not pivot.
   */
  #kinds(): readonly (KindMeta & { readonly unavailable?: string })[] {
    const s = this.#options.snapshot();
    const json = this.#options.readJson !== undefined
      && rowColumns(s).some((c) => c.name !== this.#original && isVariant(c.type));
    const children = s.pivotOn.length === 0 || this.#kind() === 'children';
    return KINDS.map((k) => {
      if (k.id === 'json' && !json) return { ...k, unavailable: 'This cube has no JSON column to read a field of' };
      if (k.id === 'children' && !children) return { ...k, unavailable: 'Not on a cube that pivots its columns' };
      return k;
    });
  }

  // -- rendering -------------------------------------------------------------

  #render(): void {
    const doc = this.#doc;
    this.#root.replaceChildren();
    this.#root.classList.add('dc-coleditor', 'dc-xc');
    const body = el(doc, 'div', 'dc-xc-body', this.#root);

    // THE RAIL: this cube's calculated columns, to move between, and the kinds
    const rail = el(doc, 'div', 'dc-xc-rail', body);
    el(doc, 'div', 'dc-xc-rail-title', rail).textContent = 'Calculated columns';
    const s = this.#options.snapshot();
    const existing = [...s.derived, ...(s.groupDerived ?? [])];
    const list = el(doc, 'div', 'dc-xc-columns', rail);
    for (const d of existing) {
      const b = el(doc, 'button', 'dc-xc-column', list) as HTMLButtonElement;
      b.type = 'button';
      b.textContent = d.name;
      b.dataset['column'] = d.name;
      const here = d.name === this.#original;
      b.classList.toggle('dc-on', here);
      b.setAttribute('aria-current', String(here));
      if (!here) b.addEventListener('click', () => this.#switchTo({ edit: d.name }));
    }
    const fresh = el(doc, 'button', 'dc-xc-column dc-xc-new', list) as HTMLButtonElement;
    fresh.type = 'button';
    fresh.textContent = '+ New column';
    fresh.classList.toggle('dc-on', this.#original === undefined);
    if (this.#original !== undefined) fresh.addEventListener('click', () => this.#switchTo({}));

    el(doc, 'div', 'dc-xc-rail-title', rail).textContent = 'Kind';
    const kinds = el(doc, 'div', 'dc-xc-kinds', rail);
    kinds.setAttribute('role', 'radiogroup');
    kinds.setAttribute('aria-label', 'What it computes');
    const kindNow = this.#kind();
    for (const k of this.#kinds()) {
      const b = el(doc, 'button', 'dc-xc-kind', kinds) as HTMLButtonElement;
      b.type = 'button';
      b.dataset['kind'] = k.id;
      b.setAttribute('role', 'radio');
      b.setAttribute('aria-checked', String(k.id === kindNow));
      b.classList.toggle('dc-on', k.id === kindNow);
      el(doc, 'span', 'dc-xc-kind-label', b).textContent = k.label;
      el(doc, 'span', 'dc-xc-kind-hint', b).textContent = k.hint;
      if (k.unavailable) {
        b.disabled = true;
        b.title = k.unavailable;
      }
      b.addEventListener('click', () => { if (this.#kind() !== k.id) this.#setKind(k.id); });
    }

    // THE PAGE: name, use, the builder, what the compiler says, the preview
    const main = el(doc, 'div', 'dc-xc-main', body);
    const head = el(doc, 'div', 'dc-xc-head', main);
    const nameField = el(doc, 'label', 'dc-xc-name', head);
    el(doc, 'span', 'dc-xc-label', nameField).textContent = 'Name';
    const name = el(doc, 'input', 'dc-calc-input-name', nameField) as HTMLInputElement;
    name.type = 'text';
    name.value = this.#draft.name;
    name.spellcheck = false;
    const nameMark = el(doc, 'span', 'dc-calc-namemark', nameField);
    name.addEventListener('input', () => {
      this.#draft.name = name.value;
      this.#autoName = false;
      this.#edited();
    });

    // USE AS: a value of each row is a measure (summed when grouped) or a dimension (grouped
    // by); a column computed after grouping is neither -- it is the group's own figure
    const segmented = (parent: HTMLElement, cls: string, label: string,
      options: readonly (readonly [string, string, string])[], value: string, onPick: (v: string) => void): void => {
      const wrap = el(doc, 'div', `dc-xc-seg ${cls}`, parent);
      el(doc, 'span', 'dc-xc-label', wrap).textContent = label;
      const group = el(doc, 'div', 'dc-xc-seg-buttons', wrap);
      group.setAttribute('role', 'radiogroup');
      group.setAttribute('aria-label', label);
      for (const [v, text, title] of options) {
        const b = el(doc, 'button', 'dc-xc-seg-button', group) as HTMLButtonElement;
        b.type = 'button';
        b.dataset['value'] = v;
        b.textContent = text;
        b.title = title;
        b.setAttribute('role', 'radio');
        b.setAttribute('aria-checked', String(v === value));
        b.classList.toggle('dc-on', v === value);
        b.addEventListener('click', () => { if (v !== value) onPick(v); });
      }
    };
    const kind = this.#kind();
    const meta = KINDS.find((k) => k.id === kind)!;
    const builder = doc.createElement('div');
    builder.className = 'dc-xc-builder';
    el(doc, 'div', 'dc-xc-section', builder).textContent = meta.title;
    if (kind === 'window') {
      segmented(builder, 'dc-xc-over', 'Over', [
        ['row', 'Source rows', 'Each source row, from the rows around it'],
        ['group', 'Groups', 'Each group on the grid, from the groups around it'],
      ], this.#draft.level === 'group' ? 'group' : 'row', (v) => {
        this.#draft.level = v === 'group' ? 'group' : 'measure';
        this.#render();
        this.#edited();
      });
    }
    if (this.#draft.level !== 'group') {
      segmented(head, 'dc-xc-use', 'Use as', [
        ['measure', 'Measure', 'Summed (or aggregated) when the cube groups'],
        ['dimension', 'Dimension', 'Something to group by'],
      ], this.#draft.level, (v) => {
        this.#draft.level = v as ColumnLevel;
        this.#render();
        this.#edited();
      });
      head.querySelector('.dc-xc-use')?.append(docHint(doc, 'data-cube.extended-column.levels'));
    }
    const type = el(doc, 'span', 'dc-xc-type', head);
    type.title = 'Its type, as the compiler gives it';
    main.append(builder);

    let expr: HTMLTextAreaElement | undefined;
    if (kind === 'formula' || kind === 'ratio' || kind === 'json') {
      if (kind === 'json') this.#paintJsonPick(builder, () => expr);
      expr = this.#paintFormula(builder, kind);
      if (kind === 'json') {
        // EXPLODE: the expression yields a collection; each row repeats once per element
        const explodeRow = el(doc, 'label', 'dc-calc-explode', builder);
        const explode = el(doc, 'input', 'dc-calc-input-explode', explodeRow) as HTMLInputElement;
        explode.type = 'checkbox';
        explode.checked = this.#draft.unnest;
        explodeRow.append(doc.createTextNode(' One row per element (explode)'));
        explodeRow.title = 'Each row repeats once per element of the collection the expression yields. '
          + 'A figure of the row itself (an order total) then appears once per element: summing it counts it again.';
        explode.addEventListener('change', () => {
          this.#draft.unnest = explode.checked;
          this.#edited();
        });
      }
    }
    if (kind === 'window') this.#paintWindow(el(doc, 'div', 'dc-calc-window', builder));
    if (kind === 'children') this.#paintChildren(el(doc, 'div', 'dc-calc-children', builder));

    const check = el(doc, 'div', 'dc-calc-check', main);
    check.setAttribute('role', 'status');
    const problem = el(doc, 'p', 'dc-calc-problem', main);
    problem.setAttribute('role', 'alert');

    // THE PREVIEW: the first rows, the new column beside what it reads -- recomputed as it changes
    const preview = el(doc, 'div', 'dc-xc-preview', main);
    if (!this.#options.preview) preview.hidden = true;

    const footer = el(doc, 'div', 'dc-calc-footer dc-xc-foot', this.#root);
    const button = (label: string, cls: string, onClick: () => void): HTMLButtonElement => {
      const b = el(doc, 'button', `dc-button ${cls}`, footer) as HTMLButtonElement;
      b.type = 'button';
      b.textContent = label;
      b.addEventListener('click', onClick);
      return b;
    };
    if (this.#original !== undefined) {
      button('Delete', 'dc-calc-delete', () => void this.#delete());
      button('Reset', 'dc-calc-reset', () => this.#reset());
    }
    el(doc, 'span', 'dc-xc-spacer', footer);
    button('Cancel', 'dc-calc-cancel', () => this.#options.onClose());
    const ok = button('OK', 'dc-calc-ok', () => void this.#ok());

    this.#els = { ...(expr ? { expr } : {}), nameMark, check, problem, ok, preview, type };
    this.#paint();
  }

  /** Open another calculated column (or a new one) in this window's place. */
  #switchTo(start: ColumnEditorStart): void {
    if (!this.#options.openOther) return;
    this.#options.openOther(start);
  }

  /**
   * The formula: the lambda's parameter (`x|`) fixed in front, the person's expression after it,
   * and what can go in it -- the columns in scope and the functions -- a click away.
   */
  #paintFormula(box: HTMLElement, kind: Kind): HTMLTextAreaElement {
    const doc = this.#doc;
    const split = splitHead(this.#draft.expression);
    this.#head = split.head;
    const code = el(doc, 'div', 'dc-xc-code', box);
    const prefix = el(doc, 'span', 'dc-xc-prefix', code);
    prefix.textContent = this.#head;
    prefix.title = 'The row: $x.<column> reads a column of it';
    const expr = el(doc, 'textarea', 'dc-calc-input-expr', code) as HTMLTextAreaElement;
    expr.value = split.body;
    expr.rows = 3;
    expr.spellcheck = false;
    expr.setAttribute('aria-label', 'Expression');
    expr.placeholder = kind === 'ratio' ? '$x.pnl / $x.notional' : '$x.notional * 1.1';
    expr.addEventListener('input', () => {
      // a whole lambda pasted (`x|...`): its parameter goes in front, the rest stays here
      const again = splitHead(expr.value);
      if (again.head !== '' && again.body !== expr.value) {
        this.#head = again.head;
        prefix.textContent = again.head;
        const at = Math.max(0, (expr.selectionStart ?? 0) - (expr.value.length - again.body.length));
        expr.value = again.body;
        expr.setSelectionRange(at, at);
      }
      this.#draft.expression = this.#head + expr.value;
      this.#edited();
    });
    const tools = el(doc, 'div', 'dc-xc-tools', box);
    const insert = el(doc, 'div', 'dc-calc-picker dc-xc-insert', box);
    insert.hidden = true;
    const toggle = (what: 'column' | 'function', label: string): void => {
      const b = el(doc, 'button', 'dc-button dc-xc-tool', tools) as HTMLButtonElement;
      b.type = 'button';
      b.textContent = label;
      b.dataset['insert'] = what;
      b.addEventListener('click', () => {
        const showing = !insert.hidden && insert.dataset['what'] === what;
        for (const t of tools.querySelectorAll('.dc-xc-tool')) t.classList.remove('dc-on');
        if (showing) { insert.hidden = true; return; }
        b.classList.add('dc-on');
        insert.hidden = false;
        insert.dataset['what'] = what;
        this.#paintPicker(insert, expr, what);
      });
    };
    toggle('column', '+ Column');
    toggle('function', '+ Function');
    return expr;
  }

  /** A JSON column's field, picked: the name, the use, explode and the formula are filled in. */
  #paintJsonPick(box: HTMLElement, expr: () => HTMLTextAreaElement | undefined): void {
    const doc = this.#doc;
    const pickBox = el(doc, 'div', 'dc-calc-json', box);
    // A PICK fills the page IN PLACE -- the name, the use, explode and the formula -- rather than
    // rebuilding it: the person is in the middle of the JSON fields and the ticks below.
    const take = (e: Extraction): void => {
      const q = <T extends Element>(sel: string): T | null => this.#root.querySelector<T>(sel);
      if (this.#autoName) {
        const taken = new Set(rowColumns(this.#options.snapshot()).map((c) => c.name));
        this.#draft.name = freeName(e.name, taken);
        const nameInput = q<HTMLInputElement>('.dc-calc-input-name');
        if (nameInput) nameInput.value = this.#draft.name;
      }
      this.#draft.level = e.kind;
      for (const b of this.#root.querySelectorAll<HTMLElement>('.dc-xc-use .dc-xc-seg-button')) {
        const on = b.dataset['value'] === e.kind;
        b.classList.toggle('dc-on', on);
        b.setAttribute('aria-checked', String(on));
      }
      this.#draft.unnest = e.unnest === true;
      const explode = q<HTMLInputElement>('.dc-calc-input-explode');
      if (explode) explode.checked = this.#draft.unnest;
      // the extraction is a tree; the person edits the compiler's print of it
      void this.#options.print(e.lambda).then((text) => {
        this.#draft.expression = text;
        const split = splitHead(text);
        this.#head = split.head;
        const prefix = q<HTMLElement>('.dc-xc-prefix');
        if (prefix) prefix.textContent = split.head;
        const box2 = expr();
        if (box2) box2.value = split.body;
        this.#edited();
      });
    };
    // AN EXPLODE OF OBJECTS: which of each element's fields the ONE column holds -- one ticked,
    // its value; several, a tuple `(billing, Paris)`; none, the element as JSON.
    const paintExplode = (e: Extraction, into: HTMLElement): void => {
      into.replaceChildren();
      const ex = e.explode;
      into.hidden = ex === undefined;
      if (!ex) return;
      el(doc, 'div', 'dc-calc-explode-note', into).textContent =
        'Each element becomes: the fields ticked, one column -- several make a tuple.';
      const chosen = new Set(ex.fields.map((f) => f.key));
      for (const f of ex.fields) {
        const row = el(doc, 'label', 'dc-calc-explode-field', into);
        const tick = el(doc, 'input', 'dc-calc-explode-tick', row) as HTMLInputElement;
        tick.type = 'checkbox';
        tick.checked = true;
        tick.value = f.key;
        row.append(doc.createTextNode(` ${f.key} (${f.type})`));
        tick.addEventListener('change', () => {
          if (tick.checked) chosen.add(f.key);
          else chosen.delete(f.key);
          const fields = ex.fields.filter((x) => chosen.has(x.key));
          take({
            ...e,
            name: explodeName(ex.base, fields),
            lambda: explodeLambda(ex.array, fields),
            type: explodeType(fields),
            kind: fields.length === 1 ? fields[0]!.kind : 'dimension',
          });
        });
      }
    };
    const explodeFields = el(doc, 'div', 'dc-calc-explode-fields', box);
    explodeFields.hidden = true;
    this.#paintJson(pickBox, (e) => {
      take(e);
      paintExplode(e, explodeFields);
    });
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
        const operands = c.lambda !== undefined && isEmptyOperandRefusal(c.message) ? emptyOperands(c.lambda) : [];
        if (c.lambda !== undefined && operands.length > 0) {
          const lambda = c.lambda;
          const fix = el(this.#doc, 'div', 'dc-calc-fix', check);
          const names = operands.map((o) => `'${o}'`).join(', ');
          el(this.#doc, 'span', 'dc-calc-fix-why', fix).textContent = `${names} can be empty, and arithmetic needs a value. `
            + 'Where it is empty, the result is:';
          const offer = (label: string, title: string, as: EmptyAs): void => {
            const b = el(this.#doc, 'button', 'dc-button dc-calc-fix-button', fix) as HTMLButtonElement;
            b.type = 'button';
            b.textContent = label;
            b.title = title;
            b.dataset['as'] = as;
            b.addEventListener('click', () => void this.#sayEmpty(lambda, as));
          };
          offer('Empty', 'An empty row\'s result is empty (each such column read with toOne)', 'blank');
          offer('As if zero', 'An empty value counts as zero (each such column read with coalesce)', 'zero');
        }
        break;
      }
    }

    const message = nameProblemText ?? this.#refusal;
    problem.textContent = message ?? '';
    problem.hidden = message === null;
    ok.disabled = this.#busy || !this.#canApply();
    this.#paintPreview();
  }

  // -- the preview -------------------------------------------------------------

  /** The first rows with the draft in the cube: asked once it compiles. */
  async #refreshPreview(lambda?: Lambda): Promise<void> {
    const preview = this.#options.preview;
    if (!preview) return;
    this.#previewAbort?.abort();
    const abort = new AbortController();
    this.#previewAbort = abort;
    this.#previewNote = 'Computing\u2026';
    this.#paintPreview();
    try {
      const table = await preview(this.#candidate(lambda), this.#draft.name.trim(), abort.signal);
      if (abort.signal.aborted) return;
      this.#previewTable = table;
      this.#previewNote = '';
    } catch (error: unknown) {
      if (abort.signal.aborted) return;
      this.#previewTable = undefined;
      this.#previewNote = error instanceof Error ? error.message : String(error);
    }
    this.#paintPreview();
  }

  #paintPreview(): void {
    const box = this.#els.preview;
    const type = this.#els.type;
    if (!this.#options.preview) return;
    const doc = this.#doc;
    box.replaceChildren();
    const head = el(doc, 'div', 'dc-xc-section', box);
    head.textContent = 'Preview';
    const fresh = this.#check.state === 'ok';
    const t = this.#previewTable;
    const name = this.#draft.name.trim();
    const mine = t?.columns.find((c) => c.name === name);
    type.textContent = fresh && mine?.type ? mine.type : '';
    type.hidden = type.textContent === '';
    if (this.#previewNote) el(doc, 'span', 'dc-xc-preview-note', head).textContent = this.#previewNote;
    else if (!fresh) el(doc, 'span', 'dc-xc-preview-note', head).textContent = 'as it last compiled';
    if (!t) {
      if (!this.#previewNote) {
        el(doc, 'p', 'dc-xc-preview-empty', box).textContent = 'The first rows appear here once the column compiles.';
      }
      return;
    }
    const scroll = el(doc, 'div', 'dc-xc-preview-scroll', box);
    const table = el(doc, 'table', 'dc-xc-preview-table', scroll);
    table.classList.toggle('dc-stale', !fresh);
    const tr = el(doc, 'tr', '', el(doc, 'thead', '', table));
    for (const c of t.columns) {
      const th = el(doc, 'th', c.name === name ? 'dc-xc-new-col' : '', tr);
      th.textContent = c.name;
    }
    const tbody = el(doc, 'tbody', '', table);
    for (const row of t.rows) {
      const r = el(doc, 'tr', '', tbody);
      row.forEach((v, i) => {
        const td = el(doc, 'td', t.columns[i]?.name === name ? 'dc-xc-new-col' : '', r);
        td.textContent = v;
      });
    }
    if (t.rows.length === 0) el(doc, 'p', 'dc-xc-preview-empty', box).textContent = 'No rows.';
  }

  /** A JSON column to pick from, and its fields once one is chosen. */
  #paintJson(box: HTMLElement, onPick: (e: Extraction) => void): void {
    const doc = this.#doc;
    const readJson = this.#options.readJson;
    const self = this.#original;
    const json = rowColumns(this.#options.snapshot())
      .filter((c) => c.name !== self && isVariant(c.type))
      .map((c) => c.name);
    if (!readJson || json.length === 0) {
      box.remove();
      return;
    }
    const row = field(doc, box, 'From JSON:');
    const pick = el(doc, 'select', 'dc-calc-json-column', row) as HTMLSelectElement;
    pick.setAttribute('aria-label', 'JSON column');
    for (const [value, label] of [['', 'Pick a JSON column…'], ...json.map((c) => [c, c])]) {
      const o = doc.createElement('option');
      o.value = value!;
      o.textContent = label!;
      pick.append(o);
    }
    const fields = el(doc, 'div', 'dc-calc-json-fields', box);
    const show = (): void => {
      fields.replaceChildren();
      const column = pick.value;
      if (!column) return;
      buildJsonFields(fields, {
        column, reader: readJson(column), onPick,
      });
    };
    pick.addEventListener('change', show);
    const start = this.#options.start;
    if (!('edit' in start) && start.json && json.includes(start.json)) {
      pick.value = start.json;
      show();
    }
  }

  /** The child-groups form: the function, and the measure it reads. */
  #paintChildren(box: HTMLElement): void {
    const doc = this.#doc;
    box.replaceChildren();
    const s = this.#options.snapshot();
    const c = this.#draft.child;
    const derivedNames = new Set((s.groupDerived ?? []).map((d) => d.name));
    // The group level's figures: measures (or, with none, the columns
    // the groupBy aggregates) -- not the group keys, not other
    // calculated columns, which the child query does not compute.
    const measures = columnsInScope(s, 'group', this.#original, this.#pivot()).map((x) => x.label)
      .filter((n) => !s.rows.includes(n) && !derivedNames.has(n));
    const pick = (label: string, cls: string, options: readonly (readonly [string, string])[],
      value: string, set: (v: string) => void): void => {
      const sel = el(doc, 'select', cls, field(doc, box, label)) as HTMLSelectElement;
      for (const [v, text] of options) {
        const o = doc.createElement('option');
        o.value = v;
        o.textContent = text;
        sel.append(o);
      }
      sel.value = value;
      sel.addEventListener('change', () => {
        set(sel.value);
        this.#edited();
      });
    };
    pick('Function:', 'dc-child-fn', CHILD_FUNCTIONS, c.fn, (v) => {
      this.#draft.child = { ...this.#draft.child, fn: v as ChildAggregateFn };
    });
    pick('Of:', 'dc-child-of', [['', 'Choose a measure…'], ...measures.map((m) => [m, m] as const)],
      c.of, (v) => {
        this.#draft.child = { ...this.#draft.child, of: v };
      });
    const hint = el(doc, 'p', 'dc-child-hint', box);
    hint.textContent = 'On each group row: this function over its child groups\' figures — '
      + 'a region shows the smallest of its desks\' totals. At the deepest group, over its rows.';
  }

  /** The window form: function, column, partition, order, frame. */
  #paintWindow(box: HTMLElement): void {
    const doc = this.#doc;
    box.replaceChildren();
    const w = this.#draft.window;
    const stage = this.#stage();
    const columns = columnsInScope(this.#options.snapshot(), stage, this.#original, this.#pivot())
      .map((c) => c.label);
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

  #paintPicker(picker: HTMLElement, expr: HTMLTextAreaElement, what: 'column' | 'function'): void {
    const doc = this.#doc;
    picker.replaceChildren();
    // Scope is the cube WITHOUT this column: a column cannot see itself.
    const s = this.#options.snapshot();
    const offered = completionsFor(s, this.#stage(), this.#original, this.#pivot())
      .filter((c) => (what === 'column' ? c.kind === 'column' : c.kind !== 'column'));
    const search = el(doc, 'input', 'dc-calc-search', picker) as HTMLInputElement;
    search.type = 'search';
    search.placeholder = what === 'column' ? `Search ${offered.length} columns` : `Search ${offered.length} functions`;
    search.focus();
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
      this.#draft.expression = this.#head + expr.value;
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

/**
 * An existing calculated column as a draft, and its lambda to print into it
 * (the draft's text waits for the compiler's print), or undefined if it has gone.
 */
function findColumn(s: CubeSnapshot, name: string): { readonly draft: Draft; readonly lambda?: Lambda } | undefined {
  const row = s.derived.find((d) => d.name === name);
  if (row) {
    return {
      draft: {
        name,
        // A column saved before kinds were declared behaves as its type.
        level: row.kind ?? defaultKind(row.type),
        mode: row.window ? 'window' : 'expression',
        expression: '',
        unnest: row.unnest === true,
        window: row.window ?? NEW_WINDOW,
        child: NEW_CHILD,
      },
      ...(row.lambda ? { lambda: row.lambda } : {}),
    };
  }
  const group = (s.groupDerived ?? []).find((d) => d.name === name);
  return group
    ? {
      draft: {
        name, level: 'group',
        mode: group.childAggregate ? 'children' : group.window ? 'window' : 'expression',
        expression: '', unnest: false, window: group.window ?? NEW_WINDOW,
        child: group.childAggregate ?? NEW_CHILD,
      },
      ...(group.lambda ? { lambda: group.lambda } : {}),
    }
    : undefined;
}

/**
 * Where a parse refusal points in what the person typed, as the line with a
 * caret under it -- when the parser named a `[line:col]` inside the text.
 */
export function caretFor(text: string, refusal: string): string | undefined {
  const at = /\[(\d+):(\d+)\]/.exec(refusal);
  if (!at) return undefined;
  const offset = offsetOf(text, Number(at[1]), Number(at[2]));
  if (offset === undefined || offset > text.length) return undefined;
  const lineStart = text.lastIndexOf('\n', offset - 1) + 1;
  const lineEnd = text.indexOf('\n', offset);
  const line = text.slice(lineStart, lineEnd < 0 ? undefined : lineEnd);
  return `${line}\n${' '.repeat(offset - lineStart)}^`;
}

/** A 1-based `[line:col]` as a character offset; undefined when the text has no such line. */
function offsetOf(text: string, line: number, col: number): number | undefined {
  let offset = 0;
  for (let l = 1; l < line; l += 1) {
    const nl = text.indexOf('\n', offset);
    if (nl < 0) return undefined;
    offset = nl + 1;
  }
  return col < 1 ? undefined : offset + col - 1;
}
