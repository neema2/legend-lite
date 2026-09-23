// The calculated-column editor.
//
// A list of the columns you have, and a form for one of them: a name,
// a STAGE, and a Pure expression with completion for everything in
// scope at that stage.
//
// WHY THE STAGE IS A VISIBLE CHOICE rather than something inferred.
// `$x.notional * 1.05` and `$x.profit / $x.revenue` look like the same
// kind of thing and are not: the first is per-row and the second is
// per-group, and computing the second per row and then averaging is
// the classic weighted-average defect. Nothing can tell them apart by
// reading the text, so the editor asks -- and then only offers what
// that stage can actually see, because a source column at the group
// stage produces a refusal the user cannot act on.
//
// THE EXPRESSION IS NOT VALIDATED HERE. It goes to the planner, which
// is the only thing that knows whether it compiles and what type it
// has; a refusal comes back as a message on the cube's status line and
// the snapshot reverts. Writing a second, weaker checker here would
// mean two opinions about what is valid, and the weaker one would be
// the one the user met first.

import {
  completionsFor,
  nameProblem,
  type CalcStage,
  type Completion,
} from '../calc.ts';
import type {
  ColumnKind,
  CubeSnapshot,
  DerivedColumn,
} from '../snapshot.ts';

export interface CalcEditorOptions {
  readonly snapshot: CubeSnapshot;
  /** Both stages at once: the editor writes whichever the user chose. */
  readonly onChange: (
    row: readonly DerivedColumn[],
    group: readonly DerivedColumn[],
  ) => void;
}

interface Editing {
  readonly stage: CalcStage;
  /** The name it had when editing began; absent when adding. */
  readonly original?: string;
  name: string;
  expression: string;
  /** Row stage only: a group-stage column is already aggregated. */
  kind: ColumnKind;
}

const STAGE_LABEL: Record<CalcStage, string> = {
  row: 'Per row (before grouping)',
  group: 'Per group (after grouping)',
};

const STAGE_HELP: Record<CalcStage, string> = {
  row: 'Sees the source columns. Use this for a value each row carries'
    + ' on its own — a converted amount, a flag, a label.',
  group: 'Sees the measures and pivot columns, not the source rows. Use'
    + ' this for a ratio of two aggregates — a margin, a share of'
    + ' total. Computing that per row and averaging gives a different'
    + ' and wrong answer.',
};

export class CalcEditor {
  readonly #root: HTMLElement;
  readonly #doc: Document;
  #options: CalcEditorOptions;
  #row: DerivedColumn[];
  #group: DerivedColumn[];
  #editing: Editing | null = null;

  constructor(container: HTMLElement, options: CalcEditorOptions) {
    this.#root = container;
    this.#doc = container.ownerDocument;
    this.#options = options;
    this.#row = [...options.snapshot.derived];
    this.#group = [...(options.snapshot.groupDerived ?? [])];
    this.#render();
  }

  // -- state ----------------------------------------------------------

  #listFor(stage: CalcStage): DerivedColumn[] {
    return stage === 'row' ? this.#row : this.#group;
  }

  #commit(): void {
    this.#options.onChange(this.#row, this.#group);
  }

  #remove(stage: CalcStage, name: string): void {
    const list = this.#listFor(stage);
    const at = list.findIndex((d) => d.name === name);
    if (at < 0) return;
    list.splice(at, 1);
    this.#editing = null;
    this.#render();
    this.#commit();
  }

  #save(): void {
    const e = this.#editing;
    if (!e) return;
    const list = this.#listFor(e.stage);
    // The type is DROPPED on an edit: the expression changed, so the
    // type it used to have is no longer a fact about it. The next
    // landed result supplies the new one.
    const next: DerivedColumn = {
      name: e.name.trim(),
      expression: e.expression.trim(),
      // Only at the row stage: a group-stage column is already
      // post-aggregation, so measure-or-dimension has nothing to
      // decide. Upstream draws the same line -- GROUP_LEVEL has no
      // measure or dimension variant.
      ...(e.stage === 'row' ? { kind: e.kind } : {}),
    };
    if (e.original !== undefined) {
      const at = list.findIndex((d) => d.name === e.original);
      if (at >= 0) list.splice(at, 1, next);
      else list.push(next);
    } else {
      list.push(next);
    }
    this.#editing = null;
    this.#render();
    this.#commit();
  }

  /** The snapshot as the editor's own pending state, for scoping. */
  #pending(): CubeSnapshot {
    return {
      ...this.#options.snapshot,
      derived: this.#row,
      groupDerived: this.#group,
    };
  }

  #problem(): string | null {
    const e = this.#editing;
    if (!e) return null;
    const name = nameProblem(
      this.#pending(), e.stage, e.name,
      ...(e.original === undefined ? [] : [e.original]) as [string?],
    );
    if (name) return name;
    if (e.expression.trim().length === 0) {
      return 'an expression is required — e.g. $x.notional * 1.05';
    }
    return null;
  }

  // -- rendering ------------------------------------------------------

  #render(): void {
    this.#root.replaceChildren();
    this.#root.classList.add('dc-calc');
    for (const stage of ['row', 'group'] as const) {
      this.#renderStage(stage);
    }
    if (this.#editing) this.#renderForm();
  }

  #renderStage(stage: CalcStage): void {
    const section = this.#el('div', 'dc-calc-stage', this.#root);
    const head = this.#el('div', 'dc-calc-stage-head', section);
    const title = this.#el('span', 'dc-calc-stage-title', head);
    title.textContent = STAGE_LABEL[stage];
    const add = this.#el('button', 'dc-calc-add', head) as HTMLButtonElement;
    add.type = 'button';
    add.textContent = '+ Add';
    add.addEventListener('click', () => {
      // DIMENSION by default, and deliberately. The harm is
      // asymmetric -- the reasoning `infer.ts` already writes down:
      // summing an id, a year or a postcode gives a plausible number
      // that is meaningless and says nothing about being wrong, while
      // leaving a quantity un-summed gives a blank that reads as "no
      // aggregate chosen". The selector is right there either way.
      this.#editing = { stage, name: '', expression: '',
        kind: 'dimension' };
      this.#render();
    });

    const help = this.#el('p', 'dc-calc-help', section);
    help.textContent = STAGE_HELP[stage];

    const list = this.#listFor(stage);
    if (list.length === 0) {
      const none = this.#el('p', 'dc-calc-none', section);
      none.textContent = 'None yet.';
      return;
    }
    const ul = this.#el('ul', 'dc-calc-list', section);
    for (const d of list) {
      const li = this.#el('li', 'dc-calc-item', ul);
      const name = this.#el('span', 'dc-calc-name', li);
      name.textContent = d.name;
      const type = this.#el('span', 'dc-calc-type', li);
      // The type is absent until a result has landed, and saying so is
      // better than showing nothing: it tells the user the column has
      // not been run yet.
      type.textContent = [d.kind, d.type ?? 'not run yet']
        .filter((t) => t !== undefined).join(' · ');
      const expr = this.#el('code', 'dc-calc-expr', li);
      expr.textContent = d.expression;
      const edit = this.#el('button', 'dc-calc-edit', li) as HTMLButtonElement;
      edit.type = 'button';
      edit.textContent = 'Edit';
      edit.addEventListener('click', () => {
        this.#editing = { stage, original: d.name, name: d.name,
          expression: d.expression, kind: d.kind ?? 'dimension' };
        this.#render();
      });
      const del = this.#el('button', 'dc-calc-del', li) as HTMLButtonElement;
      del.type = 'button';
      del.textContent = 'Remove';
      del.setAttribute('aria-label', `Remove ${d.name}`);
      del.addEventListener('click', () => this.#remove(stage, d.name));
    }
  }

  #renderForm(): void {
    const e = this.#editing;
    if (!e) return;
    const form = this.#el('div', 'dc-calc-form', this.#root);
    form.dataset['stage'] = e.stage;

    const heading = this.#el('div', 'dc-calc-form-head', form);
    heading.textContent = e.original === undefined
      ? `New column — ${STAGE_LABEL[e.stage].toLowerCase()}`
      : `Editing ${e.original}`;

    const nameRow = this.#el('label', 'dc-calc-field', form);
    nameRow.append(this.#doc.createTextNode('Name'));
    const name = this.#el('input', 'dc-calc-input-name',
      nameRow) as HTMLInputElement;
    name.type = 'text';
    name.value = e.name;
    name.placeholder = 'margin';
    name.addEventListener('input', () => {
      e.name = name.value;
      this.#refreshProblem(form);
    });

    if (e.stage === 'row') {
      const kindRow = this.#el('div', 'dc-calc-field', form);
      const legend = this.#el('span', 'dc-calc-kind-label', kindRow);
      legend.textContent = 'Aggregates as';
      const choices = this.#el('div', 'dc-calc-kinds', kindRow);
      for (const kind of ['dimension', 'measure'] as const) {
        const option = this.#el('label', 'dc-calc-kind', choices);
        const radio = this.#el('input', 'dc-calc-kind-input',
          option) as HTMLInputElement;
        radio.type = 'radio';
        radio.name = 'dc-calc-kind';
        radio.value = kind;
        radio.checked = e.kind === kind;
        radio.addEventListener('change', () => {
          if (radio.checked) e.kind = kind;
        });
        const text = this.#el('span', 'dc-calc-kind-text', option);
        // Say what it DOES, not what it is called: "measure" and
        // "dimension" are the cube's words, and the choice the user is
        // making is whether the column adds up.
        text.textContent = kind === 'measure'
          ? 'a measure — sums over a group'
          : 'a dimension — shows its value, or blank if it varies';
      }
    }

    const exprRow = this.#el('label', 'dc-calc-field', form);
    exprRow.append(this.#doc.createTextNode('Pure expression'));
    const expr = this.#el('textarea', 'dc-calc-input-expr',
      exprRow) as HTMLTextAreaElement;
    expr.value = e.expression;
    expr.rows = 3;
    expr.spellcheck = false;
    expr.placeholder = e.stage === 'row'
      ? '$x.notional * 1.05'
      : '$x.profit / $x.revenue';
    expr.addEventListener('input', () => {
      e.expression = expr.value;
      this.#refreshProblem(form);
    });

    // COMPLETION, as a list rather than a popup. A popup over a
    // textarea needs caret geometry and a keyboard model of its own;
    // a list beside the field is the same information, insertable, and
    // cannot land in the wrong place.
    const offered = completionsFor(this.#pending(), e.stage, e.original);
    const picker = this.#el('div', 'dc-calc-picker', form);
    const search = this.#el('input', 'dc-calc-search',
      picker) as HTMLInputElement;
    search.type = 'search';
    search.placeholder = `Insert — ${offered.length} in scope`;
    const items = this.#el('ul', 'dc-calc-items', picker);
    const paint = (filter: string) => {
      items.replaceChildren();
      const q = filter.trim().toLowerCase();
      const shown = q.length === 0
        ? offered
        : offered.filter((c) => c.label.toLowerCase().includes(q)
          || c.detail.toLowerCase().includes(q));
      for (const c of shown.slice(0, 60)) this.#renderItem(items, c, expr, e);
      if (shown.length === 0) {
        const none = this.#el('li', 'dc-calc-noitem', items);
        none.textContent = 'Nothing in scope matches.';
      }
    };
    search.addEventListener('input', () => paint(search.value));
    paint('');

    const problem = this.#el('p', 'dc-calc-problem', form);
    problem.setAttribute('role', 'alert');

    const actions = this.#el('div', 'dc-calc-actions', form);
    const save = this.#el('button', 'dc-calc-save',
      actions) as HTMLButtonElement;
    save.type = 'button';
    save.textContent = e.original === undefined ? 'Add' : 'Save';
    save.addEventListener('click', () => this.#save());
    const cancel = this.#el('button', 'dc-calc-cancel',
      actions) as HTMLButtonElement;
    cancel.type = 'button';
    cancel.textContent = 'Cancel';
    cancel.addEventListener('click', () => {
      this.#editing = null;
      this.#render();
    });

    this.#refreshProblem(form);
    name.focus();
  }

  #renderItem(
    into: HTMLElement,
    c: Completion,
    expr: HTMLTextAreaElement,
    e: Editing,
  ): void {
    const li = this.#el('li', `dc-calc-item-${c.kind}`, into);
    const button = this.#el('button', 'dc-calc-insert',
      li) as HTMLButtonElement;
    button.type = 'button';
    button.dataset['insert'] = c.insert;
    const label = this.#el('span', 'dc-calc-item-label', button);
    label.textContent = c.label;
    const detail = this.#el('span', 'dc-calc-item-detail', button);
    detail.textContent = c.detail;
    button.addEventListener('click', () => {
      // At the CARET, not appended: half of using this is fixing the
      // middle of an expression you already wrote.
      const at = expr.selectionStart ?? expr.value.length;
      const to = expr.selectionEnd ?? at;
      expr.value = expr.value.slice(0, at) + c.insert + expr.value.slice(to);
      const after = at + c.insert.length;
      expr.setSelectionRange(after, after);
      e.expression = expr.value;
      expr.focus();
      const form = li.closest('.dc-calc-form');
      if (form) this.#refreshProblem(form);
    });
  }

  /**
   * DUCK-TYPED, not `instanceof`. The DOM constructor belongs to the
   * document's realm, so `instanceof HTMLElement` is false in an
   * iframe or a test DOM and the code silently does nothing -- which
   * bit this repo twice and is banned by its own source guardrail.
   * Check the property actually used.
   */
  #refreshProblem(form: ParentNode): void {
    const problem = form.querySelector('.dc-calc-problem');
    const save = form.querySelector('.dc-calc-save');
    const message = this.#problem();
    if (problem && 'textContent' in problem) {
      problem.textContent = message ?? '';
      (problem as { hidden?: boolean }).hidden = message === null;
    }
    if (save && 'disabled' in save) {
      (save as { disabled: boolean }).disabled = message !== null;
    }
  }

  #el(tag: string, className: string, parent: HTMLElement): HTMLElement {
    const el = this.#doc.createElement(tag);
    el.className = className;
    parent.append(el);
    return el;
  }
}
