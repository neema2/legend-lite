// The filter editor: a nested condition tree, as DataCube has.
//
// Modelled on legend-studio's own structure rather than invented. Its
// DataCubeFilterEditorTreeNode is abstract with a `not` flag; a
// condition node carries column/operation/value; a group node carries
// children and an AND/OR operation, and groups nest arbitrarily. So
// `not` belongs on EVERY node, including groups, and a flat list of
// conditions -- the obvious first design -- cannot express
// "A AND NOT (B OR C)", which is an ordinary thing to want.
//
// Three rules the shape enforces:
//
//  - The operator decides the VALUE INPUT, not the other way round.
//    `is null` takes nothing, `in` takes a list, the `*Column` forms
//    take a second column. One text box for all of them is how a UI
//    sends a filter the engine rejects, or worse accepts and misreads.
//  - An incomplete node is EXCLUDED, not guessed. A half-typed row is
//    the normal state of a UI and should narrow nothing rather than
//    narrow wrongly.
//  - The editor owns no query state. It emits a FilterNode; the
//    controller decides what to do with it, so there is no second
//    copy of the filter to drift from the snapshot.

import type {
  FilterCondition,
  FilterNode,
  FilterOperator,
  FilterValue,
} from '../snapshot.ts';

/** What kind of value input an operator needs. */
export type OperandKind = 'none' | 'single' | 'list' | 'column';

/**
 * Operator arity, with DataCube's own label for each, so a user
 * moving between the two products reads the same words.
 */
export const OPERATORS: ReadonlyArray<{
  readonly op: FilterOperator;
  readonly label: string;
  readonly operand: OperandKind;
}> = [
  { op: 'equal', label: '=', operand: 'single' },
  { op: 'notEqual', label: '!=', operand: 'single' },
  { op: 'lessThan', label: '<', operand: 'single' },
  { op: 'lessThanEqual', label: '<=', operand: 'single' },
  { op: 'greaterThan', label: '>', operand: 'single' },
  { op: 'greaterThanEqual', label: '>=', operand: 'single' },
  { op: 'isEmpty', label: 'is null', operand: 'none' },
  { op: 'isNotEmpty', label: 'is not null', operand: 'none' },
  { op: 'in', label: 'in', operand: 'list' },
  { op: 'notIn', label: 'not in', operand: 'list' },
  { op: 'contains', label: 'contains', operand: 'single' },
  { op: 'notContains', label: 'does not contain', operand: 'single' },
  { op: 'startsWith', label: 'starts with', operand: 'single' },
  { op: 'notStartsWith', label: 'does not start with', operand: 'single' },
  { op: 'endsWith', label: 'ends with', operand: 'single' },
  { op: 'notEndsWith', label: 'does not end with', operand: 'single' },
  { op: 'equalCaseInsensitive', label: '= (case-insensitive)', operand: 'single' },
  { op: 'notEqualCaseInsensitive', label: '!= (case-insensitive)', operand: 'single' },
  { op: 'containsCaseInsensitive', label: 'contains (case-insensitive)', operand: 'single' },
  { op: 'startsWithCaseInsensitive', label: 'starts with (case-insensitive)', operand: 'single' },
  { op: 'endsWithCaseInsensitive', label: 'ends with (case-insensitive)', operand: 'single' },
  { op: 'inCaseInsensitive', label: 'in (case-insensitive)', operand: 'list' },
  { op: 'notInCaseInsensitive', label: 'not in (case-insensitive)', operand: 'list' },
  { op: 'equalColumn', label: '= value in column', operand: 'column' },
  { op: 'equalCaseInsensitiveColumn', label: '= (case-insensitive) value in column', operand: 'column' },
  { op: 'notEqualColumn', label: '!= value in column', operand: 'column' },
  { op: 'notEqualCaseInsensitiveColumn', label: '!= (case-insensitive) value in column', operand: 'column' },
  { op: 'lessThanColumn', label: '< value in column', operand: 'column' },
  { op: 'lessThanEqualColumn', label: '<= value in column', operand: 'column' },
  { op: 'greaterThanColumn', label: '> value in column', operand: 'column' },
  { op: 'greaterThanEqualColumn', label: '>= value in column', operand: 'column' },
];

const OPERAND_BY_OP = new Map<FilterOperator, OperandKind>(
  OPERATORS.map((o) => [o.op, o.operand]),
);

export function operandKind(op: FilterOperator): OperandKind {
  return OPERAND_BY_OP.get(op) ?? 'single';
}

// -- the draft tree ---------------------------------------------------

export interface DraftCondition {
  readonly kind: 'condition';
  readonly id: string;
  readonly not: boolean;
  readonly column: string;
  readonly operator: FilterOperator;
  readonly text: string;
  readonly rightColumn: string;
}

export interface DraftGroup {
  readonly kind: 'group';
  readonly id: string;
  readonly not: boolean;
  readonly join: 'and' | 'or';
  readonly children: readonly DraftNode[];
}

export type DraftNode = DraftCondition | DraftGroup;

let counter = 0;
function nextId(): string {
  counter += 1;
  return `f${counter}`;
}

export function newCondition(column: string): DraftCondition {
  return {
    kind: 'condition',
    id: nextId(),
    not: false,
    column,
    operator: 'equal',
    text: '',
    rightColumn: '',
  };
}

export function newGroup(children: readonly DraftNode[] = []): DraftGroup {
  return { kind: 'group', id: nextId(), not: false, join: 'and', children };
}

/**
 * Parse a typed value.
 *
 * A numeric column must compare numerically -- '9' against '10' as
 * text puts 9 after 10 -- so a value that looks like a number becomes
 * one. Quoting forces text, which is the escape hatch for an
 * identifier that merely looks numeric, like an account code.
 */
export function parseValue(text: string): FilterValue {
  const t = text.trim();
  if (
    (t.startsWith("'") && t.endsWith("'") && t.length > 1) ||
    (t.startsWith('"') && t.endsWith('"') && t.length > 1)
  ) {
    return t.slice(1, -1);
  }
  if (t === 'true') return true;
  if (t === 'false') return false;
  if (t !== '' && Number.isFinite(Number(t))) return Number(t);
  return t;
}

/** Split a list operand. Empty entries are dropped, not sent as ''. */
export function parseList(text: string): FilterValue[] {
  return text
    .split(',')
    .map((s) => s.trim())
    .filter((s) => s !== '')
    .map(parseValue);
}

function conditionOf(d: DraftCondition): FilterCondition | null {
  if (!d.column) return null;
  const kind = operandKind(d.operator);

  if (kind === 'none') {
    return { kind: 'condition', column: d.column, operator: d.operator };
  }
  if (kind === 'column') {
    return d.rightColumn
      ? {
          kind: 'condition',
          column: d.column,
          operator: d.operator,
          rightColumn: d.rightColumn,
        }
      : null;
  }
  if (kind === 'list') {
    const values = parseList(d.text);
    return values.length > 0
      ? { kind: 'condition', column: d.column, operator: d.operator, value: values }
      : null;
  }
  return d.text.trim() === ''
    ? null
    : {
        kind: 'condition',
        column: d.column,
        operator: d.operator,
        value: parseValue(d.text),
      };
}

/**
 * Convert a draft node to a filter, or null when it contributes
 * nothing.
 *
 * A group with no usable children disappears entirely rather than
 * serialising as an empty AND -- which is `true`, harmless but a
 * pointless stage in every generated query someone has to read.
 */
export function toFilterNode(node: DraftNode): FilterNode | null {
  if (node.kind === 'condition') {
    const c = conditionOf(node);
    if (!c) return null;
    return node.not ? { kind: 'not', child: c } : c;
  }

  const children = node.children
    .map(toFilterNode)
    .filter((c): c is FilterNode => c !== null);
  if (children.length === 0) return null;

  const inner: FilterNode =
    children.length === 1 && !node.not
      ? children[0]!
      : { kind: node.join, children };
  return node.not ? { kind: 'not', child: inner } : inner;
}

/** The root's filter, or undefined for "no filter at all". */
export function toFilter(root: DraftGroup): FilterNode | undefined {
  return toFilterNode(root) ?? undefined;
}

// -- immutable tree edits ---------------------------------------------

function mapNode(
  node: DraftNode,
  id: string,
  fn: (n: DraftNode) => DraftNode | null,
): DraftNode | null {
  if (node.id === id) return fn(node);
  if (node.kind !== 'group') return node;
  const children = node.children
    .map((c) => mapNode(c, id, fn))
    .filter((c): c is DraftNode => c !== null);
  return { ...node, children };
}

export function updateNode(
  root: DraftGroup,
  id: string,
  patch: Partial<DraftCondition> & Partial<DraftGroup>,
): DraftGroup {
  const next = mapNode(root, id, (n) => ({ ...n, ...patch }) as DraftNode);
  return (next as DraftGroup | null) ?? root;
}

export function removeNode(root: DraftGroup, id: string): DraftGroup {
  const next = mapNode(root, id, () => null);
  return (next as DraftGroup | null) ?? { ...root, children: [] };
}

export function addTo(
  root: DraftGroup,
  groupId: string,
  child: DraftNode,
): DraftGroup {
  const next = mapNode(root, groupId, (n) =>
    n.kind === 'group' ? { ...n, children: [...n.children, child] } : n,
  );
  return (next as DraftGroup | null) ?? root;
}

// -- the DOM editor ---------------------------------------------------

/**
 * Build a draft tree from a filter that already exists.
 *
 * The inverse of `toFilterNode`, and it has to exist: without it the
 * editor opens EMPTY on a cube that is already filtered, and the
 * user's first change writes that emptiness back -- silently
 * dropping a filter they could see in force on the grid behind the
 * dialog.
 *
 * `not` is a wrapping node in the model and a flag on a draft, so
 * unwrapping folds it onto the child. A double negation collapses,
 * which is the one place the round trip is not literal: `NOT NOT x`
 * and `x` are the same filter and the editor should show the simpler
 * one.
 */
export function fromFilterNode(node: FilterNode): DraftNode {
  if (node.kind === 'not') {
    const inner = fromFilterNode(node.child);
    return { ...inner, not: !inner.not } as DraftNode;
  }
  if (node.kind === 'condition') {
    return {
      kind: 'condition',
      id: nextId(),
      not: false,
      column: node.column,
      operator: node.operator,
      text: textOf(node.value),
      rightColumn: node.rightColumn ?? '',
    };
  }
  return {
    kind: 'group',
    id: nextId(),
    not: false,
    join: node.kind,
    children: node.children.map(fromFilterNode),
  };
}

/**
 * Render a stored value back into what a user would have typed.
 *
 * A list joins with commas because that is how `parseList` reads it;
 * a string that LOOKS numeric is requoted, or reopening the editor
 * would silently turn an account code into a number.
 */
function textOf(value: FilterValue | readonly FilterValue[] | undefined): string {
  if (value === undefined) return '';
  if (Array.isArray(value)) {
    return (value as readonly FilterValue[]).map(scalarText).join(', ');
  }
  return scalarText(value as FilterValue);
}

function scalarText(value: FilterValue): string {
  if (value instanceof Date) return value.toISOString();
  if (typeof value === 'string' && value.trim() !== '' && !Number.isNaN(Number(value))) {
    return `"${value}"`;
  }
  return String(value);
}

export interface FilterEditorOptions {
  readonly columns: readonly string[];
  readonly onChange: (filter: FilterNode | undefined) => void;
  /** The filter already in force, so the editor opens showing it. */
  readonly value?: FilterNode;
}

/** Indentation per level, matching DataCube's FILTER_TREE_INDENTATION_SPACE. */
export const INDENT_PX = 36;

export class FilterEditor {
  readonly #root: HTMLElement;
  readonly #options: FilterEditorOptions;
  #tree: DraftGroup = newGroup();

  constructor(container: HTMLElement, options: FilterEditorOptions) {
    this.#root = container;
    this.#options = options;
    if (options.value) {
      // A seeded filter that is already a top-level group keeps that
      // group rather than being nested inside a fresh one, or every
      // reopen adds a level of brackets to a filter nobody changed.
      const seeded = fromFilterNode(options.value);
      this.#tree =
        seeded.kind === 'group' && !seeded.not ? seeded : newGroup([seeded]);
    }
    this.#root.classList.add('dc-filters');
    this.render();
  }

  get filter(): FilterNode | undefined {
    return toFilter(this.#tree);
  }

  get tree(): DraftGroup {
    return this.#tree;
  }

  set tree(next: DraftGroup) {
    this.#tree = next;
    this.render();
    this.#options.onChange(this.filter);
  }

  addCondition(groupId = this.#tree.id): void {
    this.tree = addTo(
      this.#tree,
      groupId,
      newCondition(this.#options.columns[0] ?? ''),
    );
  }

  addGroup(groupId = this.#tree.id): void {
    this.tree = addTo(
      this.#tree,
      groupId,
      newGroup([newCondition(this.#options.columns[0] ?? '')]),
    );
  }

  remove(id: string): void {
    this.tree = removeNode(this.#tree, id);
  }

  update(id: string, patch: Partial<DraftCondition> & Partial<DraftGroup>): void {
    this.tree = updateNode(this.#tree, id, patch);
  }

  clear(): void {
    this.tree = newGroup();
  }

  render(): void {
    this.#root.replaceChildren();
    this.#renderGroup(this.#tree, 0, this.#root, true);
  }

  #renderGroup(
    group: DraftGroup,
    level: number,
    into: HTMLElement,
    isRoot: boolean,
  ): void {
    const doc = this.#root.ownerDocument;

    if (!isRoot) {
      into.appendChild(
        this.#nodeRow(group, level, [
          this.#notToggle(group),
          this.#joinSelect(group),
          this.#button('+ condition', () => this.addCondition(group.id)),
          this.#button('+ group', () => this.addGroup(group.id)),
        ]),
      );
    }

    group.children.forEach((child) => {
      if (child.kind === 'group') {
        this.#renderGroup(child, level + 1, into, false);
      } else {
        into.appendChild(this.#conditionRow(child, level + (isRoot ? 0 : 1)));
      }
    });

    if (isRoot) {
      const bar = doc.createElement('div');
      bar.className = 'dc-filter-actions';
      // The root's join only matters once there is something to join.
      if (group.children.length > 1) bar.appendChild(this.#joinSelect(group));
      bar.appendChild(
        this.#button(
          group.children.length === 0 ? 'Add filter' : '+ condition',
          () => this.addCondition(),
        ),
      );
      bar.appendChild(this.#button('+ group', () => this.addGroup()));
      if (group.children.length > 0) {
        bar.appendChild(this.#button('Clear', () => this.clear()));
      }
      into.appendChild(bar);
    }
  }

  #conditionRow(c: DraftCondition, level: number): HTMLElement {
    const doc = this.#root.ownerDocument;
    const kind = operandKind(c.operator);

    const parts: HTMLElement[] = [
      this.#notToggle(c),
      this.#select(this.#options.columns, c.column, 'dc-filter-column', (v) =>
        this.update(c.id, { column: v }),
      ),
      this.#select(
        OPERATORS.map((o) => o.op),
        c.operator,
        'dc-filter-op',
        (v) => this.update(c.id, { operator: v as FilterOperator }),
        OPERATORS.map((o) => o.label),
      ),
    ];

    // The operator decides the operand input; a fixed text box is how
    // a UI produces a filter the engine cannot run.
    if (kind === 'column') {
      parts.push(
        this.#select(
          this.#options.columns,
          c.rightColumn,
          'dc-filter-value',
          (v) => this.update(c.id, { rightColumn: v }),
        ),
      );
    } else if (kind !== 'none') {
      const input = doc.createElement('input');
      input.className = 'dc-filter-value';
      input.type = 'text';
      input.value = c.text;
      input.placeholder = kind === 'list' ? 'a, b, c' : 'value';
      input.addEventListener('change', () =>
        this.update(c.id, { text: input.value }),
      );
      parts.push(input);
    }

    parts.push(
      this.#button('×', () => this.remove(c.id), 'dc-filter-remove', 'Remove condition'),
    );
    return this.#nodeRow(c, level, parts);
  }

  #nodeRow(
    node: DraftNode,
    level: number,
    parts: readonly HTMLElement[],
  ): HTMLElement {
    const doc = this.#root.ownerDocument;
    const row = doc.createElement('div');
    row.className =
      node.kind === 'group' ? 'dc-filter-row dc-filter-group' : 'dc-filter-row';
    row.dataset['id'] = node.id;
    // Depth by indentation, with a gutter line, as DataCube does.
    row.style.setProperty('--dc-filter-level', String(level));
    parts.forEach((p) => row.appendChild(p));
    return row;
  }

  #notToggle(node: DraftNode): HTMLElement {
    const doc = this.#root.ownerDocument;
    const b = doc.createElement('button');
    b.type = 'button';
    b.className = node.not ? 'dc-filter-not dc-filter-not-on' : 'dc-filter-not';
    b.textContent = 'NOT';
    b.setAttribute('aria-pressed', String(node.not));
    b.setAttribute(
      'aria-label',
      node.kind === 'group' ? 'Negate this group' : 'Negate this condition',
    );
    b.addEventListener('click', () => this.update(node.id, { not: !node.not }));
    return b;
  }

  #joinSelect(group: DraftGroup): HTMLSelectElement {
    return this.#select(
      ['and', 'or'],
      group.join,
      'dc-filter-join',
      (v) => this.update(group.id, { join: v === 'or' ? 'or' : 'and' }),
      ['AND', 'OR'],
    );
  }

  #button(
    text: string,
    onClick: () => void,
    className = 'dc-filter-btn',
    ariaLabel?: string,
  ): HTMLButtonElement {
    const b = this.#root.ownerDocument.createElement('button');
    b.type = 'button';
    b.className = className;
    b.textContent = text;
    if (ariaLabel) b.setAttribute('aria-label', ariaLabel);
    b.addEventListener('click', onClick);
    return b;
  }

  #select(
    values: readonly string[],
    selected: string,
    className: string,
    onChange: (value: string) => void,
    labels?: readonly string[],
  ): HTMLSelectElement {
    const doc = this.#root.ownerDocument;
    const el = doc.createElement('select');
    el.className = className;
    values.forEach((v, i) => {
      const o = doc.createElement('option');
      o.value = v;
      o.textContent = labels?.[i] ?? v;
      if (v === selected) o.selected = true;
      el.appendChild(o);
    });
    el.addEventListener('change', () => onChange(el.value));
    return el;
  }
}
