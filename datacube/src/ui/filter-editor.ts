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
//  - Nothing reaches the cube until APPLY. Upstream batches the edits
//    of the Filter window and publishes once (Cancel / Apply / OK);
//    applying on every keystroke ran a query per character and put
//    every half-typed filter on the undo stack.

import {
  isNumericType,
  isRelativeDate,
  type FilterCondition,
  type FilterNode,
  type FilterOperator,
  type FilterValue,
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

/** A column as the editor needs it: its name and its Pure type. */
export interface FilterColumn {
  readonly name: string;
  readonly type: string;
}

/** Upstream's DataCubeColumnDataType, plus the time of day. */
export type DataType = 'text' | 'number' | 'date' | 'time' | 'boolean';

export function dataTypeOf(type: string | undefined): DataType {
  if (type === 'Boolean') return 'boolean';
  if (type === 'StrictTime') return 'time';
  if (type === 'Date' || type === 'StrictDate' || type === 'DateTime'
    || type === 'Timestamp') return 'date';
  if (isNumericType(type) || type === 'Number') return 'number';
  return 'text';
}

/** Whether values of this type carry a time of day. */
function hasTime(type: string | undefined): boolean {
  return type === 'DateTime' || type === 'Timestamp';
}

const TEXT = new Set<DataType>(['text']);
const EQUALITY = new Set<DataType>(['text', 'number', 'date', 'time', 'boolean']);
const ORDERING = new Set<DataType>(['number', 'date', 'time']);
const LISTS = new Set<DataType>(['text', 'number', 'date']);
const NULLS = new Set<DataType>(['text', 'number', 'date', 'time', 'boolean']);
const COLUMN_EQUALITY = new Set<DataType>(['text', 'number', 'date', 'time']);

/**
 * Which column types each operator accepts -- upstream's
 * `isCompatibleWithColumn`, operator by operator. "Starts with" on a
 * number or "<" on a boolean is not a question, and offering it is how
 * a user builds a filter the engine refuses.
 */
const COMPATIBLE: Readonly<Record<FilterOperator, ReadonlySet<DataType>>> = {
  equal: EQUALITY,
  notEqual: EQUALITY,
  lessThan: ORDERING,
  lessThanEqual: ORDERING,
  greaterThan: ORDERING,
  greaterThanEqual: ORDERING,
  isEmpty: NULLS,
  isNotEmpty: NULLS,
  in: LISTS,
  notIn: LISTS,
  contains: TEXT,
  notContains: TEXT,
  startsWith: TEXT,
  notStartsWith: TEXT,
  endsWith: TEXT,
  notEndsWith: TEXT,
  equalCaseInsensitive: TEXT,
  notEqualCaseInsensitive: TEXT,
  containsCaseInsensitive: TEXT,
  startsWithCaseInsensitive: TEXT,
  endsWithCaseInsensitive: TEXT,
  inCaseInsensitive: TEXT,
  notInCaseInsensitive: TEXT,
  equalColumn: COLUMN_EQUALITY,
  equalCaseInsensitiveColumn: TEXT,
  notEqualColumn: COLUMN_EQUALITY,
  notEqualCaseInsensitiveColumn: TEXT,
  lessThanColumn: ORDERING,
  lessThanEqualColumn: ORDERING,
  greaterThanColumn: ORDERING,
  greaterThanEqualColumn: ORDERING,
};

/** The operators a column of this type accepts, in the table's order. */
export function operatorsFor(type: string | undefined): FilterOperator[] {
  const t = dataTypeOf(type);
  return OPERATORS.filter((o) => COMPATIBLE[o.op].has(t)).map((o) => o.op);
}

export function isCompatible(op: FilterOperator, type: string | undefined): boolean {
  return COMPATIBLE[op].has(dataTypeOf(type));
}

/**
 * Evaluate arithmetic typed into a number field -- upstream's number
 * editor evaluates `1e6 * 3` on Enter. Numbers (with exponents), the
 * four operators, unary minus and parentheses; anything else is NaN.
 * A tiny recursive descent, never `eval`.
 */
export function evaluateArithmetic(text: string): number {
  const src = text.replace(/,/g, '').trim();
  let i = 0;
  const peek = (): string => src[i] ?? '';
  const skip = (): void => { while (peek() === ' ') i += 1; };
  const num = (): number => {
    skip();
    const m = /^(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?/.exec(src.slice(i));
    if (!m) return NaN;
    i += m[0].length;
    return Number(m[0]);
  };
  const factor = (): number => {
    skip();
    if (peek() === '-') { i += 1; return -factor(); }
    if (peek() === '+') { i += 1; return factor(); }
    if (peek() === '(') {
      i += 1;
      const v = expr();
      skip();
      if (peek() !== ')') return NaN;
      i += 1;
      return v;
    }
    return num();
  };
  const term = (): number => {
    let v = factor();
    for (;;) {
      skip();
      const op = peek();
      if (op !== '*' && op !== '/') return v;
      i += 1;
      const r = factor();
      v = op === '*' ? v * r : v / r;
    }
  };
  const expr = (): number => {
    let v = term();
    for (;;) {
      skip();
      const op = peek();
      if (op !== '+' && op !== '-') return v;
      i += 1;
      const r = term();
      v = op === '+' ? v + r : v - r;
    }
  };
  if (src === '') return NaN;
  const out = expr();
  skip();
  return i === src.length && Number.isFinite(out) ? out : NaN;
}

const pad = (n: number): string => String(n).padStart(2, '0');

/** A local date as the editor writes it: YYYY-MM-DD[THH:mm:ss]. */
export function dateText(d: Date, withTime: boolean): string {
  const day = `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;
  return withTime
    ? `${day}T${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`
    : day;
}

/**
 * Parse a value against its COLUMN's type, or null when it is not a
 * value of that type yet (a half-typed number, an empty date).
 *
 * Typed rather than guessed: `parseValue` turns anything numeric into
 * a number, which is right for a number column and wrong for a text
 * column holding account codes.
 */
export function parseTyped(text: string, type: string | undefined):
FilterValue | null {
  const t = text.trim();
  switch (dataTypeOf(type)) {
    case 'number': {
      const n = evaluateArithmetic(t);
      return Number.isFinite(n) ? n : null;
    }
    case 'boolean':
      return t === 'true' ? true : t === 'false' ? false : null;
    case 'date': {
      if (t === 'today()') return { relative: 'today' };
      if (t === 'now()') return { relative: 'now' };
      const m = /^(\d{4})-(\d{2})-(\d{2})(?:[T ](\d{2}):(\d{2})(?::(\d{2}))?)?/
        .exec(t);
      if (!m) return null;
      const at = new Date(Number(m[1]), Number(m[2]) - 1, Number(m[3]),
        Number(m[4] ?? 0), Number(m[5] ?? 0), Number(m[6] ?? 0));
      return Number.isNaN(at.getTime()) ? null : at;
    }
    case 'time':
      return t === '' ? null : t;
    case 'text':
      if (t === '') return null;
      if ((t.startsWith("'") && t.endsWith("'") && t.length > 1)
        || (t.startsWith('"') && t.endsWith('"') && t.length > 1)) {
        return t.slice(1, -1);
      }
      return text;
  }
}

/**
 * The value a condition starts with for a column type -- upstream's
 * `generateDefaultValue`: zero, false, today; empty text.
 */
export function defaultText(type: string | undefined): string {
  switch (dataTypeOf(type)) {
    case 'number': return '0';
    case 'boolean': return 'false';
    case 'date': return dateText(new Date(), hasTime(type));
    default: return '';
  }
}

// -- the draft tree ---------------------------------------------------

export interface DraftCondition {
  readonly kind: 'condition';
  readonly id: string;
  readonly not: boolean;
  readonly column: string;
  readonly operator: FilterOperator;
  readonly text: string;
  /** The values of a list operand (`in` / `not in`), one per entry. */
  readonly items?: readonly string[] | undefined;
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

export function newCondition(column: string, type?: string): DraftCondition {
  return {
    kind: 'condition',
    id: nextId(),
    not: false,
    column,
    operator: 'equal',
    text: type === undefined ? '' : defaultText(type),
    rightColumn: '',
  };
}

/**
 * A copy of a node with fresh ids -- what upstream's `+` and group
 * buttons insert, so adding a condition leaves the filter's MATCH
 * unchanged until the copy is edited.
 */
export function cloneNode(node: DraftNode): DraftNode {
  return node.kind === 'condition'
    ? { ...node, id: nextId() }
    : { ...node, id: nextId(), children: node.children.map(cloneNode) };
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
  // Upstream's TODAY / NOW: a date relative to when the query runs.
  // Only the exact call spelling, so a text value "today" is still text.
  if (t === 'today()') return { relative: 'today' };
  if (t === 'now()') return { relative: 'now' };
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

/** Column types, by name, for the conversion below. Absent: untyped. */
type TypeOf = (column: string) => string | undefined;

function conditionOf(d: DraftCondition, typeOf?: TypeOf):
FilterCondition | null {
  if (!d.column) return null;
  const kind = operandKind(d.operator);
  const type = typeOf?.(d.column);
  const one = (text: string): FilterValue | null => (type === undefined
    ? (text.trim() === '' ? null : parseValue(text))
    : parseTyped(text, type));

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
    const values = d.items !== undefined
      ? d.items.map(one).filter((v): v is FilterValue => v !== null)
      : parseList(d.text);
    return values.length > 0
      ? { kind: 'condition', column: d.column, operator: d.operator, value: values }
      : null;
  }
  const value = one(d.text);
  return value === null
    ? null
    : { kind: 'condition', column: d.column, operator: d.operator, value };
}

/**
 * Convert a draft node to a filter, or null when it contributes
 * nothing.
 *
 * A group with no usable children disappears entirely rather than
 * serialising as an empty AND -- which is `true`, harmless but a
 * pointless stage in every generated query someone has to read.
 */
export function toFilterNode(node: DraftNode, typeOf?: TypeOf): FilterNode | null {
  if (node.kind === 'condition') {
    const c = conditionOf(node, typeOf);
    if (!c) return null;
    return node.not ? { kind: 'not', child: c } : c;
  }

  const children = node.children
    .map((c) => toFilterNode(c, typeOf))
    .filter((c): c is FilterNode => c !== null);
  if (children.length === 0) return null;

  // A group of ONE is that one, negated or not. The `not` wraps
  // whatever comes out, so keeping the group around a single child
  // would emit NOT(AND(x)) where NOT(x) says the same thing -- and
  // the difference is visible, because the user reads the generated
  // Pure. This also makes wrapping a node in a sub-group leave the
  // query byte-for-byte unchanged, which is what lets the group
  // button be a safe, reversible gesture.
  const inner: FilterNode =
    children.length === 1 ? children[0]! : { kind: node.join, children };
  return node.not ? { kind: 'not', child: inner } : inner;
}

/** The root's filter, or undefined for "no filter at all". */
export function toFilter(root: DraftGroup, typeOf?: TypeOf): FilterNode | undefined {
  return toFilterNode(root, typeOf) ?? undefined;
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

/**
 * Remove a node, as upstream does: a group left holding ONE child is
 * replaced by that child (its NOT folded on), so removing B from
 * `A AND (B OR C)` leaves `A AND C` rather than `A AND (C)`. The root
 * is never flattened; emptied, it means "no filter".
 */
export function removeNode(root: DraftGroup, id: string): DraftGroup {
  const next = (mapNode(root, id, () => null) as DraftGroup | null)
    ?? { ...root, children: [] };
  const flatten = (group: DraftGroup): DraftGroup => ({
    ...group,
    children: group.children.map((child) => {
      if (child.kind !== 'group') return child;
      const inner = flatten(child);
      if (inner.children.length === 1) {
        const only = inner.children[0]!;
        return inner.not ? { ...only, not: !only.not } as DraftNode : only;
      }
      return inner;
    }).filter((c) => c.kind !== 'group' || c.children.length > 0),
  });
  return flatten(next);
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

/** The group a node sits in, or null for the root. */
export function parentOf(root: DraftGroup, id: string): DraftGroup | null {
  if (root.children.some((c) => c.id === id)) return root;
  for (const child of root.children) {
    if (child.kind === 'group') {
      const found = parentOf(child, id);
      if (found) return found;
    }
  }
  return null;
}

/**
 * Insert a node immediately AFTER a sibling.
 *
 * DataCube's `+` button reads "insert a new column filter, just
 * after this filter", and the position matters: appending to the end
 * of the group instead puts the new condition somewhere the user was
 * not looking, which in a deep tree means losing it.
 */
export function insertAfter(
  root: DraftGroup,
  siblingId: string,
  node: DraftNode,
): DraftGroup {
  const rebuild = (group: DraftGroup): DraftGroup => {
    const children: DraftNode[] = [];
    let placed = false;
    for (const child of group.children) {
      children.push(child.kind === 'group' ? rebuild(child) : child);
      if (child.id === siblingId) {
        children.push(node);
        placed = true;
      }
    }
    return placed || children !== group.children
      ? { ...group, children }
      : group;
  };
  return rebuild(root);
}

/**
 * Wrap a node in a new group, in place.
 *
 * DataCube's third controller button: "put this filter in its own
 * sub-group (and combine it with other filters)". This is the only
 * way to build `A AND (B OR C)` from an existing flat list without
 * deleting and retyping B.
 *
 * The new group inherits the node's `not` and the node loses it, so
 * the meaning of the tree is unchanged by the wrapping itself.
 */
export function layerNode(
  root: DraftGroup,
  id: string,
  fresh: () => DraftCondition = () => newCondition(''),
): DraftGroup {
  // UPSTREAM'S LAYER: an OR group of the node and a copy of it -- the
  // match is unchanged (x OR x is x) and the copy is the one to edit.
  // A group gets a fresh condition beside it instead of a copy of the
  // whole group. OR, because the point of a sub-group is to relax.
  const rebuild = (group: DraftGroup): DraftGroup => ({
    ...group,
    children: group.children.map((child) => {
      if (child.id === id) {
        const partner = child.kind === 'condition' ? cloneNode(child) : fresh();
        return { ...newGroup([child, partner]), join: 'or' as const };
      }
      return child.kind === 'group' ? rebuild(child) : child;
    }),
  });
  // The root itself cannot be layered: it IS the outermost group.
  return root.id === id ? root : rebuild(root);
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
      text: Array.isArray(node.value) ? '' : textOf(node.value),
      ...(Array.isArray(node.value)
        ? { items: (node.value as readonly FilterValue[]).map(itemText) }
        : {}),
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

/** One list entry as its item editor shows it: no quoting needed. */
function itemText(value: FilterValue): string {
  if (value instanceof Date) {
    return dateText(value, value.getHours() + value.getMinutes()
      + value.getSeconds() > 0);
  }
  return isRelativeDate(value) ? `${value.relative}()` : String(value);
}

function scalarText(value: FilterValue): string {
  if (isRelativeDate(value)) return `${value.relative}()`;
  if (value instanceof Date) {
    return dateText(value, value.getHours() + value.getMinutes()
      + value.getSeconds() > 0);
  }
  if (typeof value === 'string' && value.trim() !== '' && !Number.isNaN(Number(value))) {
    return `"${value}"`;
  }
  return String(value);
}

export interface FilterEditorOptions {
  /** The filterable columns, with their types. */
  readonly columns: readonly FilterColumn[];
  /**
   * Publish the filter -- Apply and OK. Resolves to null when the cube
   * took it, else the reason it did not, which the editor shows in
   * its own window rather than on a status line behind it.
   */
  readonly onApply: (filter: FilterNode | undefined) =>
    void | string | null | Promise<string | null | void>;
  /** Cancel and OK: the window's host closes it. */
  readonly onClose?: () => void;
  /** The filter already in force, so the editor opens showing it. */
  readonly value?: FilterNode;
}

/**
 * DataCube's own tree geometry, in pixels.
 *
 * Copied rather than approximated because the numbers are what make
 * the connectors meet the gutter line: change one and the little
 * horizontal stub either overshoots the vertical rule or stops
 * short of it, and the tree stops reading as a tree.
 */
export const FILTER_TREE_OFFSET = 10;
export const INDENT_PX = 36;
export const FILTER_TREE_CONTROLLER_OFFSET = 60;
export const FILTER_TREE_GUTTER_OFFSET = 6;
export const FILTER_TREE_GUTTER_PADDING = 8;

/** Left padding of a node's row, at a depth. */
export function rowIndent(level: number): number {
  return (
    level * INDENT_PX +
    FILTER_TREE_OFFSET +
    Math.max(0, level - 1) * FILTER_TREE_CONTROLLER_OFFSET
  );
}

/** Where a group's vertical gutter line sits, for its children. */
export function gutterIndent(level: number): number {
  return (
    level * INDENT_PX +
    FILTER_TREE_OFFSET +
    FILTER_TREE_GUTTER_OFFSET +
    level * FILTER_TREE_CONTROLLER_OFFSET
  );
}

export class FilterEditor {
  readonly #root: HTMLElement;
  readonly #options: FilterEditorOptions;
  readonly #types: Map<string, string>;
  #tree: DraftGroup = newGroup();
  /** The node the user last clicked. Highlights it and its subtree. */
  #selected: string | null = null;
  /** The filter as last published, so an unchanged Apply is a no-op. */
  #applied: string;
  /** Why the last Apply was refused, shown in the window. */
  #problem: string | null = null;
  /** The list condition whose value popover is open. */
  #openList: string | null = null;

  constructor(container: HTMLElement, options: FilterEditorOptions) {
    this.#root = container;
    this.#options = options;
    this.#types = new Map(options.columns.map((c) => [c.name, c.type]));
    if (options.value) {
      // A seeded filter that is already a top-level group keeps that
      // group rather than being nested inside a fresh one, or every
      // reopen adds a level of brackets to a filter nobody changed.
      const seeded = fromFilterNode(options.value);
      this.#tree =
        seeded.kind === 'group' && !seeded.not ? seeded : newGroup([seeded]);
    }
    this.#applied = JSON.stringify(this.filter ?? null);
    this.#root.classList.add('dc-filters');
    this.render();
  }

  #typeOf = (column: string): string | undefined => this.#types.get(column);

  get filter(): FilterNode | undefined {
    return toFilter(this.#tree, this.#typeOf);
  }

  get tree(): DraftGroup {
    return this.#tree;
  }

  /** Replace the draft. Nothing reaches the cube until Apply. */
  set tree(next: DraftGroup) {
    this.#tree = next;
    this.#problem = null;
    this.render();
  }

  get selected(): string | null {
    return this.#selected;
  }

  /** Whether the draft differs from what the cube is running. */
  get dirty(): boolean {
    return JSON.stringify(this.filter ?? null) !== this.#applied;
  }

  select(id: string | null): void {
    this.#selected = id;
    this.render();
  }

  #first(): FilterColumn | undefined {
    return this.#options.columns[0];
  }

  /** A default condition on the first filterable column. */
  #fresh(): DraftCondition {
    const c = this.#first();
    return newCondition(c?.name ?? '', c?.type);
  }

  /** Start a filter from the empty state. */
  initialize(): void {
    this.tree = newGroup([this.#fresh()]);
  }

  addCondition(groupId = this.#tree.id): void {
    this.tree = addTo(this.#tree, groupId, this.#fresh());
  }

  addGroup(groupId = this.#tree.id): void {
    this.tree = addTo(this.#tree, groupId, newGroup([this.#fresh()]));
  }

  /**
   * The controller's `+`: a COPY of this condition just after it, so
   * the filter's match is unchanged until the copy is edited -- or,
   * on a group, a default condition after the group (upstream's
   * `addFilterNode`).
   */
  insertAfter(id: string): void {
    const node = findNode(this.#tree, id);
    if (!node || node.id === this.#tree.id) return;
    this.tree = insertAfter(this.#tree, id,
      node.kind === 'condition' ? cloneNode(node) : this.#fresh());
  }

  /** The controller's group button: an OR of this node and a copy. */
  layer(id: string): void {
    this.tree = layerNode(this.#tree, id, () => this.#fresh());
  }

  remove(id: string): void {
    this.tree = removeNode(this.#tree, id);
    if (this.#selected === id) this.#selected = null;
  }

  update(id: string, patch: Partial<DraftCondition> & Partial<DraftGroup>): void {
    this.tree = updateNode(this.#tree, id, patch);
  }

  /**
   * Point a condition at another column. The operator is kept if the
   * new column's type takes it, else the first one it does; the value
   * resets to the type's default -- upstream's column dropdown.
   */
  setColumn(id: string, column: string): void {
    const node = findNode(this.#tree, id);
    if (!node || node.kind !== 'condition' || node.column === column) return;
    const type = this.#typeOf(column);
    const operator = isCompatible(node.operator, type)
      ? node.operator
      : (operatorsFor(type)[0] ?? 'equal');
    this.#reset(id, { column, operator }, type);
  }

  /** Change the operator; a different operand kind resets the value. */
  setOperator(id: string, operator: FilterOperator): void {
    const node = findNode(this.#tree, id);
    if (!node || node.kind !== 'condition' || node.operator === operator) return;
    if (operandKind(operator) === operandKind(node.operator)) {
      this.update(id, { operator });
      return;
    }
    this.#reset(id, { operator }, this.#typeOf(node.column));
  }

  #reset(
    id: string,
    patch: { readonly column?: string; readonly operator: FilterOperator },
    type: string | undefined,
  ): void {
    const kind = operandKind(patch.operator);
    this.tree = updateNode(this.#tree, id, {
      ...(patch.column !== undefined ? { column: patch.column } : {}),
      operator: patch.operator,
      text: kind === 'single' ? defaultText(type) : '',
      rightColumn: '',
      ...(kind === 'list' ? { items: [] } : { items: undefined }),
    });
  }

  clear(): void {
    this.tree = newGroup();
  }

  /**
   * Publish the draft -- Apply. Only when it changed: an unchanged
   * Apply is not a new cube state. A refusal stays in the window.
   */
  async apply(): Promise<boolean> {
    if (!this.dirty) return true;
    const filter = this.filter;
    const refused = await this.#options.onApply(filter);
    if (typeof refused === 'string') {
      this.#problem = refused;
      this.render();
      return false;
    }
    this.#applied = JSON.stringify(filter ?? null);
    this.#problem = null;
    this.render();
    return true;
  }

  // -- rendering -------------------------------------------------------

  render(): void {
    const doc = this.#root.ownerDocument;
    this.#root.replaceChildren();
    const body = doc.createElement('div');
    body.className = 'dc-filter-body';
    this.#root.append(body, this.#footer());

    if (this.#tree.children.length === 0) {
      const empty = doc.createElement('div');
      empty.className = 'dc-filter-empty';
      const text = doc.createElement('div');
      text.textContent =
        'No filter is specified. Click the button below to start.';
      const create = this.#button(
        'Create New Filter',
        () => this.initialize(),
        'dc-filter-btn',
      );
      empty.append(text, create);
      body.append(empty);
      return;
    }

    const tree = doc.createElement('div');
    tree.className = 'dc-filter-tree';
    // Clicking the empty space below the tree clears the selection,
    // which is the only way to deselect without changing anything.
    tree.addEventListener('click', () => this.select(null));
    const inner = doc.createElement('div');
    inner.className = 'dc-filter-tree-body';
    inner.addEventListener('click', (event) => event.stopPropagation());
    this.#renderGroup(this.#tree, 0, inner, null, 0);
    tree.append(inner);
    body.append(tree);
  }

  /** Cancel / Apply / OK, and the reason a publish was refused. */
  #footer(): HTMLElement {
    const doc = this.#root.ownerDocument;
    const footer = doc.createElement('div');
    footer.className = 'dc-filter-footer';
    const problem = doc.createElement('div');
    problem.className = 'dc-filter-problem';
    problem.setAttribute('role', 'alert');
    problem.textContent = this.#problem ?? '';
    const cancel = this.#button('Cancel', () => this.#options.onClose?.(),
      'dc-button dc-filter-cancel');
    const apply = this.#button('Apply', () => { void this.apply(); },
      'dc-button dc-filter-apply');
    const ok = this.#button('OK', () => {
      void this.apply().then((done) => { if (done) this.#options.onClose?.(); });
    }, 'dc-button dc-primary dc-filter-ok');
    footer.append(problem, cancel, apply, ok);
    return footer;
  }

  #renderGroup(
    group: DraftGroup,
    level: number,
    into: HTMLElement,
    parent: DraftGroup | null,
    index: number,
  ): void {
    const doc = this.#root.ownerDocument;
    const block = doc.createElement('div');
    block.className = 'dc-filter-group-block';
    if (group.id === this.#selected) block.classList.add('dc-selected-group');
    block.style.setProperty('--dc-f-gutter', `${gutterIndent(level)}px`);

    block.append(this.#groupRow(group, level, parent, index));

    const children = doc.createElement('div');
    children.className = 'dc-filter-children';
    group.children.forEach((child, i) => {
      if (child.kind === 'group') {
        this.#renderGroup(child, level + 1, children, group, i);
      } else {
        children.append(this.#conditionRow(child, level + 1, group, i));
      }
    });
    block.append(children);
    into.append(block);
  }

  /**
   * A group's own row.
   *
   * Reads "All of" / "Any of" rather than AND / OR, which is
   * DataCube's wording and is the right one: the row is a heading
   * for the list beneath it, and "All of" scans as one.
   */
  #groupRow(
    group: DraftGroup,
    level: number,
    parent: DraftGroup | null,
    index: number,
  ): HTMLElement {
    const parts: HTMLElement[] = [];
    if (level !== 0) {
      parts.push(this.#controller(group), ...this.#notLabel(group));
    }
    parts.push(
      this.#select(
        ['and', 'or'],
        group.join,
        'dc-filter-join',
        (v) => this.update(group.id, { join: v === 'or' ? 'or' : 'and' }),
        ['All of', 'Any of'],
      ),
    );
    return this.#nodeRow(group, level, parts, parent, index);
  }

  #conditionRow(
    c: DraftCondition,
    level: number,
    parent: DraftGroup,
    index: number,
  ): HTMLElement {
    const type = this.#typeOf(c.column);
    // The operators THIS column's type takes, plus the current one if
    // it somehow is not among them (a filter opened from elsewhere),
    // so the dropdown never silently shows a different operator.
    const offered = type === undefined ? OPERATORS.map((o) => o.op) : operatorsFor(type);
    if (!offered.includes(c.operator)) offered.unshift(c.operator);
    const label = (op: FilterOperator): string =>
      OPERATORS.find((o) => o.op === op)?.label ?? op;
    const names = this.#options.columns.map((x) => x.name);
    if (c.column !== '' && !names.includes(c.column)) names.unshift(c.column);

    const parts: HTMLElement[] = [
      this.#controller(c),
      ...this.#notLabel(c),
      this.#select(names, c.column, 'dc-filter-column', (v) => this.setColumn(c.id, v)),
      this.#select(offered, c.operator, 'dc-filter-op',
        (v) => this.setOperator(c.id, v as FilterOperator), offered.map(label)),
      ...this.#valueEditor(c, type),
    ];
    return this.#nodeRow(c, level, parts, parent, index);
  }

  /**
   * The value input the operator and the column's TYPE call for --
   * upstream's value editors: arithmetic in a number, a date picker
   * with Today / Now, a checkbox, a list of typed entries, a column of
   * the same type.
   */
  #valueEditor(c: DraftCondition, type: string | undefined): HTMLElement[] {
    const doc = this.#root.ownerDocument;
    const kind = operandKind(c.operator);
    if (kind === 'none') return [];
    if (kind === 'column') {
      const sameType = this.#options.columns
        .filter((x) => dataTypeOf(x.type) === dataTypeOf(type))
        .map((x) => x.name);
      return [this.#select(['', ...sameType], c.rightColumn,
        'dc-filter-value dc-filter-rightcolumn',
        (v) => this.update(c.id, { rightColumn: v }),
        ['(choose a column)', ...sameType])];
    }
    if (kind === 'list') return [this.#listEditor(c, type)];

    switch (dataTypeOf(type)) {
      case 'number':
        return [this.#numberInput(c.text, (text) => this.update(c.id, { text }),
          'dc-filter-value dc-filter-number')];
      case 'boolean': {
        const box = doc.createElement('input');
        box.type = 'checkbox';
        box.className = 'dc-filter-value dc-filter-bool';
        box.checked = c.text.trim() === 'true';
        box.addEventListener('click', (e) => e.stopPropagation());
        box.addEventListener('change', () =>
          this.update(c.id, { text: box.checked ? 'true' : 'false' }));
        return [box];
      }
      case 'date':
        return this.#dateEditor(c, type);
      default: {
        const input = this.#textInput(c.text, (text) => this.update(c.id, { text }),
          'dc-filter-value');
        input.placeholder = 'value';
        return [input];
      }
    }
  }

  #textInput(value: string, onChange: (text: string) => void, className: string):
  HTMLInputElement {
    const input = this.#root.ownerDocument.createElement('input');
    input.className = className;
    input.type = 'text';
    input.value = value;
    input.addEventListener('click', (e) => e.stopPropagation());
    // Escape selects everything, as upstream's inputs do.
    input.addEventListener('keydown', (e) => {
      if (e.key === 'Escape') { e.preventDefault(); e.stopPropagation(); input.select(); }
    });
    input.addEventListener('change', () => onChange(input.value));
    return input;
  }

  /**
   * A number that takes ARITHMETIC: `1e6*3` evaluates on Enter or on
   * leaving the field, `#ERR` when it cannot; the arrow keys step by
   * one. Text on purpose, so the expression can be typed at all.
   */
  #numberInput(value: string, onChange: (text: string) => void, className: string):
  HTMLInputElement {
    const input = this.#textInput(value, () => {}, className);
    input.inputMode = 'decimal';
    const commit = (): void => {
      // A field a re-render already took away commits nothing.
      if (!input.isConnected || input.value.trim() === '#ERR') return;
      const n = evaluateArithmetic(input.value);
      if (Number.isFinite(n)) {
        input.value = String(n);
        if (input.value !== value) onChange(input.value);
      } else {
        input.value = '#ERR';
      }
    };
    input.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') { e.preventDefault(); commit(); }
      if (e.key === 'ArrowUp' || e.key === 'ArrowDown') {
        e.preventDefault();
        const n = evaluateArithmetic(input.value);
        const base = Number.isFinite(n) ? n : 0;
        input.value = String(base + (e.key === 'ArrowUp' ? 1 : -1));
        onChange(input.value);
      }
    });
    input.addEventListener('blur', commit);
    return input;
  }

  /**
   * A date: Date, Date Time, Today or Now, and a picker for the two
   * absolute ones -- upstream's date value editor. The picker shows
   * seconds for a date and time.
   */
  #dateEditor(c: DraftCondition, type: string | undefined): HTMLElement[] {
    const doc = this.#root.ownerDocument;
    const text = c.text.trim();
    const mode = text === 'today()' ? 'today'
      : text === 'now()' ? 'now'
      : /T\d/.test(text) ? 'datetime' : 'date';
    const moment = parseTyped(text, 'DateTime');
    const at = moment instanceof Date ? moment : new Date();
    const modeSelect = this.#select(
      ['date', 'datetime', 'today', 'now'], mode, 'dc-filter-date-mode',
      (v) => this.update(c.id, {
        text: v === 'today' ? 'today()'
          : v === 'now' ? 'now()'
          : dateText(at, v === 'datetime'),
      }),
      ['Date', 'Date Time', 'Today', 'Now'],
    );
    modeSelect.title = type === undefined ? '' : `${type} value`;
    if (mode === 'today' || mode === 'now') return [modeSelect];
    const picker = doc.createElement('input');
    picker.className = 'dc-filter-value dc-filter-date';
    picker.type = mode === 'datetime' ? 'datetime-local' : 'date';
    if (mode === 'datetime') picker.step = '1';
    picker.value = dateText(at, mode === 'datetime');
    picker.addEventListener('click', (e) => e.stopPropagation());
    picker.addEventListener('change', () => {
      if (picker.value !== '') this.update(c.id, { text: picker.value });
    });
    return [modeSelect, picker];
  }

  /**
   * A list of typed entries: a summary button ("a, b (+2)") that opens
   * the entries -- each editable and removable -- and a field to add
   * one with Enter. Upstream's list value editor.
   */
  #listEditor(c: DraftCondition, type: string | undefined): HTMLElement {
    const doc = this.#root.ownerDocument;
    const items = c.items ?? parseListText(c.text);
    const wrap = doc.createElement('span');
    wrap.className = 'dc-filter-listwrap';
    const summary = doc.createElement('button');
    summary.type = 'button';
    summary.className = 'dc-filter-value dc-filter-list';
    summary.textContent = items.length === 0
      ? 'Add...'
      : `${items.slice(0, 2).join(', ')}${items.length > 2 ? ` (+${items.length - 2})` : ''}`;
    summary.addEventListener('click', (e) => {
      e.stopPropagation();
      this.#openList = this.#openList === c.id ? null : c.id;
      this.render();
    });
    wrap.append(summary);
    if (this.#openList !== c.id) return wrap;

    const pop = doc.createElement('div');
    pop.className = 'dc-filter-listpop';
    pop.addEventListener('click', (e) => e.stopPropagation());
    const set = (next: readonly string[]): void => {
      this.update(c.id, { items: next });
    };
    if (items.length === 0) {
      const none = doc.createElement('div');
      none.className = 'dc-filter-listnone';
      none.textContent = 'No values added.';
      pop.append(none);
    }
    items.forEach((item, i) => {
      const row = doc.createElement('div');
      row.className = 'dc-filter-listitem';
      const edit = (text: string): void =>
        set(items.map((x, j) => (j === i ? text : x)));
      const input = dataTypeOf(type) === 'number'
        ? this.#numberInput(item, edit, 'dc-filter-listinput')
        : this.#textInput(item, edit, 'dc-filter-listinput');
      if (dataTypeOf(type) === 'date') input.type = 'date';
      const del = this.#button('×', () => set(items.filter((_x, j) => j !== i)),
        'dc-filter-listdel', 'Remove');
      row.append(input, del);
      pop.append(row);
    });
    const add = doc.createElement('input');
    add.className = 'dc-filter-listadd';
    add.placeholder = 'Add value';
    if (dataTypeOf(type) === 'date') add.type = 'date';
    const push = (): void => {
      const v = add.value.trim();
      if (v === '') return;
      set([...items, dataTypeOf(type) === 'number'
        ? String(evaluateArithmetic(v)) : v]);
    };
    add.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') { e.preventDefault(); push(); }
      if (e.key === 'Escape') {
        e.preventDefault(); e.stopPropagation();
        this.#openList = null; this.render();
      }
    });
    const plus = this.#button('+', push, 'dc-filter-listplus', 'Add');
    const done = this.#button('Done', () => { this.#openList = null; this.render(); },
      'dc-button dc-filter-listdone');
    const addRow = doc.createElement('div');
    addRow.className = 'dc-filter-listaddrow';
    addRow.append(add, plus);
    pop.append(addRow, done);
    wrap.append(pop);
    return wrap;
  }

  /**
   * One row: the connector, then the node's own controls.
   *
   * The connector is not decoration. The short horizontal stub joins
   * the row to its parent's vertical gutter, and from the second
   * child onwards it carries the parent's operator as a word -- so
   * the reader sees `and` / `or` BETWEEN the things being combined,
   * which is where the meaning is, rather than in a dropdown at the
   * bottom of a flat list.
   */
  #nodeRow(
    node: DraftNode,
    level: number,
    parts: readonly HTMLElement[],
    parent: DraftGroup | null,
    index: number,
  ): HTMLElement {
    const doc = this.#root.ownerDocument;
    const row = doc.createElement('div');
    row.className =
      node.kind === 'group' ? 'dc-filter-row dc-filter-group' : 'dc-filter-row';
    row.dataset['id'] = node.id;
    row.style.setProperty('--dc-filter-level', String(level));
    row.style.setProperty('--dc-f-indent', `${rowIndent(level)}px`);
    if (node.id === this.#selected) row.classList.add('dc-selected');
    row.addEventListener('click', (event) => {
      event.stopPropagation();
      this.select(node.id);
    });

    // The lead is a FIXED-WIDTH box, and the operator word lives
    // inside it. In flow, the word pushes everything after it, so a
    // row carrying `or` sits further right than its own siblings and
    // the columns stop lining up. Fixing the box and letting its
    // contents spill left into the indent keeps every sibling's
    // controls on the same x, which is what DataCube does.
    const lead = doc.createElement('div');
    lead.className = 'dc-filter-lead';
    if (parent) {
      const connector = doc.createElement('span');
      connector.className = 'dc-filter-connector';
      connector.setAttribute('aria-hidden', 'true');
      lead.append(connector);
      if (index > 0) {
        lead.classList.add('dc-has-word');
        const word = doc.createElement('span');
        word.className = 'dc-filter-joinword';
        word.textContent = parent.join;
        lead.append(word);
      }
    }
    row.append(lead);
    parts.forEach((p) => row.appendChild(p));
    return row;
  }

  /**
   * The four-button controller DataCube puts on every node.
   *
   * Insert-after, remove, wrap-in-a-group, and NOT. The third is the
   * one that matters most: without it there is no way to turn a flat
   * `A AND B AND C` into `A AND (B OR C)` except by deleting and
   * retyping, which is why a flat list with a join dropdown is not
   * the same product.
   */
  #controller(node: DraftNode): HTMLElement {
    const doc = this.#root.ownerDocument;
    const bar = doc.createElement('div');
    bar.className = 'dc-filter-controller';
    bar.append(
      this.#button(
        '+',
        () => this.insertAfter(node.id),
        'dc-filter-ctl',
        'Insert a new column filter, just after this filter',
      ),
      this.#button(
        '−',
        () => this.remove(node.id),
        'dc-filter-ctl',
        'Remove this filter',
      ),
      this.#button(
        '( )',
        () => this.layer(node.id),
        'dc-filter-ctl',
        'Put this filter in its own sub-group (and combine it with other filters)',
      ),
    );
    const not = this.#button(
      '!',
      () => this.update(node.id, { not: !node.not }),
      node.not ? 'dc-filter-ctl dc-filter-not dc-filter-not-on'
               : 'dc-filter-ctl dc-filter-not',
      node.not
        ? 'Turn off the NOT operator on this filter to select only what matches'
        : 'Turn on the NOT operator on this filter to select all but what matches',
    );
    not.setAttribute('aria-pressed', String(Boolean(node.not)));
    bar.append(not);
    return bar;
  }

  /** The standing NOT badge, shown beside a negated node. */
  #notLabel(node: DraftNode): HTMLElement[] {
    if (!node.not) return [];
    const el = this.#root.ownerDocument.createElement('span');
    el.className = 'dc-filter-notlabel';
    el.textContent = 'NOT';
    el.title = 'Filter is inverted: select all but what matches.';
    return [el];
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
    if (ariaLabel) {
      b.setAttribute('aria-label', ariaLabel);
      b.title = ariaLabel;
    }
    b.addEventListener('click', (event) => {
      event.stopPropagation();
      onClick();
    });
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
    el.addEventListener('click', (event) => event.stopPropagation());
    el.addEventListener('change', () => onChange(el.value));
    return el;
  }
}

/** A node by id, anywhere in the tree. */
function findNode(node: DraftNode, id: string): DraftNode | undefined {
  if (node.id === id) return node;
  if (node.kind !== 'group') return undefined;
  for (const child of node.children) {
    const hit = findNode(child, id);
    if (hit) return hit;
  }
  return undefined;
}

/** A legacy comma-separated list operand, as entries. */
function parseListText(text: string): string[] {
  return text.split(',').map((x) => x.trim()).filter((x) => x !== '');
}
