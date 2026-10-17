// A FILTER'S OPERATORS: each one's operand -- none, one value, a list, another column -- with DataCube's own label for
// it, and which a column of a type takes (offers.ts); and the filter editor's draft tree and its conversion to the
// cube's filter (toFilterNode: a value typed by its column, a group of one that one, an empty group none). Moved out of
// the filter editor (ui/filter-editor.ts, which says them as before) so code with no screen builds a filter by them too:
// Python's filters, headless (headless/grid.ts gridFilter; docs/PYTHON_ASKS_DATACUBE_2026_10_15.md, item 6).

import { takesOperator } from './offers.ts';
import type { FilterCondition, FilterNode, FilterOperator, FilterValue } from './snapshot.ts';
import { hasTimeOfDay, isBoolean, isNumeric, isTemporal, isTimeOfDay, isVariant } from '../../engine-client/src/types.ts';

/** What kind of value input an operator needs. */
export type OperandKind = 'none' | 'single' | 'list' | 'column';

/**
 * Operator arity, with DataCube's own label for each, so a user
 * moving between the two products reads the same words.
 */
export const OPERATORS = [
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
] as const satisfies ReadonlyArray<{
  readonly op: FilterOperator; readonly label: string; readonly operand: OperandKind;
}>;

// every operator is in the table: one added to FilterOperator and not here, or here and not there, does not compile --
// so an operator's operand is the table's, never a default
type Same<A, B> = [A] extends [B] ? ([B] extends [A] ? true : false) : false;
const everyOperatorListed: Same<(typeof OPERATORS)[number]['op'], FilterOperator> = true;
void everyOperatorListed;

const OPERAND_BY_OP = Object.fromEntries(OPERATORS.map((o) => [o.op, o.operand])) as
  Record<FilterOperator, OperandKind>;

export function operandKind(op: FilterOperator): OperandKind {
  return OPERAND_BY_OP[op];
}

/**
 * The operators a column of this type accepts, in the table's order: the compiler's
 * answer (`takesOperator`, src/offers.ts). "Starts with" on a number or "<" on a JSON
 * document is refused by the compiler, and offering it is how a user builds a filter the
 * engine refuses.
 */
export function operatorsFor(type: string | undefined): FilterOperator[] {
  if (type === undefined) return OPERATORS.map((o) => o.op);
  return OPERATORS.filter((o) => takesOperator(o.op, type)).map((o) => o.op);
}

/** Whether a column of this type takes the operator (operatorsFor's answer for one operator; an untyped column, any). */
export function isCompatible(op: FilterOperator, type: string | undefined): boolean {
  return type === undefined || takesOperator(op, type);
}

/**
 * Upstream's DataCubeColumnDataType, plus the time of day and
 * semi-structured data. Upstream has no Variant bucket: a JSON column
 * there falls to text, which offers `contains` on a value that is not
 * a string.
 */
export type DataType = 'text' | 'number' | 'date' | 'time' | 'boolean'
  | 'variant';

export function dataTypeOf(type: string | undefined): DataType {
  // by the compiler type's FAMILY (generated from legend-lite's lattice), never its name
  if (isVariant(type)) return 'variant';
  if (isBoolean(type)) return 'boolean';
  if (isTimeOfDay(type)) return 'time';
  if (isTemporal(type)) return 'date';
  if (isNumeric(type)) return 'number';
  return 'text';
}

/** Whether values of this type carry a time of day. */
function hasTime(type: string | undefined): boolean {
  return hasTimeOfDay(type);
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
/** Plain digits as a person types a number: kept exact rather than read through a double. */
const EXACT_NUMBER_INPUT = /^-?\d+(\.\d+)?$/;

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
 * Typed, never guessed: a number column's value is a number, a text
 * column's is text even when it looks numeric (account codes). A value
 * the person typed as plain digits stays those EXACT digits (a decimal's
 * `12.30`, an integer past 2^53); arithmetic they typed is evaluated. A
 * date or a time is the text as typed, never a JavaScript Date (a Date is
 * in the browser's zone, and a filter is not): the COMPILER judges it when
 * the filter is applied, as it judges a calculated column's expression.
 */
export function parseTyped(text: string, type: string | undefined):
FilterValue | null {
  const t = text.trim();
  switch (dataTypeOf(type)) {
    case 'number': {
      if (EXACT_NUMBER_INPUT.test(t)) return t;
      const n = evaluateArithmetic(t);
      return Number.isFinite(n) ? n : null;
    }
    case 'boolean':
      return t === 'true' ? true : t === 'false' ? false : null;
    case 'date': {
      if (t === 'today()') return { relative: 'today' };
      if (t === 'now()') return { relative: 'now' };
      return t === '' ? null : t;
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
    // Its operators take no value (`isEmpty`, `isNotEmpty`).
    case 'variant':
      return null;
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
  /**
   * The value the cube HAS, for a condition read from its filter, with what it was shown as.
   * While the condition is untouched (same column, text and items) this value is published
   * exactly -- never its text read back, which lost '' and quoted values (P2-150).
   */
  readonly stored?: {
    readonly column: string;
    readonly text: string;
    readonly items?: readonly string[] | undefined;
    readonly value: FilterValue | readonly FilterValue[];
  } | undefined;
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
/** A fresh id for a draft node (the editor's rows are keyed by it). */
export function nextId(): string {
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

/** Split a list operand, each entry typed by the column. Empty entries are dropped, not sent as ''. */
export function parseList(text: string, type: string | undefined): FilterValue[] {
  return text
    .split(',')
    .map((s) => s.trim())
    .filter((s) => s !== '')
    .flatMap((s) => {
      const v = typedOrText(s, type);
      return v === null ? [] : [v];
    });
}

/**
 * A value by its column's type; with no type yet (the compiler has not typed the
 * column), the text as typed -- the query refuses it, rather than this guessing.
 */
function typedOrText(text: string, type: string | undefined): FilterValue | null {
  if (type !== undefined) return parseTyped(text, type);
  const t = text.trim();
  return t === '' ? null : t;
}

/** Column types, by name, for the conversion below. Absent: untyped. */
export type TypeOf = (column: string) => string | undefined;

/** The stored value, when the condition is as it was read and the operator still takes it. */
function untouched(d: DraftCondition, kind: ReturnType<typeof operandKind>): FilterValue | readonly FilterValue[] | undefined {
  const st = d.stored;
  if (!st || st.column !== d.column || st.text !== d.text) return undefined;
  const sameItems = (st.items ?? []).length === (d.items ?? []).length
    && (st.items ?? []).every((x, i) => x === d.items?.[i]);
  if (!sameItems) return undefined;
  const isList = Array.isArray(st.value);
  if ((kind === 'list') !== isList || (kind !== 'list' && kind !== 'single')) return undefined;
  return st.value;
}

function conditionOf(d: DraftCondition, typeOf?: TypeOf):
FilterCondition | null {
  if (!d.column) return null;
  const kind = operandKind(d.operator);
  const kept = untouched(d, kind);
  if (kept !== undefined) return { kind: 'condition', column: d.column, operator: d.operator, value: kept } as FilterCondition;
  const type = typeOf?.(d.column);
  const one = (text: string): FilterValue | null => typedOrText(text, type);

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
      : parseList(d.text, type);
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
