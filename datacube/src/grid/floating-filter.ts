// The floating filter row: a filter box under each column header.
//
// DataCube does NOT have this. Grepping legend-data-cube for
// `floatingFilter` returns nothing; its filters live entirely in the
// filter editor and the column menu. This is ag-Grid's idiom rather
// than DataCube's, added because the fastest filter is the one you
// can type into without opening anything, and because a cube whose
// columns are already on screen is exactly where that pays.
//
// The design decision that matters: a floating filter writes into
// the SAME FilterNode tree the editor edits, as a top-level AND
// condition, rather than into a second filter combined afterwards.
// Two filters would drift -- type in the box, open the editor, and
// the condition is missing; edit it there, and the box still shows
// the old text. One tree means the box reads back whatever the
// editor wrote, when it is simple enough to show, and is blank when
// it is not. A condition too complex for a box is left ALONE rather
// than flattened into one, because silently rewriting a user's
// nested filter to fit a text input is worse than an empty box.
//
// The text is parsed for a comparator prefix on numeric columns, so
// `>100` and `<=0` mean what they look like. A string column gets
// `contains`, which is the only default that is useful more often
// than it is wrong.

import type {
  FilterCondition,
  FilterNode,
  FilterOperator,
  FilterValue,
} from '../snapshot.ts';

/** Comparators a floating filter box understands, longest first. */
const COMPARATORS: readonly (readonly [string, FilterOperator])[] = [
  ['>=', 'greaterThanEqual'],
  ['<=', 'lessThanEqual'],
  ['!=', 'notEqual'],
  ['>', 'greaterThan'],
  ['<', 'lessThan'],
  ['=', 'equal'],
];

/** Operators a box can render back into text. */
const RENDERABLE = new Map<FilterOperator, string>([
  ['greaterThanEqual', '>='],
  ['lessThanEqual', '<='],
  ['notEqual', '!='],
  ['greaterThan', '>'],
  ['lessThan', '<'],
  ['equal', '='],
  ['contains', ''],
]);

export function isNumericType(type: string): boolean {
  return (
    type === 'Integer' ||
    type === 'Float' ||
    type === 'Number' ||
    type === 'Decimal'
  );
}

/**
 * Turn what a user typed into a condition, or nothing.
 *
 * Blank is nothing rather than an empty match: a half-typed box is
 * the normal state of a UI and should narrow nothing, which is the
 * rule the filter editor already keeps.
 */
export function parseFloating(
  column: string,
  type: string,
  text: string,
): FilterCondition | null {
  const raw = text.trim();
  if (raw === '') return null;

  if (!isNumericType(type)) {
    return {
      kind: 'condition',
      column,
      operator: 'containsCaseInsensitive',
      value: raw,
    };
  }

  for (const [prefix, operator] of COMPARATORS) {
    if (raw.startsWith(prefix)) {
      const n = Number(raw.slice(prefix.length).trim());
      if (!Number.isFinite(n)) return null;
      return { kind: 'condition', column, operator, value: n };
    }
  }
  const n = Number(raw);
  // A numeric column given non-numeric text matches nothing useful,
  // and guessing `contains` on a number is how a filter silently
  // stops meaning what it says. Nothing is the honest answer.
  if (!Number.isFinite(n)) return null;
  return { kind: 'condition', column, operator: 'equal', value: n };
}

/** Render a condition back into box text, or null if it cannot be. */
export function renderFloating(
  condition: FilterCondition,
  type: string,
): string | null {
  if (!isNumericType(type)) {
    if (
      condition.operator !== 'contains' &&
      condition.operator !== 'containsCaseInsensitive'
    ) {
      return null;
    }
    return String(condition.value ?? '');
  }
  const prefix = RENDERABLE.get(condition.operator);
  if (prefix === undefined || condition.operator === 'contains') return null;
  return `${prefix}${String(condition.value ?? '')}`;
}

function isCondition(node: FilterNode): node is FilterCondition {
  return node.kind === 'condition';
}

/**
 * Whether a column appears anywhere inside a node.
 *
 * Recursive because `not` is a wrapping node and a group nests: a
 * condition on this column buried two levels down still means the
 * column is filtered, and a box that ignored it would show blank on
 * a filtered column.
 */
export function mentions(node: FilterNode, column: string): boolean {
  switch (node.kind) {
    case 'condition':
      return node.column === column || node.rightColumn === column;
    case 'not':
      return mentions(node.child, column);
    default:
      return node.children.some((c) => mentions(c, column));
  }
}

/** The top-level AND conjuncts of a filter, as a flat list. */
export function conjuncts(filter: FilterNode | undefined): FilterNode[] {
  if (!filter) return [];
  if (filter.kind === 'and') return [...filter.children];
  return [filter];
}

/** Rebuild a filter from its conjuncts. */
export function fromConjuncts(
  parts: readonly FilterNode[],
): FilterNode | undefined {
  if (parts.length === 0) return undefined;
  if (parts.length === 1) return parts[0] as FilterNode;
  return { kind: 'and', children: [...parts] };
}

/**
 * What a column's box should show, given the whole filter.
 *
 * Only a top-level condition on exactly that column counts. A
 * condition buried inside an OR is part of an expression the box
 * cannot represent, and showing its text would invite the user to
 * edit it and silently destroy the rest.
 */
export function floatingText(
  filter: FilterNode | undefined,
  column: string,
  type: string,
): string {
  const own = conjuncts(filter).filter((n) => mentions(n, column));
  if (own.length !== 1) return '';
  const only = own[0] as FilterNode;
  if (!isCondition(only) || only.column !== column) return '';
  return renderFloating(only, type) ?? '';
}

/**
 * Whether a column's filter is beyond what a box can show.
 *
 * The box is then disabled rather than blank-and-editable, because a
 * blank box on a filtered column reads as "not filtered".
 */
export function isComplex(
  filter: FilterNode | undefined,
  column: string,
  type: string,
): boolean {
  const own = conjuncts(filter).filter((n) => mentions(n, column));
  if (own.length === 0) return false;
  if (own.length > 1) return true;
  const only = own[0] as FilterNode;
  if (!isCondition(only) || only.column !== column) return true;
  return renderFloating(only, type) === null;
}

/**
 * Replace a column's top-level condition, keeping everything else.
 *
 * Returns the SAME filter when nothing changed, so a box that emits
 * an identical condition on every keystroke costs no query.
 */
export function withFloating(
  filter: FilterNode | undefined,
  column: string,
  condition: FilterCondition | null,
): FilterNode | undefined {
  const parts = conjuncts(filter);
  const kept = parts.filter(
    (n) => !(isCondition(n) && n.column === column),
  );
  const next = condition ? [...kept, condition] : kept;
  if (
    next.length === parts.length &&
    next.every((n, i) => JSON.stringify(n) === JSON.stringify(parts[i]))
  ) {
    return filter;
  }
  return fromConjuncts(next);
}

/**
 * Whether a leaf column can take a filter box.
 *
 * Only a leaf that IS a source column can. The tree column holds a
 * different dimension at every level, so no single box could filter
 * it; a pivoted leaf is named for a path (`2021__|__notional`) the
 * engine has never heard of, and filtering the measure's source
 * column instead would silently mean something else -- narrowing the
 * rows that feed the aggregate rather than the aggregate itself.
 *
 * So a fully pivoted cube has no filter boxes, and that is correct
 * rather than a gap: the grid drops the row entirely instead of
 * showing an empty strip under the header.
 */
export function canFloat(
  leaf: { readonly name: string; readonly path: readonly string[] },
  treeColumn: string,
): boolean {
  return leaf.name !== treeColumn && leaf.path.length <= 1;
}

export interface FloatingFilterColumn {
  readonly name: string;
  readonly type: string;
  /** False renders an inert placeholder rather than a box. */
  readonly filterable: boolean;
}

export interface FloatingFilterOptions {
  readonly onChange: (filter: FilterNode | undefined) => void;
  /**
   * Milliseconds to wait after the last keystroke.
   *
   * A filter change re-runs the whole cube, so emitting per keystroke
   * would issue a query per character and show the answer to a prefix
   * nobody asked about.
   */
  readonly debounceMs?: number;
  readonly setTimeoutFn?: (fn: () => void, ms: number) => unknown;
  readonly clearTimeoutFn?: (handle: unknown) => void;
}

const DEFAULT_DEBOUNCE_MS = 300;

/** The row of boxes. Owns its DOM; the grid places it in the header. */
export class FloatingFilterRow {
  readonly #doc: Document;
  readonly #options: FloatingFilterOptions;
  #filter: FilterNode | undefined;
  #pending: unknown = null;

  constructor(doc: Document, options: FloatingFilterOptions) {
    this.#doc = doc;
    this.#options = options;
  }

  setFilter(filter: FilterNode | undefined): void {
    this.#filter = filter;
  }

  /** One cell, for one leaf column. */
  cell(column: FloatingFilterColumn): HTMLElement {
    const doc = this.#doc;
    const cell = doc.createElement('div');
    cell.className = 'dc-floating-cell';
    if (!column.filterable) return cell;

    // The box carries its column's NAME, because it no longer sits
    // under that column's header: over a pivot there is no such
    // header to sit under, so the label has to be on the box.
    const label = doc.createElement('span');
    label.className = 'dc-floating-label';
    label.textContent = column.name;
    cell.append(label);

    const input = doc.createElement('input');
    input.type = 'text';
    input.className = 'dc-floating-input';
    input.dataset['column'] = column.name;
    input.value = floatingText(this.#filter, column.name, column.type);
    input.placeholder = isNumericType(column.type) ? '= value' : 'contains';
    input.setAttribute('aria-label', `Filter ${column.name}`);

    if (isComplex(this.#filter, column.name, column.type)) {
      // A blank box on a filtered column reads as "not filtered", so
      // say so instead.
      input.disabled = true;
      input.placeholder = 'filtered';
      input.title =
        'This column has a filter the box cannot show. Open the filter editor.';
      cell.classList.add('dc-floating-complex');
      cell.append(input);
      return cell;
    }

    input.addEventListener('input', () => this.#queue(column, input.value));
    input.addEventListener('keydown', (event) => {
      if (event.key === 'Enter') {
        event.preventDefault();
        this.#commit(column, input.value);
      } else if (event.key === 'Escape') {
        event.stopPropagation();
        input.value = '';
        this.#commit(column, '');
      }
    });
    cell.append(input);
    return cell;
  }

  #queue(column: FloatingFilterColumn, text: string): void {
    const set = this.#options.setTimeoutFn ?? setTimeout;
    const clear = this.#options.clearTimeoutFn ?? clearTimeout;
    if (this.#pending !== null) clear(this.#pending as never);
    this.#pending = set(
      () => {
        this.#pending = null;
        this.#commit(column, text);
      },
      this.#options.debounceMs ?? DEFAULT_DEBOUNCE_MS,
    );
  }

  #commit(column: FloatingFilterColumn, text: string): void {
    const clear = this.#options.clearTimeoutFn ?? clearTimeout;
    if (this.#pending !== null) {
      clear(this.#pending as never);
      this.#pending = null;
    }
    const condition = parseFloating(column.name, column.type, text);
    const next = withFloating(this.#filter, column.name, condition);
    if (next === this.#filter) return;
    this.#filter = next;
    this.#options.onChange(next);
  }
}

/** Re-exported for the grid's own typing. */
export type { FilterValue };
