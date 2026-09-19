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
 * The tree column's single box: a quick filter over the row
 * dimensions.
 *
 * The tree column holds a different dimension at every level, so
 * there is no ONE column for a box under it to filter -- which is
 * why the first attempt spread the boxes into a free-floating strip
 * and they stopped lining up with anything. One box that matches
 * ANY of the row dimensions is what a single box under a tree
 * column means to a reader, and it restores per-column alignment
 * for everything else.
 *
 * Built as an OR of case-insensitive contains, one per row
 * dimension. With a single row dimension it collapses to that one
 * condition, which is then indistinguishable from an ordinary
 * column filter -- and correctly so.
 */
export function treeFilterNode(
  rows: readonly string[],
  text: string,
): FilterNode | null {
  const raw = text.trim();
  if (raw === '' || rows.length === 0) return null;
  const children: FilterNode[] = rows.map((column) => ({
    kind: 'condition',
    column,
    operator: 'containsCaseInsensitive',
    value: raw,
  }));
  return children.length === 1
    ? (children[0] as FilterNode)
    : { kind: 'or', children };
}

/** Whether a node is exactly the tree quick filter over these rows. */
function isTreeFilter(node: FilterNode, rows: readonly string[]): boolean {
  const text = treeFilterText(node, rows);
  return text !== null;
}

/** The text a tree filter node carries, or null if it is not one. */
function treeFilterText(
  node: FilterNode,
  rows: readonly string[],
): string | null {
  const parts =
    node.kind === 'or' ? node.children : node.kind === 'condition' ? [node] : [];
  if (parts.length !== rows.length || parts.length === 0) return null;
  const columns = new Set<string>();
  let value: string | null = null;
  for (const part of parts) {
    if (part.kind !== 'condition') return null;
    if (part.operator !== 'containsCaseInsensitive') return null;
    if (typeof part.value !== 'string') return null;
    if (value === null) value = part.value;
    else if (value !== part.value) return null;
    columns.add(part.column);
  }
  if (columns.size !== rows.length) return null;
  if (!rows.every((r) => columns.has(r))) return null;
  return value;
}

/** What the tree box should show, given the whole filter. */
export function readTreeFilter(
  filter: FilterNode | undefined,
  rows: readonly string[],
): string {
  const own = conjuncts(filter).filter((n) =>
    rows.some((r) => mentions(n, r)),
  );
  if (own.length !== 1) return '';
  return treeFilterText(own[0] as FilterNode, rows) ?? '';
}

/** Whether the rows carry a filter the tree box cannot show. */
export function isTreeComplex(
  filter: FilterNode | undefined,
  rows: readonly string[],
): boolean {
  const own = conjuncts(filter).filter((n) =>
    rows.some((r) => mentions(n, r)),
  );
  if (own.length === 0) return false;
  if (own.length > 1) return true;
  return !isTreeFilter(own[0] as FilterNode, rows);
}

/** Replace the tree quick filter, keeping everything else. */
export function withTreeFilter(
  filter: FilterNode | undefined,
  rows: readonly string[],
  text: string,
): FilterNode | undefined {
  const parts = conjuncts(filter);
  const kept = parts.filter((n) => !isTreeFilter(n, rows));
  const next = treeFilterNode(rows, text);
  const combined = next ? [...kept, next] : kept;
  if (
    combined.length === parts.length &&
    combined.every((n, i) => JSON.stringify(n) === JSON.stringify(parts[i]))
  ) {
    return filter;
  }
  return fromConjuncts(combined);
}

export interface FloatingFilterColumn {
  readonly name: string;
  readonly type: string;
  /** False renders an inert placeholder rather than a box. */
  readonly filterable: boolean;
  /**
   * 'tree' means the single box under the tree column, which
   * matches any of `rows`. 'column' is an ordinary one.
   */
  readonly mode?: 'column' | 'tree';
  /** The row dimensions a tree box filters. */
  readonly rows?: readonly string[];
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
  /**
   * Pending commits, PER COLUMN.
   *
   * One shared handle looked like an obvious simplification and was
   * a bug: typing in a second box cancelled the first box's pending
   * commit, so a filter the user had typed and could still see in
   * the box was silently never applied. Caught by a screenshot, not
   * by a test -- the two boxes have to be used within the debounce
   * window for it to show.
   */
  readonly #pending = new Map<string, unknown>();

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

    const tree = column.mode === 'tree';
    const rows = column.rows ?? [];

    const input = doc.createElement('input');
    input.type = 'text';
    input.className = 'dc-floating-input';
    input.dataset['column'] = column.name;
    input.value = tree
      ? readTreeFilter(this.#filter, rows)
      : floatingText(this.#filter, column.name, column.type);
    input.placeholder = tree
      ? 'filter rows...'
      : isNumericType(column.type)
        ? '= value'
        : 'contains';
    input.setAttribute(
      'aria-label',
      tree ? `Filter rows by ${rows.join(', ')}` : `Filter ${column.name}`,
    );
    if (tree) {
      input.title = `Matches any of: ${rows.join(', ')}`;
    }

    if (
      tree
        ? isTreeComplex(this.#filter, rows)
        : isComplex(this.#filter, column.name, column.type)
    ) {
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
    this.#cancel(column.name);
    this.#pending.set(
      column.name,
      set(() => {
        this.#pending.delete(column.name);
        this.#commit(column, text);
      }, this.#options.debounceMs ?? DEFAULT_DEBOUNCE_MS),
    );
  }

  #cancel(column: string): void {
    const clear = this.#options.clearTimeoutFn ?? clearTimeout;
    const handle = this.#pending.get(column);
    if (handle !== undefined) {
      clear(handle as never);
      this.#pending.delete(column);
    }
  }

  #commit(column: FloatingFilterColumn, text: string): void {
    this.#cancel(column.name);
    const next =
      column.mode === 'tree'
        ? withTreeFilter(this.#filter, column.rows ?? [], text)
        : withFloating(
            this.#filter,
            column.name,
            parseFloating(column.name, column.type, text),
          );
    if (next === this.#filter) return;
    this.#filter = next;
    this.#options.onChange(next);
  }
}

/** Re-exported for the grid's own typing. */
export type { FilterValue };
