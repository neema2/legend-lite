// The row-group tree: which groups exist, which are open, and which
// rows are therefore on screen.
//
// Kept pure and separate from both the grid and the engine, because
// this is where pivot tables usually rot. Two decisions matter:
//
//  - Expansion is keyed by group IDENTITY, never by row position. A
//    refresh, a re-sort or a filter change renumbers every row, so
//    position-keyed state silently opens the wrong groups. Identity
//    keying is why "state survives refresh" is a property rather than
//    a hope, and it is the documented cause of a whole class of
//    complaints in other products.
//  - A subtotal is the SAME measure expression with a grouping column
//    dropped -- never a second aggregation pass. That makes "the
//    subtotal disagrees with the detail", the single most-reported
//    pivot defect, structurally impossible rather than merely tested
//    against.

/**
 * A group's key: its cell's EXACT text as the database gave it (values.ts:
 * a day, a timestamp to the microsecond, a decimal's digits, an integer
 * past 2^53), or a real null for the group of rows where the key is null.
 * Read back into a query by the column's compiler type (query.ts), never
 * by guessing at the text.
 */
export type GroupKey = string | null;

/** Values of the row dimensions from the root down. Empty = grand total. */
export type RowPath = readonly GroupKey[];

/**
 * The last segment of a DETAIL row's path: its position under its
 * group. A detail row has no key of its own -- it is a source row, not
 * a group -- so its identity is its parent and its place. A control
 * character, so no value a database returns can forge one.
 */
export const DETAIL_ROW = '\u0001';

/** Whether a path names a detail row rather than a group. */
export function isDetailPath(path: RowPath): boolean {
  const last = path[path.length - 1];
  return typeof last === 'string' && last.startsWith(DETAIL_ROW);
}

/**
 * Stable key for a path: its JSON, so a null key and the text 'null' are
 * different groups and no value, whatever it holds, can forge another's key.
 */
export function pathKey(path: RowPath): string {
  return JSON.stringify(path);
}

/** Inverse of {@link pathKey}, so a saved view can restore expansion. */
export function parsePathKey(key: string): RowPath {
  const path: unknown = JSON.parse(key);
  if (!Array.isArray(path) || !path.every((k) => k === null || typeof k === 'string')) {
    throw new Error(`not a path key: ${key}`);
  }
  return path as RowPath;
}

/** Whether `path` lies beneath `ancestor` (strictly). */
function isBeneath(path: RowPath, ancestor: RowPath): boolean {
  return path.length > ancestor.length && ancestor.every((k, i) => path[i] === k);
}

export interface TreeRow {
  readonly path: RowPath;
  /** Logical depth: the number of path segments. 0 is the grand total. */
  readonly level: number;
  /**
   * Presentation depth, 1-based, for aria-level and indentation.
   *
   * When totals are shown the grand total is the ROOT, so it takes
   * depth 1 and everything below shifts down -- otherwise a screen
   * reader announces the total and its own children as siblings.
   * aria-level cannot be 0, so clamping level 0 to 1 is not an option.
   */
  readonly depth: number;
  /** True when this row can be expanded (it has a level beneath it). */
  readonly isGroup: boolean;
  /** Only meaningful when isGroup. */
  readonly expanded: boolean;
  /** A subtotal or grand-total row rather than a leaf. */
  readonly isTotal: boolean;
  /** A source row under the deepest group: no label, no expander. */
  readonly isDetail?: boolean;
}

/** One fetch the tree needs: the children of `parent` at `level`. */
export interface LevelRequest {
  /** Depth of the rows wanted; 0 is the grand total. */
  readonly level: number;
  /** Parent group whose children are wanted. Empty at level 1. */
  readonly parent: RowPath;
}

export function requestKey(r: LevelRequest): string {
  return `${r.level}:${pathKey(r.parent)}`;
}

/**
 * Expansion state.
 *
 * Immutable: every change returns a new instance, so it can live in a
 * snapshot beside everything else and be compared by reference.
 */
export class TreeState {
  readonly #open: ReadonlySet<string>;
  readonly #showTotals: boolean;
  /**
   * Groups at this depth or shallower are open unless the user closed
   * them: General Properties > "Initially expand to level", upstream's
   * `isServerSideGroupOpenByDefault` (a group whose level is within
   * `initialExpandLevel` opens when it loads). 0 opens nothing.
   */
  readonly #expandTo: number;
  /** Groups the user closed that `#expandTo` would otherwise open. */
  readonly #closed: ReadonlySet<string>;

  private constructor(
    open: ReadonlySet<string>,
    showTotals: boolean,
    expandTo = 0,
    closed: ReadonlySet<string> = new Set(),
  ) {
    this.#open = open;
    this.#showTotals = showTotals;
    this.#expandTo = expandTo;
    this.#closed = closed;
  }

  /**
   * A fresh tree, with NO grand total.
   *
   * Matches their showRootAggregation default. The total is an
   * extra level-0 query on every refresh, and it changes what the
   * top of the grid means, so it is the user's to switch on.
   */
  static empty(showTotals = false): TreeState {
    return new TreeState(new Set(), showTotals);
  }

  static fromPaths(paths: readonly RowPath[], showTotals = false): TreeState {
    return new TreeState(new Set(paths.map(pathKey)), showTotals);
  }

  /** A tree as a saved cube keeps it (cube-document.ts treeDocument): its open groups, its grand total, its expand
   *  level and the groups closed under that level. */
  static saved(open: readonly RowPath[], showTotals: boolean, expandTo: number, closed: readonly RowPath[]): TreeState {
    return new TreeState(new Set(open.map(pathKey)), showTotals, Math.max(0, expandTo), new Set(closed.map(pathKey)));
  }

  get showTotals(): boolean {
    return this.#showTotals;
  }

  get expandTo(): number {
    return this.#expandTo;
  }

  /** Open paths, for persisting a saved view. */
  get openPaths(): string[] {
    return [...this.#open];
  }

  /**
   * Whether a group may be open in a grid of `depth` row groups: one opened by hand, or one its expand level opens --
   * which never opens the deepest (isOpen), so a grid of one row group opened to a level has none (the S7 confirmation
   * audit's G2-4: Collapse All offered there changed nothing a person sees).
   */
  opensAny(depth: number): boolean {
    return this.#open.size > 0 || (this.#expandTo > 0 && depth > 1);
  }

  /** Groups the user closed that the expand level would otherwise open. */
  get closedPaths(): string[] {
    return [...this.#closed];
  }

  /**
   * What decides which groups show open, as one comparable string: the open and closed
   * paths (sorted), the expand level and the grand total. History and "did this change"
   * compare trees by it -- the open paths alone missed a group closed from the expand level
   * (P2-109).
   */
  get key(): string {
    return JSON.stringify([
      [...this.#open].sort(), [...this.#closed].sort(), this.#expandTo, this.#showTotals,
    ]);
  }

  isOpen(path: RowPath, depth = Infinity): boolean {
    const key = pathKey(path);
    if (this.#open.has(key)) return true;
    // The expand level never opens the DEEPEST groups: their children
    // are detail rows, one query per group, and a setting that fired
    // hundreds of them on every load would be a trap. The user opens
    // those one at a time.
    return path.length > 0 && path.length <= this.#expandTo
      && path.length < depth && !this.#closed.has(key);
  }

  toggle(path: RowPath): TreeState {
    return this.isOpen(path) ? this.collapse(path) : this.expand(path);
  }

  /**
   * The group OPEN or CLOSED, as asked -- this same tree when it already is. A request, not a
   * flip: a double-click, or a key repeated while a query ran, toggled twice and undid itself
   * (P2-127).
   */
  setOpen(path: RowPath, open: boolean): TreeState {
    if (this.isOpen(path) === open) return this;
    return open ? this.expand(path) : this.collapse(path);
  }

  expand(path: RowPath): TreeState {
    const next = new Set(this.#open);
    const closed = new Set(this.#closed);
    // Opening a deep path implies its ancestors are open, otherwise the
    // row would be unreachable -- which is what restoring a saved view
    // needs.
    for (let i = 1; i <= path.length; i++) {
      const key = pathKey(path.slice(0, i));
      next.add(key);
      closed.delete(key);
    }
    return new TreeState(next, this.#showTotals, this.#expandTo, closed);
  }

  collapse(path: RowPath): TreeState {
    const key = pathKey(path);
    const next = new Set<string>();
    for (const k of this.#open) {
      // Closing a group closes everything beneath it, so reopening does
      // not surprise the user with a tree they left open three levels
      // down.
      if (k !== key && !isBeneath(parsePathKey(k), path)) next.add(k);
    }
    // A group the expand level opens stays shut once the user shuts it.
    const closed = new Set(this.#closed);
    if (path.length <= this.#expandTo) closed.add(key);
    return new TreeState(next, this.#showTotals, this.#expandTo, closed);
  }

  /** Collapse All closes everything, the expand level's groups too. */
  collapseAll(): TreeState {
    return new TreeState(new Set(), this.#showTotals);
  }

  /**
   * Expand All opens every group above the detail rows, `depth` the grid's row groups -- the closes made by hand
   * forgotten. The deepest groups stay shut: open, each would ask for its source rows.
   */
  expandAll(depth: number): TreeState {
    return new TreeState(new Set(), this.#showTotals, Math.max(0, depth));
  }

  withTotals(show: boolean): TreeState {
    return new TreeState(this.#open, show, this.#expandTo, this.#closed);
  }

  /**
   * A new expand level, as the setting changed: what the user opened
   * stays open, and their closes are forgotten -- they were closes of
   * groups the OLD level opened.
   */
  withExpandTo(level: number): TreeState {
    return new TreeState(this.#open, this.#showTotals, Math.max(0, level));
  }
}

/**
 * The fetches needed to draw the tree, in the order they should appear.
 *
 * Only OPEN branches are requested, so a collapsed cube costs one query
 * for the top level rather than one per group. The grand total is a
 * separate request because it is the same measure with every grouping
 * column dropped.
 */
export function requiredLevels(
  state: TreeState,
  depth: number,
  knownChildren: (parent: RowPath) => readonly RowPath[] | undefined,
): LevelRequest[] {
  const out: LevelRequest[] = [];
  if (depth === 0) return out;

  if (state.showTotals) out.push({ level: 0, parent: [] });
  out.push({ level: 1, parent: [] });

  const walk = (parent: RowPath): void => {
    if (parent.length >= depth) return;
    const children = knownChildren(parent);
    if (!children) return;
    for (const child of children) {
      if (!state.isOpen(child, depth)) continue;
      // An open DEEPEST group asks for its detail rows: level depth+1,
      // which has nothing beneath it to walk.
      out.push({ level: child.length + 1, parent: child });
      walk(child);
    }
  };
  walk([]);
  return out;
}

/**
 * Flatten the tree into the row list the grid renders.
 *
 * `childrenOf` returns the groups fetched for a parent, in engine
 * order; the engine has already sorted them, so this never re-sorts and
 * cannot disagree with the ORDER BY that produced the pagination.
 */
export function flattenTree(
  state: TreeState,
  depth: number,
  childrenOf: (parent: RowPath) => readonly RowPath[] | undefined,
): TreeRow[] {
  const rows: TreeRow[] = [];
  if (depth === 0) return rows;

  if (state.showTotals) {
    rows.push({
      path: [],
      level: 0,
      depth: 1,
      isGroup: false,
      expanded: false,
      isTotal: true,
    });
  }

  const walk = (parent: RowPath): void => {
    const children = childrenOf(parent);
    if (!children) return;
    for (const child of children) {
      // Every group opens, the deepest onto its detail rows (upstream:
      // at the last level the groupBy is dropped); a detail row is a
      // leaf.
      const isDetail = child.length > depth;
      const isGroup = !isDetail;
      const expanded = isGroup && state.isOpen(child, depth);
      rows.push({
        path: child,
        level: child.length,
        // Shifted down by one when a root total is present.
        depth: child.length + (state.showTotals ? 1 : 0),
        isGroup,
        expanded,
        // A group row that is OPEN shows an aggregate of what is
        // beneath it, so it reads as a subtotal; closed, it is simply
        // the collapsed group.
        isTotal: isGroup && expanded,
        ...(isDetail ? { isDetail: true } : {}),
      });
      if (expanded) walk(child);
    }
  };
  walk([]);
  return rows;
}

/** The label a tree row shows in the dimension column for its level. */
export function rowLabel(row: TreeRow, totalsLabel = 'Total'): string {
  if (row.level === 0) return totalsLabel;
  if (row.isDetail) return '';
  return row.path[row.path.length - 1] ?? '';
}
