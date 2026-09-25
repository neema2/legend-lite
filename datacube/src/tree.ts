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

/** Values of the row dimensions from the root down. Empty = grand total. */
export type RowPath = readonly string[];

/**
 * The path separator.
 *
 * NUL, because it cannot occur in a SQL identifier or in a value
 * DuckDB would return as text, so no dimension value can forge a
 * different group's key -- a comma or a slash can. Written as an
 * escape rather than as a literal control character, since an
 * invisible byte in source survives no reformat and no code review.
 */
const PATH_SEP = '\u0000';

/** Stable key for a path. */
export function pathKey(path: RowPath): string {
  return path.join(PATH_SEP);
}

/**
 * Inverse of {@link pathKey}, so a saved view can restore expansion
 * without any caller hardcoding the separator. The empty key is the
 * root, not a one-element path containing an empty string.
 */
export function parsePathKey(key: string): RowPath {
  return key === '' ? [] : key.split(PATH_SEP);
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

  isOpen(path: RowPath): boolean {
    const key = pathKey(path);
    if (this.#open.has(key)) return true;
    return path.length > 0 && path.length <= this.#expandTo
      && !this.#closed.has(key);
  }

  toggle(path: RowPath): TreeState {
    return this.isOpen(path) ? this.collapse(path) : this.expand(path);
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
      if (k !== key && !k.startsWith(key + PATH_SEP)) next.add(k);
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
      if (child.length >= depth || !state.isOpen(child)) continue;
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
      const isGroup = child.length < depth;
      const expanded = isGroup && state.isOpen(child);
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
  return row.path[row.path.length - 1] ?? '';
}
