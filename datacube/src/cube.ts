// The controller: snapshot in, rows on screen.
//
// Holds the single source of truth (the snapshot), asks the planner for
// SQL, runs it under an epoch guard, and pushes the result at the grid.
// Every user interaction goes through `update`, which is what keeps
// "one snapshot, one query, one render" true rather than aspirational.

import type { QueryEngine } from './engine.ts';
import { EpochGuard, isStale, type Stale } from './epoch.ts';
import {
  buildColumnModel,
  type ColumnLayout,
  type ColumnModel,
} from './grid/columns.ts';
import type { CubeSnapshot } from './snapshot.ts';
import { referencedColumns } from './snapshot.ts';
import { serialize, type LevelScope } from './serialize.ts';
import type { ResultTable } from './result.ts';
import { SnapManager } from './snap.ts';
import { requestKey } from './tree.ts';
import {
  TreeState,
  type LevelRequest,
  type RowPath,
  type TreeRow,
} from './tree.ts';
import { fetchTree } from './treeview.ts';
import { History, type CubeState } from './history.ts';

/**
 * Turns a snapshot into SQL.
 *
 * This is a seam, not a detail. legend-lite is the SINGLE planner: the
 * snapshot serialises to Pure grammar, legend-lite lowers that to SQL
 * once, and the only thing that differs between the server plane and
 * the browser plane is where the SQL runs. Implementing a second
 * planner in TypeScript would mean two things that must agree about
 * null ordering, type coercion and aggregate semantics -- the exact
 * class of divergence the differential tests exist to catch. So the
 * interface stays narrow and the real implementation calls the engine.
 */
export interface Planner {
  /**
   * Pure grammar in, SQL out.
   *
   * `scope` names the tree level the grammar was generated for. A real
   * planner ignores it, because the grammar already says which columns
   * group -- it is passed so that a caller which cannot parse Pure
   * still knows what it was handed.
   */
  plan(
    pureGrammar: string,
    snapshot: CubeSnapshot,
    scope?: LevelScope,
    signal?: AbortSignal,
  ): Promise<string>;
}

export interface CubeView {
  readonly snapshot: CubeSnapshot;
  readonly columns: ColumnModel;
  readonly rows: ResultTable;
  /** Tree metadata per row, parallel to `rows`. Empty for a flat cube. */
  readonly treeRows: readonly TreeRow[];
  /**
   * Levels that hit the row cap. Non-empty means the grid shows a
   * prefix, and the UI must say so rather than leave the user to
   * infer it from a suspiciously round row count.
   */
  readonly truncated: readonly LevelRequest[];
  /**
   * The query, for the "show me the query" panel.
   *
   * BOTH, because they answer different questions: the Pure is what
   * this product emitted, the SQL is what the planner made of it
   * and what the engine actually ran. `sql` held Pure for the whole
   * life of this project -- a panel labelled SQL that had never
   * shown any.
   */
  readonly pure: string;
  readonly sql: string;
}

export interface CubeControllerOptions {
  /** Column order, visibility and widths. */
  readonly layout?: ColumnLayout;
  readonly onView?: (view: CubeView) => void;
  readonly onError?: (error: unknown) => void;
  readonly onBusy?: (busy: boolean) => void;
  /**
   * Where a snap materialises, when the source is a model relation.
   *
   * With the demo shim the source is a bare SQL identifier and a
   * generated `dc_snap_N` works. With the real planner the source
   * is `#>{db.TABLE}#`, and the snapped source must be another
   * relation the SAME model declares -- so the host names it.
   */
  readonly snapTarget?: { readonly table: string; readonly expression: string };
  /** How many undo steps to keep. */
  readonly historyLimit?: number;
  /** Fired whenever undo/redo availability changes, for the UI. */
  readonly onHistory?: (state: {
    readonly canUndo: boolean;
    readonly canRedo: boolean;
  }) => void;
}

export class CubeController {
  readonly #engine: QueryEngine;
  readonly #planner: Planner;
  readonly #guard = new EpochGuard();
  readonly #snaps: SnapManager;
  readonly #options: CubeControllerOptions;
  #snapshot: CubeSnapshot | null = null;
  #view: CubeView | null = null;
  #tree = TreeState.empty();
  readonly #history: History;

  constructor(
    engine: QueryEngine,
    planner: Planner,
    options: CubeControllerOptions = {},
  ) {
    this.#engine = engine;
    this.#planner = planner;
    this.#options = options;
    this.#snaps = new SnapManager(engine);
    this.#history = new History(
      options.historyLimit !== undefined
        ? { limit: options.historyLimit }
        : {},
    );
  }

  get snaps(): SnapManager {
    return this.#snaps;
  }

  get view(): CubeView | null {
    return this.#view;
  }

  get snapshot(): CubeSnapshot | null {
    return this.#snapshot;
  }

  get tree(): TreeState {
    return this.#tree;
  }

  /**
   * Open or close a group and refresh.
   *
   * Only the newly-opened branch is fetched; the rest of the tree is
   * re-requested only because the level results are not yet cached
   * across refreshes, which is a pure optimisation and not a
   * correctness concern.
   */
  async toggle(path: RowPath): Promise<void> {
    this.#remember();
    this.#tree = this.#tree.toggle(path);
    await this.refresh();
  }

  async setTree(state: TreeState): Promise<void> {
    this.#remember();
    this.#tree = state;
    await this.refresh();
  }

  /**
   * Apply a new snapshot and refresh.
   *
   * Takes the whole snapshot rather than a patch, because a partial
   * update is how two sources of truth start: the caller derives the
   * next snapshot from the current one and hands it over whole.
   */
  async update(next: CubeSnapshot): Promise<CubeView | Stale> {
    this.#remember();
    this.#snapshot = next;
    return this.refresh();
  }

  /** Re-run the current snapshot, e.g. after snapping or releasing. */
  async refresh(): Promise<CubeView | Stale> {
    const snapshot = this.#snapshot;
    if (!snapshot) return this.#fail(new Error('no snapshot set'));

    this.#options.onBusy?.(true);
    try {
      const out = await this.#guard.issue(async (epoch, signal) => {
        // The snapshot's own epoch is advisory; the guard's is
        // authoritative, so a stale answer cannot win a race.
        // The PLANE decides what a query reads from. Without this
        // the snap was cosmetic: a table was materialised and every
        // subsequent query still went to the live source.
        const source = this.#snaps.sourceFor(snapshot.source.expression);
        const withEpoch: CubeSnapshot = {
          ...snapshot,
          epoch,
          source: { ...snapshot.source, expression: source },
        };
        const measureNames = withEpoch.measures.map((m) => m.name);

        // A cube with row dimensions is a tree: the grand total and
        // each open branch are separate queries, stitched in order.
        if (withEpoch.rows.length > 0) {
          const view = await fetchTree(withEpoch, this.#tree, {
            planner: this.#planner,
            engine: this.#engine,
            guard: this.#guard,
            epoch,
            signal,
          });
          return {
            snapshot: withEpoch,
            columns: buildColumnModel(
              view.table,
              withEpoch.rows,
              measureNames,
              this.#options.layout ?? {},
              withEpoch.pivotOn.length,
            ),
            rows: view.table,
            treeRows: view.rows,
            truncated: view.truncated,
            pure: serialize(withEpoch, { level: 1, parent: [] }),
            // The level-1 plan is the representative one: it is the
            // query behind the rows a user is looking at.
            sql:
              view.levels.get(requestKey({ level: 1, parent: [] }))?.sql ??
              '',
          } satisfies CubeView;
        }

        const grammar = serialize(withEpoch);
        const sql = await this.#planner.plan(
          grammar,
          withEpoch,
          undefined,
          signal,
        );
        const rows = await this.#engine.execute(sql, epoch, signal);
        const columns = buildColumnModel(
          rows,
          withEpoch.rows,
          measureNames,
          this.#options.layout ?? {},
          withEpoch.pivotOn.length,
        );
        return {
          snapshot: withEpoch,
          columns,
          rows,
          treeRows: [],
          truncated: [],
          pure: grammar,
          sql,
        } satisfies CubeView;
      });

      if (isStale(out)) return out;
      this.#view = out;
      this.#options.onView?.(out);
      return out;
    } catch (error) {
      return this.#fail(error);
    } finally {
      this.#options.onBusy?.(false);
    }
  }

  /**
   * Freeze the rows behind the current view, then refresh against them.
   *
   * The pivot columns are captured at snap time, which is what removes
   * the per-query discovery pass while snapped.
   */
  async snap(label?: string): Promise<void> {
    const snapshot = this.#snapshot;
    if (!snapshot) throw new Error('no snapshot set');

    // Through the PLANNER, like every other query. This used to
    // build `SELECT "a", "b" FROM <source>` by hand, which worked
    // only because the demo's source happened to be a bare SQL
    // identifier; against the real planner the source is a Pure
    // accessor and the hand-built SQL was nonsense. "One planner"
    // is an architectural commitment and this was the one place
    // that quietly broke it.
    const columns = referencedColumns(snapshot).join(', ');
    const pure = `${snapshot.source.expression}->select(~[${columns}])`;
    const sourceSql = await this.#planner.plan(pure, snapshot);

    await this.#snaps.snap(sourceSql, this.#guard.current, {
      ...(label !== undefined ? { label } : {}),
      pivotCandidates: snapshot.pivotOn,
      ...(this.#options.snapTarget
        ? { target: this.#options.snapTarget }
        : {}),
    });
    await this.refresh();
  }

  async release(): Promise<void> {
    await this.#snaps.release();
    await this.refresh();
  }

  // -- undo / redo ----------------------------------------------------

  get canUndo(): boolean {
    return this.#history.canUndo;
  }

  get canRedo(): boolean {
    return this.#history.canRedo;
  }

  /** Steps available each way. Diagnostics and tests. */
  get historyDepth(): { readonly past: number; readonly future: number } {
    return this.#history.depth;
  }

  /**
   * Push the state about to be replaced onto the undo stack.
   *
   * Called by the mutators rather than by refresh(), and the
   * distinction is the whole design: refresh re-runs the CURRENT cube
   * (after a snap, on a retry, when the plane changes) and is not a
   * step a user would ever want to undo. Recording there would fill
   * the stack with entries that all undo to the same screen.
   */
  #remember(): void {
    if (!this.#snapshot) return;
    this.#history.record({ snapshot: this.#snapshot, tree: this.#tree });
    this.#announceHistory();
  }

  #announceHistory(): void {
    this.#options.onHistory?.({
      canUndo: this.#history.canUndo,
      canRedo: this.#history.canRedo,
    });
  }

  /**
   * Apply a state from the history WITHOUT recording it as a new step.
   *
   * Going through update() here would record the undo itself, so the
   * next undo would return to where you just came from and the stack
   * would never advance past two entries -- undo that toggles.
   */
  async #applyHistory(state: CubeState): Promise<CubeView | Stale> {
    this.#snapshot = state.snapshot;
    this.#tree = state.tree;
    this.#announceHistory();
    return this.refresh();
  }

  async undo(): Promise<CubeView | Stale | null> {
    if (!this.#snapshot) return null;
    const previous = this.#history.undo({
      snapshot: this.#snapshot,
      tree: this.#tree,
    });
    if (!previous) return null;
    return this.#applyHistory(previous);
  }

  async redo(): Promise<CubeView | Stale | null> {
    if (!this.#snapshot) return null;
    const next = this.#history.redo({
      snapshot: this.#snapshot,
      tree: this.#tree,
    });
    if (!next) return null;
    return this.#applyHistory(next);
  }

  /** Drop the history, e.g. when a wholly different cube is opened. */
  clearHistory(): void {
    this.#history.clear();
    this.#announceHistory();
  }

  #fail(error: unknown): Stale {
    this.#options.onError?.(error);
    throw error;
  }
}
