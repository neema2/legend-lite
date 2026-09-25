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
import { CubeRefusal, referencedColumns } from './snapshot.ts';
import { serialize, type LevelScope } from './serialize.ts';
import type { ResultTable } from './result.ts';
import { SnapManager } from './snap.ts';
import {
  PlanThenRun,
  type QueryRunner,
  type RunOutcome,
} from './runner.ts';
import { requestKey } from './tree.ts';
import {
  TreeState,
  type LevelRequest,
  type RowPath,
  type TreeRow,
} from './tree.ts';
import { fetchTree, takeRows } from './treeview.ts';
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
  /**
   * Capture and restore the host's half of the undoable state.
   *
   * The controller owns the query; the host owns presentation. Undo
   * has to cover both or it reverts itself -- see CubeState.host.
   * Whatever `captureHost` returns is handed back to `restoreHost`
   * unchanged, so an immutable configuration object is exactly the
   * right thing to pass.
   */
  readonly captureHost?: () => unknown;
  readonly restoreHost?: (host: unknown) => void;
}

export class CubeController {
  readonly #runner: QueryRunner;
  /**
   * The local pair, when there is one.
   *
   * Snapping freezes a cube by materialising its source into a local
   * store, so it needs both halves: the planner for the source SQL,
   * the engine to hold the table. A remote engine answers queries
   * for us and cannot do that on our behalf, so there it is null and
   * `snap` refuses by name.
   */
  readonly #local: PlanThenRun | null;
  readonly #guard = new EpochGuard();
  readonly #snaps: SnapManager;
  readonly #options: CubeControllerOptions;
  #snapshot: CubeSnapshot | null = null;
  #view: CubeView | null = null;
  #tree = TreeState.empty();
  readonly #history: History;
  /** The last state that reached the screen. See #remember. */
  #lastState: CubeState | null = null;

  /**
   * Two arrangements, and the choice is the CALL, not a flag.
   *
   * `new CubeController(engine, planner)` plans then executes
   * locally -- both browser planes. `new CubeController(runner)`
   * takes whatever turns Pure into rows, which is how a remote
   * engine plane is built: `new CubeController(new RemoteRun(...))`.
   *
   * Settled at construction and unable to change afterwards, for the
   * reason `test/guardrails.test.ts` records: the one time shipped
   * code could pick a planner at runtime, a health-check fallback hid
   * three real bugs for the life of the project.
   */
  constructor(runner: QueryRunner, options?: CubeControllerOptions);
  constructor(
    engine: QueryEngine,
    planner: Planner,
    options?: CubeControllerOptions,
  );
  constructor(
    first: QueryEngine | QueryRunner,
    second?: Planner | CubeControllerOptions,
    third: CubeControllerOptions = {},
  ) {
    // WHICH FORM, by the shape of what arrived. A runner runs; an
    // engine executes. This is a construction-time reading of the
    // caller's intent, not a choice the cube makes for itself.
    const asRunner = 'run' in first ? (first as QueryRunner) : null;
    const local = asRunner
      ? null
      : new PlanThenRun(second as Planner, first as QueryEngine);
    this.#runner = asRunner ?? (local as PlanThenRun);
    this.#local = local;
    const options = asRunner
      ? ((second as CubeControllerOptions | undefined) ?? {})
      : third;
    this.#options = options;
    this.#snaps = new SnapManager(local ? local.engine : null);
    this.#history = new History(
      options.historyLimit !== undefined
        ? { limit: options.historyLimit }
        : {},
    );
  }

  get snaps(): SnapManager {
    return this.#snaps;
  }

  /** Which arrangement answers queries, for diagnostics. */
  get runnerName(): string {
    return this.#runner.name;
  }

  /**
   * One query, for a host that needs rows of its own.
   *
   * Drill-through is the case: it asks for the rows behind a cell,
   * which is a query the cube did not plan. It goes through the same
   * runner as everything else, so it works on every plane -- before
   * this, the host planned and executed it by hand, which meant the
   * one plane where the engine executes would have had a
   * drill-through that could not run.
   */
  async runQuery(
    pureGrammar: string,
    snapshot: CubeSnapshot,
    scope?: LevelScope,
    signal?: AbortSignal,
  ): Promise<RunOutcome> {
    return this.#runner.run(pureGrammar, snapshot, scope, signal);
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

  /**
   * Take a tree WITHOUT re-querying; the caller refreshes.
   *
   * `setTree` refreshes, and a refresh pushes a view at the host --
   * which is how loading a saved view silently restored nothing.
   * `loadView` set the app's snapshot, then called `setTree`, whose
   * refresh ran the CONTROLLER's snapshot (still the old one) and
   * handed that view back; the host's `onView` assigns
   * `this.#snapshot = view.snapshot`, so the freshly loaded snapshot
   * was overwritten by the stale one, and the refresh that followed
   * queried the shape the user had just replaced. The status line
   * said `loaded "..."` either way.
   *
   * So a caller that is about to refresh anyway adopts the tree
   * quietly and gets ONE query with both halves in place, instead of
   * two where the first clobbers the second.
   */
  adoptTree(state: TreeState): void {
    this.#tree = state;
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
            runner: this.#runner,
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

        // THE ROW LIMIT, on a flat cube too. General Properties > Row
        // Limit capped every level of a tree and nothing here: a flat
        // cube fetched all its rows whatever the setting said (2026-09-25
        // sweep). Upstream limits every query. One more than the cap is
        // asked for, as the tree does, so "there is more" costs no
        // second query and the truncation warning can say so.
        // Only a limit the USER set: unset means none, as upstream.
        const maxRows = withEpoch.maxRows;
        const scope = maxRows === undefined
          ? undefined
          : ({ level: 1, parent: [], limit: maxRows + 1 } as const);
        const grammar = serialize(withEpoch, scope);
        const { rows: full, sql } = await this.#runner.run(
          grammar,
          withEpoch,
          scope,
          signal,
        );
        const cut = maxRows !== undefined && full.rowCount > maxRows;
        const rows = cut ? takeRows(full, maxRows) : full;
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
          truncated: cut ? [{ level: 1, parent: [] }] : [],
          pure: grammar,
          sql,
        } satisfies CubeView;
      });

      if (isStale(out)) return out;
      this.#view = out;
      // Snapshot the state that just landed, so the NEXT change has
      // something truthful to record as its "before".
      this.#lastState = this.#state();
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
    if (!this.#local) {
      // The SnapManager says the same thing; saying it here too
      // keeps the reason next to the attempt.
      throw new CubeRefusal(
        'this cube\u2019s queries run on a remote engine: there is no'
        + ' local store to freeze a snapshot into.',
      );
    }
    const sourceSql = await this.#local.planner.plan(pure, snapshot);

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
  /**
   * Push the last RENDERED state onto the undo stack.
   *
   * Not the state as it is right now. By the time a mutator runs, the
   * host may already have changed its half -- a pin is applied to the
   * configuration and only then does the refresh begin -- so
   * capturing at this moment would record the new configuration as
   * though it were the old one, and undo would restore the very thing
   * it was meant to remove. The last state that actually reached the
   * screen is the one a person means by "back".
   */
  #remember(): void {
    if (!this.#lastState) return;
    this.#history.record(this.#lastState);
    this.#announceHistory();
  }

  #state(): CubeState {
    const host = this.#options.captureHost?.();
    return {
      snapshot: this.#snapshot as CubeSnapshot,
      tree: this.#tree,
      ...(host !== undefined ? { host } : {}),
    };
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
  /** Put the cube into a state without re-querying. */
  #install(state: CubeState): void {
    this.#snapshot = state.snapshot;
    this.#tree = state.tree;
    // BEFORE the refresh, not after: the host folds its configuration
    // into the snapshot on refresh, so restoring it afterwards would
    // let the stale config overwrite the state just restored.
    if (state.host !== undefined) this.#options.restoreHost?.(state.host);
  }

  /**
   * Move to a state from the history, ALL OR NOTHING.
   *
   * Undo mutates the cube and then re-queries, and that query can
   * fail -- the engine is down, the planner refuses it. Without a
   * rollback the cube had already moved, the step was already spent,
   * and the screen still showed the old view: the model and the
   * display disagreeing, with no way back and a redo pointing at a
   * state that was never rendered.
   *
   * A SUPERSEDED refresh is not a failure and is not rolled back: the
   * user did something else while this was in flight, and the newer
   * interaction legitimately owns the cube from here.
   */
  async #applyHistory(
    state: CubeState,
    rollback: (current: CubeState) => void,
  ): Promise<CubeView | Stale> {
    const current = this.#state();
    this.#install(state);
    this.#announceHistory();
    try {
      return await this.refresh();
    } catch (error) {
      this.#install(current);
      rollback(current);
      this.#announceHistory();
      throw error;
    }
  }

  async undo(): Promise<CubeView | Stale | null> {
    if (!this.#snapshot) return null;
    const previous = this.#history.undo(this.#state());
    if (!previous) return null;
    return this.#applyHistory(previous, () =>
      this.#history.rollbackUndo(previous),
    );
  }

  async redo(): Promise<CubeView | Stale | null> {
    if (!this.#snapshot) return null;
    const next = this.#history.redo(this.#state());
    if (!next) return null;
    return this.#applyHistory(next, () => this.#history.rollbackRedo(next));
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
