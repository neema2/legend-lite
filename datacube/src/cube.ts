// The controller: snapshot in, rows on screen.
//
// Holds the single source of truth (the snapshot), asks the planner for
// SQL, runs it under an epoch guard, and pushes the result at the grid.
// Every user interaction goes through `update`, which is what keeps
// "one snapshot, one query, one render" true rather than aspirational.

import type { QueryEngine } from './engine.ts';
import { EpochGuard, isStale, type Stale } from './epoch.ts';
import { buildColumnModel, type ColumnModel } from './grid/columns.ts';
import type { CubeSnapshot } from './snapshot.ts';
import { referencedColumns } from './snapshot.ts';
import { serialize } from './serialize.ts';
import type { ResultTable } from './result.ts';
import { SnapManager } from './snap.ts';

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
  /** Pure grammar in, SQL out. */
  plan(pureGrammar: string, snapshot: CubeSnapshot): Promise<string>;
}

export interface CubeView {
  readonly snapshot: CubeSnapshot;
  readonly columns: ColumnModel;
  readonly rows: ResultTable;
  readonly sql: string;
}

export interface CubeControllerOptions {
  readonly onView?: (view: CubeView) => void;
  readonly onError?: (error: unknown) => void;
  readonly onBusy?: (busy: boolean) => void;
}

export class CubeController {
  readonly #engine: QueryEngine;
  readonly #planner: Planner;
  readonly #guard = new EpochGuard();
  readonly #snaps: SnapManager;
  readonly #options: CubeControllerOptions;
  #snapshot: CubeSnapshot | null = null;
  #view: CubeView | null = null;

  constructor(
    engine: QueryEngine,
    planner: Planner,
    options: CubeControllerOptions = {},
  ) {
    this.#engine = engine;
    this.#planner = planner;
    this.#options = options;
    this.#snaps = new SnapManager(engine);
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

  /**
   * Apply a new snapshot and refresh.
   *
   * Takes the whole snapshot rather than a patch, because a partial
   * update is how two sources of truth start: the caller derives the
   * next snapshot from the current one and hands it over whole.
   */
  async update(next: CubeSnapshot): Promise<CubeView | Stale> {
    this.#snapshot = next;
    return this.refresh();
  }

  /** Re-run the current snapshot, e.g. after snapping or releasing. */
  async refresh(): Promise<CubeView | Stale> {
    const snapshot = this.#snapshot;
    if (!snapshot) return this.#fail(new Error('no snapshot set'));

    this.#options.onBusy?.(true);
    try {
      const out = await this.#guard.issue(async (epoch) => {
        // The snapshot's own epoch is advisory; the guard's is
        // authoritative, so a stale answer cannot win a race.
        const withEpoch: CubeSnapshot = { ...snapshot, epoch };
        const grammar = serialize(withEpoch);
        const sql = await this.#planner.plan(grammar, withEpoch);
        const rows = await this.#engine.execute(sql, epoch);
        const columns = buildColumnModel(
          rows,
          withEpoch.rows,
          withEpoch.measures.map((m) => m.name),
        );
        return { snapshot: withEpoch, columns, rows, sql } satisfies CubeView;
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

    const columns = referencedColumns(snapshot).map(quoteIdent).join(', ');
    const source = `SELECT ${columns} FROM ${snapshot.source.expression}`;
    await this.#snaps.snap(source, this.#guard.current, {
      ...(label !== undefined ? { label } : {}),
      pivotCandidates: snapshot.pivotOn,
    });
    await this.refresh();
  }

  async release(): Promise<void> {
    await this.#snaps.release();
    await this.refresh();
  }

  #fail(error: unknown): Stale {
    this.#options.onError?.(error);
    throw error;
  }
}

function quoteIdent(name: string): string {
  return `"${name.replace(/"/g, '""')}"`;
}
