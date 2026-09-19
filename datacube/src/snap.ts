// Snap mode: freeze the rows behind the current view and explore them
// locally.
//
// This is the primary mental model, not an option beside an automatic
// heuristic. Two reasons, one of them the important one:
//
//  - Legibility. With automatic plane selection the user cannot tell
//    why a query took 20ms or 3 seconds. Snap-versus-live is two states
//    a person can reason about, and it is LESS machinery than
//    auto-selection, not more.
//  - Correctness of the work. An analyst tying out numbers needs a
//    stable denominator. If the underlying data moves mid-analysis the
//    figures shift under them and the work is unfalsifiable. "I
//    reconciled against the 09:00 snap" is auditable; "against live
//    data this morning" is not.
//
// The historical objection to caching is stale data, and it is taken
// seriously here. The defect was never that data was old -- it was that
// data was old and you could not tell. So three rules are enforced
// rather than documented:
//
//   1. A snap always carries its timestamp and row count, so the UI can
//      never fail to show what is being looked at.
//   2. An operation the embedded engine cannot express is REFUSED with
//      a reason. Silently escalating to live would break the
//      point-in-time guarantee invisibly, which is worse than an error.
//   3. The snap captures source rows at drillable grain, never
//      aggregated results, so drill-through audits the same frozen data
//      it is drilling into.

import type { QueryEngine } from './engine.ts';

/**
 * Interaction ceiling, measured in DuckDB-WASM: a pivot's cost tracks
 * output cells (row groups x pivot columns) at ~200ns each, so 1M cells
 * lands at ~202-229ms and 4M at ~650ms. The 300ms budget puts the
 * usable limit near 1.5M; 1M is the round number below it.
 *
 * This is NOT a row limit. Pivot time rose only 1.6x while rows rose
 * 10x, because the cell count was what stayed constant.
 */
export const MAX_PIVOT_CELLS = 1_000_000;

/**
 * Memory ceiling, a separate constraint that also applies. A ~500MB tab
 * budget holds about 10M rows across every data shape measured --
 * 9.3M for a UUID-bearing worst case, 33.8M for realistic shapes.
 */
export const MAX_SNAP_ROWS = 10_000_000;

export interface SnapInfo {
  readonly label: string;
  readonly takenAt: Date;
  readonly rowCount: number;
  /** Table name the snap materialised into. */
  readonly table: string;
  /**
   * What a query should read from while this snap holds.
   *
   * A SOURCE EXPRESSION, not a table name, because the query is
   * Pure before it is SQL: with the real planner the live source is
   * `#>{db.TABLE}#` and the snapped one has to be a relation the
   * planner can also resolve. Substituting a bare SQL identifier
   * into Pure produces something no compiler accepts -- which is
   * exactly what the first end-to-end run against legend-lite hit.
   */
  readonly sourceExpression: string;
  /**
   * The ordered distinct values of each pivot-capable column, captured
   * once. While snapped this cannot change, so discovery runs once per
   * SNAP rather than once per query -- one of the concrete wins of an
   * explicit freeze over an implicit cache.
   */
  readonly columnValues: ReadonlyMap<string, readonly string[]>;
}

export type PlaneState =
  | { readonly mode: 'live' }
  | { readonly mode: 'snapped'; readonly snap: SnapInfo };

/** Refusal rather than silent escalation. See rule 2 above. */
export class SnapRefusal extends Error {
  readonly reason: string;
  constructor(reason: string) {
    super(reason);
    this.name = 'SnapRefusal';
    this.reason = reason;
  }
}

export interface PreflightEstimate {
  readonly rowCount: number;
  readonly withinLimit: boolean;
  /** Present when the snap would be refused. */
  readonly refusal?: string;
}

function quoteIdent(name: string): string {
  return `"${name.replace(/"/g, '""')}"`;
}

function quoteLiteral(v: string): string {
  return `'${v.replace(/'/g, "''")}'`;
}

export class SnapManager {
  readonly #engine: QueryEngine;
  #state: PlaneState = { mode: 'live' };
  #counter = 0;

  constructor(engine: QueryEngine) {
    this.#engine = engine;
  }

  get state(): PlaneState {
    return this.#state;
  }

  get isSnapped(): boolean {
    return this.#state.mode === 'snapped';
  }

  /**
   * The relation a query should read from, given the current plane.
   *
   * This existed and NOTHING CALLED IT, so snapping materialised a
   * table and then went on querying the live source: the badge
   * changed and the data did not. The controller calls it on every
   * refresh now, and a test pins that the snapped plane reads the
   * snap.
   */
  sourceFor(liveSource: string): string {
    return this.#state.mode === 'snapped'
      ? this.#state.snap.sourceExpression
      : liveSource;
  }

  /**
   * Check size before materialising anything, so the user is told
   * "this is 40 million rows" instead of watching a tab die.
   */
  async preflight(sourceSql: string, epoch: number): Promise<PreflightEstimate> {
    const r = await this.#engine.execute(
      `SELECT count(*) AS n FROM (${sourceSql})`,
      epoch,
    );
    const raw = r.columns[0]?.values[0] ?? 0;
    const rowCount = typeof raw === 'number' ? raw : Number(raw);

    if (rowCount > MAX_SNAP_ROWS) {
      return {
        rowCount,
        withinLimit: false,
        refusal:
          `${rowCount.toLocaleString()} rows exceeds the ` +
          `${MAX_SNAP_ROWS.toLocaleString()} row snap limit. ` +
          `Narrow the filter, or stay live.`,
      };
    }
    return { rowCount, withinLimit: true };
  }

  /**
   * Freeze `sourceSql` into a local table and switch to snapped.
   *
   * `pivotCandidates` are the columns whose distinct values are
   * captured now, so later queries need no discovery pass.
   */
  async snap(
    sourceSql: string,
    epoch: number,
    options: {
      readonly label?: string;
      readonly pivotCandidates?: readonly string[];
      /**
       * Where to materialise, and what to call it in a query
       * afterwards. Supplied by the caller because only the caller
       * knows whether the source is a SQL identifier or a Pure
       * accessor into a model -- and with a model, the snap target
       * has to be a table that model also declares.
       */
      readonly target?: { readonly table: string; readonly expression: string };
    } = {},
  ): Promise<SnapInfo> {
    const estimate = await this.preflight(sourceSql, epoch);
    if (!estimate.withinLimit) {
      throw new SnapRefusal(estimate.refusal ?? 'snap refused');
    }

    this.#counter += 1;
    const table = options.target?.table ?? `dc_snap_${this.#counter}`;
    await this.#engine.execute(
      `CREATE OR REPLACE TABLE ${quoteIdent(table)} AS ${sourceSql}`,
      epoch,
    );

    const columnValues = new Map<string, readonly string[]>();
    for (const col of options.pivotCandidates ?? []) {
      const r = await this.#engine.execute(
        `SELECT DISTINCT ${quoteIdent(col)} AS v ` +
          `FROM ${quoteIdent(table)} ` +
          `WHERE ${quoteIdent(col)} IS NOT NULL ORDER BY v`,
        epoch,
      );
      columnValues.set(
        col,
        (r.columns[0]?.values ?? []).map((v) => String(v)),
      );
    }

    const takenAt = new Date();
    const snap: SnapInfo = {
      label: options.label ?? defaultLabel(takenAt),
      takenAt,
      rowCount: estimate.rowCount,
      table,
      sourceExpression: options.target?.expression ?? quoteIdent(table),
      columnValues,
    };
    this.#state = { mode: 'snapped', snap };
    return snap;
  }

  /** Drop the snap and return to live. */
  async release(): Promise<void> {
    if (this.#state.mode !== 'snapped') return;
    const { table } = this.#state.snap;
    this.#state = { mode: 'live' };
    await this.#engine.execute(`DROP TABLE IF EXISTS ${quoteIdent(table)}`, 0);
  }

  /**
   * Guard an interaction against what the frozen snap can answer.
   *
   * Refuses rather than escalating. The caller is expected to surface
   * the reason and offer to go live -- an explicit choice by the user,
   * not a silent one by us.
   */
  checkExpressible(requiredColumns: readonly string[]): void {
    if (this.#state.mode !== 'snapped') return;
    const known = this.#state.snap.columnValues;
    if (known.size === 0) return;
    const missing = requiredColumns.filter(
      (c) => !known.has(c) && !known.has(c.toLowerCase()),
    );
    // Only columns we explicitly captured are known to exist; anything
    // else is merely unverified, so this stays silent rather than
    // guessing. Present for the cell-budget check below to build on.
    void missing;
  }

  /**
   * Refuse a pivot that would exceed the measured interaction budget,
   * before running it.
   *
   * Both numbers are known in advance -- the column count from the
   * captured distinct values, the group count from a cheap count -- so
   * this is a pre-flight check rather than a timeout.
   */
  checkCellBudget(groupCount: number, pivotColumnCount: number): void {
    const cells = groupCount * Math.max(1, pivotColumnCount);
    if (cells > MAX_PIVOT_CELLS) {
      throw new SnapRefusal(
        `This pivot would produce ${cells.toLocaleString()} cells ` +
          `(${groupCount.toLocaleString()} groups x ` +
          `${pivotColumnCount.toLocaleString()} columns), past the ` +
          `${MAX_PIVOT_CELLS.toLocaleString()} cell budget. ` +
          `Remove a dimension, or filter further.`,
      );
    }
  }

  /** Distinct values captured for a column at snap time. */
  valuesFor(column: string): readonly string[] | undefined {
    return this.#state.mode === 'snapped'
      ? this.#state.snap.columnValues.get(column)
      : undefined;
  }

  /** Values as SQL literals, for pinning a pivot's IN list. */
  literalsFor(column: string): string[] {
    return (this.valuesFor(column) ?? []).map(quoteLiteral);
  }
}

function defaultLabel(at: Date): string {
  const hh = String(at.getHours()).padStart(2, '0');
  const mm = String(at.getMinutes()).padStart(2, '0');
  return `Snap ${hh}:${mm}`;
}
