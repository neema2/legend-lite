// Epoch-guarded dispatch: the one thing standing between a fast grid
// and a grid that shows the wrong numbers.
//
// A user scrolling, sorting or re-pivoting issues queries faster than
// they complete, so answers arrive out of order. In the browser we
// cannot cancel the loser: DuckDB-WASM's `connection.query()` runs to
// completion inside a single C++ call and has no cancellation path at
// all. `send()`/`cancelSent()` can cancel, but only while a query is
// still pending and has produced no result. So discarding stale answers
// is not a fallback for cancellation -- it IS the mechanism, and it has
// to be airtight.
//
// Two behaviours here are less obvious than they look:
//
//  - A superseded query that FAILS must fail silently. Issuing a second
//    query on one DuckDB connection makes the engine interrupt the
//    first, surfacing as EXECUTION_CANCELLED with the message "likely
//    caused by executing a different query". That is the system working
//    correctly; showing the user an error for a query they already
//    replaced would be a bug.
//  - Superseding must never reject the caller's promise. A component
//    awaiting a result it no longer needs should simply be told the
//    answer is stale, not handed an exception to remember to catch.

/** Returned instead of a value when the caller's epoch was superseded. */
export const STALE = Symbol('stale');
export type Stale = typeof STALE;

export function isStale<T>(v: T | Stale): v is Stale {
  return v === STALE;
}

export interface EpochGuardOptions {
  /**
   * Called when a superseded task throws. Defaults to swallowing it,
   * because a failure nobody is waiting for is not a user-facing event.
   * Wire it to telemetry rather than to an error banner.
   */
  readonly onDiscardedError?: (error: unknown, epoch: number) => void;
}

export class EpochGuard {
  #current = 0;
  #inflight = 0;
  readonly #onDiscardedError: (error: unknown, epoch: number) => void;

  constructor(options: EpochGuardOptions = {}) {
    this.#onDiscardedError = options.onDiscardedError ?? (() => {});
  }

  /** The epoch new work should be issued under. */
  get current(): number {
    return this.#current;
  }

  /** Tasks started and not yet settled. Diagnostics only. */
  get inflight(): number {
    return this.#inflight;
  }

  /**
   * Supersede everything outstanding and return the new epoch. Call
   * this once per user interaction, before issuing its query.
   */
  advance(): number {
    this.#current += 1;
    return this.#current;
  }

  /** Whether work issued at `epoch` is still wanted. */
  isCurrent(epoch: number): boolean {
    return epoch === this.#current;
  }

  /**
   * Run `task` under `epoch`, resolving to STALE if it was superseded
   * either before the task started or while it ran.
   *
   * The epoch is checked twice on purpose. Checking only afterwards
   * would still start work that is already pointless; checking only
   * beforehand would let a slow answer overwrite a newer one.
   */
  async run<T>(
    epoch: number,
    task: () => Promise<T>,
  ): Promise<T | Stale> {
    if (!this.isCurrent(epoch)) return STALE;
    this.#inflight += 1;
    try {
      const value = await task();
      return this.isCurrent(epoch) ? value : STALE;
    } catch (error) {
      if (!this.isCurrent(epoch)) {
        // Superseded AND failed: almost certainly the engine
        // interrupting the loser. Report to telemetry, never to the user.
        this.#onDiscardedError(error, epoch);
        return STALE;
      }
      throw error;
    } finally {
      this.#inflight -= 1;
    }
  }

  /**
   * Issue work for a fresh epoch in one step -- the common case, and
   * the one that cannot forget to advance first.
   */
  async issue<T>(task: (epoch: number) => Promise<T>): Promise<T | Stale> {
    const epoch = this.advance();
    return this.run(epoch, () => task(epoch));
  }
}
