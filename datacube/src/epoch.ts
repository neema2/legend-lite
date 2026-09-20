// Epoch-guarded dispatch: the one thing standing between a fast grid
// and a grid that shows the wrong numbers.
//
// A user scrolling, sorting or re-pivoting issues queries faster than
// they complete, so answers arrive out of order. Discarding the stale
// ones has to be airtight, because it is what keeps the numbers right
// no matter what else fails.
//
// CANCELLATION IS A SEPARATE CONCERN, and for a long time this file
// claimed it was impossible. That was true of the API in use and not
// of the engine: DuckDB-WASM's `connection.query()` runs to completion
// inside one C++ call with no interrupt, but `send()` streams batches
// and `cancelSent()` stops a query that is still pending. The engine
// now takes that path, so a superseded query is genuinely abandoned
// rather than merely ignored on arrival.
//
// The two remain different jobs and both are needed. Discarding keeps
// the grid CORRECT -- it must hold even for work nothing can stop, such
// as an HTTP response already on the wire. Cancelling keeps it FAST, by
// not spending the machine on answers nobody is waiting for.
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

/**
 * The abort reason for work a newer interaction replaced.
 *
 * A distinct type because the two ways a request can end early read
 * identically at the catch site otherwise: the planner being
 * unreachable is worth reporting, and a request we deliberately
 * cancelled is not. Telemetry that cannot tell them apart reports a
 * fast, correctly-behaving grid as a stream of network failures.
 */
export class Superseded extends Error {
  constructor(epoch: number) {
    super(`superseded: epoch ${epoch} was replaced before it finished`);
    this.name = 'Superseded';
  }
}

export function isSuperseded(error: unknown): error is Superseded {
  return error instanceof Superseded;
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
  #controller = new AbortController();
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
   * The current epoch's cancellation signal.
   *
   * Hand this to anything that can stop early. Discarding a stale
   * ANSWER keeps the grid correct; aborting the stale REQUEST is what
   * keeps it fast, and the two are not the same thing -- for the whole
   * life of this class the loser's work ran to completion and its
   * result was thrown away on arrival.
   */
  get signal(): AbortSignal {
    return this.#controller.signal;
  }

  /**
   * Supersede everything outstanding and return the new epoch. Call
   * this once per user interaction, before issuing its query.
   *
   * This ABORTS the previous epoch as well as superseding it. What can
   * actually stop varies by executor, and the difference is worth
   * being honest about: an HTTP planning round trip stops at once and
   * frees the connection; a streamed DuckDB query is cancelled between
   * batches; a query already deep inside one long operation stops when
   * it next yields. In every case the queries BEHIND it -- levels are
   * fetched in sequence -- never start at all, which during a burst is
   * most of the saving.
   */
  advance(): number {
    this.#controller.abort(new Superseded(this.#current));
    this.#controller = new AbortController();
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
   *
   * The signal is passed alongside the epoch so a task cannot pick up
   * the wrong one: reading `guard.signal` inside a task is a race, as
   * a later interaction may already have replaced the controller by
   * the time the line runs.
   */
  async issue<T>(
    task: (epoch: number, signal: AbortSignal) => Promise<T>,
  ): Promise<T | Stale> {
    const epoch = this.advance();
    const { signal } = this;
    return this.run(epoch, () => task(epoch, signal));
  }
}
