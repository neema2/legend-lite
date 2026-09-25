// Undo and redo over the whole cube.
//
// This is a ROBUSTNESS feature at least as much as a convenience one.
// Most of the ways a person "breaks" an analytical grid are not
// crashes: they drag a dimension into the wrong zone, clear a filter
// they meant to edit, collapse a tree they spent a minute opening, or
// pivot on a column with forty thousand distinct values. None of that
// throws, and without undo every one of them is a manual repair job
// against a UI they are now mistrustful of. With undo they are all one
// keystroke, which is the difference between a tool that feels sturdy
// and one that feels dangerous.
//
// WHAT IS UNDOABLE is the pair that decides what is on screen: the
// snapshot (columns, filters, pivots, sorts, measures) and the tree's
// expansion. Expansion is included deliberately -- "I collapsed
// everything by accident" is exactly the moment somebody reaches for
// undo, and a history that quietly ignored it would feel broken at
// precisely the wrong time.
//
// WHAT IS NOT is the plane: snapping and releasing move where data
// lives, not what the cube asks, and rolling that back under someone
// by way of an undo would be a surprise rather than a repair.

import type { CubeSnapshot } from './snapshot.ts';
import type { TreeState } from './tree.ts';

/** Everything that decides what the grid shows. */
export interface CubeState {
  readonly snapshot: CubeSnapshot;
  readonly tree: TreeState;
  /**
   * The HOST's half of the state, opaque here.
   *
   * A cube is not only its query. Pinned columns, widths, colours,
   * number formats and the row cap live on the host, and a person
   * changing one has no idea they have crossed an internal boundary
   * -- so an undo that covered only the snapshot was broken in two
   * ways at once. A cosmetic change undid to an identical snapshot
   * and looked like nothing happened; a setting that also shapes the
   * query (the row cap, the grand total) rolled the snapshot back
   * while the host kept the new value, and the next refresh folded it
   * straight back in. An undo that reverts itself is worse than none.
   *
   * Kept opaque because presentation is genuinely not this layer's
   * business: the host captures and restores it, and history only has
   * to store it and compare it.
   */
  readonly host?: unknown;
}

/**
 * A stable identity for a state, used to avoid recording a step that
 * changed nothing.
 *
 * The epoch is EXCLUDED: it advances on every refresh, so including it
 * would make every state unique and fill the stack with duplicates
 * that undo to the same screen -- the classic "I pressed undo five
 * times and nothing happened" bug.
 */
export function stateKey(state: CubeState): string {
  const { epoch: _epoch, ...rest } = state.snapshot;
  return JSON.stringify({
    snapshot: rest,
    open: [...state.tree.openPaths].sort(),
    totals: state.tree.showTotals,
    host: state.host ?? null,
  });
}

export interface HistoryOptions {
  /**
   * How many steps back the stack holds. Bounded on purpose: a
   * snapshot is small but not free, and an unbounded stack in a
   * long-lived analytical session is a slow leak.
   */
  readonly limit?: number;
}

export const DEFAULT_HISTORY_LIMIT = 50;

export class History {
  #past: CubeState[] = [];
  #future: CubeState[] = [];
  #limit: number;

  constructor(options: HistoryOptions = {}) {
    this.#limit = Math.max(1, options.limit ?? DEFAULT_HISTORY_LIMIT);
  }

  /** Settings > Max History Stack Size: the oldest steps go first. */
  setLimit(limit: number): void {
    this.#limit = Math.max(1, limit);
    while (this.#past.length > this.#limit) this.#past.shift();
  }

  get canUndo(): boolean {
    return this.#past.length > 0;
  }

  get canRedo(): boolean {
    return this.#future.length > 0;
  }

  /** Steps available in each direction. Diagnostics and tests. */
  get depth(): { readonly past: number; readonly future: number } {
    return { past: this.#past.length, future: this.#future.length };
  }

  /**
   * Record the state being REPLACED, just before applying a new one.
   *
   * Recording the outgoing state rather than the incoming one is what
   * makes the first undo land on the screen the user actually had.
   * A new step clears the redo branch, because redoing into a future
   * that no longer follows from the present is how history models
   * start lying.
   */
  record(previous: CubeState): void {
    const top = this.#past[this.#past.length - 1];
    if (top && stateKey(top) === stateKey(previous)) return;
    this.#past.push(previous);
    if (this.#past.length > this.#limit) this.#past.shift();
    this.#future = [];
  }

  /** The state to go back to, given where we are now. */
  undo(current: CubeState): CubeState | null {
    const previous = this.#past.pop();
    if (!previous) return null;
    this.#future.push(current);
    return previous;
  }

  /** The state to go forward to, given where we are now. */
  redo(current: CubeState): CubeState | null {
    const next = this.#future.pop();
    if (!next) return null;
    this.#past.push(current);
    return next;
  }

  /**
   * Put back an undo that could not be applied.
   *
   * A step is only spent when the cube actually moved. If the refresh
   * that follows an undo fails, the stacks have to look exactly as
   * they did before it was attempted, or the user loses a step they
   * never got the benefit of -- and gains a redo pointing at a state
   * that was never on screen.
   */
  rollbackUndo(previous: CubeState): void {
    this.#future.pop();
    this.#past.push(previous);
  }

  /** The same, for a redo that could not be applied. */
  rollbackRedo(next: CubeState): void {
    this.#past.pop();
    this.#future.push(next);
  }

  clear(): void {
    this.#past = [];
    this.#future = [];
  }
}
