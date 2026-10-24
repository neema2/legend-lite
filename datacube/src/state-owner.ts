// ONE OWNER of a state -- the cube's, Ad Hoc Analysis mode's grid (docs/DATACUBE_LEG_B_STATE_OWNER_2026_09_28.md,
// Leg B). What makes a state a kind of state is its `StateRules` (cube-state.ts has the cube's,
// adhoc/session.ts Ad Hoc's); the transaction rules are these, the same for every kind.
//
// A state lives HERE and nowhere else, as one immutable value in three places:
//
//   committed  the last state the user's actions produced and the engine ACCEPTED, as the
//              engine returned it (`StateRules.land`)
//   rendered   the state of the view on screen; differs from committed only while a query runs
//   pending    the change in flight, if any
//
// Presentation paints `current` (pending ?? committed); anything read off the rows on screen
// reads `rendered`.
//
// Two kinds of change, told apart by the QUERY itself, never by a list of settings: a change
// whose state folds to the same query (`queryKey`) is PRESENTATION -- colours, widths,
// formats, labels -- and commits at once, with no query, and cannot be refused. Anything else
// is a TRANSACTION:
//
//   success     committed = rendered = the state the engine returned; one undo step if the
//               change was the user's; one `committed` event
//   refusal     the pending state is dropped and everything repaints from `committed`; a change
//               made on top of another still pending reverts BOTH, and says which
//   superseded  a newer change owns the cube: this one is dropped quietly (latest wins)
//
// Undo and redo are transactions over a stack of committed states that MOVES ONLY WHEN THE
// MOVE LANDS: an undo refused or overtaken leaves the stacks as they were. Undo while a change
// is in flight cancels that change -- the one thing the user just did -- and nothing more; undo
// while an undo is in flight goes one step further (two presses, two steps). A change made on
// top of an undo in flight lands as that undo AND the change, one step each.
//
// No DOM, no engine: the query runs through `run`, so every rule here is proven by fast tests
// (test/cube-state.test.ts, over the cube's rules and a toy state's).

import { isStale, type Stale } from './epoch.ts';

/**
 * How many undo steps are kept when nothing says otherwise: Settings > Max History Stack Size's
 * own default, for the cube and Ad Hoc alike.
 */
export const DEFAULT_HISTORY_LIMIT = 100;

/**
 * Run a state: the query side. Resolves the view, or STALE when a newer run superseded this
 * one; throws when the engine refuses.
 */
export type Runner<S, V> = (state: S) => Promise<V | Stale>;

export type StateOutcome<V> =
  | { readonly kind: 'applied'; readonly view?: V }
  | { readonly kind: 'refused'; readonly error: unknown; readonly reverted: readonly string[] }
  | { readonly kind: 'superseded' }
  | { readonly kind: 'cancelled' }
  | { readonly kind: 'nothing' };

export interface ChangeOptions {
  /** A user's action is an undo step (the default); a machine's re-run is not. */
  readonly record?: boolean;
  /** How the change is named when it has to be reported undone. */
  readonly label?: string;
  /** Run the query even if the state folds to the same one (a refresh after a snap, a retry). */
  readonly force?: boolean;
}

export type StateEvent<V, E = never> =
  | { readonly kind: 'pending' }
  /** `moved`: an undo or a redo landed with it (a change made on top of one in flight lands the move too); `edits`: the
   *  kind's own edits that take the committed state before to the one landed (`StateRules.between`), told as any edit's
   *  are (docs/DATACUBE_LIVE_EDITING_2026_10_24.md W3). */
  | { readonly kind: 'committed'; readonly view: V; readonly moved: boolean; readonly edits?: readonly E[] }
  /** Committed WITHOUT a query (`StateRules.defer`): the view on screen is the old state's. */
  | { readonly kind: 'deferred' }
  /** `moved`: an undo or a redo that differs from the committed state only in presentation (no query) landed; `edits` as
   *  a committed move's. */
  | { readonly kind: 'presentation'; readonly moved: boolean; readonly edits?: readonly E[] }
  /** `moved`: an undo or a redo was part of the change refused (a change made on top of one in flight). */
  | { readonly kind: 'refused'; readonly error: unknown; readonly reverted: readonly string[]; readonly moved: boolean }
  | { readonly kind: 'cancelled'; readonly reverted: readonly string[] };

export interface OwnerOptions {
  /** How many undo steps are kept (Settings > Max History Stack Size). */
  readonly historyLimit?: number;
  /** Stop the query in flight, when a change is cancelled rather than replaced. */
  readonly abort?: () => void;
}

/**
 * What makes a kind of state a kind of state: the one owner (below) runs the same transaction
 * rules over the cube and over Ad Hoc Analysis mode's grid.
 */
export interface StateRules<S, V, E = never> {
  /** A state as the query sees it (the cube folds its configuration in). */
  fold(state: S): S;
  /**
   * The kind's own edits that take `from` to `to` (the cube's: grid-between.ts gridBetween), and the state reached by
   * them (`toward`: what `from` becomes by those edits -- `to` as content, as the edits spell it). An undo or a redo
   * then moves to what its edits make, and is told as them, so what the screen shows after it is what the same edits
   * make elsewhere (docs/DATACUBE_LIVE_EDITING_2026_10_24.md W3). Absent, a move puts the state back as it was.
   */
  between?(from: S, to: S): readonly E[];
  toward?(from: S, to: S): S;
  /** What the query of a state IS: equal keys run the same queries; a change that keeps it is presentation. */
  queryKey(state: S): string;
  /** A state's identity for undo, presentation included. */
  stateKey(state: S): string;
  /** The state to commit when `view` landed for `pending` (the engine's own facts go in here). */
  land(pending: S, view: V): S;
  /** The view laid out again for a presentation change: the same answers, the new settings. */
  represent?(state: S, view: V): V;
  /** Commit this state WITHOUT querying it (Ad Hoc's Navigate Without Data): refresh queries it. */
  defer?(state: S): boolean;
}

/** The undo and redo stacks. A move is only made once the state it moves to has landed. */
export class UndoStack<T> {
  #past: T[] = [];
  #future: T[] = [];
  #limit: number;
  readonly #key: (t: T) => string;

  constructor(key: (t: T) => string, limit = DEFAULT_HISTORY_LIMIT) {
    this.#key = key;
    this.#limit = Math.max(1, limit);
  }

  get canUndo(): boolean {
    return this.#past.length > 0;
  }

  get canRedo(): boolean {
    return this.#future.length > 0;
  }

  get depth(): { readonly past: number; readonly future: number } {
    return { past: this.#past.length, future: this.#future.length };
  }

  setLimit(limit: number): void {
    this.#limit = Math.max(1, limit);
    while (this.#past.length > this.#limit) this.#past.shift();
  }

  /** A new step replaced `previous`: remember it, and the redo branch no longer follows. */
  record(previous: T): void {
    const top = this.#past[this.#past.length - 1];
    if (top === undefined || this.#key(top) !== this.#key(previous)) {
      this.#past.push(previous);
      if (this.#past.length > this.#limit) this.#past.shift();
    }
    this.#future = [];
  }

  /** Where `steps` undos would go, without going. */
  peekUndo(steps = 1): T | undefined {
    return this.#past[this.#past.length - steps];
  }

  peekRedo(steps = 1): T | undefined {
    return this.#future[this.#future.length - steps];
  }

  /** `steps` undos LANDED, leaving `from`: the states passed over become redo steps, in order. */
  undone(from: T, steps = 1): void {
    const passed = this.#past.splice(this.#past.length - steps, steps);
    this.#future.push(from);
    for (let i = passed.length - 1; i >= 1; i -= 1) this.#future.push(passed[i] as T);
  }

  /** `steps` redos LANDED, leaving `from`. */
  redone(from: T, steps = 1): void {
    const passed = this.#future.splice(this.#future.length - steps, steps);
    this.#past.push(from);
    for (let i = passed.length - 1; i >= 1; i -= 1) this.#past.push(passed[i] as T);
  }

  clear(): void {
    this.#past = [];
    this.#future = [];
  }
}

/** A pending history move: which way, and how many steps from the committed state. */
interface Move {
  readonly dir: 'undo' | 'redo';
  readonly steps: number;
}

interface Pending<S> {
  readonly id: number;
  /**
   * The history move this pending state includes: the move itself in flight, or a move a
   * change was made on top of (`on` is then the move's state, the change's undo step).
   */
  readonly move?: Move & { readonly on?: S };
  readonly state: S;
  /** The user's changes this pending state carries, oldest first: what a refusal reverts. */
  readonly labels: readonly string[];
  /** Whether landing it is an undo step. */
  readonly record: boolean;
}

export class StateOwner<S, V, E = never> {
  #committed: S;
  #rendered: S;
  #view: V | null = null;
  #pending: Pending<S> | null = null;
  /** The last change refused, until another starts: what `retryRefused` applies again. */
  #refused: Pending<S> | null = null;
  #seq = 0;
  readonly #run: Runner<S, V>;
  readonly #rules: StateRules<S, V, E>;
  readonly #stack: UndoStack<S>;
  readonly #abort: (() => void) | undefined;
  readonly #listeners = new Set<(event: StateEvent<V, E>) => void>();

  constructor(initial: S, run: Runner<S, V>, rules: StateRules<S, V, E>, options: OwnerOptions = {}) {
    this.#rules = rules;
    this.#committed = rules.fold(initial);
    this.#rendered = this.#committed;
    this.#run = run;
    this.#stack = new UndoStack((s: S) => rules.stateKey(s), options.historyLimit);
    this.#abort = options.abort;
  }

  /** What presentation paints: the change in flight, else the committed state. */
  get current(): S {
    return this.#pending?.state ?? this.#committed;
  }

  get committed(): S {
    return this.#committed;
  }

  /** The state of the view on screen. */
  get rendered(): S {
    return this.#rendered;
  }

  get view(): V | null {
    return this.#view;
  }

  /** A change is in flight. */
  get busy(): boolean {
    return this.#pending !== null;
  }

  get canUndo(): boolean {
    return this.#pending !== null || this.#stack.canUndo;
  }

  get canRedo(): boolean {
    return this.#stack.canRedo;
  }

  get historyDepth(): { readonly past: number; readonly future: number } {
    return this.#stack.depth;
  }

  setHistoryLimit(limit: number): void {
    this.#stack.setLimit(limit);
  }

  clearHistory(): void {
    this.#stack.clear();
  }

  /** Told of every change of state: pending, committed, presentation, refused, cancelled. */
  subscribe(listener: (event: StateEvent<V, E>) => void): () => void {
    this.#listeners.add(listener);
    return () => this.#listeners.delete(listener);
  }

  /** Run the committed state and show it: the first load, a refresh after a snap. Not a step. */
  async refresh(): Promise<StateOutcome<V>> {
    return this.change((s) => s, { record: false, force: true, label: 'refresh' });
  }

  /**
   * Apply again the change that was last refused -- once what refused it is put right (the
   * person signed in again) -- as it was asked for: its state, its undo step, its labels.
   * Nothing refused since the last change started: the committed state is re-run.
   */
  async retryRefused(): Promise<StateOutcome<V>> {
    const refused = this.#refused;
    if (!refused) return this.refresh();
    this.#refused = null;
    return this.#transact(refused.state, refused.labels.join(', ') || 'refresh', refused.record, refused.move);
  }

  /**
   * Change the cube. `update` derives the next state from the CURRENT one (a change made
   * while another is in flight builds on it). Presentation commits at once; anything else is
   * a transaction.
   */
  async change(update: (state: S) => S, options: ChangeOptions = {}): Promise<StateOutcome<V>> {
    const base = this.current;
    const next = this.#rules.fold(update(base));
    // asking for what already is changes nothing: no query, no step (P2-127's rule, everywhere)
    if (!options.force && this.#rules.stateKey(next) === this.#rules.stateKey(base)) return { kind: 'nothing' };
    if (!options.force && this.#rules.queryKey(next) === this.#rules.queryKey(base)) {
      return this.#present(update, options);
    }
    if (!options.force && this.#rules.defer?.(next)) return this.#defer(next, undefined, options.record ?? true);
    return this.#transact(next, options.label ?? 'change', options.record ?? true);
  }

  /**
   * Cancel the change in flight: the screen stays on the committed state, and the query is
   * stopped. Nothing when nothing is in flight.
   */
  cancel(): StateOutcome<V> {
    const pending = this.#pending;
    if (!pending) return { kind: 'nothing' };
    this.#pending = null;
    this.#abort?.();
    this.#emit({ kind: 'cancelled', reverted: pending.labels });
    return { kind: 'cancelled' };
  }

  /**
   * Back one step. While the user's own change is in flight, undo cancels THAT change -- the
   * last thing they did -- and nothing more. While an undo is in flight, it goes one step
   * further back (two quick presses are two steps, never a cancel).
   */
  async undo(): Promise<StateOutcome<V>> {
    return this.#history('undo');
  }

  /** Forward one step, the same way round. */
  async redo(): Promise<StateOutcome<V>> {
    return this.#history('redo');
  }

  // ---------------------------------------------------------------------------------

  async #history(dir: 'undo' | 'redo'): Promise<StateOutcome<V>> {
    // a move in flight on its own (not a change made on top of one)
    const pendingMove = this.#pending?.move?.on === undefined ? this.#pending?.move : undefined;
    // the user's own change in flight: undo cancels it; redo continues from what landed
    if (this.#pending && !pendingMove) {
      if (dir === 'undo') return this.cancel();
      this.cancel();
    }
    // an undo in flight and another undo: one step further (and a redo cancels the undo)
    let steps = 1;
    if (pendingMove) {
      if (pendingMove.dir !== dir) return this.cancel();
      steps = pendingMove.steps + 1;
    }
    const stacked = dir === 'undo' ? this.#stack.peekUndo(steps) : this.#stack.peekRedo(steps);
    if (stacked === undefined) return { kind: 'nothing' };
    // the state the kind's own edits make of the committed one toward the step's: the one the move lands (W3)
    const target = this.#rules.toward ? this.#rules.toward(this.#committed, stacked) : stacked;
    const move: Move = { dir, steps };
    if (this.#rules.queryKey(target) !== this.#rules.queryKey(this.#committed)) {
      return this.#rules.defer?.(target) ? this.#defer(target, move, false) : this.#transact(target, dir, false, move);
    }
    // only presentation differs: no query (a move still in flight is dropped, its query stopped)
    this.#drop();
    const edits = this.#land(target, move, false, this.#rules.queryKey(target) === this.#rules.queryKey(this.#rendered));
    this.#represent();
    this.#emit({ kind: 'presentation', moved: true, ...(edits !== undefined ? { edits } : {}) });
    return { kind: 'applied' };
  }

  /** The view on screen is not of the committed state's query (a deferred change waits for refresh). */
  get stale(): boolean {
    return this.#rules.queryKey(this.#committed) !== this.#rules.queryKey(this.#rendered);
  }

  /** Drop the change in flight, its query stopped. */
  #drop(): void {
    if (!this.#pending) return;
    this.#pending = null;
    this.#abort?.();
  }

  /** Commit WITHOUT a query (`StateRules.defer`): the view on screen stays the old state's. */
  #defer(next: S, move: Pending<S>['move'], record: boolean): StateOutcome<V> {
    this.#drop();
    this.#land(next, move, record, false);
    this.#emit({ kind: 'deferred' });
    return { kind: 'applied' };
  }

  /** The view laid out again for the state on screen (a presentation change). */
  #represent(): void {
    if (this.#rules.represent && this.#view !== null) this.#view = this.#rules.represent(this.#rendered, this.#view);
  }

  /** The one place the committed state moves with the history: stacks first, then the state. For a move, the kind's own
   *  edits from the committed state before to the one landed (`StateRules.between`), for the event to carry. */
  #land(state: S, move: Pending<S>['move'], record = false, rendered = true): readonly E[] | undefined {
    const from = this.#committed;
    if (move) {
      if (move.dir === 'undo') this.#stack.undone(from, move.steps);
      else this.#stack.redone(from, move.steps);
    }
    if (record) this.#stack.record(move?.on ?? from);
    this.#committed = state;
    if (rendered) this.#rendered = state;
    return move && this.#rules.between ? this.#rules.between(from, state) : undefined;
  }

  /**
   * Presentation: laid over committed, rendered and pending alike (it changes none of their
   * queries), one undo step, no query. Should laying it over one of them change that one's
   * query -- a change that is presentation only relative to the pending state -- the whole
   * change is a transaction instead.
   */
  #present(update: (state: S) => S, options: ChangeOptions): StateOutcome<V> | Promise<StateOutcome<V>> {
    // While a history move is in flight the stacks are that move's: the presentation change
    // rides the move (it is on both sides of it) rather than being a step of its own.
    const committed = this.#rules.fold(update(this.#committed));
    const rendered = this.#rules.fold(update(this.#rendered));
    if (this.#rules.queryKey(committed) !== this.#rules.queryKey(this.#committed) || this.#rules.queryKey(rendered) !== this.#rules.queryKey(this.#rendered)) {
      const next = this.#rules.fold(update(this.current));
      return this.#rules.defer?.(next)
        ? this.#defer(next, undefined, options.record ?? true)
        : this.#transact(next, options.label ?? 'change', options.record ?? true);
    }
    if ((options.record ?? true) && !this.#pending?.move) this.#stack.record(this.#committed);
    this.#committed = committed;
    this.#rendered = rendered;
    const pending = this.#pending;
    if (pending) {
      const on = pending.move?.on;
      this.#pending = {
        ...pending,
        state: this.#rules.fold(update(pending.state)),
        ...(pending.move && on ? { move: { ...pending.move, on: this.#rules.fold(update(on)) } } : {}),
      };
    }
    this.#represent();
    this.#emit({ kind: 'presentation', moved: false });
    return { kind: 'applied' };
  }

  async #transact(next: S, label: string, record: boolean, move?: Move): Promise<StateOutcome<V>> {
    const id = (this.#seq += 1);
    this.#refused = null;
    // A history move REPLACES a pending move rather than building on it; anything else is
    // carried: a change made on a pending one is reverted with it, and one made on a pending
    // move lands the move too.
    const carried = move ? null : this.#pending;
    const carriedMove = carried?.move ? { ...carried.move, on: carried.move.on ?? carried.state } : undefined;
    const pendingMove = move ?? carriedMove;
    this.#pending = {
      id,
      state: next,
      labels: [...(carried?.labels ?? []), ...(label === 'refresh' ? [] : [label])],
      record: record || (carried?.record ?? false),
      ...(pendingMove ? { move: pendingMove } : {}),
    };
    this.#emit({ kind: 'pending' });
    let out: V | Stale;
    try {
      out = await this.#run(next);
    } catch (error) {
      const pending = this.#pending;
      if (pending?.id !== id) return { kind: 'superseded' };
      this.#pending = null;
      this.#refused = pending;
      this.#emit({ kind: 'refused', error, reverted: pending.labels, moved: pending.move !== undefined });
      return { kind: 'refused', error, reverted: pending.labels };
    }
    const pending = this.#pending;
    if (isStale(out) || pending?.id !== id) return { kind: 'superseded' };
    // The pending state, not `next`: a presentation change made while it ran is laid on it --
    // on the state (`land`) and on the view, which was laid out for `next` (`represent`).
    const landed = this.#rules.land(pending.state, out);
    const view = this.#rules.represent && this.#rules.stateKey(pending.state) !== this.#rules.stateKey(next)
      ? this.#rules.represent(landed, out)
      : out;
    const edits = this.#land(landed, pending.move, pending.record);
    this.#view = view;
    this.#pending = null;
    this.#emit({ kind: 'committed', view, moved: pending.move !== undefined, ...(edits !== undefined ? { edits } : {}) });
    return { kind: 'applied', view };
  }

  #emit(event: StateEvent<V, E>): void {
    for (const listener of [...this.#listeners]) listener(event);
  }
}
