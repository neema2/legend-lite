// One query being edited: its state (or, when the form cannot show it, its text), where it was
// saved, undo/redo, the parameter values a run uses, and the last run's result. Panels subscribe
// and redraw; every edit goes through `update`, which records history and marks the query changed.

import type { Lambda } from '../../../pure-protocol/src/index.ts';
import { toJson } from '../../../pure-protocol/src/index.ts';
import type { LoadedProject } from './context.ts';
import type { ExecutionResult, Query } from '../backend/wire.ts';
import type { QueryState, Value } from '../builder/state.ts';

/** A query the form cannot show: kept as its lambda, still run, saved and edited as text. */
export interface TextOnly {
  readonly lambda: Lambda;
  readonly reason: string;
}

export type RunState =
  | { readonly status: 'idle' }
  | { readonly status: 'running'; readonly started: number; readonly abort: AbortController }
  | { readonly status: 'done'; readonly result: ExecutionResult; readonly ms: number; readonly limit: number | undefined; readonly queryHash: string }
  /** The results are a DataCube over the query (`app/cube.ts`); it runs its own queries. */
  | { readonly status: 'cube'; readonly queryHash: string }
  | { readonly status: 'error'; readonly message: string };

export type Change = 'query' | 'run' | 'saved' | 'params';

const HISTORY = 50;

export class Session {
  readonly project: LoadedProject;
  #query: QueryState;
  #text: TextOnly | undefined;
  #saved: Query | undefined;
  #savedHash: string;
  readonly #undo: QueryState[] = [];
  readonly #redo: QueryState[] = [];
  readonly paramValues = new Map<string, Value>();
  /** Opened from a share link: its name, offered when it is saved (it is not saved anywhere yet). */
  sharedAs: string | undefined;
  #run: RunState = { status: 'idle' };
  readonly #listeners = new Set<(c: Change) => void>();

  constructor(project: LoadedProject, query: QueryState, saved?: Query, text?: TextOnly) {
    this.project = project;
    this.#query = query;
    this.#saved = saved;
    this.#text = text;
    // a new query counts as unchanged until something is added to it
    this.#savedHash = this.hash();
  }

  get query(): QueryState { return this.#query; }
  get text(): TextOnly | undefined { return this.#text; }
  get saved(): Query | undefined { return this.#saved; }
  get run(): RunState { return this.#run; }
  get canUndo(): boolean { return this.#undo.length > 0; }
  get canRedo(): boolean { return this.#redo.length > 0; }

  /** A fingerprint of what would be saved: the query (or its text) -- not results or UI state. */
  hash(): string {
    return this.#text ? `text:${toJson(this.#text.lambda)}` : hashOf(this.#query);
  }

  get changed(): boolean {
    return this.hash() !== this.#savedHash;
  }

  subscribe(f: (c: Change) => void): () => void {
    this.#listeners.add(f);
    return () => this.#listeners.delete(f);
  }

  #emit(c: Change): void {
    for (const f of [...this.#listeners]) f(c);
  }

  /** Edit the query; recorded for undo unless `record` is false (a keystroke that will be merged). */
  update(f: (q: QueryState) => QueryState, record = true): void {
    const next = f(this.#query);
    if (next === this.#query) return;
    if (record) {
      this.#undo.push(this.#query);
      if (this.#undo.length > HISTORY) this.#undo.shift();
      this.#redo.length = 0;
    }
    this.#query = next;
    this.#text = undefined;
    this.#emit('query');
  }

  undo(): void {
    const prev = this.#undo.pop();
    if (!prev) return;
    this.#redo.push(this.#query);
    this.#query = prev;
    this.#emit('query');
  }

  redo(): void {
    const next = this.#redo.pop();
    if (!next) return;
    this.#undo.push(this.#query);
    this.#query = next;
    this.#emit('query');
  }

  /** The query became text-only (the form cannot show it), or back to form (`undefined`). */
  setText(text: TextOnly | undefined, query?: QueryState): void {
    if (query) {
      this.#undo.push(this.#query);
      this.#query = query;
    }
    this.#text = text;
    this.#emit('query');
  }

  markSaved(q: Query): void {
    this.#saved = q;
    this.#savedHash = this.hash();
    this.#emit('saved');
  }

  /** Kept by an embedding host (Studio's service): what is shown now is what it holds. */
  markKept(): void {
    this.#savedHash = this.hash();
    this.#emit('saved');
  }

  setParam(name: string, value: Value | undefined): void {
    if (value === undefined) this.paramValues.delete(name);
    else this.paramValues.set(name, value);
    this.#emit('params');
  }

  setRun(r: RunState): void {
    if (this.#run.status === 'running' && r.status !== 'running') this.#run.abort.abort();
    this.#run = r;
    this.#emit('run');
  }

  /** Is the shown result from an earlier version of the query? */
  get stale(): boolean {
    return (this.#run.status === 'done' || this.#run.status === 'cube') && this.#run.queryHash !== this.hash();
  }
}

function hashOf(q: QueryState): string {
  return JSON.stringify(q, (_k, v: unknown) => (v instanceof Map ? [...v] : v));
}
