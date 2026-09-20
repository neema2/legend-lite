// The planner, in the browser: Pure grammar out, SQL back, no server.
//
// This is the SAME planner as `LegendLitePlanner`, not a second one.
// That client POSTs to /engine/plan, whose handler calls
// `Compiler.plan(model, query, runtime)`; this one calls that exact
// method inside a WebAssembly build of legend-lite. Only the transport
// differs, which is the whole point -- a TypeScript reimplementation
// would be a second thing that has to agree with the first about null
// ordering, type coercion and aggregate semantics.
//
// Verified: a 69-query differential (research/wasm/differential.mjs)
// runs the same corpus through the WASM module and through the JVM and
// compares every answer, refusals included. 69/69 byte-identical --
// same SQL, same exception class, same message, same source positions.
//
// What this buys: the cached-client plane stops needing a server at
// all. Snapshot -> Pure -> SQL -> DuckDB-WASM, entirely in the tab.
//
// What it costs: ~1.5 MB gzipped of module, and roughly 4x the JVM's
// planning time -- about 10ms per plan rather than 2.5ms. DataCube
// plans once per user action, not per keystroke, so 10ms sits far
// inside a frame budget. The cold cost is the one to design around:
// the first plan pays ~550ms for class initialisation and parsing the
// Pure prelude, which is why `warmUp()` exists and why the demo calls
// it while the user is still looking at an empty grid.

import type { Planner } from './cube.ts';
import { PlanError } from './planner.ts';
import type { LevelScope } from './serialize.ts';
import type { CubeSnapshot } from './snapshot.ts';

/** The subset of TeaVM's module surface this file uses. */
interface TeavmModule {
  readonly exports: {
    planOrError(model: string, query: string, runtime: string): string;
    warmModel(model: string): number;
  };
}

interface TeavmRuntime {
  load(
    src: string,
    options?: {
      stackDeobfuscator?: { enabled: boolean };
      installImports?(imports: Record<string, unknown>): void;
    },
  ): Promise<TeavmModule>;
}

export interface WasmPlannerOptions {
  /** Pure model source: database, connection, runtime. */
  readonly model: string;
  /** Runtime to plan against, e.g. 'trades::RT'. */
  readonly runtime: string;
  /**
   * Directory holding `classes.wasm` and `wasm-gc-module-runtime.js`,
   * as `npm run planner:vendor` copies them. Trailing slash optional.
   */
  readonly assetBaseUrl?: string;
  /**
   * Cache plans by grammar text. Safe because planning is pure: the
   * same grammar and runtime always lower to the same SQL. Worth it
   * because scrolling re-issues structurally identical queries.
   */
  readonly cache?: boolean;
  /** Injectable for tests; defaults to a dynamic import of the URL. */
  readonly loadRuntime?: (url: string) => Promise<TeavmRuntime>;
  /**
   * Run the module on a WORKER, loaded from this URL.
   *
   * Strongly preferred in a browser. Building the boot layer is
   * ~600ms of synchronous WebAssembly with no yield point, so on the
   * main thread it is a visible freeze — and measurably worse than
   * that: starting it early to "overlap" DuckDB instead starved
   * DuckDB's own startup, pushing it from 409ms to 1038ms and making
   * the page slower. Two tasks do not overlap when one never yields.
   *
   * Omit it off the browser, where there is no UI to block: the Node
   * differential harnesses run the module in-thread.
   */
  readonly workerUrl?: string;
}

/**
 * Thrown when the module itself cannot be loaded.
 *
 * <p>Deliberately NOT a `PlanError`. A `PlanError` means the planner
 * answered and the answer was a refusal -- the user's query is wrong,
 * and the message is worth showing them. This means the planner never
 * ran, which is an operational fault: a missing asset, a browser
 * without WASM-GC. Collapsing the two would file every failed deploy
 * as a bad query.
 */
export class PlannerUnavailableError extends Error {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options);
    this.name = 'PlannerUnavailableError';
  }
}

export class WasmPlanner implements Planner {
  #options: WasmPlannerOptions;
  readonly #cache = new Map<string, string>();
  #module: Promise<TeavmModule> | undefined;
  #worker: Worker | undefined;
  readonly #pending = new Map<number, {
    resolve(answer: string): void;
    reject(e: unknown): void;
  }>();
  #nextId = 1;

  constructor(options: WasmPlannerOptions) {
    this.#options = options;
  }

  /** True when the module should run off the main thread. */
  #useWorker(): boolean {
    return this.#options.workerUrl !== undefined
      && typeof Worker !== 'undefined';
  }

  #ensureWorker(): Worker {
    if (this.#worker) return this.#worker;
    const w = new Worker(this.#options.workerUrl!, { type: 'module' });
    w.onmessage = (e: MessageEvent<{
      id: number; ok: boolean; answer?: string; error?: string;
    }>) => {
      const waiting = this.#pending.get(e.data.id);
      if (!waiting) return;
      this.#pending.delete(e.data.id);
      if (e.data.ok) waiting.resolve(e.data.answer ?? '');
      else {
        waiting.reject(new PlannerUnavailableError(
          `the planner worker failed: ${e.data.error}`));
      }
    };
    w.onerror = (e: ErrorEvent) => {
      // A worker that dies takes every in-flight request with it, and
      // leaving them pending would hang the grid rather than fail it.
      const dead = new PlannerUnavailableError(
        `the planner worker died: ${e.message}`);
      for (const waiting of this.#pending.values()) waiting.reject(dead);
      this.#pending.clear();
      this.#worker = undefined;
    };
    this.#worker = w;
    return w;
  }

  #ask(request: Record<string, unknown>): Promise<string> {
    const w = this.#ensureWorker();
    const id = this.#nextId++;
    return new Promise<string>((resolve, reject) => {
      this.#pending.set(id, { resolve, reject });
      w.postMessage({ ...request, id, base: this.#base() });
    });
  }

  /** Release the worker. The grid owns one planner for its lifetime,
   *  so this is for tests and for a host that tears a cube down. */
  dispose(): void {
    this.#worker?.terminate();
    this.#worker = undefined;
    this.#pending.clear();
  }

  /**
   * Where `classes.wasm` and the TeaVM runtime live, as an absolute URL.
   *
   * A relative specifier would be ambiguous and quietly wrong: dynamic
   * `import()` resolves one against the IMPORTING MODULE, while
   * `WebAssembly` fetches resolve against the document. Bundled into
   * `demo/bundle.js` those are the same place; in a source tree they
   * are not, and the two halves of the module would load from
   * different directories. Resolving against the document (or, off
   * the browser, the process's working directory) gives one answer
   * for both.
   */
  #base(): string {
    const raw = this.#options.assetBaseUrl ?? './vendor/';
    const withSlash = raw.endsWith('/') ? raw : `${raw}/`;
    const doc = (globalThis as { document?: { baseURI?: string } }).document;
    const against = doc?.baseURI
      ?? (globalThis as { location?: { href?: string } }).location?.href
      ?? `file://${(globalThis as { process?: { cwd(): string } })
        .process?.cwd() ?? ''}/`;
    try {
      return new URL(withSlash, against).href;
    } catch {
      return withSlash;
    }
  }

  /**
   * Load and instantiate the module, at most once.
   *
   * The promise is memoised rather than a `loaded` boolean, so that
   * concurrent first calls -- which is exactly what an initial render
   * produces -- share one 4 MB fetch instead of racing several.
   */
  #load(): Promise<TeavmModule> {
    if (this.#module) return this.#module;
    const base = this.#base();
    const runtimeUrl = `${base}wasm-gc-module-runtime.js`;
    // TeaVM's loader branches on the host: in a browser it fetches the
    // string, under Node it opens it as a FILESYSTEM PATH. A file: URL
    // therefore works for the `import()` above and fails for this,
    // with an ENOENT that surfaces as "could not instantiate" and
    // reads like a missing WASM-GC feature. Hand each the form it
    // actually takes.
    const wasmUrl = base.startsWith('file:')
      ? decodeURIComponent(new URL(`${base}classes.wasm`).pathname)
      : `${base}classes.wasm`;
    const importRuntime = this.#options.loadRuntime
      ?? ((url: string) => import(/* @vite-ignore */ url) as Promise<TeavmRuntime>);

    this.#module = (async () => {
      let runtime: TeavmRuntime;
      try {
        runtime = await importRuntime(runtimeUrl);
      } catch (cause) {
        throw new PlannerUnavailableError(
          `could not load the planner runtime from ${runtimeUrl}`
            + ' — run `npm run planner:vendor`',
          { cause },
        );
      }
      try {
        return await runtime.load(wasmUrl, {
          stackDeobfuscator: { enabled: false },
          installImports(imports: Record<string, unknown>) {
            // The module writes compiler diagnostics to stdout/stderr.
            // Left unbound they reach the host console one CHARACTER
            // at a time, which is unreadable; the messages that matter
            // come back through planOrError's return value anyway.
            imports.teavmConsole = { putcharStdout() {}, putcharStderr() {} };
          },
        });
      } catch (cause) {
        // Quote the underlying failure. Without it this message names
        // the likeliest cause (a runtime without WebAssembly GC) and
        // is confidently wrong whenever the real reason is anything
        // else — a missing file, a bad path — which sends the reader
        // hunting for a browser bug that is not there.
        // Name the likeliest FIX, not just the failure. The two
        // causes need different actions and the message has to point
        // at the right one: an un-vendored checkout 404s here, while
        // a browser without WebAssembly GC rejects a module that
        // downloaded perfectly.
        const detail = cause instanceof Error ? cause.message : String(cause);
        const looksMissing = /404|not ok|not found|ENOENT|status code/i
          .test(detail);
        throw new PlannerUnavailableError(
          `could not instantiate the planner module at ${wasmUrl}: ${detail}`
            + (looksMissing
              ? ' — run `npm run planner:vendor` to put it there'
              : ' — this runtime may lack WebAssembly GC'),
          { cause },
        );
      }
    })();
    // A failed load must not be cached as a permanent verdict: a
    // retry after a transient network fault should be allowed to work.
    this.#module.catch(() => {
      this.#module = undefined;
    });
    return this.#module;
  }

  /**
   * Pay the cold cost before the user can feel it.
   *
   * Loading the module is NOT warming it -- a mistake this method
   * made in its first version, which browser timings caught.
   * Instantiate costs ~45ms and resolved almost at once, while the
   * ~1.1s that actually makes a first plan slow sat in static
   * initialisers and a content-addressed cache, still on the
   * critical path and still after DuckDB. The marks read
   * `planner-ready` at 134ms and the first row at 1058ms, which is
   * the shape of a warm-up that warms nothing.
   *
   * So this also builds the boot layer -- the Pure prelude, the
   * system metamodel, and both resolved and normalized -- against
   * the real model, and throws the result away. `boot` calls it
   * concurrently with DuckDB's own startup, so the cost lands inside
   * a wait the page was making anyway.
   *
   * Safe to call repeatedly and safe never to call at all; `plan`
   * loads on demand either way.
   */
  async warmUp(): Promise<void> {
    if (this.#useWorker()) {
      await this.#ask({ kind: 'warm', model: this.#options.model });
      return;
    }
    const module = await this.#load();
    module.exports.warmModel(this.#options.model);
  }

  async plan(
    pureGrammar: string,
    _snapshot: CubeSnapshot,
    _scope?: LevelScope,
    signal?: AbortSignal,
  ): Promise<string> {
    const useCache = this.#options.cache !== false;
    if (useCache) {
      const hit = this.#cache.get(pureGrammar);
      if (hit !== undefined) return hit;
    }

    // Planning inside the module is synchronous and uninterruptible,
    // so an abort cannot stop it -- but it can stop a stale answer
    // reaching the grid. Check on both sides of the call: before, to
    // skip work already known to be pointless; after, because the
    // user may have moved on while it ran.
    if (signal?.aborted) throw signal.reason ?? new Error('aborted');

    const answer = this.#useWorker()
      ? await this.#ask({
        kind: 'plan',
        model: this.#options.model,
        query: pureGrammar,
        runtime: this.#options.runtime,
      })
      : (await this.#load()).exports.planOrError(
        this.#options.model,
        pureGrammar,
        this.#options.runtime,
      );

    if (signal?.aborted) throw signal.reason ?? new Error('aborted');

    // "OK\n<sql>" or "ERR\n<exception class>\n<message>". Failure
    // travels in the return value rather than as a thrown Java
    // exception so that the answer does not depend on how TeaVM
    // bridges throwables into JS -- see research/wasm/README.md.
    const nl = answer.indexOf('\n');
    const tag = nl < 0 ? answer : answer.slice(0, nl);
    const rest = nl < 0 ? '' : answer.slice(nl + 1);

    if (tag === 'OK') {
      if (useCache) this.#cache.set(pureGrammar, rest);
      return rest;
    }
    if (tag === 'ERR') {
      // Drop the exception class and keep the compiler's own message:
      // the same text the HTTP planner surfaces, so the two transports
      // are indistinguishable to the user and to the UI that renders
      // the failure.
      const split = rest.indexOf('\n');
      const message = split < 0 ? rest : rest.slice(split + 1);
      throw new PlanError(message || 'the planner refused the query',
        pureGrammar);
    }
    throw new PlannerUnavailableError(
      `the planner module returned an unrecognised answer: ${
        JSON.stringify(answer.slice(0, 120))}`,
    );
  }

  /**
   * Point the planner at a DIFFERENT model, e.g. one inferred from an
   * uploaded file.
   *
   * The plan cache is keyed by grammar text alone, which is only
   * sound while the model is fixed: the same
   * `#>{local::DB.t}#->select(~[a])` lowers to different SQL against
   * a different table. So the cache is dropped here -- a stale entry
   * would be wrong SQL, not merely a slow query.
   *
   * The module is NOT reloaded. Its boot layer is content-addressed
   * by the Pure prelude, which has not changed, so switching models
   * costs one graph build rather than another 4 MB download and
   * ~600ms of boot.
   */
  useModel(model: string, runtime: string): void {
    this.#options = { ...this.#options, model, runtime };
    this.#cache.clear();
  }

  /** Cached plan count, for tests and diagnostics. */
  get cacheSize(): number {
    return this.#cache.size;
  }
}
