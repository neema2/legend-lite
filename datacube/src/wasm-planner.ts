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
  readonly #options: WasmPlannerOptions;
  readonly #cache = new Map<string, string>();
  #module: Promise<TeavmModule> | undefined;

  constructor(options: WasmPlannerOptions) {
    this.#options = options;
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
   * The first plan is ~550ms -- class initialisation plus parsing the
   * Pure prelude -- against ~10ms warm. Calling this at startup moves
   * that off the first interaction. It is safe to call repeatedly and
   * safe never to call at all; `plan` loads on demand either way.
   */
  async warmUp(): Promise<void> {
    await this.#load();
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

    const module = await this.#load();

    // Planning inside the module is synchronous and uninterruptible,
    // so an abort cannot stop it -- but it can stop a stale answer
    // reaching the grid. Check on both sides of the call: before, to
    // skip work already known to be pointless; after, because ~10ms
    // is long enough for the user to have moved on.
    if (signal?.aborted) throw signal.reason ?? new Error('aborted');

    const answer = module.exports.planOrError(
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

  /** Cached plan count, for tests and diagnostics. */
  get cacheSize(): number {
    return this.#cache.size;
  }
}
