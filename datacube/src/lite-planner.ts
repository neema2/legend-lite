// The planner, in the browser: Pure grammar out, SQL back, no server.
//
// This is the SAME planner as `UpstreamPlanner`, not a second one.
// That client POSTs to legend-lite's `pure/v1/execution/generatePlan`, whose handler calls
// `Compiler.plan(model, query, runtime)`; this one calls that exact
// method inside a WebAssembly build of legend-lite. Only the transport
// differs, which is the whole point -- a TypeScript reimplementation
// would be a second thing that has to agree with the first about null
// ordering, type coercion and aggregate semantics.
//
// Verified: a 69-query differential (wasm/differential.mjs)
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

import { element, fn, fromJson, lambda, readLambda, readValueSpecification, toJson, type Lambda } from '../../pure-protocol/src/index.ts';
import type { Planner } from './cube.ts';
import type { CatalogColumn, InferOptions, InferredModel, ModelTable, TableModels, TablesModel } from './infer.ts';
import { PlanError, type ModelOptions } from './planner.ts';
import { writerBodies } from '../../model-changes/src/wire.ts';
import type { AssociationAction, ClassAction, DataSpaceAction, GridSource, ModelAnswer, ModelWriter } from './model-writer.ts';
import type { PrintStyle } from '../../engine-client/src/pure-v1.ts';
import { relationColumns, type Plan, type PlanColumn } from '../../engine-client/src/relation-type.ts';
import type { SeedTable, TableSeed } from '../../engine-client/src/model-data.ts';
import type { ProjectDataStatements } from '../../engine-client/src/project-data.ts';
import type { HostOperation } from '../../engine-client/src/host-operations.ts';
import { answer as answerOf, type PlannerModule } from '../../engine-client/src/legend/planner-answer.ts';
import { callsLibrary, loadLibrary, withLibrary } from './pure-library.ts';
import type { PlannerWorker, RequestBody, Response } from '../../engine-client/src/legend/planner-protocol.ts';

interface TeavmRuntime {
  load(
    src: string,
    options?: {
      stackDeobfuscator?: { enabled: boolean };
      installImports?(imports: Record<string, unknown>): void;
    },
  ): Promise<PlannerModule>;
}

export interface LitePlannerOptions {
  /** Pure model source: database, connection, runtime. */
  readonly model: string;
  /** Runtime to plan against, e.g. 'trades::RT'. */
  readonly runtime: string;
  /** The mapping a class source reads through: planned as `->from(mapping, runtime)` (ModelOptions). */
  readonly mapping?: string;
  /** The model's enumerations (ModelOptions). */
  readonly enumerations?: readonly string[];
  /**
   * Directory holding `classes.wasm` and `wasm-gc-module-runtime.js`,
   * as `bazel build //datacube:site` copies them. Trailing slash optional.
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
  /**
   * Run the module on a worker the HOST makes: one that answers engine-client's planner-protocol.ts, as its planner-worker.ts
   * does in a tab -- Python's headless host answers each with the native library's function of the same name
   * (docs/DATACUBE_HEADLESS_PYTHON_DESIGN_2026_10_11.md §3). Given, it is used instead of a `Worker` from `workerUrl`.
   */
  readonly worker?: () => PlannerWorker;
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

/** What a planner and the planners `withModel` made from it share. */
interface Transport {
  module: Promise<PlannerModule> | undefined;
  worker: PlannerWorker | undefined;
  readonly pending: Map<number, { resolve(answer: string): void; reject(e: unknown): void }>;
  nextId: number;
  /** Which module holds the models held: one more each time a module, or a worker with its own, starts (a handle
   *  taken under another is no longer held by anything). */
  generation: number;
}

/**
 * legend-lite's own planner, in-process: named for what it is, not for how a tab delivers its module (WebAssembly). Its
 * module answers on a worker, or in-thread (docs/DATACUBE_HEADLESS_PYTHON_DESIGN_2026_10_11.md §3).
 */
export class LitePlanner implements Planner, TableModels, ModelWriter {
  #options: LitePlannerOptions;
  readonly #cache = new Map<string, Plan>();
  readonly #types = new Map<string, PlanColumn[]>();
  /**
   * The module and its worker: SHARED by every planner `withModel` made from this one, since
   * each request carries its own model (planner-worker.ts). One worker, one boot layer.
   */
  #t: Transport = { module: undefined, worker: undefined, pending: new Map(), nextId: 1, generation: 0 };
  /** Made by `withModel`: the worker is its maker's, and `dispose` leaves it running. */
  #borrowed = false;
  /** This planner's model, held by the module (compiled once, every plan against it), and under which module. */
  #held: { readonly handle: Promise<number>; readonly generation: number } | undefined;
  /** A planner over this one's model and DataCube's Pure library after it (pure-library.ts), made when a query first
   *  calls the library -- its text fetched then, a chunk of its own: what plans such a query, this one's model held as
   *  it is for every other. */
  #library: Promise<LitePlanner> | undefined;
  /** It is that planner: its model has the library already. */
  #ofLibrary = false;

  constructor(options: LitePlannerOptions) {
    this.#options = options;
  }

  /**
   * A planner over ANOTHER model -- another source on the page (page/cube-page.ts) -- on the
   * same module and worker as this one, with caches of its own. Nothing is loaded twice.
   */
  withModel(model: string, runtime: string, how: ModelOptions = {}): LitePlanner {
    const other = new LitePlanner({ ...readsOf(this.#options), model, runtime, ...readsHow(how) });
    other.#t = this.#t;
    other.#borrowed = true;
    return other;
  }

  /**
   * One request of the planner protocol, answered by the worker or, in-thread, by the module through the same answer
   * the worker gives (engine-client's planner-answer.ts): one mapping of kinds to exports, wherever the module runs.
   */
  #call(request: RequestBody): Promise<string> {
    return this.#useWorker() ? this.#ask(request) : this.#load().then((m) => answerOf(m, request));
  }

  /** True when the module should run off the main thread. */
  #useWorker(): boolean {
    return this.#options.worker !== undefined
      || (this.#options.workerUrl !== undefined && typeof Worker !== 'undefined');
  }

  #ensureWorker(): PlannerWorker {
    if (this.#t.worker) return this.#t.worker;
    const w = this.#options.worker?.()
      ?? (new Worker(this.#options.workerUrl!, { type: 'module' }) as unknown as PlannerWorker);
    // its module is its own: none of the models held before are held by it
    this.#t.generation += 1;
    w.onmessage = (e: { readonly data: Response }) => {
      const waiting = this.#t.pending.get(e.data.id);
      if (!waiting) return;
      this.#t.pending.delete(e.data.id);
      if (e.data.ok) waiting.resolve(e.data.answer);
      else {
        waiting.reject(new PlannerUnavailableError(
          `the planner worker failed: ${e.data.error}`));
      }
    };
    w.onerror = (e: { readonly message: string }) => {
      // A worker that dies takes every in-flight request with it, and
      // leaving them pending would hang the grid rather than fail it.
      const dead = new PlannerUnavailableError(
        `the planner worker died: ${e.message}`);
      for (const waiting of this.#t.pending.values()) waiting.reject(dead);
      this.#t.pending.clear();
      this.#t.worker = undefined;
      // the models it held died with it: no planner's handle names anything now
      this.#t.generation += 1;
    };
    this.#t.worker = w;
    return w;
  }

  #ask(request: RequestBody): Promise<string> {
    const w = this.#ensureWorker();
    const id = this.#t.nextId++;
    return new Promise<string>((resolve, reject) => {
      this.#t.pending.set(id, { resolve, reject });
      w.postMessage({ ...request, id, base: this.#base() });
    });
  }

  /** Release the worker. The grid owns one planner for its lifetime,
   *  so this is for tests and for a host that tears a cube down. */
  dispose(): void {
    const released = this.#letGoAll();
    if (this.#borrowed) return;
    const worker = this.#t.worker;
    if (worker === undefined) {
      this.#t.pending.clear();
      return;
    }
    // a host's worker (Python's) keeps the models it holds in the host, not in itself: each let go before the worker is
    // ended (the Phase 6 review's 3: a typer's hold of a 4 MB model outlived it); a browser's takes its own with it
    void released.finally(() => {
      if (this.#t.worker !== worker) return;
      worker.terminate();
      this.#t.worker = undefined;
      this.#t.generation += 1;
      this.#t.pending.clear();
    });
  }

  /** Everything this planner holds let go -- its plans and types, its model, the planner over its library -- the
   *  release of each model asked: answered once every one is. */
  #letGoAll(): Promise<void> {
    this.#cache.clear();
    this.#types.clear();
    return Promise.all([this.#letGo(), this.#dropLibrary()]).then(() => undefined);
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
    const cwd = (globalThis as { process?: { cwd(): string } }).process?.cwd();
    const against = doc?.baseURI
      ?? (globalThis as { location?: { href?: string } }).location?.href
      // A DIRECTORY URL for the working directory. Spelled by
      // `pathToFileUrl`, not `file://${cwd}`: on Windows that gives
      // `file://C:\x\y/`, a URL whose HOST is `c` -- the base every
      // asset then resolved against.
      ?? (cwd === undefined ? 'file:///' : pathToFileUrl(`${cwd}/`, onWindows()));
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
  #load(): Promise<PlannerModule> {
    if (this.#t.module) return this.#t.module;
    const base = this.#base();
    const runtimeUrl = `${base}wasm-gc-module-runtime.js`;
    // TeaVM's loader branches on the host: in a browser it fetches the
    // string, under Node it opens it as a FILESYSTEM PATH. A file: URL
    // therefore works for the `import()` above and fails for this,
    // with an ENOENT that surfaces as "could not instantiate" and
    // reads like a missing WASM-GC feature. Hand each the form it
    // actually takes.
    const wasmUrl = base.startsWith('file:')
      ? fileUrlToPath(`${base}classes.wasm`, onWindows())
      : `${base}classes.wasm`;
    const importRuntime = this.#options.loadRuntime
      ?? ((url: string) => import(/* @vite-ignore */ url) as Promise<TeavmRuntime>);

    // a module of its own: none of the models held before are held by it
    this.#t.generation += 1;
    this.#t.module = (async () => {
      let runtime: TeavmRuntime;
      try {
        runtime = await importRuntime(runtimeUrl);
      } catch (cause) {
        throw new PlannerUnavailableError(
          `could not load the planner runtime from ${runtimeUrl}`
            + ' — run `bazel build //datacube:site`',
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
            // come back through each export's return value anyway.
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
              ? ' — run `bazel build //datacube:site` to put it there'
              : ' — this runtime may lack WebAssembly GC'),
          { cause },
        );
      }
    })();
    // A failed load must not be cached as a permanent verdict: a
    // retry after a transient network fault should be allowed to work.
    this.#t.module.catch(() => {
      this.#t.module = undefined;
    });
    return this.#t.module;
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
   * system metamodel, and both resolved and normalized -- and the
   * real model on it, which the module then holds for every plan
   * (#handle). `boot` calls it concurrently with DuckDB's own
   * startup, so the cost lands inside a wait the page was making
   * anyway.
   *
   * Safe to call repeatedly and safe never to call at all; `plan`
   * loads on demand either way.
   */
  async warmUp(): Promise<void> {
    // the model held: its compile is the work a first plan would do, kept for every plan after it
    await this.#handle();
  }

  /**
   * This planner's model as the module holds it: held on first need (its compile refused as a plan is, and then held
   * by no one), and again when the module holding it is not the one there now (a worker started anew).
   */
  #handle(): Promise<number> {
    if (this.#held !== undefined && this.#held.generation === this.#t.generation) return this.#held.handle;
    const model = this.#options.model;
    if (this.#useWorker()) this.#ensureWorker();
    const handle = this.#call({ kind: 'hold', model }).then((held) => Number(decode(held, model)));
    const held = { handle, generation: this.#t.generation };
    this.#held = held;
    handle.catch(() => {
      if (this.#held === held) this.#held = undefined;
    });
    return handle;
  }

  /** Let go of this planner's model, if it holds one under the module there now. */
  #letGo(): Promise<void> {
    const held = this.#held;
    this.#held = undefined;
    if (held === undefined || held.generation !== this.#t.generation) return Promise.resolve();
    return held.handle.then((handle) => {
      // a module or worker since replaced, or none running now, holds nothing to let go: never start one to do it
      if (held.generation !== this.#t.generation) return undefined;
      if (this.#useWorker()) {
        return this.#t.worker ? this.#ask({ kind: 'release', handle }).then(() => undefined) : undefined;
      }
      return this.#t.module ? this.#t.module.then((m) => { answerOf(m, { kind: 'release', handle }); }) : undefined;
    }).catch(() => {
      // a model never held, or a worker gone with the models it held: nothing to let go
    });
  }

  /**
   * Pure TEXT planned: the text entry (planHeldOrError), kept for callers that send grammar -- a
   * harness, another tool (the user, 2026-09-27: upstream accepts grammar). The cube sends trees
   * (`plan`).
   */
  async planText(pureGrammar: string, signal?: AbortSignal): Promise<Plan> {
    const useCache = this.#options.cache !== false;
    const key = `text:${pureGrammar}`;
    const hit = useCache ? this.#cache.get(key) : undefined;
    if (hit !== undefined) return hit;
    // Planning inside the module is synchronous and uninterruptible,
    // so an abort cannot stop it -- but it can stop a stale answer
    // reaching the grid. Check on both sides of the call.
    if (signal?.aborted) throw signal.reason ?? new Error('aborted');
    // the model and its runtime as they are now, together: a useModel while this waits changes neither
    const { model, runtime } = this.#options;
    const handle = await this.#handle();
    const answer = await this.#call({ kind: 'plan', handle, query: pureGrammar, runtime });
    if (signal?.aborted) throw signal.reason ?? new Error('aborted');
    // `{"sql", "type"}`: the SQL and the compiler's type of its result, in upstream's
    // RelationType shape -- the same renderer legend-lite's pure/v1 answers use.
    const body = JSON.parse(decode(answer, pureGrammar)) as { sql: string; type: unknown };
    const plan: Plan = { sql: body.sql, columns: relationColumns(body.type) };
    // kept only while it is the planner's model still: an answer for a model since replaced is not this one's
    if (useCache && this.#options.model === model) this.#cache.set(key, plan);
    return plan;
  }

  /** Pure TEXT typed, compile-only: the text entry (relationTypeHeldOrError). */
  async relationTypeText(pureGrammar: string, signal?: AbortSignal): Promise<PlanColumn[]> {
    const useCache = this.#options.cache !== false;
    const key = `text:${pureGrammar}`;
    const hit = useCache ? this.#types.get(key) : undefined;
    if (hit !== undefined) return hit;
    if (signal?.aborted) throw signal.reason ?? new Error('aborted');
    const { model } = this.#options;
    const handle = await this.#handle();
    const answer = await this.#call({ kind: 'relationType', handle, query: pureGrammar });
    if (signal?.aborted) throw signal.reason ?? new Error('aborted');
    const columns = relationColumns(JSON.parse(decode(answer, pureGrammar)));
    if (useCache && this.#options.model === model) this.#types.set(key, columns);
    return columns;
  }

  // ---- protocol trees: each the in-tab twin of a pure/v1 endpoint (T4a) ----

  /** The planner of `query`: this one, or -- a query calling DataCube's Pure library -- the one over this one's model
   *  with the library after it, on the same module and worker. */
  async #over(query: Lambda): Promise<LitePlanner> {
    if (this.#ofLibrary || !callsLibrary(query)) return this;
    if (this.#library === undefined) {
      const options = this.#options;
      const made = loadLibrary().then((library) => {
        const over = this.withModel(withLibrary(options.model, library), options.runtime, {
          ...(options.mapping === undefined ? {} : { mapping: options.mapping }),
          ...(options.enumerations === undefined ? {} : { enumerations: options.enumerations }),
        });
        over.#ofLibrary = true;
        return over;
      });
      this.#library = made;
      // a fetch of its text that failed is told to the query that asked, and asked again by the next
      made.catch(() => { if (this.#library === made) this.#library = undefined; });
    }
    return this.#library;
  }

  /** The planner over the library let go, once made. */
  #dropLibrary(): Promise<void> {
    const library = this.#library;
    this.#library = undefined;
    // the planner over the library borrows this one's module: its model let go on it, as this one's is
    return library === undefined ? Promise.resolve() : library.then((over) => over.#letGoAll(), () => undefined);
  }

  /** E9's twin: a query (a protocol tree) planned -- its SQL and the compiler's result type. */
  async plan(query: Lambda, signal?: AbortSignal): Promise<Plan> {
    const over = await this.#over(query);
    if (over !== this) return over.plan(query, signal);
    const { lambda: json, model, runtime } = await this.planRequest(query);
    const useCache = this.#options.cache !== false;
    const hit = useCache ? this.#cache.get(json) : undefined;
    if (hit !== undefined) return hit;
    if (signal?.aborted) throw signal.reason ?? new Error('aborted');
    const handle = await this.#handle();
    const answer = await this.#call({ kind: 'planJson', handle, lambda: json, runtime });
    if (signal?.aborted) throw signal.reason ?? new Error('aborted');
    const plan = this.readPlan(answer, query);
    if (useCache && this.#options.model === model) this.#cache.set(json, plan);
    return plan;
  }

  /**
   * A PROGRAM THAT WRITES planned (docs/DATACUBE_FLOW_MODE_2026_10_24.md M6, an Output into a table): `program` as an
   * execution plan, PlanJson's text (version 19) -- its writes' `Update`s and its answer's `TdsResult` of typed columns
   * in one `Sequence`, which the host's plan runner runs in one transaction and whose rows it writes as lite's JSON
   * rows (engine-client/src/plan-runner.ts; a plan names no output format since the result boundary program's phase
   * 4). Never cached: a write is planned for the one run that asks it.
   */
  async executionPlan(program: Lambda, signal?: AbortSignal): Promise<string> {
    // a program calling DataCube's Pure library planned over it, as a query is (plan)
    const over = await this.#over(program);
    if (over !== this) return over.executionPlan(program, signal);
    const { lambda: json, runtime } = await this.planRequest(program);
    if (signal?.aborted) throw signal.reason ?? new Error('aborted');
    const handle = await this.#handle();
    return decode(await this.#call({ kind: 'executionPlan', handle, lambda: json, runtime }), program);
  }

  /**
   * What a plan of `query` asks (planJson's request, its model as text where the planner holds it by a handle): what a
   * host that plans AND runs in one step sends (PlanAndRun on Python: src/services/python-host.ts), so its plan is the
   * one this planner would make.
   */
  async planRequest(query: Lambda): Promise<{ readonly lambda: string; readonly model: string; readonly runtime: string; readonly library?: string }> {
    const { model, runtime } = this.#options;
    // a query calling DataCube's Pure library: the library beside its model, which the host plans it over after the
    // model it holds of that text (Python's engine: the model admitted by its own rule, the library by its text)
    const library = !this.#ofLibrary && callsLibrary(query) ? { library: await loadLibrary() } : {};
    return { lambda: toJson(this.#overMapping(query)), model, runtime, ...library };
  }

  /** A plan's answer (planJson's, tagged) read as this planner reads its own: its SQL and the compiler's columns. */
  readPlan(answer: string, query: Lambda): Plan {
    const body = JSON.parse(decode(answer, query)) as { sql: string; type: unknown };
    return { sql: body.sql, columns: relationColumns(body.type) };
  }

  /**
   * E5 (`compilation/lambdaRelationType`): a query typed, compile-only. How the cube types its source and calculated
   * columns before any level query runs. Cached by the query's JSON, like plans.
   */
  async relationType(query: Lambda, signal?: AbortSignal): Promise<PlanColumn[]> {
    const over = await this.#over(query);
    if (over !== this) return over.relationType(query, signal);
    const json = toJson(query);
    const useCache = this.#options.cache !== false;
    const hit = useCache ? this.#types.get(json) : undefined;
    if (hit !== undefined) return hit;
    if (signal?.aborted) throw signal.reason ?? new Error('aborted');
    // the model held first, its compile kept for every question and plan after: the boundary answers a pure/v1 request
    // whose model text it holds over the held model, compiled no more (HeldModels.withText; a saved query of the 4 MB
    // trading model was typed by two requests of 5-6 s each, docs/DATACUBE_TIMESERIES_DESIGN_2026_10_25.md §11). A
    // model that does not compile is held by no one, and the request below says why in its own words.
    await this.#handle().catch(() => undefined);
    if (signal?.aborted) throw signal.reason ?? new Error('aborted');
    // the lambda's JSON spliced in as toJson wrote it, so its numbers keep their digits
    const body = `{"model":${JSON.stringify({ _type: 'text', code: this.#options.model })},"lambda":${json}}`;
    const answer = await this.#pureV1('/api/pure/v1/compilation/lambdaRelationType', '', body, query);
    if (signal?.aborted) throw signal.reason ?? new Error('aborted');
    const columns = relationColumns(JSON.parse(answer), undefined, new Set(this.#options.enumerations ?? []));
    if (useCache) this.#types.set(json, columns);
    return columns;
  }

  /**
   * One legend-engine pure/v1 call, answered by the planner as legend-lite's server answers it (its pure/v1 route):
   * the body of a 200, or the refusal's message as a PlanError. The grammar is asked so, not through private twins of
   * its endpoints (docs/PROTOCOL_PROGRAM_2026_10_05.md, invariant 5).
   */
  async #pureV1(path: string, query: string, body: string, subject: Lambda | string): Promise<string> {
    const answer = await this.#call({ kind: 'pureV1', path, query, body });
    return pureV1Body(decode(answer, subject), subject);
  }

  /**
   * THE MODEL'S WRITER (model-writer.ts; docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §15.6a): legend-lite's createClass,
   * createAssociation and createDataSpace, asked through pure/v1's routing as every compiler question is -- each one
   * call, atomic, over the project's files (`project`), each class over its grid's own source. Nothing is held: a model
   * is written once, against the project as it is then. This module writes on every plane: beside a server's planner,
   * too (demo/planners.ts).
   */
  createClass(project: string, pkg: string, cls: ClassAction): Promise<ModelAnswer> {
    return this.#writeModel('createClass', writerBodies.createClass(project, pkg, cls));
  }

  createAssociation(project: string, pkg: string, association: AssociationAction | undefined,
    classes: readonly ClassAction[] = []): Promise<ModelAnswer> {
    return this.#writeModel('createAssociation', writerBodies.createAssociation(project, pkg, association, classes));
  }

  createDataSpace(project: string, pkg: string, dataSpace: DataSpaceAction, classes: readonly ClassAction[] = [],
    associations: readonly AssociationAction[] = []): Promise<ModelAnswer> {
    return this.#writeModel('createDataSpace', writerBodies.createDataSpace(project, pkg, dataSpace, classes, associations));
  }

  async #writeModel(route: string, body: string): Promise<ModelAnswer> {
    return JSON.parse(await this.#pureV1(`/api/lite/v1/model/${route}`, '', body, body)) as ModelAnswer;
  }

  /** Where this planner's queries are planned from: its model, and the runtime they run on. */
  source(): GridSource {
    return { model: this.#options.model, runtime: this.#options.runtime };
  }

  /** E4 (`grammar/jsonToGrammar/lambda`): a query as Pure text, as upstream prints it. */
  async compose(query: Lambda, style: PrintStyle = 'PRETTY'): Promise<string> {
    return this.#pureV1('/api/pure/v1/grammar/jsonToGrammar/lambda', `renderStyle=${style}`, toJson(query), query);
  }

  /** A query printed for a person to read (the Planner's `print`): PRETTY unless asked. */
  print(query: Lambda, style: PrintStyle = 'PRETTY'): Promise<string> {
    return this.compose(query, style);
  }

  /**
   * The query as it is sent to be planned: over a mapping, `->from(mapping, runtime)` outermost (#overMapping); else as
   * it is -- what a host plans when it is handed a grid's query as text (headless grid.ts gridRelation).
   */
  sent(query: Lambda): Lambda {
    return this.#overMapping(query);
  }

  /**
   * Over a mapping, the query as Query sends it: `->from(mapping, runtime)` outermost. Without
   * one the runtime travels beside the query, as it always has here.
   */
  #overMapping(query: Lambda): Lambda {
    const mapping = this.#options.mapping;
    const body = query.body[0];
    if (mapping === undefined || body === undefined || query.body.length !== 1) return query;
    return lambda(query.parameters, fn('from', body, element(mapping), element(this.#options.runtime)));
  }

  /** A model's elements, as the compiler reads its text (E2, `grammar/grammarToJson/model`): data spaces, enumerations. */
  async modelElements(text: string): Promise<unknown[]> {
    const json = await this.#pureV1('/api/pure/v1/grammar/grammarToJson/model', 'returnSourceInformation=false', text, text);
    const elements = (JSON.parse(json) as { elements?: unknown }).elements;
    if (!Array.isArray(elements)) throw new PlanError('the model came back without elements', text);
    return elements;
  }

  /** E1 (`grammar/grammarToJson/lambda`): what a person typed, as its lambda, numbers exact. */
  async parse(text: string): Promise<Lambda> {
    return readLambda(await this.#pureV1('/api/pure/v1/grammar/grammarToJson/lambda', 'returnSourceInformation=false', text, text));
  }

  /**
   * A model's test data as the statements the server seeds DuckDB with, and each table's names as they spell them
   * (engine-client's SeedSource, model-data.ts): for each table `database` declares, its TableSeed, in order. The model
   * is the one given, not this planner's own.
   */
  async testDataSql(model: string, database: string, tables: readonly SeedTable[]): Promise<TableSeed[]> {
    const tablesJson = JSON.stringify(tables);
    const answer = await this.#call({ kind: 'testData', model, database, tables: tablesJson });
    return JSON.parse(decode(answer, `the test data of ${database}`)) as TableSeed[];
  }

  /**
   * THE MODEL FOR A TABLE, from the rows its CATALOG reports (structured: `catalogColumnsSql`):
   * legend-lite's one writer (planner.Boundary.tableModel, the module's tableModelOrError) -- the Database, read by the table's
   * own dialect, its connection and runtime; the conversions a copy applies, and the columns left out. The compiler decides every column; nothing here does. The
   * same function answers Python's frames.
   */
  async tableModel(columns: readonly CatalogColumn[], options: InferOptions): Promise<InferredModel> {
    const table = JSON.stringify({ ...options, columns });
    const answer = await this.#call({ kind: 'tableModel', table });
    const { source, ...written } = fromJson(decode(answer, `the columns of ${options.table}`)) as
      Omit<InferredModel, 'source'> & { readonly source: unknown; readonly accessor: string };
    const { accessor: _accessor, ...model } = written;
    return { ...model, source: readValueSpecification(source) };
  }

  /**
   * THE MODEL OF SEVERAL TABLES of one database (a join's inputs, read in one place): the same writer, given `tables`
   * (planner.Boundary.tableModel) -- each package's Database, one connection, one runtime over them all.
   */
  async tablesModel(tables: readonly ModelTable[], databaseType: string): Promise<TablesModel> {
    const answer = await this.#call({ kind: 'tableModel', table: JSON.stringify({ databaseType, tables }) });
    const written = fromJson(decode(answer, `the model of ${tables.map((t) => t.table).join(', ')}`)) as
      Omit<TablesModel, 'tables'> & { readonly tables: readonly (Omit<TablesModel['tables'][number], 'source'> & { readonly source: unknown; readonly accessor: string })[] };
    return {
      model: written.model,
      runtime: written.runtime,
      tables: written.tables.map(({ source, accessor: _accessor, ...t }) => ({ ...t, source: readValueSpecification(source) })),
    };
  }

  /** THE catalog question for one table of a DuckDB, its schema and table as SQL string literals. */
  async catalogColumnsSql(schema: string, table: string): Promise<string> {
    const answer = await this.#call({ kind: 'catalogColumnsSql', schema, table });
    return decode(answer, `the catalog of ${table}`);
  }

  /** What a session of the database runs before it is queried (the dialect's own setup: a DuckDB session in UTC). */
  async sessionSetup(databaseType: string): Promise<readonly string[]> {
    const answer = await this.#call({ kind: 'sessionSetup', databaseType });
    return JSON.parse(decode(answer, `a ${databaseType} session's setup`)) as string[];
  }

  /**
   * A project's own database on a database of that type (C5, Boundary.projectData; engine-client's project-data.ts):
   * its name and the statements that make it, point a connection at it, fill it from the model's Data elements, and
   * drop it. The model is the one given, not this planner's own.
   */
  async projectData(model: string, databaseType: string): Promise<ProjectDataStatements> {
    const answer = await this.#call({ kind: 'projectData', model, databaseType });
    return JSON.parse(decode(answer, `a project's database on ${databaseType}`)) as ProjectDataStatements;
  }

  /** A host's named operations on a database of that type (D5, Boundary.hostOperations): each one's statements. */
  async hostOperations(databaseType: string, operations: readonly HostOperation[]): Promise<string[][]> {
    const answer = await this.#call({ kind: 'hostOperations', databaseType, operations: JSON.stringify(operations) });
    return JSON.parse(decode(answer, `a host's operations on ${databaseType}`)) as string[][];
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
  useModel(model: string, runtime: string, how: ModelOptions = {}): void {
    if (model !== this.#options.model) void this.#letGo();
    void this.#dropLibrary();
    this.#options = { ...readsOf(this.#options), model, runtime, ...readsHow(how) };
    this.#cache.clear();
    this.#types.clear();
  }

  /** Cached plan count, for tests and diagnostics. */
  get cacheSize(): number {
    return this.#cache.size;
  }
}

/**
 * A `file:` URL as the path Node's filesystem opens -- Node's own
 * `url.fileURLToPath`, which this file cannot import: it is bundled for
 * the browser too, where `node:url` does not resolve. It only runs
 * under Node (a browser never hands the loader a `file:` URL).
 *
 * The URL's pathname alone is NOT the path on Windows: `/C:/x/y`
 * opened there is `C:\C:\x\y` (CI, 2026-09-23 — every case of the
 * WASM differential refused with that ENOENT). Pinned against
 * `fileURLToPath(url, { windows })` in both modes by
 * test/lite-planner.test.ts, on every platform.
 */
export function fileUrlToPath(url: string, windows: boolean): string {
  const u = new URL(url);
  if (u.protocol !== 'file:') throw new TypeError(`not a file: URL: ${url}`);
  const path = decodeURIComponent(u.pathname);
  if (!windows) {
    if (u.hostname !== '') throw new TypeError(`a file: URL with a host has no POSIX path: ${url}`);
    return path;
  }
  // A host is a UNC share (\\server\share\...); otherwise the path
  // must start with a drive letter, and loses the URL's leading slash.
  if (u.hostname !== '') return `\\\\${u.hostname}${path.replace(/\//g, '\\')}`;
  if (!/^\/[A-Za-z]:\//.test(path)) throw new TypeError(`a Windows file: URL needs a drive letter: ${url}`);
  return path.slice(1).replace(/\//g, '\\');
}

/**
 * A filesystem path as a `file:` URL -- Node's `url.pathToFileURL`, for
 * the same reason `fileUrlToPath` exists: this file cannot import
 * `node:url`. A trailing separator is kept, so a directory stays a
 * directory to resolve against. Pinned against Node's own in both
 * platform modes by test/lite-planner.test.ts.
 */
export function pathToFileUrl(path: string, windows: boolean): string {
  // Node's own steps: escape what `pathname` would not, then let URL
  // encode the rest.
  const url = new URL('file://');
  let p = path;
  if (windows) {
    p = p.replace(/\\/g, '/');
    if (p.startsWith('//')) {
      // A UNC path (\\server\share\...) is a URL with a host.
      const [host = '', ...rest] = p.slice(2).split('/');
      url.hostname = host;
      p = `/${rest.join('/')}`;
    } else {
      p = `/${p}`;
    }
  }
  p = p.replace(/%/g, '%25');
  if (!windows) p = p.replace(/\\/g, '%5C');
  url.pathname = p.replace(/\n/g, '%0A').replace(/\r/g, '%0D')
    .replace(/\t/g, '%09').replace(/#/g, '%23').replace(/\?/g, '%3F');
  return url.href;
}

/** Whether this is Node on Windows; false in a browser, which has no `process`. */
function onWindows(): boolean {
  const proc = (globalThis as { process?: { platform?: string } }).process;
  return proc?.platform === 'win32';
}

/**
 * A module answer: "OK\n<json>" or "ERR\n<kind>\n<message>" (the kind as the server names a refusal). Failure
 * travels in the return value rather than as a thrown Java exception so that the
 * answer does not depend on how TeaVM bridges throwables into JS -- see
 * wasm/README.md. A refusal keeps the compiler's own message: the same text the
 * HTTP planner surfaces, so the two transports are indistinguishable.
 */
/** A planner's options without what its model said: another model brings its own. */
function readsOf(o: LitePlannerOptions): LitePlannerOptions {
  const { mapping: _mapping, enumerations: _enumerations, ...rest } = o;
  return rest;
}

/** What a model says beside its text, as options (legend-lite types BIT Boolean itself: no bitColumns, S23). */
function readsHow(how: ModelOptions): Partial<LitePlannerOptions> {
  return {
    ...(how.mapping === undefined ? {} : { mapping: how.mapping }),
    ...(how.enumerations === undefined ? {} : { enumerations: how.enumerations }),
  };
}

/**
 * A pure/v1 answer (`<status>\n<media type>\n<body>`, as the planner's route folds it): the body of a 200; any other
 * status is the engine's refusal, `{message, ...}`, thrown as the planner refusing.
 */
function pureV1Body(answer: string, subject: Lambda | string): string {
  const first = answer.indexOf('\n');
  const second = answer.indexOf('\n', first + 1);
  if (first < 0 || second < 0) {
    throw new PlannerUnavailableError(
      `the planner module returned an unrecognised pure/v1 answer: ${JSON.stringify(answer.slice(0, 120))}`,
    );
  }
  const body = answer.slice(second + 1);
  if (answer.slice(0, first) === '200') return body;
  let message = body;
  try {
    const refusal = JSON.parse(body) as { message?: unknown };
    if (typeof refusal.message === 'string') message = refusal.message;
  } catch { /* not JSON: the body is the message */ }
  throw new PlanError(message || 'the planner refused the request', subject);
}

function decode(answer: string, subject: Lambda | string): string {
  const nl = answer.indexOf('\n');
  const tag = nl < 0 ? answer : answer.slice(0, nl);
  const rest = nl < 0 ? '' : answer.slice(nl + 1);
  if (tag === 'OK') return rest;
  if (tag === 'ERR') {
    const split = rest.indexOf('\n');
    const message = split < 0 ? rest : rest.slice(split + 1);
    throw new PlanError(message || 'the planner refused the query', subject);
  }
  throw new PlannerUnavailableError(
    `the planner module returned an unrecognised answer: ${JSON.stringify(answer.slice(0, 120))}`,
  );
}
