// Server mode: the ENGINE runs the query.
//
// The other two planes plan and execute in two steps -- Pure in, SQL
// out, then DuckDB-WASM runs the SQL in the tab. That is what
// `Planner` is for, and it is what upstream does for a CACHED source:
// `generatePlan`, take the SQL out of the plan, run it locally.
//
// An engine-backed source is the other shape entirely. Upstream posts
// the query to `/api/pure/v1/execution/execute` and renders the rows
// the engine sends back (`_runQuery`); the SQL it shows comes back as
// an execution ACTIVITY and is never run by the browser. The data
// lives wherever the engine's store points, which is the whole point
// of the mode: a cube over a database the tab cannot reach.
//
// So this is not a planner. Pure in, ROWS out.

import type { CubeSnapshot } from './snapshot.ts';
import type { LevelScope } from './serialize.ts';
import type { ResultColumn, ResultTable, Scalar } from './result.ts';

/** What a remote engine answers with. */
export interface RemoteResult {
  readonly rows: ResultTable;
  /**
   * The SQL the engine REPORTS having run, for display only.
   *
   * It arrives as an execution activity, after the fact. Running it
   * here would need the engine's own connection, which is the thing
   * this mode exists to avoid.
   */
  readonly sql: string;
}

export interface RemoteExecutor {
  execute(
    pureGrammar: string,
    snapshot: CubeSnapshot,
    scope?: LevelScope,
    signal?: AbortSignal,
  ): Promise<RemoteResult>;
}

export class RemoteExecutionError extends Error {
  readonly pure: string;
  constructor(message: string, pure: string) {
    super(message);
    this.name = 'RemoteExecutionError';
    this.pure = pure;
  }
}

export interface LegendEngineOptions {
  /** e.g. `http://127.0.0.1:6300`. */
  readonly baseUrl: string;
  /** The model, as Pure grammar. Parsed once, then reused. */
  readonly model: string;
  /** The runtime the query reads through, e.g. `trades::h2::RT`. */
  readonly runtime: string;
  /** Defaults to the global fetch; injectable for tests. */
  readonly fetch?: typeof fetch;
}

/** The protocol shapes this client touches, and only those. */
interface TdsResponse {
  readonly builder?: {
    readonly columns?: readonly {
      readonly name: string;
      readonly type?: string;
    }[];
  };
  readonly activities?: readonly { readonly sql?: string }[];
  readonly result?: {
    readonly columns?: readonly string[];
    readonly rows?: readonly { readonly values?: readonly Scalar[] }[];
  };
}

/**
 * The engine answers with its own type names, some of them paths.
 *
 * `meta::pure::precisePrimitives::Varchar` and `Float` describe the
 * same two things the rest of this product calls `String` and
 * `Float`, and the column model reads that name -- so normalise here
 * rather than teaching every reader the engine's spelling.
 */
export function pureTypeName(type: string | undefined): string {
  if (!type) return 'Unknown';
  const leaf = type.slice(type.lastIndexOf(':') + 1);
  switch (leaf) {
    case 'Varchar':
    case 'Char':
    case 'Text':
    case 'String':
      return 'String';
    case 'Float':
    case 'Double':
    case 'Decimal':
    case 'Numeric':
    case 'Number':
      return 'Float';
    case 'Integer':
    case 'Int':
    case 'TinyInt':
    case 'SmallInt':
    case 'BigInt':
      return 'Integer';
    case 'Boolean':
    case 'Bit':
      return 'Boolean';
    case 'StrictDate':
    case 'Date':
      return 'StrictDate';
    case 'DateTime':
    case 'Timestamp':
      return 'DateTime';
    default:
      return leaf;
  }
}

/** A TDS as the engine sends it, as a ResultTable. */
export function toResultTable(
  body: TdsResponse,
  epoch: number,
  elapsedMs: number,
): ResultTable {
  // THE BUILDER NAMES THE COLUMNS AND THEIR TYPES; `result.columns`
  // repeats the names alone. Read the builder, and fall back to the
  // names when a response carries no builder at all.
  const declared = body.builder?.columns;
  const names = declared?.map((c) => c.name)
    ?? body.result?.columns
    ?? [];
  const rows = body.result?.rows ?? [];
  const columns: ResultColumn[] = names.map((name, i) => {
    const values: Scalar[] = new Array(rows.length);
    for (let r = 0; r < rows.length; r++) {
      values[r] = (rows[r]?.values?.[i] ?? null) as Scalar;
    }
    return {
      name,
      type: pureTypeName(declared?.[i]?.type),
      values,
    };
  });
  return { columns, rowCount: rows.length, epoch, elapsedMs };
}

/**
 * Pure in, rows out, through a running legend-engine.
 *
 * Three calls the first time and two after: the MODEL is parsed once
 * (`grammarToJson/model`) and kept, because it does not change while
 * a cube is open and it is the biggest part of the payload; the query
 * is parsed each time (`grammarToJson/lambda`), because it changes
 * with every interaction; then `execution/execute`.
 */
export class LegendEngineExecutor implements RemoteExecutor {
  readonly #options: LegendEngineOptions;
  readonly #fetch: typeof fetch;
  #model: Promise<unknown> | null = null;

  constructor(options: LegendEngineOptions) {
    this.#options = options;
    this.#fetch = options.fetch ?? globalThis.fetch.bind(globalThis);
  }

  get baseUrl(): string {
    return this.#options.baseUrl.replace(/\/$/, '');
  }

  async execute(
    pureGrammar: string,
    snapshot: CubeSnapshot,
    _scope?: LevelScope,
    signal?: AbortSignal,
  ): Promise<RemoteResult> {
    // THE RUNTIME IS NAMED IN THE QUERY. Our own planners take it
    // out-of-band; a relation query handed to the engine carries it,
    // which is how `from` reads in every example upstream has.
    const pure = `${pureGrammar}->from(${this.#options.runtime})`;
    const started = Date.now();
    const model = await this.#modelContext(pure, signal);
    const lambda = await this.#post(
      '/grammar/grammarToJson/lambda', pure, pure, signal, true,
    );
    const body = (await this.#post('/execution/execute', {
      // vX_X_X carries the protocol models production versions lack
      // -- DuckDB's among them, which is upstream's reason too.
      clientVersion: 'vX_X_X',
      function: lambda,
      model,
      // REQUIRED. Without it the engine answers 500 with a
      // NullPointerException out of `processExecutionContext` rather
      // than naming the field it wanted.
      context: {
        _type: 'BaseExecutionContext',
        queryTimeOutInSeconds: 60,
        enableConstraints: true,
      },
    }, pure, signal)) as TdsResponse;

    return {
      rows: toResultTable(body, snapshot.epoch, Date.now() - started),
      sql: body.activities?.at(-1)?.sql ?? '',
    };
  }

  /** The parsed model, once per executor. */
  #modelContext(pure: string, signal?: AbortSignal): Promise<unknown> {
    this.#model ??= this.#post(
      '/grammar/grammarToJson/model', this.#options.model, pure, signal, true,
    ).catch((cause: unknown) => {
      // A failed parse must not be cached: the next query would get
      // the same rejection with no attempt made.
      this.#model = null;
      throw cause;
    });
    return this.#model;
  }

  async #post(
    path: string,
    body: unknown,
    pure: string,
    signal: AbortSignal | undefined,
    text = false,
  ): Promise<unknown> {
    const url = `${this.baseUrl}/api/pure/v1${path}`;
    let response: Response;
    try {
      response = await this.#fetch(url, {
        method: 'POST',
        headers: {
          'Content-Type': text ? 'text/plain' : 'application/json',
        },
        body: text ? (body as string) : JSON.stringify(body),
        ...(signal ? { signal } : {}),
      });
    } catch (cause) {
      // An ABORT is this client hanging up, not the engine failing.
      if (signal?.aborted) throw signal.reason ?? cause;
      throw new RemoteExecutionError(
        `could not reach the engine at ${url}: ${String(cause)}`,
        pure,
      );
    }
    const raw = await response.text();
    if (!response.ok) {
      throw new RemoteExecutionError(
        `${engineMessage(raw) ?? `engine returned ${response.status}`}`,
        pure,
      );
    }
    try {
      return JSON.parse(raw);
    } catch {
      throw new RemoteExecutionError(
        `the engine's answer was not JSON: ${raw.slice(0, 200)}`,
        pure,
      );
    }
  }
}

/**
 * The engine's own words for what went wrong.
 *
 * Its errors arrive as `{code, message, status, trace}` and the trace
 * is a Java stack hundreds of lines long. The message is the part a
 * person can act on -- "Can't find a match for function
 * 'toLower(Varchar(32)[0..1])'" told us exactly what to change.
 */
function engineMessage(raw: string): string | null {
  try {
    const body = JSON.parse(raw) as { message?: unknown };
    return typeof body.message === 'string'
      ? body.message.replace(/\s+/g, ' ').slice(0, 400)
      : null;
  } catch {
    return raw ? raw.replace(/\s+/g, ' ').slice(0, 200) : null;
  }
}
