// legend-engine's `pure/v1` API: the ONE client every server call goes
// through (docs/UPSTREAM_ENDPOINTS_DESIGN_2026_09_27.md, U4).
//
// legend-lite serves these calls exactly and nothing of its own, so the
// same requests go to either server and only the base URL differs. Two
// callers, one transport:
//
//   - `UpstreamPlanner` (planner.ts): E9, `generatePlan`, and the tab runs the plan's SQL --
//     upstream's cached path;
//   - `LegendEngineExecutor` (engine-remote.ts): E8, `execute`, and the server runs it.
//
// The cube's queries travel as protocol JSON, built as trees (query.ts, pure-protocol): no E1
// on their way. Pure text crosses only at the human edges -- E1 parses what a person typed, E4
// prints a query for a person to read (docs/DATACUBE_TYPES_TO_SERVER_2026_09_27.md, T4b).
//
// Both type a cube's columns before its first query through E5,
// `lambdaRelationType` (`relationType` below each).
//
// The model travels as `PureModelContextText` ({_type: text, code}), which
// both servers accept on every call (measured against legend-engine
// 4.145.0), so no model JSON is parsed, cached or sent. The runtime rides
// the query (`->from(runtime)`), as it does in every relation query
// upstream has.

import { element, fn, lambda, readLambda, toJson, type Lambda } from '../../pure-protocol/src/index.ts';
import { parseExact as parseExactValues } from './values.ts';

/** A server's JSON answer with its numbers exact (values.ts): a SyntaxError names the answer. */
function parseExact(raw: string): unknown {
  try {
    return parseExactValues(raw);
  } catch (e) {
    if (e instanceof SyntaxError) throw new Error(`the server's answer was not JSON: ${raw.slice(0, 200)}`);
    throw e;
  }
}

export interface PureV1Options {
  /** e.g. `http://127.0.0.1:6300` (legend-engine) or `http://localhost:8080` (legend-lite). */
  readonly baseUrl: string;
  /** The model, as Pure grammar. */
  readonly model: string;
  /** The runtime the query reads through, e.g. `trades::h2::RT`. */
  readonly runtime: string;
  /**
   * The mapping a CLASS source reads through (a saved query's `Trade.all()->project(...)`): the
   * query then goes as `->from(mapping, runtime)`, as Query sends it -- legend-engine finds no
   * mapping from the runtime alone. A store source (`#>{db.t}#`) has none.
   */
  readonly mapping?: string;
  /**
   * The format `execute` answers in, as this engine is set up to serve it: DECLARED, never tried and fallen back
   * from (DataCube's rule, test/guardrails.test.ts). Absent, upstream's default: the TDS as JSON. `ARROW_IPC`,
   * upstream's Arrow (`?serializationFormat=ARROW_IPC`): one zstd frame around an Arrow IPC stream, its schema's
   * metadata carrying the builder and the SQL that ran. Python's engine serves ARROW_IPC
   * (docs/DATACUBE_PYTHON_SHOW_DESIGN_2026_10_08.md); legend-engine and legend-lite's server are read as JSON.
   */
  readonly serializationFormat?: 'ARROW_IPC';
  /**
   * The `Authorization` header every call carries, when the engine asks for one: Python's engine answers only a page
   * holding its one-time token (`Bearer <token>`).
   */
  readonly authorization?: string;
  /** Defaults to the global fetch; injectable for tests. */
  readonly fetch?: typeof fetch;
}

/** The first four bytes of a zstd frame (RFC 8878 §3.1.1): how an ARROW_IPC answer begins. */
const ZSTD_MAGIC = [0x28, 0xb5, 0x2f, 0xfd];

/**
 * An ARROW_IPC answer's body as its Arrow IPC stream: upstream writes the stream as one zstd frame
 * (RelationalResultToArrowIPCSerializer). A body that is not one -- the JSON an engine answers when it does not serve
 * the format -- is refused, naming what came instead; it is never read in its place. The decoder (fzstd) is loaded on
 * the first Arrow answer, so a page whose engines answer JSON never carries it (datacube's bundle budget).
 */
export async function arrowIpcStream(body: Uint8Array): Promise<Uint8Array> {
  if (!ZSTD_MAGIC.every((b, i) => body[i] === b)) {
    throw new Error(`the answer is not ARROW_IPC (a zstd frame); it begins: ${new TextDecoder().decode(body.subarray(0, 120))}`);
  }
  const { decompress } = await import('fzstd');
  try {
    return decompress(body);
  } catch (e) {
    throw new Error(`the ARROW_IPC answer does not decompress: ${String(e)}`);
  }
}

/**
 * How a caller names a failure: its own error class, carrying what was asked -- the query (a
 * tree), or, for a parse, the text.
 */
export type Failure = (message: string, subject: Lambda | string) => Error;

/** E4's `renderStyle`: PRETTY across lines (upstream's default for a person), STANDARD on one. */
export type PrintStyle = 'PRETTY' | 'STANDARD';

export class PureV1Client {
  #options: PureV1Options;
  readonly #fetch: typeof fetch;
  readonly #fail: Failure;

  constructor(options: PureV1Options, fail: Failure) {
    this.#options = options;
    this.#fetch = options.fetch ?? globalThis.fetch.bind(globalThis);
    this.#fail = fail;
  }

  get baseUrl(): string {
    return this.#options.baseUrl.replace(/\/$/, '');
  }

  /**
   * The query as the server runs it: its relation read from the runtime, `->from(runtime)` -- or,
   * over a mapping, `->from(mapping, runtime)`.
   */
  fromRuntime(query: Lambda): Lambda {
    const body = query.body[0];
    if (query.body.length !== 1 || body === undefined || query.parameters.length > 0) {
      throw this.#fail('a cube query is one relation expression with no parameters', query);
    }
    const { mapping, runtime } = this.#options;
    return lambda([], mapping === undefined
      ? fn('from', body, element(runtime))
      : fn('from', body, element(mapping), element(runtime)));
  }

  /** E1 `grammar/grammarToJson/lambda`: what a person typed, as its lambda. */
  async parse(text: string, signal?: AbortSignal): Promise<Lambda> {
    const raw = await this.#post('/grammar/grammarToJson/lambda?returnSourceInformation=false', text, text, signal, 'text');
    return readLambda(raw);
  }

  /** E4 `grammar/jsonToGrammar/lambda`: a query as Pure text, for a person to read. */
  print(query: Lambda, style: PrintStyle = 'PRETTY', signal?: AbortSignal): Promise<string> {
    return this.#post(`/grammar/jsonToGrammar/lambda?renderStyle=${style}`, query, query, signal, 'json');
  }

  /** E5 `compilation/lambdaRelationType`: the compiler's type of a query's result. */
  async lambdaRelationType(query: Lambda, signal?: AbortSignal): Promise<unknown> {
    return parseExact(await this.#post('/compilation/lambdaRelationType', {
      lambda: query,
      model: { _type: 'text', code: this.#options.model },
    }, query, signal, 'json'));
  }

  /**
   * Compile against another model from now on: the page's model grows as it opens a file (each
   * request carries the model, so the server holds nothing to update).
   */
  useModel(model: string, runtime: string, mapping?: string): void {
    const { mapping: _was, ...rest } = this.#options;
    this.#options = { ...rest, model, runtime, ...(mapping === undefined ? {} : { mapping }) };
  }

  /** `grammar/grammarToJson/model`: a model's elements, as the compiler reads its text. */
  async modelElements(text: string, signal?: AbortSignal): Promise<unknown[]> {
    const raw = await this.#post('/grammar/grammarToJson/model?returnSourceInformation=false', text, text, signal, 'text');
    const elements = (parseExact(raw) as { elements?: unknown }).elements;
    if (!Array.isArray(elements)) throw this.#fail('the model came back without elements', text);
    return elements;
  }

  /** E9 `execution/generatePlan`: the execution plan for a query. */
  async generatePlan(query: Lambda, signal?: AbortSignal): Promise<unknown> {
    return parseExact(await this.#post('/execution/generatePlan', this.#input(this.fromRuntime(query), {}), query, signal, 'json'));
  }

  get serializationFormat(): 'ARROW_IPC' | undefined {
    return this.#options.serializationFormat;
  }

  /** E8 `execution/execute`: the rows for a query, run by the server, as the TDS JSON (an engine read as JSON). */
  async execute(query: Lambda, signal?: AbortSignal): Promise<unknown> {
    if (this.#options.serializationFormat !== undefined) {
      throw this.#fail(`this engine is read as ${this.#options.serializationFormat}: its rows come from executeArrow`, query);
    }
    const response = await this.#send('/execution/execute', this.#executeInput(query), query, signal, 'json');
    return parseExact(await response.text());
  }

  /**
   * E8 in upstream's Arrow format (`?serializationFormat=ARROW_IPC`, an engine read as ARROW_IPC): the Arrow IPC
   * stream, out of its zstd frame (arrowIpcStream).
   */
  async executeArrow(query: Lambda, signal?: AbortSignal): Promise<Uint8Array> {
    if (this.#options.serializationFormat !== 'ARROW_IPC') {
      throw this.#fail('this engine is read as JSON: its rows come from execute', query);
    }
    const response = await this.#send('/execution/execute?serializationFormat=ARROW_IPC', this.#executeInput(query),
      query, signal, 'json');
    // the body read outside: an abort while it arrives is the caller's signal, as #send keeps it
    const body = new Uint8Array(await response.arrayBuffer());
    try {
      return await arrowIpcStream(body);
    } catch (e) {
      throw this.#fail(`the server at ${this.baseUrl}: ${e instanceof Error ? e.message : String(e)}`, query);
    }
  }

  #executeInput(query: Lambda): object {
    return this.#input(this.fromRuntime(query), {
      queryTimeOutInSeconds: 60,
      enableConstraints: true,
    });
  }

  #input(query: Lambda, context: object): object {
    return {
      // vX_X_X carries the protocol models production versions lack
      // -- DuckDB's among them, which is upstream's reason too.
      clientVersion: 'vX_X_X',
      function: query,
      model: { _type: 'text', code: this.#options.model },
      // REQUIRED. Without it legend-engine answers 500 with a
      // NullPointerException out of `processExecutionContext` rather
      // than naming the field it wanted.
      context: { _type: 'BaseExecutionContext', ...context },
    };
  }

  /**
   * One call; the answer's raw text. A request carrying a query is written with the protocol
   * library's exact JSON (a decimal's digits and an integer past 2^53 kept; JSON.stringify would
   * refuse or round them).
   */
  async #post(
    path: string,
    body: unknown,
    subject: Lambda | string,
    signal: AbortSignal | undefined,
    as: 'text' | 'json',
  ): Promise<string> {
    return (await this.#send(path, body, subject, signal, as)).text();
  }

  /** One call; its answer, refused unless the server answered 200. */
  async #send(
    path: string,
    body: unknown,
    subject: Lambda | string,
    signal: AbortSignal | undefined,
    as: 'text' | 'json',
  ): Promise<Response> {
    const url = `${this.baseUrl}/api/pure/v1${path}`;
    const { authorization } = this.#options;
    let response: Response;
    try {
      response = await this.#fetch(url, {
        method: 'POST',
        headers: {
          'Content-Type': as === 'text' ? 'text/plain' : 'application/json',
          ...(authorization === undefined ? {} : { Authorization: authorization }),
        },
        body: as === 'text' ? (body as string) : toJson(body),
        ...(signal ? { signal } : {}),
      });
    } catch (cause) {
      // An ABORT is this client hanging up because the answer stopped
      // mattering, not the server failing. Report it as what it is, or
      // telemetry reads a responsive grid as an outage.
      if (signal?.aborted) throw signal.reason ?? cause;
      throw this.#fail(`could not reach the server at ${url}: ${String(cause)}`, subject);
    }
    if (!response.ok) {
      const raw = await response.text();
      throw this.#fail(serverMessage(raw) ?? `the server returned ${response.status}`, subject);
    }
    return response;
  }
}

/**
 * The server's own words for what went wrong.
 *
 * Errors arrive as `{code, message, status, trace}`, and legend-engine's
 * trace is a Java stack hundreds of lines long. The message is the part a
 * person can act on -- "Can't find a match for function
 * 'toLower(Varchar(32)[0..1])'" told us exactly what to change.
 */
function serverMessage(raw: string): string | null {
  try {
    const body = JSON.parse(raw) as { message?: unknown };
    return typeof body.message === 'string'
      ? body.message.replace(/\s+/g, ' ').slice(0, 400)
      : null;
  } catch {
    return raw ? raw.replace(/\s+/g, ' ').slice(0, 200) : null;
  }
}
