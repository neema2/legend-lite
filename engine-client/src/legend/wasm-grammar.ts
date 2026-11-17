// Grammar and typing answered in the tab by legend-lite's planner (WebAssembly, in a worker). The grammar, a query's
// relation type and a model's compile are legend-engine's pure/v1 (and lite's own compile route) asked of the planner
// (plannerFetch): the same client as a server's (planning-client.ts HttpPlanning, the server's planning client), the
// same code answering it (PureV1Api), so the tab and a server cannot answer differently (docs/PROTOCOL_PROGRAM_2026_10_05.md, invariant 5; leg 6). The rest are the
// planner's own calls (docs/QUERY_APP_DESIGN_2026_09_30.md D3).

import type { Lambda } from '../../../pure-protocol/src/index.ts';
import { fromJson, readLambda, toJson } from '../../../pure-protocol/src/index.ts';
import type { PureModelContextData } from './pmcd.ts';
import { EngineError } from './engine-api.ts';
import type { Grammar } from './grammar.ts';
import { HttpPlanning } from './planning-client.ts';
import type { LiteOperation, RequestBody, Response as PlannerResponse } from './planner-protocol.ts';
import { liteError, LiteRefusal, type LiteError } from '../lite-error.ts';
import type { QueryColumn } from '../relation-type.ts';
import type { CompileResult, ExecuteInput, PureModelContext, RelationTypeAnswer } from './wire.ts';
import type {
  FileLoadRequest, HostOperation, ProjectDataStatements, RemoteMountRequest, SnapCopyRequest, SnapStreamRequest,
  SynthesizeRequest,
} from '../requests.ts';
import { allFromPlan, fromPlan, planFromAnswer, plannedProjectData, type PlannedSql, type PlanText } from '../planned-sql.ts';
import { declareSites } from '../declared-raises.ts';
import { planning } from '../plan-guard.ts';

/** Where a request goes: a worker, or (in tests) the module called directly. */
export interface PlannerPort {
  ask(request: RequestBody): Promise<string>;
}

/** A worker running planner-worker.ts; `base` is where classes.wasm and its runtime live. */
export class WorkerPort implements PlannerPort {
  readonly #worker: Worker;
  readonly #base: string;
  readonly #pending = new Map<number, { resolve(a: string): void; reject(e: Error): void }>();
  #next = 1;

  constructor(workerUrl: string | URL, base: string) {
    this.#worker = new Worker(workerUrl, { type: 'module' });
    this.#base = new URL(base, globalThis.location?.href).href;
    this.#worker.onmessage = (e: MessageEvent<PlannerResponse>) => {
      const p = this.#pending.get(e.data.id);
      if (!p) return;
      this.#pending.delete(e.data.id);
      if (e.data.ok) p.resolve(e.data.answer);
      else p.reject(new Error(`the planner could not load: ${e.data.error}`));
    };
  }

  async ask(request: RequestBody): Promise<string> {
    // a worker's call is asked here, on the caller's flow: refused inside a run of it (run-frame.ts, the witness), by a
    // rejected promise (async; the C4 final fix round's nit, 2026-11-14)
    planning(request.kind);
    const id = this.#next++;
    return new Promise((resolve, reject) => {
      this.#pending.set(id, { resolve, reject });
      this.#worker.postMessage({ ...request, id, base: this.#base });
    });
  }
}

/**
 * An export's folded answer as its value or a refusal (planner.Folded): `OK\n<answer>`, or `ERR\n` and lite's refusal as
 * its JSON (`{kind, message, where?, raise?}`, lite's API v1 spec §3; core's LiteV1Api.refusal, the one classifier),
 * thrown as a LiteRefusal. An answer in neither form is a defect, said as one.
 */
export function unfold(answer: string): string {
  if (answer.startsWith('OK\n')) return answer.slice(3);
  if (answer.startsWith('ERR\n')) {
    let refusal: LiteError;
    try {
      refusal = liteError(JSON.parse(answer.slice(4)));
    } catch (e) {
      throw new LiteRefusal({ kind: 'internal', message: `the planner answered a refusal in no form it has (${(e as Error).message})` });
    }
    throw new LiteRefusal(refusal);
  }
  throw new LiteRefusal({ kind: 'internal', message: `the planner answered in no form it has: ${JSON.stringify(answer.slice(0, 120))}` });
}

/**
 * A `fetch` that answers legend-engine's pure/v1 requests in the tab: each is routed by the planner's `pureV1OrError`,
 * the code legend-lite's server answers them with, its status, media type and body made a Response. An HttpEngine
 * over it asks and is answered exactly as over the network, a refusal included. The planner itself failing (its
 * folded `ERR` answer) rejects with the EngineError `unfold` makes of it; an aborted request rejects with its reason.
 */
export function plannerFetch(port: PlannerPort): typeof fetch {
  return async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
    init?.signal?.throwIfAborted();
    const href = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url;
    const url = new URL(href, 'http://planner.invalid');
    const body = init?.body;
    if (body !== undefined && body !== null && typeof body !== 'string') {
      throw new TypeError('the planner answers a pure/v1 request with a text body only');
    }
    const answer = httpAnswer(await port.ask({ kind: 'pureV1', path: url.pathname, query: url.search.slice(1), body: body ?? '' }));
    init?.signal?.throwIfAborted();
    return new Response(answer.body, { status: answer.status, headers: { 'Content-Type': answer.type } });
  };
}

/** A pure/v1 answer's body when it is a 200; else the engine's refusal, in its words and status, as a server's is. */
function answered(folded: string): string {
  const answer = httpAnswer(folded);
  if (answer.status === 200) return answer.body;
  let message = answer.body || `${answer.status}`;
  let errorType: string | undefined;
  try {
    const e = JSON.parse(answer.body) as { message?: string; errorType?: string };
    if (typeof e.message === 'string') message = e.message;
    errorType = e.errorType;
  } catch { /* not JSON: the text is the message */ }
  throw new EngineError(message, answer.status, errorType);
}

/** A pure/v1 answer the planner gives (`OK\n<status>\n<type>\n<body>`): its status, media type and body. */
function httpAnswer(folded: string): { status: number; type: string; body: string } {
  const answer = unfold(folded);
  const first = answer.indexOf('\n');
  const second = answer.indexOf('\n', first + 1);
  if (first < 0 || second < 0) {
    throw new EngineError(`the planner answered a pure/v1 call in no form it has: ${JSON.stringify(answer.slice(0, 120))}`, 500);
  }
  return { status: Number(answer.slice(0, first)), type: answer.slice(first + 1, second), body: answer.slice(second + 1) };
}

/**
 * Execute's plan half in the engine's JSON (PureV1Api.jsonPlan; docs/ONE_PLATFORM_PROGRAM_2026_10_12.md C1): `run`, the
 * typed statement the tab runs -- a relation's rows, or a graph fetch's objects, one per row -- its answer's shape and
 * result columns, and the answer's every part but its rows (its text, for the planner's executeAnswer).
 */
export interface ExecutePlan {
  /** The plan's statement: what the answer's activity reports. */
  readonly sql: string;
  /** What the host runs: the same statement, its rows handed back to executeAnswer (typed where the answer is read). */
  readonly run: PlannedSql;
  /** A relation's rows, or a graph fetch's objects. */
  readonly shape: 'RELATION' | 'OBJECTS';
  /** The statement's result columns with their type facts, by which the planner reads the rows back. */
  readonly resultColumns: readonly unknown[];
  readonly builder: { readonly _type: string };
  /** Where `run` binds values (a data: payload, the tenets audit's M3): the execution plan, which carries them by digest
   *  (plan-params.ts reads them). None for a statement that runs alone. */
  readonly plan?: unknown;
}

/** `model`'s text, the one form the tab's lite kinds read a model in; any other refused by name. */
function modelTextOf(model: PureModelContext, what: string): string {
  const m = model as { readonly _type?: string; readonly code?: unknown };
  if (m._type !== 'text' || typeof m.code !== 'string') {
    throw new EngineError(`a query is ${what} in the tab over a model given as its text, not a ${String(m._type)}`, 400);
  }
  return m.code;
}

export class WasmGrammar implements Grammar {
  readonly #port: PlannerPort;
  /** pure/v1 in the tab: the server's client over the planner. */
  readonly #pureV1: HttpPlanning;

  constructor(port: PlannerPort) {
    this.#port = port;
    this.#pureV1 = new HttpPlanning('/api', plannerFetch(port));
  }

  // ---- LITE'S API V1 IN THE TAB (docs/records/lite-api-v1-2026-11-17.md §2, the user's go of 2026-11-17): one call per
  // ---- operation, the planner's kind of the operation's name, its request the server route's body, its answer the
  // ---- route's JSON (planner.Boundary over core's LiteV1Api.operate: one road, spec §5)

  /** One operation of lite's API asked of the planner: its request's JSON in, its answer's JSON text out; a refusal
   *  thrown as lite's (LiteRefusal). */
  async operate(operation: LiteOperation, request: unknown): Promise<string> {
    return unfold(await this.#port.ask({ kind: operation, request: toJson(request) }));
  }

  /** `model/check`: every error of the model, none when it compiles. */
  async modelCheck(model: string): Promise<readonly LiteError[]> {
    const { errors } = JSON.parse(await this.operate('modelCheck', { model })) as { readonly errors: readonly unknown[] };
    return errors.map(liteError);
  }

  /** `model/elements`: the model's protocol elements (lite's protocol, PureModelContextData). */
  async modelElements(model: string): Promise<PureModelContextData> {
    const { elements } = JSON.parse(await this.operate('modelElements', { model })) as Pick<PureModelContextData, 'elements'>;
    return { _type: 'data', elements };
  }

  /** `query/parse`: the query's tree, its numbers exact. */
  async queryParse(text: string): Promise<Lambda> {
    return readLambda((fromJson(await this.operate('queryParse', { text })) as { readonly query: unknown }).query);
  }

  /** `query/print`: the query's text, pretty unless standard is asked. */
  async queryPrint(query: Lambda, style: 'pretty' | 'standard' = 'pretty'): Promise<string> {
    return (JSON.parse(await this.operate('queryPrint', { query, style })) as { readonly text: string }).text;
  }

  /** `query/type`: each column's name and Pure type, an enumeration's flagged. */
  async queryType(model: string, query: Lambda): Promise<readonly QueryColumn[]> {
    return (JSON.parse(await this.operate('queryType', { model, query })) as { readonly columns: readonly QueryColumn[] }).columns;
  }

  /** `query/plan`: lite's plan of the query on `runtime`, its JSON text -- which is data (planned-sql.ts reads it). The
   *  model is held by the planner once per text (`queryPlanHeld`, the binding's cache: §7a) and released when no plan
   *  of it is asked (#held). */
  async queryPlan(model: string, query: Lambda, runtime: string): Promise<PlanText> {
    return this.#withHeld(model, async (handle) => {
      const answer = unfold(await this.#port.ask({ kind: 'queryPlanHeld', handle, request: toJson({ query, runtime }) }));
      const { plan } = JSON.parse(answer) as { readonly plan: unknown };
      return planFromAnswer(JSON.stringify(plan));
    });
  }

  // ---- legend-engine's grammar interface (Grammar), answered over lite's operations: the same calls, its shapes

  /** A model's text as its protocol JSON: `model/elements`. */
  modelJson(text: string): Promise<PureModelContextData> {
    return this.modelElements(text);
  }

  /** A lambda's text as its protocol JSON: `query/parse`. */
  lambdaJson(text: string): Promise<Lambda> {
    return this.queryParse(text);
  }

  /** As `lambdaJson` (DataCube's `Planner.parse`): `query/parse`, its numbers exact. */
  lambda(text: string): Promise<Lambda> {
    return this.queryParse(text);
  }

  /** A lambda's protocol JSON as its Pure text: `query/print`. */
  lambdaText(lambda: Lambda, style: 'PRETTY' | 'STANDARD'): Promise<string> {
    return this.queryPrint(lambda, style === 'PRETTY' ? 'pretty' : 'standard');
  }

  modelText(model: PureModelContextData, style: 'PRETTY' | 'STANDARD'): Promise<string> {
    return this.#pureV1.modelText(model, style);
  }

  /** A lambda's relation type over a model given as its text, in Grammar's shape: `query/type`, each column's Pure type
   *  its genericType's path (relation-type.ts reads it back); a model given otherwise is refused by name. */
  async relationType(model: PureModelContext, lambda: Lambda): Promise<RelationTypeAnswer> {
    const columns = await this.queryType(modelTextOf(model, 'typed'), lambda);
    return { columns: columns.map((c) => ({ name: c.name, genericType: { rawType: { fullPath: c.type } } })) } as unknown as RelationTypeAnswer;
  }

  /** `compilation/lambdaReturnType`, answered in the tab as the server answers it (PureV1Api.route). */
  returnType(model: PureModelContext, lambda: Lambda): Promise<string> {
    return this.#pureV1.returnType(model, lambda);
  }

  /** `execution/generatePlan`, answered in the tab as the server answers it: the plan legend-engine generates. */
  generatePlan(input: ExecuteInput): Promise<unknown> {
    return this.#pureV1.generatePlan(input);
  }

  /**
   * Execute's plan half (ExecutePlan) and its text, asked of the planner over `input`'s own model. A refusal -- a
   * parameter with no value, a query that does not compile -- is the engine's, in its words and status, as a server's
   * execute refuses it (500, its errorType COMPILATION).
   */
  async executePlan(input: ExecuteInput): Promise<{ readonly plan: ExecutePlan; readonly text: string }> {
    // the tab runs the model it is handed (its data the page's): that one, the planner's check then always met; a
    // model not given as text is the planner's to refuse, in the server's words
    const model = input.model as { readonly _type?: string; readonly code?: string };
    const models = model._type === 'text' && typeof model.code === 'string' ? [model.code] : [];
    const text = answered(await this.#port.ask({ kind: 'executePlan', body: toJson(input), models: JSON.stringify(models) }));
    const read = JSON.parse(text) as Omit<ExecutePlan, 'run'> & { readonly run: string; readonly raises?: unknown };
    // the raises `run` can send, worded by the planner (declared-raises.ts; phase D's option (D), 2026-11-15)
    declareSites(read, 'an execute plan');
    // the statement the host runs, as the planner's answer carries it (planned-sql.ts)
    return { plan: { ...read, run: fromPlan(read.run) }, text };
  }

  /**
   * Execute's answer (PureV1Api.jsonAnswer): `plan` (executePlan's text) and the rows the tab's database answered its
   * `run` statement with (browser-engine's hostRows), decoded by the plan's columns and written by the writer
   * legend-lite's server answers with -- its text, the server's.
   */
  async executeAnswer(plan: string, rows: string): Promise<string> {
    return answered(await this.#port.ask({ kind: 'executeAnswer', plan, rows }));
  }

  /** The SQL a query compiles to on its runtime, the type of its result (upstream's relationType shape), and -- where the
   *  text does not run alone -- the execution plan it renders from, whose data: payloads its `?`s bind (plan-params.ts
   *  reads them: the tenets audit's M3). */
  async plan(model: PureModelContext, lambda: Lambda, runtime: string): Promise<{ sql: PlannedSql; type: unknown; plan?: unknown }> {
    // lite API v1's query/plan (the rename train, 2026-11-17: planModel until then; the TS host's own rewrite of this
    // file supersedes these lines): lite's plan, its answering statement and its columns read from it
    const { plan } = JSON.parse(unfold(await this.#port.ask({ kind: 'queryPlan',
      request: `{"model":${JSON.stringify(model.code)},"query":${toJson(lambda)},"runtime":${JSON.stringify(runtime)}}` }))) as {
      plan: { root: { columns: { name: string; type: string }[]; sql: { statement: string } } } };
    declareSites(plan, 'a plan');
    const type = { columns: plan.root.columns.map((c) => ({ name: c.name, genericType: { rawType: { fullPath: c.type } } })) };
    return { sql: fromPlan(plan.root.sql.statement), type, plan };
  }

  /** The SQL a query compiles to on its runtime -- legend-lite's plan, shown to a person ("Show SQL"). */
  async sql(model: PureModelContext, lambda: Lambda, runtime: string): Promise<string> {
    return (await this.plan(model, lambda, runtime)).sql;
  }

  /**
   * What a session of the database runs before it is queried, so that it answers as the planner's SQL expects -- the
   * dialect's own setup (Boundary.sessionSetup: a DuckDB session in UTC), as legend-lite's server sets up its own.
   */
  async sessionSetup(databaseType: string): Promise<readonly PlannedSql[]> {
    return allFromPlan(JSON.parse(unfold(await this.#port.ask({ kind: 'sessionSetup', databaseType }))) as string[]);
  }

  /**
   * A project's own database on a database of that type (C5, Boundary.projectData): its name and the statements that
   * make it, point a connection at it, fill it from the model's Data elements, and drop it.
   */
  async projectData(model: string, databaseType: string): Promise<ProjectDataStatements> {
    return plannedProjectData(JSON.parse(unfold(await this.#port.ask({ kind: 'projectData', model, databaseType }))));
  }

  /** A host's named operations on a database of that type (D5, Boundary.hostOperations): each one's statements. */
  async hostOperations(databaseType: string, operations: readonly HostOperation[]): Promise<PlannedSql[][]> {
    const asked = await this.#port.ask({ kind: 'hostOperations', databaseType, operations: JSON.stringify(operations) });
    return (JSON.parse(unfold(asked)) as string[][]).map(allFromPlan);
  }

  /** Build the planner's boot layer for this model now, before a person waits on it. */
  async warm(model: PureModelContext): Promise<void> {
    unfold(await this.#port.ask({ kind: 'warm', model: model.code }));
  }

  /** `compilation/compile` in the tab, as the server answers it: OK, or the first failure, 400 COMPILATION. */
  compile(model: PureModelContext): Promise<CompileResult> {
    return this.#pureV1.compile(model);
  }

  /** The models this planner holds, by text: each held once, its users counted, the older ones let go when unused. */
  readonly #held = new Map<string, { readonly handle: Promise<number>; users: number }>();

  async #withHeld<T>(text: string, use: (handle: number) => Promise<T>): Promise<T> {
    let held = this.#held.get(text);
    if (held === undefined) {
      const handle = this.#port.ask({ kind: 'hold', model: text }).then((a) => Number(unfold(a)));
      held = { handle, users: 0 };
      this.#held.set(text, held);
      // a model that does not compile is not kept: asked again, it is compiled again
      handle.catch(() => { if (this.#held.get(text)?.handle === handle) this.#held.delete(text); });
    } else {
      // the most recent last
      this.#held.delete(text);
      this.#held.set(text, held);
    }
    held.users++;
    try {
      return await use(await held.handle);
    } finally {
      held.users--;
      await this.#letGo();
    }
  }

  /** The models beyond the two most recent, unused, released (the planner's `release` kind). */
  async #letGo(): Promise<void> {
    const texts = [...this.#held.keys()];
    for (const text of texts.slice(0, Math.max(0, texts.length - 2))) {
      const held = this.#held.get(text)!;
      if (held.users > 0) continue;
      this.#held.delete(text);
      const handle = await held.handle.catch(() => undefined);
      if (handle !== undefined) unfold(await this.#port.ask({ kind: 'release', handle }));
    }
  }

  /**
   * A host's own file read into its database, as a plan (Boundary.fileLoad, B3): one HOST_SESSION load the hosts'
   * runner runs (plan-runner.ts runLoad), the file a reference the host's store binds -- PlanJson's text.
   */
  async fileLoad(request: FileLoadRequest): Promise<PlanText> {
    return planFromAnswer(unfold(await this.#port.ask({ kind: 'fileLoad', request: JSON.stringify(request) })));
  }

  /**
   * A remote file mounted, as a plan (Boundary.remoteMount, B3's Q3): one HOST_SESSION load the hosts' runner runs,
   * binding by name what this host holds (its keys, a bearer URL) -- PlanJson's text, holding no value.
   */
  async remoteMount(request: RemoteMountRequest): Promise<PlanText> {
    return planFromAnswer(unfold(await this.#port.ask({ kind: 'remoteMount', request: JSON.stringify(request) })));
  }

  /**
   * A snapshot from a remote plane, as a plan (Boundary.snapStream, the user's ruling of 2026-11-11): one HOST_SESSION
   * load whose stream step the hosts' runner runs in one transaction -- PlanJson's text, the plane by name, never a row.
   */
  async snapStream(request: SnapStreamRequest): Promise<PlanText> {
    return planFromAnswer(unfold(await this.#port.ask({ kind: 'snapStream', request: JSON.stringify(request) })));
  }

  /** A snapshot's local copy, as a plan (Boundary.snapCopy, the final audit's item 1(b)): one write in its target's
   *  transaction -- the table made from the pull, planned in the call -- PlanJson's text, never the pull's statement. */
  async snapCopy(request: SnapCopyRequest): Promise<PlanText> {
    return planFromAnswer(unfold(await this.#port.ask({ kind: 'snapCopy', request: JSON.stringify(request) })));
  }

  /**
   * Rows the database generates into a declared table, as a plan (Boundary.synthesize: SYNTHESIZE, the user's ruling of
   * 2026-11-12): one HOST_SESSION load -- the table made as declared, then one statement filling it -- PlanJson's text,
   * never a row.
   */
  async synthesize(request: SynthesizeRequest): Promise<PlanText> {
    return planFromAnswer(unfold(await this.#port.ask({ kind: 'synthesize', request: JSON.stringify(request) })));
  }

  /**
   * A statement a person wrote, as a plan (Boundary.authored: Studio's SQL playground, the user's ruling of 2026-11-12):
   * the person's one text on a database of `databaseType`, carried as written -- never parsed, rewritten or split --
   * PlanJson's text, its answer the relation the database reports (plan-runner.ts runAuthored).
   */
  async authored(databaseType: string, text: string): Promise<PlanText> {
    return planFromAnswer(unfold(await this.#port.ask({ kind: 'authored', request: JSON.stringify({ databaseType, text }) })));
  }
}
