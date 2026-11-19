// LITE'S PLANNER IN THE TAB (WebAssembly, in a worker): lite's API v1 operations (spec §2: model/check, model/elements,
// query/parse, query/print, query/type, query/plan), the same code a server answers them with (LiteV1Api.operate: one
// road, spec §5), and the planner's own calls (docs/QUERY_APP_DESIGN_2026_09_30.md D3): plans, held models, a host's
// operations. legend-engine's pure/v1 over the same planner is the edge's (wasm-edge.ts; H11, 2026-11-17): this file
// imports none of legend-engine's client (engine-api, grammar, planning-client).

import type { Lambda } from '../../../pure-protocol/src/index.ts';
import { fromJson, readLambda, toJson } from '../../../pure-protocol/src/index.ts';
import type { PureModelContextData } from './pmcd.ts';
import type { LiteOperation, RequestBody, Response as PlannerResponse } from './planner-protocol.ts';
import { liteError, LiteRefusal, type LiteError } from '../lite-error.ts';
import type { QueryColumn } from '../relation-type.ts';
import type { PureModelContext } from './wire.ts';
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
 * LITE'S PLANNER IN THE TAB: lite's API v1 operations (spec §2), the plans the hosts' runner runs, and the models it
 * holds -- the product's. legend-engine's shapes over the same planner (its Grammar, pure/v1's compile, return type,
 * generatePlan and execute) are the edge's: wasm-edge.ts WasmEdgeGrammar (H11, 2026-11-17).
 */
export class WasmGrammar {
  readonly #port: PlannerPort;

  constructor(port: PlannerPort) {
    this.#port = port;
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

  // ---- the names lite's own callers read a model and a query by (Studio's sessions, DataCube's Planner.parse, the tests):
  // ---- lite's operations, under the names they had; legend-engine's Grammar over them is the edge's (WasmEdgeGrammar)

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

  /** `query/plan` over a model's text: the SQL a query compiles
   *  to on its runtime, its columns (each its name and Pure type, as the plan states them), and the execution plan, whose data: payloads its `?`s bind (plan-params.ts
   *  reads them: the tenets audit's M3). */
  async plan(model: PureModelContext, lambda: Lambda, runtime: string): Promise<{
    sql: PlannedSql; columns: readonly { readonly name: string; readonly type: string }[]; plan?: unknown;
  }> {
    // lite API v1's query/plan (2026-11-17): lite's plan, its answering statement and its columns read from it
    const { plan } = JSON.parse(unfold(await this.#port.ask({ kind: 'queryPlan',
      request: `{"model":${JSON.stringify(model.code)},"query":${toJson(lambda)},"runtime":${JSON.stringify(runtime)}}` }))) as {
      plan: { root: { columns: { name: string; type: string }[]; sql: { statement: string } } } };
    declareSites(plan, 'a plan');
    return { sql: fromPlan(plan.root.sql.statement), columns: plan.root.columns, plan };
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
