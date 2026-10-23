// The planner client: Pure grammar out, SQL back, over legend-engine's own
// API (docs/UPSTREAM_ENDPOINTS_DESIGN_2026_09_27.md).
//
// Upstream's cached path, through the one `pure/v1` client (pure-v1.ts):
// the query parsed (`grammarToJson/lambda`), then `execution/generatePlan`,
// whose SQL node carries the query the tab then runs. The SAME requests go to
// legend-lite and to legend-engine: lite serves those calls exactly and
// nothing of its own (the made-up `/engine/plan` this replaced was deleted,
// 2026-09-27).

import type { Planner } from './cube.ts';
import { PureV1Client, type PrintStyle, type PureV1Options } from '../../engine-client/src/pure-v1.ts';
import { relationColumns, tdsColumns, type Plan, type PlanColumn } from '../../engine-client/src/relation-type.ts';
import { toJson, type Lambda } from '../../pure-protocol/src/index.ts';
import type { GridSource } from './model-writer.ts';

export interface UpstreamPlannerOptions extends PureV1Options {
  /**
   * Cache plans by grammar text. Safe because planning is pure: the
   * same grammar and runtime always lower to the same SQL. Worth it
   * because scrolling re-issues structurally identical queries.
   */
  readonly cache?: boolean;
}

/** How a planner reads a model besides its text and runtime. */
export interface ModelOptions {
  /** The columns the model declares BIT (relation-type.ts, ENGINE DEFECT S23). */
  readonly bitColumns?: readonly string[];
  /** The mapping a class source reads through: the query goes `->from(mapping, runtime)` (pure-v1.ts). */
  readonly mapping?: string;
  /** The model's enumerations: a column typed by one is said so (relation-type.ts `PlanColumn.enumeration`). */
  readonly enumerations?: readonly string[];
  /**
   * Its rows are its project's own (a saved query's version: C5), in a database of the project's own on its host -- never
   * the host's tables. A host that plans and runs where the rows are (Python's plan-and-run) reads them there, and reads
   * any other model's rows only from the tables it serves (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md F28); the tab reads
   * a project's through its project databases, by the source it opens.
   */
  readonly project?: true;
}

export class PlanError extends Error {
  /** What was asked: the query (a tree), or, for a parse, the text a person typed. */
  readonly subject: Lambda | string;
  constructor(message: string, subject: Lambda | string) {
    super(message);
    this.name = 'PlanError';
    this.subject = subject;
  }
}

/**
 * The planner on a server: legend-lite's or legend-engine's `pure/v1`, E9 `generatePlan` for a
 * query's SQL and E5 `lambdaRelationType` for its type -- the query sent as the protocol tree the
 * cube built. E1 and E4 only at the human edges (`parse`, `print`).
 */
export class UpstreamPlanner implements Planner {
  readonly #client: PureV1Client;
  readonly #useCache: boolean;
  readonly #cache = new Map<string, Plan>();
  readonly #types = new Map<string, PlanColumn[]>();
  /** The model's BIT columns: engine types them TinyInt (relation-type.ts, ENGINE DEFECT S23). */
  #bitColumns: ReadonlySet<string> = new Set();
  /** The model's enumerations (ModelOptions). */
  #enumerations: ReadonlySet<string> = new Set();

  readonly #options: UpstreamPlannerOptions;

  constructor(options: UpstreamPlannerOptions) {
    this.#options = options;
    this.#useCache = options.cache !== false;
    this.#client = new PureV1Client(options, (m, subject) => new PlanError(m, subject));
  }

  /** Where this planner's queries are planned from: its model, and the runtime they run on. */
  source(): GridSource {
    return { model: this.#options.model, runtime: this.#options.runtime };
  }

  /**
   * A planner over ANOTHER model -- another source on the page -- on the same server, with caches
   * of its own. Every request carries its model, so the server holds nothing for either.
   */
  withModel(model: string, runtime: string, how: ModelOptions = {}): UpstreamPlanner {
    const other = new UpstreamPlanner({ ...this.#options, model, runtime });
    other.useModel(model, runtime, how);
    return other;
  }

  /**
   * Plan against another model from now on (a file opened in this tab joins it). Every request
   * carries the model, so the server holds nothing; what was planned against the old one goes.
   */
  useModel(model: string, runtime: string, how: ModelOptions = {}): void {
    this.#client.useModel(model, runtime, how.mapping);
    this.#bitColumns = new Set(how.bitColumns ?? []);
    this.#enumerations = new Set(how.enumerations ?? []);
    this.#cache.clear();
    this.#types.clear();
  }

  async plan(query: Lambda, signal?: AbortSignal): Promise<Plan> {
    const key = toJson(query);
    const hit = this.#useCache ? this.#cache.get(key) : undefined;
    if (hit !== undefined) return hit;
    const plan = serverPlan(await this.#client.generatePlan(query, signal), query, this.#bitColumns);
    if (this.#useCache) this.#cache.set(key, plan);
    return plan;
  }

  /** E5 `lambdaRelationType`, cached by the query's JSON. */
  async relationType(query: Lambda, signal?: AbortSignal): Promise<PlanColumn[]> {
    const key = toJson(query);
    const hit = this.#useCache ? this.#types.get(key) : undefined;
    if (hit !== undefined) return hit;
    const columns = relationColumns(await this.#client.lambdaRelationType(query, signal), this.#bitColumns, this.#enumerations);
    if (this.#useCache) this.#types.set(key, columns);
    return columns;
  }

  /** E1: what a person typed, as its lambda. */
  parse(text: string, signal?: AbortSignal): Promise<Lambda> {
    return this.#client.parse(text, signal);
  }

  /** A model's elements, as the compiler reads its text (a project's data spaces, enumerations). */
  modelElements(text: string, signal?: AbortSignal): Promise<unknown[]> {
    return this.#client.modelElements(text, signal);
  }

  /** E4: a query as Pure text, for a person to read. */
  print(query: Lambda, style: PrintStyle = 'PRETTY', signal?: AbortSignal): Promise<string> {
    return this.#client.print(query, style, signal);
  }

  /** Cached plan count, for tests and diagnostics. */
  get cacheSize(): number {
    return this.#cache.size;
  }
}

/**
 * A server's `generatePlan` answer as a Plan: its one SQL node's statement (`sqlNode`), and the plan's own
 * `tdsColumns`, the result's type as the compiler gave it. The one reading of a server's plan: the tab's planner on a
 * server reads it so (its cases: test/fixtures/upstream-plans.json). A Python host plans in process and reads no
 * server's plan (the program's §1.1, 2026-10-15). INTERIM: core reads no legend-engine plan into its own ExecutionPlan
 * yet; when it does (docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md §8, phase 2) the tab reads a server's plan through the
 * module and this reader goes, the fixture its cases.
 */
export function serverPlan(body: unknown, query: Lambda, bitColumns: ReadonlySet<string> = new Set(),
  database = 'DuckDB'): Plan {
  return { sql: sqlNode(body, query, database), columns: tdsColumns(body, bitColumns) };
}

interface PlanNode {
  readonly _type?: string;
  readonly sqlQuery?: unknown;
  readonly connection?: { readonly type?: unknown };
  readonly executionNodes?: readonly (PlanNode | null)[];
}

/**
 * The plan's one SQL statement, from the one shape read here: a root `relationalTdsInstantiation` with exactly one `sql`
 * node under it, written for `database` (its connection's type: `DuckDB`, where the tab and Python run it), with no
 * placeholder the engine fills in at execution (`${`). Anything else is refused by name, never run in part (the Plan Gen
 * line's review of G1).
 */
function sqlNode(plan: unknown, query: Lambda, database: string): string {
  const root = (plan as { rootExecutionNode?: PlanNode | null }).rootExecutionNode ?? undefined;
  const children = root?.executionNodes ?? [];
  const node = children[0] ?? undefined;
  if (root?._type !== 'relationalTdsInstantiation' || children.length !== 1 || node?._type !== 'sql'
    || (node.executionNodes?.length ?? 0) > 0) {
    const shape = [root?._type ?? null, ...children.map((c) => c?._type ?? null)];
    throw new PlanError(`a plan read here is a relationalTdsInstantiation over one sql node, not ${JSON.stringify(shape)}`, query);
  }
  if (typeof node.sqlQuery !== 'string') throw new PlanError('the plan carries no SQL node', query);
  const written = typeof node.connection?.type === 'string' ? node.connection.type : 'none';
  if (written !== database) throw new PlanError(`the plan's SQL is written for ${written}, and it would run on ${database}`, query);
  if (node.sqlQuery.includes('${')) {
    throw new PlanError("the plan's SQL holds a placeholder the engine fills in at execution (${...})", query);
  }
  return node.sqlQuery;
}
