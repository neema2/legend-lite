// DataCube's planner seam (`Planner`, datacube/src/cube.ts) over the tab's own planner worker, for
// one model and runtime: the results grid is a CubeApp, and it plans on the same legend-lite
// module the rest of the app does -- one worker, one warm-up, not a second copy of the WASM.
// It asks lite's own operations (lite's API v1: query/plan, query/type, query/parse, query/print),
// each column read as DataCube reads a type (`pureType`) -- with one difference: a column of an
// ENUMERATION reads as String.
// The engines carry an enum column as its values' names (legend-engine types it String in a TDS
// result; so do legend-lite's server and the tab's engine), and DataCube reads primitives only.

import { toJson, type Lambda } from '../../../pure-protocol/src/index.ts';
import type { Planner } from '../../../datacube/src/embed.ts';
import { pureType, type Plan, type PlanColumn } from '../../../engine-client/src/relation-type.ts';
import { planParams } from '../../../engine-client/src/plan-params.ts';
import type { WasmGrammar } from '../../../engine-client/src/legend/wasm-grammar.ts';
import type { PureModelContext } from './wire.ts';

/** The cube's print styles, as its seam declares them (lite's words). */
type PrintStyle = NonNullable<Parameters<Planner['print']>[1]>;

export class CubePlanner implements Planner {
  readonly #grammar: WasmGrammar;
  readonly #model: PureModelContext;
  readonly #runtime: string;
  readonly #isEnumeration: (type: string) => boolean;
  /** Plans by query: planning is pure for a fixed model, and scrolling re-asks the same queries. */
  readonly #plans = new Map<string, Plan>();

  constructor(grammar: WasmGrammar, model: PureModelContext, runtime: string, isEnumeration: (type: string) => boolean) {
    this.#grammar = grammar;
    this.#model = model;
    this.#runtime = runtime;
    this.#isEnumeration = isEnumeration;
  }

  async plan(query: Lambda, signal?: AbortSignal): Promise<Plan> {
    const key = toJson(query);
    const hit = this.#plans.get(key);
    if (hit) return hit;
    signal?.throwIfAborted();
    const planned = await this.#grammar.plan(this.#model, query, this.#runtime);
    signal?.throwIfAborted();
    // the values its statement binds (a JSON connection's data: payload), read once here from the plan beside the text
    // (plan-params.ts: by digest, verified; the tenets audit's M3) -- the engine binds them
    const params = planParams(planned.plan);
    const plan: Plan = { sql: planned.sql, columns: this.#columns(planned.columns), ...(params.length > 0 ? { params } : {}) };
    this.#plans.set(key, plan);
    return plan;
  }

  async relationType(query: Lambda, signal?: AbortSignal): Promise<PlanColumn[]> {
    signal?.throwIfAborted();
    return this.#columns(await this.#grammar.queryType(this.#model.code, query));
  }

  parse(text: string): Promise<Lambda> {
    return this.#grammar.lambda(text);
  }

  /** `query/print` (lite's API v1 spec §2), in the cube's own styles, lite's words ('pretty' | 'standard'). */
  print(query: Lambda, style: PrintStyle = 'pretty'): Promise<string> {
    return this.#grammar.queryPrint(query, style);
  }

  /** Lite's columns (each its name and Pure type) as DataCube's, an enumeration's column as String. */
  #columns(columns: readonly { readonly name: string; readonly type: string }[]): PlanColumn[] {
    return columns.map((c) => ({ name: c.name, type: this.#isEnumeration(c.type) ? 'String' : pureType(c.type) }));
  }
}
