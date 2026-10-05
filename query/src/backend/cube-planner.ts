// DataCube's planner seam (`Planner`, datacube/src/cube.ts) over the tab's own planner worker, for
// one model and runtime: the results grid is a CubeApp, and it plans on the same legend-lite
// module the rest of the app does -- one worker, one warm-up, not a second copy of the WASM.
// Answers are read by DataCube's own reader (`relationColumns`), so the cube sees what its
// `WasmPlanner` would give it -- with one difference: a column of an ENUMERATION reads as String.
// The engines carry an enum column as its values' names (legend-engine types it String in a TDS
// result; so do legend-lite's server and the tab's engine), and DataCube reads primitives only.

import { toJson, type Lambda } from '../../../pure-protocol/src/index.ts';
import type { Planner } from '../../../datacube/src/embed.ts';
import type { PrintStyle } from '../../../engine-client/src/pure-v1.ts';
import { relationColumns, type Plan, type PlanColumn } from '../../../engine-client/src/relation-type.ts';
import type { WasmGrammar } from '../../../engine-client/src/legend/wasm-grammar.ts';
import type { PureModelContext } from './wire.ts';

interface RelationTypeJson {
  readonly columns?: readonly { readonly name?: unknown; readonly genericType?: { readonly rawType?: { readonly fullPath?: unknown } } }[];
}

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
    const plan: Plan = { sql: planned.sql, columns: this.#columns(planned.type) };
    this.#plans.set(key, plan);
    return plan;
  }

  async relationType(query: Lambda, signal?: AbortSignal): Promise<PlanColumn[]> {
    signal?.throwIfAborted();
    return this.#columns(await this.#grammar.relationType(this.#model, query));
  }

  parse(text: string): Promise<Lambda> {
    return this.#grammar.lambda(text);
  }

  print(query: Lambda, style: PrintStyle = 'PRETTY'): Promise<string> {
    return this.#grammar.lambdaText(query, style);
  }

  /** The compiler's relation type as DataCube's columns, an enumeration's column as String. */
  #columns(type: unknown): PlanColumn[] {
    const t = type as RelationTypeJson;
    return relationColumns({
      ...t,
      columns: (t.columns ?? []).map((c) => {
        const path = c.genericType?.rawType?.fullPath;
        return typeof path === 'string' && this.#isEnumeration(path)
          ? { ...c, genericType: { ...c.genericType, rawType: { ...c.genericType?.rawType, fullPath: 'String' } } }
          : c;
      }),
    });
  }
}
