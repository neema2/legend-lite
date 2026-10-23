// A GRID'S BASE ON THE DEEPHAVEN SERVER (docs/DEEPHAVEN_PLAN_2026_10_22.md B1): every query a grid sends reads its base
// -- the base query and its leaf-level calculated columns -- from ONE table the engine keeps on the server, stored there,
// so the server joins and calculates once a change for every query and every result the grid follows, and each query
// is only its own filter, grouping, pivot, sort and limit. Without it the planner folds a calculated column into each
// query that reads it (as every SQL database wants), and each followed result computes it again every tick.
//
// The seam is Snap's (runner.ts `rebased`, docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §18.3): wherever a query reads the
// grid's live base, it reads the copy -- here a table of the session's own, `legend::Bases.legend_base_<key>`, declared
// in a model fragment bound to the grid's own connection, whose ticket the engine fills with the base's export when it
// sends (DeephavenEngine.keepBase). A new base -- a calculated column added, the source changed -- is made once, its
// joins still kept alive below it. A base whose columns no table can declare (a name the server cannot hold, a type
// the fragment does not write) is not made: the grid's queries read the base as written, said by `based`.

import type { DeephavenEngine } from '../../engine-client/src/deephaven-engine.ts';
import type { PlanColumn } from '../../engine-client/src/relation-type.ts';
import { familyOf, hasTimeOfDay, isDay, isFloat, isFractional, isNumeric } from '../../engine-client/src/types.ts';
import { accessor, toJson, type Lambda, type ValueSpecification } from '../../pure-protocol/src/index.ts';
import type { LitePlanner } from './lite-planner.ts';
import { sourceWithDerived, type LevelScope } from './query.ts';
import { PlanThenRun, QueryFailure, rebased, type QueryRunner, type RunOutcome } from './runner.ts';
import type { CubeSnapshot } from './snapshot.ts';
import type { ResultTable } from '../../engine-client/src/result.ts';

/** The Database the bases are declared in, and the runtime that binds it beside the grid's own stores. */
const BASES = 'legend::Bases';
const BASES_RUNTIME = 'legend::BasesRuntime';

export interface ServerBaseOptions {
  /** The grid's model, its connection to the Deephaven server, and the stores its runtime binds to it. */
  readonly model: string;
  readonly connection: string;
  readonly stores: readonly string[];
}

/** A base, as the runner reads it: its live form's text (what it is), and the runner its queries go through. */
interface Base {
  readonly key: string;
  /** The table the server keeps it in, or null: its columns no table declares, so its queries read it as written. */
  readonly name: string | null;
  readonly runner: QueryRunner;
}

/** A plan column's type as the base's table declares it; null: one the fragment does not write. */
function columnType(c: PlanColumn): string | null {
  if (isNumeric(c.type) && !isFractional(c.type)) return 'BIGINT';
  if (isFloat(c.type)) return 'DOUBLE';
  if (familyOf(c.type) === 'text') return 'VARCHAR(65535)';
  if (isDay(c.type)) return 'DATE';
  if (hasTimeOfDay(c.type)) return 'TIMESTAMP';
  return null;
}

export class ServerBaseRunner implements QueryRunner {
  readonly name = 'deephaven';
  readonly #planner: LitePlanner;
  readonly #engine: DeephavenEngine;
  readonly #options: ServerBaseOptions;
  readonly #inner: PlanThenRun;
  #base: Base | null = null;
  #making: { readonly key: string; readonly base: Promise<Base> } | null = null;
  #counts = { based: 0, asWritten: 0, made: 0 };

  constructor(planner: LitePlanner, engine: DeephavenEngine, options: ServerBaseOptions) {
    this.#planner = planner;
    this.#engine = engine;
    this.#options = options;
    this.#inner = new PlanThenRun(planner, engine);
  }

  /** Queries read from a base on the server, queries that read the base as written, and bases made. */
  get counts(): { readonly based: number; readonly asWritten: number; readonly made: number } {
    return { ...this.#counts };
  }

  /** The table the grid's base is kept in now, if it is. */
  get baseTable(): string | null {
    return this.#base?.name ?? null;
  }

  async run(query: Lambda, snapshot: CubeSnapshot, scope?: LevelScope, signal?: AbortSignal): Promise<RunOutcome> {
    return (await this.#on(query, snapshot)).run(query, snapshot, scope, signal);
  }

  async stream(query: Lambda, snapshot: CubeSnapshot, onChunk: (chunk: ResultTable) => void,
    signal?: AbortSignal): Promise<void> {
    return (await this.#on(query, snapshot)).stream(query, snapshot, onChunk, signal);
  }

  // what compiles and what a query's columns are is the base's own: asked of it as written
  compile(query: Lambda, snapshot: CubeSnapshot, signal?: AbortSignal): Promise<void> {
    return this.#inner.compile(query, snapshot, signal);
  }

  relationType(query: Lambda, signal?: AbortSignal): Promise<PlanColumn[]> {
    return this.#inner.relationType(query, signal);
  }

  parse(text: string, signal?: AbortSignal): Promise<Lambda> {
    return this.#inner.parse(text, signal);
  }

  print(query: Lambda, style?: Parameters<QueryRunner['print']>[1], signal?: AbortSignal): Promise<string> {
    return this.#inner.print(query, style, signal);
  }

  /** The runner for `snapshot`'s base, the base made on the server first when it is not kept there now. */
  async #on(query: Lambda, snapshot: CubeSnapshot): Promise<QueryRunner> {
    const live = sourceWithDerived(snapshot).node;
    const key = toJson(live);
    const now = this.#base;
    let base: Base;
    if (now !== null && now.key === key && (now.name === null || this.#engine.keepsBase(now.name))) {
      base = now;
    } else {
      if (this.#making?.key !== key) {
        const making = { key, base: this.#make(key, live, snapshot) };
        this.#making = making;
        making.base.finally(() => { if (this.#making === making) this.#making = null; }).catch(() => { /* said below */ });
      }
      try {
        base = await (this.#making as { readonly base: Promise<Base> }).base;
      } catch (error) {
        throw new QueryFailure(error, query);
      }
    }
    if (base.name === null) this.#counts.asWritten++;
    else this.#counts.based++;
    return base.runner;
  }

  /** `live` made on the server, stored, and declared as a table of the bases' model; its queries rebased onto it. */
  async #make(key: string, live: ValueSpecification, snapshot: CubeSnapshot): Promise<Base> {
    const lambda = sourceWithDerived(snapshot).lambda();
    const plan = await this.#planner.plan(lambda);
    // a column the lowering never leaves null is declared so: no null guard, no null flag on a sort by it (G1)
    const nullable = await this.#planner.nullable(lambda);
    const kept = await this.#engine.keepBase(plan.sql);
    const types = plan.columns.map(columnType);
    const declared = kept.columns.length === plan.columns.length && nullable.length === plan.columns.length
      && kept.columns.every((c, i) => c === plan.columns[i]?.name) && types.every((t) => t !== null);
    const old = this.#base;
    let base: Base;
    if (!declared) {
      this.#engine.letBaseGo(kept.name);
      base = { key, name: null, runner: this.#inner };
    } else {
      const columns = plan.columns.map((c, i) => `${c.name} ${types[i]}${nullable[i] ? '' : ' NOT NULL'}`).join(', ');
      const bound = [...this.#options.stores, BASES].map((s) => `${s}: [ c: ${this.#options.connection} ]`).join(', ');
      this.#planner.useModel(`${this.#options.model}
###Relational
Database ${BASES}
(
  Table ${kept.name} (${columns})
)
###Runtime
Runtime ${BASES_RUNTIME}
{
  mappings: [];
  connections: [ ${bound} ];
}`, BASES_RUNTIME);
      const copy = accessor(BASES, kept.name);
      // the base as each query writes it: with its calculated columns, or, having none, the source alone
      base = { key, name: kept.name, runner: rebased(this.#inner, live, copy) };
      this.#counts.made++;
    }
    this.#base = base;
    if (old?.name && old.name !== base.name) this.#engine.letBaseGo(old.name);
    return base;
  }
}
