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
//
// SEVERAL GRIDS (docs/DEEPHAVEN_PLAN_2026_10_22.md V2): the page's bases are one set (`ServerBases`) over its one
// planner and engine -- grids over the same base read one table, made once and let go when no grid reads it -- and the
// fragment declares every base a grid reads, so one grid's new base never takes another's from the planner's model.

import type { DeephavenEngine } from '../../engine-client/src/deephaven-engine.ts';
import type { QueryEngine } from '../../engine-client/src/engine.ts';
import type { PlanColumn } from '../../engine-client/src/relation-type.ts';
import { familyOf, hasTimeOfDay, isDay, isFloat, isFractional, isNumeric } from '../../engine-client/src/types.ts';
import { accessor, toJson, type Lambda, type ValueSpecification } from '../../pure-protocol/src/index.ts';
import type { LitePlanner } from './lite-planner.ts';
import type { LevelScope } from './query.ts';
import { PlanThenRun, QueryFailure, rebased, type QueryRunner, type RunOutcome, type RunWindow } from './runner.ts';
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
  /**
   * THE BASES IN A MODEL OF THEIR OWN: the connection's text (named `connection`), its store `store`. Given, the bases'
   * Database is declared beside that connection alone, in a planner of its own -- never in the page's model, so the
   * page's planner holds its model once however many bases its grids make (the trader screens, 2026-10-25: the one
   * trading model, 2.3 MB, held again for each new base took 4.4 s). A query read from a base names only the base's
   * columns, so it plans there; one that calls a function of the page's model (a group's calculated column) is refused
   * there, by name.
   */
  readonly connectionOf?: (store: string) => string;
  /**
   * A grid's base as its snapshot writes it: its source and its leaf-level calculated columns (query.ts
   * `sourceWithDerived`). Handed in by the page, whose own code it is: so the code of a server's grids (deephaven-grids.ts,
   * a chunk fetched when a model names a server) reaches none of the query writer the page starts with.
   */
  readonly baseOf: (snapshot: CubeSnapshot) => { lambda(): Lambda; readonly node: ValueSpecification };
}

/** A base as the page holds it: the table the server keeps it in -- or null, its columns no table declares, so its
 *  queries read it as written -- and its columns as the fragment declares them. */
interface MadeBase {
  readonly name: string | null;
  readonly columns: string;
}

/** A base held for the page's grids: how many read it, and its making (made again after a reconnect). */
interface HeldBase {
  holders: number;
  made: Promise<MadeBase>;
}

/** A base as a grid's runner reads it: its key (its live form's text), its table, and the runner its queries go to. */
interface Base {
  readonly key: string;
  readonly name: string | null;
  readonly runner: QueryRunner;
}

/** A base column's type as the base's table declares it: the store type the server HOLDS it as (`held`, its
 *  `deephaven:type`), as every server table is checked against its declaration (DH16, decided strict) -- where that
 *  is the kind the plan computes it as; null: one the fragment does not write. */
function columnType(c: PlanColumn, held: string | null): string | null {
  const integral = isNumeric(c.type) && !isFractional(c.type);
  switch (held) {
    case 'java.lang.Boolean': return familyOf(c.type) === 'boolean' ? 'BIT' : null;
    case 'byte': return integral ? 'TINYINT' : null;
    case 'short': return integral ? 'SMALLINT' : null;
    case 'int': return integral ? 'INTEGER' : null;
    case 'long': return integral ? 'BIGINT' : null;
    case 'float': return isFloat(c.type) ? 'REAL' : null;
    case 'double': return isFloat(c.type) ? 'DOUBLE' : null;
    case 'java.lang.String': return familyOf(c.type) === 'text' ? 'VARCHAR(65535)' : null;
    case 'java.time.LocalDate': return isDay(c.type) ? 'DATE' : null;
    case 'java.time.Instant': return hasTimeOfDay(c.type) ? 'TIMESTAMP' : null;
    default: return null;
  }
}

/**
 * THE PAGE'S BASES (V2): every grid's base on the page's one planner and engine, each made once and kept while a grid
 * reads it, and the one model fragment that declares them all. `runner(reader)` is a grid's runner, its queries run by
 * its own reader (`DeephavenEngine.reader()`: what it follows is its own).
 */
export class ServerBases {
  readonly #planner: LitePlanner;
  readonly #engine: DeephavenEngine;
  readonly #options: ServerBaseOptions;
  /** The bases the page's grids read, by key. */
  readonly #held = new Map<string, HeldBase>();
  /** Each base's columns as its table is declared, by its table: the fragment's tables. */
  readonly #declared = new Map<string, string>();

  /** The bases' own planner, when they are declared in a model of their own (`connectionOf`). */
  readonly #bases: LitePlanner | null;

  constructor(planner: LitePlanner, engine: DeephavenEngine, options: ServerBaseOptions) {
    this.#planner = planner;
    this.#engine = engine;
    this.#options = options;
    // the page's planner's module, a model of its own: held when a base is first declared in it
    this.#bases = options.connectionOf ? planner.withModel(this.#basesModel(), BASES_RUNTIME) : null;
  }

  /** The tables the page's grids read their bases from now. */
  get tables(): readonly string[] {
    return [...this.#declared.keys()];
  }

  /** A grid's base as its snapshot writes it (`ServerBaseOptions.baseOf`). */
  baseOf(snapshot: CubeSnapshot): { lambda(): Lambda; readonly node: ValueSpecification } {
    return this.#options.baseOf(snapshot);
  }

  /** A grid's runner: its queries planned by the page's planner -- read from a base, by the bases' own, when they have
   *  one -- and run by `reader`, its own. */
  runner(reader: QueryEngine = this.#engine): ServerBaseRunner {
    return new ServerBaseRunner(this, new PlanThenRun(this.#planner, reader),
      this.#bases ? new PlanThenRun(this.#bases, reader) : undefined);
  }

  /** The base `key` held for one more grid: made the first time a grid reads it, and made again when the session that
   *  kept it went (a reconnect). */
  async hold(key: string, snapshot: CubeSnapshot): Promise<MadeBase> {
    let held = this.#held.get(key);
    if (held === undefined) {
      held = { holders: 0, made: this.#make(snapshot) };
      this.#held.set(key, held);
    }
    held.holders++;
    try {
      return await this.current(key, snapshot);
    } catch (error) {
      this.release(key);
      throw error;
    }
  }

  /** The base `key` as it is kept now, made again if its session went; for a grid that holds it. */
  async current(key: string, snapshot: CubeSnapshot): Promise<MadeBase> {
    const held = this.#held.get(key);
    if (held === undefined) throw new Error('a base no grid holds was read');
    const was = held.made;
    const made = await was;
    if (made.name === null || this.#engine.keepsBase(made.name)) return made;
    // its session went: made again, once for every grid that reads it
    if (held.made === was) held.made = this.#make(snapshot);
    return held.made;
  }

  /** One grid fewer reads the base `key`: let go, and no longer declared, when none does. */
  release(key: string): void {
    const held = this.#held.get(key);
    if (held === undefined) return;
    held.holders--;
    if (held.holders > 0) return;
    this.#held.delete(key);
    held.made.then((made) => {
      if (made.name === null) return;
      this.#engine.letBaseGo(made.name);
      this.#declared.delete(made.name);
      this.#declare();
    }, () => { /* never made: nothing to let go */ });
  }

  /** `snapshot`'s base made on the server, stored, and declared as a table of the bases' model. */
  async #make(snapshot: CubeSnapshot): Promise<MadeBase> {
    const lambda = this.#options.baseOf(snapshot).lambda();
    const plan = await this.#planner.plan(lambda);
    // a column the lowering never leaves null is declared so: no null guard, no null flag on a sort by it (G1)
    const nullable = await this.#planner.nullable(lambda);
    const kept = await this.#engine.keepBase(plan.sql);
    const types = plan.columns.map((c) => columnType(c, kept.held.find((h) => h.name === c.name)?.type ?? null));
    const declared = kept.columns.length === plan.columns.length && nullable.length === plan.columns.length
      && kept.columns.every((c, i) => c === plan.columns[i]?.name) && types.every((t) => t !== null);
    if (!declared) {
      this.#engine.letBaseGo(kept.name);
      return { name: null, columns: '' };
    }
    const columns = plan.columns.map((c, i) => `${c.name} ${types[i]}${nullable[i] ? '' : ' NOT NULL'}`).join(', ');
    this.#declared.set(kept.name, columns);
    this.#declare();
    return { name: kept.name, columns };
  }

  /** The planner's model: the page's, and every base a grid reads declared in one Database bound beside its stores --
   *  or, the bases in a model of their own, theirs alone. */
  #declare(): void {
    if (this.#bases) {
      this.#bases.useModel(this.#basesModel(), BASES_RUNTIME);
      return;
    }
    const tables = [...this.#declared].map(([name, columns]) => `  Table ${name} (${columns})`).join('\n');
    const bound = [...this.#options.stores, BASES].map((s) => `${s}: [ c: ${this.#options.connection} ]`).join(', ');
    this.#planner.useModel(`${this.#options.model}
###Relational
Database ${BASES}
(
${tables}
)
###Runtime
Runtime ${BASES_RUNTIME}
{
  mappings: [];
  connections: [ ${bound} ];
}`, BASES_RUNTIME);
  }

  /** The bases' own model (`connectionOf`): their Database, the connection, and a runtime binding the one to the other. */
  #basesModel(): string {
    const tables = [...this.#declared].map(([name, columns]) => `  Table ${name} (${columns})`).join('\n');
    return `###Relational
Database ${BASES}
(
${tables}
)
###Connection
${this.#options.connectionOf!(BASES)}
###Runtime
Runtime ${BASES_RUNTIME}
{
  mappings: [];
  connections: [ ${BASES}: [ c: ${this.#options.connection} ] ];
}`;
  }
}

/** A grid's runner (B1): every query reads the grid's base from the table the page keeps it in (`ServerBases`). */
export class ServerBaseRunner implements QueryRunner {
  readonly name = 'deephaven';
  readonly #bases: ServerBases;
  readonly #inner: PlanThenRun;
  /** Its queries read from a base, planned by the bases' own planner when they have one. */
  readonly #based: PlanThenRun;
  /**
   * THE BASES ITS GRID AND CHARTS READ NOW, by key, the one read latest last: a chart of calculated columns of its own (a
   * bin across) reads another base than its grid's, and a runner that held one base at a time moved between the two at
   * every tick -- each move a hold, a release and its queries planned again (the trader screens' option chain and its
   * smile, 2026-10-25: 175 moves in a minute, and results read again from the server). The one read least lately is let
   * go past `HOLDS`.
   */
  readonly #held = new Map<string, Base>();
  /** The bases being made for it, by key. */
  readonly #making = new Map<string, Promise<Base>>();
  /** The base its latest query read. */
  #latest: Base | null = null;
  #closed = false;
  /** The most bases a grid's runner holds at once: its grid's, and a few of its charts'. */
  static readonly HOLDS = 4;
  #counts = { based: 0, asWritten: 0, made: 0 };
  /** A window of a query's result (V1), read from the base as the query's rows are. */
  readonly window: RunWindow;

  constructor(bases: ServerBases, inner: PlanThenRun, based?: PlanThenRun) {
    this.#bases = bases;
    this.#inner = inner;
    this.#based = based ?? inner;
    this.window = async (query, snapshot, window, signal) => {
      const read = (await this.#on(query, snapshot)).window;
      if (read === undefined) throw new QueryFailure(new Error('the Deephaven engine reads no window of a result'), query);
      return read(query, snapshot, window, signal);
    };
  }

  /** Queries read from a base on the server, queries that read the base as written, and bases held for this grid. */
  get counts(): { readonly based: number; readonly asWritten: number; readonly made: number } {
    return { ...this.#counts };
  }

  /** The table the base its latest query read is kept in, if it is. */
  get baseTable(): string | null {
    return this.#latest?.name ?? null;
  }

  async run(query: Lambda, snapshot: CubeSnapshot, scope?: LevelScope, signal?: AbortSignal): Promise<RunOutcome> {
    return (await this.#on(query, snapshot)).run(query, snapshot, scope, signal);
  }

  /**
   * `snapshot`'s base made on the server and held for this grid, no query of it run and no row read: what its first
   * view reads, ready before the view is asked (a grid on a sheet not yet shown, warmed once the sheet shown is drawn:
   * the server's work -- a base's formulas compiled, its table made -- done before a person opens the sheet, and
   * nothing followed into the page meanwhile).
   */
  async warm(snapshot: CubeSnapshot): Promise<void> {
    await this.#on(this.#bases.baseOf(snapshot).lambda(), snapshot);
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

  /** The runner for `snapshot`'s base: the page's, held for this grid while its grid or a chart of it reads it. */
  async #on(query: Lambda, snapshot: CubeSnapshot): Promise<QueryRunner> {
    const live = this.#bases.baseOf(snapshot).node;
    const key = toJson(live);
    let base: Base;
    try {
      const held = this.#held.get(key);
      if (held !== undefined) {
        // the page's base as kept now: made again if the session that kept it went
        const made = await this.#bases.current(key, snapshot);
        base = made.name === held.name ? held : this.#runnerOver(key, live, made);
        // read latest: last in the order they are let go in
        this.#held.delete(key);
        this.#held.set(key, base);
      } else {
        let making = this.#making.get(key);
        if (making === undefined) {
          const made = this.#hold(key, live, snapshot);
          this.#making.set(key, made);
          made.finally(() => { if (this.#making.get(key) === made) this.#making.delete(key); }).catch(() => { /* said below */ });
          making = made;
        }
        base = await making;
      }
      this.#latest = base;
    } catch (error) {
      throw new QueryFailure(error, query);
    }
    if (base.name === null) this.#counts.asWritten++;
    else this.#counts.based++;
    return base.runner;
  }

  /** The grid is gone (B2): every base it reads let go, and one being made for it let go once it is held. */
  close(): void {
    this.#closed = true;
    this.#latest = null;
    for (const key of this.#held.keys()) this.#bases.release(key);
    this.#held.clear();
  }

  /** The base `key` held for this grid, the one read least lately let go past `HOLDS`. */
  async #hold(key: string, live: ValueSpecification, snapshot: CubeSnapshot): Promise<Base> {
    const made = await this.#bases.hold(key, snapshot);
    if (this.#closed) {
      this.#bases.release(key);
      throw new Error('the grid is closed: it reads no base');
    }
    const base = this.#runnerOver(key, live, made);
    this.#held.set(key, base);
    if (made.name !== null) this.#counts.made++;
    for (const old of this.#held.keys()) {
      if (this.#held.size <= ServerBaseRunner.HOLDS) break;
      if (old === key) continue;
      this.#held.delete(old);
      this.#bases.release(old);
    }
    return base;
  }

  #runnerOver(key: string, live: ValueSpecification, made: MadeBase): Base {
    // the base as each query writes it: with its calculated columns, or, having none, the source alone
    return made.name === null
      ? { key, name: null, runner: this.#inner }
      : { key, name: made.name, runner: rebased(this.#based, live, accessor(BASES, made.name)) };
  }
}
