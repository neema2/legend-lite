// A PROJECT'S OWN DATABASE in this tab (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md C5): where a project version's Data
// rows live -- a database of their own inside the tab's DuckDB, its schemas as the model declares them, never beside a
// person's own tables. What it is, and every statement that makes, points at and drops it, is the planner's
// (Boundary.projectData, the platform's ProjectData in Java, by the tab's database type); this tab spells none. Its rows
// are the plan's: the ONCE load a server's project plan carries, run here by the hosts' plan runner (runLoad), its
// statements a blob checked against its digest (B3, 2026-11-11: until then the fill's statements as text, run outside
// any plan, census M14).

import type { HeldConnection } from './duckdb.ts';
import type { QueryEngine } from './engine.ts';
import { runLoad } from './plan-runner.ts';
import type { PlannedSql, PlanText } from './planned-sql.ts';

/** The planner's answer for one project's database: its name, and the statements of each step, in order. */
export interface ProjectDataStatements {
  /** Its name, from what it holds: the model's Databases and Data rows. */
  readonly name: string;
  /** Make it, empty (on any connection). */
  readonly open: readonly PlannedSql[];
  /** Point a connection of its own at it. */
  readonly use: readonly PlannedSql[];
  /** The plan whose root is the ONCE load that makes and fills every table a relational Data element fills (on that
   *  connection), PlanJson's text as the planner answered it; absent for a model with no Data rows. */
  readonly load?: PlanText;
  /** Drop it, with everything in it (on any connection). */
  readonly close: readonly PlannedSql[];
}

export interface ProjectDataSource {
  /** Named by what it holds (the model's Databases and Data rows): an edited row is a fresh database. */
  projectData(model: string, databaseType: string): Promise<ProjectDataStatements>;
}

/** A connection of a project's database: its queries, and itself held for a run of statements with bound values (a
 *  person's file put in: a planned load's, model-data.ts loadFileTable; DuckDbEngine.exclusive). */
export interface HeldEngine extends QueryEngine {
  exclusive<T>(work: (held: Pick<HeldConnection, 'rows'>) => Promise<T>): Promise<T>;
}

/** What a project's database needs of the tab's DuckDB: its type, statements on any connection, and connections of its own. */
export interface ProjectHost {
  /** The tab's database type, as the planner names it ('DuckDB'). */
  readonly databaseType: string;
  /** Run a statement on the host's own connection (a project's database is made there). */
  run(sql: PlannedSql): Promise<unknown>;
  /** A connection of its own, set up as the host's (duckdb-tab.ts `connect`). */
  connect(): Promise<HeldEngine>;
}

/** What `engineFor` answers: the engine whose queries read the project's rows. */
export interface ProjectEngine {
  readonly engine: QueryEngine;
  /** The project database's connection, held for a run of statements with bound values (a person's file put in);
   *  none where the queries run on the host's own connection. */
  readonly held?: HeldEngine;
  /** Its name; none when the model has no Data rows and its queries run on the host's own connection. */
  readonly name: string | undefined;
}

/**
 * The tab's project databases: each made and filled the first time a query over a model with Data rows asks, on a
 * connection of its own, and kept for the page's life (the user, 2026-10-13: no strategy for letting one go -- a tab
 * mostly shows one project, a database is named by what it holds, so a new one is only another project's or an edited
 * row's; legend-lite's server keeps its own so too). No policy here lets one go, so an engine a screen holds never goes
 * stale; an owner may drop its own (Studio replaces its workspace's database when what fills it changes). If a
 * measured page ever holds too much, a cap comes back then (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md C5). A model with
 * no Data rows has none, unless asked for (`own`): its queries run on the host's own connection (a demo seeded there).
 */
export class ProjectDatabases {
  readonly #host: ProjectHost;
  readonly #source: ProjectDataSource;
  /** Each database held: its connection, its users' engine over it, and the planner's statements that drop it. */
  readonly #open = new Map<string, Promise<Held>>();
  /** A drop still running, by name: a database made again under that name waits for it. */
  readonly #closing = new Map<string, Promise<void>>();

  constructor(host: ProjectHost, source: ProjectDataSource) {
    this.#host = host;
    this.#source = source;
  }

  /** The project databases this tab holds, by name. */
  get names(): readonly string[] {
    return [...this.#open.keys()];
  }

  /**
   * `model`'s project database (named by what it holds): made and filled the first time, and `made` run on it then,
   * before any query of anyone's reaches it (Studio puts a person's files in over its rows). The engine answered is
   * the keeper's: its close does nothing (the database is kept). `own`: a database even for a model with no Data rows
   * (Studio's workspace, whose files go nowhere else).
   */
  async engineFor(model: string, hostEngine: QueryEngine,
    made?: (engine: HeldEngine, name: string) => Promise<void>, options: { readonly own?: boolean } = {}): Promise<ProjectEngine> {
    const statements = await this.#source.projectData(model, this.#host.databaseType);
    if (statements.load === undefined && !options.own) return { engine: hostEngine, name: undefined };
    let opening = this.#open.get(statements.name);
    if (!opening) {
      opening = this.#make(statements, made);
      this.#open.set(statements.name, opening);
      // a failed make is forgotten: the next ask makes it again
      opening.catch(() => this.#open.delete(statements.name));
    }
    const users = (await opening).users;
    return { engine: users, held: users, name: statements.name };
  }

  /**
   * `name` dropped, if this tab holds it: its owner's to ask (Studio replacing its workspace's database when what fills
   * it changed) -- no policy here lets one go.
   */
  async drop(name: string): Promise<void> {
    const opening = this.#open.get(name);
    if (!opening) return;
    this.#open.delete(name);
    const closing = (async () => {
      const held = await opening.catch(() => undefined);
      if (!held) return;
      await held.connection.close();
      for (const sql of held.close) await this.#host.run(sql);
    })();
    this.#closing.set(name, closing);
    try {
      await closing;
    } finally {
      if (this.#closing.get(name) === closing) this.#closing.delete(name);
    }
  }

  /** Every project database dropped -- between a test's cases, which share one DuckDB. */
  async dropAll(): Promise<void> {
    for (const name of [...this.#open.keys()]) await this.drop(name);
  }

  async #make(statements: ProjectDataStatements, made?: (engine: HeldEngine, name: string) => Promise<void>): Promise<Held> {
    // the same name dropped a moment ago: its database gone before this one is made
    await this.#closing.get(statements.name)?.catch(() => undefined);
    for (const sql of statements.open) await this.#host.run(sql);
    let engine: HeldEngine | undefined;
    let users: HeldEngine | undefined;
    try {
      engine = await this.#host.connect();
      for (const sql of statements.use) await engine.run(sql, 0);
      // its rows: the plan's load, run on its own connection
      const on = engine;
      if (statements.load !== undefined) await runLoad(statements.load, (sql) => on.run(sql, 0));
      users = kept(engine);
      if (made) await made(users, statements.name);
    } catch (e) {
      // a half-made database left behind would be found again by its name: drop it
      if (engine) await engine.close().catch(() => undefined);
      for (const sql of statements.close) await this.#host.run(sql).catch(() => undefined);
      throw e;
    }
    return { connection: engine, users: users!, close: statements.close };
  }
}

/** A database held: its connection, the engine its users are given over it, and the statements that drop it. */
interface Held {
  readonly connection: HeldEngine;
  readonly users: HeldEngine;
  readonly close: readonly PlannedSql[];
}

/** A kept database's connection for its users: every call but close, which is the keeper's alone. */
function kept(engine: HeldEngine): HeldEngine {
  return {
    name: engine.name,
    execute: (...args) => engine.execute(...args),
    stream: (...args) => engine.stream(...args),
    run: (...args) => engine.run(...args),
    exclusive: (work) => engine.exclusive(work),
    close: () => Promise.resolve(),
  };
}

/**
 * The engine an app's screens run on when one project is open at a time (Query's, Studio's): the host's own
 * connection, or the open project's database once `use`d. Switching drops nothing: a query running keeps the
 * connection it began on, and the project left keeps its database.
 */
export class ProjectRunner implements QueryEngine {
  readonly #host: QueryEngine;
  #current: QueryEngine;

  constructor(host: QueryEngine) {
    this.#host = host;
    this.#current = host;
  }

  get name(): string {
    return this.#host.name;
  }

  /** Run on `engine` from now (a project's, from ProjectDatabases), or on the host's own with none. */
  use(engine: QueryEngine | undefined): void {
    this.#current = engine ?? this.#host;
  }

  execute(...args: Parameters<QueryEngine['execute']>): ReturnType<QueryEngine['execute']> {
    return this.#current.execute(...args);
  }

  stream(...args: Parameters<QueryEngine['stream']>): ReturnType<QueryEngine['stream']> {
    return this.#current.stream(...args);
  }

  run(...args: Parameters<QueryEngine['run']>): ReturnType<QueryEngine['run']> {
    return this.#current.run(...args);
  }

  close(): Promise<void> {
    return this.#host.close();
  }
}
