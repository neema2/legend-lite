// THE DOOR TO A PROJECT'S OWN DATABASE (C5-T, 2026-11-15: the TypeScript split of plan and run, as Java's doors only
// compose): the planner asked for the database's statements (Boundary.projectData) outside any run, then the database
// made from them inside a run frame (run-frame.ts: a run plans nothing). The tab's databases themselves -- the
// connections held, the cache by model -- are the run side's (project-data.ts ProjectDatabases), which asks nothing.
// A model this tab answered before, its database still open, is not asked of the planner again (its statements are
// its Data rows: 1.9 MB for the markets project, docs/TS_SCALE_2026_11_07.md item 8).

import type { QueryEngine } from '../engine.ts';
import { runLoad } from '../plan-runner.ts';
import type { ProjectDataSource } from '../planner-sources.ts';
import { ProjectDatabases, ProjectRunner, type HeldEngine, type ProjectEngine, type ProjectHost } from '../project-data.ts';
import type { PointedEngine } from '../query-engine.ts';
import type { ProjectDataStatements } from '../requests.ts';
import { inRun } from '../run-frame.ts';

/** The tab's project databases (project-data.ts), each made from what `planner` answers for its model. */
export class ProjectDoor {
  /** The databases this tab holds: the run side, reached through this door alone, inside its runs (2026-11-15, the C5-T
   *  final audit's item 4: until then public, so an app could make or drop one outside any run). */
  readonly #databases: ProjectDatabases;
  readonly #planner: ProjectDataSource;

  constructor(host: ProjectHost, planner: ProjectDataSource) {
    this.#databases = new ProjectDatabases(host);
    this.#planner = planner;
  }

  /** The project databases this tab holds, by name. */
  get names(): readonly string[] {
    return this.#databases.names;
  }

  /** Whether this tab holds the database `name` (no statement: the tab's own record of what it made). */
  holds(name: string): boolean {
    return this.#databases.holds(name);
  }

  /** The planner's statements for `model`'s database on this tab's database type. */
  statements(model: string): Promise<ProjectDataStatements> {
    return this.#planner.projectData(model, this.#databases.databaseType);
  }

  /**
   * The workspace's database (tab-data.ts, a door that composes more), in ONE run: `model`'s, made from `statements` --
   * the planner's, asked before (statements) -- the one of that name dropped first when `how.remake`, `made` run on it
   * as it is made, `how.opened` told of it, then `how.before` dropped when it is another. The steps of the run side, each
   * inside this run: no caller reaches the databases themselves (the C5-T final audit's item 4, 2026-11-15).
   */
  remade(model: string, statements: ProjectDataStatements, hostEngine: QueryEngine,
    made: (engine: HeldEngine, name: string) => Promise<void>,
    how: { readonly remake: boolean; readonly before: string | undefined; readonly opened: (project: ProjectEngine) => void }): Promise<ProjectEngine> {
    return inRun("the workspace's database", async () => {
      if (how.remake) await this.#databases.drop(statements.name);
      const project = await this.#databases.engineFor(model, statements, hostEngine, made, { own: true });
      how.opened(project);
      if (how.before !== undefined && how.before !== statements.name) await this.#databases.drop(how.before);
      return project;
    });
  }

  /**
   * `model`'s project database (ProjectDatabases.engineFor): the one this tab holds for it, else the planner asked for
   * its statements and the database made from them, `made` run on it then -- inside the run, so `made` plans nothing.
   */
  async engineFor(model: string, hostEngine: QueryEngine,
    made?: (engine: HeldEngine, name: string) => Promise<void>, options: { readonly own?: boolean } = {}): Promise<ProjectEngine> {
    const known = options.own ? undefined : this.#databases.known(model, hostEngine);
    if (known !== undefined) return known;
    const statements = await this.statements(model);
    return inRun("a project's database", () => this.#databases.engineFor(model, statements, hostEngine, made, options));
  }

  /** `name` dropped, if this tab holds it (ProjectDatabases.drop): the planner's statements it was made with, run. */
  drop(name: string): Promise<void> {
    return inRun("a project's database dropped", () => this.#databases.drop(name));
  }

  /** Every project database dropped -- between a test's cases, which share one DuckDB. */
  dropAll(): Promise<void> {
    return inRun("the project databases dropped", () => this.#databases.dropAll());
  }
}

/** The engine an app's screens run on (the C5-T last round's T1: made here, so an app reaches the run side through its
 *  doors alone): the host's own connection, pointed at a project's database as one opens (ProjectRunner.use). */
export function projectRunner(host: QueryEngine): PointedEngine {
  return new ProjectRunner(host);
}

/**
 * A model's Data rows loaded where `engine` runs, a database it already is (a warehouse's catalog: Query's warehouse demo
 * seed): the project's Data load as the planner makes it for the server's project plan and the tab's project database
 * (Boundary.projectData, the ONCE load), asked outside any run, then run inside one by the hosts' plan runner -- the
 * statements that make, point at and drop a database of the project's own are not run, the engine being that database.
 * `what` names the rows for a refusal (2026-11-15, the C5-T fold-in: Query's page ran this load itself until then).
 */
export async function dataLoaded(engine: QueryEngine & { readonly databaseType: string }, planner: ProjectDataSource,
  model: string, what: string): Promise<void> {
  const { load } = await planner.projectData(model, engine.databaseType);
  if (load === undefined) throw new Error(`${what} holds no Data rows to load`);
  await inRun("a project's Data rows loaded", () => runLoad(load, (sql, params) => {
    if (params.length > 0) throw new Error("a project's Data load binds no value");
    return engine.run(sql, 0);
  }));
}
