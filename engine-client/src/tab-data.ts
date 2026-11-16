// Where each table's rows come from, in this tab (plan A2): every table a model's Databases declare, filled from the
// model's own test data (its Data elements, in the model's own database inside the tab's DuckDB: project-data.ts) or
// from a file the person dropped in for it (CSV or Parquet) -- which stays, whatever the model's test data says, until
// it is reset -- or from nowhere yet.
//
// A DOOR (C5-T, 2026-11-15: the TypeScript split of plan and run; a multi-step door, as the orchestrator's note 3
// accepts): TabTables asks the planner -- the workspace's database's statements, each file's put, the tables' counts --
// outside any run, then runs what it answered inside a run frame (run-frame.ts: a run plans nothing). Each run step is
// the run side's: the databases (project-data.ts), a file's put (model-data.ts putFile).

import type { QueryEngine } from './engine.ts';
import { loadFileTable, planFilePut } from './doors/file-load.ts';
import type { ProjectDoor } from './doors/project-data.ts';
import { databaseOf, databaseTables, dataTables, putFile, type FilePut, type FileSink } from './model-data.ts';
import type { PlanText } from './planned-sql.ts';
import type { FileLoadSource, HostOperationsSource } from './planner-sources.ts';
import type { HeldEngine, ProjectEngine } from './project-data.ts';
import type { PointedEngine } from './query-engine.ts';
import { inRun } from './run-frame.ts';

type Elements = Parameters<typeof dataTables>[0];

/** A model: its text (what the planner compiles) and its JSON's elements (what this file reads). */
export interface Model {
  readonly text: string;
  readonly elements: Elements;
}

/** Where a table's rows are from: a Data element of the model, a person's file, or nothing yet. */
export type TableSource =
  | { readonly kind: 'test'; readonly element: string }
  | { readonly kind: 'file'; readonly name: string }
  | { readonly kind: 'none' };

export interface TableState {
  readonly database: string;
  readonly schema: string;
  readonly table: string;
  readonly columns: readonly { readonly name: string }[];
  readonly source: TableSource;
  /** How many rows it has; undefined when there is no table yet, or its count failed (countFailed). */
  readonly rows: number | undefined;
  /**
   * Why its rows were not counted -- the count's failure, in the database's words, or a count that answered no count
   * (2026-11-15, the C5-T final audit's item 3): said on this table alone, the other tables' rows still counted.
   */
  readonly countFailed?: string;
  /**
   * The person's file for it that the model no longer takes (its columns changed, say): not in the database, which
   * holds the model's own rows for it, and kept, to go in again when it fits -- why, in the database's words.
   */
  readonly refusedFile?: { readonly name: string; readonly why: string };
}

const key = (schema: string, table: string): string => `${schema}.${table}`;

/** A person's file for a table: its name and bytes (put in again whenever the workspace's database is made). */
interface PutFile {
  readonly schema: string;
  readonly table: string;
  readonly name: string;
  readonly bytes: Uint8Array;
}

/**
 * The tables of the model a workspace runs (Studio's), in this tab: ONE database at a time, the workspace's own
 * (project-data.ts) -- its model's Data rows, and a person's file for a table put in over them. Whenever what fills it
 * changes -- a Data row or a table edited, an undo, a file reset -- it is dropped and made again from the model and the
 * person's files as they are now, so it can never hold anything else (the user, 2026-10-13: "blow away and recreate";
 * nothing about earlier databases is tracked). An edit that changes neither (a class) keeps it: it is named by what it
 * holds. A file put goes straight into the database there. The screens run on `runner`, pointed at it.
 */
export class TabTables {
  readonly #projects: ProjectDoor;
  readonly #runner: PointedEngine;
  readonly #host: QueryEngine & { readonly databaseType: string };
  readonly #sink: Pick<FileSink, 'registerFileBuffer' | 'dropFile'>;
  readonly #source: FileLoadSource & HostOperationsSource;
  /** The tables a person's file fills, by `schema.table`: the person's own state. */
  readonly #files = new Map<string, PutFile>();
  /** How many times the person's files changed; the database there now was made at `#current.files`. */
  #filesChanged = 0;
  /** The database there now: its name, and the files' count it was made with. */
  #current: { readonly name: string; readonly files: number } | undefined;
  /**
   * The person's files the database there now does not hold, by `schema.table`, and why: the model no longer declares
   * the table once, or the file no longer loads into it. Kept in `#files`, to go in again when it fits.
   */
  readonly #refused = new Map<string, string>();
  #uploads = 0;
  /**
   * The last step begun (2026-11-15, the C5-T final audit's MUST 2): load, put, reset and tables run ONE AT A TIME, each
   * after the one before has settled. A step plans before it runs (its files hashed, each put asked of the planner:
   * seconds), so two at once interleaved -- a reset planned without a file whose put then landed, the database made
   * again without it and recorded as current, and the panel naming a file that Run never read. Planning stays inside
   * each step, outside its run.
   */
  #turn: Promise<unknown> = Promise.resolve();

  /**
   * `projects`: the workspace's databases, each made from the planner's statements; `host`: the tab's own connection
   * (no model's database is ever it here); `files`: where a file's bytes are registered for a read, and let go of after
   * (the tab's database); `source`: the planner's plan for a table a file fills (fileLoad: a planned load, B3) and the
   * tables' counts (its host operations).
   */
  constructor(projects: ProjectDoor, runner: PointedEngine, host: QueryEngine & { readonly databaseType: string },
    files: Pick<FileSink, 'registerFileBuffer' | 'dropFile'>, source: FileLoadSource & HostOperationsSource) {
    this.#projects = projects;
    this.#runner = runner;
    this.#host = host;
    this.#sink = files;
    this.#source = source;
  }

  /** `step`, once every step begun before it has settled (its failure no bar to the next). */
  #oneAtATime<T>(step: () => Promise<T>): Promise<T> {
    const taken = this.#turn.then(step, step);
    this.#turn = taken.catch(() => undefined);
    return taken;
  }

  /** The workspace's database for the model as it is now, and the screens on it. */
  load(model: Model): Promise<void> {
    return this.#oneAtATime(async () => {
      await this.#open(model);
    });
  }

  /**
   * `schema.table` filled from a person's file (a .csv or .parquet), until reset -- the table the one Database that
   * declares it makes (databaseOf: none, or two, is refused). A refused file leaves the table as it was.
   */
  put(model: Model, schema: string, table: string, file: { readonly name: string; readonly bytes: Uint8Array }): Promise<void> {
    return this.#oneAtATime(() => this.#put(model, schema, table, file));
  }

  async #put(model: Model, schema: string, table: string, file: { readonly name: string; readonly bytes: Uint8Array }): Promise<void> {
    const { held } = await this.#open(model);
    // the workspace's own database (own: true), always made: its connection takes the put's bound statements
    if (held === undefined) throw new Error("the workspace's database was not made");
    await loadFileTable(this.#fileSink(held), this.#source, this.#host.databaseType, model.text,
      this.#target(model, schema, table), file, `upload-${++this.#uploads}`);
    this.#files.set(key(schema, table), { schema, table, name: file.name, bytes: file.bytes });
    this.#refused.delete(key(schema, table));
    // the database there holds it already: it is as the files are now
    this.#current = { name: this.#current!.name, files: ++this.#filesChanged };
  }

  /** `schema.table` back to the model's own rows (none: no table): the database made again without the file. */
  reset(model: Model, schema: string, table: string): Promise<void> {
    return this.#oneAtATime(async () => {
      if (this.#files.delete(key(schema, table))) this.#filesChanged++;
      this.#refused.delete(key(schema, table));
      await this.#open(model);
    });
  }

  /** Every table the model declares: where its rows are from, and how many there are (counted by the planner's name). */
  tables(model: Model): Promise<TableState[]> {
    return this.#oneAtATime(() => this.#tables(model));
  }

  async #tables(model: Model): Promise<TableState[]> {
    const { engine } = await this.#open(model);
    const tests = new Map(dataTables(model.elements).map((t) => [key(t.schema, t.table), t.element]));
    const declared = databaseTables(model.elements).map((t) => {
      const refused = this.#refused.get(key(t.schema, t.table));
      const file = refused === undefined ? this.#files.get(key(t.schema, t.table)) : undefined;
      const element = tests.get(key(t.schema, t.table));
      const source: TableSource = file !== undefined ? { kind: 'file', name: file.name } : element !== undefined ? { kind: 'test', element } : { kind: 'none' };
      return { t, refused, source };
    });
    // a table nothing fills (no Data element's rows, no file of the person's) is not made, so it has no rows to count and
    // none is asked; every other table's rows are counted by the planner's statement for it (D5), the table named as the
    // model declares it, and a count that fails, or answers no count, is said -- never read as a table not made (G7; the
    // C5-T last round's T3, 2026-11-15: a catch-all here read every failure so) -- on THAT table's row, by its words, the
    // others still counted (the C5-T final audit's item 3: one table's failure no longer blanks the whole list)
    const made = declared.filter((d) => d.source.kind !== 'none');
    const counts = made.length === 0 ? [] : await this.#source.hostOperations(this.#host.databaseType, made.map(({ t }) => ({
      op: 'count' as const, from: { relation: { schema: t.schema, name: t.table, declared: true } },
    })));
    // one count of one statement for each made table, or the planner's answer is wrong, said once by name -- never as a
    // count that failed on every row (the C5-T re-check's item 2, 2026-11-15: the row's catch is the run's and the
    // count's alone)
    if (counts.length !== made.length || counts.some((c) => c.length !== 1)) {
      throw new Error(`the planner answered ${made.length} tables' counts with ${counts.length} plans of `
        + `[${counts.map((c) => c.length).join(', ')}] statements, not one plan of one statement each`);
    }
    const out: TableState[] = [];
    for (const d of declared) {
      const { t, refused, source } = d;
      let rows: number | undefined;
      let countFailed: string | undefined;
      if (source.kind !== 'none') {
        const count = counts[made.indexOf(d)]![0]!;
        try {
          const counted = await inRun("a table's rows counted", () => engine.run(count, 0));
          const raw = counted.rowCount === 1 ? counted.columns[0]?.values[0] : undefined;
          const n = raw === null || raw === undefined ? Number.NaN : Number(raw);
          if (!Number.isSafeInteger(n) || n < 0) throw new Error(`the rows of ${t.schema}.${t.table} were counted as ${String(raw)}, which is no count`);
          rows = n;
        } catch (e) {
          countFailed = e instanceof Error ? e.message : String(e);
        }
      }
      out.push({ ...t, source, rows, ...(countFailed !== undefined ? { countFailed } : {}),
        ...(refused !== undefined ? { refusedFile: { name: this.#files.get(key(t.schema, t.table))!.name, why: refused } } : {}) });
    }
    return out;
  }

  /**
   * The workspace's database for `model` as it is now: the one there when nothing that fills it changed, else made
   * from the model's Data rows and the person's files (they go in before any query reaches it; a file the model no
   * longer takes stays out, its table the model's own rows, and is said in `tables`), the screens pointed at it. When
   * the model changed, the one before is dropped after, so a run the screens began on it finishes there; when only
   * the files did (a reset), the same name is dropped first and made again, so a run on it then fails once.
   */
  async #open(model: Model): Promise<ProjectEngine> {
    // the planner asked first, outside the run: the database's statements (own: always asked), and -- only when the
    // database is made now -- each of the person's files' puts (a file the planner refuses is said, kept out)
    const statements = await this.#projects.statements(model.text);
    const name = statements.name;
    const remake = this.#current?.name === name && this.#current.files !== this.#filesChanged;
    const puts = remake || !this.#projects.holds(name) ? await this.#planPuts(model) : new Map<string, Put>();
    const made = async (engine: HeldEngine): Promise<void> => {
      this.#refused.clear();
      for (const [k, put] of puts) {
        try {
          if ('refused' in put) throw put.refused;
          await putFile(this.#fileSink(engine), put.plan, put.put);
        } catch (e) {
          // the person's own edit made it not fit (a table renamed, its columns changed): the rest of the workspace runs
          this.#refused.set(k, e instanceof Error ? e.message : String(e));
        }
      }
    };
    // in ONE run, the door's (ProjectDoor.remade): the same model with the person's files changed (a reset) dropped and
    // made again; the screens pointed at it; the one before dropped when it is another
    return this.#projects.remade(model.text, statements, this.#host, made, {
      remake,
      before: this.#current?.name,
      opened: (project) => {
        this.#current = { name, files: this.#filesChanged };
        this.#runner.use(project.engine);
      },
    });
  }

  /** Each of the person's files planned for its table, by `schema.table`: its put and plan, or why it was refused. */
  async #planPuts(model: Model): Promise<Map<string, Put>> {
    const out = new Map<string, Put>();
    for (const [k, file] of this.#files) {
      try {
        out.set(k, await planFilePut(this.#source, this.#host.databaseType, model.text,
          this.#target(model, file.schema, file.table), file, `upload-${++this.#uploads}`));
      } catch (e) {
        out.set(k, { refused: e });
      }
    }
    return out;
  }

  /** The table `schema.table` of the one Database of `model` that declares it (databaseOf: none, or two, is refused). */
  #target(model: Model, schema: string, table: string): { readonly database: string; readonly schema: string; readonly table: string } {
    const database = databaseOf(model.elements, schema, table, 'a file fills');
    return databaseTables(model.elements).find((t) => t.database === database && t.schema === schema && t.table === table)!;
  }

  /** Where a file's bytes go on `engine`: registered on the tab's database, the put's statements on `engine`, held. */
  #fileSink(engine: HeldEngine): FileSink {
    return {
      registerFileBuffer: (name, bytes) => this.#sink.registerFileBuffer(name, bytes),
      ...(this.#sink.dropFile ? { dropFile: (name: string) => this.#sink.dropFile!(name) } : {}),
      hold: (work) => engine.exclusive((held) => work((sql, params) => held.rows(sql, params))),
    };
  }
}

/** A person's file planned for its table: its put and the planner's plan, or why the planner (or the model) refused it. */
type Put = { readonly put: FilePut; readonly plan: PlanText } | { readonly refused: unknown };
