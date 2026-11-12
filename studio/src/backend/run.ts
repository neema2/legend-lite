// Running in Studio (plan A3): a function's body, or a service's query on its mapping and runtime, executed on the
// session's engine over the workspace's whole model. In the tab that is engine-client's in-tab legend engine: the
// planner writes the SQL, DuckDB in this tab runs it, on the model's own test data (its Data elements, loaded before
// each run, plan A2); on a legend server, its pure/v1 execute (the server has its own data).

import { element, fn, type Lambda, type Variable } from '../../../pure-protocol/src/index.ts';
import type { QueryEngine, RawTable } from '../../../engine-client/src/engine.ts';
import type { Engine } from '../../../engine-client/src/legend/engine.ts';
import type { PFunction, PService, PureModelContextData } from '../../../engine-client/src/legend/pmcd.ts';
import type { ExecutionResult, ParameterValue } from '../../../engine-client/src/legend/wire.ts';
import type { TableState, TabTables } from '../../../engine-client/src/tab-data.ts';
import { runAuthored } from '../../../engine-client/src/plan-runner.ts';

export type { TableState };

/** A function's parameter, as Run asks for it: its name, type and multiplicity, as the signature declares them. */
export interface RunParameter {
  readonly name: string;
  readonly type: string;
  readonly multiplicity: string;
}

export interface Runner {
  /** What Run must ask for first: the parameters of the function `fileText` declares ([] for a service, or none). */
  parameters(fileText: string): Promise<RunParameter[]>;
  /**
   * The function or service `fileText` declares, run over `modelText` (the workspace's model, dependencies included);
   * `values`: each parameter's value as Pure text (`'GB'`, `42`, `%2024-06-01`), parsed by the compiler.
   */
  run(fileText: string, modelText: string, values?: ReadonlyMap<string, string>): Promise<ExecutionResult>;
  /** Every table the model's Databases declare: where its rows in the tab are from, and how many (plan A2). */
  tables(modelText: string): Promise<TableState[]>;
  /** A table filled from a person's file (.csv or .parquet), kept over the model's test data until reset. */
  putTable(modelText: string, schema: string, table: string, file: { readonly name: string; readonly bytes: Uint8Array }): Promise<void>;
  /** A table back to the model's own rows. */
  resetTable(modelText: string, schema: string, table: string): Promise<void>;
}

/** What a run needs of the session: the grammar (to read the element), the engine, and the in-tab data step. */
export interface RunSession {
  modelJson(text: string): Promise<PureModelContextData>;
  /** `grammarToJson/lambda`: a parameter's value, read as Pure. */
  lambdaJson(text: string): Promise<Lambda>;
  /** The engine that executes: started the first time it is needed (DuckDB in the tab), then kept. */
  engine(): Promise<Engine>;
  /** The tab's DuckDB itself, for the SQL playground (upstream's: SQL on a connection); a server's has none here. */
  sqlEngine?(): Promise<QueryEngine>;
  /** The person's one statement as the planner carries it on the playground's database, a plan (Boundary.authored: the
   *  playground's, 2026-11-12). */
  authored?(text: string): Promise<string>;
  /** Puts the model's own test data where the engine reads it (the tab's DuckDB); a server needs none. */
  loadData?(model: PureModelContextData, text: string): Promise<void>;
  /** The tab's tables and where their rows are from (plan A2); a server has its own data. */
  tabTables?(): Promise<TabTables>;
}

/** A multiplicity as Pure writes it: [1], [0..1], [*], [1..*]. */
function multiplicityText(m: { readonly lowerBound: number; readonly upperBound?: number } | undefined): string {
  if (!m) return '[1]';
  if (m.upperBound === undefined) return m.lowerBound === 0 ? '[*]' : `[${m.lowerBound}..*]`;
  return m.lowerBound === m.upperBound ? `[${m.lowerBound}]` : `[${m.lowerBound}..${m.upperBound}]`;
}

/** What runs: a function's body with its parameters, or a service's query with `->from(mapping, runtime)`. */
function lambdaOf(own: PureModelContextData): Lambda {
  const f = own.elements.find((e): e is PFunction => e._type === 'function');
  if (f) {
    const parameters: Variable[] = f.parameters.map((p) => ({
      _type: 'var', name: p.name, ...(p.genericType ? { genericType: p.genericType } : {}), ...(p.multiplicity ? { multiplicity: p.multiplicity } : {}),
    }));
    return { _type: 'lambda', parameters, body: [...f.body] } as Lambda;
  }
  const s = own.elements.find((e): e is PService => e._type === 'service');
  if (s) {
    const ex = s.execution;
    if (!ex.func || !ex.mapping || !ex.runtime?.runtime) {
      throw new Error(`${s.package}::${s.name} has no single execution with a mapping and a runtime: Run runs that kind of service, for now`);
    }
    if (ex.func.parameters.length > 0) throw new Error(`${s.package}::${s.name} takes parameters: Run runs a service without any, for now`);
    const body = ex.func.body;
    const last = body[body.length - 1]!;
    return { ...ex.func, body: [...body.slice(0, -1), fn('from', last, element(ex.mapping), element(ex.runtime.runtime))] };
  }
  throw new Error('this element is neither a function nor a service: Run runs those');
}

/**
 * UPSTREAM'S SQL PLAYGROUND: a person's own SQL on the tab's DuckDB, the model's own test data loaded first; refused on a
 * server. Through the planner (the user's ruling of 2026-11-12, "Yes do 5 and 6"): the person's text is a plan's one
 * statement -- carried as written, as the author's SQL (the census's M7-authored), never parsed, rewritten or split
 * (Boundary.authored) -- run by the hosts' plan runner (runAuthored), so the playground sends a plan's statement like
 * every other send. (Until then the one statement no plan carried, named by notPlanned, deleted with it.) An object of
 * its own, apart from the Runner every run is handed: the app makes it and hands it to the editor's SQL playground.
 */
export interface Playground {
  /** `statement`, as the person typed it, over `modelText`'s own test data. */
  sql(statement: string, modelText: string): Promise<RawTable>;
}

export function playground(session: RunSession): Playground {
  return {
    async sql(statement, modelText) {
      if (!session.sqlEngine) throw new Error('the SQL playground runs on DuckDB in this tab; this session runs on a server');
      if (!session.authored) throw new Error('the SQL playground plans the person\'s statement in this tab: this session has no planner of its own');
      const engine = await session.sqlEngine();
      if (session.loadData) await session.loadData(await session.modelJson(modelText), modelText);
      // the person's own SQL, carried by the planner as written: the plan's one statement, run as the plan says
      const plan = await session.authored(statement);
      return runAuthored(plan, (sql) => engine.run(sql, 0));
    },
  };
}

export function runner(session: RunSession): Runner {
  return {
    async parameters(fileText) {
      const lambda = lambdaOf(await session.modelJson(fileText));
      return lambda.parameters.map((p) => ({
        name: p.name,
        type: !p.genericType ? '(untyped)' : p.genericType.rawType._type === 'packageableType' ? p.genericType.rawType.fullPath : 'Relation',
        multiplicity: multiplicityText(p.multiplicity),
      }));
    },
    async run(fileText, modelText, values = new Map()) {
      const lambda = lambdaOf(await session.modelJson(fileText));
      // each value as the compiler reads `|<value>`: a literal of any type, never parsed here
      const parameterValues: ParameterValue[] = [];
      for (const p of lambda.parameters) {
        const text = values.get(p.name);
        if (text === undefined || text.trim() === '') throw new Error(`a value for ${p.name} is needed`);
        const parsed = await session.lambdaJson(`|${text}`);
        if (parsed.body.length !== 1) throw new Error(`a value for ${p.name} is one expression; "${text}" is ${parsed.body.length}`);
        parameterValues.push({ name: p.name, value: parsed.body[0]! });
      }
      const engine = await session.engine();
      if (session.loadData) await session.loadData(await session.modelJson(modelText), modelText);
      return engine.execute({ function: lambda, model: { _type: 'text', code: modelText }, parameterValues });
    },
    async tables(modelText) {
      const { tabs, model } = await tablesOf(modelText);
      return tabs.tables({ text: modelText, elements: model.elements });
    },
    async putTable(modelText, schema, table, file) {
      const { tabs, model } = await tablesOf(modelText);
      await tabs.put({ text: modelText, elements: model.elements }, schema, table, file);
    },
    async resetTable(modelText, schema, table) {
      const { tabs, model } = await tablesOf(modelText);
      await tabs.reset({ text: modelText, elements: model.elements }, schema, table);
    },
  };

  /** The tab's tables, the model's own rows loaded first; refused on a server. */
  async function tablesOf(modelText: string): Promise<{ tabs: TabTables; model: PureModelContextData }> {
    if (!session.tabTables || !session.loadData) throw new Error("a table's rows are put in this tab's DuckDB; this session runs on a server, which has its own data");
    const model = await session.modelJson(modelText);
    await session.loadData(model, modelText);
    return { tabs: await session.tabTables(), model };
  }
}
