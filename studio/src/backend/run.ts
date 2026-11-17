// Running in Studio (plan A3): a function's body, or a service's query on its mapping and runtime, run on the session's
// engine over the workspace's whole model -- legend-lite's own (engine-client's LiteEngine; the edge program's H3,
// 2026-11-17): lite's planner plans it, the hosts' plan runner runs the plan, its answer typed rows. In the tab, DuckDB
// in this tab runs the plan's statements on the model's own test data (its Data elements, loaded before each run, plan
// A2); on a server, lite's /api/lite/v1 plans and runs it on the server's own data. legend-engine's execute is the
// edge's, never asked here.

import { element, fn, type ExactNumber, type Lambda, type ValueSpecification, type Variable } from '../../../pure-protocol/src/index.ts';
import type { QueryEngine, RawTable } from '../../../engine-client/src/query-engine.ts';
import type { LiteAnswer, LiteEngine } from '../../../engine-client/src/legend/browser-engine.ts';
import type { PFunction, PService, PureModelContextData } from '../../../engine-client/src/legend/pmcd.ts';
import { runtimeOf, statementOf, type ParameterValue } from '../../../engine-client/src/legend/browser-engine.ts';
import type { TableState, TabTables } from '../../../engine-client/src/tab-data.ts';
import { runAuthoredStatement } from '../../../engine-client/src/doors/authored.ts';
import type { PlanText } from '../../../engine-client/src/planned-sql.ts';

export type { TableState };

/** A function's parameter, as Run asks for it: its name, type and multiplicity, as the signature declares them. */
export interface RunParameter {
  readonly name: string;
  readonly type: string;
  readonly multiplicity: string;
}

/** A run's answer as Studio shows it: lite's typed rows (query/run) and the statement its plan answers with
 *  (query/plan), for the SQL badge. */
export type RunAnswer = LiteAnswer & { readonly sql: string };

export interface Runner {
  /** What Run must ask for first: the parameters of the function `fileText` declares ([] for a service, or none). */
  parameters(fileText: string): Promise<RunParameter[]>;
  /**
   * The function or service `fileText` declares, run over `modelText` (the workspace's model, dependencies included);
   * `values`: each parameter's value as Pure text (`'GB'`, `42`, `%2024-06-01`), parsed by the compiler.
   */
  run(fileText: string, modelText: string, values?: ReadonlyMap<string, string>): Promise<RunAnswer>;
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
  /** The engine that plans and runs (legend-lite's): started the first time it is needed (DuckDB in the tab), then
   *  kept. It reads each parameter's value as Pure. */
  engine(): Promise<LiteEngine>;
  /** The tab's DuckDB itself, for the SQL playground (upstream's: SQL on a connection); a server's has none here. */
  /** The playground's database: its engine, and its type as the planner names it (what an authored plan's target is
   *  checked against: plan-runner.ts runAuthored, the C final audit's H9). */
  sqlEngine?(): Promise<{ readonly engine: QueryEngine; readonly databaseType: string }>;
  /** The person's one statement as the planner carries it on the playground's database, a plan (Boundary.authored: the
   *  playground's, 2026-11-12). */
  authored?(text: string): Promise<PlanText>;
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
      const database = await session.sqlEngine();
      if (session.loadData) await session.loadData(await session.modelJson(modelText), modelText);
      // the person's own SQL, carried by the planner as written: the plan's one statement, run as the plan says -- the
      // playground's door (engine-client's doors/authored.ts)
      const authored = session.authored.bind(session);
      return runAuthoredStatement(statement, { authored }, database);
    },
  };
}

/**
 * A parameter's value as its run binds it (plan-runner.ts checkParameters), read from the literal the person wrote (its
 * protocol node, `text` as parsed): a string, a boolean, an Integer or a Float as its number, a Decimal as its digits, a
 * date as its text, a value of the parameter's enumeration as its name, a list as its elements, and a signed number
 * (`-5`) as one literal, as legend-engine's model-data walker reads it. Whether it is a value of the parameter's TYPE the
 * runner checks; anything that is not a literal is refused here by name -- a value is never computed.
 */
export function literalValue(p: Variable, text: string, node: ValueSpecification): ParameterValue {
  const refused = (why: string): never => {
    throw new Error(`a value for ${p.name} is a literal (a string, a number, a boolean, a date, a value of its enumeration, or a list of them): "${text}" ${why}`);
  };
  const enumPath = p.genericType?.rawType._type === 'packageableType' ? p.genericType.rawType.fullPath : undefined;
  const scalar = (n: ValueSpecification): string | number | bigint | boolean => {
    switch (n._type) {
      case 'string': case 'boolean': return n.value;
      case 'strictDate': case 'dateTime': return n.value;
      case 'integer': return wholeNumber(n.value);
      case 'float': return typeof n.value === 'number' ? n.value : Number(n.value.text);
      case 'decimal': return n.value.text;
      case 'func': {
        // `-5`: a minus of one number, the number's sign as written (SpecParser: the ###Pure walker keeps a minus call)
        const [only] = n.parameters;
        if (n.function === 'minus' && n.parameters.length === 1 && only !== undefined
          && (only._type === 'integer' || only._type === 'float' || only._type === 'decimal')) {
          const v = scalar(only);
          return typeof v === 'number' ? -v : typeof v === 'bigint' ? -v : `-${String(v)}`;
        }
        return refused(`is a call of ${n.function}`);
      }
      case 'enumValue':
        return enumPath !== undefined && n.fullPath !== enumPath ? refused(`names a value of ${n.fullPath}, not of ${enumPath}`) : n.value;
      case 'property': {
        // an enumeration's value, `my::Colour.RED`: its name, when the enumeration is the parameter's
        const [owner] = n.parameters;
        if (n.parameters.length === 1 && owner?._type === 'packageableElementPtr') {
          if (enumPath !== undefined && owner.fullPath !== enumPath) {
            return refused(`names a value of ${owner.fullPath}, not of ${enumPath}`);
          }
          return n.property;
        }
        return refused(`reads the property ${n.property}`);
      }
      default: return refused(`is a ${n._type}`);
    }
  };
  if (node._type === 'collection') return node.values.map(scalar);
  return scalar(node);
}

/** An Integer literal's value, exact: a number while safe, a bigint beyond 2^53 (lite's API v1 spec §4: an Integer
 *  parameter takes the 64-bit range; the run refuses one beyond it by name). */
function wholeNumber(v: number | ExactNumber): number | bigint {
  if (typeof v === 'number') {
    if (!Number.isSafeInteger(v)) throw new Error(`the Integer ${String(v)} is past 2^53 as a number: its digits are already lost`);
    return v;
  }
  const exact = BigInt(v.text.replace(/^\+/, ''));
  return Number.isSafeInteger(Number(exact)) ? Number(exact) : exact;
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
      const engine = await session.engine();
      // each value as the compiler reads `|<value>`: a literal of the parameter's type, read by its node (literalValue)
      const parameters: Record<string, ParameterValue> = {};
      for (const p of lambda.parameters) {
        const text = values.get(p.name);
        if (text === undefined || text.trim() === '') throw new Error(`a value for ${p.name} is needed`);
        const parsed = await engine.queryParse(`|${text}`);
        if (parsed.body.length !== 1) throw new Error(`a value for ${p.name} is one expression; "${text}" is ${parsed.body.length}`);
        parameters[p.name] = literalValue(p, text, parsed.body[0]!);
      }
      if (session.loadData) await session.loadData(await session.modelJson(modelText), modelText);
      // lite's query/run, and the statement it runs from query/plan (spec §2: query/run answers rows alone)
      const runtime = runtimeOf(lambda);
      const [answer, plan] = await Promise.all([engine.queryRun(modelText, lambda, runtime, parameters),
        engine.queryPlan(modelText, lambda, runtime)]);
      return { ...answer, sql: statementOf(plan) };
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
