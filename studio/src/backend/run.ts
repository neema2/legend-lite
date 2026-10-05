// Running in Studio (plan A3): a function's body, or a service's query on its mapping and runtime, executed on the
// session's engine over the workspace's whole model. In the tab that is engine-client's in-tab legend engine: the
// planner writes the SQL, DuckDB in this tab runs it, on the model's own test data (its Data elements, loaded before
// each run, plan A2); on a legend server, its pure/v1 execute (the server has its own data).

import { element, fn, type Lambda, type Variable } from '../../../pure-protocol/src/index.ts';
import type { Engine } from '../../../engine-client/src/legend/engine.ts';
import type { PFunction, PService, PureModelContextData } from '../../../engine-client/src/legend/pmcd.ts';
import type { ExecutionResult, ParameterValue } from '../../../engine-client/src/legend/wire.ts';

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
}

/** What a run needs of the session: the grammar (to read the element), the engine, and the in-tab data step. */
export interface RunSession {
  modelJson(text: string): Promise<PureModelContextData>;
  /** `grammarToJson/lambda`: a parameter's value, read as Pure. */
  lambdaJson(text: string): Promise<Lambda>;
  /** The engine that executes: started the first time it is needed (DuckDB in the tab), then kept. */
  engine(): Promise<Engine>;
  /** Puts the model's own test data where the engine reads it (the tab's DuckDB); a server needs none. */
  loadData?(model: PureModelContextData): Promise<void>;
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
        parameterValues.push({ name: p.name, value: parsed.body[0]! });
      }
      const engine = await session.engine();
      if (session.loadData) await session.loadData(await session.modelJson(modelText));
      return engine.execute({ function: lambda, model: { _type: 'text', code: modelText }, parameterValues });
    },
  };
}
