// Running in Studio (plan A3): a function's body, or a service's query on its mapping and runtime, executed on the
// session's engine over the workspace's whole model. In the tab that is engine-client's in-tab legend engine: the
// planner writes the SQL, DuckDB in this tab runs it, on the model's own test data (its Data elements, loaded before
// each run, plan A2); on a legend server, its pure/v1 execute (the server has its own data).

import { element, fn, type Lambda } from '../../../pure-protocol/src/index.ts';
import type { Engine } from '../../../engine-client/src/legend/engine.ts';
import type { PFunction, PService, PureModelContextData } from '../../../engine-client/src/legend/pmcd.ts';
import type { ExecutionResult } from '../../../engine-client/src/legend/wire.ts';

export interface Runner {
  /** The function or service `fileText` declares, run over `modelText` (the workspace's model, dependencies included). */
  run(fileText: string, modelText: string): Promise<ExecutionResult>;
}

/** What a run needs of the session: the grammar (to read the element), the engine, and the in-tab data step. */
export interface RunSession {
  modelJson(text: string): Promise<PureModelContextData>;
  /** The engine that executes: started the first time it is needed (DuckDB in the tab), then kept. */
  engine(): Promise<Engine>;
  /** Puts the model's own test data where the engine reads it (the tab's DuckDB); a server needs none. */
  loadData?(model: PureModelContextData): Promise<void>;
}

/** What runs: a function's body, or a service's query with `->from(mapping, runtime)` (as Query runs its queries). */
function lambdaOf(own: PureModelContextData): Lambda {
  const f = own.elements.find((e): e is PFunction => e._type === 'function');
  if (f) {
    if (f.parameters.length > 0) throw new Error(`${f.package}::${f.name} takes parameters: Run runs a function without any, for now`);
    return { _type: 'lambda', parameters: [], body: [...f.body] } as Lambda;
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
    async run(fileText, modelText) {
      const lambda = lambdaOf(await session.modelJson(fileText));
      const engine = await session.engine();
      if (session.loadData) await session.loadData(await session.modelJson(modelText));
      return engine.execute({ function: lambda, model: { _type: 'text', code: modelText } });
    },
  };
}
