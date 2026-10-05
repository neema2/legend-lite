// Running a function in Studio (plan A3): its body -- the lambda the editor's file declares -- executed on the session's
// engine over the workspace's whole model. In the tab that is engine-client's in-tab legend engine: the planner writes
// the SQL, DuckDB in this tab runs it, on the model's own test data (its Data elements, loaded before each run, plan
// A2); on a legend server, its pure/v1 execute (the server has its own data).

import type { Engine } from '../../../engine-client/src/legend/engine.ts';
import type { PFunction, PureModelContextData } from '../../../engine-client/src/legend/pmcd.ts';
import type { ExecutionResult } from '../../../engine-client/src/legend/wire.ts';

export interface Runner {
  /** The function `fileText` declares, run over `modelText` (the workspace's whole model, dependencies included). */
  run(fileText: string, modelText: string): Promise<ExecutionResult>;
}

/** What a run needs of the session: the grammar (to read the function), the engine, and the in-tab data step. */
export interface RunSession {
  modelJson(text: string): Promise<PureModelContextData>;
  /** The engine that executes: started the first time it is needed (DuckDB in the tab), then kept. */
  engine(): Promise<Engine>;
  /** Puts the model's own test data where the engine reads it (the tab's DuckDB); a server needs none. */
  loadData?(model: PureModelContextData): Promise<void>;
}

export function runner(session: RunSession): Runner {
  return {
    async run(fileText, modelText) {
      const own = await session.modelJson(fileText);
      const fn = own.elements.find((e): e is PFunction => e._type === 'function');
      if (!fn) throw new Error('this element is not a function: Run runs a function');
      if (fn.parameters.length > 0) throw new Error(`${fn.package}::${fn.name} takes parameters: Run runs a function without any, for now`);
      const engine = await session.engine();
      if (session.loadData) await session.loadData(await session.modelJson(modelText));
      return engine.execute({ function: { _type: 'lambda', parameters: [], body: [...fn.body] }, model: { _type: 'text', code: modelText } });
    },
  };
}
