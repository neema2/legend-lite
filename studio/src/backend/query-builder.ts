// The query builder in Studio (plan A5): Query's builder (query/src/embed.ts), opened over the workspace's model on
// the session's engine -- for a service, its query, which Save Query writes back into the service's text (upstream's
// "Edit Query"); for a class, a new query on it (upstream's "Query…"), and for a mapping, a query on its first class
// (upstream's mapping execution) -- which run but are kept nowhere.

import type { Lambda, ValueSpecification } from '../../../pure-protocol/src/index.ts';
import type { PClass, PMapping, PService, PureModelContextData } from '../../../engine-client/src/legend/pmcd.ts';
import type { EditorHandle, EmbedHost, EmbedOptions, EmbedStart } from '../../../query/src/embed.ts';
import { withServiceQuery } from '../model/service-query.ts';

/** Where the builder's queries run, as the session has it: started the first time it is asked for. */
export type Plane = Pick<EmbedOptions, 'execution' | 'engine' | 'planner' | 'cubeRows' | 'user'>;

/** What the builder needs of the session: the grammar, where queries run, and the in-tab data step. */
export interface BuilderSession {
  modelJson(text: string): Promise<PureModelContextData>;
  /** `grammarToJson/lambda`: a query's text, read as Pure. */
  lambdaJson(text: string): Promise<Lambda>;
  plane(): Promise<Plane>;
  /** Puts the model's own test data where the engine reads it (the tab's DuckDB); a server needs none. */
  loadData?(model: PureModelContextData): Promise<void>;
}

export interface QueryBuilder {
  /** The builder in `root`, on the element `elementText` declares (a service or a class), over `modelText`. */
  open(root: HTMLElement, elementText: string, modelText: string, host: EmbedHost): Promise<EditorHandle>;
  /** The service's text with its query replaced by `content`, read back to check it holds exactly that query. */
  serviceWithQuery(serviceText: string, content: string): Promise<string>;
}

export function queryBuilder(session: BuilderSession): QueryBuilder {
  return {
    async open(root, elementText, modelText, host) {
      const start = startOf(await session.modelJson(elementText));
      const json = await session.modelJson(modelText);
      if (session.loadData) await session.loadData(json);
      const plane = await session.plane();
      // Query's builder and DataCube's grid with it, read when first opened
      const { openBuilder } = await import('../../../query/src/embed.ts');
      return openBuilder(root, { ...plane, model: { code: modelText, json }, start, host });
    },
    async serviceWithQuery(serviceText, content) {
      const next = withServiceQuery(serviceText, content);
      const [want, model] = await Promise.all([session.lambdaJson(content), session.modelJson(next)]);
      const got = model.elements.find((e): e is PService => e._type === 'service')?.execution.func;
      if (!got || JSON.stringify(strip(got)) !== JSON.stringify(strip(want))) {
        throw new Error('the query could not be written into the service text as it is laid out: edit it in the text');
      }
      return next;
    },
  };
}

/** What the builder opens on: a single-execution service's query, or a new query on a class. */
function startOf(own: PureModelContextData): EmbedStart {
  const s = own.elements.find((e): e is PService => e._type === 'service');
  if (s) {
    const ex = s.execution;
    if (!ex.func || !ex.mapping || !ex.runtime?.runtime) {
      throw new Error(`${s.package}::${s.name} has no single execution with a mapping and a runtime: the query builder edits that kind of service, for now`);
    }
    return { kind: 'lambda', lambda: ex.func, mapping: ex.mapping, runtime: ex.runtime.runtime };
  }
  const c = own.elements.find((e): e is PClass => e._type === 'class');
  if (c) return { kind: 'class', class: `${c.package}::${c.name}` };
  const m = own.elements.find((e): e is PMapping => e._type === 'mapping');
  if (m) return { kind: 'mapping', mapping: `${m.package}::${m.name}` };
  throw new Error('the query builder opens on a service, a class or a mapping');
}

/** A lambda without where its text was (`sourceInformation`), to compare two readings of it. */
function strip(v: Lambda | ValueSpecification): unknown {
  return JSON.parse(JSON.stringify(v, (k, x: unknown) => (k === 'sourceInformation' ? undefined : x)));
}
