// SAVED QUERIES as a cube's source (the user, 2026-10-01: "load from a saved Query"): read through
// the one query-store client (query-store/README.md) -- a server's `/api/pure/v1/query`, or the same
// API answered in the page from this origin's browser store, which the Query app shares. The record is upstream's `Query`, and the reading rules
// are the ones Query writes by (fixtures/saved-queries/README.md, tested against those records):
//
//   1. the execution context names the mapping and runtime: `explicitExecutionContext` directly;
//      `dataSpaceExecutionContext` through the data space's execution context named
//      `executionKey` (else its default) -- read from the project's model as the compiler gives
//      it (`grammarToJson/model`), never from its text;
//   2. `content` is the lambda as Pure text without `->from(...)`: parsed by the compiler, its one
//      expression is the cube's source, and the planner reads every query of the cube through
//      `->from(mapping, runtime)` (planner.ts `ModelOptions.mapping`) -- outermost, as Query runs it;
//   3. a parameter takes its `defaultParameterValues` entry, parsed as `|<content>`; a parameter
//      with none cannot be a source (the cube has nowhere to ask for it).
//
// And one of DataCube's own: 4. a column the compiler types with an ENUMERATION is read as its
// value's name -- `->toString()` in the projection that makes it (`enumsAsStrings`). legend-lite
// cannot plan an enumeration column on DuckDB at all, and DataCube's types are the primitives; the
// name is what both engines' SQL yields once the mapping's EnumerationMapping is applied (probed
// 2026-10-01: `CASE WHEN SIDE = 'BUY' THEN 'BUY' ...`).
//
// A query whose answer is objects (graphFetch, serialize) is not a cube's source: it is listed,
// and says why.

import {
  findAll, fn, isLambda, lambda, transform, type ColSpec, type Lambda, type ValueSpecification,
} from '../../pure-protocol/src/index.ts';
import { contextOf as readContext, projectOf, type QueryContext } from '../../query-store/src/context.ts';
import type { Query } from '../../query-store/src/wire.ts';
import type { ModelOptions } from './planner.ts';
import type { RelationTyper } from './source-columns.ts';

/** A project's model as the compiler gives it (PureModelContextData elements): what is read here. */
export interface ModelElement {
  readonly _type: string;
  readonly package?: string;
  readonly name?: string;
  readonly defaultExecutionContext?: string;
  readonly executionContexts?: readonly {
    readonly name: string;
    readonly mapping?: { readonly path: string };
    readonly defaultRuntime?: { readonly path: string };
  }[];
}

const pathOf = (e: ModelElement): string => (e.package ? `${e.package}::${e.name ?? ''}` : (e.name ?? ''));

/** The enumerations of a project's model: their columns are read as String (the engines carry names). */
export function enumerationsOf(elements: readonly ModelElement[]): ReadonlySet<string> {
  return new Set(elements.filter((e) => e._type === 'Enumeration').map(pathOf));
}

/**
 * Rule 1: the mapping and runtime a saved query runs on -- query-store's one reader (context.ts, Query's too), over this
 * project's model elements' data spaces.
 */
export function contextOf(
  q: Pick<Query, 'executionContext' | 'groupId' | 'artifactId' | 'versionId'>,
  elements: readonly ModelElement[],
): QueryContext {
  return readContext(q, (path) => elements.find((e) => e._type === 'dataSpace' && pathOf(e) === path));
}

export { projectOf };

/** Functions whose answer is objects, not rows. */
const OBJECT_ANSWERS = new Set(['graphFetch', 'graphFetchChecked', 'serialize']);

/** Why a parsed saved query cannot be a cube's source, or undefined when it can. */
export function unusable(lambda: Lambda): string | undefined {
  const called = new Set<string>();
  for (const f of findAll(lambda, (n): n is ValueSpecification & { function: string } => n._type === 'func')) {
    called.add(f.function.slice(f.function.lastIndexOf(':') + 1));
  }
  if ([...called].some((f) => OBJECT_ANSWERS.has(f))) return 'its answer is objects, not rows: open it in Query';
  if (lambda.body.length !== 1) return 'it is several statements: a cube reads one expression';
  return undefined;
}

/**
 * Rules 2 and 3: the saved query as a cube's source -- its parameters bound to their saved values
 * (`values`, each already parsed), its one expression. The mapping and runtime are the planner's
 * (rule 1, `contextOf`).
 */
export function sourceOf(lambda: Lambda, values: ReadonlyMap<string, ValueSpecification>): ValueSpecification {
  const why = unusable(lambda);
  if (why) throw new Error(why);
  const names = new Set(lambda.parameters.map((p) => p.name));
  const missing = [...names].filter((n) => !values.has(n));
  if (missing.length > 0) throw new Error(`it needs a value for ${missing.map((m) => `'${m}'`).join(', ')}, and none is saved`);
  // a parameter's name reused by an inner lambda would be bound in the wrong place
  if (findAll(lambda.body[0]!, isLambda).some((inner) => inner.parameters.some((p) => names.has(p.name)))) {
    throw new Error('an inner lambda reuses a parameter’s name: open it in Query');
  }
  return transform(lambda.body[0]!, (n) => (n._type === 'var' && values.has(n.name) ? values.get(n.name)! : n));
}

/**
 * Rule 4: the columns `named` -- typed by an enumeration -- read as their values' names: the
 * column spec that MAKES each (a projection's `Side: x|$x.side`, no aggregate) gets
 * `->toString()` on its value. A column made where no such spec is refuses, by name.
 */
export function enumsAsStrings(source: ValueSpecification, named: ReadonlySet<string>): ValueSpecification {
  const left = new Set(named);
  const spec = (c: ColSpec): ColSpec => {
    if (!left.has(c.name) || !c.function1 || c.function2 || c.function1.body.length !== 1) return c;
    left.delete(c.name);
    return { ...c, function1: { ...c.function1, body: [fn('toString', c.function1.body[0]!)] } };
  };
  const out = transform(source, (n) => {
    if (n._type !== 'classInstance') return n;
    if (n.type === 'colSpec') return { ...n, value: spec(n.value as ColSpec) } as ValueSpecification;
    if (n.type === 'colSpecArray') {
      const v = n.value as { colSpecs: readonly ColSpec[] };
      return { ...n, value: { ...v, colSpecs: v.colSpecs.map(spec) } } as ValueSpecification;
    }
    return n;
  });
  if (left.size > 0) {
    throw new Error(`its column${left.size === 1 ? '' : 's'} ${[...left].map((c) => `'${c}'`).join(', ')} ${left.size === 1 ? 'is an enumeration' : 'are enumerations'} a cube cannot read here: project ${left.size === 1 ? 'it' : 'them'} in the query`);
  }
  return out;
}

/** What reads a project's model for a saved query: the host's compiler -- its elements, a lambda's text parsed. */
export interface QueryReading {
  elements(model: string): Promise<readonly unknown[]>;
  parse(text: string): Promise<Lambda>;
}

/**
 * A SAVED QUERY OPENED AS A GRID'S BASE, the one rule (the tab's app/sources.ts openedRecord; headless host-tables.ts, a
 * page on the host: docs/DATACUBE_ONE_API_PLAN_2026_10_21.md X1, X5): rules 1-4 over its project's model text (`model`)
 * -- the mapping and runtime it runs on and the model's enumerations (`how`), its one expression with each parameter's
 * saved value, and each column the compiler types with an enumeration read as its value's name (rule 4: typed by
 * `typerOf`, a typer over the model through the runtime and mapping found).
 */
export async function savedQueryOpened(
  q: Pick<Query, 'name' | 'groupId' | 'artifactId' | 'versionId' | 'content' | 'executionContext' | 'defaultParameterValues'>,
  model: string, reading: QueryReading, typerOf: (runtime: string, how: ModelOptions) => RelationTyper,
): Promise<{ readonly runtime: string; readonly how: ModelOptions; readonly source: ValueSpecification }> {
  const elements = await reading.elements(model) as ModelElement[];
  const context = contextOf(q, elements);
  const parsed = await reading.parse(q.content);
  const values = new Map<string, ValueSpecification>();
  for (const v of q.defaultParameterValues ?? []) {
    const value = (await reading.parse(`|${v.content}`)).body[0];
    if (value) values.set(v.name, value);
  }
  let source = sourceOf(parsed, values);
  // its rows its project's own (C5): a host that plans and runs them reads them there, never its own tables (F28)
  const how: ModelOptions = { mapping: context.mapping, enumerations: [...enumerationsOf(elements)], project: true };
  // rule 4: an enumeration column is read as its value's name
  const named = new Set((await typerOf(context.runtime, how).relationType(lambda([], source))).filter((c) => c.enumeration).map((c) => c.name));
  if (named.size > 0) source = enumsAsStrings(source, named);
  return { runtime: context.runtime, how, source };
}
