// Small queries the editor asks on a person's behalf (census §5.2, §5.4): value suggestions as
// they type a filter value, and a property's preview (its common values, or for a number its
// count, distinct count, sum, min, max and average). Each is built as protocol JSON and run on
// the query's own source.

import {
  agg, colSpec, colSpecs, derive, desc, element, fn, from, lambda, lit, property, variable, type Lambda, type ValueSpecification,
} from '../../../pure-protocol/src/index.ts';
import { isToMany, isNumericFamily, primitiveFamily } from '../model/graph.ts';
import type { PropertyPath } from '../builder/state.ts';
import { isTds } from '../backend/wire.ts';
import { propertyAt } from './actions.ts';
import type { AppContext } from './context.ts';
import type { Session } from './session.ts';

function chainFrom(path: PropertyPath): ValueSpecification {
  let node: ValueSpecification = variable('x');
  for (const s of path) node = property(node, s.property);
  return node;
}

/** A probe runs on the query's source: `Class.all()->...->from(mapping, runtime)`. */
function onSource(session: Session, relation: ValueSpecification): Lambda {
  const src = session.query.source;
  return lambda([], fn('from', relation, element(src.mapping), element(src.runtime)));
}

/** Can a path be probed? Only through to-one steps (a to-many step would multiply rows). */
export function probeable(session: Session, path: PropertyPath): boolean {
  const graph = session.project.graph;
  let owner = session.query.source.class;
  for (let i = 0; i < path.length; i++) {
    const p = graph.property(owner, path[i]!.property);
    if (!p || (isToMany(p.multiplicity) && i < path.length - 1) || (path[i]!.args?.length ?? 0) > 0) return false;
    owner = p.type;
  }
  return true;
}

async function rows(app: AppContext, session: Session, l: Lambda, signal?: AbortSignal): Promise<{ columns: readonly string[]; rows: unknown[][] }> {
  const r = await app.engine.execute({ function: l, model: session.project.context }, signal);
  if (!isTds(r)) throw new Error('the probe did not answer a table');
  return { columns: r.result.columns, rows: r.result.rows.map((x) => [...x.values]) };
}

/** Up to 10 distinct values of a string property starting with `prefix` (upstream's typeahead). */
export async function suggest(app: AppContext, session: Session, path: PropertyPath, prefix: string, signal?: AbortSignal): Promise<string[]> {
  const v = from(fn('getAll', element(session.query.source.class)))
    .apply('project', { _type: 'classInstance', type: 'colSpecArray', value: { colSpecs: [derive('v', lambda(['x'], chainFrom(path)))] } } as ValueSpecification)
    .filter(lambda(['r'], fn('startsWith', property(variable('r'), 'v'), lit.string(prefix))))
    .distinct()
    .sort([fn('ascending', colSpec('v'))])
    .limit(10);
  const r = await rows(app, session, onSource(session, v.node), signal);
  return r.rows.map((x) => x[0]).filter((x): x is string => typeof x === 'string');
}

export interface Preview {
  readonly columns: readonly string[];
  readonly rows: unknown[][];
}

/** A property's preview: a number's aggregates as one row each, anything else its 10 commonest values. */
export async function preview(app: AppContext, session: Session, path: PropertyPath): Promise<Preview> {
  const { prop } = propertyAt(session.project.graph, session.query.source.class, path);
  const projected = from(fn('getAll', element(session.query.source.class)))
    .apply('project', { _type: 'classInstance', type: 'colSpecArray', value: { colSpecs: [derive('v', lambda(['x'], chainFrom(path)))] } } as ValueSpecification);
  const y = variable('y');
  const map = lambda(['x'], property(variable('x'), 'v'));
  if (isNumericFamily(primitiveFamily(prop.type))) {
    const aggs = [
      ['Count', fn('count', y)], ['Distinct Count', fn('count', fn('distinct', y))], ['Sum', fn('sum', y)],
      ['Min', fn('min', y)], ['Max', fn('max', y)], ['Average', fn('average', y)],
    ] as const;
    // `aggregate(~[...])`, not `groupBy(~[], ...)`: legend-engine fails on a groupBy with no keys (measured, 4.145.0)
    const r = await rows(app, session, onSource(session, projected.apply('aggregate', colSpecs(aggs.map(([n, f]) => agg(n, map, lambda(['y'], f))))).node));
    const one = r.rows[0] ?? [];
    return { columns: ['Aggregation', 'Value'], rows: aggs.map(([n], i) => [n, one[i] ?? null]) };
  }
  const r = await rows(app, session, onSource(session, projected
    .groupBy(['v'], [agg('Count', map, lambda(['y'], fn('count', y)))])
    .sort([desc('Count'), fn('ascending', colSpec('v'))])
    .limit(10).node));
  return { columns: ['Value', 'Count'], rows: r.rows };
}
