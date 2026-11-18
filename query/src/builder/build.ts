// The query state as a lambda: V1 protocol JSON built with pure-protocol, the Relation API for
// tables (design D4, D5). The shape follows upstream's typed query builder, in its order:
//
//   {params | Class.all()->filter(x|...)->project(~[...])[->groupBy(~[...], ~[...])]
//             [->distinct()][->sort([...])][->limit(n)][->slice(a, b)]}
//
// and for execution `->from(mapping, runtime)` is appended, as upstream does in typed mode.

import {
  and, collection, element, enumValue, findAll, fn, lambda, lit, not, or, parameter, property, type, variable,
  type AppliedFunction, type ColSpec, type Lambda, type ValueSpecification, type Variable,
} from '../../../pure-protocol/src/index.ts';
import { agg, asc, colSpecs, derive, desc, from, over } from '../../../pure-protocol/src/index.ts';
import { isToMany, milestoningDates, simpleName, type ModelGraph } from '../model/graph.ts';
import type {
  AggregateOp, Condition, Constant, DateFunction, FilterNode, GraphNode, Group, ProjectionColumn, PropertyPath, QueryState, Value, WindowColumn,
  WindowOp,
} from './state.ts';

/** A path's property chain from `$root`, and where it first crosses a to-many property. */
interface ResolvedPath {
  /** Indices of steps whose property is to-many (the chain splits there into `exists`). */
  readonly toManyAt: readonly number[];
  /** Each step's owning class, in order. */
  readonly owners: readonly string[];
}

export class BuildError extends Error {}

export function resolvePath(graph: ModelGraph, root: string, path: PropertyPath): ResolvedPath {
  const toManyAt: number[] = [];
  const owners: string[] = [];
  let owner = root;
  path.forEach((step, i) => {
    const p = graph.property(owner, step.property);
    if (p === undefined) throw new BuildError(`${owner} has no property '${step.property}'`);
    owners.push(owner);
    if (isToMany(p.multiplicity) && i < path.length - 1) toManyAt.push(i);
    owner = p.type;
  });
  return { toManyAt, owners };
}

/** `$v.a.b(...)` for steps [from, to). */
function chain(start: ValueSpecification, path: PropertyPath, fromStep: number, toStep: number): ValueSpecification {
  let node = start;
  for (let i = fromStep; i < toStep; i++) {
    const step = path[i]!;
    node = step.args !== undefined && step.args.length > 0
      ? { _type: 'property', property: step.property, parameters: [node, ...step.args.map(valueSpec)] }
      : property(node, step.property);
  }
  return node;
}

/** A value as protocol JSON. */
export function valueSpec(v: Value): ValueSpecification {
  switch (v.kind) {
    case 'string': return lit.string(v.value);
    case 'boolean': return lit.boolean(v.value);
    case 'integer': return lit.integer(v.value);
    case 'float': return lit.float(v.value);
    case 'decimal': return lit.decimal(v.value);
    case 'strictDate': return lit.strictDate(v.value);
    case 'dateTime': return lit.dateTime(v.value);
    case 'enum': return enumValue(v.enumeration, v.value);
    case 'dateFunction': return dateFunction(v.function);
    case 'list': return collection(v.values.map(valueSpec));
    case 'variable': return variable(v.name);
  }
}

function dateFunction(d: DateFunction): ValueSpecification {
  switch (d.kind) {
    case 'today': return fn('today');
    case 'now': return fn('now');
    case 'firstDayOfThis': return fn(`firstDayOfThis${d.unit}`);
    case 'adjust': return fn('adjust', fn(d.from), lit.integer(d.amount) as ValueSpecification,
      enumValue('meta::pure::functions::date::DurationUnit', d.unit));
  }
}

/** The predicate a condition asserts about `value`, the chain's end. */
function predicate(c: Condition, value: ValueSpecification): ValueSpecification {
  const rhs = (): ValueSpecification => {
    if (c.value === undefined) throw new BuildError(`the condition on '${c.path.map((s) => s.property).join('.')}' has no value`);
    return valueSpec(c.value);
  };
  switch (c.operator) {
    case 'equal': return fn('equal', value, rhs());
    case 'notEqual': return not(fn('equal', value, rhs()));
    case 'lessThan': return fn('lessThan', value, rhs());
    case 'lessThanEqual': return fn('lessThanEqual', value, rhs());
    case 'greaterThan': return fn('greaterThan', value, rhs());
    case 'greaterThanEqual': return fn('greaterThanEqual', value, rhs());
    case 'startsWith': return fn('startsWith', value, rhs());
    case 'notStartsWith': return not(fn('startsWith', value, rhs()));
    case 'contains': return fn('contains', value, rhs());
    case 'notContains': return not(fn('contains', value, rhs()));
    case 'endsWith': return fn('endsWith', value, rhs());
    case 'notEndsWith': return not(fn('endsWith', value, rhs()));
    case 'in': return fn('in', value, rhs());
    case 'notIn': return not(fn('in', value, rhs()));
    case 'isEmpty': return fn('isEmpty', value);
    case 'isNotEmpty': return fn('isNotEmpty', value);
  }
}

/**
 * A condition on `$var`: the property chain, split at each to-many step into
 * `->exists(x_n|...)`, as upstream's filter builds it (census §5.4).
 */
function condition(graph: ModelGraph, root: string, c: Condition, varName: string, depth: number): ValueSpecification {
  const { toManyAt } = resolvePath(graph, root, c.path);
  const build = (start: ValueSpecification, fromStep: number, splits: readonly number[], d: number): ValueSpecification => {
    const [split, ...rest] = splits;
    if (split === undefined) return predicate(c, chain(start, c.path, fromStep, c.path.length));
    const inner = `${varName}_${d}`;
    const collectionNode = chain(start, c.path, fromStep, split + 1);
    return fn('exists', collectionNode, lambda([inner], build(variable(inner), split + 1, rest, d + 1)));
  };
  return build(variable(varName), 0, toManyAt, depth + 1);
}

function filterNode(graph: ModelGraph, root: string, n: FilterNode, varName: string): ValueSpecification {
  if (n.kind === 'condition') return condition(graph, root, n, varName, 0);
  // an empty group (being filled in) says nothing: it is left out
  const terms = n.children
    .filter((c) => c.kind === 'condition' || hasConditions(c))
    .map((c) => filterNode(graph, root, c, varName));
  if (terms.length === 0) throw new BuildError('an empty filter group');
  return n.op === 'and' ? and(...terms) : or(...terms);
}

/** Does a filter group hold at least one condition? (An empty filter is no filter.) */
export function hasConditions(g: Group | undefined): boolean {
  return g !== undefined && g.children.some((c) => c.kind === 'condition' || hasConditions(c));
}

/**
 * A column aggregate's map and reduce, as upstream's operators build them: the column's values and
 * the reducer -- `percentile(p)` (p = the value over 100), or `percentile(p, ascending,
 * continuous)` when either is off; for `wavg`, `wavgRowMapper(column, weight)` then `wavg()`.
 */
function columnAggregate(c: ProjectionColumn, columns: readonly ProjectionColumn[]): [Lambda, Lambda] {
  const x = variable('x');
  const y = variable('y');
  if (c.aggregate === 'wavg') {
    const weight = c.weight;
    if (!weight || !columns.some((o) => o.name === weight && o.id !== c.id && o.aggregate === undefined)) {
      throw new BuildError(`${c.name}: a weighted average needs a weight column`);
    }
    return [lambda(['x'], fn('wavgRowMapper', property(x, c.name), property(x, weight))), lambda(['y'], fn('wavg', y))];
  }
  if (c.aggregate === 'percentile') {
    const o = c.percentile ?? { value: 50, ascending: true, continuous: true };
    if (!(o.value >= 0 && o.value <= 100)) throw new BuildError(`${c.name}: a percentile is between 0 and 100`);
    const p = lit.float(Number((o.value / 100).toFixed(10))) as ValueSpecification;
    return [lambda(['x'], property(x, c.name)), o.ascending && o.continuous
      ? lambda(['y'], fn('percentile', y, p))
      : lambda(['y'], fn('percentile', y, p, lit.boolean(o.ascending), lit.boolean(o.continuous)))];
  }
  return [lambda(['x'], property(x, c.name)), reducer(c.aggregate!)];
}

function reducer(op: AggregateOp | WindowOp): Lambda {
  const y = variable('y');
  switch (op) {
    case 'count': return lambda(['y'], fn('count', y));
    case 'distinctCount': return lambda(['y'], fn('count', fn('distinct', y)));
    case 'sum': return lambda(['y'], fn('sum', y));
    case 'average': return lambda(['y'], fn('average', y));
    case 'min': return lambda(['y'], fn('min', y));
    case 'max': return lambda(['y'], fn('max', y));
    case 'stdDevPopulation': return lambda(['y'], fn('stdDevPopulation', y));
    case 'stdDevSample': return lambda(['y'], fn('stdDevSample', y));
    case 'joinStrings': return lambda(['y'], fn('joinStrings', y, lit.string(',')));
    default: throw new BuildError(`${op} is not an aggregate`);
  }
}

export interface BuildOptions {
  /** Append `->from(mapping, runtime)` (for execution; saved content leaves it out). */
  readonly withFrom: boolean;
  /** Cap the rows: a preview's `->limit(n)` after the query's own options. */
  readonly previewLimit?: number;
}

/** A window column's function over its window: a ranking, or an aggregate of a column. */
function windowSpec(w: WindowColumn): ColSpec {
  const p = variable('p'), wv = variable('w'), r = variable('r');
  const params = ['p', 'w', 'r'];
  switch (w.op) {
    case 'rank': case 'denseRank': case 'percentRank':
      return derive(w.name, lambda(params, fn(w.op, p, wv, r)));
    case 'rowNumber':
      return derive(w.name, lambda(params, fn('rowNumber', p, r)));
    default: {
      if (w.column === undefined) throw new BuildError(`the window column ${w.name} needs a column to ${w.op}`);
      return agg(w.name, lambda(params, property(r, w.column)), reducer(w.op));
    }
  }
}

/** The graph fetch tree, `#{Class{a, b{c}}}#`, as its protocol node. */
export function graphTree(root: string, tree: readonly GraphNode[]): ValueSpecification {
  const node = (n: GraphNode): unknown => ({
    _type: 'propertyGraphFetchTree', parameters: [], property: n.property, subTrees: n.children.map(node), subTypeTrees: [],
  });
  return {
    _type: 'classInstance', type: 'rootGraphFetchTree',
    value: { _type: 'rootGraphFetchTree', class: root, subTrees: tree.map(node), subTypeTrees: [] },
  } as unknown as ValueSpecification;
}

/** The query as its lambda. */
export function buildLambda(graph: ModelGraph, q: QueryState, options: BuildOptions): Lambda {
  const root = q.source.class;
  let rel: ValueSpecification = classSource(graph, q);
  if (hasConditions(q.filter)) {
    rel = fn('filter', rel, lambda(['x'], filterNode(graph, root, q.filter!, 'x')));
  }
  let body: ValueSpecification;
  if (q.graph) {
    // a graph fetch: objects as JSON; a preview takes its rows before fetching (as upstream)
    if (q.graph.tree.length === 0) throw new BuildError('add at least one property to fetch');
    if (options.previewLimit !== undefined) rel = fn('take', rel, lit.integer(options.previewLimit) as ValueSpecification);
    const tree = graphTree(root, q.graph.tree);
    body = fn('serialize', fn(q.graph.checked ? 'graphFetchChecked' : 'graphFetch', rel, tree), tree);
  } else {
    if (q.columns.length === 0) throw new BuildError('add at least one column');
    const cols: ColSpec[] = q.columns.map((c) => derive(c.name, c.derivation ?? lambda(['x'], chain(variable('x'), c.path, 0, c.path.length))));
    let r = from(rel).apply('project', colSpecs(cols));
    const aggregated = q.columns.filter((c) => c.aggregate !== undefined);
    if (aggregated.length > 0) {
      // a wavg's weight column is consumed by the average: neither a key nor in the result
      const weights = new Set(aggregated.flatMap((c) => (c.aggregate === 'wavg' && c.weight ? [c.weight] : [])));
      const keys = q.columns.filter((c) => c.aggregate === undefined && !weights.has(c.name)).map((c) => c.name);
      // the aggregate replaces its column: map the row to the column, reduce the values
      const aggs = aggregated.map((c) => {
        const [map, reduce] = columnAggregate(c, q.columns);
        return agg(c.name, map, reduce);
      });
      // with no key: `aggregate(~[...])` -- legend-engine fails on `groupBy(~[], ...)` (measured, 4.145.0)
      r = keys.length > 0 ? r.groupBy(keys, aggs) : r.apply('aggregate', colSpecs(aggs));
    }
    for (const w of q.windows ?? []) {
      const sort = w.sort ? [w.sort.direction === 'asc' ? asc(w.sort.column) : desc(w.sort.column)] : [];
      r = r.extend(windowSpec(w), over(w.partition, sort));
    }
    if (hasConditions(q.postFilter)) {
      r = r.filter(lambda(['r'], postFilterNode(q.postFilter!)));
    }
    if (q.options.distinct) r = r.distinct();
    if (q.options.sort.length > 0) {
      r = r.sort(q.options.sort.map((s): AppliedFunction => (s.direction === 'asc' ? asc(s.column) : desc(s.column))));
    }
    if (q.options.limit !== undefined) r = r.limit(q.options.limit);
    if (q.options.slice !== undefined) r = r.slice(q.options.slice.start, q.options.slice.end);
    if (options.previewLimit !== undefined) r = r.limit(options.previewLimit);
    body = r.node;
  }
  if (options.withFrom) body = fn('from', body, element(q.source.mapping), element(q.source.runtime));
  return lambda(q.parameters.map((p) => parameter(p.name, type(p.type), p.multiplicity)), ...(q.constants ?? []).map(letOf), body);
}

/**
 * The source's instances: `Class.all()`; a temporal class as of its dates (processing first:
 * `all($processingDate, $businessDate)`), or every version, `allVersions()`.
 */
function classSource(graph: ModelGraph, q: QueryState): ValueSpecification {
  const cls = element(q.source.class);
  const t = graph.temporalOf(q.source.class);
  if (t === undefined) return fn('getAll', cls);
  const m = q.milestoning;
  if (m === undefined) throw new BuildError(`${simpleName(q.source.class)} is milestoned: choose its dates in the query options`);
  if (m.kind === 'allVersions') return fn('getAllVersions', cls);
  return fn('getAll', cls, ...milestoningDates(t).map((d) => {
    const v = m.dates[d];
    if (v === undefined) throw new BuildError(`${simpleName(q.source.class)} is milestoned: set its ${d === 'businessDate' ? 'business' : 'processing'} date in the query options`);
    // Pure's grammar reads `Class.all(today())` as a call of all(), not getAll: a function goes
    // through a constant (`let d = today(); Class.all($d)`), as legend-engine 4.145.0 requires too
    if (v.kind === 'dateFunction') throw new BuildError(`a class's ${d === 'businessDate' ? 'business' : 'processing'} date is a parameter, a constant or a date: make a constant for a relative date`);
    return valueSpec(v);
  }));
}

/** A constant as upstream writes it: `let name = value;`, ahead of the query. */
function letOf(c: Constant): ValueSpecification {
  return fn('letFunction', lit.string(c.name) as ValueSpecification, 'calculated' in c ? c.calculated : valueSpec(c.value));
}

/** A post-filter on the result's columns: each condition names a column as its one-step path, `$r.column`. */
function postFilterNode(n: FilterNode): ValueSpecification {
  if (n.kind === 'condition') {
    if (n.path.length !== 1) throw new BuildError('a post-filter condition names one column');
    return predicate(n, property(variable('r'), n.path[0]!.property));
  }
  const terms = n.children.filter((c) => c.kind === 'condition' || hasConditions(c)).map(postFilterNode);
  if (terms.length === 0) throw new BuildError('an empty filter group');
  return n.op === 'and' ? and(...terms) : or(...terms);
}

/** Every variable (parameter or constant) a query references, by name -- constants' values included. */
export function referencedVariables(q: QueryState): Set<string> {
  const out = new Set<string>();
  const visitValue = (v: Value | undefined): void => {
    if (v === undefined) return;
    if (v.kind === 'variable') out.add(v.name);
    if (v.kind === 'list') v.values.forEach(visitValue);
  };
  if (q.milestoning?.kind === 'asOf') Object.values(q.milestoning.dates).forEach(visitValue);
  for (const c of q.constants ?? []) {
    if ('calculated' in c) findAll(c.calculated, (n): n is Variable => n._type === 'var').forEach((v) => out.add(v.name));
    else visitValue(c.value);
  }
  const visit = (n: FilterNode): void => {
    if (n.kind === 'condition') {
      visitValue(n.value);
      n.path.forEach((s) => s.args?.forEach(visitValue));
    } else n.children.forEach(visit);
  };
  if (q.filter) visit(q.filter);
  if (q.postFilter) visit(q.postFilter);
  q.columns.forEach((c) => c.path.forEach((s) => s.args?.forEach(visitValue)));
  return out;
}
