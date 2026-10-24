// A MODEL MEASURE ADDED AS THE STEPS A PERSON WOULD MAKE (docs/DATACUBE_BI_EXCEL_PARITY_2026_10_24.md §3.4; the measures
// read by model-measures.ts): each AGGREGATE OVER THE INSTANCES (`$t.<path>-><agg>()`, after `$t->filter(x|...)` or
// not) a row-level calculated column of its own with its Aggregation, and what remains -- arithmetic of those, another
// measure called (inlined) -- a group-level column over them. The body rearranged, never computed (AGENTS.md's
// compare-not-compute): the database aggregates, as it does every measure of a grid. A body that does not take apart
// so is refused in words, naming the measure and the part. Its own chunk, fetched with the first measure a grid adds
// (bundle-budget.test.ts); no DOM, no planner.

import { collection, fn, lambda, property, variable, type Lambda, type ValueSpecification } from '../../pure-protocol/src/index.ts';
import { className, relatedName, relatedProblem, type ClassTree } from './class-tree.ts';
import type { ColumnFormat } from './format.ts';
import type { GridCommand } from './grid-commands.ts';
import { readFormatCode } from './format-code.ts';
import type { ModelMeasure } from './model-measures.ts';
import { CubeRefusal, rowColumns, type AggregateFn, type CubeSnapshot, type RelatedPath } from './snapshot.ts';

/** The aggregates a measure's body takes over its instances, as DataCube aggregates a column. */
const AGGREGATES: Readonly<Record<string, AggregateFn>> = { sum: 'sum', count: 'count', average: 'average', min: 'min', max: 'max' };

type Node = ValueSpecification & { readonly _type: string; readonly function?: string; readonly property?: string; readonly name?: string;
  readonly parameters?: readonly ValueSpecification[]; readonly body?: readonly ValueSpecification[] };

/** One aggregate over the instances: its Aggregation, and the value of one instance's row it aggregates. */
interface Term {
  readonly agg: AggregateFn;
  readonly value: ValueSpecification;
}

/**
 * THE COMMANDS THAT ADD `measure` TO THE GRID (`s`, over `cls` of the model `tree`): the related columns its paths reach
 * (a to-one hop's value; a to-many end summed or counted per instance), each aggregate's row-level column and its
 * Aggregation -- named after the measure, hidden but where it is the measure itself -- and the group-level column of
 * what remains. Its format, from its tag, where DataCube reads that format. Refused in words (CubeRefusal).
 */
export function modelMeasureCommands(s: CubeSnapshot, tree: ClassTree, cls: string, measures: readonly ModelMeasure[], measure: ModelMeasure): GridCommand[] {
  const taken = new Set([...rowColumns(s).map((c) => c.name), ...(s.groupDerived ?? []).map((d) => d.name)]);
  if (taken.has(measure.name)) throw new CubeRefusal(`the grid has a column named ${measure.name} already`);
  const own = new Set(s.columns.map((c) => c.name));
  const relatedHad = new Set(s.derived.flatMap((d) => (d.related ? [relatedName(d.related)] : [])));
  const relatedNew = new Map<string, RelatedPath>();
  const terms: Term[] = [];
  const said = (what: string): never => {
    throw new CubeRefusal(`${measure.name} does not take apart into a grid's columns: ${what}`);
  };

  /** A path of properties from `root` (`$x.customer.region`), or undefined. */
  const pathFrom = (n: Node, root: string): string[] | undefined => {
    if (n._type === 'var') return n.name === root ? [] : undefined;
    if (n._type !== 'property' || n.parameters?.length !== 1) return undefined;
    const at = pathFrom(n.parameters[0] as Node, root);
    return at === undefined ? undefined : [...at, n.property!];
  };

  /** An instance's path as a column of its row: its own property's, or a related column's (added when new). */
  const column = (path: readonly string[], aggregate?: RelatedPath['aggregate']): string => {
    if (path.length === 1 && aggregate === undefined) {
      if (!own.has(path[0]!)) said(`${path[0]} is no column of the grid's class (${[...own].join(', ')})`);
      return path[0]!;
    }
    const related: RelatedPath = { path, ...(aggregate !== undefined ? { aggregate } : {}) };
    const problem = relatedProblem(tree, cls, related);
    if (problem !== null) said(problem);
    const name = relatedName(related);
    if (!relatedHad.has(name) && !relatedNew.has(name)) {
      if (taken.has(name)) said(`it reads ${path.join('.')}, and the grid has a column named ${name} that is not it`);
      relatedNew.set(name, related);
    }
    return name;
  };

  /** A row-level value: `n` (over the instance `root`) with each path of `root` read as its row's column. */
  const rowValue = (n: Node, root: string): ValueSpecification => {
    const path = pathFrom(n, root);
    if (path !== undefined) {
      if (path.length === 0) said(`it reads ${root} itself, where a row reads its properties`);
      return property(variable('x'), column(path));
    }
    if (n._type === 'lambda') said('a lambda inside a row\'s value');
    if (n.parameters === undefined) return n;
    return { ...n, parameters: n.parameters.map((p) => rowValue(p as Node, root)) } as ValueSpecification;
  };

  /** The value of one aggregate term's argument: the instances' path, after a filter or not; a count's 1. */
  const term = (agg: AggregateFn, of: Node, param: string): ValueSpecification => {
    // $t->count(): each instance's row, counted
    if (of._type === 'var' && of.name === param) {
      if (agg !== 'count') said(`${agg} of the instances themselves: a count of them, or an aggregate of a property`);
      return add({ agg: 'count', value: { _type: 'integer', value: 1 } as ValueSpecification });
    }
    // $t->filter(x|pred) at the root of the path: the predicate's rows only
    let root: Node = of;
    const props: string[] = [];
    while (root._type === 'property' && root.parameters?.length === 1) {
      props.unshift(root.property!);
      root = root.parameters[0] as Node;
    }
    let when: ValueSpecification | undefined;
    if (root._type === 'func' && root.function === 'filter' && root.parameters?.length === 2) {
      const [from, pred] = root.parameters as [Node, Node];
      if (!(from._type === 'var' && from.name === param)) said('a filter of something but the instances');
      if (pred._type !== 'lambda' || pred.parameters?.length !== 1 || pred.body?.length !== 1) said('a filter that is no one-expression lambda');
      when = rowValue(pred.body![0] as Node, (pred.parameters![0] as Node).name!);
    } else if (!(root._type === 'var' && root.name === param)) {
      said(`an aggregate of something but the instances (${root._type === 'func' ? root.function : root._type})`);
    }
    const counted = props.length === 0;
    if (counted && agg !== 'count') said(`${agg} of the instances themselves`);
    // through a to-many end: each instance's own figure first (a sum or a count), then summed over the instances
    const toMany = !counted && isToMany(tree, cls, props);
    if (toMany && agg !== 'sum' && agg !== 'count') {
      said(`${agg} through ${props.join('.')}, a to-many end: only a sum or a count of it adds up across the instances`);
    }
    const one = { _type: 'integer', value: 1 } as ValueSpecification;
    const value: ValueSpecification = counted ? one
      : property(variable('x'), column(props, toMany ? (agg === 'count' ? 'count' : 'sum') : undefined));
    if (agg === 'count' && !toMany) {
      // a grid's count counts its rows (query.ts aggregateSpec), Pure's its values: each row's 1 where it has one and the
      // filter holds, else 0, summed -- a group with none of them 0, as Pure's count of nothing is
      const has = counted ? when : when === undefined ? fn('isNotEmpty', value) : fn('and', when, fn('isNotEmpty', value));
      if (has === undefined) return add({ agg: 'count', value: one });
      return add({ agg: 'sum', value: fn('if', has, lambda([], one), lambda([], { _type: 'integer', value: 0 } as ValueSpecification)) });
    }
    const kept: ValueSpecification = when === undefined ? value : fn('if', when, lambda([], value), lambda([], collection([])));
    return add({ agg: toMany ? 'sum' : agg, value: kept });
  };

  const add = (t: Term): ValueSpecification => {
    terms.push(t);
    return { _type: 'term', index: terms.length - 1 } as unknown as ValueSpecification;
  };

  /** The measure's body with each aggregate made a term, each measure called inlined; `seen`, the measures on the way. */
  const groupValue = (n: Node, param: string, seen: readonly string[]): ValueSpecification => {
    if (n._type === 'func') {
      const agg = AGGREGATES[n.function!];
      if (agg !== undefined && n.parameters?.length === 1) return term(agg, n.parameters[0] as Node, param);
      const called = measures.find((m) => m.path === n.function);
      if (called !== undefined) {
        if (seen.includes(called.path)) said(`${[...seen, called.path].map(className).join(' calls ')}: a measure calling itself`);
        const [arg] = n.parameters ?? [];
        if (n.parameters?.length !== 1 || (arg as Node)._type !== 'var' || (arg as Node).name !== param) {
          said(`it calls ${called.name} of something but its own instances`);
        }
        return groupValue(called.body as Node, called.param, [...seen, called.path]);
      }
    }
    if (n._type === 'var' && n.name === param) said('it reads its instances outside an aggregate');
    if (n._type === 'property' && pathFrom(n, param) !== undefined) said(`it reads ${pathFrom(n, param)!.join('.')} outside an aggregate`);
    if (n._type === 'lambda') said('a lambda outside an aggregate\'s filter');
    if (n.parameters === undefined) return n;
    return { ...n, parameters: n.parameters.map((p) => groupValue(p as Node, param, seen)) } as ValueSpecification;
  };

  const group = groupValue(measure.body as Node, measure.param, [measure.path]);
  if (terms.length === 0) said('it aggregates nothing of its instances');
  const alone = (group as unknown as { _type: string })._type === 'term';
  const termName = (i: number): string => (alone ? measure.name : `${measure.name}__${i + 1}`);
  for (let i = 0; i < terms.length; i++) if (taken.has(termName(i))) said(`the grid has a column named ${termName(i)} already`);
  const commands: GridCommand[] = [];
  const hidden: Record<string, unknown> = {};
  for (const [name, related] of relatedNew) {
    commands.push({ kind: 'calculate', level: 'row', column: { name, related, kind: related.aggregate !== undefined ? 'measure' : 'dimension' } });
    hidden[name] = { hidden: true };
  }
  terms.forEach((t, i) => {
    const name = termName(i);
    commands.push({ kind: 'calculate', level: 'row', column: { name, lambda: lambda(['x'], t.value), kind: 'measure' } });
    commands.push({ kind: 'column', name, change: { aggregate: t.agg } });
    if (!alone) hidden[name] = { hidden: true };
  });
  if (!alone) {
    // each term read as its group's one figure
    const read = (n: ValueSpecification): ValueSpecification => {
      const node = n as unknown as { _type: string; index?: number; parameters?: ValueSpecification[] };
      if (node._type === 'term') return fn('toOne', property(variable('x'), termName(node.index!)));
      return node.parameters === undefined ? n : { ...n, parameters: node.parameters.map(read) } as ValueSpecification;
    };
    commands.push({ kind: 'calculate', level: 'group', column: { name: measure.name, lambda: lambda(['x'], read(group)) as Lambda } });
  }
  const format = measure.format === undefined ? undefined : formatOfCode(measure.format);
  const settings: Record<string, unknown> = { ...hidden, ...(format !== undefined ? { [measure.name]: { format } } : {}) };
  if (Object.keys(settings).length > 0) commands.push({ kind: 'configure', patch: { columns: settings } });
  return commands;
}

/** Whether a path from `cls` goes through a to-many property or end. */
function isToMany(tree: ClassTree, cls: string, path: readonly string[]): boolean {
  let at = cls;
  for (const name of path) {
    const p = (tree[at] ?? []).find((x) => x.name === name);
    if (p === undefined) return false;
    if (p.many) return true;
    at = p.type;
  }
  return false;
}

/**
 * A MEASURE'S FORMAT TAG (an Excel number format: `$#,##0`, `0.00%`, `#,##0.0,,"M"`) as a DataCube column format: a
 * plain one as the format its Properties would set -- a percentage by its `%`, a currency by its `$`, `€` or `£`, else a
 * number, its places the zeros after its point -- and any other the code itself (format-code.ts), its sections,
 * scaling and colours as Excel's; one DataCube does not draw left to the column's own (undefined), never guessed at.
 */
export function formatOfCode(code: string): ColumnFormat | undefined {
  const m = /^([$€£]?)#?,?#*0(?:\.(0+))?(%?)$/.exec(code.trim());
  if (m === null) {
    try {
      readFormatCode(code);
      return { kind: 'number', code };
    } catch (e) {
      if (!(e instanceof CubeRefusal)) throw e;
      return undefined;
    }
  }
  const decimals = m[2]?.length ?? 0;
  if (m[3] === '%') return { kind: 'percent', decimals };
  const currency = m[1] === '$' ? 'USD' : m[1] === '€' ? 'EUR' : m[1] === '£' ? 'GBP' : undefined;
  return currency !== undefined ? { kind: 'currency', currency, decimals } : { kind: 'number', decimals };
}
