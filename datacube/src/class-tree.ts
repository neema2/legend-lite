// THE CLASS TREE (docs/DATACUBE_BI_EXCEL_PARITY_2026_10_24.md §3.3, the model design's step 5 brought forward): a model's
// classes as the compiler gives them -- each one's properties, its supertypes', and every association's end that reaches
// it -- and what a grid over a class may read through them: a RELATED COLUMN, a path to a related class's property, a
// to-many end's values aggregated per instance (snapshot.ts RelatedPath). Read from the elements, never from a name's look;
// no DOM, no planner, no project text: the grid's menu asks it as it opens, and Python's grid.related, headless.

import { isNumeric } from '../../engine-client/src/types.ts';
import type { ModelMeasure } from './model-measures.ts';
import { PATH_AGGREGATES, type PathAggregate, type RelatedPath } from './snapshot.ts';

/** A property as the compiler gives it: a class's, or an association's end. */
interface ElementProperty {
  readonly name: string;
  readonly genericType: { readonly rawType: { readonly _type: string; readonly fullPath?: string } };
  readonly multiplicity: { readonly lowerBound: number; readonly upperBound?: number };
}

/** A model element as the compiler gives it (PureModelContextData's): what a class source and the class tree read of
 *  one -- a class, an enumeration, an association (its two ends). */
export interface ClassElement {
  readonly _type: string;
  readonly package?: string;
  readonly name?: string;
  readonly superTypes?: readonly (string | { readonly fullPath?: string; readonly path?: string })[];
  readonly properties?: readonly ElementProperty[];
}

/** An element's path. */
export const pathOf = (e: ClassElement): string => (e.package ? `${e.package}::${e.name ?? ''}` : (e.name ?? ''));

/** The class's last name: what a person calls it. */
export function className(path: string): string {
  return path.slice(path.lastIndexOf(':') + 1);
}

/** A property as the class tree offers it: its name, its type, what it is, whether it holds many. */
export interface TreeProperty {
  readonly name: string;
  /** Its type's path: a primitive's name, a class's or an enumeration's path. */
  readonly type: string;
  readonly kind: 'value' | 'class' | 'enumeration';
  /** A to-many property or association end: read through it, its values are aggregated per instance. */
  readonly many: boolean;
}

/** THE CLASS TREE: each class of the model, by path, and every property it has -- its own, then each supertype's, then
 *  each association's end that reaches it from another class. */
export type ClassTree = Readonly<Record<string, readonly TreeProperty[]>>;

/** A grid's class and its model's tree: what its related columns may name -- and the model's measures (model-measures.ts),
 *  those of the class or of a supertype of it (`supertypes`) offered on its grid. */
export interface ClassOf {
  readonly class: string;
  readonly tree: ClassTree;
  readonly measures?: readonly ModelMeasure[];
  readonly supertypes?: readonly string[];
}

/** Every supertype of `cls` the model declares, nearest first, each once. */
export function supertypesOf(elements: readonly ClassElement[], cls: string): string[] {
  const classes = new Map(elements.filter((e) => e._type === 'class').map((e) => [pathOf(e), e]));
  const out: string[] = [];
  const visit = (at: string): void => {
    for (const s of classes.get(at)?.superTypes ?? []) {
      const path = typeof s === 'string' ? s : (s.fullPath ?? s.path ?? '');
      if (path === '' || out.includes(path) || path === cls) continue;
      out.push(path);
      visit(path);
    }
  };
  visit(cls);
  return out;
}

/** The measures a grid over `cls` may add (model-measures.ts): of the class, or of a supertype it has -- here, beside the
 *  supertypes' walk, so the grid's menu reads them without the measures' reader. */
export function measuresOf(measures: readonly ModelMeasure[], supertypes: (cls: string) => readonly string[], cls: string): ModelMeasure[] {
  const mine = new Set([cls, ...supertypes(cls)]);
  return measures.filter((m) => mine.has(m.of));
}

/** The class tree of the model's elements. */
export function classTree(elements: readonly ClassElement[]): ClassTree {
  const classes = new Map(elements.filter((e) => e._type === 'class').map((e) => [pathOf(e), e]));
  const enumerations = new Set(elements.filter((e) => e._type === 'Enumeration').map(pathOf));
  const typeOf = (p: ElementProperty): string => (p.genericType.rawType._type === 'packageableType' ? p.genericType.rawType.fullPath ?? '' : '');
  const asTree = (p: ElementProperty): TreeProperty => {
    const type = typeOf(p);
    return {
      name: p.name, type, kind: classes.has(type) ? 'class' : enumerations.has(type) ? 'enumeration' : 'value',
      many: p.multiplicity.upperBound === undefined || p.multiplicity.upperBound > 1,
    };
  };
  // an association's end is a property of the class at its other end: `trades` of the customer, `customer` of the trade
  const ends = new Map<string, TreeProperty[]>();
  for (const a of elements.filter((e) => e._type === 'association')) {
    const [one, two] = a.properties ?? [];
    if (one === undefined || two === undefined) continue;
    ends.set(typeOf(two), [...(ends.get(typeOf(two)) ?? []), asTree(one)]);
    ends.set(typeOf(one), [...(ends.get(typeOf(one)) ?? []), asTree(two)]);
  }
  const tree: Record<string, TreeProperty[]> = {};
  for (const path of classes.keys()) {
    const out: TreeProperty[] = [];
    const seen = new Set<string>();
    const visit = (at: string, from: ReadonlySet<string>): void => {
      const c = classes.get(at);
      // a supertype outside the project (Any, a platform class) declares nothing a grid reads
      if (!c || from.has(at)) return;
      for (const p of [...(c.properties ?? []).map(asTree), ...(ends.get(at) ?? [])]) {
        if (seen.has(p.name)) continue;
        seen.add(p.name);
        out.push(p);
      }
      const next = new Set([...from, at]);
      for (const s of c.superTypes ?? []) visit(typeof s === 'string' ? s : (s.fullPath ?? s.path ?? ''), next);
    };
    visit(path, new Set());
    tree[path] = out;
  }
  return tree;
}

/** A related column's name, as its path says it: `customer_region`, `trades_notional_sum`, `trades_count`. */
export function relatedName(r: RelatedPath): string {
  return [...r.path, ...(r.aggregate !== undefined ? [r.aggregate] : [])].join('_');
}

/** A related column in words, past its first hop (a menu's entry under that hop): `region`, `Count`, `Sum of notional`. */
export function relatedLabel(r: RelatedPath): string {
  const rest = r.path.slice(1).join(' › ');
  if (r.aggregate === undefined) return rest;
  const fn = `${r.aggregate[0]!.toUpperCase()}${r.aggregate.slice(1)}`;
  return rest === '' ? fn : `${fn} of ${rest}`;
}

/**
 * WHY A RELATED COLUMN IS NO COLUMN OF `cls`'s GRID, or null: each hop but the last a class the property reaches -- the
 * class's own, a supertype's, or an association's end; the last a value (or, counted, a class); through a to-many end
 * its values aggregated -- never a row per related instance, whose own figures a total would count again -- and a
 * to-one path not; a sum or an average of numbers. In words.
 */
export function relatedProblem(tree: ClassTree, cls: string, r: RelatedPath): string | null {
  if (r.path.length === 0) return 'a related column is a path of properties from the class';
  if (tree[cls] === undefined) return `the project has no class ${cls}`;
  let at = cls;
  let many = false;
  for (let i = 0; i < r.path.length; i++) {
    const name = r.path[i]!;
    const dotted = r.path.slice(0, i + 1).join('.');
    const p = (tree[at] ?? []).find((x) => x.name === name);
    if (p === undefined) return `${className(at)} has no property ${name}`;
    many ||= p.many;
    if (i < r.path.length - 1) {
      if (p.kind !== 'class') return `${dotted} is ${p.kind === 'value' ? `a ${p.type}` : 'an enumeration'}: a path goes on only through a class`;
      at = p.type;
      continue;
    }
    if (p.kind === 'enumeration') return `${dotted} is an enumeration (${className(p.type)}): a grid reads no enumeration's value yet`;
    if (p.kind === 'class' && r.aggregate !== 'count') {
      return `${dotted} is a ${className(p.type)}: a class at a path's end is counted (aggregate 'count'), or a path goes on to one of its properties`;
    }
    if (many && r.aggregate === undefined) {
      return `${dotted} holds many for each ${className(cls)}: aggregate them (${PATH_AGGREGATES.join(', ')}) -- a row for each would count `
        + `a ${className(cls)}'s own figures again`;
    }
    if (!many && r.aggregate !== undefined) return `${dotted} is one for each ${className(cls)}: nothing to aggregate`;
    if ((r.aggregate === 'sum' || r.aggregate === 'average') && !isNumeric(p.type)) {
      return `${dotted} is ${p.type}: a ${r.aggregate} is of numbers`;
    }
  }
  return null;
}

/** One hop from a class as a menu offers it (relatedOffers): the class-typed property, its class, whether it holds
 *  many, and the related columns through it. */
export interface RelatedOffer {
  readonly property: string;
  readonly target: string;
  readonly many: boolean;
  readonly columns: readonly RelatedPath[];
}

/** The related columns one hop from `cls` (the header menu's Add Related Column ▸): a to-one end's values; a to-many
 *  end's count, and each of its values' aggregates -- each one relatedProblem passes. */
export function relatedOffers(tree: ClassTree, cls: string): RelatedOffer[] {
  const aggregates = (v: TreeProperty): PathAggregate[] => (isNumeric(v.type) ? ['sum', 'average', 'min', 'max'] : ['min', 'max']);
  return (tree[cls] ?? []).filter((p) => p.kind === 'class').map((hop) => {
    const values = (tree[hop.type] ?? []).filter((v) => v.kind === 'value' && !v.many);
    const columns: RelatedPath[] = hop.many
      ? [{ path: [hop.name], aggregate: 'count' }, ...values.flatMap((v) => aggregates(v).map((aggregate) => ({ path: [hop.name, v.name], aggregate })))]
      : values.map((v) => ({ path: [hop.name, v.name] }));
    return { property: hop.name, target: hop.type, many: hop.many, columns: columns.filter((c) => relatedProblem(tree, cls, c) === null) };
  }).filter((o) => o.columns.length > 0);
}
