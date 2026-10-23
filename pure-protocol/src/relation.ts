// The Relation API, and nothing else: a query is a relation source and a chain of relation
// functions. (The user, 2026-09-27: queries are built with the Relation API only; the older TDS
// API is read and printed for compatibility, never built -- there is no builder for it here.)
//
//   from(accessor('model::DB', 'sales', 'orders'))
//     .filter(lambda(['x'], gt(col('x', 'qty'), lit.integer(0))))
//     .groupBy(['region'], [agg('total', lambda(['x'], col('x', 'qty')), lambda(['y'], fn('sum', variable('y'))))])
//     .sort([desc('total')])
//     .limit(10)
//     .lambda()
//
// Each step is a new value (nothing mutates), so a query is shared and extended freely.

import { collection, fn, lambda as makeLambda, element, enumValue } from './build.ts';
import { ProtocolError } from './exact.ts';
import { lit } from './literal.ts';
import type {
  AppliedFunction, ColSpec, ColSpecArrayInstance, ColSpecInstance, GenericType, Lambda,
  RelationStoreAccessor, ValueSpecification,
} from './wire.ts';

// ---- sources ----

/** `#>{db.schema.table}#` -- a table of a Database; `schema` omitted for the default one. */
export function accessor(database: string, schemaOrTable: string, table?: string): RelationStoreAccessor {
  const path = table === undefined ? [database, schemaOrTable] : [database, schemaOrTable, table];
  for (const part of path.slice(1)) {
    // upstream splits the accessor on '.', and its grammar ends at these characters
    if (/[.(){}|;=\n]/.test(part)) throw new ProtocolError(`'${part}' cannot be carried in #>{...}#`);
  }
  return { _type: 'classInstance', type: '>', value: { path } };
}

// ---- column specs ----

/** `~name`. */
export function colSpec(n: string): ColSpecInstance {
  return { _type: 'classInstance', type: 'colSpec', value: { name: n } };
}

/** `~[a, b]`. */
export function colSpecs(specs: readonly (string | ColSpec)[]): ColSpecArrayInstance {
  return {
    _type: 'classInstance',
    type: 'colSpecArray',
    value: { colSpecs: specs.map((s) => (typeof s === 'string' ? { name: s } : s)) },
  };
}

/** `name: x|expr` -- a derived column (extend, a window's function). */
export function derive(n: string, map: Lambda): ColSpec {
  return { name: n, function1: map };
}

/** `name: x|expr: y|reduce` -- an aggregate (groupBy, pivot). */
export function agg(n: string, map: Lambda, reduce: Lambda): ColSpec {
  return { name: n, function1: map, function2: reduce };
}

/** `name:Type` -- a column declared by type (a relation literal's, a cast's). */
export function typed(n: string, t: GenericType): ColSpec {
  return { name: n, genericType: t };
}

// ---- sorting and windows ----

/** `~name->ascending()`. */
export function asc(n: string): AppliedFunction {
  return fn('ascending', colSpec(n));
}

/** `~name->descending()`. */
export function desc(n: string): AppliedFunction {
  return fn('descending', colSpec(n));
}

/** A window: `over(~[partition], [sort])`, either part optional. */
export function over(partition: readonly string[], sort: readonly AppliedFunction[] = []): AppliedFunction {
  const parts: ValueSpecification[] = [];
  if (partition.length > 0) parts.push(colSpecs(partition));
  if (sort.length > 0) parts.push(collection(sort));
  return fn('over', ...parts);
}

export type JoinKind = 'INNER' | 'LEFT' | 'RIGHT' | 'FULL';

// ---- objects as rows ----

/**
 * `instances->project(~[name: x|$x.p, ...])`: a class's instances (`Trade.all()`, filtered or not) as a relation, a
 * column per lambda -- the Relation API's project of objects (legend-engine's `meta::pure::functions::relation::project`,
 * its columns a colSpec array), never TDS's project of lambdas and names.
 */
export function projectObjects(instances: ValueSpecification, columns: readonly ColSpec[]): Relation {
  return new Relation(fn('project', instances, colSpecs(columns)));
}

// ---- the chain ----

/** A relation, being built. Every method returns a new one. */
export class Relation {
  readonly node: ValueSpecification;

  constructor(node: ValueSpecification) {
    this.node = node;
  }

  #then(function_: string, ...args: ValueSpecification[]): Relation {
    return new Relation(fn(function_, this.node, ...args));
  }

  /** `->select(~[a, b])`; no columns is `->select()`, every column. */
  select(columns: readonly string[] = []): Relation {
    return columns.length === 0 ? this.#then('select') : this.#then('select', colSpecs(columns));
  }

  /** `->extend(~name: x|...)` or `->extend(~[...])`; with a window, `->extend(over(...), ~...)`. */
  extend(columns: ColSpec | readonly ColSpec[], window?: AppliedFunction): Relation {
    const cols = Array.isArray(columns)
      ? colSpecs(columns as readonly ColSpec[])
      : ({ _type: 'classInstance', type: 'colSpec', value: columns } as ColSpecInstance);
    return window === undefined ? this.#then('extend', cols) : this.#then('extend', window, cols);
  }

  /** `->filter(x|...)`. */
  filter(predicate: Lambda): Relation {
    return this.#then('filter', predicate);
  }

  /** `->groupBy(~[keys], ~[aggregates])`. */
  groupBy(keys: readonly string[], aggregates: readonly ColSpec[]): Relation {
    return this.#then('groupBy', colSpecs(keys), colSpecs(aggregates));
  }

  /** `->pivot(~[columns], ~[aggregates])`. */
  pivot(columns: readonly string[], aggregates: readonly ColSpec[]): Relation {
    return this.#then('pivot', colSpecs(columns), colSpecs(aggregates));
  }

  /** `->sort([~a->ascending(), ...])`. */
  sort(keys: readonly AppliedFunction[]): Relation {
    return this.#then('sort', collection(keys));
  }

  /** `->distinct()` or `->distinct(~[a, b])`. */
  distinct(columns: readonly string[] = []): Relation {
    return columns.length === 0 ? this.#then('distinct') : this.#then('distinct', colSpecs(columns));
  }

  /** `->limit(n)`. */
  limit(n: number): Relation {
    return this.#then('limit', count(n, 'limit'));
  }

  /** `->drop(n)`. */
  drop(n: number): Relation {
    return this.#then('drop', count(n, 'drop'));
  }

  /** `->slice(start, stop)`. */
  slice(start: number, stop: number): Relation {
    return this.#then('slice', count(start, 'slice'), count(stop, 'slice'));
  }

  /** `->rename(~old, ~new)`. */
  rename(from: string, to: string): Relation {
    return this.#then('rename', colSpec(from), colSpec(to));
  }

  /** `->join(other, JoinKind.K, {a, b|...})`. */
  join(other: Relation, kind: JoinKind, condition: Lambda): Relation {
    return this.#then('join', other.node, enumValue('meta::pure::functions::relation::JoinKind', kind), condition);
  }

  /**
   * `->asOfJoin(other, {a, b|<match>})`, or with `join`, `->asOfJoin(other, {a, b|<match>}, {a, b|<join>})`: each row
   * with the other's nearest row by the match (`$a.time >= $b.time`: the last at or before), among those the join
   * keeps.
   */
  asOfJoin(other: Relation, match: Lambda, join?: Lambda): Relation {
    return join === undefined ? this.#then('asOfJoin', other.node, match) : this.#then('asOfJoin', other.node, match, join);
  }

  /**
   * `->lateral(x|<relation>)`: each row joined to the relation its function makes -- with
   * `flatten`, each row once per element of a collection (an explode, an unnest).
   */
  lateral(each: Lambda): Relation {
    return this.#then('lateral', each);
  }

  /** `->concatenate(other)`. */
  concatenate(other: Relation): Relation {
    return this.#then('concatenate', other.node);
  }

  /** `->cast(@Relation<(...)>)`: the relation's declared columns (a pivot's, typed by the host). */
  castTo(columns: GenericType): Relation {
    return this.#then('cast', {
      _type: 'genericTypeInstance',
      genericType: {
        rawType: { _type: 'packageableType', fullPath: 'meta::pure::metamodel::relation::Relation' },
        typeArguments: [columns],
        multiplicityArguments: [],
        typeVariableValues: [],
      },
    });
  }

  /** Any other relation function, `->name(args)`: an escape hatch that still builds a node. */
  apply(function_: string, ...args: ValueSpecification[]): Relation {
    return this.#then(function_, ...args);
  }

  /** The query: `|<relation>`. */
  lambda(): Lambda {
    return makeLambda([], this.node);
  }
}

/** `values->flatten(~name)`: a collection as a one-column relation (for `lateral`). */
export function flatten(values: ValueSpecification, name: string): AppliedFunction {
  return fn('flatten', values, colSpec(name));
}

/** A relation from a source: an accessor, a function's result, another query's body. */
export function from(source: ValueSpecification): Relation {
  return new Relation(source);
}

/** A relation from a packageable function or element, `my::source()`-style: `element(path)`. */
export function fromElement(fullPath: string): Relation {
  return new Relation(element(fullPath));
}

function count(n: number, what: string): ValueSpecification {
  if (!Number.isSafeInteger(n) || n < 0) throw new ProtocolError(`${what} takes a count, got ${n}`);
  return lit.integer(n);
}
