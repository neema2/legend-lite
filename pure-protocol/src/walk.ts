// Walking and rewriting a query: every node's children, a visitor, a bottom-up transform, and
// guards that narrow a node by what it is. What a UI needs to inspect a query it did not build
// (the columns a filter reads, the functions a query calls) or to change one (rename a column
// everywhere, swap a source).

import type {
  AppliedFunction, ColSpec, ColSpecArrayInstance, ColSpecInstance, GenericType, Lambda,
  RelationStoreAccessor, ValueSpecification,
} from './wire.ts';

/** A node's direct children, in order: parameters, bodies, a column spec's lambdas, type values. */
export function children(node: ValueSpecification): ValueSpecification[] {
  switch (node._type) {
    case 'lambda': return [...node.parameters, ...node.body];
    case 'func': case 'property': return [...(node.parameters as ValueSpecification[])];
    case 'collection': return [...node.values];
    case 'genericTypeInstance': return typeValues(node.genericType);
    case 'classInstance':
      if (node.type === 'colSpec') return specLambdas((node as ColSpecInstance).value);
      if (node.type === 'colSpecArray') return (node as ColSpecArrayInstance).value.colSpecs.flatMap(specLambdas);
      return [];
    default: return [];
  }
}

function specLambdas(c: ColSpec): ValueSpecification[] {
  return [c.function1, c.function2].filter((l): l is Lambda => l !== undefined);
}

function typeValues(t: GenericType): ValueSpecification[] {
  return [...t.typeVariableValues, ...t.typeArguments.flatMap(typeValues)];
}

/** Every node, depth first, parents before children; `visit` returning false skips a subtree. */
export function walk(node: ValueSpecification, visit: (n: ValueSpecification) => boolean | void): void {
  if (visit(node) === false) return;
  for (const c of children(node)) walk(c, visit);
}

/** Every node that satisfies `test`. */
export function findAll<T extends ValueSpecification>(node: ValueSpecification,
  test: (n: ValueSpecification) => n is T): T[] {
  const out: T[] = [];
  walk(node, (n) => {
    if (test(n)) out.push(n);
  });
  return out;
}

/** The names of every function a query calls (`filter`, `groupBy`, `sum` ...), each once. */
export function functionsCalled(node: ValueSpecification): Set<string> {
  return new Set(findAll(node, isFunction).map((f) => f.function));
}

/**
 * A bottom-up rewrite: each node's children are rewritten first, then `f` sees the node with
 * its new children and returns its replacement (or the node itself). Nothing is mutated.
 */
export function transform(node: ValueSpecification, f: (n: ValueSpecification) => ValueSpecification): ValueSpecification {
  return f(withChildren(node, (c) => transform(c, f)));
}

/**
 * A top-down rewrite: `f` sees each node before its children, and what it answers replaces the node whole (its children
 * not visited) -- or, undefined, the node is kept and its children rewritten so. What a bottom-up transform cannot say:
 * a variable put in place of its reads everywhere but under a lambda whose own parameter has its name. Nothing is mutated.
 */
export function rewrite(node: ValueSpecification, f: (n: ValueSpecification) => ValueSpecification | undefined): ValueSpecification {
  return f(node) ?? withChildren(node, (c) => rewrite(c, f));
}

function withChildren(node: ValueSpecification, g: (n: ValueSpecification) => ValueSpecification): ValueSpecification {
  switch (node._type) {
    case 'lambda':
      return {
        ...node,
        parameters: node.parameters.map((p) => g(p) as typeof p),
        body: node.body.map(g),
      };
    case 'func': case 'property':
      return { ...node, parameters: node.parameters.map(g) } as ValueSpecification;
    case 'collection':
      return { ...node, values: node.values.map(g) };
    case 'classInstance': {
      const spec = (c: ColSpec): ColSpec => ({
        ...c,
        ...(c.function1 ? { function1: g(c.function1) as Lambda } : {}),
        ...(c.function2 ? { function2: g(c.function2) as Lambda } : {}),
      });
      if (node.type === 'colSpec') return { ...(node as ColSpecInstance), value: spec((node as ColSpecInstance).value) };
      if (node.type === 'colSpecArray') {
        const a = node as ColSpecArrayInstance;
        return { ...a, value: { colSpecs: a.value.colSpecs.map(spec) } };
      }
      return node;
    }
    default:
      return node;
  }
}

// ---- guards ----

export const isLambda = (n: ValueSpecification): n is Lambda => n._type === 'lambda';

export function isFunction(n: ValueSpecification, name?: string): n is AppliedFunction {
  return n._type === 'func' && (name === undefined || (n as AppliedFunction).function === name);
}

export const isAccessor = (n: ValueSpecification): n is RelationStoreAccessor =>
  n._type === 'classInstance' && (n as { type: string }).type === '>';

export const isColSpec = (n: ValueSpecification): n is ColSpecInstance =>
  n._type === 'classInstance' && (n as { type: string }).type === 'colSpec';

export const isColSpecArray = (n: ValueSpecification): n is ColSpecArrayInstance =>
  n._type === 'classInstance' && (n as { type: string }).type === 'colSpecArray';
