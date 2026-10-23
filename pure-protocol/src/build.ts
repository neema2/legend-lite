// Expressions: lambdas, variables, properties, function calls, collections, enum values, types.
// Each builder makes exactly the node the grammar makes for the same text (pinned against
// legend-lite's own parser, test/twins.test.ts), so a built query and a parsed one are one JSON.

import { ProtocolError } from './exact.ts';
import type {
  AppliedFunction, AppliedProperty, Collection, GenericType, GenericTypeInstance, Lambda,
  Multiplicity, PackageableElementPtr, RelationColumn, ValueSpecification, Variable,
} from './wire.ts';

/**
 * An identifier's letters: a letter or `_`, then letters, digits and `_` -- the plain form, stricter than the lexer
 * (which also takes a `$` after the first letter).
 */
export const IDENTIFIER_LETTERS = '[A-Za-z_][A-Za-z0-9_]*';
const IDENT = new RegExp(`^${IDENTIFIER_LETTERS}$`);

/**
 * THE identifier rule on the TypeScript side (the one copy DataCube asks: the other DataCube line's follow-up,
 * 2026-10-18): a name the grammar reads bare -- an identifier's letters, and not `true` or `false`, which its lexer
 * reads as literals. Any other name is quoted where the grammar quotes one (a column: `$x.'true'`), or refused where it
 * quotes none (a lambda's parameter). Its twins elsewhere: the compiler's own (ModelChanges.isIdentifier) and Python's
 * frames (`_NAME` and `_LITERALS`), which writes models before any of this runs.
 */
export function isIdentifier(n: string): boolean {
  return IDENT.test(n) && n !== 'true' && n !== 'false';
}

/** A parameter or variable name: an identifier (a lambda's parameter is never quoted, so a builder writes none). */
function name(n: string, what: string): string {
  if (!isIdentifier(n)) throw new ProtocolError(`${what} '${n}' is not an identifier`);
  return n;
}

/** A lambda: `x|body`, `{a, b|body}`, `|body`. A body of several expressions is a block. */
export function lambda(parameters: readonly (string | Variable)[], ...body: ValueSpecification[]): Lambda {
  if (body.length === 0) throw new ProtocolError('a lambda has at least one expression');
  return {
    _type: 'lambda',
    parameters: parameters.map((p) => (typeof p === 'string' ? variable(p) : p)),
    body,
  };
}

/** `$name`, or an untyped lambda parameter `name`. */
export function variable(n: string): Variable {
  return { _type: 'var', name: name(n, 'a variable') };
}

/** A typed lambda parameter, `name: Type[m]`. */
export function parameter(n: string, type: GenericType, multiplicity: Multiplicity): Variable {
  return { _type: 'var', name: name(n, 'a parameter'), genericType: type, multiplicity };
}

/** `owner.property` -- any name at all (the grammar quotes a name that is not an identifier). */
export function property(owner: ValueSpecification, prop: string): AppliedProperty {
  if (prop.length === 0) throw new ProtocolError('a property has a name');
  return { _type: 'property', property: prop, parameters: [owner] };
}

/** `$x.column`: a column of the lambda parameter `x`. */
export function col(param: string, column: string): AppliedProperty {
  return property(variable(param), column);
}

/** `f(a, b)`, which is `a->f(b)`. */
export function fn(function_: string, ...parameters: ValueSpecification[]): AppliedFunction {
  if (function_.length === 0) throw new ProtocolError('a function has a name');
  return { _type: 'func', function: function_, parameters };
}

/** `[a, b, c]`: the multiplicity is its size, as the grammar writes it. */
export function collection(values: readonly ValueSpecification[]): Collection {
  return { _type: 'collection', multiplicity: { lowerBound: values.length, upperBound: values.length }, values };
}

/** A packageable element by path: a class, an enumeration, a function, a database. */
export function element(fullPath: string): PackageableElementPtr {
  if (fullPath.length === 0) throw new ProtocolError('an element has a path');
  return { _type: 'packageableElementPtr', fullPath };
}

/** `my::Enum.VALUE`. */
export function enumValue(enumeration: string, value: string): AppliedProperty {
  return { _type: 'property', property: value, parameters: [element(enumeration)] };
}

// ---- types ----

export const ONE: Multiplicity = { lowerBound: 1, upperBound: 1 };
export const ZERO_ONE: Multiplicity = { lowerBound: 0, upperBound: 1 };
export const MANY: Multiplicity = { lowerBound: 0 };

/** A type by path, with its type arguments: `String`, `Relation<(...)>`. */
export function type(fullPath: string, ...typeArguments: GenericType[]): GenericType {
  return {
    rawType: { _type: 'packageableType', fullPath },
    typeArguments,
    multiplicityArguments: [],
    typeVariableValues: [],
  };
}

/** A relation's columns as a type, `(a:String, n:Integer)`; a column is `[0..1]` unless said. */
export function relationType(columns: readonly { name: string; type: GenericType; multiplicity?: Multiplicity }[]): GenericType {
  const cols: RelationColumn[] = columns.map((c) => ({
    name: c.name,
    genericType: c.type,
    multiplicity: c.multiplicity ?? ZERO_ONE,
  }));
  return { rawType: { _type: 'relationType', columns: cols }, typeArguments: [], multiplicityArguments: [], typeVariableValues: [] };
}

/** `@Type`, a type as an argument (`to(@Integer)`, `cast(@Relation<...>)`). */
export function typeArg(t: GenericType): GenericTypeInstance {
  return { _type: 'genericTypeInstance', genericType: t };
}

/** `value->to(@Type)`: a Variant read as a type. */
export function to(value: ValueSpecification, t: GenericType): AppliedFunction {
  return fn('to', value, typeArg(t));
}

/** `value->toMany(@Type)`. */
export function toMany(value: ValueSpecification, t: GenericType): AppliedFunction {
  return fn('toMany', value, typeArg(t));
}

/** `value->cast(@Type)`. */
export function cast(value: ValueSpecification, t: GenericType): AppliedFunction {
  return fn('cast', value, typeArg(t));
}

// ---- operators: the grammar's infix forms, as the functions they are ----

/** `a == b`. */
export const eq = (a: ValueSpecification, b: ValueSpecification): AppliedFunction => fn('equal', a, b);
/** `a != b`, which is `!(a == b)`. */
export const ne = (a: ValueSpecification, b: ValueSpecification): AppliedFunction => not(eq(a, b));
export const lt = (a: ValueSpecification, b: ValueSpecification): AppliedFunction => fn('lessThan', a, b);
export const le = (a: ValueSpecification, b: ValueSpecification): AppliedFunction => fn('lessThanEqual', a, b);
export const gt = (a: ValueSpecification, b: ValueSpecification): AppliedFunction => fn('greaterThan', a, b);
export const ge = (a: ValueSpecification, b: ValueSpecification): AppliedFunction => fn('greaterThanEqual', a, b);
export const not = (a: ValueSpecification): AppliedFunction => fn('not', a);

/** `a && b && c`: left-nested, as the grammar nests it. */
export function and(...terms: ValueSpecification[]): ValueSpecification {
  return chain('and', terms);
}

/** `a || b || c`. */
export function or(...terms: ValueSpecification[]): ValueSpecification {
  return chain('or', terms);
}

function chain(op: string, terms: ValueSpecification[]): ValueSpecification {
  if (terms.length === 0) throw new ProtocolError(`${op} of nothing`);
  return terms.slice(1).reduce((acc, t) => fn(op, acc, t), terms[0]!);
}

/** `a + b + c`: the grammar's arithmetic takes ONE collection argument. */
export const plus = (...terms: ValueSpecification[]): AppliedFunction => fn('plus', collection(terms));
export const minus = (...terms: ValueSpecification[]): AppliedFunction => fn('minus', collection(terms));
export const times = (...terms: ValueSpecification[]): AppliedFunction => fn('times', collection(terms));
/** `a / b`. */
export const divide = (a: ValueSpecification, b: ValueSpecification): AppliedFunction => fn('divide', a, b);
