// What a column is offered -- aggregates and filter operators -- as the COMPILER answers it
// (docs/DATACUBE_TYPES_TO_SERVER_2026_09_27.md, T5): src/generated/offer-facts.ts holds its
// answers to DataCube's own queries over a column of each type, and the rules here name no
// type. The one place every panel, menu and editor asks.

import { OFFER_FACTS, type OfferFact } from './generated/offer-facts.ts';
import type { AggregateFn, FilterOperator } from './snapshot.ts';
import { familyOf, plainType } from '../../engine-client/src/types.ts';

function factsOf(type: string): OfferFact | undefined {
  return OFFER_FACTS[plainType(type)];
}

/**
 * Whether a column of this type takes the aggregate: the compiler accepts the level query
 * and the result stays in the column's family -- upstream's "an aggregate keeps the column's
 * type", by the compiler's answer. By FAMILY, never the exact type: an Integer's average is
 * a Float, a precise `BigInt` column's sum an `Integer`. A type with no facts (Pure's Any)
 * takes none.
 */
export function takesAggregate(fn: AggregateFn, type: string): boolean {
  const result = factsOf(type)?.aggregates[fn];
  return result !== undefined && result !== null && familyOf(result) === familyOf(type);
}

/**
 * Whether a column of this type takes the filter operator: its condition compiles. The query
 * names the function the operator means where Pure's name is ambiguous (query.ts: the text
 * "contains" is `string::contains`), so compiling is the whole answer.
 */
export function takesOperator(op: FilterOperator, type: string): boolean {
  if (familyOf(type) === 'boolean' && BOOLEAN_ORDER_AGAINST_A_VALUE.has(op)) return false;
  return factsOf(type)?.operators[op] ?? false;
}

/**
 * ENGINE DEFECT (docs/SEMANTICS_REGISTER.md S23) -- delete with relation-type.ts's compensation.
 * legend-engine types a BIT column TinyInt, and refuses ordering it against a boolean VALUE
 * (`lessThan(TinyInt[0..1], Boolean[1])`); legend-lite compiles it. Measured 2026-10-01 on
 * 4.145 (runs/bool-probe.mjs): every other operator lite offers a Boolean -- equal, not equal,
 * in, not in, empty, not empty, and every comparison with another column -- compiles there
 * and returns the right rows. So these four are not offered on a boolean, on any planner.
 */
const BOOLEAN_ORDER_AGAINST_A_VALUE: ReadonlySet<FilterOperator> = new Set<FilterOperator>(
  ['lessThan', 'lessThanEqual', 'greaterThan', 'greaterThanEqual']);
