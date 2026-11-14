// PLANTED VIOLATORS of the brand-cast census (test/brand-casts.test.ts, the re-audit's S5, 2026-11-12): every way the
// regex census missed, and the others TypeScript allows, of making a plan's statement (PlannedSql) or a planner's answer
// (PlanText) out of a value that is neither. Each line marked `planted:` is read as that kind, and no other line is.
// Never imported: the census reads it through its own tsconfig.json, beside it. (An angle-bracket assertion, `<T>x`, is
// read as `x as T` is; tsc refuses one here -- erasableSyntaxOnly -- so none is planted.)

import type { PlannedSql, PlanText } from '../../../../engine-client/src/planned-sql.ts';
import type { Plan } from '../../../../engine-client/src/relation-type.ts';
import type { DuckDbEngine } from '../../../../engine-client/src/duckdb.ts';

declare const text: string;
declare const json: unknown;
/** A holder of the brand under a name of its own. */
interface Holder extends Plan {}
type Alias = PlannedSql;

export const indexed = text as Parameters<DuckDbEngine['run']>[0]; // planted: brand
export const extended = json as Holder; // planted: brand
export const omitted = json as Omit<Plan, 'columns'>; // planted: brand
export const listed = json as readonly { readonly sql: PlannedSql }[]; // planted: brand
export const promised = json as Promise<PlanText>; // planted: brand
export const called = json as (s: string) => PlannedSql; // planted: brand
export const optional = text as PlannedSql | undefined; // planted: brand
export const aliased = text as Alias; // planted: brand
export const keyed = json as Readonly<Record<string, PlannedSql>>; // planted: brand
export function guarded(x: string): x is PlannedSql { // planted: predicate
  return x.length > 0;
}
export function laundered<T>(x: unknown): T {
  return x as T; // planted: generic
}

// casts that make no brand: none is read
export const plain = json as { readonly sql: string };
export const literal = ['x'] as const;
export const widened = text as string;
