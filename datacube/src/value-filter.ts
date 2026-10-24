// THE VALUE LIST (Excel's autofilter checklist): a column's values, each with how many rows have it, read under the grid's
// other filters by the database (query.ts columnValuesLambda); the values a person keeps become one condition on the
// column, in place of the column's own conditions the filter had. One rule for the screen's Filter Values window and
// Python's grid.filter_values (grid-gestures.ts 'filter.values'). Nothing here computes a figure: the counts are the
// database's, and a choice is only words for a condition.

import { isVariant } from '../../engine-client/src/types.ts';
import { CubeRefusal, type FilterNode, type FilterValue } from './snapshot.ts';

/** The most values a value list offers; one more is asked, so the list can say it was cut. */
export const MAX_FILTER_VALUES = 1000;

/** The column a value list's query counts each value's rows in. */
export const VALUE_COUNT_COLUMN = '__rows__';

/**
 * A cell's value as a filter value -- the rule the menu's Add Filter has (app.ts): a big integer as its digits (a filter
 * is saved as JSON, which has no bigint), a JSON column's text as a document (`fromJson`), null as null (no value: a
 * blank). The value's literal is the column type's (query.ts literalNode).
 */
export function cellFilterValue(raw: unknown, columnType: string | undefined): FilterValue | null {
  if (raw === null || raw === undefined) return null;
  const value = (typeof raw === 'bigint' ? raw.toString() : raw) as FilterValue;
  return isVariant(columnType) && typeof value === 'string' ? { json: value } : value;
}

/** The filter's own conditions on `column`: its top-level ones (the whole filter, or the children of its AND). */
export function ownConditions(filter: FilterNode | undefined, column: string): FilterNode[] {
  if (filter === undefined) return [];
  const on = (n: FilterNode): boolean => n.kind === 'condition' && n.column === column && n.rightColumn === undefined
    || (n.kind === 'or' && n.children.length > 0 && n.children.every(on));
  if (filter.kind === 'and') return filter.children.filter(on);
  return on(filter) ? [filter] : [];
}

/** The filter without `column`'s own conditions: what a value list of `column` is read under. */
export function filterWithout(filter: FilterNode | undefined, column: string): FilterNode | undefined {
  if (filter === undefined) return undefined;
  const own = new Set(ownConditions(filter, column));
  if (own.size === 0) return filter;
  if (filter.kind !== 'and') return undefined;
  const rest = filter.children.filter((c) => !own.has(c));
  return rest.length === 0 ? undefined : rest.length === 1 ? rest[0] : { ...filter, children: rest };
}

/**
 * The condition keeping `kept` of `column`'s values (null, the blanks): `in` the values, `isEmpty` for the blanks, the
 * two OR-ed when both are kept. Nothing kept is refused -- a filter that keeps no row is never what was meant.
 */
export function keptCondition(column: string, kept: readonly (FilterValue | null)[]): FilterNode {
  const values = kept.filter((v): v is FilterValue => v !== null);
  const blank = kept.length > values.length;
  if (values.length === 0 && !blank) throw new CubeRefusal(`keep at least one value of '${column}'`);
  const listed: FilterNode = values.length === 1
    ? { kind: 'condition', column, operator: 'equal', value: values[0]! }
    : { kind: 'condition', column, operator: 'in', value: values };
  if (!blank) return listed;
  const empty: FilterNode = { kind: 'condition', column, operator: 'isEmpty' };
  return values.length === 0 ? empty : { kind: 'or', children: [listed, empty] };
}
