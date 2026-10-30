// DYNAMIC REPLACE (docs/DATACUBE_FLOW_MODE_2026_10_24.md §4, Alteryx's Dynamic Replace): a box's values replaced by
// RULES read from another box's rows -- each row a rule: the column it replaces, a condition in Pure over `$x` (the row),
// and the value it puts there (a value of the column's type, or -- `expressions` -- Pure over `$x` too); of a column's
// rules, the first whose condition holds, as Alteryx's, the value kept where none does. Pinned as a calculated column per
// column the rules name (`<column>_replaced`, handed on in its place): `if(<c1>, |<v1>, |if(<c2>, |<v2>, |$x.<column>))`.
//
// What a box reads back, with the canvas (`replacedOf`); the rules read and the commands that pin them in
// dynamic-replace-rules.ts, fetched with the first Dynamic Replace (as multi-field.ts and multi-field-columns.ts are).
//
// PURE: no DOM, no planner.

import type { ValueSpecification } from '../../pure-protocol/src/index.ts';
import type { DerivedColumn } from './snapshot.ts';

/** One rule: the column it replaces, its condition (Pure over `$x`), and its value (a cell's, or Pure over `$x`). */
export interface ReplaceRule {
  readonly column: string;
  readonly condition: string;
  readonly value: string | number | boolean;
}

/** A Dynamic Replace, as its window and Python set it: its rules in order, and whether their values are expressions. */
export interface DynamicReplace {
  readonly rules: readonly ReplaceRule[];
  readonly expressions: boolean;
}

/** The columns of a rules box that hold each rule's three parts, by their names. */
export interface RuleColumns {
  readonly column: string;
  readonly condition: string;
  readonly value: string;
}

/** The most rules a Dynamic Replace takes: each is a branch of its columns' expressions, which the database judges row
 *  by row -- a box of more rows is no list of rules. */
export const MAX_RULES = 200;

/** What a replaced column is called: its column's name and this. */
export const REPLACED_SUFFIX = '_replaced';

/**
 * A replaced column read off its expression: the column it replaces, and how many rules -- `<column>_replaced`, its body
 * a chain of `if(<condition>, |<value>, |...)` ending in `$x.<column>` itself; undefined for any other.
 */
export function replacedOf(d: DerivedColumn): { readonly column: string; readonly rules: number } | undefined {
  if (!d.name.endsWith(REPLACED_SUFFIX) || d.lambda === undefined || d.lambda.parameters.length !== 1 || d.lambda.body.length !== 1) return undefined;
  const column = d.name.slice(0, -REPLACED_SUFFIX.length);
  const row = d.lambda.parameters[0]!.name;
  let node = d.lambda.body[0]!;
  let rules = 0;
  while (node._type === 'func' && node.function === 'if' && node.parameters.length === 3) {
    const [, then, otherwise] = node.parameters as [ValueSpecification, ValueSpecification, ValueSpecification];
    if (then._type !== 'lambda' || otherwise._type !== 'lambda' || then.parameters.length > 0 || otherwise.body.length !== 1) return undefined;
    rules++;
    node = otherwise.body[0]!;
  }
  const last = node;
  const isColumn = last._type === 'property' && last.property === column && last.parameters.length === 1
    && last.parameters[0]!._type === 'var' && last.parameters[0]!.name === row;
  return rules > 0 && isColumn ? { column, rules } : undefined;
}

/** A Dynamic Replace in words: "region by 3 rules". */
export const replacedWords = (r: { readonly column: string; readonly rules: number }): string =>
  `${r.column} by ${r.rules} rule${r.rules === 1 ? '' : 's'}`;
