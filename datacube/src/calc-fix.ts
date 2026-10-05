// A calculated column refused for ARITHMETIC OVER A POSSIBLY-EMPTY COLUMN, and the two ways to say
// what an empty row should give -- offered to the person, never applied for them.
//
// `$x.notional * 1.1` is `times([$x.notional, 1.1])`, and a literal of more than one value takes
// each element exactly [1]: legend-engine, legend-pure and legend-lite all refuse a nullable
// column there ("Collection element must have a multiplicity [1]"). The person means one of:
//   - a blank where the column is empty: `$x.notional->toOne() * 1.1` (the SQL is plain
//     `notional * 1.1`, so an empty row's result is empty);
//   - the empty column read as zero: `$x.notional->coalesce(0.0) * 1.1`.
// The rewrite is on the lambda's tree; the editor prints it with the compiler (E4) and compiles
// it again, so the compiler -- not this file -- judges the result.

import {
  fn, lit, transform, type AppliedFunction, type AppliedProperty, type Collection, type Lambda, type ValueSpecification,
} from '../../pure-protocol/src/index.ts';
import { isNumeric, plainType } from '../../engine-client/src/types.ts';

/** The refusal, in each compiler's words (legend-lite's and legend-engine's begin alike). */
export function isEmptyOperandRefusal(message: string): boolean {
  return message.includes('Collection element must have a multiplicity [1]');
}

/** Pure's arithmetic runs: the parser's `a + b`, `a - b`, `a * b` are one call over a collection. */
const RUNS: ReadonlySet<string> = new Set(['plus', 'minus', 'times']);

function runOperands(n: ValueSpecification): readonly ValueSpecification[] | undefined {
  if (n._type !== 'func' || !RUNS.has((n as AppliedFunction).function)) return undefined;
  const [only, ...rest] = (n as AppliedFunction).parameters;
  if (only?._type !== 'collection' || rest.length > 0) return undefined;
  const values = (only as Collection).values;
  return values.length > 1 ? values : undefined;
}

/** `$x.column`: a column read straight off the row. */
function columnRead(n: ValueSpecification): string | undefined {
  if (n._type !== 'property') return undefined;
  const [owner] = (n as AppliedProperty).parameters;
  return owner?._type === 'var' ? (n as AppliedProperty).property : undefined;
}

/** The columns read bare inside an arithmetic run: the operands the refusal can be about. */
export function emptyOperands(l: Lambda): string[] {
  const found: string[] = [];
  transform(l, (n) => {
    for (const v of runOperands(n) ?? []) {
      const c = columnRead(v);
      if (c !== undefined && !found.includes(c)) found.push(c);
    }
    return n;
  });
  return found;
}

/** How an empty operand reads: a blank result, or zero. */
export type EmptyAs = 'blank' | 'zero';

/**
 * The lambda with each bare column operand of an arithmetic run said: `->toOne()` (blank) or
 * `->coalesce(<zero of the column's type>)`. `typeOf` gives a column's compiler type.
 */
export function sayEmpty(l: Lambda, as: EmptyAs, typeOf: (column: string) => string | undefined): Lambda {
  const said = (v: ValueSpecification): ValueSpecification => {
    const c = columnRead(v);
    if (c === undefined) return v;
    return as === 'blank' ? fn('toOne', v) : fn('coalesce', v, zeroOf(typeOf(c)));
  };
  return transform(l, (n) => {
    const values = runOperands(n);
    if (values === undefined) return n;
    const f = n as AppliedFunction;
    const coll = f.parameters[0] as Collection;
    return { ...f, parameters: [{ ...coll, values: values.map(said) }] };
  }) as Lambda;
}

/** Zero, as a literal of the column's own type (coalesce takes one type); a Float where unknown. */
function zeroOf(type: string | undefined): ValueSpecification {
  return type !== undefined && isNumeric(type) ? lit.of(plainType(type), 0) : lit.float(0);
}
