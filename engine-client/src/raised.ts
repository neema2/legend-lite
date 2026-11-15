// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

/**
 * A PRODUCT RAISE, WORDED (#104's M2(b), 2026-11-15; the user's ruling of 2026-11-10: "a product error is its kind plus
 * where it happened ... never the value"): a statement lite planned raises its kind and its where inside an envelope
 * (sql.RaiseKind, the dialect's raise), and a host that runs the plan words it as Java's runner does
 * (exec.RaisedErrors, error.DataError): the kind's sentence, then its where. The envelope's marks and the sentences
 * are GENERATED from legend-lite (src/generated/lite-facts.ts, error.ErrorKind): one source, never copied.
 */

import { RAISE, RAISED_KINDS } from './generated/lite-facts.ts';

/** A product raise a plan's statement made: its kind, its where (a fixed text known at planning, or none), and its
 *  words -- the kind's sentence, then the where -- as its message. */
export class PlanRaise extends Error {
  readonly kind: string;
  readonly where: string | undefined;

  constructor(kind: string, where: string | undefined, options?: { readonly cause?: unknown }) {
    super(where === undefined ? sentenceOf(kind) : `${sentenceOf(kind)}: ${where}`, options);
    this.name = 'PlanRaise';
    this.kind = kind;
    this.where = where;
  }
}

/** The raise inside a database's `message`, as Java's runner reads one (exec.RaisedErrors.unwrapped, kindOf,
 *  whereOf): the text between the first two sentinels, a source position before its separator dropped, a held quote
 *  restored; a kind's raise as a PlanRaise, any other raised text (a user's fail message) as an Error of that text.
 *  Undefined where `message` holds no raise: a database's own error, passed on as it is. */
export function raised(message: string, cause?: unknown): Error | undefined {
  const first = message.indexOf(RAISE.SENTINEL);
  const last = first < 0 ? -1 : message.indexOf(RAISE.SENTINEL, first + 1);
  if (first < 0 || last <= first) return undefined;
  let inner = message.slice(first + 1, last).split(RAISE.QUOTE_MARK).join("'");
  const sep = inner.indexOf(RAISE.POSITION_SEP);
  if (sep >= 0) inner = inner.slice(sep + 1);
  if (!inner.startsWith(RAISE.KIND_MARK)) return new Error(inner, { cause });
  const end = inner.indexOf(RAISE.KIND_MARK, 1);
  return new PlanRaise(end < 0 ? inner.slice(1) : inner.slice(1, end), end < 0 ? undefined : inner.slice(end + 1),
    { cause });
}

/** The sentence of `kind`; a kind legend-lite does not name is refused by name (no default). */
function sentenceOf(kind: string): string {
  const sentence = RAISED_KINDS[kind];
  if (sentence === undefined) throw new Error(`a raise of the kind '${kind}', which legend-lite does not name`);
  return sentence;
}
