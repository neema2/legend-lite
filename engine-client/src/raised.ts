// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

/**
 * A PRODUCT RAISE, WORDED (#104's M2(b), 2026-11-15; the user's ruling of 2026-11-10: "a product error is its kind plus
 * where it happened ... never the value"): a statement lite planned raises its kind and its where inside an envelope
 * (sql.RaiseKind, the dialect's raise), and the tab words it as Java's runner does (exec.RaisedErrors, error.DataError):
 * the kind's sentence, then its where -- at its one send's failure (DuckDbEngine's), so every route that sends a plan's
 * statement says the same words, as Python's one send does. The envelope's marks and the sentences are GENERATED from
 * legend-lite (src/generated/lite-facts.ts, error.ErrorKind): one source, never copied; the envelope's cases are one
 * shared fixture (test/fixtures/raise-envelopes.json) Java's, the tab's and Python's readers all read.
 */

import { QueryError } from './engine.ts';
import { RAISE, RAISED_KINDS } from './generated/lite-facts.ts';

/** A product raise a plan's statement made: its kind, its where (a fixed text known at planning, or none), and its
 *  words -- the kind's sentence, then the where -- as its message. A QueryError, as every failure of a send is. */
export class PlanRaise extends QueryError {
  readonly kind: string;
  readonly where: string | undefined;

  constructor(kind: string, sentence: string, where: string | undefined, sql: string, options?: { cause?: unknown }) {
    super(where === undefined ? sentence : `${sentence}: ${where}`, sql, options);
    this.name = 'PlanRaise';
    this.kind = kind;
    this.where = where;
  }
}

/** What a database's `message` holds of a raise, as Java's runner reads it (exec.RaisedErrors.unwrapped, kindOf,
 *  whereOf): the text between the first two sentinels, a source position before its separator dropped, a held quote
 *  restored; a kind's name behind its mark, and the where behind a second. Undefined where it holds no raise. */
export function raiseIn(message: string): { readonly kind?: string; readonly where?: string; readonly text: string } | undefined {
  const first = message.indexOf(RAISE.SENTINEL);
  const last = first < 0 ? -1 : message.indexOf(RAISE.SENTINEL, first + 1);
  if (first < 0 || last <= first) return undefined;
  let inner = message.slice(first + 1, last).split(RAISE.QUOTE_MARK).join("'");
  const sep = inner.indexOf(RAISE.POSITION_SEP);
  if (sep >= 0) inner = inner.slice(sep + 1);
  if (!inner.startsWith(RAISE.KIND_MARK)) return { text: inner };
  const end = inner.indexOf(RAISE.KIND_MARK, 1);
  return end < 0 ? { kind: inner.slice(1), text: inner } : { kind: inner.slice(1, end), where: inner.slice(end + 1), text: inner };
}

/**
 * A send's failure (`message`, the database's words, already redacted) as the error the tab throws: a product raise its
 * kind's words (a PlanRaise), any other raised text (a user's `fail` message) that text, a database's own error its
 * words -- each a QueryError of `sql`, `options` its cause. A kind legend-lite does not name is refused by name, the
 * database's error kept as its cause.
 */
export function failure(message: string, sql: string, options: { cause?: unknown }): QueryError {
  const raise = raiseIn(message);
  if (raise === undefined) return new QueryError(message, sql, options);
  if (raise.kind === undefined) return new QueryError(raise.text, sql, options);
  if (!Object.hasOwn(RAISED_KINDS, raise.kind)) {
    return new QueryError(`a raise of the kind '${raise.kind}', which legend-lite does not name`, sql, options);
  }
  return new PlanRaise(raise.kind, RAISED_KINDS[raise.kind]!, raise.where, sql, options);
}
