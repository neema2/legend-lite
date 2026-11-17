// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

/**
 * A PRODUCT RAISE, WORDED (#104's M2(b), 2026-11-15; phase D's Phase 2, raises as SITES, 2026-11-16): a statement lite
 * planned raises a bare token `LG#<n>` inside an envelope (the dialect's raise), and the tab reads it as Java's runner
 * does (exec.RaisedErrors): site `n` of the statement it sent, from the sites the plans it read declared for that text
 * (declared-raises.ts) -- its kind, its where, and its words rendered at planning (`upstream` where the plan carries
 * them, else `message`) -- at its one send's failure (DuckDbEngine's), so every route that sends a plan's statement
 * says the same words, as Python's one send does. The envelope's marks are GENERATED from legend-lite
 * (src/generated/lite-raise.ts); the envelope's cases are one shared fixture (test/fixtures/raise-envelopes.json) Java's,
 * the tab's and Python's readers all read.
 */

import { QueryError } from './engine.ts';
import { declaredSites } from './declared-raises.ts';
import { RAISE } from './generated/lite-raise.ts';

/** A product raise a plan's statement made: its kind, its where (a fixed text known at planning, or none), and its
 *  planned words as its message. A QueryError, as every failure of a send is. */
export class PlanRaise extends QueryError {
  readonly kind: string;
  readonly where: string | undefined;

  constructor(kind: string, words: string, where: string | undefined, sql: string, options?: { cause?: unknown }) {
    super(words, sql, options);
    this.name = 'PlanRaise';
    this.kind = kind;
    this.where = where;
  }
}

/** A failure the database raised itself, with no envelope of lite's (phase D's Phase 2, item 4, 2026-11-16; the user's
 *  ruling: "I'm good with database only giving the database message too"): kind DATABASE_ERROR, its message the
 *  database's own, verbatim, carried as the database's (`databaseMessage`), as Java's runner tells it
 *  (exec.RaisedErrors, error.DataError). A QueryError, as every failure of a send is. */
export class DatabaseError extends QueryError {
  readonly kind = 'DATABASE_ERROR';

  /** The database's own words, verbatim: the message itself. */
  get databaseMessage(): string {
    return this.message;
  }
}

/** What a database's `message` holds of a raise, as Java's runner reads it (exec.RaisedErrors.unwrapped): the text
 *  between the first two sentinels, a source position before its separator dropped, a held quote restored; and the
 *  site's number where the text is a token. Undefined where it holds no raise. */
export function raiseIn(message: string): { readonly site?: number; readonly text: string } | undefined {
  const first = message.indexOf(RAISE.SENTINEL);
  const last = first < 0 ? -1 : message.indexOf(RAISE.SENTINEL, first + 1);
  if (first < 0 || last <= first) return undefined;
  let inner = message.slice(first + 1, last).split(RAISE.QUOTE_MARK).join("'");
  const sep = inner.indexOf(RAISE.POSITION_SEP);
  if (sep >= 0) inner = inner.slice(sep + 1);
  const digits = inner.slice(RAISE.TOKEN.length);
  if (!inner.startsWith(RAISE.TOKEN) || !/^[0-9]{1,9}$/.test(digits) || Number(digits) < 1) return { text: inner };
  return { site: Number(digits), text: inner };
}

/**
 * A send's failure (`message`, the database's words, already redacted) as the error the tab throws: a site's token
 * its planned words (a PlanRaise), any other raised text (a user's `fail` message) that text, a database's own error
 * its words -- each a QueryError of `sql`, `options` its cause. A token the statement `sql` does not declare is refused
 * by name, the database's error kept as its cause.
 */
export function failure(message: string, sql: string, options: { cause?: unknown }): QueryError {
  const raise = raiseIn(message);
  // no envelope: the database's own failure, its words verbatim
  if (raise === undefined) return new DatabaseError(message, sql, options);
  if (raise.site === undefined) return new QueryError(raise.text, sql, options);
  // read through the sites the plans the tab read declare for this statement (declared-raises.ts), never a table of
  // its own
  const site = declaredSites(sql)?.[raise.site - 1];
  if (!site) return new QueryError(`the database raised ${raise.text}, which the statement does not declare`, sql, options);
  return new PlanRaise(site.kind, site.upstream ?? site.message, site.where ?? undefined, sql, options);
}
