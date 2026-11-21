// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

/**
 * A PRODUCT RAISE, WORDED (#104's M2(b), 2026-11-15; phase D's Phase 2, raises as SITES, 2026-11-16): a statement lite
 * planned raises a bare token `LG#<n>` inside an envelope (the dialect's raise), and the tab reads it as Java's runner
 * does (exec.RaisedErrors): site `n` of the statement it sent, from the sites the plans it read declared for that text
 * (declared-raises.ts) -- its kind and its operands, worded here in the one catalog's words (RAISE_FORMS, generated from
 * error.ErrorKind: the plan carries no words since stage B's B1, v59, 2026-11-20) -- at its one send's failure (DuckDbEngine's), so every route that sends a plan's statement
 * says the same words, as Python's one send does. The envelope's marks are GENERATED from legend-lite
 * (src/generated/lite-raise.ts); the envelope's cases are one shared fixture (test/fixtures/raise-envelopes.json) Java's,
 * the tab's and Python's readers all read.
 */

import { QueryError } from './engine.ts';
import { declaredSites } from './declared-raises.ts';
import { RAISE, RAISE_FORMS } from './generated/lite-raise.ts';

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
 *  database's own, verbatim, its kind DATABASE_ERROR, as Java's runner tells it
 *  (exec.RaisedErrors, error.DataError). A QueryError, as every failure of a send is. */
export class DatabaseError extends QueryError {
  readonly kind = 'DATABASE_ERROR';
}

/** What a database's `message` holds of a raise, as Java's runner reads it (exec.RaisedErrors.unwrapped): the text
 *  between the first two sentinels, a source position before its separator dropped, a held quote restored; and the
 *  site's number where the text is a token. Undefined where it holds no raise. */
export function raiseIn(message: string): { readonly site?: number; readonly text: string } | undefined {
  const [, inner] = message.split(RAISE.SENTINEL, 3);
  if (inner === undefined || !message.includes(RAISE.SENTINEL, message.indexOf(RAISE.SENTINEL) + 1)) return undefined;
  const text = inner.split(RAISE.QUOTE_MARK).join("'").split(RAISE.POSITION_SEP).pop()!;
  const digits = text.startsWith(RAISE.TOKEN) ? text.slice(RAISE.TOKEN.length) : '';
  return /^[1-9]\d{0,8}$/.test(digits) ? { site: +digits, text } : { text };
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
  return new PlanRaise(site.kind, sentence(site), site.where ?? undefined, sql, options);
}

/** A site of a plan's statement as the tab reads it (PlanJson v59): its kind and its operands, each one key -- a literal
 *  by its Pure kind (string, integer, float, decimal, boolean, date, dateTime) or computed. */
export interface RaiseSiteFacts {
  readonly kind: string;
  readonly operands?: readonly Readonly<Record<string, unknown>>[];
}

/** A site's sentence, the one catalog's (error.ErrorKind's forms, generated: RAISE_FORMS): the first form that takes
 *  the operands, each operand printed as the form's slot says, a value the database computed `<computed>` -- what
 *  Java's runner says (result.RaiseWords). A kind or a count of operands no form says is refused by name. */
export function sentence(site: RaiseSiteFacts): string {
  const forms = RAISE_FORMS[site.kind];
  if (forms === undefined) throw new Error(`a raise of kind ${site.kind}, which this tab has no sentence for`);
  const ops = site.operands ?? [];
  const empty = (i: number): boolean => ops[i]?.string === '';
  const form = forms.find((f) => (f.arity < 0 || f.arity === ops.length)
      && (f.emptySlot < 0 || (f.emptySlot < ops.length && empty(f.emptySlot))));
  if (form === undefined) throw new Error(`no sentence of ${site.kind} says ${ops.length} values`);
  const values = ops.map((o, i) => print(o, form.slots[i] ?? 'TEXT'));
  if (form.shape === 'JOINED') return values.join('');
  return form.template.replace(/%(\d+)\$s/g, (_, n: string) => values[Number(n) - 1]);
}

const COMPUTED = '<computed>';

function print(o: Readonly<Record<string, unknown>>, slot: string): string {
  const [key, value] = Object.entries(o)[0] ?? ['computed', true];
  if (key === 'computed') return COMPUTED;
  switch (slot) {
    case 'DATE':
      return key === 'string' ? dated(String(value)) : text(key, value);
    case 'FLOAT':
      return key === 'float' || key === 'integer' || key === 'decimal' ? carried(Number(value)) : text(key, value);
    case 'PLAIN':
      return key === 'float' ? plain(String(value), true) : text(key, value);
    default:
      return text(key, value);
  }
}

/** As the program wrote it: a text itself, an Integer's digits, a Float as Pure prints one, a Decimal's plain digits,
 *  a Boolean's word, a date's ISO text. */
function text(key: string, value: unknown): string {
  switch (key) {
    case 'float': return carried(Number(value));
    case 'decimal': return plain(String(value), true);
    default: return String(value);
  }
}

/** A date literal at the precision it was written, `+0000` from a minute on (legend-pure's DateFormat.append). */
function dated(literal: string): string {
  return /T\d{2}:\d{2}/.test(literal) ? `${literal}+0000` : literal;
}

/** A Float as Pure prints one (result.CellText.carriedFloat): its shortest digits laid out with a point always, never
 *  an exponent. */
function carried(d: number): string {
  const p = plain(String(d), false);
  return p.includes('.') ? p : `${p}.0`;
}

/** `s`, a number's digits possibly with an exponent, laid out plainly: every digit kept where `keep`
 *  (BigDecimal.toPlainString), trailing fraction zeros dropped otherwise. */
function plain(s: string, keep: boolean): string {
  const m = /^(-?)(\d+)(?:\.(\d+))?(?:[eE]([+-]?\d+))?$/.exec(s);
  if (m === null) return s;
  const [, sign, int, frac = '', exp = '0'] = m;
  let digits = int + frac;
  let point = int.length + Number(exp);
  if (point <= 0) {
    digits = '0'.repeat(1 - point) + digits;
    point = 1;
  } else if (point > digits.length) {
    digits = digits + '0'.repeat(point - digits.length);
  }
  let whole = digits.slice(0, point).replace(/^0+(?=\d)/, '');
  let fraction = digits.slice(point);
  if (!keep) fraction = fraction.replace(/0+$/, '');
  return sign + whole + (fraction.length > 0 ? `.${fraction}` : '');
}
