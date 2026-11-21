// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

/**
 * THE RAISE SITES THE PLANS DECLARE (phase D's Phase 2, raises as SITES, 2026-11-16; option (D) of 2026-11-15 before
 * it): a statement lite planned raises a bare token `LG#<n>`, and the plan carries what it means -- each statement's
 * `sites` (PlanJson v60: `n`, `kind`, `operands` -- facts, no words since stage B's B1, 2026-11-20, no `where` since
 * stage B's BE, 2026-11-21; the tab words a site itself, its kind's sentence from lite-raise.ts). Each reader of a
 * planner's answer declares them here as it reads it, by the statement's text, and a send's
 * failure is read through the sites of the statement it sent (raised.ts) -- so the tab holds no table of words of its
 * own: a token no answer it read declared for that text is refused by name. A text two answers declare differently is
 * refused by name. Small, and outside raised.ts, so a reader that declares carries none of the failure path's code.
 */

/** One raise site of a statement, as the plan carries it. */
export interface Site {
  readonly n: number;
  readonly kind: string;
  /** The literals the statement holds for the raise, one key each (`integer`, `string`, ... or `computed`). */
  readonly operands?: readonly Readonly<Record<string, unknown>>[];
}

const declared = new Map<string, readonly Site[]>();

/** The sites `sites` of the statement `text`, declared; a text declared otherwise before refused by name. */
export function declareStatementSites(text: string, sites: readonly Site[], what: string): void {
  const held = declared.get(text);
  if (held && JSON.stringify(held) !== JSON.stringify(sites)) throw new Error(`${what} declares a statement's raise sites otherwise`);
  declared.set(text, sites);
}

/** Every statement's sites a planner's answer carries (a plan, a text plan), declared: wherever an object of it holds
 *  `sites`, each statement text beside them (`statement`, `sql`, `run`, `statements`). Refused by name where the answer
 *  carries no `raises`, the kinds its statements can send (a plan of PlanJson v60 always does). */
export function declareSites(answer: unknown, what: string): void {
  if (!Array.isArray((answer as { raises?: unknown } | null)?.raises)) throw new Error(`${what} carries no raises`);
  const walk = (node: unknown): void => {
    if (!node || typeof node !== 'object') return;
    const o = node as Record<string, unknown>;
    if (Array.isArray(o.sites)) {
      for (const t of [o.statement, o.sql, o.run, ...(Array.isArray(o.statements) ? o.statements : [])]) {
        if (typeof t === 'string') declareStatementSites(t, o.sites as Site[], what);
      }
    }
    Object.values(o).forEach(walk);
  };
  walk(answer);
}

/** The sites a plan this tab read declared for the statement `text`, or undefined: none did. */
export function declaredSites(text: string): readonly Site[] | undefined {
  return declared.get(text);
}
