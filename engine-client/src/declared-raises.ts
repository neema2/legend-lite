// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

/**
 * THE RAISE SITES THE PLANS DECLARE (phase D's Phase 2, raises as SITES, 2026-11-16; option (D) of 2026-11-15 before
 * it): a statement lite planned raises a bare token `LG#<n>`, and the plan carries what it means -- each statement's
 * `sites` (PlanJson v56: `n`, `kind`, `where`, `message`, and in an edge plan `upstream`), its words rendered at
 * planning. Each reader of a planner's answer declares them here as it reads it, by the statement's text, and a send's
 * failure is read through the sites of the statement it sent (raised.ts) -- so the tab holds no table of words of its
 * own: a token no answer it read declared for that text is refused by name. A text two answers declare differently is
 * refused by name. Small, and outside raised.ts, so a reader that declares carries none of the failure path's code.
 */

/** One raise site of a statement, as the plan carries it. */
export interface Site {
  readonly n: number;
  readonly kind: string;
  readonly where?: string | null;
  readonly message: string;
  readonly upstream?: string | null;
}

const declared = new Map<string, readonly Site[]>();

function sitesOf(value: unknown, what: string): readonly Site[] {
  if (!Array.isArray(value)) throw new Error(`${what}'s raise sites are not a list`);
  return value.map((s, i) => {
    const site = s as Site;
    if (site === null || typeof site !== 'object' || site.n !== i + 1 || typeof site.kind !== 'string' || typeof site.message !== 'string') {
      throw new Error(`${what}'s raise site ${i + 1} is not a site numbered in order`);
    }
    return site;
  });
}

/** The sites `sites` of the statement `text`, declared; refused by name where the text was declared otherwise. */
export function declareStatementSites(text: string, sites: readonly Site[], what: string): void {
  const held = declared.get(text);
  if (held !== undefined && JSON.stringify(held) !== JSON.stringify(sites)) throw new Error(`${what} declares the raise sites of a statement otherwise than before`);
  declared.set(text, sites);
}

/** Every statement's sites a planner's answer carries (a plan, a text plan), declared: wherever an object of it holds
 *  `sites`, each statement text beside them (`statement`, `sql`, `run`, `statements`). Refused by name where the answer
 *  carries no `raises` (a plan of PlanJson v53 or later always does). */
export function declareSites(answer: unknown, what: string): void {
  if (answer === null || typeof answer !== 'object' || (answer as { raises?: unknown }).raises === null
    || typeof (answer as { raises?: unknown }).raises !== 'object') throw new Error(`${what} carries no raises`);
  const walk = (node: unknown): void => {
    if (Array.isArray(node)) { node.forEach(walk); return; }
    if (node === null || typeof node !== 'object') return;
    const o = node as Record<string, unknown>;
    if (o.sites !== undefined) {
      const sites = sitesOf(o.sites, what);
      for (const key of ['statement', 'sql', 'run'] as const) if (typeof o[key] === 'string') declareStatementSites(o[key] as string, sites, what);
      if (Array.isArray(o.statements)) for (const t of o.statements) if (typeof t === 'string') declareStatementSites(t, sites, what);
    }
    for (const v of Object.values(o)) walk(v);
  };
  walk(answer);
}

/** The sites a plan this tab read declared for the statement `text`, or undefined: none did. */
export function declaredSites(text: string): readonly Site[] | undefined {
  return declared.get(text);
}
