// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

/**
 * THE RAISES THE PLANS DECLARE (phase D's option (D), 2026-11-15, the orchestrator's go: "the plan-level raises map ...
 * One source: ErrorKind through the planner"): each plan and text plan the planner answers carries `raises`, every
 * product raise its statements can send, by kind, to the kind's sentence (PlanJson.raises, error.ErrorKind's words).
 * Each reader of a planner's answer declares them here as it reads it, and a send's failure is worded from what was
 * declared (raised.ts) -- so the tab holds no table of words of its own: a kind no answer it read declared is refused by
 * name. One sentence per kind, the planner's: a plan that says otherwise for a kind already declared is refused by name.
 * Small, and outside raised.ts, so a reader that declares carries none of the failure path's code.
 */

const declared = new Map<string, string>();

/** The raises a planner's answer carries (`raises`: kind to sentence), declared; refused by name where the answer has
 *  none (a plan of PlanJson v53 always carries them), or a kind's sentence is not text or differs from one declared. */
export function declareRaises(raises: unknown, what: string): void {
  if (raises === null || typeof raises !== 'object') throw new Error(`${what} carries no raises`);
  for (const [kind, sentence] of Object.entries(raises)) {
    if (typeof sentence !== 'string' || (declared.get(kind) ?? sentence) !== sentence) throw new Error(`${what}'s raise ${kind} is not worded as before`);
    declared.set(kind, sentence);
  }
}

/** The sentence a plan this tab read declared for `kind`, or undefined: none did. */
export function declaredSentence(kind: string): string | undefined {
  return declared.get(kind);
}
