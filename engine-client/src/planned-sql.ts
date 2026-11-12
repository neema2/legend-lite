// THE ONE TEXT A HOST SENDS ITS DATABASE (C3, 2026-11-12, the user's: "the hosts' separation proven structurally"): a
// statement a PLAN carries, or one the PLANNER wrote as its answer (a host operation's, a session's setup, a catalog's
// read, a project database's statements). Every engine method that sends SQL -- QueryEngine.run, DuckDbEngine's
// execute/stream (a Plan's sql), the held connection's rows/table, the hosts' plan runner -- takes this type and no
// other, so a host's hand-written SQL does not compile: a `string` is not a PlannedSql.
//
// It is minted in ONE place each, where a plan or a planner's answer is READ (fromPlan's callers): the hosts' plan
// runner (plan-runner.ts: a plan's steps), and the planner clients that read the planner's answers
// (legend/wasm-grammar.ts: Query's and Studio's; DataCube's lite-planner.ts, and planner.ts's reader of a server's
// generatePlan). The guard (tools/guards/PlannedSqlTest.java) holds the minting to those files; a test's own SQL is
// typed by the test-only `unplanned` (engine-client/test/unplanned.ts), which no product source imports.
//
// What a host still sends that no plan carries is NAMED, one call of `notPlanned` each, and the guard pins the list
// (PlannedSqlTest.NOT_PLANNED): it only shrinks.

import type { ProjectDataStatements } from './project-data.ts';

declare const planned: unique symbol;

/** A statement a plan carries or the planner wrote: the only SQL a host's engine sends. */
export type PlannedSql = string & { readonly [planned]: true };

/** `text`, as the plan or the planner's answer it was READ from carries it -- called by the plan readers and the planner
 *  clients alone (the guard holds it so): the text is never edited, only typed. */
export function fromPlan(text: string): PlannedSql {
  return text as PlannedSql;
}

/** Each of `texts`, as {@link fromPlan} types one. */
export function allFromPlan(texts: readonly string[]): PlannedSql[] {
  return texts.map(fromPlan);
}

/**
 * NOT from a plan: a statement a host sends that no plan carries, named by `why` where it is sent. The guard pins each
 * caller (tools/guards/PlannedSqlTest.java, NOT_PLANNED; the total wall's C3, 2026-11-12): Studio's SQL
 * playground (a person's own SQL, run as typed) and Query's demo warehouse fixture (its seed files' statements). Both
 * were reported to the orchestrator for a ruling: plan each, or keep it named.
 */
export function notPlanned(why: string, text: string): PlannedSql {
  if (why.trim() === '') throw new Error('a statement no plan carries is sent only by name');
  return text as PlannedSql;
}

/** The planner's answer for a project's database (Boundary.projectData), its statements typed as the planner wrote
 *  them: what WasmGrammar and DataCube's planner client read it as. Here, beside the mints, so a planner client that
 *  reads it carries no project database's code (project-data.ts imports the plan runner). */
export function plannedProjectData(answer: unknown): ProjectDataStatements {
  const a = answer as { name: string; open: string[]; use: string[]; load?: unknown; close: string[] };
  return { name: a.name, open: allFromPlan(a.open), use: allFromPlan(a.use), ...(a.load === undefined ? {} : { load: a.load }),
    close: allFromPlan(a.close) };
}
