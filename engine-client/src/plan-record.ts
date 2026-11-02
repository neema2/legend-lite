// THE PLAN RECORD's head, as a host reads it (docs/HOST_EXECUTORS_PROGRAM_2026_11_02.md R2: every host path runs the one
// plan the planner writes, PlanJson's text): its format and version checked, and the one statement a host's execute runs.
// The record's format is core's (core/.../executionplan/PlanJson.java, VERSION its one owner); this file reads no more
// of it than a host needs, and refuses by name what it cannot read.
//
// PURE: no DOM, no planner.

/** The execution plan's format a host reads (core/.../executionplan/PlanJson.java VERSION): 20 since 2026-11-02, a
 *  column's declared type (R2); 19 since 2026-10-29, the LITERAL slot word retired (track L's L2). */
export const PLAN_VERSION = 20;

/** A plan record whose format or version this host does not read, or whose shape it does not run: refused by name,
 *  never run some other way. */
export class PlanRefusal extends Error {
  override readonly name = 'PlanRefusal';
}

/** A plan record's text read as JSON, its format and version this host's; anything else refused by name. */
export function readPlanRecord(plan: string): { readonly parameters?: unknown; readonly root?: { readonly _type?: string } } {
  const read = JSON.parse(plan) as { format?: unknown; version?: unknown; parameters?: unknown; root?: { _type?: string } };
  if (read.format !== 'legend-lite-plan' || read.version !== PLAN_VERSION) {
    throw new PlanRefusal(`not a legend-lite execution plan of version ${PLAN_VERSION}: ${JSON.stringify({ format: read.format, version: read.version })}`);
  }
  return read;
}

/** The one statement a host's execute runs (PureV1Api.jsonPlan's plan: one typed result over a statement of text,
 *  everything else refused there, at planning): its text. A plan of another shape is refused by name. */
export function answerStatement(plan: string): string {
  const root = readPlanRecord(plan).root as { readonly _type?: string; readonly sql?: { readonly statement?: unknown; readonly slots?: unknown } } | undefined;
  if (root?._type !== 'tdsResult' || typeof root.sql?.statement !== 'string' || !Array.isArray(root.sql.slots) || root.sql.slots.length > 0) {
    throw new PlanRefusal(`an execute's plan is one typed result over a statement of text, not a ${String(root?._type)}`);
  }
  return root.sql.statement;
}
