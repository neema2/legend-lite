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

/** One column of a plan record's typed result: its name, its Pure type as the typer names it (`Decimal(10,2)`, an
 *  enumeration's path), and whether the compiler typed it with an enumeration (null where no compiler typed it). */
export interface RecordColumn {
  readonly name: string;
  readonly type: string;
  readonly enumeration: boolean | null;
}

/** A query's rows as a host runs them (the plan record's one typed result over a statement of text: HostPlans.read,
 *  and PureV1Api.jsonPlan's; anything else refused there, at planning): its statement and its columns. A plan of
 *  another format, version or shape is refused by name, never run some other way. */
export function hostResult(plan: string): { readonly statement: string; readonly columns: readonly RecordColumn[] } {
  const root = readPlanRecord(plan).root as { readonly _type?: string; readonly columns?: unknown;
    readonly sql?: { readonly statement?: unknown; readonly slots?: unknown } } | undefined;
  if (root?._type !== 'tdsResult' || typeof root.sql?.statement !== 'string' || !Array.isArray(root.sql.slots) || root.sql.slots.length > 0
    || !Array.isArray(root.columns)) {
    throw new PlanRefusal(`a host's plan is one typed result over a statement of text, not a ${String(root?._type)}`);
  }
  const columns = root.columns.map((c: unknown) => {
    const col = c as { name?: unknown; type?: unknown; enumeration?: unknown };
    if (typeof col.name !== 'string' || typeof col.type !== 'string' || !(typeof col.enumeration === 'boolean' || col.enumeration === null)) {
      throw new PlanRefusal(`a plan's column without a name, a type and its enumeration fact: ${JSON.stringify(c)}`);
    }
    return { name: col.name, type: col.type, enumeration: col.enumeration };
  });
  return { statement: root.sql.statement, columns };
}

/** The one statement a host runs for a query's rows (hostResult): its text. */
/** A pivot whose columns its data names (a keyed plan, HostPlans.query; D18 D): its keys statement, which the host runs
 *  first; undefined for a plan of one statement. */
export function keyedStatement(plan: string): string | undefined {
  const root = readPlanRecord(plan).root as { readonly _type?: string; readonly keys?: { readonly statement?: unknown; readonly slots?: unknown } } | undefined;
  if (root?._type !== 'keyed') return undefined;
  if (typeof root.keys?.statement !== 'string' || !Array.isArray(root.keys.slots) || root.keys.slots.length > 0) {
    throw new PlanRefusal("a host's pivot reads its keys with a statement of text");
  }
  return root.keys.statement;
}

export function answerStatement(plan: string): string {
  return hostResult(plan).statement;
}

/** A plan record's column type as legend-engine's relation type names it, by the record's one rule
 *  (core's ExecutionPlan.Column.plainType; Python's compiler.plain_type): a precision decimal, which the typer spells
 *  `Decimal(p,s)`, is `Decimal` (its precision and scale are its SQL type's); any other type is its own name. */
export function plainType(type: string): string {
  if (type.startsWith('Decimal(') && type.endsWith(')')) {
    const [precision, scale, ...rest] = type.slice('Decimal('.length, -1).split(',');
    if (rest.length === 0 && precision !== undefined && scale !== undefined && /^[0-9]+$/.test(precision) && /^[0-9]+$/.test(scale)) {
      return 'Decimal';
    }
  }
  return type;
}
