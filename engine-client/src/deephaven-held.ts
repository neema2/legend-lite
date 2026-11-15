// WHAT A SERVER MUST HOLD WHEN A RUNTIME CONNECTS (docs/DEPLOYMENT_DESIGN_2026_11_14.md §9, step 3): a Deephaven
// server's tables are its deployment's, made by lite's loader when it started, and no runtime knows what is on a
// server -- so before a desk reads anything, each table a deployment lands on the runtime's server is read once by the
// plan the planner wrote for it (its `deephaven` request, op `holds`: data), its columns checked against its store's
// declaration as every read is (DH16). Every table is tried; one missing or held otherwise refuses the whole, naming
// each and why. This decides nothing about a table: the planner wrote what is read, the server answers it.

/** A table a runtime's server must hold, as the planner answers it (`DeploymentPlanner.onConnect`). */
export interface HeldTable {
  /** The server's connection, as the runtime binds it. */
  readonly connection: string;
  /** The deployment that lands it there. */
  readonly deployment: string;
  readonly table: string;
  /** The plan that reads it whole on the server. */
  readonly sql: string;
}

/** The tables the planner answered (its `deephaven` request, op `holds`: `OK` and the JSON, or `ERR` and why). */
export function heldTablesOf(answer: string): HeldTable[] {
  if (!answer.startsWith('OK\n')) {
    throw new Error(`the planner refused what the runtime's server must hold: ${answer.split('\n').slice(2).join('\n') || answer}`);
  }
  return (JSON.parse(answer.slice(3)) as { readonly tables: HeldTable[] }).tables;
}

/** A server that does not hold what its deployments land: each table and why, as the server or the check said it. */
export class DeephavenNotHeld extends Error {
  readonly missing: readonly { readonly table: string; readonly deployment: string; readonly why: string }[];
  constructor(missing: DeephavenNotHeld['missing']) {
    super(`the Deephaven server does not hold what its deployment lands: ${missing.map((m) =>
      `${m.table} (${m.deployment}): ${m.why}`).join('; ')}`);
    this.name = 'DeephavenNotHeld';
    this.missing = missing;
  }
}

/**
 * Each of `tables` read by `read` (a session's `keepAlive`: the table made on the server and let go, its columns
 * checked), all at once: every one is tried, and any refused refuses the whole, naming each.
 */
export async function checkHeld(tables: readonly HeldTable[],
  read: (sql: string) => Promise<{ release(): void }>): Promise<void> {
  const answers = await Promise.allSettled(tables.map(async (t) => (await read(t.sql)).release()));
  const missing = answers.flatMap((a, i) => a.status === 'rejected'
    ? [{ table: tables[i]!.table, deployment: tables[i]!.deployment,
      why: a.reason instanceof Error ? a.reason.message : String(a.reason) }]
    : []);
  if (missing.length > 0) throw new DeephavenNotHeld(missing);
}
