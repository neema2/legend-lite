// THE PLAN RUNNER, a host's (docs/WRITES_DESIGN_2026_10_18.md leg 7; docs/DATACUBE_FLOW_MODE_2026_10_24.md M6, an Output
// into a table): an execution plan -- PlanJson's text, version 19, as legend-lite's planner writes it -- run on a host's own
// database, for a host that runs its SQL itself: the tab (DuckDB-WASM) and Python's headless DataCube (its DuckDB). The
// rules are Java's PlanRunner's (core/.../exec/PlanRunner.java), for the plans a host's program makes: a SEQUENCE of
// steps in one transaction -- each `update` run for the change it makes, its count kept where a later step binds it
// (`#count`), each `check` run for what its database may raise and its rows dropped, the one `tdsResult` read once the
// transaction has committed -- or a `tdsResult` alone. Every value is the database's: its counts, its rows. The answer
// is TYPED rows (the result boundary program, 2026-10-27: a plan names no output format), and this runner writes them
// as lite's JSON rows, one object per row keyed by the plan's column names, for the cells a host's write answers (a
// count, a text, a boolean); a richer answer is the planner's to write (executeAnswer), refused here by name. What a
// host's program never plans is refused by name, never run some other way: a plan that takes parameters, a type hole,
// a list or a key's cell bound, a statement of per-key regions, a step of another database or of several, a database set
// up first.
//
// PURE: no DOM, no planner; a host gives its session.

import type { HostOperationsSource } from './host-operations.ts';

/** A value bound into a statement's `?`. */
export type PlanValue = string | number | boolean | null;

/** Where a plan runs: a host's own database, one statement at a time, its transaction the host's to begin and end. */
export interface PlanSession {
  /** The database's type, as the planner names it (`DuckDB`): a plan's steps must be of it. */
  readonly databaseType: string;
  /** A statement run with its `?`s bound in order: its rows, each its values by column. */
  run(sql: string, params: readonly PlanValue[]): Promise<readonly (readonly unknown[])[]>;
  /** A transaction begun, committed or rolled back, as this database spells it (the planner's `transaction` host
   *  operation). */
  transaction(step: 'begin' | 'commit' | 'rollback'): Promise<void>;
}

interface Slot { readonly parameter: string; readonly binding: { readonly _type: string; readonly typeHole?: unknown } }
interface Sql { readonly statement: string; readonly slots: readonly Slot[]; readonly regions?: readonly unknown[]; readonly target: unknown }

/** The execution plan's format this runner reads (core/.../executionplan/PlanJson.java VERSION): 18 since 2026-10-29
 *  (the result boundary program: a plan's answer is a `tdsResult` of typed columns, never a text result of a format;
 *  the written-precision fact on a column since S1.4) -- `update`, `check` and `sequence` as version 5 wrote them; a
 *  statement's per-key regions and a key's cell (a dynamic pivot's probe plan) are no write's. */
// 19 since 2026-10-29: the LITERAL slot word retired (track L's L2; core's PlanJson.VERSION is the one owner)
const PLAN_VERSION = 19;
interface Column { readonly name: string; readonly type: string }
type Step =
  | { readonly _type: 'update'; readonly sql: Sql; readonly count?: string | null }
  | { readonly _type: 'check'; readonly sql: Sql }
  | { readonly _type: 'tdsResult'; readonly shape: string; readonly columns: readonly Column[]; readonly sql: Sql }
  | { readonly _type: 'sequence'; readonly steps: readonly Step[] }
  | { readonly _type: string };

/**
 * A host's session over its database (`run`, one statement at a time, held for the plan: duckdb.ts exclusive), its
 * transaction's statements the planner's (`transaction` host operations, D5), asked before anything runs.
 */
export async function hostSession(databaseType: string, run: PlanSession['run'], operations: HostOperationsSource): Promise<PlanSession> {
  const steps = ['begin', 'commit', 'rollback'] as const;
  const spelled = await operations.hostOperations(databaseType, steps.map((step) => ({ op: 'transaction' as const, step })));
  const of = new Map(steps.map((s, i) => [s, spelled[i] ?? []]));
  return {
    databaseType, run,
    transaction: async (step) => {
      for (const statement of of.get(step)!) await run(statement, []);
    },
  };
}

/** A plan's refusal, in words: nothing of it ran. */
export class PlanRefusal extends Error {
  override readonly name = 'PlanRefusal';
}

/**
 * A plan run on `session` (`plan`: PlanJson's text): its answer as lite's JSON rows, written here from the typed rows
 * its `tdsResult` answers (one object per row, the plan's column names its keys) -- once its writes have committed, all
 * of them or none (a failure rolls back and is thrown, the database's words in it). `joined`: the caller's transaction
 * is open already (a table made before it is written, one change: an Output's create), so the plan runs inside it and
 * neither begins nor ends it -- as Java's runner joins a caller's.
 */
export async function runPlan(plan: string, session: PlanSession, o: { readonly joined?: boolean } = {}): Promise<string> {
  const read = JSON.parse(plan) as { format?: unknown; version?: unknown; parameters?: unknown; root?: Step };
  if (read.format !== 'legend-lite-plan' || read.version !== PLAN_VERSION) {
    throw new PlanRefusal(`not a legend-lite execution plan of version ${PLAN_VERSION}: ${JSON.stringify({ format: read.format, version: read.version })}`);
  }
  if (!Array.isArray(read.parameters) || read.parameters.length > 0) throw new PlanRefusal('a plan that takes parameters is not run here: its program binds none');
  const root = read.root;
  if (root === undefined) throw new PlanRefusal('a plan with no root');
  const steps = root._type === 'sequence' ? (root as { readonly steps: readonly Step[] }).steps
    : root._type === 'tdsResult' ? [root] : undefined;
  if (steps === undefined) throw new PlanRefusal(`a plan's root is a sequence or a typed result, not a ${root._type}`);
  const sqls = steps.map((s) => stepSql(s));
  const results = steps.filter((s) => s._type === 'tdsResult');
  if (results.length !== 1) throw new PlanRefusal(`a plan answers with one typed result, not ${results.length}`);
  // one database, as it is: every step's target the same, of this session's type, set up by nothing first
  const [first] = sqls;
  const target = JSON.stringify(first!.target);
  if (sqls.some((s) => JSON.stringify(s.target) !== target)) throw new PlanRefusal("a plan's steps run on one database: these name several");
  const t = first!.target as { database?: { _type?: string; connection?: { databaseType?: string }; databaseType?: string };
    servers?: { _type?: string }; session?: readonly string[]; setup?: readonly unknown[] };
  const type = t.database?.connection?.databaseType ?? t.database?.databaseType;
  if (type !== session.databaseType) throw new PlanRefusal(`a plan of a ${String(type)} database is not run on this ${session.databaseType} one`);
  if (t.servers?._type !== 'every') throw new PlanRefusal("a plan for some servers' versions alone is not run here");
  if ((t.setup ?? []).length > 0) throw new PlanRefusal('a plan that sets its database up first is not run here: it runs on the host\'s own');
  for (const s of sqls) {
    if ((s.regions ?? []).length > 0) throw new PlanRefusal('a statement of per-key regions (a dynamic pivot\'s) is not run here: a write\'s has none');
    for (const slot of s.slots) {
      if (slot.binding._type !== 'one') {
        throw new PlanRefusal(`'${slot.parameter}' binds ${slot.binding._type === 'key' ? "a key's cell" : 'a list'}, which is not run here`);
      }
      if (slot.binding.typeHole !== undefined) throw new PlanRefusal(`'${slot.parameter}' takes its type from its value, which is not run here`);
    }
  }
  const changes = steps.some((s) => s._type === 'update') && o.joined !== true;
  for (const statement of t.session ?? []) await session.run(statement, []);
  const values = new Map<string, PlanValue>();
  const bound = (s: Sql): PlanValue[] => s.slots.map((slot) => {
    if (!values.has(slot.parameter)) throw new PlanRefusal(`'${slot.parameter}' is bound before any step gives it`);
    return values.get(slot.parameter)!;
  });
  let text: string | undefined;
  if (changes) await session.transaction('begin');
  try {
    for (const [i, step] of steps.entries()) {
      const sql = sqls[i]!;
      const rows = await session.run(sql.statement, bound(sql));
      if (step._type === 'update') {
        // the database's count, a value it produced, bound where a later step reads it
        const count = (step as { readonly count?: string | null }).count;
        if (count !== undefined && count !== null) values.set(count, countOf(rows));
      } else if (step._type === 'tdsResult') {
        text = rowsOf((step as { readonly columns: readonly Column[] }).columns, rows);
      }
    }
    if (changes) await session.transaction('commit');
  } catch (e) {
    if (changes) await session.transaction('rollback').catch(() => {});
    throw e;
  }
  return text!;
}

/** A step's SQL; refused by name for a step a host's program does not plan. */
function stepSql(s: Step): Sql {
  if (s._type === 'update' || s._type === 'check' || s._type === 'tdsResult') return (s as { readonly sql: Sql }).sql;
  throw new PlanRefusal(s._type === 'sequence' ? 'a sequence inside a sequence' : `a plan's step is an update, a check or a typed result, not a ${s._type}`);
}

/** The count a statement that changes the database answers: its one row's first value. */
function countOf(rows: readonly (readonly unknown[])[]): number {
  const v = rows[0]?.[0];
  const n = typeof v === 'bigint' ? Number(v) : typeof v === 'number' ? v : typeof v === 'string' ? Number(v) : NaN;
  if (!Number.isSafeInteger(n)) throw new Error(`the database answered a change with no count: ${JSON.stringify(rows[0] ?? null)}`);
  return n;
}

/** A typed result's rows as lite's JSON rows (`[{"value":2}]`: what the planner's JsonRowsWriter writes for the same
 *  rows), each cell as the host's driver read it: an integer (a bigint within a safe integer, a number), a text, a
 *  boolean, a null -- a write's answer is its count; any other cell (a date, a decimal, a float, a structure) is the
 *  planner's to spell by the plan's column facts (executeAnswer), not this runner's: refused by name, never guessed. */
function rowsOf(columns: readonly Column[], rows: readonly (readonly unknown[])[]): string {
  const cell = (c: Column, v: unknown): string => {
    if (v === null || v === undefined) return 'null';
    if (typeof v === 'boolean') return String(v);
    if (typeof v === 'string') return JSON.stringify(v);
    if (typeof v === 'bigint' && Number.isSafeInteger(Number(v))) return String(v);
    if (typeof v === 'number' && Number.isSafeInteger(v)) return String(v);
    throw new PlanRefusal(`the host's runner writes an integer, a text or a boolean cell, and '${c.name}' (${c.type}) holds ${typeof v}: the planner writes it (executeAnswer)`);
  };
  return `[${rows.map((r) => `{${columns.map((c, i) => `${JSON.stringify(c.name)}:${cell(c, r[i])}`).join(',')}}`).join(',')}]`;
}
