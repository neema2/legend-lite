// THE PLANNER'S SIDE OF THE RUN FRAME (2026-11-15, the C5-T final audit's MUST 1). Each of the planner's entries asks
// here first (planning), and one asked inside a run of its own flow is refused by name: plan only plans, exec only
// execs (G4; Java's RunScope). This module is a RECORD -- the planner and the server layers reach it -- so it holds no
// way to OPEN a run: that is run-frame.ts's, in //engine-client:engine_api, which no planner or server layer reaches
// (tools/deps/ts-layers.txt). It reads the run a flow is in through the reader run-frame.ts installs as it loads, once;
// a second install is refused, so no other module can stand a blind reader in its place. Where no reader is installed,
// run-frame.ts was never loaded, so no run can be open: nothing is refused, and nothing could be.

/** The run this flow is inside, by its name, as run-frame.ts reads it; undefined outside any. */
let runOf: (() => string | undefined) | undefined;

/** run-frame.ts's reader of the run a flow is in: installed as it loads, once (a second install is refused). */
export function readRunsWith(reader: () => string | undefined): void {
  if (runOf !== undefined) {
    throw new Error('the run frame reader is installed once');
  }
  runOf = reader;
}

/** The planner protocol's kinds that plan nothing, let through inside a run: a held model let go, which a page's
 *  teardown or a planner's release may do at any point, a run's included -- Python's `_NOT_PLANNING` holds
 *  `lite_release_model` so (the final audit of 9af16fbe58's T6, 2026-11-14: until then a release inside a run was
 *  refused, LitePlanner's let-go swallowed the refusal, and the model stayed held). */
export const NOT_PLANNING: ReadonlySet<string> = new Set(['release']);

/** A planner entry, asked for `kind` (the planner protocol's kind of request): refused inside a run of its own flow,
 *  unless it plans nothing (NOT_PLANNING). */
export function planning(kind: string): void {
  if (NOT_PLANNING.has(kind)) return;
  const run = runOf?.();
  if (run !== undefined) {
    throw new Error(`the planner was asked '${kind}' inside a run (${run}): a run sends its plan's statements and plans`
      + ' nothing (G4)');
  }
}
