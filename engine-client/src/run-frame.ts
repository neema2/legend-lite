// THE RUN FRAME (the next PR's item 2, "separation made foolproof": the standing witness, 2026-11-13; Java's is
// RunScope). While a plan's statements run, the flow that runs them asks the planner nothing: plan only plans, exec
// only execs (G4). A run is marked around the steps that send a plan's statements (inRun); each of the planner's
// entries asks first (planning), and one asked inside a run of its own flow is refused by name.
//
// The frame is the FLOW's, not the process's: two requests in flight at once -- an await between them -- each have
// their own, so one flow planning while another flow's plan runs is no crossing (Java's RunScope is per thread for the
// same reason). Under Node the flow is AsyncLocalStorage's, reached through process.getBuiltinModule (no import a
// browser bundle would carry). Where no async context exists -- a browser tab, the headless V8 -- the frame is not
// kept and the witness sees nothing there: a gap, named (frameKept).

/** AsyncLocalStorage's two calls the frame uses. */
interface Frames {
  run<T>(store: string, work: () => T): T;
  getStore(): string | undefined;
}

const frames: Frames | null = (() => {
  const host = (globalThis as { process?: { getBuiltinModule?: (id: string) => unknown } }).process;
  const hooks = host?.getBuiltinModule?.('node:async_hooks') as { AsyncLocalStorage?: new () => Frames } | undefined;
  return hooks?.AsyncLocalStorage ? new hooks.AsyncLocalStorage() : null;
})();

/** Whether this host keeps a flow's frame at all: false where the witness is blind (a browser, the headless V8). */
export const frameKept: boolean = frames !== null;

/** `work`, run as a run that `what` names: every planner entry it reaches, on its own flow, refused. */
export function inRun<T>(what: string, work: () => Promise<T>): Promise<T> {
  return frames === null ? work() : frames.run(what, work);
}

/** The run this flow is inside, by its name; undefined outside any (or where no frame is kept). */
export function currentRun(): string | undefined {
  return frames?.getStore();
}

/** A planner entry, asked for `kind` (the planner protocol's kind of request): refused inside a run of its own flow. */
export function planning(kind: string): void {
  const run = currentRun();
  if (run !== undefined) {
    throw new Error(`the planner was asked '${kind}' inside a run (${run}): a run sends its plan's statements and plans`
      + ' nothing (G4)');
  }
}
