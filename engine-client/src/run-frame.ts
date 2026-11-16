// THE RUN FRAME (the next PR's item 2, "separation made foolproof": the standing witness, 2026-11-13; Java's is
// RunScope). While a plan's statements run, the flow that runs them asks the planner nothing: plan only plans, exec
// only execs (G4). A run is marked around the steps that send a plan's statements (inRun); each of the planner's
// entries asks first (plan-guard.ts planning, a record), and one asked inside a run of its own flow is refused by name.
// This module OPENS a run, so it is //engine-client:engine_api's, which no planner or server layer reaches (2026-11-15,
// the C5-T final audit's MUST 1).
//
// The frame is the FLOW's, not the process's: two requests in flight at once -- an await between them -- each have
// their own, so one flow planning while another flow's plan runs is no crossing (Java's RunScope is per thread for the
// same reason). Under Node the flow is AsyncLocalStorage's, reached through process.getBuiltinModule (no import a
// browser bundle would carry). Where no async context exists -- a browser tab, the headless V8 -- the frame is not
// kept and the witness sees nothing there: a gap, named (frameKept).

import { readRunsWith } from './plan-guard.ts';

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

// the planner's check reads the run a flow is in through this reader, installed here once (plan-guard.ts, a record the
// planner reaches; this module, which opens a run, is //engine-client:engine_api's, which no planner or server reaches:
// the C5-T final audit's MUST 1, 2026-11-15)
readRunsWith(() => frames?.getStore());

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

/**
 * A statement about to be sent (`where`: the choke point sending it -- DuckDB's connection, the warehouse's submit):
 * refused outside a run, where the host keeps a flow's frame (Node). Every send is a run's -- a door's or the plan
 * runner's, inRun -- so a send outside one is a statement no run asked for (C5-T, 2026-11-15: every send on Node framed;
 * a browser and the headless V8 keep no frame, and the layers' rule holds the line there, //tools/deps:layers_alone).
 */
export function sending(where: string): void {
  if (frames !== null && frames.getStore() === undefined) {
    // in few words: the grid's startup script carries this text, within its budget (2026-11-15, the C5-T re-check's
    // MUST 1: Windows measured the grid 14 bytes over; the tail naming a door and the plan runner cost 31 gzipped)
    throw new Error(`${where} was sent a statement outside a run: a statement is sent by a run (inRun, G4)`);
  }
}
