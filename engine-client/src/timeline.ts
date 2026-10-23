// THE BROWSER'S PERFORMANCE TIMELINE (docs/DEEPHAVEN_PLAN_2026_10_22.md O1): a step's time as a `dh:<step>` measure --
// the planner's, an engine's (naming a plan's tables, encoding its request, the server's batch, reading its answers, a
// result's rows), a view's paint -- which the browser's own tools show and a test reads. A diagnostic, not a result: a
// runtime with no timeline (the headless runner's: no `performance.measure`) records none, and nothing reads one to
// decide anything.

/** `work`, its time measured as `dh:<step>`. */
export async function timed<T>(step: string, work: () => Promise<T>): Promise<T> {
  const start = performance.now();
  try {
    return await work();
  } finally {
    measured(step, start);
  }
}

/** A `dh:<step>` measure from `start` to now (`timed`), where the runtime keeps a timeline. */
export function measured(step: string, start: number): void {
  if (typeof performance.measure !== 'function') return;
  performance.measure(`dh:${step}`, { start, end: performance.now() });
}
