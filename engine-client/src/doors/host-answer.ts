// THE DOOR TO A HOST'S PLAN-AND-RUN ANSWER (the C5-T last round's T1, 2026-11-15): a host that plans and runs in one
// step -- Python's plan-and-run, which DataCube's python-host.ts calls as the tab calls its server's execute -- answers
// the database's rows as Arrow IPC, the planner's answer in their schema's metadata. The app reads them here, through the
// run side's readers (arrow-ipc.ts, duckdb.ts), and reaches no run module itself.

import { arrowIpcAnswer } from '../arrow-ipc.ts';
import { toRawTable } from '../duckdb.ts';
import type { RawTable } from '../query-engine.ts';

/** The answer's Arrow IPC stream read: what its schema's metadata says (the plan, the receipt), and its rows. */
export function hostAnswer(stream: Uint8Array): { readonly metadata: ReadonlyMap<string, string>; rows(epoch: number, elapsedMs: number): RawTable } {
  const { table, metadata } = arrowIpcAnswer(stream);
  return { metadata, rows: (epoch, elapsedMs) => toRawTable(table, epoch, elapsedMs) };
}
