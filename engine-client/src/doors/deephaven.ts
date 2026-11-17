// THE DOOR TO A DEEPHAVEN SERVER (docs/DEEPHAVEN_RUN_SIDE_2026_11_16.md §8; main's app rule, 2026-11-17: an app
// reaches the run side through its doors alone, //tools/deps:app_runs_test). The engine is the run side's and takes no
// planner (deephaven-engine.ts): an app asks the planner for a plan and hands the plan to the engine. Two things an app
// needs here: the server signed in to, its live reader on the server's own library (deephaven-api.ts, this door's
// vendor load); and the check on connect -- what the server must hold asked of the planner, the answer data, then each
// table read in a run and checked as its store declares it (deephaven-held.ts, DH16). An app names the engine's type
// as this door hands it (`Awaited<ReturnType<typeof openDeephaven>>`), as main's apps name the tab DuckDB's.

import { DeephavenEngine, type DeephavenEngineOptions } from '../deephaven-engine.ts';
import { heldTablesOf } from '../deephaven-held.ts';
import type { DeephavenServer } from '../deephaven.ts';
import { deephavenApi } from './deephaven-api.ts';

/** The Deephaven server at `server.url`, signed in to: its live reader on the library the server serves. */
export function openDeephaven(server: DeephavenServer, options: Omit<DeephavenEngineOptions, 'live'> = {}): Promise<DeephavenEngine> {
  return DeephavenEngine.open(server, { ...options, live: deephavenApi(server.url) });
}

/**
 * THE CHECK ON CONNECT (docs/DEPLOYMENT_DESIGN_2026_11_14.md §9, step 3): `holds`, the planner's answer to what the
 * server must hold (its `deephaven` request, op `holds`), asked first and outside any run; then every table it names read
 * on `engine` and checked, or the whole refused naming each table missing or held otherwise (DeephavenNotHeld).
 */
export async function checkOnConnect(engine: DeephavenEngine, holds: () => Promise<string>): Promise<void> {
  await engine.holds(heldTablesOf(await holds()));
}
