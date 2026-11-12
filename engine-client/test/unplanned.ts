// A TEST's own statement, typed as a plan's (planned-sql.ts, the total wall's C3, 2026-11-12).
//
// A host sends only what a plan says: the engine and the host runner take a PlannedSql, and only the plan readers make
// one. A test is not a host: it makes its fixtures' tables, asks its own questions of the database it set up, and stands
// in for a planner with a stub. Those statements are the test's, and say so here, by name, at each place one is sent.
//
// Test code only: no product source (src/, a demo page) imports this file, and the guard
// (tools/guards/PlannedSqlTest.java) holds it so.

import type { PlannedSql } from '../src/planned-sql.ts';

/** Every text a test typed as its own, so a host that runs both a test's statements and the product's can tell them
 *  apart (the C final audit's H7, 2026-11-12: the headless lane's database sends a test's own to the lane's test calls,
 *  and every other statement -- the product's -- across the product's bridge, test/host/headless.ts). */
const OWN = new Set<string>();

/** A statement the test wrote itself, sent where a plan's statement goes. */
export function unplanned(text: string): PlannedSql {
  OWN.add(text);
  return text as PlannedSql;
}

/** Whether `text` is one a test typed as its own (`unplanned`): never a statement a planner answered. */
export function isTestsOwn(text: string): boolean {
  return OWN.has(text);
}

/** Several of them. */
export function allUnplanned(texts: readonly string[]): PlannedSql[] {
  return texts.map(unplanned);
}
