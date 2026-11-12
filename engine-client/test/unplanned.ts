// A TEST's own statement, typed as a plan's (planned-sql.ts, the total wall's C3, 2026-11-12).
//
// A host sends only what a plan says: the engine and the host runner take a PlannedSql, and only the plan readers make
// one. A test is not a host: it makes its fixtures' tables, asks its own questions of the database it set up, and stands
// in for a planner with a stub. Those statements are the test's, and say so here, by name, at each place one is sent.
//
// Test code only: no product source (src/, a demo page) imports this file, and the guard
// (tools/guards/PlannedSqlTest.java) holds it so.

import type { PlannedSql } from '../src/planned-sql.ts';

/** A statement the test wrote itself, sent where a plan's statement goes. */
export function unplanned(text: string): PlannedSql {
  return text as PlannedSql;
}

/** Several of them. */
export function allUnplanned(texts: readonly string[]): PlannedSql[] {
  return texts.map(unplanned);
}
