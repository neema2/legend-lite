// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.compiler.spec.typed.TypedSpec;

/**
 * THE PROGRAM ROAD'S EXTENSION POINT FOR A TEST'S KINDS (the test-is-the-product plan's M3, brief B12 items 1 and 3;
 * R17 C7): the planner keeps the program loop and what a product query can use; the statements only a test's program
 * holds ({@link QueryDoor.TestOnly}: effects, seeds, the eager run, asserts) are planned by the test-planning library,
 * which the product's query door, the server's planning and the WebAssembly and native planners never carry. The
 * planner asks it first at each statement of a program that came through the test door; the query door's program
 * asks none (its test-only kinds refused before, QueryDoor).
 *
 * <p>ONE INSTANCE PER PROGRAM: an implementation may hold what it plans across the program's statements (the test
 * library's open script and the seeds it placed), owned by that one program and never shared; the program's planning
 * makes it for the program it plans ({@code TestDoor.plan}). A stateless one (the query door's: none) may be a lambda.
 */
public interface ProgramKinds {

    /** Plans {@code statement}, the program's statement at {@code index} ({@code last} whether it is the last), into
     *  {@code program} and answers true when it holds a kind the library plans; false leaves it to the planner. */
    boolean plan(ProgramState program, int index, TypedSpec statement, boolean last);

    /** The target {@code statement} (the program's at {@code index}, one the planner plans) runs on where it is not
     *  the program's: the graph's system database for a statement that reads the system metamodel store alone, beside
     *  a program whose target is the user's (per-statement routing, the walker-deletion program, 2026-11-03); null for
     *  the program's own. */
    default com.legend.executionplan.ExecutionPlan.@com.legend.base.Nullable Target targetOf(ProgramState program,
            int index,
            TypedSpec statement) {
        return null;
    }

    /** The statements of the steps planned before the program, which the library places among its statements (a
     *  test's asserts, each planned by its judge first): the planner reads them ahead of the loop for what the program
     *  must hold before its first step -- the graph's system store's seed where one of them reads it (2026-11-05,
     *  planning sends nothing). None where the library plans nothing ahead (the query door's). */
    default java.util.List<com.legend.executionplan.ExecutionPlan.Node> plannedAhead() {
        return java.util.List.of();
    }

    /** The {@code data:} tables the steps planned before the program read through (a test's asserts' sides and verdict
     *  statements: B2a, 2026-11-11), which the program's statements read through too -- one table per content for the
     *  whole body -- and whose steps it places ahead of the step that first reads one (the next statement's, or the
     *  extension's step at its place); null where nothing was planned ahead (the program's own, fresh). */
    default DataSources.@com.legend.base.Nullable Tables dataTables() {
        return null;
    }
}
