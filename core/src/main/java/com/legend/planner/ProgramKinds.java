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
 */
@FunctionalInterface
public interface ProgramKinds {

    /** Plans {@code statement}, the program's statement at {@code index} ({@code last} whether it is the last), into
     *  {@code program} and answers true when it holds a kind the library plans; false leaves it to the planner. */
    boolean plan(ProgramState program, int index, TypedSpec statement, boolean last);
}
