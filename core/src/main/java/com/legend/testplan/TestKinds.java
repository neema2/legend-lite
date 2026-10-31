// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;

import com.legend.TypedQuery;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.planner.ProgramKinds;
import com.legend.planner.ProgramState;
import com.legend.planner.QueryDoor;

/**
 * A TEST'S KINDS ON THE PROGRAM ROAD (the test-is-the-product plan's M3, brief B12): the statements of a test's program
 * that hold a kind the query door refuses ({@link QueryDoor.TestOnly}, found by the door's own scan), planned by this
 * library. Each kind is planned from the sub-step its brief names; until then it is refused by name, the step that
 * plans it said: effects at M3d, seeds at M3e, the eager run at M3f, asserts at M6 (the verdict record), assertError
 * at M9 (T10).
 */
final class TestKinds implements ProgramKinds {

    private final QueryDoor.Scan scan;

    TestKinds(TypedQuery body, String runtime) {
        this.scan = new QueryDoor.Scan(body, runtime);
    }

    @Override
    public boolean plan(ProgramState program, int index, TypedSpec statement, boolean last) {
        QueryDoor.TestOnly kind = scan.of(statement);
        if (kind == null) {
            return false;
        }
        String step = switch (kind) {
            case EFFECT -> "M3d";
            case INLINE_SEEDS, RUNTIME_SEEDS -> "M3e";
            case LET_EXECUTE -> "M3f";
            case ASSERT -> "M6, the verdict record";
            case ASSERT_ERROR -> "M9, T10";
        };
        throw new com.legend.error.NotImplementedException(kind.what() + " on the test door's program road: not"
                + " planned yet (the test-is-the-product plan's " + step + ")");
    }
}
