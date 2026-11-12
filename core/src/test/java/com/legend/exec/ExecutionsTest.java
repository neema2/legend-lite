// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.base.Executions;
import com.legend.base.RunScope;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * A RUN'S EXECUTIONS, as exec reads them (the re-audit's SHOULD-FIX 2, 2026-11-12): a product run is ONE execution, every
 * stamp its id; a run whose plan declares its executions answers each stamp its own id, and refuses by name a stamp it
 * does not declare -- never the run's id in its place (G7), so an empty declared set refuses every stamp. Reverting
 * {@link Executions#of(String)} to answer the run's id for an undeclared stamp turns this red.
 */
class ExecutionsTest {

    private static final UUID RUN = UUID.fromString("00000000-0000-4000-8000-000000000001");
    private static final UUID X = UUID.fromString("00000000-0000-4000-8000-000000000002");
    private static final String STAMP_X = "#stamp.let.x";

    @Test
    void aProductRunIsOneExecution_everyStampItsId() {
        Executions one = Executions.one(RUN);
        assertEquals(RUN, one.of(STAMP_X));
        assertEquals(RUN, one.of("#stamp.anything"));
    }

    @Test
    void aDeclaredStampIsItsOwnExecution() {
        assertEquals(X, Executions.of(RUN, Map.of(STAMP_X, X)).of(STAMP_X));
    }

    @Test
    void anUndeclaredStampIsRefusedByName() {
        IllegalStateException refused = assertThrows(IllegalStateException.class,
                () -> Executions.of(RUN, Map.of(STAMP_X, X)).of("#stamp.let.y"));
        assertEquals("the plan reads the execution '#stamp.let.y', which it does not declare (it declares ["
                + STAMP_X + "]): an execution's id is its door's, one per declared execution", refused.getMessage());
    }

    @Test
    void anEmptyDeclaredSetRefusesEveryStamp() {
        Executions none = Executions.of(RUN, Map.of());
        IllegalStateException refused = assertThrows(IllegalStateException.class, () -> none.of(STAMP_X));
        assertEquals("the plan reads the execution '" + STAMP_X + "', which it does not declare (it declares []): an"
                + " execution's id is its door's, one per declared execution", refused.getMessage());
    }

    /** The run's scope reads them so: a stamp the run's plan did not declare is refused where it is read. */
    @Test
    void theRunsScopeRefusesAnUndeclaredStamp() {
        try (var run = RunScope.enter(Executions.of(RUN, Map.of(STAMP_X, X)))) {
            assertEquals(X, RunScope.execution(STAMP_X));
            assertEquals(RUN, RunScope.execution());
            assertThrows(IllegalStateException.class, () -> RunScope.execution("#stamp.let.y"));
        }
    }
}
