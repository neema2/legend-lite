// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.base.RunScope;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The runtime wall ({@link RunScope}): planning outside a run is free; inside one it is refused by name, the refusal
 *  naming the callers that asked (read off a throwable's trace since 2026-11-05, which the WebAssembly planner's class
 *  library has); runs nest, and the wall lifts when the outermost closes. */
class RunScopeTest {

    @Test
    void planningOutsideARunIsFree() {
        assertFalse(RunScope.running());
        RunScope.planning("lower a spec");
    }

    @Test
    void planningInsideARunIsRefusedByNameWithItsCallers() {
        try (var run = RunScope.enter()) {
            assertTrue(RunScope.running());
            RunScope.Refused refused = assertThrows(RunScope.Refused.class, () -> RunScope.planning("lower a spec"));
            String message = refused.getMessage();
            assertTrue(message.startsWith("the runtime wall: lower a spec while a plan runs (lower a spec <- "), message);
            assertTrue(message.contains("RunScopeTest."), message);
        }
        assertFalse(RunScope.running());
        RunScope.planning("lower a spec");
    }

    @Test
    void runsNest() {
        try (var outer = RunScope.enter()) {
            try (var inner = RunScope.enter()) {
                assertThrows(RunScope.Refused.class, () -> RunScope.planning("render a statement"));
            }
            assertTrue(RunScope.running());
            assertThrows(RunScope.Refused.class, () -> RunScope.planning("render a statement"));
        }
        assertEquals(false, RunScope.running());
    }
}
