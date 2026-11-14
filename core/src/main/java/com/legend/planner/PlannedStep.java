// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;


import com.legend.executionplan.ExecutionPlan;

import java.util.List;
import java.util.Objects;

/**
 * A STEP OF A PROGRAM AS THE PLANNER HOLDS IT (docs/PLAN_AS_DATA_2026_11_07.md 2.1, the design's step 3, 2026-11-08):
 * a product node, or a step the program's extension planned ({@link ProgramKinds}: a test's verdict group or branch),
 * which the planner knows only by the extension's handle and the statements it may read. The product's plan holds
 * product nodes alone; the test door's caller turns each extension step back into its own (the test plan's).
 */
public sealed interface PlannedStep permits PlannedStep.Product, PlannedStep.FrameRun, PlannedStep.Extension {

    /** A product node. */
    record Product(ExecutionPlan.Node node) implements PlannedStep {
        public Product {
            Objects.requireNonNull(node, "node");
        }
    }

    /** A frame let's eager run (M9): a product check the test door sends under its own statement origin (LET). Marked
     *  where it is planned, so its place needs no reckoning after the steps placed ahead of the program's first (the
     *  second audit's S7, 2026-11-11: it was tracked by its place, shifted by their count). The product's plan holds
     *  none. */
    record FrameRun(ExecutionPlan.Check check) implements PlannedStep {
        public FrameRun {
            Objects.requireNonNull(check, "check");
        }
    }

    /** The extension's step {@code handle} (its own numbering), reading {@code declared} -- typed or keyed results --
     *  which the planner reads for what the program must hold (the stamps bound, the system store seeded). */
    record Extension(int handle, List<ExecutionPlan.Node> declared) implements PlannedStep {
        public Extension {
            declared = List.copyOf(declared);
        }
    }
}
