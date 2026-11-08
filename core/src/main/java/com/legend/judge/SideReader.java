// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.judge;

import com.legend.exec.PlanRunner;
import com.legend.testexecutionplan.TestPlan;

import java.io.IOException;

/** A side of a test body's plan read on demand -- a verdict's caller's message, where the verdict fails -- through the
 *  run's open program (the plan-as-data design's step 3, 2026-11-08: the test plan's sides are no exec type, so the
 *  reader is the judges'). */
@FunctionalInterface
public interface SideReader {
    PlanRunner.Rows read(TestPlan.Side side) throws IOException;
}
