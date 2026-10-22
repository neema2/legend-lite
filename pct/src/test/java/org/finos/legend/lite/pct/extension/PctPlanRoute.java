// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package org.finos.legend.lite.pct.extension;

import com.legend.Compiler;
import com.legend.TypedQuery;
import com.legend.exec.ExecutionResult;
import com.legend.exec.PlanRunner;
import com.legend.executionplan.ExecutionPlan;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Map;
import java.util.Set;

/**
 * Channel A through the plan path (docs/MONGODB_PLAN_2026_10_16.md §6, 2026-10-17): the case's expression and its
 * injected model planned by the one compiler to Pure's TDS text ({@link TypedQuery.Output#TDS_TEXT}, under the PCT's
 * feature flags) and run by the plan runner on the session the lane's connector opens. ONE representation crosses:
 * a relation as its TDS text, a value as the one-column relation {@code value}, which the adapter unwraps in Pure by
 * the function's own return type. The lane whose database has no JDBC session ({@link PctBackend#throughThePlan})
 * runs here; the JDBC lanes keep the legacy route until the callers switch (the database owner's step 4).
 */
final class PctPlanRoute {

    private PctPlanRoute() {
    }

    static ExecutionResult execute(String model, String expression) throws IOException {
        // the plan for the server this lane's sessions are (H2's dialect spells by the version; step 9, 2026-10-19)
        ExecutionPlan plan = Compiler.query(Compiler.compileModel(PctBackend.withStorelessRuntime(model)), expression)
                .executionPlan(com.legend.test.StorelessRuntime.RUNTIME, TypedQuery.Output.TDS_TEXT,
                        Set.of(com.legend.platform.Feature.CORRECT_SQL_SUBSTRING_INDEXING), PctBackend.serverVersion());
        StringWriter out = new StringWriter();
        // the plan's own session for an in-process lane, the harness's for a server lane (PctBackend.planSessions)
        PlanRunner.run(plan, Map.of(), PctBackend.planSessions(), out);
        return new ExecutionResult.TdsText(out.toString(), com.legend.compiler.element.type.Type.Primitive.STRING);
    }
}
