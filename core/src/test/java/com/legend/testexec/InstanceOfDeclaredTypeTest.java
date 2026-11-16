// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexec;

import com.legend.Compiler;
import com.legend.ExecuteOptions;
import com.legend.testbackend.StorelessRuntime;
import com.legend.testbackend.TestBackends;
import com.legend.testplan.PlanningOptions;
import org.junit.jupiter.api.Test;

import java.sql.Connection;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The host judge's {@code assertInstanceOf} decides a value's class by the side's DECLARED type, never by its text
 * (E-Java's ledger audit D3, 2026-11-16). A String whose text looks like a class instance's wire form
 * ({@code {"__type": ...}}) is a String, as it is in legend-pure: the host had parsed it and judged the classifier
 * inside it.
 */
class InstanceOfDeclaredTypeTest {

    private static String run(String body, PlanningOptions.JudgeMode mode) {
        try (Connection conn = TestBackends.platform().open()) {
            var ctx = Compiler.compileModel(StorelessRuntime.with("", TestBackends.platform().type()));
            com.legend.TestExecution.runProgram(Compiler.query(ctx, body), StorelessRuntime.RUNTIME,
                    com.legend.exec.SessionSource.given(com.legend.testbackend.TestSessions.over(conn)), null, null,
                    ExecuteOptions.NONE.withJudgeMode(mode));
            return "PASS";
        } catch (Exception | AssertionError e) {
            return e.getClass().getSimpleName() + ": " + e.getMessage();
        }
    }

    @Test
    void aStringThatLooksLikeAnInstanceIsAString() {
        String body = "{| assertInstanceOf('{\"__type\":\"meta::pure::metamodel::type::Any\"}', String);}";
        for (PlanningOptions.JudgeMode mode : PlanningOptions.JudgeMode.values()) {
            assertEquals("PASS", run(body, mode), mode.toString());
        }
    }
}
