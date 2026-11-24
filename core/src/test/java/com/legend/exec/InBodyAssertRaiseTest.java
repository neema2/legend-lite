// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.Compiler;
import com.legend.error.ErrorKind;
import com.legend.executionplan.ExecutionPlan;
import com.legend.sql.RaiseKind;
import com.legend.testbackend.StorelessRuntime;
import com.legend.testbackend.TestBackend;
import com.legend.testbackend.TestBackends;
import com.legend.testcases.PlanCases;
import org.junit.jupiter.api.Test;

import java.io.StringWriter;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * AN IN-BODY ASSERT RAISES ITS KIND (item 3's S6, 2026-11-23; the user's Q-C = C1 and Q-S6): a guard in a lambda body
 * whose caller gives no message raises its assert kind -- {@code assert}, {@code assertTrue} and {@code fail()} the
 * condition's ({@code ASSERT_TRUE}), {@code assertFalse} its own, {@code assertNotEmpty} its own, and the guards whose
 * catalog sentence prints their values ({@code assertEquals}, {@code assertNotEquals}, {@code assertEmpty}) the
 * condition's, since a raise carries facts fixed at planning and never a value the database computed. The plan holds a
 * site of the kind and no sentence; every in-process database raises it, worded by the one catalog (the assert's
 * sentence), at the assert's own place. A caller's message stays the caller's text ({@code USER_FAIL}).
 */
class InBodyAssertRaiseTest {

    /** A guard, the kind it raises, and its catalog sentence. */
    private record Case(String guard, RaiseKind kind, String sentence) {
    }

    private static final String TRUE = "Assert failed";

    private static final List<Case> CASES = List.of(
            new Case("assert($r.x == 0)", RaiseKind.ASSERT_TRUE, TRUE),
            new Case("assertFalse($r.x < 0)", RaiseKind.ASSERT_FALSE, "Assert failed"),
            new Case("assertEquals(0, $r.x)", RaiseKind.ASSERT_TRUE, TRUE),
            new Case("assertNotEquals(-1, $r.x)", RaiseKind.ASSERT_TRUE, TRUE),
            new Case("assertEmpty($r.x)", RaiseKind.ASSERT_TRUE, TRUE),
            new Case("assertNotEmpty(if($r.x > 0, |$r.x, |[]))", RaiseKind.ASSERT_NOT_EMPTY,
                    "Expected non-empty collection"),
            new Case("fail()", RaiseKind.ASSERT_TRUE, TRUE));

    private static String query(String guard) {
        return "|#TDS\n  x:Integer\n  -1\n#->extend(~y: r|if($r.x > 0, |$r.x, {|" + guard + "; 0;}))->from("
                + StorelessRuntime.RUNTIME + ")";
    }

    @Test
    void eachGuardRaisesItsKind_aSiteWithNoSentenceInThePlan() throws Exception {
        for (TestBackend backend : TestBackends.inProcess()) {
            var ctx = Compiler.compileModel(backend.withStorelessRuntime(""));
            for (Case c : CASES) {
                String where = backend.name() + " " + c.guard();
                ExecutionPlan plan = Compiler.query(ctx, query(c.guard())).executionPlan(StorelessRuntime.RUNTIME);
                assertTrue(plan.answerSql().sites().stream().anyMatch(s -> s.kind() == c.kind()
                        && s.operands().isEmpty()), where + ": a site of " + c.kind() + " with no operand: "
                        + plan.answerSql().sites());
                String statement = plan.answerSql().statement();
                assertFalse(statement.contains(c.sentence()) || statement.contains("Assert failed"),
                        where + ": no sentence in the statement: " + statement);
                com.legend.error.DataError raised = raised(backend, plan);
                assertEquals(ErrorKind.valueOf(c.kind().name()), raised.kind(), where);
                assertEquals(c.sentence(), raised.getMessage(), where);
                if (!c.guard().startsWith("fail")) {
                    // the assert's own place, for assertError's line and column
                    assertNotNull(raised.position(), where + ": the raise's position");
                }
            }
        }
    }

    @Test
    void aCallersMessageStaysTheCallersText() throws Exception {
        for (TestBackend backend : TestBackends.inProcess()) {
            var ctx = Compiler.compileModel(backend.withStorelessRuntime(""));
            for (String guard : List.of("assert($r.x == 0, 'mine')", "assertFalse($r.x < 0, 'mine')",
                    "assertNotEmpty(if($r.x > 0, |$r.x, |[]), 'mine')", "fail('mine')")) {
                ExecutionPlan plan = Compiler.query(ctx, query(guard)).executionPlan(StorelessRuntime.RUNTIME);
                com.legend.error.DataError raised = raised(backend, plan);
                assertNull(raised.kind(), backend.name() + " " + guard + ": the caller's message, no kind");
                assertEquals("mine", raised.getMessage(), backend.name() + " " + guard);
            }
        }
    }

    /** A body whose asserts are in-body guards reaches an assert, with or without the caller's message (the audit of
     *  e13a9c6d: the guard's raise had stopped counting, so such a body read as calling no verdict function). */
    @Test
    void anInBodyGuardIsAnAssertTheBodyReaches() {
        var ctx = Compiler.compileModel(TestBackends.platform().withStorelessRuntime(""));
        for (String body : List.of("|[1]->map({x|assert($x > 0); $x;})", "|[1]->map({x|assertEquals(1, $x); $x;})",
                "|[1]->map({x|assert($x > 0, 'm'); $x;})")) {
            assertTrue(com.legend.TestExecution.facts(Compiler.query(ctx, body)).verdicts(), body);
        }
    }

    private static com.legend.error.DataError raised(TestBackend backend, ExecutionPlan plan) {
        return assertThrows(com.legend.error.DataError.class, () -> {
            try (java.sql.Connection c = backend.open()) {
                PlanRunner.stream(plan, Map.of(), PlanSessions.of(com.legend.testbackend.TestSessions.over(c)),
                        PlanCases.Written.JSON.writer(new StringWriter(), plan.answerShape()),
                        java.util.UUID.randomUUID());
            }
        });
    }
}
