// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.server;

import com.legend.exec.PlanSessions;
import com.legend.exec.ProductRun;
import com.legend.exec.SessionSource;
import com.legend.executionplan.ExecutionPlan;
import com.legend.result.Output;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * AN ENGINE ANSWER THE EDGE HAS NO FORM FOR IS REFUSED BEFORE THE FIRST STATEMENT (the P2 checkpoint audit's S8,
 * 2026-11-16): JSON or Arrow is the caller's choice of output at run time, which the plan cannot know, so the edge's
 * receiver refuses an answer it cannot write (a value under pure/v1's JSON or Arrow) when the run makes its writer from
 * the plan and the output -- before any session opens or any statement is sent. Every session here refuses to open:
 * the answer is the receiver's refusal, never a session's.
 */
class EngineAnswerRefusedBeforeTheRunTest {

    private static final String NO_SESSION = "refused before the first statement: no session is opened";

    private static ExecutionPlan valuePlan() {
        String model = """
                ###Relational
                Database s::DB ( Table T ( ID INTEGER PRIMARY KEY ) )
                ###Connection
                RelationalDatabaseConnection s::Conn
                {
                    store: s::DB;
                    type: DuckDB;
                    specification: LocalH2 { testDataSetupCSV: 'default\\nT\\nID\\n1\\n'; };
                    auth: DefaultH2;
                }
                ###Runtime
                Runtime s::RT { mappings: []; connections: [ s::DB: [ c1: s::Conn ] ]; }
                """;
        return com.legend.Compiler.query(com.legend.Compiler.compileModel(model),
                "|#>{s::DB.T}#->map(r|$r.ID)->toOne()").executionPlan("s::RT");
    }

    private static final com.legend.edge.PureV1Writer.Facts FACTS = new com.legend.edge.PureV1Writer.Facts(Map.of(),
            Map.of(), "__|__", "a value's answer: refused by this test's facts");

    @Test
    void aValueUnderTheEnginesJsonOrArrowIsRefusedBeforeAnySessionOpens() {
        ExecutionPlan plan = valuePlan();
        assertEquals(ExecutionPlan.TdsResult.Shape.VALUE, plan.answerShape());
        for (Output output : java.util.List.of(
                new Output.Rows(com.legend.edge.EngineAnswers.engine(new java.io.StringWriter(), FACTS)),
                new Output.Rows(com.legend.edge.EngineAnswers.arrow(new java.io.ByteArrayOutputStream(), FACTS)))) {
            RuntimeException refused = assertThrows(RuntimeException.class, () -> ProductRun.run(plan,
                    new SessionSource.None(NO_SESSION), PlanSessions.refusing(NO_SESSION), ProductRun.Establish.AS_GIVEN,
                    output, 100, java.util.UUID.randomUUID()));
            assertTrue(refused instanceof com.legend.error.NotImplementedException
                    && refused.getMessage().contains("refused by this test's facts"), String.valueOf(refused));
        }
    }
}
