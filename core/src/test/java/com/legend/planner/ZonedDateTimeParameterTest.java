// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.Compiler;
import com.legend.TypedQuery;
import com.legend.exec.PlanRunner;
import com.legend.exec.PlanSessions;
import com.legend.executionplan.ExecutionPlan;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.result.Batch;
import com.legend.result.ResultSink;
import com.legend.testbackend.TestBackends;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A date-time parameter on a connection with a time zone answers as its literal does (the correctness audit's S3, A6,
 * 2026-11-10): upstream wraps the placeholder in {@code GMTtoTZ}, and lite's lowering converts it in the database
 * ({@code SqlFn.TIMEZONE}, DuckDB's {@code timezone(zone, timezone('UTC', ?))}). H2 has no {@code timezone} function, so
 * until A6 every such parameter failed there at run; H2 spells the conversion with {@code AT TIME ZONE} now. A Date
 * parameter there is refused by name on every database (upstream's conversion depends on the value's shape; S180,
 * ruled "Keep refusal"). A connection's zone applies inside an {@code execute(...)} frame (outside one, S183's known gap):
 * the statement is lowered here with it, as the frame lowers it, and run by the plan's runner on each database.
 */
class ZonedDateTimeParameterTest {

    /** A one-row relation's computed column of the value {@code %s}. */
    static final String COLUMN = "#>{s::DB.T}#->filter(r|$r.ID == 1)->extend(~v: r|%s)->select(~v)";

    /** The one cell {@code query} answers on {@code type}, its parameters {@code values}, lowered as a connection whose
     *  time zone is {@code zone} lowers it (a frame's: {@code Lowerer.withDbTimeZone}, as {@code PlanMakerTest} lowers
     *  it; null, as planned) -- the plan's answer statement the zoned lowering's, run by the plan's runner. */
    static Object answer(DatabaseType type, @com.legend.base.Nullable String zone, String query, Map<String, ?> values)
            throws Exception {
        TypedQuery typed = Compiler.query(Compiler.compileModel(PlanMakerTest.model(type.name())), query);
        ExecutionPlan plan = typed.executionPlan("s::RT");
        if (zone != null) {
            List<com.legend.sql.SqlExpr.PlanParam> slots = typed.parameters().stream()
                    .map(QueryParameters.Declared::slot).toList();
            com.legend.sql.SqlQuery q = Lowered.lowerer(typed, false, false, slots, java.util.Set.of()).withDbTimeZone(zone)
                    .lower(typed.body());
            ExecutionPlan.TdsResult t = (ExecutionPlan.TdsResult) plan.root();
            plan = ExecutionPlan.reading(plan.parameters(), new ExecutionPlan.TdsResult(t.shape(), t.columns(),
                    PlanMaker.sql(q, com.legend.database.Databases.dialect(type), t.sql().target())));
        }
        List<Object> cells = new ArrayList<>();
        try (Connection c = TestBackends.of(type).open()) {
            PlanRunner.stream(plan, values, PlanSessions.acquired(com.legend.testbackend.TestSessions.over(c),
                    plan.answerSql().target()), new ResultSink() {
                        @Override
                        public void columns(List<ExecutionPlan.Column> columns) {
                        }

                        @Override
                        public void batch(Batch batch) {
                            for (int r = 0; r < batch.size(); r++) {
                                cells.add(batch.get(r, 0));
                            }
                        }

                        @Override
                        public void end() {
                        }
                    }, 16, java.util.UUID.randomUUID());
        }
        assertEquals(1, cells.size(), "one cell: " + cells);
        return cells.get(0);
    }

    /** A date-time's text with its fraction's trailing zeros dropped: the literal and the parameter answer the same
     *  instant, the one at six digits, the other at nine. */
    static String instant(Object cell) {
        return String.valueOf(cell).replaceAll("(\\.\\d*?)0+(?=\\D|$)", "$1").replace(".+", "+").replaceAll("\\.$", "");
    }

    @Test
    void aDateTimeParameterInAZoneAnswersAsItsLiteral_onDuckDbAndH2() throws Exception {
        for (DatabaseType type : List.of(DatabaseType.DuckDB, DatabaseType.H2)) {
            for (String zone : List.of("America/New_York", "Asia/Kolkata", "UTC")) {
                for (String at : List.of("2024-01-02T10:00:00", "2024-07-02T23:59:59.123456", "2024-03-10T06:30:00")) {
                    // each a computed column's value, where the lowering converts a literal (MatchFold.dateLit) and a
                    // parameter (Lowerer.zoned) alike
                    Object literal = answer(type, zone, "|" + COLUMN.formatted("%" + at), Map.of());
                    Object parameter = answer(type, zone, "{t: DateTime[1]|" + COLUMN.formatted("$t") + "}",
                            Map.of("t", LocalDateTime.parse(at)));
                    assertEquals(instant(literal), instant(parameter), type + " in " + zone + " at " + at);
                    if (!zone.equals("UTC")) {
                        // the zone shifts it: the wall time there, not the value as written
                        assertTrue(!instant(parameter).startsWith(at), type + " in " + zone + ": "
                                + parameter);
                    }
                }
            }
        }
    }

    @Test
    void aDateParameterInAZoneIsRefusedByName_onDuckDbAndH2() {
        for (DatabaseType type : List.of(DatabaseType.DuckDB, DatabaseType.H2)) {
            var refused = assertThrows(com.legend.error.NotImplementedException.class,
                    () -> answer(type, "America/New_York", "{d: Date[1]|$d}", Map.of("d", java.time.LocalDate.of(2024,
                            1, 2))));
            assertTrue(String.valueOf(refused.getMessage()).contains("time zone is America/New_York"),
                    refused.getMessage());
        }
    }
}
