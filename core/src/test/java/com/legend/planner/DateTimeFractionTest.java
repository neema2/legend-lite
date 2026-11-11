// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.Compiler;
import com.legend.database.Databases;
import com.legend.model.ConnectionDefinition.DatabaseType;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A date-time with more fraction digits than its target keeps is REFUSED, never cut and never sent for the database to
 * round (S181, the user's ruling of 2026-11-10, A7). A value known at planning -- a literal in the query, a cell of a
 * connection's test data, a cell of a project's Data -- is refused by the planner by name: the value, its digits and
 * the digits the target keeps. A run-time parameter is the database's to check: the plan wraps its placeholder in a raise
 * of the product's own error where the slot keeps fewer than nine digits, and the runner sends every digit. Zeros past
 * the kept digits are no digit of the value. Until A7 the runner cut a parameter's text to the slot's digits, Postgres,
 * BigQuery and ClickHouse cut a literal's, and MariaDB, Databricks and every column sent the digits to be rounded.
 */
class DateTimeFractionTest {

    /** A table of a date-time column on {@code type}, the connection's test data {@code cell}'s row. */
    static String model(DatabaseType type, String cell) {
        return """
                ###Relational
                Database s::DB ( Table T ( ID INTEGER PRIMARY KEY, TS TIMESTAMP ) )
                ###Connection
                RelationalDatabaseConnection s::Conn
                {
                    store: s::DB;
                    type: %s;
                    specification: LocalH2 { testDataSetupCSV: 'default\\nT\\nID,TS\\n1,%s\\n'; };
                    auth: DefaultH2;
                }
                ###Runtime
                Runtime s::RT { mappings: [ ]; connections: [ s::DB: [ c1: s::Conn ] ]; }
                """.formatted(type.name(), cell);
    }

    /** {@code query}'s plan on {@code type}, its test data the one cell {@code cell}. */
    static com.legend.executionplan.ExecutionPlan plan(DatabaseType type, String cell, String query) {
        return Compiler.query(Compiler.compileModel(model(type, cell)), query).executionPlan("s::RT");
    }

    /** {@code e}'s message and its causes', one text. */
    static String messages(Throwable e) {
        StringBuilder out = new StringBuilder();
        for (Throwable t = e; t != null; t = t.getCause()) {
            out.append(t.getMessage()).append(" | ");
        }
        return out.toString();
    }

    static String dialect(DatabaseType type) {
        return Databases.dialect(type).getClass().getSimpleName();
    }

    private static final String WHOLE = "2024-01-02 10:00:00";

    /** A query comparing the column with the literal {@code iso}: the literal a timestamp literal of the statement (a
     *  query of the literal alone answers its text, written as the value was). */
    static String compared(String iso) {
        return "|#>{s::DB.T}#->filter(r|$r.TS < %" + iso + ")->select(~[ID])";
    }

    /** The digits of a second each database's date-time LITERAL keeps (each dialect's {@code literalFractionDigits}). */
    static final Map<DatabaseType, Integer> LITERAL_KEEPS = new LinkedHashMap<>();
    static {
        // ClickHouse 9 since round 34 (2026-11-11, the user's rule "refuse if the db cannot handle": its DateTime64(9)
        // keeps them)
        for (DatabaseType t : List.of(DatabaseType.Postgres, DatabaseType.MariaDB,
                DatabaseType.Databricks, DatabaseType.BigQuery)) {
            LITERAL_KEEPS.put(t, 6);
        }
        for (DatabaseType t : List.of(DatabaseType.DuckDB, DatabaseType.H2, DatabaseType.SQLite, DatabaseType.Trino,
                DatabaseType.Snowflake, DatabaseType.ClickHouse)) {
            LITERAL_KEEPS.put(t, 9);
        }
    }

    @Test
    void aLiteralFinerThanItsDatabasesLiteralKeepsIsRefusedAtPlanning_zerosPastThemAreNoDigit() {
        for (var e : LITERAL_KEEPS.entrySet()) {
            DatabaseType type = e.getKey();
            int kept = e.getValue();
            String digits = "123456789".substring(0, kept);
            if (kept < 9) {
                String finer = "2024-01-02T10:00:00." + digits + "7";
                var refused = assertThrows(RuntimeException.class, () -> System.err.println(type + " planned: "
                        + plan(type, WHOLE, compared(finer)).answerSql().statement()), type.name());
                assertTrue(messages(refused).contains("the date-time " + finer + " has " + (kept + 1)
                        + " fraction digits; " + dialect(type) + " keeps " + kept + ": refused, never cut (S181)"),
                        type + ": " + messages(refused));
            }
            // its kept digits, then zeros to nine: planned, the zeros dropped where the literal keeps fewer
            String zeros = "2024-01-02T10:00:00." + digits + "0".repeat(9 - kept);
            String sql = plan(type, WHOLE, compared(zeros)).answerSql().statement();
            assertTrue(sql.contains(digits), type + ": " + sql);
            if (kept < 9) {
                assertFalse(sql.contains(digits + "0"), type + ": the zeros past the kept digits dropped: " + sql);
            }
        }
    }

    /** The digits of a second a TIMESTAMP column keeps on each database (each dialect's {@code timestampColumnDigits}). */
    static final Map<DatabaseType, Integer> COLUMN_KEEPS = new LinkedHashMap<>();
    static {
        COLUMN_KEEPS.put(DatabaseType.H2, 6);
        COLUMN_KEEPS.put(DatabaseType.DuckDB, 6);
        COLUMN_KEEPS.put(DatabaseType.Postgres, 6);
        COLUMN_KEEPS.put(DatabaseType.Trino, 6);
        COLUMN_KEEPS.put(DatabaseType.MariaDB, 6);
        COLUMN_KEEPS.put(DatabaseType.ClickHouse, 0);
        COLUMN_KEEPS.put(DatabaseType.SQLite, 9);
        COLUMN_KEEPS.put(DatabaseType.Snowflake, 9);
    }

    @Test
    void aTestDataCellFinerThanItsColumnKeepsIsRefusedAtPlanning() {
        String query = "|#>{s::DB.T}#->select(~[ID, TS])";
        for (var e : COLUMN_KEEPS.entrySet()) {
            DatabaseType type = e.getKey();
            int kept = e.getValue();
            String digits = "123456789".substring(0, kept);
            if (kept < 9) {
                String finer = WHOLE + "." + digits + "7";
                var refused = assertThrows(RuntimeException.class, () -> plan(type, finer, query));
                assertTrue(messages(refused).contains("the table T's column TS is TIMESTAMP, which keeps " + kept
                        + " fraction digits on " + dialect(type) + ", and its data holds '" + finer + "', of "
                        + (kept + 1) + ": refused, never cut (S181)"), type + ": " + messages(refused));
            }
            // the kept digits and zeros: planned
            plan(type, WHOLE + "." + (kept == 0 ? "" : digits) + "000", query);
        }
    }

    @Test
    void aProjectDataCellFinerThanItsColumnKeepsIsRefusedAtPlanning() {
        String model = """
                ###Relational
                Database s::DB ( Table T ( ID INTEGER PRIMARY KEY, TS TIMESTAMP ) )
                ###Connection
                RelationalDatabaseConnection s::Conn
                {
                    store: s::DB;
                    type: H2;
                    specification: LocalH2 { };
                    auth: DefaultH2;
                }
                ###Runtime
                Runtime s::RT { mappings: [ ]; connections: [ s::DB: [ c1: s::Conn ] ]; }
                ###Data
                Data s::D
                {
                  Relational
                  #{
                    default.T:
                      'ID,TS\\n1,%s\\n';
                  }#
                }
                """;
        String query = "|#>{s::DB.T}#->select(~[ID, TS])";
        var refused = assertThrows(RuntimeException.class, () -> Compiler.query(Compiler.compileModel(
                model.formatted(WHOLE + ".1234567")), query).projectPlan("s::RT"));
        assertTrue(messages(refused).contains("the table T's column TS is TIMESTAMP, which keeps 6 fraction digits on"
                + " H2, and its data holds '" + WHOLE + ".1234567', of 7: refused, never cut (S181)"), messages(refused));
        Compiler.query(Compiler.compileModel(model.formatted(WHOLE + ".123456000")), query).projectPlan("s::RT");
    }

    /** {@code query} on {@code type} with {@code values}: its one cell's text, its fraction's trailing zeros dropped (a
     *  parameter's value carries no written precision, its literal does), or its refusal's messages. */
    static String answer(DatabaseType type, String query, Map<String, ?> values) {
        try {
            return String.valueOf(ZonedDateTimeParameterTest.answer(type, "UTC", query, values))
                    .replaceAll("(\\.\\d*?)0+(?=\\D|$)", "$1").replace(".+", "+");
        } catch (Exception e) {
            return "refused: " + messages(e);
        }
    }

    /** The parameter's guard on each in-process database (Postgres's: PostgresArmTest): a value of no more digits than
     *  the slot keeps answers as its literal does; one of more is refused by the database, by the statement's own
     *  raise, naming the parameter and the digits kept; a Date given a StrictDate passes the guard. */
    @Test
    void aParameterFinerThanItsSlotKeepsIsRefusedByTheDatabase_onDuckDbH2AndSqlite() {
        String dt = "{t: DateTime[1]|$t}";
        for (DatabaseType type : List.of(DatabaseType.DuckDB, DatabaseType.H2, DatabaseType.SQLite)) {
            // six digits, and a value with zeros past them: as the literal
            assertEquals(answer(type, "|%2024-01-02T10:00:00.123456", Map.of()),
                    answer(type, dt, Map.of("t", LocalDateTime.parse("2024-01-02T10:00:00.123456"))), type.name());
            assertEquals(answer(type, "|%2024-01-02T23:59:59.999999", Map.of()),
                    answer(type, dt, Map.of("t", LocalDateTime.parse("2024-01-02T23:59:59.999999"))), type.name());
            // a Date parameter given a StrictDate: as its literal (the guard reads no digit of a date)
            assertEquals(answer(type, "{d: Date[1]|$d}", Map.of("d", LocalDate.parse("2024-01-02"))),
                    answer(type, "{d: Date[1]|$d}", Map.of("d", LocalDate.parse("2024-01-02"))), type.name());
            String nanos = answer(type, dt, Map.of("t", LocalDateTime.parse("2024-01-02T23:59:59.999999999")));
            if (type == DatabaseType.DuckDB) {
                // DuckDB's slot is its microsecond TIMESTAMP: refused, never cut to .999999
                assertTrue(nanos.contains("the DateTime parameter 't' has more fraction digits than DuckDb keeps (6):"
                        + " refused, never cut (S181)"), nanos);
                String date = answer(type, "{d: Date[1]|$d}", Map.of("d",
                        LocalDateTime.parse("2024-01-02T10:00:00.0000001")));
                assertTrue(date.contains("the Date parameter 'd' has more fraction digits than DuckDb keeps (6)"),
                        date);
            } else {
                // H2's TIMESTAMP(9) and SQLite's text keep nine
                assertEquals(answer(type, "|%2024-01-02T23:59:59.999999999", Map.of()), nanos, type.name());
                assertTrue(nanos.contains("23:59:59.999999999"), type + ": " + nanos);
            }
        }
    }

    /** A list of date-times is bound as the driver's timestamps, which no statement's raise reads (the A delta audit's 2,
     *  2026-11-11: DuckDB's cut an element of nine digits to six in silence): the plan says the digits its database keeps
     *  of an element, and the runner refuses a finer one by name; one of the kept digits answers. H2 keeps nine. */
    @Test
    void aListsElementFinerThanItsDatabaseKeepsIsRefusedByName() throws Exception {
        String in = "{ts: DateTime[*]|#>{s::DB.T}#->filter(r|$r.TS->in($ts))->select(~[ID])}";
        String cell = WHOLE + ".123456";
        assertEquals(List.of(1), ids(DatabaseType.DuckDB, cell, in,
                List.of(LocalDateTime.parse("2024-01-02T10:00:00.123456"))));
        var refused = assertThrows(com.legend.error.DataError.class, () -> ids(DatabaseType.DuckDB, cell, in,
                List.of(LocalDateTime.parse("2024-01-02T10:00:00.123456789"))));
        assertTrue(messages(refused).contains("the list parameter 'ts' holds 2024-01-02T10:00:00.123456789, which has"
                + " more fraction digits than its database keeps (6): refused, never cut (S181)"), messages(refused));
        assertEquals(6, ((com.legend.executionplan.ExecutionPlan.Binding.Array) plan(DatabaseType.DuckDB, cell, in)
                .answerSql().slots().get(0).binding()).fraction());
        assertEquals(null, ((com.legend.executionplan.ExecutionPlan.Binding.Array) plan(DatabaseType.H2, cell, in)
                .answerSql().slots().get(0).binding()).fraction(), "H2 keeps nine of an element");
        assertEquals(List.of(1), ids(DatabaseType.H2, cell, in,
                List.of(LocalDateTime.parse("2024-01-02T10:00:00.123456"))));
    }

    /** The IDs {@code query} answers on {@code type}, the table's one row's TS {@code cell}, its list parameter
     *  {@code ts}. */
    private static List<Object> ids(DatabaseType type, String cell, String query, List<LocalDateTime> ts)
            throws Exception {
        com.legend.executionplan.ExecutionPlan plan = plan(type, cell, query);
        List<Object> out = new java.util.ArrayList<>();
        try (java.sql.Connection c = com.legend.testbackend.TestBackends.of(type).open()) {
            com.legend.exec.PlanRunner.stream(plan, Map.of("ts", ts), com.legend.exec.PlanSessions.acquired(
                    com.legend.testbackend.TestSessions.over(c), plan.answerSql().target()),
                    new com.legend.result.ResultSink() {
                        @Override
                        public void columns(List<com.legend.executionplan.ExecutionPlan.Column> columns) {
                        }

                        @Override
                        public void batch(com.legend.result.Batch batch) {
                            for (int r = 0; r < batch.size(); r++) {
                                out.add(((Number) batch.get(r, 0)).intValue());
                            }
                        }

                        @Override
                        public void end() {
                        }
                    }, 16);
        }
        return out;
    }
}
