// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.testing.LaneFloor;

import com.legend.exec.PlanRunner;
import com.legend.exec.PlanSessions;
import com.legend.executionplan.ExecutionPlan;
import com.legend.executionplan.PlanJson;
import com.legend.testbackend.TestBackend;
import org.junit.jupiter.api.Test;

import java.io.StringWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Write plans (docs/WRITES_DESIGN_2026_10_18.md §8, leg 7): a program that writes a store table, planned
 * ({@link TypedQuery#executionPlan}) as the steps of one sequence and run by the model-free runner
 * ({@link PlanRunner}), every parameter a value its statement binds. Its writes run in one transaction of the session (a failing one leaves
 * the tables as they were; a caller's open transaction is joined), its answer the last statement's: a write's count, or
 * a query's text, both written by the database. Each database the writes are measured on runs {@link #plans} in a test
 * named for it (as {@link StoreWritesExecutionTest}'s writes are).
 */
@org.junit.jupiter.api.extension.ExtendWith(LaneFloor.class)
class WritePlansTest {

    static final String MODEL = """
            ###Relational
            Database w::DB (
              Table T (ID INTEGER PRIMARY KEY, NAME VARCHAR(20), AGE INTEGER)
              Table SRC (ID INTEGER, NAME VARCHAR(20), AGE INTEGER)
              Table REV (ID INTEGER, NAME VARCHAR(20), AGE INTEGER)
            )
            ###Connection
            RelationalDatabaseConnection w::Conn {
              store: w::DB;
              %s
            }
            ###Runtime
            Runtime w::RT { mappings: [ ]; connections: [ w::DB: [ c: w::Conn ] ]; }
            """;

    /** A program's plan holds a step for each of its statements and runs where a query's does. */
    @Test
    void aProgramThatWritesIsPlannedAsItsSteps() {
        var ctx = com.legend.Compiler.compileModel(MODEL.formatted(LaneFloor.backend().connectionBody()));
        ExecutionPlan plan = com.legend.Compiler.query(ctx, "{age: Integer[1] |let s = #>{w::DB.SRC}#->filter(r|$r.AGE"
                + " > $age)->meta::legend::lite::stage(); $s->write(#>{w::DB.T}#); $s->select(~[ID]);}")
                .executionPlan("w::RT");
        List<ExecutionPlan.Node> steps = ((ExecutionPlan.Sequence) plan.root()).steps();
        // the staged table created, the write, the answer, the staged table dropped
        assertEquals(4, steps.size(), steps.toString());
        assertTrue(steps.get(0) instanceof ExecutionPlan.Update u && u.sql().statement().contains("legend_stage_s")
                && u.sql().slots().size() == 1, steps.get(0).toString());
        assertTrue(steps.get(1) instanceof ExecutionPlan.Update u && u.count() == null, steps.get(1).toString());
        assertTrue(steps.get(2) instanceof ExecutionPlan.TdsResult, steps.get(2).toString());
        assertTrue(steps.get(3) instanceof ExecutionPlan.Update u && u.sql().statement().contains("legend_stage_s"),
                steps.get(3).toString());
        // the plan reads back from its JSON (the typed SQL tree is not written yet: PlanJson)
        assertEquals(PlanJson.write(plan), PlanJson.write(PlanJson.read(PlanJson.write(plan))));
    }

    /** Every check, on {@code backend}'s own workspace. */
    static void plans(TestBackend backend) throws Exception {
        String model = MODEL.formatted(backend.connectionBody());
        String name = backend.name();
        try (Connection c = backend.openWorkspace()) {
            run(c, "CREATE TABLE T (ID INTEGER PRIMARY KEY, NAME VARCHAR(20), AGE INTEGER)");
            run(c, "CREATE TABLE SRC (ID INTEGER, NAME VARCHAR(20), AGE INTEGER)");
            run(c, "INSERT INTO SRC VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30)");
            // a write of the rows a parameter selects: its count, written by the database
            String write = "{age: Integer[1] |#>{w::DB.SRC}#->filter(r|$r.AGE > $age)->write(#>{w::DB.T}#)}";
            assertEquals(count(model, c, "|2"), answer(model, c, write, Map.of("age", 10L)), name);
            assertEquals(List.of("2|b|20", "3|c|30"), rows(c), name);
            assertTrue(c.getAutoCommit(), name + ": the runner restores the session's autocommit");
            // a failing write is the database's error, and the table is as it was
            assertThrows(RuntimeException.class, () -> answer(model, c, write, Map.of("age", 0L)), name);
            assertEquals(List.of("2|b|20", "3|c|30"), rows(c), name);
            assertTrue(c.getAutoCommit(), name);
            // a program's writes land together: the second's failure takes the first back
            String two = "{id: Integer[1] |#>{w::DB.SRC}#->filter(r|$r.ID == $id)->write(#>{w::DB.T}#);"
                    + " #>{w::DB.SRC}#->write(#>{w::DB.T}#);}";
            assertThrows(RuntimeException.class, () -> answer(model, c, two, Map.of("id", 1L)), name);
            assertEquals(List.of("2|b|20", "3|c|30"), rows(c), name);
            // ... a staging between them included (PR #67's review, 2026-10-22: H2's CREATE TEMPORARY TABLE had
            // committed the first), its table gone
            String staging = "{id: Integer[1] |#>{w::DB.SRC}#->filter(r|$r.ID == $id)->write(#>{w::DB.T}#);"
                    + " let s = #>{w::DB.T}#->meta::legend::lite::stage(); #>{w::DB.SRC}#->write(#>{w::DB.T}#);}";
            assertThrows(RuntimeException.class, () -> answer(model, c, staging, Map.of("id", 1L)), name);
            assertEquals(List.of("2|b|20", "3|c|30"), rows(c), name);
            assertThrows(Exception.class, () -> run(c, "SELECT * FROM legend_stage_s"), name);
            assertTrue(c.getAutoCommit(), name);
            // update and delete, their values and predicates bound
            assertEquals(count(model, c, "|1"), answer(model, c, "{id: Integer[1], age: Integer[1] |#>{w::DB.T}#"
                    + "->update(r|$r.ID == $id, ~[AGE: r|$age])}", Map.of("id", 2L, "age", 99L)), name);
            assertEquals(List.of("2|b|99", "3|c|30"), rows(c), name);
            assertEquals(count(model, c, "|1"), answer(model, c, "{n: String[1] |#>{w::DB.T}#->delete(r|$r.NAME =="
                    + " $n)}", Map.of("n", "c")), name);
            assertEquals(List.of("2|b|99"), rows(c), name);
            // merge: the key the table holds updated, the others inserted
            assertEquals(count(model, c, "|3"), answer(model, c, "|#>{w::DB.SRC}#->merge(#>{w::DB.T}#, ~[ID])",
                    Map.of()), name);
            assertEquals(List.of("1|a|10", "2|b|20", "3|c|30"), rows(c), name);
            // a transaction the caller opened is joined: its rollback takes the write back
            c.setAutoCommit(false);
            answer(model, c, "|#>{w::DB.T}#->delete(r|true)", Map.of());
            assertEquals(List.of(), rows(c), name);
            c.rollback();
            c.setAutoCommit(true);
            assertEquals(List.of("1|a|10", "2|b|20", "3|c|30"), rows(c), name);
            // staged rows: the rows the let holds are the ones before the delete, its table dropped after; the
            // answer is the query's text, as a query's plan writes it
            String staged = "{age: Integer[1] |let s = #>{w::DB.T}#->filter(r|$r.AGE > $age)"
                    + "->meta::legend::lite::stage(); #>{w::DB.T}#->delete(r|true); $s->sort(~ID->ascending());}";
            String text = answer(model, c, staged, Map.of("age", 15L));
            assertEquals(List.of(), rows(c), name);
            run(c, "INSERT INTO T VALUES (2, 'b', 20), (3, 'c', 30)");
            assertEquals(read(model, c, "|#>{w::DB.T}#->sort(~ID->ascending())"), text, name);
            assertThrows(Exception.class, () -> run(c, "SELECT * FROM legend_stage_s"), name);
            // the plan read back from its JSON runs alike
            ExecutionPlan plan = com.legend.Compiler.query(com.legend.Compiler.compileModel(model), write)
                    .executionPlan("w::RT");
            run(c, "DELETE FROM T");
            assertEquals(answer(PlanJson.read(PlanJson.write(plan)), c, Map.of("age", 15L)),
                    count(model, c, "|2"), name);
            assertEquals(List.of("2|b|20", "3|c|30"), rows(c), name);
            // rows the database holds in another order than the store declares, staged by name under a let of a
            // mixed-case name (PR #67's review: Postgres had swapped the values, and made a table its reads missed)
            run(c, "CREATE TABLE REV (AGE INTEGER, NAME VARCHAR(20), ID INTEGER)");
            run(c, "INSERT INTO REV VALUES (70, 'g', 7)");
            assertEquals(count(model, c, "|1"), answer(model, c, "{|let rowsA = #>{w::DB.REV}#"
                    + "->meta::legend::lite::stage(); #>{w::DB.T}#->delete(r|true); $rowsA->write(#>{w::DB.T}#);}",
                    Map.of()), name);
            assertEquals(List.of("7|g|70"), rows(c), name);
        }
    }

    /** The plan's answer to {@code query} with {@code values}, run on the caller's session {@code c}. */
    private static String answer(String model, Connection c, String query, Map<String, ?> values) throws Exception {
        ExecutionPlan plan = com.legend.Compiler.query(com.legend.Compiler.compileModel(model), query)
                .executionPlan("w::RT");
        return answer(plan, c, values);
    }

    private static String answer(ExecutionPlan plan, Connection c, Map<String, ?> values) throws Exception {
        return run(plan, PlanSessions.of(com.legend.testbackend.TestSessions.over(c)), values);
    }

    static String run(ExecutionPlan plan, PlanSessions.Source sessions, Map<String, ?> values)
            throws Exception {
        StringWriter out = new StringWriter();
        PlanRunner.stream(plan, values, sessions, new com.legend.result.JsonRowsWriter(out, plan.answerShape()));
        return out.toString();
    }

    /** The text a query's plan answers with, for the count {@code n} ({@code |n}): what a write's count must read. */
    private static String count(String model, Connection c, String query) throws Exception {
        return read(model, c, query);
    }

    private static String read(String model, Connection c, String query) throws Exception {
        return answer(model, c, query, Map.of());
    }

    private static void run(Connection c, String sql) throws Exception {
        try (Statement st = c.createStatement()) {
            st.execute(sql);
        }
    }

    private static List<String> rows(Connection c) throws Exception {
        List<String> out = new ArrayList<>();
        try (Statement st = c.createStatement();
                ResultSet rs = st.executeQuery("SELECT ID, NAME, AGE FROM T ORDER BY ID")) {
            while (rs.next()) {
                out.add(rs.getInt(1) + "|" + rs.getString(2) + "|" + (rs.getObject(3) == null ? "null" : rs.getInt(3)));
            }
        }
        return out;
    }
}
