// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.testing.LaneFloor;

import com.legend.testing.Lean;

import com.legend.exec.PlanRunner;
import com.legend.exec.PlanSessions;
import com.legend.executionplan.ExecutionPlan;
import com.legend.json.Json;
import com.legend.testbackend.TestBackend;
import com.legend.testcases.CypherAnswers;
import com.legend.testcases.CypherGraph;
import com.legend.testcases.SqlParity;
import org.junit.jupiter.api.Test;

import java.io.StringWriter;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A Cypher statement that updates the native graph store, run from its plan (docs/WRITES_DESIGN_2026_10_18.md §8, leg
 * 7): staged rows, the store writes, the guards and the rows a RETURN projects, every step on one session in one
 * transaction, answering as the statement's execution does -- its rows, and the graph it leaves. Each statement runs
 * both ways in a transaction rolled back after, so each starts from the graph as seeded.
 */
@org.junit.jupiter.api.extension.ExtendWith(LaneFloor.class)
class CypherWritePlansTest {

    private static final TestBackend PLATFORM = LaneFloor.backend();

    /** The graph each statement starts from: two nodes, a relationship from b to a. */
    private static final CypherAnswers.Graph GRAPH = new CypherAnswers.Graph(List.of(
            new CypherAnswers.GraphNode(0, List.of("N"), Map.of("name", "'a'", "num", "1")),
            new CypherAnswers.GraphNode(1, List.of("N"), Map.of("name", "'b'", "num", "2"))),
            List.of(new CypherAnswers.GraphRel(0, 1, 0, "T", Map.of())));

    @Test
    void aWriteStatementsPlanAnswersAsItsExecution() throws Exception {
        CypherGraph.Inferred inferred = CypherGraph.infer(GRAPH, CypherGraph.Realization.NATIVE);
        CypherGraph.Native store = java.util.Objects.requireNonNull(inferred.nativeStore());
        String model = CypherGraph.model(inferred, PLATFORM.connectionBody());
        try (Connection session = PLATFORM.open()) {
            SqlParity.seed(PLATFORM, session, store.tables());
            try (Statement s = session.createStatement()) {
                for (String view : store.views()) {
                    s.execute(view);
                }
            }
            for (String cypher : List.of(
                    "MATCH (a:N {name: 'a'}), (b:N {name: 'b'}) CREATE (a)-[r:T]->(b)"
                            + " RETURN startNode(r).name AS s, endNode(r).name AS e",
                    "CREATE (n:N {name: 'c', num: 3}) RETURN n.name AS name, n.num AS num",
                    "MATCH (n:N {name: 'a'}) SET n.num = 5 RETURN n.name AS name, n.num AS num",
                    "MATCH (n:N) WHERE n.num > 1 SET n:M REMOVE n.num RETURN n.name AS name",
                    "MATCH (n:N {name: 'b'}) DETACH DELETE n",
                    "MERGE (n:N {name: 'z'}) ON CREATE SET n.num = 9 RETURN n.num AS num",
                    "MERGE (n:N {name: 'a'}) ON MATCH SET n.num = 7 RETURN n.num AS num",
                    "UNWIND [1, 2, 3] AS i CREATE (:K {i: i})",
                    "MATCH (a:N {name: 'a'}), (b:N {name: 'b'}) MERGE (a)-[r:T]-(b) RETURN startNode(r).name AS s")) {
                Answer executed = inTransaction(session, () -> new Answer(executed(session, model, cypher),
                        graph(session, model)));
                Answer planned = inTransaction(session, () -> new Answer(planned(session, model, cypher),
                        graph(session, model)));
                assertEquals(executed, planned, cypher);
                // (never vacuous: a RETURN answers rows, and the graph's dump holds a node -- the dump reads the
                // seeded graph's classes, so a node relabelled to a label set it lacks is not in it)
                assertTrue(!cypher.contains("RETURN") || !planned.rows().isEmpty(), cypher);
                assertTrue(!planned.graph().isEmpty(), cypher + ": " + planned.graph());
            }
            assertEquals(List.of("a|b"), inTransaction(session, () -> planned(session, model,
                    "MATCH (a:N {name: 'a'}), (b:N {name: 'b'}) CREATE (a)-[r:T]->(b)"
                            + " RETURN startNode(r).name AS s, endNode(r).name AS e")));
            // an undirected MERGE matches the stored relationship, from b to a
            assertEquals(List.of("b"), inTransaction(session, () -> planned(session, model,
                    "MATCH (a:N {name: 'a'}), (b:N {name: 'b'}) MERGE (a)-[r:T]-(b) RETURN startNode(r).name AS s")));
            // Neo4j's DeleteConnectedNode, raised by the database in the plan's guard as in the execution
            String connected = "MATCH (n:N {name: 'a'}) DELETE n";
            Exception byExecution = assertThrows(Exception.class, () -> inTransaction(session,
                    () -> executed(session, model, connected)));
            Exception byPlan = assertThrows(Exception.class, () -> inTransaction(session,
                    () -> planned(session, model, connected)));
            assertEquals(byExecution.getMessage(), byPlan.getMessage());
            assertTrue(String.valueOf(byPlan.getMessage()).contains("Cannot delete node"), byPlan.getMessage());
            // over upstream's execute (the server's path), a write with no RETURN runs from its plan and answers its
            // empty relation
            String detach = "MATCH (n:N {name: 'b'}) DETACH DELETE n";
            StringWriter answer = new StringWriter();
            List<String> after = inTransaction(session, () -> {
                com.legend.TypedQuery typed = com.legend.Compiler.query(com.legend.Compiler.compileModel(model),
                        com.legend.Compiler.parseQuery(island(detach)));
                com.legend.executionplan.ExecutionPlan ran = com.legend.Execution.run(typed, CypherGraph.RUNTIME,
                        (target, ctx) -> {
                            throw new AssertionError("a program that writes opens no query session");
                        }, PlanSessions.of(com.legend.testbackend.TestSessions.over(session)), com.legend.Execution.Establish.DECLARED_SETUP,
                        plan -> new com.legend.result.JsonRowsWriter(answer, plan.answerShape()));
                assertEquals(com.legend.executionplan.ExecutionPlan.TdsResult.Shape.RELATION, ran.answerShape(), detach);
                return graph(session, model);
            });
            assertEquals(List.of(), ((Json.Arr) Json.parse(answer.toString())).items(), answer.toString());
            assertEquals(inTransaction(session, () -> {
                planned(session, model, detach);
                return graph(session, model);
            }), after, detach);
        }
    }

    /** A native store whose {@code nodes} does not declare {@code id} its primary key: a CREATE is refused by name,
     *  before anything runs, since two writers at once could mint one id (PR #67's second review, 2026-10-24). */
    @Test
    void aStoreWhoseIdsAreNoKeyRefusesACreate() throws Exception {
        CypherGraph.Inferred inferred = CypherGraph.infer(GRAPH, CypherGraph.Realization.NATIVE);
        String keyed = CypherGraph.model(inferred, PLATFORM.connectionBody());
        String model = keyed.replace("Table nodes (id BIGINT PRIMARY KEY,", "Table nodes (id BIGINT,");
        assertTrue(!model.equals(keyed), "the store's nodes declared no key to take away");
        try (Connection session = PLATFORM.open()) {
            Exception e = assertThrows(Exception.class, () -> executed(session, model, "CREATE (:N {name: 'c'})"));
            assertTrue(String.valueOf(e.getMessage()).contains("does not declare id its primary key"),
                    String.valueOf(e.getMessage()));
        }
    }

    /** A statement's rows, each row's cells as text, and the graph after it (its dump's rows). */
    private record Answer(List<String> rows, List<String> graph) {
    }

    @FunctionalInterface
    private interface Work<T> {
        T run() throws Exception;
    }

    /** {@code work} in a transaction rolled back after (each statement leaves the graph as it was). */
    private static <T> T inTransaction(Connection session, Work<T> work) throws Exception {
        session.setAutoCommit(false);
        try {
            return work.run();
        } finally {
            session.rollback();
            session.setAutoCommit(true);
        }
    }

    /** The statement's rows by its execution, read as the twin reads lite's JSON rows ({@link Lean#run}). */
    private static List<String> executed(Connection session, String model, String cypher) throws Exception {
        Lean.Result r = Lean.run(model, island(cypher), CypherGraph.RUNTIME, session);
        List<String> rows = new ArrayList<>();
        for (var row : r.rows()) {
            List<String> cells = new ArrayList<>();
            for (Object value : row) {
                cells.add(String.valueOf(value));
            }
            rows.add(String.join("|", cells));
        }
        rows.sort(null);
        return rows;
    }

    /** The statement's rows by its plan, run on {@code session} (its transaction joined): its typed rows, each row's
     *  cells as text in the relation's order (as {@link #executed} spells the execution's). */
    private static List<String> planned(Connection session, String model, String cypher) throws Exception {
        ExecutionPlan plan = com.legend.Compiler.query(com.legend.Compiler.compileModel(model), island(cypher))
                .executionPlan(CypherGraph.RUNTIME);
        assertTrue(plan.root() instanceof ExecutionPlan.Sequence, cypher);
        List<String> rows = new ArrayList<>();
        for (List<Object> row : PlanRunner.rows(plan, Map.of(), PlanSessions.of(com.legend.testbackend.TestSessions.over(session))).rows()) {
            List<String> cells = new ArrayList<>();
            for (Object value : row) {
                cells.add(String.valueOf(value));
            }
            rows.add(String.join("|", cells));
        }
        rows.sort(null);
        return rows;
    }

    /** The graph as the statement left it: each node's labels and properties, each relationship's type. */
    private static List<String> graph(Connection session, String model) throws Exception {
        List<String> out = new ArrayList<>(executed(session, model,
                "MATCH (n) RETURN labels(n) AS l, n.name AS name, n.num AS num, n.i AS i"));
        out.addAll(executed(session, model, "MATCH (a)-[r]->(b) RETURN a.name AS a, type(r) AS t, b.name AS b"));
        return out;
    }

    private static String island(String cypher) {
        return "#Cypher{USE " + CypherGraph.DATASPACE + " " + cypher + "}#";
    }
}
