// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.exec.ExecutionResult;
import com.legend.server.QueryService;
import com.legend.testbackend.TestBackend;
import com.legend.testbackend.TestBackends;
import com.legend.testcases.CypherAnswers;
import com.legend.testcases.CypherGraph;
import com.legend.testcases.SqlParity;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code startNode(r)} and {@code endNode(r)} of a relationship a write created or merged, on the native graph store
 * (docs/WRITES_DESIGN_2026_10_18.md §8, leg 6): the nodes the clause wrote it between, a CREATE's by the pattern's
 * direction, a MERGE's by the stored relationship's source, which an undirected MERGE matches either way (the rows
 * derived by hand from Cypher's semantics; the TCK's one case, Merge5/[11], is a MERGE that creates).
 */
class CypherNativeEndsTest {

    private static final TestBackend PLATFORM = TestBackends.platform();

    @Test
    void theNodesAtAWrittenRelationshipsEndsAreItsStartAndItsEnd() throws Exception {
        // two nodes, a relationship from b to a
        CypherAnswers.Graph graph = new CypherAnswers.Graph(List.of(
                new CypherAnswers.GraphNode(0, List.of("N"), Map.of("name", "'a'")),
                new CypherAnswers.GraphNode(1, List.of("N"), Map.of("name", "'b'"))),
                List.of(new CypherAnswers.GraphRel(0, 1, 0, "T", Map.of())));
        CypherGraph.Inferred inferred = CypherGraph.infer(graph, CypherGraph.Realization.NATIVE);
        CypherGraph.Native store = java.util.Objects.requireNonNull(inferred.nativeStore());
        String model = CypherGraph.model(inferred, PLATFORM.connectionBody());
        try (Connection session = PLATFORM.open()) {
            SqlParity.seed(PLATFORM, session, store.tables());
            try (Statement s = session.createStatement()) {
                for (String view : store.views()) {
                    s.execute(view);
                }
            }
            String both = "MATCH (a:N {name: 'a'}), (b:N {name: 'b'}) ";
            // a CREATE's: the pattern's direction
            assertEquals(List.of("b|a"), rows(written(session, model, both + "CREATE (a)<-[r:T]-(b)"
                    + " RETURN startNode(r).name AS s, endNode(r).name AS e")));
            assertEquals(List.of("a|b"), rows(written(session, model, both + "CREATE (a)-[r:T]->(b)"
                    + " RETURN startNode(r).name AS s, endNode(r).name AS e")));
            // an undirected MERGE's: the stored relationship it matched, from b to a, whichever end the pattern
            // writes first
            assertEquals(List.of("b|a"), rows(written(session, model, both + "MERGE (a)-[r:T]-(b)"
                    + " RETURN startNode(r).name AS s, endNode(r).name AS e")));
            assertEquals(List.of("b|a"), rows(written(session, model, both + "MERGE (b)-[r:T]-(a)"
                    + " RETURN startNode(r).name AS s, endNode(r).name AS e")));
            // one it creates: outgoing from the pattern's first node
            assertEquals(List.of("a|b"), rows(written(session, model, both + "MERGE (a)-[r:T]->(b)"
                    + " RETURN startNode(r).name AS s, endNode(r).name AS e")));
            // the node itself, as a value
            List<String> node = rows(written(session, model, both + "MERGE (a)-[r:T]-(b) RETURN startNode(r) AS n"));
            assertEquals(1, node.size());
            assertTrue(node.get(0).contains("\"b\""), node.toString());
            // of null, null
            assertEquals(List.of("null"), rows(written(session, model, "RETURN startNode(null) AS n")));
        }
    }

    /** The statement's rows, in a transaction rolled back after (each write leaves the graph as it was). */
    private static ExecutionResult written(Connection session, String model, String cypher) throws Exception {
        session.setAutoCommit(false);
        try {
            // (the graph the island reads and writes: the DataSpace marked native)
            return new QueryService().execute(model, "#Cypher{USE " + CypherGraph.DATASPACE + " " + cypher + "}#",
                    CypherGraph.RUNTIME, session);
        } finally {
            session.rollback();
            session.setAutoCommit(true);
        }
    }

    private static List<String> rows(ExecutionResult result) {
        List<String> rows = new ArrayList<>();
        for (var row : result.rows()) {
            List<String> cells = new ArrayList<>();
            for (Object value : row.values()) {
                cells.add(String.valueOf(value));
            }
            rows.add(String.join("|", cells));
        }
        return rows;
    }
}
