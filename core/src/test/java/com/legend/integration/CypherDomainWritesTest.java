// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.server.QueryService;
import com.legend.testbackend.TestBackend;
import com.legend.testbackend.TestBackends;
import com.legend.testcases.CypherAnswers;
import com.legend.testcases.CypherGraph;
import com.legend.testcases.SqlParity;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A Cypher write over a graph of a domain model (graph A, docs/WRITES_DESIGN_2026_10_18.md §8 leg 5) where the
 * domain's tables are not the harness's (PR #67's review, 2026-10-22): a column the store declares NOT NULL, which a
 * CREATE writes with its row; two classes whose instances share an id, which a write refuses by name before it writes
 * anything. On the platform's engine here; each other database the writes are measured on runs {@link #writes} in a
 * test named for it (DatabaseOwners).
 */
class CypherDomainWritesTest {

    private static CypherAnswers.GraphNode node(int id, String label, String name) {
        return new CypherAnswers.GraphNode(id, List.of(label), Map.of("name", "'" + name + "'"));
    }

    static void writes(TestBackend backend) throws Exception {
        String n = backend.name();
        // a CREATE writes its node's row with its properties: a NOT NULL column takes its value with the row (the key
        // had been inserted alone, its other columns merged after)
        CypherGraph.Inferred one = CypherGraph.infer(new CypherAnswers.Graph(List.of(node(0, "N", "a")), List.of()),
                CypherGraph.Realization.DOMAIN);
        try (Connection c = backend.openWorkspace()) {
            SqlParity.seed(backend, c, one.tables());
            SqlParity.Table t = one.tables().get(0);
            String nameColumn = t.columns().stream().map(SqlParity.Column::name)
                    .filter(x -> x.toLowerCase(java.util.Locale.ROOT).contains("name")).findFirst().orElseThrow();
            run(c, "ALTER TABLE " + t.name() + " ALTER COLUMN " + nameColumn + " SET NOT NULL");
            cypher(backend, one, c, "CREATE (:N {name: 'c'})");
            assertEquals(2L, count(c, t.name()), n);
            assertEquals(1L, count(c, t.name() + " WHERE " + nameColumn + " = 'c'"), n);
        }
        // two classes whose instances share an id: the writer names an element by its id, so a write refuses the
        // graph by name, before it writes (deleting A's would have deleted B's too)
        CypherGraph.Inferred two = CypherGraph.infer(new CypherAnswers.Graph(List.of(node(0, "A", "a"), node(1, "B",
                "b")), List.of()), CypherGraph.Realization.DOMAIN);
        try (Connection c = backend.openWorkspace()) {
            SqlParity.seed(backend, c, two.tables());
            SqlParity.Table b = two.tables().stream().filter(x -> x.rows().stream().anyMatch(r -> r.contains(1L)
                    || r.contains(1))).findFirst().orElseThrow();
            run(c, "UPDATE " + b.name() + " AS b SET id = 0");
            Exception e = assertThrows(Exception.class, () -> cypher(backend, two, c, "MATCH (n:A) DELETE n"), n);
            assertTrue(String.valueOf(e.getMessage()).contains("instances share an id"), n + ": " + e.getMessage());
            for (SqlParity.Table x : two.tables()) {
                assertEquals(1L, count(c, x.name()), n + ": " + x.name());
            }
        }
    }

    private static void cypher(TestBackend backend, CypherGraph.Inferred inferred, Connection c, String statement)
            throws Exception {
        new QueryService().execute(CypherGraph.model(inferred, backend.connectionBody()), "#Cypher{USE "
                + CypherGraph.DATASPACE + " " + statement + "}#", CypherGraph.RUNTIME, c);
    }

    private static void run(Connection c, String sql) throws Exception {
        try (Statement st = c.createStatement()) {
            st.execute(sql);
        }
    }

    private static long count(Connection c, String table) throws Exception {
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM " + table)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    @Test
    void writesOnThePlatform() throws Exception {
        writes(TestBackends.platform());
    }
}
