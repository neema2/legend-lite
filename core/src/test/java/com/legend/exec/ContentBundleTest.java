// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.executionplan.ExecutionPlan;
import com.legend.executionplan.PlanJson;
import com.legend.result.Collector;
import com.legend.testbackend.TestBackends;
import com.legend.testbackend.TestSessions;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE PLAN'S BUNDLE (docs/DATA_IN_THE_PLAN_2026_11_09.md L6c, 2026-11-10, decision 1): the model's text above the inline
 * threshold travels beside the plan, one blob per digest, never in it. A plan written out and read back on a fresh
 * runner loads the same rows; a blob whose bytes are not its digest, and a bundle that lacks one, are refused by name.
 */
class ContentBundleTest {

    private static final String MODEL = """
            ###Pure
            Class store::Marker {}
            ###Relational
            Database store::Db ( Table T_X (id INTEGER PRIMARY KEY) )
            ###Connection
            RelationalDatabaseConnection store::Conn
            {
                store: store::Db;
                type: DuckDB;
                specification: DuckDB { };
                auth: Test;
            }
            ###Runtime
            Runtime test::RT
            {
                mappings: [];
                connections: [ store::Db: [ env: store::Conn ] ];
            }
            """;

    /** A data: payload of {@code n} elements, each with a text cell: well past the 16 KB inline threshold at 2,000. */
    private static String query(int n) {
        StringBuilder payload = new StringBuilder("[");
        for (int i = 0; i < n; i++) {
            payload.append(i == 0 ? "" : ",").append("{\"a\":").append(i).append(",\"b\":\"row number ").append(i)
                    .append("\"}");
        }
        payload.append(']');
        return "sourceUrl('data:application/json," + payload + "')->extend(~[a:r|cast(get($r.data, 'a'),"
                + " @meta::pure::metamodel::type::Integer)])->select(~[a])->from(test::RT)";
    }

    private static ExecutionPlan plan(int n) {
        return com.legend.Compiler.query(com.legend.Compiler.compileModel(MODEL), query(n)).executionPlan("test::RT");
    }

    /** {@code plan}'s rows on a fresh connection of the platform's database. */
    private static List<List<Object>> rows(ExecutionPlan plan) throws Exception {
        try (Connection c = TestBackends.platform().open()) {
            Collector collector = new Collector();
            ProductRun.run(plan, SessionSource.given(TestSessions.over(c)), PlanSessions.of(TestSessions.over(c)),
                    ProductRun.Establish.DECLARED_SETUP, new com.legend.result.Output.Rows(collector), 1024, java.util.UUID.randomUUID());
            return collector.collected().rows();
        }
    }

    @Test
    void aPlanWrittenOutAndReadBackLoadsTheSameRows() throws Exception {
        ExecutionPlan planned = plan(2000);
        PlanJson.Bundle bundle = PlanJson.writeBundle(planned);
        // the plan's JSON holds no payload: one blob, the rows, beside it
        assertFalse(bundle.json().contains("row number 1999"), "no payload in the plan's JSON");
        assertEquals(1, bundle.blobs().size(), String.valueOf(bundle.blobs().keySet()));
        assertTrue(bundle.blobs().values().iterator().next().contains("row number 1999"));
        ExecutionPlan read = PlanJson.read(bundle.json(), bundle.blobs());
        List<List<Object>> again = rows(read);
        assertEquals(2000, again.size());
        assertEquals(rows(planned), again);
        // a plan written whole carries its bundle inline and reads back as it was
        assertEquals(PlanJson.write(planned), PlanJson.write(PlanJson.read(PlanJson.write(planned))));
    }

    @Test
    void aBlobWhoseBytesAreNotItsDigestIsRefused() {
        PlanJson.Bundle bundle = PlanJson.writeBundle(plan(2000));
        Map<String, String> tampered = new HashMap<>();
        bundle.blobs().forEach((digest, text) -> tampered.put(digest, text.replaceFirst("row number 7", "row number X")));
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> PlanJson.read(bundle.json(), tampered));
        assertTrue(e.getMessage().contains("not their digest"), e.getMessage());
    }

    @Test
    void aBundleThatLacksADigestIsRefused() {
        PlanJson.Bundle bundle = PlanJson.writeBundle(plan(2000));
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> PlanJson.read(bundle.json(), Map.of()));
        // the payload is its statement's bound parameter, its text the statement's content (the JSON pass, 2026-11-11)
        assertTrue(e.getMessage().contains("which its content lacks"), e.getMessage());
    }

    @Test
    void rowsBelowTheThresholdRideInline() {
        PlanJson.Bundle bundle = PlanJson.writeBundle(plan(10));
        assertTrue(bundle.blobs().isEmpty(), "no blob below the threshold");
    }
}
