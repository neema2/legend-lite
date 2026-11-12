// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.executionplan.ExecutionPlan;
import com.legend.result.Collector;
import com.legend.testbackend.StorelessRuntime;
import com.legend.testbackend.TestBackends;
import com.legend.testbackend.TestSessions;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A PRODUCT QUERY ON THE PROCESS'S CATALOG (L4's product half, step 1, 2026-11-10; docs/SYSTEM_CATALOG_DESIGN_2026_11_08.md
 * 4.4, the one-request model): a query that reads the metamodel alone, on an engine in this process, names the catalog --
 * the platform's rows its ONCE load, the model's own a registration run in the run's turn and rolled back with it. The
 * metamodel's tables are no longer made in the query's own database (census M6's first form, deleted).
 */
class CatalogProductTest {

    private static final String MODEL = "Class test::Person { name: String[1]; }\n";
    private static final String COUNT = "|meta::pure::metamodel::type::Class.all()->filter(c | $c.name == '%s')->size()";

    private static ExecutionPlan plan(String model, String name) {
        var type = TestBackends.platform().type();
        return com.legend.Compiler.query(com.legend.Compiler.compileModel(StorelessRuntime.with(model, type)),
                COUNT.formatted(name)).executionPlan(StorelessRuntime.RUNTIME);
    }

    private static long count(ExecutionPlan plan) throws Exception {
        try (Connection c = TestBackends.platform().open()) {
            Collector collector = new Collector();
            ProductRun.run(plan, SessionSource.given(TestSessions.over(c)), PlanSessions.of(TestSessions.over(c)),
                    ProductRun.Establish.DECLARED_SETUP, new com.legend.result.Output.Rows(collector), 1024, java.util.UUID.randomUUID());
            List<List<Object>> rows = collector.collected().rows();
            assertEquals(1, rows.size(), String.valueOf(rows));
            return ((Number) rows.get(0).get(0)).longValue();
        }
    }

    @Test
    void aMetamodelQueryNamesTheCatalogWithTheModelsRegistration() {
        ExecutionPlan.Target target = plan(MODEL, "Person").answerSql().target();
        assertInstanceOf(ExecutionPlan.Database.System.class, target.database());
        assertEquals(List.of(ExecutionPlan.Load.Cadence.ONCE, ExecutionPlan.Load.Cadence.PER_ACQUISITION),
                target.loads().stream().map(ExecutionPlan.Load::cadence).toList());
        // the registration inserts the model's rows: no table is made by it
        assertTrue(target.loads().get(1).steps().stream().noneMatch(s -> s instanceof ExecutionPlan.LoadStep.Statement st
                && st.sql().toUpperCase(java.util.Locale.ROOT).startsWith("CREATE")), String.valueOf(target.loads()));
    }

    @Test
    void theModelsClassesAreListedAndLeaveWithItsRun() throws Exception {
        assertEquals(1L, count(plan(MODEL, "Person")), "the model's class, registered in the run");
        assertEquals(1L, count(plan(MODEL, "Any")), "the platform's class, once");
        String other = "Class test::Trade { id: Integer[1]; }\n";
        assertEquals(0L, count(plan(other, "Person")), "another model's run sees none of the first's classes");
        assertEquals(1L, count(plan(other, "Trade")));
        assertEquals(1L, count(plan(MODEL, "Person")), "registered once more, never twice");
    }
}
