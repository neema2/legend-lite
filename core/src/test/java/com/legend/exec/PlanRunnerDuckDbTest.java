// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.executionplan.ExecutionPlan;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.model.ConnectionSpecification;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.legend.exec.PlanRunnerTest.inMemory;
import static com.legend.exec.PlanRunnerTest.plan;
import static com.legend.exec.PlanRunnerTest.run;
import static com.legend.exec.PlanRunnerTest.statement;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The runner on DuckDB ({@link PlanRunnerTest}'s case that is DuckDB's own; moved here from it by the dialect program's
 * Phase 6, 2026-10-16): rows set up as the plan writes every model's data since A2 (2026-11-10), a content blob of
 * inserts (until then through its bulk loader, the Appender).
 */
class PlanRunnerDuckDbTest {

    /** A target declaring settings on DuckDB, whose database takes none (its connector's {@code takesSettings}), is
     *  refused by name where a session is checked against it (the C final audit's J4, 2026-11-12), as an H2 target
     *  declaring none is ({@code DatabaseSettingsH2Test}). */
    @Test
    void aTargetDeclaringSettingsOnDuckDbIsRefusedByName() throws Exception {
        ExecutionPlan.Target duck = com.legend.planner.Targets.of(new ExecutionPlan.Database.Platform(
                com.legend.planner.ConnectionPlans.type(DatabaseType.DuckDB), null), new com.legend.sql.dialect.DuckDb());
        ExecutionPlan.DatabaseSettings settings = new ExecutionPlan.DatabaseSettings(
                List.of(new ExecutionPlan.Setting("MODE", "LEGACY")), "SELECT 1 WHERE FALSE");
        ExecutionPlan.Target withSettings = new ExecutionPlan.Target(duck.database(), duck.session(), List.of(), null,
                settings, null);
        try (java.sql.Connection c = com.legend.testbackend.TestBackends.of(DatabaseType.DuckDB).open();
                var run = com.legend.base.RunScope.enter()) {
            JdbcStatements st = new JdbcStatements(c, new SessionState());
            com.legend.error.NotImplementedException refused = org.junit.jupiter.api.Assertions.assertThrows(
                    com.legend.error.NotImplementedException.class, () -> st.check(withSettings));
            assertEquals("a plan's target on DuckDB declares no settings, and this one declares"
                    + " [Setting[name=MODE, value=LEGACY]]: never run on other semantics", refused.getMessage());
        }
    }

    /** Two targets are one database only when their content is equal, cell for cell: a cell holding a comma and a
     *  null cell are spelled apart from their look-alikes (the audit's S4: a key of the targets' printed text gave
     *  {@code ["a, b", "c"]} and {@code ["a", "b, c"]} one database). */
    @Test
    void aTargetIsKeyedByItsWholeContent_soLookAlikeCellsNeverShareADatabase() throws Exception {
        var duck = new ExecutionPlan.Database.Declared(com.legend.planner.ConnectionPlans.of(inMemory("s::Duck", DatabaseType.DuckDB,
                new ConnectionSpecification.InMemory())), null);
        String read = "SELECT string_agg(COALESCE(A, '<none>') || '/' || COALESCE(B, '<none>'), ';') FROM K";
        Map<List<String>, String> rows = new java.util.LinkedHashMap<>();
        rows.put(List.of("a, b", "c"), "a, b/c");
        rows.put(List.of("a", "b, c"), "a/b, c");
        rows.put(Arrays.asList(null, "x"), "<none>/x");
        rows.put(List.of("null", "x"), "null/x");
        for (var e : rows.entrySet()) {
            com.legend.setup.RowLoad load = new com.legend.setup.RowLoad(null, "K", com.legend.sql.NameQuoting.BARE,
                    List.of("A", "B"), java.util.Set.of(), 2, List.of(e.getKey()));
            ExecutionPlan p = plan(read, com.legend.planner.LoadPlanner.withLoads(new ExecutionPlan.Target(duck,
                    List.of(), List.of(), null, null, null), com.legend.setup.LoadSteps.of(List.of(statement(
                            "CREATE TABLE K (A VARCHAR, B VARCHAR)"))).then(com.legend.setup.LoadSteps.rows(load,
                            com.legend.database.Databases.dialect(DatabaseType.DuckDB)))));
            assertEquals(e.getValue(), run(p, PlanSessions.shared()), String.valueOf(e.getKey()));
        }
    }
}
