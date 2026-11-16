// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.executionplan.ExecutionPlan;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.model.ConnectionSpecification;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * A DATABASE THE USER'S DECLARATION REACHES SERVES ONE REQUEST AT A TIME on the server's road (C5-J's batch, 4(a),
 * 2026-11-16). The server runs its requests concurrently since then; each request on a user-named in-memory database, or
 * on a URL's database whose target loads data on acquisition, opened a fresh connection with no lock, so one request's
 * acquisition loads (a drop, a make, the rows) ran under another's reads. Each such database is now one kept connection
 * behind its lock, as the unnamed in-memory database already was: a second request waits until the first closes.
 */
class UsersH2DatabaseRequestsTest {

    private static ExecutionPlan.Target target(ConnectionSpecification spec, String project,
            List<ExecutionPlan.Load> loads, ExecutionPlan.@com.legend.base.Nullable DatabaseSettings settings) {
        return new ExecutionPlan.Target(new ExecutionPlan.Database.Declared(com.legend.planner.ConnectionPlans.of(
                PlanRunnerTest.inMemory("s::Users", DatabaseType.H2, spec)), new ExecutionPlan.Project(project, null)),
                List.of(), loads, null, settings, null);
    }

    /** {@code second} is held until {@code first} closes, then opens. */
    private static void servedOneAtATime(ExecutionPlan.Target first, ExecutionPlan.Target second) throws Exception {
        Sessions.Session held = PlanSessions.projectDatabase(first);
        // the second request opens and closes on its own thread, as a request does
        CompletableFuture<Void> waiting = CompletableFuture.runAsync(() -> {
            try (Sessions.Session s = PlanSessions.projectDatabase(second)) {
                assertTrue(s != null);
            }
        });
        try {
            Thread.sleep(300);
            assertFalse(waiting.isDone(), "a second request waits while the first holds the database");
        } finally {
            held.close();
        }
        waiting.get(10, TimeUnit.SECONDS);
    }

    /** The user's named in-memory database, whichever project reads it. */
    @Test
    void aDatabaseTheUserNamedServesOneRequestAtATime() throws Exception {
        ConnectionSpecification spec = new ConnectionSpecification.EmbeddedH2("c5j_users_own", "/never", false);
        var settings = com.legend.testbackend.TestBackends.settings(DatabaseType.H2);
        servedOneAtATime(target(spec, "p1", List.of(), settings), target(spec, "p1", List.of(), settings));
        servedOneAtATime(target(spec, "p1", List.of(), settings), target(spec, "p2", List.of(), settings));
    }

    /** A URL's database whose target establishes data on each acquisition; one that loads nothing opens as before. */
    @Test
    void aUrlsDatabaseThatLoadsOnAcquisitionServesOneRequestAtATime() throws Exception {
        String dir = System.getenv().getOrDefault("TEST_TMPDIR", System.getProperty("java.io.tmpdir"));
        ConnectionSpecification spec = new ConnectionSpecification.LocalFile(
                java.nio.file.Path.of(dir, "c5j_url_db").toString());
        ExecutionPlan.Target bare = target(spec, "p1", List.of(), ExecutionPlan.DatabaseSettings.THE_DATABASES_OWN);
        ExecutionPlan.Target loading = com.legend.planner.LoadPlanner.withLoads(bare,
                List.of(new ExecutionPlan.LoadStep.Statement("CREATE TABLE IF NOT EXISTS T (A INT)")));
        assertFalse(loading.loads().isEmpty());
        servedOneAtATime(loading, loading);
        // a target that loads nothing reads the user's database on a connection of its own, as it did
        try (Sessions.Session a = PlanSessions.projectDatabase(bare);
                Sessions.Session b = PlanSessions.projectDatabase(bare)) {
            assertFalse(Sessions.identity(a).equals(Sessions.identity(b)), "two connections of their own");
        }
    }
}
