// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.executionplan.ExecutionPlan;
import com.legend.testbackend.StorelessRuntime;
import com.legend.testbackend.TestBackends;
import com.legend.testbackend.TestSessions;
import java.sql.Connection;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

/**
 * A REQUEST WAITS A BOUNDED TIME FOR WHAT ANOTHER REQUEST HOLDS (C5-J's batch, 4(b), P3, the user's ruling of
 * 2026-11-16): a kept database's session, which serves one request at a time, and the system catalog's turn, which
 * serves one run at a time, are each waited for at most {@link Sessions#WAIT} -- 30 s, legend-engine's pool's wait for a
 * connection -- and then refused by name, where a request waited for ever. Here the wait is 200 ms.
 */
class SessionWaitTest {

    private static final Duration SHORT = Duration.ofMillis(200);

    /** What {@code attempt} raised on another thread (another request's), or null. */
    private static Throwable onAnotherThread(Supplier<Object> attempt) throws Exception {
        return CompletableFuture.supplyAsync(() -> {
            try {
                var unused = attempt.get();
                return (Throwable) null;
            } catch (Throwable t) {
                return t;
            }
        }).get(20, TimeUnit.SECONDS);
    }

    @Test
    void theWaitIsLegendEnginesPools() {
        assertEquals(Duration.ofSeconds(30), Sessions.WAIT);
    }

    @Test
    void aSessionAnotherRequestHoldsIsRefusedByNameAfterTheWait() throws Exception {
        try (Connection c = TestBackends.platform().open()) {
            Sessions.Session owner = TestSessions.over(c);
            Sessions.Session held = Sessions.locked(owner, SHORT);
            try {
                Throwable refused = onAnotherThread(() -> {
                    Sessions.locked(owner, SHORT).close();
                    return null;
                });
                assertNotNull(refused, "a session another request holds is waited for, then refused");
                assertEquals("Connection is not available, request timed out after 200ms: another request holds this"
                        + " database's session", refused.getMessage());
            } finally {
                held.close();
            }
            // let go: another request takes it
            assertNull(onAnotherThread(() -> {
                Sessions.locked(owner, SHORT).close();
                return null;
            }));
        }
    }

    @Test
    void theCatalogsTurnAnotherRunHoldsIsRefusedByNameAfterTheWait() throws Exception {
        var type = TestBackends.platform().type();
        ExecutionPlan plan = com.legend.Compiler.query(com.legend.Compiler.compileModel(
                StorelessRuntime.with("Class test::Person { name: String[1]; }\n", type)),
                "|meta::pure::metamodel::type::Class.all()->size()").executionPlan(StorelessRuntime.RUNTIME);
        ExecutionPlan.Target target = plan.answerSql().target();
        assertTrue(target.database() instanceof ExecutionPlan.Database.System, "a catalog query: " + target);
        // each lease inside a run of its own, as a run takes it (a catalog's statements are a run's alone)
        Supplier<Object> anotherRun = () -> {
            try (com.legend.base.RunScope run = com.legend.base.RunScope.enter();
                    SystemDatabase.Leases other = SystemDatabase.leases(SHORT)) {
                return other.sessionFor(target);
            }
        };
        try (com.legend.base.RunScope run = com.legend.base.RunScope.enter();
                SystemDatabase.Leases holding = SystemDatabase.leases(SHORT)) {
            holding.sessionFor(target);
            Throwable refused = onAnotherThread(anotherRun);
            assertNotNull(refused, "the catalog's turn another run holds is waited for, then refused");
            assertEquals("Connection is not available, request timed out after 200ms: another request holds the system"
                    + " catalog's turn", refused.getMessage());
        }
        // released: another run takes it
        assertNull(onAnotherThread(anotherRun));
    }
}
