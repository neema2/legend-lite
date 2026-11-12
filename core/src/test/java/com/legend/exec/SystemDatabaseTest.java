// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.database.Databases;
import com.legend.executionplan.ExecutionPlan;
import com.legend.testbackend.TestBackends;
import com.legend.testbackend.TestSessions;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The process's CATALOG's leases (B4, 2026-11-11): what a target may hold, and how a run's turns end. */
class SystemDatabaseTest {

    /** A host's own load never reaches the catalog's target (the B checkpoint's tenets audit, LOW 3, 2026-11-11): a
     *  target holds its ONCE loads, then its PER_ACQUISITION ones, and refuses any other by name -- so the catalog's
     *  switch over a load's cadence (made an expression, javac holding it to every cadence, with a throwing arm for a
     *  host's own load, which a statement with no arm passed in silence) meets none. */
    @Test
    void aHostsOwnLoadIsNoTargetsLoad() {
        ExecutionPlan.Target catalog = com.legend.planner.SystemCatalog.target(com.legend.Compiler.compileModel(""),
                com.legend.planner.ConnectionPlans.type(Databases.PLATFORM));
        List<ExecutionPlan.LoadStep> steps = List.of(new ExecutionPlan.LoadStep.Statement("SELECT 1"));
        ExecutionPlan.Load host = new ExecutionPlan.Load(ExecutionPlan.Load.IDENTITY_PREFIX
                + com.legend.cache.Hash.ofUtf8(ExecutionPlan.Load.canonical(ExecutionPlan.Load.Cadence.HOST_SESSION,
                        steps)).hex(), ExecutionPlan.Load.Cadence.HOST_SESSION, steps, catalog, java.util.Map.of());
        List<ExecutionPlan.Load> loads = new ArrayList<>(catalog.loads());
        loads.add(host);
        var refused = assertThrows(IllegalArgumentException.class,
                () -> new ExecutionPlan.Target(catalog.database(), catalog.session(), loads, null, null));
        assertTrue(refused.getMessage().contains("HOST_SESSION"), refused.getMessage());
    }

    /**
     * Every turn ends and is released whatever its end throws (the B checkpoint's correctness audit, C8, 2026-11-11:
     * only a {@code DataError} was caught, so any other failure left the turns after it locked, the run's list
     * uncleared and the catalog not marked broken): each turn whose end failed marks its catalog broken, the first
     * failure is thrown with the later ones suppressed into it.
     */
    @Test
    void everyTurnEndsWhateverItsEndThrows() throws Exception {
        try (Connection a = TestBackends.platform().open(); Connection b = TestBackends.platform().open();
             Connection c = TestBackends.platform().open()) {
            SystemDatabase.Held first = new SystemDatabase.Held("a", TestSessions.over(a));
            SystemDatabase.Held second = new SystemDatabase.Held("b", TestSessions.over(b));
            SystemDatabase.Held third = new SystemDatabase.Held("c", TestSessions.over(c));
            List<SystemDatabase.Held> turns = new ArrayList<>(List.of(first, second, third));
            turns.forEach(h -> h.turn.lock());
            IllegalStateException notData = new IllegalStateException("not a DataError");
            com.legend.error.DataError data = new com.legend.error.DataError("the rollback failed", null);
            List<Sessions.Session> ended = new ArrayList<>();
            var thrown = assertThrows(IllegalStateException.class, () -> SystemDatabase.endTurns(turns, session -> {
                ended.add(session);
                if (session == first.session) {
                    throw notData;
                }
                if (session == third.session) {
                    throw data;
                }
            }));
            assertSame(notData, thrown, "the first failure");
            assertEquals(List.of(data), List.of(thrown.getSuppressed()), "the later one suppressed into it");
            assertEquals(3, ended.size(), "every turn's end tried");
            for (SystemDatabase.Held h : List.of(first, second, third)) {
                assertFalse(h.turn.isHeldByCurrentThread(), h.key + " released");
            }
            assertTrue(first.broken && third.broken, "a turn whose end failed: its catalog broken");
            assertFalse(second.broken, "a turn that ended: its catalog as it was");
            assertTrue(turns.isEmpty(), "the run's turns cleared");
        }
    }
}
