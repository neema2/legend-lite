// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.database.Databases;
import com.legend.error.NotImplementedException;
import com.legend.exec.Sessions;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.testbackend.TestBackend;
import com.legend.testbackend.TestBackends;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The tests' databases (the dialect program's Phase 6, 2026-10-16): {@code Databases.implemented()} is every type the
 * platform gives a dialect, each has a test backend of its own type and name, and an in-process backend's session is
 * on its own database. A server's or a cloud's session is its lane's to open (this test starts no server).
 */
class TestBackendsTest {

    @Test
    void theImplementedDatabasesAreThoseWithADialect() {
        Set<DatabaseType> withDialect = new HashSet<>();
        for (DatabaseType type : DatabaseType.values()) {
            try {
                Databases.dialect(type);
                withDialect.add(type);
            } catch (NotImplementedException none) {
                // no dialect: not implemented
            }
        }
        assertEquals(withDialect, Set.copyOf(Databases.implemented()));
    }

    @Test
    void eachImplementedDatabaseHasABackendOfItsOwn() {
        Set<String> names = new HashSet<>();
        for (DatabaseType type : Databases.implemented()) {
            TestBackend backend = TestBackends.of(type);
            assertEquals(type, backend.type());
            assertTrue(names.add(backend.name()), backend.name());
            assertEquals(backend.name().toLowerCase(java.util.Locale.ROOT), backend.name());
            assertSame(backend, TestBackends.named(backend.name()));
            assertEquals(Sessions.inProcess(type), backend.inProcess(), backend.name());
        }
        assertEquals(Databases.implemented(), TestBackends.all().stream().map(TestBackend::type).toList());
        assertSame(TestBackends.of(Databases.PLATFORM), TestBackends.platform());
        assertSame(TestBackends.of(Databases.REPLAY_REFEREE), TestBackends.referee());
    }

    @Test
    void aDatabaseTheTestsDoNotRunOnIsRefused() {
        for (DatabaseType type : DatabaseType.values()) {
            if (!Databases.implemented().contains(type)) {
                assertThrows(NotImplementedException.class, () -> TestBackends.of(type), type.toString());
            }
        }
        IllegalStateException unknown = assertThrows(IllegalStateException.class, () -> TestBackends.named("nosuch"));
        assertTrue(unknown.getMessage().contains(TestBackends.platform().name()), unknown.getMessage());
    }

    @Test
    void anInProcessSessionIsOnItsOwnDatabase() throws Exception {
        List<TestBackend> inProcess = TestBackends.all().stream().filter(TestBackend::inProcess).toList();
        assertTrue(inProcess.contains(TestBackends.platform()));
        for (TestBackend backend : inProcess) {
            try (var session = backend.openSession()) {
                Sessions.check(session, backend.type());
            }
            assertThrows(IllegalStateException.class, backend::openSystem, backend.name());
            assertTrue(backend.withStorelessRuntime("").contains("Runtime"), backend.name());
        }
    }
}
