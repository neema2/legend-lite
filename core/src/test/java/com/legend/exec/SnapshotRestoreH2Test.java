// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.executionplan.ExecutionPlan;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.testbackend.TestBackends;
import org.junit.jupiter.api.Test;

import java.sql.Connection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A KEYED SNAPSHOT THE SEAM OPENS IS UNDONE WHATEVER ENDS ITS WORK (the final audit, 2026-11-12): an {@link Error} from
 * the work -- the run's wall refusing ({@code RunScope.Refused}), an {@code OutOfMemoryError} while the keyed rows
 * stream -- rolls the snapshot back and sets the session back, its auto-commit and isolation as they were and the
 * seam's transaction ended, as an exception's does; the error is told as it was. Until then only an
 * {@code IOException} or a {@code RuntimeException} did, and a kept session was left in the open transaction, every
 * later request joining it. H2: its sessions take the snapshot's isolation.
 */
class SnapshotRestoreH2Test {

    @Test
    void anErrorFromTheWorkLeavesTheSessionAsItWas() throws Exception {
        try (Connection c = TestBackends.of(DatabaseType.H2).open(); var run = com.legend.base.RunScope.enter()) {
            boolean autoCommit = c.getAutoCommit();
            int isolation = c.getTransactionIsolation();
            assertTrue(autoCommit, "a session the seam owns the transaction of");
            SessionState state = new SessionState();
            JdbcStatements st = new JdbcStatements(c, state);
            Error planted = new Error("the work's own error");
            boolean[] inside = {false};
            Error told = assertThrows(Error.class, () -> st.inSnapshot(ExecutionPlan.Keyed.Snapshot.SERIALIZABLE,
                    () -> {
                        try {
                            inside[0] = !c.getAutoCommit() && state.inOwnTransaction()
                                    && c.getTransactionIsolation() == Connection.TRANSACTION_SERIALIZABLE;
                        } catch (java.sql.SQLException e) {
                            throw new java.io.UncheckedIOException(new java.io.IOException(e));
                        }
                        throw planted;
                    }));
            assertSame(planted, told, "the work's error, told as it was");
            assertTrue(inside[0], "the work ran inside the seam's serializable snapshot");
            assertEquals(autoCommit, c.getAutoCommit(), "auto-commit as it was: no transaction left open");
            assertEquals(isolation, c.getTransactionIsolation(), "the isolation as it was");
            assertFalse(state.inOwnTransaction(), "the seam's transaction ended");
        }
    }
}
