// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.testbackend.TestBackends;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A CATALOG READ THAT FAILS INSIDE THE CATALOG'S TURN (the B4 audit's finding 2, 2026-11-11): a test body's statements
 * on the process's catalog run inside its turn, a transaction rolled back when the run ends; a statement an assert
 * expects to fail ({@code assertError}) fails inside that transaction, and the body's next catalog read must still be
 * answered -- on every database the platform runs in process, and on a server, whose body reads the platform's own
 * engine's catalog (the server matrix's test calls {@link #assertPasses}).
 */
class CatalogTurnFailureTest {

    /** A guarded catalog read that fails at run (the model's classes counted, divided by none), then another catalog
     *  read of the model's own class -- a row the run's registration holds. */
    static final String BODY = "{| assertError(|meta::pure::metamodel::type::Class.all()->filter(c|$c.name == 'P')"
            + "->size() / (meta::pure::metamodel::type::Class.all()->filter(c|$c.name == 'Nope')->size()),"
            + " 'Division by zero');"
            + " assertEquals(1, meta::pure::metamodel::type::Class.all()->filter(c|$c.name == 'P')->size());}";

    /** {@link #BODY}'s asserts beside a read of the user's table: each catalog assert routed to the catalog beside the
     *  body's own database (per-statement routing). */
    static final String MIXED = "{| assertError(|meta::pure::metamodel::type::Class.all()->filter(c|$c.name == 'P')"
            + "->size() / (meta::pure::metamodel::type::Class.all()->filter(c|$c.name == 'Nope')->size()),"
            + " 'Division by zero');"
            + " assertSize(x::P.all(), 2);"
            + " assertEquals(1, meta::pure::metamodel::type::Class.all()->filter(c|$c.name == 'P')->size());}";

    static List<DatabaseType> inProcess() {
        return TestBackends.inProcess().stream().map(b -> b.type()).distinct().toList();
    }

    @ParameterizedTest
    @MethodSource("inProcess")
    void aFailedGuardedCatalogReadLeavesTheNextOneAnswered(DatabaseType db, @TempDir Path dir) throws Exception {
        assertPasses(alone(db), db, dir);
    }

    @ParameterizedTest
    @MethodSource("inProcess")
    void aFailedGuardedCatalogReadBesideTheBodysOwnLeavesTheNextOneAnswered(DatabaseType db, @TempDir Path dir)
            throws Exception {
        assertPasses(mixed(db), db, dir);
    }

    /** {@link #BODY} on {@code db}, reading the catalog alone (a system-routed body). */
    static DataDeliveryMatrixTest.Data alone(DatabaseType db) {
        return new DataDeliveryMatrixTest.Data(DataDeliveryMatrixTest.model(DataDeliveryMatrixTest.bareConnection(db),
                ""), BODY, List.of(List.of("ok")), "x::RT");
    }

    /** {@link #MIXED} on {@code db}, the user's table its test data's two rows (the matrix's CSV test data cell's
     *  model). */
    static DataDeliveryMatrixTest.Data mixed(DatabaseType db) throws java.io.IOException {
        String model = DataDeliveryMatrixTest.data(DataDeliveryMatrixTest.Kind.CSV_TEST_DATA, db,
                new DataDeliveryMatrixTest.Env(Path.of("unused"))).model();
        return new DataDeliveryMatrixTest.Data(model, MIXED, List.of(List.of("ok")), "x::RT");
    }

    /** {@code data}'s body on {@code db}: every assert passes. */
    static void assertPasses(DataDeliveryMatrixTest.Data data, DatabaseType db, Path dir) throws Exception {
        try (java.sql.Connection c = DataDeliveryMatrixTest.session(db)) {
            assertEquals(data.rows(), DataDeliveryMatrixTest.testBodyOn(c, data, dir), db + ": " + data.query());
        }
    }
}
