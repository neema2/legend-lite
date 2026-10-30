// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testdatagen;

import com.legend.Compiler;
import com.legend.compiler.element.ModelContext;
import com.legend.database.Databases;
import com.legend.error.NotImplementedException;
import com.legend.exec.Sessions;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.setup.CsvSeed;
import com.legend.sql.dialect.SqlDialect;
import com.legend.testcases.ReservedNames;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * PARK-16's acceptance (docs/PARKED_WORK_LEDGER.md), closed 2026-10-16 by the dialect program's Phase 3: a test-data
 * generation over a default-schema table named {@code order}, and over a table in a schema named {@code select}, runs
 * on every database with an in-memory instance. The generator's statements are IR its session's dialect writes, so
 * each names a table as the session's queries do; until then it spelled the names raw, which DuckDB and H2 refuse.
 */
class ReservedNamesGenerationTest {

    /** {@link ReservedNames}' store, mapped; the generator reads no connection or runtime. */
    private static final String MODEL = ReservedNames.STORE + """

            ###Pure
            Class s::Order { id: Integer[1]; name: String[0..1]; }
            Class s::Item { id: Integer[1]; }
            ###Mapping
            Mapping s::M
            (
              s::Order: Relational { ~mainTable [s::DB]order id: [s::DB]order.ID, name: [s::DB]order.NAME }
              s::Item: Relational { ~mainTable [s::DB]select.T id: [s::DB]select.T.ID }
            )
            """;

    /** Each database type an in-memory session opens on: {@link Sessions#openPrivate} refuses the rest by name, and a
     *  type whose opener fails fails here, rather than leaving the test (the PR audit of 2026-10-16). */
    static List<DatabaseType> inMemory() throws SQLException {
        List<DatabaseType> out = new ArrayList<>();
        for (DatabaseType t : DatabaseType.values()) {
            try (Connection ignored = Sessions.openPrivate(t)) {
                out.add(t);
            } catch (NotImplementedException refused) {
                // no in-memory instance of this type
            }
        }
        return out;
    }

    @ParameterizedTest
    @MethodSource("inMemory")
    void generates(DatabaseType type) throws Exception {
        SqlDialect dialect = Databases.dialect(type);
        ModelContext ctx = Compiler.compileModel(MODEL);
        try (Connection c = Sessions.openPrivate(type)) {
            try (Statement s = c.createStatement()) {
                for (String sql : CsvSeed.sqls(ReservedNames.CSV, "s::DB", ctx, dialect)) {
                    s.execute(sql);
                }
            }
            TestDataGenerator.Result order = TestDataGenerator.generate(ctx,
                    query("|s::Order.all()->project([o|$o.name], ['name'])"), "s::M",
                    List.of(new TestDataGenerator.TableRowIds("default", "order",
                            List.of(new TestDataGenerator.RowId(List.of("ID"), List.of(1L))))),
                    null, false, com.legend.exec.PlanSessions.given(c), target(type), dialect, NO_VIEWS);
            assertFalse(order.sqls().isEmpty(), type.name());
            assertEquals("default\norder\nID,NAME\n1,a\n-----\n", order.dataCsvString(), type.name());
            TestDataGenerator.Result item = TestDataGenerator.generate(ctx,
                    query("|s::Item.all()->project([i|$i.id], ['id'])"), "s::M",
                    List.of(new TestDataGenerator.TableRowIds("select", "T",
                            List.of(new TestDataGenerator.RowId(List.of("ID"), List.of(7L))))),
                    null, false, com.legend.exec.PlanSessions.given(c), target(type), dialect, NO_VIEWS);
            assertEquals("select\nT\nID\n7\n-----\n", item.dataCsvString(), type.name());
        }
    }

    /** The private session's target: its database type, the one fact the runner checks a session against. */
    private static com.legend.executionplan.ExecutionPlan.Target target(DatabaseType type) {
        return new com.legend.executionplan.ExecutionPlan.Target(
                new com.legend.executionplan.ExecutionPlan.Database.Platform(type),
                new com.legend.executionplan.ExecutionPlan.Servers.Every(), List.of(), List.of());
    }

    private static LambdaFunction query(String text) {
        return (LambdaFunction) com.legend.compiler.NameResolver.resolveQuery(Compiler.parseQuery(text));
    }

    /** No view is fetched here. */
    private static final TestDataGenerator.ViewSql NO_VIEWS = (db, view, temps) -> {
        throw new AssertionError("no view in this model: " + view);
    };
}
