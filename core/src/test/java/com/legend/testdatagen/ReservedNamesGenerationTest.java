// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testdatagen;

import com.legend.Compiler;
import com.legend.compiler.element.ModelContext;
import com.legend.database.Databases;
import com.legend.error.NotImplementedException;
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
 * Since B2d.2 (2026-11-11) the generator is one planned read-only statement ({@link TestDataPlanner}), run here.
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

    /** Each database type an in-memory session opens on: {@code Sessions.openPrivate} refuses the rest by name, and a
     *  type whose opener fails fails here, rather than leaving the test (the PR audit of 2026-10-16). */
    static List<DatabaseType> inMemory() throws SQLException {
        List<DatabaseType> out = new ArrayList<>();
        for (DatabaseType t : DatabaseType.values()) {
            try (Connection ignored = com.legend.exec.Connectors.of(com.legend.planner.ConnectionPlans.type(t)).openPrivate()) {
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
        try (Connection c = com.legend.exec.Connectors.of(com.legend.planner.ConnectionPlans.type(type)).openPrivate()) {
            try (Statement s = c.createStatement()) {
                for (String sql : CsvSeed.sqls(ReservedNames.CSV, "s::DB", ctx, dialect)) {
                    s.execute(sql);
                }
            }
            List<TestDataGenerator.TableRowIds> one = List.of(new TestDataGenerator.TableRowIds("default", "order",
                    List.of(new TestDataGenerator.RowId(List.of("ID"), List.of(1L)))));
            assertEquals("default\norder\nID,NAME\n1,a\n-----\n", text(c, dialect, TestDataPlanner.plan(ctx,
                    query("|s::Order.all()->project([o|$o.name], ['name'])"), "s::M", one, null, false, PlannerViews.of(ctx))), type.name());
            List<TestDataGenerator.TableRowIds> seven = List.of(new TestDataGenerator.TableRowIds("select", "T",
                    List.of(new TestDataGenerator.RowId(List.of("ID"), List.of(7L)))));
            assertEquals("select\nT\nID\n7\n-----\n", text(c, dialect, TestDataPlanner.plan(ctx,
                    query("|s::Item.all()->project([i|$i.id], ['id'])"), "s::M", seven, null, false, PlannerViews.of(ctx))), type.name());
        }
    }

    /** {@code planned}'s text, its one statement run on {@code c}, and each fetch text run there too. */
    private static String text(Connection c, SqlDialect dialect, TestDataPlanner.Planned planned) throws SQLException {
        assertFalse(planned.fetches().isEmpty());
        for (com.legend.sql.SqlQuery fetch : planned.fetches()) {
            try (Statement s = c.createStatement(); java.sql.ResultSet r = s.executeQuery(dialect.render(fetch))) {
                while (r.next()) {
                    r.getObject(1);
                }
            }
        }
        try (Statement s = c.createStatement(); java.sql.ResultSet r = s.executeQuery(dialect.render(planned.csv()))) {
            r.next();
            return r.getString(1);
        }
    }

    private static LambdaFunction query(String text) {
        return (LambdaFunction) com.legend.compiler.NameResolver.resolveQuery(Compiler.parseQuery(text));
    }
}
