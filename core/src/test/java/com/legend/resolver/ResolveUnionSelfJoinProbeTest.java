// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.resolver;

import com.legend.Compiler;
import com.legend.compiler.NameResolver;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.lowering.Lowerer;
import com.legend.parser.SpecParser;
import com.legend.sql.SqlQuery;
import com.legend.sql.dialect.DuckDb;
import com.legend.testing.Rows;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Union SELF-JOIN probe (testIdentificationOfFKColumnsForUnionSelfJoin):
 * a class-typed property navigating from a union INTO THE SAME union with
 * a full per-(source-member, target-member) route matrix — the engine
 * merges the coinciding FK columns into the un-suffixed shared key. The
 * emitted SQL runs on DuckDB and its rows are asserted (Bazel workplan P3-17:
 * it printed the SQL and asserted nothing).
 */
class ResolveUnionSelfJoinProbeTest {

    private static final String UNION_FQN =
            "meta::pure::router::operations::union_OperationSetImplementation_1__SetImplementation_MANY_";

    private static final String MODEL = ("""
            Class u::Person { lastName: String[1]; manager: u::Person[0..1]; }
            ###Relational
            Database u::DB (
              Table P1 (ID INTEGER PRIMARY KEY, lastName_s1 VARCHAR(200))
              Table P2 (ID INTEGER PRIMARY KEY, lastName_s2 VARCHAR(200))
              Join person_person_join (P1.lastName_s1 = {target}.lastName_s1)
              Join person_person2_join (P1.lastName_s1 = P2.lastName_s2)
              Join person2_person_join (P2.lastName_s2 = P1.lastName_s1)
              Join person2_person2_join (P2.lastName_s2 = {target}.lastName_s2)
            )
            ###Mapping
            Mapping u::M (
              *u::Person : Operation { %s(set1, set2) }
              u::Person[set1] : Relational { ~mainTable [u::DB] P1
                lastName: P1.lastName_s1,
                manager[set1]: [u::DB] @person_person_join,
                manager[set2]: [u::DB] @person_person2_join }
              u::Person[set2] : Relational { ~mainTable [u::DB] P2
                lastName: P2.lastName_s2,
                manager[set1]: [u::DB] @person2_person_join,
                manager[set2]: [u::DB] @person2_person2_join }
            )
            ###Runtime
            Runtime u::RT { mappings: [u::M]; }
            """).formatted(UNION_FQN);

    private static String sqlOf(String query) {
        var ctx = Compiler.compileModel(MODEL);
        SpecCompiler specs = new SpecCompiler(ctx);
        List<TypedSpec> body = specs.typeQueryBody(
                NameResolver.resolveQuery(com.legend.testing.Own.spec(query)));
        List<TypedSpec> resolved = new StoreResolver(ctx, specs).resolve(body, null);
        SqlQuery plan = new Lowerer(com.legend.lowering.PlatformRegistrations.catalogTable()).lower(resolved);
        return new DuckDb().render(plan);
    }

    @Test
    @DisplayName("union self-join: per-pair route matrix into the same union")
    void unionSelfJoin() throws SQLException {
        String sql = sqlOf("u::Person.all()"
                + "->project([p|$p.lastName, p|$p.manager.lastName],"
                + " ['Name','manager'])->from(u::M, u::RT)");
        // the joins match on last name: each person's only manager is itself, through its own set's route (the
        // cross-set routes find no one with a different last name; a join that lost its condition would pair Smith
        // and Brown)
        List<List<Object>> rows = new ArrayList<>();
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:"); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE P1 (ID INTEGER PRIMARY KEY, lastName_s1 VARCHAR(200))");
            st.execute("CREATE TABLE P2 (ID INTEGER PRIMARY KEY, lastName_s2 VARCHAR(200))");
            st.execute("INSERT INTO P1 VALUES (1, 'Smith'), (3, 'Brown')");
            st.execute("INSERT INTO P2 VALUES (2, 'Jones')");
            try (ResultSet rs = st.executeQuery(sql)) {
                while (rs.next()) {
                    rows.add(java.util.Arrays.asList(rs.getString(1), rs.getString(2)));
                }
            }
        }
        Rows.assertSameRows(List.of(List.of("Smith", "Smith"), List.of("Brown", "Brown"), List.of("Jones", "Jones")), rows);
    }
}
